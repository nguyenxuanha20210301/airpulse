"""load the yaml settings of a profile and apply environment overrides.

the base file holds values shared by every profile, a profile file names the file it extends and
overrides single keys, and any variable named AIRPULSE_<section>__<key> overrides the merged
result. nested keys are joined with a double underscore because key names contain underscores.
"""

from __future__ import annotations

import copy
import csv
import os
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

ENV_PREFIX = "AIRPULSE_"
NESTED_SEP = "__"
# these names select the profile or the config path, they are not config keys.
RESERVED_ENV_KEYS = frozenset({"profile", "config"})
DEFAULT_PROFILE = "dev"
EXTENDS_KEY = "extends"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_DIR = REPO_ROOT / "configs"


def _split_commas(value: Any) -> Any:
    # environment variables are plain strings, so list fields accept a comma separated form.
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


StrList = Annotated[list[str], BeforeValidator(_split_commas)]


class StrictModel(BaseModel):
    # unknown keys are rejected so a typo in yaml or in an env name fails at startup.
    model_config = ConfigDict(extra="forbid")


class LocationsConfig(StrictModel):
    file: str


class OpenMeteoConfig(StrictModel):
    weather_url: str
    air_quality_url: str
    archive_url: str
    timeout_s: int = 30
    locations_per_request: int = Field(default=10, ge=1)
    weather_variables: StrList
    air_variables: StrList


class KeyedSourceConfig(StrictModel):
    base_url: str
    timeout_s: int = 30
    api_key: str = ""


class OpenAqConfig(KeyedSourceConfig):
    # stations within radius_m of a sampling point are attached to it, nearest first.
    radius_m: int = Field(default=25000, ge=100)
    max_stations: int = Field(default=3, ge=1)


class OpenWeatherMapConfig(KeyedSourceConfig):
    calls_per_minute: int = Field(default=50, ge=1)


class SourcesConfig(StrictModel):
    open_meteo: OpenMeteoConfig
    openaq: OpenAqConfig
    openweathermap: OpenWeatherMapConfig


class IngestionConfig(StrictModel):
    enabled_sources: StrList
    interval_minutes: int = Field(ge=1)
    past_hours: int = Field(ge=1)
    history_start: date


class KafkaTopics(StrictModel):
    weather_raw: str
    air_raw: str
    weather_clean: str
    air_clean: str
    air_alerts: str


class KafkaConfig(StrictModel):
    bootstrap_servers: str
    partitions: int = Field(ge=1)
    replication_factor: int = Field(ge=1)
    consumer_group_prefix: str
    topics: KafkaTopics


class HdfsConfig(StrictModel):
    url: str
    root: str
    replication: int = Field(ge=1)

    def dataset_path(self, layer: str, dataset: str) -> str:
        return f"{self.url}{self.root}/{layer}/{dataset}"


class CassandraTables(StrictModel):
    air_latest: str
    weather_latest: str
    air_windows: str
    weather_windows: str
    alerts: str
    forecast: str


ConsistencyLevel = Literal[
    "ANY", "ONE", "TWO", "THREE", "QUORUM", "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE", "ALL"
]


class CassandraConfig(StrictModel):
    hosts: StrList
    port: int = 9042
    keyspace: str
    replication_factor: int = Field(ge=1)
    consistency: ConsistencyLevel
    tables: CassandraTables


class HiveConfig(StrictModel):
    metastore_uri: str = ""
    # relative to the hdfs root, where managed tables and schemas created by trino live.
    warehouse_dir: str = "warehouse"
    ddl_file: str = "schemas/hive/tables.sql"


class TrinoConfig(StrictModel):
    url: str
    hive_catalog: str
    cassandra_catalog: str
    schema_name: str = Field(alias="schema")
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class SparkConfig(StrictModel):
    master: str = ""
    driver_memory: str
    executor_instances: int = Field(ge=1)
    executor_memory: str
    executor_cores: int = Field(ge=1)
    shuffle_partitions: int = Field(ge=1)
    use_packages: bool = True
    packages: StrList = []


class StreamingConfig(StrictModel):
    trigger_interval: str
    watermark_delay: str
    windows: StrList
    slide: str
    checkpoint_dir: str


class AqiConfig(StrictModel):
    standard: Literal["us_epa", "vn_aqi"]
    pm2_5_daily_limit_who: float
    pm2_5_daily_limit_qcvn: float


class MlConfig(StrictModel):
    max_iter: int = Field(ge=1)
    max_depth: int = Field(ge=1)
    test_days: int = Field(ge=1)
    min_rows: int = Field(ge=1)


class AlertsConfig(StrictModel):
    aqi_level: int
    temperature_hot_c: float
    temperature_cold_c: float


class ReplayConfig(StrictModel):
    speed_factor: float = Field(gt=0)
    pacing: bool = True
    shift_to_now: bool = False
    topic_suffix: str
    start_date: date | None = None
    end_date: date | None = None


class Config(StrictModel):
    project: str
    profile: str
    schema_version: int
    timezone: str
    log_level: str = "INFO"
    config_dir: Path
    locations: LocationsConfig
    sources: SourcesConfig
    ingestion: IngestionConfig
    kafka: KafkaConfig
    hdfs: HdfsConfig
    cassandra: CassandraConfig
    hive: HiveConfig = Field(default_factory=HiveConfig)
    trino: TrinoConfig
    spark: SparkConfig
    streaming: StreamingConfig
    aqi: AqiConfig
    ml: MlConfig
    alerts: AlertsConfig
    replay: ReplayConfig

    def resolve_path(self, relative: str | Path) -> Path:
        # relative entries in the yaml files are resolved against the directory holding them.
        path = Path(relative)
        return path if path.is_absolute() else self.config_dir / path


class Location(StrictModel):
    location_id: str
    name: str
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    region: str
    province: str


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """return a new dict where nested mappings are merged and everything else is replaced."""
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        current = merged.get(key)
        if isinstance(value, Mapping) and isinstance(current, Mapping):
            merged[key] = deep_merge(current, value)
        else:
            merged[key] = value
    return merged


def read_yaml_chain(path: Path, _seen: frozenset[Path] = frozenset()) -> dict[str, Any]:
    """read a yaml file and, when it has an extends key, merge it over the file it extends."""
    # absolute instead of resolve, a kubernetes configmap mounts files through symlinks into a
    # timestamped directory and the sibling files must be looked up next to the symlink.
    path = path.absolute()
    if path in _seen:
        raise ValueError(f"extends cycle at {path.name}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path.name} must contain a mapping at the top level")
    parent = data.pop(EXTENDS_KEY, None)
    if parent is None:
        return data
    parent_data = read_yaml_chain(path.parent / parent, _seen | {path})
    return deep_merge(parent_data, data)


def apply_env_overrides(data: Mapping[str, Any], environ: Mapping[str, str]) -> dict[str, Any]:
    """copy data and set one key per AIRPULSE_ variable, values stay strings for pydantic."""
    result = copy.deepcopy(dict(data))
    for name, value in environ.items():
        if not name.startswith(ENV_PREFIX):
            continue
        parts = name[len(ENV_PREFIX) :].lower().split(NESTED_SEP)
        if len(parts) == 1 and parts[0] in RESERVED_ENV_KEYS:
            continue
        node = result
        for part in parts[:-1]:
            child = node.get(part)
            if not isinstance(child, dict):
                child = {}
                node[part] = child
            node = child
        node[parts[-1]] = value
    return result


def default_config_path(environ: Mapping[str, str]) -> Path:
    explicit = environ.get(ENV_PREFIX + "CONFIG")
    if explicit:
        return Path(explicit)
    profile = environ.get(ENV_PREFIX + "PROFILE") or DEFAULT_PROFILE
    return DEFAULT_CONFIG_DIR / f"{profile}.yaml"


def load_config(path: str | Path | None = None, environ: Mapping[str, str] | None = None) -> Config:
    """build the validated config for one profile.

    the path is the profile file, usually configs/<profile>.yaml. when it is omitted the
    AIRPULSE_CONFIG or AIRPULSE_PROFILE variable picks it. with the process environment, the
    .env file at the repo root is read first without overriding variables already set.
    """
    if environ is None:
        load_dotenv(REPO_ROOT / ".env", override=False)
        environ = os.environ
    config_path = Path(path) if path is not None else default_config_path(environ)
    config_path = config_path.absolute()
    data = apply_env_overrides(read_yaml_chain(config_path), environ)
    data["profile"] = config_path.stem
    data["config_dir"] = config_path.parent
    return Config.model_validate(data)


def load_locations(config: Config) -> list[Location]:
    """read the sampling points listed in the locations file of the config."""
    path = config.resolve_path(config.locations.file)
    with path.open(newline="", encoding="utf-8") as handle:
        rows = [Location.model_validate(row) for row in csv.DictReader(handle)]
    ids = [row.location_id for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError(f"duplicate location_id in {path.name}")
    return rows
