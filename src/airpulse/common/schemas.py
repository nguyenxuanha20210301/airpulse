"""data contracts for every message and table, defined once and reused by all layers.

the pydantic models are the source of truth. the json schema files under schemas/events are
exported from them with the write_json_schemas function and a unit test keeps both in sync, and
the spark struct types used by streaming and batch jobs are derived from the same models.
"""

from __future__ import annotations

import hashlib
import json
import sys
import types
import typing
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict

SCHEMA_VERSION = 1
EVENTS_DIR = Path(__file__).resolve().parents[3] / "schemas" / "events"

# layer and dataset names used in hdfs paths, the path itself comes from the hdfs config.
LAYER_RAW = "raw"
LAYER_CLEAN = "clean"
LAYER_CURATED = "curated"
LAYER_MODELS = "models"
# small static tables copied from the config, so the query layer can join names and regions.
LAYER_REFERENCE = "reference"
DS_WEATHER_HOURLY = "weather_hourly"
DS_AIR_HOURLY = "air_hourly"
DS_LOCATIONS = "locations"


class EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    schema_version: int = SCHEMA_VERSION
    source: str
    location_id: str
    lat: float
    lon: float
    event_ts_utc: datetime
    ingested_ts_utc: datetime
    run_id: str
    # set for station measurements, model based sources leave it empty.
    station_id: str | None = None


class WeatherEvent(EventBase):
    """one hourly weather observation or model value for one location."""

    temperature_c: float | None = None
    humidity_pct: float | None = None
    dew_point_c: float | None = None
    apparent_temperature_c: float | None = None
    precipitation_mm: float | None = None
    rain_mm: float | None = None
    weather_code: int | None = None
    pressure_msl_hpa: float | None = None
    surface_pressure_hpa: float | None = None
    cloud_cover_pct: float | None = None
    wind_speed_kmh: float | None = None
    wind_direction_deg: float | None = None
    wind_gusts_kmh: float | None = None


class AirEvent(EventBase):
    """one hourly air quality record for one location, concentrations in micrograms per m3."""

    pm2_5_ugm3: float | None = None
    pm10_ugm3: float | None = None
    co_ugm3: float | None = None
    no2_ugm3: float | None = None
    so2_ugm3: float | None = None
    o3_ugm3: float | None = None
    dust_ugm3: float | None = None
    uv_index: float | None = None
    aqi_us: int | None = None
    aqi_eu: int | None = None


class AlertEvent(BaseModel):
    """an anomaly detected by the speed layer."""

    model_config = ConfigDict(extra="forbid")

    alert_id: str
    alert_day: str
    schema_version: int = SCHEMA_VERSION
    location_id: str
    event_ts_utc: datetime
    detected_ts_utc: datetime
    kind: str
    metric: str
    value: float
    threshold: float
    severity: str
    run_id: str


WEATHER_METRICS = [name for name in WeatherEvent.model_fields if name not in EventBase.model_fields]
AIR_METRICS = [name for name in AirEvent.model_fields if name not in EventBase.model_fields]
EVENT_MODELS: dict[str, type[BaseModel]] = {
    "weather_raw": WeatherEvent,
    "air_raw": AirEvent,
    "air_alerts": AlertEvent,
}


def _spark_types() -> dict:
    # imported here so the producer image can use the models without pyspark installed.
    from pyspark.sql import types as T

    return {
        str: T.StringType(),
        int: T.IntegerType(),
        float: T.DoubleType(),
        bool: T.BooleanType(),
        datetime: T.TimestampType(),
    }


def make_event_id(source: str, location_id: str, event_ts_utc: datetime, domain: str) -> str:
    """stable id for one source, place, hour and domain, so retries and replays deduplicate."""
    key = "|".join([source, location_id, event_ts_utc.isoformat(), domain])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:32]


def location_key(location_id: str, station_id: str | None = None) -> str:
    """the place part of an event id, stations near the same point keep separate ids."""
    return f"{location_id}/{station_id}" if station_id else location_id


def event_id_column(domain: str):
    """make_event_id as a spark column, for jobs that move whole datasets in time.

    the timestamp is rendered like datetime.isoformat on a UTC value, which holds because every
    session runs in UTC.
    """
    from pyspark.sql import functions as F

    key = F.when(
        F.col("station_id").isNotNull(), F.concat_ws("/", "location_id", "station_id")
    ).otherwise(F.col("location_id"))
    stamp = F.date_format("event_ts_utc", "yyyy-MM-dd'T'HH:mm:ssxxx")
    return F.substring(F.sha1(F.concat_ws("|", F.col("source"), key, stamp, F.lit(domain))), 1, 32)


def _unwrap(annotation: object) -> tuple[type, bool]:
    """return the concrete type of a field and whether it accepts None."""
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        args = [arg for arg in typing.get_args(annotation) if arg is not type(None)]
        return args[0], True
    return annotation, False  # type: ignore[return-value]


def spark_schema(model: type[BaseModel]):
    """derive a struct type from a pydantic model, optional fields become nullable columns."""
    from pyspark.sql import types as T

    types = _spark_types()
    fields = []
    for name, info in model.model_fields.items():
        python_type, optional = _unwrap(info.annotation)
        fields.append(T.StructField(name, types[python_type], nullable=optional))
    return T.StructType(fields)


def json_schema(model: type[BaseModel]) -> dict:
    return model.model_json_schema()


def write_json_schemas(target_dir: Path = EVENTS_DIR) -> list[Path]:
    """export one json schema file per event model, run through make schemas."""
    written = []
    target_dir.mkdir(parents=True, exist_ok=True)
    for name, model in EVENT_MODELS.items():
        path = target_dir / f"{name}.json"
        path.write_text(json.dumps(json_schema(model), indent=2) + "\n", encoding="utf-8")
        written.append(path)
    return written


def main() -> None:
    for path in write_json_schemas():
        print("wrote", path.relative_to(Path.cwd()) if path.is_relative_to(Path.cwd()) else path)


if __name__ == "__main__":
    sys.exit(main())
