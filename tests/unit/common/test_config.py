from pathlib import Path

import pytest
from pydantic import ValidationError

from airpulse.common.config import (
    DEFAULT_CONFIG_DIR,
    Config,
    apply_env_overrides,
    deep_merge,
    load_config,
    load_locations,
    read_yaml_chain,
)

PROFILES = ["dev", "demo", "bench"]


def test_deep_merge_merges_mappings_and_replaces_everything_else() -> None:
    base = {"a": {"x": 1, "y": [1, 2]}, "b": "keep"}
    override = {"a": {"y": [3], "z": 2}}

    merged = deep_merge(base, override)

    assert merged == {"a": {"x": 1, "y": [3], "z": 2}, "b": "keep"}
    assert base == {"a": {"x": 1, "y": [1, 2]}, "b": "keep"}


def test_env_override_sets_nested_keys_and_skips_reserved_names() -> None:
    data = {"kafka": {"bootstrap_servers": "", "topics": {"air_raw": "air.raw"}}}
    environ = {
        "AIRPULSE_KAFKA__BOOTSTRAP_SERVERS": "localhost:9092",
        "AIRPULSE_KAFKA__TOPICS__AIR_RAW": "air.test",
        "AIRPULSE_PROFILE": "demo",
        "HOME_UNRELATED": "x",
    }

    result = apply_env_overrides(data, environ)

    assert result["kafka"]["bootstrap_servers"] == "localhost:9092"
    assert result["kafka"]["topics"]["air_raw"] == "air.test"
    assert "profile" not in result
    assert data["kafka"]["bootstrap_servers"] == ""


def test_read_yaml_chain_follows_extends(tmp_path: Path) -> None:
    (tmp_path / "base.yaml").write_text("a: 1\nnested:\n  x: 1\n  y: 2\n", encoding="utf-8")
    (tmp_path / "child.yaml").write_text("extends: base.yaml\nnested:\n  y: 3\n", encoding="utf-8")

    data = read_yaml_chain(tmp_path / "child.yaml")

    assert data == {"a": 1, "nested": {"x": 1, "y": 3}}


def test_read_yaml_chain_rejects_cycles(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text("extends: b.yaml\n", encoding="utf-8")
    (tmp_path / "b.yaml").write_text("extends: a.yaml\n", encoding="utf-8")

    with pytest.raises(ValueError, match="cycle"):
        read_yaml_chain(tmp_path / "a.yaml")


@pytest.mark.parametrize("profile", PROFILES)
def test_every_profile_validates(profile: str) -> None:
    config = load_config(DEFAULT_CONFIG_DIR / f"{profile}.yaml", environ={})

    assert isinstance(config, Config)
    assert config.profile == profile
    assert config.kafka.bootstrap_servers
    assert config.hdfs.url.startswith("hdfs://")
    assert config.cassandra.hosts


def test_bench_extends_demo() -> None:
    demo = load_config(DEFAULT_CONFIG_DIR / "demo.yaml", environ={})
    bench = load_config(DEFAULT_CONFIG_DIR / "bench.yaml", environ={})

    assert bench.kafka.replication_factor == demo.kafka.replication_factor
    assert bench.replay.speed_factor > demo.replay.speed_factor
    assert bench.replay.start_date is not None


def test_env_values_are_coerced_by_the_model() -> None:
    environ = {
        "AIRPULSE_KAFKA__PARTITIONS": "12",
        "AIRPULSE_CASSANDRA__HOSTS": "cass-0, cass-1",
        "AIRPULSE_REPLAY__START_DATE": "2021-06-01",
        "AIRPULSE_SOURCES__OPENAQ__API_KEY": "123456",
    }

    config = load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ=environ)

    assert config.kafka.partitions == 12
    assert config.cassandra.hosts == ["cass-0", "cass-1"]
    assert config.replay.start_date.isoformat() == "2021-06-01"
    assert config.sources.openaq.api_key == "123456"


def test_unknown_key_is_rejected() -> None:
    with pytest.raises(ValidationError):
        load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={"AIRPULSE_KAFKA__NOPE": "1"})


def test_profile_selected_from_environment() -> None:
    config = load_config(environ={"AIRPULSE_PROFILE": "demo"})

    assert config.profile == "demo"
    assert config.config_dir == DEFAULT_CONFIG_DIR


def test_dataset_path_joins_url_root_layer_and_dataset() -> None:
    config = load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={})

    path = config.hdfs.dataset_path("raw", "weather_hourly")

    assert path == f"{config.hdfs.url}{config.hdfs.root}/raw/weather_hourly"


def test_locations_cover_the_63_former_provinces() -> None:
    config = load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={})

    locations = load_locations(config)

    assert len(locations) == 63
    assert len({loc.location_id for loc in locations}) == 63
    assert len({loc.province for loc in locations}) == 34
    assert {loc.region for loc in locations} == {"north", "central", "south"}
    for loc in locations:
        assert loc.location_id == loc.location_id.lower()
        assert 8 <= loc.lat <= 24
        assert 102 <= loc.lon <= 110
