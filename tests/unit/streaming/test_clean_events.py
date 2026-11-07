import json

from tests.unit.streaming.conftest import air_event

from airpulse.common.config import DEFAULT_CONFIG_DIR, load_config
from airpulse.common.schemas import AirEvent
from airpulse.streaming.clean_events import (
    clean_air_stream,
    kafka_source_options,
    parse_events,
    to_kafka_frame,
)


def test_parse_events_tolerates_malformed_messages(spark) -> None:
    good = air_event("ha_noi", 0, pm2_5_ugm3=12.0)
    raw = spark.createDataFrame([(good.model_dump_json(),), ("not json",)], ["value"])

    parsed = parse_events(raw, AirEvent)
    rows = parsed.collect()

    assert len(rows) == 2
    assert {row["event_id"] for row in rows} == {good.event_id, None}


def test_clean_air_stream_nulls_bad_values_dedupes_and_scores(spark) -> None:
    first = air_event("ha_noi", 0, pm2_5_ugm3=36.6, pm10_ugm3=-3.0)
    retry = air_event("ha_noi", 0, run_id="r2", pm2_5_ugm3=36.6, pm10_ugm3=-3.0)
    other = air_event("ha_noi", 1, pm2_5_ugm3=5000.0)
    raw = spark.createDataFrame(
        [
            (e.model_dump_json(),)
            for e in (
                first,
                retry,
                other,
            )
        ]
        + [("{}",)],
        ["value"],
    )

    cleaned = clean_air_stream(parse_events(raw, AirEvent), "2 hours", "clean-1").collect()

    assert len(cleaned) == 2
    by_hour = {row["event_ts_utc"].hour: row for row in cleaned}
    assert by_hour[0]["pm10_ugm3"] is None
    assert by_hour[0]["aqi_computed"] == 104
    assert by_hour[0]["aqi_category"] == "unhealthy_for_sensitive_groups"
    assert by_hour[1]["pm2_5_ugm3"] is None
    assert by_hour[1]["aqi_computed"] is None
    assert by_hour[0]["clean_run_id"] == "clean-1"


def test_kafka_frame_has_key_and_json_value(spark, clean_air_df) -> None:
    frame = to_kafka_frame(clean_air_df.limit(1))
    row = frame.first()

    assert frame.columns == ["key", "value"]
    assert row["key"] in ("ha_noi", "da_nang")
    assert json.loads(row["value"])["location_id"] == row["key"]


def test_kafka_source_reads_from_the_requested_offsets_and_caps_batches() -> None:
    cfg = load_config(DEFAULT_CONFIG_DIR / "demo.yaml", environ={})
    capped = load_config(
        DEFAULT_CONFIG_DIR / "demo.yaml",
        environ={"AIRPULSE_STREAMING__MAX_OFFSETS_PER_TRIGGER": "50000"},
    )

    plain = kafka_source_options(cfg, "air.raw.bench", "earliest")
    limited = kafka_source_options(capped, "air.raw.bench")

    assert plain["startingOffsets"] == "earliest"
    assert plain["subscribe"] == "air.raw.bench"
    assert "maxOffsetsPerTrigger" not in plain
    assert limited["maxOffsetsPerTrigger"] == "50000"
    assert limited["startingOffsets"] == "latest"
