import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

from airpulse.common.schemas import (
    AIR_METRICS,
    EVENT_MODELS,
    EVENTS_DIR,
    WEATHER_METRICS,
    AirEvent,
    WeatherEvent,
    json_schema,
    make_event_id,
    spark_schema,
)

TS = datetime(2026, 11, 6, 17, tzinfo=timezone.utc)


def _air_event(**metrics) -> AirEvent:
    return AirEvent(
        event_id=make_event_id("open_meteo", "ha_noi", TS, "air"),
        source="open_meteo",
        location_id="ha_noi",
        lat=21.0285,
        lon=105.8542,
        event_ts_utc=TS,
        ingested_ts_utc=TS,
        run_id="run-1",
        **metrics,
    )


def test_event_id_is_stable_and_differs_per_domain() -> None:
    weather = make_event_id("open_meteo", "ha_noi", TS, "weather")

    assert weather == make_event_id("open_meteo", "ha_noi", TS, "weather")
    assert weather != make_event_id("open_meteo", "ha_noi", TS, "air")
    assert len(weather) == 32


def test_metric_lists_exclude_metadata() -> None:
    assert "pm2_5_ugm3" in AIR_METRICS
    assert "event_id" not in AIR_METRICS
    assert "temperature_c" in WEATHER_METRICS
    assert "run_id" not in WEATHER_METRICS


def test_spark_schema_marks_optional_metrics_nullable() -> None:
    schema = spark_schema(WeatherEvent)
    by_name = {field.name: field for field in schema.fields}

    assert by_name["event_id"].dataType == T.StringType()
    assert by_name["event_id"].nullable is False
    assert by_name["event_ts_utc"].dataType == T.TimestampType()
    assert by_name["temperature_c"].dataType == T.DoubleType()
    assert by_name["temperature_c"].nullable is True
    assert by_name["weather_code"].dataType == T.IntegerType()


def test_json_schema_files_match_models() -> None:
    for name, model in EVENT_MODELS.items():
        path = EVENTS_DIR / f"{name}.json"
        assert path.exists(), f"{path.name} is missing, run make schemas"
        assert json.loads(path.read_text(encoding="utf-8")) == json_schema(model)


def test_spark_reads_the_json_produced_by_pydantic(spark) -> None:
    event = _air_event(pm2_5_ugm3=36.6, aqi_us=103)
    df = spark.createDataFrame([(event.model_dump_json(),)], ["value"])

    parsed = (
        df.select(F.from_json("value", spark_schema(AirEvent)).alias("e")).select("e.*").first()
    )

    assert parsed["location_id"] == "ha_noi"
    assert parsed["pm2_5_ugm3"] == 36.6
    assert parsed["aqi_us"] == 103
    assert parsed["pm10_ugm3"] is None
    assert parsed["event_ts_utc"] == TS.replace(tzinfo=None)
