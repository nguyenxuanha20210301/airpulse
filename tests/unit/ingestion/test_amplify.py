from datetime import timedelta

from pyspark.sql import functions as F
from tests.unit.streaming.conftest import T0, air_df, air_event

from airpulse.common.schemas import event_id_column, location_key, make_event_id
from airpulse.common.spark import with_dt
from airpulse.ingestion.amplify import copies_for, shifted_copies, span_days


def raw_air(spark, days=3):
    events = [
        air_event(location, hour, pm2_5_ugm3=10.0 + hour)
        for location in ("ha_noi", "da_nang")
        for hour in range(days * 24)
    ]
    return with_dt(air_df(spark, events))


def test_spark_event_id_matches_the_python_one(spark) -> None:
    frame = raw_air(spark, days=1).withColumn("rebuilt", event_id_column("air"))
    rows = frame.select("event_id", "rebuilt", "source", "location_id", "event_ts_utc").collect()

    assert all(row["event_id"] == row["rebuilt"] for row in rows)
    station = frame.limit(1).withColumn("station_id", F.lit("42"))
    row = station.withColumn("rebuilt", event_id_column("air")).first()
    expected = make_event_id("open_meteo", location_key(row["location_id"], "42"), T0, "air")
    assert row["rebuilt"] == expected


def test_copies_tile_the_calendar_backwards(spark) -> None:
    raw = raw_air(spark)
    span = span_days(raw)
    copies = shifted_copies(raw, "air", 4, span, "bench").persist()
    days = sorted(row["dt"] for row in copies.select("dt").distinct().collect())

    assert span == 3
    assert copies.count() == 4 * raw.count()
    assert copies.select("event_id").distinct().count() == 4 * raw.count()
    assert len(days) == 4 * 3
    assert days[0] == (T0 - timedelta(days=9)).strftime("%Y-%m-%d")
    assert days[-1] == (T0 + timedelta(days=2)).strftime("%Y-%m-%d")
    assert {row["run_id"] for row in copies.select("run_id").distinct().collect()} == {"bench"}
    assert copies.columns == raw.columns
    # the source itself is copy zero and keeps its ids.
    original = raw.select("event_id").subtract(copies.select("event_id")).count()
    assert original == 0


def test_copy_count_rounds_up_to_cover_the_years() -> None:
    assert copies_for(1, 67) == 6
    assert copies_for(10, 67) == 55
    assert copies_for(0.01, 67) == 1
