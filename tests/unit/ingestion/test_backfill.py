from datetime import date

from tests.unit.ingestion.conftest import NOW

from airpulse.common.schemas import AirEvent, make_event_id
from airpulse.common.spark import write_partitioned
from airpulse.ingestion.backfill import events_to_df, parse_args, raw_path


def test_raw_path_prefers_the_override(config) -> None:
    assert (
        raw_path(config, "air_hourly", None)
        == f"{config.hdfs.url}{config.hdfs.root}/raw/air_hourly"
    )
    assert raw_path(config, "air_hourly", "/tmp/x/") == "/tmp/x/air_hourly"


def test_args_parse_dates() -> None:
    args = parse_args(
        ["--config", "c.yaml", "--start", "2020-01-01", "--end", "2020-02-01", "--domain", "air"]
    )

    assert args.start == date(2020, 1, 1)
    assert args.domain == "air"


def test_events_are_written_as_parquet_partitioned_by_day(spark, tmp_path, locations) -> None:
    events = []
    for loc in locations:
        events.append(
            AirEvent(
                event_id=make_event_id("open_meteo", loc.location_id, NOW, "air"),
                source="open_meteo",
                location_id=loc.location_id,
                lat=loc.lat,
                lon=loc.lon,
                event_ts_utc=NOW,
                ingested_ts_utc=NOW,
                run_id="r1",
                pm2_5_ugm3=12.0,
                aqi_us=50,
            )
        )
    target = str(tmp_path / "air_hourly")

    write_partitioned(events_to_df(spark, events, AirEvent), target)
    back = spark.read.parquet(target)

    assert back.count() == 2
    assert set(back.select("dt").distinct().toPandas()["dt"]) == {"2026-11-06"}
    row = back.where("location_id = 'ha_noi'").first()
    assert row["pm2_5_ugm3"] == 12.0
    assert row["aqi_us"] == 50
    assert row["event_ts_utc"].hour == 17
