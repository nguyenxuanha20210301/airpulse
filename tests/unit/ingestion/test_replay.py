from datetime import date, datetime, timedelta, timezone

from tests.unit.ingestion.conftest import NOW

from airpulse.common.schemas import AirEvent, make_event_id, spark_schema
from airpulse.ingestion.replay import (
    hourly_groups,
    replay_topic,
    rewrite,
    select_range,
    shift_delta,
)


def _event(hour: int, location_id: str = "ha_noi") -> AirEvent:
    ts = datetime(2020, 1, 1, hour, tzinfo=timezone.utc)
    return AirEvent(
        event_id=make_event_id("open_meteo", location_id, ts, "air"),
        source="open_meteo",
        location_id=location_id,
        lat=21.0,
        lon=105.8,
        event_ts_utc=ts,
        ingested_ts_utc=ts,
        run_id="old",
        pm2_5_ugm3=1.0,
    )


def test_shift_maps_first_hour_onto_now() -> None:
    delta = shift_delta(datetime(2020, 1, 1, 5, tzinfo=timezone.utc), now=NOW)

    assert delta == datetime(2026, 11, 6, 17, tzinfo=timezone.utc) - datetime(
        2020, 1, 1, 5, tzinfo=timezone.utc
    )


def test_rewrite_keeps_or_moves_the_timestamp() -> None:
    event = _event(5)

    same = rewrite(event, "new", timedelta(0), NOW)
    moved = rewrite(event, "new", timedelta(hours=1), NOW)

    assert same.event_id == event.event_id and same.run_id == "new"
    assert moved.event_ts_utc == event.event_ts_utc + timedelta(hours=1)
    assert moved.event_id != event.event_id


def test_rows_group_by_hour_in_order(spark) -> None:
    events = [_event(5), _event(5, "da_nang"), _event(6), _event(8)]
    df = spark.createDataFrame([e.model_dump() for e in events], schema=spark_schema(AirEvent))

    groups = list(hourly_groups(df.orderBy("event_ts_utc").toLocalIterator(), AirEvent))

    assert [hour.hour for hour, _ in groups] == [5, 6, 8]
    assert len(groups[0][1]) == 2
    assert groups[0][1][0].event_ts_utc.tzinfo is not None


def test_select_range_and_topic_name(spark) -> None:
    df = spark.createDataFrame(
        [("2020-01-01", 1), ("2020-02-01", 2), ("2020-03-01", 3)], ["dt", "v"]
    )

    kept = select_range(df, date(2020, 1, 15), date(2020, 2, 15)).collect()

    assert [row["v"] for row in kept] == [2]
    assert replay_topic("air.raw", "replay") == "air.raw.replay"
