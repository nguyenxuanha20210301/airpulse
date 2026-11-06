from datetime import date, datetime, timezone

import pytest
from tests.unit.ingestion.conftest import FakeSession, open_meteo_payload

from airpulse.common.schemas import WeatherEvent
from airpulse.ingestion.sources.base import FetchWindow
from airpulse.ingestion.sources.open_meteo import (
    AIR_MAP,
    WEATHER_MAP,
    OpenMeteoSource,
    base_params,
    parse_hourly,
    select_mapping,
)

TIMES = ["2026-11-06T15:00", "2026-11-06T16:00", "2026-11-06T17:00", "2026-11-06T18:00"]
INGESTED = datetime(2026, 11, 6, 17, 30, tzinfo=timezone.utc)


def test_base_params_for_live_and_history_windows(locations) -> None:
    live = base_params(locations, FetchWindow(past_hours=3, forecast_hours=1))
    history = base_params(
        locations, FetchWindow(start_date=date(2020, 1, 1), end_date=date(2020, 1, 31))
    )

    assert live["latitude"] == "21.0285,16.0544"
    assert live["past_hours"] == 3
    assert live["timezone"] == "UTC"
    assert history["start_date"] == "2020-01-01"
    assert "past_hours" not in history


def test_select_mapping_rejects_unknown_variables() -> None:
    assert select_mapping(["pm2_5", "ozone"], AIR_MAP) == {
        "pm2_5": "pm2_5_ugm3",
        "ozone": "o3_ugm3",
    }
    with pytest.raises(ValueError, match="unknown"):
        select_mapping(["pm2_5", "nope"], AIR_MAP)


def test_parse_hourly_rebuilds_rows_and_drops_future_and_empty_hours(locations) -> None:
    payload = open_meteo_payload(
        ["temperature_2m", "weather_code"],
        TIMES,
        [
            {"temperature_2m": [23.1, None, 23.7, 24.0], "weather_code": [1, None, 3, 3]},
            {"temperature_2m": [27.0, 27.2, 27.7, 28.0], "weather_code": [0, 0, 0, 0]},
        ],
    )
    mapping = select_mapping(["temperature_2m", "weather_code"], WEATHER_MAP)

    events = parse_hourly(
        payload,
        locations,
        mapping,
        WeatherEvent,
        "weather",
        "open_meteo",
        "r1",
        INGESTED,
        cutoff=datetime(2026, 11, 6, 17, tzinfo=timezone.utc),
    )

    assert [e.location_id for e in events] == ["ha_noi", "ha_noi", "da_nang", "da_nang", "da_nang"]
    assert events[0].temperature_c == 23.1
    assert events[0].weather_code == 1
    assert events[0].event_ts_utc == datetime(2026, 11, 6, 15, tzinfo=timezone.utc)
    assert events[1].event_ts_utc.hour == 17
    assert len({e.event_id for e in events}) == 5


def test_parse_hourly_checks_location_count(locations) -> None:
    payload = open_meteo_payload(["pm2_5"], TIMES, [{"pm2_5": [1, 2, 3, 4]}])
    with pytest.raises(ValueError, match="expected 2 locations"):
        parse_hourly(
            payload,
            locations,
            {"pm2_5": "pm2_5_ugm3"},
            WeatherEvent,
            "air",
            "open_meteo",
            "r1",
            INGESTED,
        )


def test_source_fetches_weather_and_air_per_chunk(config, locations) -> None:
    config = config.model_copy(deep=True)
    config.sources.open_meteo.locations_per_request = 1
    weather_vars = config.sources.open_meteo.weather_variables
    air_vars = config.sources.open_meteo.air_variables

    def weather(params):
        return open_meteo_payload(weather_vars, TIMES[:2], [{"temperature_2m": [20.0, 21.0]}])

    def air(params):
        return open_meteo_payload(
            air_vars, TIMES[:2], [{"pm2_5": [10.0, 12.5], "us_aqi": [40, 50]}]
        )

    session = FakeSession([("air-quality-api", air), ("api.open-meteo.com", weather)])
    source = OpenMeteoSource(config, session)

    batch = source.fetch(locations, FetchWindow(past_hours=3), "r1")

    assert len(session.calls) == 4
    assert len(batch.weather) == 4
    assert len(batch.air) == 4
    assert batch.air[0].aqi_us == 40
    assert batch.air[0].source == "open_meteo"
    assert session.calls[0]["params"]["hourly"] == ",".join(weather_vars)


def test_history_window_uses_the_archive_url(config, locations) -> None:
    captured = []

    def weather(params):
        captured.append(params)
        return open_meteo_payload(
            ["temperature_2m"],
            ["2020-01-01T00:00"],
            [{"temperature_2m": [15.0]}, {"temperature_2m": [25.0]}],
        )

    session = FakeSession([("archive-api", weather)])
    source = OpenMeteoSource(config, session)

    events = source.fetch_weather(
        locations, FetchWindow(start_date=date(2020, 1, 1), end_date=date(2020, 1, 1)), "r1"
    )

    assert session.calls[0]["url"] == config.sources.open_meteo.archive_url
    assert captured[0]["start_date"] == "2020-01-01"
    assert len(events) == 2
