import pytest
from tests.unit.ingestion.conftest import NOW, FakeSession

from airpulse.ingestion.sources.base import FetchWindow
from airpulse.ingestion.sources.openaq import OpenAqSource, parse_latest, parse_stations, to_ugm3

STATIONS = {
    "results": [
        {
            "id": 2161,
            "name": "US Embassy Hanoi",
            "sensors": [
                {"id": 1, "parameter": {"name": "pm25", "units": "µg/m³"}},
                {"id": 2, "parameter": {"name": "co", "units": "ppm"}},
                {"id": 3, "parameter": {"name": "temperature", "units": "c"}},
            ],
        }
    ]
}

LATEST = {
    "results": [
        {"sensorsId": 1, "value": 48.2, "datetime": {"utc": "2026-11-06T17:00:00Z"}},
        {"sensorsId": 2, "value": 0.5, "datetime": {"utc": "2026-11-06T17:10:00Z"}},
        {"sensorsId": 3, "value": 24.0, "datetime": {"utc": "2026-11-06T17:00:00Z"}},
        {"sensorsId": 1, "value": -1.0, "datetime": {"utc": "2026-11-06T16:00:00Z"}},
    ]
}


def test_units_are_normalized_to_micrograms() -> None:
    assert to_ugm3(10.0, "µg/m³", "pm2_5_ugm3") == 10.0
    assert round(to_ugm3(100.0, "ppb", "no2_ugm3"), 1) == 188.2
    assert round(to_ugm3(1.0, "ppm", "co_ugm3")) == 1146
    assert to_ugm3(1.0, "ppm", "pm2_5_ugm3") is None


def test_latest_values_group_by_hour_and_keep_the_station(locations) -> None:
    station = parse_stations(STATIONS)[0]

    events = parse_latest(LATEST, station, locations[0], "r1", NOW)

    assert len(events) == 1
    event = events[0]
    assert event.station_id == "2161"
    assert event.location_id == "ha_noi"
    assert event.pm2_5_ugm3 == 48.2
    assert round(event.co_ugm3) == 573
    assert event.event_ts_utc.hour == 17


def test_source_requires_a_key_and_caches_station_lookups(config, locations) -> None:
    with pytest.raises(ValueError, match="api key"):
        OpenAqSource(config)
    config = config.model_copy(deep=True)
    config.sources.openaq.api_key = "k"
    session = FakeSession([("/latest", LATEST), ("/locations", STATIONS)])
    source = OpenAqSource(config, session)

    first = source.fetch(locations[:1], FetchWindow(), "r1")
    second = source.fetch(locations[:1], FetchWindow(), "r2")

    assert len(first.air) == 1 and len(second.air) == 1
    lookups = [c for c in session.calls if c["url"].endswith("/locations")]
    assert len(lookups) == 1
    assert lookups[0]["headers"] == {"X-API-Key": "k"}
