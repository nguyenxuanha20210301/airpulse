import pytest
from tests.unit.ingestion.conftest import NOW, FakeSession

from airpulse.ingestion.sources.base import FetchWindow
from airpulse.ingestion.sources.openweathermap import OpenWeatherMapSource, parse_air, parse_weather

WEATHER = {
    "dt": 1794070800,
    "main": {"temp": 24.3, "feels_like": 25.0, "pressure": 1012, "humidity": 78},
    "wind": {"speed": 2.5, "deg": 90, "gust": 5.0},
    "clouds": {"all": 40},
    "rain": {"1h": 0.3},
    "weather": [{"id": 500}],
}

AIR = {
    "list": [
        {
            "dt": 1794070800,
            "main": {"aqi": 3},
            "components": {"pm2_5": 30.1, "pm10": 45.0, "co": 400.0, "no2": 12.0},
        }
    ]
}


def test_weather_units_are_converted(locations) -> None:
    event = parse_weather(WEATHER, locations[0], "r1", NOW)

    assert event.temperature_c == 24.3
    assert event.wind_speed_kmh == 9.0
    assert event.wind_gusts_kmh == 18.0
    assert event.precipitation_mm == 0.3
    assert event.weather_code is None
    assert event.event_ts_utc.minute == 0
    assert parse_weather({}, locations[0], "r1", NOW) is None


def test_air_components_map_to_columns(locations) -> None:
    events = parse_air(AIR, locations[0], "r1", NOW)

    assert len(events) == 1
    assert events[0].pm2_5_ugm3 == 30.1
    assert events[0].no2_ugm3 == 12.0
    assert events[0].aqi_us is None


def test_source_calls_both_endpoints_per_location(config, locations) -> None:
    with pytest.raises(ValueError, match="api key"):
        OpenWeatherMapSource(config)
    config = config.model_copy(deep=True)
    config.sources.openweathermap.api_key = "k"
    config.sources.openweathermap.calls_per_minute = 100000
    session = FakeSession([("/air_pollution", AIR), ("/weather", WEATHER)])

    batch = OpenWeatherMapSource(config, session).fetch(locations, FetchWindow(), "r1")

    assert len(session.calls) == 4
    assert len(batch.weather) == 2
    assert len(batch.air) == 2
    assert session.calls[0]["params"]["appid"] == "k"
