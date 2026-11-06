from datetime import datetime, timedelta, timezone

import pytest

from airpulse.common.cleaning import clean_air, clean_weather
from airpulse.common.schemas import AirEvent, WeatherEvent, make_event_id, spark_schema

T0 = datetime(2026, 11, 6, 0, tzinfo=timezone.utc)


def air_event(location_id: str, hour: int, run_id: str = "r1", **metrics) -> AirEvent:
    ts = T0 + timedelta(hours=hour)
    return AirEvent(
        event_id=make_event_id("open_meteo", location_id, ts, "air"),
        source="open_meteo",
        location_id=location_id,
        lat=21.0,
        lon=105.8,
        event_ts_utc=ts,
        ingested_ts_utc=ts + timedelta(minutes=5),
        run_id=run_id,
        **metrics,
    )


def weather_event(location_id: str, hour: int, run_id: str = "r1", **metrics) -> WeatherEvent:
    ts = T0 + timedelta(hours=hour)
    return WeatherEvent(
        event_id=make_event_id("open_meteo", location_id, ts, "weather"),
        source="open_meteo",
        location_id=location_id,
        lat=21.0,
        lon=105.8,
        event_ts_utc=ts,
        ingested_ts_utc=ts + timedelta(minutes=5),
        run_id=run_id,
        **metrics,
    )


def air_df(spark, events):
    return spark.createDataFrame([e.model_dump() for e in events], schema=spark_schema(AirEvent))


def weather_df(spark, events):
    return spark.createDataFrame(
        [e.model_dump() for e in events], schema=spark_schema(WeatherEvent)
    )


@pytest.fixture
def clean_air_df(spark):
    pm = [10, 12, 15, 40, 80, 20]
    events = [
        air_event("ha_noi", h, pm2_5_ugm3=float(v), pm10_ugm3=float(v) + 5)
        for h, v in enumerate(pm)
    ]
    events += [air_event("da_nang", h, pm2_5_ugm3=5.0, pm10_ugm3=8.0) for h in range(6)]
    return clean_air(air_df(spark, events))


@pytest.fixture
def clean_weather_df(spark):
    temps = [25.0, 26.0, 40.0, 5.0, 30.0, 31.0]
    events = [
        weather_event("ha_noi", h, temperature_c=t, humidity_pct=70.0, wind_speed_kmh=5.0)
        for h, t in enumerate(temps)
    ]
    return clean_weather(weather_df(spark, events))
