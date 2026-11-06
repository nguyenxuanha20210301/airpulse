import math
from datetime import timedelta

import pytest
from tests.unit.streaming.conftest import T0, air_df, air_event, weather_df, weather_event

from airpulse.common.cleaning import clean_air, clean_weather
from airpulse.common.config import DEFAULT_CONFIG_DIR, load_config
from airpulse.common.features import build_feature_table
from airpulse.common.spark import with_dt

LOCATIONS = ("ha_noi", "da_nang")
DAYS = 5


def synthetic_events(days: int = DAYS):
    """hourly series for two locations, pollution and heat follow a daily cycle plus an offset."""
    weather, air = [], []
    for index, location in enumerate(LOCATIONS):
        for hour in range(days * 24):
            phase = math.sin(2 * math.pi * (hour % 24) / 24)
            pm = 20.0 + 10.0 * index + 8.0 * phase + (hour // 24)
            temp = 24.0 + 3.0 * index + 5.0 * phase
            air.append(
                air_event(
                    location, hour, pm2_5_ugm3=pm, pm10_ugm3=pm * 1.5, no2_ugm3=10.0, o3_ugm3=40.0
                )
            )
            weather.append(
                weather_event(
                    location,
                    hour,
                    temperature_c=temp,
                    humidity_pct=70.0 - phase * 10,
                    wind_speed_kmh=5.0 + index,
                    precipitation_mm=0.0,
                    pressure_msl_hpa=1010.0,
                    cloud_cover_pct=50.0,
                )
            )
    return weather, air


@pytest.fixture(scope="module")
def config():
    return load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={})


@pytest.fixture(scope="module")
def clean_frames(spark):
    weather, air = synthetic_events()
    air_clean = with_dt(clean_air(air_df(spark, air))).persist()
    weather_clean = with_dt(clean_weather(weather_df(spark, weather))).persist()
    return weather_clean, air_clean


@pytest.fixture(scope="module")
def t0():
    return T0


@pytest.fixture(scope="module")
def last_day():
    return T0 + timedelta(days=DAYS - 1)


@pytest.fixture(scope="module")
def feature_table(clean_frames):
    weather, air = clean_frames
    # the checkpoint cuts the lineage so later joins do not carry the whole pipeline plan.
    return build_feature_table(weather, air).localCheckpoint()
