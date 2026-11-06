"""openweathermap current weather and air pollution, a backup source with its own units.

wind arrives in m/s and the weather code uses the provider scheme instead of WMO codes, so the
speed is converted and the code is left empty. one call per location and domain, throttled to
the free plan.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from airpulse.common.config import Location
from airpulse.common.schemas import AirEvent, WeatherEvent, make_event_id
from airpulse.common.time_utils import floor_to_hour, utc_now
from airpulse.ingestion.sources.base import FetchWindow, Source, SourceBatch, get_json

COMPONENT_MAP = {
    "pm2_5": "pm2_5_ugm3",
    "pm10": "pm10_ugm3",
    "co": "co_ugm3",
    "no2": "no2_ugm3",
    "so2": "so2_ugm3",
    "o3": "o3_ugm3",
}


def _meta(location: Location, ts: datetime, domain: str, run_id: str, ingested: datetime) -> dict:
    return {
        "event_id": make_event_id("openweathermap", location.location_id, ts, domain),
        "source": "openweathermap",
        "location_id": location.location_id,
        "lat": location.lat,
        "lon": location.lon,
        "event_ts_utc": ts,
        "ingested_ts_utc": ingested,
        "run_id": run_id,
    }


def parse_weather(
    payload: Any, location: Location, run_id: str, ingested: datetime
) -> WeatherEvent | None:
    if "dt" not in payload:
        return None
    ts = floor_to_hour(datetime.fromtimestamp(payload["dt"], tz=timezone.utc))
    main = payload.get("main") or {}
    wind = payload.get("wind") or {}
    rain = (payload.get("rain") or {}).get("1h")
    snow = (payload.get("snow") or {}).get("1h")
    precipitation = None if rain is None and snow is None else (rain or 0.0) + (snow or 0.0)
    to_kmh = lambda value: None if value is None else value * 3.6  # noqa: E731
    return WeatherEvent(
        **_meta(location, ts, "weather", run_id, ingested),
        temperature_c=main.get("temp"),
        humidity_pct=main.get("humidity"),
        apparent_temperature_c=main.get("feels_like"),
        precipitation_mm=precipitation,
        rain_mm=rain,
        pressure_msl_hpa=main.get("pressure"),
        cloud_cover_pct=(payload.get("clouds") or {}).get("all"),
        wind_speed_kmh=to_kmh(wind.get("speed")),
        wind_direction_deg=wind.get("deg"),
        wind_gusts_kmh=to_kmh(wind.get("gust")),
    )


def parse_air(payload: Any, location: Location, run_id: str, ingested: datetime) -> list[AirEvent]:
    events = []
    for item in payload.get("list", []):
        if "dt" not in item:
            continue
        ts = floor_to_hour(datetime.fromtimestamp(item["dt"], tz=timezone.utc))
        components = item.get("components") or {}
        metrics = {
            column: components[name] for name, column in COMPONENT_MAP.items() if name in components
        }
        if not metrics:
            continue
        events.append(AirEvent(**_meta(location, ts, "air", run_id, ingested), **metrics))
    return events


class OpenWeatherMapSource(Source):
    name = "openweathermap"

    def __init__(self, config, session=None) -> None:
        super().__init__(config, session)
        self.cfg = config.sources.openweathermap
        if not self.cfg.api_key:
            raise ValueError(
                "openweathermap needs an api key, set AIRPULSE_SOURCES__OPENWEATHERMAP__API_KEY"
            )
        self.min_interval_s = 60.0 / self.cfg.calls_per_minute
        self._last_call = 0.0

    def _get(self, path: str, location: Location, extra: dict[str, Any]) -> Any:
        wait = self.min_interval_s - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()
        params = {"lat": location.lat, "lon": location.lon, "appid": self.cfg.api_key, **extra}
        return get_json(self.session, f"{self.cfg.base_url}/{path}", params, self.cfg.timeout_s)

    def fetch(self, locations: Sequence[Location], window: FetchWindow, run_id: str) -> SourceBatch:
        batch = SourceBatch()
        ingested = utc_now()
        for location in locations:
            weather = parse_weather(
                self._get("weather", location, {"units": "metric"}), location, run_id, ingested
            )
            if weather is not None:
                batch.weather.append(weather)
            batch.air.extend(
                parse_air(self._get("air_pollution", location, {}), location, run_id, ingested)
            )
        return batch
