"""open-meteo weather, air quality and archive apis, no key needed.

the apis return one object per location with parallel hourly arrays, so rows are rebuilt per
timestamp. variable names from the api are mapped to the canonical column names of the events.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from airpulse.common.config import Location
from airpulse.common.schemas import AirEvent, WeatherEvent, make_event_id
from airpulse.common.time_utils import floor_to_hour, parse_iso_utc, utc_now
from airpulse.ingestion.sources.base import FetchWindow, Source, SourceBatch, chunked, get_json

WEATHER_MAP = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "dew_point_2m": "dew_point_c",
    "apparent_temperature": "apparent_temperature_c",
    "precipitation": "precipitation_mm",
    "rain": "rain_mm",
    "weather_code": "weather_code",
    "pressure_msl": "pressure_msl_hpa",
    "surface_pressure": "surface_pressure_hpa",
    "cloud_cover": "cloud_cover_pct",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_direction_10m": "wind_direction_deg",
    "wind_gusts_10m": "wind_gusts_kmh",
}

AIR_MAP = {
    "pm10": "pm10_ugm3",
    "pm2_5": "pm2_5_ugm3",
    "carbon_monoxide": "co_ugm3",
    "nitrogen_dioxide": "no2_ugm3",
    "sulphur_dioxide": "so2_ugm3",
    "ozone": "o3_ugm3",
    "dust": "dust_ugm3",
    "uv_index": "uv_index",
    "us_aqi": "aqi_us",
    "european_aqi": "aqi_eu",
}


def select_mapping(variables: Sequence[str], mapping: dict[str, str]) -> dict[str, str]:
    """keep the configured variables in request order, unknown names fail at startup."""
    unknown = [name for name in variables if name not in mapping]
    if unknown:
        raise ValueError(f"unknown open-meteo variables {unknown}")
    return {name: mapping[name] for name in variables}


def base_params(locations: Sequence[Location], window: FetchWindow) -> dict[str, Any]:
    params: dict[str, Any] = {
        "latitude": ",".join(str(loc.lat) for loc in locations),
        "longitude": ",".join(str(loc.lon) for loc in locations),
        "timezone": "UTC",
    }
    if window.is_history:
        params["start_date"] = window.start_date.isoformat()
        params["end_date"] = window.end_date.isoformat()
    else:
        params["past_hours"] = window.past_hours
        params["forecast_hours"] = window.forecast_hours
    return params


def parse_hourly(
    payload: Any,
    locations: Sequence[Location],
    mapping: dict[str, str],
    model: type[BaseModel],
    domain: str,
    source: str,
    run_id: str,
    ingested_ts: datetime,
    cutoff: datetime | None = None,
) -> list[Any]:
    """rebuild one event per location and timestamp from the parallel hourly arrays."""
    items = payload if isinstance(payload, list) else [payload]
    if len(items) != len(locations):
        raise ValueError(f"expected {len(locations)} locations in response, got {len(items)}")
    events = []
    for location, item in zip(locations, items, strict=True):
        hourly = item.get("hourly") or {}
        for index, stamp in enumerate(hourly.get("time") or []):
            ts = parse_iso_utc(stamp)
            if cutoff is not None and ts > cutoff:
                continue
            metrics = {}
            for api_name, column in mapping.items():
                values = hourly.get(api_name)
                if values is not None and index < len(values):
                    metrics[column] = values[index]
            if all(value is None for value in metrics.values()):
                continue
            events.append(
                model(
                    event_id=make_event_id(source, location.location_id, ts, domain),
                    source=source,
                    location_id=location.location_id,
                    lat=location.lat,
                    lon=location.lon,
                    event_ts_utc=ts,
                    ingested_ts_utc=ingested_ts,
                    run_id=run_id,
                    **metrics,
                )
            )
    return events


class OpenMeteoSource(Source):
    name = "open_meteo"

    def __init__(self, config, session=None) -> None:
        super().__init__(config, session)
        self.cfg = config.sources.open_meteo
        self.weather_map = select_mapping(self.cfg.weather_variables, WEATHER_MAP)
        self.air_map = select_mapping(self.cfg.air_variables, AIR_MAP)

    def fetch(self, locations: Sequence[Location], window: FetchWindow, run_id: str) -> SourceBatch:
        batch = SourceBatch()
        for chunk in chunked(locations, self.cfg.locations_per_request):
            batch.weather.extend(self.fetch_weather(chunk, window, run_id))
            batch.air.extend(self.fetch_air(chunk, window, run_id))
        return batch

    def fetch_weather(
        self, locations: Sequence[Location], window: FetchWindow, run_id: str
    ) -> list[WeatherEvent]:
        url = self.cfg.archive_url if window.is_history else self.cfg.weather_url
        params = {**base_params(locations, window), "hourly": ",".join(self.weather_map)}
        payload = get_json(self.session, url, params, self.cfg.timeout_s)
        now = utc_now()
        return parse_hourly(
            payload,
            locations,
            self.weather_map,
            WeatherEvent,
            "weather",
            self.name,
            run_id,
            now,
            cutoff=None if window.is_history else floor_to_hour(now),
        )

    def fetch_air(
        self, locations: Sequence[Location], window: FetchWindow, run_id: str
    ) -> list[AirEvent]:
        params = {**base_params(locations, window), "hourly": ",".join(self.air_map)}
        payload = get_json(self.session, self.cfg.air_quality_url, params, self.cfg.timeout_s)
        now = utc_now()
        return parse_hourly(
            payload,
            locations,
            self.air_map,
            AirEvent,
            "air",
            self.name,
            run_id,
            now,
            cutoff=None if window.is_history else floor_to_hour(now),
        )
