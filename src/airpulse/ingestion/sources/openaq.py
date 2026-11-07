"""openaq v3, real station measurements used to check the model values.

each sampling point is attached to the nearest stations within a radius. the latest value of
every sensor becomes one air event with the station id set, so the same hour can hold several
rows per sampling point, one per station.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from airpulse.common.aqi import MOLAR_MASS, MOLAR_VOLUME_L
from airpulse.common.config import Location
from airpulse.common.schemas import AirEvent, location_key, make_event_id
from airpulse.common.time_utils import floor_to_hour, parse_iso_utc, utc_now
from airpulse.ingestion.sources.base import FetchWindow, Source, SourceBatch, get_json

PARAMETER_MAP = {
    "pm25": "pm2_5_ugm3",
    "pm10": "pm10_ugm3",
    "no2": "no2_ugm3",
    "so2": "so2_ugm3",
    "o3": "o3_ugm3",
    "co": "co_ugm3",
}

GAS_BY_COLUMN = {"no2_ugm3": "no2", "so2_ugm3": "so2", "o3_ugm3": "o3", "co_ugm3": "co"}


def to_ugm3(value: float, units: str, column: str) -> float | None:
    """normalize the units stations report in, gases often come in ppm or ppb."""
    unit = units.lower().replace("µ", "u")
    if unit in ("ug/m3", "ug/m³", "µg/m³"):
        return value
    gas = GAS_BY_COLUMN.get(column)
    if gas is None:
        return None
    if unit == "ppb":
        return value * MOLAR_MASS[gas] / MOLAR_VOLUME_L
    if unit == "ppm":
        return value * 1000 * MOLAR_MASS[gas] / MOLAR_VOLUME_L
    return None


def parse_stations(payload: Any) -> list[dict[str, Any]]:
    """flatten the station listing to id, name and a sensor id to parameter map."""
    stations = []
    for item in payload.get("results", []):
        sensors = {}
        for sensor in item.get("sensors", []):
            parameter = sensor.get("parameter") or {}
            sensors[sensor["id"]] = (parameter.get("name", ""), parameter.get("units", ""))
        stations.append({"id": item["id"], "name": item.get("name", ""), "sensors": sensors})
    return stations


def parse_latest(
    payload: Any,
    station: dict[str, Any],
    location: Location,
    run_id: str,
    ingested_ts: datetime,
    source: str = "openaq",
) -> list[AirEvent]:
    """group the latest sensor values of a station by hour into air events."""
    by_hour: dict[datetime, dict[str, float]] = {}
    for item in payload.get("results", []):
        parameter, units = station["sensors"].get(item.get("sensorsId"), ("", ""))
        column = PARAMETER_MAP.get(parameter)
        if column is None or item.get("value") is None:
            continue
        value = to_ugm3(float(item["value"]), units, column)
        if value is None or value < 0:
            continue
        stamp = (item.get("datetime") or {}).get("utc")
        if not stamp:
            continue
        hour = floor_to_hour(parse_iso_utc(stamp))
        by_hour.setdefault(hour, {})[column] = value
    station_id = str(station["id"])
    events = []
    for hour, metrics in sorted(by_hour.items()):
        key = location_key(location.location_id, station_id)
        events.append(
            AirEvent(
                event_id=make_event_id(source, key, hour, "air"),
                source=source,
                location_id=location.location_id,
                lat=location.lat,
                lon=location.lon,
                event_ts_utc=hour,
                ingested_ts_utc=ingested_ts,
                run_id=run_id,
                station_id=station_id,
                **metrics,
            )
        )
    return events


class OpenAqSource(Source):
    name = "openaq"

    def __init__(self, config, session=None) -> None:
        super().__init__(config, session)
        self.cfg = config.sources.openaq
        if not self.cfg.api_key:
            raise ValueError("openaq needs an api key, set AIRPULSE_SOURCES__OPENAQ__API_KEY")
        self.headers = {"X-API-Key": self.cfg.api_key}
        self._stations: dict[str, list[dict[str, Any]]] = {}

    def stations_near(self, location: Location) -> list[dict[str, Any]]:
        # station lookups are cached for the life of the process, the network changes slowly.
        if location.location_id not in self._stations:
            payload = get_json(
                self.session,
                f"{self.cfg.base_url}/locations",
                {
                    "coordinates": f"{location.lat},{location.lon}",
                    "radius": self.cfg.radius_m,
                    "limit": self.cfg.max_stations,
                },
                self.cfg.timeout_s,
                headers=self.headers,
            )
            self._stations[location.location_id] = parse_stations(payload)
        return self._stations[location.location_id]

    def fetch(self, locations: Sequence[Location], window: FetchWindow, run_id: str) -> SourceBatch:
        batch = SourceBatch()
        ingested = utc_now()
        for location in locations:
            for station in self.stations_near(location):
                payload = get_json(
                    self.session,
                    f"{self.cfg.base_url}/locations/{station['id']}/latest",
                    {},
                    self.cfg.timeout_s,
                    headers=self.headers,
                )
                batch.air.extend(parse_latest(payload, station, location, run_id, ingested))
        return batch
