"""registry of data sources, a new source is one module and one entry here."""

from __future__ import annotations

import requests

from airpulse.common.config import Config
from airpulse.ingestion.sources.base import FetchWindow, Source, SourceBatch
from airpulse.ingestion.sources.open_meteo import OpenMeteoSource
from airpulse.ingestion.sources.openaq import OpenAqSource
from airpulse.ingestion.sources.openweathermap import OpenWeatherMapSource

SOURCE_TYPES: dict[str, type[Source]] = {
    cls.name: cls for cls in (OpenMeteoSource, OpenAqSource, OpenWeatherMapSource)
}


def build_sources(config: Config, session: requests.Session | None = None) -> list[Source]:
    unknown = [name for name in config.ingestion.enabled_sources if name not in SOURCE_TYPES]
    if unknown:
        raise ValueError(f"unknown sources {unknown}, known ones are {sorted(SOURCE_TYPES)}")
    return [SOURCE_TYPES[name](config, session) for name in config.ingestion.enabled_sources]


__all__ = ["SOURCE_TYPES", "FetchWindow", "Source", "SourceBatch", "build_sources"]
