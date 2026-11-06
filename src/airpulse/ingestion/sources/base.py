"""the interface every data source implements plus the http helpers they share."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any, ClassVar, TypeVar

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airpulse.common.config import Config, Location
from airpulse.common.schemas import AirEvent, WeatherEvent

T = TypeVar("T")


@dataclass(frozen=True)
class FetchWindow:
    """a window around now in hours, or a history range when start_date is set."""

    past_hours: int = 3
    forecast_hours: int = 1
    start_date: date | None = None
    end_date: date | None = None

    @property
    def is_history(self) -> bool:
        return self.start_date is not None


@dataclass
class SourceBatch:
    weather: list[WeatherEvent] = field(default_factory=list)
    air: list[AirEvent] = field(default_factory=list)

    def extend(self, other: SourceBatch) -> None:
        self.weather.extend(other.weather)
        self.air.extend(other.air)

    def __len__(self) -> int:
        return len(self.weather) + len(self.air)


class Source(ABC):
    """a source turns locations and a window into weather and air events."""

    name: ClassVar[str]

    def __init__(self, config: Config, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or make_session()

    @abstractmethod
    def fetch(self, locations: Sequence[Location], window: FetchWindow, run_id: str) -> SourceBatch:
        """return the events for the given locations and window."""


def make_session(retries: int = 5, backoff_s: float = 1.0) -> requests.Session:
    """a session that retries on rate limits and server errors with exponential backoff."""
    retry = Retry(
        total=retries,
        backoff_factor=backoff_s,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def get_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout_s: int,
    headers: dict[str, str] | None = None,
) -> Any:
    response = session.get(url, params=params, timeout=timeout_s, headers=headers)
    response.raise_for_status()
    return response.json()


def chunked(items: Sequence[T], size: int) -> list[list[T]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]
