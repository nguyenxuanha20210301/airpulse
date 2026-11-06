import json
from datetime import datetime, timezone

import pytest

from airpulse.common.config import DEFAULT_CONFIG_DIR, Location, load_config

NOW = datetime(2026, 11, 6, 17, 30, tzinfo=timezone.utc)


class FakeResponse:
    def __init__(self, payload, status=200) -> None:
        self.payload = payload
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")

    def json(self):
        return json.loads(json.dumps(self.payload))


class FakeSession:
    """answer get calls from a list of (url fragment, payload) rules and record every call."""

    def __init__(self, rules) -> None:
        self.rules = rules
        self.calls = []

    def get(self, url, params=None, timeout=None, headers=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        for fragment, payload in self.rules:
            if fragment in url:
                return FakeResponse(payload(params) if callable(payload) else payload)
        return FakeResponse({"error": "no rule"}, status=404)


class FakeProducer:
    def __init__(self) -> None:
        self.messages = []

    def produce(self, topic, value, key, on_delivery) -> None:
        self.messages.append((topic, key.decode(), json.loads(value)))
        on_delivery(None, None)

    def poll(self, timeout) -> None:
        pass

    def flush(self) -> None:
        pass


@pytest.fixture
def config():
    return load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={})


@pytest.fixture
def locations():
    return [
        Location(
            location_id="ha_noi",
            name="Ha Noi",
            lat=21.0285,
            lon=105.8542,
            region="north",
            province="Ha Noi",
        ),
        Location(
            location_id="da_nang",
            name="Da Nang",
            lat=16.0544,
            lon=108.2022,
            region="central",
            province="Da Nang",
        ),
    ]


def open_meteo_payload(variables, times, values_per_location):
    """build the multi location shape of the api, one object per location."""
    items = []
    for values in values_per_location:
        hourly = {"time": times}
        for name in variables:
            hourly[name] = values.get(name, [None] * len(times))
        items.append({"hourly": hourly, "hourly_units": {}})
    return items
