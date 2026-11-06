from tests.unit.ingestion.conftest import NOW, FakeProducer

from airpulse.common.schemas import AirEvent, WeatherEvent, make_event_id
from airpulse.ingestion.producer import parse_args, run_cycle
from airpulse.ingestion.sources.base import FetchWindow, Source, SourceBatch


class GoodSource(Source):
    name = "good"

    def fetch(self, locations, window, run_id) -> SourceBatch:
        batch = SourceBatch()
        for loc in locations:
            meta = {
                "source": "good",
                "location_id": loc.location_id,
                "lat": loc.lat,
                "lon": loc.lon,
                "event_ts_utc": NOW,
                "ingested_ts_utc": NOW,
                "run_id": run_id,
            }
            batch.weather.append(
                WeatherEvent(
                    event_id=make_event_id("good", loc.location_id, NOW, "weather"),
                    temperature_c=20.0,
                    **meta,
                )
            )
            batch.air.append(
                AirEvent(
                    event_id=make_event_id("good", loc.location_id, NOW, "air"),
                    pm2_5_ugm3=5.0,
                    **meta,
                )
            )
        return batch


class BrokenSource(Source):
    name = "broken"

    def fetch(self, locations, window, run_id) -> SourceBatch:
        raise RuntimeError("api down")


class Log:
    def __init__(self) -> None:
        self.lines = []

    def info(self, msg, *args) -> None:
        self.lines.append(msg % args)

    def exception(self, msg, *args) -> None:
        self.lines.append("exception " + msg % args)


def test_cycle_publishes_to_raw_topics_and_survives_a_broken_source(config, locations) -> None:
    producer = FakeProducer()
    sources = [BrokenSource(config, session=object()), GoodSource(config, session=object())]
    log = Log()

    counts = run_cycle(sources, locations, FetchWindow(), producer, config, "r1", log)

    assert counts == {"weather": 2, "air": 2, "failed_sources": 1}
    topics = {topic for topic, _, _ in producer.messages}
    assert topics == {config.kafka.topics.weather_raw, config.kafka.topics.air_raw}
    assert producer.messages[0][1] == "ha_noi"
    assert any(line.startswith("exception") and "broken" in line for line in log.lines)


def test_args_default_to_a_forever_loop() -> None:
    args = parse_args(["--config", "configs/dev.yaml"])

    assert args.once is False
    assert args.interval_minutes is None
