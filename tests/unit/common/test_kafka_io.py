from datetime import datetime, timezone

from airpulse.common.config import DEFAULT_CONFIG_DIR, load_config
from airpulse.common.kafka_io import DeliveryTracker, consumer_config, producer_config, send_events
from airpulse.common.schemas import AlertEvent

TS = datetime(2026, 11, 6, 17, tzinfo=timezone.utc)


class FakeProducer:
    def __init__(self, fail_every: int = 0) -> None:
        self.messages = []
        self.fail_every = fail_every
        self.flushed = False

    def produce(self, topic, value, key, on_delivery) -> None:
        self.messages.append((topic, key, value))
        err = "boom" if self.fail_every and len(self.messages) % self.fail_every == 0 else None
        on_delivery(err, None)

    def poll(self, timeout) -> None:
        pass

    def flush(self) -> None:
        self.flushed = True


def _alert(i: int) -> AlertEvent:
    return AlertEvent(
        alert_id=f"a{i}",
        alert_day="2026-11-06",
        location_id="ha_noi",
        event_ts_utc=TS,
        detected_ts_utc=TS,
        kind="aqi_unhealthy",
        metric="aqi",
        value=160,
        threshold=150,
        severity="unhealthy",
        run_id="run-1",
    )


def test_producer_and_consumer_configs_follow_the_profile() -> None:
    cfg = load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={}).kafka

    producer = producer_config(cfg, "unit")
    consumer = consumer_config(cfg, "unit")

    assert producer["bootstrap.servers"] == cfg.bootstrap_servers
    assert producer["enable.idempotence"] is True
    assert producer["acks"] == "all"
    assert consumer["group.id"] == "airpulse-unit"
    assert consumer["enable.auto.commit"] is False


def test_send_events_keys_by_location_and_counts_deliveries() -> None:
    producer = FakeProducer()

    tracker = send_events(producer, "air.alerts", [_alert(1), _alert(2)])

    assert tracker.delivered == 2
    assert tracker.failed == 0
    assert producer.flushed
    assert producer.messages[0][0] == "air.alerts"
    assert producer.messages[0][1] == b"ha_noi"
    assert b'"alert_id":"a1"' in producer.messages[0][2]


def test_failed_deliveries_are_reported() -> None:
    tracker = send_events(FakeProducer(fail_every=2), "t", [_alert(i) for i in range(4)])

    assert tracker.delivered == 2
    assert tracker.failed == 2
    assert tracker.last_error == "boom"
    assert isinstance(tracker, DeliveryTracker)
