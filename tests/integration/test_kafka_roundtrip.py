"""publish a few events to a scratch topic and read them back, needs a reachable broker."""

import uuid
from datetime import datetime, timezone

import pytest
from confluent_kafka import Consumer

from airpulse.common.kafka_io import consumer_config, ensure_topics, make_producer, send_events
from airpulse.common.schemas import AirEvent, make_event_id

pytestmark = pytest.mark.integration


def test_events_survive_a_roundtrip(live_config) -> None:
    topic = f"airpulse.test.{uuid.uuid4().hex[:8]}"
    ensure_topics(live_config.kafka, [topic])
    now = datetime.now(timezone.utc).replace(microsecond=0)
    events = [
        AirEvent(
            event_id=make_event_id("test", f"loc_{i}", now, "air"),
            source="test",
            location_id=f"loc_{i}",
            lat=21.0,
            lon=105.8,
            event_ts_utc=now,
            ingested_ts_utc=now,
            run_id="itest",
            pm2_5_ugm3=float(i),
        )
        for i in range(5)
    ]

    tracker = send_events(make_producer(live_config.kafka, "itest"), topic, events)
    assert tracker.delivered == 5

    consumer = Consumer(consumer_config(live_config.kafka, f"itest-{uuid.uuid4().hex[:6]}"))
    consumer.subscribe([topic])
    got = []
    for _ in range(60):
        message = consumer.poll(1.0)
        if message is None or message.error():
            continue
        got.append(AirEvent.model_validate_json(message.value()))
        if len(got) == 5:
            break
    consumer.close()

    assert sorted(e.location_id for e in got) == sorted(e.location_id for e in events)
