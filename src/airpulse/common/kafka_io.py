"""thin wrappers around confluent kafka so jobs share one producer and consumer setup."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel

from airpulse.common.config import KafkaConfig

log = logging.getLogger(__name__)


def producer_config(cfg: KafkaConfig, client_id: str) -> dict:
    # idempotent producer with acks from every in sync replica, duplicates on retry are avoided
    # at the broker and ordering per partition is kept.
    return {
        "bootstrap.servers": cfg.bootstrap_servers,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "gzip",
        "linger.ms": 50,
        "batch.num.messages": 1000,
    }


def consumer_config(cfg: KafkaConfig, group: str) -> dict:
    return {
        "bootstrap.servers": cfg.bootstrap_servers,
        "group.id": f"{cfg.consumer_group_prefix}-{group}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }


def make_producer(cfg: KafkaConfig, client_id: str) -> Producer:
    return Producer(producer_config(cfg, client_id))


def ensure_topics(cfg: KafkaConfig, names: Iterable[str]) -> list[str]:
    """create the topics that do not exist yet with the partition count of the profile."""
    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = [name for name in names if name not in existing]
    if not wanted:
        return []
    specs = [NewTopic(name, cfg.partitions, cfg.replication_factor) for name in wanted]
    for future in admin.create_topics(specs).values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return wanted


class DeliveryTracker:
    """count delivered and failed messages from the producer callbacks."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0
        self.last_error: str | None = None

    def __call__(self, err, msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            self.last_error = str(err)


def send_events(
    producer: Producer,
    topic: str,
    events: Iterable[BaseModel],
    key_field: str = "location_id",
    tracker: DeliveryTracker | None = None,
    flush: bool = True,
) -> DeliveryTracker:
    """publish models as json keyed by location so one place stays in one partition.

    without flush the call returns as soon as the messages are queued, the caller shares one
    tracker across calls and flushes once at the end, which lets the client fill its batches.
    """
    tracker = tracker if tracker is not None else DeliveryTracker()
    for event in events:
        payload = event.model_dump_json().encode("utf-8")
        key = str(getattr(event, key_field)).encode("utf-8")
        while True:
            try:
                producer.produce(topic, value=payload, key=key, on_delivery=tracker)
                break
            except BufferError:
                # the local queue is full, serve callbacks until there is room again.
                producer.poll(0.1)
        producer.poll(0)
    if not flush:
        return tracker
    producer.flush()
    if tracker.failed:
        log.error(
            "%d of %d messages failed, last error %s",
            tracker.failed,
            tracker.delivered + tracker.failed,
            tracker.last_error,
        )
    return tracker
