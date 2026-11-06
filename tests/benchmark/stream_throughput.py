"""sample the end offsets of the replay topics and report messages per second."""

from __future__ import annotations

import argparse
import time

from confluent_kafka import Consumer, TopicPartition

from airpulse.common.config import load_config
from airpulse.common.kafka_io import consumer_config


def end_offsets(consumer: Consumer, topic: str) -> int:
    metadata = consumer.list_topics(topic, timeout=10)
    total = 0
    for partition in metadata.topics[topic].partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        total += high
    return total


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--interval", type=int, default=5)
    args = parser.parse_args()
    cfg = load_config(args.config)
    topics = [
        f"{cfg.kafka.topics.weather_raw}.{cfg.replay.topic_suffix}",
        f"{cfg.kafka.topics.air_raw}.{cfg.replay.topic_suffix}",
    ]
    consumer = Consumer(consumer_config(cfg.kafka, "benchmark"))
    start = {topic: end_offsets(consumer, topic) for topic in topics}
    started = time.monotonic()
    while time.monotonic() - started < args.seconds:
        time.sleep(args.interval)
        elapsed = time.monotonic() - started
        for topic in topics:
            delta = end_offsets(consumer, topic) - start[topic]
            print(f"{topic} {delta} messages in {elapsed:.0f}s, {delta / elapsed:.0f} per second")
    consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
