"""poll the enabled sources on a schedule and publish raw events to kafka.

runs forever as a deployment, or one cycle with the once flag when scheduled as a cronjob.
each cycle refetches the last few hours so late corrections from the providers are captured,
the event_id makes the repeats harmless downstream.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Sequence

from airpulse.common.config import Config, Location, load_config, load_locations
from airpulse.common.kafka_io import ensure_topics, make_producer, send_events
from airpulse.common.logging import get_logger, new_run_id
from airpulse.ingestion.sources import FetchWindow, Source, build_sources


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="publish weather and air events to kafka")
    parser.add_argument(
        "--config", required=True, help="profile file, for example configs/dev.yaml"
    )
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    parser.add_argument("--interval-minutes", type=int, help="override the configured interval")
    return parser.parse_args(argv)


def run_cycle(
    sources: Sequence[Source],
    locations: Sequence[Location],
    window: FetchWindow,
    producer,
    cfg: Config,
    run_id: str,
    log,
) -> dict[str, int]:
    """fetch every source once and publish, a failing source does not stop the others."""
    counts = {"weather": 0, "air": 0, "failed_sources": 0}
    topics = cfg.kafka.topics
    for source in sources:
        try:
            batch = source.fetch(locations, window, run_id)
        except Exception:
            log.exception("source %s failed, continuing with the others", source.name)
            counts["failed_sources"] += 1
            continue
        weather = send_events(producer, topics.weather_raw, batch.weather)
        air = send_events(producer, topics.air_raw, batch.air)
        counts["weather"] += weather.delivered
        counts["air"] += air.delivered
        log.info(
            "source %s published %d weather and %d air events, %d failed deliveries",
            source.name,
            weather.delivered,
            air.delivered,
            weather.failed + air.failed,
        )
    return counts


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("ingestion.producer", run_id, cfg.log_level)
    locations = load_locations(cfg)
    sources = build_sources(cfg)
    topics = cfg.kafka.topics
    producer = make_producer(cfg.kafka, f"producer-{run_id}")
    created = ensure_topics(cfg.kafka, [topics.weather_raw, topics.air_raw, topics.air_alerts])
    if created:
        log.info("created topics %s", created)
    window = FetchWindow(past_hours=cfg.ingestion.past_hours, forecast_hours=1)
    interval_s = 60 * (args.interval_minutes or cfg.ingestion.interval_minutes)
    log.info(
        "polling %d sources for %d locations every %d s", len(sources), len(locations), interval_s
    )
    while True:
        started = time.monotonic()
        counts = run_cycle(sources, locations, window, producer, cfg, run_id, log)
        log.info("cycle done %s", counts)
        if args.once:
            return 1 if counts["failed_sources"] == len(sources) else 0
        time.sleep(max(0.0, interval_s - (time.monotonic() - started)))


if __name__ == "__main__":
    sys.exit(main())
