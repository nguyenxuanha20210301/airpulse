"""replay history from the raw layer into kafka at a configurable speed.

replayed events go to separate topics so the watermarks of the live stream are not disturbed.
with pacing on, speed_factor data hours are published per wall clock hour. with shift_to_now
the timestamps are moved so the first replayed hour is the current hour, which keeps dashboards
looking live during a demo.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Iterator, Sequence
from datetime import date, datetime, timedelta

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.common.config import Config, load_config
from airpulse.common.kafka_io import ensure_topics, make_producer, send_events
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.schemas import (
    DS_AIR_HOURLY,
    DS_WEATHER_HOURLY,
    LAYER_RAW,
    AirEvent,
    WeatherEvent,
    make_event_id,
)
from airpulse.common.spark import build_spark_session
from airpulse.common.time_utils import UTC, floor_to_hour, utc_now


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="replay raw history into kafka")
    parser.add_argument("--config", required=True)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--domain", choices=["weather", "air", "both"], default="both")
    parser.add_argument("--speed", type=float, help="override replay.speed_factor")
    parser.add_argument("--no-pacing", action="store_true", help="publish as fast as possible")
    parser.add_argument("--shift-to-now", action="store_true")
    parser.add_argument("--input", help="override the raw layer root, for example a local dir")
    return parser.parse_args(argv)


def replay_topic(base: str, suffix: str) -> str:
    return f"{base}.{suffix}"


def select_range(df: DataFrame, start: date, end: date) -> DataFrame:
    return df.where((F.col("dt") >= start.isoformat()) & (F.col("dt") <= end.isoformat()))


def shift_delta(first_ts: datetime, now: datetime | None = None) -> timedelta:
    """offset that maps the first replayed hour onto the current hour."""
    now = now or utc_now()
    return floor_to_hour(now) - floor_to_hour(first_ts)


def rewrite(event: BaseModel, run_id: str, delta: timedelta, ingested: datetime) -> BaseModel:
    """stamp the replay run on the event, moving its timestamp and id when shifting."""
    changes = {"run_id": run_id, "ingested_ts_utc": ingested}
    if delta:
        ts = event.event_ts_utc + delta
        domain = "air" if isinstance(event, AirEvent) else "weather"
        changes["event_ts_utc"] = ts
        changes["event_id"] = make_event_id(event.source, event.location_id, ts, domain)
    return event.model_copy(update=changes)


def hourly_groups(
    rows: Iterator, model: type[BaseModel]
) -> Iterator[tuple[datetime, list[BaseModel]]]:
    """yield rows ordered by time, grouped by event hour."""
    current_hour = None
    bucket: list[BaseModel] = []
    for row in rows:
        data = row.asDict()
        data.pop("dt", None)
        for key in ("event_ts_utc", "ingested_ts_utc"):
            if data[key].tzinfo is None:
                data[key] = data[key].replace(tzinfo=UTC)
        event = model(**data)
        hour = floor_to_hour(event.event_ts_utc)
        if current_hour is not None and hour != current_hour:
            yield current_hour, bucket
            bucket = []
        current_hour = hour
        bucket.append(event)
    if bucket:
        yield current_hour, bucket


def replay_dataset(
    df: DataFrame, model: type[BaseModel], topic: str, producer, cfg: Config, run_id: str, args, log
) -> int:
    speed = args.speed or cfg.replay.speed_factor
    pacing = cfg.replay.pacing and not args.no_pacing
    shift = cfg.replay.shift_to_now or args.shift_to_now
    rows = df.orderBy("event_ts_utc").toLocalIterator()
    delta = timedelta(0)
    sent = 0
    for index, (hour, events) in enumerate(hourly_groups(rows, model)):
        if index == 0 and shift:
            delta = shift_delta(hour)
        ingested = utc_now()
        tracker = send_events(
            producer, topic, [rewrite(e, run_id, delta, ingested) for e in events]
        )
        sent += tracker.delivered
        if sent and sent % 10000 < len(events):
            log.info("replayed %d events into %s, at hour %s", sent, topic, hour.isoformat())
        if pacing:
            time.sleep(3600.0 / speed)
    return sent


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("ingestion.replay", run_id, cfg.log_level)
    start = args.start or cfg.replay.start_date
    end = args.end or cfg.replay.end_date
    if start is None or end is None:
        raise SystemExit("a start and end date are needed, in the config or on the command line")
    spark = build_spark_session(cfg, "replay")
    producer = make_producer(cfg.kafka, f"replay-{run_id}")
    topics = cfg.kafka.topics
    plan = []
    if args.domain in ("weather", "both"):
        plan.append(
            (
                DS_WEATHER_HOURLY,
                WeatherEvent,
                replay_topic(topics.weather_raw, cfg.replay.topic_suffix),
            )
        )
    if args.domain in ("air", "both"):
        plan.append(
            (DS_AIR_HOURLY, AirEvent, replay_topic(topics.air_raw, cfg.replay.topic_suffix))
        )
    ensure_topics(cfg.kafka, [topic for _, _, topic in plan])
    for dataset, model, topic in plan:
        root = (
            args.input.rstrip("/") + "/" + dataset
            if args.input
            else cfg.hdfs.dataset_path(LAYER_RAW, dataset)
        )
        df = select_range(spark.read.parquet(root), start, end)
        sent = replay_dataset(df, model, topic, producer, cfg, run_id, args, log)
        log.info("finished %s, %d events sent to %s", dataset, sent, topic)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
