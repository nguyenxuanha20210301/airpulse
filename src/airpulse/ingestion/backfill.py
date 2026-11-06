"""load years of hourly history from the open-meteo archives straight into the raw layer.

the history is fetched month by month per group of locations and appended as parquet under
raw/<dataset>/dt=YYYY-MM-DD. the same datasets receive the live stream, so the batch layer
sees one master dataset whatever the origin of a row.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date, timedelta

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession

from airpulse.common.config import Config, load_config, load_locations
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.schemas import (
    DS_AIR_HOURLY,
    DS_WEATHER_HOURLY,
    LAYER_RAW,
    AirEvent,
    WeatherEvent,
    spark_schema,
)
from airpulse.common.spark import build_spark_session, with_dt, write_partitioned
from airpulse.common.time_utils import month_chunks, utc_now
from airpulse.ingestion.sources.base import FetchWindow
from airpulse.ingestion.sources.open_meteo import OpenMeteoSource


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="backfill hourly history into the raw layer")
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--start", type=date.fromisoformat, help="first day, defaults to history_start"
    )
    parser.add_argument("--end", type=date.fromisoformat, help="last day, defaults to yesterday")
    parser.add_argument("--domain", choices=["weather", "air", "both"], default="both")
    parser.add_argument("--output", help="override the raw layer root, for example a local dir")
    return parser.parse_args(argv)


def events_to_df(
    spark: SparkSession, events: Sequence[BaseModel], model: type[BaseModel]
) -> DataFrame:
    rows = [event.model_dump() for event in events]
    return with_dt(spark.createDataFrame(rows, schema=spark_schema(model)))


def raw_path(cfg: Config, dataset: str, output: str | None) -> str:
    if output:
        return f"{output.rstrip('/')}/{dataset}"
    return cfg.hdfs.dataset_path(LAYER_RAW, dataset)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("ingestion.backfill", run_id, cfg.log_level)
    start = args.start or cfg.ingestion.history_start
    end = args.end or (utc_now().date() - timedelta(days=1))
    locations = load_locations(cfg)
    source = OpenMeteoSource(cfg)
    spark = build_spark_session(cfg, "backfill")
    total = {"weather": 0, "air": 0}
    for chunk_start, chunk_end in month_chunks(start, end):
        window = FetchWindow(start_date=chunk_start, end_date=chunk_end)
        groups = [
            locations[i : i + source.cfg.locations_per_request]
            for i in range(0, len(locations), source.cfg.locations_per_request)
        ]
        for group in groups:
            if args.domain in ("weather", "both"):
                events = source.fetch_weather(group, window, run_id)
                if events:
                    write_partitioned(
                        events_to_df(spark, events, WeatherEvent),
                        raw_path(cfg, DS_WEATHER_HOURLY, args.output),
                    )
                total["weather"] += len(events)
            if args.domain in ("air", "both"):
                events = source.fetch_air(group, window, run_id)
                if events:
                    write_partitioned(
                        events_to_df(spark, events, AirEvent),
                        raw_path(cfg, DS_AIR_HOURLY, args.output),
                    )
                total["air"] += len(events)
        log.info("stored %s through %s, running total %s", chunk_start, chunk_end, total)
    log.info("backfill finished with %s", total)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
