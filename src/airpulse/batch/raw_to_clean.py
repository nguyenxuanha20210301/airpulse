"""rebuild the clean layer from the raw master dataset for a range of days.

the batch version of cleaning sees every copy of an event, so the last ingested copy wins and
the result is exact, unlike the speed layer which can only dedupe inside its watermark.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.batch.common_io import add_io_args, read_layer, write_layer
from airpulse.common.cleaning import clean_air, clean_weather, dedupe_latest
from airpulse.common.config import load_config
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.schemas import DS_AIR_HOURLY, DS_WEATHER_HOURLY, LAYER_CLEAN, LAYER_RAW
from airpulse.common.spark import build_spark_session


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="raw to clean for a range of days")
    add_io_args(parser)
    parser.add_argument("--domain", choices=["weather", "air", "both"], default="both")
    return parser.parse_args(argv)


def raw_to_clean_frame(raw: DataFrame, domain: str, run_id: str) -> DataFrame:
    deduped = dedupe_latest(raw)
    cleaned = clean_weather(deduped) if domain == "weather" else clean_air(deduped)
    return cleaned.withColumn("clean_run_id", F.lit(run_id))


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("batch.raw_to_clean", run_id, cfg.log_level)
    spark = build_spark_session(cfg, "raw-to-clean")
    plan = []
    if args.domain in ("weather", "both"):
        plan.append(("weather", DS_WEATHER_HOURLY))
    if args.domain in ("air", "both"):
        plan.append(("air", DS_AIR_HOURLY))
    for domain, dataset in plan:
        raw = read_layer(spark, cfg, LAYER_RAW, dataset, args.input, args.start, args.end)
        cleaned = raw_to_clean_frame(raw, domain, run_id)
        path = write_layer(cleaned, cfg, LAYER_CLEAN, dataset, args.output)
        log.info("wrote clean %s to %s", domain, path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
