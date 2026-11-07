"""copies of the raw history moved back in time, a controlled volume for batch benchmarks.

the archive holds weeks of real observations while the batch layer is built for years, and the
free api quota does not allow downloading decades in one sitting. each copy moves every event
back by a whole number of spans, the span being the days the source covers, so the copies tile
the calendar without overlap. ids are rebuilt from the new timestamps, so the deduplication of
the batch layer keeps every copy, and the result lands under a separate root next to the live
layers, never inside them.
"""

from __future__ import annotations

import argparse
import math
import sys
from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.common.config import load_config
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.schemas import DS_AIR_HOURLY, DS_WEATHER_HOURLY, LAYER_RAW, event_id_column
from airpulse.common.spark import build_spark_session, with_dt

DATASETS = {DS_WEATHER_HOURLY: "weather", DS_AIR_HOURLY: "air"}


def span_days(df: DataFrame) -> int:
    """whole days from the first to the last event, both ends included."""
    first, last = df.agg(F.min("event_ts_utc"), F.max("event_ts_utc")).first()
    return (last.date() - first.date()).days + 1


def copies_for(years: float, span: int) -> int:
    return max(1, math.ceil(years * 365.25 / span))


def shifted_copies(df: DataFrame, domain: str, copies: int, span: int, run_id: str) -> DataFrame:
    """copy k of every row moves back by k spans, copy 0 is the source itself with a new run_id."""
    offsets = df.sparkSession.range(copies).withColumnRenamed("id", "_copy")
    shift = F.expr(f"make_dt_interval(CAST(_copy * {span} AS INT), 0, 0, 0)")
    columns = [c for c in df.columns if c != "dt"]
    moved = (
        df.drop("dt")
        .crossJoin(F.broadcast(offsets))
        .withColumn("event_ts_utc", F.col("event_ts_utc") - shift)
        .withColumn("ingested_ts_utc", F.col("ingested_ts_utc") - shift)
        .withColumn("event_id", event_id_column(domain))
        .withColumn("run_id", F.lit(run_id))
        .select(*columns)
    )
    return with_dt(moved)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="write time shifted copies of the raw layer")
    parser.add_argument("--config", required=True)
    parser.add_argument("--years", type=float, required=True, help="history to cover")
    parser.add_argument("--output", required=True, help="root that receives raw/<dataset>")
    parser.add_argument("--input", help="root to copy from, the profile root by default")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("ingestion.amplify", run_id, cfg.log_level)
    live_root = f"{cfg.hdfs.url}{cfg.hdfs.root}"
    source_root = (args.input or live_root).rstrip("/")
    output_root = args.output.rstrip("/")
    if output_root == live_root.rstrip("/"):
        raise SystemExit("the output must be a separate root, not the live layers")
    spark = build_spark_session(cfg, "amplify")
    frames = {
        dataset: spark.read.parquet(f"{source_root}/{LAYER_RAW}/{dataset}") for dataset in DATASETS
    }
    # one span for both datasets so weather and air copies cover the same days.
    span = max(span_days(frame) for frame in frames.values())
    copies = copies_for(args.years, span)
    for dataset, frame in frames.items():
        path = f"{output_root}/{LAYER_RAW}/{dataset}"
        result = shifted_copies(frame, DATASETS[dataset], copies, span, run_id)
        result.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(path)
        rows = spark.read.parquet(path).count()
        log.info(
            "wrote %d rows of %s in %d copies of %d days to %s", rows, dataset, copies, span, path
        )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
