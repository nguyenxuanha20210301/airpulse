"""monthly pm2_5 average per location written in the MapReduce pattern.

the mapper emits (location, month) with (value, 1), the reducer sums both parts, and the final
map divides them. the same two functions run on a spark rdd here and would run unchanged under
hadoop streaming, which is the point of the illustration.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Sequence

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from airpulse.batch.common_io import add_io_args, read_layer, write_layer
from airpulse.common.config import load_config
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.schemas import DS_AIR_HOURLY, LAYER_CLEAN, LAYER_CURATED
from airpulse.common.spark import build_spark_session

Key = tuple[str, str]
Value = tuple[float, int]


def mapper(location_id: str, dt: str, pm2_5: float | None) -> Iterable[tuple[Key, Value]]:
    """emit one partial sum per record, records without a value emit nothing."""
    if pm2_5 is None:
        return
    yield (location_id, dt[:7]), (pm2_5, 1)


def reducer(left: Value, right: Value) -> Value:
    return left[0] + right[0], left[1] + right[1]


def finalize(key: Key, value: Value) -> Row:
    location_id, month = key
    total, count = value
    return Row(location_id=location_id, year_month=month, avg_pm2_5=total / count, n_hours=count)


def monthly_average(clean_air: DataFrame) -> DataFrame:
    spark = clean_air.sparkSession
    pairs = clean_air.select("location_id", "dt", "pm2_5_ugm3").rdd.flatMap(
        lambda row: mapper(row["location_id"], row["dt"], row["pm2_5_ugm3"])
    )
    reduced = pairs.reduceByKey(reducer).map(lambda kv: finalize(kv[0], kv[1]))
    return spark.createDataFrame(reduced)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="monthly pm2_5 average in the MapReduce pattern")
    add_io_args(parser)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("mapreduce.monthly_average", run_id, cfg.log_level)
    spark: SparkSession = build_spark_session(cfg, "mapreduce-monthly-average")
    air = read_layer(spark, cfg, LAYER_CLEAN, DS_AIR_HOURLY, args.input, args.start, args.end)
    result = monthly_average(air).withColumn("run_id", F.lit(run_id))
    path = write_layer(
        result, cfg, LAYER_CURATED, "mapreduce_monthly_pm2_5", args.output, partitioned=False
    )
    log.info("wrote monthly averages to %s", path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
