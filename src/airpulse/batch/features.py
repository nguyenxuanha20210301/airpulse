"""materialize the forecast feature table from the clean layer."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from pyspark.sql import functions as F

from airpulse.batch.common_io import add_io_args, read_layer, write_layer
from airpulse.common.config import load_config
from airpulse.common.features import build_feature_table
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.schemas import DS_AIR_HOURLY, DS_WEATHER_HOURLY, LAYER_CLEAN, LAYER_CURATED
from airpulse.common.spark import build_spark_session, with_dt

DS_FEATURES = "features"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="forecast features from the clean layer")
    add_io_args(parser)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("batch.features", run_id, cfg.log_level)
    spark = build_spark_session(cfg, "features")
    air = read_layer(spark, cfg, LAYER_CLEAN, DS_AIR_HOURLY, args.input, args.start, args.end)
    weather = read_layer(
        spark, cfg, LAYER_CLEAN, DS_WEATHER_HOURLY, args.input, args.start, args.end
    )
    table = with_dt(build_feature_table(weather, air)).withColumn("feature_run_id", F.lit(run_id))
    path = write_layer(table, cfg, LAYER_CURATED, DS_FEATURES, args.output)
    log.info("wrote features to %s", path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
