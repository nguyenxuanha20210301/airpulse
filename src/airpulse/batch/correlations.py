"""pearson correlations between pm2_5 and the weather, per location, region, month and overall."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from airpulse.batch.common_io import add_io_args, read_layer, write_layer
from airpulse.common.config import Config, load_config, load_locations
from airpulse.common.features import join_weather_air
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.schemas import DS_AIR_HOURLY, DS_WEATHER_HOURLY, LAYER_CLEAN, LAYER_CURATED
from airpulse.common.spark import build_spark_session

WEATHER_VARIABLES = [
    "wind_speed_kmh",
    "humidity_pct",
    "temperature_c",
    "precipitation_mm",
    "pressure_msl_hpa",
    "cloud_cover_pct",
]


def locations_frame(spark: SparkSession, cfg: Config) -> DataFrame:
    rows = [(loc.location_id, loc.region, loc.province) for loc in load_locations(cfg)]
    return spark.createDataFrame(rows, ["location_id", "region", "province"])


def _correlate(df: DataFrame, group: list, scope: str, target: str = "pm2_5_ugm3") -> DataFrame:
    """one row per group and weather variable with the correlation and the sample size."""
    result = None
    for variable in WEATHER_VARIABLES:
        part = df.groupBy(*group).agg(
            F.corr(target, variable).alias("correlation"),
            F.count(F.when(F.col(target).isNotNull() & F.col(variable).isNotNull(), 1)).alias("n"),
        )
        key = F.concat_ws("|", *[F.col(g).cast("string") for g in group]) if group else F.lit("all")
        part = part.select(
            F.lit(scope).alias("scope"),
            key.alias("key"),
            F.lit(variable).alias("variable"),
            "correlation",
            "n",
        )
        result = part if result is None else result.unionByName(part)
    return result


def correlation_table(joined: DataFrame, locations: DataFrame) -> DataFrame:
    with_region = joined.join(locations, "location_id", "left").withColumn(
        "month", F.month("event_ts_utc")
    )
    return (
        _correlate(with_region, ["location_id"], "location")
        .unionByName(_correlate(with_region, ["region"], "region"))
        .unionByName(_correlate(with_region, ["location_id", "month"], "location_month"))
        .unionByName(_correlate(with_region, [], "all"))
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="weather and pollution correlations")
    add_io_args(parser)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("batch.correlations", run_id, cfg.log_level)
    spark = build_spark_session(cfg, "correlations")
    air = read_layer(spark, cfg, LAYER_CLEAN, DS_AIR_HOURLY, args.input, args.start, args.end)
    weather = read_layer(
        spark, cfg, LAYER_CLEAN, DS_WEATHER_HOURLY, args.input, args.start, args.end
    )
    table = correlation_table(join_weather_air(weather, air), locations_frame(spark, cfg))
    path = write_layer(
        table.withColumn("run_id", F.lit(run_id)),
        cfg,
        LAYER_CURATED,
        "correlations",
        args.output,
        partitioned=False,
    )
    log.info("wrote correlations to %s", path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
