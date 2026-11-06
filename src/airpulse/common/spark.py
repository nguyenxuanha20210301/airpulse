"""one place to build spark sessions so every job shares the same timezone and connector setup."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from airpulse.common.config import Config


def build_spark_session(cfg: Config, app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(f"{cfg.project}-{app_name}")
    if cfg.spark.master:
        builder = builder.master(cfg.spark.master)
    builder = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(cfg.spark.shuffle_partitions))
        # the dt partition column stays a string instead of being inferred as a date.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    )
    if cfg.spark.use_packages and cfg.spark.packages:
        builder = builder.config("spark.jars.packages", ",".join(cfg.spark.packages))
    if cfg.cassandra.hosts:
        builder = builder.config(
            "spark.cassandra.connection.host", ",".join(cfg.cassandra.hosts)
        ).config("spark.cassandra.connection.port", str(cfg.cassandra.port))
    return builder.getOrCreate()


def with_dt(df: DataFrame, ts_column: str = "event_ts_utc") -> DataFrame:
    """add the dt partition column derived from a UTC timestamp."""
    return df.withColumn("dt", F.date_format(F.col(ts_column), "yyyy-MM-dd"))


def write_partitioned(df: DataFrame, path: str, mode: str = "append") -> None:
    df.write.mode(mode).partitionBy("dt").parquet(path)
