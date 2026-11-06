"""reading and writing the hdfs layers from batch jobs, with local overrides for development."""

from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from airpulse.common.config import Config
from airpulse.common.spark import write_partitioned


def add_io_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", required=True)
    parser.add_argument("--start", type=date.fromisoformat, help="first dt partition, inclusive")
    parser.add_argument("--end", type=date.fromisoformat, help="last dt partition, inclusive")
    parser.add_argument("--input", help="override the hdfs root for reads, for example a local dir")
    parser.add_argument(
        "--output", help="override the hdfs root for writes, for example a local dir"
    )


def layer_path(cfg: Config, layer: str, dataset: str, override: str | None) -> str:
    if override:
        return f"{override.rstrip('/')}/{layer}/{dataset}"
    return cfg.hdfs.dataset_path(layer, dataset)


def read_layer(
    spark: SparkSession,
    cfg: Config,
    layer: str,
    dataset: str,
    override: str | None,
    start: date | None = None,
    end: date | None = None,
) -> DataFrame:
    df = spark.read.parquet(layer_path(cfg, layer, dataset, override))
    if start is not None:
        df = df.where(F.col("dt") >= start.isoformat())
    if end is not None:
        df = df.where(F.col("dt") <= end.isoformat())
    return df


def write_layer(
    df: DataFrame,
    cfg: Config,
    layer: str,
    dataset: str,
    override: str | None,
    partitioned: bool = True,
) -> str:
    path = layer_path(cfg, layer, dataset, override)
    if partitioned:
        # only the partitions present in the frame are replaced, older days are untouched.
        df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        write_partitioned(df, path, mode="overwrite")
    else:
        df.write.mode("overwrite").parquet(path)
    return path
