"""write and read the realtime view in cassandra through the spark connector."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from airpulse.common.config import Config

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"


def cassandra_options(cfg: Config, table: str) -> dict[str, str]:
    return {
        "keyspace": cfg.cassandra.keyspace,
        "table": table,
        "spark.cassandra.output.consistency.level": cfg.cassandra.consistency,
        "spark.cassandra.input.consistency.level": cfg.cassandra.consistency,
    }


def write_batch(df: DataFrame, cfg: Config, table: str) -> None:
    """append rows, cassandra upserts on the primary key so repeated batches are harmless."""
    df.write.format(CASSANDRA_FORMAT).options(**cassandra_options(cfg, table)).mode("append").save()


def read_table(spark: SparkSession, cfg: Config, table: str) -> DataFrame:
    return spark.read.format(CASSANDRA_FORMAT).options(**cassandra_options(cfg, table)).load()


def foreach_batch_to_cassandra(
    cfg: Config, table: str, transform: Callable[[DataFrame], DataFrame] | None = None
) -> Callable[[DataFrame, int], None]:
    """build a foreachBatch function, the optional transform runs on each micro batch first."""

    def write(batch_df: DataFrame, batch_id: int) -> None:
        frame = transform(batch_df) if transform is not None else batch_df
        write_batch(frame, cfg, table)

    return write
