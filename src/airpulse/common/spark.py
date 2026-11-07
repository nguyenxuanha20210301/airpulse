"""one place to build spark sessions so every job shares the same timezone and connector setup."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from airpulse.common.config import Config, Location, load_locations

LOCATION_COLUMNS = list(Location.model_fields)


def session_conf(cfg: Config) -> dict[str, str]:
    """spark settings derived from the config, apart from the builder so they can be checked."""
    conf = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(cfg.spark.shuffle_partitions),
        # the dt partition column stays a string instead of being inferred as a date.
        "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
        # the writing client picks the replication of each file and falls back to three, whatever
        # the namenode is configured with, so the profile value is passed down explicitly.
        "spark.hadoop.dfs.replication": str(cfg.hdfs.replication),
    }
    if cfg.spark.use_packages and cfg.spark.packages:
        conf["spark.jars.packages"] = ",".join(cfg.spark.packages)
    if cfg.cassandra.hosts:
        conf["spark.cassandra.connection.host"] = ",".join(cfg.cassandra.hosts)
        conf["spark.cassandra.connection.port"] = str(cfg.cassandra.port)
    if cfg.hive.metastore_uri:
        # tables registered by spark land in the shared metastore, which is what trino reads.
        conf["hive.metastore.uris"] = cfg.hive.metastore_uri
        conf["spark.sql.catalogImplementation"] = "hive"
        conf["spark.sql.warehouse.dir"] = f"{cfg.hdfs.url}{cfg.hdfs.root}/{cfg.hive.warehouse_dir}"
    return conf


def build_spark_session(cfg: Config, app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(f"{cfg.project}-{app_name}")
    if cfg.spark.master:
        builder = builder.master(cfg.spark.master)
    for key, value in session_conf(cfg).items():
        builder = builder.config(key, value)
    if cfg.hive.metastore_uri:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()


def with_dt(df: DataFrame, ts_column: str = "event_ts_utc") -> DataFrame:
    """add the dt partition column derived from a UTC timestamp."""
    return df.withColumn("dt", F.date_format(F.col(ts_column), "yyyy-MM-dd"))


def write_partitioned(df: DataFrame, path: str, mode: str = "append") -> None:
    df.write.mode(mode).partitionBy("dt").parquet(path)


def locations_frame(spark: SparkSession, cfg: Config) -> DataFrame:
    """the sampling points of the config as a frame, with name, coordinates, region and province."""
    rows = [
        tuple(getattr(loc, column) for column in LOCATION_COLUMNS) for loc in load_locations(cfg)
    ]
    return spark.createDataFrame(rows, LOCATION_COLUMNS)
