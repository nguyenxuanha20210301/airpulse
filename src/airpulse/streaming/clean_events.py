"""parse raw kafka messages and apply the shared cleaning rules in the speed layer.

late records are tolerated up to the watermark delay and repeated event ids inside that delay
are dropped, so provider retries and producer restarts do not double count.
"""

from __future__ import annotations

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from airpulse.common.cleaning import clean_air, clean_weather
from airpulse.common.config import Config
from airpulse.common.schemas import AirEvent, WeatherEvent, spark_schema


def read_kafka_stream(
    spark: SparkSession, cfg: Config, topic: str, starting: str = "latest"
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw: DataFrame, model: type[BaseModel]) -> DataFrame:
    """turn the kafka value column into typed event columns, malformed messages become nulls."""
    return raw.select(
        F.from_json(F.col("value").cast("string"), spark_schema(model)).alias("event")
    ).select("event.*")


def dedupe_within_watermark(df: DataFrame, delay: str) -> DataFrame:
    # the timestamp is part of the key so the dedup state is dropped once the watermark passes it.
    return df.withWatermark("event_ts_utc", delay).dropDuplicates(["event_id", "event_ts_utc"])


def clean_weather_stream(parsed: DataFrame, delay: str, run_id: str) -> DataFrame:
    return dedupe_within_watermark(clean_weather(parsed), delay).withColumn(
        "clean_run_id", F.lit(run_id)
    )


def clean_air_stream(parsed: DataFrame, delay: str, run_id: str) -> DataFrame:
    return dedupe_within_watermark(clean_air(parsed), delay).withColumn(
        "clean_run_id", F.lit(run_id)
    )


def to_kafka_frame(df: DataFrame, key_column: str = "location_id") -> DataFrame:
    """the two columns the kafka sink expects, the whole row serialized as json."""
    return df.select(
        F.col(key_column).cast("string").alias("key"),
        F.to_json(F.struct(*df.columns)).alias("value"),
    )


def raw_streams(
    spark: SparkSession, cfg: Config, suffix: str | None = None
) -> tuple[DataFrame, DataFrame]:
    """parsed weather and air streams, from the live topics or the replay topics."""
    topics = cfg.kafka.topics
    weather_topic = topics.weather_raw if suffix is None else f"{topics.weather_raw}.{suffix}"
    air_topic = topics.air_raw if suffix is None else f"{topics.air_raw}.{suffix}"
    weather = parse_events(read_kafka_stream(spark, cfg, weather_topic), WeatherEvent)
    air = parse_events(read_kafka_stream(spark, cfg, air_topic), AirEvent)
    return weather, air


def clean_streams(
    spark: SparkSession, cfg: Config, run_id: str, suffix: str | None = None
) -> tuple[DataFrame, DataFrame]:
    weather, air = raw_streams(spark, cfg, suffix)
    delay = cfg.streaming.watermark_delay
    return clean_weather_stream(weather, delay, run_id), clean_air_stream(air, delay, run_id)
