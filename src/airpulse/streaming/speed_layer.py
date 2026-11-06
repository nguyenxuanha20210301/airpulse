"""the speed layer as one spark application running several streaming queries.

queries can be selected so the same code runs as one application on a small cluster or as
several applications when scaling tests need them. all queries read the raw topics, derive
the clean streams and write to kafka, hdfs or cassandra through foreachBatch.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from airpulse.common.config import Config, load_config
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.model_registry import ModelRegistry
from airpulse.common.schemas import DS_AIR_HOURLY, DS_WEATHER_HOURLY, LAYER_MODELS, LAYER_RAW
from airpulse.common.spark import build_spark_session, with_dt
from airpulse.serving.cassandra_sink import read_table, write_batch
from airpulse.streaming.anomaly_alerts import (
    air_threshold_alerts,
    spike_alerts,
    weather_threshold_alerts,
)
from airpulse.streaming.clean_events import clean_streams, raw_streams, to_kafka_frame
from airpulse.streaming.online_forecast import forecast_from_windows, load_models
from airpulse.streaming.window_metrics import (
    air_windows,
    latest_per_location,
    latest_windows,
    weather_windows,
    window_label,
)

QUERIES = ("archive", "clean", "windows", "alerts", "forecast")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="run the speed layer queries")
    parser.add_argument("--config", required=True)
    parser.add_argument("--queries", default=",".join(QUERIES), help="comma separated subset")
    parser.add_argument("--replay", action="store_true", help="consume the replay topics instead")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    return parser.parse_args(argv)


def checkpoint_path(cfg: Config, name: str, replay: bool) -> str:
    suffix = "-replay" if replay else ""
    return f"{cfg.hdfs.url}{cfg.hdfs.root}/{cfg.streaming.checkpoint_dir}/{name}{suffix}"


def write_kafka(df: DataFrame, cfg: Config, topic: str) -> None:
    (
        to_kafka_frame(df)
        .write.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("topic", topic)
        .save()
    )


def start_archive(spark: SparkSession, cfg: Config, args, log) -> list:
    """copy every raw message to the master dataset on hdfs, partitioned by day."""
    suffix = cfg.replay.topic_suffix if args.replay else None
    weather, air = raw_streams(spark, cfg, suffix)
    queries = []
    for name, stream, dataset in (
        ("weather", weather, DS_WEATHER_HOURLY),
        ("air", air, DS_AIR_HOURLY),
    ):
        query = (
            with_dt(stream.where(F.col("event_id").isNotNull()))
            .writeStream.format("parquet")
            .option("path", cfg.hdfs.dataset_path(LAYER_RAW, dataset))
            .option("checkpointLocation", checkpoint_path(cfg, f"archive-{name}", args.replay))
            .partitionBy("dt")
            .trigger(processingTime=cfg.streaming.trigger_interval)
            .queryName(f"archive-{name}")
            .start()
        )
        queries.append(query)
    log.info("archive queries started")
    return queries


def start_clean(spark: SparkSession, cfg: Config, run_id: str, args, log) -> list:
    """publish clean events and keep the latest row per location in cassandra."""
    weather, air = clean_streams(
        spark, cfg, run_id, cfg.replay.topic_suffix if args.replay else None
    )
    tables = cfg.cassandra.tables
    topics = cfg.kafka.topics
    plan = (
        ("weather", weather, topics.weather_clean, tables.weather_latest),
        ("air", air, topics.air_clean, tables.air_latest),
    )
    queries = []
    for name, stream, topic, table in plan:

        def sink(batch_df: DataFrame, batch_id: int, topic=topic, table=table) -> None:
            batch_df.persist()
            write_kafka(batch_df, cfg, topic)
            write_batch(latest_per_location(batch_df), cfg, table)
            batch_df.unpersist()

        queries.append(
            stream.writeStream.foreachBatch(sink)
            .option("checkpointLocation", checkpoint_path(cfg, f"clean-{name}", args.replay))
            .trigger(processingTime=cfg.streaming.trigger_interval)
            .queryName(f"clean-{name}")
            .start()
        )
    log.info("clean queries started")
    return queries


def start_windows(spark: SparkSession, cfg: Config, run_id: str, args, log) -> list:
    """sliding window aggregates per location, every size in the config, upserted in cassandra."""
    weather, air = clean_streams(
        spark, cfg, run_id, cfg.replay.topic_suffix if args.replay else None
    )
    tables = cfg.cassandra.tables
    queries = []
    for size in cfg.streaming.windows:
        label = window_label(size)
        plan = (
            ("air", air_windows(air, size, cfg.streaming.slide), tables.air_windows),
            (
                "weather",
                weather_windows(weather, size, cfg.streaming.slide),
                tables.weather_windows,
            ),
        )
        for name, windowed, table in plan:

            def sink(batch_df: DataFrame, batch_id: int, table=table) -> None:
                write_batch(batch_df, cfg, table)

            queries.append(
                windowed.withWatermark("window_end", cfg.streaming.watermark_delay)
                .writeStream.outputMode("update")
                .foreachBatch(sink)
                .option(
                    "checkpointLocation",
                    checkpoint_path(cfg, f"windows-{name}-{label}", args.replay),
                )
                .trigger(processingTime=cfg.streaming.trigger_interval)
                .queryName(f"windows-{name}-{label}")
                .start()
            )
    log.info("window queries started for %s", cfg.streaming.windows)
    return queries


def start_alerts(spark: SparkSession, cfg: Config, run_id: str, args, log) -> list:
    """threshold alerts from each micro batch plus spikes against the 24 hour windows."""
    weather, air = clean_streams(
        spark, cfg, run_id, cfg.replay.topic_suffix if args.replay else None
    )
    tables = cfg.cassandra.tables
    topic = cfg.kafka.topics.air_alerts

    def publish(alerts: DataFrame) -> None:
        alerts.persist()
        if alerts.head(1):
            write_kafka(alerts, cfg, topic)
            write_batch(alerts, cfg, tables.alerts)
        alerts.unpersist()

    def air_sink(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        alerts = air_threshold_alerts(batch_df, cfg.alerts, run_id)
        windows = read_table(spark, cfg, tables.air_windows).where(F.col("window_size") == "24h")
        alerts = alerts.unionByName(
            spike_alerts(latest_per_location(batch_df), latest_windows(windows), run_id)
        )
        publish(alerts)
        batch_df.unpersist()

    def weather_sink(batch_df: DataFrame, batch_id: int) -> None:
        publish(weather_threshold_alerts(batch_df, cfg.alerts, run_id))

    queries = []
    for name, stream, sink in (("air", air, air_sink), ("weather", weather, weather_sink)):
        queries.append(
            stream.writeStream.foreachBatch(sink)
            .option("checkpointLocation", checkpoint_path(cfg, f"alerts-{name}", args.replay))
            .trigger(processingTime=cfg.streaming.trigger_interval)
            .queryName(f"alerts-{name}")
            .start()
        )
    log.info("alert queries started")
    return queries


def start_forecast(spark: SparkSession, cfg: Config, run_id: str, args, log) -> list:
    """score the latest air and weather values with the models from the registry."""
    _, air = clean_streams(spark, cfg, run_id, cfg.replay.topic_suffix if args.replay else None)
    tables = cfg.cassandra.tables
    registry = ModelRegistry(spark, cfg.hdfs.dataset_path(LAYER_MODELS, ""))
    state = {"models": load_models(registry)}
    log.info("loaded %d models", len(state["models"]))

    def sink(batch_df: DataFrame, batch_id: int) -> None:
        if batch_id % 60 == 0:
            state["models"] = load_models(registry)
        if not state["models"]:
            return
        latest_air = latest_per_location(batch_df)
        weather = read_table(spark, cfg, tables.weather_latest)
        latest = latest_air.join(weather.drop("event_ts_utc"), "location_id", "left")
        windows = read_table(spark, cfg, tables.air_windows)
        weather_windows_df = read_table(spark, cfg, tables.weather_windows)
        w3 = latest_windows(windows.where(F.col("window_size") == "3h")).join(
            latest_windows(weather_windows_df.where(F.col("window_size") == "3h")).select(
                "location_id", "avg_temperature"
            ),
            "location_id",
            "left",
        )
        w24 = latest_windows(windows.where(F.col("window_size") == "24h")).join(
            latest_windows(weather_windows_df.where(F.col("window_size") == "24h")).select(
                "location_id", "avg_temperature", "avg_wind_speed", "avg_humidity"
            ),
            "location_id",
            "left",
        )
        forecast = forecast_from_windows(state["models"], latest, w3, w24, run_id)
        if forecast is not None:
            write_batch(forecast, cfg, tables.forecast)

    query = (
        air.writeStream.foreachBatch(sink)
        .option("checkpointLocation", checkpoint_path(cfg, "forecast", args.replay))
        .trigger(processingTime=cfg.streaming.trigger_interval)
        .queryName("forecast")
        .start()
    )
    log.info("forecast query started")
    return [query]


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("streaming.speed_layer", run_id, cfg.log_level)
    selected = [name.strip() for name in args.queries.split(",") if name.strip()]
    unknown = [name for name in selected if name not in QUERIES]
    if unknown:
        raise SystemExit(f"unknown queries {unknown}, choose from {QUERIES}")
    spark = build_spark_session(cfg, "speed-layer")
    starters = {
        "archive": lambda: start_archive(spark, cfg, args, log),
        "clean": lambda: start_clean(spark, cfg, run_id, args, log),
        "windows": lambda: start_windows(spark, cfg, run_id, args, log),
        "alerts": lambda: start_alerts(spark, cfg, run_id, args, log),
        "forecast": lambda: start_forecast(spark, cfg, run_id, args, log),
    }
    queries = []
    for name in selected:
        queries.extend(starters[name]())
    log.info("%d queries running, profile %s, replay %s", len(queries), cfg.profile, args.replay)
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
