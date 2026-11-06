"""sliding window aggregates and latest values per location for the realtime view."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def air_aggregates() -> list:
    # built lazily, column expressions need a live spark context.
    return [
        F.avg("pm2_5_ugm3").alias("avg_pm2_5"),
        F.max("pm2_5_ugm3").alias("max_pm2_5"),
        F.stddev_samp("pm2_5_ugm3").alias("std_pm2_5"),
        F.avg("pm10_ugm3").alias("avg_pm10"),
        F.avg("no2_ugm3").alias("avg_no2"),
        F.avg("o3_ugm3").alias("avg_o3"),
        F.avg("aqi_computed").alias("avg_aqi"),
        F.max("aqi_computed").alias("max_aqi"),
        F.count(F.lit(1)).alias("n_samples"),
    ]


def weather_aggregates() -> list:
    return [
        F.avg("temperature_c").alias("avg_temperature"),
        F.min("temperature_c").alias("min_temperature"),
        F.max("temperature_c").alias("max_temperature"),
        F.avg("humidity_pct").alias("avg_humidity"),
        F.avg("wind_speed_kmh").alias("avg_wind_speed"),
        F.sum("precipitation_mm").alias("sum_precipitation"),
        F.count(F.lit(1)).alias("n_samples"),
    ]


def window_label(size: str) -> str:
    """turn a spark duration like 24 hours into a short label like 24h."""
    number, unit = size.split()
    return f"{number}{unit[0]}"


def _windowed(df: DataFrame, size: str, slide: str, aggregates: list) -> DataFrame:
    grouped = df.groupBy("location_id", F.window("event_ts_utc", size, slide).alias("w")).agg(
        *aggregates
    )
    return grouped.select(
        "location_id",
        F.lit(window_label(size)).alias("window_size"),
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        *[c for c in grouped.columns if c not in ("location_id", "w")],
    )


def air_windows(clean_air: DataFrame, size: str, slide: str) -> DataFrame:
    return _windowed(clean_air, size, slide, air_aggregates())


def weather_windows(clean_weather: DataFrame, size: str, slide: str) -> DataFrame:
    return _windowed(clean_weather, size, slide, weather_aggregates())


def latest_per_location(df: DataFrame, ts_column: str = "event_ts_utc") -> DataFrame:
    """one row per location, the newest by event time, used on each micro batch."""
    ranked = df.withColumn(
        "_rank",
        F.row_number().over(Window.partitionBy("location_id").orderBy(F.col(ts_column).desc())),
    )
    return ranked.where(F.col("_rank") == 1).drop("_rank")


def latest_windows(df: DataFrame) -> DataFrame:
    """the most recent window per location and size inside a micro batch."""
    ranked = df.withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("location_id", "window_size").orderBy(F.col("window_end").desc())
        ),
    )
    return ranked.where(F.col("_rank") == 1).drop("_rank")
