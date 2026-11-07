"""the feature set of the short term forecast, built the same way in batch and in streaming.

columns are current values, time of day and year, and rolling statistics of the last 3 and 24
hours. the batch layer computes them from the clean history, the speed layer from its sliding
windows, so a model trained on one can score the other.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TARGETS = {"pm2_5": "pm2_5_ugm3", "temperature": "temperature_c"}
HORIZONS_H = (1, 6, 24)

CURRENT_FEATURES = [
    "pm2_5_ugm3",
    "pm10_ugm3",
    "no2_ugm3",
    "o3_ugm3",
    "so2_ugm3",
    "co_ugm3",
    "temperature_c",
    "humidity_pct",
    "wind_speed_kmh",
    "wind_direction_deg",
    "pressure_msl_hpa",
    "precipitation_mm",
    "cloud_cover_pct",
]
TIME_FEATURES = ["hour_of_day", "day_of_week", "month", "hour_sin", "hour_cos"]
ROLLING_FEATURES = [
    "pm2_5_avg_3h",
    "pm2_5_avg_24h",
    "pm2_5_max_24h",
    "temperature_avg_3h",
    "temperature_avg_24h",
    "wind_speed_avg_24h",
    "humidity_avg_24h",
]
FEATURE_COLUMNS = CURRENT_FEATURES + TIME_FEATURES + ROLLING_FEATURES
KEY_COLUMNS = ["location_id", "event_ts_utc"]


def target_column(target: str, horizon_h: int) -> str:
    return f"{target}_t{horizon_h}h"


def add_time_features(df: DataFrame, ts_column: str = "event_ts_utc") -> DataFrame:
    hour = F.hour(F.col(ts_column))
    return (
        df.withColumn("hour_of_day", hour)
        .withColumn("day_of_week", F.dayofweek(F.col(ts_column)))
        .withColumn("month", F.month(F.col(ts_column)))
        .withColumn("hour_sin", F.sin(hour * F.lit(2 * 3.141592653589793 / 24)))
        .withColumn("hour_cos", F.cos(hour * F.lit(2 * 3.141592653589793 / 24)))
    )


def join_weather_air(weather: DataFrame, air: DataFrame) -> DataFrame:
    """one row per location and hour with both domains, missing weather or air stays null."""
    air_columns = [c for c in air.columns if c not in weather.columns or c in KEY_COLUMNS]
    weather_columns = [
        c
        for c in weather.columns
        if c not in ("event_id", "source", "run_id", "ingested_ts_utc", "dt", "station_id")
    ]
    return weather.select(*weather_columns).join(air.select(*air_columns), KEY_COLUMNS, "full")


def add_rolling_features(df: DataFrame) -> DataFrame:
    """rolling statistics over the previous hours of the same location, by time range."""
    seconds = F.col("event_ts_utc").cast("long")
    base = Window.partitionBy("location_id").orderBy(seconds)
    last_3h = base.rangeBetween(-3 * 3600, 0)
    last_24h = base.rangeBetween(-24 * 3600, 0)
    return (
        df.withColumn("pm2_5_avg_3h", F.avg("pm2_5_ugm3").over(last_3h))
        .withColumn("pm2_5_avg_24h", F.avg("pm2_5_ugm3").over(last_24h))
        .withColumn("pm2_5_max_24h", F.max("pm2_5_ugm3").over(last_24h))
        .withColumn("temperature_avg_3h", F.avg("temperature_c").over(last_3h))
        .withColumn("temperature_avg_24h", F.avg("temperature_c").over(last_24h))
        .withColumn("wind_speed_avg_24h", F.avg("wind_speed_kmh").over(last_24h))
        .withColumn("humidity_avg_24h", F.avg("humidity_pct").over(last_24h))
    )


def add_targets(df: DataFrame) -> DataFrame:
    """future values at exactly the horizon, gaps in the series simply leave nulls.

    a frame of width zero placed h hours ahead holds the row of that very hour or nothing, and
    it shares the partitioning and order of the rolling features, so the targets cost no shuffle
    of their own. joining the table to itself once per target and horizon did, six times over a
    wide table, and that pushed executors past their memory limit from a year of history on.
    """
    seconds = F.col("event_ts_utc").cast("long")
    base = Window.partitionBy("location_id").orderBy(seconds)
    result = df
    for target, column in TARGETS.items():
        for horizon in HORIZONS_H:
            ahead = horizon * 3600
            frame = base.rangeBetween(ahead, ahead)
            result = result.withColumn(target_column(target, horizon), F.first(column).over(frame))
    return result


def build_feature_table(weather: DataFrame, air: DataFrame, with_targets: bool = True) -> DataFrame:
    joined = join_weather_air(weather, air)
    features = add_rolling_features(add_time_features(joined))
    if with_targets:
        features = add_targets(features)
    return features


def feature_rows_from_windows(
    latest: DataFrame, windows_3h: DataFrame, windows_24h: DataFrame
) -> DataFrame:
    """assemble the same columns from the speed layer latest values and window aggregates."""
    w3 = windows_3h.select(
        "location_id",
        F.col("avg_pm2_5").alias("pm2_5_avg_3h"),
        F.col("avg_temperature").alias("temperature_avg_3h"),
    )
    w24 = windows_24h.select(
        "location_id",
        F.col("avg_pm2_5").alias("pm2_5_avg_24h"),
        F.col("max_pm2_5").alias("pm2_5_max_24h"),
        F.col("avg_temperature").alias("temperature_avg_24h"),
        F.col("avg_wind_speed").alias("wind_speed_avg_24h"),
        F.col("avg_humidity").alias("humidity_avg_24h"),
    )
    return (
        add_time_features(latest).join(w3, "location_id", "left").join(w24, "location_id", "left")
    )
