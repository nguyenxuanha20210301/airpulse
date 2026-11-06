"""cleaning rules shared by the speed layer and the batch layer.

values outside a physical range are set to null instead of dropping the row, so one broken
sensor channel does not erase the other metrics of that hour. rows without a location or a
timestamp are dropped. the computed aqi is added to air rows next to the provider value.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from airpulse.common.aqi import aqi_columns, category_column

# plausible ranges per column, anything outside becomes null.
WEATHER_RANGES: dict[str, tuple[float, float]] = {
    "temperature_c": (-50.0, 60.0),
    "humidity_pct": (0.0, 100.0),
    "dew_point_c": (-60.0, 50.0),
    "apparent_temperature_c": (-60.0, 70.0),
    "precipitation_mm": (0.0, 500.0),
    "rain_mm": (0.0, 500.0),
    "pressure_msl_hpa": (850.0, 1100.0),
    "surface_pressure_hpa": (500.0, 1100.0),
    "cloud_cover_pct": (0.0, 100.0),
    "wind_speed_kmh": (0.0, 400.0),
    "wind_direction_deg": (0.0, 360.0),
    "wind_gusts_kmh": (0.0, 500.0),
}

AIR_RANGES: dict[str, tuple[float, float]] = {
    "pm2_5_ugm3": (0.0, 2000.0),
    "pm10_ugm3": (0.0, 5000.0),
    "co_ugm3": (0.0, 100000.0),
    "no2_ugm3": (0.0, 5000.0),
    "so2_ugm3": (0.0, 5000.0),
    "o3_ugm3": (0.0, 2000.0),
    "dust_ugm3": (0.0, 10000.0),
    "uv_index": (0.0, 20.0),
    "aqi_us": (0.0, 500.0),
    "aqi_eu": (0.0, 500.0),
}

AQI_INPUTS = {
    "pm2_5": "pm2_5_ugm3",
    "pm10": "pm10_ugm3",
    "o3": "o3_ugm3",
    "no2": "no2_ugm3",
    "so2": "so2_ugm3",
    "co": "co_ugm3",
}

REQUIRED = ["event_id", "location_id", "event_ts_utc"]


def null_out_of_range(df: DataFrame, ranges: dict[str, tuple[float, float]]) -> DataFrame:
    for column, (low, high) in ranges.items():
        if column in df.columns:
            df = df.withColumn(
                column,
                F.when(F.col(column).between(low, high), F.col(column)).otherwise(F.lit(None)),
            )
    return df


def drop_incomplete(df: DataFrame) -> DataFrame:
    condition = None
    for column in REQUIRED:
        clause = F.col(column).isNotNull()
        condition = clause if condition is None else condition & clause
    return df.where(condition)


def dedupe_latest(df: DataFrame, keys: list[str] | None = None) -> DataFrame:
    """keep the most recently ingested row per event_id, for batch where the whole set is seen."""
    keys = keys or ["event_id"]
    ranked = df.withColumn(
        "_rank",
        F.row_number().over(Window.partitionBy(*keys).orderBy(F.col("ingested_ts_utc").desc())),
    )
    return ranked.where(F.col("_rank") == 1).drop("_rank")


def add_computed_aqi(df: DataFrame) -> DataFrame:
    inputs = {name: F.col(column) for name, column in AQI_INPUTS.items() if column in df.columns}
    aqi, dominant = aqi_columns(inputs)
    return (
        df.withColumn("aqi_computed", aqi)
        .withColumn("aqi_dominant", dominant)
        .withColumn("aqi_category", category_column(F.col("aqi_computed")))
    )


def clean_weather(df: DataFrame) -> DataFrame:
    return null_out_of_range(drop_incomplete(df), WEATHER_RANGES)


def clean_air(df: DataFrame) -> DataFrame:
    return add_computed_aqi(null_out_of_range(drop_incomplete(df), AIR_RANGES))
