"""daily, monthly, seasonal and multi year aggregates of the clean layer.

every table is a plain function from a clean frame to a curated frame, the main only reads and
writes. the limits for exceedance days come from the config so WHO and QCVN can be swapped.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.batch.common_io import add_io_args, read_layer, write_layer
from airpulse.common.config import AqiConfig, load_config
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.schemas import DS_AIR_HOURLY, DS_WEATHER_HOURLY, LAYER_CLEAN, LAYER_CURATED
from airpulse.common.spark import build_spark_session

SEASONS = {
    12: "winter",
    1: "winter",
    2: "winter",
    3: "spring",
    4: "spring",
    5: "spring",
    6: "summer",
    7: "summer",
    8: "summer",
    9: "autumn",
    10: "autumn",
    11: "autumn",
}


def season_column(month) -> F.Column:
    expr = F.lit("winter")
    for season in ("spring", "summer", "autumn"):
        months = [m for m, s in SEASONS.items() if s == season]
        expr = F.when(month.isin(months), F.lit(season)).otherwise(expr)
    return expr


def air_daily(clean_air: DataFrame, aqi: AqiConfig) -> DataFrame:
    daily = clean_air.groupBy("location_id", "dt").agg(
        F.avg("pm2_5_ugm3").alias("avg_pm2_5"),
        F.min("pm2_5_ugm3").alias("min_pm2_5"),
        F.max("pm2_5_ugm3").alias("max_pm2_5"),
        F.percentile_approx("pm2_5_ugm3", 0.5).alias("p50_pm2_5"),
        F.percentile_approx("pm2_5_ugm3", 0.95).alias("p95_pm2_5"),
        F.avg("pm10_ugm3").alias("avg_pm10"),
        F.avg("no2_ugm3").alias("avg_no2"),
        F.avg("o3_ugm3").alias("avg_o3"),
        F.avg("aqi_computed").alias("avg_aqi"),
        F.max("aqi_computed").alias("max_aqi"),
        F.count("pm2_5_ugm3").alias("n_hours"),
    )
    return daily.withColumn("above_who", F.col("avg_pm2_5") > aqi.pm2_5_daily_limit_who).withColumn(
        "above_qcvn", F.col("avg_pm2_5") > aqi.pm2_5_daily_limit_qcvn
    )


def weather_daily(clean_weather: DataFrame) -> DataFrame:
    return clean_weather.groupBy("location_id", "dt").agg(
        F.avg("temperature_c").alias("avg_temperature"),
        F.min("temperature_c").alias("min_temperature"),
        F.max("temperature_c").alias("max_temperature"),
        F.sum("precipitation_mm").alias("sum_precipitation"),
        F.avg("humidity_pct").alias("avg_humidity"),
        F.avg("wind_speed_kmh").alias("avg_wind_speed"),
        F.avg("pressure_msl_hpa").alias("avg_pressure"),
        F.count("temperature_c").alias("n_hours"),
    )


def _with_year_month(daily: DataFrame) -> DataFrame:
    return daily.withColumn("year", F.year(F.to_date("dt"))).withColumn(
        "month", F.month(F.to_date("dt"))
    )


def air_monthly(daily: DataFrame) -> DataFrame:
    return (
        _with_year_month(daily)
        .groupBy("location_id", "year", "month")
        .agg(
            F.avg("avg_pm2_5").alias("avg_pm2_5"),
            F.max("max_pm2_5").alias("max_pm2_5"),
            F.avg("avg_pm10").alias("avg_pm10"),
            F.avg("avg_aqi").alias("avg_aqi"),
            F.sum(F.col("above_who").cast("int")).alias("days_above_who"),
            F.sum(F.col("above_qcvn").cast("int")).alias("days_above_qcvn"),
            F.count("dt").alias("n_days"),
        )
    )


def weather_monthly(daily: DataFrame) -> DataFrame:
    return (
        _with_year_month(daily)
        .groupBy("location_id", "year", "month")
        .agg(
            F.avg("avg_temperature").alias("avg_temperature"),
            F.min("min_temperature").alias("min_temperature"),
            F.max("max_temperature").alias("max_temperature"),
            F.sum("sum_precipitation").alias("sum_precipitation"),
            F.avg("avg_humidity").alias("avg_humidity"),
            F.count("dt").alias("n_days"),
        )
    )


def heatmap_month_hour(clean_air: DataFrame, timezone: str) -> DataFrame:
    """average pm2_5 per location, month and hour of day across every year, the seasonality view.

    month and hour follow the local clock, since traffic peaks and night inversions do, and the
    UTC hour is kept next to the local one.
    """
    local_ts = F.from_utc_timestamp("event_ts_utc", timezone)
    return clean_air.groupBy(
        "location_id",
        F.month(local_ts).alias("month"),
        F.hour(local_ts).alias("hour_local"),
        F.hour("event_ts_utc").alias("hour_utc"),
    ).agg(F.avg("pm2_5_ugm3").alias("avg_pm2_5"), F.count("pm2_5_ugm3").alias("n_hours"))


def seasonal(air_daily_df: DataFrame, weather_daily_df: DataFrame) -> DataFrame:
    joined = air_daily_df.join(weather_daily_df, ["location_id", "dt"], "full")
    with_season = joined.withColumn("season", season_column(F.month(F.to_date("dt"))))
    return with_season.groupBy("location_id", "season").agg(
        F.avg("avg_pm2_5").alias("avg_pm2_5"),
        F.avg("avg_temperature").alias("avg_temperature"),
        F.avg("sum_precipitation").alias("avg_daily_precipitation"),
        F.sum(F.col("above_who").cast("int")).alias("days_above_who"),
        F.count("dt").alias("n_days"),
    )


def yearly_trend(air_daily_df: DataFrame, weather_daily_df: DataFrame) -> DataFrame:
    """yearly means per location plus the linear slope across years, the long term trend."""
    joined = _with_year_month(air_daily_df.join(weather_daily_df, ["location_id", "dt"], "full"))
    yearly = joined.groupBy("location_id", "year").agg(
        F.avg("avg_pm2_5").alias("avg_pm2_5"),
        F.avg("avg_temperature").alias("avg_temperature"),
        F.sum("sum_precipitation").alias("sum_precipitation"),
        F.sum(F.col("above_who").cast("int")).alias("days_above_who"),
        F.count("dt").alias("n_days"),
    )
    slopes = yearly.groupBy("location_id").agg(
        F.regr_slope("avg_temperature", "year").alias("temperature_slope_per_year"),
        F.regr_slope("avg_pm2_5", "year").alias("pm2_5_slope_per_year"),
        F.regr_slope("sum_precipitation", "year").alias("precipitation_slope_per_year"),
        F.count("year").alias("n_years"),
    )
    return yearly.join(slopes, "location_id", "left")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="curated trend tables from the clean layer")
    add_io_args(parser)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("batch.trends", run_id, cfg.log_level)
    spark = build_spark_session(cfg, "trends")
    air = read_layer(spark, cfg, LAYER_CLEAN, DS_AIR_HOURLY, args.input, args.start, args.end)
    weather = read_layer(
        spark, cfg, LAYER_CLEAN, DS_WEATHER_HOURLY, args.input, args.start, args.end
    )
    a_daily = air_daily(air, cfg.aqi).persist()
    w_daily = weather_daily(weather).persist()
    tables = {
        "air_daily": a_daily,
        "weather_daily": w_daily,
        "air_monthly": air_monthly(a_daily),
        "weather_monthly": weather_monthly(w_daily),
        "heatmap_month_hour": heatmap_month_hour(air, cfg.timezone),
        "seasonal": seasonal(a_daily, w_daily),
        "yearly_trend": yearly_trend(a_daily, w_daily),
    }
    for name, frame in tables.items():
        path = write_layer(
            frame.withColumn("run_id", F.lit(run_id)),
            cfg,
            LAYER_CURATED,
            name,
            args.output,
            partitioned=False,
        )
        log.info("wrote %s to %s", name, path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
