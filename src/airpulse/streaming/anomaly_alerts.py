"""alerts from fixed thresholds and from spikes against the rolling 24 hour statistics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.common.aqi import category_column
from airpulse.common.config import AlertsConfig
from airpulse.common.schemas import SCHEMA_VERSION

ALERT_COLUMNS = [
    "alert_id",
    "alert_day",
    "schema_version",
    "location_id",
    "event_ts_utc",
    "detected_ts_utc",
    "kind",
    "metric",
    "value",
    "threshold",
    "severity",
    "run_id",
]


def _as_alert(
    df: DataFrame, kind: str, metric: str, value, threshold, severity, run_id: str
) -> DataFrame:
    return df.select(
        F.sha1(
            F.concat_ws(
                "|", F.col("location_id"), F.lit(kind), F.col("event_ts_utc").cast("string")
            )
        ).alias("alert_id"),
        F.date_format("event_ts_utc", "yyyy-MM-dd").alias("alert_day"),
        F.lit(SCHEMA_VERSION).alias("schema_version"),
        F.col("location_id"),
        F.col("event_ts_utc"),
        F.current_timestamp().alias("detected_ts_utc"),
        F.lit(kind).alias("kind"),
        F.lit(metric).alias("metric"),
        value.cast("double").alias("value"),
        threshold.cast("double").alias("threshold"),
        severity.alias("severity"),
        F.lit(run_id).alias("run_id"),
    )


def air_threshold_alerts(clean_air: DataFrame, cfg: AlertsConfig, run_id: str) -> DataFrame:
    hit = clean_air.where(F.col("aqi_computed") >= cfg.aqi_level)
    return _as_alert(
        hit,
        "aqi_unhealthy",
        "aqi",
        F.col("aqi_computed"),
        F.lit(cfg.aqi_level),
        category_column(F.col("aqi_computed")),
        run_id,
    )


def weather_threshold_alerts(clean_weather: DataFrame, cfg: AlertsConfig, run_id: str) -> DataFrame:
    hot = _as_alert(
        clean_weather.where(F.col("temperature_c") >= cfg.temperature_hot_c),
        "temperature_hot",
        "temperature_c",
        F.col("temperature_c"),
        F.lit(cfg.temperature_hot_c),
        F.lit("hot"),
        run_id,
    )
    cold = _as_alert(
        clean_weather.where(F.col("temperature_c") <= cfg.temperature_cold_c),
        "temperature_cold",
        "temperature_c",
        F.col("temperature_c"),
        F.lit(cfg.temperature_cold_c),
        F.lit("cold"),
        run_id,
    )
    return hot.unionByName(cold)


def spike_alerts(
    latest_air: DataFrame,
    windows_24h: DataFrame,
    run_id: str,
    z_threshold: float = 3.0,
    min_samples: int = 12,
    min_jump_ugm3: float = 10.0,
) -> DataFrame:
    """flag a pm2_5 value far above the 24 hour mean of the same location.

    the absolute jump guard keeps a flat series with a tiny deviation from raising alerts.
    """
    stats = windows_24h.select("location_id", "avg_pm2_5", "std_pm2_5", "n_samples")
    jump = F.col("pm2_5_ugm3") - F.col("avg_pm2_5")
    joined = latest_air.join(stats, "location_id", "inner").where(
        (F.col("n_samples") >= min_samples)
        & (F.col("std_pm2_5") > 0)
        & (jump >= min_jump_ugm3)
        & ((jump / F.col("std_pm2_5")) >= z_threshold)
    )
    return _as_alert(
        joined,
        "pm2_5_spike",
        "pm2_5_ugm3",
        F.col("pm2_5_ugm3"),
        F.col("avg_pm2_5") + F.lit(z_threshold) * F.col("std_pm2_5"),
        F.lit("spike"),
        run_id,
    )
