"""simple forecasts every model has to beat, and the error metrics used everywhere."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.common.features import TARGETS, target_column

BASELINES = ("persistence", "moving_average_3h", "moving_average_24h", "same_hour_yesterday")


def add_lag(df: DataFrame, column: str, hours: int, name: str) -> DataFrame:
    """the value of a column hours earlier for the same location, joined on the exact timestamp."""
    past = df.select(
        F.col("location_id"),
        (F.col("event_ts_utc") + F.expr(f"INTERVAL {hours} HOURS")).alias("event_ts_utc"),
        F.col(column).alias(name),
    )
    if hours == 0:
        return df.withColumn(name, F.col(column))
    return df.join(past, ["location_id", "event_ts_utc"], "left")


def baseline_predictions(df: DataFrame, target: str, horizon_h: int) -> DataFrame:
    """add one prediction column per baseline for the given target and horizon."""
    column = TARGETS[target]
    avg_3h = "pm2_5_avg_3h" if target == "pm2_5" else "temperature_avg_3h"
    avg_24h = "pm2_5_avg_24h" if target == "pm2_5" else "temperature_avg_24h"
    # the value seen 24 hours before the forecast time, which is horizon minus 24 hours from now.
    lagged = add_lag(df, column, 24 - horizon_h, "_yesterday")
    return (
        lagged.withColumn("pred_persistence", F.col(column))
        .withColumn("pred_moving_average_3h", F.col(avg_3h))
        .withColumn("pred_moving_average_24h", F.col(avg_24h))
        .withColumn("pred_same_hour_yesterday", F.col("_yesterday"))
        .drop("_yesterday")
    )


def error_metrics(df: DataFrame, actual: str, predicted: str) -> DataFrame:
    """mae, rmse and the sample size over rows where both values exist."""
    both = df.where(F.col(actual).isNotNull() & F.col(predicted).isNotNull())
    diff = F.col(predicted) - F.col(actual)
    return both.agg(
        F.avg(F.abs(diff)).alias("mae"),
        F.sqrt(F.avg(diff * diff)).alias("rmse"),
        F.count(F.lit(1)).alias("n"),
    )


EVALUATION_COLUMNS = ["target", "horizon_h", "model", "mae", "rmse", "n"]


def evaluate_baselines(df: DataFrame, target: str, horizon_h: int) -> DataFrame:
    """one row per baseline with its errors against the future value.

    each metric is collected on its own and the rows are assembled into a small frame, keeping
    the plans flat instead of one union of many copies of the feature pipeline.
    """
    actual = target_column(target, horizon_h)
    scored = baseline_predictions(df, target, horizon_h).persist()
    rows = []
    for name in BASELINES:
        metrics = error_metrics(scored, actual, f"pred_{name}").first()
        rows.append((target, horizon_h, name, metrics["mae"], metrics["rmse"], metrics["n"]))
    scored.unpersist()
    return df.sparkSession.createDataFrame(rows, EVALUATION_COLUMNS)
