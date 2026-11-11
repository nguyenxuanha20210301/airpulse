"""score the latest values with the models trained in the batch layer."""

from __future__ import annotations

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.common.features import FEATURE_COLUMNS, HORIZONS_H, TARGETS, feature_rows_from_windows
from airpulse.common.model_registry import ModelRegistry

FORECAST_COLUMNS = [
    "location_id",
    "event_ts_utc",
    "target",
    "horizon_h",
    "forecast_ts_utc",
    "predicted",
    "model_version",
    "run_id",
]


def model_name(target: str, horizon_h: int) -> str:
    return f"{target}_h{horizon_h}"


Models = dict[tuple[str, int], tuple[PipelineModel, str]]


def refresh_models(registry: ModelRegistry, models: Models) -> Models:
    """load every model whose latest version is not the one in memory, keep the others.

    training writes the pipeline first and moves the latest pointer of the model last, so a
    version is only seen here once it is complete. reading the pointers is a few small calls
    on hdfs, cheap enough for every micro batch. a model that was never trained is skipped.
    """
    refreshed = dict(models)
    for target in TARGETS:
        for horizon in HORIZONS_H:
            name = model_name(target, horizon)
            latest = registry.latest_version(name)
            loaded = models.get((target, horizon))
            if latest is None or (loaded is not None and loaded[1] == latest):
                continue
            model, meta = registry.load(name, latest)
            refreshed[(target, horizon)] = (model, meta["version"])
    return refreshed


def load_models(registry: ModelRegistry) -> Models:
    """every available target and horizon at its latest version."""
    return refresh_models(registry, {})


def model_versions(models: Models) -> dict[str, str]:
    return {
        model_name(target, horizon): version for (target, horizon), (_, version) in models.items()
    }


def fill_missing_features(df: DataFrame) -> DataFrame:
    """tree models in spark cannot take nulls, absent inputs become zero like in training."""
    for column in FEATURE_COLUMNS:
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast("double"))
    return df.fillna(0.0, subset=FEATURE_COLUMNS)


def score(models: dict, features: DataFrame, run_id: str) -> DataFrame | None:
    """one forecast row per location, target and horizon."""
    prepared = fill_missing_features(features)
    result = None
    for (target, horizon), (model, version) in models.items():
        scored = model.transform(prepared).select(
            "location_id",
            "event_ts_utc",
            F.lit(target).alias("target"),
            F.lit(horizon).alias("horizon_h"),
            (F.col("event_ts_utc") + F.expr(f"INTERVAL {horizon} HOURS")).alias("forecast_ts_utc"),
            F.col("prediction").alias("predicted"),
            F.lit(version).alias("model_version"),
            F.lit(run_id).alias("run_id"),
        )
        result = scored if result is None else result.unionByName(scored)
    return result


def forecast_from_windows(
    models: dict, latest: DataFrame, windows_3h: DataFrame, windows_24h: DataFrame, run_id: str
) -> DataFrame | None:
    return score(models, feature_rows_from_windows(latest, windows_3h, windows_24h), run_id)
