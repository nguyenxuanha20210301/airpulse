import time

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions as F

from airpulse.common.features import FEATURE_COLUMNS
from airpulse.common.model_registry import ModelRegistry
from airpulse.streaming.online_forecast import (
    FORECAST_COLUMNS,
    fill_missing_features,
    forecast_from_windows,
    load_models,
    model_name,
    model_versions,
    refresh_models,
)


def _train_tiny(spark):
    rows = [(float(i), float(i) * 2, float(i) * 3 + 1) for i in range(20)]
    df = spark.createDataFrame(rows, ["pm2_5_ugm3", "pm2_5_avg_3h", "label"])
    df = fill_missing_features(df)
    pipeline = Pipeline(
        stages=[
            VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features"),
            LinearRegression(maxIter=5),
        ]
    )
    return pipeline.fit(df)


def test_registry_saves_versions_and_serves_the_latest(spark, tmp_path) -> None:
    registry = ModelRegistry(spark, str(tmp_path / "models"))
    model = _train_tiny(spark)

    assert load_models(registry) == {}
    first = registry.save(model, model_name("pm2_5", 1), {"run_id": "r1", "mae": 0.5})
    loaded, meta = registry.load(model_name("pm2_5", 1))

    assert registry.versions(model_name("pm2_5", 1)) == [first]
    assert registry.latest_version(model_name("pm2_5", 1)) == first
    assert meta["run_id"] == "r1" and meta["version"] == first
    assert loaded.stages[-1].coefficients.size == len(FEATURE_COLUMNS)


def test_refresh_serves_a_new_version_and_keeps_the_unchanged_models(spark, tmp_path) -> None:
    registry = ModelRegistry(spark, str(tmp_path / "models"))
    registry.save(_train_tiny(spark), model_name("pm2_5", 1), {})
    registry.save(_train_tiny(spark), model_name("temperature", 24), {})
    models = load_models(registry)
    kept = models[("temperature", 24)]

    unchanged = refresh_models(registry, models)
    # versions are timestamps to the second, the next save needs a later one.
    time.sleep(1.1)
    second = registry.save(_train_tiny(spark), model_name("pm2_5", 1), {})
    refreshed = refresh_models(registry, models)

    assert unchanged[("pm2_5", 1)] is models[("pm2_5", 1)]
    assert refreshed[("pm2_5", 1)][1] == second
    assert second != models[("pm2_5", 1)][1]
    assert refreshed[("temperature", 24)] is kept
    assert model_versions(refreshed) == {"pm2_5_h1": second, "temperature_h24": kept[1]}


def test_forecast_rows_per_location_and_model(spark, tmp_path) -> None:
    registry = ModelRegistry(spark, str(tmp_path / "models"))
    registry.save(_train_tiny(spark), model_name("pm2_5", 1), {})
    registry.save(_train_tiny(spark), model_name("temperature", 24), {})
    models = load_models(registry)
    latest = spark.createDataFrame(
        [("ha_noi", 30.0, 25.0), ("da_nang", 10.0, 28.0)],
        ["location_id", "pm2_5_ugm3", "temperature_c"],
    ).withColumn("event_ts_utc", F.to_timestamp(F.lit("2026-11-06 05:00:00")))
    w3 = spark.createDataFrame(
        [("ha_noi", 28.0, 24.0)], ["location_id", "avg_pm2_5", "avg_temperature"]
    )
    w24 = spark.createDataFrame(
        [("ha_noi", 20.0, 40.0, 23.0, 4.0, 70.0)],
        [
            "location_id",
            "avg_pm2_5",
            "max_pm2_5",
            "avg_temperature",
            "avg_wind_speed",
            "avg_humidity",
        ],
    )

    forecast = forecast_from_windows(models, latest, w3, w24, "r1")
    rows = forecast.collect()

    assert set(models) == {("pm2_5", 1), ("temperature", 24)}
    assert forecast.columns == FORECAST_COLUMNS
    assert len(rows) == 4
    by_key = {(r["location_id"], r["target"], r["horizon_h"]): r for r in rows}
    assert by_key[("ha_noi", "pm2_5", 1)]["forecast_ts_utc"].hour == 6
    assert by_key[("da_nang", "temperature", 24)]["forecast_ts_utc"].day == 7
    assert (
        by_key[("ha_noi", "pm2_5", 1)]["predicted"] > by_key[("da_nang", "pm2_5", 1)]["predicted"]
    )
