from airpulse.common.model_registry import ModelRegistry
from airpulse.ml.evaluate import evaluate_models
from airpulse.ml.train import model_name, prepare, split_by_time, train_one


def _small_cfg(config):
    cfg = config.model_copy(deep=True)
    cfg.ml.max_iter = 3
    cfg.ml.max_depth = 2
    cfg.ml.test_days = 1
    return cfg


def test_split_by_time_holds_out_the_last_days(feature_table) -> None:
    data = prepare(feature_table, "pm2_5", 1)

    train, test = split_by_time(data, 1)

    assert train.count() + test.count() == data.count()
    assert (
        test.agg({"event_ts_utc": "min"}).first()[0] > train.agg({"event_ts_utc": "max"}).first()[0]
    )
    # the newest row with a known target is at 22h, so the holdout day starts at 22h the day before.
    assert test.count() == 2 * 25


def test_train_evaluate_and_register(spark, config, feature_table, tmp_path) -> None:
    cfg = _small_cfg(config)
    features = feature_table

    model, evaluation, metadata = train_one(cfg, features, "pm2_5", 1)
    rows = {r["model"]: r for r in evaluation.collect()}

    assert set(rows) == {
        "persistence",
        "moving_average_3h",
        "moving_average_24h",
        "same_hour_yesterday",
        "gbt",
    }
    assert metadata["n_test"] == rows["gbt"]["n"]
    assert 0 <= metadata["mae"] < 10
    registry = ModelRegistry(spark, str(tmp_path / "models"))
    version = registry.save(model, model_name("pm2_5", 1), metadata)
    loaded, meta = registry.load(model_name("pm2_5", 1))
    assert meta["version"] == version and meta["horizon_h"] == 1
    assert (
        loaded.transform(prepare(features, "pm2_5", 1).limit(3)).select("prediction").count() == 3
    )

    comparison = evaluate_models(registry, features).collect()
    gbt_rows = [r for r in comparison if r["model"] == "gbt"]
    assert len(gbt_rows) == 1 and gbt_rows[0]["model_version"] == version
    assert len(comparison) == 6 * 4 + 1
