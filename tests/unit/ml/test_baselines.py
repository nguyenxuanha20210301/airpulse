from pyspark.sql import functions as F

from airpulse.common.features import target_column
from airpulse.ml.baselines import BASELINES, baseline_predictions, error_metrics, evaluate_baselines


def test_error_metrics_on_known_values(spark) -> None:
    df = spark.createDataFrame([(1.0, 2.0), (3.0, 1.0), (5.0, None)], ["actual", "pred"])

    row = error_metrics(df, "actual", "pred").first()

    assert row["n"] == 2
    assert row["mae"] == 1.5
    assert round(row["rmse"], 6) == round((2.5) ** 0.5, 6)


def test_baselines_for_pm2_5_one_hour_ahead(feature_table) -> None:
    table = feature_table
    scored = baseline_predictions(table, "pm2_5", 1)
    row = scored.where(
        "location_id = 'ha_noi' and hour(event_ts_utc) = 6 and date(event_ts_utc) = '2026-11-08'"
    ).first()
    yesterday = table.where(
        "location_id = 'ha_noi' and hour(event_ts_utc) = 7 and date(event_ts_utc) = '2026-11-07'"
    ).first()

    assert row["pred_persistence"] == row["pm2_5_ugm3"]
    assert row["pred_moving_average_3h"] == row["pm2_5_avg_3h"]
    assert row["pred_same_hour_yesterday"] == yesterday["pm2_5_ugm3"]
    evaluation = evaluate_baselines(table, "pm2_5", 1).collect()
    assert {r["model"] for r in evaluation} == set(BASELINES)
    assert all(r["n"] > 0 and r["mae"] >= 0 for r in evaluation)
    persistence = [r for r in evaluation if r["model"] == "persistence"][0]
    assert persistence["mae"] < 5
    assert table.where(F.col(target_column("pm2_5", 1)).isNull()).count() == 2
