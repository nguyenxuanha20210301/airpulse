from datetime import timedelta

from pyspark.sql import functions as F

from airpulse.common.features import FEATURE_COLUMNS, target_column


def test_feature_table_columns_rolling_values_and_targets(clean_frames, feature_table, t0) -> None:
    weather, air = clean_frames
    table = feature_table
    row = table.where(
        (F.col("location_id") == "ha_noi") & (F.col("event_ts_utc") == t0 + timedelta(hours=30))
    ).first()
    source = air.where("location_id = 'ha_noi'").orderBy("event_ts_utc").collect()
    pm = [r["pm2_5_ugm3"] for r in source]

    assert set(FEATURE_COLUMNS) <= set(table.columns)
    assert table.count() == 2 * 5 * 24
    assert row["hour_of_day"] == 6 and row["month"] == 11
    assert round(row["pm2_5_avg_3h"], 6) == round(sum(pm[27:31]) / 4, 6)
    assert round(row["pm2_5_max_24h"], 6) == round(max(pm[6:31]), 6)
    assert row[target_column("pm2_5", 1)] == pm[31]
    assert row[target_column("pm2_5", 24)] == pm[54]
    assert row[target_column("temperature", 6)] is not None


def test_last_rows_have_no_future_targets(feature_table, last_day) -> None:
    table = feature_table
    tail = table.where(F.col("event_ts_utc") >= last_day + timedelta(hours=23)).collect()

    assert all(row[target_column("pm2_5", 1)] is None for row in tail)
