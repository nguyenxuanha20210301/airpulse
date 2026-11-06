from pyspark.sql import functions as F

from airpulse.common.config import AlertsConfig
from airpulse.streaming.anomaly_alerts import (
    ALERT_COLUMNS,
    air_threshold_alerts,
    spike_alerts,
    weather_threshold_alerts,
)
from airpulse.streaming.window_metrics import air_windows, latest_per_location, latest_windows

CFG = AlertsConfig(aqi_level=150, temperature_hot_c=39, temperature_cold_c=13)


def test_air_threshold_alerts(spark, clean_air_df) -> None:
    alerts = air_threshold_alerts(clean_air_df, CFG, "r1").collect()

    assert alerts[0].asDict().keys() == set(ALERT_COLUMNS)
    assert len(alerts) == 1
    assert alerts[0]["location_id"] == "ha_noi"
    assert alerts[0]["event_ts_utc"].hour == 4
    assert alerts[0]["severity"] == "unhealthy"
    assert alerts[0]["threshold"] == 150.0


def test_weather_threshold_alerts(clean_weather_df) -> None:
    alerts = {
        row["kind"]: row for row in weather_threshold_alerts(clean_weather_df, CFG, "r1").collect()
    }

    assert set(alerts) == {"temperature_hot", "temperature_cold"}
    assert alerts["temperature_hot"]["value"] == 40.0
    assert alerts["temperature_cold"]["value"] == 5.0


def test_spike_alerts_need_enough_samples_and_a_large_deviation(spark) -> None:
    latest = spark.createDataFrame(
        [("ha_noi", 100.0), ("da_nang", 21.0)], ["location_id", "pm2_5_ugm3"]
    ).withColumn("event_ts_utc", F.to_timestamp(F.lit("2026-11-06 05:00:00")))
    windows = spark.createDataFrame(
        [("ha_noi", 20.0, 10.0, 24), ("da_nang", 20.0, 0.1, 24), ("hai_phong", 1.0, 1.0, 3)],
        ["location_id", "avg_pm2_5", "std_pm2_5", "n_samples"],
    )

    alerts = spike_alerts(latest, windows, "r1", z_threshold=3.0, min_samples=12).collect()

    assert [row["location_id"] for row in alerts] == ["ha_noi"]
    assert alerts[0]["kind"] == "pm2_5_spike"
    assert alerts[0]["threshold"] == 50.0


def test_spike_alerts_compose_with_window_output(clean_air_df) -> None:
    windows = air_windows(clean_air_df, "24 hours", "1 hour")
    latest = latest_per_location(clean_air_df)

    assert spike_alerts(latest, latest_windows(windows), "r1", min_samples=3).count() == 0
