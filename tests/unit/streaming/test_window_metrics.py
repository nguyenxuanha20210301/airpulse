from airpulse.streaming.window_metrics import (
    air_windows,
    latest_per_location,
    latest_windows,
    weather_windows,
    window_label,
)


def test_window_labels() -> None:
    assert window_label("24 hours") == "24h"
    assert window_label("1 hour") == "1h"


def test_air_windows_average_the_last_hours(clean_air_df) -> None:
    windows = air_windows(clean_air_df, "3 hours", "1 hour")
    row = windows.where("location_id = 'ha_noi' and hour(window_end) = 6").first()

    assert row["window_size"] == "3h"
    # the window from 03h to 06h holds the values 40, 80 and 20.
    assert round(row["avg_pm2_5"], 2) == round((40 + 80 + 20) / 3, 2)
    assert row["max_pm2_5"] == 80
    assert row["n_samples"] == 3
    assert row["max_aqi"] is not None


def test_weather_windows_and_latest_rows(clean_weather_df, clean_air_df) -> None:
    windows = weather_windows(clean_weather_df, "24 hours", "1 hour")
    full_day = windows.where("hour(window_start) = 0 and hour(window_end) = 0").first()
    latest = latest_per_location(clean_air_df).collect()
    latest_w = latest_windows(windows).collect()

    assert full_day["n_samples"] == 6
    assert full_day["max_temperature"] == 40.0
    assert {row["location_id"]: row["event_ts_utc"].hour for row in latest} == {
        "ha_noi": 5,
        "da_nang": 5,
    }
    assert len(latest_w) == 1
    assert latest_w[0]["window_end"] > full_day["window_end"]
