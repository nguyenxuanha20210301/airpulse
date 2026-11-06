from pyspark.sql import functions as F

from airpulse.batch.trends import (
    air_daily,
    air_monthly,
    heatmap_month_hour,
    season_column,
    seasonal,
    weather_daily,
    weather_monthly,
    yearly_trend,
)


def test_daily_aggregates(config, clean_frames) -> None:
    weather, air = clean_frames
    a = air_daily(air, config.aqi)
    w = weather_daily(weather)
    row = a.where("location_id = 'da_nang' and dt = '2026-11-06'").first()

    assert a.count() == 10 and w.count() == 10
    assert row["n_hours"] == 24
    assert round(row["avg_pm2_5"], 1) == 30.0
    assert row["above_who"] is True
    assert row["above_qcvn"] is False
    assert w.where("location_id = 'ha_noi'").agg(F.max("max_temperature")).first()[0] == 29.0


def test_monthly_counts_exceedance_days(config, clean_frames) -> None:
    weather, air = clean_frames
    monthly = air_monthly(air_daily(air, config.aqi)).where("location_id = 'ha_noi'").first()
    w_monthly = weather_monthly(weather_daily(weather)).first()

    assert monthly["year"] == 2026 and monthly["month"] == 11
    assert monthly["n_days"] == 5
    assert monthly["days_above_who"] == 5
    assert monthly["days_above_qcvn"] == 0
    assert w_monthly["n_days"] == 5


def test_heatmap_and_seasons(config, clean_frames) -> None:
    weather, air = clean_frames
    heat = heatmap_month_hour(air)
    seasons = seasonal(air_daily(air, config.aqi), weather_daily(weather)).collect()

    assert heat.count() == 2 * 24
    assert heat.where("location_id = 'ha_noi' and hour_utc = 6").first()["n_hours"] == 5
    assert {row["season"] for row in seasons} == {"autumn"}
    mapping = {m: s for m, s in [(1, "winter"), (4, "spring"), (7, "summer"), (10, "autumn")]}
    frame = air.sparkSession.createDataFrame([(m,) for m in mapping], ["month"])
    got = {
        r["month"]: r["season"]
        for r in frame.select("month", season_column(F.col("month")).alias("season")).collect()
    }
    assert got == mapping


def test_yearly_trend_has_slopes(config, clean_frames) -> None:
    weather, air = clean_frames
    trend = yearly_trend(air_daily(air, config.aqi), weather_daily(weather)).collect()

    assert len(trend) == 2
    assert trend[0]["n_years"] == 1
    assert trend[0]["pm2_5_slope_per_year"] is None
