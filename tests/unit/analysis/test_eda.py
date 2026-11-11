import numpy as np
import pandas as pd
import pytest

from airpulse.analysis import eda


def hourly(location, values, start="2026-11-06 00:00") -> pd.DataFrame:
    stamps = pd.date_range(start, periods=len(values), freq="h")
    return pd.DataFrame({"location_id": location, "event_ts_utc": stamps, "pm2_5_ugm3": values})


def test_local_time_moves_the_evening_to_the_next_day() -> None:
    frame = hourly("ha_noi", [10.0, 11.0], start="2026-11-06 16:00")

    local = eda.with_local_time(frame, "Asia/Ho_Chi_Minh")

    assert local["hour_local"].tolist() == [23, 0]
    assert local["day_local"].tolist() == [pd.Timestamp("2026-11-06"), pd.Timestamp("2026-11-07")]
    assert local["ts_local"].iloc[1] == pd.Timestamp("2026-11-07 00:00")


def test_coverage_counts_the_share_of_expected_point_hours() -> None:
    frame = pd.concat([hourly("ha_noi", [1.0] * 30), hourly("da_nang", [2.0] * 24)])

    table = eda.coverage_by_day(frame, n_points=2)

    first, second = table.loc[pd.Timestamp("2026-11-06")], table.loc[pd.Timestamp("2026-11-07")]
    assert (first["rows"], first["points"], first["hours"], first["share"]) == (48, 2, 24, 1.0)
    assert (second["rows"], second["points"], second["share"]) == (6, 1, 0.125)


def test_shared_series_groups_points_with_equal_values() -> None:
    values = [5.0, 7.5, 9.0]
    frame = pd.concat(
        [hourly("ha_noi", values), hourly("bac_ninh", values), hourly("hue", [5.0, 7.5, 9.1])]
    )

    table = eda.shared_series(frame, "pm2_5_ugm3").set_index("location_id")

    assert table.loc["ha_noi", "cell"] == "bac_ninh"
    assert table.loc["bac_ninh", "cell_size"] == 2
    assert table.loc["hue", "cell"] == "hue"
    assert table.loc["hue", "cell_size"] == 1


def test_diurnal_profile_has_one_column_per_group() -> None:
    north = hourly("ha_noi", list(range(48))).assign(region="north")
    south = hourly("can_tho", [1.0] * 48).assign(region="south")
    frame = eda.with_local_time(pd.concat([north, south]), "UTC")

    profile = eda.diurnal_profile(frame, "pm2_5_ugm3", "region")

    assert profile.shape == (24, 2)
    assert profile.loc[0, "north"] == 12.0
    assert profile.loc[23, "north"] == 35.0
    assert (profile["south"] == 1.0).all()


def test_exceedance_share_per_limit_and_group() -> None:
    daily = pd.DataFrame(
        {
            "region": ["north"] * 4 + ["south"] * 2,
            "avg_pm2_5": [10.0, 20.0, 60.0, 70.0, 5.0, 16.0],
        }
    )

    table = eda.exceedance_share(daily, "avg_pm2_5", {"who": 15.0, "qcvn": 50.0}, "region")

    assert table.loc["north", "who"] == 0.75
    assert table.loc["north", "qcvn"] == 0.5
    assert table.loc["south", "who"] == 0.5
    assert table.loc["south", "n"] == 2


def test_episodes_split_on_gaps_and_keep_long_runs_only() -> None:
    first = hourly("ha_noi", [10.0, 60.0, 80.0, 70.0, 10.0, 90.0])
    # the same point continues after a missing hour, which must start a new run.
    later = hourly("ha_noi", [95.0, 99.0], start="2026-11-06 07:00")
    other = hourly("hue", [55.0, 56.0, 57.0, 58.0])
    frame = pd.concat([first, later, other])

    runs = eda.episodes(frame, "pm2_5_ugm3", threshold=50.0, min_hours=2)

    assert runs[["location_id", "hours", "peak"]].values.tolist() == [
        ["ha_noi", 2, 99.0],
        ["ha_noi", 3, 80.0],
        ["hue", 4, 58.0],
    ]
    assert runs.loc[1, "start"] == pd.Timestamp("2026-11-06 01:00")
    assert runs.loc[1, "end"] == pd.Timestamp("2026-11-06 03:00")


def test_autocorrelation_follows_a_daily_cycle() -> None:
    hours = np.arange(24 * 6)
    cycle = 20.0 + 5.0 * np.sin(2 * np.pi * hours / 24)
    frame = pd.concat([hourly("ha_noi", cycle), hourly("hue", cycle * 2)])

    table = eda.autocorrelation(frame, "pm2_5_ugm3", [12, 24])

    assert table.loc[24, "median"] == pytest.approx(1.0)
    assert table.loc[12, "median"] == pytest.approx(-1.0)
    assert table.loc[24, "q25"] == pytest.approx(table.loc[24, "q75"])


def test_binned_mean_uses_bins_closed_on_the_left() -> None:
    frame = pd.DataFrame(
        {
            "region": ["north"] * 4,
            "wind_speed_kmh": [0.0, 2.9, 3.0, 8.0],
            "pm2_5_ugm3": [40.0, 60.0, 20.0, 10.0],
        }
    )

    table = eda.binned_mean(frame, "wind_speed_kmh", "pm2_5_ugm3", [0, 3, 10], "region")

    assert table["mean"].tolist() == [50.0, 15.0]
    assert table["count"].tolist() == [2, 2]
    assert table["bin"].astype(str).tolist() == ["[0, 3)", "[3, 10)"]


def test_correlations_are_computed_inside_each_group() -> None:
    wind = [1.0, 2.0, 3.0, 4.0]
    frame = pd.DataFrame(
        {
            "region": ["north"] * 4 + ["south"] * 4,
            "wind_speed_kmh": wind * 2,
            "humidity_pct": [50.0, 60.0, 70.0, 80.0] * 2,
            "pm2_5_ugm3": [40.0, 30.0, 20.0, 10.0, 10.0, 20.0, 30.0, 40.0],
        }
    )

    table = eda.correlations(frame, "pm2_5_ugm3", ["wind_speed_kmh", "humidity_pct"], "region")

    assert table.loc["north", "wind_speed_kmh"] == pytest.approx(-1.0)
    assert table.loc["south", "humidity_pct"] == pytest.approx(1.0)


def test_rank_correlation_sees_a_monotone_curve_as_perfect() -> None:
    frame = pd.DataFrame(
        {
            "region": ["north"] * 5,
            "wind_speed_kmh": [1.0, 2.0, 3.0, 4.0, 5.0],
            "pm2_5_ugm3": [100.0, 30.0, 12.0, 11.0, 10.5],
        }
    )

    pearson = eda.correlations(frame, "pm2_5_ugm3", ["wind_speed_kmh"], "region")
    spearman = eda.correlations(
        frame, "pm2_5_ugm3", ["wind_speed_kmh"], "region", method="spearman"
    )

    assert spearman.loc["north", "wind_speed_kmh"] == pytest.approx(-1.0)
    assert pearson.loc["north", "wind_speed_kmh"] > -0.9
    with pytest.raises(ValueError, match="unknown correlation method"):
        eda.correlations(frame, "pm2_5_ugm3", ["wind_speed_kmh"], "region", method="kendall")


def test_skill_is_the_relative_gain_over_the_baseline() -> None:
    evaluation = pd.DataFrame(
        {
            "target": ["pm2_5"] * 4,
            "horizon_h": [1, 1, 24, 24],
            "model": ["gbt", "persistence", "gbt", "persistence"],
            "mae": [1.0, 2.0, 9.0, 6.0],
        }
    )

    table = eda.skill_against(evaluation, "gbt", "persistence").set_index("horizon_h")

    assert table.loc[1, "skill"] == pytest.approx(0.5)
    assert table.loc[24, "skill"] == pytest.approx(-0.5)
