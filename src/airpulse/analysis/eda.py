"""pandas summaries behind the exploratory notebooks.

every function takes hourly rows as trino returns them, event_ts_utc as a UTC timestamp and one
row per point and hour, and returns a small frame ready to print or plot. nothing here reads or
writes, the notebooks load the frames and draw the charts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd

KEY = "location_id"
TS = "event_ts_utc"
HOUR = pd.Timedelta(hours=1)


def with_local_time(frame: pd.DataFrame, timezone: str, ts: str = TS) -> pd.DataFrame:
    """add ts_local, hour_local and day_local, daily cycles follow the local clock, not UTC."""
    stamps = pd.to_datetime(frame[ts])
    if stamps.dt.tz is None:
        stamps = stamps.dt.tz_localize("UTC")
    local = stamps.dt.tz_convert(timezone).dt.tz_localize(None)
    return frame.assign(ts_local=local, hour_local=local.dt.hour, day_local=local.dt.normalize())


def coverage_by_day(
    frame: pd.DataFrame, n_points: int, ts: str = TS, key: str = KEY
) -> pd.DataFrame:
    """rows, points and hours per UTC day with the share of the expected point hours."""
    grouped = frame.groupby(frame[ts].dt.normalize().rename("day"))
    table = pd.DataFrame(
        {
            "rows": grouped.size(),
            "points": grouped[key].nunique(),
            "hours": grouped[ts].nunique(),
        }
    )
    table["share"] = table["rows"] / (n_points * 24)
    return table


def shared_series(
    frame: pd.DataFrame, value: str, key: str = KEY, ts: str = TS, decimals: int = 3
) -> pd.DataFrame:
    """find the points whose whole series are equal, they fall in one cell of the model grid.

    one row per point with cell, the first point of its group by name, and cell_size.
    """
    wide = frame.pivot(index=ts, columns=key, values=value).round(decimals)
    signature = wide.apply(
        lambda column: int(pd.util.hash_pandas_object(column, index=False).sum())
    )
    table = signature.rename("signature").reset_index()
    groups = table.groupby("signature")[key]
    table["cell"] = groups.transform("min")
    table["cell_size"] = groups.transform("size")
    return table.drop(columns="signature").sort_values(["cell", key]).reset_index(drop=True)


def diurnal_profile(
    frame: pd.DataFrame, value: str, by: str, hour: str = "hour_local"
) -> pd.DataFrame:
    """mean of a value per hour of the day, one column per group."""
    return frame.groupby([hour, by])[value].mean().unstack(by)


def exceedance_share(
    daily: pd.DataFrame, value: str, limits: Mapping[str, float], by: str
) -> pd.DataFrame:
    """share of the rows of each group above every limit, for daily means against daily limits."""
    grouped = daily.groupby(by)[value]
    table = pd.DataFrame(
        {
            name: grouped.apply(lambda values, limit=limit: (values > limit).mean())
            for name, limit in limits.items()
        }
    )
    table["n"] = grouped.size()
    return table


def episodes(
    frame: pd.DataFrame,
    value: str,
    threshold: float,
    min_hours: int = 6,
    key: str = KEY,
    ts: str = TS,
) -> pd.DataFrame:
    """runs of consecutive hours above a threshold per point, longest peaks first."""
    ordered = frame.sort_values([key, ts])
    above = ordered[value] > threshold
    # a run ends when the point changes, an hour is missing or the value drops back.
    breaks = (ordered.groupby(key)[ts].diff() != HOUR) | (above != above.shift())
    run = breaks.cumsum()
    runs = (
        ordered[above]
        .groupby([key, run[above]])
        .agg(start=(ts, "min"), end=(ts, "max"), hours=(ts, "size"), peak=(value, "max"))
        .reset_index(level=1, drop=True)
        .reset_index()
    )
    kept = runs[runs["hours"] >= min_hours]
    return kept.sort_values(["peak", "hours"], ascending=False).reset_index(drop=True)


def autocorrelation(
    frame: pd.DataFrame, value: str, lags: Sequence[int], key: str = KEY, ts: str = TS
) -> pd.DataFrame:
    """correlation of each hourly series with itself some hours earlier, summarized over points."""
    # a regular hourly index keeps a lag of n rows equal to n hours when hours are missing.
    wide = frame.pivot(index=ts, columns=key, values=value).asfreq("h")
    rows = []
    for lag in lags:
        values = wide.apply(lambda column, lag=lag: column.autocorr(lag))
        rows.append(
            {
                "lag_h": lag,
                "median": values.median(),
                "q25": values.quantile(0.25),
                "q75": values.quantile(0.75),
            }
        )
    return pd.DataFrame(rows).set_index("lag_h")


def binned_mean(
    frame: pd.DataFrame, driver: str, value: str, edges: Sequence[float], by: str
) -> pd.DataFrame:
    """mean and count of a value per bin of a driver, bins are closed on the left."""
    bins = pd.cut(frame[driver], list(edges), right=False).rename("bin")
    table = frame.groupby([bins, frame[by]], observed=True)[value].agg(["mean", "count"])
    return table.reset_index()


def correlations(
    frame: pd.DataFrame, target: str, variables: Sequence[str], by: str, method: str = "pearson"
) -> pd.DataFrame:
    """correlation of the target with every variable inside each group, one row per group."""
    if method not in ("pearson", "spearman"):
        raise ValueError(f"unknown correlation method {method}")
    rows = {}
    for name, group in frame.groupby(by):
        values = group[[target, *variables]]
        if method == "spearman":
            # the rank correlation is the pearson correlation of the ranks, no extra library.
            values = values.rank()
        rows[name] = values[list(variables)].corrwith(values[target])
    return pd.DataFrame(rows).T.rename_axis(by)


def skill_against(
    evaluation: pd.DataFrame,
    model: str,
    baseline: str,
    metric: str = "mae",
    keys: Sequence[str] = ("target", "horizon_h"),
) -> pd.DataFrame:
    """relative gain of a model over a baseline per target and horizon, positive is better."""
    wide = evaluation.pivot_table(index=list(keys), columns="model", values=metric)
    skill = (wide[baseline] - wide[model]) / wide[baseline]
    return skill.rename("skill").reset_index()
