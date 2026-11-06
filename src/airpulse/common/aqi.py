"""US EPA air quality index with the 2024 PM2.5 breakpoints.

the index is computed from hourly concentrations, so it is an instant estimate rather than the
official 24 hour or 8 hour average. that is enough for alerts and dashboards, and the us_aqi
value published by open-meteo is kept alongside it for comparison.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column


@dataclass(frozen=True)
class Breakpoint:
    c_low: float
    c_high: float
    i_low: int
    i_high: int


# concentrations are in the units of the epa tables, micrograms per cubic meter for particles,
# ppb for NO2 and SO2, ppm for O3 and CO.
BREAKPOINTS: dict[str, tuple[Breakpoint, ...]] = {
    "pm2_5": (
        Breakpoint(0.0, 9.0, 0, 50),
        Breakpoint(9.1, 35.4, 51, 100),
        Breakpoint(35.5, 55.4, 101, 150),
        Breakpoint(55.5, 125.4, 151, 200),
        Breakpoint(125.5, 225.4, 201, 300),
        Breakpoint(225.5, 325.4, 301, 500),
    ),
    "pm10": (
        Breakpoint(0, 54, 0, 50),
        Breakpoint(55, 154, 51, 100),
        Breakpoint(155, 254, 101, 150),
        Breakpoint(255, 354, 151, 200),
        Breakpoint(355, 424, 201, 300),
        Breakpoint(425, 604, 301, 500),
    ),
    "o3": (
        Breakpoint(0.000, 0.054, 0, 50),
        Breakpoint(0.055, 0.070, 51, 100),
        Breakpoint(0.071, 0.085, 101, 150),
        Breakpoint(0.086, 0.105, 151, 200),
        Breakpoint(0.106, 0.200, 201, 300),
        Breakpoint(0.201, 0.604, 301, 500),
    ),
    "no2": (
        Breakpoint(0, 53, 0, 50),
        Breakpoint(54, 100, 51, 100),
        Breakpoint(101, 360, 101, 150),
        Breakpoint(361, 649, 151, 200),
        Breakpoint(650, 1249, 201, 300),
        Breakpoint(1250, 2049, 301, 500),
    ),
    "so2": (
        Breakpoint(0, 35, 0, 50),
        Breakpoint(36, 75, 51, 100),
        Breakpoint(76, 185, 101, 150),
        Breakpoint(186, 304, 151, 200),
        Breakpoint(305, 604, 201, 300),
        Breakpoint(605, 1004, 301, 500),
    ),
    "co": (
        Breakpoint(0.0, 4.4, 0, 50),
        Breakpoint(4.5, 9.4, 51, 100),
        Breakpoint(9.5, 12.4, 101, 150),
        Breakpoint(12.5, 15.4, 151, 200),
        Breakpoint(15.5, 30.4, 201, 300),
        Breakpoint(30.5, 50.4, 301, 500),
    ),
}

# molar masses in g/mol, used to turn micrograms per cubic meter into ppb at 25 degrees and 1 atm.
MOLAR_MASS = {"no2": 46.01, "so2": 64.07, "o3": 48.00, "co": 28.01}
MOLAR_VOLUME_L = 24.45

# the epa tables truncate concentrations to a fixed number of decimals before lookup.
DECIMALS = {"pm2_5": 1, "pm10": 0, "o3": 3, "no2": 0, "so2": 0, "co": 1}

CATEGORIES = (
    (50, "good"),
    (100, "moderate"),
    (150, "unhealthy_for_sensitive_groups"),
    (200, "unhealthy"),
    (300, "very_unhealthy"),
    (500, "hazardous"),
)

MAX_AQI = 500


def ugm3_to_ppb(value: float, pollutant: str) -> float:
    return value * MOLAR_VOLUME_L / MOLAR_MASS[pollutant]


def to_epa_units(pollutant: str, ugm3: float) -> float:
    """convert an open-meteo concentration to the unit of the epa breakpoint table."""
    if pollutant in ("pm2_5", "pm10"):
        return ugm3
    ppb = ugm3_to_ppb(ugm3, pollutant)
    if pollutant in ("o3", "co"):
        return ppb / 1000.0
    return ppb


def _truncate(value: float, decimals: int) -> float:
    factor = 10**decimals
    return int(value * factor) / factor


def sub_index(pollutant: str, ugm3: float | None) -> int | None:
    """the index contribution of one pollutant, or None when the value is missing or negative."""
    if ugm3 is None or ugm3 < 0:
        return None
    value = _truncate(to_epa_units(pollutant, ugm3), DECIMALS[pollutant])
    table = BREAKPOINTS[pollutant]
    if value > table[-1].c_high:
        return MAX_AQI
    for bp in table:
        if value <= bp.c_high:
            # concentrations that fall in the gap between two rows round to the lower row.
            low = max(bp.c_low, min(value, bp.c_high))
            span = bp.c_high - bp.c_low
            return round((bp.i_high - bp.i_low) / span * (low - bp.c_low) + bp.i_low)
    return MAX_AQI


@dataclass(frozen=True)
class AqiResult:
    aqi: int | None
    dominant: str | None
    sub_indices: dict[str, int]


def compute_aqi(concentrations: dict[str, float | None]) -> AqiResult:
    """overall index is the largest sub index, the pollutant that sets it is the dominant one."""
    subs = {
        pollutant: index
        for pollutant, value in concentrations.items()
        if pollutant in BREAKPOINTS and (index := sub_index(pollutant, value)) is not None
    }
    if not subs:
        return AqiResult(None, None, {})
    dominant = max(subs, key=lambda name: subs[name])
    return AqiResult(subs[dominant], dominant, subs)


def category(aqi: int | None) -> str | None:
    if aqi is None:
        return None
    for upper, name in CATEGORIES:
        if aqi <= upper:
            return name
    return CATEGORIES[-1][1]


def sub_index_column(pollutant: str, ugm3: Column) -> Column:
    """the same piecewise linear lookup as sub_index, built from spark expressions.

    keeping the lookup in native expressions avoids python workers when the batch layer scores
    tens of millions of rows.
    """
    from pyspark.sql import functions as F

    if pollutant in ("pm2_5", "pm10"):
        value = ugm3
    else:
        value = ugm3 * F.lit(MOLAR_VOLUME_L / MOLAR_MASS[pollutant])
        if pollutant in ("o3", "co"):
            value = value / F.lit(1000.0)
    factor = 10 ** DECIMALS[pollutant]
    value = F.floor(value * F.lit(factor)) / F.lit(factor)
    table = BREAKPOINTS[pollutant]
    expr = F.when(ugm3.isNull() | (ugm3 < 0), F.lit(None).cast("int"))
    for bp in table:
        slope = (bp.i_high - bp.i_low) / (bp.c_high - bp.c_low)
        clamped = F.greatest(F.lit(float(bp.c_low)), F.least(value, F.lit(float(bp.c_high))))
        expr = expr.when(
            value <= bp.c_high,
            F.round(F.lit(slope) * (clamped - F.lit(float(bp.c_low))) + bp.i_low),
        )
    return expr.otherwise(F.lit(MAX_AQI)).cast("int")


def aqi_columns(columns: dict[str, Column]) -> tuple[Column, Column]:
    """return the overall aqi column and the dominant pollutant column for the given pollutants."""
    from pyspark.sql import functions as F

    subs = {
        name: sub_index_column(name, col) for name, col in columns.items() if name in BREAKPOINTS
    }
    names = list(subs)
    aqi = F.greatest(*subs.values()) if len(subs) > 1 else subs[names[0]]
    dominant = F.lit(None).cast("string")
    for name in reversed(names):
        dominant = F.when(subs[name] == aqi, F.lit(name)).otherwise(dominant)
    return aqi, F.when(aqi.isNull(), F.lit(None).cast("string")).otherwise(dominant)


def category_column(aqi: Column) -> Column:
    from pyspark.sql import functions as F

    expr = F.when(aqi.isNull(), F.lit(None).cast("string"))
    for upper, name in CATEGORIES:
        expr = expr.when(aqi <= upper, F.lit(name))
    return expr.otherwise(F.lit(CATEGORIES[-1][1]))
