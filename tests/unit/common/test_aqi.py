import pytest
from pyspark.sql import functions as F

from airpulse.common.aqi import (
    aqi_columns,
    category,
    category_column,
    compute_aqi,
    sub_index,
    sub_index_column,
    to_epa_units,
)


@pytest.mark.parametrize(
    ("pollutant", "ugm3", "expected"),
    [
        ("pm2_5", 0.0, 0),
        ("pm2_5", 9.0, 50),
        ("pm2_5", 35.4, 100),
        ("pm2_5", 55.4, 150),
        ("pm2_5", 12.0, 56),
        ("pm2_5", 36.6, 104),
        ("pm2_5", 400.0, 500),
        ("pm10", 154.0, 100),
        ("pm10", 100.0, 73),
    ],
)
def test_particle_sub_index_matches_epa_tables(pollutant: str, ugm3: float, expected: int) -> None:
    assert sub_index(pollutant, ugm3) == expected


def test_gas_concentrations_are_converted_before_lookup() -> None:
    # 100 ppb of NO2 is 188.2 ug/m3 and sits at the top of the moderate range.
    assert round(to_epa_units("no2", 188.2)) == 100
    assert sub_index("no2", 188.2) == 100
    # 9.4 ppm of CO is 10769 ug/m3.
    assert sub_index("co", 10769.0) == 100


def test_missing_or_negative_values_are_ignored() -> None:
    assert sub_index("pm2_5", None) is None
    assert sub_index("pm2_5", -1.0) is None

    result = compute_aqi({"pm2_5": None, "pm10": 20.0, "dust": 5.0})

    assert result.aqi == 19
    assert result.dominant == "pm10"
    assert compute_aqi({"pm2_5": None}).aqi is None


def test_overall_index_takes_the_largest_pollutant() -> None:
    result = compute_aqi({"pm2_5": 36.6, "pm10": 100.0, "o3": 10.0})

    assert result.aqi == 104
    assert result.dominant == "pm2_5"
    assert category(result.aqi) == "unhealthy_for_sensitive_groups"
    assert category(None) is None
    assert category(501) == "hazardous"


def test_spark_columns_agree_with_python(spark) -> None:
    rows = [
        (36.6, 100.0, 10.0, None),
        (9.0, None, None, 188.2),
        (None, None, None, None),
        (400.0, 20.0, 0.0, 0.0),
        (-5.0, 20.0, None, None),
    ]
    df = spark.createDataFrame(rows, ["pm2_5", "pm10", "o3", "no2"])
    aqi, dominant = aqi_columns({name: F.col(name) for name in df.columns})
    scored = df.select(
        "*",
        sub_index_column("pm2_5", F.col("pm2_5")).alias("sub_pm2_5"),
        aqi.alias("aqi"),
        dominant.alias("dominant"),
        category_column(aqi).alias("category"),
    ).collect()

    for row in scored:
        expected = compute_aqi({name: row[name] for name in ["pm2_5", "pm10", "o3", "no2"]})
        assert row["sub_pm2_5"] == sub_index("pm2_5", row["pm2_5"])
        assert row["aqi"] == expected.aqi
        assert row["dominant"] == expected.dominant
        assert row["category"] == category(expected.aqi)
