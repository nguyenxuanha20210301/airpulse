from pyspark.sql import functions as F

from airpulse.mapreduce.monthly_average import finalize, mapper, monthly_average, reducer


def test_mapper_reducer_finalize_in_pure_python() -> None:
    emitted = list(mapper("ha_noi", "2026-11-06", 10.0)) + list(
        mapper("ha_noi", "2026-11-07", 30.0)
    )
    skipped = list(mapper("ha_noi", "2026-11-08", None))

    assert skipped == []
    assert emitted[0][0] == ("ha_noi", "2026-11")
    total = reducer(emitted[0][1], emitted[1][1])
    assert total == (40.0, 2)
    assert finalize(("ha_noi", "2026-11"), total)["avg_pm2_5"] == 20.0


def test_rdd_result_matches_a_dataframe_group_by(clean_frames) -> None:
    _, air = clean_frames
    expected = {
        (r["location_id"], r["ym"]): r["avg"]
        for r in air.groupBy("location_id", F.substring("dt", 1, 7).alias("ym"))
        .agg(F.avg("pm2_5_ugm3").alias("avg"))
        .collect()
    }

    got = {
        (r["location_id"], r["year_month"]): r["avg_pm2_5"] for r in monthly_average(air).collect()
    }

    assert got.keys() == expected.keys()
    for key in expected:
        assert abs(got[key] - expected[key]) < 1e-9
