from datetime import timedelta

from tests.unit.streaming.conftest import air_df, air_event

from airpulse.batch.raw_to_clean import raw_to_clean_frame


def test_latest_copy_of_an_event_wins(spark) -> None:
    first = air_event("ha_noi", 0, pm2_5_ugm3=10.0)
    corrected = first.model_copy(
        update={
            "pm2_5_ugm3": 12.0,
            "ingested_ts_utc": first.ingested_ts_utc + timedelta(hours=1),
            "run_id": "r2",
        }
    )
    other = air_event("ha_noi", 1, pm2_5_ugm3=-4.0)

    cleaned = raw_to_clean_frame(
        air_df(spark, [first, corrected, other]), "air", "clean-run"
    ).collect()

    assert len(cleaned) == 2
    by_hour = {row["event_ts_utc"].hour: row for row in cleaned}
    assert by_hour[0]["pm2_5_ugm3"] == 12.0
    assert by_hour[0]["run_id"] == "r2"
    assert by_hour[0]["clean_run_id"] == "clean-run"
    assert by_hour[1]["pm2_5_ugm3"] is None
    assert "aqi_computed" in cleaned[0].asDict()
