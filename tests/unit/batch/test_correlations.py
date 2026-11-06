from airpulse.batch.correlations import WEATHER_VARIABLES, correlation_table, locations_frame
from airpulse.common.features import join_weather_air


def test_correlation_scopes_and_values(spark, config, clean_frames) -> None:
    weather, air = clean_frames
    table = correlation_table(join_weather_air(weather, air), locations_frame(spark, config))
    rows = table.collect()
    scopes = {row["scope"] for row in rows}
    humidity = {
        row["key"]: row["correlation"]
        for row in rows
        if row["scope"] == "location" and row["variable"] == "humidity_pct"
    }

    assert scopes == {"location", "region", "location_month", "all"}
    assert len([r for r in rows if r["scope"] == "all"]) == len(WEATHER_VARIABLES)
    # humidity moves against the daily cycle that drives pm2_5, so the correlation is negative.
    assert humidity["ha_noi"] < -0.9
    assert all(row["n"] == 120 for row in rows if row["scope"] == "location")
