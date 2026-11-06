from datetime import date

from airpulse.batch.common_io import layer_path, read_layer, write_layer


def test_layer_path_override(config) -> None:
    assert layer_path(config, "clean", "air_hourly", None).endswith("/airpulse/clean/air_hourly")
    assert layer_path(config, "clean", "air_hourly", "/data/") == "/data/clean/air_hourly"


def test_write_then_read_a_range_of_days(spark, config, tmp_path, clean_frames) -> None:
    _, air = clean_frames
    root = str(tmp_path)

    write_layer(air, config, "clean", "air_hourly", root)
    first_two = read_layer(
        spark, config, "clean", "air_hourly", root, date(2026, 11, 6), date(2026, 11, 7)
    )

    assert first_two.count() == 2 * 2 * 24
    assert set(first_two.select("dt").distinct().toPandas()["dt"]) == {"2026-11-06", "2026-11-07"}


def test_partition_overwrite_only_touches_written_days(
    spark, config, tmp_path, clean_frames
) -> None:
    _, air = clean_frames
    root = str(tmp_path)
    write_layer(air, config, "clean", "air_hourly", root)

    write_layer(air.where("dt = '2026-11-06'").limit(5), config, "clean", "air_hourly", root)
    back = read_layer(spark, config, "clean", "air_hourly", root)

    assert back.where("dt = '2026-11-06'").count() == 5
    assert back.where("dt = '2026-11-07'").count() == 48
