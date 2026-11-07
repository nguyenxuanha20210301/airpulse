from airpulse.serving.register_tables import (
    ddl_path,
    statements,
    table_names,
    with_drops,
    write_reference,
)


def test_ddl_statements_fill_the_root_and_name_every_dataset(config) -> None:
    ddl = statements(ddl_path(config).read_text(encoding="utf-8"), "hdfs://nn:8020/airpulse")
    names = table_names(ddl)

    assert ddl[0].startswith("CREATE DATABASE")
    assert all("${root}" not in statement for statement in ddl)
    assert "airpulse.clean_air_hourly" in names
    assert "airpulse.curated_features" in names
    assert "airpulse.ref_locations" in names
    assert any("hdfs://nn:8020/airpulse/raw/weather_hourly" in statement for statement in ddl)
    assert len(names) == 16


def test_every_create_table_is_preceded_by_its_drop(config) -> None:
    ddl = statements(ddl_path(config).read_text(encoding="utf-8"), "hdfs://nn:8020/airpulse")
    planned = with_drops(ddl)

    assert planned[0].startswith("CREATE DATABASE")
    assert len(planned) == len(ddl) + len(table_names(ddl))
    for index, statement in enumerate(planned):
        if statement.startswith("CREATE TABLE"):
            assert planned[index - 1] == f"DROP TABLE IF EXISTS {table_names([statement])[0]}"


def test_reference_locations_are_written_from_the_config(spark, config, tmp_path) -> None:
    path = write_reference(spark, config, str(tmp_path))
    frame = spark.read.parquet(path)
    hanoi = frame.where("location_id = 'ha_noi'").first()

    assert path.endswith("/reference/locations")
    assert frame.count() == 63
    assert set(frame.columns) == {"location_id", "name", "lat", "lon", "region", "province"}
    assert hanoi["region"] == "north"
    assert frame.select("region").distinct().count() == 3
