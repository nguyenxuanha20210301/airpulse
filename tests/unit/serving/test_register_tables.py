from airpulse.serving.register_tables import DDL_FILE, statements, table_names


def test_ddl_statements_fill_the_root_and_name_every_dataset() -> None:
    ddl = statements(DDL_FILE.read_text(encoding="utf-8"), "hdfs://nn:8020/airpulse")
    names = table_names(ddl)

    assert ddl[0].startswith("CREATE DATABASE")
    assert all("${root}" not in statement for statement in ddl)
    assert "airpulse.clean_air_hourly" in names
    assert "airpulse.curated_features" in names
    assert any("hdfs://nn:8020/airpulse/raw/weather_hourly" in statement for statement in ddl)
    assert len(names) == 15
