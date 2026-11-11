import pandas as pd
import pytest

from airpulse.analysis.trino_io import FrameStore, connection_args, query_frame


class FakeCursor:
    def __init__(self, connection) -> None:
        self.connection = connection
        self.description = [(name, "varchar") for name in connection.columns]

    def execute(self, sql) -> None:
        self.connection.statements.append(sql)

    def fetchall(self):
        return self.connection.rows


class FakeConnection:
    def __init__(self, columns, rows) -> None:
        self.columns = columns
        self.rows = rows
        self.statements = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


def test_connection_args_come_from_the_config(config) -> None:
    args = connection_args(config)

    assert args["host"] == "trino.airpulse.svc"
    assert args["port"] == 8080
    assert args["http_scheme"] == "http"
    assert args["user"] == "airpulse"
    assert args["catalog"] == "hive"
    assert args["schema"] == "airpulse"


def test_connection_args_fall_back_to_the_scheme_port_and_reject_other_urls(config) -> None:
    config = config.model_copy(deep=True)
    config.trino.url = "https://trino.example.org"
    assert connection_args(config)["port"] == 443

    config.trino.url = ""
    with pytest.raises(ValueError, match="trino.url"):
        connection_args(config)


def test_query_frame_keeps_the_column_names() -> None:
    connection = FakeConnection(["location_id", "avg_pm2_5"], [("ha_noi", 41.5), ("hue", 12.0)])

    frame = query_frame(connection, "select 1")

    assert list(frame.columns) == ["location_id", "avg_pm2_5"]
    assert frame["avg_pm2_5"].tolist() == [41.5, 12.0]
    assert connection.statements == ["select 1"]


def test_frame_store_asks_trino_once_then_reads_the_parquet_copy(config, tmp_path) -> None:
    connection = FakeConnection(["n"], [(1,), (2,)])
    store = FrameStore(config, tmp_path / "eda", connection=connection)

    first = store.load("counts", "select n from t")
    second = store.load("counts", "select n from t")

    assert store.path("counts").exists()
    assert len(connection.statements) == 1
    pd.testing.assert_frame_equal(first, second)


def test_frame_store_refresh_goes_back_to_trino(config, tmp_path) -> None:
    connection = FakeConnection(["n"], [(1,)])
    FrameStore(config, tmp_path, connection=connection).load("counts", "select n from t")
    connection.rows = [(5,)]

    refreshed = FrameStore(config, tmp_path, refresh=True, connection=connection).load(
        "counts", "select n from t"
    )

    assert refreshed["n"].tolist() == [5]
    assert len(connection.statements) == 2
