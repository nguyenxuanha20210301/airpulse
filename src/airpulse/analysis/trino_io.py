"""run sql on trino and get pandas frames back, with a parquet copy of every result.

the notebooks read the serving layer the way the dashboards do, through trino. each named
result is also kept as a parquet file, so an analysis runs again while the cluster is stopped
and only goes back to trino when a refresh is asked for.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pandas as pd

from airpulse.common.config import Config

DEFAULT_PORTS = {"http": 80, "https": 443}


def connection_args(cfg: Config) -> dict[str, Any]:
    """the keyword arguments of the trino client for the url of the config."""
    parts = urlsplit(cfg.trino.url)
    if not parts.hostname or parts.scheme not in DEFAULT_PORTS:
        raise ValueError("trino.url must be an http or https url, see AIRPULSE_TRINO__URL")
    return {
        "host": parts.hostname,
        "port": parts.port or DEFAULT_PORTS[parts.scheme],
        "http_scheme": parts.scheme,
        "user": cfg.project,
        "catalog": cfg.trino.hive_catalog,
        "schema": cfg.trino.schema_name,
    }


def connect(cfg: Config):
    # imported here so the jobs of the package run without the client, only notebooks need it.
    from trino.dbapi import connect as trino_connect

    return trino_connect(**connection_args(cfg))


def query_frame(connection, sql: str) -> pd.DataFrame:
    """one statement to one frame, column names as trino returns them."""
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=[column[0] for column in cursor.description])


class FrameStore:
    """named query results, read from parquet when a copy exists and from trino otherwise.

    the connection is opened on the first query that needs it, so a notebook whose results are
    all on disk never touches the cluster.
    """

    def __init__(self, cfg: Config, cache_dir: Path, refresh: bool = False, connection=None):
        self.cfg = cfg
        self.cache_dir = Path(cache_dir)
        self.refresh = refresh
        self._connection = connection

    def path(self, name: str) -> Path:
        return self.cache_dir / f"{name}.parquet"

    def load(self, name: str, sql: str) -> pd.DataFrame:
        path = self.path(name)
        if path.exists() and not self.refresh:
            return pd.read_parquet(path)
        if self._connection is None:
            self._connection = connect(self.cfg)
        frame = query_frame(self._connection, sql)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        return frame
