"""register the hdfs datasets as external tables in the hive metastore.

the ddl lives in schemas/hive/tables.sql with a root placeholder, this job fills it from the
profile, runs each statement and recovers the day partitions so trino sees new days.
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import SparkSession

from airpulse.common.config import REPO_ROOT, Config, load_config
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.spark import build_spark_session


def ddl_path(cfg: Config) -> Path:
    """the ddl file from the config, absolute as is, otherwise under the repo root or the cwd."""
    path = Path(cfg.hive.ddl_file)
    if path.is_absolute():
        return path
    for base in (REPO_ROOT, Path.cwd()):
        if (base / path).exists():
            return base / path
    raise FileNotFoundError(f"ddl file {path} not found, set AIRPULSE_HIVE__DDL_FILE")


def statements(text: str, root: str) -> list[str]:
    """split the file into statements, dropping comments and filling the root placeholder."""
    body = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    body = body.replace("${root}", root)
    return [part.strip() for part in body.split(";") if part.strip()]


def table_names(ddl: list[str]) -> list[str]:
    found = []
    for statement in ddl:
        match = re.search(r"CREATE TABLE IF NOT EXISTS\s+([\w.]+)", statement, re.IGNORECASE)
        if match:
            found.append(match.group(1))
    return found


def register(spark: SparkSession, cfg: Config, ddl_text: str, log) -> list[str]:
    root = f"{cfg.hdfs.url}{cfg.hdfs.root}"
    ddl = statements(ddl_text, root)
    registered = []
    for statement in ddl:
        try:
            spark.sql(statement)
        except Exception as exc:
            # a dataset that has not been produced yet has no files, skip it until the next run.
            log.warning("skipped statement, %s", str(exc).splitlines()[0])
            continue
        registered.append(statement)
    for name in table_names(registered):
        # unpartitioned curated tables have nothing to recover and raise, which is fine.
        with contextlib.suppress(Exception):
            spark.sql(f"MSCK REPAIR TABLE {name}")
    return table_names(registered)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="register hdfs datasets in the hive metastore")
    parser.add_argument("--config", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("serving.register_tables", run_id, cfg.log_level)
    spark = build_spark_session(cfg, "register-tables")
    names = register(spark, cfg, ddl_path(cfg).read_text(encoding="utf-8"), log)
    log.info("registered %d tables, %s", len(names), names)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
