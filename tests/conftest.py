import os
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

JVM_DIR = Path("/usr/lib/jvm")


def _java_17_home() -> str | None:
    if os.environ.get("JAVA_HOME"):
        return None
    if not JVM_DIR.is_dir():
        return None
    for candidate in sorted(JVM_DIR.iterdir()):
        if "17" in candidate.name and (candidate / "bin" / "java").exists():
            return str(candidate)
    return None


@pytest.fixture(scope="session")
def spark() -> Iterator:
    # the python side converts timestamps with the process timezone, pinning it to UTC keeps
    # assertions independent of the machine.
    os.environ["TZ"] = "UTC"
    time.tzset()
    # workers must run the same interpreter as the driver, which is the one running the tests.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    java_home = _java_17_home()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("airpulse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
