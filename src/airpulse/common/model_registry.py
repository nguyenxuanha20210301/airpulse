"""versioned storage of spark ml models on hdfs, shared by training and online scoring.

a model lives under <root>/<name>/<version>/ with a metadata.json next to the pipeline, and
<root>/<name>/latest holds the version to serve. paths work on hdfs and on a local filesystem.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession


def _fs_and_path(spark: SparkSession, path: str):
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration()), hadoop_path


def write_text(spark: SparkSession, path: str, text: str) -> None:
    fs, hadoop_path = _fs_and_path(spark, path)
    stream = fs.create(hadoop_path, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


def read_text(spark: SparkSession, path: str) -> str:
    fs, hadoop_path = _fs_and_path(spark, path)
    stream = fs.open(hadoop_path)
    try:
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(stream, "UTF-8")
        )
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        return "\n".join(lines)
    finally:
        stream.close()


def exists(spark: SparkSession, path: str) -> bool:
    fs, hadoop_path = _fs_and_path(spark, path)
    return fs.exists(hadoop_path)


def list_dirs(spark: SparkSession, path: str) -> list[str]:
    fs, hadoop_path = _fs_and_path(spark, path)
    if not fs.exists(hadoop_path):
        return []
    return sorted(
        status.getPath().getName() for status in fs.listStatus(hadoop_path) if status.isDirectory()
    )


class ModelRegistry:
    def __init__(self, spark: SparkSession, root: str) -> None:
        self.spark = spark
        self.root = root.rstrip("/")

    def model_dir(self, name: str, version: str) -> str:
        return f"{self.root}/{name}/{version}"

    def save(self, model: PipelineModel, name: str, metadata: dict[str, Any]) -> str:
        """store the pipeline under a timestamp version and make it the latest."""
        version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        target = self.model_dir(name, version)
        model.write().overwrite().save(f"{target}/pipeline")
        record = {**metadata, "name": name, "version": version}
        write_text(self.spark, f"{target}/metadata.json", json.dumps(record, indent=2, default=str))
        write_text(self.spark, f"{self.root}/{name}/latest", version)
        return version

    def versions(self, name: str) -> list[str]:
        return list_dirs(self.spark, f"{self.root}/{name}")

    def latest_version(self, name: str) -> str | None:
        pointer = f"{self.root}/{name}/latest"
        if not exists(self.spark, pointer):
            return None
        return read_text(self.spark, pointer).strip() or None

    def load(self, name: str, version: str | None = None) -> tuple[PipelineModel, dict[str, Any]]:
        version = version or self.latest_version(name)
        if version is None:
            raise FileNotFoundError(f"no version of model {name} under {self.root}")
        target = self.model_dir(name, version)
        model = PipelineModel.load(f"{target}/pipeline")
        metadata = json.loads(read_text(self.spark, f"{target}/metadata.json"))
        return model, metadata
