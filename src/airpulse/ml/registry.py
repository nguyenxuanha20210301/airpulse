"""list the models in the registry with their metadata."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from airpulse.common.config import load_config
from airpulse.common.features import HORIZONS_H, TARGETS
from airpulse.common.model_registry import ModelRegistry, read_text
from airpulse.common.schemas import LAYER_MODELS
from airpulse.common.spark import build_spark_session
from airpulse.ml.train import model_name


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="list stored forecast models")
    parser.add_argument("--config", required=True)
    parser.add_argument("--models-root")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    spark = build_spark_session(cfg, "registry")
    registry = ModelRegistry(spark, args.models_root or cfg.hdfs.dataset_path(LAYER_MODELS, ""))
    for target in TARGETS:
        for horizon in HORIZONS_H:
            name = model_name(target, horizon)
            latest = registry.latest_version(name)
            for version in registry.versions(name):
                meta = json.loads(
                    read_text(spark, f"{registry.model_dir(name, version)}/metadata.json")
                )
                marker = "latest" if version == latest else ""
                print(name, version, marker, "mae", round(meta.get("mae", float("nan")), 3))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
