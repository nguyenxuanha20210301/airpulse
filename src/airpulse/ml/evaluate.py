"""score the stored models and the baselines on a date range and write the comparison."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.batch.common_io import add_io_args, read_layer, write_layer
from airpulse.batch.features import DS_FEATURES
from airpulse.common.config import load_config
from airpulse.common.features import FEATURE_COLUMNS, HORIZONS_H, TARGETS, target_column
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.model_registry import ModelRegistry
from airpulse.common.schemas import LAYER_CURATED, LAYER_MODELS
from airpulse.common.spark import build_spark_session
from airpulse.ml.baselines import EVALUATION_COLUMNS, error_metrics, evaluate_baselines
from airpulse.ml.train import DS_EVALUATION, model_name


def evaluate_models(registry: ModelRegistry, features: DataFrame) -> DataFrame | None:
    result = None
    for target in TARGETS:
        for horizon in HORIZONS_H:
            label = target_column(target, horizon)
            data = (
                features.where(F.col(label).isNotNull())
                .fillna(0.0, subset=FEATURE_COLUMNS)
                .persist()
            )
            evaluation = evaluate_baselines(data, target, horizon)
            try:
                model, meta = registry.load(model_name(target, horizon))
            except FileNotFoundError:
                version = None
            else:
                version = meta["version"]
                errors = error_metrics(model.transform(data), label, "prediction").first()
                evaluation = evaluation.unionByName(
                    data.sparkSession.createDataFrame(
                        [(target, horizon, "gbt", errors["mae"], errors["rmse"], errors["n"])],
                        EVALUATION_COLUMNS,
                    )
                )
            data.unpersist()
            evaluation = evaluation.withColumn("model_version", F.lit(version))
            result = evaluation if result is None else result.unionByName(evaluation)
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="evaluate stored models against the baselines")
    add_io_args(parser)
    parser.add_argument("--models-root")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("ml.evaluate", run_id, cfg.log_level)
    spark = build_spark_session(cfg, "evaluate")
    features = read_layer(spark, cfg, LAYER_CURATED, DS_FEATURES, args.input, args.start, args.end)
    registry = ModelRegistry(spark, args.models_root or cfg.hdfs.dataset_path(LAYER_MODELS, ""))
    result = evaluate_models(registry, features)
    path = write_layer(
        result.withColumn("run_id", F.lit(run_id)),
        cfg,
        LAYER_CURATED,
        DS_EVALUATION,
        args.output,
        partitioned=False,
    )
    log.info("wrote evaluation to %s", path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
