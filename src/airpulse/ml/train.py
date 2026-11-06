"""train one gradient boosted tree per target and horizon and store it in the registry.

the split is by time, the last test_days of the range are held out. the model is evaluated
against the baselines on that holdout and both land in curated/forecast_evaluation, which the
dashboards read.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import timedelta

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airpulse.batch.common_io import add_io_args, read_layer, write_layer
from airpulse.batch.features import DS_FEATURES
from airpulse.common.config import Config, load_config
from airpulse.common.features import FEATURE_COLUMNS, HORIZONS_H, TARGETS, target_column
from airpulse.common.logging import get_logger, new_run_id
from airpulse.common.model_registry import ModelRegistry
from airpulse.common.schemas import LAYER_CURATED, LAYER_MODELS
from airpulse.common.spark import build_spark_session
from airpulse.ml.baselines import EVALUATION_COLUMNS, error_metrics, evaluate_baselines

DS_EVALUATION = "forecast_evaluation"


def model_name(target: str, horizon_h: int) -> str:
    return f"{target}_h{horizon_h}"


def prepare(df: DataFrame, target: str, horizon_h: int) -> DataFrame:
    """rows with a known future value, missing inputs set to zero as at scoring time."""
    label = target_column(target, horizon_h)
    return df.where(F.col(label).isNotNull()).fillna(0.0, subset=FEATURE_COLUMNS)


def split_by_time(df: DataFrame, test_days: int) -> tuple[DataFrame, DataFrame]:
    last = df.agg(F.max("event_ts_utc")).first()[0]
    cutoff = last - timedelta(days=test_days)
    return df.where(F.col("event_ts_utc") < cutoff), df.where(F.col("event_ts_utc") >= cutoff)


def build_pipeline(cfg: Config, label: str) -> Pipeline:
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="keep"
    )
    gbt = GBTRegressor(
        labelCol=label,
        featuresCol="features",
        maxIter=cfg.ml.max_iter,
        maxDepth=cfg.ml.max_depth,
        seed=7,
    )
    return Pipeline(stages=[assembler, gbt])


def train_one(
    cfg: Config, features: DataFrame, target: str, horizon_h: int
) -> tuple[PipelineModel, DataFrame, dict]:
    """fit on the training part and report errors on the holdout next to the baselines."""
    label = target_column(target, horizon_h)
    data = prepare(features, target, horizon_h)
    train, test = split_by_time(data, cfg.ml.test_days)
    train.persist()
    test.persist()
    model = build_pipeline(cfg, label).fit(train)
    model_errors = error_metrics(model.transform(test), label, "prediction").first()
    evaluation = evaluate_baselines(test, target, horizon_h).unionByName(
        test.sparkSession.createDataFrame(
            [
                (
                    target,
                    horizon_h,
                    "gbt",
                    model_errors["mae"],
                    model_errors["rmse"],
                    model_errors["n"],
                )
            ],
            EVALUATION_COLUMNS,
        )
    )
    metadata = {
        "target": target,
        "horizon_h": horizon_h,
        "features": FEATURE_COLUMNS,
        "n_train": train.count(),
        "n_test": model_errors["n"],
        "mae": model_errors["mae"],
        "rmse": model_errors["rmse"],
        "max_iter": cfg.ml.max_iter,
        "max_depth": cfg.ml.max_depth,
    }
    train.unpersist()
    test.unpersist()
    return model, evaluation, metadata


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="train the short term forecast models")
    add_io_args(parser)
    parser.add_argument("--targets", default=",".join(TARGETS))
    parser.add_argument("--horizons", default=",".join(str(h) for h in HORIZONS_H))
    parser.add_argument("--models-root", help="override the registry root, for example a local dir")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    run_id = new_run_id()
    log = get_logger("ml.train", run_id, cfg.log_level)
    spark = build_spark_session(cfg, "train")
    features = read_layer(
        spark, cfg, LAYER_CURATED, DS_FEATURES, args.input, args.start, args.end
    ).persist()
    n_rows = features.count()
    if n_rows < cfg.ml.min_rows:
        log.error("only %d feature rows, at least %d are needed", n_rows, cfg.ml.min_rows)
        return 1
    registry = ModelRegistry(spark, args.models_root or cfg.hdfs.dataset_path(LAYER_MODELS, ""))
    evaluations = None
    for target in args.targets.split(","):
        for horizon in (int(h) for h in args.horizons.split(",")):
            model, evaluation, metadata = train_one(cfg, features, target, horizon)
            version = registry.save(
                model,
                model_name(target, horizon),
                {**metadata, "run_id": run_id, "start": str(args.start), "end": str(args.end)},
            )
            log.info(
                "saved %s version %s with mae %.3f on %d rows",
                model_name(target, horizon),
                version,
                metadata["mae"],
                metadata["n_test"],
            )
            evaluation = evaluation.withColumn("model_version", F.lit(version)).withColumn(
                "run_id", F.lit(run_id)
            )
            evaluations = evaluation if evaluations is None else evaluations.unionByName(evaluation)
    path = write_layer(
        evaluations, cfg, LAYER_CURATED, DS_EVALUATION, args.output, partitioned=False
    )
    log.info("wrote evaluation to %s", path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
