# AirPulse

Real time analysis and forecasting of weather and air quality trends for the provinces of Vietnam.
The project is the course assignment for Big Data Storage and Processing. It follows the lambda
architecture and runs on a local multi node Kubernetes cluster so that distribution, scalability
and fault tolerance can be exercised on a single machine.

## What the system does

- Collects weather and air quality observations (PM2.5, PM10, NO2, O3, CO, SO2, AQI, temperature,
  humidity, wind, rain) for every province every 10 to 60 minutes from open APIs.
- Backfills several years of hourly history and can replay it at a configurable speed to generate
  load for benchmarks.
- Speed layer computes sliding window metrics, the current AQI, top polluted provinces and
  threshold or z score alerts, and writes them to a realtime view.
- Batch layer keeps raw, clean and curated datasets on HDFS, computes daily, monthly, seasonal and
  multi year trends, weather and pollution correlations, and trains a short term forecast model
  with Spark MLlib.
- A single SQL layer joins the batch views and the realtime view for dashboards.

## Architecture

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Python producers on Kubernetes CronJobs | Call the APIs on schedule and publish events to Kafka |
| Messaging | Kafka in KRaft mode, 3 brokers | Topics `weather.raw`, `air.raw`, `air.alerts` and replay topics |
| Speed layer | Spark Structured Streaming | Clean, aggregate by window, detect anomalies, online forecast |
| Batch layer | Spark batch, Spark MLlib, one MapReduce style job | Trends, correlations, features, model training |
| Raw and batch storage | HDFS with Parquet, Hive Metastore | Raw, clean and curated layers partitioned by date and province |
| Realtime storage | Cassandra, 3 nodes | Latest metrics, windows and alerts per province |
| Query | Trino | One SQL interface over the Hive and Cassandra connectors |
| Dashboards | Superset and Grafana | Analytics dashboards, realtime view and cluster monitoring |
| Monitoring | Prometheus and Grafana | Throughput, latency and resource usage during benchmarks |

Data sources are Open-Meteo (weather, air quality and historical archive), OpenAQ (station
measurements and the public archive) and OpenWeatherMap as a backup source. Details will live in
`docs/data_sources.md`.

## Repository layout

```
configs/        non secret settings per profile (dev, demo, bench) and the province list
schemas/        data contracts, Kafka event schemas, Hive DDL, Cassandra CQL, Trino views
src/airpulse/   the python package, one subpackage per layer
  common/       config, logging, schemas, AQI formulas, Kafka helpers, time utilities
  ingestion/    api sources, producer, backfill and replay
  streaming/    Spark Structured Streaming jobs, the speed layer
  batch/        Spark batch jobs, the batch layer
  mapreduce/    one job written in the MapReduce pattern for illustration
  ml/           baselines, training, evaluation, model registry on HDFS
  serving/      sinks for the realtime view
deploy/         kubernetes cluster definition, images, helm values, manifests and scripts
dashboards/     exported Superset and Grafana dashboards
notebooks/      exploration notebooks, committed without outputs
tests/          unit, integration, fault tolerance and benchmark tests
docs/           public documentation, architecture, data sources, runbook and results
```

Every job is a module with a `main` function and runs as
`python -m airpulse.<layer>.<job> --config configs/<profile>.yaml`. Nothing is hardcoded, every
host, port, path and topic comes from `configs/`. `base.yaml` holds the shared values, each
profile file extends it and overrides a few keys, and any environment variable named
`AIRPULSE_<section>__<key>` (nested keys joined with a double underscore, for example
`AIRPULSE_KAFKA__BOOTSTRAP_SERVERS`) overrides the merged result. Secrets only live in `.env`,
which is ignored by git, and in Kubernetes secrets.

## Getting started

Requirements on the host, Python 3.11, GNU make, a JDK 17 for running Spark locally, Docker,
and for the cluster kind, kubectl and Helm. `uv` is used when available, otherwise the standard
`venv` and `pip` are used. The makefile finds a JDK 17 under `/usr/lib/jvm` on its own.

```bash
cp .env.example .env
make setup
make check
```

`make help` lists every target. The cluster targets call the scripts under `deploy/scripts/`.

```bash
make cluster-up PROFILE=dev
make deploy PROFILE=dev
make port-forward PROFILE=dev
make demo PROFILE=dev
```

`make batch PROFILE=dev` runs the batch layer again, one job after the other. The nightly
schedules of the batch jobs are created suspended, a laptop that was off would otherwise start
every missed run at once.

The step by step procedure, including laptop runs of the jobs on local directories, is in
`docs/runbook.md`. The architecture and the reasons behind each component are in
`docs/architecture.md`, the sources and their quirks in `docs/data_sources.md`.

## Jobs

| Module | Layer | What it does |
|---|---|---|
| `airpulse.ingestion.producer` | ingestion | polls the sources and publishes raw events to Kafka |
| `airpulse.ingestion.backfill` | ingestion | loads years of hourly history into the raw layer |
| `airpulse.ingestion.replay` | ingestion | replays history into Kafka at a configurable speed |
| `airpulse.streaming.speed_layer` | speed | archive, clean, windows, alerts and forecast queries |
| `airpulse.batch.raw_to_clean` | batch | exact rebuild of the clean layer |
| `airpulse.batch.trends` | batch | daily, monthly, seasonal, yearly tables and the heatmap |
| `airpulse.batch.correlations` | batch | weather and pollution correlations |
| `airpulse.batch.features` | batch | the forecast feature table |
| `airpulse.mapreduce.monthly_average` | batch | the monthly mean in the MapReduce pattern |
| `airpulse.ml.train` | ml | trains and evaluates the forecast models |
| `airpulse.ml.evaluate` | ml | compares stored models with the baselines |
| `airpulse.serving.register_tables` | serving | registers the datasets in the Hive metastore |

Every job runs as `python -m <module> --config configs/<profile>.yaml` and accepts `--input`
and `--output` overrides for local runs.

## Version matrix

| Component | Version |
|---|---|
| Python | 3.11 locally, 3.10 in the Spark image |
| Spark and PySpark | 3.5.9, Scala 2.12 connectors, Kafka connector 3.5.9, Cassandra connector 3.5.1 |
| Java for Spark | 17 |
| Kafka | Strimzi operator 1.3.x, api `kafka.strimzi.io/v1`, KRaft mode |
| Spark operator | kubeflow spark-operator chart 2.5.x |
| Hadoop HDFS | 3.4.3 |
| Hive metastore | 3.1.3 with embedded Derby |
| Cassandra | 4.1 |
| Trino | chart 1.45.x, Trino 484 |
| Superset | chart 0.x, Superset 6.1 |
| Monitoring | kube-prometheus-stack, Grafana with the trino-datasource plugin |
| Kubernetes | kind 0.33, Kubernetes 1.37 |

## Conventions

- File and directory names are lowercase `snake_case`, Kubernetes resources use `kebab-case`.
- Comments, logs and notebook text are short english sentences in lowercase.
- Commits follow Conventional Commits, for example `feat(ingestion): add open-meteo source`.
- `make check` must pass before every commit.
