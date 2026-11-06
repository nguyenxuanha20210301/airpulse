# Architecture

AirPulse follows the lambda architecture. One stream of raw events feeds two paths, a speed
layer that answers "what is happening now" within a minute, and a batch layer that rebuilds
exact, complete views from the master dataset every night. A query layer joins both views for
the dashboards.

## Data flow

1. Producers call the public APIs every 30 minutes for 63 points, one per former province.
   Every observation becomes a JSON event with an `event_id`, the source, the location, the
   event time in UTC, the ingestion time and the `run_id` of the producer. Events are published
   to the Kafka topics `weather.raw` and `air.raw`, keyed by `location_id` so one place always
   lands in one partition and keeps its order.
2. The speed layer is one Spark Structured Streaming application with several queries.
   - `archive` copies every raw message to HDFS as Parquet under `raw/<dataset>/dt=YYYY-MM-DD`.
     This is the append only master dataset.
   - `clean` parses, validates and deduplicates events inside a two hour watermark, computes
     the US EPA index from the pollutant concentrations and publishes to `weather.clean` and
     `air.clean`. The newest row per location is upserted in Cassandra.
   - `windows` keeps sliding 1h, 3h and 24h aggregates per location in Cassandra.
   - `alerts` raises threshold alerts (AQI above 150, temperature above 39 or below 13) and
     spike alerts (PM2.5 more than three standard deviations above its 24h mean) to the
     `air.alerts` topic and Cassandra.
   - `forecast` scores the latest values with the models of the batch layer and writes the
     1h, 6h and 24h forecasts to Cassandra.
3. The batch layer runs nightly as scheduled Spark applications.
   - `raw_to_clean` rebuilds the clean layer exactly, the last ingested copy of each event wins.
   - `trends` produces daily, monthly, seasonal and yearly tables, the month by hour heatmap
     and the count of days above the WHO and QCVN limits.
   - `correlations` computes Pearson correlations between PM2.5 and the weather per location,
     region and month.
   - `monthly_average` is the same monthly mean written in the MapReduce pattern, a mapper that
     emits partial sums and a reducer that adds them.
   - `features` materializes the forecast feature table, current values, time of day and
     rolling statistics, with the future values as targets.
   - `train` fits one gradient boosted tree per target and horizon, evaluates it against the
     persistence, moving average and same hour yesterday baselines on a held out month, and
     stores the model in a versioned registry on HDFS.
   - `register_tables` registers every dataset in the Hive metastore so Trino can read it.
4. Trino serves one SQL surface over the Hive catalog (HDFS) and the Cassandra catalog. The
   views in `schemas/trino/views.sql` join the realtime view with the batch view, for example
   the current value next to the long term monthly mean of the same place.
5. Grafana shows the realtime dashboard through the Trino datasource and the cluster dashboard
   through Prometheus. Superset is used for the analytical dashboards on the curated tables.

## Why these components

| Need | Choice | Reason |
|---|---|---|
| Durable, ordered, replayable stream | Kafka in KRaft mode, three brokers, replication factor three | Partitions give ordering per location, replication gives tolerance to one broker loss, replay topics let history be pushed through the same code |
| Master dataset | HDFS with Parquet | Append only, replicated across datanodes, columnar files make the batch jobs cheap, Hive partitions by day prune the reads |
| Processing | Spark | One engine for streaming, batch, SQL and MLlib, the same cleaning and feature functions run on both paths |
| Realtime view | Cassandra | Writes are fast and idempotent on the primary key, so the speed layer can re emit windows and alerts freely, reads by partition key are the shape the dashboard needs, tunable consistency is demonstrated with ONE on dev and LOCAL_QUORUM on demo |
| Query | Trino | Joins HDFS and Cassandra in one statement, which is exactly the merge step of the lambda architecture |
| Metadata | Hive metastore | Shared catalog between Spark and Trino |
| Platform | Kubernetes with kind | Several nodes on one machine, pods can be killed and nodes stopped for the fault tolerance tests |

## Traceability

Every Kafka message carries `event_id`, `source`, `event_ts_utc`, `ingested_ts_utc`,
`schema_version` and `run_id`. Clean rows add `clean_run_id`, curated rows add the `run_id` of
the batch job, forecasts carry the `model_version` and models carry the `run_id` and date range
they were trained on. A number on a dashboard can therefore be followed back to the raw
message and the run that produced each step.

## Resource plan

The demo profile fits a 32 GB machine with the limits set in the manifests. Kafka uses about
1.5 GB per broker, HDFS 1 GB for the namenode and 0.75 GB per datanode, Cassandra 2 GB per
node, the speed layer 2 GB, Trino 2 GB per node, monitoring 1.5 GB and Superset 2 GB. Batch
jobs run at night when the speed layer is the only other Spark application. Superset can be
skipped with `SKIP_SUPERSET=1` when memory is tight.
