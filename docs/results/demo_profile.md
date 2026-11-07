# Demo profile run

The demo profile was brought up from an empty kind cluster on 2026-11-07 with
`CLUSTER_NAME=airpulse-demo`, next to the dev cluster, which was stopped and kept for later.

## Timeline

| Step | Command | Duration |
|---|---|---|
| Cluster, 1 control plane and 3 workers | `make cluster-up PROFILE=demo` | 40 s |
| Images, operators, stores, query and dashboards | `make deploy PROFILE=demo` | 14 min 55 s, of which 2 min 12 s for the two images |
| Backfill of 2026-09-01 to 2026-11-06 | first step of `make demo` | 2 min 25 s |
| `raw_to_clean`, `trends`, `correlations`, `mapreduce`, `features` | `make demo` | 92 s, 71 s, 81 s, 41 s, 91 s |
| `ml-train`, six models and five baselines | `make demo` | 12 min 17 s |
| `register_tables`, Trino views, Superset import | `make demo` | 61 s plus a few seconds |
| Whole demo scenario | `make demo PROFILE=demo` | 22 min 20 s |

The producer restarted five times right after the deployment, Kafka had fewer than three
brokers registered and the creation of topics with replication factor three failed. The
restarts stopped once the third broker joined, the first cycle then published 252 weather and
252 air events with no failed delivery.

## Placement

Every worker received one broker, one datanode and one Cassandra node, the anti affinity rules
of the three stateful sets and of the Kafka node pool prefer different nodes.

| Node | Pods |
|---|---|
| worker | kafka 2, datanode 0, cassandra 1, hive metastore, trino coordinator, grafana, prometheus, superset web, one speed layer executor |
| worker2 | kafka 0, datanode 1, namenode, cassandra 2, speed layer driver, one trino worker, spark operator, strimzi entity operator |
| worker3 | kafka 1, datanode 2, cassandra 0, one trino worker, one speed layer executor, superset worker and metadata database, strimzi operator |

The namenode and the metastore are the two components with a single instance, losing the node
that runs them stops the batch layer and the Hive catalog of Trino until they come back.

## Replication checks

| Store | Check | Result |
|---|---|---|
| Kafka | `kafka-topics.sh --describe` | `weather.raw`, `air.raw`, `air.alerts` with 6 partitions and replication factor 3, `min.insync.replicas` 2 |
| HDFS | `hdfs fsck /airpulse` | 649 blocks, average replication 2.0, no under replicated or missing block, 3 live datanodes |
| Cassandra | `nodetool status`, `system_schema.keyspaces` | 3 nodes up and normal, keyspace `airpulse` with SimpleStrategy and replication factor 3, writes at LOCAL_QUORUM |

On the dev cluster the same fsck had reported 78 percent of the blocks under replicated, the
Spark jobs wrote every file with the Hadoop client default of three copies while one datanode
existed. `hdfs.replication` of the profile is now passed to Spark as `spark.hadoop.dfs.replication`.

## Memory

Working set of the pods in the namespace, read from the kubelet summary API.

| Moment | Pods in the namespace | Per worker | Host used, swap used |
|---|---|---|---|
| after the deployment, speed layer just started | 15.3 GiB | 5.5, 5.6, 5.5 GiB | 21.2 GiB, 2.9 GiB |
| during `ml-train`, three executors of 2.5 GiB | 21.4 GiB | 7.3, 9.4, 6.4 GiB | 27.9 GiB, 3.2 GiB |

The largest pods are the three Cassandra nodes at about 1.35 GiB each, the brokers and Trino
nodes at about 0.8 GiB each, and the speed layer executors, which grow to about 2.3 GiB after
some hours. The profile fits the 31 GiB machine, the training run leaves about 4 GiB free.

## Data and dashboards

The archive endpoint of Open-Meteo refused every request with `429 Too many concurrent
requests` during the run, so the weather history of the last eight weeks came from the forecast
endpoint, see `docs/data_sources.md`. That endpoint has no values before 2026-09-10, weather
for 2026-09-01 to 2026-09-09 is therefore missing for most points while air quality is
complete, 4,284 point days in `daily_by_point` of which 3,987 have weather.

The imported Superset dashboard rendered its eighteen charts without error on the new
instance, screenshots are under `docs/images/superset_*.png`. The Grafana overview showed the
realtime panels, `docs/images/grafana_overview.png`.

| Forecast, holdout MAE | 1 h | 6 h | 24 h |
|---|---|---|---|
| PM2.5, gradient boosted trees | 2.14 | 6.83 | 7.97 |
| PM2.5, persistence | 2.24 | 8.96 | 7.98 |
| temperature, gradient boosted trees | 0.37 | 0.83 | 1.05 |
| temperature, persistence | 0.65 | 2.84 | 0.85 |

The trees beat persistence everywhere except temperature one day ahead, where the value of the
same hour yesterday is hard to improve on with two months of training data.

## Speed layer latency

The first micro batches after the start carried one producer cycle, 252 events per domain, and
took between 25 s for the archive queries and 150 s for the alert queries. Thirteen queries
share two executors of one core each, so a batch waits for the others, and the first batch of
each query also pays for the connections and the state store setup. The trigger is one minute,
the realtime view therefore lags the source by one to three minutes on this profile.
