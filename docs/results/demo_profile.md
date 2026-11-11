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
complete, 4,284 point days in `daily_by_point` of which 3,987 have weather. The gap was filled
later, see the gap fill section below.

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

## Gap fill

The archive endpoint answered again later on 2026-11-07. The weather of 2026-09-01 to 2026-09-09
was backfilled with `--domain weather`, 13,608 rows for 63 points and 216 hours, with the command
of `docs/runbook.md`. The hours lost while the machine was off, 15 on 2026-11-07 and 20 on
2026-11-08, were filled the same way for both domains on 2026-11-09, 3,024 rows each, the
forecast endpoint still holds those days. A filled event gets the same id as the copy the
producer had already sent, the two days hold 3,024 distinct ids in 3,024 clean rows per domain.

Each fill was followed by `make batch PROFILE=demo`, which runs the jobs one after the other with
two executors each and then refreshes the Trino views. Superset was scaled to zero during both
chains. At the peak of `ml-train` the host still had about 2.2 GiB available with 1.9 GiB of swap
in use, and the memory pressure stayed below one percent.

| Job | After the September fill | After the outage fill |
|---|---|---|
| `raw_to_clean` | 163 s | 91 s |
| `trends` | 71 s | 61 s |
| `correlations` | 71 s | 71 s |
| `mapreduce` | 40 s | 40 s |
| `features` | 61 s | 71 s |
| `ml-train` | 304 s | 305 s |
| `register_tables` | 61 s | 61 s |
| Whole chain with the views | 12 min 51 s | 11 min 40 s |

The first `raw_to_clean` overlapped with the start of the thirteen speed layer queries. After
both fills `daily_by_point` holds 4,347 point days, 63 points for the 69 days from 2026-09-01 to
2026-11-08, all with weather and air quality, and the clean layer holds 104,328 hourly rows per
domain, one per point and hour.

| Forecast after the fill, holdout MAE | 1 h | 6 h | 24 h |
|---|---|---|---|
| PM2.5, gradient boosted trees | 2.023 | 6.112 | 7.601 |
| PM2.5, persistence | 2.141 | 8.467 | 7.525 |
| PM2.5, same hour yesterday | 7.420 | 7.466 | 7.525 |
| temperature, gradient boosted trees | 0.380 | 0.835 | 1.041 |
| temperature, persistence | 0.649 | 2.829 | 0.848 |
| temperature, same hour yesterday | 0.841 | 0.841 | 0.848 |

The trees win at 1 h and 6 h for both targets. One day ahead persistence, which at that horizon
is the value of the same hour yesterday, is now better for both, by 1 percent for PM2.5 and 23
percent for temperature, while the first run still had the trees ahead for PM2.5. The holdout is
the last 30 days. Its mean temperature is 26.7 degrees against 26.9 in the training days and 0.8
percent of its hours are colder than anything the model saw at the same point, so a change of
season between the two parts does not explain the gap.

## Restarts of the machine

Three incidents in the days after the first run stopped the machine or the cluster. The causes
were in the setup of the cluster rather than in the jobs, each one changed a default of the repo.

| Moment | What happened | Change |
|---|---|---|
| 2026-11-07, during `features` | The batch job with three executors, the speed layer, Trino and Superset pushed the host into swap. `systemd-oomd` of Ubuntu then killed the terminal session that drove the run, it acts on the desktop session and not on the containers | batch jobs of the demo profile run with two executors, the benchmark found four only 3 percent faster than two |
| 2026-11-08, after a restart | The Spark operator came back with seven nightly runs missed and started all of them within 24 s, next to the speed layer and the stopped dev cluster, which Docker had started again because of its restart policy. Every kind node reports the whole memory of the host as allocatable, 31 GiB each, so the scheduler placed every driver and executor, the kernel ran out of memory and swap and the machine froze | the schedules are created suspended, `make batch` runs the jobs one after the other, `docker update --restart=no` keeps the dev nodes down |
| 2026-11-09, after a restart | The inotify limits were back at the default of 128 instances, the line in `/etc/sysctl.conf` is not read at boot by Ubuntu 26.04. kube-proxy failed on every node with `fsnotify watcher init: too many open files`, service addresses stopped answering and Kafka, Cassandra and the operators restarted in a loop | the limits go to `/etc/sysctl.d/90-kind.conf`, see `docs/runbook.md` |

After the third incident the Spark operator controller was scaled to zero until the schedules were
suspended, so it could not start the missed runs a second time. Once the limits were raised
kube-proxy stayed up, the brokers, the Cassandra nodes and the operators left their back off
within about ten minutes, and the operator submitted the speed layer again, its restart policy is
`Always`. The speed layer resumed from its checkpoints, the producer published its next cycle
and the realtime view caught up without further steps.

## Stop and start by hand

The demo cluster was stopped on 2026-11-09 to free the machine, with `docker update --restart=no`
and `docker stop -t 120` on its four nodes, and started again with `docker start` about 65 hours
later. Docker gives out addresses in start order, so the nodes were started one by one in an order
that returned the control plane to its former address. Two workers came back with another
address and rejoined without further steps.

| Time after `docker start` | State |
|---|---|
| 1 min 34 s | the producer is back and has published 252 weather and 252 air events |
| 1 min 52 s | the first submission of the speed layer fails, the namenode is still in safe mode and refuses to create the checkpoint directory |
| 4 min 25 s | the operator submits the speed layer again |
| about 5 min | Kafka, HDFS, Cassandra, the metastore, Trino and the speed layer driver are ready, HDFS reports three live datanodes and no missing or under replicated block |
| 7 min 2 s | thirteen queries are running, each resumed at its next batch, 20 for the archive and 25 to 27 for the others, from the offsets committed before the stop |

The first micro batches after the start read the 252 events of the producer per topic, the
archive wrote them in 20 s and 24 s on a cold executor and the next cycle in 6 s and 9 s.

The live stream had stopped after 01:00 UTC on 2026-11-09 and a producer cycle only refetches
three hours, so 61 hours were missing, from 02:00 UTC on 2026-11-09 to 14:00 UTC on 2026-11-11.
The backfill of 2026-11-09 to 2026-11-11 stored 4,284 events per domain in 50 s, 63 points for
the 68 hours up to the running one. A range that ends today used to bring the forecast for the
rest of the day with it, the Open-Meteo source now leaves out the hours that have not started
for history ranges as well. `make batch PROFILE=demo` followed with Superset scaled to zero and
took 11 min 1 s, `raw_to_clean` 91 s, `trends` 61 s, `correlations` 71 s, `mapreduce` 40 s,
`features` 61 s, `ml-train` 284 s and `register_tables` 50 s. During `ml-train` the host had
about 3.3 GiB available with 1.7 GiB of swap in use.

After the chain the clean layer holds 108,612 hourly rows per domain, one per point and hour
from 2026-09-01 00:00 to 2026-11-11 19:00 UTC and none later than the run, and `daily_by_point`
holds 4,536 point days, 63 points for 72 days, all with weather and air quality.

| Forecast after this fill, holdout MAE | 1 h | 6 h | 24 h |
|---|---|---|---|
| PM2.5, gradient boosted trees | 1.825 | 5.601 | 7.182 |
| PM2.5, persistence | 1.972 | 7.872 | 6.975 |
| PM2.5, same hour yesterday | 6.927 | 6.923 | 6.975 |
| temperature, gradient boosted trees | 0.444 | 0.900 | 1.001 |
| temperature, persistence | 0.639 | 2.775 | 0.945 |
| temperature, same hour yesterday | 0.949 | 0.947 | 0.945 |

The order did not change, the trees win at 1 h and 6 h and persistence stays ahead one day
ahead, now by 3 percent for PM2.5 and 6 percent for temperature.

## Model versions in the speed layer

The same restart showed that the forecast query kept serving the models it had loaded at its
start. It read the registry again every 60 micro batches, and a micro batch only runs when the
producer has sent data, once every 30 minutes, so a new version was picked up after about 30
hours. The forecasts up to 2026-11-09 01:00 UTC carry the versions trained on 2026-11-07, an
hour after `ml-train` had published the next ones, and the forecast for 2026-11-11 19:00 UTC
still carried the versions of 2026-11-09 although training had finished a few minutes before.

The query now compares the `latest` pointer of every model with the version in memory before
each micro batch and loads the ones that changed. On the cluster `ml-train` published six
versions between 20:14 and 20:19 UTC on 2026-11-11, the micro batch of the next producer cycle
at 20:20 logged `serving model versions` with all six, and the 63 forecasts per target and
horizon for 20:00 UTC carry them. The driver was not restarted.
