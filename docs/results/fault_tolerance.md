# Fault tolerance

The drills of `tests/fault_tolerance` on the demo cluster, 2026-11-07, with the live producer
and speed layer running. Raw numbers are in `fault_tolerance.csv`, the cluster dashboard of the
period is in `docs/images/grafana_cluster_drills.png`.

## Kafka broker

`kill_broker.sh` deletes one of the three brokers while `kafka-producer-perf-test` writes 300,000
records of 200 bytes at 3,000 per second with acks all and idempotence, into a topic with three
replicas and `min.insync.replicas` 2.

| Measure | Value |
|---|---|
| records acknowledged to the producer | 300,000 |
| records found in the topic afterwards | 300,000 |
| producer retries when leaders moved, `NotLeaderOrFollower` | 9 |
| highest latency of a write, 99.9th percentile | 1,274 ms, 1,168 ms |
| under replicated partitions at the peak | 72 |
| broker back and ready | 27 s |
| every partition in sync again | 30 s |

Losing one broker of three leaves two replicas of every partition in sync, enough for acks all
with a minimum of two. The client found the new leaders within a second and a half, nothing was
lost and nothing was written twice. The speed layer and the producer of the live feed kept
running.

## HDFS datanode

`kill_datanode.sh` deletes one of the three datanodes and counts the raw air layer through Trino
while the pod is away. Every block has two copies on different nodes.

| Measure | Value |
|---|---|
| rows before | 125,424 |
| rows counted while the datanode was down | 125,424 |
| duration of that count | 64 s |
| datanode back and ready | 67 s |
| under replicated blocks afterwards | 0 |

The read succeeded with the same result, the HDFS client skipped the dead node and read the
second copy of each block. It took 64 s instead of a few seconds because the namenode still
listed the deleted datanode. This cluster sets the recheck interval to 15 s and keeps the
heartbeat at 3 s, so a datanode is only declared dead after 60 s without heartbeat, against
ten and a half minutes with the Hadoop defaults. Until then reads were still sent to its
address and waited for the connection to fail before they moved to the second copy.

## Cassandra node

`kill_cassandra.sh` deletes one of the three nodes and probes a keyspace with replication factor
three from another node.

| Measure | Value |
|---|---|
| node seen down by the ring | 2 s |
| write at QUORUM while down | ok |
| read at QUORUM while down | ok |
| read at ALL while down | refused, cannot achieve consistency level ALL |
| every node up and normal in the ring | 22 s |
| read at ALL afterwards | ok |

QUORUM needs two of three replicas and stays available, ALL needs every replica and refuses
rather than return an answer it cannot confirm, the trade between availability and consistency
of the CAP theorem, tuned per request. The demo profile reads and writes at LOCAL_QUORUM. The
ring saw the restarted node as up 22 s after the deletion, the readiness probe of Kubernetes
only checks it after 60 s.

## Speed layer executor and driver

`kill_executor.sh` deletes one of the two executors, `kill_driver.sh` deletes the driver. Both
restart the producer afterwards, which polls the APIs as it starts, to push data through.

| Measure | Value |
|---|---|
| replacement executor running | 2 s |
| first batch with data after the executor was deleted | 40 s |
| driver restarts during the executor drill | 0 |
| new driver pod running | 11 s |
| queries running again after the driver was deleted | 115 s |
| batch number of `clean-air` before and after | 9, then 10 |

The driver asks for a new executor as soon as one disappears and retries the lost tasks. When the
driver itself goes, the operator submits the application again, which takes most of the two
minutes, and every query reads its offsets and state from the checkpoint on HDFS. The batch
numbers go on from 9 to 10 instead of restarting at zero, nothing is read twice or skipped, and
the sinks upsert on their keys, so a repeated batch would not duplicate rows.

During the drills both executors of the speed layer were also killed by the kernel for memory a
few minutes after the six forecast models had been loaded, with 2g of heap and 512m of overhead.
The driver replaced them by itself. The overhead is now 1g, see
`deploy/manifests/base/spark-speed-layer.yaml`.

## Whole worker node

`stop_worker.sh` stops the container of a kind worker, which takes one broker, one datanode, one
Cassandra node, one Trino worker and whatever else runs there away at once, and starts it again
after 90 s. It was run twice. The first run picked the wrong node because of a bug in the
script, since fixed, and stopped the worker that also held the single instances of the Hive
metastore, the Trino coordinator and Prometheus, the second run stopped the intended worker.

| Measure | Worker with the single instances | Worker without them |
|---|---|---|
| node seen not ready | 43 s | 33 s |
| Cassandra read at QUORUM while down | ok | ok |
| raw air rows counted through Trino while down | unavailable | 126,180 of 126,180, in 61 s |
| node ready after its start | 2 s | 2 s |
| stateful sets, deployments and brokers ready again | 87 s | 91 s |
| node down in total | 152 s | 185 s |

With the second worker down every replicated store kept answering, Kafka with two brokers,
HDFS from the second copy of each block, Cassandra at QUORUM and Trino with its remaining
worker. The speed layer driver ran on that node, the operator submitted the application again
as soon as the node returned and the queries went on from their checkpoints.

The first run showed the limits of the single instances. Without the metastore and the
coordinator no SQL could run, and without Prometheus the dashboards lost those minutes, the gap
in `docs/images/grafana_cluster_drills.png` between 07:19 and 07:22. It also found a real bug,
the Hive metastore did not come back after its node returned, the image initializes its schema
on every start and fails once the schema exists. The stateful set now skips the initialization
when the Derby database is already on the volume, see `deploy/manifests/base/hive-metastore.yaml`,
and the metastore restarted cleanly. The readiness count of the first run is too optimistic for
the same reason, the metastore has no readiness probe and counted as ready between two crashes.

## What stays single

The namenode, the Hive metastore, the Trino coordinator, the Superset metadata database and
Prometheus run as one instance on the demo profile, as the speed layer driver does. Losing their
node stops the batch layer, the SQL layer or the dashboards until the node or the pod comes
back, while the stores that carry the data, Kafka, HDFS blocks and Cassandra, keep serving. A
production setup would run the namenode with a standby, the metastore on a replicated database
and several coordinators behind a gateway, which the course cluster leaves out to fit one
machine.
