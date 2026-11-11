# Results

Measurements taken on the demo profile, one machine running a kind cluster with one control
plane and three workers. The CSV files are appended by the scripts under `tests/benchmark` and
`tests/fault_tolerance`, the markdown files explain them.

| File | Content |
|---|---|
| `demo_profile.md` | deployment of the demo profile, placement, memory, replication checks, demo run, gap fill, restarts of the machine, stop and start by hand, model versions served by the speed layer |
| `batch_scaling.md`, `batch_scaling.csv` | batch job duration against history size and executor count |
| `stream_scaling.md`, `stream_scaling.csv` | replay rate and speed layer processing rate against executor count |
| `trino_scaling.md`, `trino_scaling.csv` | one aggregation over ten years of history with one and two Trino workers |
| `fault_tolerance.md`, `fault_tolerance.csv` | recovery times and continuity checks of the drills |

## Environment

| Item | Value |
|---|---|
| CPU | Intel Core i5-1135G7, 4 cores, 8 threads |
| Memory | 31 GiB, 24 GiB swap |
| Disk | NVMe SSD, local path volumes of kind |
| Kubernetes | kind 0.33, Kubernetes 1.37, containerd 2.3, Docker 29.5 |
| Cluster | 1 control plane and 3 workers, all containers on the same host |
| Profile | `configs/demo.yaml`, Kafka 3 brokers with 6 partitions and replication 3, HDFS 3 datanodes with replication 2, Cassandra 3 nodes with replication 3, Trino 1 coordinator and 2 workers |

Every node of the cluster shares the same CPU, memory and disk, so the numbers show how the
software scales and recovers on one machine, not how it would on separate hosts. A run that
adds executors also adds contention for the same eight threads.
