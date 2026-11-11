# Runbook

How to rebuild the system from nothing on one Ubuntu machine with 32 GB of memory.

## 1. Tools

Python 3.11, GNU make, a JDK 17, Docker, kind, kubectl and Helm. The cluster tools can be
installed with

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl
KIND_VERSION=$(curl -s https://api.github.com/repos/kubernetes-sigs/kind/releases/latest | grep -oP '"tag_name": "\K[^"]+')
curl -Lo kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
sudo install -m 0755 kind /usr/local/bin/kind && rm kind
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
printf 'fs.inotify.max_user_watches=524288\nfs.inotify.max_user_instances=512\n' | sudo tee /etc/sysctl.d/90-kind.conf
sudo sysctl --system
```

Every node of a kind cluster runs kubelet, containerd and dozens of pods that watch files, the
default of 128 inotify instances runs out and kube-proxy then fails with `fsnotify watcher init:
too many open files`, which takes down every service address of the cluster. The limits go to
`/etc/sysctl.d`, Ubuntu 26.04 no longer reads `/etc/sysctl.conf` at boot, so lines written there
only last until the next restart. `cat /proc/sys/fs/inotify/max_user_instances` shows the value
in force.

## 2. Python environment and tests

```bash
cp .env.example .env
make setup
make check
```

`make check` runs ruff and the unit tests, including the Spark based ones on a local session.
The makefile finds a JDK 17 under `/usr/lib/jvm` by itself, set `JAVA_HOME` to point elsewhere.

## 3. Cluster

```bash
make cluster-up PROFILE=dev     # one worker, or PROFILE=demo for three
make deploy PROFILE=dev          # builds the images, installs everything, applies the schema
make port-forward PROFILE=dev    # grafana on 3000, trino on 8080, hdfs on 9870, superset on 8088
```

To try the demo profile without losing the dev cluster, stop the dev cluster and create the demo
cluster under another name, the variable goes to every make target of that cluster.

```bash
make cluster-stop PROFILE=dev
CLUSTER_NAME=airpulse-demo make cluster-up PROFILE=demo
CLUSTER_NAME=airpulse-demo make deploy PROFILE=demo
CLUSTER_NAME=airpulse-demo make demo PROFILE=demo
```

On the demo profile the producer restarts a few times right after the deployment, until the
third broker has joined and topics with three replicas can be created.

A cluster is stopped and started again without losing its data with

```bash
CLUSTER_NAME=airpulse-demo make cluster-stop PROFILE=demo
CLUSTER_NAME=airpulse-demo make cluster-start PROFILE=demo
```

`cluster-stop` gives the nodes two minutes to shut down and keeps every volume. kind creates
its nodes with the restart policy `on-failure` and a stopped node exits with an error code, so
Docker would start it again at the next boot, next to whatever else the machine runs. The
target clears the policy first, the cluster then stays down until `cluster-start`.

`cluster-start` starts the nodes, selects the kubectl context and waits for the nodes, HDFS,
Cassandra and the speed layer. On 2026-11-11 the demo cluster had its thirteen streaming
queries running again 7 minutes after the nodes were started. The first submission of the
speed layer can fail with `SafeModeException`, the namenode refuses writes until the datanodes
have reported their blocks, and the operator submits it again a few minutes later. The queries
resume from their checkpoints. The target ends by printing the backfill command for the hours
the cluster was down, section 4 explains it.

A cluster that was not stopped this way comes back by itself after a restart of the machine
and the pods recover within a few minutes. If `kube-proxy` in `kube-system` keeps crashing, the
inotify limits of section 1 are not in force.

`deploy.sh` is idempotent. It creates the configmaps from `configs/`, the secret from `.env`,
installs the Strimzi and Spark operators with Helm, applies the kustomize overlay of the
profile, waits for Kafka, HDFS and Cassandra, applies the Cassandra schema with a job and
finally installs Trino, the monitoring stack and Superset. Use `SKIP_IMAGES=1` to reuse the
images already loaded and `SKIP_SUPERSET=1` to save memory.

## 4. Data

The producer starts with the deployment and publishes every 30 minutes. History is loaded with
the one off backfill application, edit the dates in `deploy/manifests/base/spark-oneoff.yaml`
and apply it, or run `make demo` which backfills two months, runs every batch job once and
starts a replay.

The nightly schedules of the batch jobs are created suspended. A laptop is rarely on all night,
and when the Spark operator comes back after the machine was off it starts every missed run at
once, seven applications next to the speed layer, which once ran the machine out of memory.
`make batch PROFILE=demo` runs the jobs one after the other and refreshes the Trino views. On a
cluster that stays on, the schedules can be resumed one by one.

```bash
kubectl -n airpulse patch scheduledsparkapplication batch-raw-to-clean --type merge -p '{"spec":{"suspend":false}}'
```

A gap in the history, for example when an API refused requests during a backfill or the cluster
was stopped for a few days, is filled by running the backfill module on the template of a
scheduled job, then the batch jobs in order. The events of the gap get the same ids as any copy
already stored, so `raw_to_clean` keeps one row per hour. The range may end today, the hours
that have not started yet are left out.

```bash
APP_NAME=backfill-gap MODULE=airpulse.ingestion.backfill EXECUTORS=1 \
  sh deploy/scripts/run_job.sh demo batch-raw-to-clean --start 2026-09-01 --end 2026-09-09 --domain weather
make batch PROFILE=demo
```

The jobs read the configuration from the `airpulse-configs` configmap, which `make deploy`
creates from `configs/`, after a change of `configs/` run `SKIP_IMAGES=1 make deploy` first.

The Superset dashboard is loaded from `dashboards/superset/assets` by
`deploy/scripts/import_dashboards.sh`, which `make demo` calls once the Trino views exist. After
changing charts in the Superset UI, `deploy/scripts/export_dashboards.sh` writes them back to
the repo. Grafana dashboards are provisioned from `dashboards/grafana` by `make deploy`.

For a laptop run without the cluster, every job accepts `--input` and `--output` to work on
local directories, for example

```bash
AIRPULSE_SPARK__MASTER='local[*]' .venv/bin/python -m airpulse.ingestion.backfill \
  --config configs/dev.yaml --start 2024-01-01 --end 2024-01-31 --output data/local
AIRPULSE_SPARK__MASTER='local[*]' .venv/bin/python -m airpulse.batch.raw_to_clean \
  --config configs/dev.yaml --input data/local --output data/local
```

## 5. Replay and benchmarks

```bash
kubectl -n airpulse apply -f deploy/manifests/base/spark-oneoff.yaml
```

The replay application publishes a month of history into `weather.raw.replay` and
`air.raw.replay` at the speed of the bench profile. A second speed layer instance can consume
those topics with `--replay` so the live watermarks are not disturbed. The scripts under
`tests/benchmark` record throughput and job durations for different partition and executor
counts.

## 6. Fault tolerance drills

The scripts under `tests/fault_tolerance` kill one component at a time and print what to
watch, a broker, a datanode, a Cassandra node, a Spark executor or a whole worker node.

## 7. Notebooks

```bash
make setup-notebook
make notebook
```

The first target adds Jupyter, matplotlib and the Trino client to the virtualenv, the second
opens Jupyter on `notebooks/`. A notebook reads its tables through Trino and keeps every result
as a parquet file under `data/eda`, which git ignores. With those files in place it runs
without the cluster. To take a new extract, forward Trino with `make port-forward`, set
`AIRPULSE_PROFILE` and `AIRPULSE_TRINO__URL=http://localhost:8080` in `.env` and set `REFRESH`
to true in the first code cell.

`make notebook-run` executes every notebook and writes an html copy with its output to
`.cache/notebooks`. The notebooks in git carry no output, `make notebook-clean` removes it and
`make check` fails while one still has some. The exploratory notebook writes its charts to
`docs/images` with the prefix `eda`.

## 8. Tear down

```bash
make cluster-down
```

Everything, including the volumes, is removed with the kind cluster.
