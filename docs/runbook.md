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
echo 'fs.inotify.max_user_watches=524288' | sudo tee -a /etc/sysctl.conf
echo 'fs.inotify.max_user_instances=512' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

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

To try the demo profile without losing the dev cluster, stop the dev nodes and create the demo
cluster under another name, the variable goes to every make target of that cluster.

```bash
docker stop airpulse-control-plane airpulse-worker
CLUSTER_NAME=airpulse-demo make cluster-up PROFILE=demo
CLUSTER_NAME=airpulse-demo make deploy PROFILE=demo
CLUSTER_NAME=airpulse-demo make demo PROFILE=demo
```

`docker start` on the two dev nodes and `kubectl config use-context kind-airpulse` bring the dev
cluster back later. On the demo profile the producer restarts a few times right after the
deployment, until the third broker has joined and topics with three replicas can be created.

`deploy.sh` is idempotent. It creates the configmaps from `configs/`, the secret from `.env`,
installs the Strimzi and Spark operators with Helm, applies the kustomize overlay of the
profile, waits for Kafka, HDFS and Cassandra, applies the Cassandra schema with a job and
finally installs Trino, the monitoring stack and Superset. Use `SKIP_IMAGES=1` to reuse the
images already loaded and `SKIP_SUPERSET=1` to save memory.

## 4. Data

The producer starts with the deployment and publishes every 30 minutes. History is loaded with
the one off backfill application, edit the dates in `deploy/manifests/base/spark-oneoff.yaml`
and apply it, or run `make demo` which backfills two months, runs every batch job once and
starts a replay. The scheduled batch applications run nightly after that.

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

## 7. Tear down

```bash
make cluster-down
```

Everything, including the volumes, is removed with the kind cluster.
