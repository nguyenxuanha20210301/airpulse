#!/bin/sh
# install or upgrade every component for a profile. safe to run again, each step is idempotent.
# set SKIP_IMAGES=1 to reuse the images already loaded and SKIP_SUPERSET=1 to save memory.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl
need helm

helm repo add strimzi https://strimzi.io/charts/ >/dev/null
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null
helm repo add trino https://trinodb.github.io/charts >/dev/null
helm repo add superset https://apache.github.io/superset >/dev/null
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update >/dev/null

echo "configs and secrets"
kubectl -n "$NAMESPACE" create configmap airpulse-configs \
  --from-file=base.yaml="$REPO_ROOT/configs/base.yaml" \
  --from-file=dev.yaml="$REPO_ROOT/configs/dev.yaml" \
  --from-file=demo.yaml="$REPO_ROOT/configs/demo.yaml" \
  --from-file=bench.yaml="$REPO_ROOT/configs/bench.yaml" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create configmap airpulse-locations \
  --from-file="$REPO_ROOT/configs/locations/vn_provinces.csv" \
  --dry-run=client -o yaml | kubectl apply -f -
if [ -f "$REPO_ROOT/.env" ]; then
  kubectl -n "$NAMESPACE" create secret generic airpulse-secrets \
    --from-env-file="$REPO_ROOT/.env" --dry-run=client -o yaml | kubectl apply -f -
fi

if [ "${SKIP_IMAGES:-0}" != "1" ]; then
  sh "$REPO_ROOT/deploy/scripts/build_images.sh" "$PROFILE"
fi

echo "operators"
helm upgrade --install strimzi strimzi/strimzi-kafka-operator -n "$NAMESPACE" \
  -f "$REPO_ROOT/deploy/helm/kafka/values.yaml" --wait --timeout 10m
helm upgrade --install spark-operator spark-operator/spark-operator -n "$NAMESPACE" \
  -f "$REPO_ROOT/deploy/helm/spark/values.yaml" --wait --timeout 10m

echo "storage, messaging and jobs"
kubectl apply -k "$REPO_ROOT/deploy/manifests/overlays/$LAYOUT"

echo "waiting for kafka, hdfs and cassandra"
kubectl -n "$NAMESPACE" wait kafka/kafka-cluster --for=condition=Ready --timeout=15m
kubectl -n "$NAMESPACE" rollout status statefulset/hdfs-namenode --timeout=10m
kubectl -n "$NAMESPACE" rollout status statefulset/hdfs-datanode --timeout=10m
kubectl -n "$NAMESPACE" rollout status statefulset/cassandra --timeout=20m

echo "cassandra schema"
if [ "$LAYOUT" = "demo" ]; then RF=3; else RF=1; fi
sed "s/__REPLICATION_FACTOR__/$RF/" "$REPO_ROOT/schemas/cassandra/keyspace.cql" > "$CACHE_DIR/keyspace.cql"
kubectl -n "$NAMESPACE" create configmap cassandra-schema \
  --from-file=keyspace.cql="$CACHE_DIR/keyspace.cql" \
  --from-file=tables.cql="$REPO_ROOT/schemas/cassandra/tables.cql" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" delete job cassandra-schema --ignore-not-found
kubectl -n "$NAMESPACE" apply -f "$REPO_ROOT/deploy/manifests/jobs/cassandra-schema.yaml"
kubectl -n "$NAMESPACE" wait job/cassandra-schema --for=condition=complete --timeout=10m

echo "query and dashboards"
if [ "$LAYOUT" = "demo" ]; then
  helm upgrade --install trino trino/trino -n "$NAMESPACE" \
    -f "$REPO_ROOT/deploy/helm/trino/values.yaml" -f "$REPO_ROOT/deploy/helm/trino/values-demo.yaml"
else
  helm upgrade --install trino trino/trino -n "$NAMESPACE" -f "$REPO_ROOT/deploy/helm/trino/values.yaml"
fi
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack -n "$NAMESPACE" \
  -f "$REPO_ROOT/deploy/helm/monitoring/values.yaml" --timeout 10m
kubectl apply -f "$REPO_ROOT/deploy/helm/monitoring/kafka-podmonitor.yaml"
kubectl -n "$NAMESPACE" create configmap grafana-dashboards \
  --from-file="$REPO_ROOT/dashboards/grafana/" --dry-run=client -o yaml \
  | kubectl label --local -f - grafana_dashboard=1 -o yaml | kubectl apply -f -
if [ "${SKIP_SUPERSET:-0}" != "1" ]; then
  helm upgrade --install superset superset/superset -n "$NAMESPACE" \
    -f "$REPO_ROOT/deploy/helm/superset/values.yaml" --timeout 15m
fi

echo "done, run deploy/scripts/port_forward.sh to reach the dashboards"
kubectl -n "$NAMESPACE" get pods
