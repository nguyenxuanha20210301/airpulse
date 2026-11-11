#!/bin/sh
# start the nodes of a stopped cluster and wait until it serves again, usage cluster_start.sh <profile>.
# the stores come back from their volumes and the speed layer resumes from its checkpoints. the
# hours the cluster was down are missing from the live stream, the last lines print the backfill
# that fills them.
set -eu
. "$(dirname "$0")/common.sh"
need docker
need kubectl

list_nodes() {
  docker ps -a --filter "label=io.x-k8s.kind.cluster=$CLUSTER_NAME" \
    --filter "label=io.x-k8s.kind.role=$1" --format '{{.Names}}' | sort
}
control=$(list_nodes control-plane)
workers=$(list_nodes worker)
if [ -z "$control" ]; then
  echo "no nodes of cluster $CLUSTER_NAME on this machine, create it with cluster_up.sh"
  exit 1
fi
if [ "$(docker inspect -f '{{.State.Running}}' $control)" = "true" ]; then
  echo "cluster $CLUSTER_NAME is already running"
  exit 0
fi
stopped_on=$(docker inspect -f '{{.State.FinishedAt}}' $control | cut -c1-10)
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker start $control $workers >/dev/null
kubectl config use-context "kind-$CLUSTER_NAME" >/dev/null

# usage wait_until <seconds> <what> <command>, the command is tried every five seconds.
wait_until() {
  limit=$1
  what=$2
  shift 2
  began=$(date +%s)
  until "$@"; do
    if [ $(( $(date +%s) - began )) -gt "$limit" ]; then
      echo "timed out waiting for $what"
      exit 1
    fi
    sleep 5
  done
  echo "$what after $(( $(date +%s) - began )) s"
}

# the api server still holds the statuses from before the stop until every kubelet reports
# again, so a node or a pod only counts once its status is newer than this start.
nodes_ready() {
  total=$(echo $control $workers | wc -w)
  ready=$(kubectl get nodes -o jsonpath='{range .items[*]}{range .status.conditions[?(@.type=="Ready")]}{.status}{" "}{.lastHeartbeatTime}{"\n"}{end}{end}' 2>/dev/null \
    | awk -v since="$started" '$1 == "True" && $2 >= since' | wc -l)
  [ "$ready" -eq "$total" ]
}
hdfs_writable() {
  kubectl -n "$NAMESPACE" exec hdfs-namenode-0 -c namenode -- hdfs dfsadmin -safemode get 2>/dev/null \
    | grep -q "Safe mode is OFF"
}
speed_layer_running() {
  kubectl -n "$NAMESPACE" get pod speed-layer-driver -o jsonpath='{.status.phase} {.status.startTime}' 2>/dev/null \
    | awk -v since="$started" '$1 == "Running" && $2 >= since' | grep -q . || return 1
  kubectl -n "$NAMESPACE" logs speed-layer-driver 2>/dev/null | grep -q "queries running"
}

wait_until 300 "nodes ready" nodes_ready
if kubectl -n "$NAMESPACE" get statefulset hdfs-namenode >/dev/null 2>&1; then
  for store in hdfs-namenode hdfs-datanode cassandra; do
    kubectl -n "$NAMESPACE" rollout status "statefulset/$store" --timeout=15m >/dev/null
  done
  # the namenode refuses writes until the datanodes have reported their blocks, a speed layer
  # submitted before that fails once and the operator submits it again.
  wait_until 600 "hdfs out of safe mode" hdfs_writable
fi
if kubectl -n "$NAMESPACE" get sparkapplication speed-layer >/dev/null 2>&1; then
  wait_until 900 "speed layer queries running" speed_layer_running
fi

echo "cluster $CLUSTER_NAME is up, the live stream has a gap since $stopped_on, fill it with"
echo "  APP_NAME=backfill-gap MODULE=airpulse.ingestion.backfill EXECUTORS=1 sh deploy/scripts/run_job.sh $PROFILE batch-raw-to-clean --start $stopped_on --end $(date -u +%Y-%m-%d)"
echo "  make batch PROFILE=$PROFILE"
