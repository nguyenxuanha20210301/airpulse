#!/bin/sh
# stops one kind worker node, which takes a broker, a datanode and a cassandra node away at once,
# checks that the replicated stores still answer while it is down, then starts it again and
# measures how long the node and every pod take to come back. usage stop_worker.sh [node], the
# default is the worker without the single instance pods, the namenode and the metastore.
set -eu
. "$(dirname "$0")/common.sh"
hold=${HOLD:-90}
single="hdfs-namenode-0 hive-metastore-0"
# one node name per line, without the newline two names would run together.
node_of() {
  kubectl -n "$NAMESPACE" get pod "$1" -o jsonpath='{.spec.nodeName}{"\n"}' 2>/dev/null || true
}
if [ -n "${1:-}" ]; then
  node=$1
else
  busy=$(for pod in $single; do node_of "$pod"; done | sort -u)
  node=$(kubectl get nodes -o name | cut -d/ -f2 | grep -- "-worker" | grep -v -x -F "$busy" | head -1)
fi
node_ready() {
  [ "$(kubectl get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')" = "True" ]
}
node_not_ready() {
  ! node_ready
}
probe=$(kubectl -n "$NAMESPACE" get pods -l app=cassandra -o wide --no-headers | awk -v n="$node" '$7 != n {print $1}' | head -1)
query="SELECT count(*) FROM hive.airpulse.raw_air_hourly"
trino() {
  rows=$(kubectl -n "$NAMESPACE" exec deploy/trino-coordinator -c trino-coordinator -- \
    trino --output-format=TSV --execute "$1" 2>/dev/null | tail -1)
  echo "${rows:-unavailable}"
}
quorum_read() {
  if kubectl -n "$NAMESPACE" exec "$probe" -- cqlsh -e "CONSISTENCY QUORUM; SELECT count(*) FROM $(cfg_value cassandra.keyspace).rt_air_latest;" 2>&1 \
    | grep -q -i -E "unavailable|error"; then echo refused; else echo ok; fi
}

echo "stopping $node, pods on it"
kubectl -n "$NAMESPACE" get pods -o wide --no-headers --field-selector "spec.nodeName=$node" | awk '{print "  " $1}'
rows_before=$(trino "$query")
docker stop "$node" >/dev/null
began=$(now)
seen=$(wait_until 300 node_not_ready) || true
record stop_worker node_not_ready_seconds "$seen"
record stop_worker cassandra_quorum_read_while_down "$(quorum_read)"
record stop_worker raw_rows_before "$rows_before"
record stop_worker raw_rows_read_while_down "$(trino "$query")"
sleep "$hold"
docker start "$node" >/dev/null
restarted=$(now)
record stop_worker node_ready_again_seconds "$(wait_until 600 node_ready || true)"
wait_until 1200 workloads_ready >/dev/null || echo "workloads not ready after 1200 s"
record stop_worker workloads_ready_again_seconds "$(since "$restarted")"
record stop_worker node_down_total_seconds "$(( restarted - began ))"
