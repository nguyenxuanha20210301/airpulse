#!/bin/sh
# stop the nodes of the cluster and keep everything on their volumes, usage cluster_stop.sh <profile>.
# kind gives its nodes the restart policy on-failure and a stopped node exits with an error code,
# so docker would start it again at the next boot next to whatever else the machine runs. the
# policy is cleared first, the cluster then stays down until cluster_start.sh brings it back.
set -eu
. "$(dirname "$0")/common.sh"
need docker

list_nodes() {
  docker ps -a --filter "label=io.x-k8s.kind.cluster=$CLUSTER_NAME" \
    --filter "label=io.x-k8s.kind.role=$1" --format '{{.Names}}' | sort
}
control=$(list_nodes control-plane)
workers=$(list_nodes worker)
if [ -z "$control" ]; then
  echo "no nodes of cluster $CLUSTER_NAME on this machine"
  exit 1
fi
began=$(date +%s)
docker update --restart=no $workers $control >/dev/null
# two minutes let kafka, cassandra and hdfs close their files, the workers go before the api server.
docker stop -t 120 $workers $control >/dev/null
echo "cluster $CLUSTER_NAME stopped in $(( $(date +%s) - began )) s, cluster_start.sh brings it back"
