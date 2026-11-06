#!/bin/sh
# stop or start one kind worker node, usage stop_worker.sh [stop|start] [node name]
set -eu
. "$(dirname "$0")/common.sh"
action=${1:-stop}
node=${2:-airpulse-worker2}
if [ "$action" = "stop" ]; then
  echo "stopping $node, pods there are rescheduled after the node becomes not ready"
  docker stop "$node"
  sleep 60
  kubectl get nodes
  kubectl -n "$NAMESPACE" get pods -o wide
else
  echo "starting $node"
  docker start "$node"
  sleep 30
  kubectl get nodes
fi
