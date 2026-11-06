#!/bin/sh
set -eu
. "$(dirname "$0")/common.sh"
echo "deleting cassandra-1, reads and writes at LOCAL_QUORUM keep working with two nodes"
kubectl -n "$NAMESPACE" delete pod cassandra-1 --wait=false
sleep 15
kubectl -n "$NAMESPACE" exec cassandra-0 -- nodetool status || true
watch_pods 180 app=cassandra
kubectl -n "$NAMESPACE" exec cassandra-0 -- nodetool status
