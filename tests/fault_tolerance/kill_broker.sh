#!/bin/sh
set -eu
. "$(dirname "$0")/common.sh"
pod=$(kubectl -n "$NAMESPACE" get pods -l strimzi.io/broker-role=true -o name | head -1)
echo "deleting $pod, watch under replicated partitions on the cluster dashboard"
kubectl -n "$NAMESPACE" delete "$pod" --wait=false
watch_pods 120 strimzi.io/broker-role=true
