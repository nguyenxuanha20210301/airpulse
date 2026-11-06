#!/bin/sh
set -eu
. "$(dirname "$0")/common.sh"
echo "deleting the speed layer driver, the operator restarts it from the checkpoint"
kubectl -n "$NAMESPACE" delete pod speed-layer-driver --wait=false
watch_pods 180 sparkoperator.k8s.io/app-name=speed-layer
