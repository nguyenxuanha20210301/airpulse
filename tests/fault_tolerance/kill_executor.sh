#!/bin/sh
set -eu
. "$(dirname "$0")/common.sh"
pod=$(kubectl -n "$NAMESPACE" get pods -l spark-role=executor,sparkoperator.k8s.io/app-name=speed-layer -o name | head -1)
echo "deleting $pod, the driver requests a replacement and retries the micro batch"
kubectl -n "$NAMESPACE" delete "$pod" --wait=false
watch_pods 90 sparkoperator.k8s.io/app-name=speed-layer
