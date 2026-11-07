#!/bin/sh
# create the local kind cluster for a profile and the namespace every component lives in.
set -eu
. "$(dirname "$0")/common.sh"
need kind
need kubectl

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "cluster $CLUSTER_NAME already exists"
else
  kind create cluster --name "$CLUSTER_NAME" --config "$REPO_ROOT/deploy/cluster/kind-$LAYOUT.yaml" --wait 120s
fi
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"
kubectl get nodes
