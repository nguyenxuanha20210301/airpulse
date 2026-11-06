#!/bin/sh
# delete the kind cluster and everything in it, data on the volumes is lost.
set -eu
. "$(dirname "$0")/common.sh"
need kind
kind delete cluster --name "$CLUSTER_NAME"
