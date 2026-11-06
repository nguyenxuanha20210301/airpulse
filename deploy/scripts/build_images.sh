#!/bin/sh
# build the two images and load them into the kind nodes, there is no registry.
set -eu
. "$(dirname "$0")/common.sh"
need docker
need kind

docker build -f "$REPO_ROOT/deploy/images/app/dockerfile" -t "$APP_IMAGE" "$REPO_ROOT"
docker build -f "$REPO_ROOT/deploy/images/spark/dockerfile" -t "$SPARK_IMAGE" "$REPO_ROOT"
kind load docker-image "$APP_IMAGE" "$SPARK_IMAGE" --name "$CLUSTER_NAME"
