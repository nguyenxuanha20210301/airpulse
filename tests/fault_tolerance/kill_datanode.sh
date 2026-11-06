#!/bin/sh
set -eu
. "$(dirname "$0")/common.sh"
echo "deleting hdfs-datanode-0, check the namenode ui on port 9870 for live datanodes and under replicated blocks"
kubectl -n "$NAMESPACE" delete pod hdfs-datanode-0 --wait=false
watch_pods 120 app=hdfs-datanode
