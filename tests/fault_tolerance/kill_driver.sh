#!/bin/sh
# deletes the driver of the speed layer. the operator submits the application again and the
# queries resume from the offsets kept in their checkpoints on hdfs, so the batch numbers go on
# from where they stopped instead of starting over at zero.
set -eu
. "$(dirname "$0")/common.sh"
last_batch() {
  kubectl -n "$NAMESPACE" logs speed-layer-driver 2>/dev/null \
    | grep -o "query clean-air batch [0-9]*" | tail -1 | awk '{print $4}'
}
replaced_running() {
  current_uid=$(pod_uid speed-layer-driver)
  [ -n "$current_uid" ] && [ "$current_uid" != "$1" ] \
    && [ "$(kubectl -n "$NAMESPACE" get pod speed-layer-driver -o jsonpath='{.status.phase}')" = "Running" ]
}
queries_running() {
  kubectl -n "$NAMESPACE" logs speed-layer-driver 2>/dev/null | grep -q "queries running"
}
batch_after() {
  [ -n "$(last_batch)" ]
}

# a cycle before the drill gives the clean query a batch number to compare with.
kick_producer
wait_until 900 batch_after >/dev/null || true
before=$(last_batch)
uid=$(pod_uid speed-layer-driver)
echo "deleting the speed layer driver, clean-air is at batch $before"
kubectl -n "$NAMESPACE" delete pod speed-layer-driver --wait=false >/dev/null
began=$(now)
running=$(wait_until 900 replaced_running "$uid") || true
wait_until 900 queries_running >/dev/null || echo "queries not running after 900 s"
started=$(since "$began")
kick_producer
wait_until 900 batch_after >/dev/null || echo "no batch with data after 900 s"
processed=$(since "$began")
record kill_driver driver_running_again_seconds "$running"
record kill_driver queries_running_again_seconds "$started"
record kill_driver first_batch_with_data_seconds "$processed"
record kill_driver clean_air_batch_before "$before"
record kill_driver clean_air_batch_after "$(last_batch)"
