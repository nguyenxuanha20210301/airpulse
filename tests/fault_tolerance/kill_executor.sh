#!/bin/sh
# deletes one executor of the speed layer. the driver asks for a replacement and retries the tasks
# it lost, the drill measures the time until the executor count is complete again and checks that
# a producer cycle started afterwards is still processed, without a restart of the driver.
set -eu
. "$(dirname "$0")/common.sh"
selector=spark-role=executor,sparkoperator.k8s.io/app-name=speed-layer
running() {
  kubectl -n "$NAMESPACE" get pods -l "$selector" -o jsonpath='{range .items[*]}{.status.phase} {.metadata.deletionTimestamp}{"\n"}{end}' \
    | grep -c "^Running $" || true
}
complete() {
  [ "$(running)" -ge "$expected" ] && ! kubectl -n "$NAMESPACE" get "$victim" >/dev/null 2>&1
}
processed_since() {
  progress_since "$1" | grep -q "rows"
}
restarts() {
  kubectl -n "$NAMESPACE" get pod speed-layer-driver -o jsonpath='{.status.containerStatuses[0].restartCount}'
}

expected=$(running)
victim=$(kubectl -n "$NAMESPACE" get pods -l "$selector" -o name | head -1)
driver_restarts=$(restarts)
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "deleting $victim, $expected executors running"
kubectl -n "$NAMESPACE" delete "$victim" --wait=false >/dev/null
began=$(now)
back=$(wait_until 600 complete) || true
kick_producer
wait_until 900 processed_since "$started_at" >/dev/null || echo "no batch with data after 900 s"
processed=$(since "$began")
record kill_executor executors_complete_seconds "$back"
record kill_executor first_batch_with_data_seconds "$processed"
record kill_executor driver_restarts_during_drill "$(( $(restarts) - driver_restarts ))"
