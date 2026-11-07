# shared helpers for the drills. every drill appends its measurements to
# docs/results/fault_tolerance.csv as one line per measure.
NAMESPACE=airpulse
FT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$FT_DIR/../.." && pwd)
RESULTS="$REPO_ROOT/docs/results/fault_tolerance.csv"
mkdir -p "$(dirname "$RESULTS")"
[ -f "$RESULTS" ] || echo "time_utc,drill,measure,value" >"$RESULTS"

now() {
  date +%s
}

record() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$1,$2,$3" | tee -a "$RESULTS"
}

# run a command every two seconds until it succeeds and print the seconds it took. the first
# argument is the timeout in seconds, after it the function prints timeout and fails.
wait_until() {
  limit=$1
  shift
  waited_from=$(now)
  until "$@" >/dev/null 2>&1; do
    if [ $(( $(now) - waited_from )) -gt "$limit" ]; then
      echo timeout
      return 1
    fi
    sleep 2
  done
  echo $(( $(now) - waited_from ))
}

# seconds since a start time, the value recorded once a wait is over.
since() {
  echo $(( $(now) - $1 ))
}

# every stateful set, deployment and kafka broker of the namespace has all its replicas ready.
workloads_ready() {
  kubectl -n "$NAMESPACE" get statefulsets,deployments \
    -o jsonpath='{range .items[*]}{.status.readyReplicas} {.spec.replicas}{"\n"}{end}' \
    | awk '($1 + 0) != ($2 + 0) {bad = 1} END {exit bad}' || return 1
  ! kubectl -n "$NAMESPACE" get pods -l strimzi.io/broker-role=true \
    -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -v -q True
}

pod_uid() {
  kubectl -n "$NAMESPACE" get pod "$1" -o jsonpath='{.metadata.uid}' 2>/dev/null || true
}

# true once a pod of that name exists with another uid than the deleted one and is ready. shell
# functions share the variables of the script, so the current uid gets a name of its own.
replaced_and_ready() {
  current_uid=$(pod_uid "$1")
  [ -n "$current_uid" ] && [ "$current_uid" != "$2" ] \
    && [ "$(kubectl -n "$NAMESPACE" get pod "$1" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')" = "True" ]
}

# the producer polls the apis as soon as it starts, a restart pushes a fresh cycle through the
# speed layer without waiting for the next scheduled one.
kick_producer() {
  kubectl -n "$NAMESPACE" rollout restart deploy/producer >/dev/null
}

# log lines of the speed layer progress listener since a given time, rfc3339.
progress_since() {
  kubectl -n "$NAMESPACE" logs speed-layer-driver --since-time="$1" 2>/dev/null | grep -o "query [a-z-]* batch [0-9]* rows [0-9]*" || true
}

PROFILE=${PROFILE:-demo}

# one value of the profile config, for example cfg_value kafka.bootstrap_servers.
cfg_value() {
  "$REPO_ROOT/.venv/bin/python" -c "
from airpulse.common.config import load_config
value = load_config('$REPO_ROOT/configs/$PROFILE.yaml', environ={})
for part in '$1'.split('.'):
    value = getattr(value, part)
print(value)"
}
