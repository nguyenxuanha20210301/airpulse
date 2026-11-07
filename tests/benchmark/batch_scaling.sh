#!/bin/sh
# batch layer scaling on the cluster, usage batch_scaling.sh [profile]. the raw history is copied
# back in time to every size of SIZES (years) under a bench root next to the live layers, then
# raw_to_clean, trends and features run on each size with FIXED_EXECUTORS executors, and on the
# size SWEEP_SIZE, the largest by default, with every count of EXECUTORS_LIST. each run appends
# one line to docs/results/batch_scaling.csv.
# every run gets the same executors, EXECUTOR_MEMORY of heap and EXECUTOR_OVERHEAD outside it,
# and SHUFFLE_PARTITIONS, adaptive execution merges them again when the data is small. the
# profile defaults, twelve partitions and 512m of overhead, are sized for weeks of history and
# get executors killed for memory from one year on.
# with QUIET=1 trino and superset are scaled to zero during the runs, which leaves the memory to
# spark on a single machine, and scaled back at the end. with REUSE=1 a size already copied is
# kept and its row count read back from .cache/bench, which saves the copy when only a job
# changed.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
profile=${1:-demo}
sizes=${SIZES:-"1 5 10"}
counts=${EXECUTORS_LIST:-"1 2 4"}
fixed=${FIXED_EXECUTORS:-2}
jobs=${JOBS:-"batch-raw-to-clean batch-trends batch-features"}
export EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-1536m}
export EXECUTOR_OVERHEAD=${EXECUTOR_OVERHEAD:-1g}
export APP_ENV="AIRPULSE_SPARK__SHUFFLE_PARTITIONS=${SHUFFLE_PARTITIONS:-96}"
namespace=airpulse
run_job="$repo/deploy/scripts/run_job.sh"
results="$repo/docs/results/batch_scaling.csv"
bench_root=$("$repo/.venv/bin/python" -c "
from airpulse.common.config import load_config
cfg = load_config('$repo/configs/$profile.yaml', environ={})
print(f'{cfg.hdfs.url}{cfg.hdfs.root}-bench')")

if [ "${QUIET:-0}" = "1" ]; then
  kubectl -n "$namespace" scale deploy trino-worker trino-coordinator superset superset-worker --replicas=0 >/dev/null
  trap 'kubectl -n "$namespace" scale deploy trino-coordinator superset superset-worker --replicas=1 >/dev/null; kubectl -n "$namespace" scale deploy trino-worker --replicas="${TRINO_WORKERS:-2}" >/dev/null' EXIT
fi
[ -f "$results" ] || echo "finished_utc,profile,years,raw_rows,job,executors,executor_memory,overhead,shuffle_partitions,seconds" >"$results"

# seconds from the submission to the end of the application, read from the operator status.
app_seconds() {
  kubectl -n "$namespace" get sparkapplication "$1" -o json | python3 -c '
import json, sys
from datetime import datetime
status = json.load(sys.stdin)["status"]
parse = lambda t: datetime.fromisoformat(t.replace("Z", "+00:00"))
print(int((parse(status["terminationTime"]) - parse(status["lastSubmissionAttemptTime"])).total_seconds()))'
}

amplify() {
  marker="$repo/.cache/bench/rows_y$1"
  if [ "${REUSE:-0}" = "1" ] && [ -f "$marker" ] \
    && kubectl -n "$namespace" exec hdfs-namenode-0 -c namenode -- hdfs dfs -test -d "$bench_root/y$1/raw" 2>/dev/null; then
    cat "$marker"
    return
  fi
  app="bench-amplify-y$1"
  APP_NAME="$app" MODULE=airpulse.ingestion.amplify EXECUTORS="$fixed" \
    sh "$run_job" "$profile" batch-features --years "$1" --output "$bench_root/y$1" >/dev/null
  mkdir -p "$(dirname "$marker")"
  kubectl -n "$namespace" logs "$app-driver" | grep -o "wrote [0-9]* rows" | awk '{s += $2} END {print s}' | tee "$marker"
  kubectl -n "$namespace" delete sparkapplication "$app" >/dev/null
}

measure() {
  for job in $jobs; do
    app="bench-${job#batch-}-y$1-e$3"
    APP_NAME="$app" EXECUTORS="$3" \
      sh "$run_job" "$profile" "$job" --input "$bench_root/y$1" --output "$bench_root/y$1" >/dev/null
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$profile,$1,$2,$job,$3,$EXECUTOR_MEMORY,$EXECUTOR_OVERHEAD,${SHUFFLE_PARTITIONS:-96},$(app_seconds "$app")" \
      | tee -a "$results"
    kubectl -n "$namespace" delete sparkapplication "$app" >/dev/null
  done
}

sweep=${SWEEP_SIZE:-$(echo "$sizes" | awk '{print $NF}')}
for years in $sizes; do
  rows=$(amplify "$years")
  echo "history of $years years has $rows raw rows"
  measure "$years" "$rows" "$fixed"
  if [ "$years" = "$sweep" ]; then
    for count in $counts; do
      if [ "$count" != "$fixed" ]; then measure "$years" "$rows" "$count"; fi
    done
  fi
done
