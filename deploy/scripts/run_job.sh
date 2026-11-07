#!/bin/sh
# run one scheduled batch job right now and wait for it, usage run_job.sh <profile> <job> [args]
# the scheduled application template is copied into a one off application named <job>-now.
# benchmarks reuse a template with other settings, APP_NAME renames the copy, MODULE runs another
# module with the same image and mounts, EXECUTORS, EXECUTOR_MEMORY and EXECUTOR_OVERHEAD size the
# executors and APP_ENV adds space separated NAME=value pairs to the driver and executor
# environment, for AIRPULSE_ overrides.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl
job=${2:?job name, for example batch-trends}
shift 2
name=${APP_NAME:-$job-now}
kubectl -n "$NAMESPACE" delete sparkapplication "$name" --ignore-not-found >/dev/null
kubectl -n "$NAMESPACE" get scheduledsparkapplication "$job" -o json \
  | EXTRA="$*" NAME="$name" NS="$NAMESPACE" python3 -c '
import json, os, sys
source = json.load(sys.stdin)
spec = source["spec"]["template"]
if os.environ.get("MODULE"):
    spec["arguments"][0] = os.environ["MODULE"]
spec["arguments"] = spec["arguments"] + [a for a in os.environ["EXTRA"].split() if a]
if os.environ.get("EXECUTORS"):
    spec["executor"]["instances"] = int(os.environ["EXECUTORS"])
if os.environ.get("EXECUTOR_MEMORY"):
    spec["executor"]["memory"] = os.environ["EXECUTOR_MEMORY"]
if os.environ.get("EXECUTOR_OVERHEAD"):
    spec["executor"]["memoryOverhead"] = os.environ["EXECUTOR_OVERHEAD"]
for pair in os.environ.get("APP_ENV", "").split():
    key, value = pair.split("=", 1)
    for role in ("driver", "executor"):
        spec[role].setdefault("env", []).append({"name": key, "value": value})
app = {"apiVersion": source["apiVersion"], "kind": "SparkApplication",
       "metadata": {"name": os.environ["NAME"], "namespace": os.environ["NS"]}, "spec": spec}
print(json.dumps(app))' \
  | kubectl apply -f - >/dev/null
began=$(date +%s)
state=$(wait_app "$name" 7200)
if [ "$state" != "COMPLETED" ]; then
  echo "$job did not complete, state $state"
  kubectl -n "$NAMESPACE" logs "$name-driver" --tail=60 2>/dev/null | grep -E 'airpulse|Traceback|Error|error' | tail -15
  exit 1
fi
echo "$job completed in $(( $(date +%s) - began )) seconds"
