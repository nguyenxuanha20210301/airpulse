#!/bin/sh
# run one scheduled batch job right now and wait for it, usage run_job.sh <profile> <job> [args]
# the scheduled application template is copied into a one off application named <job>-now.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl
job=${2:?job name, for example batch-trends}
shift 2
name="$job-now"
kubectl -n "$NAMESPACE" delete sparkapplication "$name" --ignore-not-found >/dev/null
kubectl -n "$NAMESPACE" get scheduledsparkapplication "$job" -o json \
  | EXTRA="$*" NAME="$name" NS="$NAMESPACE" python3 -c '
import json, os, sys
source = json.load(sys.stdin)
spec = source["spec"]["template"]
spec["arguments"] = spec["arguments"] + [a for a in os.environ["EXTRA"].split() if a]
app = {"apiVersion": source["apiVersion"], "kind": "SparkApplication",
       "metadata": {"name": os.environ["NAME"], "namespace": os.environ["NS"]}, "spec": spec}
print(json.dumps(app))' \
  | kubectl apply -f - >/dev/null
began=$(date +%s)
state=""
while [ "$state" != "COMPLETED" ] && [ "$state" != "FAILED" ]; do
  sleep 10
  state=$(kubectl -n "$NAMESPACE" get sparkapplication "$name" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
  if [ $(( $(date +%s) - began )) -gt 7200 ]; then state="TIMEOUT"; fi
done
if [ "$state" != "COMPLETED" ]; then
  echo "$job did not complete, state $state"
  kubectl -n "$NAMESPACE" logs "$name-driver" --tail=60 2>/dev/null | grep -E 'airpulse|Traceback|Error|error' | tail -15
  exit 1
fi
echo "$job completed in $(( $(date +%s) - began )) seconds"
