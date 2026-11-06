#!/bin/sh
# time the batch jobs for a date range on the cluster, usage batch_timing.sh start end [profile]
set -eu
start=${1:?start date}
end=${2:?end date}
profile=${3:-demo}
ns=airpulse
for job in batch-raw-to-clean batch-trends batch-features; do
  name="$job-bench"
  kubectl -n "$ns" delete sparkapplication "$name" --ignore-not-found >/dev/null
  kubectl -n "$ns" get scheduledsparkapplication "$job" -o json \
    | python3 -c "import json,sys; s=json.load(sys.stdin); t=s['spec']['template']; t['arguments'] = t['arguments'][:3] + ['--start', '$start', '--end', '$end']; t['arguments'][2] = '/etc/airpulse/$profile.yaml'; print(json.dumps({'apiVersion': s['apiVersion'], 'kind': 'SparkApplication', 'metadata': {'name': '$name', 'namespace': '$ns'}, 'spec': t}))" \
    | kubectl apply -f - >/dev/null
  began=$(date +%s)
  kubectl -n "$ns" wait "sparkapplication/$name" --for=jsonpath='{.status.applicationState.state}'=COMPLETED --timeout=120m >/dev/null
  echo "$job $start $end took $(( $(date +%s) - began )) seconds"
  kubectl -n "$ns" delete sparkapplication "$name" >/dev/null
done
