#!/bin/sh
# end to end demo, backfill a short range of history, replay it into kafka and show the tables.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl

echo "one off backfill of the last two months"
kubectl -n "$NAMESPACE" delete sparkapplication backfill --ignore-not-found
kubectl -n "$NAMESPACE" apply -f "$REPO_ROOT/deploy/manifests/base/spark-oneoff.yaml"
kubectl -n "$NAMESPACE" wait sparkapplication/backfill --for=jsonpath='{.status.applicationState.state}'=COMPLETED --timeout=60m

echo "batch jobs once, in order"
for job in batch-raw-to-clean batch-trends batch-correlations batch-mapreduce batch-features ml-train register-tables; do
  kubectl -n "$NAMESPACE" get scheduledsparkapplication "$job" -o json \
    | python3 -c 'import json,sys; s=json.load(sys.stdin); print(json.dumps({"apiVersion": s["apiVersion"], "kind": "SparkApplication", "metadata": {"name": s["metadata"]["name"] + "-now", "namespace": s["metadata"]["namespace"]}, "spec": s["spec"]["template"]}))' \
    | kubectl apply -f -
  kubectl -n "$NAMESPACE" wait "sparkapplication/$job-now" --for=jsonpath='{.status.applicationState.state}'=COMPLETED --timeout=60m
  kubectl -n "$NAMESPACE" delete "sparkapplication/$job-now" --ignore-not-found
done

echo "replay one month of history at speed into the replay topics"
kubectl -n "$NAMESPACE" apply -f "$REPO_ROOT/deploy/manifests/base/spark-oneoff.yaml"
kubectl -n "$NAMESPACE" get sparkapplications
