#!/bin/sh
# end to end demo, backfill a short range of history, replay it into kafka and show the tables.
# the one off templates name a config file, it is swapped for the one of the profile. the replay
# runs REPLAY_SPEED data hours per wall clock hour, 3600 by default.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl

echo "one off backfill of the last two months"
start=$(date -u -d "$(date -u +%Y-%m-01) -2 months" +%Y-%m-%d)
end=$(date -u -d "yesterday" +%Y-%m-%d)
kubectl -n "$NAMESPACE" delete sparkapplication backfill --ignore-not-found
# the template carries example dates, the first document is the backfill application.
awk 'BEGIN { RS = "---\n" } NR == 1' "$REPO_ROOT/deploy/manifests/base/spark-oneoff.yaml" \
  | sed "s/2024-01-01/$start/; s/2024-12-31/$end/; s|/etc/airpulse/[a-z]*\.yaml|/etc/airpulse/$PROFILE.yaml|" \
  | kubectl -n "$NAMESPACE" apply -f -
state=$(wait_app backfill 3600)
if [ "$state" != "COMPLETED" ]; then
  echo "backfill did not complete, state $state"
  exit 1
fi

echo "batch jobs once, in order"
for job in batch-raw-to-clean batch-trends batch-correlations batch-mapreduce batch-features ml-train register-tables; do
  sh "$REPO_ROOT/deploy/scripts/run_job.sh" "$PROFILE" "$job"
done
sh "$REPO_ROOT/deploy/scripts/apply_views.sh" "$PROFILE"
if kubectl -n "$NAMESPACE" get deploy superset >/dev/null 2>&1; then
  sh "$REPO_ROOT/deploy/scripts/import_dashboards.sh" "$PROFILE"
fi

echo "replay the first backfilled month at speed into the replay topics"
replay_end=$(date -u -d "$start +1 month -1 day" +%Y-%m-%d)
kubectl -n "$NAMESPACE" delete sparkapplication replay --ignore-not-found
awk 'BEGIN { RS = "---\n" } NR == 2' "$REPO_ROOT/deploy/manifests/base/spark-oneoff.yaml" \
  | sed "s|/etc/airpulse/[a-z]*\.yaml|/etc/airpulse/$PROFILE.yaml|; s|- --shift-to-now|- --shift-to-now\n    - --speed\n    - \"${REPLAY_SPEED:-3600}\"\n    - --start\n    - \"$start\"\n    - --end\n    - \"$replay_end\"|" \
  | kubectl -n "$NAMESPACE" apply -f -
kubectl -n "$NAMESPACE" get sparkapplications
