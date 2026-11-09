#!/bin/sh
# run the batch layer once, each job after the previous one has finished, then refresh the trino
# views. it does what the suspended nightly schedules would do, without their risk of starting
# every missed run at once, usage run_batch.sh <profile>.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl
for job in batch-raw-to-clean batch-trends batch-correlations batch-mapreduce batch-features ml-train register-tables; do
  sh "$REPO_ROOT/deploy/scripts/run_job.sh" "$PROFILE" "$job"
done
sh "$REPO_ROOT/deploy/scripts/apply_views.sh" "$PROFILE"
