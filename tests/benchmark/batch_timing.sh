#!/bin/sh
# time the batch jobs for a date range on the cluster, usage batch_timing.sh start end [profile]
set -eu
start=${1:?start date}
end=${2:?end date}
profile=${3:-demo}
for job in batch-raw-to-clean batch-trends batch-features; do
  sh "$(dirname "$0")/../../deploy/scripts/run_job.sh" "$profile" "$job" --start "$start" --end "$end"
done
