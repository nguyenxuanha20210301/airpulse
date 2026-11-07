#!/bin/sh
# trino with two workers and with one on the same aggregation, usage trino_scaling.sh [profile].
# the clean air layer of the bench history of YEARS years becomes an external table in the schema
# airpulse_bench, the query runs REPEAT times per worker count of WORKERS_LIST, and the elapsed,
# queued and planning times reported by trino are appended to docs/results/trino_scaling.csv. the
# worker count of the profile is restored at the end.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
profile=${1:-demo}
years=${YEARS:-10}
repeat=${REPEAT:-3}
counts=${WORKERS_LIST:-"2 1"}
namespace=airpulse
results="$repo/docs/results/trino_scaling.csv"
eval "$("$repo/.venv/bin/python" -c "
from airpulse.common.config import load_config
cfg = load_config('$repo/configs/$profile.yaml', environ={})
print(f'hdfs_url={cfg.hdfs.url}')
print(f'live_root={cfg.hdfs.root}')
print(f'catalog={cfg.trino.hive_catalog}')
print(f'schema={cfg.trino.schema_name}')
")"
table="clean_air_y$years"
location="$hdfs_url$live_root-bench/y$years/clean/air_hourly"
original=$(kubectl -n "$namespace" get deploy trino-worker -o jsonpath='{.spec.replicas}')
trap 'kubectl -n "$namespace" scale deploy trino-worker --replicas="$original" >/dev/null' EXIT
[ -f "$results" ] || echo "finished_utc,years,rows,workers,run,elapsed_ms,queued_ms,planning_ms" >"$results"

trino() {
  kubectl -n "$namespace" exec deploy/trino-coordinator -c trino-coordinator -- \
    trino --output-format=TSV --execute "$1" 2>/dev/null
}
active_workers() {
  trino "SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false AND state = 'active'"
}

# the bench table takes the columns of the live clean table, dt last as the partition key.
columns=$(trino "DESCRIBE $catalog.$schema.clean_air_hourly" | awk -F'\t' '{printf "%s%s %s", sep, $1, $2; sep = ", "}')
trino "CREATE SCHEMA IF NOT EXISTS $catalog.${schema}_bench" >/dev/null
trino "DROP TABLE IF EXISTS $catalog.${schema}_bench.$table" >/dev/null
trino "CREATE TABLE $catalog.${schema}_bench.$table ($columns) WITH (external_location = '$location', format = 'PARQUET', partitioned_by = ARRAY['dt'])" >/dev/null
trino "CALL $catalog.system.sync_partition_metadata('${schema}_bench', '$table', 'ADD')" >/dev/null
rows=$(trino "SELECT count(*) FROM $catalog.${schema}_bench.$table")
echo "bench table $table has $rows rows"

for workers in $counts; do
  kubectl -n "$namespace" scale deploy trino-worker --replicas="$workers" >/dev/null
  until [ "$(active_workers)" = "$workers" ]; do sleep 5; done
  run=1
  while [ "$run" -le "$repeat" ]; do
    tag="bench-$workers-$run-$(date +%s)"
    trino "SELECT '$tag' AS tag, location_id, month(event_ts_utc) AS month, avg(pm2_5_ugm3),
      approx_percentile(pm2_5_ugm3, 0.95), max(aqi_computed), count(*)
      FROM $catalog.${schema}_bench.$table GROUP BY 2, 3" >/dev/null
    stats=$(trino "SELECT to_milliseconds(\"end\" - created), queued_time_ms, planning_time_ms
      FROM system.runtime.queries WHERE query LIKE '%''$tag''%' AND query NOT LIKE '%system.runtime%' AND state = 'FINISHED'")
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$years,$rows,$workers,$run,$(echo "$stats" | tr '\t' ',')" | tee -a "$results"
    run=$((run + 1))
  done
done
