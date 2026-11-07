#!/bin/sh
# speed layer scaling on the cluster, usage stream_scaling.sh [profile]. a bench copy of YEARS of
# history is replayed without pacing into bench replay topics, then a second speed layer runs the
# QUERIES on that backlog once per executor count of EXECUTORS_LIST and stops when it has read
# everything. the second instance writes to a bench keyspace, bench clean topics and a bench hdfs
# root, so the live views are untouched. every micro batch reads at most MAX_OFFSETS records per
# topic and triggers every TRIGGER, so the backlog goes through batches of the same size. the
# replay rate and the processing rate of every run are appended to
# docs/results/stream_scaling.csv. QUIET=1 scales trino and superset to zero meanwhile.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
profile=${1:-demo}
years=${YEARS:-1}
counts=${EXECUTORS_LIST:-"1 2 4"}
queries=${QUERIES:-clean}
namespace=airpulse
run_job="$repo/deploy/scripts/run_job.sh"
results="$repo/docs/results/stream_scaling.csv"
eval "$("$repo/.venv/bin/python" -c "
from airpulse.common.config import load_config
cfg = load_config('$repo/configs/$profile.yaml', environ={})
print(f'hdfs_url={cfg.hdfs.url}')
print(f'live_root={cfg.hdfs.root}')
print(f'keyspace={cfg.cassandra.keyspace}')
print(f'rf={cfg.cassandra.replication_factor}')
print(f'partitions={cfg.kafka.partitions}')
print(f'weather_raw={cfg.kafka.topics.weather_raw}')
print(f'air_raw={cfg.kafka.topics.air_raw}')
")"
partitions=${PARTITIONS:-$partitions}
suffix=bench
# bench runs on the demo layout, like in deploy/scripts/common.sh.
layout=$profile
if [ "$profile" = "bench" ]; then layout=demo; fi
source_root="$hdfs_url$live_root-bench/y$years"
[ -f "$results" ] || echo "finished_utc,profile,partitions,executors,max_offsets,query,rows,batches,processing_ms,rows_per_second,replay_events,replay_seconds" >"$results"
if [ "${QUIET:-0}" = "1" ]; then
  kubectl -n "$namespace" scale deploy trino-worker trino-coordinator superset superset-worker --replicas=0 >/dev/null
  trap 'kubectl -n "$namespace" scale deploy trino-coordinator superset superset-worker --replicas=1 >/dev/null; kubectl -n "$namespace" scale deploy trino-worker --replicas="${TRINO_WORKERS:-2}" >/dev/null' EXIT
fi

broker=$(kubectl -n "$namespace" get pods -l strimzi.io/broker-role=true -o name | head -1)
topics() {
  kubectl -n "$namespace" exec "$broker" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}

echo "bench keyspace ${keyspace}_bench with replication factor $rf"
{
  sed "s/__REPLICATION_FACTOR__/$rf/; s/EXISTS $keyspace$/EXISTS ${keyspace}_bench/" "$repo/schemas/cassandra/keyspace.cql"
  sed "s/$keyspace\./${keyspace}_bench./g" "$repo/schemas/cassandra/tables.cql"
} | kubectl -n "$namespace" exec -i cassandra-0 -- cqlsh

if ! kubectl -n "$namespace" exec hdfs-namenode-0 -- hdfs dfs -test -d "$source_root/raw" 2>/dev/null; then
  echo "no bench history at $source_root, copying $years years"
  APP_NAME=bench-amplify MODULE=airpulse.ingestion.amplify EXECUTORS=2 \
    sh "$run_job" "$profile" batch-features --years "$years" --output "$source_root" >/dev/null
fi

echo "fresh replay topics with $partitions partitions"
for topic in "$weather_raw.$suffix" "$air_raw.$suffix"; do
  topics --delete --if-exists --topic "$topic" >/dev/null 2>&1 || true
done
while topics --list | grep -q "\.$suffix$"; do sleep 5; done
APP_NAME=bench-replay MODULE=airpulse.ingestion.replay EXECUTORS=1 \
  APP_ENV="AIRPULSE_REPLAY__TOPIC_SUFFIX=$suffix AIRPULSE_KAFKA__PARTITIONS=$partitions" \
  sh "$run_job" "$profile" batch-features --input "$source_root/raw" --no-pacing --start 1900-01-01 --end 2100-12-31 \
  >/dev/null
replay_events=$(kubectl -n "$namespace" logs bench-replay-driver | grep -o "[0-9]* events sent" | awk '{s += $1} END {print s}')
replay_seconds=$(kubectl -n "$namespace" get sparkapplication bench-replay -o json | python3 -c '
import json, sys
from datetime import datetime
s = json.load(sys.stdin)["status"]
parse = lambda t: datetime.fromisoformat(t.replace("Z", "+00:00"))
print(int((parse(s["terminationTime"]) - parse(s["lastSubmissionAttemptTime"])).total_seconds()))')
echo "replayed $replay_events events in $replay_seconds seconds"
kubectl -n "$namespace" delete sparkapplication bench-replay >/dev/null

for count in $counts; do
  app="bench-speed-e$count"
  kubectl -n "$namespace" delete sparkapplication "$app" --ignore-not-found >/dev/null
  # the speed layer of the overlay is the template, its arguments and outputs go to the bench.
  kubectl kustomize "$repo/deploy/manifests/overlays/$layout" | APP="$app" COUNT="$count" \
    QUERIES="$queries" ROOT="$live_root-bench/stream-e$count" KEYSPACE="${keyspace}_bench" \
    SUFFIX="$suffix" MEMORY="${EXECUTOR_MEMORY:-1g}" TRIGGER="${TRIGGER:-10 seconds}" \
    MAX_OFFSETS="${MAX_OFFSETS:-100000}" "$repo/.venv/bin/python" -c '
import json, os, sys
import yaml
items = [doc for doc in yaml.safe_load_all(sys.stdin) if doc]
source = next(i for i in items if i["kind"] == "SparkApplication" and i["metadata"]["name"] == "speed-layer")
spec = source["spec"]
spec["arguments"] += ["--replay", "--starting-offsets", "earliest", "--queries", os.environ["QUERIES"]]
spec["executor"]["instances"] = int(os.environ["COUNT"])
# the clean queries hold no models, a smaller executor than the live one fits more of them.
spec["executor"]["memory"] = os.environ["MEMORY"]
spec["restartPolicy"] = {"type": "Never"}
suffix = os.environ["SUFFIX"]
env = {
    "AIRPULSE_REPLAY__TOPIC_SUFFIX": suffix,
    "AIRPULSE_HDFS__ROOT": os.environ["ROOT"],
    "AIRPULSE_CASSANDRA__KEYSPACE": os.environ["KEYSPACE"],
    "AIRPULSE_KAFKA__TOPICS__WEATHER_CLEAN": f"weather.clean.{suffix}",
    "AIRPULSE_KAFKA__TOPICS__AIR_CLEAN": f"air.clean.{suffix}",
    "AIRPULSE_KAFKA__TOPICS__AIR_ALERTS": f"air.alerts.{suffix}",
    "AIRPULSE_STREAMING__TRIGGER_INTERVAL": os.environ["TRIGGER"],
    "AIRPULSE_STREAMING__MAX_OFFSETS_PER_TRIGGER": os.environ["MAX_OFFSETS"],
}
for role in ("driver", "executor"):
    spec[role].setdefault("env", []).extend({"name": k, "value": v} for k, v in env.items())
app = {"apiVersion": source["apiVersion"], "kind": "SparkApplication",
       "metadata": {"name": os.environ["APP"], "namespace": source["metadata"].get("namespace", "airpulse")},
       "spec": spec}
print(json.dumps(app))' | kubectl apply -f - >/dev/null
  # done once the clean queries have read every replayed event, or after half an hour.
  began=$(date +%s)
  while :; do
    sleep 15
    read_rows=$(kubectl -n "$namespace" logs "$app-driver" 2>/dev/null | grep -o "query clean-[a-z]* batch [0-9]* rows [0-9]*" | awk '{s += $6} END {print s + 0}')
    if [ "$read_rows" -ge "$replay_events" ] || [ $(( $(date +%s) - began )) -gt 1800 ]; then break; fi
  done
  sleep 20
  kubectl -n "$namespace" logs "$app-driver" | grep -o "query [a-z-]* batch .*" | awk \
    -v when="$(date -u +%Y-%m-%dT%H:%M:%SZ)" -v profile="$profile" -v parts="$partitions" -v count="$count" \
    -v cap="${MAX_OFFSETS:-100000}" -v events="$replay_events" -v seconds="$replay_seconds" '
    { rows[$2] += $6; ms[$2] += $8; batches[$2] += 1 }
    END { for (q in rows) printf "%s,%s,%s,%s,%s,%s,%d,%d,%d,%.0f,%s,%s\n", when, profile, parts, count, cap, q, rows[q], batches[q], ms[q], rows[q] * 1000 / ms[q], events, seconds }' \
    | tee -a "$results"
  kubectl -n "$namespace" delete sparkapplication "$app" >/dev/null
done
