#!/bin/sh
# deletes one kafka broker while a producer writes with acks all, then checks that every
# acknowledged record is in the topic. the drill topic has three replicas and min.insync.replicas
# two, so losing one broker must neither stop nor lose writes. the kafka tools run in a pod of
# their own, a second jvm inside a broker container could push it over its memory limit.
set -eu
. "$(dirname "$0")/common.sh"
topic=ft.broker
records=${RECORDS:-300000}
rate=${RATE:-3000}
bootstrap=$(cfg_value kafka.bootstrap_servers)
victim=$(kubectl -n "$NAMESPACE" get pods -l strimzi.io/broker-role=true -o name | sort | tail -1 | cut -d/ -f2)
image=$(kubectl -n "$NAMESPACE" get pod "$victim" -o jsonpath='{.spec.containers[0].image}')
bin=/opt/kafka/bin
kubectl -n "$NAMESPACE" run ft-kafka-tools --image="$image" --restart=Never --command -- sleep 3600 >/dev/null
trap 'kubectl -n "$NAMESPACE" delete pod ft-kafka-tools --wait=false >/dev/null 2>&1' EXIT
kubectl -n "$NAMESPACE" wait pod/ft-kafka-tools --for=condition=Ready --timeout=300s >/dev/null
kafka() {
  kubectl -n "$NAMESPACE" exec ft-kafka-tools -- "$@"
}
end_offsets() {
  kafka $bin/kafka-get-offsets.sh --bootstrap-server "$bootstrap" --topic "$topic" --time -1 \
    | awk -F: '{s += $3} END {print s + 0}'
}
under_replicated() {
  kafka $bin/kafka-topics.sh --bootstrap-server "$bootstrap" --describe --under-replicated-partitions \
    | grep -c "Partition:" || true
}
no_under_replicated() {
  [ "$(under_replicated)" -eq 0 ]
}

kafka $bin/kafka-topics.sh --bootstrap-server "$bootstrap" --create --if-not-exists --topic "$topic" \
  --partitions 6 --replication-factor 3 --config min.insync.replicas=2 >/dev/null
before=$(end_offsets)
log="$REPO_ROOT/.cache/ft_broker_producer.log"
mkdir -p "$(dirname "$log")"
kafka $bin/kafka-producer-perf-test.sh --topic "$topic" --num-records "$records" --record-size 200 \
  --throughput "$rate" --producer-props bootstrap.servers="$bootstrap" acks=all enable.idempotence=true \
  >"$log" 2>&1 &
writer=$!
sleep 20
uid=$(pod_uid "$victim")
echo "deleting $victim while $records records are written at $rate per second"
kubectl -n "$NAMESPACE" delete pod "$victim" --wait=false >/dev/null
began=$(now)
peak=0
while ! replaced_and_ready "$victim" "$uid" && [ "$(since "$began")" -lt 900 ]; do
  count=$(under_replicated)
  if [ "$count" -gt "$peak" ]; then peak=$count; fi
  sleep 5
done
ready=$(since "$began")
wait_until 900 no_under_replicated >/dev/null || echo "partitions still under replicated after 900 s"
caught_up=$(since "$began")
wait "$writer" || true
acknowledged=$(grep -o "^[0-9]* records sent" "$log" | tail -1 | awk '{print $1}')
retried=$(grep -c "retrying" "$log" || true)
stored=$(( $(end_offsets) - before ))
record kill_broker broker_back_ready_seconds "$ready"
record kill_broker under_replicated_partitions_peak "$peak"
record kill_broker in_sync_again_seconds "$caught_up"
record kill_broker records_acknowledged "${acknowledged:-0}"
record kill_broker records_in_topic "$stored"
record kill_broker producer_retries_on_leader_change "$retried"
tail -1 "$log"
