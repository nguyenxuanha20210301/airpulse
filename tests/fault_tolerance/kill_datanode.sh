#!/bin/sh
# deletes one hdfs datanode and counts the whole raw air layer through trino while it is away.
# every block has a second copy on another node, so the count must equal the one taken before.
# the hdfs client skips the dead node by itself, the namenode only declares it dead after about
# ten minutes, which a pod restart never reaches.
set -eu
. "$(dirname "$0")/common.sh"
victim=${1:-hdfs-datanode-1}
query="SELECT count(*) FROM hive.airpulse.raw_air_hourly"
trino() {
  kubectl -n "$NAMESPACE" exec deploy/trino-coordinator -c trino-coordinator -- \
    trino --output-format=TSV --execute "$1" 2>/dev/null | tail -1
}
fsck_under_replicated() {
  kubectl -n "$NAMESPACE" exec hdfs-namenode-0 -c namenode -- hdfs fsck / 2>/dev/null \
    | awk '/Under-replicated blocks/ {print $3}'
}
gone() {
  [ "$(pod_uid "$victim")" != "$1" ]
}

before=$(trino "$query")
uid=$(pod_uid "$victim")
echo "deleting $victim, the raw layer has $before air rows"
kubectl -n "$NAMESPACE" delete pod "$victim" --wait=false >/dev/null
began=$(now)
wait_until 120 gone "$uid" >/dev/null || true
read_began=$(now)
during=$(trino "$query")
read_seconds=$(( $(now) - read_began ))
wait_until 600 replaced_and_ready "$victim" "$uid" >/dev/null || echo "$victim not ready after 600 s"
ready=$(since "$began")
record kill_datanode rows_before "$before"
record kill_datanode rows_read_while_down "$during"
record kill_datanode read_while_down_seconds "$read_seconds"
record kill_datanode datanode_back_ready_seconds "$ready"
record kill_datanode under_replicated_blocks_after "$(fsck_under_replicated)"
