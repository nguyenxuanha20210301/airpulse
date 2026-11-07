#!/bin/sh
# deletes one cassandra node, then writes and reads a probe row at QUORUM, which needs two of the
# three replicas, and reads it once more at ALL, which needs every replica and is expected to be
# refused until the node is back. that is the availability and consistency trade of the CAP
# theorem in practice. the probe keyspace has the replication factor of the profile.
set -eu
. "$(dirname "$0")/common.sh"
victim=${1:-cassandra-1}
probe=$(kubectl -n "$NAMESPACE" get pods -l app=cassandra -o name | cut -d/ -f2 | grep -v "^$victim$" | head -1)
rf=$(cfg_value cassandra.replication_factor)
cql() {
  kubectl -n "$NAMESPACE" exec "$probe" -- cqlsh -e "$1" 2>&1 || true
}
up_nodes() {
  kubectl -n "$NAMESPACE" exec "$probe" -- nodetool status 2>/dev/null | grep -c "^UN" || true
}
node_down() {
  [ "$(up_nodes)" -lt "$expected" ]
}
all_up() {
  [ "$(up_nodes)" -eq "$expected" ]
}
outcome() {
  if echo "$1" | grep -q -i -E "unavailable|cannot achieve|error"; then echo refused; else echo ok; fi
}

cql "CREATE KEYSPACE IF NOT EXISTS ft_probe WITH replication = {'class': 'SimpleStrategy', 'replication_factor': $rf};"
cql "CREATE TABLE IF NOT EXISTS ft_probe.probe (id int PRIMARY KEY, written timestamp);"
expected=$(up_nodes)
echo "deleting $victim, $expected nodes up, probing through $probe"
kubectl -n "$NAMESPACE" delete pod "$victim" --wait=false >/dev/null
began=$(now)
seen_down=$(wait_until 300 node_down) || true
write=$(cql "CONSISTENCY QUORUM; INSERT INTO ft_probe.probe (id, written) VALUES (1, toTimestamp(now()));")
read_quorum=$(cql "CONSISTENCY QUORUM; SELECT id, written FROM ft_probe.probe WHERE id = 1;")
read_all=$(cql "CONSISTENCY ALL; SELECT id, written FROM ft_probe.probe WHERE id = 1;")
wait_until 900 all_up >/dev/null || echo "not every node up after 900 s"
back=$(since "$began")
record kill_cassandra seen_down_seconds "$seen_down"
record kill_cassandra quorum_write_while_down "$(outcome "$write")"
record kill_cassandra quorum_read_while_down "$(outcome "$read_quorum")"
record kill_cassandra all_read_while_down "$(outcome "$read_all")"
record kill_cassandra all_nodes_up_seconds "$back"
record kill_cassandra all_read_after_recovery "$(outcome "$(cql "CONSISTENCY ALL; SELECT id FROM ft_probe.probe WHERE id = 1;")")"
