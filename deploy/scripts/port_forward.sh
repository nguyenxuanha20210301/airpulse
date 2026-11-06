#!/bin/sh
# expose the user facing services on localhost, stop with ctrl c.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl

forward() {
  kubectl -n "$NAMESPACE" port-forward "$1" "$2" >/dev/null 2>&1 &
  echo "$3 on http://localhost:${2%%:*}"
}

forward svc/monitoring-grafana 3000:80 "grafana, admin and admin"
forward svc/trino 8080:8080 "trino"
forward svc/hdfs-namenode 9870:9870 "hdfs namenode"
forward svc/monitoring-kube-prometheus-prometheus 9090:9090 "prometheus"
if kubectl -n "$NAMESPACE" get svc superset >/dev/null 2>&1; then
  forward svc/superset 8088:8088 "superset, admin and admin"
fi
echo "press ctrl c to stop"
wait
