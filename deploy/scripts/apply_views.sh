#!/bin/sh
# create or replace the trino views for the dashboards, run after register-tables so the hive
# tables they reference exist. the sql is piped into the trino cli of the coordinator pod.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl
grep -v '^--' "$REPO_ROOT/schemas/trino/views.sql" \
  | kubectl -n "$NAMESPACE" exec -i deploy/trino-coordinator -c trino-coordinator -- trino --file /dev/stdin
echo "views applied"
