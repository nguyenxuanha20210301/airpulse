#!/bin/sh
# load dashboards/superset/assets into the running superset. objects are matched by uuid and
# overwritten, so the repo stays the source of the dashboards. the charts read the trino views,
# run this after apply_views.sh or they stay empty until the views exist.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl

source_dir="$REPO_ROOT/dashboards/superset/assets"
target=/tmp/airpulse_assets
log="$CACHE_DIR/superset_import.log"

kubectl -n "$NAMESPACE" rollout status deploy/superset --timeout=15m >/dev/null
kubectl -n "$NAMESPACE" exec deploy/superset -c superset -- rm -rf "$target"
tar -C "$source_dir" -cf - . \
  | kubectl -n "$NAMESPACE" exec -i deploy/superset -c superset -- sh -c "mkdir -p $target && tar -C $target -xf -"
if ! kubectl -n "$NAMESPACE" exec deploy/superset -c superset -- \
  superset import-directory --overwrite "$target" >"$log" 2>&1; then
  tail -20 "$log"
  exit 1
fi
# the directory loader logs validation errors instead of failing, so the log is checked too.
if grep -q "An error occurred" "$log"; then
  grep "An error occurred" "$log"
  exit 1
fi
echo "superset dashboards imported"
