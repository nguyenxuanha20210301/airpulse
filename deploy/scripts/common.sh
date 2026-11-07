# shared variables for the deploy scripts, sourced by each of them.
# the repo root is derived from the script location so nothing depends on the working directory.
REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PROFILE=${1:-dev}
NAMESPACE=airpulse
# the name can be overridden, so a demo cluster can live next to a stopped dev one.
CLUSTER_NAME=${CLUSTER_NAME:-airpulse}
APP_IMAGE=airpulse-app:local
SPARK_IMAGE=airpulse-spark:local
CACHE_DIR="$REPO_ROOT/.cache/deploy"
mkdir -p "$CACHE_DIR"

case "$PROFILE" in
  dev|demo|bench) ;;
  *) echo "unknown profile $PROFILE, use dev, demo or bench"; exit 1 ;;
esac

# bench runs on the demo cluster layout.
if [ "$PROFILE" = "bench" ]; then
  LAYOUT=demo
else
  LAYOUT=$PROFILE
fi

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "$1 is required, see docs/runbook.md"; exit 1; }
}

# wait for a spark application to finish and print its final state, the second argument is the
# timeout in seconds.
wait_app() {
  began=$(date +%s)
  state=""
  while [ "$state" != "COMPLETED" ] && [ "$state" != "FAILED" ]; do
    sleep 10
    state=$(kubectl -n "$NAMESPACE" get sparkapplication "$1" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
    if [ $(( $(date +%s) - began )) -gt "${2:-7200}" ]; then state="TIMEOUT"; fi
  done
  echo "$state"
}
