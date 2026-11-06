# shared helpers for the drills.
NAMESPACE=airpulse
watch_pods() {
  echo "watching pods for $1 seconds"
  end=$(( $(date +%s) + $1 ))
  while [ "$(date +%s)" -lt "$end" ]; do
    kubectl -n "$NAMESPACE" get pods -l "$2" --no-headers
    echo
    sleep 10
  done
}
