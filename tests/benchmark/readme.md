# benchmarks

two measurements, the throughput of the stream path under replayed load and the duration of
the batch jobs against the amount of history. results go to `docs/results`.

## stream throughput

1. point `AIRPULSE_KAFKA__BOOTSTRAP_SERVERS` at a broker, through port forwarding or inside a
   pod, and start the replay without pacing so kafka receives events as fast as it can.
2. run `python tests/benchmark/stream_throughput.py --config configs/bench.yaml --seconds 120`
   which samples the end offsets of the replay topics and prints messages per second.
3. repeat with 3, 6 and 12 partitions and 1, 2 and 4 executors for the speed layer and record
   the end to end lag from the cluster dashboard.

## batch duration

`sh tests/benchmark/batch_timing.sh 2023-01-01 2023-12-31` runs raw_to_clean, trends and
features for the range and prints the wall clock time of each. run it for one, five and ten
years and for 1, 2 and 4 executors by editing the overlay.
