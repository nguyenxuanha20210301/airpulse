# benchmarks

two measurements on the demo cluster, the batch layer against the amount of history and the
number of executors, and the speed layer against the number of executors. the numbers of the
last campaign are in `docs/results`.

## data volume

the free Open-Meteo quota of about ten thousand weighted calls per day does not allow fetching
decades of hourly history for 63 points in one sitting, so the benchmarks work on copies of the
backfilled weeks moved back in time by `python -m airpulse.ingestion.amplify`. each copy shifts
every event by a whole number of spans and gets new event ids, so the batch deduplication keeps
all copies and the copies tile the calendar without overlap. the copies live under a bench root
next to the live layers, `<hdfs root>-bench/y<years>`, and never mix with real data.

## batch scaling

```bash
QUIET=1 sh tests/benchmark/batch_scaling.sh demo
```

copies 1, 5 and 10 years of history, runs `raw_to_clean`, `trends` and `features` on each size
with two executors, and on the largest size with one and four executors as well. every run adds
a line to `docs/results/batch_scaling.csv` with the raw row count and the seconds between the
submission and the end of the application. `SIZES`, `EXECUTORS_LIST`, `FIXED_EXECUTORS` and
`JOBS` change the plan, `QUIET=1` scales trino and superset to zero during the runs.

## stream scaling

```bash
QUIET=1 sh tests/benchmark/stream_scaling.sh demo
```

replays one year of bench history without pacing into the topics `weather.raw.bench` and
`air.raw.bench`, then starts a second speed layer with the clean queries on that backlog, once
per executor count, and reads the rows and durations of every micro batch from its progress log.
the second instance writes to the keyspace `airpulse_bench`, the topics `*.clean.bench` and its
own hdfs root, the live views are not touched. it triggers every `TRIGGER` (10 seconds) and a
micro batch reads at most `MAX_OFFSETS` records per topic (100000), so the backlog goes through
batches of the same size whatever the executor count. results go to
`docs/results/stream_scaling.csv` with the replay rate. `YEARS`, `EXECUTORS_LIST`, `PARTITIONS`
and `QUERIES` change the plan.

## trino scaling

```bash
sh tests/benchmark/trino_scaling.sh demo
```

maps the clean air layer of the ten year bench history as an external table in the schema
`airpulse_bench` and runs one aggregation `REPEAT` times with two workers and with one, results
go to `docs/results/trino_scaling.csv`. `WORKERS_LIST` sets the order, the worker count of the
profile is restored at the end.

## other tools

`stream_throughput.py` samples the end offsets of the replay topics from inside a pod and prints
messages per second while a replay runs, for example
`kubectl -n airpulse exec -i deploy/producer -- python - --config /etc/airpulse/demo.yaml --seconds 60 < tests/benchmark/stream_throughput.py`.

`batch_timing.sh` times the batch jobs on the live layers for a date range.
