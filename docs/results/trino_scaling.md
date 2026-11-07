# Trino scaling

`tests/benchmark/trino_scaling.sh` on the demo cluster, 2026-11-07. Raw numbers are in
`trino_scaling.csv`.

## Method

The clean air layer of the ten year bench history, 5,484,024 rows in 3,672 day partitions, is
mapped as the external table `hive.airpulse_bench.clean_air_y10`. The query groups it by point
and month with a mean, a 95th percentile, a maximum and a count, a typical dashboard
aggregation over the whole history. It runs several times with two workers and with one, the
elapsed time comes from `system.runtime.queries`. The first round went from two workers to one,
the second round the other way, to separate the worker count from the order of the runs.

## Results

| Workers | Runs | Elapsed, warm | First run after the change |
|---|---|---|---|
| 2 | 5 | 40.0 to 42.0 s | 73.2 s on the first query of the campaign, 61.6 s after scaling from one to two |
| 1 | 5 | 34.7 to 38.6 s | 38.3 s |

Planning takes 0.1 to 0.8 s and queuing nothing, almost all the time is the scan and the
aggregation. A second worker does not make the query faster on one machine, it is about ten
percent slower once warm. Both workers share the same cores and disk, and the table is made of
3,672 files of about 80 KB, each one a split that the coordinator schedules on its own, so a
second worker mostly adds coordination. The first query after a worker joins also pays for its
empty caches of file metadata. On separate machines the second worker would bring its own CPU
and disk, and fewer, larger files would let each split do more work.
