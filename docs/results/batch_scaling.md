# Batch scaling

`tests/benchmark/batch_scaling.sh` on the demo cluster, 2026-11-07. Raw numbers are in
`batch_scaling.csv`.

## Method

The history is the backfilled weeks copied back in time by `airpulse.ingestion.amplify`, 68 days
per copy, 6 copies for one year, 27 for five and 54 for ten, under `/airpulse-bench/y<years>`.
Each copy gets new event ids, so `raw_to_clean` keeps them all. One day partition holds about
3,400 rows and 80 KB of Parquet per dataset.

Every application gets executors of one core, 1536m of heap and 1g of overhead, and 96 shuffle
partitions that adaptive execution merges again for small inputs. The time is read from the
operator status, from the submission to the end of the application, so it includes about half a
minute for spark-submit and the driver and executor pods. The live speed layer was paused and
Trino and Superset scaled to zero during the runs, every node of the cluster shares the same
four cores and disk.

## History size, two executors

| Years | Raw rows | raw_to_clean | trends | features | Total |
|---|---|---|---|---|---|
| 1 | 1,402,272 | 123 s | 94 s | 123 s | 340 s |
| 5 | 6,310,224 | 544 s | 232 s | 472 s | 1,248 s |
| 10 | 12,647,664 | 1,520 s | 555 s | 1,343 s | 3,418 s |

Up to five years `raw_to_clean` keeps a steady rate of about 11,500 raw rows per second, and
`trends` and `features` grow slower than the data because a fixed part, the submission, the
listing of the partitions and the writes of small output tables, weighs more on small inputs.
From five to ten years the data doubles and the total grows 2.7 times, `raw_to_clean` falls to
8,300 rows per second. The part of the work that grows with the number of day directories, the
listing and the commit described below, is done by the driver alone and takes over.

## Executors, five years of history

| Executors | raw_to_clean | trends | features | Total | Speedup |
|---|---|---|---|---|---|
| 1 | 718 s | 281 s | 622 s | 1,621 s | 1.00 |
| 2 | 544 s | 232 s | 472 s | 1,248 s | 1.30 |
| 4 | 499 s | 253 s | 464 s | 1,216 s | 1.33 |

A model with a serial part and a part divided by the executor count, fitted on one and two
executors, gives 875 s serial and 746 s parallel. Four executors should then take about 1,060 s,
they took 1,216 s, the extra executors compete for the same four cores and disk as Kafka,
Cassandra and HDFS. The serial part comes from the driver and the storage layout.

- Listing the day partitions takes the driver 16 to 27 s per dataset at five years, 1,836
  directories each, before a single row is read, measured in the driver logs of `trends` and
  `features`.
- The writes of `raw_to_clean` and `features` replace one directory per day, the commit renames
  them one after the other on the driver. At ten years the commit of the air layer alone took
  140 s for 3,672 directories, against 531 s for the write job itself.
- Every file holds a single day of 63 points, about 80 KB, far below the 64 MB block size this
  cluster sets for HDFS, so the namenode and the datanodes handle many tiny requests.

On separate machines more executors would also bring more disks and cores. On one machine the
storage layout matters more, monthly partitions for closed months and a compaction of the raw
layer would cut the directory count thirtyfold.

## Memory and the feature job

With the profile defaults, twelve shuffle partitions and 512m of overhead, `features` lost
executors to the memory limit of their containers from one year of history on, `OOMKilled` with
exit code 137, and the operator retried the whole application. More partitions and 1g of
overhead were not enough. The cause was the forecast targets, computed by joining the feature
table to itself once per target and horizon, six shuffles of a wide table. They are now read
with window frames of width zero placed one, six and twenty four hours ahead, on the same
partitioning and order as the rolling features, which adds no shuffle.

| One year, two executors | Duration | Executors killed |
|---|---|---|
| targets by six joins | 288 s | 3 |
| targets by window frames | 123 s | 0 |

The first line was measured while the live speed layer was running, the second with it paused,
the gap in duration is partly the retries of the killed executors.
