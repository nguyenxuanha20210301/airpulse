# Stream scaling

`tests/benchmark/stream_scaling.sh` on the demo cluster, 2026-11-07. Raw numbers are in
`stream_scaling.csv`.

## Method

One year of bench history, 1,402,272 events, is replayed without pacing into the topics
`weather.raw.bench` and `air.raw.bench`, six partitions and three replicas each. A second speed
layer then runs the clean queries on that backlog from the earliest offsets, once with one, two
and four executors of one core and 1g of heap. It triggers every ten seconds and a micro batch
reads at most 100,000 records per topic, so the backlog goes through eight batches for air and
seven for weather whatever the executor count. Its outputs go to the keyspace `airpulse_bench`,
the topics `*.clean.bench` and its own HDFS root. The live speed layer was paused during the
runs. The rows and the duration of every batch come from the progress log of the listener in
`airpulse.streaming.progress`.

## Ingestion into Kafka

| Measure | Value |
|---|---|
| events replayed | 1,402,272 |
| application, submission to end, including the sort of the history | 124 s |
| peak rate at the brokers, `air.raw.bench` | 14,575 messages per second |
| peak rate at the brokers, `weather.raw.bench` | 8,463 messages per second |

The peaks come from `kafka_server_brokertopicmetrics_messagesin_total` in Prometheus. The replay
is one Python process that builds and serializes every event before handing it to the client,
weather events carry more fields than air events, the brokers are far from busy at these rates.
Without pacing the replay now queues every event and flushes once at the end, before it waited
for the acknowledgment of every replayed hour.

## Processing by the speed layer

| Executors | clean-air rows per second | clean-weather rows per second | First batch | Later batches | Backlog drained in |
|---|---|---|---|---|---|
| 1 | 4,914 | 4,662 | not kept | not kept | not kept |
| 2 | 5,354 | 5,118 | 46 to 48 s | 14 to 17 s | about 146 s |
| 4 | 4,591 | 4,314 | 69 s | 15 to 20 s | about 169 s |

The rate of a query is its rows divided by the summed duration of its batches, the two queries
share the executors. The first and the last column come from the driver logs of the runs with
two and four executors, the log of the run with one executor was not kept.

About 95 percent of a batch is spent in `addBatch`, the sink, which deduplicates against the
state of the watermark, writes the clean events to Kafka with acks from all three replicas and
upserts the newest row per point in Cassandra. Query planning and the offset and commit logs on
HDFS take the rest. The first batch of every query pays for the connections, the code
generation and the setup of the state store, more executors make it longer.

The clean queries reach about 13,000 events per second together once warm, and adding
executors does not raise it on this machine. The sink, the checkpoint writes and the driver
that coordinates both queries are shared by every executor, and the extra executors compete for
the same four cores as the brokers, Cassandra and HDFS. The live feed brings 504 events every
30 minutes and a replay at 3,600 times real time about 125 events per second, both two orders
of magnitude below what the speed layer absorbs.
