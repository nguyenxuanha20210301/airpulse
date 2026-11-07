# fault tolerance drills

each script removes one component of the running demo cluster, checks that the system keeps
answering while it is gone, measures how long it takes to come back and appends one line per
measure to `docs/results/fault_tolerance.csv`. run them one at a time from the repo root with
the demo profile, which has three brokers, three datanodes and three cassandra nodes spread over
three workers. `docs/results/fault_tolerance.md` holds the numbers of the last campaign.

| script | what it does | what is checked |
|---|---|---|
| `kill_broker.sh` | deletes one kafka broker while a producer writes with acks all | the producer never stops, acknowledged records equal the records found in the topic, under replicated partitions go back to zero |
| `kill_datanode.sh` | deletes one datanode | the whole raw air layer is counted through trino while it is away and the count matches the one taken before |
| `kill_cassandra.sh` | deletes one cassandra node | writes and reads at QUORUM succeed with two of three replicas, a read at ALL is refused until the node is back |
| `kill_executor.sh` | deletes one speed layer executor | a replacement executor starts, a later producer cycle is processed, the driver does not restart |
| `kill_driver.sh` | deletes the speed layer driver | the operator submits the application again and the queries go on from the batch numbers in their checkpoints |
| `stop_worker.sh` | stops a whole kind worker node, then starts it again | cassandra QUORUM reads and HDFS reads keep working with one node of each gone, the time until the node and every pod are back |

the scripts read hosts and the replication factor from the profile config through the
virtualenv, `make setup` must have run. `kill_executor.sh` and `kill_driver.sh` restart the
producer to push a fresh cycle through the speed layer instead of waiting half an hour, which
costs one round of api calls. `stop_worker.sh` picks a worker that runs neither the namenode nor
the metastore, the two components that have a single instance, and `HOLD` sets how long the
node stays down.
