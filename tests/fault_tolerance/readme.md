# fault tolerance drills

each script removes one component on the running cluster and prints what to watch. run them
one at a time from the repo root with the demo profile, which has three of everything.

| script | what it does | expected outcome |
|---|---|---|
| `kill_broker.sh` | deletes one kafka broker pod | producer and speed layer keep running, under replicated partitions rise then return to zero when the pod is back, no gap in the clean topics |
| `kill_datanode.sh` | deletes one datanode pod | reads of the raw layer still succeed because every block has two copies, the namenode re replicates after the heartbeat timeout |
| `kill_cassandra.sh` | deletes one cassandra pod | writes and reads at LOCAL_QUORUM succeed with two of three nodes, `nodetool status` shows the node down then up again |
| `kill_executor.sh` | deletes one speed layer executor pod | the driver asks for a new executor, the micro batch is retried from the checkpoint, no duplicate rows in cassandra thanks to the upsert keys |
| `kill_driver.sh` | deletes the speed layer driver pod | the operator restarts the application and it resumes from the kafka offsets in the checkpoint |
| `stop_worker.sh` | stops one kind worker node | pods on that node are rescheduled, the services stay reachable, start the node again with the same script and `start` |

the cluster dashboard in grafana shows the effect on partitions, lag and pod readiness.
