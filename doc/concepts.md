# [Stateful Stream Processing](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/concepts/stateful-stream-processing/)

## stateful operations

some operations remember information across multiple events (for example window operators)
- When aggregating events per minute/hour/day, the state holds the pending aggregates.
- When training a machine learning model over a stream of data points, the state holds the current version of the model parameters.

make State fault tolerant using checkpoints and savepoints.

## State Persistence

Flink implements fault tolerance using a combination of **stream replay** and **checkpointing**.

A streaming dataflow can be resumed from a checkpoint while maintaining consistency
(exactly-once processing semantics) by **restoring the state** of the operators and 
**replaying the records** from the point of the checkpoint.

The checkpoint interval is a means of trading off the overhead of fault tolerance during execution 
with the **recovery time** (the number of records that need to be replayed).

Because Flink’s checkpoints are realized through distributed snapshots, we use the words snapshot and checkpoint interchangeably.
Often we also use the term snapshot to mean either checkpoint or savepoint.

### Aligned Checkpointing -> Barrier

The central part of Flink’s fault tolerance mechanism is drawing consistent snapshots of the distributed data stream and operator state.

Flink’s mechanism for drawing these snapshots is described in “Lightweight Asynchronous Snapshots for Distributed Dataflows”.

https://frankma.me/posts/database/grokking_streaming_systems_notes/#checkpoint

#### stream barriers

A core element in Flink’s distributed snapshotting are the stream barriers (injected into the data stream and flow with the records).

A barrier separates the records in the data stream into the set of records that goes into the current snapshot, and the records that go into the next snapshot.

#### Recovery

Recovery under this mechanism is straightforward: Upon a failure, Flink selects the latest completed checkpoint k. 
The system then **re-deploys** the entire distributed dataflow, and gives each operator the state that was snapshotted as part of checkpoint k. 
The sources are set to start reading the stream from position Sk. For example in Apache Kafka, that means telling the consumer to start fetching from offset Sk.

### Unaligned Checkpointing ->

The basic idea is that checkpoints can overtake all in-flight data as long as the in-flight data becomes part of the operator state.

Unaligned checkpointing ensures that barriers are arriving at the sink as fast as possible. It’s especially suited for applications with at least one slow moving data path, where alignment times can reach hours

### Savepoints

Savepoints are manually triggered checkpoints, which take a snapshot of the program and write it out to a state backend.

Savepoints allow both updating your programs and your Flink cluster without losing any state.


### Exactly Once vs. At Least Once

For applications that require consistently super low latencies (few milliseconds) for all records, 
Flink has a switch to skip the stream alignment during a checkpoint. 
Checkpoint snapshots are still drawn as soon as an operator has seen the checkpoint barrier from each input.

The operator also processes elements that belong to checkpoint n+1 before the state snapshot for checkpoint n was taken.
On a restore, these records will occur as duplicates, because they are both included in the state snapshot of checkpoint n, 
and will be replayed as part of the data after checkpoint n.

Alignment happens only for operators with multiple predecessors (joins) as well as operators with multiple senders (after a stream repartitioning/shuffle). 
Because of that, dataflows with only embarrassingly parallel streaming operations (map(), flatMap(), filter(), …) actually 
give exactly once guarantees even in at least once mode.

