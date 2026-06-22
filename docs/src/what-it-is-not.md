# What It Is Not

Nervix is not an exactly-once transactional engine.

With ACK-based ingestion modes, Nervix behaves as an at-least-once system. Without ACK tracking, it behaves as at-most-once. That is an intentional design choice: Nervix prioritizes throughput and low latency over transactional persistence on every action.

Operationally, Nervix is much closer to a high-performance in-memory relaying runtime with snapshot-style replicated state than to a transactional database or durable event log.

Important consequences:

- Raft-backed strong consistency applies to the control plane, not to the data plane.
- Records moving through ingestors, processors, and emitters do not cross an exactly-once transactional boundary.
- ACKs control retry and replay behavior. They do not provide exactly-once execution and are not persisted.
- Runtime state persistence and replication are practical recovery mechanisms for selected execution node state, not atomic commit boundaries across the whole graph.

The data plane operates in two broad modes:

1. Pure in-memory relay processing.
   Records traveling through relays and runtime nodes stay in memory while they are being routed and processed. ACK guards, ACK tokens, and ACK maps are part of this hot path and are not persisted.
2. Snapshot-style replicated runtime state.
   Selected runtime state such as Kafka domain offsets, deduplicator state, and materialized relay state is snapshotted to persistent storage and replicated to followers.

The execution graph itself is different: NSPL models and domain schedules are control-plane state and are persisted with strong consistency guarantees before the data plane runs them.

Each runtime graph node is scheduled onto one cluster node and may have replicas. Replicas maintain eventually consistent copies of replicated runtime state and are designed to catch up through the replication channel.
