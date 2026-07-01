# Data Plane

The data plane is the runtime execution engine.

It is responsible for:

- receiving records from ingestors
- decoding payloads through codecs
- running optional node-local filter-map programs
- grouping records into isolated execution branches
- batching rows into Arrow record batches at node boundaries
- moving Arrow batches across processors and relays
- materializing selected state
- encoding and emitting outbound payloads

The data plane is intentionally non-transactional.

Decoded rows are processed in memory and are usually carried between runtime nodes as Apache Arrow batches rather than as individually serialized documents. That gives the runtime a columnar format suitable for fast vectorized processing and cheap batch serialization/deserialization.

Nervix has three separate persistence boundaries:

- Execution graph configuration is control-plane state. NSPL models, domain lifecycle, and schedules are persisted with strong consistency guarantees before runtime nodes execute them.
- Execution node state is runtime state. Selected state such as domain offsets, deduplicator history, materialized relay entries, window accumulators, metric summaries, and WASM guest state is persisted through periodic snapshot/replication mechanisms.
- Message streaming is the hot path. In-flight records, relay batches, processor handoff, outbound emitter attempts, ACK guards, ACK tokens, and ACK maps stay in memory and are never persisted as runtime state.

Nervix is not a durable event log for every in-flight row. If hot-path message or ACK state is lost, sources and ingestors react according to their delivery mode, offsets, and retry policy.

Branch grouping is native runtime isolation based on explicit `CREATE BRANCH` declarations. A branch declares the branch-key schema shape with `BY <schema>`, TTL, and optional eviction policy. Ingestors and reingestors select that branch with `BRANCHED BY <branch> VALUES { ... }` to map output records to concrete branch keys. Relays and branch-preserving processors either select that branch with `BRANCHED BY <branch>` or declare `UNBRANCHED` execution. Each concrete branch instance is handled independently. Runtime relay instances, processor buffers, deduplicator state, window state, and materialized entries are scoped to that branch instance until an emitter drains records externally or a reingestor starts a new grouping.

Filter-map programs are compiled from NSPL into a typed VM program before local graph instantiation. They are not distributed as bytecode. The leader validates them eagerly so invalid `SET` / `UNSET` / `WHERE` programs fail at command time instead of surfacing later during runtime startup.

The current VM surface covers:

- arithmetic operators: `+`, `-`, `*`, `/`, `%`
- comparisons and boolean operators: `=`, `!=`, `>`, `<`, `>=`, `<=`, `AND`, `OR`, `NOT`
- explicit casts
- built-ins: `lower`, `upper`, `trim`, `length`, `coalesce`, `is_null`, `nullif`, `abs`, `contains`, `starts_with`, `ends_with`

These expressions can be nested, and builtin calls can be chained.

The VM now executes over the full Nervix internal schema type set:

- `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`
- `F32`, `F64`
- `BOOL`, `STRING`, `DATETIME`

`DATETIME` is stored internally as an Arrow `Timestamp(Nanosecond, "+00:00")`. RFC3339 remains a wire-level string representation rather than an internal schema type.

Examples of replicated runtime state:

- Kafka offsets when using `OFFSET BY DOMAIN`
- deduplicator state
- materialized relay state
- metric summaries used by `DESCRIBE` output
- WASM guest state

Kafka partition scheduling for `OFFSET BY DOMAIN` is control-plane state instead. The leader observes Kafka topology, commits the partition-to-instance assignment into the Raft-backed domain schedule, and the data plane executes only that committed assignment.

Examples of state that is not treated as a durable commit log:

- normal in-flight relay batches
- ACK guards, tokens, and maps
- outbound emitter operations
- intermediate processor handoff

For relay movement between nodes, Nervix uses Arrow IPC batch serialization on the interconnect path. Control traffic such as lookups and state-sync RPCs still uses separate control-envelope formats.

Runtime graph metrics are maintained alongside the data plane. Prometheus export uses branch-aggregated series to keep label cardinality bounded, while `DESCRIBE` can report branch-local metrics where a concrete relay branch is being inspected. See [Metrics And Observability](metrics-and-observability.md).

This design keeps latency low and avoids turning the runtime into a transactional storage engine.
