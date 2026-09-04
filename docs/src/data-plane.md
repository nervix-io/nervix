# Data Plane

The data plane is the runtime execution engine.

It is responsible for:

- receiving records from ingestors
- decoding payloads through codecs
- evaluating structured filters, construction expressions, and side-effect invocations
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

Every relay has one scheduled owner. Producers on other cluster nodes use one fixed dispatch slot
per relay and serialize each batch once for the owner. The owner alone maintains the bounded relay
buffer, concrete branch presence, metrics, subscriptions, and fan-out. It sends at most one
serialized copy to each remote consuming cluster node, where all runtime consumers and any local
subscription share that delivery. Only a relay's optional materialized records have
scheduler-selected state replicas; the relay's hot-path runtime is never replicated.

Nervix is not a durable event log for every in-flight row. If hot-path message or ACK state is lost, sources and ingestors react according to their delivery mode, offsets, and retry policy.

Branch grouping is native runtime isolation based on explicit `CREATE BRANCH` declarations. A
branch declares the branch-key schema shape with `SCHEMA <schema>`, TTL, and optional eviction
policy. The branch name is part of its identity: differently named branches remain incompatible
even when they reference the same schema. Ingestor routes construct keys with `BRANCHED BY
<branch> SET ...`; reingestor routes preserve the input key, construct another named branch, or
become unbranched. Relays and branch-preserving processors use that exact named branch or declare
`UNBRANCHED`. Relay presence, processor buffers, deduplicator state, window state, and materialized
entries remain scoped to one concrete branch; batches for those branches share the declared
relay's owner buffer.

Structured Model expressions are compiled into typed VM programs before local graph instantiation.
The leader validates them eagerly so invalid scopes, construction, types, nullability, sensitivity,
or branch relationships fail at command time. Runtime nodes consume Models directly and never
reparse stored NSPL.

## Working-Message Execution

Transforming construction is compiled as one ordered columnar program. The runtime projects the
input batch into the route program, reuses input columns for inherited or still-current values,
and constructs new columns only for rewritten or newly initialized output fields. Repeated `SET`
targets replace the current output column in written order. Finalization validates required and
optional output columns before route filtering.

This is the implementation of the Manual's [working-message model](working-message.md), not a
second field-resolution contract. The Manual owns the normative scopes and edge cases.

## ACK Composition

Relay fan-out gives each attached runtime consumer a descendant of the incoming ACK state.
Detached consumers receive the batch without an upstream ACK dependency. The source ACK succeeds
only when all attached descendants succeed. Any attached failure fails the shared source attempt,
even when another descendant has already completed an external side effect.

ACK guards, tokens, and maps remain in memory. They do not record a transactional per-sink commit
ledger. After source redelivery, every attached path processes the record again. This is why an
already successful non-idempotent sink can receive a duplicate after a sibling path fails. See
[ACK Semantics And Effective Delivery](emitters.md#ack-semantics-and-effective-delivery) for the
sink consequences and mitigations.

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

For relay movement between nodes, Nervix uses Arrow IPC batch serialization on the interconnect
path. Admission responses hold the producer or owner dispatch slot until the receiving node has
accepted the batch, while the attached ACK chain continues through downstream consumers. Control
traffic such as lookups and state-sync RPCs still uses separate control-envelope formats.

Runtime graph metrics are maintained alongside the data plane. Prometheus export uses branch-aggregated series to keep label cardinality bounded, while `DESCRIBE` can report branch-local metrics where a concrete relay branch is being inspected. See [Metrics And Observability](metrics-and-observability.md).

The runtime ownership above produces a per-branch resource cost. See
[Capacity Planning For Branched Graphs](capacity-planning.md) for the operator-facing cost
structure and the current signal gaps.

This design keeps latency low and avoids turning the runtime into a transactional storage engine.
