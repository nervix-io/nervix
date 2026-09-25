# Data Plane

The data plane is the runtime execution engine.

[Connector Crates And The Connector Contract](./connector-contract.md) defines how the host
admits external source messages and publishes through external sinks, including their ACK and
commit boundaries. This chapter follows the Arrow batches those boundaries hand to the graph.

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
- Execution node state is runtime state. Selected state such as domain offsets, deduplicator history, materialized relay entries, window accumulators, and metric summaries is persisted through periodic snapshot/replication mechanisms, and WASM guest state through a durable checkpoint at the end of every guest callback. Each persisted state is keyed by the identity the committed schedule publishes for its node. Domain offsets and metric summaries depend on no schema and are keyed by their node alone, so they survive schema changes. Every other state is also keyed by the fingerprint of the schemas it is laid out by, so a schema change starts it anew, and a node without a published fingerprint for an entity cannot place that entity's schema-bound state at all.
  A materialized relay's snapshot is columnar: it carries the relay's records as Arrow sections under the relay's exact schema, with each record's concrete branch key, watermarks, and the state revision, ownership assignment, and branch lifecycle it was captured at described beside them. A snapshot holds exactly the branches of the branch lifecycle it names and at least the committed revision it names: an update to an existing branch's record that lands while the records are read may already be present, and the next revision carries it again. Updates to existing records continue while a snapshot is captured and written out; a branch that arrives or is evicted waits only while the records are read, and a snapshot taken before a branch was evicted never restores that branch.
  A snapshot larger than the transfer budget crosses the bulk pool in bounded chunks and lands on the receiving node's staging disk, where its length and digest are checked before anything reads it. A cancelled, truncated, or corrupted transfer leaves the state it would have replaced untouched.
  A deduplicator's keys and a window processor's window belong to the branch task that processes the branch, which changes them without waiting on snapshots or replicas. While that state keeps changing, the task publishes an immutable copy of it at least once per replication poll interval, and it publishes again before an ownership handoff checkpoint and when the branch stops. Snapshots, replica synchronization, and the next task for the same branch read only the published copy, so a replica holds a branch's state as of its latest publication. A branch task that is aborted after exceeding its shutdown grace period still has its latest publication persisted, but loses the changes it made after that publication. A published window carries the rows it retains together with each row's aggregate arguments; restoring it re-admits those rows in order, which rebuilds every aggregate structure, and only a histogram percentile's removals that are still waiting out their delay are carried beside the rows. A WASM processor checkpoints its guest state at the end of every guest callback and keeps the bytes the guest returned instead of copying them. Unlike the periodic snapshots above, a guest-state checkpoint is written to the node's stable storage and synchronized, and when the schedule assigns the processor replicas, each of them writes and synchronizes it too before acknowledging it. The inputs a callback decided are acknowledged only after its checkpoint completes, and a checkpoint that fails leaves the previous one committed, negatively acknowledges those inputs, and recreates the branch's guest from the committed checkpoint; see [WASM Processor Guests](wasm-processor-guests.md#checkpoints-and-acknowledgements) and [WASM State And Recovery](wasm-state.md). A guest saves only its computation state there, never the input it buffers or that input's ACK tokens. Each branch's guest state is keyed by its state generation, the lifetime the committed schedule names for it, so a checkpoint, a replica installation, or a recovered checkpoint of an earlier generation can never address the current state; the node's state store writes guest state on its storage workers rather than on the async worker that runs the branch.
- Message streaming is the hot path. In-flight records, relay batches, processor handoff, outbound emitter attempts, ACK guards, ACK tokens, and ACK maps stay in memory and are never persisted as runtime state.

The [Data-Plane Concurrency](./data-plane-concurrency.md) chapter defines how this hot path avoids
shared lock acquisition, publishes reconfigurable state, owns per-row mutation, and retains only
the ordering fences required by delivery and ownership contracts.

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

Structured Model expressions are compiled into typed VM programs. The leader validates them when a
statement is applied, so invalid scopes, construction, types, nullability, sensitivity, or branch
relationships fail at command time. Runtime nodes never reparse stored NSPL. A program executes
over the Arrow batch its runtime node projects for it, reusing input columns for inherited or
still-current values and constructing new columns only for rewritten or newly initialized output
fields. [VM Functions](./vm-functions.md) defines the compilation pipeline, columnar execution,
row and batch errors, kernels, every function family, and branch-local window aggregates, and
[Expression Functions](./filter-map-functions.md) owns the public function contracts.

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

Every internal schema type has one Arrow representation, which relays, processors, and the VM share;
[Typed Batches And Registers](./vm-functions.md#typed-batches-and-registers) lists them. The
[Expression Functions](./filter-map-functions.md) reference lists every operator and function.

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
path. A producer or owner dispatch slot is scoped to one concrete branch and remains held until the
receiving runtime atomically admits or rejects that batch. Other branches continue concurrently,
while the attached ACK chain reports downstream processing after admission. A body receipt alone
does not mean the runtime admitted the batch. Control traffic such as lookups and state-sync RPCs
still uses separate control-envelope formats.

Runtime graph metrics are maintained alongside the data plane. Prometheus export uses branch-aggregated series to keep label cardinality bounded, while `DESCRIBE` can report branch-local metrics where a concrete relay branch is being inspected. See [Metrics And Observability](metrics-and-observability.md).

The runtime ownership above produces a per-branch resource cost. See
[Capacity Planning For Branched Graphs](capacity-planning.md) for the operator-facing cost
structure and the current signal gaps.

This design keeps latency low and avoids turning the runtime into a transactional storage engine.
