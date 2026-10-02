# Capacity Planning For Branched Graphs

Nervix bounds work; the operator sizes it. The runtime provides backpressure for message-batch
processing through bounded [relay capacity](relay.md#capacity), and it bounds branch population
through branch `TTL` and optional `MAX INSTANCES <n> EVICT LRU`. The capacity-planning contract
also requires observable eviction and resource consumption. Current signals cover process memory,
traffic, latency, relay-buffer occupancy, branch population, eviction counts, and branch-local
relay inspection, but not per-node state size. Remaining signals are stated in
[Branch Lifecycle Signals](metrics-and-observability.md#branch-lifecycle-signals).

Sizing inside those mechanisms is the operator's responsibility. There is no universal
branches-per-node figure. Data rate, flush policy, graph shape, and stateful-node configuration
change the answer.

## Per-Branch Cost Structure

One live branch instance can hold the following at each graph node that belongs to the branch:

- branch-local task and runtime-node state;
- route buffers until the route's flush boundary;
- the relay owner's shared buffer, bounded by `CAPACITY` across all concrete branches;
- pending flush timers;
- deduplication entries retained within `MAX TIME`;
- open window state retained by `WIDTH` and advanced by `STEP`;
- reorderer and correlator buffers retained within `MAX TIME`;
- for WASM processors, an isolated store and the guest linear-memory pages dirtied by that branch.

`FLUSH EACH` has a configured byte boundary. `FLUSH IMMEDIATE` has no size boundary; its pending
depth depends on arrivals during the system-owned 100 µs window. See the
[authoritative flush rule and tuning guidance](nspl-overview.md).

WASM code compilation is shared while mutable guest state is not. See
[Module Sharing And Branch Memory](wasm-processor-guests.md#module-sharing-and-branch-memory).

UDF-bearing expressions and JAQ transformations run on the node's extension workers, which
nothing else uses. Heavy UDF or JAQ use and the number of concurrent paths that run them are
extension sizing inputs. A native UDF that never returns permanently occupies one extension worker,
and never a worker another class needs. See the
[UDF watchdog consequences](udfs.md#nulls-errors-and-volatility).

## Bounded Execution And Transient Memory

Every variable-size encode, decode, validation, hash, compilation and program execution a node
runs off its asynchronous runtime is admitted into one of seven bounded classes, and is charged
against a reserved byte budget before it allocates. Each class has its own admission, so work
saturating one cannot take the slots another is entitled to.

| Class | Concurrent jobs | Work |
| --- | --- | --- |
| Control | 1 | Control-plane and consensus work: heartbeats, votes, acknowledgements, administrative replies |
| Credentials | 1 | Password hashing and verification for users being created or authenticated |
| Data | available CPUs − 1 | Per-message relay body encoding, decoding and validation, expressions over batches above 1,024 messages, ingest branch preparation and model inference |
| Extension | available CPUs − 1 | Operator-supplied code the node cannot bound: UDF-bearing expressions and JAQ transformations |
| Bulk | available CPUs − 1 | Whole-transfer work: resource archives, large read results, and compiling UDFs, WASM modules, protobuf descriptors and ONNX models |
| Consensus storage | 1, ordered | Consensus storage batches, applied in the order they were admitted |
| Filesystem storage | 2 | Every other synchronous filesystem and database operation, including Iceberg's staged files |

These counts bound admission, not threads. Jobs run on the process-wide blocking pool, so a
class's count is the number of its jobs that may be on that pool at once. A class is guaranteed its
share of admission; it is not guaranteed an idle thread. Apart from the classes, only work that
waits rather than computes uses the pool: a Kafka producer's final drain and partition
inspection, which wait on the broker, and the one read of the resolver configuration at startup.
Each class also holds at most 1,024 jobs waiting for a worker; a job beyond that is refused, and
the work that submitted it reports the refusal as its own failure. The one exception is work the
node already accepted and keeps: the unfolding of a payload a quiesce buffer retained waits for a
place instead, while its payload stays counted in the buffer.

Transient memory is 275 MiB per node, divided into ceilings that cannot borrow from each other:
8 MiB for management, 24 MiB for commands and replication, 192 MiB for relay work, 32 MiB for
bulk buffers and 19 MiB for credentials. One relay operation may hold at most 32 MiB of encoded body, 32 MiB of decoded data
and 16 MiB of conversion scratch, and the relay budget is sized to hold two such operations at
once so one blocked channel cannot exhaust the capacity another needs.
The commands budget also holds the 16 MiB working reservation for one normalized consensus state
write; half is the keyed payload ceiling and half covers database journal encoding and descriptors.
The credentials budget holds the 19 MiB Argon2id works in for one password, so a node computes one
hash at a time and a burst of login attempts waits there without touching any other class. A stored
hash whose parameters need more memory than that never verifies.

These budgets cover transient work: data in flight between nodes and the working memory of the
jobs the classes admit. They are separate from the process memory a running graph holds, which the [memory-pressure watermarks](metrics-and-observability.md) govern.
The limits are checked against each other when a node starts: a budget that could not hold the
largest operation of its class is reported with that class and size rather than discovered by
stalling on the first such operation.

Two multipliers usually dominate:

1. live branch count × per-branch buffered depth;
2. stateful retention window × per-branch arrival rate.

The second multiplier applies separately to every stateful node. A long deduplication horizon does
not pay for a window's state, and a window width does not bound a correlator.

## Sizing Knobs

- Branch `TTL` trades branch reuse against how long idle branch-local tasks, buffers, and state
  remain live. Shorter TTL releases idle branches sooner. Idle time is measured from a branch's
  last accepted input, and a physical maintenance scan releases expired branches, so peak branch
  population reflects the TTL plus that scan latency. See [TTL](relay.md#ttl).
- `MAX INSTANCES <n> EVICT LRU` trades branch coverage against a hard branch-population ceiling.
  Eviction drops the least recently used branch and its suspended or buffered branch-local work.
- Relay `CAPACITY` trades burst absorption against Arrow batches in the single owner buffer.
  Smaller capacity applies backpressure sooner. In addition to that buffer, each producer cluster
  node can hold one batch on its dispatch to the owner and the owner can hold one batch on its
  dispatch to each remote consumer cluster node. Those fixed slots do not resize with `CAPACITY`
  and do not multiply with consumers or subscriptions on the same node.
- Route `FLUSH EACH` interval and `MAX BATCH SIZE` bound how long rows wait and how much a route
  buffers. `MAX BATCH SIZE` only clamps a batch and never grows one: the batch a route emits is
  roughly arrival rate × interval, up to the byte cap. Larger batches reduce per-batch cost only
  at boundaries that work per batch; see the [flush tuning guidance](nspl-overview.md) for which
  sinks those are. `FLUSH IMMEDIATE` minimizes configured wait but still micro-batches.
- Cluster interconnect carries each relay batch as one Arrow IPC body with a fixed 32 MiB limit.
  It reserves one unadmitted batch and one terminal outcome per active logical channel. Channels
  are concrete-branch local, so waiting work in one branch does not consume another branch's
  ordering slot. Management subquotas independently reserve streams for discovery, liveness,
  replaceable domain-clock progress, relay admission, cancellation, and terminal outcomes even when
  ordinary management requests are full.
  Bulk subquotas independently reserve streams for resource transfer and for runtime and Raft
  snapshot transfer. A runtime state snapshot is described before it is fetched, so a node that
  already holds the current revision causes no scan and no encoding on the node that owns it.
  Consensus replication uses one ordered append stream per follower, bounded at 16 outstanding
  batches and 16 MiB, and charges each batch against the shared command budget for as long as it
  is unacknowledged. That stream reserves its own replication stream slot, so the ownership
  handoff requests sharing the replication pool never wait behind it. A Raft snapshot is sealed
  and moved one bounded section at a time, so neither construction nor transfer grows with the size
  of the replicated state.
  Keep `MAX BATCH SIZE` well below 32 MiB on any route whose consumer may be scheduled on another
  node. Each node publishes what its pools and budgets are actually holding; see the
  [interconnection metrics](metrics-and-observability.md) for the series to size against.
- Stateful `MAX TIME`, `WIDTH`, and `STEP` trade history and aggregation coverage against retained
  entries, open windows, and buffered rows.
- Source `INSTANCES` trades source parallelism against concurrent admission pressure. It does not
  reduce the cost of any branch instance that becomes live.

See the existing [FLUSH tuning guidance](nspl-overview.md) instead of treating example values as
defaults.

## What To Watch

- `nervix_jemalloc_allocated_bytes`, `nervix_jemalloc_active_bytes`, and
  `nervix_jemalloc_resident_bytes` show process-memory pressure from all workloads on the node.
- `nervix_branch_instances` shows the current concrete branch-key population per domain, branch
  declaration, and physical node.
- `nervix_branch_evictions_total` shows LRU pressure and TTL churn through its `reason` label.
- Sustained high `nervix_relay_buffer_len` percentiles show downstream backpressure at the relay
  owner buffer.
- High `nervix_delivery_latency_seconds` percentiles show downstream lag between graph nodes.
- `DESCRIBE RELAY <relay> WHERE (...)` confirms whether one concrete branch-local relay exists and
  reports its buffer metrics when available.
- `DESCRIBE INGESTOR <name>` reports `memory-backpressure: active|inactive`.

Prometheus does not currently expose branch-creation counters, deduplication entry counts, or
open-window counts. `DESCRIBE` also does not provide a branch-population inventory or eviction
history.

For runtime ownership and snapshot boundaries, see [Data Plane](data-plane.md). For the current
metric families and cardinality policy, see
[Metrics And Observability](metrics-and-observability.md).
