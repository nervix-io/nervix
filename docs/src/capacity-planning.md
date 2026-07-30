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
- relay fan-out buffers up to `CAPACITY` Arrow batches per bounded consumer channel;
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

UDF-bearing expressions run on the process-wide blocking worker pool. Heavy UDF use and the
maximum number of concurrent UDF-bearing paths are blocking-pool sizing inputs. A native UDF that
never returns permanently occupies one worker. See the
[UDF watchdog consequences](udfs.md#nulls-errors-and-volatility).

Two multipliers usually dominate:

1. live branch count × per-branch buffered depth;
2. stateful retention window × per-branch arrival rate.

The second multiplier applies separately to every stateful node. A long deduplication horizon does
not pay for a window's state, and a window width does not bound a correlator.

## Sizing Knobs

- Branch `TTL` trades branch reuse against how long idle branch-local tasks, buffers, and state
  remain live. Shorter TTL releases idle branches sooner.
- `MAX INSTANCES <n> EVICT LRU` trades branch coverage against a hard branch-population ceiling.
  Eviction drops the least recently used branch and its suspended or buffered branch-local work.
- Relay `CAPACITY` trades burst absorption against queued Arrow batches per consumer channel.
  Smaller capacity applies backpressure sooner.
- Route `FLUSH EACH` interval and size trade latency and per-route memory against batch throughput.
  `FLUSH IMMEDIATE` minimizes configured wait but still micro-batches.
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
- Sustained high `nervix_relay_buffer_len` percentiles show downstream backpressure at relay
  fan-out buffers.
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
