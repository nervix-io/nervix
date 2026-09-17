# Data-Plane Concurrency

Nervix keeps the paths that carry records, batches, remote relay frames, and acknowledgements free
from shared coordination that is unrelated to the record being processed. This is the
contentionless data-plane rule. It applies after a runtime task has started and its graph, routing,
state, metric, and connector handles have been resolved.

The hot paths are:

- accepting one record from an ingestor
- admitting and fanning out one relay batch
- executing one processor batch for a concrete branch
- receiving and admitting one remote relay frame
- creating, sharing, and resolving one acknowledgement

These paths may apply bounded backpressure and preserve required ordering. Their steady-state work
must not acquire a `Mutex` or `RwLock`, including a read lock, merely to discover configuration,
ownership, topology, a service, or a metric series. A read lock still changes shared lock state,
competes with writers, and can join an unrelated reader to a writer's delay. Calling
`DashMap::entry()` for an established value is also excluded: the occupied case still takes the
shard's write side. Stable dependencies are resolved before the hot loop; a keyed registry that
cannot be resolved ahead of time uses a borrowed lookup first and reaches `entry()` only for the
first or racing installation.

The resulting design uses four forms of ownership:

- Reconfigurable read-mostly state is built privately and published by atomically replacing one
  immutable reference.
- A task resolves stable services, state authorities, slots, and metric series when it starts and
  retains those handles.
- Scalar progress and counts use atomics with an ordering chosen for the contract they enforce.
- State that changes for every row or batch belongs to one task, branch, delivery channel, or
  source attempt.

The [Data Plane](./data-plane.md) chapter defines payload, persistence, branch, and ACK semantics.
This chapter owns the concurrency contract for executing them. The [Cluster
Interconnect](./interconnect.md), [Domain Clock](./domain-clock.md), and [Shutdown And
Recovery](./shutdown.md) chapters remain authoritative for their respective distributed and
lifecycle protocols.

## Publication Boundaries

An atomic publication is a complete immutable value. A writer derives the next value without
changing the published one, then replaces the reference once. A reader keeps the reference it
loaded for as long as it needs it and therefore observes either the complete preceding value or the
complete replacement. It never observes fields being changed in place.

Publication is a node-local visibility mechanism, not a durability boundary. The control plane,
runtime-state store, or gossip protocol still owns the source of truth. Runtime publication makes
one already-decided revision available to the data plane without putting its readers behind the
writer.

| State | Publication owner and scope | Reader contract |
| --- | --- | --- |
| Active graph | The runtime lifecycle publishes one optional graph for each domain on a node. Installation replaces the complete graph, and stop or removal publishes absence. | A unit of work sees one complete graph or no active graph. An in-flight reader may finish against the graph it already loaded. |
| Domain routing snapshot | Each domain retains one stable publication handle across execution rebuilds. Schedule application stages and replaces the relay services, schemas, branch declarations, materialized-state ownership, lookups, UDFs, codecs, and signaling protocols together. | A task sees the old routing revision or the new routing revision, never a mixture of their fields. Long-lived tasks use a local pointer cache instead of returning to the domain execution registry per batch. |
| Node identity and remote dispatcher | The node runtime publishes this once after cluster join, when the authenticated interconnect and process incarnation are known. Relay boundaries created afterwards retain the same dispatcher handle. | Readers borrow the stable node identity, incarnation, transport, admission service, and ACK registry without a write-once lock or repeated name allocation. |
| Relay owner state | Each relay boundary publishes its scheduled owner, installed owner buffer, and remote runtime-consumer set. Schedule and relay lifecycle operations replace these values at their cutover points. | A batch borrows the current owner and buffer. Multi-step ownership changes use the dispatch gate described below so teardown cannot race an admitted dispatch. |
| Subscription interest | The cluster live-state watcher rebuilds an immutable index from domain and relay to interested node incarnations whenever gossip changes. | A relay owner performs borrowed lookups in one published index. It neither formats gossip keys nor waits on the gossip mutex per batch. Subscription creation waits until every live node has observed the exact subscriber incarnation before reporting success. |
| Clock installation | Each domain-clock lifecycle on each node publishes the complete missing, stopped, uninstalled, unpaced, or paced installation. | A read validates its bound lifecycle generation against one installation, then advances that installation's nondecreasing timestamp watermark atomically. A same-generation replacement retains the watermark; a different generation cannot be clamped by a stale reader. |
| Runtime-state assignment | Each state placement publishes one packed atomic binding containing its generation and capability. Replication roles are a separate immutable published snapshot. | A per-message operation admits itself, compares the exact binding it was granted, and proceeds only while that generation still grants the required capability. It never takes the assignment barrier. |
| Ingestor quiesce decision | Each ingestor publishes the declared and pending modes, active causes, source support, and derived intake decision as one value. Concurrent lifecycle changes derive their replacement from the current publication. | Polling and per-message intake make one load to decide whether to dispatch, suspend, skip, buffer, drop, or reject. The retained-payload lock is reached only after the published decision selects buffering. |
| Metric series handles | Each relay, node, ingestor, emitter, or concrete branch resolves its label set, internal series, and Prometheus child when its owning task or branch is created. | Recording uses the retained series directly. Counters update atomically; a histogram records through its already-resolved per-series accumulator without a registry lookup, key construction, or map guard. Registration and removal stay on lifecycle paths. |

An atomic replacement gives consistency for the value it publishes. A transition involving several
owners still needs a protocol. Graph and schedule changes use quiescence and relay gates; clock and
state operations carry generations; subscription creation uses a visibility handshake. Publication
removes read-side contention without weakening those transition contracts.

## Mutable Execution State

Per-row mutation is kept close to the lane that orders it. A global concurrent map is an index into
those lanes, not the owner of their inner state. Once a task has found its lane, later mutation does
not repeatedly acquire the registry guard.

### Branch processor state

Each concrete branch has one dispatch lane and one mutable processor runtime. The lane admits one
batch for that branch and queues later batches, which preserves processor order. Different branches
have different runtimes and execute independently. Expiry and eviction first detach a branch from
the instance registry and then take ownership of its runtime, so registry access is not nested over
branch execution.

### Deduplicator keys

A deduplicator keyspace belongs to the task for one concrete branch. That task prunes, reserves, and
releases keys directly. When the keyspace changes, the task marks it dirty and periodically
publishes an immutable generation whose key allocations are shared with the live keyspace.

Snapshot persistence, replication, ownership handoff, and a replacement branch task read the last
published generation. They never read or lock the live keyspace. Publication occurs during the
replication cadence, before a handoff checkpoint, and when the branch stops, so snapshot encoding
can proceed while the current task continues processing rows.

### Window and WASM state

A window and its aggregate accumulators belong to one branch task. The task mutates the live window
directly and publishes a complete immutable window generation for persistence, replication,
handoff, and restoration. Snapshot encoding reads that generation instead of holding the branch
runtime while it serializes.

A WASM guest instance likewise belongs to one branch task. After every batch the host asks
the guest to save its computation state and publishes the returned buffer as the latest save. The
publication retains that buffer rather than copying it for every persistence or replication reader.
Input buffered by the guest host and ACK tokens remain execution state and are never included in a
guest save.

### Materialized relay entries

One assigned materialized-relay originator owns updates. The state contains at most one latest
record for each concrete branch. Replacing an existing branch's record is admitted under the
originator's assignment generation. Adding the first record for a branch or evicting a branch also
changes the branch lifecycle, so that narrower operation uses the assignment barrier and advances a
branch generation.

A snapshot capture holds the assignment boundary only long enough to take immutable row views,
their revision, their ownership fence, and their branch generation. The Arrow columns stay shared,
and sealing and encoding happen after the boundary is released. A snapshot from an earlier
assignment or branch generation cannot restore state that a newer owner or eviction superseded.

### Acknowledgement state

Each source attempt owns one in-memory acknowledgement tree. Fan-out reserves shares in an atomic
pending count, and each completion resolves one share with a compare-and-swap. Separate atomic
state records whether shares still count against ownership handoff; a message parked on materialized
`REQUIRED WAIT` does not keep a handoff blocked.

Root trackers count attempts rather than individual shares. Their transition ordering may briefly
overcount but cannot let handoff miss active work. Exactly one terminal transition takes the
one-shot sender from its slot and releases that guard before notifying the waiter. ACK trees,
shares, and trackers are volatile hot-path state and are never persisted.

## Ordering Fences

Three fences remain because they define externally observable order or ownership. Each is scoped to
the smallest identity that carries the guarantee. They do not provide general-purpose mutual
exclusion for unrelated batches.

### Per-slot delivery gate

A remote delivery slot is scoped to one destination, relay, payload role, and concrete branch. Its
asynchronous gate permits one encode-and-send operation at a time for that logical channel. The
gate preserves FIFO order from serialization through admission and ensures the channel sequence is
allocated in the same order. Other destinations, relays, roles, and branches use different slots
and continue independently.

Established slots are found with a borrowed lookup. `entry()` is used only when concurrent first
deliveries must agree on a single slot; allowing two racing installations would create two sequence
owners and could interleave the channel. The small sequence state is updated only while the slot
gate is held, and its synchronous guard never crosses an await.

### Dispatch-gate engagement

The relay dispatch gate separates ordinary publication from a model change, ownership handoff, or
shutdown operation that must know every earlier dispatch has left. On the open path, a publisher
increments an atomic in-flight count and checks an atomic closed flag. An engagement closes the
gate before inspecting the count. These operations use one total order, so either the publisher
enters and is counted by the engagement or it observes closure, rolls back its count, and waits.

The engagement state is consulted only while the gate is closed. Once all earlier permits leave,
the engagement owns a lease and the protected mutation can proceed. The acquisition deadline bounds
waiting for that fence; a successfully acquired lease remains engaged until its owner releases or
drops it.

### Assignment generations

A runtime-state assignment packs its generation and capability into one atomic word. A hot
operation increments the admission counter for that generation before comparing its token with the
published binding. A rebind serializes with other rebinds, publishes the successor binding, and
waits only for operations admitted under the generation it superseded. Even and odd generation
counters let work admitted under the successor proceed without extending that wait.

This fence prevents a former owner from mutating or describing state after reassignment. Snapshot
capture and whole-state replacement use the assignment barrier because their contents and ownership
fence must describe the same moment. Ordinary record updates use atomic admission and never take
that barrier.

## Bounded Synchronization Outside The Open Path

Some synchronization remains after a hot operation has selected a state that explicitly requires
it:

- a quiesced ingestor locks only the bounded retained-payload buffer selected by its declared
  `MAX SIZE` policy
- one concrete processor branch serializes its own batches, while other branches remain
  independent
- one already-resolved histogram series serializes its bounded accumulator update; counter series
  are atomic
- one inbound relay attempt serializes its small receipt, admission, rejection, and cancellation
  transition within the interconnect's quotas and deadline
- snapshot construction and state installation serialize per placement, off the row path
- an ACK root locks only its single terminal sender transition

These sites are accepted for the contract and bound named above. A lock that merely makes shared
state convenient, protects immutable configuration, or repeats registry discovery on every batch
does not belong in this category.

Relay fan-out itself uses one bounded queue per consumer. Publishers share no fan-out lock;
capacity and receiver counts are atomic, and a publisher registers for notification only when a
consumer queue is full. Removing one consumer does not stop the others, and changing capacity does
not discard batches already queued.

## Ratchet And Review

`just ratchet` keeps the synchronization debt from growing. The
`data_plane_lock_acquisitions` count scans the runtime, connector, interconnect, ACK, and metrics
owners for `.lock()`, `.read()`, `.write()`, and `.entry()` calls outside tests. Its checked-in
value in `debt-baseline.json` is a ceiling: the count may fall and may never rise. When it falls,
`just ratchet --update` records the lower baseline in the same change.

The count is deliberately textual and broad. It includes lifecycle synchronization, cold
registration, task-local collection `entry()` calls, and I/O methods named `read` or `write`.
Passing the ratchet therefore proves only that the total did not increase. Review classifies every
new site on its own, even when another deletion hides it in the net count.

Use the site listing while reviewing:

```text
just ratchet --show data_plane_lock_acquisitions
```

For every new or moved site, the reviewer establishes all of the following:

1. **Frequency.** Trace its callers and decide whether it can run per record, row, batch, remote
   frame, ACK share, or steady poll iteration. A site on one of those paths is rejected unless it
   is an ordering fence or task-owned mutation described by this chapter.
2. **Owner.** Identify the one component that changes the state. Immutable reconfiguration uses a
   whole-value publication, a scalar uses an atomic, a stable dependency is bound when the task
   starts, and branch-local state belongs directly to the branch task.
3. **Lookup behavior.** A steady keyed lookup must not take `entry()`. Resolve and retain the handle
   when possible; otherwise perform a borrowed lookup and reserve `entry()` for a cold or racing
   installation whose single-winner property is part of the contract.
4. **Fence contract.** An allowed ordering fence names the order it preserves, the exact key that
   limits its scope, the capacity or deadline that bounds waiting, and whether any guard crosses an
   await. Unrelated branches, relays, destinations, or assignments must still progress.
5. **Lifecycle separation.** Startup, registration, schedule application, snapshot sealing, and
   teardown may synchronize with their peers, but their guards must not leak into a hot callback or
   be held while awaiting data-plane work.
6. **Mechanical result.** Run `just ratchet`; inspect the site list when the count changes; update
   the baseline only when the count fell. A lower aggregate count does not make a newly introduced
   hot-path lock acceptable.

The companion `write_once_rwlock_fields` count rejects names and shared references stored as
`RwLock<Option<...>>`. Such a field states that readers should coordinate forever around a value
whose actual lifecycle is publication. The current shape uses an atomic optional reference and
deletes the lock-backed form.
