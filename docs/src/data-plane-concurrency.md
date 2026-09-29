# Data-Plane Concurrency

Nervix keeps the paths that carry records, batches, remote relay frames, and acknowledgements free
from shared coordination that is unrelated to the record being processed. This is the
contentionless data-plane rule. It applies after a runtime task has started and its graph, routing,
state, metric, and connector handles have been resolved.

[Execution Plans](./execution-plans.md) describes the revision that installs and publishes those
handles before record and batch work begins.

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
| Domain routing snapshot | Each domain retains one stable publication handle across execution rebuilds. Schedule application stages and replaces the relay services, schemas, branch declarations, materialized-state ownership, lookups, UDFs, codecs, signaling protocols, and the complete map of bound processor plans together. Each plan carries a typed identity and prepared VM and WASM artifacts for one installed node revision. | A task sees the old routing revision or the new routing revision, never a mixture of their fields. Long-lived tasks use a local pointer cache instead of returning to the domain execution registry per batch. Existing processor branches compare typed plan identities between batches; new branches resolve their template from the same published map. |
| Ingestor and reingestor programs | Schedule application installs the domain's decision-layer entrypoint plans inside its complete typed execution revision. An ingestor start binds its node filter and routes against the staged routing revision into one immutable route set before its source opens. A reingestor start prepares the branched entrypoints of its routes and binds each input's source filter, its node filter and its routes before any entrypoint or input task starts. A running relay registers the consumer of a swapped-in input only after that. | Every source instance, endpoint request and ingest group of one ingestor shares the same bound route allocation, and a reingestor input task owns its bound programs. Neither compiles a program on the hot path nor returns to the domain execution registry per batch. A reingestor start that fails leaves no entrypoint, task or registered consumer behind, so a relay never refuses attached delivery for a consumer that has no receiver. |
| Emitter execution plans | The decision layer prepares the domain's typed sink and ordered source edges with lowered expressions from one schedule revision; schedule application installs them in that revision. Startup, swaps and relocation bind a selected plan to local mounts, schemas and UDFs; relay inputs are registered during those transitions. Remote consumer edges come from the same plans. | An emitter task owns its bound programs and sink configuration for its lifetime. Per-batch work does not read the schedule or client Models; sink retries reuse the typed configuration. A dynamic flush change sends only its new flush policy to the running task. |
| Message-error route plans | The registry selects each DLQ route from the committed schedule. Domain installation binds its schemas, branch declaration, flush contract, relay target and SET program before tasks start. Schedule application replaces the complete bound route map under the domain execution's write guard with the planned nodes, placement, entrypoints and emitters of one typed revision. | A failed record looks up one prepared route under the domain execution read guard and releases the guard before running its VM program or delivery. A buffered route compares the bound plan allocation with its running delivery task; a replacement drains the preceding task and starts a task with the new target and cadence. |
| Node identity and remote dispatcher | The node runtime publishes this once after cluster join, when the authenticated interconnect and process incarnation are known. Relay boundaries created afterwards retain the same dispatcher handle. | Readers borrow the stable node identity, incarnation, transport, admission service, and ACK registry without a write-once lock or repeated name allocation. |
| Relay owner state | Each relay boundary publishes its scheduled owner, installed owner buffer, remote runtime-consumer set, and immutable branch-reset gate set. Schedule and relay lifecycle operations replace these values at their cutover points. | A batch borrows the current owner and buffer, then takes permits only from reset gates whose typed scope selects its branch. Multi-step ownership changes use the whole-relay dispatch gate described below so teardown cannot race an admitted dispatch. |
| Subscription interest | The cluster live-state watcher rebuilds an immutable index from domain and relay to interested node incarnations and advertisement versions whenever gossip changes. | A relay owner performs borrowed lookups in one published index. It neither formats gossip keys nor waits on the gossip mutex per batch. Subscription creation waits until every live node has observed the exact subscriber incarnation and at least the current advertisement version before reporting success. |
| Clock installation | Each domain-clock lifecycle on each node publishes the complete missing, stopped, uninstalled, unpaced, or paced installation. | A read validates its bound lifecycle generation against one installation, then advances that installation's nondecreasing timestamp watermark atomically. A same-generation replacement retains the watermark; a different generation cannot be clamped by a stale reader. |
| Accepted clock progress | Each runtime domain publishes its newest accepted generation, tick id, logical boundary, and authority UTC observation through a watch. | The watch serializes comparison and replacement, so concurrent progress deliveries cannot regress the id. Generation changes and stops publish absence. A session observer subscribes before reading, retains a sender until it observes domain removal, and uses an execution snapshot only to add its serving node's logical reading to a tick frame. |
| Committed domain installation | Each node's runtime counts its installations of the committed domain states through a watch. An installation advances the count only after it has inserted, updated, or removed every domain. | A session attach waits for the first count before it looks up a domain, so a node that is still starting never refuses a domain the cluster has. Because the count advances after the domains are in place, the wait never releases before they are observable. |
| Runtime-state assignment | Each state placement publishes one packed atomic binding containing its generation and capability. Replication roles are a separate immutable published snapshot. | A per-message operation admits itself, compares the exact binding it was granted, and proceeds only while that generation still grants the required capability. It never takes the assignment barrier. |
| Ingestor quiesce decision | Each ingestor publishes the declared and pending modes, active causes, source support, and derived intake decision as one value. Concurrent lifecycle changes derive their replacement from the current publication. | Polling and per-message intake make one load to decide whether to dispatch, suspend, skip, buffer, drop, or reject. A source host retains its last observed publication across dispatch awaits; its change wait registers before comparing that publication with the current one, so an engagement or release in the gap wakes it. The retained-payload lock is reached only after the published decision selects buffering. |
| Metric series handles | Each relay, node, ingestor, emitter, or concrete branch resolves its label set, internal series, and Prometheus child when its owning task or branch is created. | Recording uses the retained series directly. Counters update atomically; a histogram records through its already-resolved per-series accumulator without a registry lookup, key construction, or map guard. A node input folds a whole batch's delivery latencies in one kernel pass over the batch's high-watermark column before recording, so each latency series locks its accumulator and reads the wall clock once per batch rather than once per row, and its Prometheus child takes the batch in one flush. Whether a series was ever observed is an atomic flag set under that lock and read without it. Registration and removal stay on lifecycle paths. |
| MQTT broker packet limit | Each MQTT sink's event-loop task stores the Maximum Packet Size of every `CONNACK` its client receives, or the broker's silence about one, as one atomic scalar. A reconnect replaces it. | A publish loads the scalar once per record to reject a packet the broker would refuse before handing it to the client. The value stands alone, so relaxed ordering is its whole contract, and a record published before the first `CONNACK` is measured only against the protocol's largest packet. |

An atomic replacement gives consistency for the value it publishes. A transition involving several
owners still needs a protocol. Graph and schedule changes use quiescence and relay gates; clock and
state operations carry generations; subscription creation uses a visibility handshake. Publication
removes read-side contention without weakening those transition contracts.

The control plane classifies each committed schedule change and builds a complete typed execution
revision before handing it to the runtime. The revision keeps placement, state identity, handoff
fingerprint, and all node plans together. Runtime application reads that revision for incremental
swaps, relocation, remote consumer edges, and full or passive rebuilds. A failed application keeps
the last successfully applied schedule as the predecessor for its retry; a stale revision cannot
replace that predecessor. Each planned domain also carries that predecessor's source identity. If
an earlier attempt left a domain partly or wholly installed at another revision, the runtime
rebuilds it instead of applying a delta against the wrong predecessor.

Schedule application prepares every changed local processor plan before publication. A preparation
failure leaves the preceding routing revision observable. Publication swaps the complete typed
routing snapshot with its installed execution revision. Unchanged processor specifications, schema
fingerprints and resolved branch contracts keep their exact plan allocation, including prepared
programs and compiled WASM modules.

## Mutable Execution State

Per-row mutation is kept close to the lane that orders it. A global concurrent map is an index into
those lanes, not the owner of their inner state. Once a task has found its lane, later mutation does
not repeatedly acquire the registry guard.

### Emitter payload assembly

One emitter task owns the released carriers and their flush order. Its batch packer borrows each
carrier's source relay, concrete branch key and ordered metadata while it prepares selected Arrow
rows. It clones the Arc-backed Arrow batch once per carrier passed to the packer; no row
acquires a lock or increments an Arc reference count for source identity. The open candidate holds
at most `BATCH MAX MESSAGES` prepared members and is sealed on metadata or source changes, a failed
member, or the message-count limit. Each encoding attempt uses the codec's bounded writer under
`MAX SIZE`. The emitter retains original batch and row positions for acknowledgements and errors.

A payload offered to the sink stays in the same task's buffer until the sink answers for it. The
task marks each member row prepared, and the one answer for the payload resolves every member: a
row resolves once, so neither a repeated answer nor a later attempt resolves its acknowledgement
share again. An HTTP emitter's prepared request is such a payload with exactly one member,
retained by the same owner with its request fields and body bytes, so its answers follow the same
rules, and so is an OTEL emitter's Export request, retained with the protobuf bytes its connector
prepared and the rows they carry. No lock guards this state; only the emitter task touches its
buffer, and resolving a member is one operation on the lock-free ACK tree. A confirmed payload's members are acknowledged
in the same step that releases the payload, while a rejected payload's members return to pending
until their message errors are delivered, so an attempt its stop deadline cuts short leaves every
member either resolved or still owned by the buffer.

Terminal teardown races the whole emitter task against its domain shutdown signal. It can cancel
an in-flight connector await after the graph drain budget expires, dropping the task's volatile
payloads and ACK guards together. That cancellation cannot resolve a member as delivered; an
acknowledged source retains its own redelivery boundary. Entity-pause swaps do not use this
terminal cancellation: the old task must complete its deadline-bounded stop before the new task
starts.
The Shuttle check over the emitter's prepared payload and ACK owner races a connector await with
the terminal shutdown signal and asserts that dropping an unanswered request leaves its attached
source unacknowledged.

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
The publication shares the retained input and aggregate-argument Arrow columns through row views.
The snapshot task seals those views as bounded Arrow sections on the bulk executor, while a
separate bounded typed section carries each group of histogram delayed removals. Encoding never
materializes a scalar-field copy of the retained payload. Restore opens the sections on the bulk
executor and reuses their columns to rebuild exact accumulators and sketch panes.

Evicting a concrete branch resets its retained window rows and aggregate structures before the
branch task's final publication. The published generation is empty, so a later appearance of the
same branch key cannot inherit the evicted window or its sketch panes. After the final checkpoint,
the owner releases the evicted branch's in-memory publication. A branch that appears without a
restored lifecycle entry also publishes an empty initial window, even if a previous lifetime of its
key left a checkpoint behind. Stopping a branch for an ownership handoff follows the normal
finalization path and publishes its retained window instead.
The branch lifecycle and window checkpoint carry the same incarnation, assigned when the concrete
branch appears. A restore whose incarnations differ begins with empty window state and marks it
for publication, so a delayed checkpoint from the preceding lifetime cannot restore its panes.

A WASM guest instance likewise belongs to one branch task. At the end of every guest callback the
task asks the guest to save its computation state and checkpoints the returned buffer: it writes it
to stable storage, waits for the replicas the checkpoint names, and only then publishes it as the
committed checkpoint a recreated instance restores. The branch runs no further callback until that
checkpoint completes or fails. The publications retain the saved buffer rather than copying it for
every persistence or replication reader. Input buffered by the guest host and ACK tokens remain
execution state and are never included in a guest save. See
[WASM State And Recovery](./wasm-state.md#the-checkpoint) for the checkpoint's stages.

Each branch publishes checkpoint revision, boundary, and stage together through one immutable
observation. An inspection read samples that publication and replica progress without taking the
branch task's execution lane or advancing durability. A failure before guest state was captured is
an explicit observation without a new revision; any earlier committed checkpoint remains the
restore source. The published observation contains no guest bytes.

Every branch save is addressed by the guest-state generation in the committed schedule. Forced
recovery publishes a new generation with the replacement schedule, so a late save, replica
installation, or recovered checkpoint from the generation it replaced cannot address current
state. The state store performs durable guest-state writes on its storage workers; the async worker
that owns the branch never performs that storage operation synchronously.

A coordinated reset enters the selected branch task through its existing supervisor command lane.
That lane is the serialization point with input callbacks, timeout callbacks, branch eviction, and
task replacement. Preparation takes ownership of the selected task, reaches its stop boundary, and
keeps the handoff only until either pre-publication abort restores it or generation publication
makes it obsolete. Sibling branch tasks keep their own lanes and continue running. After
publication, the supervisor creates fresh tasks and waits for their initial checkpoints before it
drops the retained old tasks and accepts completion.

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
pending count, and each completion resolves one share with a compare-and-swap. A second atomic word
encodes either a typed tracking state with its active-share count or the completed state; its
reserved raw representation stays inside the encoding's owner. That state records whether shares
still count against ownership handoff; a message parked on materialized `REQUIRED WAIT` does not
keep a handoff blocked.

A handle owns an active share only when its attachment reserved a pending share. An attachment that
finds the root complete may observe that root, but cannot park or remove a share owned by another
handle. Resolution removes the owned pending share before changing active-share tracking, so a
terminal transition cannot make an unsuccessful attachment look like active work.

Root trackers count attempts rather than individual shares. Their transition ordering may briefly
overcount but cannot let handoff miss active work. Exactly one terminal transition takes the
one-shot sender from its slot and releases that guard before notifying the waiter. ACK trees,
shares, and trackers are volatile hot-path state and are never persisted.

A WASM branch holds back the success of the inputs a guest callback decided with one more attached
share per input. The branch task owns those shares for the length of the callback's checkpoint and
resolves them itself: it releases them when the checkpoint completes and negatively acknowledges
them when it fails. Holding them needs no lock and no shared registry.

A share a node forwarded to another node with a relay delivery waits in the node's registry of
forwarded acknowledgements under its registration number, beside two atomics: whether the delivery
was admitted, and how many sweeps found no report from the receiver since its last one. A report,
the admission, and the once-a-second sweep change them through a borrowed lookup, under the shard's
shared lock. The sweep fails a share only through an exclusive removal whose predicate rechecks the
count, so a report that took the shared lock first keeps the share, and a report after the removal
finds no entry. A terminal outcome, the delivery's own failure, and the sweep each take the share
out of the registry with one removal, so exactly one of them resolves it. The atomics establish no
ordering across locations; the shard lock orders the recheck against every earlier report.

### Client ingestor endpoints

Each client ingestor a node executes has one endpoint task that owns every producer attached to it:
the batches each queued, the round-robin order among them, the batches handed to the execution's
admission worker or admitted and awaiting acknowledgement, and whether admission is open. Producers,
the admission worker, the quiesce watch, and lifecycle changes reach that state only as commands on
the task's channel, applied in the order they were sent, so nothing locks it. An execution is installed before its admission worker and quiesce watch start, so the watch's
first report, which opens admission, is applied after the installation.

The admission worker takes one batch at a time over a channel of one. A worker that stops refuses
the batch it was handed and never took, and closes the channel so that the endpoint refuses any
later batch itself. The worker's task is joined before the endpoint learns that the execution
stopped, so a batch the endpoint still holds then was taken by a worker that was aborted when its
stop outlasted the grace period, possibly while dispatching it: its outcome is unknown, never
refused. The endpoint task awaits each admitted batch's acknowledgement itself, among its commands
and before further ones once it resolves, so an ending endpoint leaves no task behind, and a
resolution for an attachment that already ended finds nothing to answer.

A producer's turn gives its next queued batch a slot of the window whether or not the worker is
free, so a busy worker never passes a producer over. A local producer's batch is then ready for the
worker. A batch that another node forwards first waits among the producer's batches awaiting
clearance while the endpoint asks the serving node over the producer's own clearance channel. The
clearance returns as a command, and clearances arrive in the order they were requested, so the one
that arrives names the oldest batch awaiting it, or a batch the endpoint has already refused, which
it ignores; the cleared batch is then ready for the worker too. A free worker takes a ready batch,
which already holds its slot. A refusal, a close, an end, or a detach answers or drops the batches
still awaiting clearance or the worker, which never reached it, and releases their slots in the same
command, so a lost serving node never leaks a slot of the window.

After each command the task publishes its producer, outstanding, and window counts into plain
atomics that `DESCRIBE` reads, and into metric gauges it resolved once. Each count is exact when
written, and a reader may see one count of a change before another.

The node's producer byte budget is one atomic counter. A reservation adds its bytes with a
compare-and-swap that refuses to pass the budget, and dropping the reservation subtracts exactly
those bytes, so concurrent opens through different sessions and links never overcommit it.

A session's producer credit is held under a short mutex, taken by the receive loop when a batch
arrives and returned by the producer's task before it queues that batch's reply, so a client that
sends only after reading a reply always finds room. The mutex never crosses an await.

### Node quiesce accounting

Every entity on a node keeps one set of quiesce counts. A drain reads them to decide whether the
node still holds work, and the hot paths that take a message in, collect it, park it on required
materialized state, buffer its output, or finish it adjust them.

Each total a drain reads is a counter of its own: everything the node holds, and the admitted
subset of it that a local drain waits for. A total summed across several counters at read time can
miss an item that is between two of them, because the reader loads the count the item has already
left and then the count it has not yet joined. A drain that misses the last item reports a node
holding no work while it still holds one, and an entity gate that believes that alters the entity
under the message. Messages parked on `REQUIRED WAIT` and outstanding force-flush obligations are
counted on their own for the drain's report, and neither exchanges items with the admitted count.

An adjustment that moves work between the counts raises every count that rises before it lowers any
count that falls, so a drain reading during the move sees at least the work the node holds. A
processor publishes its collected inputs, parked messages, and buffered outputs together for the
same reason: a batch that left an input collector for an output buffer was admitted work throughout
the move. Overcounting only delays a drain, while undercounting lets one conclude early, so the
ordering is chosen in that direction. The counts are hot-path scalars and are never persisted.

A task that must not publish while an ownership handoff has frozen its entity observes that freeze
through one watch, which registers for the next freeze change before it reads the freeze. A release
landing between a read and a registration wakes nothing, because the waiter does not exist yet, and
a task that missed one keeps a stale freeze that disables exactly the force-flush and deadline arms
that would have woken it again. The release lifts the freeze before it wakes anyone, so a waiter
that rereads it sees the entity thawed rather than parking on a change that has already happened.

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

A WASM state reset adds a narrower gate without putting a lock on relay dispatch. Under a short
whole-relay publication fence, the relay atomically replaces an immutable list of scoped reset
gates. Dispatch loads that list once, compares the batch's branch fingerprint with each typed scope,
and waits only on matching gates. Removing a lease atomically republishes the list and releases its
gate. This gives publication and dispatch one total order while unrelated branches neither acquire
shared locks nor wait for the reset.

### Client batch admission fence

A client batch is validated before it is dispatched, and a quiesce can engage in between. The
admission worker therefore tracks the batch's ACK root with the ingestor's drain accounting first,
and only then reads the quiesce publication. Either the read comes before the engagement, so the
drain that follows counts the root and waits for it, or the read observes the engagement and the
batch is refused with its root resolved before anything was dispatched under it. No batch can be
dispatched after a drain concluded that the ingestor held no admitted work.

### Assignment generations

A runtime-state assignment packs its generation and capability into one atomic word. A hot
operation increments the admission counter for that generation before comparing its token with the
published binding. A rebind serializes with other rebinds, publishes the successor binding, and
waits only for operations admitted under the generation it superseded. Even and odd generation
counters let work admitted under the successor proceed without extending that wait.

This fence prevents a former owner from mutating or describing state after reassignment. Snapshot
capture holds the assignment barrier so its observed binding stays current for the whole capture.
Whole-state installation is authorized exclusively under its exact binding: it cannot overlap a
capture or an admitted origination. Ordinary record updates use atomic admission, continue while a
capture merely holds the barrier, and never take that barrier.

## Bounded Synchronization Outside The Open Path

Some synchronization remains after a hot operation has selected a state that explicitly requires
it:

- a quiesced ingestor locks only the bounded retained-payload buffer selected by its declared
  `MAX SIZE` policy
- one concrete processor branch serializes its own batches, while other branches remain
  independent
- one already-resolved histogram series serializes its bounded accumulator update, once per
  recording: a batch's delivery latencies arrive as one folded recording, so the lock is taken once
  per batch; counter series are atomic
- one inbound relay attempt serializes its small receipt, admission, rejection, and cancellation
  transition within the interconnect's quotas and deadline
- snapshot construction and state installation serialize per placement, off the row path
- a WASM branch waits for its own checkpoint after each guest callback, bounded by the checkpoint
  deadline. The state store's durability barrier lets one writer at a time run a storage
  synchronization for every writer waiting, through one atomic runner slot and a ticket watermark;
  it holds no lock across that wait, and the synchronization runs on the storage workers. Replica
  progress reaches the waiting branch through its own state's notification.
- an ACK root locks only its single terminal sender transition

These sites are accepted for the contract and bound named above. A lock that merely makes shared
state convenient, protects immutable configuration, or repeats registry discovery on every batch
does not belong in this category.

The [simulated relay fault checks](./interconnect-simulation.md#relay-reconciliation-and-cancellation)
drive the production receipt, admission, and cancellation APIs through authenticated connections.
They synchronize on the received Arrow batch and verify the same attempt cannot be admitted twice
after a lost reply. Cancellation before grant and while receipt or reconnection is unresolved must
win the attempt's existing admission fence before the runtime can admit it. A cancellation after
admission returns the admitted outcome.

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

## Execution-Sensitive Primitives

Every atomic, ordering and fence in Nervix comes from `nervix_primitives::sync::atomic`. The
`nervix-primitives` crate sits below the vocabulary and selects that family, together with the
threads a model runs, for the build's execution mode: ordinary execution, Shuttle, Loom or Turmoil.
A build uses one mode across its whole dependency graph. Every package that owns a `shuttle`, `loom`
or `turmoil` feature forwards it to the primitive crate, and Cargo unifies that crate's features, so
a vocabulary type, an engine and the server compiled into one test binary all use the same backend.
Selection depends only on features, never on `cfg(test)`. Enabling two modes fails to compile with a
diagnostic that names both, including when two different dependencies each enable one.

Ordinary execution pays nothing for the boundary: each path re-exports the standard library item
itself, with no wrapper, allocation, dispatch or scheduling point, so an atomic on a hot path costs
exactly what it did before. The atomic surface is portable and builds for the browser console.
Operating-system threads are the explicit `native` capability, and asking for a capability or a
mode the target cannot provide is a compile error rather than another implementation.

| Surface | Path | Ordinary | Shuttle | Loom | Turmoil |
| --- | --- | --- | --- | --- | --- |
| Atomic values, `Ordering`, `fence` | `nervix_primitives::sync::atomic` | The standard library's | Modeled: every operation is a scheduling point, and every ordering behaves as `SeqCst` | Modeled: explored under the C11 orderings Loom supports | The standard library's, on the simulated host's thread |
| Threads, with `native` | `nervix_primitives::thread` | Operating-system threads | Modeled threads | Modeled threads | Operating-system threads outside the simulation |
| Unmodeled atomics | `nervix_primitives::unmodeled::sync::atomic` | The standard library's | Outside the model | Outside the model | The standard library's |

A modeled primitive exists only inside a run of its model. Using one outside that run is a test
configuration failure the backend reports, and nothing falls back to a real primitive. A real atomic
that must stay outside every model is reached through the unmodeled path, and each use needs a
permission in `crates/primitives/unmodeled-permissions.toml` that names the file, the items, the
owner, why a real atomic is required, and what that leaves unverified:

| Owner | Why the atomic is real | What stays outside every check |
| --- | --- | --- |
| The Shuttle runners of execution, the interconnect, the server and the Rust client, and the Loom runner | Their statistics span every model execution they start and are read after the last one | Nothing a check claims; they are runner bookkeeping |
| The WASM runtime's epoch driver | Its stop flag is read by an operating-system thread no model runs | When the epoch thread observes shutdown |
| The VM benchmarks' allocation probe | A global allocator counts allocations made on every thread | Nothing; the benchmark claims nothing about synchronization |
| The application unit-test fixtures | Test databases and node ports must differ across every unit test the process runs in parallel, so their identities outlive each test | Nothing a check claims; no model builds the fixtures |
| The records of the relay gate and fan-out, entity gate, emitter record-write, durability barrier, WASM checkpoint, source host-loop, stream-slot, retained-archive and client ingestor Shuttle checks | A record changes in the same scheduling step as the operation it records, so recording adds no scheduling point | Nothing the owner does: records observe and never synchronize, and the owners' own atomics are modeled |

A real atomic never carries the protocol under test, chooses its branches, supplies its wakeups or
establishes an ordering an assertion relies on.

A selected atomic belongs to the model execution that constructs it, so it never lives in a
`static`. A static is constructed once per process and would carry its state from one execution into
the next, and Loom's atomics have no const constructor, so a crate that declares one does not build
with Loom at all. Process-wide state lives on the owner whose lifetime it has instead: a node's
session service owns the draws its subscriptions sample with, and a node's Raft network owns the
identities of the snapshot transfers it sends. A count a unit test reads is kept per thread, and
state that must outlive every test and model is a real atomic under a permission, which may live in
a `static`.

`just validate-primitive-boundary` rejects every other path to a backend's atomics however it is
spelled, an unmodeled use without its permission, and a permission nothing uses. It also rejects a
`static`, including one a `thread_local!` declares, whose declared type names a selected atomic,
directly or through a wrapper, an array, a reference, a module path or a local type alias, and a
`static` or `const` initializer, `const fn` or `const` block that constructs one. It reads declared
types and constructions, so a struct holding an atomic that a static builds lazily is left to review.
`just validate-loom-dependencies` keeps Loom out of every ordinary dependency graph, and
`just validate-execution-mode-conflicts` requires the combined-mode diagnostic.

The other primitive families still reach their libraries through their current access paths. Each
joins the boundary as one complete move, with every consumer migrated and its enforcement extended,
and until then keeps the behavior this chapter describes:

| Family | Current access path |
| --- | --- |
| Synchronous and asynchronous synchronization | `parking_lot`, `tokio::sync`, `tokio-util`, `tokio-stream` and `dashmap`, selected for Shuttle by feature-gated `extern crate` aliases in the server, execution, interconnect, consensus, WASM host, client and connector crates |
| Publication and concurrent maps | The `nervix_execution::sync` adapters over `arc-swap` and `dashmap`, which add Shuttle scheduling points around each operation |
| Tasks and threads | Tokio's task API through the same aliases; `nervix_execution::sync` for abort-on-drop handles, cancellation-token identity and the synchronous yield; `std::thread` directly, and a Shuttle alias in termination-signal supervision |
| Shared ownership | `triomphe::Arc`, and `std::sync::Arc` where an external API such as a Tokio semaphore requires it; opaque in every mode |
| Monotonic scheduling | Tokio's timers and instants, within the existing clock permissions |
| Networking | Tokio's sockets, or Turmoil's when the interconnect's `turmoil` feature is enabled |

## Deterministic Concurrency Verification

The ordering contracts above are checked against the production owners under Shuttle. A check
runs several tasks or threads through one scheduler, which chooses an interleaving at visible
synchronization points. It can expose a lost notification, an early drain, a double completion, or
a stale generation without depending on which OS thread happened to run first. A deadlock is a
failed check. The model is the surrounding schedule and test data; the protocol under test is the
same type used by the data plane, not a copied implementation of it.

Shuttle does not model elapsed time. A wrapped sleep yields once; wrapped timeouts do not measure
their deadlines and fire only when a test triggers them by task label. `Instant::now` still reads
the wall clock, while paused-time controls do not advance a simulated clock. Checks of deadline
ordering therefore use an already-passed or far-future instant and explicitly choose whether the
timeout wins. Tokio paused-time tests retain responsibility for actual timer behavior. No
concurrency check uses a wall-clock bound, sleep poll, or `recv_timeout` to establish progress.

Only scheduler-visible operations create interleavings. The Shuttle feature selects wrapped
Tokio, Tokio Util, Tokio Stream, parking_lot, and DashMap dependencies, forwarded through the
server, interconnect, execution, and crates whose public synchronization types cross those
boundaries. With the feature off, these wrappers re-export the real libraries. Feature-gated
crate imports keep the product's `tokio`, `parking_lot`, and `dashmap` names; the ordinary build
uses their normal behavior. The feature changes the test execution environment, not the public
protocol. Edge I/O, networking, filesystem access, and signals remain real re-exports and are
outside a Shuttle schedule. Every package that owns the feature forwards it to the primitive
boundary, so every Nervix atomic in a Shuttle build is Shuttle's, including the ones inside
vocabulary types such as `AtomicTimestamp` and inside dependencies such as the execution crate's
cancellation. A check's own records use unmodeled atomics on purpose, under the permissions
above, so that recording an operation adds no scheduling point to it.

Some primitives are opaque to Shuttle: `arc-swap`, `async-broadcast`, `triomphe`, and
`futures-channel` have no scheduler wrapper. Nervix routes `ArcSwap` and `ArcSwapOption` loads and
stores, plus `ArcSwap` compare-and-swap and read-copy-update, through its execution synchronization
boundary. That boundary yields under Shuttle and calls the underlying primitive directly
otherwise. It also supplies a scheduler-visible synchronous yield for admission spin waits.
A check may claim an ordering around an opaque primitive only when its relevant calls have
visible scheduling points. Shuttle cannot interrupt an arbitrary instruction inside it.

Even a wrapped Tokio primitive can hide a scheduling window. `shuttle-tokio` currently keeps
`Notify` waiter registration behind a standard-library mutex, so registering `notified()` is not
a scheduling point. A read followed by registration may therefore look safe in a check even
though a release between the two would be lost in production. The corresponding `watch`
`borrow()`/`subscribe()` window has the same verification limit. The ownership-handoff freeze
check exercises release-before-wake, but does not prove its separate register-before-read order.
That order remains a production owner contract until both sides of the race are scheduler-visible.

Shuttle explores sequentially consistent schedules. It cannot prove that a chosen `Relaxed`,
`Acquire`, or `Release` ordering is sufficient on weak-memory hardware; a claim that depends on one
is a Loom model, described under [Memory-ordering models](#memory-ordering-models). Cucumber
remains the public behavior test: when a scheduling defect affects an NSPL operation, runtime
output, or process outcome, its Shuttle regression is paired with a scenario through that
interface. A scenario cannot exhaust the interleavings of an in-process protocol, and neither a
Shuttle check nor a Loom model can verify the whole cluster, socket, disk, or browser path.

### Check contract and runner

Each check names the invariant in its test name, drives competing operations on the production
owner, and asserts the state at meaningful transitions or after all participants have joined.
Use an explicit handshake or scheduler-visible yield to position a race. When both the decision
read and waiter registration are scheduler-visible, a missed notification leaves the waiter
pending and Shuttle reports the resulting deadlock. Name a bounded number of participants so the
search remains reviewable; use bounded depth-first search for small races and random plus
probabilistic concurrency testing (PCT) for larger ones. The server's shared runner supplies
random, PCT, and bounded DFS modes; interconnect, execution, and the Rust client use random and
PCT. The runner caps each schedule at 10,000 steps. Individual checks choose their iteration counts and PCT
depth; `SHUTTLE_REPORT_STEPS=1` reports the highest observed step count when tuning a check. A
step cap is an exploration bound, not a product timeout.

`just test-shuttle` runs only library tests whose full names contain `shuttle_`, one test per
process, in `nervix-execution`, `nervix-interconnect`, `nervix-client-core`, and `nervix-server`. It then repeats each
package under Shuttle's uncontrolled-nondeterminism detector. The recipe uses the repository's
kache-backed build and prepares the server's test dependencies; `just test` continues to run the
ordinary suite. CI runs `just test-shuttle` and uploads `target/shuttle-failures` when a check
fails. `just test-shuttle <filter>` selects checks whose full name contains the filter. The
runner stores a failing schedule under
`target/shuttle-failures/<package>/<fully-qualified-test-name>/`; replay uses that path and exact
test name. See the command recipe in [Developing Nervix](./developing-nervix.md).

### Protocols and their checks

The checks below hold the scheduler-visible parts of each protocol to their invariants. Test
names are given relative to their owning module; the `shuttle` feature selects the modeled build.
A family of names means each member runs independently through the recipe.

| Protocol | Invariant and check |
| --- | --- |
| Execution budgets and storage jobs (`crates/execution/src/tests.rs`) | `shuttle_saturated_class_keeps_live_reservations_within_each_class_capacity` keeps live reservations within each class capacity; `shuttle_occupied_bulk_execution_leaves_control_execution_untouched` keeps bulk saturation from charging control; `shuttle_queued_job_drop_releases_its_reservation_and_exact_queue_slot` and `shuttle_running_job_observes_cancellation_and_keeps_its_charge_until_exit` balance queue slots and permits across drop and cancellation; `shuttle_full_wait_queue_is_exact_typed_backpressure` checks a full wait queue's typed rejection; `shuttle_consensus_storage_preserves_admission_order_and_returns_every_permit` checks storage admission order and permit return. |
| Force-flush obligations (`src/runtime/force_flush.rs`) | `shuttle_two_participant_generation_waits_for_every_obligation` prevents completion before all participants live at publication complete and redelivers a dropped, unhandled completion; `shuttle_stale_completions_never_clear_a_newer_generation` prevents an old completion from clearing new work; `shuttle_published_generation_wakes_a_waiting_participant` catches a lost publication wakeup; `shuttle_participant_lifecycle_balances_obligations_through_close` balances `pending()` across subscribe, request, participant drop, and close. |
| Ingestor intake (`src/runtime/ingestors/source_shuttle_tests.rs`; `src/runtime/ingestor_quiesce.rs`) | `shuttle_broker_source_observes_engagement_during_dispatch`, `shuttle_paced_source_observes_engagement_during_dispatch`, and `shuttle_request_source_observes_engagement_during_dispatch` exercise host-loop engagement for memory pressure, entity gate, handoff, and shutdown: after engagement returns, no further payload dispatches, and a change during dispatch is observed. `shuttle_an_open_control_answers_intake_without_waiting_on_retained_payloads` keeps the open decision independent of a retained-payload lock. |
| Relay dispatch gate and fan-out (`src/runtime/relay_channel_shuttle_tests.rs`) | `shuttle_dispatch_permits_never_overlap_a_quiescent_lease_and_release_frees_every_waiter`, `shuttle_overlapping_gate_leases_all_release_before_dispatch_resumes`, `shuttle_expired_gate_fence_frees_every_waiter_without_reporting_quiescence`, `shuttle_canceled_dispatch_returns_its_permit_to_the_gate_fence`, and `shuttle_dispatches_parked_behind_a_lease_wake_only_on_its_release` hold the fence, lease, expiry, cancellation, and waiter contract; in-flight dispatches drain before quiescence. `shuttle_capacity_shrink_keeps_buffered_batches_and_wakes_publishers_after_the_drain`, `shuttle_capacity_growth_admits_waiting_publishers_without_a_take`, `shuttle_publishers_wait_for_the_slowest_consumer_and_skip_consumers_that_leave`, and `shuttle_losing_every_consumer_delivers_or_returns_the_waiting_batch` keep queued batches across capacity changes, release waiting publishers, and return or deliver each batch when receivers leave. |
| Assignment authority and state updates (`src/runtime/state_store_shuttle_tests.rs`, `materialized_state.rs`, `kafka_offset_state.rs`) | `shuttle_a_rebind_yields_until_the_operation_admitted_under_its_replaced_binding_finishes` and `shuttle_no_operation_admitted_under_a_superseded_binding_outlives_its_superseding_rebind` fence admitted work even when generations reuse an even or odd counter. `shuttle_snapshot_installation_never_overlaps_origination_or_a_capture` keeps exclusive installation apart from originators and captures. `shuttle_an_originator_update_proceeds_while_the_assignment_barrier_is_held` and `shuttle_a_committed_offset_proceeds_while_the_assignment_barrier_is_held` keep ordinary admitted updates independent of a capture's barrier. |
| ACK tree (`src/runtime_ack.rs`) | The `shuttle_tests` checks `concurrent_attachment_and_final_ack_leave_exact_tracking`, `concurrent_wait_and_active_ack_exempt_the_remaining_root`, `concurrent_wait_release_and_completion_leave_no_tracking`, `attachment_losing_its_reservation_to_completion_resolves_no_share`, `attachment_losing_its_reservation_to_completion_parks_no_share`, `wait_release_racing_the_last_active_ack_holds_domain_and_ingestor_handoff_once`, and `attachment_racing_the_last_active_share_into_wait_publishes_one_active_share` keep pending, active, and handoff counts exact across attachment and `REQUIRED WAIT`. `concurrent_success_and_failure_choose_one_terminal_transition`, `fan_out_across_ingestors_with_a_failing_root_resolves_each_root_once_with_exact_counts`, and `parked_and_fanned_out_roots_hold_exact_counts_at_every_quiescent_point` require one terminal result per root, one observed completion, and zero outstanding counts after resolution. |
| Forwarded acknowledgement silence (`src/runtime/remote_dispatch_shuttle_tests.rs`) | `shuttle_a_terminal_outcome_racing_the_final_sweep_resolves_the_share_once` races the receiver's terminal outcome against the sweep that would fail a forwarded share: exactly one of them removes it, and the root delivers that one's outcome. `shuttle_a_report_racing_the_final_sweep_keeps_the_share_it_reached` races a report against the same sweep: a report that reached the share keeps it pending with its root unresolved, and only a report that found it removed lets the sweep fail it. |
| Entity gate and node quiesce (`src/runtime/entity_gate_shuttle_tests.rs`) | `shuttle_an_entity_gate_hold_fences_every_relay_and_admits_no_work_until_it_is_released` requires admitted work to drain before quiescence and prevents new admission while closed. `shuttle_a_work_item_parked_for_materialized_state_is_never_missing_from_a_drain` and `shuttle_every_node_quiesce_gauge_withdraws_exactly_what_it_contributed` keep parked, buffered, and branch work counted without underflow and back to zero. `shuttle_every_engagement_waiter_wakes_and_exactly_one_release_takes_the_hold`, `shuttle_a_hold_dropped_before_its_fence_completes_reopens_every_relay_it_engaged`, `shuttle_a_failed_engagement_wakes_every_waiter_with_its_failure`, and `shuttle_releasing_an_ownership_handoff_wakes_every_waiter_frozen_by_it` cover release, drop, failure, and publication-before-wake. The last check does not exercise the separate waiter-registration gap described above. |
| Interconnect slots and membership (`crates/interconnect/src/connection/stream_slots/shuttle_checks.rs`, `request/shuttle_checks.rs`) | `management_drain_stops_leasing_and_waits_for_every_leased_slot`, `replication_drain_stops_leasing_and_waits_for_every_leased_slot`, `bulk_drain_stops_leasing_and_waits_for_every_leased_slot`, and `relay_drain_stops_leasing_and_waits_for_every_leased_slot` keep partition and subquota reservations isolated, forbid leases after drain starts, and wait for every lease to return. `racing_registrations_lose_no_handler_and_publish_each_name_once` prevents a lost handler registration and duplicate name. `a_membership_change_between_a_callers_check_and_its_wait_is_never_lost` prevents a missed discovery wakeup. |
| Shutdown and signals (`src/application/shutdown.rs`, `termination_signals.rs`) | `shuttle_racing_stop_requests_accept_exactly_one_and_keep_its_deadline` retains the first stop request and its deadline. `shuttle_phases_only_advance_and_every_completion_waiter_observes_the_one_outcome` keeps phase order and one completion. `shuttle_an_expired_deadline_and_a_repeated_signal_let_exactly_one_forced_exit_end_the_process` and `shuttle_a_repeated_signal_before_the_deadline_ends_the_process_with_the_status_of_that_signal` give one forced-exit claimant and the exit status of the cause that won. |
| Emitter batch payloads (`src/runtime/emitter_record_writes_shuttle_tests.rs`) | `shuttle_a_retried_payload_acknowledges_each_fanned_in_member_once_after_every_emitter` and `shuttle_a_sibling_failure_resolves_each_fanned_in_member_once_despite_a_retry` fan two source messages out to a batching emitter and a sibling: each source acknowledgement completes once, successfully only after both emitters confirmed it, and the retry writes the retained payload's first bytes. `shuttle_a_cancelled_attempt_leaves_each_member_to_resolve_once` cuts an attempt short at any point and requires the next one to write only unanswered payloads and deliver each rejected member's message error once. `shuttle_a_drain_never_finds_the_emitter_empty_while_a_member_is_retained` races a drain's reads against a stalled write and the force flush that repeats it. |
| Client ingestors (`src/runtime/client_ingestor_shuttle_tests.rs`) | `shuttle_racing_reservations_never_exceed_the_node_budget_and_return_every_byte` races opens that each need more than half the node's producer budget: at most one holds it at a time and every reservation returns its bytes. `shuttle_a_batch_racing_a_quiesce_is_either_counted_by_its_drain_or_refused_undispatched` races the admission fence against an engagement and its drain: no batch is dispatched after the drain concluded. `shuttle_a_closing_producer_answers_every_admitted_batch_once_before_its_release` and `shuttle_an_ending_endpoint_answers_every_batch_once_and_ends_its_producer_last` race a close or an endpoint end against the worker's admission reports and the batches' acknowledgements: every batch is answered exactly once, a close answers each with its real outcome before the release, and an end reports no admitted batch as not admitted and comes last. `shuttle_a_detach_racing_a_clearance_admits_only_a_cleared_batch_and_returns_its_slot` races a forwarded producer's detach against the clearance of its batch while a local producer waits for the window's one slot: the forwarded batch reaches the worker only after its clearance was recorded, and the local batch is admitted whichever comes first, so no slot leaks. `shuttle_an_end_racing_clearances_reports_a_batch_not_admitted_exactly_when_the_worker_never_took_it` races an endpoint end against the clearance of two batches while the worker holds the first without reporting it: each batch is answered once, not admitted exactly when the worker never took it, whether it was still being cleared or cleared and waiting for the worker, and of unknown outcome when it did. |
| Rust client submission slots (`crates/client-core/src/producer/slots_shuttle_tests.rs`) | `shuttle_a_wait_racing_its_resolution_takes_the_outcome_once_and_returns_the_credit`, `shuttle_a_cancelled_wait_loses_neither_the_outcome_nor_the_credit`, and `shuttle_a_release_racing_its_resolution_returns_the_credit_exactly_once` race a submission's resolution against the application's wait, an aborted wait followed by a new one, and a release: the outcome is taken at most once, a cancelled wait leaves it retrievable, and the credit comes back exactly once. |
| Domain clock (`src/runtime/domain_clock.rs`) | `shuttle_lifecycle_tests::concurrent_reads_of_one_installed_generation_never_decrease` checks the nondecreasing watermark; `a_clock_bound_to_a_replaced_generation_is_refused_by_revalidation` rejects a superseded generation; `readers_never_observe_an_installation_older_than_one_they_observed` prevents publication regression. `shuttle_delivery_sends_state_before_ticks_without_regressing_progress` explores the production observer and attachment delivery order across accepted ticks, same-generation unassignment and reassignment, and a generation change. `shuttle_an_attach_waiting_for_the_first_installation_observes_its_domains` races an attach's wait and lookup against the node's first installation of the committed domains and requires the lookup to find the domain and its clock. `a_logical_waiter_wakes_when_its_generation_stops`, `a_logical_waiter_wakes_when_its_generation_is_replaced`, `a_logical_waiter_wakes_when_its_domain_is_removed`, and `a_logical_waiter_wakes_when_a_replacement_mapping_reaches_its_deadline` cover each lifecycle wakeup. |

The checks of WASM checkpoint holds and the durability barrier use the same runner and replay
contract. Their state semantics live in the WASM state documentation; they do not turn Shuttle
into a disk or replica simulator. [Deterministic interconnect simulation](./interconnect-simulation.md)
and Cucumber cover the network and process behavior outside this in-process scheduling boundary.

`shuttle_a_vocabulary_atomic_is_a_scheduling_point_in_the_server_build`
(`src/shuttle_selection_tests.rs`) holds the selection itself to account: in the server's Shuttle
build, an operation on the vocabulary crate's `AtomicTimestamp` must advance Shuttle's
scheduling-point count, which it can only do when the feature reached that dependency's atomics.

### Memory-ordering models

A claim that one thread's writes are visible to another because of an ordering, rather than
because both threads were scheduled in some order, is checked under Loom. Loom runs a model once for
every schedule and every reordering the C11 memory model allows that it can distinguish, so a
`Release` weakened to `Relaxed` shows up as an execution in which a reader sees the flag but not the
write it was meant to publish. A model drives the actual synchronous production owner from Loom
threads with Loom's atomics, both selected through the primitive boundary by the package's `loom`
feature. When a protocol is embedded in asynchronous orchestration, the synchronous protocol is
made independently testable and the runtime uses that same owner; a copied algorithm, a witness
that a join or an extra lock publishes, or a real atomic does not make a model.

The execution, consensus and server crates own a `loom` feature, and each forwards it to the
primitive crate and to every dependency that owns one, so the whole library graph of each builds
with Loom's primitives. `just cargo-clippy-loom`, which `just lint` runs, lints every Loom build:
the models and their harness, the primitive boundary, and the server and consensus libraries both
as they ship and in test mode, where models of their owners are compiled.

Each model names its invariant with an `InvariantId` and runs through
`nervix_model_harness::loom::explore`, which explores it to exhaustion: no preemption bound, no
permutation or time budget, a branch limit of 1,000 thread switches per execution that fails the
model rather than ending the search, and Loom's full thread count. A Loom setting in the environment
that would change that search is refused. A completed search prints a record naming its invariant,
its execution count and its bounds, and `just test-loom` accepts nothing else as a completed model.

`crates/model-harness/loom-inventory.toml` registers every model by invariant. `just test-loom`
lists the `loom_*` library tests of every registered package built with its `loom` feature, and a
run over the whole inventory fails when a registered invariant's test is missing, ignored or did not
complete, or when a discovered model is unregistered. Each model runs in its own process, and the
command reports how many models it discovered, selected, executed and saw complete; a filter that
selects none fails. A failed model leaves `target/loom-failures/<package>/<test>/`: the Loom
checkpoint of the failed execution, the run's output, and metadata naming the invariant, revision,
toolchain, Loom version and exploration bounds. `just test-loom-replay` resumes Loom from that
checkpoint with location tracking and tracing, so the failed execution runs first. The artifacts
hold model output only, never payloads or secrets. CI runs the models on every change and uploads
the failure directory.

Every model also registers a weakening that must make it fail. `just test-loom-qualification`
applies each to a copy of the working tree, requires the model to fail with the registered message,
and requires the checkpoint of that failure to replay it. This is what shows a model depends on the
ordering it claims, rather than passing because something else synchronized its threads. A
weakening whose original text no longer appears exactly once fails as well, so changing an owner's
ordering means revisiting its qualification.

Loom's own limits bound every claim. It does not model every relaxed behavior the C11 model
permits, and an operation inside a third-party dependency, such as a `triomphe` reference count or
an `arc-swap` publication, is invisible to it and excluded from the claim rather than given a
fictional model. A standalone counter carries no cross-location claim, whatever its ordering.

| Invariant | Claim | Model and qualification |
| --- | --- | --- |
| `execution.cancellation.publication` | A job that observes its cancellation also observes every write its awaiting caller made before the cancellation: raising the flag releases and observing it acquires. The witness is read the moment the job observes the cancellation, before any join could order the two threads | `loom_a_job_that_observes_cancellation_observes_every_write_made_before_it` (`crates/execution/src/cancellation.rs`); fails when either the raising store or the observing load is weakened to `Relaxed` |
| `execution.cancellation.cancel-on-drop` | Dropping an armed obligation cancels its job: once the drop is ordered before a check, every clone of the job's signal reports it, and a check never loses a cancellation an earlier check observed | `loom_dropping_the_obligation_cancels_every_later_check_of_the_job` |
| `execution.cancellation.disarm` | A disarmed obligation never cancels its job, whether the job checks while it is disarmed or after it is dropped. Disarming writes nothing, so the model has a single schedule and fails if disarming or the drop after it ever raises the flag | `loom_a_disarmed_obligation_never_cancels_its_job` |

The cancellation protocol these models check is the bounded executor's. `Cancellation::armed`
creates both ends of one job's cancellation: the executor keeps the obligation while its caller
awaits the job and moves the signal into the job, which checks it between its bounded units.
Dropping the awaiting caller drops the obligation and cancels the job, which keeps its memory charge
until it returns; observing the job's value disarms the obligation first, so an ordinary completion
never reports itself as cancelled. The executor keeps admission, charges and cancellation policy;
the protocol it runs is the one the models explore.
