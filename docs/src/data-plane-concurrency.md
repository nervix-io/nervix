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
| Relay owner state | Each relay boundary publishes its scheduled owner, installed owner buffer, remote runtime-consumer set, and immutable branch-reset gate set. Schedule and relay lifecycle operations replace these values at their cutover points. | A batch borrows the current owner and buffer, then takes permits only from reset gates whose typed scope selects its branch. Multi-step ownership changes use the whole-relay dispatch gate described below so teardown cannot race an admitted dispatch. |
| Subscription interest | The cluster live-state watcher rebuilds an immutable index from domain and relay to interested node incarnations and advertisement versions whenever gossip changes. | A relay owner performs borrowed lookups in one published index. It neither formats gossip keys nor waits on the gossip mutex per batch. Subscription creation waits until every live node has observed the exact subscriber incarnation and at least the current advertisement version before reporting success. |
| Clock installation | Each domain-clock lifecycle on each node publishes the complete missing, stopped, uninstalled, unpaced, or paced installation. | A read validates its bound lifecycle generation against one installation, then advances that installation's nondecreasing timestamp watermark atomically. A same-generation replacement retains the watermark; a different generation cannot be clamped by a stale reader. |
| Runtime-state assignment | Each state placement publishes one packed atomic binding containing its generation and capability. Replication roles are a separate immutable published snapshot. | A per-message operation admits itself, compares the exact binding it was granted, and proceeds only while that generation still grants the required capability. It never takes the assignment barrier. |
| Ingestor quiesce decision | Each ingestor publishes the declared and pending modes, active causes, source support, and derived intake decision as one value. Concurrent lifecycle changes derive their replacement from the current publication. | Polling and per-message intake make one load to decide whether to dispatch, suspend, skip, buffer, drop, or reject. A source host retains its last observed publication across dispatch awaits; its change wait registers before comparing that publication with the current one, so an engagement or release in the gap wakes it. The retained-payload lock is reached only after the published decision selects buffering. |
| Metric series handles | Each relay, node, ingestor, emitter, or concrete branch resolves its label set, internal series, and Prometheus child when its owning task or branch is created. | Recording uses the retained series directly. Counters update atomically; a histogram records through its already-resolved per-series accumulator without a registry lookup, key construction, or map guard. Registration and removal stay on lifecycle paths. |

An atomic replacement gives consistency for the value it publishes. A transition involving several
owners still needs a protocol. Graph and schedule changes use quiescence and relay gates; clock and
state operations carry generations; subscription creation uses a visibility handshake. Publication
removes read-side contention without weakening those transition contracts.

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
- one already-resolved histogram series serializes its bounded accumulator update; counter series
  are atomic
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
outside a Shuttle schedule.

Some primitives are opaque to Shuttle: `arc-swap`, `async-broadcast`, `triomphe`, and
`futures-channel` have no scheduler wrapper. Nervix routes `ArcSwap` and `ArcSwapOption` loads and
stores, plus `ArcSwap` compare-and-swap and read-copy-update, through its execution synchronization
boundary. That boundary yields under Shuttle and calls the underlying primitive directly
otherwise. It also supplies a scheduler-visible synchronous yield for admission spin waits.
A check may claim an ordering around an opaque primitive only when its relevant calls have
visible scheduling points. Shuttle cannot interrupt an arbitrary instruction inside it or a
standard-library atomic. The protocol atomics that the checks explore use Shuttle's atomic types
under the feature.

Even a wrapped Tokio primitive can hide a scheduling window. `shuttle-tokio` currently keeps
`Notify` waiter registration behind a standard-library mutex, so registering `notified()` is not
a scheduling point. A read followed by registration may therefore look safe in a check even
though a release between the two would be lost in production. The corresponding `watch`
`borrow()`/`subscribe()` window has the same verification limit. The ownership-handoff freeze
check exercises release-before-wake, but does not prove its separate register-before-read order.
That order remains a production owner contract until both sides of the race are scheduler-visible.

Shuttle explores sequentially consistent schedules. It cannot prove that a chosen `Relaxed`,
`Acquire`, or `Release` ordering is sufficient on weak-memory hardware. Loom is reserved for an
actual memory-ordering claim over modeled atomics. The old copied dispatch-gate model was retired
when Shuttle began exercising the production gate and fan-out. Cucumber remains the public
behavior test: when a scheduling defect affects an NSPL operation, runtime output, or process
outcome, its Shuttle regression is paired with a scenario through that interface. A scenario
cannot exhaust the interleavings of an in-process protocol, and a Shuttle check cannot verify the
whole cluster, socket, disk, or browser path.

### Check contract and runner

Each check names the invariant in its test name, drives competing operations on the production
owner, and asserts the state at meaningful transitions or after all participants have joined.
Use an explicit handshake or scheduler-visible yield to position a race. When both the decision
read and waiter registration are scheduler-visible, a missed notification leaves the waiter
pending and Shuttle reports the resulting deadlock. Name a bounded number of participants so the
search remains reviewable; use bounded depth-first search for small races and random plus
probabilistic concurrency testing (PCT) for larger ones. The server's shared runner supplies
random, PCT, and bounded DFS modes; interconnect and execution use random and PCT. The runner
caps each schedule at 10,000 steps. Individual checks choose their iteration counts and PCT
depth; `SHUTTLE_REPORT_STEPS=1` reports the highest observed step count when tuning a check. A
step cap is an exploration bound, not a product timeout.

`just test-shuttle` runs only library tests whose full names contain `shuttle_`, one test per
process, in `nervix-execution`, `nervix-interconnect`, and `nervix-server`. It then repeats each
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
| Entity gate and node quiesce (`src/runtime/entity_gate_shuttle_tests.rs`) | `shuttle_an_entity_gate_hold_fences_every_relay_and_admits_no_work_until_it_is_released` requires admitted work to drain before quiescence and prevents new admission while closed. `shuttle_a_work_item_parked_for_materialized_state_is_never_missing_from_a_drain` and `shuttle_every_node_quiesce_gauge_withdraws_exactly_what_it_contributed` keep parked, buffered, and branch work counted without underflow and back to zero. `shuttle_every_engagement_waiter_wakes_and_exactly_one_release_takes_the_hold`, `shuttle_a_hold_dropped_before_its_fence_completes_reopens_every_relay_it_engaged`, `shuttle_a_failed_engagement_wakes_every_waiter_with_its_failure`, and `shuttle_releasing_an_ownership_handoff_wakes_every_waiter_frozen_by_it` cover release, drop, failure, and publication-before-wake. The last check does not exercise the separate waiter-registration gap described above. |
| Interconnect slots and membership (`crates/interconnect/src/connection/stream_slots/shuttle_checks.rs`, `request/shuttle_checks.rs`) | `management_drain_stops_leasing_and_waits_for_every_leased_slot`, `replication_drain_stops_leasing_and_waits_for_every_leased_slot`, `bulk_drain_stops_leasing_and_waits_for_every_leased_slot`, and `relay_drain_stops_leasing_and_waits_for_every_leased_slot` keep partition and subquota reservations isolated, forbid leases after drain starts, and wait for every lease to return. `racing_registrations_lose_no_handler_and_publish_each_name_once` prevents a lost handler registration and duplicate name. `a_membership_change_between_a_callers_check_and_its_wait_is_never_lost` prevents a missed discovery wakeup. |
| Shutdown and signals (`src/application/shutdown.rs`, `termination_signals.rs`) | `shuttle_racing_stop_requests_accept_exactly_one_and_keep_its_deadline` retains the first stop request and its deadline. `shuttle_phases_only_advance_and_every_completion_waiter_observes_the_one_outcome` keeps phase order and one completion. `shuttle_an_expired_deadline_and_a_repeated_signal_let_exactly_one_forced_exit_end_the_process` and `shuttle_a_repeated_signal_before_the_deadline_ends_the_process_with_the_status_of_that_signal` give one forced-exit claimant and the exit status of the cause that won. |
| Domain clock (`src/runtime/domain_clock.rs`) | `shuttle_lifecycle_tests::concurrent_reads_of_one_installed_generation_never_decrease` checks the nondecreasing watermark; `a_clock_bound_to_a_replaced_generation_is_refused_by_revalidation` rejects a superseded generation; `readers_never_observe_an_installation_older_than_one_they_observed` prevents publication regression. `a_logical_waiter_wakes_when_its_generation_stops`, `a_logical_waiter_wakes_when_its_generation_is_replaced`, `a_logical_waiter_wakes_when_its_domain_is_removed`, and `a_logical_waiter_wakes_when_a_replacement_mapping_reaches_its_deadline` cover each lifecycle wakeup. |

The checks of WASM checkpoint holds and the durability barrier use the same runner and replay
contract. Their state semantics live in the WASM state documentation; they do not turn Shuttle
into a disk or replica simulator. [Deterministic interconnect simulation](./interconnect-simulation.md)
and Cucumber cover the network and process behavior outside this in-process scheduling boundary.
