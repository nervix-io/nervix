# Concurrent map inventory ledger

This ledger is the reviewed inventory of every concurrent map, and of every lock that guards a map,
that the data plane owns or reaches, which
[Typed Ratchet 01](https://app.clickup.com/t/86bc9eqfu) of the
[typed architecture lints epic](https://app.clickup.com/t/86bc9eph0) delivers. Later repairs
consume it: each map reached on a recurring record, batch, remote-frame, acknowledgement or
steady-poll path has a concrete disposition and names the delivery that removes the access.
[Data-Plane Concurrency](../docs/src/data-plane-concurrency.md) owns the rules applied here. A change
that adds a concurrent map, or changes how often one is reached or what disposes of it, updates this
ledger in the same change.

The inventory was traced at revision `edc1c8f9`, the parent of the relay presence repair, and
covers the runtime, the connector contract and connector crates, the interconnect, metrics, the
authored primitive wrappers, and the application, consensus and client maps next to them.

The compiler synchronization gate derives execution contracts and operation-specific exceptions
from current source annotations. This inventory supplies the ownership review; executable policy
belongs at the owning function, trait, type, module or exact exceptional operation.
[Typed Ratchet 02A](https://app.clickup.com/t/86bcau18u) delivers generated calibration and
qualification evidence, and [Data-Plane Concurrency](../docs/src/data-plane-concurrency.md#ratchet-and-review)
states the current compiler contract. The executor-saturation lookup is testing fault control.
Replica catch-up retains the entity's lifecycle handle and the replica task's own record of each
branch (see [Replica catch-up](#replica-catch-up-the-repair-typed-ratchet-14-makes)); Typed
Ratchet 15 owns the remaining state-replication frame, synchronization, listing and announcer
reads, including the lifecycle lookup those requests share. The primary's synchronization request
handler remains recurring debt. Source hosts retain an exact instance readiness handle; installation and observers alone reach
the readiness registry. Relay channels, transport selection and domain publications use the
immutable tables and retained lifetimes described below.

## Diagnostic acquisition history

`nervix-primitives::deadlock::OrderHistory::edges` is a diagnostic-only
`DashMap<EdgeKey, EdgeRecord>`. An order-enabled attempted nested acquisition reaches it once per
held source context; the findings owner reads it when correlating a historical cycle. It retains
at most 8,192 directed run-local instance edges and 1,024 contexts per edge until process exit,
including ended locks. Refusal is explicit overload. No borrowed shard crosses a tracked lock
acquisition. Runtime-disabled order checking skips edge access; the compiled instrumentation and
actual-guard tracking still remain. Ordinary execution contains neither the map nor its access.

The existing diagnostic live registry retains construction, waiting-attempt and thread-name maps
for their corresponding lifetimes. The order selection also reads construction/name entries while
recording nested attempts and copies bounded source context into history before they disappear.
This is retained diagnostic bookkeeping, outside ordinary recurring data-plane execution, not a
repair or suppression of a product map. The canonical
[diagnostic contract](../docs/src/data-plane-concurrency.md#diagnostic-deadlock-detection) states its
upstream graph, callback and modeled-scheduling limits.

## How accesses are classified

A `DashMap` synchronizes on every access. `get`, `contains_key`, `len` and iteration take a shard's
read side; `get_mut`, `entry`, `insert`, `remove`, `retain` and `alter` take its write side even
when the key is absent or already present; a borrowed `Ref` holds its shard until it is dropped.
Method names are not evidence: each access below was traced from its call site to the loop that
drives it.

The frequency of a map is its hottest access:

- **per record**: once per accepted record, row or delivered message
- **per batch**: once per relay, processor, ingest or emitter batch
- **per remote frame**: once per inbound or outbound interconnect payload or request
- **per ACK**: once per acknowledgement created, forwarded or resolved
- **steady poll**: once per iteration of a steady-state loop or timer
- **lifecycle**: task, branch or connection start and stop, registration, schedule application,
  snapshot cadence
- **observer**: `DESCRIBE`, `SHOW`, metric scrapes, console snapshots, drain polls
- **test-only** or **client-side**

Dispositions:

- **Typed Ratchet 01** — repaired by this delivery.
- **[Typed Ratchet 03](https://app.clickup.com/t/86bc9eqjv)** — retain generation-aware relay,
  channel, route and connection-selection handles instead of recurring shared-map discovery,
  preserving cancellation, replacement and single-slot creation.
- **[Typed Ratchet 04](https://app.clickup.com/t/86bc9eqp3)** — publish materialized branch state
  from its mutable owner and give readers retained publication handles, preserving timestamps,
  fences, eviction and snapshots.
- **[Typed Ratchet 05](https://app.clickup.com/t/86bc9erep)** — give remote acknowledgement,
  admission and grant state bounded protocol owners.
- **[Typed Ratchet 11](https://app.clickup.com/t/86bc9v74y)** — retain task status, accounting,
  freeze and clock handles resolved when the task or branch starts, and write display-only status
  only on transitions.
- **[Typed Ratchet 12](https://app.clickup.com/t/86bc9v76a)** — publish endpoint intake routes as
  one immutable route table.
- **[Typed Ratchet 13](https://app.clickup.com/t/86bc9v77p)** — gave state-replication progress and
  checkpoint notifications bounded owners; see
  [Checkpoint replication](#checkpoint-replication-the-repair-typed-ratchet-13-makes).
- **[Typed Ratchet 14](https://app.clickup.com/t/86bca1wch)** — caught replica branch states up once
  per entity from the owner's catalog of branch checkpoints instead of once per branch; see
  [Replica catch-up](#replica-catch-up-the-repair-typed-ratchet-14-makes).
- **[Typed Ratchet 15](https://app.clickup.com/t/86bca1web)** — resolve state-replication frames,
  synchronization and listing requests, and announcer steps through retained, generation-fenced
  handles instead of node-wide registry reads.
- **retain: bounded protocol** — the access is an ordering fence or bounded protocol with the named
  requirement.
- **retain: lifecycle registry**, **retain: observer**, **retain: single owner** — no recurring
  path reaches it, or only its one owner does.

## Relay branch presence: the repair this delivery makes

Before this delivery every relay owner batch called `RelayRegistry::touch`: a shard read of a
node-shared `DashMap<Option<BranchKey>, Arc<RelayPresence>>` and a store of `last_seen_at`, or a
shard write on first sight. No reader ever read `last_seen_at`; expiry and eviction used the owner
task's own `BranchInstanceRegistry`. The presence handle was also cloned into every ingress path,
branch template, error route, generator route and remote relay target, and each handed it to relay
ingress, which ignored it; the remote relay path looked it up and cloned it for every frame.

Now the owner task alone holds its branch instances and a persistent set of their keys, and
publishes a complete membership only when a branch appears, is evicted or expires, or the owner
starts or stops. An established branch's batch takes no lock and publishes nothing. The presence
handle lives in the relay's boundary services and in the relay's state placement, and the
pass-throughs are gone. See
[Relay branch presence](../docs/src/data-plane-concurrency.md#relay-branch-presence).

| Access | Frequency before | Frequency now |
| --- | --- | --- |
| `touch` on every owner batch | per batch, shard read and atomic store | none: the owner's own `IndexMap` |
| presence insert on a new branch | per new branch, shard write | one publication per changed step |
| presence remove on eviction or expiry | per released branch, shard write | folded into that step's publication |
| presence clear at owner teardown | lifecycle | release on drop, fenced by the owner lifetime |
| registry lookup and clone per remote relay frame | per remote frame | none |
| `DESCRIBE RELAY ... WHERE`, materialized visibility, console branch list | observer, shard read | observer, lock-free load |

## Checkpoint replication: the repair Typed Ratchet 13 makes

Before this repair, the owner of a placement kept what its replicas acknowledged, and the revision
it announced to them, in `pending_state_checkpoint_announcements`: every WASM checkpoint and every
Kafka offset commit awaiting replicas entered that node-wide map, and every replica acknowledgement
took its write side. A replica woke its synchronization through `state_checkpoint_notifications`,
which every checkpoint-available frame entered, creating entries for per-branch placements nothing
waited on. The Kafka offset state kept a second record of replica progress, overwritten rather than
raised and keyed by `String`, and its quorum wait read it before registering for the next report,
so an acknowledgement landing in between left the commit to its five-second deadline. The
branch-aggregated state kept a third record that nothing read.

Now each replicated state owns one checkpoint replication from `nervix-checkpoint-replication`: the
Kafka offset state, the deduplicator, window and WASM branch states, the materialized relay state,
the branch-aggregated state, and the branch lifecycle handle that `replicated_branch_lifecycles`
keeps for each branch-keyed entity. It records each replica's highest reported revision, offers the
newest checkpoint through one announcer at a time, and carries the owner's announcements to a
replica's synchronization task, and it retires with the state, ending its announcer. See
[Checkpoint replication](../docs/src/data-plane-concurrency.md#checkpoint-replication).

| Access | Before | Now |
| --- | --- | --- |
| announcement per WASM checkpoint and per Kafka commit awaiting replicas | `entry` in the node-wide `pending_state_checkpoint_announcements` | the replication of the state the checkpointer or committer holds; one placement's lock, no map |
| replica acknowledgement | `get_mut` in `pending_state_checkpoint_announcements`, then reads of the Kafka, WASM and branch-aggregated registries | one borrowed read of the registry that keeps the placement's kind of state |
| checkpoint-available frame | `entry` in `state_checkpoint_notifications`, creating an entry for every placement announced | one borrowed read of the placement's state registry; a placement without state wakes nothing |
| Kafka replica quorum wait | reads progress, then registers; an acknowledgement in between waited for the deadline | registers, then reads a monotonic maximum keyed by `ClusterNodeName` |
| a WASM state reset confirming its branch lifecycle | a read of `pending_state_checkpoint_announcements` every 10 ms | registers, then reads the lifecycle's replication |
| branch-keyed replica installation | a clone and decode of the whole branch lifecycle per checkpoint | one lookup in the lifecycle's branch set, decoded once per lifecycle checkpoint |
| a replica's copy of a branch checkpoint | a full payload clone under the `passive_runtime_state_snapshots` write guard | a revision comparison and a move under the guard |
| `ReplicatedBranchAggregatedState::replica_progress` and its notification | written per acknowledgement and never read | deleted |

## Replica catch-up: the repair Typed Ratchet 14 makes

Before this repair, a replica's poll task walked every branch its entity's lifecycle named once
every replication poll interval. For each branch it resolved the placement through
`state_placement`, a read of `state_identities`, read the revision it held through
`passive_state_replica_lsm`, a read of `passive_runtime_state_snapshots` and a storage read when it
held nothing, and sent one synchronization request whether or not the branch changed. The owner
answered each request by probing the deduplicator, Kafka offset, window, WASM and branch-aggregated
registries in turn, and a window or WASM branch with nothing newer fell through to
`replicated_branch_lifecycles` and a storage read. A replica also spawned one reconcile task per
announced placement, keyed in `pending_state_replica_syncs`, which read the held revision and, to
install a branch checkpoint, `state_identities` and `replicated_branch_lifecycles` again. With no
branch changing, a replica's catch-up cost grew with the number of branches, not with the rate of
change.

Now one replica task per entity owns everything it learns of the entity's branch checkpoints and
retains the entity's lifecycle handle. The owner catalogues the newest replicable revision of every
branch state it owns with the entity's lifecycle, and a round reads only what changed after the
cursor its previous read returned. Announcements are left with the entity's lifecycle and taken by
the task in its next round, and `pending_state_replica_syncs` is deleted. See
[Replica catch-up](../docs/src/data-plane-concurrency.md#replica-catch-up).

| Access | Before | Now |
| --- | --- | --- |
| synchronization requests per round with no branch changing | one for the lifecycle and one per branch | one for the lifecycle and one catalog listing |
| `state_identities` read per named branch per round | `state_placement` for every branch | none: a catalog entry names its state; once per task start for the lifecycle |
| held revision per branch per round | `passive_runtime_state_snapshots` get, or a storage read | none: the task's own record, read once per branch it looks at |
| lifecycle discovery per round and per installation | `replicated_branch_lifecycles` get, and get or `entry` on installation | none: the handle the task retains |
| owner answer to a synchronization request | up to five registry probes, then the lifecycle registry and storage | the one registry of the placement's kind; storage only without state |
| announcement of a branch checkpoint or lifecycle | `entry` in `pending_state_replica_syncs` and a reconcile task per placement | one short entity-scoped lock on the lifecycle's pending announcements |

## Recurring sites and their dispositions

| Map | Hottest access | Delivery |
| --- | --- | --- |
| `ReplicatedMaterializedRelayState::entries` (immutable publication) | retained row views for reads and captures; membership changes at branch installation, deletion or snapshot installation | completed: Typed Ratchet 04 |
| `RuntimeInner::replicated_materialized_stream_states` | cold installation, public observation and active-domain lifecycle backup capture (running or paused); stopped backups select immutable checkpoint readers; dependency readers and generators retain the per-relay publication | retain: lifecycle registry |
| `RuntimeInner::relay_branch_presences` | domain construction retains presence in relay services; materialized reads load those services | retain: lifecycle registry |
| `RuntimeInner::state_identities` | cold assignment registration, observation and a read per retained checkpoint at backup capture; materialized placement reads use the shared immutable assignment publication | retain: lifecycle registry |
| `RuntimeInner::relay_state_epochs` | domain routing retains the epoch once; branches load their routing epoch directly | retain: lifecycle registry |
| interconnect `grants`, `relay_attempts`, `active_relay_channels`, `relay_admissions`, `relay_watermarks` | several reads and writes per granted relay frame and per terminal acknowledgement; a 100 ms full scan of attempts | Typed Ratchet 05 |
| interconnect `outbound_relay_epochs`, `outbound_relay_admissions` | `entry` per sent relay batch, lookup per received terminal acknowledgement, full `retain` on reconciliation | Typed Ratchet 05 |
| `RemoteDispatchRegistry::pending_acks` | insert per forwarded row; get per admitted delivery row and per `Alive`, every 100 ms per unresolved row; a full iteration every second; remove per terminal | Typed Ratchet 05 |
| `RemoteDispatchRegistry::pending_relay_admissions` | insert per relay payload; get per `Alive`; write-locked remove per terminal, including misses for record acknowledgements | Typed Ratchet 05 |
| `RuntimeInner::in_flight_by_domain` | generators bind their tracker once; retained roots adjust it directly; per remote row resolution remains with Typed Ratchet 05 | Typed Ratchet 05; generator registration retained |
| `RuntimeInner::in_flight_by_ingestor` | ingest executions, groups and endpoint bindings retain the tracker pair resolved at startup | retain: lifecycle registry |
| `RuntimeInner::ingestor_statuses` | sources retain one failure/retry publication; healthy operations read it without a registry access or publication write; observers load one complete status | retain: lifecycle registry |
| `RuntimeInner::emitter_statuses` | sinks and their event tasks retain one failure/retry publication; healthy clears read it; task teardown removes its registration | retain: lifecycle registry |
| `RuntimeInner::pool_waits` | register/remove once per pooled sink; only a borrow returning Pending publishes its retained wait slot, cleared on completion or cancellation | retain: lifecycle registry |
| `RuntimeInner::emitter_confirmation_waits` | counters resolve at emitter spawn and are removed with the task; flush/commit guards use the retained counter | retain: lifecycle registry |
| `RuntimeInner::replicated_branch_aggregated_states` | tasks and replication routes retain metric placement and progress; the registry handles registration and observation | retain: lifecycle registry |
| `RuntimeInner::frozen_ownership_handoff_entities` | tasks bind one entity publication; watches register before reading it and handoff release publishes before waking | retain: lifecycle registry |
| `RuntimeInner::domains` | task startup binds the lifecycle allocation; ingestion reads pause and clock from one publication, Kafka reads its generation, and generators use their retained clock | retain: lifecycle registry |
| `RuntimeInner::executions` | schedule installation and observation bind clock, routing and state assignment publications; announcers retain their installed route and assignment slot | retain: lifecycle registry |
| `DomainForceFlush::state` mutex | participant idle and claimed polls read their readiness hint; an available obligation acquires the coordinator for its authoritative generation claim | retain: bounded generation protocol |
| Prometheus `MetricVec` children | client outcome children, quiesce payload counters/gauges and subscription drop counters resolve once with their owner | retain: lifecycle registry |
| `RuntimeInner::endpoint_intake_routes` | one immutable publication; borrowed host/path resolution per HTTP request, retained route per WebSocket connection | completed publication: Typed Ratchet 12 |
| replicated-state registries, `RuntimeInner::replicated_branch_lifecycles` | state installation binds the actual state to published routes; frames and announcers use those retained handles | retain: lifecycle registry |
| `RuntimeInner::passive_runtime_state_snapshots` | non-branch handoff staging and cold promotion; branch copies and pruning belong to the retained entity lifecycle | retain: lifecycle registry |


## Inventory by owner

### Relay boundary and remote dispatch

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `RelayBoundaryServices::branch_presence` (not a map: the owner task's published `BranchPresence`) | the relay owner publishes on branch creation, eviction, expiry, claim and release; `DESCRIBE`, materialized visibility and the console branch list load it | lifecycle (publication); observer (loads) | owner task, fenced by owner lifetime | lock-free load; one publication per changed owner step | retain: bounded protocol (owner-published membership) |
| `RuntimeInner::relay_boundary_fanouts` | inserted at domain build; read by capacity changes, gates, generators, drain polls | lifecycle, observer | services keep the fanout; never removed | values cloned out | retain: lifecycle registry |
| `RemoteDispatchRegistry::pending_acks` | insert per forwarded row with acknowledgements, naming its receiver; get per admitted delivery row and per `Alive`, and get_mut per ordered parked or resumed progress report; a sweep iterates every entry once a second and fails a silent one through a predicate-rechecked `remove_if`; remove per terminal | per record, per ACK; sweep every second | shared by dispatchers, the incoming loop and the sweep; process-run identity fence | `Ref` over the admission and `Alive` updates; an admitted share fails after 15 s without a report from its receiver; no cap | Typed Ratchet 05 |
| `RemoteDispatchRegistry::pending_relay_admissions` | insert per relay payload and destination; get per `Alive`; remove per terminal | per remote frame, per ACK | waiter keeps its receiver; process-run identity fence | ≤1 per outbound channel; 5 s inactivity, 300 s total | Typed Ratchet 05 |

### Materialized and replicated state

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `ReplicatedMaterializedRelayState::entries` (immutable persistent index) | lifecycle publishes branch row slots; readers and captures load them | membership changes only | one exclusive originator task; assignment fence, branch generation and installed revision | established replacements mutate task-local selection and publish one Arrow view; capture holds the assignment barrier while sharing views; backup admits at most 8 MiB of generation metadata before capture | completed publication: Typed Ratchet 04 |
| `RuntimeInner::replicated_materialized_stream_states` | installation, cold recovery, public observation and active-domain lifecycle backup capture (running or paused); stopped backups select immutable checkpoint readers | lifecycle, observer | writers, snapshots and relay routing retain installed state; assignment identity fences replacement | cloned out only at cold boundaries | retain: lifecycle registry |
| `RuntimeInner::relay_branch_presences` | get-or-insert at domain build under the schedule lock | lifecycle | retained relay services share the presence across rebuilds | no dependency or generator shard lookup | retain: lifecycle registry |
| `RuntimeInner::restored_materialized_stream_states` | lookup before native preparation; insert after bounded asynchronous open of a pinned checkpoint reader; take at build; clear after durable restore publication | lifecycle | schedule application and generation replacement | short | retain: lifecycle registry |
| `RuntimeInner::relay_state_epochs` | created during domain routing installation; bumped at schedule application | lifecycle | routing retains the shared epoch; each branch caches the observed number | no branch dispatch registry access | retain: lifecycle registry |
| `RuntimeInner::state_identities` | schedule installation publishes assignment slots; state construction binds them; observation and backup read identity, including stopped materialized checkpoints | lifecycle, observer | one entity slot publishes identity, primary, executors and replicas; frames, catch-up and materialized placement use immutable routing | no recurring registry guard | retain: lifecycle registry |
| `RuntimeInner::replicated_deduplicator_states`, `replicated_window_processor_states`, `replicated_wasm_processor_states`, `replicated_kafka_offset_states` | get-then-insert at branch or source start; cold replacement, recovery and teardown; frames use installed route handles | lifecycle | executing tasks and resolved replication routes retain the actual state; fingerprint and guest generation fence assignment; exact intake ends before withdrawal | no frame registry acquisition | retain: lifecycle registry |
| `RuntimeInner::replicated_branch_aggregated_states` | registration, recovery and observation; frame routes retain the metric state | lifecycle, observer | exact metric state and its progress retained; byte capture follows the explicit metrics snapshot contract | no frame registry acquisition | retain: lifecycle registry |
| Each replicated state's `CheckpointReplication` (not a map: one placement's replica progress and announcement) | the originator offers per checkpoint or commit; one announcer steps every 100 ms while a replica lags; acknowledgements record; waits register before they read | per batch, per ACK | owned by the replicated state, which retires it and ends its announcer | one lock scoped to the placement, never held across an await; WASM 10 s, Kafka 5 s and reset deadlines | retain: bounded protocol (one placement's announcement and replica progress) |
| `RuntimeInner::passive_runtime_state_snapshots` | cold non-branch handoff and recovery staging; taken at activation; cleared at entity or domain teardown | lifecycle | coordination and placement; branch passive copies belong to their entity lifecycle | no recurring installation or pruning | retain: lifecycle registry |
| `RuntimeInner::replicated_branch_lifecycles` | get-or-insert at owner publication, branch construction or replica task start; backup iteration and cold teardown | lifecycle, observer | replica tasks and resolved routes retain the lifecycle; each branch retains its catalog registration | no frame or listing registry acquisition | retain: lifecycle registry |
| Each entity's `BranchCheckpointCatalog` (not a map: one published immutable catalog) | each owned branch state registers at creation, records every revision it offers its replicas, and leaves when it is dropped; a catalog listing loads it | per branch publication or WASM checkpoint (record); per remote frame (load) | owned by the entity's lifecycle handle; a registration fences a replaced branch state; an epoch fences another catalog's cursor | one read-copy-update per change, lock-free loads; at most 1,024 removals kept | retain: bounded protocol (one entity's catalog; a change never waits for a reader) |
| `ReplicatedBranchLifecycle::announcements` mutex | an owner's announcement of the entity's lifecycle or of a branch checkpoint adds the newest per branch; the entity's replica task takes all at the start of each round | per remote frame | one replica task per entity takes them | never held across an await; one entry per branch | retain: bounded protocol (one entity's pending announcements) |
| `StateReplicationRouting` (immutable publication, not a concurrent map) | cold state and assignment installation publishes resolved handles; frames select them; teardown ends routes before withdrawal | lifecycle writes; per remote frame loads | existing assignment slot; exact state intake lifetime; no second identity | persistent maps share unchanged paths; no registry guard | retain: published state |
| `ReplicatedBranchLifecycle::passive` (immutable publication, not a concurrent map) | the entity replica task moves newer copies in and prunes unnamed branches; assignment cleanup discards superseded generations; promotion takes a copy | per changed branch or lifecycle; cold assignment cleanup and promotion | one entity; retained assignment and LSM monotonicity | persistent publication; no shared map shard or payload copy during installation | retain: published entity state |
| `RuntimeInner::prepared_runtime_state_handoffs`, `activated_runtime_state_handoffs`, `prepared_forced_runtime_state_recoveries`, `prepared_runtime_state_snapshots` | handoff and recovery protocol steps | lifecycle | coordination and handoff identities | the prepare `entry` spans payload comparison and store writes | retain: lifecycle registry |
| `RaisedWasmStateRecoveries::raised` | claim per refused restore; released by the coordinator | failure path, per batch | generation in the key | ≤33 entries | retain: bounded protocol (one outstanding recovery per refused lifetime) |
| `PendingGuestWasmStateResets::requests` | insert per fenced batch; drained by the coordinator | failure path, per batch | generation in the value | one request per fenced branch | retain: bounded protocol (its per-batch `info!` belongs at `debug`) |

### Runtime tasks, gates and routing

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `RuntimeInner::executions` | schedule installation and observation; tasks bind clock, routing and assignment handles | lifecycle, observer | start version and published lifecycle generation | no record or batch guard for the repaired sites | retain: lifecycle registry |
| `RuntimeInner::domain_routings` (`ArcSwap` persistent table) | installed at successful publication, withdrawn at domain removal and cleared on shutdown; tasks retain the selected domain publisher; fresh intake and eviction read the immutable table | per fresh request or source batch; per eviction | one stable routing publisher per installed domain | changed key copies a persistent path; reads acquire no discovery shard | retain: immutable publication |
| `RuntimeInner::domains` | committed lifecycle installation and task binding | lifecycle, observer | pause, generation and start point publish with clock installation | no ingest-group, Kafka-poll or generator-record guard | retain: lifecycle registry |
| `RuntimeInner::ingestors`, `ingestor_quiescence`, `client_ingestors` | start, stop and swap; intake-only and full alteration hold engagement/release; drain polls and `DESCRIBE` | lifecycle, observer | hosts and both overlapping holds retain the exact control; releasing one hold removes only its share | short | retain: lifecycle registry |
| `RuntimeInner::ingestor_readiness` | prepare and retire exact instance handles; readiness observers read aggregate state | lifecycle, observer | source host retains its instance scalar; retirement is final | source polling acquires no registry guard; predecessor retired before replacement | retain: lifecycle registry |
| `RuntimeInner::ingestor_statuses` | sources retain a coherent failure/retry publication; healthy clears do not write | lifecycle, observer | one status per ingestor, shared by its instances | task preparation installs the slot; stop removes it | retain: lifecycle registry |
| `RuntimeInner::emitter_statuses` | sinks retain a coherent failure/retry publication; healthy clears do not write | lifecycle, observer | sink and event-loop readers share status; retry remains drain work even without buffered messages | task registration and teardown; no recurring map guard | retain: lifecycle registry |
| `RuntimeInner::emitter_confirmation_waits` | resolve at emitter spawn; guards increment/decrement the retained scalar | lifecycle, observer | registration follows emitter task lifetime | removed at task end with pointer identity | retain: lifecycle registry |
| `RuntimeInner::emitter_buffers`, `generator_activity_by_domain`, `node_quiesce_counters` | `entry` at task or branch start; source-only drain reads before full subgraph drain reads | lifecycle, observer | hot paths use the retained atomics | single-winner installation | retain: lifecycle registry |
| `RuntimeInner::shared_clients` | `entry` at sink open; release decrements then removes | lifecycle | the lease keeps the client | release removes without rechecking its users | retain: lifecycle registry |
| `RuntimeInner::pool_waits` | register at pooled sink creation, remove at sink drop; DESCRIBE reads publication | lifecycle, observer | one serialized borrow per sink; a pending borrow retains its guard | no ready-borrow publication or registry guard; cancellation clears pending | retain: lifecycle registry |
| `RuntimeInner::in_flight_by_domain`, `in_flight_by_ingestor` | startup binds generator trackers and ingest tracker pairs; intake-only and full alteration drains read those trackers; remote row binding remains | lifecycle, observer; per remote row | ingest groups and endpoint bindings retain the same tracker pair as their execution | atomic accounting after cold single-winner registration | Typed Ratchet 05 owns remote row binding; remaining registry retained |
| `RuntimeInner::force_flush_by_domain` and `DomainForceFlush::state` | register participant/request generation at intake-only and full hold engagement; claim, complete, release or unregister an obligation; pending drain polls request only if idle | lifecycle and flush transitions | participant retains an idle/available/closed scalar hint | only available polls acquire the authoritative mutex; idle and claimed polls acquire none | retain: bounded generation protocol |
| `RuntimeInner::entity_gate_holds`, `active_domain_alters` | gate engagement and release; ALTER exclusion; an alteration with affected ingestors and shared relay gates retains an intake-only operation until the full operation engages | lifecycle, control plane | distinct coordination identities for overlapping operations and pointer identity on retirement; guard drop, receiver-owned release and lease expiry dispose of each exact operation | the drain `Ref` spans the entity drain status | retain: lifecycle registry |
| `RuntimeInner::frozen_ownership_handoff_entities` | register watches and engage/release handoffs; observation reads retained publication | lifecycle, observer | slot stays stable through repeated freezes; domain removal drops registry interest | register-before-read; publish-before-wake | retain: lifecycle registry |
| `RuntimeInner::endpoint_intake_routes` (`ArcSwap`) and each `EndpointBindingLifetime::intake` (`ArcSwapOption`) | definitions replaced at domain install/teardown; exact lifetimes bound at source start and ended before unbind | one table load per HTTP request or WebSocket upgrade; one borrowed intake lease per payload | request retains its route; WebSocket retains its route and signaling protocol; an admitted request retains its intake lease | whole-table RCU preserves unrelated writers; ended lifetimes refuse later intake through retained tables | retain: immutable publication, Typed Ratchet 12 |
| `RuntimeInner::compiled_domain_udfs`, `compiled_wasm_modules`, `domain_instantiation_errors` | domain installation; observers | lifecycle, observer | content identity | short | retain: lifecycle registry |
| `IngestorQuiesceControl::buffers` (`Mutex<HashMap>`) | locked only after the published decision selects buffering; a drain locks it to take the oldest payload out for delivery and again to end that delivery | per record, only while quiesced or draining what a quiesce retained | instance tasks and endpoints; a delivery borrows the control until it ends | `MAX SIZE` per instance, counting a payload out for delivery; no guard crosses an await | retain: bounded protocol (retained-payload buffer) |
| `RelayConsumerQueue::batches` (`ConcurrentQueue`) | one push per batch per consumer; one receiver pops | per batch | receiver owns its queue | lock-free, bounded by admitted count | retain: bounded protocol (relay fan-out queue) |
| `DeduplicatorKeyspace::recent_keys` (`ExpiryMap`), `BranchInstanceRegistry`, `WasmAckMap` | their one task mutates them through `&mut self` | per record, single owner | branch or task owner | no locks | retain: single owner |

### Relay channel publications

| Publication | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `RelayChannels::branches` (`ArcSwap` persistent table) | producer binding, exact branch withdrawal | first use and explicit branch invalidation; fresh fanout selection | retained branch allocation; receiver and producer endings are distinct | CAS gives first-use races one winner; withdrawal cancels before removing the exact allocation; persistent changed-key path | retain: immutable publication |
| `RelayBranchChannels::routes` and `BranchRoutes::destinations` | producers select route generation and destination slot | per batch; insertion only on first use or route replacement | owner/consumer publication cancellation parent, branch lifetime and exact slot token | one ordering gate per channel; cancellation bounds gate and transport waits; no per-batch table rebuild | retain: immutable publication and bounded ordering |
| `BranchRoutes::subscriptions` | observed gossip snapshot replaces the live advertisement selection | first observed snapshot; stable fanout reads | node incarnation and advertisement version; exact subscription generation | equal advertisements retain slots; changed live set cancels the preceding generation; table bounded to advertised peers | retain: immutable publication |

### Interconnect

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `targets` (`ArcSwap` persistent table) | health, gossip and bootstrap publish endpoints; leases read immutable targets | per operation | target retains a fixed array of slots for each pool class | at most `max_peers`; persistent changed-key path | retain: immutable publication |
| `SlotControl::connection` (`ArcSwapOption`) | one claimed slot worker publishes authenticated connections and clears on loss | per lease, connection lifecycle | endpoint/TLS cancellation lifetime and authenticated peer epoch; lease retains exact connection | one current connection per slot; one atomic worker claim; fixed pool scan | retain: immutable publication |
| `connections` | connection registration, exact retirement, statistics and shutdown | lifecycle, observer | cancel-token and allocation identity prevent predecessor teardown from removing its replacement | one connection per slot key; no recurring selection guard | retain: lifecycle registry |
| `peer_connections`, `inbound_pool_connections` | connection open and close | lifecycle | registration paths | peer and pool caps | retain: lifecycle registry |
| `grants` | insert per grant request; claim per body; expiry task per grant | per remote frame | no single owner; random id checked against peer, epochs and expiry | queue and terminal permits, 5 s lifetime | Typed Ratchet 05 |
| `relay_attempts`, `active_relay_channels`, `relay_admissions`, `relay_watermarks` | grant, body, cancel and terminal handlers; 100 ms progress scan; 60 s sweep | per remote frame, per ACK, steady poll | channel, sequence and process epochs; pointer identity on retirement | consistent lock order; ≤1 unadmitted batch per channel; watermarks time-bounded only | Typed Ratchet 05 |
| `outbound_relay_epochs`, `outbound_relay_admissions` | `entry` per sent relay batch; removed per terminal acknowledgement | per remote frame, per ACK | receiver epoch; registration incarnation | no cap; an outcome that never arrives keeps its entry | Typed Ratchet 05 |
| `RequestState::handlers` (`ArcSwap` of an immutable map) | copy-on-write registration; one load per request | per remote frame, lock-free | fixed at startup | none | retain: bounded protocol (copy-on-write table) |

### Metrics

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `MetricSeries::counters`, `histograms` | `entry` when a task or branch resolves its series; relay teardown and snapshot replacement; observers | lifecycle, observer | recorders keep the resolved series | observer iteration holds shard read locks over summaries | retain: lifecycle registry |
| `MetricSeries::branch_counters`, `branch_histograms` | `entry` per branch appearance; removed only with a relay | lifecycle, observer | branch recorders keep the series | processor branch series are never removed | retain: lifecycle registry (unbounded growth recorded under follow-up findings) |
| `MetricSeries::branch_instance_references` | reference count per branch create, remove and detach | lifecycle | key rebuilt per event | removed at zero | retain: bounded protocol (per-key reference count) |
| `HistogramSeries::rolling_histograms` mutex | one lock per recording through a retained handle | per batch | shared by every branch of a node | bounded accumulator, no await | retain: bounded protocol (documented accumulator fence) |
| Prometheus `MetricVec` children (`RwLock<HashMap>`) | resolve client outcome, quiesce and subscription children at owner startup | lifecycle | task retains children; increments perform no label resolution | third-party metric registry reached only at registration | retain: lifecycle registry |
| `InterconnectionCollector::sources` | installed once; read per scrape | observer | write-once | read lock per scrape | retain: observer |

### Connectors and host maps reached through them

The connector crates and the connector contract crate own no concurrent map, cache or lock over a
map in product code. Their plan- and task-local maps have one owner. RabbitMQ's one-slot
`ConcurrentQueue` hands one broker stream to the client library per connection: retain: bounded
protocol. The host maps connectors reach through the contract are the runtime maps above:
`pool_waits` is resolved once by Redis, MySQL and Postgres pooled sinks; their pending borrow
publishes through a retained slot. Sources and sinks retain `ingestor_statuses` and
`emitter_statuses`, so success and error reporting do not reach their registries. Kafka offsets
retain their domain lifecycle handle. A Kafka commit
awaiting replicas offers its revision through the offset state it retains and reaches no map.

### Primitive wrappers and application owners

| Map | Frequency | Disposition |
| --- | --- | --- |
| `nervix_primitives::collections::DashMap` | the boundary itself | retain: boundary. The Shuttle adapter models one lock over the whole map, which serializes more than production's per-shard locks and iterates in a deterministic order. |
| `nervix_primitives::collections::ConcurrentQueue` | the boundary itself | retain: boundary |
| gossip `routes` | per gossip datagram, management plane | retain: bounded protocol (route learning) |
| gossip `outgoing` | `entry` per gossip datagram, management plane | retain: lifecycle registry (idle workers recorded under follow-up findings) |
| `ProducerLinks::links`, `SubscriptionInterests::leases` | producer and subscription open and close | retain: lifecycle registry |
| session service maps, command execution locks, retained backups and restore archives | per command, control plane; 250 ms sweeps | retain: lifecycle registry |
| consensus `incoming_snapshots` | per snapshot chunk, consensus bulk traffic | retain: bounded protocol (one transfer per peer) |
| client-core `previews`, `servers`, `submissions`, exchange requests | client-side | retain: client-side |
| `FaultInjectionState::executions` | registered once per testing-node startup; queried by explicit occupancy, saturation and release controls | test-only |
| Other `src/fault_injection.rs` maps, consensus `append_stream_opens`, the test DNS authority | test-only | test-only |

### Rust client attachment recovery

Client I/O 03 extends the client registries below. These are client-side lifecycle maps, outside
the server's recurring data-plane paths. Producer submission and consumer delivery retain their
attachment directly; neither discovers it through a desired-handle map. Registry guards are
released before notifications, handle transitions or network awaits. A reconnect takes a snapshot
of live handles under the guard and restores that snapshot after releasing it.

| Map | Frequency and key | Disposition and disposal |
| --- | --- | --- |
| `ProducerRegistryState::producers` (`crates/client-core/src/producer.rs`) | client-side: open reply registration, admission-change event lookup, attachment close/end and exchange loss; keyed by exchange generation and `ProducerId` | retain: client-side lifecycle registry. Each entry retains its exchange generation so reused attachment IDs cannot alias it. Close, endpoint end and loss of that exchange remove its entries. |
| `ProducerRegistryState::current` (`crates/client-core/src/producer.rs`) | client-side: attachment binding, endpoint end, close and exchange loss; keyed by exchange generation and `ProducerId` | retain: client-side lifecycle registry. Weak application handles connect a wire attachment to its desired owner; unbinding, close, endpoint end and exchange loss remove the attachment entry. |
| `ProducerRegistryState::desired` (`crates/client-core/src/producer.rs`) | client-side: open/bind, close/drop and reconnect snapshot; keyed by application-handle address | retain: client-side lifecycle registry. Entries are weak and scale with application-retained handles. Close/drop removes the desired entry; reconnect and exchange-loss snapshots also prune expired weak entries. |
| `DesiredConsumers::desired` (`crates/client-core/src/consumer.rs`) | client-side: open, close/drop and reconnect or exchange-loss snapshot; keyed by application-handle address | retain: client-side lifecycle registry. Entries are weak and scale with application-retained handles. Close/drop unregisters the handle, and snapshots prune expired weak entries. Delivery and ACK use the retained original attachment. |

The maps do not retain payloads, delivery attempts or server reservations. Wire attachment entries
are limited by the server's granted attachments; desired entries have the lifetime of application
handles. Per-handle lifecycle owners fence restoration against close and another exchange loss;
see [Rust client attachment recovery](../docs/src/data-plane-concurrency.md#rust-client-attachment-recovery).

## Follow-up findings outside the hot paths

These maps are not reached on a recurring data-plane path, so they have no repair delivery in this
epic, but tracing them found defects:

- Processor branch metric series in `branch_counters` and `branch_histograms` are never removed
  when their branch is evicted or expires, and global series of dropped graph nodes stay until
  their relay is removed.
- `transaction_executions`, `resource_upload_executions` and `resource_replication_executions` in
  the session service keep entries after failed or completed work.
- `shared_clients` release removes its slot after decrementing without rechecking its users, so a
  lease that joins in between can be orphaned.
- The gossip `outgoing` map keeps one idle worker per address forever.
- `relay_boundary_fanouts` is never removed with its domain.
- The Shuttle `DashMap` adapter documentation claims it observes every shard acquisition; it models
  one lock over the whole map.

### Backup and restore cold paths

| Owner and map | Access and lifecycle | Classification |
| --- | --- | --- |
| `RuntimeInner::state_identities` | Backup capture reads current scheduled identities before decoding retained checkpoints. Restore clears domain state handles only after complete publication under the consensus authority guard. | observer and stopped-domain lifecycle |
| Session capture sections and restore uploads | Coordinator-scoped registration, bounded chunk transfer and expiration; checkpoint finish stages bytes, and publish replaces the complete set. | control-plane transfer; quota and deadline bounded |
| Test command pauses and failed restore checkpoints | Armed by a scenario and consumed at the restore publication or staging boundary. | test-only fault observation |

## Task dependency qualification

The retained owners are measured individually by `just bench-task-handles`, including allocations
for 100 samples of 1,000 operations on one host. The [measurement report](../benches/reports/task-handles.md)
records the host, operation timings, allocation samples and limits. Status and freeze transition sequences use the
registered `task-status-transitions` and `entity-freeze-transitions` Bolero properties. The
production freeze release, status observer and force-flush generation checks run through the
Shuttle inventory. The idle/claimed force-flush regression counts coordinator acquisitions and
requires zero for repeated idle polls. Pool regressions cover ready, pending, completed, cancelled
and replaced sink lifetimes. Public slow-domain force-flush coverage interleaves alpha and beta
branches on one and three nodes; DESCRIBE, handoff and connector suites qualify their public
outcomes. External smoke and soak integration belongs to Typed Ratchet 10.

## Endpoint intake publication evidence

Typed Ratchet 12 publishes configured definitions and bound source lifetimes through
`EndpointIntakeRoutes`. HTTP resolves a borrowed host/path once; an established WebSocket keeps its
route and signaling protocol, including signaling data intake. The source lifetime's optional
intake fences admission through retained routes before unbind or domain withdrawal publishes the
replacement. A lease already admitted may finish; closing a preceding source removes only its
exact allocation. Intakes need not implement `Clone`.

Both endpoint DashMap mutation sites are gone. The current spelling-based ratchet also counts the
replacement table's ordinary, privately owned `HashMap::entry` during cold publication, so its
complete count falls from 161 to 160. That local entry is permitted and acquires no lock; Typed
Ratchet 02's resolved synchronization detector owns correcting this classification.

`endpoint_requests_reuse_bound_routes` dispatches real JSON requests and observes the prepared
output-route reference count during header capture. It holds steady across requests. The registered
`endpoint-route-table` Bolero target exercises bind, unbind, replace, domain withdrawal, and clear
against an independent visible-route and retained-lifetime reference; its two committed corpus
inputs and 256 randomized sequences passed. Unit regressions cover shared intake allocations,
other-domain preservation, exact unbind identity, teardown, and retained signaling allocation.

The production owner uses the existing opaque `ArcSwap` and `ArcSwapOption` boundary. Shuttle
checks whole-domain publication, concurrent replacement and unbind with a retained request, and
teardown with a retained table. Loom does not model those publication internals; this change adds
no independent memory-ordering protocol. Listener ownership and network primitives retain their
existing contracts, so Turmoil is outside this owner's scope. The endpoint Chaos workload remains
owned by Typed Ratchet 10.

Local verification passed 1,538 instrumented server unit tests and 83 public endpoint scenarios
covering HTTP, WebSockets, signaling, codecs, virtual hosts, shared-domain withdrawal and retained
connection lifetimes. The three new Shuttle checks passed their random, PCT and bounded DFS
schedules and replay. `just validate`, `just book` and `just ratchet` passed. Combined LLVM and
Python runner reports cover 329 of 339 executable changed lines (97.05%) before integration with
the latest main branch.

After integrating main's codec error-reporting, representation properties and checked arithmetic,
all 1,589 instrumented server unit tests passed with the downloaded ONNX runtime configured. The
merged runner passed 30 unit tests against the complete 26-target inventory, and `just book`
passed its 282 Python checks and documentation build. Fresh server and Python reports cover
301 of 331 executable patch lines (90.94%) against the merged main. Public endpoint scenarios and
Shuttle evidence above were collected before this integration; unchanged endpoint HTTP edge
paths are exercised by the full PR scenario gate as well.

The final pre-push integration also preserves the subsequent NSPL repairs and Typed Ratchet 13's
checkpoint-replication publication. Endpoint owner and HTTP edge code are unchanged by that merge;
server Clippy across all targets with `testing`, formatting, the 30 runner tests and the debt
ratchet passed. The inventory now has 27 Bolero targets, and the combined recurring-lock count is
157 (main's 158 minus this delivery's one-site decrease).

The full-server sanitizer compilation exceeded the existing 1,800-second local build limit under
shared-host paging. Its failed and interrupted run evidence remains in `target/bolero/runs`.
The server's fuzz package profile uses lighter optimization without debug output; assertions,
AddressSanitizer, coverage feedback, target selection and case/input/campaign bounds are unchanged.
The real sanitizer CI campaign remains a required merge gate; local compilation is not reported
as completed fuzz execution.

### Same-host routing measurement

`just bench-endpoint-routing` ran before and after on the same Intel Core i9-14900HX host with
Rust 1.99.0, the repository's kache wrapper, and the debug test profile (`testing,benchmarks`).
Each run used five samples of 10,000 operations against the same live endpoint ingestor, with
per-thread jemalloc allocated-byte counters and the cooperative budget inside the loop. The request
case measures endpoint selection and intake admission; the retained case measures intake admission
without route resolution. Neither includes payload copying, codec work, or relay delivery. Timing
is subject to other work on the host; byte averages below are whole bytes per operation.

| Case | Median ns per operation | Range ns | Allocated bytes per operation after warm-up |
| --- | ---: | ---: | ---: |
| Request routing and admission, before | 6,268 | 4,930–7,979 | 424 |
| Published request routing and admission | 1,552 | 1,542–1,838 | 0 |
| Retained route admission | 331 | 330–334 | 0 |

The request measurement is approximately 4.0 times faster on this host. The allocation result,
non-Clone intake type, and prepared-route reference probe establish that endpoint routing shares
its prepared state. They do not claim an end-to-end ingestion throughput improvement.

## Restore generation installation

The restore failure-control map is `failed_restore_state_installations`, keyed by domain and
holding a typed guest-staging, materialized-staging or durable-publication failure. It is reached
once at the corresponding guest or materialized staging boundary and once after durable generation publication, as test-only lifecycle control. It is not
reached from record or acknowledgement execution. The receiver upload map retains its existing
per-transfer lifetime and cadence; finalization moves the sealed file into the admitted storage job
and keeps its disk-quota owner there. Successful generation publication clears runtime state maps
under the same applied authority guard after durable pointer publication and bounded storage
cleanup. Checkpoint reads and writes add no concurrent map lookup.
