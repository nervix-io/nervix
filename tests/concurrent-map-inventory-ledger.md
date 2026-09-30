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

## Hot-path violations and their deliveries

| Map | Hottest access | Delivery |
| --- | --- | --- |
| `RelayBoundaryServices::ingress_slots` | `ingress_slot` get per batch forwarded to a remote owner | Typed Ratchet 03 |
| `RelayBoundaryServices::outbound_slots` | `outbound_slot` get with a rebuilt key per batch per remote consumer and per interested node; full `retain` per eviction for every relay of the domain | Typed Ratchet 03 |
| interconnect `targets`, `slots`, `connections` | `try_lease` and `ensure_slot` reads per outbound request, acknowledgement and relay send | Typed Ratchet 03 |
| `RuntimeInner::message_error_routes` | occupied `entry` per buffered failed record, holding the write lock across route construction | Typed Ratchet 03 |
| `RuntimeInner::domain_routings` | get for every fresh ingest route collector (per endpoint request and per acknowledged source batch) and per branch eviction | Typed Ratchet 03 |
| `ReplicatedMaterializedRelayState::entries` | `get_mut` per record; `record` per batch per dependency | Typed Ratchet 04 |
| `RuntimeInner::replicated_materialized_stream_states` | get per materialized dependency read; full iteration per generator tick | Typed Ratchet 04 |
| `RuntimeInner::relay_branch_presences` | get per materialized dependency read and per generated record in `materialized_stream_key_is_visible` | Typed Ratchet 04 |
| `RuntimeInner::state_identities` | `state_placement` per materialized read | Typed Ratchet 04 |
| `RuntimeInner::relay_state_epochs` | occupied `entry` per branch relay dispatch | Typed Ratchet 04 |
| interconnect `grants`, `relay_attempts`, `active_relay_channels`, `relay_admissions`, `relay_watermarks` | several reads and writes per granted relay frame and per terminal acknowledgement; a 100 ms full scan of attempts | Typed Ratchet 05 |
| interconnect `outbound_relay_epochs`, `outbound_relay_admissions` | `entry` per sent relay batch, lookup per received terminal acknowledgement, full `retain` on reconciliation | Typed Ratchet 05 |
| `RemoteDispatchRegistry::pending_acks` | insert per forwarded row; get per admitted delivery row and per `Alive`, every 100 ms per unresolved row; a full iteration every second; remove per terminal | Typed Ratchet 05 |
| `RemoteDispatchRegistry::pending_relay_admissions` | insert per relay payload; get per `Alive`; write-locked remove per terminal, including misses for record acknowledgements | Typed Ratchet 05 |
| `RuntimeInner::in_flight_by_domain` | occupied `entry` in `tracked_ack_root` per remote-acknowledgement row (Typed Ratchet 05) and per generated record (Typed Ratchet 11) | Typed Ratchet 05, 11 |
| `RuntimeInner::in_flight_by_ingestor` | re-resolved per ingest group although the source host holds the tracker | Typed Ratchet 11 |
| `RuntimeInner::ingestor_transient_errors`, `ingestor_reconnect_backoffs` | two write-locked removes after every broker receive and every paced poll | Typed Ratchet 11 |
| `RuntimeInner::emitter_transient_errors`, `emitter_retry_statuses` | two write-locked removes, with two key allocations, after every successful publish and every MQTT event | Typed Ratchet 11 |
| `RuntimeInner::pool_waits` | insert and remove per pooled connection borrow: per Redis record, per SQL insert | Typed Ratchet 11 |
| `RuntimeInner::emitter_confirmation_waits` | occupied `entry` per flush or commit attempt for a value that never changes | Typed Ratchet 11 |
| `RuntimeInner::replicated_branch_aggregated_states` | get with a rebuilt placement key, several times per batch, to mark metrics dirty | Typed Ratchet 11 |
| `RuntimeInner::frozen_ownership_handoff_entities` | `contains_key` on every processor, route, dispatcher, supervisor and relay-state loop iteration | Typed Ratchet 11 |
| `RuntimeInner::domains` | `ingestion_time` per ingest group; Kafka `needs_resume` per source loop turn; generator status per occurrence | Typed Ratchet 11 |
| `RuntimeInner::executions` | `bind_domain_clock` per processor batch, per materialized relay state batch and per filtered subscription batch, bypassing the clock the task retains; per WASM checkpoint; per failed record | Typed Ratchet 11 |
| `DomainForceFlush::state` mutex | `pending_completion` per participant loop iteration | Typed Ratchet 11 |
| Prometheus `MetricVec` children | `with_label_values` per answered client batch, per quiesced payload, per dropped subscription frame | Typed Ratchet 11 |
| `RuntimeInner::endpoint_bindings`, `routed_endpoints` | get, deep `Vec` clone and key allocations per HTTP request and per WebSocket message | Typed Ratchet 12 |

## Inventory by owner

### Relay boundary and remote dispatch

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `RelayBoundaryServices::branch_presence` (not a map: the owner task's published `BranchPresence`) | the relay owner publishes on branch creation, eviction, expiry, claim and release; `DESCRIBE`, materialized visibility and the console branch list load it | lifecycle (publication); observer (loads) | owner task, fenced by owner lifetime | lock-free load; one publication per changed owner step | retain: bounded protocol (owner-published membership) |
| `RelayBoundaryServices::ingress_slots` | `entry` on a branch's first forwarded batch; get on every later one; remove and cancel on eviction | per batch | first batch installs; channel incarnation reopened after idleness or an indeterminate outcome | slot gate held across encode and dispatch by design | Typed Ratchet 03 |
| `RelayBoundaryServices::outbound_slots` | `entry` on a channel's first batch; get with a rebuilt key per batch and destination; `retain` per eviction | per batch per destination | relay owner task; as ingress | as ingress; slots of departed consumers stay until eviction | Typed Ratchet 03 |
| `RuntimeInner::relay_boundary_fanouts` | inserted at domain build; read by capacity changes, gates, generators, drain polls | lifecycle, observer | services keep the fanout; never removed | values cloned out | retain: lifecycle registry |
| `RemoteDispatchRegistry::pending_acks` | insert per forwarded row with acknowledgements, naming its receiver; get per admitted delivery row and per `Alive`; a sweep iterates every entry once a second and fails a silent one through a predicate-rechecked `remove_if`; remove per terminal | per record, per ACK; sweep every second | shared by dispatchers, the incoming loop and the sweep; process-run identity fence | `Ref` over the admission and `Alive` updates; an admitted share fails after 15 s without a report from its receiver; no cap | Typed Ratchet 05 |
| `RemoteDispatchRegistry::pending_relay_admissions` | insert per relay payload and destination; get per `Alive`; remove per terminal | per remote frame, per ACK | waiter keeps its receiver; process-run identity fence | ≤1 per outbound channel; 5 s inactivity, 300 s total | Typed Ratchet 05 |

### Materialized and replicated state

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `ReplicatedMaterializedRelayState::entries` | `get_mut` replace or first insert per record; `record` per dependency read; `capture` for snapshots, handoff, `SHOW` and remote reads | per record | one originator per assignment; assignment fence, branch generation, installed fence | `capture` holds the barrier over iteration and sort | Typed Ratchet 04 |
| `RuntimeInner::replicated_materialized_stream_states` | get-then-insert at placement; get per dependency read; iteration per generator tick; borrowed read per replica acknowledgement and checkpoint announcement of a materialized snapshot | per batch | writers retain; readers resolve each time; assignment generation | cloned out | Typed Ratchet 04 |
| `RuntimeInner::relay_branch_presences` | get-or-insert at domain build under the schedule lock; get per materialized visibility check | per batch, per generated record | shares the relay's presence across rebuilds; placement carries the schema fingerprint | `Ref` over one lock-free load | Typed Ratchet 04 (the reader's placement lookup) |
| `RuntimeInner::restored_materialized_stream_states` | insert after an asynchronous open; take at build | lifecycle | schedule application | short | retain: lifecycle registry |
| `RuntimeInner::relay_state_epochs` | occupied `entry` per branch relay dispatch; bumped at schedule application | per batch | branch caches only the number | dropped after clone | Typed Ratchet 04 |
| `RuntimeInner::state_identities` | installed at schedule application; `state_placement` per materialized read; checked per WASM callback | per batch | the value is the fence | `Ref` held while fingerprinting the branch key | Typed Ratchet 04 (materialized reads), Typed Ratchet 11 (WASM check) |
| `RuntimeInner::replicated_deduplicator_states`, `replicated_window_processor_states`, `replicated_wasm_processor_states`, `replicated_kafka_offset_states` | get-then-insert at branch or source start; synchronization requests, replica acknowledgements and checkpoint announcements find their placement's state with a borrowed read | lifecycle; per remote frame (borrowed read) | tasks retain their state, and with it its replication; fingerprint, incarnation or generation | the Kafka sync `Ref` spans the barrier and encode | retain: lifecycle registry |
| `RuntimeInner::replicated_branch_aggregated_states` | get per batch to mark metrics dirty; sync capture; borrowed read per replica acknowledgement and checkpoint announcement | per batch | not retained on the data plane | sync `Ref` spans snapshot and encode | Typed Ratchet 11 |
| Each replicated state's `CheckpointReplication` (not a map: one placement's replica progress and announcement) | the originator offers per checkpoint or commit; one announcer steps every 100 ms while a replica lags; acknowledgements record; waits register before they read | per batch, per ACK | owned by the replicated state, which retires it and ends its announcer | one lock scoped to the placement, never held across an await; WASM 10 s, Kafka 5 s and reset deadlines | retain: bounded protocol (one placement's announcement and replica progress) |
| `RuntimeInner::pending_state_replica_syncs` | `entry` per frame; removed by the reconcile task | per remote frame | one reconcile task per placement; monotonic target | 100 ms retry | retain: bounded protocol (single reconcile task per placement) |
| `RuntimeInner::passive_runtime_state_snapshots` | `entry` per replica installation of a branch checkpoint, comparing revisions and moving the newer one in; taken when a replica is promoted; pruned when a branch lifecycle drops a branch | per remote frame | LSM-monotonic | the guard covers one comparison and a move | retain: bounded protocol (LSM-monotonic single-winner installation) |
| `RuntimeInner::replicated_branch_lifecycles` | get-or-insert when an owner publishes a lifecycle or a replica poll task starts; borrowed reads by acknowledgements, announcements, synchronization requests and branch-keyed installations; backup iterates owned lifecycles; restore publication clears the domain's handles | lifecycle; per remote frame (borrowed read); domain cut or restore publication | the replica poll task retains its handle; each lifecycle checkpoint decodes its branches once | readers load the handle's published checkpoint | retain: lifecycle registry |
| `RuntimeInner::prepared_runtime_state_handoffs`, `activated_runtime_state_handoffs`, `prepared_forced_runtime_state_recoveries`, `prepared_runtime_state_snapshots` | handoff and recovery protocol steps | lifecycle | coordination and handoff identities | the prepare `entry` spans payload comparison and store writes | retain: lifecycle registry |
| `RaisedWasmStateRecoveries::raised` | claim per refused restore; released by the coordinator | failure path, per batch | generation in the key | ≤33 entries | retain: bounded protocol (one outstanding recovery per refused lifetime) |
| `PendingGuestWasmStateResets::requests` | insert per fenced batch; drained by the coordinator | failure path, per batch | generation in the value | one request per fenced branch | retain: bounded protocol (its per-batch `info!` belongs at `debug`) |

### Runtime tasks, gates and routing

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `RuntimeInner::executions` | installed and replaced by schedule application; read by task starts, observers and the per-batch sites above | per batch | tasks retain clocks the sites bypass; start version and clock generation | the error-route write guard spans SET compilation | map: retain: lifecycle registry; per-batch sites: Typed Ratchet 11; failed-record route plans: Typed Ratchet 03 |
| `RuntimeInner::domain_routings` | inserted at install; read at task start and by the sites above | per batch, per record | one stable publication handle per domain | cloned out | map: retain: lifecycle registry; recurring sites: Typed Ratchet 03 |
| `RuntimeInner::domains` | installed with the committed domains; read by the sites above | per batch, steady poll | start version and clock generation | `ingestion_time` holds its `Ref` across the clock bind | Typed Ratchet 11 |
| `RuntimeInner::message_error_routes` | `entry` per buffered failed record; removed at domain stop | per record (failure path) | plan pointer identity | write guard spans route construction | Typed Ratchet 03 |
| `RuntimeInner::ingestors`, `ingestor_quiescence`, `ingestor_readiness`, `client_ingestors` | start, stop and swap; drain polls and `DESCRIBE` | lifecycle, observer | hosts keep their control and command handles | short | retain: lifecycle registry |
| `RuntimeInner::ingestor_transient_errors`, `ingestor_reconnect_backoffs` | insert on failure; two removes after every receive | per record | key rebuilt per call; no generation | write guards; display only | Typed Ratchet 11 |
| `RuntimeInner::emitter_transient_errors`, `emitter_retry_statuses` | insert on failure; two removes after every success | per record (MQTT), per batch | two writers for MQTT sinks; a stale retry status counts as publishing work in a drain | write guards; display and drain state | Typed Ratchet 11 |
| `RuntimeInner::emitter_confirmation_waits` | `entry` per flush or commit attempt | per batch | never removed | occupied write guard | Typed Ratchet 11 |
| `RuntimeInner::emitter_buffers`, `generator_activity_by_domain`, `node_quiesce_counters` | `entry` at task or branch start; drain reads | lifecycle, observer | hot paths use the retained atomics | single-winner installation | retain: lifecycle registry |
| `RuntimeInner::shared_clients` | `entry` at sink open; release decrements then removes | lifecycle | the lease keeps the client | release removes without rechecking its users | retain: lifecycle registry |
| `RuntimeInner::pool_waits` | insert and remove per connection borrow; `DESCRIBE` reads | per record | the emitter's pooled sink | display only | Typed Ratchet 11 |
| `RuntimeInner::in_flight_by_domain`, `in_flight_by_ingestor` | `entry` or get-then-entry per tracked root | per record, per batch | source hosts and client intake keep their trackers; other sites do not | single-winner installation | Typed Ratchet 05 (remote rows), Typed Ratchet 11 |
| `RuntimeInner::force_flush_by_domain` and `DomainForceFlush::state` | `entry` at participant start and per request; the state mutex per participant loop iteration | per batch (the mutex) | participants keep the coordinator | the request runs under the `entry` guard | map: retain: bounded protocol (single coordinator per domain); mutex: Typed Ratchet 11 |
| `RuntimeInner::entity_gate_holds`, `active_domain_alters` | gate engagement and release; ALTER exclusion | lifecycle, control plane | coordination identity and pointer identity | the drain `Ref` spans the entity drain status | retain: lifecycle registry |
| `RuntimeInner::frozen_ownership_handoff_entities` | engaged and released by handoff; read every task-loop iteration | per batch | the watch keeps the map and key but still takes the shard lock | freeze published before notification | Typed Ratchet 11 |
| `RuntimeInner::endpoint_bindings`, `routed_endpoints` | bound at source start; read per request and message | per record | no handle retained | clones under the guard | Typed Ratchet 12 |
| `RuntimeInner::compiled_domain_udfs`, `compiled_wasm_modules`, `domain_instantiation_errors` | domain installation; observers | lifecycle, observer | content identity | short | retain: lifecycle registry |
| `IngestorQuiesceControl::buffers` (`Mutex<HashMap>`) | locked only after the published decision selects buffering; replay | per record, only while quiesced | instance tasks and endpoints | `MAX SIZE` per instance | retain: bounded protocol (retained-payload buffer) |
| `RelayConsumerQueue::batches` (`ConcurrentQueue`) | one push per batch per consumer; one receiver pops | per batch | receiver owns its queue | lock-free, bounded by admitted count | retain: bounded protocol (relay fan-out queue) |
| `DeduplicatorKeyspace::recent_keys` (`ExpiryMap`), `BranchInstanceRegistry`, `WasmAckMap` | their one task mutates them through `&mut self` | per record, single owner | branch or task owner | no locks | retain: single owner |

### Interconnect

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `targets` | health passes, gossip registration and bootstrap write; `try_lease` reads | per remote frame | no retained selection; endpoint identity renews slot keys | ≤ `max_peers` | Typed Ratchet 03 |
| `slots` | `entry` installs one `run_slot` per key; `contains_key` per lease | per remote frame | per-key slot task; cancel-token identity | single run task per key | Typed Ratchet 03 |
| `connections` | registered after connect; get per lease | per remote frame | retiring token and peer epoch | one connection per slot key | Typed Ratchet 03 |
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
| Prometheus `MetricVec` children (`RwLock<HashMap>`) | resolved children for most series; `with_label_values` per event for the sites above | per record, per batch | third-party registry | read lock per lookup | Typed Ratchet 11 |
| `InterconnectionCollector::sources` | installed once; read per scrape | observer | write-once | read lock per scrape | retain: observer |

### Connectors and host maps reached through them

The connector crates and the connector contract crate own no concurrent map, cache or lock over a
map in product code. Their plan- and task-local maps have one owner. RabbitMQ's one-slot
`ConcurrentQueue` hands one broker stream to the client library per connection: retain: bounded
protocol. The host maps connectors reach through the contract are the runtime maps above:
`pool_waits` (Redis, MySQL, Postgres), `emitter_transient_errors` and `emitter_retry_statuses`
(every sink, and every MQTT event), `ingestor_transient_errors` and
`ingestor_reconnect_backoffs` (every source) and `domains` (Kafka domain offsets). A Kafka commit
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
| `src/fault_injection.rs` maps, consensus `append_stream_opens`, the test DNS authority | test-only | test-only |

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
- `relay_boundary_fanouts` and `domain_routings` are never removed with their domain.
- The Shuttle `DashMap` adapter documentation claims it observes every shard acquisition; it models
  one lock over the whole map.

### Backup and restore cold paths

| Owner and map | Access and lifecycle | Classification |
| --- | --- | --- |
| `RuntimeInner::state_identities` | Backup capture reads current scheduled identities before decoding retained checkpoints. Restore clears domain state handles only after complete publication under the consensus authority guard. | observer and stopped-domain lifecycle |
| Session capture sections and restore uploads | Coordinator-scoped registration, bounded chunk transfer and expiration; checkpoint finish stages bytes, and publish replaces the complete set. | control-plane transfer; quota and deadline bounded |
| Test command pauses and failed restore checkpoints | Armed by a scenario and consumed at the restore publication or staging boundary. | test-only fault observation |
