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
- **[Typed Ratchet 13](https://app.clickup.com/t/86bc9v77p)** — give state-replication progress and
  checkpoint notifications bounded owners.
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

## Recurring sites and their dispositions

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
| `RuntimeInner::in_flight_by_domain` | generators bind their tracker once; retained roots adjust it directly; per remote row resolution remains with Typed Ratchet 05 | Typed Ratchet 05; generator registration retained |
| `RuntimeInner::in_flight_by_ingestor` | ingest executions, groups and endpoint bindings retain the tracker pair resolved at startup | retain: lifecycle registry |
| `RuntimeInner::ingestor_statuses` | sources retain one failure/retry publication; healthy operations read it without a registry access or publication write; observers load one complete status | retain: lifecycle registry |
| `RuntimeInner::emitter_statuses` | sinks and their event tasks retain one failure/retry publication; healthy clears read it; task teardown removes its registration | retain: lifecycle registry |
| `RuntimeInner::pool_waits` | register/remove once per pooled sink; only a borrow returning Pending publishes its retained wait slot, cleared on completion or cancellation | retain: lifecycle registry |
| `RuntimeInner::emitter_confirmation_waits` | counters resolve at emitter spawn and are removed with the task; flush/commit guards use the retained counter | retain: lifecycle registry |
| `RuntimeInner::replicated_branch_aggregated_states` | tasks bind a placement dirty mark beside their metric series; recurring marks reach only that retained state | retain: lifecycle registry |
| `RuntimeInner::frozen_ownership_handoff_entities` | tasks bind one entity publication; watches register before reading it and handoff release publishes before waking | retain: lifecycle registry |
| `RuntimeInner::domains` | task startup binds the lifecycle allocation; ingestion reads pause and clock from one publication, Kafka reads its generation, and generators use their retained clock | retain: lifecycle registry |
| `RuntimeInner::executions` | processor, relay-state and subscription clocks bind at startup; WASM states retain assignment publication; error handlers use retained routing snapshots | retain: lifecycle registry |
| `DomainForceFlush::state` mutex | participant idle and claimed polls read their readiness hint; an available obligation acquires the coordinator for its authoritative generation claim | retain: bounded generation protocol |
| Prometheus `MetricVec` children | client outcome children, quiesce payload counters/gauges and subscription drop counters resolve once with their owner | retain: lifecycle registry |
| `RuntimeInner::endpoint_bindings`, `routed_endpoints` | get, deep `Vec` clone and key allocations per HTTP request and per WebSocket message | Typed Ratchet 12 |
| `RuntimeInner::state_checkpoint_notifications` | `entry` per checkpoint-available frame; entries for placements nothing waits on accumulate | Typed Ratchet 13 |
| `RuntimeInner::pending_state_checkpoint_announcements` | `entry` per WASM checkpoint and per Kafka partition commit that waits for replicas; `get_mut` per replica acknowledgement | Typed Ratchet 13 |
| `RuntimeInner::passive_runtime_state_snapshots` | `entry` with a full payload clone under the guard per replicated checkpoint | Typed Ratchet 13 |
| `RuntimeInner::replicated_branch_lru_snapshots` | clone and decode per branch-keyed replica installation | Typed Ratchet 13 |
| `ReplicatedKafkaOffsetState::replica_progress` | read per partition commit awaiting replicas; progress is overwritten rather than raised, and the wait reads before it registers | Typed Ratchet 13 |
| `ReplicatedBranchAggregatedState::replica_progress` | insert per replication acknowledgement; nothing reads it | Typed Ratchet 13 |

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
| `RuntimeInner::replicated_materialized_stream_states` | get-then-insert at placement; get per dependency read; iteration per generator tick | per batch | writers retain; readers resolve each time; assignment generation | cloned out | Typed Ratchet 04 |
| `RuntimeInner::relay_branch_presences` | get-or-insert at domain build under the schedule lock; get per materialized visibility check | per batch, per generated record | shares the relay's presence across rebuilds; placement carries the schema fingerprint | `Ref` over one lock-free load | Typed Ratchet 04 (the reader's placement lookup) |
| `RuntimeInner::restored_materialized_stream_states` | insert after an asynchronous open; take at build | lifecycle | schedule application | short | retain: lifecycle registry |
| `RuntimeInner::relay_state_epochs` | occupied `entry` per branch relay dispatch; bumped at schedule application | per batch | branch caches only the number | dropped after clone | Typed Ratchet 04 |
| `RuntimeInner::state_identities` | schedule installation replaces each retained assignment slot; state placement binds it; WASM callbacks read it directly | lifecycle, observer; materialized reads remain per batch | one entity publication contains identity and checkpoint owners; removal publishes absence | no callback registry guard | registry retained; Typed Ratchet 04 owns remaining materialized resolution |
| `RuntimeInner::replicated_deduplicator_states`, `replicated_window_processor_states`, `replicated_wasm_processor_states`, `replicated_kafka_offset_states` | get-then-insert at branch or source start; sync and acknowledgement handlers look up by placement | lifecycle; per remote frame on replicas | tasks retain their state; fingerprint, incarnation or generation | the Kafka sync `Ref` spans the barrier and encode | retain: lifecycle registry |
| `RuntimeInner::replicated_branch_aggregated_states` | registration, snapshot synchronization and observation; tasks retain dirty marks | lifecycle, observer | exact metric placement retained with its series | synchronization guards stay on snapshot paths | retain: lifecycle registry |
| `ReplicatedKafkaOffsetState::replica_progress` | insert per replica acknowledgement; read by the quorum wait per partition commit | per ACK | not cleared on rebind | outer `Ref` over the inner insert; 5 s wait; overwrite instead of maximum; read before registration | Typed Ratchet 13 |
| `ReplicatedBranchAggregatedState::replica_progress` | insert per replication acknowledgement; no reader | per remote frame | — | — | Typed Ratchet 13 (delete) |
| `ReplicatedWasmProcessorState::replica_progress` | maximum-insert and notify per replica acknowledgement; read per checkpoint wait | per batch | placement carries the guest-state generation | registers before it reads; checkpoint deadline | retain: bounded protocol (checkpoint replica boundary) |
| `RuntimeInner::state_checkpoint_notifications` | `entry` per checkpoint-available frame; poll tasks keep their `Notify` | per remote frame | waiters retain; notifier does not | entries for placements nothing waits on accumulate | Typed Ratchet 13 |
| `RuntimeInner::pending_state_replica_syncs` | `entry` per frame; removed by the reconcile task | per remote frame | one reconcile task per placement; monotonic target | 100 ms retry | retain: bounded protocol (single reconcile task per placement) |
| `RuntimeInner::pending_state_checkpoint_announcements` | `entry` per WASM checkpoint and Kafka commit awaiting replicas; `get_mut` per replica acknowledgement | per batch, per ACK | split between checkpointer, control loop and announcer | WASM 10 s and Kafka 5 s waits | Typed Ratchet 13 |
| `RuntimeInner::passive_runtime_state_snapshots`, `replicated_branch_lru_snapshots` | replica installation compares, clones and decodes under the guard | per remote frame | LSM-monotonic | write guard across a payload clone | Typed Ratchet 13 |
| `RuntimeInner::prepared_runtime_state_handoffs`, `activated_runtime_state_handoffs`, `prepared_forced_runtime_state_recoveries`, `prepared_runtime_state_snapshots` | handoff and recovery protocol steps | lifecycle | coordination and handoff identities | the prepare `entry` spans payload comparison and store writes | retain: lifecycle registry |
| `RaisedWasmStateRecoveries::raised` | claim per refused restore; released by the coordinator | failure path, per batch | generation in the key | ≤33 entries | retain: bounded protocol (one outstanding recovery per refused lifetime) |
| `PendingGuestWasmStateResets::requests` | insert per fenced batch; drained by the coordinator | failure path, per batch | generation in the value | one request per fenced branch | retain: bounded protocol (its per-batch `info!` belongs at `debug`) |

### Runtime tasks, gates and routing

| Map | Readers and writers | Frequency | Handle, owner and generation | Guard and bound | Disposition |
| --- | --- | --- | --- | --- | --- |
| `RuntimeInner::executions` | schedule installation and observation; tasks bind clock, routing and assignment handles | lifecycle, observer | start version and published lifecycle generation | no record or batch guard for the repaired sites | retain: lifecycle registry |
| `RuntimeInner::domain_routings` | inserted at install; read at task start and by the sites above | per batch, per record | one stable publication handle per domain | cloned out | map: retain: lifecycle registry; recurring sites: Typed Ratchet 03 |
| `RuntimeInner::domains` | committed lifecycle installation and task binding | lifecycle, observer | pause, generation and start point publish with clock installation | no ingest-group, Kafka-poll or generator-record guard | retain: lifecycle registry |
| `RuntimeInner::message_error_routes` | `entry` per buffered failed record; removed at domain stop | per record (failure path) | plan pointer identity | write guard spans route construction | Typed Ratchet 03 |
| `RuntimeInner::ingestors`, `ingestor_quiescence`, `ingestor_readiness`, `client_ingestors` | start, stop and swap; drain polls and `DESCRIBE` | lifecycle, observer | hosts keep their control and command handles | short | retain: lifecycle registry |
| `RuntimeInner::ingestor_statuses` | sources retain a coherent failure/retry publication; healthy clears do not write | lifecycle, observer | one status per ingestor, shared by its instances | task preparation installs the slot; stop removes it | retain: lifecycle registry |
| `RuntimeInner::emitter_statuses` | sinks retain a coherent failure/retry publication; healthy clears do not write | lifecycle, observer | sink and event-loop readers share status; retry remains drain work even without buffered messages | task registration and teardown; no recurring map guard | retain: lifecycle registry |
| `RuntimeInner::emitter_confirmation_waits` | resolve at emitter spawn; guards increment/decrement the retained scalar | lifecycle, observer | registration follows emitter task lifetime | removed at task end with pointer identity | retain: lifecycle registry |
| `RuntimeInner::emitter_buffers`, `generator_activity_by_domain`, `node_quiesce_counters` | `entry` at task or branch start; drain reads | lifecycle, observer | hot paths use the retained atomics | single-winner installation | retain: lifecycle registry |
| `RuntimeInner::shared_clients` | `entry` at sink open; release decrements then removes | lifecycle | the lease keeps the client | release removes without rechecking its users | retain: lifecycle registry |
| `RuntimeInner::pool_waits` | register at pooled sink creation, remove at sink drop; DESCRIBE reads publication | lifecycle, observer | one serialized borrow per sink; a pending borrow retains its guard | no ready-borrow publication or registry guard; cancellation clears pending | retain: lifecycle registry |
| `RuntimeInner::in_flight_by_domain`, `in_flight_by_ingestor` | startup binds generator trackers and ingest tracker pairs; remote row binding remains | lifecycle; per remote row | ingest groups and endpoint bindings retain the same tracker pair as their execution | atomic accounting after cold single-winner registration | Typed Ratchet 05 owns remote row binding; remaining registry retained |
| `RuntimeInner::force_flush_by_domain` and `DomainForceFlush::state` | register participant/request generation; claim, complete, release or unregister an obligation | lifecycle and flush transitions | participant retains an idle/available/closed scalar hint | only available polls acquire the authoritative mutex; idle and claimed polls acquire none | retain: bounded generation protocol |
| `RuntimeInner::entity_gate_holds`, `active_domain_alters` | gate engagement and release; ALTER exclusion | lifecycle, control plane | coordination identity and pointer identity | the drain `Ref` spans the entity drain status | retain: lifecycle registry |
| `RuntimeInner::frozen_ownership_handoff_entities` | register watches and engage/release handoffs; observation reads retained publication | lifecycle, observer | slot stays stable through repeated freezes; domain removal drops registry interest | register-before-read; publish-before-wake | retain: lifecycle registry |
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
retain their domain lifecycle handle. `pending_state_checkpoint_announcements` remains on Kafka
commit and WASM checkpoint paths and belongs to Typed Ratchet 13.

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
| client-core `previews`, `servers`, `producers`, `submissions`, exchange requests | client-side | retain: client-side |
| `src/fault_injection.rs` maps, consensus `append_stream_opens`, the test DNS authority | test-only | test-only |

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
