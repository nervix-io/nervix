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
competes with writers, and can join an unrelated reader to a writer's delay.

A shared concurrent map synchronizes on every access, not only on `entry()`. `DashMap::get`,
`contains_key`, `len` and iteration take a shard's read side, and `get_mut`, `entry`, `insert`,
`remove`, `retain` and `alter` take its write side, whether or not the key is present; a borrowed
`Ref` holds its shard until it is dropped. A steady hot path therefore does not look an established
value up in a shared map at all, whether by borrowed lookup or by `entry()`. It retains the handle
it resolved when its task, branch, channel, or attempt was created. A shared registry serves the
cold paths around that handle: registration and first installation, where `entry()` gives racing
installers a single winner, replacement, teardown, and observers.

The resulting design uses four forms of ownership:

- Reconfigurable read-mostly state is built privately and published by atomically replacing one
  immutable reference.
- A task resolves stable services, state authorities, slots, and metric series when it starts and
  retains those handles.
- Scalar progress and counts use atomics with an ordering chosen for the contract they enforce.
- State that changes for every row or batch belongs exclusively to one task, branch, delivery
  channel, or source attempt. That owner mutates ordinary collections directly: a task-local
  `HashMap`, `BTreeMap`, `IndexMap` or JSON map and its `entry` API are not synchronization, and
  exclusive ownership does not pin the task to an operating-system thread. When other tasks need to
  observe such state, the owner publishes an immutable value of it; observers never reach into the
  owner's collection.

A shared concurrent map is justified only for state that several owners register into or that
observers read across owners, and only while its accesses stay on those cold paths. Hot-path
synchronization that remains is an ordering fence or bounded protocol this chapter names, with the
exact key that scopes it and the capacity or deadline that bounds its wait. The
[concurrent map inventory](https://github.com/nervix-io/nervix/blob/main/tests/concurrent-map-inventory-ledger.md)
records every concurrent map on the data plane and its neighbours, how often each is reached, and
the disposition of every map that is still reached on a record, batch, remote-frame,
acknowledgement or steady-poll path. Until its named repair lands, such a site is debt, not accepted
design.

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
| Message-error route plans | The registry selects each DLQ route from the committed schedule. Domain installation binds its schemas, branch declaration, flush contract, relay target and SET program before tasks start. Schedule application publishes the complete bound route map with the routing state of one typed revision. | A failed record looks up its prepared route in the task’s retained immutable routing snapshot, which publishes the bound plans together with the routing revision, before running its VM program or delivery. A buffered route compares the bound plan allocation with its running delivery task; a replacement drains the preceding task and starts a task with the new target and cadence. |
| Node identity and remote dispatcher | The node runtime publishes this once after cluster join, when the authenticated interconnect and process incarnation are known. Relay boundaries created afterwards retain the same dispatcher handle. | Readers borrow the stable node identity, incarnation, transport, admission service, and ACK registry without a write-once lock or repeated name allocation. |
| Relay owner state | Each relay boundary publishes its scheduled owner, installed owner buffer, remote runtime-consumer set, and immutable branch-reset gate set. Schedule and relay lifecycle operations replace these values at their cutover points. | A batch borrows the current owner and buffer, then takes permits only from reset gates whose typed scope selects its branch. Multi-step ownership changes use the whole-relay dispatch gate described below so teardown cannot race an admitted dispatch. |
| Relay branch presence | The relay's owner task publishes the complete membership of the concrete branches it holds, and whether it admitted unbranched work, when that membership changes: a branch appears, is evicted or expires, or the owner starts or stops. The relay's state placement keeps the same presence across execution rebuilds. See [Relay branch presence](#relay-branch-presence). | `DESCRIBE RELAY ... WHERE`, materialized reads, and the console's dataflow graph load one membership without a lock. A batch for a branch the owner already holds publishes nothing. |
| Subscription interest | The cluster live-state watcher rebuilds an immutable index from domain and relay to interested node incarnations and advertisement versions whenever gossip changes. | A relay owner performs borrowed lookups in one published index. It neither formats gossip keys nor waits on the gossip mutex per batch. Subscription creation waits until every live node has observed the exact subscriber incarnation and at least the current advertisement version before reporting success. |
| Clock installation | Each domain-clock lifecycle on each node publishes the complete missing, stopped, uninstalled, unpaced, or paced installation. | A read validates its bound lifecycle generation against one installation, then advances that installation's nondecreasing timestamp watermark atomically. A same-generation replacement retains the watermark; a different generation cannot be clamped by a stale reader. |
| Accepted clock progress | Each runtime domain publishes its newest accepted generation, tick id, logical boundary, and authority UTC observation through a watch. | The watch serializes comparison and replacement, so concurrent progress deliveries cannot regress the id. Generation changes and stops publish absence. A session observer subscribes before reading, retains a sender until it observes domain removal, and uses an execution snapshot only to add its serving node's logical reading to a tick frame. |
| Committed domain installation | Each node's runtime counts its installations of the committed domain states through a watch. An installation advances the count only after it has inserted, updated, or removed every domain. | A session attach waits for the first count before it looks up a domain, so a node that is still starting never refuses a domain the cluster has. Because the count advances after the domains are in place, the wait never releases before they are observable. |
| Runtime-state assignment | Each state placement publishes one packed atomic binding containing its generation and capability. Replication roles are a separate immutable published snapshot. | A per-message operation admits itself, compares the exact binding it was granted, and proceeds only while that generation still grants the required capability. It never takes the assignment barrier. |
| Ingestor quiesce decision | Each ingestor publishes the declared and pending modes, active causes, source support, and derived intake decision as one value. Concurrent lifecycle changes derive their replacement from the current publication. | Polling and per-message intake make one load to decide whether to dispatch, suspend, skip, buffer, drop, or reject. A source host retains its last observed publication across dispatch awaits; its change wait registers before comparing that publication with the current one, so an engagement or release in the gap wakes it. The retained-payload lock is reached only after the published decision selects buffering. |
| Backup state snapshot | Branch lifecycle publications, Kafka offset commits and WASM checkpoints register on a per-domain atomic generation before changing their published state. After the domain is paused and drained, the owner closes that generation with a Release RMW and waits with Acquire loads for registered publishers to leave. It then serializes Kafka positions and branch lifecycle into the runtime state store and opens one database snapshot for those records and durable WASM saves. The closing RMW is in the release sequence that a publisher entering the next generation acquires. | A publisher that entered before the cut completes into the captured view; a publisher that enters after it waits for the next generation. An older periodic storage write cannot replace a newer forced publication. The backup reads the database snapshot off the record path and stages owned section bytes. A live backup does not close the generation and reports its weaker cut explicitly. The server's Shuttle and Loom checks drive this production fence. |

| Restore state publication | The control plane stages checkpoints under a replicated authority, then admits and publishes one complete domain batch. It holds the consensus applied-state read guard through authority validation, synchronous storage mutation and runtime-handle clearing. The state store reuses its existing latest-snapshot installation mutex for staging and publication. | A newer applied generation and the release of the start gate require the state-machine write guard. Every node therefore replaces a whole state set before `START` is available, and stale coordinators cannot mutate a running restored domain. The store retains the publication generation for exact retry and monotonic rejection. These are stopped-domain cold paths; record paths add no lock. |

The backup coordinator waits for each node's admitted-work counters to reach zero, then orders one
confirming force-flush generation across the cluster. The generation's obligations use the same
atomic counters as shutdown; a parked `REQUIRED WAIT` batch stays visible separately and cannot
hold the cut open.
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

## Retained Task Dependencies

A source or sink retains one failure publication containing both its safe error and optional retry.
Success reads that slot and clears it only when it holds a failure. Identical failure reports keep
the same immutable observation. Reporting an error without a new retry keeps the active retry,
which remains visible as publishing work to drains even when the emitter buffer is empty. DESCRIBE
loads error, backoff and remaining wait from one observation. Emitter confirmation guards retain
one counter resolved at startup, and task teardown removes its registry interest.

Every pooled Redis or SQL sink registers a wait slot once. The connector serializes its connection
borrows. A ready borrow writes nothing; the first Pending poll publishes its wait and owns a guard
that clears it on success, error or cancellation. Sink destruction removes only its own registry
slot, so a replacement sink's slot survives an earlier sink's teardown.

Generators retain the domain acknowledgement tracker. Ingest executions, groups and endpoint
bindings retain the same domain/ingestor tracker pair. Task and branch metric handles retain the
exact placement's dirty mark. Client batch outcome counters, quiesced payload counters/gauges and
subscription drop counters retain their Prometheus children, including every declared outcome
label combination, so recording resolves no label set.

Subscription generations also retain the node's executor and pass it directly to predicate
evaluation. Executor admission continues to own worker and memory bounds for extension calls.
The admission benchmark also constructs the current emitter context, resolving its routing,
metrics mark, status publication and confirmation counter before the timed encoding runs.

Each handoff entity has one immutable coordination-owner set and notification owner. Tasks retain
that entity slot across repeated freezes. A watch registers before reading the publication;
engagement and release replace it before notifying. Concurrent owners derive their replacement
with the primitive publication boundary's reference update. Domain removal drops registry interest.

Domain-clock publication contains lifecycle pause, generation and start-point observations alongside
its installation. Ingestion reads admission and time from that one publication; Kafka offset polls
read its generation; generators, processors, materialized relay tasks and subscriptions use the
clock or lifecycle handle bound at startup. Each WASM state retains its entity assignment slot,
which publishes state identity and checkpoint execution/replica owners together. A generation
replacement or removal therefore fences a callback without a runtime registry lookup.

Force-flush participants retain a private idle, available or closed scalar. Idle and already-claimed
polls acquire no coordinator mutex. An available indication enters the mutex, which remains the
sole authority for generations and claims. The coordinator visits participants in participant-ID
order when publishing readiness, so deterministic replay sees the same sequence of scheduling
points. Publication of a request precedes the watch wakeup;
claim release makes the obligation available again. The relaxed hint publishes no data from another
location. Watch synchronization and ArcSwap ordering belong to opaque primitive dependencies;
this change introduces no Nervix cross-location memory-ordering claim. Production Shuttle checks
explore generation publication, claims, release and close, and the idle regression counts actual
coordinator acquisitions.

The status and freeze transition contracts have registered Bolero sequence properties. Status
observers and freeze registration/release have production-owner Shuttle checks. `just
bench-task-handles` records raw same-host timing and allocated-byte samples for the repaired paths.
The [retained task handle measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/task-handles.md) record the measured
operations, host, samples and limits.
The concurrent map inventory records their lifecycle registration, observation and teardown sites.

### Server endpoint intake

`EndpointIntakeRoutes` owns one immutable host-and-path table containing each domain's configured
endpoint definitions and the source lifetimes bound to them. Domain install or teardown and source
bind or unbind derive a replacement from the current publication with `ArcSwap::rcu`. Concurrent
writers retry against the latest complete table, preserving unrelated domains and sources. A
passive installation publishes no endpoint definitions. The listening socket remains present on
every node independently of this table.

An HTTP request resolves one route using borrowed host and path keys; only uppercase hosts need a
normalization allocation. A WebSocket retains the resolved route and its signaling protocol from
upgrade through all later frames, including signaling data frames. Readers share the route
allocation and borrow its prepared intakes; they do not clone route vectors, programs, codecs, or
sender maps and do not return to a concurrent registry for each frame.

Each source lifetime publishes its optional intake through `ArcSwapOption`. Close, drop, domain
replacement, domain teardown, and runtime clear publish absence before withdrawing that lifetime.
Retained routes therefore refuse later admission even after a replacement source starts. A request
that already loaded a present intake keeps its lease until dispatch finishes. Unbind removes the
exact source allocation, so a late close cannot remove a replacement with the same typed identity.
Configuration withdrawal removes the route; source withdrawal alone leaves configured metadata
available to report unavailable intake. These publications remain volatile data-plane state.

Unit regressions and the registered Bolero operation sequence exercise the production owner and
retained lifetimes. Shuttle races whole-domain publication with resolution, unbind with admitted
requests and replacement, and domain teardown with retained routes. These checks treat publication
operations as opaque scheduling points. Both publications reuse the primitive boundary's existing
implementation: no new memory-ordering protocol is introduced, and Loom does not model their
internals. Listener ownership and network primitives retain their existing contracts; Turmoil is
outside this owner's scope. Public scenarios cover one and three nodes; the endpoint Chaos workload
belongs to Typed Ratchet 10. The inventory ledger records the routing measurement and verification
evidence.

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

### Checkpoint replication

Every replicated runtime state owns its replication: the domain offsets of a Kafka ingestor, the
state of a deduplicator, window or WASM processor branch, a materialized relay, an entity's
branch-aggregated metrics, and the branch lifecycle of a branch-keyed entity. On the node that owns
a placement, the replication records the highest revision each replica reported holding on its
stable storage, and offers the placement's newest checkpoint to the replicas that do not hold it
yet. On a replica, it carries the owner's announcements to the task that keeps the replica's copy
current. It lives and ends with the state it replicates, so a WASM checkpoint or a Kafka offset
commit offers its revision through the state it already holds, and no checkpoint, commit,
offer enters a node-wide announcement or progress map. Incoming frames still resolve their
placement in the existing state registry, and announcer steps still resolve the execution's
replicas; those recurring lookups are debt owned by Typed Ratchet 15.

A replica's progress only rises. Acknowledgements travel independently, and a replica acknowledges
what it holds again whenever it is offered a checkpoint, so an acknowledgement of an older revision
can arrive after a newer one; it leaves the newer one in place. Each wait for replicas registers for
the next report before it reads the progress: a WASM checkpoint waiting for the replicas its
boundary names, a Kafka offset commit waiting for its replica quorum, and a WASM state reset waiting
for the branch lifecycle that authorizes its first checkpoint. A report that lands between the read
and the wait therefore wakes the wait instead of leaving it to its deadline.

One announcer at a time offers a placement's revision. An offer raises the revision the running
announcer offers or, with none running, starts one. The announcer sends the revision to the
replicas that lag behind it, again every 100 milliseconds, and ends in the same step that finds
every replica the committed schedule assigns holding it, so a revision offered while it ends is
either sent by that step or starts the next announcer. It also ends when this node stops being the
placement's primary, when the replicated state goes away, and when the runtime stops. An announcer
whose task ends before it finished hands its announcement back to the next offer.

The announcement, the progress and the announcer's handover change together under a short lock
that belongs to the one placement and is never held across an await. The placement's originator,
its one announcer and its replicas' acknowledgements are the only participants; unrelated
placements never share it. An acknowledgement or an announcement names its placement, and the node
that receives it finds the placement's state in the registry that already keeps that kind of state,
through a borrowed read that creates nothing. On a replica, an announcement of a branch lifecycle or
of the state of one of its branches finds the entity's lifecycle, which the entity's replica task
retains. An announcement of a placement a node holds no state for wakes nothing.

A replica installs a branch checkpoint only while the branch lifecycle it holds names the branch.
Each lifecycle checkpoint a node holds is decoded once into the set of branches it names, so
installing a branch checkpoint looks its branch up in that set, through the lifecycle handle the
replica task retains, instead of copying and decoding the lifecycle each time. Holding a replica's
copy of a branch checkpoint compares revisions and moves the newer checkpoint in; its payload is
never copied under the registry's guard.

### Replica catch-up

A replica keeps each branch-keyed entity it replicates current through one replica task: the
entity's branch lifecycle and, for a deduplicator, window or WASM processor, the state of every
branch. The task retains the entity's lifecycle handle and alone owns what it learned: the owner's
catalog as far as it read it, the revision this node holds of each branch it looked at, and the
branches it still has to fetch or acknowledge. Nothing else reads or changes that record, so it
takes no lock.

The owner keeps, with each entity's lifecycle, a catalog of the newest replicable revision of every
branch state it owns. A branch state registers when it is created, records every revision it offers
its replicas, each generation a deduplicator or window branch publishes and each WASM checkpoint on
the owner's stable storage, and leaves the catalog when it goes away. A branch state that another
one of the same branch replaced neither changes nor removes its successor's entry. The catalog is
one immutable value that each change replaces by a read-copy-update, so recording a revision never
waits for a reader and a reader never waits for a branch. Every change is numbered, and a replica
asks for the changes after the cursor its previous read returned. A cursor of another catalog, a
cursor older than the oldest removal the catalog kept, or no cursor at all restarts the listing
from the catalog's beginning. A listing is paged, 256 changes at a time.

A round runs when the task starts, when the owner announces a checkpoint, and once every replication
poll interval. It synchronizes the lifecycle, reads the catalog's changes, and looks only at the
branches that changed, that the lifecycle names for the first time, that the owner announced, or
whose earlier step failed. The task reads what this node holds of a branch once, from its own copy,
and keeps that current itself. A round in which no branch changed therefore sends two requests to
the owner, reaches no shared map for any branch on either node, and does no work per branch, however
many branches the entity has. The branches that changed are fetched and installed at most 16 at a
time.

The owner's announcements of the entity's lifecycle and branch checkpoints are left with the
entity's lifecycle handle on the replica, the newest one of each, under a short lock that belongs to
that one entity and is never held across an await, and each wakes the task. The task takes them all
at the start of its next round, so an announcement that lands while a round runs is taken by the
next one, and one of a revision the replica already holds is acknowledged again. An announcement
that is lost delays a checkpoint by at most one poll interval.

### Relay branch presence

A relay's owner task owns its concrete branch instances: their activity order, the TTL scan that
expires them, and the LRU eviction that bounds them. It keeps them in an ordinary indexed map beside
a persistent set of their keys, and admits, evicts and expires through one owner type that changes
both in the same step. A step that changes the membership publishes the set, sharing its structure
with the membership published before it, so creating or releasing one branch copies only the path
to that branch, never every branch the owner holds. A batch that creates one branch and evicts
another publishes once, so an observer never sees the relay above its capacity. A batch for an
established branch refreshes that branch's activity in the owner's map and publishes nothing; it
takes no lock and writes no shared timestamp.

Each owner lifetime claims the presence when its task is spawned. The claim publishes an empty
membership that replaces whatever an earlier owner left behind, including the branches of an owner
aborted before its teardown ran, and from then on only the claiming owner changes what the presence
publishes: a replaced owner that is still ending compares its lifetime with the published one and
publishes nothing. Dropping the owner state, whether the task finishes its drain or is aborted,
releases the presence by publishing an empty membership unless a successor has claimed it.
Publication and claims are one `ArcSwap` compare-and-swap or read-copy-update each; the protocol
adds no atomic of its own.

The owner publishes a new branch before it fans out the batch that created it and an eviction
before it invalidates the evicted branch's delivery slots, so a consumer that has processed a batch
reads a membership that holds its branch.

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
Remote ACK progress publishes a monotonic sequence with the root's parked handoff state. An
upstream remote share follows that state, and its root publishes the resulting transition farther
upstream. A delayed progress event cannot reactivate a newer parked state. Terminal ACK resolution
still owns the pending share and releases the parked guard with it.

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

### Client emitter attempts

Each running native client emitter has one actor for its volatile output. Its channel serializes
consumer attach/detach, prepared-payload publication, assignment, settlement, timeout and
cancellation. An attempt has exactly one owner and ACK reference. Revocation returns its worker's
batch and byte credit and marks that reference stale before the payload can be assigned again.
Sequential dispatch checks every earlier unresolved delivery of the same source relay and
concrete branch; parallel dispatch counts assigned attempts against that stream's shared window.
The actor keeps only a bounded queue of completed references for duplicate ACK recognition.

The node's consumer grant and retained-output byte counters are separate atomics. A session's
consumer count and credit use one short mutex; neither that mutex nor a runtime map guard crosses
an await. A retained payload reserves actual bytes until application settlement or cancellation,
and its prepared Arrow bytes and source members remain owned by the emitter host until the
result is applied. A consumer read uses an asynchronous receiver lock only for that consumer; it
cannot hold the session receive loop or the ordered command lane. Settlement is a concurrent
request, so a quiescing command cannot prevent the application ACK it waits for.
The `shuttle_competing_consumer_grants_never_exceed_the_node_budget` check explores competing
session grants against the production atomic budget. The
`shuttle_consumer_loss_and_ack_race_release_one_retained_delivery` check runs the production
delivery owner with deterministic attempt references and no timer wakeups to explore ACK versus
consumer detachment. The `shuttle_publish_cancellation_and_ack_race_release_one_reservation`
check also races publisher cancellation against application ACK and verifies retained byte credit
returns once. Timeout revocation is covered by the ordinary owner and public scenarios.

### Rust client attachment recovery

The client retains desired producer and consumer handles independently of their wire attachments.
Their weak registries are reached for attachment registration, admission changes, termination and
reconnect snapshots, not for discovering an attachment on each submission or delivery. Producer
wire entries include the exchange generation beside the attachment ID. A snapshot upgrades live
weak handles and releases its map guard before any handle transition or network await. Close/drop
removes desired entries, exchange termination removes its producer wire entries, and snapshots
prune expired weak handles. The
[concurrent map inventory](https://github.com/nervix-io/nervix/blob/main/tests/concurrent-map-inventory-ledger.md#rust-client-attachment-recovery)
records each map's access frequency and disposal.

Each desired handle has one lifecycle owner, with a short selected mutex over its typed attachment
phase and a watch notification for changes. Beginning or installing a restore checks that phase
and its exchange generation under the same mutex as close. Close is terminal: a restore reply
that loses that transition cannot install its attachment and releases the grant it obtained.
Another exchange loss can interrupt only the restore or active attachment of that exchange.
No lifecycle guard crosses an await. Submissions and deliveries keep their originating attachment,
so replacing it cannot retarget an uncertain submission or an application ACK.

The four production-owner Shuttle checks listed below race close with starting restoration and
with another loss during restoration. They establish that the handle stays closed and cannot
begin a subsequent restore. Public scenarios cover exchange I/O, late open replies, reservation
cleanup and delivery interruption; those network effects are outside these in-process checks.
[Client Session Protocol](./client-session-protocol.md) and its implementation manual own the
restoration and application-outcome contract.

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
The ingestor route task, ingestor branch maintenance loop, and processor branch task all take this
observation before deciding which force-flush and deadline arms to enable. Their wake contract is
therefore the watch owner's registration order, checked by
`shuttle_an_ownership_handoff_freeze_observation_registers_before_its_read` against the production
release path. Other data-plane waits on `watch` subscribe before their deciding read, or use an
existing receiver whose `borrow` leaves a subsequent `changed` able to see an intervening send.

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

An owner buffer admits a batch before its fan-out begins. Fan-out therefore takes a nonwaiting
dispatch permit before it reads any local or remote runtime-consumer route and holds the permit
until fan-out has attached the consumer ACK shares and resolved the owner's share. A schedule swap
cannot replace the routes while that permit is live. If the gate has closed first, fan-out fails
the record ACKs and returns the batch for source retry. Waiting for the gate here would deadlock
the swap, because the swap's drain counts the
buffered batch. The same sequentially consistent gate protocol orders the nonwaiting attempt and
the swap engagement.

The three-node attached-emitter move scenario injects a stale zero buffered-batch drain report
while an owner batch is paused. That puts fan-out beside the local swap even when the coordinator's
usual drain would wait for the batch, and verifies that the local gate independently prevents an
ACK for a batch the moved consumer missed.

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

- a quiesced ingestor, and one draining what it retained, locks only the bounded retained-payload
  buffer selected by its declared `MAX SIZE` policy: to retain a payload, to take the oldest out for
  delivery, and to end that delivery. No guard crosses an await.
- a retained payload whose unfolding finds the extension class's wait queue full waits for a place
  in that queue. The executor's semaphore hands a freed place to waiting jobs in the order they
  asked, ahead of any job asking afterwards. The wait is scoped to the one class, holds no lock,
  and ends when a place frees or when the ingestor's shutdown or a new quiesce interrupts it
- a live payload a paced source or a source whose loop may be held handed over without an
  acknowledgement waits for a place the same way. The source host registers for the next quiesce
  publication before it compares it with the decision the payload was taken in under, so a new
  decision always ends the wait, and the host then decides on the payload again; the ingestor's
  stop ends it as well. The race holds no lock and no guard across an await
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
  progress reaches the waiting branch through the replication of its own state.
- one placement's replication changes its announcement, its replicas' reported progress and the
  handover between its announcers under a lock scoped to that placement, taken by its originator,
  its one announcer and its replicas' acknowledgements, and never held across an await; see
  [Checkpoint replication](#checkpoint-replication)
- a replica's copy of one branch checkpoint is held under the registry's guard for one revision
  comparison and a move, never a copy of its payload
- the owner's announcements pending for one branch-keyed entity on a replica are added to under a
  lock scoped to that entity, one newest announcement per branch, and taken all at once by the
  entity's one replica task, never across an await; see [Replica catch-up](#replica-catch-up)
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

`just ratchet`, `just validate` and `just validate-ci` require the pinned compiler's Nervix
source diagnostics to pass across the complete declared matrix. Finite API recognition lives in
`tools/nervix-lint/report/src/rules.rs`. Architectural contracts live with their source owners;
generated inventories under `target/` are evidence, never approval inputs. Other structural
ratchets retain their own checked-in counts.

The compiler recognizes resolved Mutex, RwLock and DashMap acquisitions through aliases,
re-exports, dereference adjustments, UFCS and selected trait implementations. Borrowed reads,
mutating and try operations, and iteration remain visible. Ordinary collection entries,
held-guard entries and I/O do not acquire a recognized lock. Borrowed DashMap iteration acquires
lazily; consuming iteration owns its storage. Authored arguments remain checked through external
macros, including operation expectation macros.

### Source contracts

Gate annotations with `cfg_attr(nervix_lint, ...)`. Only the isolated analysis driver registers the
`nervix` tool and enables that cfg; ordinary stable and browser source needs no unstable feature.
Contracts use the narrowest meaningful function, trait, type, impl, module or crate boundary:

```rust,ignore
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "one callback per admitted batch"))]
fn process_batch() { /* retained execution state */ }

#[cfg_attr(nervix_lint, nervix::context(bounded,
    reason = "the retained grant serializes its terminal transition",
    key = "one admitted relay grant", bound = "one synchronous transition; no guard across await"))]
struct GrantState { /* retained bounded state */ }
```

`recurring` identifies record, batch, frame, acknowledgement and steady poll execution.
`lifecycle` identifies installation, replacement, snapshot cadence and teardown. `observer`
identifies observation outside execution, and `outside` names an edge or harness outside this
rule's data plane. `bounded` remains recurring but permits the explicitly described retained
protocol; its reason, identity key and capacity or deadline are required. These are reviewed
architectural assertions, not proofs of exclusive ownership, bounded waiting or runtime cadence.

An explicit callable contract wins over defaults. An implementation inherits its trait method's
contract before its impl/type or lexical defaults; a recurring trait method's override remains
recurring or names a bounded protocol. A type contract applies to its implementations. Duplicate,
conflicting, malformed and misplaced annotations fail. A binding whose initializer owns exactly one
anonymous body can supply that body's context: use it when a constructor installs a recurring task. Nested task bodies
still require their actual entry contract.

The compiler re-evaluates supported local call edges and closure/callback bodies to a fixed point.
A recurring caller makes an unannotated local helper recurring, including one in a lifecycle module.
An explicit lifecycle callable is an installation boundary: entering it from recurring execution
emits `nervix::lifecycle_call`. Callee and trait contracts survive cross-crate metadata and renamed
imports. During recurring execution, unresolved generic, dynamic trait and function-pointer
application calls require a callable/trait contract or `nervix::dispatch(reason = "...")` on their
owning callable; a cold module default is insufficient. A dispatch contract states the external
driver or callback boundary; it does not exempt compiler-visible local callback bodies or acquisitions.

Local iteration and polling implementations remain call edges. Generated `await` polling is the
Rust scheduling protocol, rather than a separate application callback diagnostic. Synchronization
inside external libraries, opaque external future implementations and unsupported whole-program
relationships remain outside the recognizer's claim. The checker supplies no universal ownership
or effect proof.

### Diagnostics and reviewed exceptions

`nervix::sync_acquisition` diagnoses an acquisition on ordinary recurring execution.
`nervix::unknown_effect` diagnoses an acquisition without a context or an unresolved application
call reached from recurring execution. `nervix::invalid_contract` rejects contract and exception
errors. The lints use ordinary Rust warn/deny/expect levels; the required gate also rejects unresolved Nervix warnings.

Retained repair debt names its owning task at the exact operation:

```rust,ignore
let channel = nervix_primitives::expect_lint!(nervix::sync_acquisition,
    "Typed Ratchet 03 (86bc9eqjv): retain the channel selected at branch installation",
    channels.get(&branch));
```

`expect_lint!` puts a normal reason-bearing Rust expectation on one binding, evaluates the
operation once and returns its value. It supplies stable syntax and introduces no runtime policy,
allocation or lock. A direct gated expectation on one operation binding works too. A lifecycle
call admitted only during first installation or terminal teardown explains that phase and its
concrete lifetime in the same narrow form. Blanket allow, undocumented or broad expectations,
lint caps and unfulfilled expectations fail. The checker counts distinct HIR operations against
Rust's effective expectation identity, including macro expansion; widening a binding to cover a
second operation fails even when Rust would regard the expectation as fulfilled.

Task handles remove recurring status, freeze, metric and checkpoint lookups. Source readiness
registry reads during polling remain operation-specific debt for Typed Ratchet 03, alongside
relay/channel selection. Typed Ratchet 04 owns materialized branch discovery and Typed Ratchet 05
owns remaining remote acknowledgement/admission discovery. Replica catch-up retains the entity's
lifecycle handle and its own record of each branch, and Typed Ratchet 15 owns state-replication
frame, synchronization, listing and announcer registry reads.
Bounded retained placement progress remains explicitly documented. Testing fault selectors name
their exact emitter, ingestor, domain/branch, checkpoint window or acknowledgement link and finite
read/removal steps; they release map guards before a scenario-controlled pause. These contracts
describe test selection, not a product wait deadline. The executor-saturation lookup belongs to
testing fault control. The [concurrent map inventory](https://github.com/nervix-io/nervix/blob/main/tests/concurrent-map-inventory-ledger.md)
records the runtime ownership review; source contracts and expectations are the executable policy.

### Complete analysis and qualification

The isolated tooling workspace uses `nightly-2026-09-17`; product builds use stable. The matrix
analyzes ordinary workspace libraries/binaries, the server's testing capability,
server/interconnect/primitives under Shuttle, server/consensus/primitives under Loom,
interconnect/primitives under Turmoil, and the server's library and binary, the deadlock
diagnostics and primitives under Deloxide, where every blocking acquisition resolves to a tracked
adapter that the rules recognize as the lock it wraps. Turmoil's runtime cfg comes only from its just recipe,
which preserves the parent's diagnostic mode. Compilation supplies no concurrency execution
evidence. Test targets, browser analysis and undeclared feature combinations are not claimed.
Mandatory source/manifest checks retain import provenance, inactive cfg and authored macro checks.

Cargo artifacts, complete side reports and declared roots must agree, including when there are
zero sites. Reports retain every owner, configuration and expansion instance; generated target
and external source sites are exclusions. Completion identities cover current source and
annotations, Rust rules, compiler, driver, validator, locks, dependency declarations, Cargo
configuration, diagnostic mode and relevant flags. Source changes during a run fail. The workspace
wrapper nests beneath configured kache. Missing reports establish a new supported cache namespace
and rebuild only isolated authored artifacts; the gate preserves `RUSTC_WRAPPER`.

`just test-typed-ratchet` qualifies APIs and source diagnostics in the legal modes, ordinary Bolero
properties, stable gated syntax and paired Rust API doctests. `just qualify-typed-ratchet-cache`
qualifies fresh/Cargo-fresh runs, an actual kache dependency hit, changed inputs, missing and
interrupted output, and two worktrees. Native coverage uses the matching LLVM tools and records
the driver executions that ran. Sanitizer campaigns follow the shared Bolero label policy.
Calibration, measured cost and complete generated evidence belong on
[Typed Ratchet 02A](https://app.clickup.com/t/86bcau18u).

Use the generated listing during review:

```text
just typed-ratchet --show
just typed-ratchet --configuration shuttle --inventory
```

Partial selections and warning-mode inventories identify their diagnostic/matrix scope explicitly;
they cannot satisfy the required full diagnostic gate.

For every new or moved site and every shared-map access, the reviewer
establishes all of the following:

1. **Frequency.** Trace its callers and decide whether it can run per record, row, batch, remote
   frame, ACK share, or steady poll iteration. A site on one of those paths is rejected unless it
   is an ordering fence or bounded protocol described by this chapter. Registration, first
   installation, replacement, teardown and observer reads are cold and are classified as such,
   separately from the recurring path.
2. **Owner.** Identify the one component that changes the state. Immutable reconfiguration uses a
   whole-value publication, a scalar uses an atomic, a stable dependency is bound when the task
   starts, and branch-local state belongs directly to the branch task. An ordinary collection that
   one owner holds, and its `entry` API, are not synchronization and need no justification.
3. **Lookup behavior.** A steady path does not look an established value up in a shared map, with
   `entry()` or with a borrowed `get`, `contains_key` or iteration, all of which take a shard lock.
   It retains the handle resolved when its task, branch, channel or attempt was created. `entry()`
   belongs to a cold or racing installation whose single-winner property is part of the contract.
4. **Fence contract.** An allowed ordering fence or bounded protocol names the order it preserves,
   the exact key that limits its scope, the capacity or deadline that bounds waiting, and whether
   any guard crosses an await. Unrelated branches, relays, destinations, or assignments must still
   progress.
5. **Lifecycle separation.** Startup, registration, schedule application, snapshot sealing, and
   teardown may synchronize with their peers, but their guards must not leak into a hot callback or
   be held while awaiting data-plane work.
6. **Mechanical result.** Run `just ratchet`; inspect the site list when the count changes; update
   the baseline only when the count fell. A lower aggregate count does not make a newly introduced
   hot-path lock acceptable, and renaming a method or moving a file is not a repair.
7. **Inventory.** Record a new concurrent map, or a changed frequency or disposition of an existing
   one, in the concurrent map inventory in the same change.

The companion `write_once_rwlock_fields` count rejects names and shared references stored as
`RwLock<Option<...>>`. Such a field states that readers should coordinate forever around a value
whose actual lifecycle is publication. The current shape uses an atomic optional reference and
deletes the lock-backed form.

## Execution-Sensitive Primitives

Every execution-sensitive primitive Nervix uses comes from the `nervix-primitives` crate, which sits
below the vocabulary and selects each family for the build's execution mode: ordinary execution,
Shuttle, Loom, Turmoil or the Deloxide diagnostic mode. A build uses one mode across its whole
dependency graph. Every package that owns a `shuttle`, `loom`, `turmoil` or `deloxide` feature
forwards it to the primitive crate, and Cargo unifies
that crate's features, so a vocabulary type, an engine and the server compiled into one test binary
all use the same backend. Selection depends only on features, never on `cfg(test)`. Enabling two
modes fails to compile with a diagnostic that names both, including when two different dependencies
each enable one.

Ordinary execution pays nothing for the boundary: each path re-exports the library item itself,
with no wrapper, allocation, dispatch or scheduling point, so a primitive on a hot path costs
exactly what it did before. The atomic and shared-ownership families are portable and build for the
browser console. Everything else is the explicit `native` capability, and asking for a capability or
a mode the target cannot provide is a compile error rather than another implementation: a mode or
the `native` capability requested for the browser's target fails with the boundary's own diagnostic
as its first error, because the libraries of every mode and of the native capability exist only for
native targets and none of them is compiled there.

| Family | Path | Ordinary, Turmoil and Deloxide | Shuttle | Loom |
| --- | --- | --- | --- | --- |
| Atomic values, `Ordering`, `fence` | `sync::atomic` | The standard library's | Modeled: every operation is a scheduling point, and every ordering behaves as `SeqCst` | Modeled: explored under the C11 orderings Loom supports |
| Shared ownership: Nervix-owned references, and the standard strong and weak references a weak reference or an external API such as Arrow or a Tokio semaphore requires | `sync::Arc`, `sync::StdArc`, `sync::StdWeak` | `triomphe`'s `Arc`, and the standard library's `Arc` and `Weak` | Real, with no scheduling point: no reference count is modeled | Real, outside every model |
| Async locks, semaphores and one-time cells; the `mpsc`, `oneshot` and `broadcast` channels | `sync` | Tokio's | Modeled: Shuttle's Tokio | Real: Tokio's, outside every model |
| `Notify` and the `watch` channel | `sync` | Tokio's | Modeled: the boundary's own, with Tokio's semantics and a scheduling point before every registration, notification and deciding read | Real: Tokio's, outside every model |
| The waker registration of one waiting task | `sync::AtomicWaker` | The `futures` crate's | Opaque, with a scheduling point before and after each registration, wake and take | Real: the `futures` crate's, outside every model |
| Cancellation tokens and their guards | `sync` | Tokio Util's | Modeled: Shuttle's token, wrapped for Tokio Util's clone identity and owned operations | Real: Tokio Util's, outside every model |
| Blocking locks and their condition variable | `sync::blocking` | `parking_lot`'s; under Deloxide, the boundary's tracked adapters over Deloxide's locks, described in [Diagnostic deadlock detection](#diagnostic-deadlock-detection) | Modeled: Shuttle's `parking_lot` locks, and the boundary's condition variable over them | Real: `parking_lot`'s, outside every model |
| Barriers, `Once` and the synchronous channel | `sync::blocking` | The standard library's | Modeled: Shuttle's | Real: the standard library's, outside every model |
| `OnceLock` | `sync::blocking` | The standard library's | Opaque, with a scheduling point before and after each read and write; no initializing read | Real: the standard library's, outside every model |
| `LazyLock` | `sync::blocking` | The standard library's | Unavailable | Real: the standard library's, outside every model |
| Task spawning, joining, yielding, aborting, tracking and the cooperative budget | `task` | Tokio's and Tokio Util's | Modeled: Shuttle's Tokio, with the boundary's abort-on-drop handle | Real: Tokio's and Tokio Util's, outside every model |
| Blocking the runtime worker thread that calls it: the declared owners outside the executor | `task::block_in_place` | Tokio's | Unavailable | Real: Tokio's, outside every model |
| Running a CPU job the bounded executor admitted | `task::spawn_cpu` | Tokio's blocking pool; under Turmoil, one task of the simulated host's scheduler | Modeled: Shuttle's `spawn_blocking` | Real: Tokio's blocking pool, outside every model |
| Running a job on the blocking pool itself: the executor's storage jobs, and the declared owners outside the executor | `task::spawn_blocking` | Tokio's blocking pool | Modeled: Shuttle's `spawn_blocking` | Real: Tokio's blocking pool, outside every model |
| Timers and the monotonic clock: sleeps, deadlines, timeouts, intervals and instants | `time` | Tokio's, following the clock of the runtime that polls them: the operating system's, a test's paused clock, or the simulated host's under Turmoil | Shuttle's: a sleep or an interval's tick is one scheduling point that takes no time, and a timeout expires only when a check triggers it; `Instant::now` reads the operating system's clock | Real: Tokio's, outside every model |
| Controls of a paused clock: `pause`, `advance` and `resume` | `time`, with the `test-util` capability | Tokio's | Shuttle's, which take no time | Real: Tokio's, outside every model |
| Sockets: TCP listeners and streams with their owned halves, UDP and local sockets | `net` | Tokio's over the operating system's network; under Turmoil, Turmoil's simulated sockets and `lookup_host`, which answers from the simulated DNS table, and no local socket or socket configured before it connects | Real: Tokio's, outside every model. A Shuttle execution has no Tokio reactor, so a socket created inside a check panics and fails it | Real: Tokio's, outside every model |
| The runtime, `#[nervix_primitives::test]`, `#[nervix_primitives::main]`, `select!` | `runtime`, crate root | Tokio's | Shuttle's runtime and `select!`; the attributes build Shuttle's runtime | Real: Tokio's, outside every model |
| Streams over channels | `stream` | Tokio Stream's | Shuttle's Tokio Stream | Real: Tokio Stream's, outside every model |
| Atomic reference publication and its cache | `publication` | ArcSwap's | Opaque, with a yield before and after each load, store, compare-and-swap and read-copy-update | Real: ArcSwap's, outside every model |
| Concurrent maps | `collections` | DashMap's | Modeled: Shuttle's DashMap | Real: DashMap's, outside every model |
| Lock-free queues | `collections` | Concurrent Queue's | Opaque, with a scheduling point before and after each operation | Real: Concurrent Queue's, outside every model |
| Threads: spawning, joining, building, scopes, parking, sleeping, yielding | `thread` | The operating system's | Modeled threads; sleeping and parking with a timeout are scheduling points that take no time | Modeled: Loom's threads to spawn, build, join, park and yield. Real: the operating system's sleeping, timed parking and scopes, outside every model |
| A thread nothing joins | `thread::spawn_detached` | A named operating-system thread | A detached Shuttle task, abandoned when the model's main thread returns | Real: a named operating-system thread, outside every model |
| Thread-local storage | `thread_local!` | The standard library's | Shuttle's, one per task | Modeled: Loom's, one per model thread |
| The host's parallelism | `thread::available_parallelism` | The host's | The host's | The host's |
| Deadlock findings, and the detector that reports them | `deadlock` | The findings in every mode; the detector only under Deloxide | The findings; no detector | The findings; no detector |
| Real primitives outside every model | `unmodeled` | Real | Real | Real |

Every exposed operation has one of three classifications in each mode. A **modeled** operation is
observed by the mode's engine, and a check may make claims within that engine's limits. An
**opaque** operation runs a real primitive between explicit scheduling points: the scheduler can
order an owner's use of it against other tasks, but the primitive's own instructions and memory
ordering stay unmodeled, so a check claims nothing about them. An operation **outside the modeled
execution** is a real primitive reached through `nervix_primitives::unmodeled` under a named
permission, and supplies no evidence about a checked protocol. An operation a mode cannot provide is
unavailable in that mode and fails to compile; it never falls back to a real primitive. Shared
ownership is real in every mode and on every target: no model checker counts references, so a check
claims nothing about an ordering a reference count establishes, such as the one `get_mut` or
`try_unwrap` relies on. Loom models isolated synchronous owners: atomics, the threads a model
spawns, joins, parks and yields, and thread-local storage. It models no async family, lock,
collection or publication, so a Loom build, in which the server and consensus compile for the models
of their owners, takes the ordinary library for each of those, outside every model.
`just validate-primitive-boundary` rejects every such family in Loom model code, an inline module or
a module file whose declaration compiles it only for Loom, so a model never names a real primitive
silently; what an owner a model drives uses internally is excluded from that model's claim. The check
reads each `cfg` of a module through its `all`, `any` and `not`: a module is Loom model code when one
of them is false in every build without the `loom` feature, and a module whose `cfg` the check
cannot read is rejected. Model
code may name shared ownership and a permitted unmodeled primitive, because each says it is real.
`just test-primitives` shows each mode's selection.

Turmoil replaces the network and the clock, not synchronization. A Turmoil build runs the ordinary
primitives on the simulated host whose task uses them: `net` selects Turmoil's sockets, and
`net::lookup_host`, which only that mode has, answers from the simulated DNS table; the timers and
instants of `time` follow the host's simulated clock; and `task::spawn_cpu` runs an admitted CPU job
as one task of the host's scheduler, so its synchronous body is one scheduling step. What the
simulated hosts run, and what the simulation establishes, belong to
[Deterministic interconnect simulation](./interconnect-simulation.md).

Deloxide models nothing and replaces only the thread-blocking locks and their condition variable. A
`deloxide` build runs every other family as ordinary execution does, and its tracked locks are real
locks whose acquisitions a deadlock detector also observes, so it runs as a product does and claims
no interleaving. What it reports, and what it cannot, belong to
[Diagnostic deadlock detection](#diagnostic-deadlock-detection).

The boundary supplies mechanisms and decides no policy. `net` never decides what a name means: a
node resolves through its own resolver in `nervix-dns`, no mode but Turmoil's offers a lookup, and
the boundary check rejects `tokio::net::lookup_host` and the `ToSocketAddrs` traits, which resolve
through the operating system. `time` grants no clock permission: actual UTC and physical deadlines
keep the owners `scripts/check_clock_boundaries.py` declares, and a logical deadline stays in the
domain clock's coordinate, as [Domain Clock](./domain-clock.md) describes. `task::spawn_cpu` belongs
to the bounded executor, which admits, charges and cancels every job; the boundary check rejects it
in any file but the executor's worker pools, so it is no way around admission. `task::spawn_blocking`
is the runtime's blocking pool, which the executor's storage workers run on, and
`task::block_in_place` blocks the runtime worker thread that calls it, which no file owns by
default. Work a node runs off its async workers goes through the executor instead, so the check
rejects either in any other file unless a permission in `crates/primitives/blocking-permissions.toml`
lists it for that file, with the owner, why the owner stays outside the executor, and what bounds
its work instead, as [Blocking work outside the executor](#blocking-work-outside-the-executor)
lists. `pause`, `advance` and `resume` exist only with the `test-util` capability, which ordinary
tests of elapsed-time behavior enable and no production graph has, because a clock that can be
paused is checked on every read.

The runtime attributes build the selected runtime through a crate path fixed to the boundary: Tokio's
attribute builds whatever runtime its crate path names, and an attribute that named `tokio` at the
call site would construct whichever crate a package happened to call `tokio`. Shuttle's own test
attribute finds its runtime by reading the calling package's manifest for a dependency of a fixed
name, so the boundary does not use it; in a Shuttle build a test attribute builds Shuttle's runtime,
which runs only inside a Shuttle execution.

A modeled primitive exists only inside a run of its model. Using one outside that run is a test
configuration failure the backend reports, and nothing falls back to a real primitive. A real
primitive that must stay outside every model is reached through the unmodeled path, and each use
needs a permission in `crates/primitives/unmodeled-permissions.toml` that names the file, the items
by their paths below `unmodeled`, the owner, why a real primitive is required, and what that leaves
unverified:

| Owner | Why the primitive is real | What stays outside every check |
| --- | --- | --- |
| The Shuttle and Loom runners of `nervix-model-harness` | Their statistics span every model execution they start and are read after the last one | Nothing a check claims; they are runner bookkeeping |
| The WASM runtime's epoch driver and guest-callback time | The epoch driver is an operating-system thread no model runs, which sleeps for real and is stopped by a flag; a guest callback reads its domain time from Tokio's task-local storage, which no model isolates | When the epoch thread observes shutdown, and a callback's time scope, which Shuttle tasks would share while polled |
| The C binding's session | The binding owns the runtime its host's threads enter and shuts it down in the background, which is its contract with the host and which Shuttle's runtime does not provide | Everything the binding runs; the Rust client it drives has its own checks |
| The cluster membership transport | Chitchat hands out Tokio's own lock and watch receiver and runs on Tokio's runtime, so the transport tasks it drives wait, sleep and select on Tokio's primitives | Cluster membership gossip |
| Process-wide one-time state: the SIMD instruction level, the interconnect's cryptography provider, and the test and benchmark schema and certificate fixtures | Initialized once per process and read by every later model execution in it | Detection, installation and fixture construction |
| The browser console: its executor installation, and the `futures` channel, `select!` and abort handles its tasks use on the browser's event loop | The browser target runs in no execution mode and has no native capability, so the console takes those families from the `futures` crate, which a native build takes from Tokio | Everything the console runs |
| The VM benchmarks' allocation probe | A global allocator counts allocations made on every thread | Nothing; the benchmark claims nothing about synchronization |
| The application unit-test fixtures | Test databases and node ports must differ across every unit test the process runs in parallel, so their identities outlive each test | Nothing a check claims; no model builds the fixtures |
| The records of the relay gate and fan-out, entity gate, emitter record-write, durability barrier, WASM checkpoint, source host-loop, stream-slot, retained-archive and client ingestor Shuttle checks | A record changes in the same scheduling step as the operation it records, so recording adds no scheduling point | Nothing the owner does: records observe and never synchronize, and the owners' own primitives are modeled |
| The Turmoil runner's real-time bound and the scenario driver's attempt deadline | A run's bound must expire even when a host blocks the scheduler thread and simulated time stops advancing, and the driver kills an attempt process that outlived its bound; both read the operating system's monotonic clock | Nothing a scenario claims: they decide only when a run fails, never how a simulated request proceeds |
| The integration-test harness's port pool and the benchmark driver | They reserve free ports from the operating system for real node processes and containers, synchronously and before anything binds them, with the standard library's blocking listener | Nothing a check claims; the ports carry the traffic of real processes |

A real primitive never carries the protocol under test, chooses its branches, supplies its wakeups
or establishes an ordering an assertion relies on. A permission covers one Rust file the boundary
governs and lists real items of the unmodeled path; a permission for a directory, a glob, a file
outside the governed sources, or an item the path does not have is rejected, and so is a permission
or a listed item nothing uses.

A selected atomic belongs to the model execution that constructs it, so it never lives in a
`static`. A static is constructed once per process and would carry its state from one execution into
the next, and Loom's atomics have no const constructor, so a crate that declares one does not build
with Loom at all. Process-wide state lives on the owner whose lifetime it has instead: a node's
session service owns the draws its subscriptions sample with, and a node's Raft network owns the
identities of the snapshot transfers it sends. A count a unit test reads is kept per thread, and
state that must outlive every test and model is a real atomic under a permission, which may live in
a `static`.

`just validate-primitive-boundary` rejects every other path to a governed family however it is
spelled: a direct, renamed, grouped or glob import, a fully qualified path, an attribute, a renamed
crate, an imported `sync`, `time` or `net` module, or a path in a macro body or an inactive `cfg`
branch. That covers shared ownership through `triomphe` or the standard library's `sync`, the
`futures` crates' channels, locks, executors, waker registration, `select!` and abort handles,
Tokio's `time` and `net` modules, Turmoil's `net`, Deloxide's locks and detector, and the standard
library's `Instant` and sockets. Each rejection names the approved path; `tokio::net::lookup_host` and the `ToSocketAddrs` traits
name the node's resolver instead. It also rejects an unmodeled use without its permission, a stale
or misplaced permission, a dependency on a library whose family the boundary selects from any
package but the boundary, a manifest entry that renames a governed crate, `task::spawn_cpu` in
any file but the executor's worker pools, `task::spawn_blocking` in any file but those pools and
the owners a blocking permission declares, `task::block_in_place` in any file but the owners a
blocking permission declares, and a blocking permission that lists an item its file no longer
names. The source and manifest rules read every tracked file and every new file Git does not
ignore, whether or not the workspace lists its package: the isolated analysis workspace under
`tools/nervix-lint`, with its driver, fixtures and fixture macros, is authored source like any
crate. What a build wrote is not: Cargo tags each build directory it creates with a `CACHEDIR.TAG`,
and the check reads nothing below a tagged directory, wherever it is nested. Turmoil is a runner as
well as the network the boundary selects, so beside the boundary, a package whose harness drives a
simulation may depend on it, only as an optional dependency its own `turmoil` feature enables.
Deloxide is the boundary's alone: no other package depends on it, and the boundary depends on it only
optionally, so no ordinary graph contains it.

An execution mode is a feature of the boundary, never a global cfg: `--cfg loom` would reach every
crate of a build, and Tokio and other dependencies read the same names. The check rejects a bare
`loom`, `shuttle`, `turmoil` or `deloxide` in a `cfg` predicate, and `--cfg` with any of those
names in any `justfile` recipe, Cargo configuration, workflow or build script. Tokio's
unstable runtime controls belong to the Turmoil build: the check rejects `--cfg tokio_unstable` in
any `justfile` recipe but a Turmoil one, and in Cargo configuration, a workflow or a build script.

The analysis cfg of the [source contracts](#source-contracts), `nervix_lint`, is tooling only and
selects nothing. Only the analysis driver sets it, for the crates it analyzes, and the analysis must
read the code a product build compiles. The check rejects `--cfg nervix_lint` in any `justfile`
recipe, Cargo configuration, workflow or build script, a `cfg` or `cfg!` predicate that names it,
and a `cfg_attr` under it that holds anything but `nervix::` contracts and lint levels, such as a
`path`, a `derive` or a nested `cfg`. An annotation hides nothing from the other rules: a governed
path is rejected in the code an annotation gates as it is anywhere else.

The check rejects a `static`, including one a `thread_local!` declares, whose declared type names a
selected atomic, directly or through a wrapper, an array, a reference, a module path or a local type
alias, and a `static` or `const` initializer, `const fn` or `const` block that constructs one. It
reads declared types and constructions, so a struct holding an atomic that a static builds lazily is
left to review.

Guest code is outside the source rules: the WASM guest SDK and every guest library built on it are
compiled into a user's WASM guest, a single-threaded program in the host's sandbox where no mode
exists and no Nervix process runs, and a user's guest takes Arrow's standard references without the
boundary. The rules still read their manifests. Tokio's I/O traits, filesystem, process and signal
modules are real in every mode, and so are the pure polling combinators of Tokio, `pin!` and
`join!`, and of the `futures` crates, which poll futures without synchronizing anything of their
own. The browser's event loop, its executor reached through `wasm-bindgen-futures` and Leptos and
its timers through `web-sys`, exists only in a target no mode runs. The cells of `std::cell` are
confined to one thread. None of these is a family the boundary selects.

### Builds, modes and product binaries

Each mode is its own build invocation, with explicit features, because Cargo unifies the features of
one invocation:

| Mode | Lint | Tests | Coverage |
| --- | --- | --- | --- |
| Ordinary | `just lint`: every package in ordinary mode, the server, the client, the formatter, the browser console and the wire crate for the browser | `just test`, `just test-primitives-ordinary` | `just test-coverage`, `just coverage-native-extras` |
| Shuttle | `just cargo-clippy-shuttle`: every Shuttle library, and each package `just test-shuttle` explores in test mode, where its checks are compiled | `just test-shuttle [filter]`, `just test-shuttle-replay-check`, the Shuttle part of `just test-primitives-modeled` | `just coverage-shuttle` |
| Loom | `just cargo-clippy-loom` | `just test-loom [filter]`, `just test-loom-qualification`, the Loom part of `just test-primitives-modeled` | Not collected: a model is evidence of an ordering, not of product coverage |
| Turmoil | The Turmoil targets of `just cargo-clippy` | `just test-turmoil`, `just test-turmoil-replay-check`, the Turmoil part of `just test-primitives-modeled` | `just coverage-turmoil` |
| Deloxide | `just cargo-clippy-deloxide`: the boundary, the deadlock diagnostics with their probes, and the server as a diagnostic node, alone and with its tests and scenario binary | `just test-deloxide`, the Deloxide parts of `just test-primitives-modeled` and `just test-primitives-compile` | Not collected: a diagnostic run is evidence of what its workload exercised, not of product coverage |

`just validate-execution-mode-dependencies` resolves the normal dependency graph of the workspace
and of every package on its own, the way a consumer builds it, with default features and without
them, and rejects Loom, Shuttle, a Shuttle wrapper, Turmoil or Deloxide in any of them, and an
execution mode, the diagnostic one included, or the `test-util` capability enabled on the boundary. It also keeps the portable graphs portable:
the vocabulary crate, and the browser console and the wire crate for the browser's target, contain
no async runtime or network library and never enable the boundary's `native` capability.
`just validate-execution-mode-conflicts` requires the combined-mode diagnostic, including for modes
two dependencies select separately, the boundary's own diagnostic for a mode or the native
capability requested for the browser's target and for the diagnostic mode requested without the
native capability it tracks, and the product-binary diagnostics.

A build that selects a modeled mode is a test artifact. Every binary Nervix ships, the server, the
CLI and the NSPL formatter, declares itself with `nervix_primitives::product_binary!`, which fails to
compile in a build that selects `loom`, `shuttle` or `turmoil` and names the binary and the mode, so
no modeled binary can be built, let alone published. A `deloxide` build is a diagnostic artifact
instead, which runs but is never released: it compiles only a binary that declares a diagnostic form,
`product_binary!("nervix-server", diagnostic)`, and such a binary installs the deadlock detector at
start-up. The server is the one binary that declares it; the CLI and the formatter fail to compile in
that build, naming themselves. `just validate-primitive-boundary` requires the declaration, in either
form, in the crate root of every binary the release image's build compiles, and the release image
builds the ordinary server.

### Diagnostic deadlock detection

A diagnostic node is the server built in the `deloxide` mode, which `just build-diagnostic-server`
builds in its own target directory so it never replaces the ordinary binary. Every thread-blocking
lock and condition variable of `sync::blocking` is then an adapter over one of Deloxide's tracked
locks. An acquisition that has to wait updates one process-wide wait-for graph, and when a waiting
thread closes a cycle in it, every thread of the cycle waits for a lock another one holds and none
can proceed: an active deadlock, which the detector reports while the cycle's threads are still
blocked. A thread waiting for a lock it holds itself, including a writer whose own read guard is
still held, is a cycle of one. Every other family of the build stays the ordinary library,
untracked.

**The tracked surface.** The adapters keep `parking_lot`'s interface where Deloxide keeps its
meaning: `new`, `lock`, `try_lock`, `read`, `write`, `try_read`, `try_write`, `get_mut` and
`into_inner`, `Default` and `From`, and guards that dereference to the value and format as it does.
A lock is never poisoned, and `Debug` never waits: it tries the lock and prints `<locked>` while
another thread holds it, as `parking_lot` does. Deloxide's locks hold sized values, are constructed
at run time, and have no timed, upgradable, mapped, fair or reentrant acquisition, so those
operations do not exist in this build, and code that uses one fails to compile there rather than
run untracked. The condition variable has the surface the Shuttle adapter keeps, `wait` and
`notify_all`. Deloxide's own notification returns no count, so the adapter keeps one: a waiter
records the generation it waits in, under the condition variable's state lock, before it releases
the caller's lock; every notification starts a new generation and returns how many waiters the one
it ended held, and each of those returns from its wait, so the count is exactly the waiters that
notification woke. A woken waiter reacquires the caller's lock as an ordinary tracked acquisition,
so a waiter whose notifier still holds that lock while it waits for one the waiter holds is a
reported cycle like any other. The compiler gate recognizes the adapters' acquisitions as the locks
they wrap, so the same authored sites carry the same acquisition kinds in ordinary and diagnostic
builds.

**Installation.** The detector is process-wide and installs once, through
`nervix_primitives::deadlock::install`, which the diagnostic run of `nervix-deadlock` calls. A
diagnostic process starts its run after it has registered the signals it must not lose and before it
constructs a tracked lock or starts a runtime worker: the server's `main` right after it registers
its termination signals, and the scenario binary's right after it configures the lifecycle of its
test dependencies. A tracked lock constructed in a process that has not installed the detector
panics, naming the configuration failure, because Deloxide drops a cycle it detects while no
callback is installed, so such a lock would deadlock unreported. A second installation is refused,
so nothing resets the detector or configures it twice, and the in-process nodes of a scenario binary
share the one detector their process installed. Deloxide prints a banner on standard output when it
starts. Standard output carries a node's logs and the completion scripts it prints, so the
installation runs with that descriptor pointed at `/dev/null`, before any other thread writes to it,
and fails if the descriptor cannot be redirected or restored.

**Reports.** Deloxide calls one callback, on a dispatcher thread of its own, through a channel it
never bounds, and it catches a panic in that callback, so a callback cannot fail the process. The
boundary's callback copies the report's thread and lock numbers into a bounded lock-free queue of
64 reports, counts a report the full queue refuses, and wakes the boundary's findings thread; it
takes no lock, writes no log and cannot panic. The findings thread correlates each report with the
boundary's registry, which holds, under the detector's numbers, where every tracked lock was
constructed, where every acquisition that waits is waiting, how it asked for the lock, and each
thread's name, and it hands the resulting finding to the installer's sink. An acquisition that
succeeds at once records nothing, because only one that waits can be part of a cycle, and the
registry's maps are never locked across a tracked acquisition, so recording can never join the cycle
it describes. Every record leaves with what it describes, a lock's when the lock is dropped, an
acquisition's once it holds the lock and a thread's name when the thread exits, so the registry holds
no more than the live locks and threads. A finding names threads and locks by their run-local numbers and source sites and
never holds a lock's value; context the registry does not hold stays absent instead of guessed. A
cycle describes at most 64 threads in cycle order and counts the rest, and every text keeps at most
512 bytes, cut at a character boundary, with the length it was cut from. A sink that panics aborts
the process, because a finding it failed to handle must not pass for no finding at all.

**The diagnostic run.** The run installs the detector with a recorder as its sink, then records the
process's evidence without findings in its evidence directory, an existing directory the server
takes as `--deadlock-evidence` (`NERVIX_DEADLOCK_EVIDENCE`) and the scenario binary from
`NERVIX_DEADLOCK_EVIDENCE`; without one, a finding is described on standard error only. The first
finding ends the process. The recorder writes the finding's bounded description to the standard
error descriptor directly, without the lock the standard library takes around it, records the
finding over the process's evidence file, and exits without running exit handlers: with status `3`
once the finding is recorded, or described when the run has no directory, and with status `4` when
recording failed, findings were lost, or recording outlived its ten-second budget, which a deadline
thread enforces. A deadlocked process cannot make progress on the work its blocked threads hold, so
it ends rather than run on half stopped; graceful shutdown does not run, as for a
[forced exit](./shutdown.md#exit-status). Evidence is one file per process, named after its
identifier and the time its run started and replaced atomically, holding rkyv behind a header of its
own: the magic `NVXDLEVD`, a record kind and a format version, checked before the payload is
validated. Its round trip, its refusal of malformed bytes and its bounds are
[registered properties](./property-testing-and-fuzzing.md).

**What it does not cover.** The detector sees only the tracked thread-blocking locks. Tokio's async
locks, channels and `Notify`, DashMap's shards, the locks inside every dependency, atomic protocols,
and waits on the network or across the cluster are untracked, and a deadlock among them is not
reported. A waiter no thread will ever notify blocks on the condition variable without a wait-for
edge, so a lost notification is not a cycle. A cycle is reported once it forms, so a run that never
takes a deadlocking interleaving reports nothing: the absence of a finding is no proof of deadlock
freedom, and Shuttle, Loom, Turmoil, Chaos and Bolero evidence keep their own responsibilities.
Every acquisition that waits, and every read, takes Deloxide's process-wide detector lock, so a
diagnostic node is slower than an ordinary one; it is never a release product.

**Verification.** `just test-deloxide` runs the deadlock probes of `nervix-deadlock`, each workload
in a disposable process its probe bounds with a watchdog. Two threads locking two mutexes in
opposite orders, a thread relocking its own mutex, a thread upgrading its own read lock, a writer
and a reader across two locks, and a notified waiter whose notifier still holds its mutex must each
be reported with every thread, lock kind, construction site and waiting acquisition, recorded, and
end their process with status `3`. Threads taking two mutexes in one order, a condition handed
between threads and readers sharing a lock must end cleanly, with evidence that records the process
and no finding. A tracked lock before the run fails its process, a second run is refused, standard
output never carries the banner, and a deadlock whose evidence cannot be recorded ends its process
with status `4`. A child that never ends is killed by its watchdog and fails its probe. The command
then runs the `@deadlock_diagnostics` and `@restore_installation` scenarios, without retries, in a
scenario binary built for the mode: in-process diagnostic nodes running a workload, real diagnostic
server processes that stop gracefully with evidence that records a running detector and no
findings, each on one and three nodes, and the restore installation workloads. The latter exercise
the blocking applied-state authority guard through staging failure, complete publication, runtime
handle clearing and a delayed coordinator across leadership transfer and a successor's START.
Those restore scenarios also run in the ordinary public suite. An invocation that executed no
check fails the run, and so does a diagnostic workload whose scenarios did not all run and pass.
The retained diagnostic evidence covers the tracked locks acquired by those workloads; async
coordination, atomic capture fencing, database dependency locks and network waits keep their
Shuttle, Loom, Turmoil and Chaos evidence.

### Blocking work outside the executor

A node runs its variable-size encoding, decoding, validation, hashing, compilation, program
execution and synchronous filesystem or database work through the bounded executor, which admits
each job into a CPU or storage class, charges its memory, and gives it the cancellation it checks
between its bounded units. Operator-supplied code the node cannot bound, a Roto UDF call or a JAQ
transformation, takes the extension class, so a program that never returns holds only that class's
workers. Password hashing takes the credentials class and its own budget, so anyone who can reach a
listener spends only those, and saturated data, extension or bulk work never delays a login. The
executor's storage workers are the one built-in owner of the runtime's blocking pool, and no file
owns `task::block_in_place`, which blocks the runtime worker thread that calls it.

Everything else that blocks a thread is a declared owner in
`crates/primitives/blocking-permissions.toml`, which names the file, the blocking items the owner
names there by their paths below `nervix_primitives`, the owner, why it stays outside the executor,
and what bounds its work instead. A permission declares its file for exactly the items it lists, so
a file whose owners block in different ways, such as the CLI's, has a permission for each:

| Owner | Blocks through | Why it stays outside the executor | What bounds it |
| --- | --- | --- | --- |
| The resolver's configuration load in `nervix-dns` | `task::spawn_blocking` | A node reads its resolver configuration before it builds its executor, and the Rust client and the CLI build the same resolver without one | Two small files, read once for each resolver built |
| The Kafka sink's final producer-queue drain and the Kafka source's partition inspection | `task::spawn_blocking` | librdkafka parks its calling thread until the broker answers; the call waits on the network and computes nothing, so it would hold a CPU or storage worker idle | The host's physical flush deadline, and a five-second metadata timeout per request |
| The Rust client's backup download and restore upload, and the CLI's event printer | `task::spawn_blocking` | A client tool is not a node and has no executor | One rename of a written archive, one sequential read of an archive, and one printer thread for the life of the command |
| The CLI's completion prompt | `task::block_in_place` | A client tool is not a node and has no executor; its line editor asks for completions synchronously on the thread that runs the CLI's main task, so the prompt waits there for the completion while the runtime's workers keep the session running | One completion at a time, only while the operator waits at the prompt; each page of suggestions ends by the client's retry deadline, and a repeated continuation ends the completion |
| The benchmark driver and the scenario harness | `task::spawn_blocking` | A harness is not a node; it waits on brokers, databases, probe threads and the C binding from its own process | Each call's own timeout, or the probe or thread it joins |

The boundary check rejects either item in a file no permission lists it for, and a listed item its
file no longer names.

### Thread creation outside the executor

Starting a thread can bypass the executor's admission, memory charges and cancellation just as
blocking work can. Every caller of `thread::spawn`, `thread::Builder`, `thread::scope`,
`thread::spawn_detached` or `unmodeled::thread::Builder` therefore declares its exact file and
items in the same `crates/primitives/blocking-permissions.toml`. The declaration states its owner,
why the work stays outside the executor, and what bounds the work, thread count and lifetime.
No caller outside the primitive crate has a built-in permission. Naming the builder includes its
ordinary and scoped spawn methods; a glob names every confined item it brings into scope.
Reimported module aliases retain the same confinement regardless of import order. A glob over
the boundary root is rejected; name its modules and items explicitly so confined and unmodeled
paths remain visible.

| Owner | Thread items | Work, count and lifetime bound |
| --- | --- | --- |
| Process termination supervision | `thread::spawn_detached` | Two supervisors per process, parked between signals or deadline changes, plus at most one forced-exit watchdog after the single exit claim; supervisors end with the process, and the watchdog waits only the forced-exit report budget |
| WASM epoch driver | `unmodeled::thread::Builder` | One thread per WASM runtime; one sleep and one engine epoch increment per configured tick; it ends on its next wake after the stop flag is set or the weak stop reference can no longer upgrade |
| Diagnostic finding recorder | `thread::spawn_detached` | At most one watchdog for the process's one finding; it exits the process after the ten-second recording budget |
| Shared Loom participant launcher | `thread::Builder` | Loom's five slots, including coordinator and body, the branch limit and runner watchdog; participants end within one model execution |
| Benchmark load driver | `thread::Builder`, `thread::scope` | One joined summary poller per drain owner, and one scoped watermark query per configured partition, each with the request timeout within the driver deadline |
| Turmoil scenario runner | `thread::Builder` | One scheduler per disposable attempt; physical run and cleanup deadlines, a normal join, and a failed process when either deadline is missed |
| NSPL completion walk | `thread::Builder`, `thread::scope` | At most the configured jobs per finite frontier; every worker is joined before the next level |
| Public C binding probes and external Kafka member | `thread::spawn`, `thread::scope`, `thread::Builder` as listed for each file | Fixed probe participants joined within their bounded call and cancellation waits; one Kafka poller per member, stopped and joined when the member drops |

Unit tests and Loom and Shuttle checks declare every file and its exact thread items too. Their
permissions describe finite participants for each case, joins or model termination, and the
registered exploration bounds where applicable. Diagnostic cycle probes instead use disposable
processes and a parent watchdog, since their tested cycle intentionally cannot join. These
declarations do not exempt a directory or evaluate away `cfg(test)`, a model-only module, an
inactive branch or a macro body. A new item needs its declaration, and a listed item its file no
longer names fails as stale. A file whose product owner and tests use different items names both.

The real epoch builder also keeps its independent unmodeled permission, with its reason and
verification limit. That permission does not supply the work and lifetime bound and cannot alone
authorize thread creation. The ordinary re-exports and every modeled thread operation retain their
existing execution semantics; this confinement changes source validation only.

## Deterministic Concurrency Verification

The ordering contracts above are checked against the production owners under Shuttle. A check
runs several tasks or threads through one scheduler, which chooses an interleaving at visible
synchronization points. It can expose a lost notification, an early drain, a double completion, or
a stale generation without depending on which OS thread happened to run first. A deadlock is a
failed check. The model is the surrounding schedule and test data; the protocol under test is the
same type used by the data plane, not a copied implementation of it.

Shuttle does not model elapsed time. Under Shuttle the boundary's `time` family is Shuttle's: a
sleep yields once, and a timeout does not measure its deadline and fires only when a check triggers
it by task label through `nervix_primitives::time::trigger_timeouts`. `Instant::now` still reads the
operating system's monotonic clock, while paused-time controls do not advance a simulated clock.
Checks of deadline ordering therefore use an already-passed or far-future instant and explicitly
choose whether the timeout wins. Ordinary tests on a paused Tokio clock retain responsibility for
actual timer behavior. No concurrency check uses a wall-clock bound, sleep poll, or `recv_timeout`
to establish progress.

Only scheduler-visible operations create interleavings. In a Shuttle build the primitive
boundary selects every family from Shuttle or from its own adapters, as the table above shows, and
every package that owns the feature forwards it to the boundary, so the synchronization inside
vocabulary types such as `AtomicTimestamp`, inside dependencies such as the execution crate's
cancellation, and inside every connector is Shuttle's too, and so are the timers of every crate in
the graph. The feature changes the test execution environment, not the public protocol. Edge I/O,
sockets, filesystem access, and signals remain real and are outside a Shuttle schedule. A Shuttle
execution has no Tokio reactor, so a socket created inside a check panics and fails it rather than
reaching the network unobserved. A check's own records use unmodeled atomics on purpose, under the
permissions above, so that recording an operation adds no scheduling point to it.

Some primitives are opaque to Shuttle: `arc-swap`, `concurrent-queue` and the `futures` crate's
atomic waker have no modeled implementation. The boundary runs each `ArcSwap` and `ArcSwapOption`
load and store, each `ArcSwap` compare-and-swap and read-copy-update, each operation of a lock-free
queue, each registration, wake and take of an atomic waker, and each read and write of a `OnceLock`
between two scheduling points, and calls the primitive directly in ordinary execution. Shared
ownership through `triomphe` or the standard library takes no scheduling point: a reference count is
real in every mode. The boundary's thread module supplies the scheduler-visible synchronous yield
that admission spin waits use. A check may claim an ordering around an opaque primitive only when
its relevant calls have visible scheduling points. Shuttle cannot interrupt an arbitrary instruction
inside it, and a check never claims the primitive's own memory safety.

A lost wakeup lives between a waiter's read of some state and its registration for the
notification a publisher sends after changing that state. Shuttle's own `Notify` keeps its waiter
registration behind a real lock and its `watch` channel reads its version without a scheduling
point, so no schedule could place a publication in that window, and a read followed by a
registration looked safe in a check even though a release between the two is lost in production.
The boundary's `Notify` and `watch` channel keep Tokio's semantics and take a scheduling point
immediately before each registration, each notification and each read of the version that
registers or decides, and the atomic waker's adapter takes one before and after each registration
and wake. Tokio's `Notify` registers a future for `notify_waiters` when the future is
created, and for `notify_one` only when it is first polled or enabled; `notify_one` without a
registered waiter stores one permit; a waiter a single notification chose passes it on when it is
dropped before it observed it. A `watch` receiver marks the current version as seen when it
subscribes. The conformance checks of `crates/primitives/src/tests` run the same scripts against
Tokio and against the adapters, and the Shuttle checks there show that a publication between a read
and the registration is reached: each broken order deadlocks in some explored schedule and the
correct one never does. The ownership-handoff checks separately exercise release-before-wake and
register-before-read on the production freeze owner. Reversing the latter order deadlocks under
both random and PCT schedules, and the failing schedule replays.

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
probabilistic concurrency testing (PCT) for larger ones.

Every check runs through the one Shuttle runner, `nervix_model_harness::shuttle`: 100 random and
100 PCT schedules of depth three, 1,000 of each with a depth-first search bounded at 1,000
schedules, or one scheduler with the check's own bounds. Every schedule may take 10,000 steps; one
that needs more fails its check, because a step cap is an exploration bound, not a product timeout.
A random or PCT search that ran fewer schedules than it declares fails too, and a search that
finishes prints a record naming what it explored. `SHUTTLE_REPORT_STEPS=1` reports the highest
observed step count when tuning a check.

The primitive boundary's own conformance and race checks run in `just test-primitives`, which
builds the boundary once per mode; its Shuttle checks explore their small models exhaustively with
depth-first search, and each deliberately broken order must deadlock in some schedule.

`crates/model-harness/shuttle-inventory.toml` registers every check of `nervix-execution`,
`nervix-interconnect`, `nervix-client-core` and `nervix-server` by its test's full name.
`just test-shuttle` lists the library tests whose full names contain `shuttle_` in each registered
package, built with its `shuttle` feature, and a run over the whole inventory fails when a
registered check is missing or ignored, or when a discovered check is unregistered. Each selected
check runs twice, each time in its own process: under the exploration it declares, then under
Shuttle's uncontrolled-nondeterminism detector, which runs 100 random schedules twice each and
fails when the second run diverges. A run counts only when its test passed and the runner printed
its record, and the command reports how many checks it discovered, selected, executed and saw
complete. `just test-shuttle <filter>` selects the checks whose full name contains the filter in
every package; a package with no match is fine, and a filter that selects nothing at all fails. The
recipe uses the repository's kache-backed build and prepares the server's test dependencies; `just
test` continues to run the ordinary suite.
`just cargo-clippy-shuttle`, whose package checks also run in `just lint`, lints every Shuttle
build with warnings denied, including these four packages in test mode, where their checks are
compiled, so a check that compiles with a warning fails validation.

A failed check leaves `target/shuttle-failures/<package>/<fully-qualified-test-name>/`: the schedule
Shuttle persisted for the failing execution, the run's output, and metadata naming the check, the
run, the revision, the toolchain, Shuttle's version and the replay command. `just
test-shuttle-replay <schedule>` replays exactly that check in a fresh process with the schedule,
which ends where its execution failed and so replays that failure. `just test-shuttle-replay-check`
proves the path end to end: it fails one check deliberately after its invariant held, requires one
persisted schedule, and requires it to reproduce the failure in a fresh process. CI's dedicated
`shuttle` job runs both and uploads `target/shuttle-failures` as the `shuttle-failures` artifact
when a check fails. See the command recipes in [Developing Nervix](./developing-nervix.md).

### Protocols and their checks

The checks below hold the scheduler-visible parts of each protocol to their invariants. Test
names are given relative to their owning module; the `shuttle` feature selects the modeled build.
A family of names means each member runs independently through the recipe.

| Protocol | Invariant and check |
| --- | --- |
| Waiter registration and cancellation (`crates/primitives/src/tests/shuttle_races.rs`) | `shuttle_reaches_a_notify_waiters_published_between_a_read_and_the_registration` and `shuttle_reaches_a_send_between_a_read_and_the_subscription` require a waiter that reads before it registers to deadlock in some schedule, which shows the scheduler reaches a publication between the read and the registration; `shuttle_registering_before_reading_never_misses_a_notify_waiters` and `shuttle_subscribing_before_reading_never_misses_a_send` require the correct order never to lose one. `shuttle_reaches_a_wake_between_a_read_and_the_waker_registration` and `shuttle_registering_the_waker_before_reading_never_misses_a_wake` hold an atomic waker to the same pair. `shuttle_a_notify_one_between_a_read_and_the_registration_is_kept_as_the_permit` shows a single notification's permit makes the other order correct for `notify_one`, and `shuttle_a_waiter_cancelled_as_it_is_notified_passes_the_notification_on` requires the surviving waiter to complete however a cancellation and a notification interleave. |
| Execution budgets and storage jobs (`crates/execution/src/tests.rs`) | `shuttle_saturated_class_keeps_live_reservations_within_each_class_capacity` keeps live reservations within each class capacity; `shuttle_occupied_bulk_execution_leaves_control_execution_untouched` keeps bulk saturation from charging control; `shuttle_queued_job_drop_releases_its_reservation_and_exact_queue_slot` and `shuttle_running_job_observes_cancellation_and_keeps_its_charge_until_exit` balance queue slots and permits across drop and cancellation; `shuttle_full_wait_queue_is_exact_typed_backpressure` checks a full wait queue's typed rejection; `shuttle_a_job_waiting_for_a_place_takes_the_first_freed_place` requires a job that waits for a place to take the first one freed and run before a job refused when full that asks after it; `shuttle_a_dropped_job_waiting_for_a_place_gives_up_its_place_and_charge` returns a dropped waiter's charge and hands its place to the job waiting after it; `shuttle_consensus_storage_preserves_admission_order_and_returns_every_permit` checks storage admission order and permit return. |
| Force-flush obligations (`src/runtime/force_flush.rs`) | `shuttle_two_participant_generation_waits_for_every_obligation` prevents completion before all participants live at publication complete and redelivers a dropped, unhandled completion; `shuttle_stale_completions_never_clear_a_newer_generation` prevents an old completion from clearing new work; `shuttle_published_generation_wakes_a_waiting_participant` catches a lost publication wakeup; `shuttle_participant_lifecycle_balances_obligations_through_close` balances `pending()` across subscribe, request, participant drop, and close. |
| Ingestor intake (`src/runtime/ingestors/source_shuttle_tests.rs`; `src/runtime/ingestor_quiesce.rs`) | `shuttle_broker_source_observes_engagement_during_dispatch`, `shuttle_paced_source_observes_engagement_during_dispatch`, and `shuttle_request_source_observes_engagement_during_dispatch` exercise host-loop engagement for memory pressure, entity gate, handoff, and shutdown: after engagement returns, no further payload dispatches, and a change during dispatch is observed. `shuttle_an_open_control_answers_intake_without_waiting_on_retained_payloads` keeps the open decision independent of a retained-payload lock. `shuttle_a_new_quiesce_ends_a_delivery_waiting_for_extension_room` and `shuttle_a_shutdown_ends_a_delivery_waiting_for_extension_room` race the engagement or the stop against a retained payload's delivery waiting for a place the extension class never frees: the waiter registers before it reads, so the delivery always ends, and its payload is back at the front of the buffer counted as it was. `shuttle_a_new_quiesce_decides_again_on_a_live_payload_waiting_for_extension_room` and `shuttle_a_stop_ends_a_live_payload_waiting_for_extension_room` race a `BUFFER` engagement or the stop against a live payload waiting for such a place: the wait always ends, a new decision becomes the one the payload is decided under next and its buffer retains it, counted with its bytes, and a stop leaves nothing retained. |
| Relay dispatch gate and fan-out (`src/runtime/relay_channel_shuttle_tests.rs`) | `shuttle_dispatch_permits_never_overlap_a_quiescent_lease_and_release_frees_every_waiter`, `shuttle_overlapping_gate_leases_all_release_before_dispatch_resumes`, `shuttle_expired_gate_fence_frees_every_waiter_without_reporting_quiescence`, `shuttle_canceled_dispatch_returns_its_permit_to_the_gate_fence`, and `shuttle_dispatches_parked_behind_a_lease_wake_only_on_its_release` hold the fence, lease, expiry, cancellation, and waiter contract; in-flight dispatches drain before quiescence. `shuttle_capacity_shrink_keeps_buffered_batches_and_wakes_publishers_after_the_drain`, `shuttle_capacity_growth_admits_waiting_publishers_without_a_take`, `shuttle_publishers_wait_for_the_slowest_consumer_and_skip_consumers_that_leave`, and `shuttle_losing_every_consumer_delivers_or_returns_the_waiting_batch` keep queued batches across capacity changes, release waiting publishers, and return or deliver each batch when receivers leave. |
| Relay owner fan-out (`src/runtime/relay_boundary_shuttle_tests.rs`) | `shuttle_owner_fanout_fails_its_ack_while_an_attached_consumer_moves` keeps a live sibling from completing a source ACK while another attached consumer leaves under a schedule fence; after release, a retry reaches the live consumer. |
| Assignment authority and state updates (`src/runtime/state_store_shuttle_tests.rs`, `materialized_state.rs`, `kafka_offset_state.rs`) | `shuttle_a_rebind_yields_until_the_operation_admitted_under_its_replaced_binding_finishes` and `shuttle_no_operation_admitted_under_a_superseded_binding_outlives_its_superseding_rebind` fence admitted work even when generations reuse an even or odd counter. `shuttle_snapshot_installation_never_overlaps_origination_or_a_capture` keeps exclusive installation apart from originators and captures. `shuttle_an_originator_update_proceeds_while_the_assignment_barrier_is_held` and `shuttle_a_committed_offset_proceeds_while_the_assignment_barrier_is_held` keep ordinary admitted updates independent of a capture's barrier. |
| ACK tree (`src/runtime_ack.rs`) | The `shuttle_tests` checks `concurrent_attachment_and_final_ack_leave_exact_tracking`, `concurrent_wait_and_active_ack_exempt_the_remaining_root`, `concurrent_wait_release_and_completion_leave_no_tracking`, `attachment_losing_its_reservation_to_completion_resolves_no_share`, `attachment_losing_its_reservation_to_completion_parks_no_share`, `wait_release_racing_the_last_active_ack_holds_domain_and_ingestor_handoff_once`, `attachment_racing_the_last_active_share_into_wait_publishes_one_active_share`, and `parked_remote_progress_survives_a_racing_heartbeat_and_resumes_once` keep pending, active, and handoff counts exact across attachment, `REQUIRED WAIT`, and remote progress. `concurrent_success_and_failure_choose_one_terminal_transition`, `fan_out_across_ingestors_with_a_failing_root_resolves_each_root_once_with_exact_counts`, and `parked_and_fanned_out_roots_hold_exact_counts_at_every_quiescent_point` require one terminal result per root, one observed completion, and zero outstanding counts after resolution. |
| Forwarded acknowledgement silence (`src/runtime/remote_dispatch_shuttle_tests.rs`) | `shuttle_a_terminal_outcome_racing_the_final_sweep_resolves_the_share_once` races the receiver's terminal outcome against the sweep that would fail a forwarded share: exactly one of them removes it, and the root delivers that one's outcome. `shuttle_a_report_racing_the_final_sweep_keeps_the_share_it_reached` races a report against the same sweep: a report that reached the share keeps it pending with its root unresolved, and only a report that found it removed lets the sweep fail it. |
| Entity gate and node quiesce (`src/runtime/entity_gate_shuttle_tests.rs`) | `shuttle_an_entity_gate_hold_fences_every_relay_and_admits_no_work_until_it_is_released` requires admitted work to drain before quiescence and prevents new admission while closed. `shuttle_a_work_item_parked_for_materialized_state_is_never_missing_from_a_drain` and `shuttle_every_node_quiesce_gauge_withdraws_exactly_what_it_contributed` keep parked, buffered, and branch work counted without underflow and back to zero. `shuttle_every_engagement_waiter_wakes_and_exactly_one_release_takes_the_hold`, `shuttle_a_hold_dropped_before_its_fence_completes_reopens_every_relay_it_engaged`, and `shuttle_a_failed_engagement_wakes_every_waiter_with_its_failure` cover release, drop, and failure. `shuttle_releasing_an_ownership_handoff_wakes_every_waiter_frozen_by_it` holds release-before-wake; `shuttle_an_ownership_handoff_freeze_observation_registers_before_its_read` holds register-before-read against that release. |
| Interconnect slots and membership (`crates/interconnect/src/connection/stream_slots/shuttle_checks.rs`, `request/shuttle_checks.rs`) | `management_drain_stops_leasing_and_waits_for_every_leased_slot`, `replication_drain_stops_leasing_and_waits_for_every_leased_slot`, `bulk_drain_stops_leasing_and_waits_for_every_leased_slot`, and `relay_drain_stops_leasing_and_waits_for_every_leased_slot` keep partition and subquota reservations isolated, forbid leases after drain starts, and wait for every lease to return. `racing_registrations_lose_no_handler_and_publish_each_name_once` prevents a lost handler registration and duplicate name. `a_membership_change_between_a_callers_check_and_its_wait_is_never_lost` prevents a missed discovery wakeup. |
| Shutdown and signals (`src/application/shutdown.rs`, `termination_signals.rs`) | `shuttle_racing_stop_requests_accept_exactly_one_and_keep_its_deadline` retains the first stop request and its deadline. `shuttle_phases_only_advance_and_every_completion_waiter_observes_the_one_outcome` keeps phase order and one completion. `shuttle_an_expired_deadline_and_a_repeated_signal_let_exactly_one_forced_exit_end_the_process` and `shuttle_a_repeated_signal_before_the_deadline_ends_the_process_with_the_status_of_that_signal` give one forced-exit claimant and the exit status of the cause that won. |
| Emitter batch payloads (`src/runtime/emitter_record_writes_shuttle_tests.rs`) | `shuttle_a_retried_payload_acknowledges_each_fanned_in_member_once_after_every_emitter` and `shuttle_a_sibling_failure_resolves_each_fanned_in_member_once_despite_a_retry` fan two source messages out to a batching emitter and a sibling: each source acknowledgement completes once, successfully only after both emitters confirmed it, and the retry writes the retained payload's first bytes. `shuttle_a_cancelled_attempt_leaves_each_member_to_resolve_once` cuts an attempt short at any point and requires the next one to write only unanswered payloads and deliver each rejected member's message error once. `shuttle_a_drain_never_finds_the_emitter_empty_while_a_member_is_retained` races a drain's reads against a stalled write and the force flush that repeats it. |
| Client ingestors (`src/runtime/client_ingestor_shuttle_tests.rs`) | `shuttle_racing_reservations_never_exceed_the_node_budget_and_return_every_byte` races opens that each need more than half the node's producer budget: at most one holds it at a time and every reservation returns its bytes. `shuttle_a_batch_racing_a_quiesce_is_either_counted_by_its_drain_or_refused_undispatched` races the admission fence against an engagement and its drain: no batch is dispatched after the drain concluded. `shuttle_a_closing_producer_answers_every_admitted_batch_once_before_its_release` and `shuttle_an_ending_endpoint_answers_every_batch_once_and_ends_its_producer_last` race a close or an endpoint end against the worker's admission reports and the batches' acknowledgements: every batch is answered exactly once, a close answers each with its real outcome before the release, and an end reports no admitted batch as not admitted and comes last. `shuttle_a_detach_racing_a_clearance_admits_only_a_cleared_batch_and_returns_its_slot` races a forwarded producer's detach against the clearance of its batch while a local producer waits for the window's one slot: the forwarded batch reaches the worker only after its clearance was recorded, and the local batch is admitted whichever comes first, so no slot leaks. `shuttle_an_end_racing_clearances_reports_a_batch_not_admitted_exactly_when_the_worker_never_took_it` races an endpoint end against the clearance of two batches while the worker holds the first without reporting it: each batch is answered once, not admitted exactly when the worker never took it, whether it was still being cleared or cleared and waiting for the worker, and of unknown outcome when it did. |
| Rust client submission slots (`crates/client-core/src/producer/slots_shuttle_tests.rs`) | `shuttle_a_wait_racing_its_resolution_takes_the_outcome_once_and_returns_the_credit`, `shuttle_a_cancelled_wait_loses_neither_the_outcome_nor_the_credit`, and `shuttle_a_release_racing_its_resolution_returns_the_credit_exactly_once` race a submission's resolution against the application's wait, an aborted wait followed by a new one, and a release: the outcome is taken at most once, a cancelled wait leaves it retrievable, and the credit comes back exactly once. |
| Rust client attachment recovery (`crates/client-core/src/producer.rs`, `consumer.rs`) | `shuttle_close_fences_a_producer_restore_started_on_the_same_exchange` and `shuttle_close_fences_a_consumer_restore_started_on_the_same_exchange` race close against beginning restoration. `shuttle_close_fences_a_producer_restore_interrupted_by_another_loss` and `shuttle_close_fences_a_consumer_restore_interrupted_by_another_loss` race close against another loss while restoring. Each check uses the production lifecycle owner and requires the final phase to remain closed, with subsequent restoration refused. |
| Relay branch presence (`src/runtime/relay_branch_presence_shuttle_tests.rs`) | `shuttle_an_observer_sees_every_owner_step_whole_and_never_an_older_one` races an owner at capacity one through admission, eviction, recreation and release against an observer that registers and reads throughout: every read is a membership the owner published whole and never older than the step the owner had finished. `shuttle_capacity_and_expiry_publish_whole_memberships` keeps every read within the owner's capacity and drops an expired branch from every read after the expiry. `shuttle_a_replaced_owner_never_publishes_over_its_successor` races a predecessor's admissions, expiry and release against its successor's claim: once the claim is visible no read holds a branch only the predecessor admitted, and the successor's branch survives the predecessor's release. |
| Checkpoint replication (`src/runtime/kafka_offset_state.rs`, `src/runtime/state_replication/checkpoint_announcement_shuttle_tests.rs`) | `shuttle_a_replica_acknowledgement_racing_the_quorum_wait_is_never_missed` races a Kafka offset commit's replica quorum wait against its replica's acknowledgement: the wait registers before it reads, so it completes without its deadline, which a Shuttle timeout only reaches when a check triggers it. `shuttle_an_offer_racing_the_end_of_an_announcement_is_always_announced` races a second offer against the announcer of the first finding its replica caught up: the second revision is always announced and acknowledged. `shuttle_a_retired_replication_ends_its_announcer` ends an announcer whose replicated state goes away while its replica never acknowledges. `shuttle_an_announcement_racing_the_replica_wait_is_never_missed` races an owner's announcement against the replica task's synchronization and wait: an announcement that lands before the wait is kept as its permit. |
| Replica catch-up announcements (`src/runtime/branch_lifecycle_state_shuttle_tests.rs`) | `shuttle_an_announced_branch_racing_the_replica_round_is_never_missed` races an owner's announcement of a branch checkpoint against the replica task taking the pending announcements and waiting for the next: the task takes it whether it lands before the take, between the take and the wait, or during the wait. `shuttle_announcements_of_one_branch_keep_the_newest_pending` delivers two announcements of one branch in either order while the task takes them: an older one never replaces a newer one still pending. |
| Backup capture and restore publication (`src/runtime/backup_capture_fence.rs`, `crates/consensus/src/restore.rs`) | `shuttle_backup_cut_includes_pre_cut_branch_publication` requires every registered publication observed before a cut to be present in its view. `shuttle_restore_publication_and_handle_clear_cannot_cross_a_successors_start` drives the production applied-state authority guard against a new restore generation and START, so stale publication changes neither checkpoints nor runtime handles. Both register in the shared Shuttle inventory and run through its exploration, nondeterminism and replay contract. |
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
with Loom's primitives. `just cargo-clippy-loom`, whose package checks also run in `just lint`,
lints every Loom build:
the models and their harness, the primitive boundary, and the server and consensus libraries both
as they ship and in test mode, where models of their owners are compiled.

Each model names its invariant with an `InvariantId` and runs through
`nervix_model_harness::loom::explore`, which explores it to exhaustion: no preemption bound, no
permutation or time budget, a branch limit of 1,000 thread switches per execution that fails the
model rather than ending the search, and Loom's full thread count. A Loom setting in the environment
that would change that search is refused. A completed search prints a record naming its invariant,
its execution count and its bounds, and `just test-loom` accepts nothing else as a completed model.

Discovery, model execution, qualification and checkpoint replay use the workspace's `loom` build
profile. It inherits `dev`, retains debug assertions, overflow checks, debug information and the
server's allocation instrumentation, and compiles with optimization level one to reduce the
allocation frames on Loom's fixed coordinator stack. Qualification cleans only this profile's
package artifacts before and after each weakening, so a weakened artifact cannot become an
ordinary build input or satisfy another qualification.

The coordinator starts and joins the model body without touching protocol state. The body and
participants started through `nervix_model_harness::loom::spawn` each request a 1 MiB coroutine
stack for instrumented allocators and debug frames. The coordinator retains Loom's fixed default
stack; its initial allocation and spawn path must fit before the larger body stack can run.
Loom forwards that request to its coroutine implementation, whose allocation units need not be
bytes. Its five thread slots include the coordinator, body and at most three other participants.
The harness stack model executes a 64 KiB frame on both the body and a participant; reducing the
stack request must reproduce the overflow and replay it from its checkpoint. Scheduling and
memory-ordering checks still explore to exhaustion.

`crates/model-harness/loom-inventory.toml` registers every model by invariant. `just test-loom`
lists the `loom_*` library tests of every registered package built with its `loom` feature, and a
run over the whole inventory fails when a registered invariant's test is missing, ignored or did not
complete, or when a discovered model is unregistered. Each model runs in its own process, and the
command reports how many models it discovered, selected, executed and saw complete; a filter that
selects none fails. A failed model leaves `target/loom-failures/<package>/<invariant>/`: the Loom
checkpoint of the failed execution, the run's output, and metadata naming the invariant, revision,
toolchain, Loom version and exploration bounds. `just test-loom-replay` resumes Loom from that
checkpoint with location tracking and tracing, so the failed execution runs first. The artifacts
hold model output only, never payloads or secrets. CI runs the models on every change and uploads
the failure directory. Invariant IDs use ASCII letters, digits, dots and hyphens, so these paths
are valid artifact names; metadata retains the exact Rust test name used for replay.

Every model also registers a weakening that must make it fail. `just test-loom-qualification`
applies each to a copy of the working tree, requires the model to fail with the registered message,
and requires the checkpoint of that failure to replay it. This is what shows a model depends on the
ordering it claims, rather than passing because something else synchronized its threads. A
weakening whose original text no longer appears exactly once fails as well, so changing an owner's
ordering means revisiting its qualification.

Loom's own limits bound every claim. It does not model every relaxed behavior the C11 model
permits, and an operation inside a third-party dependency, such as a `triomphe` reference count or
an `arc-swap` publication, is invisible to it and excluded from the claim rather than given a
fictional model. A standalone counter carries no cross-location claim, whatever its ordering. Relay
branch presence is such a case: its owner lifetimes and publications are `arc-swap` compare-and-swap
and read-copy-update operations with no Nervix-owned atomic beside them, so it has no Loom model;
its Shuttle checks order its publications against observers and successors. Checkpoint
replication is another: its announcement and progress change under one placement's lock, and its
wake-ups are the boundary's `Notify`, with no Nervix-owned atomic beside them, so it has no Loom
model either; its Shuttle checks order an announcer's end against offers, and a wait's registration
against reports and announcements.

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
