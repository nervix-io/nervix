# Data-Plane Concurrency

*Plexus, the Nervix graph definition language.*

Nervix keeps the paths that carry records, batches, remote relay frames, and acknowledgements free
from shared coordination that is unrelated to the record being processed. This is the
contentionless data-plane rule. It applies after a runtime task has started and its graph, routing,
state, metric, and connector handles have been resolved.

[Execution Plans](./execution-plans.md) describes the revision that installs and publishes those
handles before record and batch work begins.

[SIMD Kernels](./simd-kernels.md) defines the pure buffer boundary that folds batch latency and
computes admission, arithmetic, and byte masks. Its cached CPU level is process-wide detection
under an unmodeled permission; it carries no protocol state. Batch-local reductions preserve this
chapter's ownership and bounded synchronization rules, including one accumulator lock and one
wall-clock read per delivery-latency series per batch.

The hot paths are:

- accepting one record from an ingestor
- admitting and fanning out one relay batch
- executing one processor batch for a concrete branch
- receiving and admitting one remote relay frame
- creating, sharing, and resolving one acknowledgement

One Core NATS emitter owns its pending-write state, including a nonzero record count and the
connection expected to flush it. The driver exposes its successful-connection generation as a
value, which the emitter captures before hand-over and checks after the driver's flush completes.
There is no shared connector map, lock or callback publication in this check. The driver's
private socket and atomics remain outside Nervix's modeled primitives: scripted transport tests
and public reconnect/STOP scenarios qualify this boundary, while Deloxide qualifies the tracked
lifecycle paths around it and cannot observe those driver internals.

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

MQTT input retirement is a cold lifecycle path. It snapshots locally owned typed entrypoint
plans from the installed execution, releases the registry guard, stops the source tasks, and
then performs network cleanup and replacement preparation. Each connection has one mutable
owner; simultaneous attempts are bounded by the declared source instance count. Matching session
identities keep their broker state across instance-count changes. The public drop, topic-change
and unavailable-endpoint scenarios are registered for active, order and stressed lifecycle
diagnostics. MQTT sockets, asynchronous waits and concurrent-map internals remain outside the
tracked-lock detector; its lanes qualify only the tracked paths the surrounding lifecycle reaches.

Pulsar input retirement uses the same cold lifecycle boundary: it snapshots typed plans, releases
the registry guard and stops the exact source tasks before comparing resolved subscription
identities and removing an unused owned subscription. Matching replacements retain their backlog.
The public drop and replacement outlines cover running and passive installation in active,
order and stressed diagnostics. No new synchronization protocol or hot-path map access is added;
broker I/O, asynchronous waits and concurrent-map internals remain outside the detector's scope.

Transaction pause release is a cold control-plane lifecycle path. Application completion uses the
step's tracked impact recorder to recover an engaged domain pause before retaining its terminal
outcome. The public pause-release regression controls two leadership transfers and is registered
in the active, order and stressed lifecycle selections. The detector observes the surrounding
tracked acquisitions; asynchronous proposal waits and the test barriers remain outside its scope.
No new synchronization protocol or hot-path map access is introduced. A coordinator that adopts its
transaction's pause records it on the same tracked impact recorder before anything can end the
step, then drains and releases it on the existing paths. The public outline whose leader refuses a
model publication and the pause release after it, as a leader whose lease lapsed, joins the same
three selections on one and three nodes. Its saturated-log counterpart, which keeps an ordinary
command applying while its transaction commits, adds no tracked acquisition and stays outside
them; its owner test and public scenario cover it.

## Remote ACK And Admission Owners

`just bench-remote-owners` measures registration, admission, Alive, ordered parking/resume and
terminal resolution with actual ACK roots at one and 64 rows per delivery, reports delivery tail
latency and relay-memory reclamation, and measures an empty sweep of the positions its deliveries
claimed beside the fixed position count. Its
authenticated frame probe sends three current Arrow rows, admits them, returns the exact terminal
reply and checks retired attempt counts. A separate 5,000-frame peer-owner probe completes while
another peer's protocol guard is continuously held and checks returned permits and memory. These
native probes also run through the existing benchmark smoke producer; local debug timings describe
their measured build and host load, rather than a production throughput guarantee.

Remote record correlation uses an immutable array of delivery and admission routing positions.
Each delivery generation owns its mutable rows under one short synchronous guard; a row's outcome
is independent of the other rows. Each side claims unused positions on demand and returns retired
positions to its own bounded free queue, preserving admission room when delivery positions fill.
Shutdown seals first claims before closing the queues and visits only positions that were claimed.
The silence sweep, which runs once a second on every node, has the same bound: it reads each side's
first-claim count and takes a guard only for the positions below it, so a node that forwards
nothing takes none and a sweep's cost follows the positions its deliveries have used, not the
16,384 positions a node is built with. The counts bound the scan and order nothing: a position's
guard publishes its correlation, and nothing relies on a sweep having observed a claim. A share
whose first claim races a sweep is counted from that sweep or from the first one after its
registration, so it still fails between the silence timeout and one sweep interval later. Runtime
shutdown cancels the sweep task and waits for it. The task returns to its executor after every
sweep, because a sweep that overran its interval finds the next tick already due and that tick does
not suspend the task, so it observes the cancellation between sweeps and a sweep's guard count also
bounds how long a stopping node waits for it. A diagnostic
build pays for every one of those guards: the acquisition-order selection enters Deloxide's
process-wide detector on each tracked acquisition and release, and the nodes of every scenario in
one process share that detector, which is why a recurring pass takes guards in proportion to its
live work and never to a fixed capacity.
Record allocation and receiver batch watcher tasks reserve relay memory before creating their
retained rows. A batch task multiplexes row progress and keepalive polls every 100 milliseconds;
each row and the single task have fixed charges. The cadence remains shorter than a one-second
source ACK timeout while task cardinality stays bounded by admitted batches. Before admission, the
delivering side reports the batch's records alive from inside the delivery itself: the task that
runs the delivery races it against a 100-millisecond timer and, each time the timer wins, publishes
progress on each record's own acknowledgement root. That adds no task, no shared state and no
wait-and-notify protocol, and a delivery admitted within one interval publishes nothing. A receiver whose relay
budget has no room for a batch's watches holds the unadmitted batch on its own channel lane and
reads the budget again every 10 milliseconds; it registers for no release notification and never
queues in the budget's semaphore, whose released permits would otherwise go to the held charge
ahead of the relay work that frees it. The batch task returns each watch's share as that watch
ends, through its exclusively owned reservation, so the hold involves no shared state beyond the
budget's own atomic permit count. A sender storing a delivery's record acknowledgements waits for
room through the same hold, inside the delivery's channel slot, and a sender whose grant the
receiver refused for capacity waits on its own timer before it asks again. Neither registers for a
notification, so neither adds a wait-and-notify protocol: each ends on its own timer, its bound, or
the cancellation of its channel.
The registered wire number selects the position, exact generation and row without a concurrent
map lookup. The process identity is checked before routing. Generation exhaustion seals the
position, and cancellation, timeout, terminal resolution and shutdown compete to take each row
once. The ACK tree resolves after the routing guard is released.

Relay services retain the domain drain tracker resolved during construction. Received rows create
ACK roots through that handle; they never register or discover the tracker per row. Watchers read
the membership writer's immutable incarnation publication, so registrar retirement ends their
charged task lifetime without a gossip lock.

One authenticated peer transport owns its ordinary grant, attempt, channel, watermark and outbound
correlation collections. Connections and operation leases retain that owner. Its guard serializes
identity and ordering decisions for this peer; there is no node-wide actor or map guard in the
recurring protocol. The peer guard precedes the exact record guard, neither crosses an await,
and permit release remains possible while runtime borrows an admitted record. Ordered attempt and
grant retirement keeps cancellation effects deterministic. Cold binding and membership changes
publish routing through CAS. [Cluster Interconnect](./interconnect.md#bounded-correlation-and-peer-owners)
owns the capacities, grace periods and reconciliation deadlines.

Production-owner Shuttle checks cover delayed events against a reused position, registration
against shutdown, admission waiter drop against a reply, final silence sweeps against terminal and
Alive reports, a first claim against a sweep, a sweep that completes while an unclaimed position's
guard is held, body claim against cancellation, admission against peer ending, a channel's next
grant against the return of the outcome delivery of the attempt before it, and another peer's
progress while one peer holds its guard. The production admission choice has a Loom model for one
irreversible verdict; replacing its compare-and-exchange with an overwriting swap must fail and
replay. ArcSwap, queues and external semaphore internals remain outside that atomic claim.
Registered bounded Bolero sequences check complete verdicts, capacity and generation outcomes.
The `remote-correlation-lifetimes` sequence uses an independent sequential model to predict
registration identities and FIFO reuse before calling the production owner. It varies both pools'
capacity, receiver identities, attached and detached batch rows, admission receiver closure,
single and bulk admission, Alive and sequenced parking/resume, terminal replies, clear, relay-memory
refusal and shutdown. Every operation and every individual silence sweep checks exact root outcomes
and progress, drain and handoff counts, admission updates, per-receiver expiry counts, retained
memory and the positions ever first-claimed. Stale generations and non-record admission row
numbers are also checked through the public routing operations. This is sequential representation
evidence; the registered Shuttle checks above own races, and the runtime scenarios own the timed
sweep loop and its public delivery behavior.
Turmoil covers transport loss, retry, cancellation, reconnect and process restart within its
documented scope; public one/three-node branch delivery and producer-restart scenarios cover host
ACKs. External immutable-image faults remain separate evidence.

Deloxide applies to the new delivery, peer and record blocking guards and their acquisition order.
Both diagnostic selections run the remote ACK scenarios alongside their existing workloads. It
does not model publication, async channels, network waits or the atomic verdict; a clean run proves
only the tracked acquisitions the diagnostic processes executed. Generated measurements and fault
evidence belong on the task, and a failed required attempt remains recorded.

Leading empty consensus append probes use management capacity. One task owns their replies and
the transition to an ordered data stream; subsequent empty commit updates stay on that stream.
Rejected probes end the input before data admission. This transition adds no shared map, lock or
memory-ordering claim. Native real-transport and public catch-up checks cover it; Deloxide does
not track these async network waits.

Consensus append setup consumes its caller's first request before opening the transport. The
caller owns the async retry and cancellation gate; the adapter retains and submits that request
once, without adding a lock or retry timer. Native admission and ended-input regressions exercise
this boundary, and the public three-node scenario checks paced failed setup while the surviving
quorum commits and the returning voter catches up. The test probe counts attempts and successful
opens in its existing blocking map. Deloxide does not track the request-stream or network waits;
the applicable Turmoil suite and real-process Chaos faults retain their separate claims.

## Relay, Transport And Source Lifetimes

A concrete producer retains its branch channel handle. The relay publishes branch membership in a
persistent immutable table; concurrent first binding selects one allocation with CAS. Each branch
publishes an owner/consumer routing generation, whose cancellation parent fences ingress and
outbound slots, including candidates created during replacement. Eviction cancels the exact branch
before withdrawing its allocation. Invalidation is scoped to the branch that ended: a relay owner
invalidates its own relay's branch, and a processor, ingestor or reingestor invalidates the
branches of the relays it publishes through for its key. Another relay's branch for the same key
keeps its channels. An independently expiring receiver branch permits a still-live producer to
bind a fresh channel after invalidation. The producer's own ending moves it to `Ended` and
prevents rebinding. Domain terminal teardown cancels the relay publication itself.

Channel gates serialize encode, sequence assignment and dispatch for that channel only. Gate waits
select cancellation, and transport admission retains its existing physical deadline. They do not
serialize unrelated branches or peers. A subscription snapshot selects only live advertised peers,
fenced by incarnation and interest version. Equal advertisements retain channels; changed selection
cancels the preceding subscription generation. Branch and destination registration copy only a
persistent changed-key path. Tables are never rebuilt for an established batch.

Transport targets publish immutable fixed pool arrays. A slot atomically claims one reconnect
worker for its lifetime and publishes its authenticated connection through `ArcSwapOption`. Leasing
reads these publications, scans the fixed class slots and retains the selected connection. The
connection registry serves registration, exact retirement and observers. Endpoint, peer or TLS
replacement cancels slots before publishing replacement handles; a changed DNS answer affects only
the next dial. [Cluster Interconnect](./interconnect.md) owns quotas, deadlines and delivery rules.

A fresh intake selects its domain publisher from an immutable persistent table. Executing tasks
retain the selected publisher and observe complete snapshots. Domain removal withdraws its table
entry; terminal shutdown clears the table after ending executions. Buffered error routes similarly
send through the worker retained by the bound plan, with no failure-path discovery acquisition.

Source polling writes a retained instance readiness scalar. Starting and ready may alternate;
retired is final, so a late poll or drop cannot change a replacement instance's readiness. These
scalar transitions and worker claims use relaxed atomic operations and publish no other data.
Shuttle checks production binding, refresh, retirement and polling races. ArcSwap internals remain
opaque; this delivery adds no cross-location memory-ordering claim requiring a new Loom weakening.
Existing memory-ordering claims remain with their owning models. Deloxide tracks the existing
sequence and route-task blocking locks; it does not track ArcSwap, scalar atomics or async channel
ordering gates.

`just bench-retained-channels` measures published versus retained branch selection, established
stream leasing and bounded branch churn. Its ordinary native bodies also run in `bench-smoke`,
whose shared native coverage producer prepares browser, guest and runtime dependencies before
instrumentation. The measured bodies retain the usual allocation assertions; their timing output
is workload evidence, not a portable latency threshold.

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
| Emitter execution plans | The decision layer prepares the domain's typed emitter and ordered source edges with lowered expressions from one schedule revision; schedule application installs them in that revision. Startup, swaps and relocation bind a selected plan to local mounts, schemas and UDFs; relay inputs are registered during those transitions. Remote consumer edges come from the same plans. | An emitter task owns its bound programs and emitter configuration for its lifetime. Per-batch work does not read the schedule or client Models; emitter retries reuse the typed configuration. A dynamic flush change sends only its new flush policy to the running task. |
| Message-error route plans | The registry selects each DLQ route from the committed schedule. Domain installation binds its schemas, branch declaration, flush contract, relay target and SET program before tasks start. Schedule application publishes the complete bound route map with the routing state of one typed revision. | A failed record looks up its prepared route in the task’s retained immutable routing snapshot, which publishes the bound plans together with the routing revision, before running its VM program or delivery. A buffered route compares the bound plan allocation with its running delivery task; a replacement drains the preceding task and starts a task with the new target and cadence. |
| Node identity and remote dispatcher | The node runtime publishes this once after cluster join, when the authenticated interconnect and process incarnation are known. Relay boundaries created afterwards retain the same dispatcher handle. | Readers borrow the stable node identity, incarnation, transport, admission service, and ACK registry without a write-once lock or repeated name allocation. |
| Relay owner state | Each relay boundary publishes its scheduled owner, installed owner buffer, remote runtime-consumer set, and immutable branch-reset gate set. Schedule and relay lifecycle operations replace these values at their cutover points. | A batch borrows the current owner and buffer, then takes permits only from reset gates whose typed scope selects its branch. Multi-step ownership changes use the whole-relay dispatch gate described below so teardown cannot race an admitted dispatch. |
| Relay branch presence | The relay's owner task publishes the complete membership of the concrete branches it holds, and whether it admitted unbranched work, when that membership changes: a branch appears, is evicted or expires, or the owner starts or stops. The relay's state placement keeps the same presence across execution rebuilds. See [Relay branch presence](#relay-branch-presence). | `DESCRIBE RELAY ... WHERE`, materialized reads, and the console's dataflow graph load one membership without a lock. A batch for a branch the owner already holds publishes nothing. |
| Subscription interest | The cluster live-state watcher rebuilds an immutable index from domain and relay to interested node incarnations and advertisement versions whenever gossip changes. | A relay owner performs borrowed lookups in one published index. It neither formats gossip keys nor waits on the gossip mutex per batch. Subscription creation waits until every live node has observed the exact subscriber incarnation and at least the current advertisement version before reporting success. |
| Clock installation | Each domain-clock lifecycle on each node publishes the complete missing, stopped, uninstalled, unpaced, or paced installation. | A read validates its bound lifecycle generation against one installation, then advances that installation's nondecreasing timestamp watermark atomically. A same-generation replacement retains the watermark; a different generation cannot be clamped by a stale reader. |
| Accepted clock progress | Each runtime domain publishes its newest accepted generation, tick id, logical boundary, and authority UTC observation through a watch. | The watch serializes comparison and replacement, so concurrent progress deliveries cannot regress the id or its authority UTC observation. Generation changes and stops publish absence. A session observer subscribes before reading, retains a sender for as long as its attachment lives, so removing the domain wakes it instead of closing its wait, and uses an execution snapshot only to add its serving node's logical reading to a tick frame. |
| Committed domain installation | Each node's runtime counts its installations of the committed domain states through a watch. An installation advances the count only after it has inserted, updated, or removed every domain. | A session attach waits for the first count before it looks up a domain, so a node that is still starting never refuses a domain the cluster has. Because the count advances after the domains are in place, the wait never releases before they are observable. |
| Forced-recovery destination installation | A recovery subscribes to the domain installation watch before looking for its execution, and selects its matching node under the schedule application mutex. | The watch retains a publication racing the absence check; the mutex orders selection after graph construction, even when domain lifecycle publication occurred first. The existing preparation deadline bounds both waits. The mutex is released before checkpoint transfer and guest restoration. A production-owner Shuttle check explores installation racing selection; async mutex and watch waits remain outside Deloxide's detector. |
| Runtime-state assignment | Each state placement publishes one packed atomic binding containing its generation and capability. Replication roles are a separate immutable published snapshot. A running Kafka ingestor commits its domain offsets through a published originator, which schedule application replaces when a reassignment rebinds the offsets while the ingestor keeps running on its node. | A per-message operation admits itself, compares the exact binding it was granted, and proceeds only while that generation still grants the required capability. It never takes the assignment barrier. A commit loads the ingestor's published originator once; an ingestor instance the schedule stopped keeps its own superseded originator and stays refused. |
| Ingestor quiesce decision | Each ingestor publishes the declared and pending modes, active causes, source support, and derived intake decision as one value. Concurrent lifecycle changes derive their replacement from the current publication. | Polling and per-message intake make one load to decide whether to dispatch, suspend, skip, buffer, drop, or reject. A source host retains its last observed publication across dispatch awaits; its change wait registers before comparing that publication with the current one, so an engagement or release in the gap wakes it. The retained-payload lock is reached only after the published decision selects buffering. |
| Backup state snapshot | Branch lifecycle publications, Kafka offset commits and WASM checkpoints register on a per-domain atomic generation before changing their published state. After the domain is paused and drained, the owner closes that generation with a Release RMW and waits with Acquire loads for registered publishers to leave. It then serializes Kafka positions and branch lifecycle into the runtime state store and opens one database snapshot for those records and durable WASM saves. The closing RMW is in the release sequence that a publisher entering the next generation acquires. | A publisher that entered before the cut completes into the captured view; a publisher that enters after it waits for the next generation. An older periodic storage write cannot replace a newer forced publication. The backup reads the database snapshot off the record path and stages owned section bytes. A live backup does not close the generation and reports its weaker cut explicitly. The server's Shuttle and Loom checks drive this production fence. |

| Restore state publication | The control plane stages checkpoints under a replicated authority, then admits one fixed-size storage job, validates a complete generation namespace and durably publishes its active pointer. It holds the consensus applied-state read guard through authority validation, synchronous storage mutation and runtime-handle clearing. The state store reuses its existing latest-snapshot installation mutex for staging, pointer publication and bounded cleanup. Checkpoint jobs retain their selected namespace and validate it under that barrier; entity replacement and purge also select the active namespace while holding the barrier. | A newer applied generation and the release of the start gate require the state-machine write guard. Every node therefore replaces a whole state set before `START` is available, and stale coordinators cannot mutate a running restored domain. The store retains authority and inventory for exact retry and monotonic rejection. Database readers select pointer, header and chunk values from one snapshot, which retains its complete set across bounded deletion of obsolete namespaces and unreferenced active chunk sets. Publication runs on stopped-domain lifecycle paths. The admitted maintenance loop also follows running checkpoint replacement and purge under the installation barrier; record delivery adds no lock. |

The Syslog stream source gives each accepted connection its own decoder and one bounded intake
channel send. After decoding a frame, the connection completes that send even when a SUSPEND watch
change races a full channel; it observes the pause before its next read. An accept error removes
only the listening socket, leaving the connection tasks and channel owned by the same source until
resume rebinds the socket. Source close aborts those tasks. The registered Syslog Shuttle check
explores the full-channel handoff against a pause change. One-node and three-node Cucumber
scenarios exercise every listener. They check the delivered frame count through entity quiescence
and keep one connection streaming after an injected accept failure while the source rebinds its
listening socket. The test-only fault channel enters through the source plan and retains one armed
request until its selected listener acknowledges it. Another arm cannot replace an unobserved
request, and a cancelled accept leaves it available. No shared lock or map is reached per frame.

Restore storage qualification includes the store's cancellation, corruption, snapshot, queued
writer, active chunk liveness and many-key cleanup regressions in `just test-deloxide`,
`just test-deloxide-order` and `just test-deloxide-stress`, plus the public one-node and three-node `@restore_installation`
workloads, which `just test-deloxide-stress-restore` also runs under the bounded scheduling
disturbance, for large saves, many checkpoints, native lifecycle and Kafka records above the default
32 MiB bulk budget, active chunk replacement and purge, durable publication failure, restart and
delayed coordinators. The many-checkpoint three-node case includes one replica per state; its
one-MiB saves fit the replication operation's limit. Deloxide tracks the installation barrier and other
boundary blocking locks; Fjall's internal locks, async authority ordering and memory ordering keep
their separate ordinary, Shuttle, Loom and Turmoil checks. Measurements and diagnostic artifacts
are delivered to the owning task.
The registered large-WASM-handoff diagnostic scenario reaches replica installation and planned
handoff with two branch saves above that message limit. It exercises the state store's tracked
installation locks in both Deloxide selections; the bulk stream's async waits and transport faults
remain outside the detector and have their own Turmoil and Cucumber evidence.

Materialized capture shares row carriers under the existing assignment barrier, together with the
revision, ownership fence and branch generation. Membership insertion and eviction use that same
barrier; ordinary updates of existing branches keep their existing admission protocol. Fresh
backup capture admits its bounded row-view metadata before allocation. Archive conversion stages
one identity/Arrow group at a time. Native loading independently bounds typed-key and row-view
metadata to 8 MiB, charges retained decoded columns to relay memory through installation, and
uses bulk memory for one section's decoding scratch. A cumulative relay admission refusal ends
loading instead of waiting while retaining earlier groups. A pinned database reader retains one
snapshot and one stored chunk through all section reads. Replica installation checks monotonic
revision under the existing exclusive assignment barrier before replacing any entry.

`RESUME` changes the domain to running at its archived lifecycle in the consensus effect that
records complete publication and releases the installation gate. No atomic field or memory
ordering in the capture epoch, assignment protocol or relay revision changed. Existing Loom
models continue to qualify their ordering; new Shuttle checks qualify branch membership capture,
replica revision races and publication before activation. Paused generators release their
admitted-work guard after flushing and reacquire it before producing more output.

Native restore conversion retains the immutable verified-description owner and its preparation
reservation while an admitted storage job streams the current checkpoint encoding into a
quota-owned file. The job owns the disk permit until its file closes or becomes a completed
artifact, including cancellation. Entry serialization and 64 KiB writes check cancellation
between bounded units. Each bounded artifact read also retains its file and quota in the storage
job until it exits. Local and remote installation retain their exact file source and use
the existing authority and installation barriers; conversion adds no lock to record paths.

Stored branch capture also retains one immutable database selection, opens only the scheduled
checkpoint, and streams it in cancellable 64 KiB units into a quota-owned file. Its admitted
read-only mapping and size index remain owned through each bounded conversion job; cancellation
does not release their memory or disk permits before that job exits. Keyspace and window codecs
convert only the selected archive group. This adds no lock or publication protocol. Existing
capture and restore lifecycle locks remain tracked by Deloxide: the stored-branch scenario runs
in active and order selections and in the restore stress selection. Deloxide does not observe
the mapping, CPU jobs or async admission; ordinary cancellation/refusal owner tests and complete
Bolero byte comparisons cover those boundaries. The capture fence's Shuttle and Loom models
retain ownership of its unchanged publication and ordering invariants.

The backup coordinator waits for each node's admitted-work counters to reach zero, then orders one
confirming force-flush generation across the cluster. A polled node starts a draining generation
only while its own counters show admitted work, as the shutdown drain's draining step does, so a
quiet domain's participants take the confirming generation alone. The generation's obligations use
the same atomic counters as shutdown; a parked `REQUIRED WAIT` batch stays visible separately and
cannot hold the cut open. Rows a window retains park their shares too, and the cut captures them as
window state.
| Metric series handles | Each relay, node, ingestor, emitter, or concrete branch resolves its label set, internal series, and Prometheus child when its owning task or branch is created. | Recording uses the retained series directly. Counters update atomically; a histogram records through its already-resolved per-series accumulator without a registry lookup, key construction, or map guard. A node input folds a whole batch's delivery latencies in one kernel pass over the batch's high-watermark column before recording, so each latency series locks its accumulator and reads the wall clock once per batch rather than once per row, and its Prometheus child takes the batch in one flush. Whether a series was ever observed is an atomic flag set under that lock and read without it. Registration and removal stay on lifecycle paths. |
| MQTT broker packet limit | Each MQTT emitter's event-loop task stores the Maximum Packet Size of every `CONNACK` its client receives, or the broker's silence about one, as one atomic scalar. A reconnect replaces it. | A publish loads the scalar once per record to reject a packet the broker would refuse before handing it to the client. The value stands alone, so relaxed ordering is its whole contract, and a record published before the first `CONNACK` is measured only against the protocol's largest packet. |

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

A source or emitter retains one atomic failure publication. An emitter's task and its retained
connector background task own independent failures within that publication: task only, connector
only, or both. Each failure contains its safe error and optional retry. A writer clears its own
failure through a read-copy-update of the current publication, preserving the other writer's
failure even when their transitions race. Healthy polls only read, and identical reports retain
the same immutable failure observation.

The failure selected for retry scheduling gives an active task retry precedence over a connector
retry. Otherwise an active connector retry stays visible ahead of a task error without a retry; with neither retry,
the task error takes precedence. This keeps publishing work visible to drains even when the emitter
buffer is empty. Reporting a task error without a new retry preserves that task's active retry.
DESCRIBE retains the whole immutable publication, renders both owners' errors when both fail, and
loads the selected retry's backoff and remaining wait from that same publication.
The registered TaskStatus Bolero property and production-owner Shuttle check cover independent
clearing, concurrent reports, coherent selection, both diagnostic owners and immutable retained
observations. These are publication interleavings; the primitive boundary owns its atomic
memory-ordering implementation.
Emitter confirmation guards retain one counter resolved at startup, and task teardown removes its
registry interest.

An Iceberg emitter retains the first table's `FileIO` while replacing catalog metadata. Its storage
factory initializes one operator for the configured object-store root; data, manifest and metadata
files use that same initialized storage after refreshes, successful commits and ambiguous-result
reconciliation. This reuses the library's existing storage lifetime and adds no synchronization
protocol or shared map. The emitter task remains the sole owner of commit retry pacing.
Its declared `BATCH` file division measures Parquet bytes on the node's bounded bulk CPU workers
before staging Arrow IPC segments; the emitter task still owns those segments and the one retained
carrier acknowledgement until their catalog commit. The CPU queue and catalog I/O are asynchronous
and outside Deloxide's tracked blocking locks. The one- and three-node Iceberg file-bound scenarios
run in the active, order and lifecycle-stress selections to exercise the tracked host state beside
that staging and commit path. The restore-stress lane remains part of the required four-lane
qualification when this lifecycle changes, though it owns restore workloads rather than an
Iceberg-specific scenario.

Every pooled Redis or SQL emitter registers a wait slot once. The connector serializes its connection
borrows. A ready borrow writes nothing; the first Pending poll publishes its wait and owns a guard
that clears it on success, error or cancellation. Emitter destruction removes only its own registry
slot, so a replacement emitter's slot survives an earlier emitter's teardown.

A Redis command pool keeps its `POOL SIZE MIN` with one keeper task, which the pool's last handle
stops. Each connection the pool opens holds the pool's closure notification and signals it once,
when the pool closes the connection; a publish neither signals nor waits on it. The keeper
registers for the signal before it reads the pool's counts, so a closure after its reading wakes
the wait that follows, and a restoration holds checkouts of the same pool, so `bb8` accounts every
connection it opens for the keeper against the maximum. The notification and the pool's waits are
asynchronous and outside Deloxide's tracked locks; the registered Shuttle checks under
[Protocols and their checks](#protocols-and-their-checks) drive the production keeper.

Each Redis Pub/Sub source has one socket reader and one bounded asynchronous handoff. The reader
reserves a slot before decoding its next frame, retains one parsing future across its idle probe,
and hands each decoded message over once. Host read cancellation acts on the receiver and leaves
the reader's partial frame intact. Suspend and close abort and join that exact reader before a
replacement is installed. The channel and Tokio I/O are outside Deloxide's tracked locks; ordinary
owner tests cover binary payload order, capacity, protocol setup and probe outcomes. The public
BUFFER, DROP and idle recovery cases also run in the active, order and stressed diagnostic
selections to exercise the existing tracked host controls. No new tracked-lock owner or custom
memory-ordering protocol is introduced.

Postgres and MySQL borrow once per bounded insert and retain that connection through any
record-error isolation. Isolation therefore takes no second pool wait behind another writer.
The connection returns before the next insert or retry backoff. The MySQL public contention
scenario exercises the real driver and database; these external asynchronous waits are outside
Deloxide's tracked locks and the model checkers' coverage.

A MySQL pool keeps its `POOL SIZE MIN` with one keeper task, which the pool's handle stops. The
keeper holds no lock and waits on no notification: once a second it reads the driver's count of
established connections and, below the minimum, awaits the driver's checkouts one at a time,
holding each until its pass ends, so every connection it opens is one the driver accounts against
the maximum. These external asynchronous waits are outside Deloxide's tracked locks and the model
checkers' coverage; unit tests drive the production keeper over a stand-in pool on a paused clock.

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

Clock authority reconciliation binds a producer to its locally installed mapping, generation and
authority fence before starting it. Each node's domain clock owner installs a committed
authority-only reassignment ahead of graph installation. Each installed domain records the
committed revision of its lifecycle, which keeps an older graph installation from restoring a
superseded fence. A graph installation held on a departed node therefore stalls neither the
replacement authority nor a receiver. Each remote delivery loop starts after its receiver
advertises installation of that revision's domain clocks. An obsolete producer still retires
before its successor starts, and a leader proposes an authority that did not commit again on a
fixed interval instead of waiting for the next state change. The takeover Cucumber scenarios in
`domain_clock_contract.feature` exercise this ownership path in the active, order and lifecycle
stress Deloxide selections; their async installation, watch and transport waits remain outside
tracked-lock detection.

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
The [retained task handle measurements](https://app.clickup.com/t/86bc9v74y) record the measured
operations, host, samples and limits.
The concurrent map inventory records their lifecycle registration, observation and teardown sites.

### Server endpoint intake

`EndpointIntakeRoutes` owns one immutable host-and-path table containing each domain's configured
endpoint definitions and the source lifetimes bound to them. Domain install or teardown and source
bind or unbind derive a replacement from the current publication with `ArcSwap::rcu`. Concurrent
writers retry against the latest complete table, preserving unrelated domains and sources. A
passive installation publishes no endpoint definitions. The listening socket remains present on
every node independently of this table.

The table also records whether a revision installed it since the node started or the runtime last
cleared it. A revision application marks it installed once every domain of the revision has
published its definitions. A request reads the route and that state from the same load. A
published route is served whatever the state. A missing route is unrouted, answered with 404, only
in an installed table; before installation it is uninstalled, answered with 503.

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

A request dispatches all bindings of its resolved endpoint concurrently before waiting for their
combined result. One binding's downstream ACK cannot gate delivery to another binding. The
production fanout and request windows have a Shuttle check that withholds both consumer ACKs
until both bindings start, completes them in reverse order, and checks independent permit release.

An acknowledged endpoint intake owns one semaphore window, created from its typed positive count.
It tries admission once and holds the borrowed permit through graph ACK/NACK, its absolute
physical timeout, or intake ending. Exact lifetime ending cancels the lifetime token before
publishing absent intake. A separate wait observes shutdown or handoff through the quiesce owner's
existing register-before-read publication wait, so the HTTP response does not wait for source
close after drain. Resumable pauses preserve the admitted root. Completion ready in the same poll
as an ending wins; otherwise ending or deadline returns its unavailable outcome and releases the
slot. Listener admission shutdown also ends the wait before graph drain. No response slot, payload, ACK root or outcome is persisted. Dropping a request drops its
permit; admitted graph work can still complete and sender retries can duplicate effects.

Registered Shuttle checks drive this production window with scheduler-visible ACK and deadline
events, assert exact capacity and independent completion order, race ACK against expiry, and
resolve a retained lifetime while preserving its replacement. Semaphore, cancellation-token and
network waits are asynchronous and untracked by Deloxide. The tagged one-/three-node endpoint
scenarios join the active, order and stressed lifecycle selections and reach the existing quiesce,
entity-gate and root-lifecycle blocking owners; the restore selection retains its existing scope.
The bounded outcome-body property checks the public projection separately.

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

### Ownership handoff drain before freeze

The runtime closes intake and the affected boundary gates, then requests a force flush and
confirms the admission hold. The coordinator can observe pending work and its timeout diagnostics
while the installed ingestor routes and processors finish that flush. Each participant's exact
operation drain-status query publishes its ownership freeze only when the local affected scope is
drained. Capture requires both the held operation and its own coordination identity's freeze.
The status query retains the operation's registry guard through publication, so release cannot
withdraw its hold between observing drain and publishing capture readiness.

This is lifecycle work: the polls read existing quiesce and acknowledgement registries, and hot
paths continue to retain their counters and freeze publications. The existing force-flush and
quiesce checks cover completion visibility; freeze checks cover publication and wakeup order.
The ownership drain reads force-flush obligations before admitted-work counts. Acquiring a
completed flush therefore makes a parked message that the flush resumed visible to that drain.
`loom_an_ownership_handoff_drain_sees_work_resumed_before_flush_completion` exercises the production
counter method; its qualification weakens that method's existing acquire. The handoff publication
check races the production freeze decision against the actual participant's flush completion.
The public shutdown regression holds an admitted Kafka route until handoff publishes its flush
request, then verifies completed drain and exactly one acknowledged output. The diagnostic lane
registers that path in all three selections; async wakeups, transport and deadlines remain outside
tracked blocking-lock detection.

### Emitter payload assembly

One emitter task owns the released carriers and their flush order. Its batch packer borrows each
carrier's source relay, concrete branch key and ordered metadata while it prepares selected Arrow
rows. It clones the Arc-backed Arrow batch once per carrier passed to the packer; no row
acquires a lock or increments an Arc reference count for source identity. The open candidate holds
at most `BATCH MAX MESSAGES` prepared members and is sealed on metadata or source changes, a failed
member, or the message-count limit. Each encoding attempt uses the codec's bounded writer under
`MAX SIZE`. The emitter retains original batch and row positions for acknowledgements and errors.

A record emitter prepares its external bytes once before retention, including when a payload carries
one row without `BATCH`. A Sentry envelope therefore keeps its event identity and complete bytes
across retries and connector replacement; preparation adds no shared owner or synchronization.
A payload offered to the emitter stays in the same task's buffer until the emitter answers for it. The
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

### Kafka source control execution

Kafka sources and partition inspectors submit synchronous driver operations to the node's bounded
connector storage class. Each job keeps its consumer and reservation until the native operation
exits. An asynchronous semaphore gives each consumer one control-operation turn; the admitted
closure retains that turn until it exits, so cancelling an assignment cannot let a following
assignment, seek or unsubscribe overtake its native mutation. The same client deadline bounds
waiting for this turn and for the node's worker. Cancellation checks between partitions stop
further work, while a native call already running retains its worker. Consumer-group commit
ownership is separate: it is installed before awaiting its
answer and survives cancellation of that await; another commit first consumes that exact answer.
This uses the existing semaphore adapter and makes no new memory-ordering claim. The registered
`shuttle_occupied_connector_leaves_filesystem_and_consensus_execution_untouched` check holds a
production connector worker while filesystem and consensus jobs complete, then verifies permit
and byte-reservation return.

The consumer a source or inspector shares with its jobs, and the producer a sink shares with its
finish's flush, are each destroyed by one more connector storage job, which their retirement
submits once the last handle is dropped and which waits for a place rather than being refused; see
[Kafka client retirement](./connector-contract.md#kafka-client-retirement). The retirement takes no
control turn, since no job holds the client any more, and adds no synchronization of its own: the
shared pointer's own count decides which drop is the last, and the job enters the executor through
its existing wait for a place. Native broker waits, the destruction's included, are outside Shuttle
and Deloxide's lock detector; the ordinary connector and retirement regressions and the
one-/three-node stalled-broker and retirement scenarios own that evidence.

### Kafka source rejection floors

A Kafka source under `OFFSET BY CONSUMER GROUP` records the earliest rejected record of every
partition it rejects in its consumer's context, before it seeks the partitions the consumer is
still assigned. The context's rebalance handling starts each partition the group assigns at its
floor, so a revocation that loses the seek, from a rebalance, the host leaving and rejoining the
group, or a lapsed group session, never resumes the partition past the rejected record. A commit
that passes the record forgets the floor, and an eager assignment that omits a partition forgets
its floor, since another member now resumes it from the committed offset. The floors sit behind one
thread-blocking lock per consumer: the source takes it once per rejection and, while a floor is
outstanding, once per acknowledged batch, and the rebalance handling takes it once per assignment.
librdkafka serves a rebalance only while the source polls for its next batch, and the host never
polls while it rejects or acknowledges, so the lock is never contended. No guard is held across an
await or a driver call that waits on the broker: under it, the rebalance handling only sets the
start offsets of the assignment list in memory. The registered
`topic::ingestor::rejection::shuttle_tests::shuttle_a_rejected_record_is_read_again_whenever_its_group_takes_the_partition_away`
and
`topic::ingestor::rejection::shuttle_tests::shuttle_a_commit_past_the_rejected_record_lets_the_group_resume_from_the_committed_offset`
checks let a coordinator task decide a revocation and an assignment at any point of a rejection,
serve them in the source's polls as librdkafka does, and hold that the first record read again
from the partition is the rejected one, and that once a commit passes it the record after the
committed one is read next. Native consumer-group traffic is outside Shuttle. Deloxide's detector
tracks the floors' lock, which the lane's refused-commit shutdown workloads reach at assignment, at
the rejection of the refused batch and at the commit that reads it again; the mock-cluster
regressions and the one- and three-node broker restart scenario own the driver evidence.

### Kafka producer retention

A Kafka emitter counts the records its producer holds itself, because librdkafka's own out-queue
length also counts the errors, statistics and unserved delivery reports waiting in the producer's
queue. Two parties change the count. The emitter task adds a record just before it hands the record
to the producer, through the retention the record's delivery opaque carries. librdkafka gives that
opaque back exactly once, and dropping it removes the record: on the emitter task when the producer
refuses the record, or on rust-rdkafka's polling thread once that thread serves the record's
delivery report, before the report reaches a confirmed publish awaiting it. The count is one
standalone atomic counter changed by read-modify-write, and a finish reads it by read-modify-write
too, so it reads the latest count. No other location is published through it, so it carries no
memory-ordering claim and has no Loom model. Nothing waits on it: a finish waits only inside
librdkafka's flush, until its own deadline. A record is retained no longer than librdkafka holds
it, so the producer's `queue.buffering.max.messages` bounds the count and the opaques behind it.
The Shuttle checks listed under [Protocols and their checks](#protocols-and-their-checks) race
hand-overs and counts against a task that serves reports, and the delivery callback's own order
against the publish that reads its report.

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
materializes a scalar-field copy of the retained payload. A window whose rows exceed one snapshot
section is sealed group by group into quota-owned pieces and published as a segmented checkpoint
under the state store's installation barrier, as a materialized relay's periodic checkpoint is;
the snapshot task holds no lock while it seals, and the barrier is held only by the storage job
that writes the segments and the header. Restore opens the sections on the bulk executor and
reuses their columns to rebuild exact accumulators and sketch panes.

Evicting a concrete branch resets its retained window rows and aggregate structures before the
branch task's final publication. The published generation is empty, so a later appearance of the
same branch key cannot inherit the evicted window or its sketch panes. After the final checkpoint,
the owner releases the evicted branch's in-memory publication. A branch that appears without a
restored lifecycle entry also publishes an empty initial window, even if a previous lifetime of its
key left a checkpoint behind. A processor task therefore installs every branch its lifecycle names
before it dequeues any input, and a restore the bulk executor refuses installs none of them and is
attempted again; see [Restoring Processor Branches](./shutdown.md#restoring-processor-branches).
Stopping a branch for an ownership handoff follows the normal finalization path and publishes its
retained window instead.
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
offer enters a node-wide announcement or progress map. State installation publishes a resolved
route containing the actual state handle and the entity's existing assignment slot. Frames select
that route from one immutable publication, never from the state, execution or identity registries.
An announcer retains its selected route for its whole lifetime and reads the current primary and
replica set from the assignment slot.

Branch-aggregate byte capture keeps the narrow `lifecycle_call` expectation for scanning the
metrics inventory at an explicit snapshot boundary. Each placement retains one encoded snapshot
under its own blocking mutex. Capture reads the dirty revision first and chooses that revision's
bytes once; subsequent descriptors, streams and persistence reuse them, including elapsed metric
fields. Initial, replica and persisted recovery install the received bytes with their revision.
The guard serializes capture and installation, covers only that placement's snapshot work and
never crosses an await. Metric recording advances the dirty revision without taking this guard;
the next capture includes those measurements. The guard belongs to cold checkpoint work, not to
record or batch execution. The two interleaved-tenant window drain and relocation scenarios run
in both diagnostic selections and the stressed restore selection. State-route selection uses the
installed state handle and retained assignment slot.
Checkpoint synchronization sends a bounded revision, length and digest description through that
selected route. The replica fetches the exact revision through the bulk pool, admits the whole
allocation and verifies every byte before installation. A handoff destination follows the same
sequence from the source's captured checkpoint while its entity gate remains held. Neither
path adds a shared hot-path lock or a new
wait-and-notify protocol; Deloxide sees the existing state-store installation lock but cannot see
stream scheduling or async memory admission.

A replica's progress only rises. Acknowledgements travel independently, and a replica acknowledges
what it holds again whenever it is offered a checkpoint, so an acknowledgement of an older revision
can arrive after a newer one; it leaves the newer one in place. Each wait for replicas registers for
the next report before it reads the progress: a WASM checkpoint waiting for the replicas its
boundary names, a Kafka offset commit waiting for its replica quorum, and a WASM state reset waiting
for the branch lifecycle that authorizes its first checkpoint. The reset wait retains its route,
follows the current replica assignment and rejects loss of identity or primary ownership. A
checkpoint promised to replicas never confirms with fewer replicas, using the same typed boundary
as a guest checkpoint. A 100-millisecond recheck observes assignment changes even without a replica
report. A report that lands between the read and the wait therefore wakes the wait instead of
leaving it to its deadline.

One announcer at a time offers a placement's revision. An offer raises the revision the running
announcer offers or, with none running, starts one. The announcer sends the revision to the
replicas that lag behind it, again every 100 milliseconds, and ends in the same step that finds
every replica the committed schedule assigns holding it, so a revision offered while it ends is
either sent by that step or starts the next announcer. It also ends when this node stops being the
placement's primary, when the replicated state goes away, and when the runtime stops. An announcer
whose task ends before it finished hands its announcement back to the next offer.

The runtime's checkpoint announcement task owner cancels both dispatch and retry waits when it
closes after domain drain. It joins every cancelled task before withdrawing replication routes,
so shutdown releases the task's exact retained state without waiting for a remote dispatch timeout.

The snapshot task of a deduplicator or window branch has the runtime as its owner too, because it
outlives its branch task: once that task is gone, stopped or ended at its stop grace, the snapshot
task still writes what the branch published last. The checkpoint writer of the state a scheduled
node's placement keeps, a materialized relay's records, a Kafka ingestor's domain offsets or a
node's branch-aggregated state, has the same owner, because it makes its last write only after every
task that changes that state has ended. The placement keeps a handle that holds the writer's task
and a drop guard over a cancellation token: the domain's stop drops the guard once those tasks have
ended, a placement dropped without its stop drops it as it goes, and either tells the writer to make
its last write, while a reassignment that replaces the placement's runtime aborts the task and
disarms the guard. The owner also counts each task for its domain. A domain's stop, once every task
of its execution has ended and every checkpoint writer has been told, waits for that domain's count
to reach zero, so a write a branch ended at its grace left behind and every checkpoint writer's last
write finish before the stop completes, and it waits for no other domain's tasks. A stopping runtime
then closes the owner, which tells every task still waiting for its branch or its placement to make
that write, and joins every one of them before it closes the announcement owner. Nothing is
cancelled: these waits are what keep the node's storage from being released under a write, so they
last as long as the slowest of those writes. The owner is a task tracker, a cancellation token and a
concurrent map from each domain with snapshot tasks to a tracker of its own, closed as it is created
so that a wait returns as soon as it counts nothing. A branch reaches the map once, when its task
starts, and a placement once for each checkpoint writer it starts, and each counts its task while it
holds the domain's entry; a domain's stop reads the entry, releases it before it waits, and removes
it only while it counts nothing, so it never removes a tracker a starting task joins. No lock is
held across a wait.

The announcement, the progress and the announcer's handover change together under a short lock
that belongs to the one placement and is never held across an await. The placement's originator,
its one announcer and its replicas' acknowledgements are the only participants; unrelated
placements never share it. An acknowledgement or an announcement names its placement, and the node
that receives it uses the handle resolved when that state was installed. The assignment and the
placement must still name the same schema and guest-state generation. Cold replacement or teardown
ends the route before withdrawing it, so a retained route never attaches to its successor, including
when the successor has the same placement key. A frame already admitted through a borrowed state
may finish on that exact state. On a replica, announcements of a lifecycle or one of its branches
use the published entity lifecycle, which the entity's replica task retains. An announcement of an
entity a node holds no state for wakes nothing and creates no state.

A replica installs a branch checkpoint only while the branch lifecycle it holds names the branch.
Each lifecycle checkpoint a node holds is decoded once into the set of branches it names, so
installing a branch checkpoint looks its branch up in that set, through the lifecycle handle the
replica task retains, instead of copying and decoding the lifecycle each time. Holding a replica's
copy of a branch checkpoint compares revisions and moves the newer payload into an immutable
publication owned by the entity lifecycle. Lifecycle pruning visits only that entity's passive
copies without acquiring a registry shard. Catch-up steps and installation read the assignment
slot the lifecycle retains. Cold assignment cleanup also discards superseded passive generations,
while an already borrowed checkpoint can finish. Cold promotion consumes the passive copy;
non-branch handoff snapshots remain in a cold recovery registry.

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

A materialized relay initializes its task-owned retention registry from the restored branch keys
and one activation-time domain-clock snapshot. Activity is not stored with its materialized rows;
the rows' event watermarks remain ordering metadata and never become the registry's activity
times. Every restored branch receives its complete TTL from activation, and subsequent admitted
batches refresh activity in the same exclusive owner.

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
record for each concrete branch. Its origination capability moves into the branch runtime or the
scheduled relay-state task and cannot be cloned. That task owns an ordinary map of mutable branch
records. An established branch retains its row publication: timestamp comparison uses the owner's
record, then publishes one immutable Arrow-backed row view. It acquires no shared-map shard and
does not republish or scan branch membership. A larger high watermark wins, then a larger low
watermark; equal watermarks retain the first record and advance no revision.

Replacing an existing branch's record is admitted under the originator's assignment generation.
Adding the first record for a branch or evicting a branch also
changes the branch lifecycle, so that narrower operation uses the assignment barrier and advances a
branch generation. The persistent membership publication changes only at those boundaries or a
snapshot installation. Eviction first clears the exact row publication, then withdraws that member.
Recreation allocates a new publication; a retained ended member never attaches to it. Rebinding
waits for operations admitted under the preceding assignment before rebuilding the new task's
mutable selections from the published views. Previous writer tokens refuse further mutations.

Domain routing retains one materialized publication per relay and its relay-state epoch. The
installed state-replication routes supply the relay's immutable branch index. Dependency reads and
generator ticks load that retained relay index and the installed row views, rather than looking
up or iterating node-wide materialized-state, presence or epoch registries. Presence comes from the
retained relay services; placement identity comes from the shared immutable assignment publication.
Live state owns absence: an empty concrete branch does not fall back to the relay snapshot, and a
missing live row does not fall back to a pre-eviction stored snapshot.
Cold installation and public observation retain their lifecycle registries.

A snapshot capture holds the assignment boundary only long enough to take immutable row views,
their revision, their ownership fence, and their branch generation. The Arrow columns stay shared,
and sealing and encoding happen after the boundary is released. A snapshot from an earlier
assignment or branch generation cannot restore state that a newer owner or eviction superseded.
Installation also refuses a lower revision within the same fence and branch generation. A cached
sealed snapshot answers a current read only when its revision and assignment fence still match.
One sealed generation is cached and one build is admitted per placement; outstanding readers and
captures keep only their selected row views and shared Arrow carriers. Publication never copies a
row payload or a whole relay for an established update. Snapshot archives and storage ownership
retain the current container and backup contract.

A stopped backup selects materialized checkpoint readers alongside its other state from one
immutable database view. Those readers keep the selected namespace, metadata and chunks through
bounded group conversion, even if a later publication replaces or reclaims the generation.
They retain one stored chunk and one charged group at a time and publish no runtime state.
Running and paused backup capture continues to borrow current row views under the assignment barrier.

The required-wait observation creates its notification before reading dependencies. The primitive
boundary registers a `notify_waiters` observer at future creation, so an update after the read and
before its first poll still wakes it. Broadcast publication occurs once after each changed batch
and after a deletion. Periodic rechecks also cover remote changes; they do not establish the local
wake invariant.

The registered `materialized-publication-lifetimes` property exercises bounded updates, captures,
deletes and rebinding, then restores every captured schema, logical value, watermark and branch.
Production-owner Shuttle checks cover update/capture, eviction/recreation, assignment replacement
and required-wait registration without a polling timer. The Loom invariant
`server.state-assignment.drained-publication` proves the synchronous admission drain observes
preceding writes; its relaxed completion weakening must fail. ArcSwap internals are opaque to
these models. Supported Turmoil transfer checks establish network framing and replacement scope,
not disk durability or whole-cluster recovery. Typed Ratchet 10 owns mandatory immutable-image
materialized smoke/soak qualification. The same-host `just bench-materialized-state` measures row
and batch cost for 1, 256 and 4096 branches, allocation and captured-carrier retention; instrumented
benchmark smoke execution supplies coverage, not performance evidence. Assignment/capture
barriers and sealed-cache locks are tracked by Deloxide; async notifications and opaque publication
internals remain outside its detector.
`just test-deloxide`, `just test-deloxide-order` and `just test-deloxide-stress` include the materialized-publication diagnostic
process, which starts the detector and exercises timestamp replacement, capture, sealing,
installation lifetimes, rebinding, lazy relay installation, branch eviction and bounded complete
snapshot properties. The latter also checks potential acquisition-order cycles among tracked lock
instances. Each selection retains its own evidence alongside the other diagnostic processes under
`target/deloxide/test-deloxide`; these instrumented executions supply correctness evidence, while
the ordinary native benchmark supplies the row-publication cost measurements.

Sealing retains bounded identity and Arrow pieces on quota-owned staging disk and concatenates
through one 64 KiB read buffer. Row projection and its IPC result share one bulk admission and
one CPU job; projected columns never wait for a second grant while holding the first. Typed
identity conversion is charged before it allocates. The sealed cache holds the immutable file and descriptor, so its
memory does not grow with container size. Periodic writers select their namespace before executor
admission, write one 64 KiB database chunk per batch, synchronize the chunks, then synchronize the
replacement checkpoint header. A queued writer whose namespace was replaced is inert, and a
pinned reader continues through the same database snapshot. Replica installation rejects a lower
revision before clearing its current entries.
An incoming materialized snapshot releases its response stream at EOF, before local verification
or Arrow decoding. Those local operations retain their own staging and memory admission; they do
not keep Snapshot request or connection-stream permits while waiting for executor work.

Metadata and Arrow decoding retain encoded input charged to the bulk pool. Additional scratch
admission cannot wait behind those inputs: it refuses without queueing and releases the input,
so concurrent reads can free room for one another. Framing input is released before the next read
to avoid retaining a stored reader's working charge across another admission. A generator reports
the typed refusal and retries a complete read on its next occurrence. The bounded memory owner
tests and the public relocated-generator contention scenario exercise this progress contract;
these async admission waits remain outside Deloxide's tracked locks.

A generator task retains at most one decoded remote source generation, including its metadata and
column admission charges. Optional retention first reserves the complete charge from the
independent `state_read_cache` pool, whose capacity equals the relay pool's, then returns the
snapshot's `state_read` charges. Both read pools have independent relay-sized capacity and are
counted in the executor memory total. Decoded snapshots use `state_read` through installation or
evaluation and dispatch, leaving relay admission available for the work that completes the read. A full
cache pool returns the current complete read without caching or waiting for retention space;
its transient state-read admission ends with that read. Each occurrence describes the currently selected owner again. Reuse
requires the same owner, placement and complete sealed envelope: length, digest, revision, fence
and branch generation. A revision alone cannot identify contents after restart or restore. A
changed envelope drops the retained generation before replacement admission. Local routing,
failed reads, paused domain time, a closed source gate and ending intake release that view. The
cache is task-local and volatile; it adds no shared map, lock, waiter or publication protocol.
An outstanding read shares the immutable snapshot and keeps its admission until that read ends;
clearing the cache releases the cache's reference without revoking an admitted read.
The public remote-generator regression and full-value authenticated transport test cover reuse
and replacement. Deloxide reaches the existing sealing locks, but does not observe the task-local
cache or async transport waits.

Kafka offset catch-up checks the revision before retaining or encoding a checkpoint. Its table
retains the optional explicit-start generation. First initialization publishes its
generation and resolved positions under the existing exclusive assignment barrier; another instance
that resolved the same start retains the current table and its committed slots. No new lock or
concurrent map is introduced. The driver rereads the accepted positions before it begins polling.
The production-owner Shuttle check
`shuttle_instances_apply_a_generation_start_once_without_rewinding_commits` explores two instances
initializing and committing independently. The public `@kafka_start_generation` and
`@kafka_start_generation_restore` workloads cover resume, restart, partition growth and restored
starts in the active, order and corresponding stress diagnostic selections; Deloxide observes their
lifecycle and storage locks, while the assignment atomics remain outside its detector.
Capture holds the assignment barrier only to select a conservative revision and immutable partition
topology; encoding retains that topology and reads shared offset slots after the barrier returns.
A commit included ahead of the captured revision is offered again by the following revision.
The native encoder writes into quota-owned staging disk with a fixed 64 KiB I/O grant and separately
admitted serializer scratch. Bulk transfer carries bounded chunks rather than a whole resident
checkpoint. At EOF the receiver releases its Snapshot stream, verifies the staged file, and admits
native decoding separately. It builds a replacement table directly from archived entries, checks
cancellation between entries, and publishes only under the replica's existing assignment token.
A failed conversion changes no installed offsets; rebinding rejects a delayed installer. No new
concurrent map, per-record lock, or atomic ordering is introduced by this transfer path.
An unchanged poll repeats the held revision through the existing replica assignment admission.
This recovers a lost acknowledgement without encoding another checkpoint, while promotion or
replacement prevents a retained poll from reporting progress through its former token.
The runtime passes the native bulk operation's thirty-second budget into the quorum wait for both
committed and reset Kafka offsets. The offset-state owner supplies the register-before-read wait
mechanism; it no longer chooses a separate response budget that can expire while an admitted
native checkpoint is still progressing. A missing replica acknowledgement still fails at this
bounded deadline.

### Acknowledgement state

Each source attempt owns one in-memory acknowledgement tree. Fan-out reserves shares in an atomic
pending count, and each completion resolves one share with a compare-and-swap. A second atomic word
encodes either a typed tracking state with its active-share count or the completed state; its
reserved raw representation stays inside the encoding's owner. That state records whether shares
still count against ownership handoff; a message parked on materialized `REQUIRED WAIT` does not
keep a handoff blocked, and neither does a row a window retains until the window steps past it.
Both owners hold one park guard per retained message and drop it before they acknowledge, so the
acknowledgement removes the share the holder owns. A processor keeps a parked message's guard while
it re-evaluates the message and drops it only when the message leaves its park: a message whose
dependency is still absent returns to its queue with the same guard, so the re-evaluation publishes
no progress and makes no share active.
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

Brokered emitters keep each publish's pending answers in the shared `ConfirmationWindow`. When a
publish fails, the window polls every remaining answer once without waiting and settles the
confirmed and rejected records before returning the failure. Unanswered or failed answers keep
their records unresolved for the host's retry. The read uses
`nervix_primitives::task::unconstrained`: Tokio's exhausted cooperative budget can otherwise make an
already delivered channel answer appear pending. This pass is bounded by the configured `MAX`
window size.

The window's Shuttle checks exercise settlement racing broker answers, window admission, a
driver's capacity below MAX requesting the oldest answer on demand, and deadline expiry through
the production owner. Shuttle does not model Tokio's cooperative budget;
ordinary primitive and window tests deliver more answers than one budget permits and verify that
every given answer is settled. [Execution Primitives](execution-primitives.md) owns the adapter's
behavior in each mode, and [Connector Contract](connector-contract.md) owns the publish outcomes.

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

A local drain reads these counts together with the domain's relays, ingestors, generators,
acknowledgement roots and emitters, and work keeps moving between them while it reads. No single
order of reads follows every move. A node that takes a batch from a relay counts it before the relay
lets it go, so the relay has to be read first, while a relay counts a batch a node publishes before
the node lets it go, so the node has to be read first. Each relay therefore keeps two totals on
each node that only rise: the batches it admitted, from its owner's publishers or from deliveries
another node routed to its consumers here, and the batches it handed on. It holds their difference,
and the admitted total is also its admission sequence. A drain observation reads every relay's
sequence before and after everything else, and a relay whose sequence changed in between counts as
work still moving. A batch the observation missed reached the place where it rests only after the
read that would have counted it there, so it entered some relay between that relay's two readings.
The observation reads force-flush obligations first: a participant completes its obligation after
everything it did for the flush, including resuming a message parked on `REQUIRED WAIT`, so an
observation that sees the obligation complete sees that work as well. The totals cost a relay's
owner path the same two read-modify-writes per batch as the pending count they replace, a routed
delivery two more, and a drain reads them only on its own poll.

A broker ingestor's batch keeps its acknowledgement roots counted beyond their resolution. The
source host creates confirmed roots, and a confirmed root that resolves hands its domain and
ingestor counts to the completion that observes it instead of releasing them. The batch
acknowledgement drops the completion of every root that resolves while another is still
unresolved and keeps the completion of the last, which the source loop drops once `acknowledge` or
`reject` has returned. The hand-over is part of the root's one terminal transition, so a drain that
reads the counts by read-modify-write never finds them at zero between a batch's last resolution
and its transport's confirmation. An unresolved root keeps holding them itself, unless it waits only
on `REQUIRED WAIT` state, which a drain does not wait for, and a completion that dropped before its
root resolved has the counts released as the root resolves. See
[Confirmed acknowledgements](./connector-contract.md#confirmed-acknowledgements).

An independently settled transport, currently RabbitMQ, gives every delivery in a parallel batch
its own graph acknowledgement. The task-local `IndividualBatch` polls those waits concurrently;
a refusal does not cancel a sibling's wait or requeue a sibling that succeeded. Each completed
wait keeps its confirmation guard through its own transport ACK or NACK. The batch owns one
optional position per wait and takes it exactly once. If settlement loses the connection, only
the failed and still-pending positions enter recovery; positions already confirmed or requeued
are excluded. This state is task-owned, bounded by the requested batch, and adds no lock or
concurrent map.

Each delivery's intake group reads the immutable `domain_routings` publication once. The
[concurrent map inventory](https://github.com/nervix-io/nervix/blob/main/tests/concurrent-map-inventory-ledger.md) records that cadence;
drain, status and clock handles remain retained by the source host.

Transport failure does not drop the graph waits of admitted siblings. The same task retains them
to their outcomes before the source reconnects and replays the retained positions; those outcomes
cause no more writes on the failed transport. Shutdown still ends the waits and counts both
retained positions and unresolved graph waits as unconfirmed.

An ingestor route's force-flush participant releases its partial batches into branch execution;
the admitted client submission's tracked ACK root stays outstanding until its downstream work
settles. For a model alteration with a shared relay gate, the control plane suspends the affected
ingestors on every node and drains those roots before closing the relay gate. Otherwise a flushed
batch could wait behind the gate whose drain waits for that same root. Intake and full entity
holds overlap on the same quiesce control: releasing one `EntityHold` removes only its share, so
the remaining hold keeps admission suspended. The client admission Shuttle check exercises that
overlap with racing batch admission; the public buffered alteration scenarios exercise delivery
through local and remote relay owners.

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
raises an atomic in-flight count with a read-modify-write and then checks an atomic closed flag. An
engagement closes the gate and then reads the count with a read-modify-write as well.
Read-modify-writes of one count are totally ordered, so either the publisher's comes first and the
engagement counts it, or the engagement's comes first and the publisher's acquires it, observes the
gate closed, rolls its count back, and waits. A plain load of the count would not do: it may return
a count from before an increment that preceded it, while that publisher's own load of the flag still
finds the gate open. Leaving releases what a dispatch did under its permit to the engagement's next
read, and the last dispatch to leave a closed gate wakes the engagements waiting for it.

The engagement state is consulted only while the gate is closed. Once all earlier permits leave,
the engagement owns a lease and the protected mutation can proceed. The acquisition deadline bounds
waiting for that fence; a successfully acquired lease remains engaged until its owner releases or
drops it.

Forced ownership recovery closes the affected dispatch gates, then cancels changing routes'
preceding channel generations before waiting for their permits. Retirement retains that cancelled
generation until the swap publishes its new owner and consumer routes. Retained channels and
first-use bindings therefore both reject further deliveries through the preceding routes; cancelling
and immediately publishing a fresh generation would let an existing fan-out acquire a new channel
for a consumer it selected before the fence. Only relays selected by the typed forced-recovery plan
and whose routing actually changes are retired. Completed planned handoffs keep their ordinary
quiescence path. Remote admission uses its existing cancellation/reconciliation budget of five
seconds per interrupted attempt, rather than retaining a permit until the former owner resumes or
the entity gate deadline passes. Unconfirmed deliveries fail their record ACKs for source retry;
already admitted work retains its existing ACK and silence contracts.

The production channel-owner Shuttle check
`shuttle_forced_routing_fence_cancels_racing_bindings_until_successor_publication` races first-use
binding against retirement and requires every preceding slot to remain cancelled across successor
publication. The three-node `forced_failover_delivery` Cucumber scenario holds the former owner's
admission and runtime preparation, checks delivery and a later revision before releasing it, and
runs in the active, order and lifecycle-stress Deloxide selections. Those selections observe the
tracked gate, channel-ordering and ACK-owner locks; asynchronous cancellation, admission waits and
network behavior retain the complementary Shuttle, public-scenario and external Chaos evidence.

An owner buffer admits a batch before its fan-out begins. Fan-out therefore takes a nonwaiting
dispatch permit before it reads any local or remote runtime-consumer route and holds the permit
until fan-out has attached the consumer ACK shares and resolved the owner's share. A schedule swap
cannot replace the routes while that permit is live. If the gate has closed first, fan-out fails
the record ACKs and returns the batch for source retry. Waiting for the gate here would deadlock
the swap, because the swap's drain counts the
buffered batch. The same gate protocol orders the nonwaiting attempt and the swap engagement.

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
dispatched after a drain concluded that the ingestor held no admitted work. The admission tracks the
root with a read-modify-write, and the drain reads each root count with a read-modify-write after
the engagement published, so one side observes the other in the C11 memory model, not only on
processors whose read-modify-writes are full barriers.

### Assignment generations

A runtime-state assignment packs its generation and capability into one atomic word. A hot
operation increments the admission counter for that generation before comparing its token with the
published binding. A rebind serializes with other rebinds, publishes the successor binding, and
waits only for operations admitted under the generation it superseded. It reads that generation's
admission count with a read-modify-write, so an operation whose admission came first is waited for,
and one whose admission came later acquires the rebind's read and observes the successor binding. An
operation that finishes releases what it did to the rebind's read. Even and odd generation
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
- a live payload a paced ingestor or a source whose loop may be held handed over without an
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
  it holds no lock across that wait, and the synchronization runs on the storage workers. The round
  of a synchronization belongs to the storage job that flushes: it frees the runner slot and
  records its outcome when that job ends, so a writer that stops waiting neither frees the slot
  while the flush still runs nor loses a failed flush. Replica
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
- a Kafka consumer-group source's rejected-partition floors sit behind one lock per consumer, taken
  once per rejection, once per acknowledged batch while a floor is outstanding, and once per
  rebalance, never across an await or a driver call that waits on the broker; see
  [Kafka source rejection floors](#kafka-source-rejection-floors)

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
consumer queue is full. A waiting publisher raises a waiting count before it reads a consumer's
admission count, and a consumer lowers its admission count before it reads the waiting count. Both
reach the admission count by read-modify-write, so either the publisher reads the room the consumer
freed or the consumer reads the waiting count raised and wakes the publisher. Removing one consumer
does not stop the others, and changing capacity does not discard batches already queued.

A publication counts the consumers it delivers to from the same snapshot of the registered
consumers that it delivers through. A consumer that registers meanwhile receives later publications
only, so the acknowledgement shares a publication reserves for its attached consumers always match
its deliveries, and a routed delivery, which holds no dispatch permit, cannot resolve a share it
never reserved.

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

### Bounded Executor Queue Fence

The bounded executor retains one FIFO per worker class in `crates/execution/src/workers.rs`.
Submission, cancellation of an exact queued job, next-job selection and the last worker's idle
transition acquire that class's mutex. This is an execution-admission protocol keyed by one class,
with the configured pending and worker counts as its bounds. Each acquisition inserts, removes or
selects one entry. Callback execution, job destruction, memory and semaphore release, and result or
start notifications run outside the fence. A callback can submit work or read its executor without
re-entering a held queue lock.

An active worker drains admitted successors directly. Submission and returning the last active
place share the fence: either a worker sees the new job or the submitter claims a worker, with no
unobserved interval between deciding the queue is empty and publishing idle. Queued cancellation
removes the exact monotonic admission identity; after selection it raises the existing cancellation
signal and leaves the charge with the running job. The queue uses an ordinary ordered map under
this fence, registered in the concurrent-map ledger as a retained bounded admission protocol;
there is no recurring graph lookup. Interrupted-worker cleanup takes the queue under the fence
and destroys its entries after releasing it.

Deloxide tracks the fence in every diagnostic selection, including the registered management
responsiveness and materialized restore workloads. It does not track async semaphore waits,
oneshot delivery, simulated scheduling or the cancellation atomic. Existing cancellation Loom
models retain that publication claim. The execution Shuttle checks retain ordering, exact
backpressure, immediate queued charge release and running charge ownership, and
`shuttle_submission_racing_worker_exit_never_strands_an_admitted_job` explores the submit/drain
boundary through the production pool. The ordinary owner regression drains a backlog while its
submitter futures remain unpolled, and the panic regression requires a successor to finish after its
predecessor reports `JobPanicked`.

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
recurring or names a bounded protocol. A type contract applies to its implementations. An `extern`
block holds no contract, so a foreign function or static takes those of the module that holds its
block, in its own crate and through crate metadata alike. Duplicate,
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

The MySQL insert builder carries a dispatch contract for the vocabulary's opaque iterator over
at most two immutable table-name parts, with at most 128 bytes across them. The boundary is
borrowed string traversal; local SQL quoting callbacks retain their recurring-execution checks.
Statement construction reaches no internal runtime acquisition.

Local iteration and polling implementations remain call edges. Generated `await` polling is the
Rust scheduling protocol, rather than a separate application callback diagnostic. Synchronization
inside external libraries, opaque external future implementations and unsupported whole-program
relationships remain outside the recognizer's claim. The checker supplies no universal ownership
or effect proof.

### Resolved architecture boundaries

`nervix::layer(kind, reason = "...")` states the exact documented crate/module, type, trait, impl
or callable owner: primitives, vocabulary, language, engine, decisions, data_plane, control_plane,
edge or harness. The narrowest contract wins; associated implementations inherit explicit trait metadata
and impl-self contracts. Contracts survive compiler metadata and move with their source owner.
An unrelated trait module default cannot replace an implementation body's lexical owner.
An engine that receives validated plans also declares `nervix::input(plans, ...)`; general
infrastructure is not presumed to execute graph plans.

`nervix::value(model|name|cluster_access, ...)` classifies exact semantic ADTs or traits. The current
configuration Model union, shared name vocabulary and cluster observation capability carry those
roles. `nervix::model_in_data_plane` rejects the configuration union in data-plane and plan-consuming
interfaces or bodies, including normalized generic containers, aliases, constructors and patterns.
An ordinary user type called Model has its own meaning. These checks do not treat every vocabulary
field or schema descriptor as a configuration Model.

`nervix::parser_dependency` diagnoses resolved language definitions outside the language, edges and
harnesses, including imports, type/trait references, qualified calls and methods. A separate
source/manifest validator retains the owning crate's import path before cfg selection, including
renamed dependencies, inactive module imports and authored macro tokens. It derives language
packages and consumer ownership from source contracts and Cargo/module declarations. Approved plain
primitive and vocabulary re-exports keep their actual semantic owner; importing from a language
crate still requires an allowed provenance owner even if resolution reaches a plain value.

The complete [architecture rule matrix](architecture-enforcement.md) separates these resolved
contracts from compiler-independent source properties. Integer/Duration clamping and
Option/Result control-flow diagnostics use the same source-local exception machinery and legal
configuration coverage; they do not classify a custom method by its name. Their serial analysis
does not supply concurrency execution evidence. Explicit Result branches preserve the same
reports and delivery outcomes when a caller expands an adapter sequence.

`nervix::cluster_await` diagnoses future-returning access on the classified cluster capability or
trait when awaited from recurring data-plane execution. It follows the existing supported local
helper/callback graph, selected concrete methods, UFCS, async closures, awaited expression wrappers
and simple local future-binding chains. A local I/O await has no cluster role. A lifecycle cluster
read remains lifecycle work. Dynamic/generic application dispatch needs its existing source effect
contract; opaque external future implementations, arbitrary stored-future relationships and
whole-program effects are not inferred. Execution graphs and recurring reachability are conservative
within the declared local graph; annotations are reviewed architectural assertions.

`nervix::node_identity`, `nervix::installed_state` and `nervix::state_contract` implement the exact
identity and representation contracts in [Typed States](./typed-states.md#source-owned-identity-and-installation-contracts).
All six architecture diagnostics use the same normal lint levels, exact-operation expectations and
complete current configuration evidence as synchronization and failure rules. No count, product
owner catalog or source-review hash grants permission.

### Diagnostics and reviewed exceptions

`nervix::sync_acquisition` diagnoses an acquisition on ordinary recurring execution.
`nervix::unknown_effect` diagnoses an acquisition without a context or an unresolved application
call reached from recurring execution. `nervix::invalid_contract` rejects contract and exception
errors. The lints use ordinary Rust warn/deny/expect levels; the required gate also rejects unresolved Nervix warnings.

The driver also emits the resolved failure-contract, outcome-discard and panic diagnostics
described in [Errors And Diagnostics](errors-and-diagnostics.md). Their exact error/return
classifications use `nervix::error_boundary`; execution-context inheritance does not classify
an error or exempt a failure. All diagnostics share the same one-operation expectation checks
and complete configuration evidence.

Retained repair debt names its owning task at the exact operation:

```rust,ignore
let state = nervix_primitives::expect_lint!(nervix::sync_acquisition,
    "Typed Ratchet 15 (86bca1web): retain the replica entity before frame handling",
    states.get(&entity));
```

`expect_lint!` puts a normal reason-bearing Rust expectation on one binding, evaluates the
operation once and returns its value. It supplies stable syntax and introduces no runtime policy,
allocation or lock. A direct gated expectation on one operation binding works too. A lifecycle
call admitted only during first installation or terminal teardown explains that phase and its
concrete lifetime in the same narrow form. Blanket allow, undocumented or broad expectations,
lint caps and unfulfilled expectations fail. The checker counts distinct HIR operations against
Rust's effective expectation identity, including macro expansion; widening a binding to cover a
second operation fails even when Rust would regard the expectation as fulfilled.

Task handles remove recurring status, freeze, metric and checkpoint lookups. Source readiness,
relay channels, buffered error delivery, domain selection and transport leasing use retained handles
or immutable publications. Materialized writers own their mutable branch records and readers
retain per-relay publications. Remote acknowledgement rows and admission waiters use fixed routing
positions with exact generations; authenticated connections retain bounded peer protocol owners.
Replica catch-up retains the entity's
lifecycle handle, assignment slot and its own record of each branch. Replication frames,
synchronization and listing requests select published state handles; announcers retain their route
and assignment slot. Their runtime registries handle installation, replacement and teardown.
A session's clock delivery retains the observer its attach resolved and reads only that observer's
lifecycle publication and progress watch. Accepted progress reaches the runtime's domain registry
once per report and then compares and replaces inside the watch, and an attach waits on the
committed-domain installation count rather than returning to the registry.
Bounded retained placement progress remains explicitly documented. Testing fault selectors name
their exact emitter, ingestor, domain/branch, checkpoint window or acknowledgement link and finite
read/removal steps; they release map guards before a scenario-controlled pause. These contracts
describe test selection, not a product wait deadline. The executor-saturation lookup belongs to
testing fault control. The [concurrent map inventory](https://github.com/nervix-io/nervix/blob/main/tests/concurrent-map-inventory-ledger.md)
records the runtime ownership review; source contracts and expectations are the executable policy.

### Complete analysis and qualification

The isolated tooling workspace uses `nightly-2026-09-17`; product builds use stable. The matrix
analyzes ordinary workspace libraries/binaries, the browser's wasm32 library and binary, the server's testing capability,
server/interconnect/primitives under Shuttle, server/consensus/primitives under Loom,
interconnect/primitives under Turmoil, and the server's library and binary, the deadlock
diagnostics and primitives under Deloxide, where every blocking acquisition resolves to a tracked
adapter that the rules recognize as the lock it wraps. Turmoil's runtime cfg comes only from its just recipe,
which preserves the parent's diagnostic mode. Compilation supplies no concurrency execution
evidence. Test targets and undeclared feature combinations are not claimed.
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
interrupted output, and two worktrees. Cache qualification keeps its verdict outside native
coverage. The instrumented compiler-fixture producer includes its paired fixture doctests;
product API doctests and modeled fixture checks finish outside instrumentation. Native coverage
uses the matching LLVM tools and records the driver executions that ran. Sanitizer campaigns follow
the shared Bolero label policy.
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

Installation-only fields declare their lifetime at the owner. The resolved installed-state rule
rejects replaceable read-write locks in that representation, while an optional name/shared handle
with an unstated lifetime requires explicit review. The current publication or one-time primitive
must express the actual owner; type shape alone cannot establish a write-once lifetime.

## Primitive Selection

[Execution Primitives](./execution-primitives.md) owns the shared primitive surface, its complete
per-mode matrix, feature and target selection, authored-source scope, and unmodeled, blocking-work
and thread-creation permissions. Every synchronization owner in this chapter obtains its
primitives there. The boundary selects mechanisms; the source contracts and production-owner
checks below establish what a caller may do with them.

## Diagnostic Deadlock Detection

The resource-source-discovery workload exercises resource publication, a pending replica and
installation once source visibility resumes, followed by a lookup of the replicated file. Its
active, order and stressed records name the consensus-storage and cluster lifecycle owners it
reaches. The scenario's command-pause map, watch observation and source filter are test-only and
outside Deloxide's tracked locks; they establish the public interleaving directly. No data-plane
lock, memory-ordering protocol or production wait-and-notify mechanism changes.

The resource-upload-publication workload reaches the recovery caller on one and three nodes: it
refuses publication after installation, lets reconciliation read the matching local manifest and
publish that copy, and observes the original upload complete with one version. Its active, order
and stressed records reach the same consensus-storage and cluster owners. The scenario's refusal
map and watch remain outside tracked-lock detection; the public assertions establish that recovery
took up the refused upload before the client's retry could supply the archive again.

The backup cut diagnostics scenario exercises capture-budget expiry and resumption on one and
three nodes in the active, order and stressed-restore Deloxide selections. The coordinator's phase
measurements are task-owned administrative state and add no synchronization. The existing capture
and cleanup paths retain their tracked-lock coverage; phase timing itself establishes no
interleaving or memory-ordering guarantee. [Backup And Restore Architecture](backup-and-restore-architecture.md#observability)
owns the diagnostic contract.

The test-only backup cut registry releases its watch-held lease and capture gates before clearing
the map during scenario cleanup. Existing borrowers observe release; later cuts encounter no
gate. Gate-owner tests with a paused clock cover waiting cuts, timed holds and later arrivals.
The supervised failed-body Cucumber fixture reaches both public boundaries on one and three nodes
and requires every node to stop while every body retains its failure. These watch channels and
concurrent-map accesses are outside Deloxide's tracked locks. This harness cleanup adds no
data-plane synchronization or memory-ordering claim; [Integration Test Lifecycle](./integration-test-lifecycle.md#what-cleanup-releases)
owns its supervision verdict.

A diagnostic node selects the `deloxide` mode. The `deloxide-order` feature adds historical order
instrumentation to that same mode, and the `deloxide-stress` feature adds Deloxide's bounded
scheduling disturbance to its active-only selection; neither is another execution mode, and the two
cannot be enabled together. Ordinary builds keep their primitive reexports and contain no detector.
Build the diagnostic node with `just build-diagnostic-server [deloxide|deloxide-order|deloxide-stress]`;
its target directory is separate from the ordinary server.

| Selection recorded in evidence | Compiled order instrumentation | Runtime order checking |
| --- | --- | --- |
| `ActiveOnly` (`deloxide`) | absent | disabled |
| `OrderAnalysis` (`deloxide-order`) | present | enabled |
| `OrderInstrumentedActiveOnly` (`deloxide-order --deadlock-active-only`) | present | disabled |
| `StressedActiveOnly` (`deloxide-stress`), with its stress configuration | absent | disabled |

Runtime disabling does not recover an ordinary or active-only fast path. The pinned Deloxide
1.1.0 graph build enters its global detector even on uncontended mutex and write acquisitions.
Reads enter that detector in either diagnostic build. The order build also tracks actual guard
lifetimes locally, including when runtime checking is disabled, so active self waits remain
observable. No timing from these builds is ordinary product performance evidence.

**Measured cost.** A same-host comparison on Nervix's Kafka filter-map workload measured the
diagnostic selections against the ordinary build. These are results for that workload and host,
not a throughput or latency guarantee for other graphs.

| Selection | Observation against the ordinary build |
| --- | --- |
| `ActiveOnly` | Throughput and delivery latency matched ordinary execution; peak memory was 15% higher. |
| `OrderAnalysis` | Throughput was about 41% of ordinary; its delivery latency followed the resulting backlog. |
| `OrderInstrumentedActiveOnly` | Throughput was about 58% of ordinary even with runtime order checking disabled. |
| `StressedActiveOnly` | Throughput was about 39% of ordinary. |

Clean processes produced 124 to 156 bytes of evidence each, with no added logging; Deloxide's
logging-and-visualization feature is not compiled. The external chaos suite runs well below this
workload's throughput. Its measured diagnostic-image run and recovery times were within a few
percent of the release image's, which qualifies the chaos scenarios and their evidence, not the
diagnostic image's production throughput. An engineer comparing a different workload records the
ordinary baseline, active-only mode, order mode with checking enabled and disabled, and stress
mode separately on the same host.

**Scheduling stress.** A `deloxide-stress` build compiles Deloxide's random preemption into its
tracked locks. A thread that already holds a tracked lock and asks for another sleeps first with a
fixed probability, for a delay drawn between two bounds, and a configuration may also yield a thread
after every release.
An active cycle needs two threads that each hold what the other wants at the same moment; widening
the window between one nested acquisition and the next makes the lifecycle paths that take nested
locks meet that moment far more often than an idle schedule does. Disturbance never decides a
finding: the detector reports the same cycles it would report without it.

`StressConfiguration` is the typed configuration, recorded with the selection in every process's
evidence. It is bounded by construction: a probability in millionths of at most one, delays of whole
microseconds between 1µs and 2ms, the shortest no longer than the longest. The lane's configuration,
`StressConfiguration::LANE`, preempts one in twenty nested acquisitions for 20µs to 200µs and does
not yield after releases. The longest delay holds one runtime worker for a small fraction of the
shortest physical product deadline, and the product's deadlines are not widened for it. A first
configuration, a quarter of nested acquisitions for 50µs to 1ms with a yield after every release,
made the stressed nodes miss their startup and request deadlines; so did one in twenty without the
yield while four scenarios shared one process, which a plain diagnostic build of the same scenarios
met. The cost is not the delays alone: a stress build enters Deloxide's process-wide detector twice
on every mutex and write acquisition, once to decide the delay and once to acquire, so every node of
every scenario in one process contends on that detector. The stressed scenarios therefore run one
at a time. Deloxide
draws its preemptions and delays from entropy it seeds itself, so a configuration reproduces the
distribution of the disturbance, not one schedule; a replay repeats the workload and the
configuration.

Deloxide applies the disturbance only inside its blocking acquisitions, and its stress build
disables its own optimistic mutex and writer fast paths. The tracked adapters therefore skip their
own immediate try in this build and enter Deloxide's blocking acquisition directly, registering the
waiting site first. Deloxide keeps its reader fast path, so a shared acquisition that finds no writer
is not delayed. Because every mutex and write acquisition then reaches Deloxide's global detector,
its held set is populated as in an order build, and the adapters keep the caller's live guards so a
self wait is still reported. Deloxide's component-based strategy is not offered: it appends every
nested acquisition to a list it never trims and scans that list on each one, so its memory and its
cost per acquisition grow with the life of the process. The random strategy keeps one preemption
counter per lock it delayed, which grows with the locks a process constructs and is bounded by the
process's life within its invocation bound.

The stress selections run every active-only probe, conformance check and owner test again, and
between them every scenario of the active-only selection but the report tool's triage, which drives
no lifecycle path. `deloxide-stress` runs the lifecycle scenarios tagged `@deloxide_stress` and the
paced-driver scenarios with the stressed Rust driver; `deloxide-stress-restore` compiles the same
build under its own name and runs its probes and the capture, restore and checkpoint-transfer
scenarios tagged `@deloxide_stress_restore`, in a lane of its own because the stressed scenarios of
both, one at a time, would take most of one lane's budget. The `probe.stress-disturbance` probe proves the
disturbance is applied in each: twenty thousand nested acquisitions must wait at least the shortest
delay for half the preemptions the configuration expects, and record the lane's configuration. Its first run found that the adapters'
immediate try bypassed the disturbance entirely, and that a self wait went unreported once
Deloxide's held set was populated; both are fixed in the adapters, and the probe and the stress
selection's self-wait probes are their regressions.

**The tracked surface.** Every `sync::blocking` mutex, read-write lock and condition variable is an
adapter over Deloxide. The adapters retain `new`, `lock`, `try_lock`, `read`, `write`, `try_read`,
`try_write`, `get_mut`, `into_inner`, `Default`, `From` and guard dereferencing. Locks are not
poisoned; `Debug` tries the lock and never waits. Timed, upgradable, mapped, fair and reentrant
acquisitions are unavailable because this backend cannot track their contract. Constructors and
successful guards retain caller locations; an acquisition that waits separately registers its
waiting site and requested access. The compiler synchronization gate classifies these as the
acquisitions they wrap.

A condition variable keeps the Shuttle adapter's `wait` and `notify_all` interface and an exact
count of the waiters in each notification generation. Waiting removes the caller guard's held
lease while the lock is released, and reinstalls it on reacquisition. Its internal state guard's
lease also leaves while the vendor condition variable releases that lock. A notified waiter
reacquires the caller's lock through its tracked adapter, so it can participate in an active cycle.

**Installation and active findings.** The process installs once, after its signal registration and
before any tracked lock or runtime worker, through `nervix-deadlock::DiagnosticRun`. The server's
`main`, the diagnostic scenario binary and the Rust paced driver own that placement. A tracked constructor before
installation panics with a configuration error; a second installation fails. Installation keeps
the vendor banner off standard output by temporarily redirecting that descriptor to `/dev/null`.

A `WaitForGraph` finding describes threads currently waiting in a cycle. A self wait is also an
active cycle: the mutex adapter checks a failed acquisition against its calling thread's actual
live exclusive guard in an order-instrumented or stressed build and sends that one-thread cycle
through the same bounded handoff. This establishes a real incompatible owner/wait pair; it does not infer an
active cycle from historical order. Deloxide 1.1.0's common-held-lock filter otherwise discards this
case once instrumentation or stress populates its held set. Recursive mutex acquisition and an
upgrade from one's own read guard remain disposable-process regressions in every selection.

**Historical order and source evidence.** A `LockOrderViolation` is a `PotentialCycle`, never an
active outage. For example, a thread can successfully take A then B, release both, and successfully
take B then A. The run retains the potential cycle and continues the workload. The pinned upstream
graph adds requested mutex and write acquisitions; it does not add requested read edges. Opposite
read-only orders complete without an order finding. A read guard held before a requested mutex or
write still contributes a historical edge, with its shared mode and multiplicity retained. This
limit also excludes some dangerous mixed orders: absence of a finding is not coverage of every
reader/writer combination.

The boundary retains directed edges between run-local lock instances, not between construction
lines. Each edge carries construction sites, `Live`, `Ended` or `Unrecorded` lock lifetime at
correlation, and source witnesses with run-local thread identity, optional name, held and requested
sites and modes, attempt count, and the number of identical live guards at the held site. Thread
names and construction sites copied into history survive registry removal. Lifetime observations
are not an atomic snapshot of all locks. Missing context is explicit. Source attempts include
unsuccessful nonblocking calls; only upstream successful acquisitions decide that a historical
cycle exists. Attempts are not claimed as successful-acquisition counts.

A complete cycle has no repeated closing node and rotates to its smallest lock identity. Witness
sets sort by source context and reject duplicates. Deduplication keys on the directed instance
cycle within one process, merges distinct source/mode witnesses, takes the maximum cumulative
attempt count for each context, and counts delivered callbacks separately. It preserves direction,
lock instances, known construction, ended lifetimes and guard multiplicity. New source or lifetime
context invalidates an existing review. It never equates instances merely because a constructor
line was reused.

**Bounds and handoff.** The live registry owns the live lock, waiting attempt and thread records;
none of its shard guards cross a tracked acquisition. Order instrumentation adds a run-bounded
history of at most 65,536 directed instance edges, 1,024 source witnesses per edge, and 64 guard
leases per thread. Ending a lock does not reclaim the historical edge budget, so a lock that lives
for one request and is taken under the lock of an owner that outlives it spends one edge per
request: relay admission records under their peer owner leave more than 13,000 such edges in the
three-node 40-relay materialized restore with a leadership handoff, and more when its relay
deliveries retry. A process regression keeps 32,768 per-request edges under one owner within the
history, and the edge-limit regression takes one more per-request lock than it keeps. Refused
history or count overflow becomes an explicit overload finding. The witness budget retains distinct storage
worker identities across checkpoint staging. Its process regression retains all 512 worker
contexts in one historical edge; a separate 1,025-context workload must record order-history
overload and fail qualification. The independent edge and held-guard limits remain fatal too.

The vendor dispatcher has an unbounded channel and swallows callback panics. The boundary callback
therefore only retains bounded identity vectors, submits to its lock-free `ReportHandoff` of 64
reports, counts refusals, and unparks the findings thread. Each queued active report retains at
most 64 threads/waits; an order report retains at most 65 identities to describe a 64-edge prefix.
The findings thread correlates source context and calls the emitter serially. Missing order payloads
are loss, not a clean run. Emitter panic aborts. Installation failure closes the handoff. A periodic
wake also delivers history overload that produced no upstream cycle.

The history, handoff, output and artifact limits are Nervix retention limits. They do not bound the
vendor graph, its cache, its pre-callback allocation or its dispatcher backlog. No exact upstream
flush barrier is available: evidence is an observation at the time it is read, and a process can
end with upstream callbacks pending. Deloxide 07 owns dependency and upstream graph/dispatcher
hardening; full diagnostic workloads retain the actual observed bounds and gaps.

**Recording and exit policy.** `--deadlock-evidence` (`NERVIX_DEADLOCK_EVIDENCE`) selects an existing
local directory; without it the run uses standard error. The run first records empty evidence and
atomically replaces its own file after each finding. The current header is `NVXDLEVD`, evidence
kind 1, format version 3; unsupported versions fail clearly. The artifact holds at most 16 records,
with one slot reserved for retention loss after at most 15 distinct potential cycles. Each cycle
retains at most 64 edges and each text at most 512 UTF-8 bytes, remembering truncation. An artifact
is at most 128 MiB. Descriptions have a 128 MiB per-process budget; repetitions with unchanged source
context update the artifact without repeating the description. Changed context stays visible.
Exceeding a bound retains loss and cannot qualify.

An active cycle ends the process with status `3` after recording. Potential order continues with
unreviewed evidence. Handoff/history/retention overload, failed recording or output, or a recording
operation exceeding its ten-second budget ends with status `4`. The deadline is cancelled after a
successful potential recording; no detached deadline later kills a continuing workload. On expiry
it exits directly: a diagnostic write to a blocked descriptor must not delay the deadline itself.
Exit bypasses handlers, as for a [forced exit](./shutdown.md#exit-status). Ordinary logging,
visualization and browser startup are unnecessary. Nothing uploads evidence, logs protected values,
or uses source/lock identities as metric labels.

**Local inspection and triage.** The ordinary `nervix-deadlock-report` tool needs no detector or
running node. Select and export complete current values, inspect the original indexes, and write a
reviewed copy of the original process evidence:

```bash
just deadlock-report inspect /var/tmp/nervix-deadlocks/deadlock-PID-TIME.rkyv --source potential --export /var/tmp/potential.rkyv
just deadlock-report triage /var/tmp/nervix-deadlocks/deadlock-PID-TIME.rkyv --finding 0 --basis non-overlap --reason 'The owning operation serializes both paths before either lock is acquired.' --regression 'owner::tests::serialized_operations; retained run artifact' --output /var/tmp/reviewed.rkyv
just deadlock-report qualify /var/tmp/reviewed.rkyv
```

The placeholder artifact and regression names must be replaced with the observed run and a real
retained regression. Review uses `correction`, `non-overlap`, `shared-readers` or `lifecycle` and
requires a nonempty, untruncated reason and regression reference. A reader proof checks every
recorded mode is shared; other proofs are explicit engineering assertions, not machine verification
of a referenced test. A correction cites the fixed owner and the subsequent regression run. A
non-overlap proof names the enforced serialization; a lifecycle proof establishes disjoint
instance/operation lifetimes. A source line alone proves none of them. Triage keeps the complete
original cycle. Missing/truncated context or active findings cannot receive a potential proof.

Qualification of whole-process evidence fails on any active, lost, incomplete or unreviewed
finding. A selected export retains its selection scope and cannot qualify its source process,
even when the filter yielded no findings. Loss always remains visible through a selection. The
`qualify` command exits `5` for nonqualifying evidence and `4` for an invalid artifact or operation.
It qualifies recorded evidence; the engineer also verifies workload completion and successful
checks, the run selection, pending-callback limit, revision and declared coverage. Keep reviewed
copies separate from files a live recorder replaces. The operator's workload is not ended merely
because an unreviewed historical cycle exists.

**The diagnostic lane.** `just test-deloxide`, `just test-deloxide-order`,
`just test-deloxide-stress` and `just test-deloxide-stress-restore` run the lane of each selection,
and CI's `deloxide` job runs all four side by side for every pull request labeled `deloxide`, the
label each change the Deloxide rule applies to carries. A selection is named after the diagnostic
build feature it compiles, or extends that name and declares the feature when it runs workloads of
the same build in a lane, budget and CI runner of their own; a
build records one diagnostic selection in its evidence whichever selection runs it. An invocation
runs in every selection unless the inventory names the selections that run it. The stress selection
runs every probe, conformance check and owner test of the active-only selection, its
`stress-scenarios` invocation of the one- and three-node lifecycle scenarios tagged
`@deloxide_stress`, which drive client contract transactions and their rejected rollbacks, the
drain of buffered branches before an alteration, interleaved branches with remote acknowledgement
owners, a lost acknowledgement and a producer restart, session restarts that resume a producer and
consumer, a domain restart that replaces its consumer, a memory-pressure pause of scheduled
ingestors, an Iceberg commit that waits for room in the bulk memory class, a Roto function's
column builder, TLS rebinding of every HTTPS listener, batched inference over interleaved branches,
a restarted voter's ownership, and graceful shutdown with a slow cordon release and diagnostic
server processes that stop gracefully, and its
`stress-paced-simulation` invocation of the paced-driver scenarios with the stressed Rust driver.
Endpoint ACK lifecycle examples carry `@deloxide_stress_endpoint_ack` and run in the separate
`stress-endpoint-acknowledgement` invocation. Its serial process has a 900-second bound, including
the existing 360-second teardown reserve. The baseline lifecycle process retains its 1,800-second
bound, and every invocation shares the selection's unchanged 4,800-second workload budget. The
inventory checks require the baseline and endpoint groups to be disjoint and account for every
registered example; this partition changes no product deadline or ordinary scenario concurrency.
The six transaction-retention examples carry `@deloxide_stress_transaction_retention` in the
separate serial `stress-transaction-retention` invocation, with the same 900-second process bound
and 360-second teardown reserve. This phase runs before the baseline lifecycle process and shares
the selection workload budget. Inventory checks cover all three disjoint lifecycle groups and
their complete correspondence with the active-only workloads.
The restore selection runs the build's probes and its `stress-restore-scenarios` invocation of the
scenarios tagged `@deloxide_stress_restore`: backup capture and restore of WASM branches, Kafka
offsets, deduplicator keys and windows, materialized relays and native metadata, complete restore
generations, staging quota and reclamation, delayed restore coordinators, bounded multi-domain
backup waits and their recovery, and the drain of a large-state WASM owner through bulk checkpoint
streams. Every stressed scenario runs one at a time under its own `stress.` identity, with the
scenario, examples and invariant of its active-only counterpart and the owners that counterpart
reaches.
`tests/deloxide-inventory.toml` is the lane's bounded inventory. It registers every workload a
selection runs, each with a stable identity, the invariant it owns, the selections that must run it
and the coverage it declares: the disposable-process probes of `nervix-deadlock`, the tracked
locks' conformance checks of `nervix-primitives`, the diagnostic owner tests of the server library,
and the tagged scenarios with the number of example runs each must make. The three
`@remote_ack_owners` scenarios add four example runs across interleaved branches, a lost
acknowledgement and producer restart; every selection exercises the tracked correlation and peer
admission owner locks. The `@shutdown_kafka_handoff` scenario runs in every selection and reaches
the ingestor quiesce control's tracked locks while a stopping node closes intake, waits for its
attached emitter's handoff, and finishes admitted Kafka acknowledgements. The async gate wait,
remote delivery and scheduling order remain outside the detector's observation and have the
Cucumber and chaos checks in [Shutdown And Recovery](./shutdown.md). The
`@shutdown_leader_change` scenario also runs in every selection and reaches the tracked entity-gate
state while a stopping follower repeats the remaining handoff through a new leader. Its leadership
observation, transport waits and shared shutdown deadline remain outside the detector's observation.
The `@shutdown_terminating_destination` and `@shutdown_unactivated_handoff` scenarios run in every
selection too. The first reaches the tracked entity-gate state while a stopping leader engages and
releases the gates of moves that a terminating peer refuses to prepare; the second reaches it while
a node's runtime teardown ends the lease a failed activation left held and waits for the lease's
task. Their gossip hold, activation pause and transport waits remain outside the detector's
observation. All four scenarios carry `@deloxide_stress`, with separate stress workload
registrations that reach their owners under bounded scheduling disturbance. The inventory also bounds the lane: separate
real-time budgets for a selection's diagnostic builds and workloads after its prerequisites, a bound
for every invocation, a stop
grace period, the reserve the scenario binary keeps for its own teardown inside its invocation's
bound, and how many scenarios run at once, which is the same on every machine.

The fault harness shares its observation and release barriers through
`src/fault_injection/pause.rs`. The barrier owns watch-backed reached, released and delivered
state and the one-shot atomic claim; fault selection remains in the harness's owning modules.
These async observations introduce no tracked blocking-lock owner. Deloxide continues to measure
the registered product locks, while the public scenarios and model checks exercise pause ordering
at their applicable boundaries.

The `@restore_client_inspection` one- and three-node examples fill the real filesystem execution
class before an accepted restore applies its client configuration, then release capacity and require
every restore step and the configured client to survive. Their active, order and restore-stress
registrations reach the class-local FIFO admission and drain fence in `crates/execution/src/workers.rs`.
That owner record names both workload identities. The asynchronous memory and queue-capacity waits,
inspection observation and parked test workers remain outside the detector; the owner unit tests
and public scenarios retain their admission, cancellation and completion assertions.

The `@client_wire_standalone_preparation` scenario holds a coordinator after its standalone
transaction lookup and after preflight, while two leadership transfers let another coordinator
apply the same command and engage its domain pause. Its active, order-analysis and stressed
registrations reach the transaction impact recorder's tracked locks. The lookup, preflight and
resume barriers use the existing async pause owner, whose ordering remains outside Deloxide's
observation; the public outcome and dependent mutation assert recovery under the original
execution reference.

The registered `@restarted_voter_observation` workload reaches whole-cluster restart, concurrent live-policy discovery and automatic scheduling when the leader first hears a voter through a relayed heartbeat. Its test-only reconciliation gate releases after leadership is established; that gate, the watch channels and the concurrent fault map are untracked. The registered native metadata restore workload exercises distributed Kafka checkpoint replication, acknowledgement, restart and exact restoration above the resident replication message budget; its stream waits and assignment atomics retain their complementary checks.

The lane builds all selected executables and diagnostic drivers before starting its workload clock.
For each prepared invocation it lists what the build holds or reads
the tagged scenarios from the feature files, and refuses a registered workload that the build no
longer holds or holds ignored, a discovered probe, conformance check, owner test or tagged scenario
that is not registered, a scenario whose example runs changed, and an invocation that selects
nothing. It reports how many workloads it discovered, selected, executed and saw complete. Global
detector configurations never share a process: each probe workload is a disposable child, each
conformance check and each owner test installs its detector in a fresh process, and each scenario
binary process installs once in `main`. Active detection runs its tagged scenarios together; order
analysis starts a fresh process per feature and gives the six large materialized restore and four
complete-generation restore examples their own processes. These restores each start several nodes
and carry large state payloads, so per-example order histories stay within the detector and host
budgets. Their `order_tags` in the inventory select and account for every example. Every diagnostic
server and paced driver process installs its own detector. Each selection compiles one diagnostic
build, `deloxide-stress-restore` the stress selection's under its own name, and the diagnostic
builds stay apart from ordinary, fuzz, Loom, Shuttle and Turmoil builds.

The conformance checks are the tracked adapters' own: which locks a diagnostic build selects, that
`Debug` formatting tries a lock and never waits for it, that a value moves in and out of a lock, the
exact count of waiters a notification wakes, the refusal of a second installation, the registry's
record of a thread across its exit, and a tracked wait that proceeds once its lock is released.
`nervix-primitives` sits below `nervix-deadlock`, which owns evidence files, so a conformance check
records none. Its detector's emitter panics on any finding and a panicking emitter aborts the process, so
the lane ends such a check as `signaled`, with the finding in its retained output.
`just test-primitives-deloxide` runs the same checks in one process for each selection, beside the
other modes' conformance.

Stress widens timing windows; it does not make untracked waits visible. A Tokio mutex, channel or
`Notify` wait, a DashMap shard, dependency locks, an atomic publication and a wait on another node
are invisible to the detector with or without disturbance, and a workload that stalls on one of them
ends as `timed-out`, not as an active cycle. Those owners keep their Shuttle, Loom, Turmoil and
Chaos evidence. A dependency's own channels are invisible in the same way: closing a database waits
for the storage engine's background workers through the engine's worker channel and active-worker
count, with no tracked lock involved, so that close is held by the pinned engine's own worker-stop
tests and the store-close regression beside `ApplicationStartup::close_stores`, not by the lane. A
stressed workload that fails is reproduced first as a focused failing test against its owner before
the owner changes.

Every process the lane starts runs in a session of its own. A process that outlives its bound
receives `SIGTERM` and, after the stop grace period, `SIGKILL` with its whole group. The lane is a
child subreaper, so a process that left its group is found too, and anything still running after an
invocation ends is killed and fails the lane. The lane names each ending:

| Ending | Lane class | Lane status |
| --- | --- | --- |
| A process reported an active cycle, or a failed invocation's evidence records one | `active-deadlock` | `3` |
| Recording or output failed, findings or context were lost, or the recording budget expired | `diagnostic-failure` | `4` |
| A process outlived its bound, or the lane's budget expired | `timed-out`, `budget-expired` | `124` |
| A signal ended a process | `signaled` | `1` |
| A test or step failed | `failed` | `1` |
| A process was left running | `leftover-processes` | `1` |
| A registered workload did not run or ran ignored, or the scenario count differs | `incomplete` | `1` |
| Evidence is missing, partly written or does not qualify | `evidence-missing`, `evidence-partial`, `evidence-unqualified` | `1` |
| The inventory, a build or a prerequisite is wrong | `inventory`, `build-failed`, `prerequisite-missing` | `1` |

A failed invocation's evidence is qualified before the lane reports it, so a node that deadlocked
inside a scenario is reported as the active cycle it recorded rather than as the step it failed.
After each invocation that records evidence, every process observation must be present, completely
written and qualify. `nervix-deadlock-report qualify` prints one `evidence summary:` line of counts
per file, and the lane reports repeated deliveries of a retained potential cycle apart from findings
lost to overload. The first failure ends the lane.

A potential cycle fails the lane. The lane qualifies only the current run's whole-process evidence,
and it keeps no ledger that approves a recurring cycle: lock identities are run-local, and a source
line never stands in for a lock instance. A correction removes the order from the exercised
workload, and the lane is its regression. A reviewed infeasibility proof qualifies the retained
evidence of the run it reviews, for the owner's investigation and record; it never qualifies a
later run.

Each run keeps a fresh attempt directory below `<build target>/test-deloxide/<selection>/`, which no
later run reuses or removes: every log, the probes' artifacts, every process's evidence, the
description of every finding with its source sites, and `lane.json`. The record holds the revision
and whether the tree was modified, the toolchain, Cargo and Deloxide's locked version and source, the
inventory's digest, the selection, the features of every build and the bounds, and for every launch
its exact command, working directory, environment additions, bound, ending, duration, discovery and
accounting, with the qualification of every evidence file. All selected test executables and
diagnostic drivers build in a bounded stage before the workload stage starts its own clock. Each
stage records its budget, start, verdict and duration; a failed build starts no workload. A
`just build-deloxide-lane <selection>` measurement records `built` and cannot qualify as a completed
lane. `just test-deloxide-replay <lane.json>
<launch>` runs one recorded launch again, exactly, in a fresh attempt. OS schedules and stress timing
are not recorded, so a replay can take another interleaving; deterministic reproduction belongs to
Shuttle and Loom.

`just test-deloxide-qualification` proves the supervision on the probe binary's real processes,
through the lane's own launch, evidence and classification code. A two-thread deadlock, an evidence
directory that cannot be written, an untracked wait that never ends, an aborted process and, in the
order selection, an unreviewed serial inversion and retention overload must each fail the lane with
its own class and status, keep their output and the evidence they could write, and leave no process
behind. A consistent-order control must pass, and the recorded deadlock must replay to the same
class.

Under `just coverage-native-extras test-deloxide`, `test-deloxide-order`, `test-deloxide-stress` or
`test-deloxide-stress-restore`, the lane builds in the collector's instrumented target of the
feature its selection compiles, with instrumentation on workspace crates only, so dependencies such
as wasmtime's compiler keep the speed the product's deadlines assume. It takes the report tool, CLI
and shared client binding from the target its prerequisites built, starts every test executable
through Cargo's configured runner, and measures initial and warm passes of every selected build
under the same build budget and compiler context before any workload starts. It hands `lane.json`
to the collector, which exports nothing
unless the lane completed. CI's
`deloxide` job runs each selection this way, then the qualification, and uploads the completion
records, the diagnostic LCOV reports and every attempt directory whatever the verdict. Diagnostic
coverage stays out of ordinary coverage and its CRAP report. For a pull request labeled
`deloxide`, Check's `coverage-full` job requires each selection's complete collection with its
complete `lane.json`, and adds their lines to the
[full workspace union](./coverage-reports.md#the-full-workspace-union). The [Integration Test
Lifecycle](./integration-test-lifecycle.md#diagnostic-lane) chapter owns the job's bounds and
reserves.

**Applicability.** `[[owner]]` records in the same inventory cover every source file in which the
compiler finds a tracked blocking acquisition in a diagnostic configuration. Each names the
registered workloads whose processes reach its tracked locks, the path the lane does not reach and
why, or both. `just validate-deloxide-applicability`, part of `just validate` and CI's `checks`
job, reads the acquisition catalog `just ratchet` writes and fails on an owner without a record and
on a record whose file no longer acquires a tracked lock. It reports how many owners registered
workloads reach and how many record a path the lane does not reach. The catalog is the compiler's
resolution of the Deloxide adapters' operations, not a search for lock names. Every diagnostic
library and binary configuration enables `testing`, as the lane's server builds do, so acquisitions
in testing-only fault controls require owner records too. This includes the server's scheduling,
drain and executor-occupancy controls and the consensus storage fault controls. Ordinary and
modeled configurations retain their own feature selections. A record is a reviewed declaration:
the lane's coverage report is evidence about it, not a proof of ownership. Format
equality, malformed and out-of-bounds rejection, normalized-cycle deduplication and the summary's
counts are registered Bolero properties with the same assertion in ordinary and sanitizer execution.

The command transaction retention workloads reach tracked consensus state and report ownership
while cleanup runs with an execution held before finalization. Ordinary and stressed cases cover
one- and three-node retention, followed by cleanup after finalization, and three-node leadership
recovery after the transaction's configured age. Their asynchronous barriers, proposals, leader
observations and transport waits remain outside Deloxide; the public outcomes and direct
state-machine regressions cover the retention decision. No synchronization protocol or memory
ordering changes at this boundary.

The MongoDB client-identity workloads reach the filesystem executor's tracked class-queue fences
while preparing a private combined PEM and constructing native TLS on one- and three-node
clusters. Active, order-analysis and lifecycle-stress selections use the real mutual-TLS server,
require an identity-equipped client to write, and require its CA-only control to fail. Both
callbacks run outside the queue fence; driver-internal locks, certificate I/O and network waits
remain outside Deloxide's observation. All four diagnostic lanes apply to these lifecycle jobs;
restore stress retains its existing selection. The jobs add no synchronization or memory-ordering
protocol of their own, so they add no Shuttle or Loom model.

The WASM reset publication workloads reach tracked transaction state and impact recording during
initial application and recovery on one- and three-node clusters. Ordinary, order-analysis and
stressed selections distinguish typed loss of publication leadership from a retained genuine
failure, preserve the reset's execution identity and generation, and check sibling branch state.
The genuine-failure control is armed before ordinary graph publications and observed pending until
reset preparation has been aborted, so another scheduling path cannot consume it. It remains
enabled until a known failed outcome is observed, then is cleared before exact replay verifies
retention independently of the injected condition.
The test fault maps, asynchronous entity gates and consensus/network waits are untracked;
the injected refusal establishes no claim about Raft election interleavings or memory ordering.

The WebSocket ingestor lifecycle workloads drop an ingestor, move its owner and recover from repeated
peer closes. Their ordinary and stressed selections reach tracked quiesce, schedule and teardown
locks. Socket I/O, close-handshake deadlines and reconnect timers remain outside Deloxide's
observation; public peer observations measure the delays, and a paused-clock host regression checks
that successful upgrades keep backoff until a delivered batch resets it. Direct connector tests
exercise suspension and close without claiming those socket waits are tracked locks.
The declaration-buffer workloads engage the tracked entity gate, admit text and binary frames
under BUFFER, and replay them after a codec change on one and three nodes. Their scenario and
stress records name the entity-gate and quiesce-buffer owners. Connection response headers are
published once and shared by messages and retained buffer entries; this adds no lock or mutable
map on the frame path. Upgrade-field installation has a focused external-iterator dispatch
contract; its local conversions remain analyzed. The registered `websocket-retained-metadata` Bolero property checks every
field and exact snapshot identity through the production retain/replay conversion. Client ping
timer and socket scheduling remain outside the detector and are exercised by real-peer owner tests.

The WebSockets signaling handoff uses one bounded primitive mpsc channel only while signaling
accepts data and while its completed frames drain. A full queue backpressures signaling within its
TIMEOUT. The negotiating future owns the queue, and completion alone transfers it into source
reads; cancellation or failure ends its exact lifetime. Suspension retains a completed queue and
drains it before reconnecting. Once drained, the source drops the channel and steady socket reads
reach only task-owned state. This async-only queue protocol adds no tracked blocking-lock owner;
Deloxide does not establish its cancellation or capacity behavior. The three registered checks in
`crates/connectors/websockets/src/connection/ingestor/handshake/shuttle_tests.rs` exercise successful FIFO/header
handoff, a full queue racing expiry, and dropping negotiation before readiness through the
production owner. `just test-shuttle handshake` checks inventory, completed exploration and
nondeterminism; `just test-websockets-shuttle <full-check-name>` supports focused diagnosis with a
replayable failure schedule. One-/three-node public scenarios separately verify real-peer
handshake backpressure, ordered delivery and idle reconnection. Model checks claim no socket or
wall-clock coverage.

The native backup-wait workloads reach the client's pending-reply locks during multi-domain
capture, bounded wait expiry and recovery under the same execution reference. They run on one-
and three-node clusters, including expiry before the second cut, in both selections and name the
client request and exchange owners in the inventory. The detector observes their blocking
reply-registration and cancellation locks.
Async command and reconnect mutexes, physical deadlines and network scheduling remain outside
its observation; the public scenarios and client-session regressions assert their wait, expiry,
leader-change and recovery outcomes directly. The bounded native-client case runs inside the
diagnostic harness and reaches expiry and recovery with tracked client locks. The CLI scenarios
exercise diagnostic nodes around ordinary subprocesses, whose own locks remain untracked.

The lane's state-store owner test starts its detector in a fresh process before constructing store
or executor locks and runs the current generation regressions: cancellation before and after pointer publication, snapshot
readers, queued checkpoint writers, corrupt and incomplete chunks, exact retry after reopen,
stale authority, bounded cleanup across 600 checkpoints and a 40 MiB guest save admitted with a
2 MiB reservation. Its evidence is retained beside the probe and scenario evidence. The lane
also exercises quota refusal and exact staging retry, partial chunks without receipts across
reopen, reclamation with applying and future generations retained, published and initial state
preservation, snapshot readers across deletion, and interrupted bounded reclamation resumed after
restart. Active-generation owner checks also exercise ordinary and same-revision inline writes,
replica installation, handoff, forced recovery, entity and stale-identity purge, queued writers,
exact publication retries, referenced-revision cursors and cancelled active deletion across restart.
Current-shape storage fixtures also qualify segmented replacement and incomplete unpublished
revisions in both initial and restored namespaces, with retained readers and reopen.
Retained readers keep complete deleted payloads; SST observations distinguish logical deletion
from snapshot release and explicit compaction. Header copies stay bounded, while Fjall's value
size queries and database caches retain their separate allocation owner. The node-local sweeper
takes the applied consensus read guard before the checkpoint
installation barrier, matching staging and publication; both remain held through bounded deletion.
The public quota scenarios retain active staging through completed sweeps and prove failed
targets remain gated after reclamation and restart on one and three nodes. The selected
scenarios run without retries in a binary built for the mode: in-process diagnostic nodes running
a workload, real diagnostic
server processes that stop gracefully with evidence that records a running detector and no
findings, each on one and three nodes, the buffered client alterations with overlapping holds and
failed drains or full-hold engagement, and the restore installation workloads. The latter exercise
the blocking applied-state authority guard through staging failure, complete publication, runtime
handle clearing and a delayed coordinator across leadership transfer and a successor's START.
The large-generation scenarios cover two 20 MiB saves and forty 1 MiB saves on one and three nodes,
failure after durable publication, cluster restart, branch isolation, saved source offsets and
positive reclamation after resumed checkpoints and the state purge from module rebinding.
The three-node many-save case retains one replica per state and observes reclamation of both
owner and replica chunks.
The large branch-state scenario restores a 40 MiB deduplicator keyspace and a 40 MiB window beside
24 small branches on one node, through a failure after durable publication, a stopped restore and a
resumed one. Once the resumed domain runs, the window persists as a segmented checkpoint under the
installation barrier and reopens from it after a cluster restart. Its three-node example runs in
the ordinary suite only.
Restore workload setup sends durable mutations and activation through the Rust client, which
recovers the same execution reference when leadership moves; the raw subscription session keeps
its own row stream. Large-generation and staging archive captures use a two-minute command budget
so positive capture waits accommodate concurrent diagnostic workloads.
The three-node copy restore cordons its coordinator before planning, so every guest save uses the
remote chunk transfer and the receiver's quota-owned completed file.
Those restore scenarios also run in the ordinary public suite. The memory-pressure scenarios take
the node's pause lock as the pause collects every registered quiesce control and as each starting
ingestor reads the pause, on one and three nodes. The client consumer scenarios take the Rust
client consumer's state and parked-read locks in the scenario process as a consumer is restored
after a session restart and closed by a domain restart. The Roto scenario takes the bool column
builder's lock in every `push`, `push_null` and `finish` of an admitted function call, for a
selected, a dropped and an absent value. The TLS rebinding scenario replaces the HTTPS listener's
TLS configuration on every node as a TLS VHOST is created and rebound, while each connection the
background HTTPS posts open reads it. The inferencer scenario takes a branch's ONNX session lock
for each batch of two interleaved branches. Each of the three runs on one and three nodes. On three
nodes with a replica, the deduplicator, window and guest-save restore scenarios leave the owner's
lifecycle and branch checkpoint announcements with the replica's lifecycle handle, where the
replica task takes them under the same lock.

The ONNX inspection admission scenario saturates the leader's bulk workers and wait queue, observes
the typed CREATE INFERENCER refusal, releases the occupancy and creates the same inferencer on one
and three nodes. Its paired stress registration reaches the occupancy holders mutex under bounded
scheduling disturbance. It also observes the inspection's bulk-memory charge returning to zero.
The `owner.onnx-metadata-inspection` workload runs production-wrapper queue recovery in the fresh
process the lane launches for that exact owner, then the bounded-read and cancellation regressions,
including retaining the buffer charge while a
cancelled read is still in progress. Metadata inspection keeps ONNX's operator concurrency at one;
sequential preparation of an explicit native image, loading, optimization and release run on the
admitted bulk worker. The thirty-second bound includes memory admission. Phase events and budget
snapshots distinguish memory waiting, worker queueing, image reads, session initialization and
release. No shared cache, global lock or atomic protocol is added. Filesystem waits and native
synchronization remain outside the detector; the existing executor cancellation Loom models own
the flag's memory-ordering contract. The dependency's own locks and threads remain outside Deloxide; the isolated native owner
regression measures the live temporary session's process thread count, and the public branch-batch
scenario retains its exact interleaved-branch output oracle.

Shuttle checks the production handoff's delivery/refusal and close races, including counter drain,
through the registered replay-capable harness, in the Shuttle build where no detector is installed.
The queue internals and OS wakeups are opaque; the relaxed refusal counter publishes no other
location. `OnceLock` publishes the installation port; no new independent cross-location atomic
protocol is introduced. Existing primitive Loom conformance remains separate. The lane's supervisor
runs outside every diagnostic process: it adds no synchronization to a process it observes and
reads only the processes' exits and the evidence files they wrote. Deloxide models no async or network scheduling. Tokio locks,
channels, `Notify`, DashMap shards, dependency locks, atomic protocols, missing notifications and
cross-node waits remain outside its detection claims and retain Shuttle/Loom/Turmoil/Chaos checks.
No diagnostic check proves safety for an untracked path or an order the workload did not take.

**Diagnostic images under external chaos.** The lane runs diagnostic nodes in a scenario binary or
as local processes; the external chaos suite runs them as packaged containers under real process,
pause and network faults. The `diagnostic` target of `Dockerfile.debian` builds the server of one
selection, `NERVIX_DIAGNOSTIC_SELECTION`, with the ordinary CLI and formatter and the
`nervix-deadlock-report` tool, on the release image's CPU runtime. It declares the selection in its
`io.nervix.diagnostic.selection` label, and every image, release or diagnostic, declares the revision
it was built from in `org.opencontainers.image.revision`. The release targets copy their binaries
from the builder's release artifacts and the diagnostic target from its diagnostic ones, so a build
of one kind cannot produce an image of the other, and the diagnostic image never enters a published
manifest. `just docker-build-diagnostic` builds it; CI's `build-diagnostic` job builds the
`deloxide-order` image beside the release images whenever a chaos suite runs, and the suites run
against both. The chaos runner reads the label: every node records its evidence in a directory of
the run, the run freezes the nodes once its Docker event recording is closed, and the image's own
report tool qualifies every file before cleanup. A run fails unless each node holds one complete,
qualifying evidence file of the declared selection for every process start the recording shows,
and no node ended with status `3` or `4`; an unreviewed potential cycle fails it as in the lane.
The suite refuses an image whose labels name another kind or revision than its caller asks for.
[External Cluster Chaos](./cluster-chaos.md#diagnostic-images) owns the runner's contract and
artifacts. The evidence observes each process up to its kill, pause, stop or the freeze, without an
upstream flush barrier, and only its tracked thread-blocking locks; recovery, ledger and timing
verdicts stay the scenarios' own, with the product's deadlines and the suite's budgets unchanged.

The evidence verdict and the scenario verdict are separate. A run can retain qualifying evidence
from every process and still fail its ledger, recovery checks or controller budget. A soak
qualification requires both suite verdicts to pass, with every configured entry complete on the
ordinary and diagnostic images of the same revision. The owning task records the revision, both
immutable image digests, every shard verdict and the diagnostic process-start and evidence-file
counts, together with failed attempts and their owning corrections. Per-process evidence qualifies
the tracked lock activity that was recorded; the complete scenario verdict also establishes the
delivery and recovery behavior that the external runner observes.

## Deterministic Concurrency Verification

The relay interaction's ordinary output-wake test stages a collected batch through the production
admission path and awaits its domain-owned collection deadline before making the output wake
ready. It requires the wake to win and the exact batch to remain deliverable afterwards. Its initial
pending poll uses an empty input, so establishing that state does not depend on finishing a poll
within a one-millisecond wall-clock interval. This checks arbitration against two ready events
without changing production timing or synchronization.

The Pulsar emitter uses the shared `ConfirmationWindow` and its registered property and Shuttle
checks for concurrent broker answers, timeout and enqueue failure. Under `NO ACK`, one emitter task
owns its retained send receipts and the first failed receipt; it collects completed receipts
while publishing and awaits the rest at finish under the host's deadline. No receipt or outcome
is persisted or published through a shared map. The driver and its asynchronous channels remain
outside Deloxide's tracked locks. The registered `scenario.pulsar-finish-at-stop` and
`stress.pulsar-finish-at-stop` workloads exercise that finish through one- and three-node STOP,
including the host's tracked quiesce and teardown paths.

The MQTT emitter's QoS 0 publishing has the same shape. One emitter task owns the flush notices of the
publishes its client took, the driver's one-shot completions that its event loop ends once it has
written and flushed each publish. The task drops the notices that ended at the start of each
publish and awaits the rest at finish under the host's deadline, in the order the client took them.
No notice or outcome is persisted or published through a shared map, and the driver's event loop and
its channels remain outside Deloxide's tracked locks. The registered `scenario.mqtt-finish-at-stop`
and `stress.mqtt-finish-at-stop` workloads exercise that finish through one- and three-node STOP,
including the host's tracked quiesce and teardown paths.

An MQTT source connection has one mutable owner of its event loop: the driver task the source
starts once its subscription is established. The driver hands every publish to the source through
one bounded read-ahead, keyed by that one connection. In an `ACK` mode it reserves the entire
Receive Maximum for acknowledged packet outcomes and permits at most 64 QoS 0 outcomes alongside
them. A queued QoS 0 outcome owns a semaphore permit until the source takes it; acquiring a permit
never waits, and a refusal drops and counts that publish before reading the next. QoS 0 traffic
therefore cannot occupy an acknowledged outcome's capacity or stop polling. The host supplies a
pre-resolved Prometheus counter handle, so a drop reaches no shared map and is counted while the
host waits on downstream acknowledgement. Under `NO ACK` the queue holds 64 outcomes and applies
backpressure. A peer exceeding the advertised Receive Maximum can also exhaust the queue. A full
queue holds its next outcome and stops polling until the source reads one. The driver
publishes its progress as one immutable watch value: how many acknowledgements the event loop has
written, and whether it polls, holds a publish for room, or ended. The source's acknowledgement
waits on that publication and on nothing else, and the wait ends without its writes as soon as
the published value says the driver stopped polling, or once one keep-alive interval has passed,
so neither side waits on the other. The queue, capacity semaphore, counter and watch value are
bounded synchronization scoped to one connection or its pre-resolved ingestor counter. Their
asynchronous operations and the metric library's internals remain outside Deloxide's
tracked locks; the Shuttle checks below and the public keep-alive and acknowledgement scenarios
are their evidence.

The MQTT driver classifies each decoded packet before reading another. Its read-ahead contains
independently owned admitted bytes or compact discard acknowledgements, never an oversized
publish's payload or properties. One connection-owned discard state retains its prepared
acknowledgement until queued, its shared confirmation target until written, and its original
physical deadline across cancellation. Gathering ends at a discard without starting its ACK:
the host acknowledges the ordinary prefix first, and the following read starts the discard's
physical deadline, preserving MQTT receive order. Queue admission advances the connection's
request count and retained state in the same poll; confirmation returns its size outcome once. The
ordinary paused-clock test checks the retained deadline; the production-owner Shuttle check
races cancellation with these transitions and the ordinary prefix's acknowledgement, requiring
one size notification, writes in packet order and the following ordinary publish.

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
boundary selects every family from Shuttle or from its own adapters, as the [backend matrix](./execution-primitives.md#backend-matrix) shows, and
every package that owns the feature forwards it to the boundary, so the synchronization inside
vocabulary types such as `AtomicTimestamp`, inside dependencies such as the execution crate's
cancellation, and inside every connector is Shuttle's too, and so are the timers of every crate in
the graph. The feature changes the test execution environment, not the public protocol. Edge I/O,
sockets, filesystem access, and signals remain real and are outside a Shuttle schedule. A Shuttle
execution has no Tokio reactor, so a socket created inside a check panics and fails it rather than
reaching the network unobserved. A check's own records use unmodeled atomics on purpose, under the
[unmodeled permissions](./execution-primitives.md#unmodeled-primitives), so that recording an operation adds no scheduling point to it.

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
remains the public behavior test: when a scheduling defect affects a Plexus operation, runtime
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
schedules, or one scheduler with the check's own bounds. A model in which either of two threads
can spin until the other moves takes 1,000 random and 1,000 PCT schedules without the depth-first
search: that search ignores yields and tries the lowest-numbered runnable thread first, so it
lengthens such a spin by a step per schedule until a schedule exceeds the step limit, while a
random or PCT schedule leaves the spin. Every schedule may take 10,000 steps; one
that needs more fails its check, because a step cap is an exploration bound, not a product timeout.
A random or PCT search that ran fewer schedules than it declares fails too, and a search that
finishes prints a record naming what it explored. `SHUTTLE_REPORT_STEPS=1` reports the highest
observed step count when tuning a check.

The primitive boundary's own conformance and race checks run in `just test-primitives`, which
builds the boundary once per mode; its Shuttle checks explore their small models exhaustively with
depth-first search, and each deliberately broken order must deadlock in some schedule.

`crates/model-harness/shuttle-inventory.toml` registers every check of `nervix-execution`,
`nervix-interconnect`, `nervix-client-core`, `nervix-connector`, `nervix-connector-kafka`,
`nervix-connector-mqtt`, `nervix-connector-sqs`, `nervix-consensus`, `nervix-server` and
`nervix-deadlock` by its test's full name.
`just test-shuttle` lists the library tests whose full names contain `shuttle_` in each registered
package, built with its `shuttle` feature, and a run over the whole inventory fails when a
registered check is missing or ignored, or when a discovered check is unregistered. Each selected
check runs twice, each time in its own process: under the exploration it declares, then under
Shuttle's uncontrolled-nondeterminism detector, which runs 100 random schedules twice each and
fails when the second run diverges. A run counts only when its test passed and the runner printed
its record, and the command reports how many checks it discovered, selected, executed and saw
complete. Checks run beside each other, one per CPU the runner may use, so a run lasts about as
long as its longest check rather than as long as every check added together. A check's own two
runs stay one after the other, because both persist a failing schedule in its one failure
directory. The runner starts checks in selection order, records each one in its evidence as it
starts, and prints a check's output, with how long each run took, once the check ends. Nothing in a
check depends on what runs beside it: every process explores its own model under its own
scheduler, and a check opens no socket and writes no fixed path.
`just test-shuttle <filter>` selects the checks whose full name contains the filter in
every package; a package with no match is fine, and a filter that selects nothing at all fails. The
recipe uses the repository's kache-backed build and prepares the server's test dependencies; `just
test` continues to run the ordinary suite.
`just cargo-clippy-shuttle`, whose package checks also run in `just lint`, lints every Shuttle
build with warnings denied, including every inventory package in test mode, where its checks are
compiled, so a check that compiles with a warning fails validation.

A failed check leaves `target/shuttle-failures/<package>/<fully-qualified-test-name>/`: the schedule
Shuttle persisted for the failing execution, the run's output, and metadata naming the check, the
run, the revision, the toolchain, Shuttle's version and the replay command. `just
test-shuttle-replay <schedule>` replays exactly that check in a fresh process with the schedule,
which ends where its execution failed and so replays that failure. `just test-shuttle-replay-check`
proves the path end to end: it fails one check deliberately after its invariant held, requires one
persisted schedule, and requires it to reproduce the failure in a fresh process. CI's dedicated
`shuttle` job runs both, only for a pull request labeled `shuttle`, and uploads
the failure directory in a tar archive as the `shuttle-failures` artifact when a check fails;
fully qualified test names contain colons, which the artifact uploader rejects as paths. See the
command recipes in [Developing Nervix](./developing-nervix.md).

The host checks share the dev-only `nervix-connector-fake` harness. Its ingestor channel detects
a read abandoned before completion, and its scripted record emitter retains the complete
values of each attempt. The ingestor loop remains the owner under exploration; the emitter
checks drive the production retained-payload and acknowledgement owners. Moving a fixture
does not move a protocol into the harness or qualify a real transport. The fake uses the
selected primitive backend and preserves the existing check identities and replay path.

Its assertion logs are harness coordination: one shared log orders the observations of its
scripted connector callbacks with the test's reads. Recurring trait overrides carry narrow
`bounded` source contracts naming that log and one synchronous, caller-bounded observation
update. Every log guard ends before a callback awaits input, cancellation or a scripted gate.
These contracts retain the fixture's synchronization under compiler review; production
connector and host synchronization keeps its own protocol contracts.

### Protocols and their checks

The consensus log chunk owner releases its iterator allocation and Commands reservation in the
same transition that hands out its final entry. Its Shuttle check races that transition with
another reader's reservation and leaves the first consumer parked: the other reader and a small
source control reservation must finish without another poll of the exhausted chunk. This is a
control-plane memory-lifetime check, with no memory-ordering claim. The real-storage owner tests
also check that a partially consumed chunk retains its charge, and the public WASM/Kafka restore
outline checks source readiness and Commands-memory headroom after restoring onto a fresh cluster,
allowing the retirement reservations that live Kafka consumers retain.
The semaphore waits are asynchronous and outside Deloxide's tracked-lock detection; the diagnostic
restore workloads exercise the surrounding storage and restore lifecycle.

Consensus storage admits each Commands or Bulk working set through independent class-specific
turns before asking its memory class, and a working set returns its bytes before its turn passes.
Each Shuttle check holds one working set while a second admission waits for its turn and a progress
charge races both, then ends the held working set: the progress charge finds room at every
interleaving, the second working set is admitted, and the class ends with nothing reserved. The
Commands check charges a source control call; the Bulk check charges snapshot progress. The real-storage owner tests
hold a chunk with an unread entry while a log read and a storage job wait behind it, and require
the control call's room while they wait. Like the chunk check, this is a control-plane admission
check without a memory-ordering claim, and its waits are outside Deloxide's detection.
Ordinary owner tests additionally poll request-envelope, staging and captured-reader admission
under held and queued Bulk charges: each additional allocation refuses without waiting and
releases its input. The public stopped-materialized-capture scenario exercises refusal, memory
release, a fresh successful backup and complete branch-local generator recovery on one and three
nodes. Its diagnostic workloads reach the surrounding storage lifecycle; Deloxide does not
observe the asynchronous admission waits themselves.

The checks below hold the scheduler-visible parts of each protocol to their invariants. Test
names are given relative to their owning module; the `shuttle` feature selects the modeled build.
A family of names means each member runs independently through the recipe.

| Protocol | Invariant and check |
| --- | --- |
| Waiter registration and cancellation (`crates/primitives/src/tests/shuttle_races.rs`) | `shuttle_reaches_a_notify_waiters_published_between_a_read_and_the_registration` and `shuttle_reaches_a_send_between_a_read_and_the_subscription` require a waiter that reads before it registers to deadlock in some schedule, which shows the scheduler reaches a publication between the read and the registration; `shuttle_registering_before_reading_never_misses_a_notify_waiters` and `shuttle_subscribing_before_reading_never_misses_a_send` require the correct order never to lose one. `shuttle_reaches_a_wake_between_a_read_and_the_waker_registration` and `shuttle_registering_the_waker_before_reading_never_misses_a_wake` hold an atomic waker to the same pair. `shuttle_a_notify_one_between_a_read_and_the_registration_is_kept_as_the_permit` shows a single notification's permit makes the other order correct for `notify_one`, and `shuttle_a_waiter_cancelled_as_it_is_notified_passes_the_notification_on` requires the surviving waiter to complete however a cancellation and a notification interleave. |
| Execution budgets and storage jobs (`crates/execution/src/tests.rs`) | `shuttle_saturated_class_keeps_live_reservations_within_each_class_capacity` keeps live reservations within each class capacity, including optional state-read retention while the live relay class is full; `shuttle_occupied_bulk_execution_leaves_control_execution_untouched` keeps bulk saturation from charging control; `shuttle_occupied_connector_leaves_filesystem_and_consensus_execution_untouched` keeps a held connector call from consuming filesystem or consensus admission; `shuttle_queued_job_drop_releases_its_reservation_and_exact_queue_slot` and `shuttle_running_job_observes_cancellation_and_keeps_its_charge_until_exit` balance queue slots and permits across drop and cancellation; `shuttle_full_wait_queue_is_exact_typed_backpressure` checks a full wait queue's typed rejection; `shuttle_a_job_waiting_for_a_place_takes_the_first_freed_place` requires a job that waits for a place to take the first one freed and run before a job refused when full that asks after it; `shuttle_a_dropped_job_waiting_for_a_place_gives_up_its_place_and_charge` returns a dropped waiter's charge and hands its place to the job waiting after it; `shuttle_consensus_storage_preserves_admission_order_and_returns_every_permit` checks storage admission order and permit return. |
| Force-flush obligations (`src/runtime/force_flush.rs`) | `shuttle_two_participant_generation_waits_for_every_obligation` prevents completion before all participants live at publication complete and redelivers a dropped, unhandled completion; `shuttle_stale_completions_never_clear_a_newer_generation` prevents an old completion from clearing new work; `shuttle_published_generation_wakes_a_waiting_participant` catches a lost publication wakeup; `shuttle_participant_lifecycle_balances_obligations_through_close` balances `pending()` across subscribe, request, participant drop, and close. |
| Ingestor intake (`src/runtime/ingestors/connector_shuttle_tests.rs`; `src/runtime/ingestor_quiesce.rs`) | `shuttle_broker_ingestor_observes_engagement_during_dispatch`, `shuttle_paced_ingestor_observes_engagement_during_dispatch`, and `shuttle_request_ingestor_observes_engagement_during_dispatch` exercise host-loop engagement for memory pressure, entity gate, handoff, and shutdown: after engagement returns, no further payload dispatches, and a change during dispatch is observed. `shuttle_an_open_control_answers_intake_without_waiting_on_retained_payloads` keeps the open decision independent of a retained-payload lock. `shuttle_a_new_quiesce_ends_a_delivery_waiting_for_extension_room` and `shuttle_a_shutdown_ends_a_delivery_waiting_for_extension_room` race the engagement or the stop against a retained payload's delivery waiting for a place the extension class never frees: the waiter registers before it reads, so the delivery always ends, and its payload is back at the front of the buffer counted as it was. `shuttle_a_new_quiesce_decides_again_on_a_live_payload_waiting_for_extension_room` and `shuttle_a_stop_ends_a_live_payload_waiting_for_extension_room` race a `BUFFER` engagement or the stop against a live payload waiting for such a place: the wait always ends, a new decision becomes the one the payload is decided under next and its buffer retains it, counted with its bytes, and a stop leaves nothing retained. |
| Broker source confirmation (`src/runtime/ingestors/connector_shuttle_tests.rs`; `src/runtime/ingestors/connector.rs`, `src/runtime/ingestors/connector_acknowledgement.rs`, `src/runtime_ack.rs`) | `shuttle_a_drain_waits_for_the_transport_to_confirm_a_resolved_batch` lets a drain read the domain's ownership-handoff count at any point while the graph resolves the two confirmed roots of one batch in either order and the source's transport takes a round trip to confirm the batch. Whenever the drain reads no outstanding work, every batch whose roots resolved has been confirmed, and the count returns to zero once it is. The check fails under a batch acknowledgement that releases the last root before the loop drops it, and under a root released as it resolves. |
| Refused payload confirmation (`src/runtime/ingestors/connector_shuttle_tests.rs`; `src/runtime/ingestors/connector.rs`, `src/runtime/ingestors/connector_acknowledgement.rs`, `src/runtime_ack.rs`) | `shuttle_a_drain_waits_for_the_transport_to_confirm_a_batch_with_a_refused_payload` hands the broker loop a batch of one payload its codec refused, and a batch whose refused payload a dispatched one follows. The host consumes the refused payload through the production `BatchRoots`, which resolves its confirmed root at once, while the graph resolves the dispatched root, the source's transport takes a round trip to confirm the batch, and a drain reads the domain's ownership-handoff count at any point. Whenever the drain reads no outstanding work, every batch whose roots resolved has been confirmed; the batch is confirmed and never rejected, and the count returns to zero once it is. The check fails under a consumed root whose completion is released as it resolves. |
| Broker source reconnect (`src/runtime/ingestors/connector_shuttle_tests.rs`; `src/runtime/ingestors/connector.rs`) | `shuttle_a_reconnect_discards_rejections_the_broker_redelivers` loses the connection of a `broker_redelivered` source at any point, once or twice, while the host loop acknowledges two messages one at a time. The host never asks a connection to acknowledge or reject a delivery of a lost one. Every message is acknowledged exactly once, on the connection that delivered it, and each rejection a lost connection failed is discarded and counted exactly once. The check fails under a loop that retries the failed rejection after the reconnect. |
| Broker source read interruption (`src/runtime/ingestors/connector_shuttle_tests.rs`; `src/runtime/ingestors/connector_read.rs`, `src/runtime/ingestors/connector.rs`) | `shuttle_an_interrupted_broker_read_hands_the_host_every_message_it_took` lets a stop, or a suspension of intake, meet the broker loop at any point while its source gathers a batch of up to three messages from a transport that delivers two. The host cancels the read and awaits it rather than dropping it: every message the source took reaches the host's intake exactly once, and before the source is suspended or closed, and the source is never asked to read, suspend or close while a read the host dropped is still in flight. The check fails under a loop that drops the read it stops waiting for. |
| Relay dispatch gate and fan-out (`src/runtime/relay_channel_shuttle_tests.rs`) | `shuttle_dispatch_permits_never_overlap_a_quiescent_lease_and_release_frees_every_waiter`, `shuttle_overlapping_gate_leases_all_release_before_dispatch_resumes`, `shuttle_expired_gate_fence_frees_every_waiter_without_reporting_quiescence`, `shuttle_canceled_dispatch_returns_its_permit_to_the_gate_fence`, and `shuttle_dispatches_parked_behind_a_lease_wake_only_on_its_release` hold the fence, lease, expiry, cancellation, and waiter contract; in-flight dispatches drain before quiescence. `shuttle_capacity_shrink_keeps_buffered_batches_and_wakes_publishers_after_the_drain`, `shuttle_capacity_growth_admits_waiting_publishers_without_a_take`, `shuttle_publishers_wait_for_the_slowest_consumer_and_skip_consumers_that_leave`, and `shuttle_losing_every_consumer_delivers_or_returns_the_waiting_batch` keep queued batches across capacity changes, release waiting publishers, and return or deliver each batch when receivers leave. |
| Relay owner fan-out (`src/runtime/relay_boundary_shuttle_tests.rs`) | `shuttle_owner_fanout_fails_its_ack_while_an_attached_consumer_moves` keeps a live sibling from completing a source ACK while another attached consumer leaves under a schedule fence; after release, a retry reaches the live consumer. `shuttle_a_routed_batch_racing_a_joining_consumer_reserves_a_share_for_every_delivery` races a batch another node routed here against an attached consumer registering here: every consumer the batch reaches holds a share of its own, so one that fails the batch fails the source ACK. |
| Local drain observation (`src/runtime/local_drain_shuttle_tests.rs`) | `shuttle_a_batch_published_while_a_drain_observes_is_never_missing_from_it`, `shuttle_a_batch_a_consumer_takes_while_a_drain_observes_is_never_missing_from_it`, and `shuttle_a_batch_its_owner_hands_on_while_a_drain_observes_is_never_missing_from_it` race a batch moving from a node into a relay, from a relay into a consumer's node, and from a relay's transit into its consumer's queue against one drain observation of the production read order: the observation shows the batch, or the relay's admission when the move went against the order of its reads. `shuttle_a_message_a_flush_resumed_is_seen_by_a_drain_that_sees_the_flush_complete` races a force flush that resumes a parked message and completes its obligation against the observation: one that sees the obligation complete sees the message as admitted work. |
| Relay subscription definitions (`src/runtime/relay_subscription.rs`) | `shuttle_an_attachment_racing_a_redefinition_is_refused_or_closed_by_it` races a subscriber's attachment against a redeclaration of the relay with a field that became sensitive: the attachment is refused, or the redeclaration closes the receiver it attached, so no subscriber describes rows by a replaced definition. |
| Materialized dependency waits (`src/runtime/materialized_read.rs`) | `shuttle_a_parked_message_is_woken_by_the_materialized_state_it_waits_for` races the arrival of the state a `REQUIRED WAIT` message is parked on against the processor branch reading it and parking: the wait registers before the read, and again before every retry, so the arrival always wakes it. |
| Client emitter budget (`src/runtime/client_emitter_shuttle_tests.rs`) | `shuttle_competing_consumer_grants_never_exceed_the_node_budget`, `shuttle_consumer_loss_and_ack_race_release_one_retained_delivery` and `shuttle_publish_cancellation_and_ack_race_release_one_reservation` keep grants within the node budget and release each retained delivery and reservation once. `shuttle_a_reservation_waiting_for_a_full_budget_takes_the_bytes_a_release_returns` races a release against a reservation that finds the budget full: the reservation registers for the change before it reads the budget, so the release wakes it and it takes the bytes. |
| Assignment authority and state updates (`src/runtime/state_store_shuttle_tests.rs`, `materialized_state.rs`, `kafka_offset_state.rs`) | `shuttle_a_rebind_yields_until_the_operation_admitted_under_its_replaced_binding_finishes` and `shuttle_no_operation_admitted_under_a_superseded_binding_outlives_its_superseding_rebind` fence admitted work even when generations reuse an even or odd counter. `shuttle_snapshot_installation_never_overlaps_origination_or_a_capture` keeps exclusive installation apart from originators and captures. `shuttle_an_originator_update_proceeds_while_the_assignment_barrier_is_held` and `shuttle_a_committed_offset_proceeds_while_the_assignment_barrier_is_held` keep ordinary admitted updates independent of a capture's barrier. |
| ACK tree (`src/runtime_ack.rs`) | The `shuttle_tests` checks `concurrent_attachment_and_final_ack_leave_exact_tracking`, `concurrent_wait_and_active_ack_exempt_the_remaining_root`, `concurrent_wait_release_and_completion_leave_no_tracking`, `attachment_losing_its_reservation_to_completion_resolves_no_share`, `attachment_losing_its_reservation_to_completion_parks_no_share`, `wait_release_racing_the_last_active_ack_holds_domain_and_ingestor_handoff_once`, `attachment_racing_the_last_active_share_into_wait_publishes_one_active_share`, and `parked_remote_progress_survives_a_racing_heartbeat_and_resumes_once` keep pending, active, and handoff counts exact across attachment, `REQUIRED WAIT`, and remote progress. `concurrent_success_and_failure_choose_one_terminal_transition`, `fan_out_across_ingestors_with_a_failing_root_resolves_each_root_once_with_exact_counts`, and `parked_and_fanned_out_roots_hold_exact_counts_at_every_quiescent_point` require one terminal result per root, one observed completion, and zero outstanding counts after resolution. `a_confirmed_root_stays_counted_until_its_owner_confirms_and_releases_once` races a confirmed root's resolution with its owner taking the resolution and dropping its completion: a resolution the owner holds keeps the root counted, and the counts are released exactly once, after both. |
| Forwarded acknowledgement silence (`src/runtime/remote_dispatch_shuttle_tests.rs`) | `shuttle_a_terminal_outcome_racing_the_final_sweep_resolves_the_share_once` races the receiver's terminal outcome against the sweep that would fail a forwarded share: exactly one of them removes it, and the root delivers that one's outcome. `shuttle_a_report_racing_the_final_sweep_keeps_the_share_it_reached` races a report against the same sweep: a report that reached the share keeps it pending with its root unresolved, and only a report that found it removed lets the sweep fail it. |
| Entity gate and node quiesce (`src/runtime/entity_gate_shuttle_tests.rs`) | `shuttle_an_entity_gate_hold_fences_every_relay_and_admits_no_work_until_it_is_released` requires admitted work to drain before quiescence and prevents new admission while closed. `shuttle_a_work_item_parked_for_materialized_state_is_never_missing_from_a_drain` and `shuttle_every_node_quiesce_gauge_withdraws_exactly_what_it_contributed` keep parked, buffered, and branch work counted without underflow and back to zero. `shuttle_every_engagement_waiter_wakes_and_exactly_one_release_takes_the_hold`, `shuttle_a_hold_dropped_before_its_fence_completes_reopens_every_relay_it_engaged`, and `shuttle_a_failed_engagement_wakes_every_waiter_with_its_failure` cover release, drop, and failure. `shuttle_releasing_an_ownership_handoff_wakes_every_waiter_frozen_by_it` holds release-before-wake; `shuttle_an_ownership_handoff_freeze_observation_registers_before_its_read` holds register-before-read against that release. A stopping runtime ends a lease through the same exact-instance release and an interrupted engagement through the same failure that these checks cover; `runtime_shutdown_ends_an_entity_gate_lease_held_past_its_stop` and `runtime_shutdown_ends_an_engagement_still_waiting_for_its_relays` cover that teardown reaches them, and `a_stopped_runtime_refuses_a_new_entity_gate_engagement` that a stopped runtime engages nothing. |
| Interconnect slots and membership (`crates/interconnect/src/connection/stream_slots/shuttle_checks.rs`, `request/shuttle_checks.rs`) | `management_drain_stops_leasing_and_waits_for_every_leased_slot`, `replication_drain_stops_leasing_and_waits_for_every_leased_slot`, `bulk_drain_stops_leasing_and_waits_for_every_leased_slot`, and `relay_drain_stops_leasing_and_waits_for_every_leased_slot` keep partition and subquota reservations isolated, forbid leases after drain starts, and wait for every lease to return. `racing_registrations_lose_no_handler_and_publish_each_name_once` prevents a lost handler registration and duplicate name. `a_membership_change_between_a_callers_check_and_its_wait_is_never_lost` prevents a missed discovery wakeup. |
| Shutdown and signals (`src/application/shutdown.rs`, `termination_signals.rs`) | `shuttle_racing_stop_requests_accept_exactly_one_and_keep_its_deadline` retains the first stop request and its deadline. `shuttle_phases_only_advance_and_every_completion_waiter_observes_the_one_outcome` keeps phase order and one completion. `shuttle_an_expired_deadline_and_a_repeated_signal_let_exactly_one_forced_exit_end_the_process` and `shuttle_a_repeated_signal_before_the_deadline_ends_the_process_with_the_status_of_that_signal` give one forced-exit claimant and the exit status of the cause that won. |
| Brokered emitter confirmation window (`crates/connector/src/confirmation_window_shuttle_tests.rs`) | Each check hands records to the production `ConfirmationWindow` while broker tasks answer them concurrently through channels of their own, each recording when its answer could be read, and the check notes which answers were given before the publish began to end. `shuttle_a_failure_settles_every_answer_its_broker_gave_before_it` ends the publish with the driver's failure: every record whose answer was given before it is delivered or rejected as answered, and a failed answer leaves its record unresolved. `shuttle_answers_racing_a_full_window_settle_each_record_exactly_once` hands five records through a window of two whose answers arrive in any order: the window holds fewer than two answers after each hand-over, and every record is settled exactly once as its answer says. `shuttle_an_expired_oldest_settles_the_answers_given_before_it` lets a check-triggered timeout expire the publish's deadlines at any point among the answers, which a Shuttle timeout does before it polls the answer: a missed deadline still settles every answer given before it, the oldest's own included. `shuttle_a_failed_oldest_answer_settles_the_younger_answers_given_before_it` ends the publish with the oldest's failed answer and still settles a younger answer given before the end. `shuttle_driver_backpressure_settles_below_max_in_flight` hands five records through MAX eight with a driver capacity of two, requests the oldest answer on demand as that capacity fills, and checks exact complete settlement and the driver-capacity bound while broker answers race those waits. |
| Kafka producer retention (`crates/connectors/kafka/src/topic/delivery_shuttle_tests.rs`) | `shuttle_the_count_holds_every_record_until_its_report_is_served` hands three records to a task that serves their delivery reports as librdkafka's polling thread does, counting after each hand-over: no count exceeds the records handed over or misses a record whose report was not yet being served, and the count returns to zero once every report was served. `shuttle_a_publish_that_reads_a_report_finds_its_record_no_longer_retained` serves a confirmed record's report while the publish awaits it: a publish that reads the report never finds the record still counted. |
| MQTT read cancellation (`crates/connectors/mqtt/src/topic/ingestor_shuttle_tests.rs`; `crates/connectors/mqtt/src/topic/ingestor.rs`) | `shuttle_a_cancelled_read_hands_over_every_publish_the_broker_delivered` lets the host cancel an MQTT read at any point while a broker task delivers two publishes, or three that fill the batch, into the connection through the ingestor's publish seam, which keeps a publish it read for the next read as the connection's read-ahead does. Every publish the broker delivered reaches the host exactly once and in order, from the read or from the connection's next read, and a batch that is not full ends only because the host cancelled it. `shuttle_a_read_whose_batch_timeout_races_the_cancellation_hands_over_every_publish` also lets a clock expire the batch timeout at any point: every publish still reaches the host exactly once. The first check fails under a gathering that ignores the host's cancellation until its batch timeout passes. |
| MQTT ingestor connection (`crates/connectors/mqtt/src/topic/ingestor/driver_shuttle_tests.rs`; `crates/connectors/mqtt/src/topic/ingestor/driver.rs`) | Each check drives the production connection and driver over a fake event loop that writes requested acknowledgements ahead of broker deliveries. `shuttle_an_acknowledgement_returns_once_the_loop_wrote_every_acknowledgement_it_requested` acknowledges three publishes while two more arrive: writes are confirmed and every admitted packet is preserved. `shuttle_an_acknowledgement_stays_writable_during_qos_zero_overflow` holds one QoS 1 acknowledgement while 66 QoS 0 publishes and another QoS 1 publish arrive; the driver reaches the following packet without a source read, retains all 64 admitted QoS 0 publishes and both QoS 1 publishes in order, and writes both acknowledgements. The progress witness cannot make room in the queue. `shuttle_admitted_publishes_keep_order_while_the_first_acknowledgement_is_written` overlaps reads of an admitted prefix with its acknowledgement. `shuttle_an_acknowledgement_whose_connection_ends_fails_with_the_loss` retains the typed connection failure or confirms a write completed before the ending. `shuttle_a_cancelled_discard_preserves_acknowledgement_and_batch_order` races cancellation with an ordinary prefix and an oversized discard, requiring ACKs in receive order, one size outcome and the following publish. Semaphore and channel operations are modeled; the metric library's atomic implementation is outside the model, with exact exported overflow counts checked by the owner tests and public broker scenario. |
| SQS receive cancellation (`crates/connectors/sqs/src/source_shuttle_tests.rs`; `crates/connectors/sqs/src/ingestor.rs`) | `shuttle_a_cancelled_read_hands_over_every_message_its_receives_took` lets the host cancel an SQS read at any point while a service task answers its receives through the ingestor's receive seam: a receive that brings a message, or one that brings nothing and a second that brings a message. Every message the service took off the queue for a receive reaches the host, and a cancelled read sends at most the one receive it had already decided to send. `shuttle_a_cancelled_read_waits_for_an_unanswered_receive_only_until_its_budget` lets a service take the read's receive and never answer it while the host cancels the read and a clock expires its deadlines at any point: the read ends only after the host cancelled it, with the typed unanswered-receive failure, or interrupted before it sent a receive. The first check fails under a read that abandons the receive it has in flight when the host cancels it. |
| Redis command pool minimum (`crates/connectors/redis/src/pubsub/pool_minimum/shuttle_tests.rs`; `crates/connectors/redis/src/pubsub/pool_minimum.rs`) | `shuttle_a_connection_closing_while_the_keeper_reads_its_pool_is_restored` closes one of a pool's connections and signals its keeper at any point while the production keeper reads the pool, decides and waits; `shuttle_connections_closing_together_are_all_restored` closes two of three at once, one of them possibly while a restoration is in progress. The keeper registers for the closure signal before it reads the pool, so a closure after its reading wakes the wait that follows, and the pool returns to its minimum after the last closure in every schedule; a closure the keeper slept through leaves the check waiting forever, which fails it. The scripted pool stands in for `bb8`, whose checkouts and accounting are outside the claim; the Redis owner tests run the keeper against a counting Redis stand-in. |
| Emitter batch payloads (`src/runtime/emitter_record_writes_shuttle_tests.rs`) | `shuttle_a_retried_payload_acknowledges_each_fanned_in_member_once_after_every_emitter` and `shuttle_a_sibling_failure_resolves_each_fanned_in_member_once_despite_a_retry` fan two source messages out to a batching emitter and a sibling: each source acknowledgement completes once, successfully only after both emitters confirmed it, and the retry writes the retained payload's first bytes and hands the connector its envelope unchanged. `shuttle_a_cancelled_attempt_leaves_each_member_to_resolve_once` cuts an attempt short at any point and requires the next one to write only unanswered payloads and deliver each rejected member's message error once. `shuttle_a_drain_never_finds_the_emitter_empty_while_a_member_is_retained` races a drain's reads against a stalled write and the force flush that repeats it. |
| Client ingestors (`src/runtime/client_ingestor_shuttle_tests.rs`) | `shuttle_racing_reservations_never_exceed_the_node_budget_and_return_every_byte` races opens that each need more than half the node's producer budget: at most one holds it at a time and every reservation returns its bytes. `shuttle_a_batch_racing_a_quiesce_is_either_counted_by_its_drain_or_refused_undispatched` races the admission fence against an engagement and its drain: no batch is dispatched after the drain concluded. `shuttle_a_closing_producer_answers_every_admitted_batch_once_before_its_release` and `shuttle_an_ending_endpoint_answers_every_batch_once_and_ends_its_producer_last` race a close or an endpoint end against the worker's admission reports and the batches' acknowledgements: every batch is answered exactly once, a close answers each with its real outcome before the release, and an end reports no admitted batch as not admitted and comes last. `shuttle_a_detach_racing_a_clearance_admits_only_a_cleared_batch_and_returns_its_slot` races a forwarded producer's detach against the clearance of its batch while a local producer waits for the window's one slot: the forwarded batch reaches the worker only after its clearance was recorded, and the local batch is admitted whichever comes first, so no slot leaks. `shuttle_an_end_racing_clearances_reports_a_batch_not_admitted_exactly_when_the_worker_never_took_it` races an endpoint end against the clearance of two batches while the worker holds the first without reporting it: each batch is answered once, not admitted exactly when the worker never took it, whether it was still being cleared or cleared and waiting for the worker, and of unknown outcome when it did. |
| Rust client submission slots (`crates/client-core/src/producer/slots_shuttle_tests.rs`) | `shuttle_a_wait_racing_its_resolution_takes_the_outcome_once_and_returns_the_credit`, `shuttle_a_cancelled_wait_loses_neither_the_outcome_nor_the_credit`, and `shuttle_a_release_racing_its_resolution_returns_the_credit_exactly_once` race a submission's resolution against the application's wait, an aborted wait followed by a new one, and a release: the outcome is taken at most once, a cancelled wait leaves it retrievable, and the credit comes back exactly once. |
| Rust client attachment recovery (`crates/client-core/src/producer.rs`, `consumer.rs`) | `shuttle_close_fences_a_producer_restore_started_on_the_same_exchange` and `shuttle_close_fences_a_consumer_restore_started_on_the_same_exchange` race close against beginning restoration. `shuttle_close_fences_a_producer_restore_interrupted_by_another_loss` and `shuttle_close_fences_a_consumer_restore_interrupted_by_another_loss` race close against another loss while restoring. Each check uses the production lifecycle owner and requires the final phase to remain closed, with subsequent restoration refused. `shuttle_a_reply_racing_the_end_of_its_exchange_reports_the_gap_once` races a consumer's reply against the end of its exchange: the interruption is reported once, whichever observes the end first. `shuttle_a_read_parked_while_its_consumer_closes_does_not_outlive_the_close` races a read that parks for delivery against the close: the read ends with the close. `shuttle_a_submission_racing_its_producers_end_observes_the_end` races a submission's wait against its producer's end, and `shuttle_a_reader_racing_a_reported_gap_takes_it_without_another_change` (`subscriptions.rs`) races the application's next read against a subscription event the follower reports: each waiter subscribes before it reads, so neither waits for a later change. |
| Rust client domain clock restoration (`crates/client-core/src/domain_clock.rs`) | `shuttle_a_detach_racing_a_restoration_leaves_no_attachment_of_its_clock` races a detach of an interrupted clock against the production restoration decision, with a modeled session that serves both in the order it receives them and applies each reply through the production owner: once the detach is answered the session holds no attachment of the clock and the client does not follow it, whether the restoration attached first, waited for the detach, or found the clock released. |
| Relay branch presence (`src/runtime/relay_branch_presence_shuttle_tests.rs`) | `shuttle_an_observer_sees_every_owner_step_whole_and_never_an_older_one` races an owner at capacity one through admission, eviction, recreation and release against an observer that registers and reads throughout: every read is a membership the owner published whole and never older than the step the owner had finished. `shuttle_capacity_and_expiry_publish_whole_memberships` keeps every read within the owner's capacity and drops an expired branch from every read after the expiry. `shuttle_a_replaced_owner_never_publishes_over_its_successor` races a predecessor's admissions, expiry and release against its successor's claim: once the claim is visible no read holds a branch only the predecessor admitted, and the successor's branch survives the predecessor's release. |
| Checkpoint replication (`src/runtime/kafka_offset_state.rs`, `src/runtime/state_replication/checkpoint_announcement_shuttle_tests.rs`) | `shuttle_a_replica_acknowledgement_racing_the_quorum_wait_is_never_missed` races a Kafka offset commit's replica quorum wait against its replica's acknowledgement: the wait registers before it reads, so it completes without its deadline, which a Shuttle timeout only reaches when a check triggers it. `shuttle_an_offer_racing_the_end_of_an_announcement_is_always_announced` races a second offer against the announcer of the first finding its replica caught up: the second revision is always announced and acknowledged. `shuttle_a_retired_replication_ends_its_announcer` ends an announcer whose replicated state goes away while its replica never acknowledges. `shuttle_an_announcement_racing_the_replica_wait_is_never_missed` races an owner's announcement against the replica task's synchronization and wait: an announcement that lands before the wait is kept as its permit. `shuttle_checkpoint_announcement_close_cancels_pending_dispatch` exercises the production task owner with close racing the first poll and close after dispatch starts: both cancel a dispatch that never becomes ready and release its retained announcer. |
| Snapshot task and checkpoint writer ownership (`src/runtime/state_replication/tasks_shuttle_tests.rs`) | `shuttle_a_closed_snapshot_task_owner_is_waited_for_until_every_task_has_written` races a runtime closing the production task owner against three branches: one that stops in time, one ended at its stop grace that drops what it held, and one that never tells its snapshot task anything. The wait returns only once every task has made its last write, and a task that waited for its branch alone would hang it, which Shuttle reports as a deadlock. `shuttle_a_stopping_domain_waits_for_its_own_snapshot_tasks_and_keeps_late_ones` races a domain's stop against one branch of it that stops in time, one ended at its grace, a branch of another domain that keeps running until the stop returns, and a branch of the stopping domain that appears and stops while the stop waits and removes the domain's tracker. The stop returns only once both ended branches have written and never waits for the other domain's task, which would hang it; the late task is waited for by that stop or by the next one, which fails if a stop removes a tracker a starting task joined. `shuttle_a_stopping_domain_waits_for_the_checkpoint_writers_it_tells_and_the_ones_dropped` races a domain's stop, which tells one of its placements' checkpoint writers to write, against a placement dropped without its stop, a placement a reassignment aborts, and another domain's writer that keeps running until the runtime closes. The stop returns only once the told and the dropped writers have made their last write, never waits for the other domain's writer, which would hang it, and the aborted writer never writes although its guard goes away after the abort; the closing runtime then has the other domain's writer write. The task body stands in for the write; the trackers and the token are opaque dependencies, while the map's shard locks are Shuttle's, so registration and removal interleave under the exploration. |
| Resolved state replication (`src/runtime/state_replication/routing_shuttle_tests.rs`) | `shuttle_state_replacement_and_retirement_fence_frames_in_flight` exercises the production routing owner: a frame can complete only on its selected lifetime, never change a successor, and cannot enter a route after retirement. ArcSwap internals remain opaque; this check covers owner use and scheduling, with no new Nervix-owned memory-ordering claim. |
| Replica catch-up announcements (`src/runtime/branch_lifecycle_state_shuttle_tests.rs`) | `shuttle_an_announced_branch_racing_the_replica_round_is_never_missed` races an owner's announcement of a branch checkpoint against the replica task taking the pending announcements and waiting for the next: the task takes it whether it lands before the take, between the take and the wait, or during the wait. `shuttle_announcements_of_one_branch_keep_the_newest_pending` delivers two announcements of one branch in either order while the task takes them: an older one never replaces a newer one still pending. |
| Backup capture, restore publication and reclamation (`src/runtime/backup_capture_fence.rs`, `src/runtime/branch_state_capture_shuttle_tests.rs`, `crates/consensus/src/restore.rs`, `materialized_state.rs`) | `shuttle_backup_cut_includes_pre_cut_branch_publication` includes every registered publication before a cut. `shuttle_a_forced_deduplicator_publication_holds_every_key_acknowledged_before_the_cut` and `shuttle_a_forced_window_publication_holds_every_step_acknowledged_before_the_cut` race a branch task's admissions or window steps against the cut's checkpoint request over the branch task's own command and input channels: once the task answers, the keyspace or window the capture reads holds every change acknowledged before the cut asked. The cut waits for the first acknowledged change and then races the second, because an unwaited cut reads before any acknowledgement in almost every schedule and bounds nothing. `shuttle_backup_cut_includes_pre_cut_branch_publication` explores random and PCT schedules only, since its publisher and its cut each spin while waiting for the other. Restored deduplicator and window checkpoints install through the same staged publisher and domain handle clear as the other kinds, so the restore checks below hold their installation order too; the unit test `a_restored_domain_drops_its_deduplicator_and_window_handles` requires that clear to drop both kinds' handles of the restored domain and only its own. `shuttle_restore_publication_and_handle_clear_precede_resume_and_fence_delayed_publishers` drives the applied-state authority guard against a successor generation and RESUME, requiring complete publication and cleared handles before activation. `shuttle_materialized_capture_names_exactly_its_branch_generation` races capture with an update and eviction. `shuttle_replica_synchronization_preserves_the_restored_materialized_revision` races delayed and current owner snapshots. `shuttle_restore_reclamation_holds_the_revision_through_queued_installation` races the reclamation guard with a successor generation and queued installer, requiring the borrowed revision to stay fixed through deletion and preserving the current installation. They register in the shared Shuttle inventory and use its exploration, nondeterminism and replay contract. |
| Domain clock (`src/runtime/domain_clock.rs`) | `shuttle_lifecycle_tests::concurrent_reads_of_one_installed_generation_never_decrease` checks the nondecreasing watermark; `a_clock_bound_to_a_replaced_generation_is_refused_by_revalidation` rejects a superseded generation; `readers_never_observe_an_installation_older_than_one_they_observed` prevents publication regression. `shuttle_delivery_sends_state_before_ticks_without_regressing_progress` explores the production observer and attachment delivery order across accepted ticks, same-generation unassignment and reassignment, and a generation change. `shuttle_an_attach_waiting_for_the_first_installation_observes_its_domains` races an attach's wait and lookup against the node's first installation of the committed domains and requires the lookup to find the domain and its clock. `a_logical_waiter_wakes_when_its_generation_stops`, `a_logical_waiter_wakes_when_its_generation_is_replaced`, `a_logical_waiter_wakes_when_its_domain_is_removed`, and `a_logical_waiter_wakes_when_a_replacement_mapping_reaches_its_deadline` cover each lifecycle wakeup. |

RabbitMQ's production batch reader has two registered Shuttle checks:
`shuttle_a_cancelled_read_hands_over_every_delivery_once_and_in_order` and
`shuttle_a_batch_deadline_racing_cancellation_preserves_every_delivery`. Modeled delivery,
cancellation and physical-deadline events race the real reader; handover plus the unread stream
must contain every delivery exactly once and in order. Lapin deliveries are opaque payload
storage, so these checks make no claim about Lapin's synchronization or network implementation.
The host check `shuttle_individual_settlement_preserves_every_delivery_until_its_own_graph_outcome`
races three independent graph completions through `IndividualBatch`: successful positions are
ACKed individually, only the refused position is NACKed, and no sibling loses its graph waiter.
`shuttle_individual_settlement_keeps_admitted_graph_work_until_transport_recovery` makes one
rejection fail while a sibling's graph wait remains unresolved. That sibling must still deliver
its outcome before recovery, and confirmed plus retained positions partition the original batch.
Ordinary owner tests additionally measure the real drain-root counts during each transport
confirmation and partition confirmed versus retained positions after a failed rejection.
The four `@rabbitmq_consumer_modes` public outlines cover eight one-/three-node examples in the
ordinary and Deloxide active, order and stress selections. All four diagnostic lanes apply to
their shared host lifecycle paths; restore stress keeps its existing selection. No new atomic
publication or memory-ordering contract is introduced, so no new Loom model is required.

The broker ingestor confirmation, refused payload confirmation, reconnect and read interruption
checks, the brokered emitter confirmation window's checks and the MQTT and SQS read cancellation
checks are the interleaving evidence of the
[connector delivery standard](./connector-contract.md#connector-delivery-standard). A change that
adds a protocol on those paths adds its check beside them.

The checks of WASM checkpoint holds and the durability barrier use the same runner and replay
contract. Among them, `shuttle_an_abandoned_writers_synchronization_keeps_the_barrier_until_it_ends`
and `shuttle_an_abandoned_writers_failed_synchronization_still_refuses_what_it_covered` hold a
synchronization round to the storage job that runs it: a writer that stops waiting frees neither the
barrier nor the round's failure. Their state semantics live in the WASM state documentation; they do not turn Shuttle
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

The execution, interconnect, consensus and server crates own a `loom` feature, and each forwards it to the
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
hold model output only, never payloads or secrets. For a pull request labeled `loom`, CI runs the
models in its dedicated `loom` job, which uploads the failure directory, and their qualification
in the loom-qualification jobs. Invariant IDs use ASCII letters, digits, dots and hyphens, so
these paths are valid artifact names; metadata retains the exact Rust test name used for replay.

Every model also registers a weakening that must make it fail. `just test-loom-qualification`
applies each to a copy of the working tree, requires the model to fail with the registered message,
and requires the checkpoint of that failure to replay it. This is what shows a model depends on the
ordering it claims, rather than passing because something else synchronized its threads. A
weakening whose original text no longer appears exactly once fails as well, so changing an owner's
ordering means revisiting its qualification.

Successful qualifications retain `target/loom-qualification/<qualification-id>/` as well: the
counterexample checkpoint, the checkpoint advanced during replay, both outputs and metadata with
the revision, toolchain, exploration bounds and complete registered weakening. CI uploads these
records whether the qualification job passes or fails. To replay a retained counterexample, restore
its recorded revision, apply its recorded weakening in an isolated worktree and run
`just test-loom-replay <directory>`; replay against the unweakened owner is a different check.
The isolated build copy is still removed after qualification, and an unsuccessful control retains
its output and any checkpoint it produced.

Loom models a `SeqCst` access as an acquire or a release, and models only `fence(SeqCst)` exactly. A
protocol in which each side writes one location and then reads the other, such as a dispatch that
raises the in-flight count before it reads the closed flag while a fence closes the gate before it
reads the count, is therefore written so that its correctness follows from acquire and release
alone. The side off the hot path reads the other side's count with a read-modify-write, which reads
the newest value in that count's modification order: either the hot side's read-modify-write came
first and the cold side's read sees it, or the cold side's came first and the hot side's
read-modify-write acquires it, and with it the write the cold side made before. That is exact in the
C11 model and checkable by Loom, and it costs the hot path nothing. The qualification of such a
model weakens the cold side's read-modify-write to a load, which is the store-buffering fault the
protocol rules out.

Loom's own limits bound every claim. It does not model every relaxed behavior the C11 model
permits, and an operation inside a third-party dependency, such as a `triomphe` reference count or
an `arc-swap` publication, is invisible to it and excluded from the claim rather than given a
fictional model. The client batch admission fence reads its quiesce decision from such a
publication, so its model checks the root counts the admission and the drain reach by
read-modify-write, and the publication's own ordering stays outside the claim. A relay's
subscription definition is another: an attachment and a redefinition each put a sequentially
consistent fence between their own write and the read of the other's, but the attachment's write is
its registration in the fan-out's consumer list, an `arc-swap` publication, so the protocol has no
Loom model and its Shuttle check orders the two. The state store's durability barrier orders each
writer's ticket after the write it covers and each synchronization's start before its flush, but
those writes and the flush are the database's, inside its own synchronization, so it has no Loom
model either; its Shuttle checks hold which tickets a synchronization covers. A standalone counter
carries no cross-location claim, whatever its ordering. Relay
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
| `runtime.relay-dispatch-gate.entry-fence` | A relay dispatch fence counts every dispatch that finds the gate open: a dispatch raises the in-flight count before it reads the closed flag, an engagement closes the gate before it reads the count, and both reach the count by read-modify-write, so one observes the other | `loom_a_fence_counts_every_dispatch_that_finds_the_gate_open` (`src/runtime/relay_channel_loom_models.rs`); fails when `let in_flight_dispatches = self.in_flight_dispatches.fetch_add(0, Ordering::AcqRel);` weakened to `let in_flight_dispatches = self.in_flight_dispatches.load(Ordering::Acquire);` |
| `runtime.relay-dispatch-gate.lease-publication` | A fence that takes its lease observes everything each dispatch it counted did under its permit: leaving releases and the fence's read of the count acquires | `loom_a_lease_follows_what_every_counted_dispatch_did_under_its_permit` (`src/runtime/relay_channel_loom_models.rs`); fails when a dispatch's leaving is weakened from `AcqRel` to `Relaxed` |
| `runtime.relay-dispatch-gate.drain-wakeup` | The last dispatch to leave a closed gate wakes the fence waiting for it: either the fence's read of the count sees the dispatch gone, or the dispatch's leaving acquires that read and observes the gate closed | `loom_the_last_dispatch_to_leave_a_closed_gate_wakes_its_fence` (`src/runtime/relay_channel_loom_models.rs`); fails when `let in_flight_dispatches = self.in_flight_dispatches.fetch_add(0, Ordering::AcqRel);` weakened to `let in_flight_dispatches = self.in_flight_dispatches.load(Ordering::Acquire);` |
| `runtime.relay-dispatch-gate.reopen-publication` | A dispatch that finds the gate reopened observes what the protected mutation changed while it was closed: reopening releases and the dispatch's read of the flag acquires | `loom_a_dispatch_that_finds_the_gate_reopened_observes_the_protected_change` (`src/runtime/relay_channel_loom_models.rs`); fails when `.store(!state.engagements.is_empty(), Ordering::Release);` weakened to `.store(!state.engagements.is_empty(), Ordering::Relaxed);` |
| `runtime.relay-fanout.admission-wakeup` | A consumer that frees room a waiting publisher needs either admits it or wakes it: the publisher raises the waiting count before it reads the admission count, the consumer lowers the admission count before it reads the waiting count, and both reach the admission count by read-modify-write, so one observes the other | `loom_a_consumer_freeing_room_admits_or_wakes_the_publisher_waiting_for_it` (`src/runtime/relay_channel_loom_models.rs`); fails when `let admitted = self.admitted.fetch_add(0, Ordering::AcqRel);` weakened to `let admitted = self.admitted.load(Ordering::Acquire);` |
| `runtime.state-assignment.admission-fence` | An operation admitted under a runtime-state binding that a rebind supersedes either observes the new binding and is refused, or finishes before the rebind returns with everything it did visible to the rebinding side: the operation counts itself in before it reads the binding, the rebind publishes the binding before it reads the count, both reach the count by read-modify-write, and finishing releases what the read acquires | `loom_an_operation_admitted_under_a_superseded_binding_never_outlives_its_rebind` (`src/runtime/state_store_loom_models.rs`); fails when `while admitted.fetch_add(0, Ordering::AcqRel) != 0 {` weakened to `while admitted.load(Ordering::Acquire) != 0 {` or `.fetch_sub(1, Ordering::Release)` weakened to `.fetch_sub(1, Ordering::Relaxed)` |
| `runtime.client-ingestor.admission-fence` | A client batch whose admission races a quiesce is either counted by the drain that follows the quiesce or refused before anything is dispatched under it: the admission tracks its root by read-modify-write before it reads the quiesce publication, the quiesce publishes before the drain reads the root counts, and the drain reads each count by read-modify-write, so one side observes the other | `loom_a_client_batch_racing_a_quiesce_is_counted_by_its_drain_or_refused` (`src/runtime/client_ingestor_loom_models.rs`); fails when `self.outstanding.fetch_add(0, Ordering::AcqRel)` weakened to `self.outstanding.load(Ordering::Acquire)` or `self.ownership_handoff_outstanding.fetch_add(0, Ordering::AcqRel)` weakened to `self.ownership_handoff_outstanding.load(Ordering::Acquire)` |
| `runtime.local-drain.moving-batch` | A batch a node publishes into a relay while a drain observes the domain is never missing from the observation: the relay admits the batch before the node lets it go, the observation reads every relay's admission sequence before and after the node's count, and acquiring the node's release makes the admission visible to the closing read | `loom_a_batch_published_while_a_drain_observes_is_never_missing_from_it` (`src/runtime/local_drain_loom_models.rs`); fails when `self.admitted.load(Ordering::Acquire)` weakened to `self.admitted.load(Ordering::Relaxed)` |
| `runtime.local-drain.taken-batch` | A batch a consumer takes from a relay while a drain observes the domain is never missing from the observation: the consumer's node counts the batch before the consumer returns the relay's admission, the observation reads the relay before the node, and returning the admission releases the node's count to the observation that acquires it | `loom_a_batch_a_consumer_takes_while_a_drain_observes_is_never_missing_from_it` (`src/runtime/local_drain_loom_models.rs`); fails when the consumer queue's return of an admission is weakened from `AcqRel` to `Relaxed` |
| `runtime.local-drain.handed-on-batch` | A batch a relay's owner hands on to its consumer while a drain observes the domain is never missing from the observation: the consumer admits the batch before the owner hands it on, the observation reads the relay's transit before its consumers' queues, and handing on releases the consumer's admission to the observation that acquires it | `loom_a_batch_its_owner_hands_on_while_a_drain_observes_is_never_missing_from_it` (`src/runtime/local_drain_loom_models.rs`); fails when `self.handed_on.fetch_add(1, Ordering::Release);` weakened to `self.handed_on.fetch_add(1, Ordering::Relaxed);` |
| `runtime.local-drain.flush-completion` | A message a force flush resumed is admitted work to a drain observation that sees the flush complete: the participant resumes the message before it completes its obligation, the observation reads the obligations before the node's work, and acquiring the completion makes the resumed message visible | `loom_a_message_a_flush_resumed_is_seen_by_a_drain_that_sees_the_flush_complete` (`src/runtime/local_drain_loom_models.rs`); fails when `self.force_flushes.load(Ordering::Acquire)` weakened to `self.force_flushes.load(Ordering::Relaxed)` |
| `runtime.kafka-offset-state.snapshot-revision` | A Kafka offset snapshot stamped with a commit's revision encodes that commit's offset: the commit stores the partition's offset before it advances the revision, the snapshot reads the revision before the offsets, and advancing releases what reading the revision acquires | `loom_a_snapshot_carrying_a_commits_revision_carries_its_offset` (`src/runtime/kafka_offset_state.rs`); fails when `LsmSequence::advance` is weakened from `SeqCst` to `Relaxed` |
| `server.backup.cut-includes-publication` | A quiesced backup cut waits for every branch state publication that registered before the cut closed, then reads every write those publications completed: closing and registering are read-modify-writes of one word, a publication leaves with a release and the cut's wait acquires | `loom_a_cut_includes_every_registered_publication` (`src/runtime/backup_capture_fence.rs`); fails when a publication's leaving is weakened from `Release` to `Relaxed`, or the cut's wait from `Acquire` to `Relaxed` |
| `server.backup.cut-generation` | A branch state publisher that enters after a backup cut acquires the cut generation and sees the coordinator's writes before it: reopening extends the closing release sequence, and a publisher's registration acquires it | `loom_a_publisher_acquires_the_cut_generation` (`src/runtime/backup_capture_fence.rs`); fails when the registration is weakened from `AcqRel` to `Release`, or the closing from `Release` to `Relaxed` |
| `harness.loom.coroutine-stack` | A model body and the participants it spawns run debug frames with allocation instrumentation on the coroutine stacks they request, while Loom's coordinator only starts and joins the body | `loom_model_coroutines_support_instrumented_debug_frames` (`crates/model-harness/src/loom.rs`); fails when the stack request is cut to `4_096` |

The cancellation protocol these models check is the bounded executor's. `Cancellation::armed`
creates both ends of one job's cancellation: the executor keeps the obligation while its caller
awaits the job and moves the signal into the job, which checks it between its bounded units.
Dropping the awaiting caller drops the obligation and cancels the job, which keeps its memory charge
until it returns; observing the job's value disarms the obligation first, so an ordinary completion
never reports itself as cancelled. The executor keeps admission, charges and cancellation policy;
the protocol it runs is the one the models explore.

Connector entity regrouping preserves the existing production owners and synchronization protocols.
The host composition lives in `runtime::ingestors::<connector>` and
`runtime::emitter_connectors::<connector>`; concrete engines live below entity modules in their
connector crates. Kind declarations and startup label derivation add no hot-path shared state,
lock, concurrent map or memory-ordering protocol. OTEL entities delegate to one private encoder.
The HTTP and WebSockets endpoint wrappers share an intake owner whose drop still ends the exact
binding lifetime. Their existing host Shuttle checks and lifecycle Deloxide lanes remain the
concurrency evidence; moving these owners requires current inventory paths, not a new protocol model.

## Live Cluster Policy

Runtime timing and resource bounds are immutable publications through the governed `ArcSwap`.
Control-plane installation remains serialized by runtime admission. A new client attachment
captures its grant and each worker batch keeps that attachment's row bound; no record consults a
shared policy map. HTTP preparation captures its typed header-byte and field-count bounds once per publication.
Snapshot owners observe changed intervals at the existing bounded replica-poll boundary, without
a new wait/notify protocol or changes to checkpoint/acknowledgement ownership.

A live node-unavailability interval change wakes the existing control-plane health watch. Retained
watchers read the installed interval again when deriving their next deadline. The production-owner
`cluster::peer_health_watcher::shuttle_tests::shuttle_a_policy_change_racing_a_retained_health_wait_is_never_missed`
check, registered in the Shuttle inventory, races installation against wait registration without a
peer deadline: a lost wake leaves it deadlocked. It runs 1,000 random schedules and 1,000 PCT schedules
at depth three, each with the harness's 10,000-step limit. These searches supply the modeled random
data that fair `select!` polling requires. The separate nondeterminism run repeats 100 random
schedules twice for each search. Paused-clock unit tests separately check elapsed transition timing
and unchanged publication. This check establishes notification liveness for the explored schedules;
it makes no memory-ordering claim about the governed `ArcSwap` implementation.

The application's `NodeInitialization` uses the primitive once-publication owner to open public
policy-dependent work after live-policy catch-up and handler registration. Session and configured
HTTP intake read that same publication before admission; it changes once per process and carries
no row, batch or persisted state. The returning-parameter public scenario holds initial discovery
through the existing test-pause owner and exercises observation, refusal and release. Its async
pause, network waits and standard-library once publication remain outside the blocking-lock detector.
The diagnostic parameter workloads reach the surrounding tracked owners; the public scenario and
external partition recovery verify admission and observation during an isolated process restart.
