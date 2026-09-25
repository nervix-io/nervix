# Domain Clock

The domain clock is the time boundary between Nervix's control plane and data plane. The control
plane commits what time means for a domain; every data-plane node installs that definition and
uses a capability bound to the exact domain lifecycle generation. Expression engines, processors,
and connectors receive time from that capability instead of choosing a clock themselves.

This chapter describes the internal architecture. See [Domains And Time](./domains-and-time.md)
for the NSPL surface and operator-facing behavior.

The central rule is that a paced clock is a committed mapping, not a stream of ticks. Each node
projects its own current UTC observation through the same mapping. Tick progress records which
logical boundary the current authority has reached, but it never replaces the mapping or advances
a node's readable time.

## Replicated And Node-Local State

The complete clock contract is split across replicated control-plane state and bounded node-local
state:

| State | Meaning | Ownership and durability |
| --- | --- | --- |
| Domain pace | `PACED` with a positive `PERIOD` and nonnegative `SKEW`, or `UNPACED` | Part of the replicated domain configuration |
| Lifecycle generation | The domain's `start_version`, advanced by every committed `START` | Replicated with the domain lifecycle record |
| Paced mapping | Physical UTC anchor, logical origin, and positive finite time rate | Replicated for a running paced generation |
| Authority fence | Monotonic authority revision and an optional node identity, including its process incarnation | Replicated separately for each paced domain |
| Progress | Generation, authority revision and identity, plus the latest produced tick | Transient and replaceable; sent over the interconnect and never persisted |
| Installed clock | Missing, stopped, uninstalled, installed-unpaced, or installed-paced state | Node-local runtime state derived from the committed revision |
| Last read and observed progress | The node's nondecreasing read watermark and newest accepted tick observation | Node-local in-memory state |

The lifecycle generation and authority revision fence different changes. A new `START` increments
the generation and establishes a new mapping. Assigning, moving, or revoking the producer
increments the authority revision without changing the mapping or generation. Including the node
incarnation in the authority identity prevents a restarted process with the same node name from
acting as the preceding process.

An unpaced domain still binds a domain-and-generation clock capability, but its source is actual
UTC. It has no paced mapping, authority, tick production, or admission window.

## Paced-Time Projection

A paced mapping contains:

- `wall_started_at`: the UTC instant sampled when the generation was established
- `logical_start`: the logical instant requested by the domain start
- `time_rate`: the positive finite ratio of logical time to physical elapsed time

For a current UTC observation `wall_now`, the vocabulary model computes:

```text
elapsed     = max(0, wall_now - wall_started_at)
logical_now = logical_start + floor(elapsed * time_rate)
```

Rounding down prevents a read from claiming a logical nanosecond that has not been reached. A UTC
observation before the physical anchor contributes zero elapsed time. The runtime then raises the
node's read watermark for the installed generation to the projection and returns the watermark, so
a local clock adjustment cannot make normal reads move backward.

The inverse operation converts a future logical target into a physical duration:

```text
wall_wait = ceil((target_logical - current_logical) / time_rate)
```

A positive result is at least one nanosecond. Rounding up prevents a logical deadline from firing
early. Both operations use checked duration and timestamp arithmetic. Origins, projections, and
tick boundaries must remain within signed Unix-nanosecond timestamps; periods and skew must fit in
unsigned 64-bit nanosecond durations. A value that exceeds those bounds produces a typed error.

`TIME RATE` affects only the relationship between elapsed physical time and logical time. It does
not scale `PERIOD`, `SKEW`, preserved source timestamps, or a duration after that duration has been
classified as physical policy.

## Establishing A Generation

A direct or transactional `START` reaches the same replicated lifecycle transition. A transaction
resolves the concrete start, paced mapping, and initial authority while its complete commit plan is
admitted, then persists that decision with the frozen step. Restart and leadership recovery execute
that stored decision against its captured control-plane inputs instead of sampling a new anchor or
selecting a different authority.

1. The command handler reads the current domain and samples UTC once for the physical anchor.
2. It resolves the requested start into a concrete logical origin and rate. `START AT NOW` becomes
   a concrete timestamp before it is committed; `START AT <timestamp>` preserves the supplied
   logical origin. A resume uses readable current domain time when it is available and otherwise
   resolves from the sampled UTC at rate one.
3. A paced start selects one live voter incarnation as its initial authority. The command fails if
   no eligible authority exists.
4. One consensus command marks the domain running, increments its lifecycle generation, stores the
   mapping, and advances the authority fence. An unpaced start increments the generation without
   storing a paced mapping or authority.
5. Every node applies the resulting control-plane revision to its local domain-clock lifecycle
   before materializing executable work from that revision.
6. The selected authority waits until every currently live node reports that it installed at least
   that runtime revision before producing progress.

The mapping is therefore available before a graph task can bind its clock. A node joining later
installs the existing mapping, logical origin, rate, generation, and authority fence. Its join time
does not become a new clock anchor.

`STOP` commits the inverse lifecycle boundary. It marks the domain stopped, removes the active
mapping, and advances the authority fence to an unassigned state in the same transition. The
runtime clears accepted progress for the generation and wakes bound waiters, which observe a typed
stopped or stale-generation error. A later `START` creates another generation.

The internal pause used while altering a running model does not establish a generation. It keeps
the mapping and authority active while ingestion and generators are withheld, so logical time and
already-armed deadlines continue through the quiesce cycle.

## Authority Selection And Reconciliation

The current consensus leader reconciles authority ownership whenever domain state, Raft topology,
or effective node availability changes. Candidate identities are the intersection of live gossip
incarnations and current Raft voters. Duplicate observations for one node name collapse to the
newest incarnation.

Selection is deterministic. Candidates are ordered by node name, the domain name is hashed, and
the hash selects one position in that ordered set. Every leader presented with the same domain and
candidate set therefore proposes the same owner, while domains can distribute across the voter
set.

The replicated reconciliation is a compare-and-set operation. It changes the authority only if
the domain still has the expected lifecycle generation, is still paced and active, and still has
the expected authority fence. This stops work computed by a preceding leader or topology view from
overwriting newer state. Every successful reassignment or revocation increments the revision.

Leadership transfer alone does not alter the mapping or authority. Losing the authority's exact
node incarnation causes the leader to select and commit another eligible incarnation. If none is
eligible, the authority becomes unassigned and the paced clock is unavailable for execution until
an owner is committed again; the mapping itself remains replicated.

On each node, a producer task exists only when all of these values agree with committed runtime
state:

- domain name
- lifecycle generation
- mapping and period
- authority revision
- full authority node identity and incarnation

An obsolete producer is cancelled and retired before that node starts a successor for the same
domain. The old task retains its immutable fence while it exits, so even an in-flight delivery
cannot impersonate the replacement authority.

## Tick Production And Progress Delivery

Tick identifiers are one-based. Tick 1 is the logical origin, and tick `n` is at:

```text
logical_start + (n - 1) * period
```

The authority projects current UTC through the committed mapping and determines the latest due
tick by direct arithmetic. If scheduling delay spans several periods, it emits only that latest
due boundary and continues with the first future boundary. It never loops through or queues every
missed tick. A newly assigned authority can consequently reconstruct the current frontier without
persisting the previous producer's counter.

Each progress report carries the lifecycle generation, authority revision, full authority
identity, tick id, logical boundary, authority UTC observation, and period. The authority applies
the report to its own runtime directly. Remote delivery uses the typed `domain_clock_progress`
HTTP/2 management request described by [Cluster Interconnect](./interconnect.md#domain-clock-progress).

For each ready remote node and domain, the producer owns one delivery loop with at most one request
in flight and one replaceable pending report. Publishing another tick overwrites the pending value,
so a fast clock or slow peer cannot create an unbounded queue. A node-wide progress subquota bounds
concurrent attempts independently of health, relay admission, cancellation, and terminal outcomes.

Targets must be both live and ready for the runtime revision that installed the generation. A new
or reconnected target receives the producer's newest retained report when it becomes ready. Each
request has a two-second physical deadline. Failure waits on a 200-millisecond physical backoff and
then retries the newest report. Cancelling the authority stops an in-flight request and its backoff.

A process stop request does not cancel clock authority reconciliation, progress production,
progress delivery, or local installation. Those services remain active during application drain
support so work admitted before listener shutdown can still take execution snapshots and wait on
logical deadlines, and so a committed ownership handoff can install the schedule and clock state it
needs. Terminal teardown cancels them only after drain support completes or explicitly reports
abandonment. See [Shutdown And Recovery](./shutdown.md) for the phases these services span.

The receiver first gets the reporting node name from the mutually authenticated interconnect. It
accepts progress only when the domain already exists and is not stopped, and the report matches the
committed generation, authority revision, full authority identity, authenticated node name, and a
nonzero tick id. A report is newer only when its tick id advances and its authority UTC observation
does not precede the retained one. Duplicate, reordered, delayed, and superseded reports are
ignored.

The receiver retains only the newest accepted tick observation. It does not use the report's
logical timestamp, UTC timestamp, or period to replace the installed mapping. The unit response
therefore means that the receiver evaluated the report against its current fence; it does not mean
that the report established time. Progress can never create a missing domain.

## Local Installation And Bound Capabilities

Each runtime node keeps one shared lifecycle allocation per domain, and graph tasks hold thin clock
handles bound to its domain and lifecycle generation. Control-plane synchronization publishes each
installation change into that allocation by atomically replacing the complete installation, then
notifies logical waiters. This makes a lifecycle change visible to every existing handle and gives
logical waiters one notification source to observe.

The local installation states have explicit meanings:

| Installation | Read result |
| --- | --- |
| Missing | The domain is absent from this runtime |
| Stopped | The generation exists but cannot execute domain work |
| Uninstalled | A paced active generation lacks either its mapping or assigned authority |
| Installed unpaced | Reads actual UTC through the domain-bound capability |
| Installed paced | Projects actual UTC through the committed mapping, period, and skew |

Binding an active clock fails for missing, stopped, or uninstalled state. A handle that was bound
before a later `START` fails with a stale-generation result. Reads and logical waits revalidate the
shared installation; no failure path substitutes wall time for a paced clock. Passive graph state
for a stopped domain may retain a generation-bound handle for ownership purposes, but attempts to
read it still fail as stopped.

Reads take no lock. A read loads the published installation, validates the handle's generation and
the installation state against it, projects actual UTC through the source that installation holds,
and raises the read watermark published with it using one atomic maximum. A concurrent read
therefore observes either the complete preceding installation or the complete replacement, and a
replacement never alters an installation that an in-flight read already holds. Synchronization
derives each replacement from the installation it replaces and publishes it only while that
installation is still current; synchronizing an unchanged installation publishes nothing and wakes
no waiter.

Each read watermark covers one uninterrupted installation of a generation. A replacement that keeps
the same generation installed shares its predecessor's watermark, so reads of that generation do not
decrease even when they race the replacement. Any other replacement starts a new watermark. A read
that loaded an earlier generation can therefore raise only that generation's watermark and cannot
clamp reads of a later generation, and a generation that becomes uninstalled starts from a new
watermark when its mapping and authority are installed again.

Applying a cluster revision synchronizes all domain lifecycles before applying its schedule. A
joining node therefore cannot instantiate work and then discover that its clock mapping is absent.
Removing a domain marks the shared lifecycle missing before node-local state is discarded.

## Execution-Time Snapshots

The clock is sampled once when a unit of domain work is accepted. The resulting execution snapshot
contains the validated lifecycle generation and one timestamp. The runtime passes that snapshot to
VM, Roto, and WASM execution instead of giving those engines a context-free time source.

One snapshot is shared by all expressions in that unit, including construction, routing,
deduplication, ordering, correlation, windows, inferencer mappings, emitter filters, `VALUES`, and
subscription predicates. This prevents two expressions in one operation from observing different
logical instants. A paced domain may place its logical time before the Unix epoch, and expressions
consume the snapshot as it is: `now()` returns it, while `uuid_v7()`, whose timestamp field starts
at the epoch, fails each message that evaluates it rather than encoding a different instant.
Message-error handling retains the failing operation's snapshot. Work that begins later, such as a
processor flush, scheduled callback, or message released from materialized `REQUIRED WAIT`,
receives a fresh snapshot for that execution.

A coordinated WASM state reset does not change the domain lifecycle generation, mapping, authority,
or logical frontier. Preparing a fresh guest and saving its initial state each use a snapshot from
the currently installed domain clock. The snapshot belongs only to that initialization operation;
it is not inherited from the branch instance being replaced. Publication cancels the old branch
instance and all timeout handles it owned. A fresh initialization may request its own timeouts, but
no deadline armed by the replaced instance can fire in the new state lifetime even though domain
logical time continued across the reset. See [Coordinated Reset](./wasm-state.md#coordinated-reset).

Checkpoint and reset inspection samples existing state without taking a domain execution snapshot
or advancing the logical frontier. A reported checkpoint revision and reset generation describe
durability and lifetime identity, not domain time; neither may establish or alter the clock used
by a later guest callback.

An NSPL reset uses this same clock contract. Repeating its durable command reference resumes the
same guest-state lifetime replacement without changing the domain clock mapping.

Generated records use the snapshot assigned to their generating operation. Buffered emitter
batches and their retries retain the snapshot from acceptance, while external observation fields
whose contract is actual UTC obtain that value at the shared source-host intake boundary or their
connector boundary.

## Admission Windows

Paced ingestion obtains its execution time and admission window from one clock read. Given the
installed logical origin `origin`, period `period`, skew `skew`, and projected `now`, the reached
frontier is:

```text
frontier = floor((now - origin) / period)
```

The eligible centers are the newest 256 positions ending at that frontier. Before a full history
exists, retention clamps at position zero; afterwards the first retained position is
`frontier - 255`. An event is admitted when its timestamp is within the inclusive `skew` distance
of any retained center.

The window is reconstructed in constant space from the mapping. It does not scan or depend on
received progress history. Delayed progress therefore cannot change admission, and tolerance past
the latest center cannot make a future center eligible. `TIMESTAMP NOW` uses the same snapshot that
created the window. `TIMESTAMP AT <field>` preserves the decoded event timestamp before testing it
against that window.

Unpaced ingestion has no admission window. Its clock snapshot still supplies delivery time, while
an explicit event timestamp or connector-owned source timestamp remains preserved source time.

## Logical And Physical Deadlines

A logical deadline carries its domain, lifecycle generation, and target logical timestamp. Waiting
for it follows a loop:

1. Read and revalidate the bound clock.
2. Return a fresh execution snapshot if the logical target is already due.
3. Convert the remaining logical delta to a physical duration using the installed mapping.
4. Arm a process-monotonic deadline for that duration.
5. Wake on the deadline, lifecycle notification, or cancellation, then evaluate the logical
   predicate again.

The monotonic timer is an implementation mechanism for waiting; the due predicate remains in the
domain's logical coordinate. Lifecycle changes are checked after every wake. Cancellation is a
separate typed outcome rather than a clock read.

Reset coordination and its ten-second initial-checkpoint deadline are physical, monotonic bounds.
They do not wait for, pause, or re-anchor logical time. A reset that fails before generation
publication restores the old branch instance with its existing logical deadlines. After generation
publication, recovery completes the fresh lifetime and never recreates the old instance or its
timers.

Recurring domain cadence is anchored to its initial logical schedule. The source host waits for
each HTTP or Prometheus polling occurrence and passes its scheduled logical instant into the
connector's poll operation; connector crates never bind or wait on a domain clock. HTTP polling and
generator cadence begin immediately, while Prometheus polling begins after one interval. When work
misses multiple occurrences, the cadence returns the newest due instant once and advances directly
to the first future boundary. Consumers that need both meanings keep the scheduled due instant
separate from the fresh execution snapshot taken when work actually runs.

The architecture keeps four time classes distinct:

| Time class | Internal use |
| --- | --- |
| Domain logical time | Expressions, explicit domain cadence, collection and flush cadence, TTL, retention, window completion, and guest-requested timeouts |
| Preserved source time | External event timestamps, broker metadata, and window membership inputs |
| Physical monotonic time | Network deadlines, retry and backoff, acknowledgements, cancellation, shutdown, drain, state checkpoint deadlines, safety timeouts, and physical batching minima |
| Actual UTC | Paced projection input, unpaced domain reads, administrative records, security validity, and explicitly external observation fields |

Logical deadlines and physical deadlines are different types and cannot be interchanged. Actual
UTC enters the data plane through a dedicated boundary, and expression engines cannot read it
directly. That boundary and the capability that arms physical deadlines belong to the connector
contract, which the runtime and every connector crate share, so a connector stamps arrival time
through the same owner the runtime uses. Repository validation checks these ownership boundaries
across the workspace, so a new runtime or connector path must choose its time class explicitly.

## Recovery And Distributed Guarantees

The paced mapping, lifecycle generation, and authority fence recover from consensus state. Tick
progress, delivery loops, local read watermarks, and process-monotonic deadlines do not. After a
restart, nodes install the committed mapping and the leader reconciles an authority for the current
live voter incarnations. The producer recomputes the latest due boundary from the mapping.

This produces the following failure behavior:

| Event | Result |
| --- | --- |
| Leader transfer | The committed mapping and authority remain unchanged unless the effective candidate set also requires reconciliation |
| Authority loss or restart | Consensus advances the authority revision and assigns another live voter incarnation; the mapping and lifecycle generation stay fixed |
| Node join or reconnect | The node installs the committed generation before execution and receives the newest retained progress after readiness |
| Delayed old progress | Generation, revision, identity, and authenticated-peer checks discard it |
| `STOP` followed by `START` | Stop revokes the authority; start increments the generation and commits a new mapping and fence |
| Mapping or tick arithmetic overflow | Projection, deadline conversion, boundary, and tick-id operations report typed clock errors instead of wrapping or changing anchors |
| Missing mapping or authority | Paced access fails as uninstalled; it never falls back to actual UTC |

All nodes project from their local UTC observations. The shared mapping and per-node nondecrease
rule keep one generation coherent, but they do not make simultaneous reads on different hosts
identical and do not create a distributed total order. Operators must synchronize and monitor host
UTC. Host offset appears in each node's projection and is multiplied by `TIME RATE`; event `SKEW`
is an admission tolerance rather than a clock-synchronization budget.
