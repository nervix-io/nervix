# Domain Clock

*Plexus, the Nervix graph definition language.*

The domain clock is the time boundary between Nervix's control plane and data plane. The control
plane commits what time means for a domain; every data-plane node installs that definition and
uses a capability bound to the exact domain lifecycle generation. Expression engines, processors,
and connectors receive time from that capability instead of choosing a clock themselves.

This chapter describes the internal architecture. See [Domains And Time](./domains-and-time.md)
for the Plexus surface and operator-facing behavior.
Explicit transactional `START` and `STOP` establish or revoke generations at ordered step
boundaries; [Transaction Quiescence And Impact Inspection](./transaction-quiescence.md) owns how
those lifecycle steps affect later planned scopes and the retained impact report.

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

`RESTORE ... RESUME` activates the archived start generation and latest start point after complete
state publication. A paced restore installs its committed wall/logical mapping and time rate
verbatim; elapsed downtime advances its projection. The leader selects a new authority from this
cluster's live voter incarnations. Archived authority and frontier never establish that authority
or reset the mapping. A paced archive missing its mapping is refused before restore admission.
Default restoration leaves the domain stopped with no installed mapping; a subsequent `START`
establishes a new generation by the procedure below. See
[Backup And Restore](backup-and-restore.md#what-a-restore-recreates).

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
6. The selected authority produces progress once its own node has installed the mapping and
   authority fence. It delivers progress to each live peer after that peer advertises that it
   installed the domain clocks of the required revision; neither waits for graph activation.

The mapping is therefore available before a graph task can bind its clock. A node joining later
installs the existing mapping, logical origin, rate, generation, and authority fence. Its join time
does not become a new clock anchor.

`STOP` commits the inverse lifecycle boundary. It marks the domain stopped, removes the active
mapping, and advances the authority fence to an unassigned state in the same transition. The
runtime clears accepted progress for the generation and wakes bound waiters, which observe a typed
stopped or stale-generation error. A later `START` creates another generation.

A producer of a [client ingestor](./ingestors.md#client-ingestors) is bound to the generation it
attached under, which its open reply reports. `STOP` ends it, and so does an execution installed
under a later generation, both as `domain stopped`; a producer never carries batches from one
generation into the next.

The internal pause used while altering a running model does not establish a generation. It keeps
the mapping and authority active while ingestion and generators are withheld, so logical time and
already-armed deadlines continue through the quiesce cycle.

## Authority Selection And Reconciliation

The current consensus leader reconciles authority ownership whenever domain state, Raft topology,
or effective node availability changes. Candidate identities are the intersection of effectively
available node incarnations and current Raft voters. Effective availability retains an established
incarnation through a gossip loss while its latest application-health observation remains within
the node-unavailability interval and has not marked it unavailable. If that observation becomes
stale, the node follows gossip's liveness verdict. Duplicate observations for one node name
collapse to the newest incarnation.

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

A reconciliation proposal that does not commit is proposed again 200 milliseconds later, for as
long as the node remains leader, without waiting for another change in domain state, Raft topology
or availability. A newly elected leader refuses writes until a quorum acknowledges its leadership,
so the first proposal it makes on taking over can be refused. When the lost authority also held
consensus leadership, the survivors first elect a leader, and that leader's reconciliation commits
the replacement on a later attempt rather than when some unrelated state next changes.

Application-health probes finish their bounded attempts despite concurrent gossip updates. This
allows continuous failures to exclude a stopped authority even after its advertised endpoint has
changed; a stale healthy observation must not leave it indefinitely eligible for clock production.

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

Every node installs a committed reassignment ahead of its graph installation. The node's domain
clock owner applies each committed revision directly: when a domain's installed generation, status,
configuration, start point and mapping already equal the committed ones, it installs the committed
authority fence at once, which starts or retires the local producer and fences received progress.
Graph installation can wait on a node that left, for example while a forced failover drains a relay
toward it, and it does not hold back the clock takeover on the replacement authority or on any
receiver. Every other lifecycle change, such as a new generation, a stop, or an added or removed
domain, remains with graph installation, which installs a revision's lifecycles before its work.
Each node records the committed revision whose lifecycle each domain holds and never replaces it
with an older revision's, so a graph installation that completes after a newer fence was installed
cannot restore the superseded authority.

After reassignment, the replacement authority reconstructs the frontier from the unchanged mapping;
each receiver's own installation of the fence controls when it can receive progress.

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
After an actual emission, the authority waits at least one period divided by the rate in physical
time before emitting another tick. This keeps consecutive observations spaced correctly when tick
1 was delayed after `START`; a late next boundary is coalesced to the newest due id as usual.

Each progress report carries the lifecycle generation, authority revision, full authority
identity, tick id, logical boundary, authority UTC observation, and period. The authority applies
the report to its own runtime directly. Remote delivery uses the typed `domain_clock_progress`
HTTP/2 management request described by [Cluster Interconnect](./interconnect.md#domain-clock-progress).

For each installed remote node and domain, the producer owns one delivery loop with at most one
request in flight and one replaceable pending report. Publishing another tick overwrites the pending
value, so a fast clock or slow peer cannot create an unbounded queue. A node-wide progress subquota
bounds concurrent attempts independently of health, relay admission, cancellation, and terminal
outcomes.

Targets must be live and must advertise that their domain clocks match the committed runtime
revision the producer started from, or a later one. Each node's domain clock owner follows every
committed domain and schedule change, and it advertises a runtime revision once every domain clock
the node holds, authority fence included, equals that revision's committed lifecycle. It advertises
neither graph preparation nor activation, so a target whose graph installation waits still receives
progress. A lifecycle change that only graph installation installs, such as a new generation of
another domain, holds the advertisement until graph installation installs it. A new or reconnected
target receives the producer's newest retained report when its advertisement reaches the required
revision. Each request has a two-second physical deadline. Failure waits on a 200-millisecond
physical backoff and then retries the newest report. Cancelling the authority stops an in-flight
request and its backoff.

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

The receiver publishes the newest accepted tick id, logical boundary, and authority UTC observation
through a per-domain watch. The compare and replacement are serialized by the watch so concurrent
deliveries cannot replace newer progress with an older report. Generation changes and stops clear
it with `send_replace`; a late observer subscribes before reading and sees the current value at
once. Progress does not use the report's logical timestamp, UTC timestamp, or period to replace the
installed mapping. The unit response
therefore means that the receiver evaluated the report against its current fence; it does not mean
that the report established time. Progress can never create a missing domain.

## Local Installation And Bound Capabilities

Each runtime node keeps one shared lifecycle allocation per domain, and graph tasks hold thin clock
handles bound to its domain and lifecycle generation. Control-plane synchronization publishes each
installation change into that allocation by atomically replacing the complete installation, then
notifies logical waiters. This makes a lifecycle change visible to every existing handle and gives
logical waiters one notification source to observe. The same immutable publication contains its
pause state, lifecycle generation and last start point. Ingest groups read pause/admission and time
from one publication. Kafka domain-offset polls and filtered subscriptions retain the lifecycle
allocation; a subscription reads its current installed generation even when it opened before
START. Generators, processors and materialized relay tasks retain their generation-bound clock. These
reads do not resolve the runtime's domain or execution registry again. A paused paced domain keeps
its installed mapping while publishing pause, and a later generation still invalidates a clock
bound to its predecessor.

Kafka domain offsets retain the generation in which an explicit start was applied. Resuming an
instance in that generation uses its recorded positions; a partition without one uses the original
start timestamp. A later explicit start applies under its new generation. This source-position
state travels with native checkpoints and backups, independently of clock authority or mapping;
[Kafka ingestion](./ingestors.md#kafka) owns its acknowledgement and recovery contract.

A pause-only publication does not notify installation observers when it keeps the installed
mapping unchanged; intake sees pause through the lifecycle publication while execution keeps its
existing logical waiters and mapping.

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

Ownership handoff and forced recovery of a passive WASM revision transfer its validated durable
checkpoint inventory without guest callbacks or clock reads. Guest restoration waits for `START`
and its active clock generation; an unreadable stopped clock does not invalidate the saved state.

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

## Session Attachment

A client session follows a domain's clock with `ATTACH DOMAIN CLOCK` and stops following it with
`DETACH DOMAIN CLOCK`. The attachment observes the installation and the accepted tick progress of
the node serving the session. It adds no interconnect traffic: every node serves its sessions from
its own installation and accepted progress, and no node advertises its attachments. Nothing about
an attachment is persisted. [Domain Clock Attachment](./sessions.md#domain-clock-attachment)
states the public contract,
[Client Session Protocol](./client-session-protocol.md#domain-clock-attachment) how the attachment
travels in the protocol, and the
[Client Implementation Manual](./client-implementation-manual.md#domain-clock-attachment) what
every client implementation must do with it. This section owns how an installation and accepted
progress become what an attached client observes.

### The Observer

The runtime exposes an observer of one domain's lifecycle publication and progress watch. It
subscribes to both notifications before its first read, so an installation or a tick published
after a read always wakes it. It maps each installation to the public observed clock, a vocabulary
model shared by the server and the Rust client:

| Installation | Observed clock |
| --- | --- |
| Missing | None: the attachment ends |
| Stopped | Stopped, with its generation |
| Uninstalled | Uninstalled, with its generation and no mapping |
| Installed unpaced | Unpaced, with its generation |
| Installed paced | Paced, with its generation, period, skew, and committed mapping |

An observed clock carries no authority identity or revision. An authority move that keeps the
installation therefore publishes nothing to an attached client.

The observer reads a tick only while the installation is paced and of that tick's generation. It
adds the serving node's own logical reading, taken as the tick frame is built: one snapshot of the
installed generation, read through the same watermarked read that execution uses. The observer
samples accepted progress before taking that reading. A process pause between the samples can
therefore pair an earlier tick with a much later serving reading, and transport delay can deliver
both samples later still. Their difference is not a bound on projection error or tick freshness;
accepted progress does not bound the independently projected readable time.

The observer retains the progress watch's sender while the attachment lives, so removing the
domain closes neither wait before the lifecycle publishes missing and wakes delivery to send the
end frame.

### Attachments And Their Delivery

A session's attachments belong to its ordered lane. The lane holds them in one map keyed by
domain, so a session follows each domain clock at most once, and runs one delivery task per
attachment. Attach and detach run on that lane in order with the session's commands, and both are
refused while a transaction is open.

An attach first waits until the node has installed the committed domains since it started, or
until the session ends. Before that installation every domain is missing from the node's runtime,
so the node cannot tell a domain the cluster lacks from one it has not installed yet. Waiting keeps
a node that is still starting, as after a restart, from refusing a committed domain as not found.
The runtime counts its installations through a watch and advances the count only after an
installation has put every domain in place, so the lookup that follows the wait finds each domain
that installation holds. A domain the lookup misses is one the installed committed state lacks,
and the reply refuses it as not found. The wait has no bound of its own. The attach is admitted
before it waits, so a cancellation of the request does not end the wait, and the session's later
ordered requests wait behind it; short of the installation, only the end of the session ends it.

The attach reply carries the observation read when the observer was created. Delivery starts only
once that reply is queued, remembering the observation the reply carried. A reply that was not
queued, because the request was cancelled, the session ended or the reply did not fit, announced
nothing, and nothing is delivered after it. The task first sends any accepted tick of the reply's
generation, if one exists. On each wake it reads the newest installation and queues a state frame
on the session's control lane only when the observation differs from the one the client last
received. Replacing an installation with an equal one, an authority move within a generation and
the alteration pause therefore send nothing, though newly accepted progress still wakes delivery.
A state frame waits for room on the control lane; changes published meanwhile collapse into the
newest installation read after it is queued.

Before each tick the task reads the installation again and sends a changed state frame first,
withdrawing a tick it has not handed to transport yet. A tick holds one replaceable control-lane
slot: newer progress replaces its contents until transport takes it, including while the lane is
full. Each attached domain therefore has at most one pending tick, and a slow client receives the
newest accepted id instead of a backlog. A state or tick frame that does not fit the session's
frame limit is not queued: delivery logs a warning and waits for the next change. An end frame that
does not fit is logged the same way, and the attachment ends without it.

Detach stops the delivery task and waits for it to end before queueing its reply. When the observer
reports the domain missing, the task marks the attachment ending, queues the end frame with reason
`DomainRemoved`, and ends. Because the mark precedes the frame, a request the client sends after
reading the frame finds the attachment ending: an attach releases it and is served as a new
attachment, and a detach reports it not attached. The end of the session stops every delivery task
without a frame.

### Delivery Order

For each attached domain, a client can rely on this order:

1. The attach reply precedes every frame about its attachment.
2. A tick follows the attach reply or state frame that reported its generation as paced. No tick
   of an earlier generation follows a newer state, and an unpaced, stopped or uninstalled clock is
   followed by no tick until a state frame reports it paced.
3. Within one generation, delivered tick ids increase. They may skip, because the authority
   coalesces missed periods and a replaced tick slot keeps only the newest.
4. The end frame is the last frame of its attachment.
5. No frame about the domain follows the detach reply.

Frames about different domains share the control lane but have no order a client may rely on.

### What An Attached Client Observes

| Event | What the serving node sends the session |
| --- | --- |
| `START` | A state frame with the new generation: paced with its period, skew and mapping, or unpaced. Ticks of the new generation follow once the node accepts them; an unpaced domain sends none |
| `STOP` | A stopped state frame for the generation, and no further ticks |
| Automatic pause while altering a running model | Nothing: the installation and mapping are unchanged, and ticks continue |
| Authority committed unassigned | An uninstalled state frame for the same generation, and no ticks. Once an authority is committed again, a paced state frame repeats the generation and its unchanged mapping, and ticks resume with larger ids |
| Authority moved directly to another voter incarnation | No state frame: the mapping is unchanged. Ticks continue from the replacement once it produces them and the serving node has installed its fence, and their ids may skip |
| Consensus leader change | Nothing, unless authority reconciliation also commits one of the authority changes above |
| A committed revision the serving node installs no longer holds the domain | One end frame with reason `DomainRemoved`, which ends the attachment |
| The serving node stops, or the connection is lost | Nothing more: the session ends without an end frame, and its attachments end with it |
| An attach reaches a node restarted before its first installation | The reply waits for that installation, so it never reports a committed domain as not found |

A client keeps following a clock across a lost session by attaching to it again. The Rust client
turns the end of a session into an interruption of every clock that session held, keeping each
clock's latest state but not its tick. It attaches the followed clocks again on its next session
before publishing that session to the client's other requests, and so before any transaction it
attaches there. It reports the clock a restored attachment carries as an observation when the
attachment was interrupted or the clock changed; the changes in between are not reported. When the
new session answers the attach while the previous session still holds the attachment, as after a
leader redirect, the attachment moves without an interruption: a clock that stays paced in the
same generation keeps its newest tick, so the clock a caller reads is never older than a tick event
it has already taken, and a tick of the new session is reported only when it is newer. A
restoration the new session refuses is reported and sent again on that session after one second, a
wait that doubles with each refusal up to thirty seconds, for as long as the client follows the
clock and the session stays open; each new session starts again from one second. A restoration
left unanswered past the client's request timeout ends that session instead, like any request
that outlives the timeout, and the next session attaches the clock again; a caller already waiting
for the next clock event is woken and opens that session. A restoration refused as not found ends
the attachment with reason `DomainRemoved`.

A detach and a restoration of the same clock never cross. The Rust client registers a detach
before it sends it, and no restoration decides to attach the clock while a detach of it is in
flight: a restoration that would has its attach wait until the detach has ended, and sends it only
if the client still follows the clock. A restoration attach decided before the detach is sent
first, and the detach waits for it, so it reaches the session behind that attach and releases what
the attach restored. Once the detach is answered, no session holds the clock on the client's
behalf, and the client follows it only if a later attach of the caller's own attaches it again.

### Guarantees And Non-Guarantees

An attachment guarantees the delivery order above and that every state it reports is a complete
installation the serving node published. It does not provide the following:

- **Replay.** Ticks are replaceable, so a slow client receives the newest accepted tick, never a
  backlog, and installation changes published while a state frame waits collapse into the newest.
  A new attachment starts from the current installation and the newest accepted tick. Nothing
  reports the changes a lost session missed.
- **Persistence.** Attachments are session-local hot state. A session end, a node stop or a
  restart discards them on the server, and a client restores one only by attaching again.
- **A distributed order.** Each node serves its sessions from its own installation and accepted
  progress. Sessions served by different nodes can observe the same change or tick at different
  moments, and a node that has not accepted a tick or installed a change yet reports it later. No
  frame says anything about another node's state.
- **A disciplined clock.** The serving node's logical reading in a tick frame is that node's own
  projection of its UTC, so it carries that host's offset multiplied by `TIME RATE`, and it is
  older than the frame's arrival by the delivery latency. Synchronized host UTC, which the cluster
  requires of every node, is a precondition for the serving node's readings too. A client whose
  UTC is not disciplined like the cluster's projects logical instants and admission centers shifted
  by its own offset multiplied by the rate, and its physical waits end early or late by the offset
  itself; it can anchor itself on the serving node's reading instead.

### Client Libraries

The Rust client keeps, for each followed domain, the clock the attach reply or the latest state
frame carried and the newest tick of that clock's generation, and answers logical time at a UTC
instant, the physical wait until a logical target and the admission window with the vocabulary's
clock arithmetic, the arithmetic the ingestor's admission uses. Its clock events are coalesced per
domain, so a caller that reads none never holds the session's reader up: the newest state and tick
of a domain replace older unread ones, and an interruption or an end replaces the observations
before it.

The shared C binding gives hosts a separate retained `nx_clock_event` handle for later state
changes, ticks, interruptions, refused restorations, and ends through `nx_session_next_clock_event`.
Its typed accessors expose the domain, generation, state and paced mapping, tick, or end reason.
Through `nx_session_domain_clock` a host reads the latest followed clock itself as a retained
`nx_domain_clock`, so a host attaching to an already paced clock reads the generation and mapping
the attach reply carried before it uses the first tick, and projects logical time, waits and
admission with the Rust client's arithmetic instead of reimplementing it.

## Execution-Time Snapshots

The clock is sampled once when a unit of domain work is accepted. The resulting execution snapshot
contains the validated lifecycle generation and one timestamp. The runtime passes that snapshot to
VM, Roto, and WASM execution instead of giving those engines a context-free time source.

A function the VM calls out to receives that time as part of the call. The VM's
`FunctionInjector::inject_with_context` takes the snapshot's `now` as a `Timestamp` beside the
function, its arguments, the selected rows, the source span and the rows' prior errors. Roto's
`UdfExecutor` implements it, and a domain UDF's `now()` returns the injected time, so a UDF runs at
the snapshot of the unit that calls it. Paired doctests beside the trait and beside `UdfExecutor`
hold this signature: a call with a `Timestamp` compiles, while a call that passes a raw integer or
omits the time does not.

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

A Plexus reset uses this same clock contract. Repeating its durable command reference resumes the
same guest-state lifetime replacement without changing the domain clock mapping.

Generated records use the snapshot assigned to their generating operation. Buffered emitter
batches and their retries retain the snapshot from acceptance, while external observation fields
whose contract is actual UTC obtain that value at the shared source-host intake boundary or their
connector boundary. An Iceberg emitter's `COMMIT EACH` is not measured from that retained snapshot:
the host reads a fresh snapshot at the first cadence check after a flush stages rows and arms the
commit's logical deadline from it, and a retry or drain forces the commit without a clock read.

An HTTP emitter's prepared method, target, headers, and optional body retain the execution
snapshot of the admitted record through retries and an entity-pause drain. Changing the emitter
does not re-evaluate an admitted request under the replacement. Attempt timeouts, retry backoff,
an HTTP-date `Retry-After`, and shutdown or drain deadlines remain physical waits; a domain's
`TIME RATE` does not shorten them.

A native client emitter similarly freezes the Arrow IPC bytes, source members, delivery identity
and execution snapshot when the batch is prepared. Consumer retry or ACK timeout gives that same
batch a new attempt reference without re-running expressions at a later domain time. Its ACK
deadline, retry backoff, interconnect heartbeat and peer-silence deadline are physical waits.

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

An ingest group resolves its event timestamp column once. Admission compares signed nanosecond
values against the first and last reached centers in lanes, then tests the exact period remainder
for values between them. The period reduction is prepared once for that window. A bitmap selects
accepted Arrow rows and their timestamp and ACK sidecars together. Rejected rows retain their own
ACKs and follow each output route's message-error policy with code `validation` and operation
`admit`; a rejected timestamp cannot discard another row of the group. A missing declared timestamp
is also rejected for that row. The single-timestamp admission check has the same inclusive bounds.

[SIMD Kernels](./simd-kernels.md#latency-and-domain-admission) describes the buffer arithmetic,
exact period reduction, bitmap results, and scalar/level qualification. The clock owns the
eligible centers and ingestion snapshot; the kernel consumes their already-resolved bounds and
never reads time or establishes a frontier.

Unpaced ingestion has no admission window. Its clock snapshot still supplies delivery time, while
an explicit event timestamp or connector-owned source timestamp remains preserved source time.

A client ingestor takes one ingestion snapshot for each batch its admission worker dispatches, after
the batch is validated and its acknowledgement root is tracked. `TIMESTAMP NOW` gives every row of
the batch that snapshot, and `TIMESTAMP AT <field>` preserves each row's own field; paced admission
then tests each row against the window exactly as for any other ingestor. A client batch carries no
connector-owned source timestamp, so a paced domain requires its client ingestors to declare a
timestamp. The batch's `ACK TIMEOUT` and its producer's retry backoff are physical monotonic bounds;
neither waits on nor stretches domain logical time.

## Duration Windows

A duration window retains rows by their preserved event timestamps. Its next completion deadline
is the first retained timestamp plus `WIDTH`, and its branch task waits for that deadline on the
domain clock. Completion emits the rows currently retained and advances by `STEP`. A later batch
with the same event timestamp enters the state that remains after that advance; an equal timestamp
does not reopen an emitted window.

When an input batch starts an empty window bounded only by duration, its admitted rows with equal
timestamps do not reach `WIDTH` during admission. The branch processes them before its next clock check,
which can emit the group together even when the batch arrived after that deadline. The output
retains the earliest source watermark and uses the emission's domain time for its high watermark.
`TIMESTAMP NOW` assigns the ingestion snapshot to every decoded row of a batch, while
`TIMESTAMP AT` preserves each row's explicit event time.

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

Reset coordination and its ninety-second initial-checkpoint deadline are physical, monotonic bounds.
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

[Connector Crates And The Connector Contract](./connector-contract.md#ingestor-boundary) defines
the paced ingestor's host and connector responsibilities; this chapter owns the cadence's time
mapping and missed-occurrence behavior.

The architecture keeps four time classes distinct:

| Time class | Internal use |
| --- | --- |
| Domain logical time | Expressions, explicit domain cadence, collection and flush cadence, TTL, retention, window completion, and guest-requested timeouts |
| Preserved source time | External event timestamps, broker metadata, and window membership inputs |
| Physical monotonic time | Network deadlines, retry and backoff, acknowledgements, cancellation, shutdown, drain, state checkpoint deadlines, safety timeouts, and physical batching minima |
| Actual UTC | Paced projection input, unpaced domain reads, administrative records, security validity, explicitly external observation fields, and the comparison of a server-supplied HTTP date, such as `Retry-After`, with the present |

Logical and physical deadlines are different types, and neither converts to the other. A
`LogicalDeadline` carries its domain, lifecycle generation and target logical timestamp, and only a
bound `DomainClock` creates one. The clock's `wait_until(deadline, &CancellationToken)` runs the
loop above. It returns the reached deadline with a fresh execution snapshot, or a typed
`DomainClockWaitError`: the clock became unavailable, including a deadline of another domain or
generation, the wait was cancelled, or the physical deadline could not be scheduled. A
`PhysicalDeadline` holds a private process-monotonic instant. The connector contract's
`PhysicalDeadlineCapability` creates one with `after(timeout)` and waits for it with
`wait_until(deadline)`, and only the owners the repository clock-boundary check declares may take
that capability. Paired doctests beside the runtime's clock capabilities, which build with the
server's `testing` capability, hold these signatures: each wait compiles with its own deadline
type, and handing either wait the other type fails to compile. A rejected snippet is evidence about
that snippet, not proof that no other path mixes the two classes. The clock-boundary check, the
Shuttle checks of logical waiters and the public scenarios remain the evidence for the time
classes.

Actual UTC enters the data plane through a dedicated boundary, and expression engines cannot read
it directly. That boundary and the capability that arms physical deadlines belong to the connector
contract, which the runtime and every connector crate share, so a connector stamps arrival time
through the same owner the runtime uses. Repository validation checks these ownership boundaries
across the workspace, so a new runtime or connector path must choose its time class explicitly.

Materialized relay branch activity uses the bound domain clock, independently of the row event
watermarks used by `LAST BY TIMESTAMP`. Its activity tracker is not persisted: a restored relay
initializes every restored branch at its activation-time clock snapshot and measures the complete
TTL from that coordinate. Restoring or restarting the relay preserves the rows' event watermarks
without turning those timestamps into activity or expiry. The physical scan only releases a
branch once its domain-clock TTL has elapsed.

Physical monotonic time is measured with the timers and instants of the
[primitive boundary](./execution-primitives.md),
`nervix_primitives::time`, which selects them for the build's execution mode and grants no clock
permission: it reads no actual UTC, arms no physical deadline on a caller's behalf, and a logical
deadline armed on its timer stays logical, as described above. Its timers follow the clock of the
runtime that polls them, so an ordinary test on a paused runtime checks a physical deadline's
elapsed behavior exactly, and a simulated interconnect host waits in simulated time. Shuttle does
not model time: under Shuttle a timeout never measures its deadline, and a check decides whether it
wins. HTTP request deadlines and `Retry-After` therefore stay physical, flush cadence keeps the
domain clock, and no check establishes progress by waiting.

The web console's clock display is an external observer: its clock-display module reads browser UTC
to project an attached paced mapping for the screen. That projection does not enter a node's read
watermark, alter tick progress, or supply domain time to execution. The repository clock-boundary
check lists this module among the owners that may read actual UTC directly, and the console's clock
projection samples browser UTC only through it. The check rejects the Rust UTC reads it recognizes
elsewhere in the console; it does not recognize the browser's own `Date.now()`, which the console
reads for display-only freshness of its dataflow graph and its reconnect countdown.

The source check masks `cfg(test)` module bodies through the shared Rust scanner, including modules
with source contracts or additional attributes. It preserves line coordinates and product code
after each fixture module. A harness layer contract alone grants no clock permission to product code.

## Recovery And Distributed Guarantees

The paced mapping, lifecycle generation, and authority fence recover from consensus state. Tick
progress, delivery loops, local read watermarks, and process-monotonic deadlines do not. After a
restart, nodes install the committed mapping and the leader reconciles an authority for the current
live voter incarnations. The producer recomputes the latest due boundary from the mapping.

This produces the following failure behavior:

| Event | Result |
| --- | --- |
| Leader transfer | The committed mapping and authority remain unchanged unless the effective candidate set also requires reconciliation |
| Authority loss or restart | Consensus advances the authority revision and assigns another live voter incarnation; the mapping and lifecycle generation stay fixed. When the authority also led consensus, the survivors elect a leader first. Every node installs the new fence ahead of its graph installation |
| Node join or reconnect | The node installs the committed generation before execution and receives the newest retained progress once it advertises its installed domain clocks |
| Session attach before a restarted node installs | The attach waits for the node's first installation of the committed domains, so it never reports a committed domain as missing |
| Delayed old progress | Generation, revision, identity, and authenticated-peer checks discard it |
| `STOP` followed by `START` | Stop revokes the authority; start increments the generation and commits a new mapping and fence |
| Mapping or tick arithmetic overflow | Projection, deadline conversion, boundary, and tick-id operations report typed clock errors instead of wrapping or changing anchors |
| Missing mapping or authority | Paced access fails as uninstalled; it never falls back to actual UTC |

The authority's projection, cadence, tick-boundary and rate-conversion failures render the full
typed report in both the runtime event and the corresponding warning. Timestamp overflow includes
the timestamp arithmetic cause beneath the clock operation. Clock attachment and progress-apply
events, source cadence errors and message-error flush-clock failures preserve their supplied
contexts in the same way, including the unavailable lifetime beneath a failed deadline wait.

All nodes project from their local UTC observations. The shared mapping and per-node nondecrease
rule keep one generation coherent, but they do not make simultaneous reads on different hosts
identical and do not create a distributed total order. Operators must synchronize and monitor host
UTC. Host offset appears in each node's projection and is multiplied by `TIME RATE`; event `SKEW`
is an admission tolerance rather than a clock-synchronization budget.

## Evidence

The paired doctests in this chapter check specific current API restrictions: a logical wait takes a
logical deadline, a physical wait a physical one, and a function the VM calls out to receives its
execution time. They check signatures, not behavior. Clock generations, attachment ordering, the
attach's wait and paced simulation are runtime obligations, held by checks that run the production
owners:

- The Shuttle checks of the clock lifecycle, which
  [Data-Plane Concurrency](./data-plane-concurrency.md#protocols-and-their-checks) lists, explore
  nondecreasing reads of one generation, refusal of a replaced generation, publication order, each
  wakeup of a logical waiter, the attachment's delivery order across ticks, unassignment,
  reassignment and a generation change, and an attach waiting for the node's first installation.
- Public Cucumber outlines over one and three nodes cover the attachment contract, a client pacing
  simulated events by its attached clock, the CLI following a clock through its generations,
  refusing a missing domain and following a clock across a cluster restart, the console's clock
  display, and every client runtime's conformance probe reading an attached clock. Single
  scenarios cover the CLI restoring a clock after a transport loss and a reconnected console
  restoring its attachment.
- The paced branch-buffering outline checks both tenant keys and both sequence values under
  one-minute logical flush and collection at rate 1200. Each window still closes 50 ms after it
  opens, while a physical clock would hold it for a minute, so the five-second physical receive
  budget rejects the wrong clock by a factor of twelve and leaves nearly all of itself to relay
  delivery between three nodes. A companion outline makes every relay owner wait 250 ms before
  each batch it fans out and keeps the same budget. Each outline's subscription reader runs
  independently of the scenario runner. Controlled harness regressions distinguish timely rows
  learned about after a runner delay from a genuinely missing row whose original receive budget
  expires; they do not change the runtime's clock contract.
- The slow historical-domain Immediate outline keeps both tenant keys and exact payloads on one
  and three nodes at `TIME RATE 0.0001`. Its 100-microsecond physical batching minimum would take
  one physical second if scaled as logical time, so the receive budget stays 500 milliseconds.
  The caller delays collecting the result for one second while the independent reader must
  record both rows inside the original budget. A paused-clock control requires receipt before
  the caller first polls the result; a one-second cadence still expires the 500 ms sample.
- Registered Bolero properties keep the complete values of clock periods, skews and rates, of the
  attach and detach statements, and of the clock requests, replies and frames. Mutations of a
  current clock frame decode deterministically or fail within the verification budgets.

The Bolero properties run as ordinary randomized and corpus tests on every pull request. Their
sanitizer-backed libFuzzer campaigns run in CI only for a pull request labeled `fuzz`; every other
event deliberately skips them, and a skip supplies no fuzz evidence.
[Property Testing And Fuzzing](./property-testing-and-fuzzing.md#commands-and-enforcement) owns
that policy.
