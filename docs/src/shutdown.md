# Shutdown And Recovery

Shutdown is the ordered transition from a running node to a stopped process. It has one owner, three
phases, and one deadline. The owner establishes that the node is terminating, stops new intake,
keeps the services that admitted work depends on alive until that work finishes, and only then tears
down the node's own tasks, connections, and storage.

This chapter defines the externally observable contract: what starts a shutdown, what each phase
guarantees, what bounds it, what survives it, and what a restart recovers. It covers both endings.
A graceful shutdown completes admitted work within its deadline. A forced ending — a repeated
signal, an expired deadline, or `SIGKILL` — abandons the remaining phases, and the rest of the
cluster observes it exactly as it observes a crash.

Nervix is not an exactly-once system, and shutdown does not make it one. The guarantees below stop
at durable boundaries that are stated explicitly. See [What It Is Not](./what-it-is-not.md) for the
persistence boundary and [ACK Semantics And Effective Delivery](./emitters.md#ack-semantics-and-effective-delivery)
for the delivery consequences at each sink.

## Stop Requests

A node begins shutting down for exactly two reasons:

- It receives its first `SIGINT` or `SIGTERM`.
- One of its public listeners stops, whether it failed or returned.

There is no NSPL statement, API call, or cluster command that stops a node. `CORDON NODE`,
`DRAIN NODE`, `RELOCATE`, and `DROP NODE` move work and change eligibility; none of them ends a
process. Stopping a process is an operator or supervisor action delivered as a signal.

Both signals are registered before the node starts any other work, and they stay registered until
the process exits. A node that cannot register them refuses to start, reporting
`failed to register termination signal handlers` and exiting with status `1`. A diagnostic node,
the server built in the `deloxide` mode, then installs its deadlock detector before it starts a
runtime worker, so the detector's threads exist only once neither signal can end the process by its
default action; a diagnostic node whose deadlock diagnostics cannot start refuses to start,
reporting `failed to start the diagnostic node's deadlock diagnostics` and exiting with status `1`. A signal that arrives
while the node is still starting is held rather than lost: it takes effect once startup completes,
while its deadline runs from the moment the signal arrived.

Repeated requests are idempotent. A later stop request never restarts, extends, or replaces the
first one; it can only end the process sooner.

## Phases

Shutdown reports three phases in order. Each logs when it finishes and with what outcome.

| Phase | Log message | What it owns |
| --- | --- | --- |
| Stop admission | `shutdown admission phase finished` | Advertising the terminating incarnation, then closing the public listeners and the client connections they had accepted |
| Drain support | `shutdown drain-support phase finished` | Moving scheduled work to a replacement node when one exists, then completing admitted work in place, while the services that work depends on stay alive |
| Terminal teardown | `shutdown terminal-teardown phase finished` | Stopping the node's tasks, then the runtime, consensus, cluster membership, and interconnect, and releasing its storage |

```text
stop request                                                             exit 0
(first SIGINT or SIGTERM,                                                   ^
 or a public listener that stopped)                                         |
       |                                                                    |
       v                                                                    |
  StopRequested ------> DrainSupport ------> TerminalTeardown ------> Finished
       |                     |                      |
       +---------------------+----------------------+
                             |
         repeated SIGINT or SIGTERM  ->  exit 130 or 143
         shutdown deadline expired   ->  exit 1
         (the remaining phases do not run)
```

Each phase reports one of three outcomes:

- **Completed.** The phase finished its work.
- **Abandoned.** The phase gave up on work it could not finish, such as a drain that timed out or a
  leader it could not reach, and shutdown continued.
- **Forced.** The deadline passed while the phase was running.

A process that reaches terminal teardown with any phase `Forced` exits with status `1` rather than
`0`, because graceful shutdown did not finish within its deadline. The absence of
`shutdown terminal-teardown phase finished` in a node's log is the definitive sign that the process
did not shut down gracefully.

## The Shutdown Deadline

One deadline bounds all three phases. It is set by `--shutdown-timeout`
(`NERVIX_SHUTDOWN_TIMEOUT`, default `50s`) and measured on the process monotonic clock from the
first stop request.

The deadline is absolute. A later signal does not extend it, a client holding a session or an
upload open does not extend it, and domain pacing never changes its physical length: a domain
running a million times slower or faster than real time reaches the same physical deadline. A
separate supervisor enforces it independently of the runtime, so a wedged or saturated node cannot
outlive its own deadline by failing to schedule work.

When the deadline passes, the process logs the phase it had reached, reports
`shutdown deadline expired; abandoning graceful shutdown`, and exits at once with status `1` without
running its remaining phases.

Inside that deadline sits the drain timeout, `--drain-timeout` (`NERVIX_DRAIN_TIMEOUT`, default
`30s`), which bounds the drain-support phase alone. The two defaults are ordered so that a full
drain still leaves time for terminal teardown. Every drain step waits for the smaller of its
remaining drain budget and the remaining shutdown deadline, so shortening the shutdown timeout below
the drain timeout makes the shutdown deadline the effective bound.

Two bounds are deliberately outside the deadline. Terminal teardown's final stops — the runtime,
consensus, cluster membership, and the interconnect — run to completion rather than being cut short,
because stopping them is what releases the node's tasks, connections, and storage. The interconnect
applies its own ten-second transport drain. Closing the node's databases flushes and joins their
background work synchronously, so the services that own them are dropped last, as one job on the
node's filesystem storage workers; a node that cannot take that job reports teardown `Abandoned`.
The deadline supervisor, not those bounds, is what guarantees the process ends.

### Exit Status

| Ending | Status |
| --- | --- |
| Graceful shutdown finished, every phase `Completed` or `Abandoned` | `0` |
| Graceful shutdown ran to the end but a phase was `Forced` | `1` |
| Shutdown deadline expired | `1` |
| A public listener, cluster shutdown, or storage release reported an error | `1` |
| Termination signal handlers could not be registered at startup | `1` |
| A diagnostic node's deadlock diagnostics could not start | `1` |
| A diagnostic node reported an active deadlock among its tracked locks and recorded it | `3` |
| A diagnostic node reported a deadlock it could not record, lost findings, or outlived the budget for recording one | `4` |
| A command-line option or its environment variable holds a value the node cannot read, such as duration text that names no duration | `2` |
| Repeated `SIGINT` | `130` |
| Repeated `SIGTERM` | `143` |
| `SIGKILL` | Terminated by signal, no exit status |

Every `SIGINT` or `SIGTERM` after the first abandons graceful shutdown. The process logs
`repeated termination signal received; abandoning graceful shutdown` with the phase it had reached
and exits immediately with the status a shell reports for that signal, whichever signal it was that
started the shutdown. No destructor, exit handler, or remaining phase runs. Whichever comes first,
the repeated signal or the deadline, decides how the process exits.

A diagnostic node that reports an active deadlock ends the same way, immediately, whatever phase it
is in: its blocked threads hold work it can no longer finish. It describes the cycle on standard
error, records it as evidence, and exits with `3`, or with `4` when the recording failed, without
running a destructor, an exit handler or a remaining phase, as
[Diagnostic deadlock detection](./data-plane-concurrency.md#diagnostic-deadlock-detection)
describes. Its durable state recovers as after any forced exit.

## Terminating Incarnation And Placement Eligibility

An **incarnation** is one process run of a named cluster node. A restart produces a new incarnation
of the same stable node name.

The first act of shutdown is to advertise that the current incarnation is terminating. That
advertisement belongs to the incarnation, not to the node name, and is carried by cluster gossip
rather than persisted. A restarted process is a new incarnation and does not inherit it.

Terminating removes the incarnation from consideration as a **new** placement destination. It does
not remove the node from anything else: the incarnation stays a live Raft voter, keeps its
interconnect listener and registered handlers, keeps carrying admitted relay work and
acknowledgements, and can still complete an ownership handoff that is already in progress. A node
that is shutting down does not leave the Raft membership, does not step down, and does not transfer
leadership away. It simply stops, and its peers observe it going silent.

An administrative **cordon** is the other eligibility control, and the two are independent:

| | Cordon | Terminating |
| --- | --- | --- |
| Applies to | The stable node name | One process incarnation |
| Set by | `CORDON NODE <node_id>`, and by `DRAIN NODE` | The process itself, at its first stop request |
| Stored in | Replicated consensus state | Cluster gossip |
| Survives restart | Yes | No |
| Cleared by | `UNCORDON NODE <node_id>` | Process exit |
| Visible as | `raft.cordoned_nodes` in `SHOW CLUSTER STATUS` | `terminating: true` in node status |

Both filters compose when the scheduler chooses an owner: a candidate must be a live Raft voter
that is neither cordoned nor a terminating incarnation. `RELOCATE` and `DESCRIBE RELOCATION` apply
the same checks in a fixed order and name the first one that fails — not a Raft member, then not a
live Raft voter, then `node '<node_id>' is terminating`, then `node '<node_id>' is cordoned`.

### Cordon Preservation

A graceful shutdown that moves work away cordons the node so its own work does not come back to it
mid-drain, and clears that cordon afterwards. It records whether the node was **already** cordoned
before it started, and clears only a cordon it set itself.

An operator cordon therefore survives shutdown and restart. A node cordoned by an operator, stopped,
and started again comes back cordoned. When the node never requested its drain, because the drain
timeout or shutdown deadline passed before it observed a leader, nothing was cordoned and nothing is
cleared. A request the leader may have received counts as requested, even when its answer never
arrived, so the node still clears the cordon that request may have set.

## Stopping Intake

Stop admission closes the node's public surface. It stops accepting on the session gRPC, connector,
observability, and console listeners and closes the client connections those listeners had accepted.

Closing a connection cancels the requests it carries. A console session is told that the server
is shutting down, after the replies already queued for its client, and that ending is its last
frame. A native gRPC session's connection is cut when admission closes, so its client sees the
stream fail as it would after any transport loss. Neither waits for the client to read anything: a
session whose client reads nothing ends just as promptly, and its subscriptions stop and release
the relays they held. Every request the session had not yet admitted is cancelled before admission
and never begins an effect, so the client can send it again, with the same execution reference, to
another node. A session stream or a resource upload waiting on its client ends at once, so no
client can hold the drain or the process open. A backup download or a restore stream that the web
console carries over a WebSocket of its own ends at once as well, with close code `1001`, as a
native gRPC download or restore call is cut; a restore admitted with its whole archive goes on
without its call. An authenticated upload held open
at any point of its progress — before its first message, between chunks, or trickling chunks
indefinitely — is cancelled this way and does not delay the exit. [Node Stop And Restart As A
Client Observes Them](./client-session-protocol.md#node-stop-and-restart-as-a-client-observes-them)
describes what a client sees at each ending and what it recovers.

Work that a session command had already admitted is not cancelled here. Its session stops waiting
for it, and no reply follows for it, but its effect keeps running. A transaction commit in progress
keeps running, and terminal teardown waits for it until the shutdown deadline. A commit still
running when the deadline passes is cancelled with the process; its replicated progress survives,
and a later leader resumes it from its recorded step, so the transaction reaches `COMMITTED` after
the node restarts.

The terminating node then stops new intake on **every** one of its ingestors, including endpoint and
Syslog ingestors that serve on every node and ingestors whose scheduled owner did not move. Its
generators stop producing. Intake never reopens: there is no resume path out of a shutdown drain.
If a broker-style source is still resuming, the host cancels that pending resume when shutdown
arrives, drops its DNS, socket and handshake work, and closes the source before exiting. A quiesce
change also cancels an in-progress resume so the next loop turn observes the new intake state.

Source-instance readiness retirement is final and names the ending host's exact handle. Shutdown
retires installed readiness before withdrawing the registry. Terminal domain execution teardown
cancels its relay channel publication after owned tasks stop; retained producer or channel handles
then reject further dispatch and cannot attach to a restarted execution.

This intake stop ignores `ON QUIESCE`. That clause governs what an external source experiences
during a resumable hold — a model alteration, a domain pause, or memory-pressure shedding — where
the ingestor will run again. Shutdown and ownership handoff are not resumable, so polling and
endpoint admission simply stop, and no `SUSPEND`, `BUFFER`, `DROP`, or `REJECT` policy is applied on
behalf of the stop. An endpoint refuses new requests outright, without offering a retry delay.

Endpoint source close and terminal table clearing end each exact intake lifetime before withdrawing
its binding. HTTP requests and WebSocket sessions that retained a route then see absent intake on
later admission; a replacement source does not reopen that retained lifetime. A request already
holding an intake lease may finish within the existing shutdown deadline. Endpoint definitions and
intake leases are volatile publications and are reconstructed from the installed revision on startup.

Payloads already admitted continue through their routes.
Each source host retains the quiesce publication it observed before awaiting dispatch. Its next
change wait compares against that publication after registering the waiter, so a shutdown or
ownership-handoff engagement during dispatch is observed on the next loop turn even when the
notification arrived before the wait began.

A [client ingestor](./ingestors.md#client-ingestors) stops intake the same way: from the moment
intake stops, a batch that arrives or waits unadmitted is refused as `draining`, which its producer
must not send again to this execution, while admitted batches continue through their routes. Closing
the node's sessions detaches the producers they held; their admitted batches still drain. Producers
another node forwards here stay attached through the drain and learn every outcome it decides.
Terminal teardown then ends every producer still attached with `shutting down`, reporting each batch
whose acknowledgement is still unresolved as of unknown outcome with cause `interrupted`.

A raw quiesce buffer is not part of the drain. Payloads that a `BUFFER` mode retained during an
earlier hold are outside runtime graph work: a shutdown does not replay them, and they are discarded
and counted as dropped when the ingestor stops. A retained payload whose unfolding was still waiting
for the extension workers stays in the buffer, so the stop ends that wait at once and discards it
with the rest. A live payload a source handed over without an acknowledgement, such as a paced
poll, whose unfolding was still waiting for the extension workers is dropped when its ingestor
stops; the stop ends that wait at once as well. Only work already admitted into the graph is
drained.

## Draining Admitted Work

Drain support has two parts that share the drain timeout. Both are also bounded by the shutdown
deadline.

**Moving scheduled work.** When another live, schedulable Raft voter exists, the node first moves
its scheduled work there through the planned ownership handoff described below. This is the same
operation `DRAIN NODE` performs, requested against the terminating node itself: it visits domains
and schedule units in canonical order and moves one hard colocation group or independent runtime
node at a time.

**Completing admitted work in place.** The node then finishes what it has already admitted. This
part always runs, whatever happened in the first part. It is the whole drain when no replacement
exists, and it also covers listener ingestors, which bind on every node and are never moved, and any
unit whose move failed.

While admitted work finishes, the node's relays, processors, emitters, and acknowledgement paths
stay alive, and so do the supporting services that work depends on: the domain clock and its
progress delivery, schedule activation, runtime-state replication and checkpoints, ownership-handoff
coordination, and the interconnect listener and its handlers. Work admitted before the listeners
closed can therefore still take execution snapshots, wait on logical deadlines, and reach its sinks.

Nervix repeatedly force-flushes ingestor route buffers, processor collections and route buffers,
message-error routes, reingestors, and emitters, whatever their `FLUSH EACH` cadence, until nothing
remains: no relay batch, no runtime-node work item, no emitter buffer or active publish, and no
admitted acknowledgement root. A stopped or extremely slow domain clock cannot hold rows out of the
drain, because the force flush does not wait for a logical cadence to come due.

Window processors are the one exception to that release. A force flush purges timed-out aggregate
state but emits only windows that have met their declared `WIDTH`. A partially filled window is not
emitted early by a shutdown, so its rows do not reach the sink. A row's acknowledgement completes
when the window steps past the row; until then its shares are parked, so the drain does not wait
for them. The branch's final checkpoint keeps the retained rows for the next task of the branch to
restore, and teardown ends their shares unacknowledged: a source with external acknowledgements
redelivers those rows after the restart, and the restored window admits them beside the rows it
restored.

An ownership handoff publishes the remaining window for the destination to restore. Evicting a
concrete branch has a different endpoint: it drops the branch's retained rows and aggregate state
before the final checkpoint, so a later branch with the same key begins with an empty window.
The lifecycle checkpoint carries each branch incarnation; window restore accepts retained state
only from that same incarnation. A restarted owner resumes those incarnations before it accepts any
input; see [Restoring Processor Branches](#restoring-processor-branches).

Draining ends with a confirmation pass. After a flush generation observes nothing outstanding, one
more generation must also observe nothing, so work that an upstream node publishes after a
downstream node finished its own flush is not left behind. A domain is quiescent only when that
confirming generation completes with nothing visible.
Quiesced backup applies the same admitted-work view to its selected domain across all live nodes.
It requests a separate cluster-wide confirming generation before capturing checkpoints. Parked
`REQUIRED WAIT` messages and the rows windows retain do not hold that cut open, and a failed drain
resumes the domain. The cut captures retained window rows as window state.
When a parked message has crossed nodes, remote ACK progress carries its parked state back through
the source's acknowledgement chain. The drain excludes that chain while the message is parked;
resuming it reactivates the chain, and only a terminal ACK completes the source attempt.

Work keeps moving while the drain looks for it, so "nothing visible" is an exact observation rather
than a series of separate reads. Each observation reads the domain's outstanding force-flush
obligations first, so a flush it sees complete has also left everything it released or resumed
visible to the reads that follow. It reads every relay's admission sequence before and after the
other counts, and a relay that admitted a batch in between counts as work still moving: a batch that
reached a relay or a node only after the read that would have counted it there entered some relay
while the observation read. [Node quiesce accounting](./data-plane-concurrency.md#node-quiesce-accounting)
describes the counts and the order of the reads.

Each domain therefore advances independently through three states:

```text
  intake closed on            a generation requested with nothing
  every ingestor              visible completes with nothing visible
        |                                   |
        v                                   v
    Draining  ------------------------> Confirming ------------------------> Quiescent
        ^                                   |
        |   admitted work became visible    |
        +-----------------------------------+

    drain timeout or shutdown deadline reached, in either state
        |
        v
    Abandoned  ->  remaining work is negatively acknowledged
```

A WASM processor branch keeps the acknowledgements of its latest guest callback open until that
callback's checkpoint reaches stable storage and every assigned replica, so a drain waits for the
checkpoint as it waits for any other outstanding acknowledgement. The checkpoint's ten-second
deadline bounds that wait: a checkpoint that cannot complete fails and negatively acknowledges what
it held.

Runtime teardown gives each processor task a stop grace, and the processor task gives each of its
branch tasks one of its own. A processor task still stopping its branches when its grace ends is
ended together with every branch task it holds. A branch still waiting for its checkpoint therefore
never outlives its node: it cannot keep the node's runtime database open past terminal teardown,
continue a checkpoint, or settle acknowledgements after the node stopped. Its unreleased
acknowledgements are negatively acknowledged, and a restart finds whatever that checkpoint had
already written to the node's storage, exactly as after a forced ending.

A coordinated WASM state reset is serialized with domain lifecycle, placement, ownership movement,
resource rebinding, and model mutation by the domain alteration lease and its entity gate. If node
shutdown interrupts a preparation before reset publication, the old generation remains
authoritative; ordinary reset error handling restores the stopped task when it can, while terminal
teardown may discard its volatile handoff and negatively acknowledge what it held. Restart then
restores the old generation. If the reset's `Publishing` schedule wins, the old generation is
already fenced permanently. Drain support keeps schedule activation, state storage, replication,
and interconnect handlers alive so the fresh initial checkpoint and `Ready` publication can finish
within their ordinary bounds. If shutdown ends first, restart observes `Publishing` and resumes the
new generation rather than restoring the old one.

Read-only WASM state inspection after restart uses the committed schedule's generation and
retained reset and recovery outcomes. A pre-publication failure leaves the preceding generation
visible; a published but not yet usable reset remains `PUBLISHING` until its initial checkpoint
and activation finish. Runtime checkpoint observations from a replaced generation are excluded,
and a restored checkpoint reports unknown prior replica confirmation when that boundary cannot
be reconstructed. Inspection never settles an uncertain transaction outcome or resumes a reset.

An admitted NSPL reset is recorded as an ordered transaction effect. If shutdown interrupts the
command after its effect is recorded, recovery resumes it with the original execution reference;
it does not admit the text as a fresh reset. The client receives success only after the replacement
is usable, or a retained failure when that cannot be established.

Resetting one concrete branch does not turn a sibling branch into shutdown work. Its scoped relay
gate and processor command lane select only that branch; sibling callbacks, checkpoints, outputs,
ACKs, and timers continue until shutdown itself reaches them. Old timeout handles belong to the
discarded branch instance and are never transferred to the fresh instance.

Three kinds of work deliberately do not hold the drain open:

- **Pending `REQUIRED WAIT` records.** A message suspended on absent materialized state cannot
  finish, because the dependency is not there. Every force flush retries it against the state that
  is present; whatever still waits at the end is negatively acknowledged. Its source redelivers it
  after the restart, and the record is processed then against the state that has since arrived.
- **Rows a window retains.** Only further input steps a window past them, and the window's
  checkpoint keeps them, as described above.
- **Outstanding force-flush obligations.** They are the mechanism of the drain, not admitted work
  waiting inside it.

Payloads that other live nodes publish into this node's relays are admitted work like any other, so
a peer that keeps publishing extends the drain until its timeout. When every node terminates at
once, each completes its own admitted work within its own drain timeout, and work that reaches a
peer after that peer finished draining is negatively acknowledged.

### When The Drain Does Not Finish

A drain that reaches its timeout reports itself abandoned and shutdown continues. Terminal teardown
then negatively acknowledges the remaining work. A source with external acknowledgements redelivers
it after a restart; a `NO_ACK` source loses it.

This is the behavior an unavailable sink produces. An emitter whose sink will not accept records
holds the drain until the timeout, its source offsets are never committed, and the records are
redelivered and published after the node restarts and the sink recovers.

The log distinguishes the two ways a drain runs out of time:

- `local graph drain timed out with admitted work outstanding`, with per-domain counts of admitting
  ingestors, active generators, outstanding acknowledgements, buffered relay batches, runtime-node
  work items, buffered emitter messages, publishing emitters, pending `REQUIRED WAIT` records, and
  force-flush obligations.
- `local graph drain timed out before confirming that no admitted work is still moving`, where the
  work finished but the confirming generation did not complete in time.

## Ownership Handoff During Shutdown

The ownership move at the start of drain support is the same planned handoff used by `DRAIN NODE`,
`RELOCATE`, and placement consolidation. [Control Plane](./control-plane.md#planned-ownership-handoffs-and-failover)
owns the general contract; this section states what shutdown adds.

The complete target schedule is computed before anything is held. The coordinator then fences
dispatch at the affected subgraph boundary on every live node, stops intake for each moved ingestor,
and drains the work already admitted to the moved unit, including moved-ingestor acknowledgement
roots, emitter buffers and active publishing, and an Iceberg emitter's staged commit. The new
schedule is written only after that drain succeeds, and the fence opens only after the destination
activates the published revision.

Every step of the handoff carries one coordination identity, composed of the coordinating node, its
process incarnation, and a sequence. The receiver verifies that identity against the authenticated
connection before the request is handled, so a different node cannot use it and a restarted
coordinator process cannot replay it. Engagement binds the identity to one domain, relay set,
affected-entity set, and purpose; a retry succeeds only for the same identity and the complete
scope.

Gate leases are deadline-bound and release themselves if the coordinator disappears. The default
lease is sixty seconds and a planned handoff budgets two of them, one for preparation and one for
activation. Both are longer than the default shutdown timeout, so a gate a terminating node engaged
can outlive that node's own process; the lease deadline, not the node, is what reopens it.

When activation fails after the schedule is committed, the gate stays shut until its lease expires
rather than opening before the destination is ready.

### Interrupted Handoffs

Restart or owner replacement for a stopped WASM domain preserves the complete validated lifecycle
and guest checkpoint inventory in its passive revision. Ownership preparation performs no guest
callbacks and does not read the stopped domain clock. `START` restores those saves under the active
clock generation; stopped time does not authorize discarding valid checkpoints or resetting state.

A preparation written durably at a destination outlives the process that wrote it. The coordinator
records every destination as an attempted participant before it sends the side-effecting request, so
a timeout, a cancellation, or a lost response after the destination persisted the request remains
explicit cleanup work rather than an unknown.

The current leader reconciles durable preparations after a leadership or live-incarnation change. It
first commits a consensus barrier and sends its log position with the reconciliation request; each
participant applies through that position before consulting its own committed schedule. A
preparation survives when its exact transition is the committed owner change, regardless of which
processes have since restarted. An uncommitted preparation survives only while the original
coordinator process and both bound participant incarnations are live, the base schedule still has
the source owning the entity, and the exact gate is still held. Every other preparation is discarded
durably. Discard is exact and idempotent over the operation identity, so a delayed discard for one
operation cannot remove a replacement prepared by another.

A destination that restarts between preparation and activation invalidates the handoff: the
operation fails with `destination node '<node_id>' changed process incarnation during ownership
handoff`, the schedule is unchanged, and a later relocation onto that node succeeds normally.

### When The Former Owner Is Gone

Unexpected owner loss is not a handoff. The failed tasks and their volatile buffers disappear
immediately, attached work is negatively acknowledged, and the scheduler promotes a live replica or
chooses a fresh owner without waiting for a gate.

The scheduler acts on what application health and gossip report, so a forced recovery can also move
work off a node that is still running. That node stops the moved runtime when it applies the
published schedule. A batch a relay owner routed to it for a moved consumer, and that reaches its
runtime afterwards, finds no attached consumer and fails its attached acknowledgements, so the
source redelivers the record to the new owner rather than committing it; see
[Consumers That Leave The Receiver](interconnect.md#consumers-that-leave-the-receiver).

Forced recovery is the path that publishes a schedule when the source cannot participate. It stages
the destination's checkpoint inventory under the destination's process incarnation and the complete
target-schedule fingerprint, and applying staged checkpoints accepts only that exact preparation. A
missing or mismatched preparation does not imply a reset: without an exhaustive reset outcome in the
schedule, activation fails and leaves every saved checkpoint unchanged. State is recreated only when
the accepted decision either stages a recreated inventory or reports a reset for every state
component the entity owns.

A forced recovery of a WASM processor starts a new guest-state generation for every branch in the
same schedule publication, and activation publishes the staged checkpoints in that generation. The
former owner's saves, and those of any replica that missed the recovery, belong to the generation it
replaced, so a node that restarts or rejoins with them never restores, serves, or supplies them to a
later recovery. A later owner loss therefore resets a branch whose only surviving checkpoints are of
an earlier generation instead of reviving them. When the processor has replicas, a replica holds
every checkpoint whose acknowledgements the lost owner released, because the owner released them
only after its replicas had synchronized the checkpoint, so forced recovery continues each branch
from at least the state its acknowledged inputs produced. The promoted replica restores every
staged checkpoint into a guest before the schedule is published, from the module it compiled while
it was a replica, so the recovery does not wait for the module to compile. A preparation that fails,
including a guest that cannot restore a staged checkpoint, publishes the recovery with recreated
state; see [Forced Recovery](./wasm-state.md#forced-recovery).

## Topology Cases

| Case | Behavior |
| --- | --- |
| Single node | No replacement exists. Nothing is cordoned, no ownership moves, and all admitted work drains in place before the process exits. |
| Follower with a reachable leader | The node asks the leader, over the cluster interconnect, to drain it, then completes what remains in place. |
| Leader | The leader drains itself in process. Leadership is not transferred first; the cluster elects a new leader after it stops. |
| Last schedulable node | Same as a single node: no replacement candidate exists, so everything completes in place, including source offset commits. |
| Only peer is itself terminating | A terminating incarnation is not a placement candidate, so the node takes the no-replacement path and completes its work in place. |
| Every node at once | Each node advertises terminating, finds no eligible destination, and drains locally within its own drain timeout. |
| Leader unreachable, or no quorum | The node waits for a leader only until its drain budget runs out, reports the drain-support phase abandoned, and still completes its local drain. Nothing is cordoned. |

The no-replacement path is explicit in the log: `no live schedulable replacement node remains;
admitted work completes in place`.

A follower requests its drain, and then the release of the cordon that drain set, with the
`stopping_node_drain` request over the
[cluster interconnect](./interconnect.md#peer-identity-and-authentication). The request names no
node: the leader acts for the node whose certificate authenticated the connection, so a node can
drain only itself, and no user credential takes part. A follower started without
`--init-default-user-password`, as every node but the bootstrap node is in the documented
[Docker deployments](./installation-docker.md), therefore drains through the leader like any other.
A node that receives the request without leading changes nothing and says so, and the follower asks
the leader it observes next, within the same drain timeout. The leader runs the drain in a task of
its own, so a drain that has begun finishes, and releases the gates it engaged, even when the
follower's drain timeout ends its wait first. The interconnect resolves the leader's advertised
endpoint through the follower's loaded resolver, described in [Name Resolution](./name-resolution.md),
within the drain and shutdown deadlines.

## Connector Contracts

[Connector Crates And The Connector Contract](./connector-contract.md) defines normal source and
sink ownership and completion points. This section describes what a node's drain can complete
before its shared stop deadline.

Shutdown does not change any connector's delivery contract. It changes only whether a connector
reaches its completion point before the process ends.

### Sources

Acknowledgements and commits complete before the source session stops, for work that reached its
sinks. A Kafka consumer-group offset is committed during the drain once the records it covers have
been acknowledged through the graph. Work whose acknowledgement root is still unresolved when the
drain ends is negatively acknowledged, so the source's own redelivery contract applies. This is why
a drain that times out leaves Kafka offsets uncommitted rather than advancing them.

What that contract is depends on the source, and three groups differ sharply:

| Source | Source acknowledgement | Admitted work when a drain does not finish |
| --- | --- | --- |
| Kafka, Pulsar, RabbitMQ, SQS, and MQTT in an `ACK` mode | The offset is committed, the broker acknowledged, or the message deleted only after the record is acknowledged through the graph | Redelivered after the restart |
| HTTP polling, Prometheus | None; the poller re-reads its source each cadence | Read again by a later poll |
| NATS, Redis Pub/Sub, ZeroMQ, WebSocket clients, HTTP endpoints, Syslog | None exists; these sources offer no acknowledged mode | Lost, with nothing to redeliver it |
| Client ingestors | The producer receives each batch's outcome once its acknowledgement root resolves | Reported to its producer as of unknown outcome, or not at all when the producer's session ended first; the application decides whether to submit it again |

The last row is the one to plan around. Those sources have no acknowledged delivery mode at all, so
a record admitted from them and not yet emitted is lost both by a drain that runs out of time and by
every forced ending. An endpoint rejects new requests as soon as intake stops, so a client that
handles the rejection can retry; a payload it already accepted is not redelivered by anything.

A general failure handled by `ON GENERAL ERROR IGNORE` acknowledges the record, while
`ON GENERAL ERROR LOG` negatively acknowledges it. During a drain that choice decides whether an
acknowledged source redelivers the record after the restart.

### Sinks

An emitter reaches the sink completion point its `MODE` declares, and the drain waits for it. An
emitter that cannot finish reports `emitter '<name>' did not drain before its configured deadline`
and holds the drain until the timeout. A batching emitter's drain also writes every batch payload an
earlier attempt left unanswered, with the bytes and members it was first written with, as an OTEL
emitter's drain sends every Export request it prepared and did not learn the outcome of, and the
emitter buffer counts those members as work the node still holds until they resolve.

A native client emitter reaches its success boundary only on application ACK. Force flush prepares
its bounded Arrow IPC batches, but a waiting consumer read or a batch already sent to a session
does not drain them. Closing public sessions during intake stop detaches their consumers and
revokes attempts; remaining prepared batches wait for another consumer within the physical drain
deadline. On deadline expiry terminal teardown cancels the emitter task and discards volatile
attempt history. An attached upstream source may replay after restart, including a batch whose
application effect happened but whose ACK was lost. No durable consumer cursor is restored.

An HTTP emitter force-flushes its collected records into one request per eligible record, then
waits for complete successful final response headers. Each attempt and retry wait remains bounded
by the remaining physical drain deadline, even in a paced domain. A request still unanswered at
that deadline is not marked delivered. Its prepared bytes, selected destination, headers, and
retry state are volatile and disappear when the process ends. An attached acknowledged source can
redeliver the record after restart; an unacknowledged endpoint source cannot. A destination that
applied a request whose successful response was lost can receive it again after that redelivery.
Once the graph drain budget ends, terminal teardown cancels an emitter task waiting in an HTTP
attempt. It does not wait for that client's full attempt timeout or turn the canceled request into
a record-specific success or rejection.

Kafka is the only sink whose client-side queue shutdown drains explicitly: after its buffered
batches are published, the emitter host calls the sink contract's finish hook with the remaining
stop deadline. Kafka implements that hook by flushing the producer's local queue within the same
deadline. Other sinks hold no such queue, so their default finish hook completes immediately and
the publish itself is the completion point.

A sink that stages what it accepts and publishes it later reaches its completion point at its
commit instead. After the buffered batches are written into such a sink, the drain calls the sink
contract's commit hook and forces it, so the commit runs whatever its commit cadence says and
without reading the domain clock, which a stopped domain no longer has. Until that commit succeeds
the sink holds the acknowledgements of every row it staged, and the drain counts those rows as work
the node still owes. A commit that fails is retried on the
emitter's declared backoff, reported as the emitter's commit retry, and a shutdown that cuts the
backoff short fails the drain.

Iceberg is the sink that does this. It commits its staged data through its catalog, forced by the
drain rather than waiting for the `COMMIT EACH` cadence. Staged rows live as local files that only
a successful commit removes, so a forced ending leaves them behind: the rows are not in the table,
and a later run does not reclaim or commit them. An attached source redelivers that work; a
detached one loses it.

### Duplication

A graceful shutdown creates no new duplicate condition, but redelivery after a forced ending or an
abandoned drain produces exactly the duplicates the source and sink combination already allows. A
sink that already published successfully can receive a record again after a restart, because
acknowledgement state is in memory and does not survive the process. See
[ACK Semantics And Effective Delivery](./emitters.md#ack-semantics-and-effective-delivery) for the
per-sink duplicate and loss conditions and the idempotency options each sink offers.

### Branch Isolation

Branch isolation is preserved throughout. Draining walks concrete branches one at a time,
branch-local state stays branch-local, and interleaved branches complete independently. Branch
expiry is suspended while an entity is frozen for an ownership handoff, so a drain cannot silently
expire branch state mid-handoff. Nothing about shutdown merges branches or moves work between them.

## Terminal Teardown

Terminal teardown begins only after drain support completes or reports itself abandoned.

It stops the node's background tasks, giving each at most two seconds, then waits for work that
sessions and commands started until the shutdown deadline and cancels whatever still runs. It then
stops the runtime, consensus, cluster membership, and the interconnect in that order, and finally
releases the node's storage.

Runtime teardown ends each resolved replication route before clearing its state registry. A
retained route cannot attach to replacement state; a request already admitted through a borrowed
checkpoint handle may finish on that exact state. After domain drain, closing the replication task
owner cancels its announcers, including pending dispatches and retry waits, and waits for their
retained handles to be released before these routes are cleared. Teardown does not wait for an
announcement's remote dispatch timeout; synchronization supplies any missed availability hint.

Closing a Kafka offset replica task also cancels its pending revision request or bulk checkpoint
stream immediately. A partially received or cancelled conversion never publishes a checkpoint or
acknowledges its revision. Native conversion checks cancellation between entries and before
publication; the replica's assignment token still fences installation if ownership changed.

Consensus stops Raft and then waits for its storage to reach an idle barrier, which proves every
earlier durable write has returned and released the store, before its dedicated database handle is
dropped. The registry and runtime database is released separately after the services that hold its
handles. Terminal teardown does not finish until both database locks have been released. See
[Consensus Storage And Replication](./consensus-storage-and-replication.md) for the write, replay,
retention, and snapshot contracts behind this barrier.

The interconnect rejects new admission, cancels pool and operation waiters, and retires the pool
and peer protocol owners. An ending peer owner cancels unadmitted records and releases item and
terminal-outcome permits even if the runtime retains an intake already admitted; that intake's
verdict remains admitted. Decoded metadata stays charged while a borrower keeps it. Runtime
teardown cancels and joins its charged ACK watcher tasks, closes delivery and admission routing
queues, and negatively resolves remaining volatile correlations exactly once. No ACK owner state
is stored for restart. The interconnect also retires the pool
connections the node opened, giving their leased streams up to ten seconds before closing whatever
remains. Connections that peers opened to the node close at once, together with the handlers still
serving their streams, so a peer's request the node has not answered fails instead of completing.
A forced ending skips this entirely, so peers observe the connections ending exactly as they do
when a process crashes.

The node's own OTLP trace exporter is a process service. After the application returns, its tracing
guard closes resolver installation and asks the SDK to flush before the process drops its Tokio
runtime. An installed resolver remains available to queued exports. If startup failed before DNS
installation, closing the publication ends a pending collector connection. Export failures remain
telemetry diagnostics with no data-plane acknowledgement consequence, and a forced process ending
does not guarantee a final export. [Node Trace Export](./name-resolution.md#node-trace-export) owns
the resolver lifetime and export budgets.

### Consensus Work At The Ending Boundary

Consensus remains live through stop admission and drain support. A terminating node can still append
and apply control-plane entries during those phases, answer peer heartbeats, and carry replication.
The boundary below begins when terminal teardown stops Raft.

| Work in progress | Graceful `SIGINT` or `SIGTERM` | Repeated signal, expired deadline, or `SIGKILL` |
| --- | --- | --- |
| Append batch | A storage job already admitted to the ordered worker completes before the idle barrier can pass. Its complete atomic batch is synchronized before the database closes. An append alone is not a client acknowledgement. | There is no idle barrier. Recovery finds the last complete synchronized batch or its predecessor; Raft may commit or truncate an uncommitted tail. |
| Applied range | A write already submitted completes atomically with its final applied position. Entries committed but not yet submitted remain in the log and are replayed on restart. | Recovery finds the last complete applied range and streams every later committed entry from the log. A response can be lost after durable application, so client silence remains an uncertain outcome. |
| Snapshot build or transfer | Completed section writes remain durable. A generation with no published manifest stays inactive and is reclaimed later. A published installation marker is finished on restart if teardown did not reach its final publication. | The same durable boundaries decide recovery. Partial section writes are absent; staged unpublished generations stay inactive; a marked installation is redone in full. |

Stopping Raft ends each append-stream generation while the interconnect is still running. The
stream stops accepting new submissions, releases its charged follower batches, and its peer observes
a normal stream end or failure and resumes from its last confirmed progress if another leader path
exists. The terminating node is still a Raft member and does not transfer leadership as part of
shutdown, so peers otherwise react to its silence through the ordinary heartbeat and election
rules. Interconnect shutdown follows and records any remaining reset with `reason="shutdown"`.
A forced ending skips the protocol close; peers see an abrupt stream or connection loss.

## What Survives

Three persistence boundaries decide what a restart finds. Control-plane state is replicated and
strongly consistent, selected runtime state is checkpointed, and the hot path is memory only.

| State | Graceful shutdown | Forced ending or `SIGKILL` |
| --- | --- | --- |
| Committed control-plane state: models, schedules, domain lifecycle, cordons, users, resources | Reopens from the committed generation | Reopens from the last committed generation; nothing acknowledged is lost |
| Published consensus snapshots | Reopen from the current manifest; a marked installation is finished before state is exposed | Reopen from the current manifest; a marked installation is redone before state is exposed |
| Durable handoff and forced-recovery preparations | Preserved, then reconciled or activated | Preserved, then reconciled or activated |
| Runtime-state checkpoints: Kafka domain offsets, deduplicator and window state, materialized relay records | Flushed again as runtime tasks stop; a materialized relay, and a window whose retained rows exceed one 8 MiB snapshot section, are sealed to quota-owned files and written as segments synchronized before their header | Reopen at the last completed periodic checkpoint |
| WASM guest-state checkpoints | Every checkpoint that released an acknowledgement is already synchronized | Reopen at the newest checkpoint on the node's storage, which covers every acknowledged input |
| Restored backup state: materialized relay rows, WASM guest saves, Kafka domain offsets, branch lifecycle, deduplicator keys and window state | A restore synchronizes a complete chunked namespace and its atomic active-generation pointer before releasing the stopped domain's START gate or resuming its archived lifecycle | Reopens pointer and checkpoints from one complete database view on each assigned owner and replica; a durable pointer alone does not release an incomplete replicated installation gate. A resumed domain retains every restored kind. A normal START continues saved guest state, source positions, branch incarnations, deduplicator keys and windows, and resets materialized rows |
| External source offsets and sink commits | Complete when the drain succeeds | Only the external connector's own delivery and transaction guarantee applies |
| Relay batches, queued payload attempts, suspended work, ACK guards, ACK tokens, ACK maps, handoff payloads, gate leases, clock progress | The drain tries to resolve them before its deadline | Volatile; lost |
| Client producers, their credit and queued batches, producer links | Ended as `shutting down` after the drain, or detached with their sessions | Volatile; lost with the process. Every producer another node forwarded here ends there as `owner lost`: its batches that node never cleared for admission are refused as `producer ended`, and only the cleared ones are of unknown outcome. The node's own sessions end, so their clients report every batch they sent without an outcome as of unknown outcome, and the nodes executing the ingestors detach those producers and finish what they admitted |

A materialized backup captures the current relay generation independently of the periodic
snapshot interval. Restore streams its complete sealed container into the generation store,
including containers larger than the bulk memory budget. `RESUME` activates the archived domain
generation only after every assigned owner and replica has published its complete set. These
rows reopen from one pinned database snapshot across a node restart; the stored clock mapping
projects downtime under the recovered authority. A failed installation retains its durable
activation gate across restart. A normal `START` creates a new generation and clears materialized
rows while retaining compatible WASM, offset, branch lifecycle, deduplicator and window state. Periodic materialized
flushes seal to quota-owned files and write bounded database segments, synchronizing their data
before the replacement header, so a large relay has the same shutdown durability boundary. See
[Backup And Restore](backup-and-restore.md#publishing-the-state-generation). A deduplicator or
window backup likewise captures what each branch task publishes when the cut's lifecycle
checkpoint asks it to, independently of the periodic interval, and a restore rebuilds each branch's
native checkpoint from its archived Arrow groups and streams it through the same publisher, one
bounded group at a time, including a keyspace or a window larger than the bulk memory budget. A
window whose retained rows exceed one 8 MiB snapshot section is persisted the way a materialized
relay is: its periodic and stopping checkpoints seal in bounded pieces to a quota-owned file, and
its segments are synchronized before the header that selects them, so a restored window larger
than the bulk memory budget survives a graceful restart with the rows it admitted afterwards.

Durability is not uniform across those rows, and the difference is operationally visible:

- Consensus acknowledges a vote, an appended batch, or an applied range only after synchronizing
  both data and filesystem metadata, so committed control-plane state survives host power loss on a
  device that honors those requests. Synchronization is per reservation-bounded batch or applied
  range rather than per entry.
- Every handoff and forced-recovery preparation, activation, and discard is synchronized the same
  way.
- Periodic runtime checkpoints are written to the operating system without forcing a synchronization
  on every update. They therefore survive the death of the process, including `SIGKILL`, but a host
  power loss can lose the most recent ones. This is the boundary the crash qualification asserts:
  exact counts are guaranteed only for checkpoints known durable before the kill. Segmented
  checkpoints, those of materialized relays and of windows beyond one snapshot section, synchronize
  their segments and then their header, so a power loss keeps the previous complete checkpoint.
- A WASM guest-state checkpoint is synchronized, on the branch's owner and on every replica the
  schedule assigns, before the source acknowledgements it covers are released, so every checkpoint
  that released an acknowledgement survives a host power loss. Checkpoints that branches take at the
  same time share one synchronization. See
  [Failure At Each Boundary](wasm-state.md#failure-at-each-boundary) and
  [WASM Processor Guests](wasm-processor-guests.md#recovery-replay-and-duplicates) for what a
  recovered branch continues from and which inputs its source redelivers.

Branch-local processor state survives only to its latest publication. A branch task aborted after
exceeding its grace period keeps what it had published and loses the changes it made afterwards.

## Restart And Recovery

### Recovered Ownership Is Fenced

A restarting node does not execute the ownership recorded in its own database until it has proven it
is caught up with the cluster. On each process start it asks consensus for an admitted runtime state
through a linearizable read, which confirms the leader's authority with a quorum and returns a
committed log boundary. The node applies through exactly that boundary before it reads the domain,
clock-authority, and schedule state it installs, and it retries every half second until that read
succeeds.

After admission, the control plane converts the committed schedule into a complete typed execution
revision before runtime installation. Running and stopped domains use that same revision for local
placement, state identities, passive recovery, and ownership handoff. The handoff fingerprint keeps
the committed schedule's exact bytes. If an installation fails, the next attempt is planned from
the last successfully applied schedule; the failed revision is not recorded as applied.

Until admission succeeds, the node is live but inert with respect to ownership. Its public listeners and
its interconnect answer requests, which keeps configured listening entities available on every live
node, but no runtime routes exist, so a payload it accepts cannot reach recovered graph execution.
A client producer or consumer open is refused as temporarily unavailable until the node has passed
the catch-up barrier; the node does not report a stale local missing or stopped domain as a terminal
endpoint refusal during that interval. The client can retry the open on its restored session.
A former owner restarted while cut off from consensus therefore produces no output, and once
connectivity is restored it observes the current schedule and forwards traffic to the node that now
owns the work.

The node's log marks the boundary. When its first read attempt fails, it logs this once per process
at `warn`:
`runtime execution is waiting for linearizable consensus catch-up`. When admission succeeds, it logs
`runtime execution admitted after linearizable consensus catch-up` at `info`, with the
`committed_log_index` it applied through.

At startup the node offers the peer endpoints retained in its Raft membership as gossip seeds. A
former bootstrap node therefore has a path back to surviving peers even if it was originally
configured without a bootstrap host. The endpoints only initiate authenticated contact; gossip
establishes each peer's current incarnation and endpoint before normal peer routing and runtime
admission proceed. A recovered endpoint that does not resolve is skipped while other seeds and
incoming gossip remain available.

This is the fence that prevents crash recovery from reviving an obsolete owner. It is a
process-start admission proof only: connectivity lost after admission does not revoke execution.

### Whole-Cluster Restart Keeps Ownership

When every node restarts, the first node to lead can form a quorum while the others are still
starting and gossip has not observed them live yet. Its automatic scheduling therefore waits, for
the first ten seconds of its reconciliation, while any current voter has never been observed live
by this process's reconciliation task. A first heartbeat relayed by another peer can initially put
the voter in Chitchat's dead set before sufficient heartbeat intervals establish liveness. That
initial dead verdict keeps the voter unobserved during the grace.
Each owner that returns in that time keeps its work and restores it from its own storage and
replicas. Live observations made while following survive a leadership change, and the original
ten-second deadline continues running. Once it expires, a voter that never returned is eligible for
ordinary failover. The observation history is private to this process and is discarded after the
grace. See
[Planned Ownership Handoffs And Failover](./control-plane.md#planned-ownership-handoffs-and-failover).

### Restoring Processor Branches

A processor task restores the branches its branch lifecycle checkpoint names before it takes any
input. It reads the checkpoint an ownership transfer left for the node, else the lifecycle the node
holds, else the one its storage keeps, and each branch resumes the incarnation the checkpoint
records. A window branch therefore reopens the window its retained rows hold, and a deduplicator or
WASM branch resumes the state it checkpointed. The restore reads only the node's state and the
processor's execution plan, which carries the schema a window's retained rows are read under, so it
does not wait for the domain's routing, which installation publishes after it starts the tasks.

A restore installs every branch or none. It builds each branch and opens its retained state first,
starts the branch tasks only once all of them are built, and releases a transferred checkpoint only
after that. When a branch cannot be built, because storage cannot be read, a checkpoint does not
decode, or the bounded executor refuses the decode, the task installs nothing and logs `failed to
restore processor branches; their input waits for the next attempt` at `warn` with the cause. It
tries again after a backoff that starts at a quarter of a second and doubles up to thirty seconds.
Until an attempt succeeds:

- The processor dequeues no input. Its relays keep their records and apply backpressure upstream,
  so no record starts a new lifetime for a branch the restore resumes and discards its retained
  state. Input a drain still delivers is negatively acknowledged.
- A lifecycle checkpoint request is refused, so a backup capture or an ownership handoff that needs
  one fails instead of recording a lifecycle without the processor's branches. A WASM guest-state
  reset is refused the same way.
- A task that replaces it receives the branches it never installed, and restores them itself.

A failure that persists, such as a checkpoint written in a shape the node no longer reads, keeps
the processor waiting and repeats its warning on every attempt.

### Checkpoint Identity

A restart reopens a runtime-state checkpoint only under the identity the committed schedule
publishes for its entity. Kafka domain offsets and the metric summaries behind `DESCRIBE` output
depend on no schema: they are keyed by their entity alone and survive a restart whatever schemas
changed while the node was down. Every other checkpoint, including deduplicator, window, and
materialized relay state, branch lifecycle records, and WASM guest state, is keyed by the
fingerprint of the schemas its entity lays records out by, and WASM guest state also by its
generation. Window state additionally includes the current window model in its identity, so a
replacement that changes `WIDTH`, `STEP`, or aggregate expressions begins with an empty window even
when its schemas are unchanged. A checkpoint written under a replaced fingerprint is never restored
as the new layout, served, replicated, handed over, or selected by a forced recovery. Applying the
committed schedule of a running domain removes it. Until the node has applied a schedule that names
an entity,
it has no fingerprint for that entity's schema-bound state and does not place that state at all.

Recovery also validates the current representation before decoding its counts. Registry Model
frames and the dedicated consensus database identify their fixed-width 64-bit count shape;
unrecognized stored state fails with an instruction to recreate it. Window checkpoints use the
current runtime-state kind and `NVXWIN64` frame signature. Native decoding of an archived count is
checked and cannot truncate it to fit the target. Consensus recovery accepts only the records its
state writer stores, each under its own canonical key and in a state its transitions reach, and
fails on anything else with the same instruction. See
[Archived Counts](./typed-states.md#archived-counts) and
[Storage Layout And Compatibility](./consensus-storage-and-replication.md#storage-layout-and-compatibility).

### Interrupted Snapshot Installation

Installing a consensus snapshot publishes the manifest and a marker naming the generation whose
records still have to replace the state machine, then replaces those records and clears the marker.
A node that stops anywhere between the two finds the marker on its next start and redoes the
replacement in full, however far the interrupted attempt had got. It publishes no recovered state
until that replacement finishes. Sections staged before any manifest was published never become
active; startup marks every generation the current manifest does not name as unreferenced, and a
later durable consensus batch removes it. See
[Transfer And Installation](./consensus-storage-and-replication.md#transfer-and-installation) for
the section, marker, and final-publication boundaries.

### Replayed Forced Recovery

A completed forced recovery is recorded durably against the ownership transition itself, not against
the process or schedule that executed it. Reapplying that retained schedule after a destination
restart or a later domain rebuild is therefore a no-op rather than a second recovery, and any newer
checkpoints the destination has published since are preserved. The WASM guest-state generation the
recovery published is part of that committed schedule, so reapplying it names the same generation
and starts no further lifetime.

### Abandoned Preparations And Staging

A new leader reconciles durable handoff preparations after a coordinator or participant is lost, as
described above. Resource uploads that were staged but never promoted are removed at startup, so an
upload interrupted by a forced ending leaves no partial version behind. Only the store's own staging
directories begin with a dot: a resource whose name does keeps a directory whose name writes that
dot as `%2E`, so startup never removes its versions. See
[Resource Versions And Bindings](./resource-versions.md).

A backup archive a node retains for download is a temporary file in its staging area and is never
durable. Stopping the node, gracefully or not, loses it: a later download is refused, while the
backup's recorded outcome stays retained under its execution reference. See
[Backup And Restore](./backup-and-restore.md#downloading-the-archive).

Unpublished restore checkpoint namespaces remain in the runtime database after an interrupted
installation. Every restarted node's admitted maintenance borrows its applied consensus revision
before taking the checkpoint installation barrier. It retains selected publications, applying
installations and generations ahead of catch-up; without an applied log it retains all generations.
Terminal or superseded applied attempts are reclaimed in bounded deletion batches, including
chunks without receipts. Terminal teardown cancels the maintenance caller, and its storage job
checks cancellation between bounded units. A later startup resumes from the remaining keys.
The same owner reclaims selected-namespace chunks made unreachable by ordinary or replica
checkpoint replacement, ownership recovery or purge, including after restart. It keeps the
selected publication and every currently referenced segmented revision.
Reclamation preserves snapshot readers and never completes an installation or opens its `START`
gate. See [restore checkpoint storage](backup-and-restore.md#restore-checkpoint-storage-quota-and-metrics)
for quotas, metrics and physical storage limits.

### Domain Time

The paced mapping, lifecycle generation, and authority fence recover from consensus state. Tick
progress, delivery loops, local read watermarks, and process-monotonic deadlines do not. After a
restart, nodes install the committed mapping and the leader reconciles an authority among the
current live voter incarnations. Because the authority identity includes the process incarnation, a
restarted process with the same node name cannot act as the authority its predecessor was. See
[Domain Clock](./domain-clock.md#recovery-and-distributed-guarantees).

## Observability

Shutdown is observed through its log, not through a dedicated metric family. There is no
shutdown-specific metric; a node that is shutting down is identified by its phase log lines and by
`terminating: true` in its status.

The phase records are the primary signal:

| Level | Message |
| --- | --- |
| `info` | `termination signal received; requesting graceful shutdown` |
| `info` | `termination signal received while graceful shutdown is already in progress` |
| `info` | `advertised terminating process incarnation` |
| `info` | `shutdown admission phase finished` |
| `info` | `shutdown drain-support phase finished` |
| `info` | `shutdown terminal-teardown phase finished` |
| `warn` | `repeated termination signal received; abandoning graceful shutdown` |
| `warn` | `shutdown deadline expired; abandoning graceful shutdown` |

Drain decisions and failures are logged beside them: `preserving operator cordon across graceful
shutdown`, `no live schedulable replacement node remains; admitted work completes in place`, `timed
out reaching the leader before requesting a graceful shutdown drain`, `drained local node before
graceful shutdown`, `failed to drain local node before graceful shutdown`, `the leader did not
answer the graceful shutdown drain of the local node`, `timed out moving scheduled work off the
local node before graceful shutdown`, `cleared shutdown drain cordon before graceful shutdown`, and
the two drain-timeout records above. The drain and cordon records name the `leader` that acted,
which is the node itself when it leads, and carry the leader's account of each moved unit as
`message`. Each phase record carries its outcome, so `outcome=Completed` distinguishes a finished
phase from an abandoned or forced one.

Existing metric families move during shutdown without naming it. Interconnect stream resets count
`reason="shutdown"`. The ingestor quiesce families change as intake stops, but they carry no cause
label, so a shutdown hold is not distinguishable there from another hold. Live branch instances fall
without incrementing the eviction counter, because a stopping node is not evicting branches. A
client ingestor's endpoint logs `ended the producers of a client ingestor` at `info` with the count
and reason `shutting down`, its gauges fall to zero, and the batches it answered while draining are
counted under `nervix_client_ingestor_submissions_total` as `draining` refusals or as the outcomes
their roots resolved to. See
[Metrics And Observability](./metrics-and-observability.md).

The health endpoints do not describe shutdown. `/livez` answers while the process is alive, and
`/readyz` reports whether the node currently knows a leader. Neither turns negative when the node
starts terminating, so an orchestrator that removes endpoints on readiness failure will not remove a
draining node on that signal alone. Closing the public listeners in the stop-admission phase is what
ends client traffic.

### Troubleshooting An Incomplete Drain

Start from the outcome on each phase record.

- **`shutdown terminal-teardown phase finished` absent, process exited `130` or `143`.** Something
  sent a second signal. Give the node its full shutdown timeout, or raise the supervisor's grace
  period so it does not signal twice.
- **`shutdown deadline expired`, exit status `1`.** The node could not finish in its shutdown
  timeout. Read the drain-timeout record for the domain counts, and raise `--shutdown-timeout`
  together with the deployment grace period if the workload legitimately needs longer.
- **`local graph drain timed out with admitted work outstanding`.** Read the counts. Non-zero
  publishing emitters or buffered emitter messages mean a sink is not accepting; non-zero
  outstanding acknowledgements mean an ACK chain has not resolved; non-zero pending `REQUIRED WAIT`
  records are expected and do not hold the drain.
- **`local graph drain timed out before confirming that no admitted work is still moving`.** A peer
  is still publishing into this node's relays. Drain or stop the upstream node first.
- **`timed out reaching the leader before requesting a graceful shutdown drain`.** The node observed
  no leader before its drain timeout. It still completed its local drain; its scheduled work is
  reassigned by failover once it is gone.
- **`failed to drain local node before graceful shutdown`.** The leader moved only part of the
  node's work. Its `message` names each unit and why its move failed; those units fail over once the
  node is gone.
- **`the leader did not answer the graceful shutdown drain of the local node`.** The request did not
  come back from the `leader` the record names, and `error` says why, such as a leader that left the
  cluster or a connection that could not be opened. The leader may still have moved some of the
  node's work; whatever it did not move fails over once the node is gone.
- **`timed out moving scheduled work off the local node before graceful shutdown`.** The drain timeout
  or the shutdown deadline ended the wait for the leader's answer. A leader on another node still
  finishes the moves it began; whatever is not moved fails over once the node is gone.
- **Work reappears after a restart.** That is redelivery, not duplication of committed work: the
  drain ended before those records were acknowledged, so their source offered them again.

## Deployment Grace Periods

A supervisor must allow a node to finish its own shutdown. Every deployment should set its grace
period longer than the node's shutdown timeout, so the node exits on its own terms rather than being
killed partway through a drain.

| Deployment | Setting | Value alongside the `50s` default |
| --- | --- | --- |
| `docker run` | `--stop-timeout` | `60` |
| Docker Compose | `stop_grace_period` | `60s` |
| Kubernetes | `terminationGracePeriodSeconds` | `60` |

Docker's own default of ten seconds is shorter than the default drain timeout and would kill a node
mid-drain. When you raise `--shutdown-timeout`, raise the deployment grace period with it; when the
grace period ends first, the supervisor sends `SIGKILL` and the node loses whatever the drain had
not yet completed.

Raising `--drain-timeout` above `--shutdown-timeout` does not extend the drain, because every drain
step is also bounded by the shutdown deadline. Raise both together.

## Retained Drain Dependencies

Running source, generator and sink tasks retain their acknowledgement trackers and confirmation
counters. A drain observes those same registrations. Emitter retry state remains publishing work
even when its buffer is empty. Idle and already-claimed force-flush participants use a retained
readiness hint; only an available obligation enters the coordinator to claim its generation. A
released claim becomes available again. Task teardown removes its confirmation/status registration,
and pooled sink teardown removes its wait registration; cancellation clears a pending borrow's
wait through its guard. Handoff watches register before reading their retained entity publication,
and release publishes thaw before waking them.
