# WASM State And Recovery

A WASM processor runs one guest instance per concrete branch, and each guest keeps computation state
that Nervix makes durable. The host checkpoints that state at the end of every guest callback,
confirms it with the processor's replicas, restores it into every instance it recreates, and
replaces it when a command, the guest itself, or the processor's rejected-state policy asks for a
new lifetime. This chapter defines that contract end to end: who owns each piece of state, what is
durable and what is not, how a checkpoint becomes the state a recreated instance restores, which
identity fences out every earlier lifetime, how a lifetime is replaced, and what each kind of
failure leaves behind.

The central rule is that guest state and message delivery are separate guarantees. A checkpoint
makes the guest's computation state durable before the source acknowledgements it covers are
released. It does not make the guest, its output, and external sinks one transaction, so a path
through a WASM processor is at least once.

This chapter describes the architecture. [WASM Processor Guests](./wasm-processor-guests.md) owns
the guest ABI and the user-facing account of checkpoints, resets, and failure diagnostics, the [Rust
WASM Guest SDK](./wasm-guest-sdk.md) owns the Rust guest surface, and [Runtime
Nodes](./processors.md#wasm-processor) owns the NSPL statements.

## Ownership By Layer

| Layer | Owner | What it owns |
| --- | --- | --- |
| Vocabulary | WASM state models | The guest-state generation of every branch, a reset's scope, phase, and reason, the recovery attempts refused lifetimes have spent, and the typed state inspection. The processor's entry in the committed domain schedule carries all of them. |
| Language | NSPL | `ON REJECTED STATE`, `RESET WASM PROCESSOR ... STATE`, and `DESCRIBE WASM PROCESSOR`, lowered into Models. |
| Engines and infrastructure | The WASM host | Compiling a pinned module, instantiating a branch store, the fuel and memory limits of every guest operation, the domain snapshot each operation reads, and answering a guest's reset request. |
| Engines and infrastructure | The guest SDK | The guest side of the ABI: the snapshot envelope, strict restore verdicts, and keeping its own execution state out of every save. |
| Engines and infrastructure | The runtime state store | Writing a checkpoint under its placement, and the durability barrier that synchronizes the node's storage for every writer waiting on it. |
| Engines and infrastructure | The interconnect | Replica synchronization and acknowledgements, and the coordination requests a reset and a recovery send. |
| Decisions | Registry and scheduling | Planning a reset as a transaction step, and deciding which schedule publication starts a new generation. |
| Data plane | The branch task | Running guest callbacks one at a time, holding back the acknowledgements each callback decides, and checkpointing the instance the callback leaves behind. |
| Data plane | The processor supervisor | Fencing the scope of an unfinished reset, and preparing, aborting, and committing a reset on the owner. |
| Control plane | The reset coordinator on the leader | Validating a reset, gating its scope, preparing it, and publishing it through its `Publishing` and `Ready` phases. |
| Control plane | The recovery coordinator on the leader | Deciding whether a refused lifetime still has its one recovery attempt, recording the decision, and driving the reset it admits. |
| Edges | The session service and clients | Admitting an NSPL reset as a transaction step, and returning the typed state inspection beside the text of `DESCRIBE WASM PROCESSOR`. |

Guest bytes never leave the data plane and the state store. The control plane decides lifetimes and
never reads a checkpoint; the data plane executes the lifetime the committed schedule names and
never decides one.

## Branch Ownership

The runtime creates one guest instance per concrete branch. Each instance has its own Wasmtime
store, linear memory, guest state, and timeout handles; a node shares only the compiled module
between them. See [Module Sharing And Branch
Memory](./wasm-processor-guests.md#module-sharing-and-branch-memory).

An unbranched processor has exactly one execution, the explicit unbranched instance. It is never a
branch with an empty key: its `BranchInit` payload carries no `branch_key`, its state placement
names no branch, and a reset selects it with the explicit `UNBRANCHED` scope. A branched processor
never has an unbranched instance.

The committed schedule gives each processor one owner and zero or more replicas. The owner runs
every branch's instance and is the only node that checkpoints guest state. A replica holds durable
copies of the owner's checkpoints. Any other node runs a guest only to prove it can restore a
checkpoint while an ownership move prepares that node as the new owner. The control plane and the
inspection name a concrete branch by its fingerprint, a fixed-size digest of the canonical
branch-key text, never by its field values.

A concrete branch that is evicted for its `TTL` or its instance limit stops without a quiesce flush,
and its live instance and the work it buffered are dropped with it, as for every evicted processor
branch. Its guest-state checkpoint stays. Guest state is addressed by branch key and generation, not
by the incarnation of the branch, so the same key appearing again restores the last checkpoint that
branch committed. Window state differs: an evicted window branch publishes an empty final window.

## What Is Durable

A guest saves only the computation state a recreated instance needs to continue: counters,
aggregates, open windows, or whatever else it derives from the input it has accepted. Everything an
instance uses to execute belongs to that instance.

| State | Where it lives | Survives |
| --- | --- | --- |
| Guest computation state, the bytes `nervix_dump_state` returns | The state store of the owner and of every replica that confirmed it | A restart, and an owner loss when a replica holds it |
| Generations, the latest reset, and the recovery attempts refused lifetimes spent | The committed domain schedule | Everything consensus survives |
| Checkpoint revisions | Stored with each checkpoint; the owner's next revision is derived from the one it restored | With the checkpoint |
| Input the guest buffers, with its ACK tokens, row sidecars, and input-column references | The instance | Nothing |
| Output groups the guest has not emitted, and output already dispatched to relays | The instance and the relays | Nothing |
| Timeout handles | The instance's host store | Nothing |
| A guest's latched error state | The instance | Nothing |
| Acknowledgements held back for a checkpoint | The branch task | Nothing |
| Guest reset requests not yet coordinated, and raised rejected-state recoveries | The owner's memory | Nothing |
| Checkpoint progress observations | The owner's memory | Nothing |
| Compiled modules | Each assigned node's memory | Nothing; recompiled from the pinned version |

A recreated instance therefore starts without buffered input, pending output, pending timeouts, or
error state. The host keeps an instance whose callback failed unless the failure exhausted `MAX
FUEL` or `MAX MEMORY`, so a Rust SDK guest that latched into error state stays latched, and keeps
saving its computation state, until its instance is recreated. Acknowledgement trees and tokens are
in-memory hot-path state everywhere in Nervix; see [Acknowledgement
state](./data-plane-concurrency.md#acknowledgement-state).

## Guest Lifecycle

The host creates an instance when a branch has none and work arrives for it: the branch's first
input, the first input after a callback exhausted a limit or a checkpoint failed, a node restart, an
ownership move, and a replaced lifetime. Creating one runs these operations in order:

1. The host instantiates the module into a new branch store.
2. It calls `nervix_init` with the branch configuration.
3. When the branch's committed checkpoint holds bytes, it calls `nervix_load_state` with them. A
   branch in a new lifetime has no checkpoint yet, so the instance starts from its initialized
   state.

The instance then runs callbacks: `nervix_process_batch` for input, `nervix_on_timeout` for each due
timeout, and `nervix_flush` whenever the host force-flushes the branch, which every entity or domain
pause, ownership handoff, reset preparation, and shutdown drain does. After a callback the host
drains `nervix_read_emit` and saves the instance with `nervix_dump_state`. A guest asks for a new
lifetime with the `nervix_request_state_reset` import, which the host accepts only while one of
those three callbacks is running. The ABI also requires a `nervix_reset_state` export. The host
resolves it when it instantiates a branch and never calls it: every new lifetime starts in a new
instance.

Every guest operation receives its own `MAX FUEL` budget and one explicit domain execution snapshot.
Instantiation, initialization, restore, each callback, and each save are separate operations; the
emit reads after a callback share that callback's budget. See [Execution
Limits](./wasm-processor-guests.md#execution-limits) and [Execution-Time
Snapshots](./domain-clock.md#execution-time-snapshots).

A zero-length save means the guest has no state: the next instance is initialized without a
`nervix_load_state` call. The Rust SDK wraps every save in a `GuestSnapshot` envelope that also
carries the branch configuration, so empty application state is still restored as state, and a
snapshot taken under another branch configuration is rejected.

A restore either succeeds, or the guest rejects the saved state with one of the two reserved verdict
codes, or it fails without a verdict: a trap, an exhausted limit, or another negative code. Only a
verdict classifies the saved bytes as unusable. Each outcome is reported at its own [failure
stage](./wasm-processor-guests.md#failure-diagnostics), the inputs that were waiting for the
instance go through `ON GLOBAL ERROR`, and the saved state stays in place, so the next input hands
the same revision to a new instance. Only the processor's [rejected-state
policy](#rejected-state-recovery) acts on a verdict, and only a [forced recovery](#forced-recovery)
can discard state after a failed restore.

## Placements, Generations, And Revisions

### Placements

Every checkpoint is stored, served, replicated, transferred, and restored under a placement: the
domain, the processor, the concrete branch or the unbranched instance, the fingerprint of the
schemas the processor's records are laid out by, and the branch's guest-state generation. A node
places guest state only under the identity the committed schedule it applied publishes for the
processor, and acts on a placement only while that placement is current. A replica acknowledgement
counts only toward the placement it names. See [Checkpoint
Identity](./shutdown.md#checkpoint-identity) and [Membership, Consensus, And Bulk
Transfer](./interconnect.md#membership-consensus-and-bulk-transfer).

### Generations

A generation is one lifetime of a branch's guest state. The processor's schedule entry holds a
default generation for every branch and a generation of its own for each concrete branch reset
separately since the default last advanced. A new generation number is always the successor of the
highest one the processor has handed out, so a branch's generation only moves forward.

| Event | Generation effect |
| --- | --- |
| The processor is created | Every branch starts in generation 1 |
| A reset of one concrete branch | That branch moves to the next generation; every other branch keeps its own |
| A reset of every branch, or of the unbranched instance | The default moves to the next generation and every per-branch generation is cleared, which also covers branches that exist only as stored checkpoints |
| A forced recovery after owner loss | Every branch moves to the next generation, in the publication that names the new owner |
| A binding change: another resource, version, or module file | Every branch moves to the next generation, in the publication that commits the change |
| A change of the schemas the processor's records are laid out by | No change; the state moves to a placement under the new fingerprint, which starts without a checkpoint |
| A planned ownership handoff, a limits or policy change, a no-op rebinding | No change |

Only a committed schedule publishes a generation, so every transition is serialized with every other
mutation of the domain through the same alteration lease or automatic-decision fence as the schedule
itself. Reapplying a committed schedule, after a restart or a domain rebuild, publishes nothing new.

### Revisions

The owner stamps each capture with the next revision of its placement. A revision is never reused,
including the revision of a checkpoint that failed, because this node's storage or a replica may
still hold it. An owner that restores a placement continues from the revision it restored.

Each branch keeps two checkpoints on its owner. The committed checkpoint is the last one that
reached its boundary; a live instance that the owner recreates restores it. The published checkpoint
is the newest one on the owner's stable storage; replicas fetch it, and a restarted owner reopens
it. The two differ only after a checkpoint that reached the owner's storage failed at its replicas.

## The Checkpoint

A branch runs one callback at a time, and every callback that leaves an instance behind ends with a
checkpoint of that instance:

- an input batch, whether the callback succeeded or failed;
- each due timeout callback, one checkpoint per timeout;
- a quiesce flush that emitted output;
- the checkpoint an ownership handoff takes of an idle instance.

No checkpoint follows a flush that emitted nothing, because it decided no input. No checkpoint
follows a callback whose instance exhausted a limit: the instance is discarded, the committed
checkpoint already holds the state the next instance restores, and the callback's decisions are
released at once. No checkpoint follows a callback that asked for a new lifetime, because the state
it would save is the state the reset discards.

### Stages

1. **Boundary.** Before the guest saves, the owner confirms that the placement is current and that
   the committed schedule still has this node execute the processor. The replicas the schedule
   assigns at this moment are the checkpoint's boundary; with none, which is always the case with a
   replica count of `0`, the boundary is the owner's storage alone. A branch whose generation or
   ownership has moved on is refused here, before anything is saved, persisted, or replicated.
2. **Captured.** The guest saves with `nervix_dump_state`, and the bytes are stamped with the next
   revision. The owner shares the buffer with persistence and replication rather than copying it.
3. **Locally durable.** The state store writes the checkpoint on its storage workers, never on the
   worker running the guest, and returns once a synchronization of the node's storage covers the
   write. The owner then announces the revision to its replicas.
4. **Replica confirmed.** Every replica in the boundary has the revision on its own stable storage.
5. **Committed.** The checkpoint becomes the committed checkpoint, and the acknowledgements the
   callback held back are released. Only then does the branch run its next callback.

The whole checkpoint, from the guest's save to the last replica's confirmation, has ten seconds.

### Local Durability

The state store's durability barrier issues a ticket to every writer after its write is applied. At
most one writer runs a synchronization at a time; it covers every ticket issued before it started,
and the writers still waiting elect the next runner when it did not cover them. Branches that
checkpoint at the same time therefore share one synchronization instead of queuing one each. No lock
is held across the wait; see [Bounded Synchronization Outside The Open
Path](./data-plane-concurrency.md#bounded-synchronization-outside-the-open-path).

A failed synchronization is not retried. The operating system may have dropped the writes it failed
to flush, and the database refuses every later synchronization, so from then on no write on that
node is reported durable and every later checkpoint there fails until the node restarts.

### Replica Confirmation

A replica acts on an announcement only while the placement is current on it and the announcing node
is the owner its schedule names. It installs a revision newer than the one it holds, synchronizes
its storage, and only then acknowledges; a replica that already holds the announced revision or a
newer one synchronizes and acknowledges what it holds, so a lost acknowledgement is replaced by the
next announcement. A checkpoint of a branch the replica's branch lifecycle does not name yet makes
the replica fetch the owner's branch lifecycle first, and it refuses the checkpoint only when that
lifecycle does not name the branch either, as for an evicted branch. A node without stable storage
acknowledges nothing.

While the checkpoint waits, it wakes on each replica report and rereads the schedule at least every
100 milliseconds. A replica the schedule replaces is replaced in the wait. The wait fails when the
schedule assigns fewer replicas than the checkpoint was captured for, when this node no longer
executes the processor, and when the deadline passes with a replica still missing.

### Acknowledgement Holds

A callback's output is validated as a whole and dispatched before its checkpoint; only the
acknowledgements wait. For every input the callback decided successfully — carried into an output
row, listed as `acked`, routed through `ON MESSAGE ERROR`, or accepted by `ON GLOBAL ERROR IGNORE`
after a failed callback — the branch task attaches one more share to the input's acknowledgement and
owns it for the length of the checkpoint. The input succeeds once every delivery the callback made
for it succeeded and that share is released. `nacked` inputs are negatively acknowledged at once.
See [Checkpoints And Acknowledgements](./wasm-processor-guests.md#checkpoints-and-acknowledgements).

The holds gate the source only on an `ATTACHED` processor, which is the default. A `DETACHED`
processor receives its input after relay fan-out has already acknowledged it upstream, so no source
acknowledgement waits for its checkpoints, although its guest state is checkpointed the same way.

### When A Checkpoint Fails

| Stage | What failed |
| --- | --- |
| `state authority check` | The placement is no longer current on this node, or the schedule no longer has this node execute the processor. |
| `state snapshot` | `nervix_dump_state` returned a negative code, trapped, exhausted a limit, or exceeded the host's 64 MiB guest buffer. |
| `local state persistence` | The write or its synchronization failed, or did not finish before the deadline. |
| `state replication` | A replica did not confirm before the deadline, or the schedule assigns fewer replicas than the checkpoint was captured for. |

Whatever the processor's `ON GLOBAL ERROR` policy, the failure is reported as a runtime error at its
stage. Every input the callback decided, and every input the guest still buffers, is negatively
acknowledged. The committed checkpoint stays where it was. The instance is discarded with its
buffered input, pending output, and timeouts, and the next input for the branch recreates the guest
from the committed checkpoint.

## Failure At Each Boundary

A checkpoint's stages are durability boundaries, and schedule publications are authority boundaries.
Where an owner stops relative to them decides which state survives.

| The owner stopped | Its storage holds | A replica holds | A restart of that node continues from | After owner loss, the branch continues from |
| --- | --- | --- | --- | --- |
| After dispatching output, before the save | The previous checkpoint | The previous checkpoint | The previous checkpoint | The previous checkpoint |
| After capture, before local durability | The previous checkpoint | The previous checkpoint | The previous checkpoint | The previous checkpoint |
| After local durability, before every replica confirmed | The new checkpoint | The previous or the new one | The new checkpoint | Whichever the promoted node selects as newest |
| After every replica confirmed, before release | The new checkpoint | The new checkpoint | The new checkpoint | The new checkpoint |
| After the acknowledgements were released | The new checkpoint | The new checkpoint | The new checkpoint | The new checkpoint |

In every row but the last, the callback's output may already have left the node and its inputs were
not acknowledged, so a source with acknowledgements redelivers them. Without replicas, only the
owner's storage holds a branch's checkpoints, so recovering without that node resets the branch.
[Replay, Duplicates, And Exactly-Once](#replay-duplicates-and-exactly-once) describes what a
redelivered input does.

| Authority boundary | Failure before it | Failure after it |
| --- | --- | --- |
| A reset's `Publishing` schedule is applied | The previous lifetime stays authoritative; the owner restores the branch tasks it stopped | The new generation is authoritative and cannot be rolled back; the scope stays fenced until the same request reaches `Ready` |
| A reset's `Ready` schedule is applied | The reset is committed but not usable, and input of the scope is refused | The new lifetime is usable and the scope reopens |
| A recovery attempt is recorded | Nothing is spent; the next refusal raises the lifetime again | The attempt is spent whatever the reset achieves |
| A forced recovery schedule is published | Every checkpoint stays where it was, and the next recovery prepares again | The new owner continues in the new generation from the checkpoints it staged, or without state when its preparation failed |
| A binding change is published | The previous binding and its checkpoints stay current | Every branch starts the new binding without guest state |

## Ownership Fencing

### Recovered And Moved Owners

A restarted node executes nothing until a linearizable read proves it has applied the committed
schedule, so a former owner cannot resume a branch the cluster moved while it was away. See
[Recovered Ownership Is Fenced](./shutdown.md#recovered-ownership-is-fenced). During execution, the
boundary check at the start of every checkpoint, and the schedule rereads while it waits for
replicas, refuse a checkpoint whose placement or ownership moved on while its callback ran.

A planned ownership handoff keeps the generation. The source flushes each branch, checkpoints it,
and transfers the checkpoints; the destination restores every one of them into a guest before the
handoff activates, and a restore that fails fails the handoff before the schedule changes. The
replacement instances then restore the transferred checkpoints. See [Planned Ownership Handoffs And
Failover](./control-plane.md#planned-ownership-handoffs-and-failover).

### Forced Recovery

When an owner is lost, the leader publishes the new owner together with a new generation for every
branch. The destination prepares the recovery within five seconds: for each branch it collects the
checkpoints of the generation being replaced from its own storage and from every other node the new
schedule assigns the processor, and stages the newest valid one. Two different checkpoints at the
same highest revision are a conflict, and the branch is reset. A branch with no surviving checkpoint
is reset. The destination restores every staged checkpoint into a guest before the schedule is
published, a promoted replica from the module it compiled while it was a replica, and activation
publishes the staged checkpoints in the new generation. `SHOW CLUSTER STATUS` reports the transition
with `state_recovery=reset` and `resets=wasm_processor:<cause>` when it reset branches.

A preparation that fails for any reason — a guest that refuses or fails to restore a staged
checkpoint, a module the destination cannot compile, or the five seconds running out — makes the
leader publish the recovery with recreated state: every state component of the processor is reset
with the cause `missing_checkpoint`. `ON REJECTED STATE` does not govern this path. When the
processor has replicas, a replica holds every checkpoint whose acknowledgements the lost owner
released, so a recovery that prepares successfully continues each branch from at least the state its
acknowledged inputs produced. See [When The Former Owner Is
Gone](./shutdown.md#when-the-former-owner-is-gone).

A leader that has just started reconciling, as the first leader after a whole-cluster restart is,
makes no automatic scheduling decision for ten seconds while any voter is neither reported live nor
declared dead, so an owner that returns within that grace keeps its work and restores its own
checkpoints instead of being failed over. See [Whole-Cluster Restart Keeps
Ownership](./shutdown.md#whole-cluster-restart-keeps-ownership).

### Stale-Node Catch-Up

A node that was away installs the committed schedule before it accepts any runtime state, and then
synchronizes only the placements that schedule names. Applying the committed schedule of a running
domain removes every stored checkpoint whose placement is no longer current: state of a replaced
generation, state laid out by a replaced schema fingerprint, and state of a node the schedule
dropped. A reset or a forced recovery therefore never sweeps the cluster to delete old bytes:
addressing by generation makes them unreachable at once, and each node removes them locally when it
applies the schedule.

### Why State Does Not Resurrect

A lifetime that a reset, a forced recovery, or a binding change replaced cannot become current
again:

- a generation only moves forward, and only a committed schedule publishes one;
- every checkpoint is written, served, installed, transferred, staged, and restored under the
  generation the committed schedule names for its branch, so a snapshot of an earlier generation is
  never current, whatever revision it carries;
- a replica acknowledgement counts only toward the placement it names, so an acknowledgement for a
  replaced generation never satisfies the current one;
- a handoff or forced-recovery preparation is bound to the fingerprint of its complete target
  schedule, which covers every generation, so a preparation staged before a transition cannot
  activate after it;
- a restarted former owner is inert until it has applied the committed schedule, and a checkpoint of
  a branch whose generation moved on is refused before it is saved.

These fences compare generations and schema fingerprints, and they order checkpoints within one
placement by revision. A schema change keeps the generation and moves the state to a placement with
the new fingerprint, and each node removes the replaced placement's checkpoints when it applies the
committed schedule of the running domain. A node that did not apply that schedule while the domain
ran, because it was down or the domain was stopped, keeps them. When the processor's schemas later
return to the earlier fingerprint, that placement is current again, and so is every checkpoint kept
under it: an owner that kept one restores it, a replica that kept one with a higher revision than
the owner's acknowledges each new checkpoint with the revision it already holds, and a forced
recovery selects the kept checkpoint as the newest.

## Quiescence

A WASM processor participates in quiescing like every other stateful node. An `ENTITY_PAUSE` model
change, `RELOCATE`, a drain, and a planned handoff gate the processor's input relays, force-flush
its branches, which calls `nervix_flush`, checkpoint them, and wait for the acknowledgements those
checkpoints hold. A guest that keeps input past `nervix_flush` leaves it unacknowledged until the
branch resumes. See [ALTER Lock And Quiesce
Classification](./control-plane.md#alter-lock-and-quiesce-classification) and [Quiesce
Flush](./wasm-processor-guests.md#quiesce-flush).

A reset gates more narrowly: its branch-selective relay gate holds back only dispatches to the
selected scope, and sibling branches keep running. See [Dispatch-gate
engagement](./data-plane-concurrency.md#dispatch-gate-engagement).

## Coordinated Reset

A coordinated reset replaces the lifetime of one scope of one WASM processor: the explicit
unbranched instance, one concrete branch, or every concrete branch. One operation on the leader
serves every trigger.

| Trigger | Reason reported | Request reference | Entry |
| --- | --- | --- | --- |
| `RESET WASM PROCESSOR ... STATE` | `TRANSACTION` | The statement's command execution reference | An ordered transaction step |
| A guest request | `GUEST` | `wasm-guest-reset.` followed by a digest of the domain, processor, branch, and generation | The owner hands the request to the leader |
| Rejected-state recovery | `REJECTED_SNAPSHOT` | `wasm-recovery.<generation>.` followed by the branch fingerprint or `unbranched` | The owner raises the refusal to the leader |

`OPERATOR` names a reset requested of the coordinator directly rather than through a transaction; no
public interface issues one.

### Admission

The leader admits a reset only when the domain exists and is running, the processor is scheduled
with an owner, and the scope matches the processor's declaration: `UNBRANCHED` only for an
unbranched processor, and a concrete branch or every branch only for a branched one. A concrete
branch is named by exactly the declared branch-key fields with exact typed values, and it must be a
branch the owner is running. The reset takes the domain's exclusive alteration lock, so a concurrent
reset, lifecycle, model, placement, handoff, or rebinding operation is rejected rather than queued.
While a reset of the processor is `Publishing`, only the same request can proceed, and reusing a
request reference with a different scope or reason is rejected.

### Phases

1. **Gate.** The leader engages the reset's branch-selective gate on every live node for each input
   relay of the processor, bound to its coordination identity and scope, and waits for dispatches
   already admitted to that scope.
2. **Prepare.** The owner stops each selected branch at its callback boundary through the
   processor's command lane. Accepted input drains first, and completed callbacks with their
   buffered output are finalized once through the ordinary output, checkpoint, and acknowledgement
   paths. Work suspended on a materialized dependency is discarded and negatively acknowledged, and
   the old timeout handles are cancelled. The owner then creates a fresh instance for every selected
   branch, initializes it without `nervix_load_state`, and keeps its first save in memory. A failure
   here restores the stopped branch tasks and releases the gate; nothing durable has changed.
3. **Publishing.** The leader commits the schedule that advances the selected generation and records
   the reset with its request, scope, reason, and the `Publishing` phase. Once this schedule is
   applied the old checkpoints are unreachable and the reset cannot roll back. Live replicas install
   the new placement before the owner, in an activation ordered by the coordinator rather than the
   ordinary cluster runtime-revision barrier, because an owner whose initial checkpoint fails is the
   node that would keep that barrier incomplete.
4. **Initial checkpoint.** The owner installs the fresh branch tasks, republishes the branch
   lifecycle that names them and waits for its replicas to hold it, writes each fresh instance's
   first save as the initial checkpoint of the new generation, and waits for every assigned replica.
5. **Ready.** The leader commits the same reset as `Ready`, every live node applies it while the
   scope is still gated, and the gate is released.

From `Publishing` until `Ready`, the owner negatively acknowledges every input of the selected
scope, so a source with acknowledgements redelivers it into the new lifetime; sibling branches run.
A restart, a leader change, or an owner recovery reads the published phase and keeps the scope
fenced. A reset does not touch the domain clock: it changes no lifecycle generation, mapping,
authority, or frontier, and no deadline armed by the replaced instance fires in the new lifetime.
See [Execution-Time Snapshots](./domain-clock.md#execution-time-snapshots).

### Failure And Retry

A failure before `Publishing` reports an ordinary reset failure, and the previous lifetime stays
usable. A failure after it reports that the reset was committed but its new lifetime is not usable.
Retrying the same request reference resumes the missing initial checkpoints and `Ready` publication
without advancing the generation again. An NSPL reset completes only at `Ready`; its transaction
step is recovered with its original execution reference after a leader change, a lost reply returns
the retained outcome, and an expired reference cannot start another reset. See [Coordinated WASM
Reset Publications](./consensus-storage-and-replication.md#coordinated-wasm-reset-publications),
[Command Completion](./command-completion.md#lifecycle-and-ownership), and [Coordinated WASM
Guest-State Reset](./control-plane.md#coordinated-wasm-guest-state-reset).

## Guest-Requested Reset

The host answers `nervix_request_state_reset` with `0` while `nervix_process_batch`,
`nervix_on_timeout`, or `nervix_flush` runs, and with `-9` during every other operation, which
schedules nothing. An accepted request is terminal for the callback that made it, whether the
callback then succeeds or fails:

- output the callback emitted is discarded;
- every input the branch holds, including input the guest still buffers, is negatively acknowledged;
- no checkpoint is taken;
- the instance is dropped with its pending timeouts;
- effects earlier callbacks published stand, because their checkpoints completed.

The branch then fences itself: it refuses, and negatively acknowledges, every input until the reset
stops its task, so it can never instantiate the guest again from the lifetime being replaced. The
owner holds at most one request per branch in memory and hands it to the leader, and drops it
instead when the committed schedule shows the branch has already left the generation it was made
from. The request reference is derived from the domain, processor, branch, and generation, so every
request the guests of one lifetime make is the same reset, and the first request from the next
lifetime is a new one.

A reset that fails is reported as a runtime error naming the processor and domain. If it failed
before the owner stopped the branch, the branch stays fenced, and each input it refuses states the
request again. If the owner had stopped the branch, it restored it, and the guest continues in the
lifetime that could not be replaced and asks again from its next callback. A node that stops before
a request is coordinated loses it; the instance recreated from the same committed checkpoint asks
again. Acceptance is never proof of a new lifetime: a guest learns that only by being initialized
without a restore. See [Guest-requested reset](./wasm-processor-guests.md#guest-requested-reset).

## Rejected-State Recovery

A guest's verdict on the snapshot it is handed is `-7`, when it cannot decode the snapshot envelope,
or `-8`, when it refuses the application state inside it. `ON REJECTED STATE PRESERVE`, the default,
keeps the refused snapshot: every later instance of the branch is handed the same revision and
reports the same failure until a reset or a binding change replaces the lifetime. `ON REJECTED STATE
RESET` replaces the refused lifetime once. No other failure reaches the policy, so compilation,
initialization, fuel, memory, trap, storage, replication, and authority failures never discard state
through it.

The owner raises a refused lifetime once per placement and holds the raise until the leader answers;
at most 32 distinct refused lifetimes wait on one node, and a refusal beyond that is raised when the
branch is refused again. A node that is not the leader forwards the raise. The leader then:

1. refuses the recovery when the processor's policy is not `RESET` or the domain is not running, and
   does nothing when the branch has already left the refused generation;
2. records in the committed schedule that this refused lifetime is spending its one attempt, before
   anything is reset, or finds the attempt already recorded;
3. runs the coordinated reset under the reference derived from the scope and the refused generation;
4. records what the attempt achieved.

A refusal or failure is reported as a runtime error naming the processor and domain.

| Recorded outcome | Meaning |
| --- | --- |
| `attempted` | The attempt is admitted and its reset has not reported yet. A resumed attempt drives the same reset. |
| `recovered` | The reset published a fresh lifetime and the branch resumed on it. A later refusal of that lifetime is a new failure with an attempt of its own. |
| `failed` | The attempt ended without a usable lifetime. The budget is spent; the branch keeps reporting its refusal instead of resetting again on every record. |

A reset that fails after `Publishing` leaves the scope fenced exactly like any other committed but
unusable reset. The attempts are control-plane state, so a restart, a leader change, and an owner
change read the same budget, and a binding change clears them because it starts every lifetime anew.
Only an instance the owner creates for live work raises a refusal: a planned handoff whose
destination is refused fails, and a refused forced-recovery preparation resets the processor as
described in [Forced Recovery](#forced-recovery). See [Recovering A Rejected
Snapshot](./wasm-processor-guests.md#recovering-a-rejected-snapshot).

## Rebinding And Rollback

A generation belongs to the module binding it was published for: the resource, its pinned version,
and the module file. A model change that binds another one starts a new generation for every branch
in the publication that commits it, clears the processor's latest reset and its recovery attempts,
and the replacement instances initialize from the new module without guest state. No state is
migrated between module versions. A change of the execution limits, the global error policy, or the
rejected-state policy, and a rebinding that leaves the version unchanged, keep every generation.

The leader compiles the candidate module before the change commits, and activation installs that
same compiled module. A module that does not compile rejects the whole statement before any effect,
while the previous binding and its checkpoints are still current. A rebinding is `ENTITY_PAUSE` for
a running domain: the processor and its downstream pause and each branch is flushed. A rebinding in
a stopped domain publishes the new generations as well, so the processor starts without guest state.
A reset after a rebinding starts another lifetime on the rebound module, and `DESCRIBE WASM
PROCESSOR` never presents a reset of the previous binding as a reset of the new one. See
[Classification And State Effects](./resource-versions.md#classification-and-state-effects).

## Shutdown And Restart

A shutdown drain waits for the acknowledgements a WASM branch holds, as it waits for any outstanding
acknowledgement, and the checkpoint deadline bounds that wait. Runtime teardown gives each processor
task two seconds to stop, and the processor task gives each of its branch tasks two seconds inside
that. A branch task never outlives its processor task: one still in its checkpoint is ended with it,
its unreleased acknowledgements are negatively acknowledged, and a restart finds whatever that
checkpoint had already written. See [Draining Admitted Work](./shutdown.md#draining-admitted-work).

A restarted node reopens each branch at the newest checkpoint on its own storage under the identity
the committed schedule publishes, which covers every input its acknowledgements released. A reset
interrupted before `Publishing` leaves the previous generation authoritative; one interrupted after
it resumes the new generation on restart. Guest reset requests and raised recoveries do not survive
a restart; the next callback or refusal raises them again, and a spent recovery attempt stays spent.
See [What Survives](./shutdown.md#what-survives) for the kinds of ending and the state each keeps.

## Replay, Duplicates, And Exactly-Once

Snapshot recovery guarantees one thing: once a source has received a successful acknowledgement for
an input, the guest state that reflects the input is on the stable storage of the branch's owner and
of every replica the checkpoint was confirmed by. It is not an exactly-once guarantee for the guest
or for external delivery:

- output is dispatched before its checkpoint, so a sink can publish output whose input is later
  negatively acknowledged and redelivered, and the redelivered input produces it again;
- the state a branch continues from can already reflect a redelivered input: a guest counts buffered
  input as it accepts it, a checkpoint that failed at its replicas stays on the owner's storage, and
  a replica can hold a checkpoint whose acknowledgement arrived too late;
- a source that keeps redelivering while no owner accepts the input can deliver it to the recovered
  branch more than once;
- a source without acknowledgements redelivers nothing, and loses the input in every window before
  local durability.

A guest therefore applies a redelivered input again unless it recognizes the input itself, and
exactly-once external effects need a sink whose writes are idempotent. See [Recovery, Replay And
Duplicates](./wasm-processor-guests.md#recovery-replay-and-duplicates) and [ACK Semantics And
Effective Delivery](./emitters.md#ack-semantics-and-effective-delivery).

## Observability

| Signal | What it shows |
| --- | --- |
| `DESCRIBE WASM PROCESSOR <name>` | The pinned `resource`, `resource version`, and `file`, the `rejected state policy`, `state default generation`, the latest reset as `state reset: <PUBLISHING or READY>, <scope>, generation <n>` with its reason and readiness, one `rejected state recovery` line per recorded attempt, the checkpoint counts, and one `checkpoint` line per branch with its fingerprint, generation, committed and latest revision, stage, and required and confirmed replica counts |
| `DESCRIBE WASM PROCESSOR <name> FORMAT JSON` | The same typed state inspection, including each reset's request reference; `null` for a processor that is not scheduled |
| Client outcome | `CommandOutcome::wasm_state` carries the typed inspection beside either text rendering |
| `DESCRIBE TRANSACTION` | The planned and actual effect of a transaction's `RESET WASM PROCESSOR` step |
| `SHOW CLUSTER STATUS` | A forced recovery's `state_recovery` outcome and the `wasm_processor` component it reset with the cause |
| Runtime errors | Every failure in the [diagnostic shape](./wasm-processor-guests.md#failure-diagnostics), a reset that could not replace a guest-requested lifetime, and a rejected-state recovery that failed or was refused; delivered to the sessions attached to any node |
| Server log | At `info`: `WASM guest-state reset generation published` and `WASM guest-state reset became usable` on the leader, `wasm guest requested a new branch state lifetime` and `raised a refused WASM guest-state lifetime for recovery` on the owner, and `WASM rejected-state recovery admitted` and `WASM rejected-state recovery settled` on the leader |

The inspection is read-only. It samples the owner's published checkpoint observations without taking
a branch's execution lane, requesting synchronization, or advancing a checkpoint, and it counts only
checkpoints of the generation the committed schedule names. Checkpoint counts cover every branch the
owner holds state for; branch and recovery details are limited to 128 entries each, with the rest
reported as omitted. A stage is `EMPTY` while the branch has no checkpoint in its lifetime,
`CAPTURED` before local storage, `LOCALLY_DURABLE` before its boundary is complete,
`REPLICA_CONFIRMED` once it is, which without replicas reads `0` of `0`, and `FAILED` when the
latest checkpoint failed, with the previous committed revision still current. A checkpoint the owner
restored rather than took reports `LOCALLY_DURABLE` with unknown replica counts, because its earlier
boundary cannot be reconstructed. Reset readiness is `RESETTING` while the new lifetime's checkpoint
is unfinished, `AWAITING_USABLE_EXECUTION` when a selected single-scope checkpoint is confirmed but
the reset is still `Publishing`, and `READY` once `Ready` is published; an all-branches reset stays
`RESETTING` until then, because a sample of the current branches cannot prove every selected branch
completed. Nervix exports no WASM-specific metric.

## Guarantees And Limits

Nervix guarantees:

1. **No acknowledgement before durability.** An `ATTACHED` WASM processor releases no successful
   acknowledgement until the checkpoint covering it is synchronized on the owner and on every
   replica its boundary requires.
2. **Failed checkpoints change nothing committed.** A checkpoint that fails leaves the committed
   checkpoint in place, negatively acknowledges what it covers, and discards the instance.
3. **Branch isolation.** Every branch has its own instance, checkpoints, holds, and lifetime; a
   reset of one branch leaves every sibling's generation and task intact.
4. **One lifetime per request.** Retrying, forwarding, or resuming a reset under its request
   reference never advances the generation twice, and a spent recovery attempt is never spent again.
5. **No resurrection by transition.** A lifetime replaced by a reset, a forced recovery, or a
   binding change never becomes current again.
6. **Strict restoration.** Only a guest's own verdict can make `ON REJECTED STATE RESET` discard a
   lifetime.

Nervix does not provide, and these limits apply:

- **Exactly-once processing or delivery.** See [Replay, Duplicates, And
  Exactly-Once](#replay-duplicates-and-exactly-once).
- **Guest state across module versions.** A binding change always starts a new lifetime.
- **A checkpoint deadline of ten seconds**, from the save to the last replica, and one callback per
  branch at a time, whose acknowledgements are held for at most that long.
- **A save of at most 64 MiB**, the host's guest buffer limit.
- **Loss of state without replicas.** Recovering a processor without its owner resets every branch
  whose only checkpoints were on that owner.
- **Forced recovery can reset state without a verdict.** A recovery preparation that fails or runs
  past five seconds recreates the processor's state.
- **A failed storage synchronization is permanent** until the node restarts.
- **A whole-cluster restart keeps ownership** only for owners that rejoin within the ten-second
  voter observation grace.
- **Evicted branches keep their state.** A branch key that returns after eviction restores its last
  committed checkpoint.
- **A schema that returns to an earlier fingerprint** can make a checkpoint kept by a node that
  missed the intermediate schedule current again; see [Why State Does Not
  Resurrect](#why-state-does-not-resurrect).
- **A `DETACHED` processor gates no source acknowledgement** on its checkpoints.
- **Guest requests and recovery raises are volatile**, and are raised again by the next callback or
  refusal.

## Qualification Evidence

The WASM state qualification pins each checkpoint window with a harness-only pause and ends the
owner there, on one node and on three nodes with one replica, with interleaved records of two
branches.

| Guarantee | Evidence |
| --- | --- |
| Crash windows and replay | `wasm_state_qualification.feature`: the four checkpoint windows on a single-node restart, owner loss at the windows it can tell apart, a cluster stopped while its owner holds a checkpoint, and a reset committed but not usable across a cluster restart |
| No acknowledgement before durability | `wasm_checkpoint_durability.feature`, and the Shuttle checks of `ReplicatedWasmProcessorState`, `WasmCheckpointHolds`, and `DurabilityBarrier` under random, PCT, and DFS schedules |
| No resurrection, no double reset | `wasm_state_reset.feature`: failover, retry, a lost reply across a leader change, an expired identity, and rebinding |
| Recovery budget | `wasm_state_recovery.feature`: preserve, recover once, and a failed fresh initialization that spends the attempt across a restart |
| Restart | `wasm_processor.feature`: guest state restored after a cluster restart and not resurrected by a returning former owner |

The [WASM state qualification
ledger](https://github.com/nervix-io/nervix/blob/main/tests/wasm-state-qualification-ledger.md)
records the commands, the defects the qualification found and fixed, the validation record, and the
measured cost of a durable checkpoint: 3 to 18 milliseconds per round for one branch, and 6 to 37
milliseconds for sixteen concurrent branches that share a synchronization.
