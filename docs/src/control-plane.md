# Control Plane

The control plane is where Nervix applies strong consistency.

It is responsible for:

- storing NSPL models
- validating references, compatibility, and placement claims
- computing domain schedules
- tracking domain lifecycle
- handling cluster coordination
- exposing control operations like `SHOW CREATE`, `DESCRIBE INGESTOR`, and `SHOW CLUSTER STATUS`

The most important property is that control-plane state is authoritative. A runtime node only exists because the control plane says it exists.

Execution graph configuration is part of this control-plane state. NSPL models, domain schedules, and lifecycle transitions are persisted with strong consistency guarantees before runtime nodes execute them.

In practice, the control plane covers:

- domain creation and selection
- model creation and deletion
- scheduling decisions, including domain defaults and named placement rules
- explicit node removal with `DROP NODE <node_id>`
- node cordon and uncordon with `CORDON NODE <node_id>` and `UNCORDON NODE <node_id>`
- node drain with `DRAIN NODE <node_id>`, which cordons the node and moves scheduled graph nodes away one at a time
- explicit relocation with `RELOCATE <selection> ONTO NODE <node_id>`, which moves a selected subgraph onto a named cluster node, and `DESCRIBE RELOCATION`, which shows the plan without executing it
- primary and replica assignment
- Kafka `OFFSET BY DOMAIN` partition-to-instance assignment and rebalance
- domain `START` and `STOP`

This is the part of Nervix where Raft-backed consistency matters. It keeps cluster-wide definitions coherent.

## Durability And Recovery

A control-plane mutation returns its semantic result only after Raft has committed it and the
leader has durably applied it. The applied write stores the changed semantic records together with
its final log position, membership, transaction progress, and revision. Recovery therefore finds a
complete applied range or replays its committed entries; it never reconstructs an acknowledged
command from a partial state-machine update.

Consensus uses a dedicated database and journal under `<db-path>/consensus`. Registry and runtime
state remain in the node database at `<db-path>`, so consensus synchronization does not flush or
wait behind data-plane journal writes. The full durability boundary, append-stream pacing, log
reader bounds, retention policy, snapshot lifecycle, storage layout, and operator settings are
defined in [Consensus Storage And Replication](./consensus-storage-and-replication.md).

Observers see a coherent state revision only after durable success. Change notifications identify
committed revisions and may coalesce intermediate revisions; readers retrieve a coherent current
view. A storage or transport error can arrive after the durable write completed, so it does not
prove that the command was uncommitted. Persistent administrative requests keep the same execution
reference across an uncertain result and join the admitted execution or retrieve its retained
terminal result.

Each node applies a published cluster revision once. A revision at or below the one it has already
applied carries nothing newer and is ignored, and the first revision a node sees always applies. No
revision value is reserved to mean that a node has applied nothing yet, so every revision a cluster
can publish, including the largest one, still suppresses the stale revisions that follow it. A
revision whose application fails is not recorded as applied, so the node applies that revision again
instead of treating the failed attempt as its current state.

## Replicated NSPL Transactions

NSPL command grouping is explicit. `BEGIN` creates a Raft-replicated control-plane transaction and
returns its id. Following eligible statements are preflighted and then appended to that transaction
in written order; `COMMIT` applies them and `REVERT` discards them. `BEGIN` inside an active
transaction is rejected, as are `COMMIT` and `REVERT` without one. A request containing multiple
statements outside an explicit transaction is rejected instead of becoming an implicit batch.

A transaction belongs to exactly one domain. `BEGIN` binds the transaction to the session's
selected domain, which must already exist; without a selected domain, or with one that does not
exist, `BEGIN` fails and no transaction is opened. Every statement queued afterwards must select
that same domain, and a statement submitted for another domain is rejected without changing the
pending count. A transaction therefore cannot create the domain it configures, and cannot span
domains.

The cluster owns the transaction, not the TCP or WebSocket connection. Its owner, timestamps,
state, structured semantic statements, and commit progress are replicated. The original statement
source is retained for display, but execution never reparses that text. `BEGIN`, queueing,
`COMMIT`, and `REVERT` are leader operations; clients transparently follow the normal leader
redirect, including for the initial `BEGIN`.

A transaction is `OPEN`, `COMMITTING`, or finished as `COMMITTED`, `FAILED`, `REVERTED`, or
`EXPIRED`. A client retains the transaction id and attaches it after reconnecting. Attach is
restricted to the authenticated owner. Attaching from a second live session takes over the
transaction, so the displaced session's next transaction operation reports that it was taken over.
The transaction reports the domain it is bound to, and an attaching or reconnecting session adopts
that domain as its selected domain. An unclean transport loss or leadership change leaves an open
transaction available for attach. Binding is leader-local soft state, so a leader that does not
hold it reports the session as detached; clients treat that as a routing condition, attach the
transaction again, and replay the command. A clean end of the session reverts a bound open
transaction.

Only the bound domain's replicated configuration effects may be queued:

- model `CREATE`, supported model `ALTER`, and model `DROP` statements;
- `REBIND RESOURCE`, as one atomic model-mutation batch;
- `ALTER DOMAIN`, `START`, and `STOP`;
- `CREATE RESOURCE`.

Completion on the bound session resolves identifiers against the configuration the queued
statements produce, applied in written order, so a client is offered the models and resources its
own transaction defines and is no longer offered a model whose `DROP` it has queued. Only the
create and drop sequence decides a name, so an intermediate configuration that does not yet resolve
still completes. Sessions that are not bound to the transaction, including other sessions of the
same user, are offered committed configuration alone.
For a `REBIND RESOURCE ... FOR` list, completion further limits each kind to queued-result models
that bind the named resource.

Read-only `SHOW`, `DESCRIBE`, and `LOOKUP` statements are rejected at queue time. `CREATE DOMAIN`
and `CREATE USER` are rejected too: neither belongs to a domain, so neither is transaction content.
Session subscriptions, `UPLOAD RESOURCE`, and node scheduling or membership operations (`CORDON`,
`UNCORDON`, `DRAIN`, `DROP NODE`, and `RELOCATE`) are also immediate, non-transaction content. Run those
statements outside `BEGIN`/`COMMIT`.

Queue admission is not a blind append. The leader replays the replicated transaction prefix into a
side-effect-free ordered plan, then checks the new statement against that plan. The planner uses one
captured set of domain, model, resource, schedule, placement, membership, and liveness inputs. It
simulates lifecycle and placement changes, resource declarations, and model candidates in written
order, with the same execution-step boundaries `COMMIT` uses. This catches such errors as duplicate
configuration, a missing `ALTER` target or field, invalid domain lifecycle, invalid external
bindings, and invalid UDF or schedule inputs before the statement is replicated. A successfully
queued model mutation reports the effective quiesce level of its complete consecutive model run at
that prefix. Extending the run can raise that level or make cancelling changes a no-op.
`REBIND RESOURCE` resolves its target and usages from that same prefix. `LATEST` and its impact are
provisional at admission and are planned again from the captured commit basis. All selected models
pass ordinary creation and external-resource validation before the one model step can commit.
A rejected statement does not change the pending count or the transaction's activity time, so the
client can correct it and continue the same transaction. The admitted result is stored with the
statement; an exact retry with the same request reference, source, semantic statement, and expected
position returns that result without rerunning preflight or extending activity. A reused reference
with different content is rejected. Limits are checked before preflight and the same ordered
planner runs from a refreshed snapshot during `COMMIT`, because other sessions may change relevant
control-plane state after admission.

The plan records affected topology on both sides of every execution step. The before side retains
nodes and edges that the step drops or rewires; the after side records the graph that activation
will install. Edges distinguish configuration dependencies, normal delivery, message-error and
correlation-timeout routes, and materialized-state reads. Downstream pause traversal follows only
relations that can carry records or state effects, while the reported topology also retains the
configuration dependencies needed to explain those nodes. Parallel relations between the same two
nodes remain separate, and every node and edge retains all operations that contributed to it.

An entity pause names its concrete branch coverage and the admission relays from the current
schedule. Shared relay gates and every member of an affected hard placement group are included in
that scope. Schedule entity swaps raise the effective scope to an entity pause, while a schedule
rebuild raises it to a domain pause before activation begins. A domain pause covers every execution
node in the before and after graphs. Changed, paused, force-flushed, ownership-moved, activated,
rebuilt, and state-reset nodes remain separate effects in the report; because engaging any entity
gate requests a domain-wide force flush, that flush effect covers the full current execution graph.
A VHOST TLS version change is reported as an HTTPS listener refresh activation of that VHOST, with
no paused subgraph and no rebuilt node.
Commit uses the gate plan and schedule delta captured for this report, so execution cannot silently
widen the planned scope with a second decision.

An accumulated model run that already forms a complete graph receives the full registry, binding,
UDF, and scheduling preflight. Cross-model completeness may remain provisional only for the
unfinished final model run. An intermediate schema/codec mismatch or temporarily referenced model
may be repaired by a later statement in that same run. Once a lifecycle, domain, or resource step
ends the run, a later model run cannot repair it. Statement-local mutations must still be valid
against the prefix, and `COMMIT` requires every run to pass every check. This keeps coordinated
multi-model migrations possible without allowing a later execution step to make an earlier step
valid retroactively.

Within a transaction, each consecutive run of model mutations can mix `CREATE`,
`ALTER SCHEMA`, `ALTER WIRE ... SCHEMA`, `ALTER RELAY`, `ALTER JUNCTION`, `ALTER DEDUPLICATOR`,
`ALTER REORDERER`, `ALTER EMITTER`, `ALTER INGESTOR`, `ALTER REINGESTOR`, `ALTER GENERATOR`,
`ALTER PLACEMENT`, and `DROP`. Nervix applies that run as one registry mutation: all operations are
evaluated in written order against one candidate model map, the complete domain graph is
revalidated, and one atomic storage batch persists the base-to-final result. Drop/recreate and
multi-ALTER sequences are therefore classified jointly; cancelling changes can produce a no-op.
A failure writes nothing and does
not swap the active registry state. This supports coordinated wire-schema, internal-schema, codec,
relay, processor, emitter, ingestor, generator, placement, and dependent-node migrations without
exposing an invalid intermediate graph.

Other eligible statements apply individually. `COMMIT` records authoritative effect progress and
completed application separately and stops at the first definitive failure. A transaction remains
`COMMITTING` after a step's authoritative state is durable while runtime activation, source
readiness, remote stopping, ownership handoff, or gate release is outstanding. It becomes
`COMMITTED` only after the final step is usable on the current live-node set and its outcome is
authoritatively visible. Its successful output is only the highest quiesce level actually executed
across the transaction; it does not repeat the individual command outputs.

A new leader automatically resumes every `COMMITTING` transaction from its recorded applying step.
Completed effects are not repeated, and a failed remaining step records its statement number and
error while preserving the applied prefix. A model step that did not pause and whose VHOSTs an HTTPS
listener could not install is the one step whose failure removes its own effect: the record of that
failure also restores the schedule the step replaced, so the applied prefix ends before it. Both
the first attempt and a resuming leader apply this rule. Repeating the outstanding `COMMIT` joins
this execution and waits for the retained terminal result. Atomicity still does not span the whole
transaction.

Finished transactions remain as small tombstones containing the outcome, step progress, errors,
and executed quiesce levels. During retention, attach reports the exact outcome and aggregate
commit output; after removal the id is unknown. `SHOW TRANSACTIONS;`
can be served by any node from locally applied replicated state and lists the id, owner, domain,
state, pending count, progress, age, and idle time for live transactions and retained tombstones.

An unbound `OPEN` transaction expires after its idle timeout; a bound transaction does not, and a
`COMMITTING` transaction never expires. Defaults and server settings are:

| Setting | Environment variable | Default |
| --- | --- | --- |
| `--transaction-idle-timeout` | `NERVIX_TRANSACTION_IDLE_TIMEOUT` | `15m` |
| `--transaction-tombstone-retention` | `NERVIX_TRANSACTION_TOMBSTONE_RETENTION` | `15m` |
| `--transaction-max-statements` | `NERVIX_TRANSACTION_MAX_STATEMENTS` | `256` |
| `--transaction-max-source-bytes` | `NERVIX_TRANSACTION_MAX_SOURCE_BYTES` | `1048576` |
| `--transaction-max-open` | `NERVIX_TRANSACTION_MAX_OPEN` | `1024` |

These limits are enforced by replicated state, so every leader observes the same admission result.
Transaction state changes do not force schedule publication or a runtime barrier.

Data-plane records remain outside this control-plane atomicity.

## Node Health, Membership, And Scheduling

Each node publishes application-health observations independently as its probes complete. The
scheduler reads the current observation snapshot and applies the configured node-unavailability
policy. A peer leaves the live scheduling set only after a current sequence of application probe
failures has lasted for that policy's interval. A stale or unscheduled observation is unknown, and
probe-capacity exhaustion remains distinct from a peer failure, so none of those conditions alone
makes a healthy node unavailable.

Automatic scheduling runs independently of the health-probe sweep, resource downloads, and Raft
learner catch-up. It does not wait for any of them to finish. Slow health responses, learner
admission, and bulk progress can therefore overlap scheduling and unrelated administrative work.
Scheduling uses only observations that are current when it computes and publishes a candidate; a
changed topology or effective-health revision causes the candidate to be recomputed. A newer
observation with the same effective health does not invalidate an otherwise current candidate.

The current leader owns membership changes and serializes them one at a time. Learner admission or
catch-up and voting-membership changes each have a ten-second wait deadline. When that deadline
expires, Nervix stops waiting and observes effective committed membership again before a later
retry. It does not infer that the change was rolled back: an admitted learner may continue catching
up after the timed wait ends.

An automatic schedule candidate carries the leader identity and term under which it was computed,
plus the full applied control-plane revision of its inputs. Publication succeeds only during the
same leader tenure and while that applied revision and the expected current domain schedule still
match. Any intervening membership, cordon, domain, configuration, or schedule command therefore
invalidates the candidate and makes the scheduler compute again from a coherent current view.

## Placement Activation

Placement coverage is derived from the complete candidate execution graph rather than stored as a
fixed runtime-node list. During every graph activation, the control plane recomputes path-gated
rule claims, applies rank resolution, rejects equal-rank policy conflicts, and forms the effective
`REQUIRE COLOCATION` groups before publishing the schedule. A rejected candidate writes nothing
and leaves the prior models and schedule active.

Schedule publication preserves the existing primary and replicas of every single-owner ingestor
while all cluster nodes in that assignment are live. Outbound WebSocket-client ingestors follow
this rule along with the other client sources, so unrelated graph changes, cluster-node joins or
uncordons, and soft placement changes do not restart their external sessions. Endpoint-source and
Syslog ingestors are the only ingestors whose assignments follow live membership, because their
listeners execute on every cluster node.

Every relay is also scheduled with one primary owner. Ordinary relays have no replicas.
Materialized relays use the same relay schedule entry: additional assigned nodes are replicas of
materialized state only, not relay buffers, branch presence, fan-out, or metrics.

Hard colocation groups constrain every scheduler. A newly effective require group is consolidated
through the normal runtime-node handoff path, and failover or drain moves the group as one unit.
Soft policies affect only future placement decisions and do not relocate existing assignments.
`ALTER DOMAIN SET PLACEMENT` changes the active domain's fallback through the same schedule
activation boundary. See [Placement Policies](placement.md) for corridor coverage, precedence,
carve-outs, lifecycle commands, and introspection.

## ALTER Lock And Quiesce Classification

Every model-mutation batch acquires one exclusive leader-local ALTER lock for its domain before
validation. The lock remains held through candidate planning, quiescing, persistence, schedule
publication, rollback when required, and resume. A concurrent mutation is rejected instead of
queued. Raft still serializes the durable domain lifecycle and schedule, while the registry's
base-model comparison remains a final consistency check.

Nervix classifies the validated base-to-candidate model diff, not the spelling of the statements
that produced it. The batch uses the highest level contributed by any changed entity:

Quiesce level and ingestor quiesce mode are separate contracts. `DYNAMIC`, `ENTITY_PAUSE`, and
`DOMAIN_PAUSE` determine which graph work must pause before a change commits. The required `ON
QUIESCE` clause on each ingestor determines what that ingestor's external source experiences while
an entity or domain pause is active. Memory-pressure shedding consults the same mode. There is no
operator `PAUSE` or `RESUME` statement.

- `DYNAMIC` changes do not pause ingestion. Relay capacity; processor filters, source predicates,
  collection, route construction, route flush, and same-target message-error policies;
  deduplicator/reorderer `MAX TIME`; emitter flush policy; placement definitions; and a VHOST
  moving to another version of the TLS resource it already binds are hot-applied while retaining
  buffered and branch-local state when ownership stays fixed. A placement definition is a dynamic
  model change, but its effective command level rises to `ENTITY_PAUSE` when the resulting schedule
  moves a running runtime node. A VHOST TLS version change is applied as an HTTPS listener refresh:
  every live node installs the new certificate from the committed revision before the command
  succeeds, established connections keep the session they negotiated, and new handshakes present
  the new bundle. No execution node pauses or restarts, and the refresh applies to a stopped domain
  too, because the listener serves its VHOSTs while it is stopped. If the listener of any node
  cannot install the change, the batch fails and restores the previous models, and every listener
  installs the restored certificates again.
- `ENTITY_PAUSE` changes gate only the affected relays on every live node, force-flush affected
  work, and wait for the owner buffers, fixed dispatch slots, and target-node work counters to
  drain before commit.
  Other domain traffic continues. A processor topology change then swaps only the affected node
  tasks and hands pending materialized-state work to their replacements. Deduplicator key changes
  also purge the old keyspace before the replacement starts; reorderer ordering changes flush the
  old ordering buffers before swapping. Relay materialized-state changes update membership in
  place. Emitter source-predicate, sink, publishing-mode (including any confirmation window,
  timeout, or retry-policy variable), client, codec, input-collection, and attachment changes
  drain and replace only the affected emitter task. Every ingestor alteration quiesces and drains
  only the affected ingestor instances under their declared source mode, then starts their desired
  source configuration from the published schedule. A `SET QUIESCE` operation still uses this
  level; the mode active when the hold began governs that hold. Reingestor alterations replace their relay consumers and
  branch-entrypoint wiring; generator alterations quiesce and replace their timed task after
  flushing pending route output.
  Correlator, window-processor, inferencer, and WASM-processor structural changes use this level
  as well. A WASM processor participates like every other stateful node: the host gates its input
  relays, asks the guest to release what it buffers, snapshots it, and restores that snapshot into
  the replacement instance.
- A schedule change that only adjusts replica roles is `DYNAMIC`. A planned primary-owner change
  uses `ENTITY_PAUSE`, even when no model changed. `RELOCATE` is classified this way: it reports
  `ENTITY_PAUSE` when it moves at least one runtime node in a running domain, and `DYNAMIC` when it
  moves nothing or the domain is stopped. Both reach every live node as narrow activations:
  a runtime node whose primary owner and replica set stay fixed does not stop, restart, restore a
  snapshot, or reopen an external session. Its in-flight batches, retained `REQUIRED WAIT` work,
  branch instances, branch-local state, and ingestor session continue. Producers and materialized
  state readers rebind to a moved node at the published revision. A cluster node that gains or
  loses only a replica role starts or stops replication without restarting the primary. A hard
  colocation group moves in one narrow activation, and a model batch applies its model and schedule
  changes together.
- `DOMAIN_PAUSE` changes stop ingestion and generators across the domain and fully drain attached
  work before commit. Relay schema or branching changes and schema or wire-schema definition
  changes use this level. Changing the membership of an emitter's `FROM` relay list also uses this
  level because it changes graph topology. Configuration entities use this level too: codec,
  client, endpoint, signaling-protocol, hash-map, and UDF definitions, vhost hostnames, adding or
  removing a vhost's TLS, binding a vhost to another TLS resource, and branch schema, TTL, and
  eviction settings. Their consumers read that configuration when they are built, so the domain
  rebuilds around the new models rather than reconfiguring in place.

An entity-paused model change also gates everything downstream of the affected model, so a
dependent node cannot observe a half-applied change through its input relay.

## Planned Ownership Handoffs And Failover

Node drain, the ownership move of a graceful-shutdown drain, placement consolidation, and `RELOCATE`
are planned ownership handoffs.
Nervix computes the complete target schedule before it engages a hold. It then fences dispatch at
the affected subgraph boundary on every live node, stops new intake for each moved ingestor, and
drains work already admitted to the moved unit. Ownership-handoff intake does not consult `ON
QUIESCE`: an already admitted payload continues through its routes, while polling and endpoint
admission stop. The drain includes relay rings, processor work, moved-ingestor ACK roots, emitter
buffers and active publishing, and an Iceberg emitter's staged commit. Internal relays whose every
producer moves with the same hard group remain open so admitted work can reach the group's output
boundary.

Nervix writes the new schedule only after that drain succeeds. A drain writes and activates one hard
group or independent node at a time. Placement consolidation gates all of its moved groups together
and publishes its model and assignments in one schedule update. Only moved runtime nodes restart;
unaffected nodes and branches keep their sessions, buffers, state, and in-flight work. The gate is
released after the destination owners activate the published runtime revision. A fence or drain
timeout releases the old graph without writing the candidate schedule. If activation fails after a
schedule commit, the gate stays closed until its lease deadline rather than opening before the
destination is ready.

Pending `REQUIRED WAIT` materialized records cannot finish a planned drain because the dependency is
absent. They do not block the handoff; destroying the old task negatively acknowledges their
attached work. Replicated state is available immediately when the destination was already a replica.
Otherwise the state kind starts from its normal empty or local recovery boundary. When the schedule
has a replica slot, the live former owner is the first replica candidate after the new primary.

`RELOCATE` differs from the others in one respect: it holds and commits its whole unit at once,
because the unit is the plan the operator inspected and approved. A hold that cannot complete
leaves the domain on its previous schedule and moves nothing. It holds the domain's exclusive
alteration lock from planning through release, so it and a concurrent model change, placement
change, or `DRAIN NODE` of the same domain are mutually exclusive. `RELOCATE` is immediate,
non-transaction content, like `CORDON`, `UNCORDON`, `DRAIN`, and `DROP NODE`, because its plan
depends on live cluster state that a queued transaction cannot pin. `DESCRIBE RELOCATION` is
read-only content served by any cluster node. See
[Placement Policies](placement.md#relocating-runtime-nodes) for the statements, the selection
forms, and the plan output.

`DRAIN NODE` cordons first, visits domains and schedule units in canonical order, and continues with
independent units after one times out. Its result lists every successful move and failure. Any failed
unit makes the command unsuccessful, while a later `DRAIN NODE` retries the units still owned by the
cordoned node. Endpoint and Syslog listeners bind on every live node and are not schedule units.

[Shutdown And Recovery](shutdown.md) is the complete account of stopping a node and recovering from
a forced ending. This section states how a graceful shutdown uses the planned handoff above.

A server process begins graceful shutdown when it receives its first `SIGINT` or `SIGTERM`. It
registers both signals before it starts any other work and supervises them until it exits, so a
signal received during startup takes effect once startup completes. Every later `SIGINT` or
`SIGTERM` abandons graceful shutdown: the process logs the phase it had reached and exits at once
with status 128 plus the number of the signal that forced it, without running its remaining
phases. The rest of the cluster observes that exit exactly as it observes a crash.

Graceful shutdown has one deadline, set by `--shutdown-timeout` (`NERVIX_SHUTDOWN_TIMEOUT`, default
`50s`) and measured on the process monotonic clock from the first stop request: the first `SIGINT`
or `SIGTERM`, or a public listener that fails. A later request never restarts or extends it, and
domain pacing never changes its physical length. Every shutdown step described below waits at most
until the deadline. When it passes, the process logs the phase it had reached, reports
`shutdown deadline expired; abandoning graceful shutdown`, and exits at once with status 1 without
running its remaining phases, so the rest of the cluster again observes a crash. Whichever comes
first, the deadline or a repeated signal, decides how the process exits.

When graceful shutdown begins, the process advertises that its current incarnation is terminating.
The incarnation remains live for Raft and for ownership handoffs already in progress, while placement
and explicit relocation exclude it as a new destination. The advertisement is transient state of
that process incarnation, so a restarted incarnation is eligible again unless the stable node name
is cordoned in Raft. The process then stops accepting on its public gRPC, connector, observability,
and console listeners and closes the client connections they had accepted. Closing a connection
cancels the requests it carries, such as a session stream or a resource upload that is waiting for
its client, so no client can delay the drain or the process exit. Work that a session command had
already started, such as a transaction commit, keeps running, and terminal teardown waits for it
until the shutdown deadline.

Graceful shutdown records whether that stable node name was already cordoned before it invokes the
drain. Its cleanup clears the drain cordon only when shutdown began with an uncordoned node, and it
runs after a successful, failed, or timed-out drain attempt. A pre-existing operator cordon therefore
remains set across shutdown and restart. When the drain timeout or the shutdown deadline passes, or
the leader cannot be reached, before the node requests its drain, nothing was cordoned and no
cleanup runs.

A graceful-shutdown drain has two parts that share one drain timeout, and the drain also ends when
the shutdown deadline passes first. When another live, schedulable Raft voter exists, the node
first moves its scheduled work there through the planned handoff above. It then completes the work
it has already admitted in place. That second part is the whole drain when no replacement exists,
such as on a single node or on the last schedulable node of a cluster, and it also covers listener
ingestors, which bind on every node, and any unit whose move failed. The
terminating node stops new intake on all of its ingestors, whatever their `ON QUIESCE` mode, and its
generators stop producing. Work already admitted keeps flowing while the node's relays, processors,
emitters, and acknowledgement paths stay alive: Nervix repeatedly force-flushes ingestor routes,
processor collections and route buffers, message-error routes, reingestors, and emitters, whatever
their `FLUSH EACH` cadence, until no relay batch, node work item, emitter buffer or publish, or
admitted acknowledgement root remains. One more force flush then confirms that nothing is still
moving, so work an upstream flush publishes after a downstream node finished its own flush is not
left behind.
Source acknowledgements and commits, such as Kafka consumer-group offsets, complete before the source
session stops.

Terminal teardown stops the node's tasks only after that drain completes or its timeout passes. It
gives each background task at most two seconds to stop, waits for work that sessions and commands
started until the shutdown deadline and cancels whatever still runs then, and finally stops the
runtime, consensus, cluster membership, and the interconnect and releases the node's storage. A
timeout reports the drain as abandoned, and teardown negatively acknowledges the remaining work: a
source with external acknowledgements redelivers it after restart, and a `NO_ACK` source loses it. A
sink that stays unavailable therefore holds the drain until its timeout. Pending `REQUIRED WAIT`
records do not hold the drain open; every force flush retries them against the state that is
present, and teardown negatively acknowledges whatever still waits. Payloads that other live nodes
publish into this node's relays are admitted work as well, so an owner that keeps publishing extends
the drain until its timeout. When every node terminates at once, each node completes its own
admitted work within its own drain timeout, and work that reaches a peer after that peer finished
its drain is negatively acknowledged.

`DROP NODE` records the stopped process incarnation before removing its Raft membership. Delayed
gossip cannot admit that process again. Starting the node again creates a newer incarnation, which
can join the cluster normally.

Unexpected owner loss remains a termination and uses the failover path. The failed task and its
volatile buffers disappear immediately, attached work is negatively acknowledged, and the scheduler
promotes a live replica or chooses a fresh owner. Failover does not wait for the planned handoff gate.
If a former owner disappears while a planned hold is active, that hold aborts without publishing its
candidate; ordinary failover then relocates from the last committed schedule.

Forced recovery stages the destination's checkpoint inventory under the destination process
incarnation and the complete target-schedule fingerprint. Applying staged checkpoints accepts only
that exact preparation. A missing or mismatched preparation does not imply a reset; without an
exhaustive reset outcome in the schedule, activation fails and leaves every saved checkpoint
unchanged. After activation, the durable completion belongs to the ownership transition itself, so
reapplying its retained schedule after a destination restart or a later domain rebuild preserves any
newer checkpoints the destination has published. A state reset occurs only when the accepted recovery
decision either stages the recreated checkpoint inventory or reports a reset outcome for every state
component owned by the entity.

A forced recovery of a WASM processor also starts a new guest-state generation for every branch, in
the schedule publication that names the new owner, and that schedule is the one the recovery
preparation is fingerprinted against. The destination stages the checkpoints of the generation being
replaced and activation publishes them in the new generation. A snapshot of any earlier generation,
whether it is held by the lost owner, a replica that was offline, or an older preparation, is never
selected, installed, or restored again, even when its revision is higher than every current one. A
planned handoff keeps the generation. Generation transitions are published only by a committed
schedule, so they are serialized with every other mutation of the domain through the same lease or
automatic-decision fence as the schedule itself.

For a planned handoff, each prepare destination becomes a tracked participant before the
side-effecting request is sent. A lost response and cancellation of the coordinating future are
therefore cleaned up like acknowledged preparations. Cleanup retries an exact discard and never
uses gate release as evidence that the durable preparation was removed.

The surviving leader reconciles preparations after it orders all inherited schedule proposals with
a committed consensus barrier. A destination preserves a preparation when its exact transition is
the committed owner change, regardless of coordinator or participant restart, so normal schedule
application can activate it. It also temporarily preserves uncommitted work from the same leader
process while the bound source and destination incarnations remain live, the base schedule still
owns the entity on the source, and the exact handoff gate remains held. It durably removes every
other preparation. Replacement, discard, activation, and reconciliation compare the complete
operation identity, so duplicate or reordered work is idempotent and cannot affect another
operation's preparation.

Entity-gate leases are deadline-bound. They release their relay fences and ingestor holds at the
configured entity-gate deadline even if the coordinator disappears. A node that joins during a hold
applies the published schedule through its normal revision path.

An unchanged candidate contributes no aspect. An all-no-op batch therefore performs no storage
write or schedule publication and reports `DYNAMIC`, even when the running domain has work that
could not currently drain. A `DROP` followed by `CREATE` of the same key in one batch is compared as
one modification, so recreating a relay with a different schema cannot bypass domain quiescing.
An immediate model command reports the level it executed. A queued model command reports its own
preflighted model level before execution. `COMMIT` reports the maximum effective level actually
executed and the total planned relocations when the transaction moved owners. Nervix recalculates
the effective level and target schedule from the complete candidate at commit time.

For an immediate model alteration, local registry persistence and schedule publication are
separate steps. If schedule publication fails, Nervix restores the previous models and republishes
the previous schedule at every quiesce level; a domain-paused batch additionally resumes the
domain. A model batch that creates, changes, or drops a VHOST also waits until the HTTPS listener
of every live node has installed the resulting certificates, and fails with the node and reason
when one cannot. A batch that did not pause is then rolled back, and the command waits until every
listener presents the restored certificates before it reports the failure. A paused batch has
already resumed by then, so it keeps its committed models like any other failure after activation.
During a replicated transaction commit, the new schedule and transaction progress become visible in
one Raft operation. The leader rolls back an unpublished local registry candidate, and
every node synchronizes its registry cache from the committed schedule across leadership changes.

What it does not do is provide transactional semantics for the actual records flowing through the graph. Message batches and ACK state are data-plane hot-path state and are never persisted by the control plane.
