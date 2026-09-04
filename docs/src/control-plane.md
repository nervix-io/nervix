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
- primary and replica assignment
- Kafka `OFFSET BY DOMAIN` partition-to-instance assignment and rebalance
- domain `START` and `STOP`

This is the part of Nervix where Raft-backed consistency matters. It keeps cluster-wide definitions coherent.

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
- `ALTER DOMAIN`, `START`, and `STOP`;
- `CREATE RESOURCE`.

Completion on the bound session resolves identifiers against the configuration the queued
statements produce, applied in written order, so a client is offered the models and resources its
own transaction defines and is no longer offered a model whose `DROP` it has queued. Only the
create and drop sequence decides a name, so an intermediate configuration that does not yet resolve
still completes. Sessions that are not bound to the transaction, including other sessions of the
same user, are offered committed configuration alone.

Read-only `SHOW`, `DESCRIBE`, and `LOOKUP` statements are rejected at queue time. `CREATE DOMAIN`
and `CREATE USER` are rejected too: neither belongs to a domain, so neither is transaction content.
Session subscriptions, `UPLOAD RESOURCE`, and node scheduling or membership operations (`CORDON`,
`UNCORDON`, `DRAIN`, and `DROP NODE`) are also immediate, non-transaction content. Run those
statements outside `BEGIN`/`COMMIT`.

Queue admission is not a blind append. The leader replays the replicated transaction prefix into a
side-effect-free candidate, then checks the new statement against that candidate. This catches such
errors as duplicate configuration, a missing `ALTER` target or field, invalid domain lifecycle,
invalid external bindings, and invalid UDF or schedule inputs before the statement is replicated.
A successfully queued model mutation reports the quiesce level contributed by that statement
against its prefix, even though the mutation has not executed yet. Configuration statements with
no useful command output return no message instead of a queue acknowledgement.
A rejected statement does not change the pending count or the transaction's activity time, so the
client can correct it and continue the same transaction. Limits are checked before this preflight
and every check is repeated during `COMMIT`, because other sessions may change control-plane state
after a statement was admitted.

An accumulated model run that already forms a complete graph receives the full registry, binding,
UDF, and scheduling preflight. Cross-model completeness remains provisional while the run is still
being assembled: an intermediate schema/codec mismatch or temporarily referenced model may be
repaired by a later statement in the same atomic run. Statement-local mutations must still be valid
against the prefix, and `COMMIT` requires the final candidate graph to pass every check. This keeps
coordinated multi-model migrations possible without letting a malformed `ALTER` or an impossible
lifecycle transition enter the queue.

Within a transaction, each consecutive run of model mutations can mix `CREATE`,
`ALTER SCHEMA`, `ALTER WIRE ... SCHEMA`, `ALTER RELAY`, `ALTER JUNCTION`, `ALTER DEDUPLICATOR`,
`ALTER REORDERER`, `ALTER EMITTER`, `ALTER INGESTOR`, `ALTER REINGESTOR`, `ALTER GENERATOR`,
`ALTER PLACEMENT`, and `DROP`. Nervix applies that run as one registry mutation: all operations are
evaluated in written order against one candidate model map, the complete domain graph is
revalidated, and one atomic storage batch persists the result. A failure writes nothing and does
not swap the active registry state. This supports coordinated wire-schema, internal-schema, codec,
relay, processor, emitter, ingestor, generator, placement, and dependent-node migrations without
exposing an invalid intermediate graph.

Other eligible statements apply individually. `COMMIT` records each step's effect, executed
quiesce level, and progress in one Raft operation and stops at the first failure. Its successful
output is only the highest quiesce level actually executed across the transaction; it does not
repeat the individual command outputs. A new leader automatically resumes every
`COMMITTING` transaction from its recorded progress: completed steps are not repeated, and a
failed remaining step records its statement number and error while preserving the applied prefix.
Atomicity still does not span the whole transaction.

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
  deduplicator/reorderer `MAX TIME`; emitter flush policy; and placement definitions are
  hot-applied from the published schedule while retaining buffered and branch-local state.
  Placement changes can still hand off runtime nodes when a new hard colocation group requires it.
  `CREATE` and `DROP` retain their existing pause-free schedule-rebuild behavior.
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
- An **assignment-only** change contributes no quiesce level at all. It is a published schedule in
  which one or more runtime nodes changed primary owner or replica set while no model changed, and
  it reaches every live node as a narrow activation rather than a domain rebuild. Node drain,
  failover after a cluster node becomes unavailable, and consolidation of a newly effective
  `REQUIRE COLOCATION` group all produce this class. A runtime node whose primary owner and replica
  set are unchanged does not stop, restart, restore a snapshot, or re-open an external session: its
  in-flight batches are not negatively acknowledged by the activation, its retained `REQUIRED WAIT`
  messages persist, its branch instances and branch-local state persist, and its ingestor source
  session keeps ingesting throughout. A moved relay first drains its owner buffer and dispatch
  slots; producers and consumers then re-point to its new owner without restarting. Readers
  declaring `USING MATERIALIZED STATE` on that relay re-bind their state source without
  restarting. A cluster node that gains or loses a relay state-replica role starts or stops replica
  polling and local snapshotting without instantiating the relay itself. A change
  touching several runtime nodes, such as a hard colocation group move, is one activation affecting
  exactly those nodes. A batch that changes both a model and an assignment applies the model change
  at its classified quiesce level and the assignment change narrowly; runtime nodes affected by
  neither are untouched.
- `DOMAIN_PAUSE` changes stop ingestion and generators across the domain and fully drain attached
  work before commit. Relay schema or branching changes and schema or wire-schema definition
  changes use this level. Changing the membership of an emitter's `FROM` relay list also uses this
  level because it changes graph topology. Configuration entities use this level too: codec,
  client, endpoint, signaling-protocol, hash-map, and UDF definitions, vhost hostnames and TLS
  bindings, and branch schema, TTL, and eviction settings. Their consumers read that configuration
  when they are built, so the domain rebuilds around the new models rather than reconfiguring in
  place.

An entity-paused change also gates everything downstream of it in the dataflow graph, not only the
models the batch names, so a dependent node cannot observe a half-applied change through its input
relay.

Entity holds are transient and deadline-bound. A relay gate self-releases if the leader disappears;
an ingestor hold releases the old source under its declared mode when it expires. Schedule application re-engages a local
relay gate before an affected node swaps itself. Sibling consumers of a gated relay can therefore
see bounded backpressure for at most the gate deadline, but unrelated relays and nodes continue
flowing. Pending `REQUIRED WAIT` materialized records are carried through a node handoff rather than
treated as drainable work. A node that joins the cluster while an entity hold is engaged is not
covered by that hold; it re-engages its own local relay gate when it applies the new schedule, and
the hold's deadline bounds the window.

These modes govern only pauses that resume the same running graph on the same node. Stopping a
domain, dropping an ingestor, node drain or cordon relocation, failover, and graceful shutdown are
terminations: the source session ends after already admitted work drains, and a later start relies
only on external source retention. A drain, cordon relocation, or failover terminates only the
runtime nodes whose primary owner changed; every other runtime node in the domain keeps running,
including on the former and the new owner. Volatile quiesce buffers do not migrate and are lost if a
termination or crash interrupts them. Quiesced connected modes count as drained because their raw
buffers have not entered the graph.

A moved runtime node terminates on its former owner once its already admitted work drains within the
existing shutdown grace, and starts on its new owner. For a planned relay move, producer dispatch is
gated until the new owner is ready, so the owner buffer and fixed dispatch slots cut over without
loss. State migration is unchanged: replicated state kinds are present on the new owner only when
it was already a replica, and every other state kind restarts from the new owner's local snapshot.
An unplanned relay-owner failure loses its volatile buffer, presence, and metrics; materialized
records survive when a current state replica is available for promotion. Batches for unaffected
runtime nodes are not interrupted. Draining a
cluster node that hosts several runtime nodes performs one narrow activation per moved node, and
each moved node restarts exactly once. There is no domain-wide ingestion pause for an assignment
change, so a lagging cluster node no longer stalls ingestion elsewhere while the initiating command
waits for every live node to activate the revision.

An unchanged candidate contributes no aspect. An all-no-op batch therefore performs no storage
write or schedule publication and reports `DYNAMIC`, even when the running domain has work that
could not currently drain. A `DROP` followed by `CREATE` of the same key in one batch is compared as
one modification, so recreating a relay with a different schema cannot bypass domain quiescing.
An immediate model command reports the level it executed. A queued model command reports its own
preflighted level before execution, while `COMMIT` reports the maximum level actually executed for
the complete transaction. Nervix always executes exactly the level classified at commit time.

For an immediate model alteration, local registry persistence and schedule publication are
separate steps. If schedule publication fails, Nervix restores the previous models and republishes
the previous schedule at every quiesce level; a domain-paused batch additionally resumes the
domain. During a replicated transaction commit, the new schedule and transaction progress become
visible in one Raft operation. The leader rolls back an unpublished local registry candidate, and
every node synchronizes its registry cache from the committed schedule across leadership changes.

What it does not do is provide transactional semantics for the actual records flowing through the graph. Message batches and ACK state are data-plane hot-path state and are never persisted by the control plane.
