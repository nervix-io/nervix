# Control Plane

The control plane is where Nervix applies strong consistency.

It is responsible for:

- storing NSPL models
- validating references and compatibility
- computing domain schedules
- tracking domain lifecycle
- handling cluster coordination
- exposing control operations like `SHOW CREATE`, `DESCRIBE INGESTOR`, and `SHOW CLUSTER STATUS`

The most important property is that control-plane state is authoritative. A runtime node only exists because the control plane says it exists.

Execution graph configuration is part of this control-plane state. NSPL models, domain schedules, and lifecycle transitions are persisted with strong consistency guarantees before runtime nodes execute them.

In practice, the control plane covers:

- domain creation and selection
- model creation and deletion
- scheduling decisions
- explicit node removal with `DROP NODE <node_id>`
- node cordon and uncordon with `CORDON NODE <node_id>` and `UNCORDON NODE <node_id>`
- node drain with `DRAIN NODE <node_id>`, which cordons the node and moves scheduled graph nodes away one at a time
- primary and replica assignment
- Kafka `OFFSET BY DOMAIN` partition-to-instance assignment and rebalance
- domain `START` and `STOP`

This is the part of Nervix where Raft-backed consistency matters. It keeps cluster-wide definitions coherent.

NSPL command grouping is explicit. A session starts a control-plane transaction
with `BEGIN`, queues following NSPL statements, applies them with `COMMIT`, or
drops them with `REVERT`. `BEGIN` inside an active transaction is rejected, and
`COMMIT` or `REVERT` without an active transaction is rejected. A request that
contains multiple statements outside an explicit transaction is rejected instead
of being treated as an implicit batch.

Within a transaction, each consecutive run of model mutations for one domain can mix `CREATE`,
`ALTER SCHEMA`, `ALTER WIRE ... SCHEMA`, `ALTER RELAY`, `ALTER JUNCTION`, `ALTER DEDUPLICATOR`,
`ALTER REORDERER`, `ALTER EMITTER`, `ALTER INGESTOR`, `ALTER REINGESTOR`, `ALTER GENERATOR`, and
`DROP`. Nervix applies that run as one registry mutation: all operations are evaluated in written
order against one candidate model map, the complete domain graph is revalidated, and one atomic
storage batch persists the result. A failure writes nothing and does not swap the active registry
state. This supports coordinated wire-schema, internal-schema, codec, relay, processor, emitter,
ingestor, generator, and dependent-node migrations without exposing an invalid intermediate graph.

Transaction control also queues lifecycle and other server statements, but those statements are
not folded into the registry mutation batch. Data-plane records are likewise outside this
control-plane atomicity.

## ALTER Lock And Quiesce Classification

Every model-mutation batch acquires one exclusive leader-local ALTER lock for its domain before
validation. The lock remains held through candidate planning, quiescing, persistence, schedule
publication, rollback when required, and resume. A concurrent mutation is rejected instead of
queued. Raft still serializes the durable domain lifecycle and schedule, while the registry's
base-model comparison remains a final consistency check.

Nervix classifies the validated base-to-candidate model diff, not the spelling of the statements
that produced it. The batch uses the highest level contributed by any changed entity:

- `DYNAMIC` changes do not pause ingestion. Relay capacity; processor filters, source predicates,
  collection, route construction, route flush, and same-target message-error policies;
  deduplicator/reorderer `MAX TIME`; and emitter flush policy are hot-applied from the published
  schedule while retaining buffered and branch-local state.
  `CREATE` and `DROP` retain their existing pause-free schedule-rebuild behavior.
- `ENTITY_PAUSE` changes gate only the affected relays on every live node, force-flush affected
  work, and wait for the gated relay rings and target-node work counters to drain before commit.
  Other domain traffic continues. A processor topology change then swaps only the affected node
  tasks and hands pending materialized-state work to their replacements. Deduplicator key changes
  also purge the old keyspace before the replacement starts; reorderer ordering changes flush the
  old ordering buffers before swapping. Relay materialized-state changes update membership in
  place. Emitter source-predicate, sink, client, codec, input-collection, and attachment changes
  drain and replace only the affected emitter task. Every current ingestor alteration stops and
  drains only the affected ingestor instances, then starts their desired source configuration from
  the published schedule. Reingestor alterations replace their relay consumers and
  branch-entrypoint wiring; generator alterations quiesce and replace their timed task after
  flushing pending route output.
  Correlator, window-processor, inferencer, and WASM-processor structural changes use this level
  as well. A WASM processor participates like every other stateful node: the host gates its input
  relays, asks the guest to release what it buffers, snapshots it, and restores that snapshot into
  the replacement instance.
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
an ingestor hold restarts the old source when it expires. Schedule application re-engages a local
relay gate before an affected node swaps itself. Sibling consumers of a gated relay can therefore
see bounded backpressure for at most the gate deadline, but unrelated relays and nodes continue
flowing. Pending `REQUIRED WAIT` materialized records are carried through a node handoff rather than
treated as drainable work. A node that joins the cluster while an entity hold is engaged is not
covered by that hold; it re-engages its own local relay gate when it applies the new schedule, and
the hold's deadline bounds the window.

An unchanged candidate contributes no aspect. An all-no-op batch therefore performs no storage
write or schedule publication and reports `DYNAMIC`, even when the running domain has work that
could not currently drain. A `DROP` followed by `CREATE` of the same key in one batch is compared as
one modification, so recreating a relay with a different schema cannot bypass domain quiescing.
The first mutated statement's result includes the quiesce level the batch executed. Nervix always
executes exactly the classified level.

Persistence and schedule publication are separate steps, so a batch that commits its models but
fails to publish the resulting schedule restores the previous models and republishes the previous
schedule at every quiesce level. A domain-paused batch additionally resumes the domain.

What it does not do is provide transactional semantics for the actual records flowing through the graph. Message batches and ACK state are data-plane hot-path state and are never persisted by the control plane.
