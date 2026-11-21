# Inferencer State And Recovery

*Plexus, the Nervix graph definition language.*

An inferencer that declares `STATE` runs a model whose state lives between invocations: a recurrent
detector, a forecaster whose residual is the score, a streaming model with a key-value cache. The
model carries that state as explicit ports. Each `STATE` entry pairs a model input with the output
that produces its successor, and Nervix carries the tensors from one message of a concrete branch
to the next. This chapter defines that contract end to end: who owns each piece of the carried
state, what is durable and what is not, how a published generation becomes a snapshot, a replica
copy, a handoff transfer and an archive section, which identity fences out every earlier
declaration, and what each kind of failure leaves behind.

Two rules shape the design. First, compute and memory are separate, as in the WASM host. The ONNX
session is compute: it holds no state between runs. The carried tensors are memory: a typed value
the branch owns, that a run takes as input values and returns as output values. Second, durability
is the periodic snapshot path that window and deduplicator branches use, not the per-callback
checkpoint with acknowledgement holds of [WASM State And Recovery](./wasm-state.md). Inference
releases its input acknowledgements once its output is dispatched. The state therefore reflects
every input admitted before its last durable copy, and recovery can skip inputs admitted after it.
A path through an inferencer is at least once.

There is one inferencer implementation, and `STATE` is a variation of it, not a second path. Every
inferencer runs its model once for each batch it admits, before any route, and `STATE` makes that
run go one message at a time and carry the tensors from message to message.

This chapter describes the architecture. [Inferencer](./processors.md#inferencer) owns the Plexus
statements and the user-facing account of execution, state tensors and inspection; [Shutdown And
Recovery](./shutdown.md) owns the shutdown phases and what each kind of ending keeps; [Backup And
Restore](./backup-and-restore.md) owns the archive's statements, sections and limits.

## Ownership By Layer

| Layer | Owner | What it owns |
| --- | --- | --- |
| Vocabulary | Inferencer Models and state facts | The `STATE` declaration: ordered port pairs, fixed dense `F32` shapes and `INITIAL` values; `InferencerModelFingerprint`, the identity of one model binding and its declarations; the typed state summary a description reports; the reset causes a forced recovery records. |
| Language | Plexus | The `STATE` block of `CREATE INFERENCER`, its canonical text and completion, `SHOW CREATE INFERENCER`, `DESCRIBE INFERENCER` and `SHOW INFERENCER ... STATE`, lowered into Models. |
| Engines and infrastructure | The inferencer snapshot frame (`src/inferencer_state_snapshot.rs`) | The one current frame: its signature, the revision, the branch incarnation, ordered tensor shapes and exact float bits, the conservative footprint every bound is derived from, and typed refusals of a frame it does not recognize. |
| Engines and infrastructure | The runtime state store | Writing a snapshot under its placement, the durability barrier, the latest-snapshot index, and removing snapshots of a replaced placement. |
| Engines and infrastructure | The interconnect | The branch-local runtime-state kind tag `6`, its placement envelope, checkpoint descriptions and bulk fetches, unit observation for descriptions and owner pages for state inspection. |
| Engines and infrastructure | The backup archive | The inferencer state descriptor and the raw `state.bin` section of ordered little-endian `F32` bits. |
| Decisions | Registry validation and planning | Distinct model ports, fixed state shapes, deterministic exact `INITIAL` types, the derived snapshot bound, the model fingerprint computed once per scheduled revision, the typed state plan and the archive plan a restore must match. |
| Data plane | The branch task | The branch's native session and carried tensors, the model run of each admitted batch, installing successors and publishing a generation after each batch that advanced the state. |
| Data plane | The shared periodic branch-state owner | The published generations of one branch, their revision sequence, replica progress, checkpoint-catalog registration, and persistence through the inferencer snapshot codec. |
| Data plane | The snapshot task and the replica poll task | Persisting the newest publication on the snapshot interval and once more when the branch stops, announcing it, and fetching newer checkpoints onto replicas. |
| Control plane | Ownership handoff and forced recovery | Capturing the frozen publication, transferring and restoring it before activation, and staging the newest surviving snapshot or recording a reset. |
| Control plane | Backup and restore | Capturing each branch's tensors at a cut, checking their identity, and installing restored bits under the scheduled fingerprints. |
| Control plane | Observation | Composing `DESCRIBE INFERENCER` from configuration, placement and typed owner reports, and paging `SHOW INFERENCER ... STATE` from the owners. |
| Edges | The session service and clients | Returning a description as text, and a state read as the typed state inspection envelope beside its text or JSON rendering. |

The data plane never reads the inferencer Model. The decision layer converts the declaration once
into a typed state plan and computes the model fingerprint once; the branch task, the snapshot
codec and every transfer act on those typed values.

## Branch Ownership

Every concrete branch of an inferencer has its own native ONNX session and, with `STATE`, its own
carried tensors. The branch task owns both and is the only task that changes them. A session is
loaded on the branch's first admitted batch from the one resource version the inferencer pins, on
the node's bulk workers; a full bulk class makes the branch wait, because it already holds the batch
that needs the session. Every branch shares the inferencer's prepared definition: its ports, its
execution mode and the initial generation of its `STATE`. A model run moves the session into an
admitted job on the node's extension workers, and the job hands it back with the run's outcome, so
no lock guards it. See [Execution](./processors.md#execution) and [Executor Admission And
Draining](./execution-primitives.md#executor-admission-and-draining).

An unbranched inferencer carries one state sequence, the explicit unbranched instance. A branched
inferencer carries one per concrete key and has no unbranched instance. The branch belongs to one
partition of its parametrization, and the inferencer's unit for that partition owns its tensors.
When the partition count changes, the partition the new count places a key in claims the key with
its tensors, as it claims the key's other state, before the key's next record runs; see [Changing
the partition count](./relay.md#changing-the-partition-count) and [Branch-Owned
Runtime](./branch-owned-runtime.md).

The committed schedule gives each unit one owner and zero or more replicas. Only the owner runs the
branch and publishes its tensors. A replica keeps durable copies of the owner's generations. The
control plane, the inspection and the logs name a concrete branch by its fingerprint, never by its
field values.

## What Is Durable

The carried tensors are the only inferencer state Nervix makes durable.

| State | Where it lives | Survives |
| --- | --- | --- |
| The newest published generation of a branch's tensors | The owner's memory | Nothing by itself: the snapshot task persists it, a replica fetches it and a planned handoff captures it |
| Durable copies: the owner's persisted snapshots and the generations each replica fetched | The stable storage of the owner and of each replica | A restart, and an owner loss when a surviving node holds one |
| The branch incarnation | The branch lifecycle, which the unit checkpoints separately | With the lifecycle; see [Shutdown And Recovery](./shutdown.md#what-survives) |
| The ONNX session and its allocations | The branch task | Nothing; reloaded from the pinned version |
| Input a route buffers, output a route has not dispatched, and their acknowledgements | The branch task | Nothing |
| Snapshot progress and replica observations | The owner's memory | Nothing |

A model run never leaves state in the session. The state enters a run as model input values and
returns as model output values, which become the branch's next generation without passing through
Arrow. Route construction never reads a state port.

## The Carried State's Lifecycle

### Initialization

Preparing the inferencer for a processor revision materializes the initial generation once, before
the plan is published: zeros, or each entry's constant `INITIAL` value, evaluated by the VM as a
configuration constant and checked against the declared shape. The bound is checked before any
tensor is allocated. Every new branch starts from that shared generation.

### Execution

Each admitted batch runs as follows on the branch task:

1. `FILTER WHERE` rows are removed and materialized dependencies are resolved before the batch is
   admitted, so a filtered row never reaches the model.
2. `INPUTS` are evaluated over the batch. A message whose evaluation fails, or whose mapped values
   do not form the declared tensors, fails alone through each route's `ON MESSAGE ERROR`.
3. The model runs one message at a time, in arrival order. Each message's call reads the carried
   tensors as its state inputs. A call is accepted only when every public output and every state
   successor is valid; then all successors advance together and become the state the next message
   reads. A message the model refuses, or whose outputs the declaration does not describe, fails
   alone and leaves the preceding state in place. A batch too large for one charge runs as several
   admitted jobs in order, each starting from the state the earlier jobs' accepted messages left.
   When a job fails as a whole, its messages and every later one fail together and advance
   nothing, while the messages earlier jobs accepted keep their outputs.
4. The jobs' outputs are joined into the batch's outputs, and only then does the state the accepted
   messages left become the branch's carried state. When the join fails, every accepted message
   fails with it and the carried state stays where the preceding accepted message left it.
5. Every route receives the accepted outputs, and the inferencer releases its acknowledgement
   share of a message once every route has taken or buffered its own.
6. When the batch advanced the carried state, the branch publishes the carried tensors as one
   new immutable generation, stamped with the next revision of its placement.

A batch in which no message advanced the state publishes nothing. A message's working set includes
the carried state, so the state counts against the relay operation limit every message is held to.

### Publication

Publishing replaces one pointer. The published generation retains the native tensors themselves:
publication copies nothing, and later model runs build their successors in new tensors while a
snapshot, a replica fetch, a handoff capture or an inspection keeps the generation it loaded for as
long as it needs. See [Periodically snapshotted branch
state](./data-plane-concurrency.md#periodically-snapshotted-branch-state).

### Restore At Branch Creation

A branch that is created reads the newest snapshot of its placement before it admits any work: the
newest on the node's own storage, or the one a planned handoff transferred when it is at least as
new. The snapshot is decoded on a charged bulk worker; its revision must be the revision it was
stored under, and its tensors must have exactly the declared shapes. The branch then compares the
snapshot's incarnation with its own:

- the same incarnation: the branch continues from the restored generation;
- another incarnation, or no snapshot: the branch starts from `INITIAL`, and a generation left by an
  earlier incarnation is withdrawn by publishing an empty generation, so a returning key never reads
  its predecessor's tensors.

The incarnation is the branch's lifetime as the unit's branch lifecycle records it. A key that was
evicted and returns is a new incarnation, so it never matches a snapshot its predecessor left, even
after a restart.

### Eviction And Drop

A branch evicted for its `TTL` or its instance limit discards its tensors and publishes an empty
final generation before its replication route retires. A returning key therefore starts from
`INITIAL`, as a window does, and quiet keys never accumulate state. Dropping the inferencer removes
its stored snapshots.

## Identity

Every snapshot is stored, served, replicated, transferred, archived and restored under a
placement: the domain, the inferencer, its scheduled unit, the concrete branch or the unbranched
instance, and the runtime state `Inferencer { schema, model }`.

- `schema` is the fingerprint of the schemas the inferencer's records are laid out by, as for every
  schema-bound state.
- `model` is the `InferencerModelFingerprint`: a SHA-256 digest, from the `nervix-crypto` boundary,
  over the resource, the pinned version, the file and the complete ordered `STATE` declarations,
  their `INITIAL` expressions included. The decision layer computes it once for the scheduled
  revision. It is a non-security cache and placement identity, recorded in the [classification
  register](./cryptography.md#classification-register).

A stateless inferencer has no model fingerprint and no state placement. A rebinding to another
version, a different file, a redeclared `STATE` or a change of the inferencer's schemas therefore
addresses a placement that has no snapshot, and every branch starts from `INITIAL`. Applying the
committed schedule of a running domain removes snapshots under a replaced fingerprint, so a restart
never reopens them. A node that did not apply that schedule while the domain ran keeps them until it
does. See [Checkpoint Identity](./shutdown.md#checkpoint-identity).

Within one placement, the revision orders generations. The owner stamps each publication with the
next revision of its placement, and a restored branch continues from the revision it restored.

## Snapshots

### The Frame

A snapshot is the 16-byte signature `INFERENCER STATE` followed by an rkyv archive of the revision
and, for a branch that has state, its incarnation and one entry per `STATE` declaration in order:
the tensor's shape and its values, every `F32` bit preserved, signed zeros and NaN payloads
included. An evicted branch's snapshot carries no branch. A frame with another signature fails with
an instruction to recreate the stored state; there is no second frame version and no conversion.

Decoding checks the frame length against its bound before alignment, the archived lengths before
allocating any owned tensor, and the decoded tensors before installation. Encoding and decoding run
on the node's bulk CPU workers under a memory charge of twice the snapshot footprint, the two
representations held at once, and check cancellation between tensors.

### The Bound

The bytes one branch's state occupies follow from the declared shapes: four bytes for each declared
element. The snapshot footprint adds the frame's fixed header and, for each tensor, its shape axes
and the descriptors encoding and decoding hold. A declaration must fit one runtime snapshot
section, the executor's `snapshot_section_bytes` operation limit of 8 MiB. `CREATE INFERENCER`
checks the derived footprint before storing the inferencer, and preparing `INITIAL` checks it again
before the processor plan is published. The bound derives from that section size; inferencer state
has no limit or setting of its own. Total memory is the per-branch bytes times the live branches,
which the branch's `MAX INSTANCES` bounds. See [Capacity Planning](./capacity-planning.md).

### Publication, Persistence And Replicas

Each stateful branch has a snapshot task that runs under the runtime, not under its branch task:

1. At each tick of the replication poll interval (`--runtime-replication-poll`, one second by
   default), capped at the snapshot interval, the task observes the snapshot cadence.
2. When the cluster's `STATE_SNAPSHOT_INTERVAL` (thirty seconds by default) has elapsed, it encodes
   the newest published generation, if it is newer than the last one persisted, writes it to the
   state store under the branch's placement and announces the revision to the unit's replicas. A
   changed interval starts a fresh period.
3. When the branch stops, the task makes its last write of what the branch published last,
   including when the branch task was already ended at its stop grace; an evicted branch's last
   write is its empty generation. A graceful domain stop completes only after these writes. When
   the runtime stops, the task writes what was published last.
4. An enforced stop leaves the last snapshot as it was, because the generation published since may
   hold the effect of work the stop cut.

A failed write never advances persistence progress, and writing an older generation never moves
it backwards. Every failure counts in `nervix_inferencer_snapshot_failures_total` and logs at
`warn` at most once per unit per snapshot interval.

A replica learns of changed branches from the owner's checkpoint catalog, which records every
publication, in rounds that run when the replica task starts, when the owner announces a checkpoint
and once every replication poll interval. For each changed branch the owner answers with its newest
published generation, encoded at request time, and the replica fetches it through the bulk pool,
verifies its length and digest and installs it on its own storage. A replica can therefore hold a
generation newer than the owner's last periodic write. See [Cluster
Interconnect](./interconnect.md#membership-consensus-and-bulk-transfer).

## Planned Handoff

A planned handoff, by `RELOCATE`, a node drain or a placement change, moves each branch with every
input the source admitted. The handoff gates the inferencer's input relays and freezes its
branches once their admitted work completes. Freezing makes the routed publication current: it
reflects every input the source admitted before the hold, while the store holds at most the last
periodic snapshot. The capture therefore encodes that publication, and falls back to the store only
for a branch without a routed state, as in a stopped domain.

The source describes each captured snapshot by its revision, length and digest, the destination
fetches it through the bulk stream and verifies it, and the destination restores it before
activation. A fetch, verification or restore failure ends the handoff before the schedule changes.
A later handoff onto a node that previously owned the unit replaces every snapshot it stored for
that unit, so it never resumes from tensors it held before. See [Planned Ownership Handoffs And
Failover](./control-plane.md#planned-ownership-handoffs-and-failover).

## Forced Recovery And Restart

When a unit's owner is lost, the destination stages, within the recovery's preparation deadline and
for every branch of the lifecycle it recovers, the newest valid snapshot that survives on its own
storage or on another node the unit is assigned to. Ties at the same revision with different bytes
are a conflict. The branch then starts:

| What survives of a branch | The branch continues from | The unit's transition in `SHOW CLUSTER STATUS` |
| --- | --- | --- |
| A valid snapshot on the destination or another assigned node | The newest of them | No inferencer reset for this branch |
| No snapshot | `INITIAL` | `state_recovery=reset` with `resets=inferencer:missing_checkpoint` |
| Only snapshots that fail validation | `INITIAL` | `state_recovery=reset` with `resets=inferencer:invalid_checkpoint` |
| Two different snapshots at the newest revision | `INITIAL` | `state_recovery=reset` with `resets=inferencer:conflicting_checkpoint` |
| No branch lifecycle either, as after losing an owner without a replica | `INITIAL` for every branch | `state_recovery=reset`, with `resets` naming `branch_lifecycle:missing_checkpoint` and `inferencer:expired_branch_metadata` |

A promoted replica holds at least the generation it last fetched. A former owner that returns is
fenced by the committed schedule and runs none of the recovered branches. See [When The Former Owner
Is Gone](./shutdown.md#when-the-former-owner-is-gone).

A restarted node reopens each branch at the newest snapshot on its own storage under the identity
the committed schedule publishes, subject to the incarnation rule above. `STOP` followed by `START`
retains the final generation the graceful stop persisted.

## Backup And Restore

A backup carries each branch's tensors as a descriptor and a raw section:

- `domains/<domain>/state/inferencer/<inferencer>/<unit>/<branch>/descriptor.rkyv` holds the
  domain and inferencer names, the scheduled unit, the schema and model fingerprints, the typed
  branch key and its fingerprint, the nonzero branch incarnation, the revision, the ordered shapes
  and the raw section's exact length and digest;
- `state.bin` beside it concatenates the tensors in `STATE` declaration order, each row-major, as
  little-endian `F32` bit patterns.

A quiesced cut forces each branch to publish and captures that generation; `WITHOUT PAUSE` captures
the latest publication without a common quiesced boundary; a branch without a live owner, as in a
stopped domain, is captured from its stored snapshot. Capture checks that the placement names the
scheduled schema and model fingerprints and that the tensors have the declared shapes, and stages
one branch at a time. `WITHOUT STATE` omits the sections.

A restore accepts a descriptor only when its fingerprints and shapes match the restored schedule's
inferencer, wraps the identical bits in the current snapshot frame and installs them on the
scheduled owner and replicas, with no numeric conversion and no model run. Restored branches start from those tensors at
`RESUME` and at a later `START`. See [Backup And Restore](./backup-and-restore.md#archive-format) and
[Backup And Restore Architecture](./backup-and-restore-architecture.md).

## Rebinding And Redeclaration

A tensor-state lifetime belongs to one model binding and one `STATE` declaration. `REBIND RESOURCE`
to a different version starts every branch from its declared `INITIAL`, even when the new version
holds identical ONNX bytes, because the version is part of the fingerprint. Uploading a newer
version alone, rebinding to the version already pinned, and a rebinding the leader rejects because
the model does not match its declaration leave the existing tensors in place. Dropping and
recreating the inferencer with another `STATE` declaration starts from that declaration's
`INITIAL`. A rebinding keeps its `ENTITY_PAUSE` classification. No state is migrated between
bindings. See [Classification And State
Effects](./resource-versions.md#classification-and-state-effects).

## Replay, Skips And Duplicates

The state reflects every input the owner admitted before the newest durable copy the branch
continues from: the newest snapshot on the node's own storage after a restart, the newest surviving
copy after an owner loss, everything admitted before the freeze after a planned handoff, and everything admitted
before a graceful stop. It is not an exactly-once guarantee:

- inference releases its acknowledgement share of an input after dispatch, so after a restart or
  an owner loss the inputs admitted after the restored snapshot are not replayed and the state
  skips them, unless another attached consumer still holds the ingestor's position;
- an input that is redelivered invokes the model again, even when the restored state already
  reflects it;
- after a failover, a survivor can refuse the output of a branch whose state already advanced,
  because it has not yet started the consumer the failover gave it; the ingestor redelivers that
  input, and every refused attempt counts as an update (see [Planned Ownership Handoffs And
  Failover](./control-plane.md#planned-ownership-handoffs-and-failover));
- an ingestor without acknowledgements redelivers nothing.

This is the window and deduplicator contract, and it suits a derived model state. A model that must
not count an input twice recognizes it itself, and exactly-once external effects need an emitter
whose writes are idempotent.

## Observability

| Signal | What it shows |
| --- | --- |
| `DESCRIBE INFERENCER <name>` | The pinned resource version and file, the execution mode, the `INPUTS`, `STATE` and `OUTPUT SCHEMA` declarations, the static state bytes per branch, the branch policy and routes, and per unit its owner and replicas, the branches in its published state and their bytes, and publication, persistence and replica progress, rendered as text; any node serves it by asking the unit owners for their typed facts |
| `SHOW INFERENCER <name> STATE` | Through the shared state inspection envelope: one `header` row per selected branch with its fingerprint, incarnation, publication and persisted revisions, tensor bytes, replica counts and persistence stage, then one section per `STATE` entry with its ports, shape and values |
| Metrics | `nervix_inferencer_state_bytes`, `nervix_inferencer_snapshot_failures_total`, `nervix_inferencer_restored_branches_total` and `nervix_inferencer_reset_branches_total` per unit |
| `SHOW CLUSTER STATUS` | A forced recovery's `state_recovery` outcome and the `inferencer` component it reset, with the cause |
| Server log | A failed snapshot publication or persistence at `warn`, at most once per unit per snapshot interval |

Both reads are read-only. A description reads published generations and owner observations; a
state read pages the immutable generation each selected branch published. Neither takes a branch's
execution lane, requests publication or advances a snapshot. Revisions are branch-local: a unit's
revision at a stage is the highest branch revision observed there, not a checkpoint of every
branch. State values never appear in logs, errors or `DESCRIBE`, and a state read redacts the
`values` cell when any input or branch-key field is sensitive. See [Inspect inference configuration
and state](./processors.md#inspect-inference-configuration-and-state), [Inspect Carried Inference
State](./processors.md#inspect-carried-inference-state) and [Inferencer
State](./metrics-and-observability.md#inferencer-state).

## Guarantees And Limits

Nervix guarantees:

1. **One implementation.** An inferencer with `STATE` runs through the same admission-time run as
   every inferencer; `STATE` adds only one-message-at-a-time execution, the refusal of `BATCH` and
   the carried tensors.
2. **Arrival order per branch.** Each branch applies its messages to its state in arrival order,
   one model call per message, and all successors advance together.
3. **A failed message changes nothing.** A message the model refuses, that fails alone, or that
   fails because its batch's outputs cannot be joined leaves the state as the preceding accepted
   message left it, and a filtered row never reaches the model.
4. **Branch isolation.** Every branch has its own session, tensors, publications and snapshots; no
   branch reads another's state.
5. **Bit-exact transport.** Every snapshot, replica copy, handoff transfer, archive section and
   restore preserves each tensor's shape and every `F32` bit.
6. **No state across identities.** A tensor-state lifetime is never reopened under another model
   binding, `STATE` declaration, schema or branch incarnation.
7. **Nothing lost by a planned move or a graceful stop.** A planned handoff carries the state of
   every input the source admitted, and a graceful stop persists the final generation before it
   completes.

Nervix does not provide, and these limits apply:

- **Exactly-once state updates.** See [Replay, Skips And Duplicates](#replay-skips-and-duplicates).
- **Updates after the last durable copy** are skipped after a restart or an owner loss.
- **Loss of state without a surviving copy.** A branch whose snapshots were only on a lost owner
  restarts from `INITIAL`, with the reset reported.
- **State across model versions.** A rebinding to another version always starts from `INITIAL`.
- **Dense `F32` state with fixed axes**, within one 8 MiB snapshot section per branch.
- **No batched stateful execution.** A `STATE` inferencer cannot declare `BATCH`.
- **No reset command.** Rebinding, redeclaration and eviction start fresh state; Nervix has no
  statement that resets an inferencer's tensors.
- **Evicted branches lose their state.** A key that returns after eviction starts from `INITIAL`.
- **An enforced stop** keeps the last snapshot taken before it, not the state at the moment of the
  stop.

## Qualification Evidence

The inferencer state contract is proven through the public interface on one node and on three
nodes with interleaved records of two branches, and through checks over the production owners.

| Guarantee | Evidence |
| --- | --- |
| One implementation, arrival order and branch isolation | `inferencer_admission.feature` "Routes with different FLUSH policies receive the result of one model run"; `inferencer_state.feature` "Stateful inference carries one ordered sum per branch across independent routes" |
| A failed message changes nothing | `inferencer_state.feature` "A model refusal preserves the preceding state and routes the original input" and "Unbranched inference skips filtered and failed inputs without advancing its constant initial state"; `inferencer_admission.feature` "A row the model refuses fails alone while the rows around it are delivered"; the owner test `messages_that_fail_because_their_jobs_outputs_cannot_be_joined_leave_the_carried_state`, for a batch that runs as several jobs whose outputs cannot be joined |
| Restart and graceful stop | `inferencer_state.feature` "Stateful inferencer snapshots preserve interleaved branch sums across a cluster restart"; `inferencer_state_lifecycle.feature` "Stateful inference resumes both interleaved branch sums after STOP and START"; `inferencer_state_handoff.feature` "A graceful cluster stop writes the final inference generation of both branches" |
| No state across identities | `inferencer_state_lifecycle.feature` "Recreating an inferencer starts both branch sums from INITIAL", "TTL eviction starts returning inferencer branches from INITIAL" and "Capacity eviction cannot restore a previous inferencer lifetime after restart"; `inferencer_state_rebinding.feature` "Rebinding resets both branches while uploads, unchanged versions and refused models preserve them" |
| Planned handoff | `inferencer_state_handoff.feature` "Relocating a stateful inferencer moves both branch sums exactly", "A drained owner hands both inferencer branch sums to its successor" and "An entity-paused alteration upstream of a stateful inferencer keeps both branch sums"; the Shuttle check `shuttle_inferencer_publication_keeps_captured_tensors_stable_through_handoff_freeze`; the owner tests `a_handoff_carries_an_inferencer_publication_newer_than_its_stored_snapshot` and `a_handoff_without_a_routed_inferencer_state_carries_the_stored_snapshot` |
| Owner loss and fencing | `inferencer_state_lifecycle.feature` "A promoted inference replica continues both branches from at least their confirmed checkpoints"; `inferencer_state_handoff.feature` "Losing the owner without a replica restarts both branches from INITIAL and reports the lost lifecycle" and "A returning former owner is fenced and receives its successor's sums when ownership moves back"; the owner test `forced_recovery_resets_an_inferencer_branch_without_surviving_tensors_to_initial`, for a branch whose lifecycle survives without its tensors; the [`stateful` chaos scenarios](./cluster-chaos.md#stateful), whose `chaos_running` count after the fault must hold at least each branch's records before the milestone |
| Acknowledgements | `inferencer_state_acknowledgement.feature` "A stateful inferencer acknowledges every record of the batches it admits", "Records a promoted inference replica emits are acknowledged to their source" and "A stateful inferencer's source stays unacknowledged until its emitter's broker takes the results" |
| Replica transfer under partition | The Turmoil scenario `transport::materialized::inferencer_branch_state_transfers_reconnect_to_the_replacement_owner` |
| Backup and restore | `inferencer_backup.feature`; the Shuttle check `shuttle_a_forced_inferencer_publication_holds_every_row_acknowledged_before_the_cut` |
| The derived bound | `inferencer_state_lifecycle.feature` "An inferencer whose declared state exceeds a snapshot section is refused before storage"; `inferencer_state.feature` "Stateful inference refuses an invalid state declaration before activation" |
| Inspection and redaction | `inferencer_describe.feature`, `inferencer_state_inspection.feature`, and `inferencer_state.feature` "Stateful model diagnostics keep private values out of the server log under trace logging" |
| Eviction racing replacement | The Shuttle check `shuttle_inferencer_eviction_racing_replacement_keeps_the_successor_route` |
| Bit-exact representations | The registered Bolero targets `inferencer-state-snapshots`, `inferencer-state-snapshots-malformed`, `inferencer-native-snapshot-projection`, `backup-inferencer-values`, `inferencer-tensor-columns`, `inferencer-describe-envelope` and `interconnect-processor-state-inspection` |

The [inferencer state qualification ledger](https://github.com/nervix-io/nervix/blob/main/tests/inferencer-state-qualification-ledger.md)
names the executable evidence for every guarantee and the qualification record behind it.
[Stateful Inference 09](https://app.clickup.com/t/86bcyakvx) retains the qualification results,
with their commands and tested revision.
