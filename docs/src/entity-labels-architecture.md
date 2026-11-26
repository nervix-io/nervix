# Entity Label Architecture

*Plexus, the Nervix graph definition language.*

Entity labels are small maps of string keys to string values that an operator attaches to the
named things Nervix stores: every domain Model, each domain, each logical resource and each
cluster user. A label describes its entity, for example who owns it or what it is for. It changes
nothing about how the entity runs. This chapter describes how labels are built: who owns each
part, how a label set is created, edited, read and kept, what a label change is guaranteed to
leave alone, and what the design deliberately leaves out.

Three rules shape the design:

- **A label set is an ordinary value of its entity.** It is one field of the record that already
  describes the entity. It is committed with that record and removed with it. There is no label
  catalog, no second store keyed by entity, and no object that stands for a label by itself.
- **Labels are control-plane metadata.** The data plane never reads them. A change that touches
  only labels is committed and published like any other change, and it reaches no running task.
- **A label set is judged whole, under one committed policy.** Four cluster parameters bound every
  set. The complete resulting set is checked before any effect, and again wherever a stored or
  transmitted set is decoded.

[Entity Labels](./entity-labels.md) owns the statements, the limits and the guarantees an operator
relies on. Where this chapter meets another one, that chapter keeps its own contract:
[Architecture Overview](./architecture.md#statement-heads-and-body-properties) for statement
heads and addressing, [Transaction Quiescence And Impact
Inspection](./transaction-quiescence.md) for commit effects, [Execution Plans](./execution-plans.md)
for what a plan carries, [Consensus Storage And
Replication](./consensus-storage-and-replication.md) for stored shapes, [Backup And Restore
Architecture](./backup-and-restore-architecture.md) for archives, [Client Session
Protocol](./client-session-protocol.md#entity-label-inspection) for the wire, and [Node
Settings](./node-settings.md#entity-label-limits) for the four limits.

## Why Labels Are Plain Values

### No Label Objects

Nervix has no `CREATE LABEL`, no label catalog and no managed label definitions. A key exists
only as an entry in some entity's map, and it means what the operator's own convention says it
means.

A catalog would give labels a life of their own. An operator would have to create a key before
using it, a key in use could not be dropped, and every edit would touch two records that have to
agree. Today a label set commits in the same write as its entity, so the two can never disagree,
and removing the entity removes its labels with nothing left behind. A shared catalog cannot
offer that. Governing which keys are allowed is a taxonomy question that an organization answers
by convention or with tooling outside Nervix.

### No Inheritance

A Model does not inherit its domain's labels, and a resource version does not inherit its logical
resource's labels. The set an inspection returns is exactly the set someone wrote on that entity.

Inheritance would make one entity's answer depend on another entity's state:

- An edit of a domain's labels would silently change the effective labels of every Model in it.
- The limits could no longer be judged for one entity at a time.
- An inspection would have to explain where each key came from.
- A decision an outside system based on an entity's labels could change although nobody touched
  that entity.

A tool that wants a combined view reads the domain's labels and the Model's labels and combines
them by its own rule.

### No Meaning In Keys Or Values

Nervix never interprets a label. No key is reserved, a `/` or `.` in a key is not a path, and no
value selects, grants, restricts or places anything. Labels have no effect on permissions,
authentication, placement, metric dimensions or logging.

Meaning attached to particular keys would turn descriptive metadata into configuration. A label
edit could then change what a running graph does, and the rule that a label-only change is
invisible to running work would no longer hold.

### Entity Labels, Node Tags And Metric Labels

Three things in Nervix look alike and are separate. Each has its own owner, and none is derived
from another.

| | Entity labels | Node tags | Metric labels |
| --- | --- | --- | --- |
| Describes | A named Plexus entity: a Model, a domain, a logical resource or a user | A physical cluster node | One metric series |
| Set by | The operator, with `LABELS` and `SET LABELS` | The operator, with `--node-tags` when the node starts | Nervix, from a set fixed by each metric family |
| Changes | At any time, with no effect on running work | With a restart of the node, which announces a new node record | Never by an operator |
| Read by | `SHOW LABELS`, descriptions, canonical text, archives | [Node rules and node selectors](./placement.md#selecting-nodes), which decide placement | Prometheus and `DESCRIBE` metric sections |
| Stored in | The entity's own committed record | The [committed node record](./node-settings.md#the-node-record) | The node's metric store |

Node tags decide where work runs, so they are part of scheduling. Entity labels decide nothing.
An entity label's key or value never becomes a metric label or an automatic log field: label
values are unbounded text chosen by operators, and metric dimensions must stay bounded, as
[Metrics And Observability](./metrics-and-observability.md) requires.

## Ownership By Layer

| Layer | Owner | What it owns |
| --- | --- | --- |
| Vocabulary | `nervix-models`, `entity_labels` | The label key, the label map, one edit of a map, the limits, the typed owner identity, the inspection result, and the typed refusals. The `EntityMetadata` projection that every Model payload implements. Canonical text of the label clause and the two label statements. |
| Vocabulary | `nervix-models`, cluster parameters | The four `ENTITY_LABELS_MAX_*` definitions with their defaults. |
| Language | `nervix-plexus`, `labels` | The trailing `LABELS` creation clause, `SET LABELS` and `SHOW LABELS`, built on the shared typed entity references. Completion comes from the composed grammar. |
| Engines and infrastructure | `nervix-consensus`, `entity_labels` | Labels inside the replicated state: the committed schedule's Models and the domain, user and resource-catalog records. Applying a domain or user label edit. Reading one owner's labels, the applied revision and the committed policy under one read. Refusing a limit reduction that a committed set exceeds. |
| Engines and infrastructure | `nervix-client-wire`, `entity_labels` | The inspection tables of the session protocol, encoded and decoded under the captured policy and the session's bounds. |
| Engines and infrastructure | `nervix-backup` | Labels in archive records, with the policy captured at the cut. |
| Decisions | The registry | A Model label edit as a domain mutation in the candidate graph. The change aspect that marks a label-only difference. Keeping the current placement for a run that changes only labels. Validating every Model's labels with its other parameter limits. Validating restored labels against the receiving cluster. |
| Control plane | Transactions and commit | Queueing Model and resource label edits, staging them for the session that owns the transaction, and publishing them with the run they belong to. |
| Edges | The session service | Choosing the committed or the staged view for `SHOW LABELS`, rendering text and JSON from the typed result, and adding the label line to descriptions. |
| Edges | The Rust client, the host bindings, the command line client and the web console | Handing the typed inspection to applications. The console's label rows in its create forms and its label editor. |

The data plane does not appear in the table. No plan, task, relay, connector or record path holds
or reads a label.

## Owners And Identity

Four kinds of entity own a label set:

| Owner | Identity | Where the set is stored |
| --- | --- | --- |
| A domain Model | Domain, Model kind and name | The `labels` field of the Model payload |
| A domain | Domain name | The `labels` field of the domain record |
| A logical resource | Domain and resource name | The `labels` field of the resource catalog entry |
| A cluster user | User name | The `labels` field of the user record |

Labels never replace identity. Two Models with the same name in different domains, or with the
same name and different kinds, are different owners with independent sets. The three wire-schema
kinds are separate Model kinds, so `WIRE JSON SCHEMA payload` and `WIRE AVRO SCHEMA payload` own
separate sets.

The identity is typed once. A statement names its target as an `EntityLabelTarget`: a Model
reference or a resource name, both resolved in the selected domain, or a domain or user named in
full. Resolution produces an `EntityLabelOwner`, which every later step and every result carries.
No step identifies an owner by display text.

These things have no label set: runtime branches, partitions, execution instances, resource
versions, sessions, subscriptions, payload fields and cluster settings. The labels of a resource
belong to the logical resource, not to any of its versions, and physical nodes have [node
tags](./node-settings.md#tags) instead.

### Every Model Kind Is Covered

Coverage of Models is structural or checked against the vocabulary, so no list of kinds can go
stale unnoticed:

- The macro that declares the Model union implements `EntityMetadata` for every payload type from
  its `labels` field. A new Model kind without that field does not compile.
- Every creation grammar ends through one shared function that parses the optional trailing
  `LABELS` clause into that field.
- The label statements address their target through the same typed Model reference `DROP`,
  `DESCRIBE` and `SHOW CREATE` use, which is derived from the Model kinds.
- Two generated tests walk the exhaustive Model generator: one checks, for every variant, the
  creation clause, both statements, canonical text and completion, and fails unless the kinds it
  saw are all the kinds there are. The other checks, for every variant, that a label-only
  difference is the control-plane-only label aspect and nothing else.
- The web console keys its create forms by Model kind, so a new kind does not compile there until
  the console has a form for it, and every form carries the same label rows.
- One scenario runs every Model description and requires its entity's labels in the output. A
  test in the language crate asks the grammar which Model kinds have a `DESCRIBE` statement and
  fails unless that scenario's table lists exactly those kinds, so a new or renamed description
  cannot be left out.

[Adding A Labelled Entity](#adding-a-labelled-entity) lists what a new owner needs beyond these.

## The Label Map

A label set is one `EntityLabels` value: a map ordered by key, with exactly one value for each
key. Everything that iterates or encodes the map uses that key order.

- **Keys.** A `LabelKey` is a non-empty lower-case ASCII string. Its first character is a letter
  or a digit, and the rest may also be `.`, `_`, `/` or `-`. The key is checked when it is
  constructed and again when it is decoded from JSON or from an archive, and it is never
  normalized.
- **Values.** A value is any UTF-8 string, including the empty string. Nervix keeps it byte for
  byte: no trimming, no case conversion, no Unicode normalization. An empty value is different
  from an absent key.
- **Duplicates.** A statement that names a key twice is refused, even when both values agree. A
  map and an edit are built by inserting each authored entry once, so a later entry never
  overwrites an earlier one.

One edit of an existing set is an `EntityLabelMutation`: a map from key to a change, where a
change either sets the key to a value or removes it. Removing an absent key changes nothing and
is not an error. Because an edit is a map, a key cannot be both set and removed in one edit.

## Limits And The Captured Policy

Four cluster parameters bound every label set. [Entity Labels](./entity-labels.md#keys-values-and-limits)
lists them with their defaults: the number of entries, the bytes of one key, the bytes of one
value, and the combined key and value bytes of the set.

They are [cluster parameters](./node-settings.md), so the whole cluster accepts the same sets.
There is no node-local label option, and no node default can reinterpret a stored set.

The rule for applying them is the same everywhere:

1. A caller captures the limits from one committed parameter record, together with that record's
   revision. The pair is an `EntityLabelPolicy`.
2. The caller validates the **complete resulting set** against that one policy, never the edit
   alone. One edit can therefore remove a key and add another on an entity that is already at the
   count limit.
3. A set that does not fit is refused as a whole. Nothing truncates a set or drops an entry to
   make it fit.

The same check runs at each of these places:

| Where | Checked against |
| --- | --- |
| Planning a Model run in the registry | The committed parameters the run was planned under |
| Planning a resource step, `CREATE RESOURCE` or a resource label edit | The same committed parameters. The replicated state derives the result again from the inputs the plan names before it stores the set, and refuses a step whose result differs |
| Applying a domain or user label edit in the replicated state | The parameters in that same state |
| `ALTER CLUSTER SET` of a label limit | Every committed set of every owner, in the same write that publishes the new policy, so a concurrent label edit and a reduction cannot both be admitted |
| Loading the replicated state from storage | The parameters stored with it |
| Reading a Model record from the registry's store | The four limits recorded in that record, before the Model |
| Reading a backup archive | The policy captured at the cut the archive was taken from |
| Planning a restore | The receiving cluster's limits, for every domain, resource and user it would create |
| Decoding an inspection on a client | The policy the inspection itself carries, and the session's bounds |

Because a reduction is refused while any committed set exceeds it, a committed set is always
valid under the committed policy, and no later change of policy can invalidate metadata that is
already stored.

## The Language

Labels use one clause and two statements, described for users in
[Entity Labels](./entity-labels.md#creation-and-editing).

**The creation clause.** Every creation form of a label owner accepts one optional trailing
`LABELS { 'key' = 'value', ... }`. The clause has one position, at the end of the statement, and
one grammar implementation that every statement family calls as its last step. A statement that
omits it creates an empty set.

**The statements.** `SET LABELS ON <entity> { ... }` applies one edit, and
`SHOW LABELS ON <entity> [FORMAT TEXT | JSON]` reads one set. Both form an addressing family, like
`DROP` and `SHOW CREATE`: the target is written with the entity's established reference, so a
connector is `INGESTOR intake` or `EMITTER archive`, a hash map is `HASH MAP zip_codes`, and a
wire schema keeps its format in its identity. `DOMAIN <name>` and `USER <name>` name their target
in full and work without a selected domain.

The family is not a way to configure an entity. It edits and reads metadata only, and it does not
give a kind a configuration `ALTER` that the kind lacks.

**Text.** Canonical text renders the clause last, with the keys in order and every string through
the shared string renderer, so `SHOW CREATE` of an entity reproduces its labels exactly. An empty
set renders no clause. Completion offers `LABELS` after a complete creation statement and the
composed `SET LABELS ON` and `SHOW LABELS ON` heads, with the reference kinds the shared grammar
knows.

## Creation, Edits And Removal

A label set follows its entity through the entity's own lifecycle.

| Event | What happens to the labels |
| --- | --- |
| `CREATE` with a `LABELS` clause | The set is committed in the same write as the entity. |
| `CREATE` without the clause | The entity starts with an empty set. |
| `CREATE IF NOT EXISTS` on an existing entity | Nothing changes, the labels included. |
| `SET LABELS` | The edit is applied to the current set as one change: every entry applies, or none does. A target that does not exist is an error. |
| A configuration `ALTER` | The labels stay as they are. |
| `DROP`, where the owner has one | The labels go with the entity. |
| `DROP` followed by `CREATE` of the same name | The new entity has only the labels its own statement wrote. |
| A supported rename, such as renaming a placement or `RESTORE DOMAIN ... AS` | The labels stay with the entity. |
| `UPLOAD RESOURCE` and `REBIND RESOURCE` | The logical resource keeps its labels. A version has none. |
| Editing a user's labels | The user's credential is untouched. |

The four owners commit their edits along different paths, because each follows the lifecycle its
entity already had.

### Models

A Model label edit is a domain mutation, like a `CREATE`, an `ALTER` or a `DROP`. The registry
folds it into the candidate set of Models of its run, in written order with the other statements.
The run is validated once, on its final Models, and is committed as one step. An edit can
therefore name a Model created earlier in the same run, and a `DROP` followed by a `CREATE` in one
run ends with the labels of the new Model only.

Commit publishes the Models and the schedule together. The committed schedule carries each
Model's complete configuration, labels included, and it is the copy every node reads a Model's
committed labels from.

Like every model mutation, a Model label edit is refused while its domain is paused by an
alteration or is stopping, because another execution owns the domain's Models then.

### Logical Resources

A resource label edit is a step of the resource catalog. It runs alone or inside the domain's
transaction, where it is applied in statement order with the other steps, including after a
`CREATE RESOURCE` earlier in the same transaction. Planning computes the complete resulting set
and the step carries it. An edit that leaves the set as it was has no effect to report.

### Domains And Users

A domain or a user is an administrative entity. Its label edit is an administrative command with
its own execution record. The replicated state applies the edit and records the command's exact
outcome in the same write. A replay of an earlier request therefore finds its recorded outcome and
changes nothing, so it can never undo a later edit. A domain edit also takes the domain's
mutation lease, so it does not interleave with another change of that domain.

## Transactions

Model and resource label edits belong to the selected domain's `BEGIN`, `COMMIT` and `REVERT`
lifecycle, exactly as the configuration statements beside them. [Control
Plane](./control-plane.md) owns what a transaction may hold.

- Queued edits are part of the transaction's candidate. Only the session that owns the transaction
  sees them. Every other session reads committed labels until the commit applies the edit.
- Conflict checks treat a label edit like any other change of the same entity. A commit preview
  made before another session's label edit is invalid afterwards.
- A domain's own labels are not an input of the plans made for that domain. Editing them
  invalidates no open transaction and no commit preview.
- `REVERT` discards the queued edits with everything else.
- Domain and user label edits are refused inside a transaction before they have any effect. They
  are never run silently outside a transaction that the session asked for.
- `SHOW LABELS` is a read. Sent as a single-statement request inside a transaction, it reads the
  candidate beside the queue. It appends no operation and does not move the transaction's
  position. A request that holds it together with other statements is refused, because a read
  must not become part of a durable command and its replay.

## Inspection And Revisions

`SHOW LABELS` returns one typed value, an `EntityLabelsInspection`, with four parts:

| Part | Content |
| --- | --- |
| Owner | The resolved `EntityLabelOwner`. |
| Labels | The complete map. There is no paging and no partial result: the limits bound the size. |
| View | Either *committed*, with the applied revision of the replicated state the read captured, or *staged*, with the transaction's identity, its position and the planning basis of its candidate. |
| Policy | The revision of the committed parameter record and the four limits, captured in the same read. |

**A committed read is coherent.** The labels, the applied revision and the policy are taken from
the replicated state under one read. A reader can never see the labels of one revision with the
policy of another, and a read that races a `DROP` and a `CREATE` of the same name returns either
the old entity's set or the new one's, never a mixture.

**Any node answers.** A committed read needs no leader. The node that receives it answers from the
state it has applied and reports the revision it read. A node that has not applied an edit yet
answers with the whole previous map, and with the whole new map once it has.

**The revision is the cluster's, not the entity's.** It is the applied position of the replicated
state at the moment of the read. Two reads that report the same revision saw the same state. A
higher revision does not mean that this entity's labels changed.

**A staged read is explicit.** When the session has an attached open transaction and the target
is a Model or a resource, the read plans the queued statements against the committed state and
returns the candidate's labels with a staged view. The view's discriminant says which identity is
present, so a staged candidate is never mistaken for committed state. Domains and users have no
staged view, because their edits never queue: a read of one inside a transaction returns the
committed set.

**Text and JSON are projections.** `FORMAT TEXT` and `FORMAT JSON` are rendered from the typed
value. A client receives the typed value itself in the command outcome and never reconstructs it
from display text.

Descriptions expose the same metadata. `DESCRIBE DOMAIN` and `DESCRIBE RESOURCE` carry the typed
inspection beside their text. Every Model description shows its entity's labels: as a `labels:`
line holding the map as JSON, or, for an inferencer, inside the canonical definition the
description prints. `SHOW CREATE` renders the label clause as part of the canonical statement.

## What A Label-Only Change Leaves Alone

A change that edits only labels must not disturb running work. It changes no execution or state
identity, restarts and relocates nothing, resets no rate-limit allowance, refreshes no resource
binding, reconnects no driver, drops no buffered input and changes no acknowledgement outcome.
Four mechanisms make that true, and none of them is a special case in the data plane.

**The execution configuration.** Every Model has a projection, its *execution configuration*,
which is the Model with an empty label set. Everything that decides how a Model runs reads that
projection:

- the difference between two versions of a Model, which decides pause levels and state resets
- the schema and state fingerprints, the ownership-handoff fingerprint, the source digest of an
  execution revision and the window model digest
- the typed execution plans
- the client endpoint contract a connected producer or consumer is held to
- the checks that decide whether external validation or a UDF build must run again

The projection borrows the Model when its labels are already empty, so it costs nothing where
labels are not used.

**The label aspect.** Comparing two versions of a Model yields a set of change aspects. A
difference in labels is one aspect of its own. It is dynamic, it purges no state, it carries no
runtime update, and it is marked control-plane-only. A change set that holds only such aspects
reaches a node as a newer schedule with nothing to do: no pause, no entity swap, no rebuild. The
digest that identifies what a node executes is computed from execution configurations, so a
schedule that differs only in labels has the digest the node already runs, and the node applies
no change at all.

**Placement.** A run whose every change is label-only does not place the domain again. It takes
the current schedule, replaces the changed Models' definitions in it, and keeps every unit on the
node the schedule already names. This holds even while that node is down or cordoned, so a label
edit needs no ownership handoff and commits while a unit's owner is unavailable. Placement moves
only through configuration changes, relocation, drain and failover.

**No reader in the data plane.** Plans are built from the execution configuration and carry no
labels. No task, branch, relay, connector, metric or log statement reads a label, so there is
nothing to refresh when one changes.

A mixed run, one that changes labels and configuration together, keeps every effect of its
configuration changes, placement included. [Transaction
Quiescence](./transaction-quiescence.md) owns that classification, and [Execution
Plans](./execution-plans.md#incremental-changes-and-rebuilds) owns what each class of change does
on a node.

## Persistence

Labels are stored wherever their entity is stored, in the same record, so there is no separate
label state to snapshot, replay or repair.

- **The replicated state** holds the committed schedule with every Model, the domain records, the
  user records and the resource catalog. Snapshots and log replay carry them as part of that
  state. Loading the state validates every set against the parameters stored with it, and a state
  that holds labels but no committed parameters is refused.
- **The registry's Model store** keeps each Model as one record: a fixed header, the four limits
  the Model was stored under, and the Model. A reader validates the decoded map against the
  limits in that record.
- **Transactions and command executions** keep label edits as typed statements and typed effects,
  never as text.

A change to the label vocabulary or to a record that holds labels is a change of stored shape. It
follows the [one-shape rule](./consensus-storage-and-replication.md): the stored-shape marker and
the archive headers change in the same commit, and a store written in another shape is recreated,
not reinterpreted.

## Backup And Restore

An archive keeps the labels of every entity it includes, exactly, under the archive's existing
scope rules. [Backup And Restore](./backup-and-restore.md) owns the statements and the scope.

- A **domain archive** holds the domain's labels, its Models' labels and its resource catalog's
  labels. It holds no user.
- A **cluster archive** also holds the users, with their labels.
- `WITHOUT STATE` and `WITHOUT RESOURCES` leave the labels in. `WITHOUT RESOURCES` omits resource
  version bytes, not the catalog entries that own the labels.
- Each domain capture records the label policy that was committed at its cut. Reading the archive
  validates the labels of its users, domains, resources and Models against that recorded policy.
- A restore plans before it is admitted, so every refusal happens before it writes anything. The
  plan validates the labels of every domain, resource and user it would create against the
  **receiving** cluster's limits and names the owner whose set does not fit. It also plans each
  domain's Model run as the transaction planner would, which validates the restored Models'
  labels against the same limits. The replicated state checks the owners' labels again when it
  applies each step.
- `RESTORE DOMAIN ... AS` restores the labels under the new domain name.

## Clients

The inspection reaches every client as the same typed value.

| Surface | What it carries |
| --- | --- |
| Session protocol | `CommandOutcome.entity_labels`: the owner as a typed union, the ordered entries, the view as a typed union and the policy. [Entity Label Inspection](./client-session-protocol.md#entity-label-inspection) owns the tables. |
| Rust client | The decoded `EntityLabelsInspection` on the command outcome. |
| Python, Ruby, JVM and C bindings | Typed getters over one retained inspection: the owner, the entries, whether the view is committed or staged, the committed revision or the transaction identity, and the policy. Each [client library guide](./client-libraries.md) has an *Inspect Entity Labels* section. |
| Command line client | The text or JSON projection the statement asked for. |
| Web console | Label rows in every create form, and a label editor that reads the typed inspection and saves one `SET LABELS` statement. [Web Console](./client-tools-web-console.md#entity-labels) describes both. |

A client validates a received inspection against the policy inside it and against its transport's
bounds. A set that does not fit the policy it arrived with is a malformed message, not a label
set to display.

## Failures

Every refusal is typed, and none has a partial effect.

| Failure | Where it is raised | Effect |
| --- | --- | --- |
| A key outside the allowed alphabet | Parsing, and every decoder | The statement or the stored value is refused. |
| A key written twice in one statement | Parsing | The statement is refused. No entry overwrites another. |
| A resulting set over a limit | Planning a Model run, or applying an administrative or resource edit | The whole edit is refused. The refusal names the parameter, the actual amount and the maximum. The committed set is unchanged. |
| A target that does not exist | Planning or applying the edit | The edit is refused. `SET LABELS` never creates an entity. |
| A domain or user edit inside a transaction | Admission | The command is refused before any effect. |
| A Model edit while its domain is paused or stopping | Planning the run | The run is refused, like every model mutation in that state. |
| `SHOW LABELS` in a request with other statements | Admission of the request | The request is refused. The read is sent alone. |
| A limit reduction that a committed set exceeds | Applying `ALTER CLUSTER SET` | The parameter change is refused as a whole. |
| A read of a Model or resource in a session whose attached transaction is not open or belongs to another user | The read | The read fails. It does not fall back to committed state. |
| Stored labels that do not fit the policy stored with them | Loading stored state or an archive | The load is refused. Nothing normalizes or drops the labels. |
| Restored labels over the receiving cluster's limits | Planning the restore | The restore is refused before it writes anything. For a domain, a resource or a user the refusal names the owner. |

A Model label edit that fails takes its whole run with it, like any failing statement of that
run, and writes none of the run's candidate Models. An interrupted commit follows the rules of
every replicated change: the edit is committed, and then every node applies it, or it is not
committed at all. A client that lost its reply recovers the exact outcome by its execution
reference.

## A Facts Contract For Future Authorization

Nervix does not authorize by labels today. No role, grant, policy or permission check reads one,
and this chapter claims no enforcement. Labels were built so that a later authorization design
can use them without changing what they are.

The contract is a set of facts Nervix can state about one request:

- **Who and what.** The authenticated user and the action, which Nervix already knows.
- **The exact target.** The typed owner identity: domain, kind and name for a Model, and the full
  identity of a domain, a resource or a user. A name alone never stands for an entity.
- **A coherent label snapshot.** The target's complete label set with the revision it was read
  at, taken in one read.

With these facts a decision can combine conditions on identity ("this relay") with conditions on
labels ("anything labelled `team` = `payments`"). The party that decides needs no copy of the
Nervix entity inventory: Nervix remains the authority for which entities exist and which labels
they carry, and supplies the facts with the question.

Two responsibilities belong to that future design and are not implemented now:

- **Protecting label changes.** If labels influence decisions, changing a label is a privileged
  action. Today nothing distinguishes a label edit from any other statement a signed-in user may
  run. A future design must authorize label assignment and removal as their own actions. The
  statement family keeps them distinguishable: one statement, one target, and the exact keys set
  and removed.
- **Fencing a decision against change.** A decision made on one label snapshot must not be
  applied after the entity or its labels changed. The revision in every inspection and the
  atomic commit of labels with their entity give a future design something to fence on. Nervix
  does not fence any operation on a label revision today.

No provider interface, adapter or external schema exists, and none is implied.

## Adding A Labelled Entity

A new Model kind gets most of its label support from the structure described under
[Every Model Kind Is Covered](#every-model-kind-is-covered). The change that adds a label owner
of any kind still delivers all of the following together:

1. **The field.** A `labels` field in the entity's own record, with no copy anywhere else.
2. **Creation.** The trailing `LABELS` clause on every creation form, through the shared grammar.
3. **Addressing.** `SET LABELS ON` and `SHOW LABELS ON` with the entity's established reference.
4. **Canonical text.** `SHOW CREATE`, where the entity has one, renders the clause.
5. **Descriptions.** Every `DESCRIBE` of the entity shows its labels, and the description
   scenario in `tests/features/runtime/entity_labels.feature` gains its row. For a Model kind the
   generated check fails until it does.
6. **Limits.** The complete set is validated before commit and at every boundary that decodes
   the record, and a limit reduction counts the new owner's sets.
7. **Execution.** Nothing that decides how the entity runs reads its labels: fingerprints, plans
   and change classification use the execution configuration.
8. **Archives.** A backup that includes the entity includes its labels, and a restore validates
   them.
9. **Clients.** The typed inspection names the new owner on the wire, in the Rust client and in
   every host binding, with their suites and guides.
10. **The console.** The entity's create form carries the label rows, and the label editor can
    target it.

## Guarantees And Limits

- A committed label set is exactly what was written: every key, every value byte, and the
  difference between an empty value and an absent key.
- A label set commits and disappears with its entity. It is never visible without it.
- A label-only change causes no pause, flush, rebuild, restart, relocation or loss, and commits
  while a unit's owner is unavailable.
- An edit applies completely or not at all, and a replayed request never undoes a later edit.
- A committed read returns labels, revision and policy from one state.
- Labels are visible to everyone who can read the entity. They are not a place for secrets.
- Labels grant, restrict, select and place nothing.
- An inspection's revision is the cluster's applied revision. It does not say when the entity's
  labels last changed, and Nervix keeps no history of earlier label sets.
- A staged view exists only for Models and resources, and only for the session that owns the
  open transaction.
- Nervix offers no search for entities by label. A tool that needs one reads the inspections and
  filters them.

## Qualification Evidence

Each form of evidence below covers what it names and nothing more.

| Evidence | What it establishes |
| --- | --- |
| Generated grammar and vocabulary tests over every Model variant | Every kind has the creation clause, both statements, canonical text and completion, and a label-only difference is the control-plane-only aspect for every kind. The description scenario lists every kind the grammar can describe. |
| Bolero properties `entity-label-maps`, `entity-label-mutations`, `entity-label-decoding`, `client-wire-entity-labels` and `console-label-drafts` | Exact round trips of maps and inspections, edits against an independent map after every operation, refusal of invalid and repeated keys by the decoders, and the console's drafts lowering to the edit the operator wrote. [Property Testing And Fuzzing](./property-testing-and-fuzzing.md) holds the register. |
| Shuttle checks over the replicated state's label owner | A read holds one map, one revision and one policy while edits apply. Reads stay coherent across a drop and a recreation. Two user edits each apply once, and a late replay keeps the later edit. A Model edit planned before a drop never reaches the recreated Model. |
| `tests/features/runtime/entity_labels.feature`, on one and three nodes | The public contract: creation, edits, limits, transactions and staged reads, running branches and acknowledgements untouched by a label commit, restart, archive restore, every owner kind, and every description. |
| `tests/features/cluster/entity_label_recovery.feature` | A lost answer and a leadership move around a label edit, each applying the edit once. A follower that catches up by log or by snapshot. Killed processes that restart with the exact labels. An edit while a unit's owner is down. A running rate-limited, partitioned Kafka graph and a WASM processor that keep their consumer, allowance, placement, deduplication and pinned version through label edits. |
| `tests/features/web-console/entity_labels.feature` | The console's create forms and label editor through a real browser. |
| The [label probe](./cluster-chaos.md#the-label-probe) of the external chaos suite | On the packaged image, under process loss and a network partition: every acknowledged edit is in every node's map, all nodes agree, and a domain backup keeps its label scope. |

No Loom model covers labels: the label paths add no atomic protocol of their own, and the read
is taken under the replicated state's lock. No Turmoil scenario is specific to labels: the
simulation covers the interconnect exchanges a label commit uses, and consensus faults are
covered by the cluster scenarios and the chaos probe. Deloxide sees the tracked locks the label
scenarios registered in `tests/deloxide-inventory.toml` reach, and says nothing about the
asynchronous waits beside them.
