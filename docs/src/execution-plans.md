# Execution Plans

*Plexus, the Nervix graph definition language.*

An execution plan is the in-memory boundary between a committed domain schedule and the tasks
that run it. Plexus becomes a semantic Model, registry validation establishes its contracts, and
scheduling commits the graph and its placement. The decision layer then converts the committed
schedule into one complete typed execution revision. The runtime binds node-local resources and
executes that revision. No production data-plane task reads a semantic Model, a scheduled node's
Model configuration, or the active registry graph.

This chapter follows that boundary through construction, installation, changes, and recovery.
[Control Plane](./control-plane.md) owns schedule publication and transaction quiescence;
[Data Plane](./data-plane.md) owns record movement and state. The chapters linked below own the
detailed contracts of their respective engines and integrations.

## Ownership And Persistence

| Layer | Owns | Passes to the next layer |
| --- | --- | --- |
| Language and vocabulary | Structured semantic Models, typed names, branch declarations, resource versions, schedules, and state identities | A Model whose expressions are structured rather than executable text |
| Registry and decisions | Reference, type, sensitivity, capability, branch, and placement validation; pure schedule classification; construction of typed node and domain plans | A complete execution revision and a typed change decision |
| Control plane | Committing models, domain state and schedules through consensus; ordering activation, quiescence, ownership handoff, and local installation | The committed schedule and domain state to the planner; the resulting revision to the runtime |
| Engines and infrastructure | VM lowering and compilation, schema and codec engines, connector contracts, resource storage, WASM preparation, and state storage | Prepared programs or local resources when the runtime binds a plan |
| Data plane | Binding a revision to local capabilities, publishing routing state, starting and refreshing tasks, branch-local execution, and ACK tracking | Arrow batches and external source or emitter operations |

Models, domain lifecycle, and schedules are strongly persisted control-plane state. Execution
revisions and their compiled programs are reconstructed in memory from the committed schedule;
they are never persisted as a second graph definition. Selected node state has its own snapshot,
replication, or checkpoint contract. In-flight batches, payload attempts, ACK guards, tokens,
and maps remain volatile. [Typed States And Validation Boundaries](./typed-states.md) explains why
absence, identity, and conversion failures are decided at their owning boundary.

## From A Schedule To One Revision

The application reads a coherent committed cluster state. Its planning boundary compares the
new cluster schedule with the last schedule successfully applied on this node. For every domain
in the desired schedule, it builds a complete `ExecutionRevision` before asking the runtime to
apply anything. The revision holds:

- the domain and one source digest derived from the complete scheduled domain;
- typed nodes with identity, schema fingerprint, resolved branch contract, placement, ownership
  transition, gate relays, and applicable state generations;
- activation, resource, entrypoint, emitter, processor, and message-error plans from that same
  schedule; and
- the ownership-handoff fingerprint computed from the committed schedule's established bytes.

The source digest identifies which schedule produced the installed revision. A planned change
also carries its predecessor digest, so an incremental application is attempted only when the
currently installed revision is the predecessor it was planned against. The handoff fingerprint
has a separate purpose: ownership preparation and activation must agree on the exact committed
schedule, including its assignments and state generations. Planning preserves its established
encoding and does not substitute the source digest for it.

Registry validation rejects invalid candidate Models before they become an active graph.
Execution-revision construction resolves references and lowers the remaining schedule-specific
contracts before local installation. A missing relay, schema, codec, branch, source, client, or
error destination fails at that planning boundary with the owning node or route identified. The
runtime never repairs a missing decision with a Model lookup or an implicit default. Only a
committed schedule supplies the placement and state authority used for installed execution.

The WebSockets connection plan carries a validated `WebsocketsUpgradeHeaders` collection beside
its resolved client and signaling reference. Validation and planning both preserve a typed refusal
if literal fields violate HTTP bounds or protocol-owned names. Runtime composition passes the
validated collection directly to the connector; it does not read or reinterpret the source Model.

Process startup has an earlier registry path: `RuntimeChanges` carries each recovered active graph
inside the registry and application boundary. Before runtime application, it projects that graph
into an unplaced typed revision; the runtime receives only the revision, never the graph or its
Models. This prepares the local runtime boundary before consensus admission. The subsequently
admitted committed cluster state supplies the schedule, placement, and ownership authority for
normal running or passive execution. The cluster-state listener and local WASM-reset application
both use the same schedule-to-revision planning boundary.

### Typed contents

| Revision part | Decision made before installation | Node-local binding or execution |
| --- | --- | --- |
| Domain activation | Compiled schemas; wire formats and codecs; relay schema, branch retention, capacity, and materialized-state presence; VHOST, endpoint, and signaling references | Load pinned descriptors, compile codecs and signaling protocols, install relay services and endpoint routes |
| Entrypoints | Ingestor source, client, codec, ACK and quiesce contracts; ordered routes, filters, branch construction, and reingestor input edges | Bind VM programs and connector resources; open sources and attach relay consumers only after preparation succeeds |
| Processor specifications | Node topology, exact branch policy, ordered outputs, materialized dependencies, and schedule residue that affects a revision | Bind prepared VM, window, inferencer, and WASM artifacts into a published processor plan; instantiate one task per concrete branch |
| Resources | Pinned lookup file and codec, UDF program, generator source and ordered routes, WASM module file and scheduled assignment | Load the named local version, build lookup indexes, prepare modules, and start branch-local generator or processor work |
| Emitters | Typed emitter and optional connector client or codec, ordered source relays and predicates, construction, request, ordering and envelope expressions, flush, batching, ACK, and error policies | Resolve local mounts and schemas, bind programs, register local or remote consumers, and open the emitter |
| Message errors | Source and optional partial-output schemas, target relay and branch, route flush contract, and ordered `SET` program | Bind once for the installed revision; execute it for failed records using their captured state |

Emitter planning reads the concrete destination's own publishing enum. Broker, MQTT, NATS and
SQS alternatives lower exhaustively into their connector policies; request-only and unconfirmed-only
entities need no mode-admission recheck. Shared retry and acknowledgement-duration validation
borrows the entity-owned policy. The destination's validated external-name type passes through the
plan into connector configuration without normalization. Iceberg resolves its configured storage
client into the S3, GCS or Azure Blob backend while constructing this plan; the runtime receives
that resolved backend and never infers it from the statement or location.

The registry establishes batching legality and direct-mapping key and type contracts from each
entity's vocabulary declaration before planning. A emitter that requires `BATCH` therefore receives
the already-validated batch configuration; planning does not maintain another required-batching
table. Transport-ingestor ACK, batch and retry durations are likewise validated before admission,
including the shared schedulable bound, so an unusable policy cannot reach source installation.

Connector plans preserve the vocabulary's connector/entity nesting. The ingestor planner emits,
for example, `IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(...))`; the emitter planner
emits `EmitterConnectorPlan::Otel(OtelEmitterPlan::{Logs, Traces, Metrics}(...))`. Each entity has
its own typed leaf plan and concrete implementation. Per-connector runtime composition matches
those leaves exhaustively and asserts the implementation's declared kind at compile time. OTEL's
private encoder shares transport and encoding without exposing an entity selector in public
configuration. HTTP and WebSockets endpoint plans remain distinct entities while sharing the
host's intake lifetime mechanism. Labels are derived from declarations and retained at plan/start
time, without an allocation per published batch.

An HTTP polling leaf carries the statement's validated method, literal headers and effective
response size limit together with its cadence and client reference. Runtime composition resolves
the client's mount and node DNS handle once, then passes those typed values to the connector;
polling never reads a configuration Model to recover the request method.

An Iceberg plan carries the statement's validated catalog namespace with its table identity.
Local opening uses exactly those namespace levels, independently of the Nervix domain name.
Its commit-size value has already crossed the same positive whole-byte validation boundary as a
batch-size value; plan construction reads its byte count without interpreting another string.
The plan also carries the optional validated `BATCH` row and byte limits unchanged into the
Iceberg-specific connector starter. The connector owns the Parquet measurement and staged-file
division; the host applies no second Iceberg batching policy.

A Sentry event plan carries the effective platform as a required string. Planning resolves an
omitted `PLATFORM` from the entity's declared default once; composition passes that value into
the connector configuration. Preparation uses it only when the codec omits `platform`. The
runtime neither reads the Sentry Model nor selects its own default.

Kafka source installation also binds the node-owned executor into the connector plan. Partition
inspectors used by scheduling and the host's partition watch use that same node executor. The
connector interprets the effective native client deadline and admits its synchronous driver calls;
the decision layer neither opens a consumer nor selects a worker backend.

RabbitMQ's typed input keeps its complete acknowledgement policy. A parallel maximum is a
`NonZeroU16` at the vocabulary boundary, so installation cannot truncate AMQP's prefetch field.
Composition converts the policy once into either an automatic consumer or a manual consumer with
its exact prefetch count; the connector receives that resolved transport choice without reading
configuration Models. Automatic consumers use the host's refuse-and-count admission policy.

The tables describe distinct decisions in one revision, not independently selectable runtime
configurations. Every node, including one placed elsewhere, is planned from the same domain
schedule. This lets a local owner, its replicas, a remote relay consumer, and a future owner
agree on the same route and state identities.

Expression lowering produces engine-level programs in the decision layer. Node installation
compiles those programs against the exact installed schemas, sensitivity, lookups, and UDFs and
retains the prepared result. Validation may compile an expression earlier to reject an invalid
statement; that validation result is not a persisted executable program. Branch tasks share
their installed processor plan and its prepared artifacts. A newly appearing branch and an
existing branch refreshing between batches take the same published revision. [VM Functions](./vm-functions.md)
owns compilation lifetime, selected-row execution, and VM failure behavior.

## Installation And Publication

Application serializes local installations. It plans against the last successfully applied
schedule, then passes the typed cluster revision, domain lifecycle state, and clock authorities
to the runtime. The runtime records a cluster revision as applied only after its schedule
application succeeds. An unsuccessful attempt leaves that revision unapplied and keeps the same
predecessor for a retry, including after a local WASM reset operation. A node starting after a
forced ending first establishes its committed-state catch-up fence before installing execution;
an earlier owner's local state cannot make it act under an obsolete assignment.

One background supervisor follows both committed schedule and domain changes. If installation fails,
it reports the complete error chain and retries on the admission loop's half-second cadence even
when neither committed value changes. A new schedule or domain update wakes it sooner. Each attempt
reads the current coherent state through the same admission and installation sequence; it does not
replay a captured failed revision or issue another configuration command. Successful installation
waits for the next committed update without periodic application. Shutdown ends a pending retry
wait.

Forced-recovery preparation subscribes to domain installation before checking for its execution,
and takes the runtime's application sequence while selecting that execution. This waits for a
startup installation whose lifecycle is already visible but whose graph is still being built.
The same preparation deadline bounds the wait. A completed installation must match the requested
schedule fingerprint; the sequence is released before transferring checkpoints or restoring guests.

A full running build binds the domain clock, pinned resources, codecs, relay boundaries, state
placements, processor and error plans, connectors, and node tasks from the revision. Its
`DomainExecution` retains the revision and node-local task ownership. The domain routing snapshot
holds the coherent relay services, schemas, branch declarations, materialized-state ownership,
lookups, UDFs, codecs, signaling protocols, and processor plans that record and batch paths need.
Lifecycle code stages changes privately and replaces the published snapshot with one pointer
operation. A reader sees one complete routing snapshot; long-lived tasks cache its handle and
refresh their typed plan identity between batches. The endpoint routing index and other lifecycle
resources are installed at their own cutover points under the application sequence. This is a
node-local publication guarantee, not a distributed atomic transaction across cluster nodes.

The routing snapshot also retains each materialized relay's installed-state publication and the
domain's relay-state epoch. Assignments are registered before these handles are staged; a missing
required publication is an installation invariant failure. A materialized origination capability
moves once into its concrete branch runtime or scheduled relay-state task. That task owns the
mutable selections, while dependency readers, generators and captures retain immutable row views.
Lazy branch installation binds every materialized relay independently within an unchanged routing
epoch; having installed one relay does not suppress another. Eviction withdraws the branch's
materialized record before releasing its writer. Read and replica-installation capabilities remain
independently retainable.

A stopped domain uses a passive build from the same activation and resource decisions. It keeps
schemas, codecs, relay descriptions, materialized-state identities, lookups, bound error routes,
and endpoint definitions available for inspection and recovery, while routing is marked passive
and its graph intake and node execution tasks do not run. Configured server listeners remain
bound on every live node regardless of domain status, leadership, or placement; a stopped domain
does not admit its graph's traffic. Resuming or changing the domain start version builds the
appropriate running revision rather than treating passive state as an active task graph.

Schedule planning assigns endpoint and Syslog sources from live voter membership, independently
of the eligible placement set. A cordoned restore coordinator therefore still serves its local
endpoint intake after `RESUME`, while placed relays and processors execute on eligible nodes.
Removing a voter recomputes listener assignments from the remaining live voters before publication.

WASM ownership preparation follows the installed revision's execution mode. A passive revision
retains and transfers the validated checkpoint inventory without instantiating guests or reading
its stopped clock. Guest restoration is deferred until `START` installs a running revision.

Server endpoint installation replaces that domain's complete set of host-and-path definitions in
the node's immutable intake table. Source starts bind prepared intake lifetimes to the same table;
source ending and domain teardown end those lifetimes before withdrawing them. Other domains'
definitions and intakes survive the replacement. Passive execution keeps endpoint definitions for
inspection but publishes none for request routing. HTTP resolves one route per request and a
WebSocket retains the route and signaling protocol selected at upgrade. The detailed admission and
publication contract remains in [Data-Plane Concurrency](./data-plane-concurrency.md).

The endpoint ingestor plan also lowers request policy once: `NoAck`, or an acknowledged policy
with positive response capacity, physical timeout and retry delay. The decision rejects an
unusable duration, incompatible quiesce policy or missing WebSocket signaling reference. Source
composition creates the intake's window and checks its prepared signaling encoder. Runtime
requests consume that prepared policy and never read configuration Models or parse durations.

[Data-Plane Concurrency](./data-plane-concurrency.md) owns the publication and branch refresh
protocols. [Domain Clock](./domain-clock.md) owns the execution-time capability bound during
installation. [Client Session Protocol](./client-session-protocol.md) owns how a client observes
an installed domain and recovers a command outcome.

## Incremental Changes And Rebuilds

The decision layer compares the previous and desired schedules while both Model-bearing
schedules are still available there. It translates the result into an `ExecutionDelta` that
contains only typed runtime instructions:

| Change | Runtime application |
| --- | --- |
| Unchanged | Keep the installed tasks and state when the predecessor, lifecycle, and start version still match. |
| Dynamic | Apply the typed capacity, processor, WASM reset, emitter flush, or VHOST TLS update; bind and publish the desired processor and error plans. |
| Entity swap | Fence affected input relays, force-flush as required, rebind reassigned placement and selected node tasks, then publish the desired revision and release the gates. Unaffected nodes and their branch state stay in place. |
| Rebuild | Stop the preceding domain execution and install the complete desired running or passive revision. Domain-wide changes, a missing predecessor, and lifecycle changes can require this path. |

The schedule classifier retains Model-based change-aspect and quiescence decisions in the
registry. The runtime receives typed dynamic values, affected node identities, state purges,
and gate relays. It does not reclassify the new schedule. An entity swap that fails locally
falls back to a full rebuild of the desired revision; the actual wider engagement is reported as
a recovery expansion. A failed emitter stop is returned instead: its task is retained with the
emitter's held records, and rebuilding would discard the stop's exact finish outcome. If a fallback
rebuild also fails, application fails and the committed revision remains eligible for retry. A plan
is complete even for a narrow change, so fallback uses the same desired revision rather than
reconstructing one from live tasks.

The swap sequence fences delivery before stopping an owner, prepares new local placement and
programs, replaces relay owner and remote-consumer edges, and publishes the routing snapshot
before waking materialized-state waiters. An owner that still executes locally across a placement
change keeps its task; a moved owner rebinds only its affected runtime and state placement. A
reassignment rebinds the replicated state of every node it names, so a Kafka ingestor kept this way
takes the originator its domain offsets' new binding issued: a change that only replaces a replica
of those offsets never refuses the ingestor's commits. An ingestor instance the schedule stopped
keeps the originator it last held, which every later binding refuses.

The affected relay gates remain closed through publication. An already buffered owner batch takes
a nonwaiting permit before fan-out: it finishes under the prior consumer set if admitted before
the fence, or fails its record ACKs so its source retries under the published consumer set.

A forced ownership change additionally identifies the input relays of its reassigned nodes. Once
their local dispatch gates close, installation retires the preceding channel generation of each
selected relay whose owner or remote-consumer routes change, before waiting for existing dispatch
permits. It leaves the preceding routes published and creates no successor channel until normal
post-swap publication. A dispatch already using a retired channel cancels its unresolved remote
admission; another destination or branch selected during the fence cannot open a fresh delivery
under the preceding routes. Unchanged routing and completed planned handoffs retain their ordinary
drain path.

The decision requires an actual execution-owner change: retained recovery metadata on a later
replica-only assignment change does not retire delivery channels.

This removes the remote admission wait on a paused or partitioned former owner from graph
installation. Admission reconciliation retains its existing five-second budget per interrupted
attempt, independently of the entity gate deadline and the remote admission's longer total budget.
Other local preparation and drain work retain their own bounds. Admission that already won keeps
its record-ACK obligations; an unconfirmed outcome fails those obligations and the acknowledged
source retries using the replacement channel. Recovery can replay records already processed, so
this is not an exactly-once guarantee. Prepared forced-recovery checkpoints still activate under
the desired placement before its tasks start. [Cluster Interconnect](./interconnect.md#cancellation-and-branch-changes)
owns cancellation and its uncertainty; [Data-Plane Concurrency](./data-plane-concurrency.md#dispatch-gate-engagement)
owns the local routing-generation fence.

Dynamic processor revisions reuse unchanged prepared plans, while a changed node gets a new
typed plan identity. [Transaction Quiescence And Impact Inspection](./transaction-quiescence.md)
owns planned versus actual pause scope; [Control Plane](./control-plane.md) owns schedule
publication and relocation.

## Sources, Emitters, And Error Routes

An ingestor start uses the scheduled source class, client configuration, codec, ACK boundary,
and bound ordered routes from its entrypoint plan. A reingestor binds its source predicates and
incoming-branch materialized dependencies before attaching relay inputs. Initial build, entity
swap, reassignment, memory-pressure resume, and Kafka domain-offset placement all use those
typed contracts. Route plans decide whether output is unbranched, preserves an incoming key, or
constructs a new key; the actual branch state remains local to each concrete branch. Transport
headers remain connector-owned and enter a relay only when copied into schema-backed fields.

An input that owns external session state is retired from its previous typed plan after its source
tasks stop. `IngestorStartPlan` decides whether the source changed, independently of codec or route
changes. Runtime installation snapshots locally owned retiring inputs before withdrawing the
installed revision, and the composition root dispatches retirement. Passive revisions retain the
same plans, so removing an ingestor after `STOP` can still release its MQTT sessions. The MQTT
connector compares validated session identities and subscriptions, cleans up obsolete identities
best-effort, and prepares changed sessions at the replacement endpoint before its source starts.
No Model, source driver, or registry guard is retained across this transition. Ordinary shutdown
and ownership transfer with an unchanged input preserve external sessions. For Pulsar, a changed
owned input reaches retirement too: the connector compares the two resolved service endpoints,
qualified topics and subscription names before deciding whether to remove the preceding
subscription. An unchanged subscription is retained even when the replacement declares it
`PROVISIONED`; replacing its routes, codec, mode or instance count does not remove its backlog.

The emitter plan pairs its ordered relay inputs with one typed emitter and its applicable client,
codec, expression, flush, batching, and error contracts. A native `TO CLIENT` emitter has no
external client or codec. Its plan carries the exact output schema, acknowledging sequential or
parallel window, physical ACK timeout and retry policy, maximum rows and IPC bytes, construction
program, and attachment boundary. Registry validation rejects an incompatible schema or missing
batch contract before the plan can run. A flush-only dynamic change keeps that delivery contract;
a contract change replaces the endpoint and ends existing consumers. The host binds mounts and
programs before opening an emitter; reconnects and retries reuse the typed configuration and retained
prepared write. Swaps install consumer edges from the new plan. The connector receives source or
emitter settings and per-record outcomes, never graph placement authority, Models, ACK maps, or
runtime state. [Connector Crates And The Connector Contract](./connector-contract.md) owns each
external transport's header, retry, ACK, and commit behavior. [Client Session Protocol](./client-session-protocol.md)
owns native consumer delivery and settlement.

An error route is keyed by its domain, owning node, optional source route, and destination relay.
The planner resolves the eligible original input, the optional all-nullable partial output,
captured materialized-state shape, destination schema and exact branch, flush policy, and ordered
assignments. Installation binds its program against local capabilities once. A failed record
uses that bound plan with a structured, non-sensitive error reference, code, operation, fields,
and timestamp. A handler failure cannot invoke the same handler recursively; an unavailable
route is reported and does not acknowledge the source record. A route change replaces its bound
plan with the installed revision. Buffered plans retain their prepared delivery queue and exact
worker handle. Binding starts no worker and retires no active route. Successful running publication
activates each prepared worker once, retires unselected/replaced workers and publishes the complete
routing snapshot. Failed and passive builds cannot replace running delivery. Subsequent failed
records send through the bound handle, selecting retirement against bounded queue admission;
accepted deliveries drain through the worker that already owns them. [Errors And Diagnostics](./errors-and-diagnostics.md) owns the
error taxonomy and public reporting contract.

## Placement, Resources, And Recovery

Each typed node carries the scheduled primary, assigned nodes, branch and schema fingerprint,
ownership transition, and the state components it may own. State placement and restoration use
that identity, including a WASM processor's branch generations. Relays have one owner; only
their optional materialized state has scheduler-selected replicas. A configured listener is
different: it runs on every live server node even when its graph node is not the local primary.
Remote relay-consumer edges are derived from the same planned nodes and inputs as local tasks.

Planned handoff prepares state against the exact committed-schedule fingerprint. Reassignment
then activates prepared state under the desired plan, fences the previous owner, and starts or
retains only the tasks that should execute here. A crash or disconnected owner follows the
control plane's failover and state-recovery rules; in-flight batches and ACKs do not survive it.
Restart reconstructs the plan from committed control-plane state and loads only state whose
published identity and generation match the desired placement. A processor task restores its
branches from its own plan: the plan carries the schema a window processor's retained rows are read
under, so the restore never reads the domain's routing snapshot, which installation publishes only
after it starts the tasks. The restore installs every branch its lifecycle names or none, and the
task takes no input until it has; [Restoring Processor Branches](./shutdown.md#restoring-processor-branches)
owns its retry and its failure semantics. A stopped domain reconstructs
passive surfaces; a running or paused domain reconstructs the applicable execution and intake
state. [Shutdown And Recovery](./shutdown.md) and [WASM State And Recovery](./wasm-state.md) own
the detailed handoff, checkpoint, duplicate-window, and restart guarantees.

An archive restore first publishes the restored models as a stopped domain and recomputes its
schedule. Archived node assignments are never installation authority. The coordinator requires
each archived entity's schema fingerprint to match the new schedule and stages its checkpoints on
the scheduled owner and replicas. Branch lifecycle is staged before WASM guest saves, and each
save uses the state generation from the published schedule. Each node atomically publishes its
complete replacement set through a small durable pointer to its verified generation namespace,
including an empty set on unassigned nodes, then clears passive handles. Restart and passive-state
loading select pointer, headers and checkpoint chunks from one database view. Checkpoint jobs
queued for a different namespace cannot cross into the new installation.
The replicated installation gate prevents `START` until all nodes finish. A retry of an unfinished
model step admits a new installation generation bound to its leader tenure, execution and lease;
a delayed preceding attempt cannot mutate the state or its handles after completion.
Materialized restore resolves the scheduled Arrow schema once, verifies bounded archive groups,
and installs the native checkpoint under the scheduled schema fingerprint combined with the
archived domain start generation. Native loading retains one database snapshot while reading
bounded sections. A `RESUME` completion changes the domain to running at that same generation
and mapping only after publication and passive-handle clearing on every target node. The new
running revision therefore binds generators and branch tasks to installed materialized rows.
Each generator task may retain one admitted remote source generation. Every occurrence resolves
its current source owner and describes the complete sealed identity before reusing that view;
owner, placement or envelope changes replace it. Quiescence, paused time, ending intake and read
failures discard the remote view. The cache belongs to the running task and is never an execution
plan, installed state, persisted checkpoint or restore authority.
[Backup And Restore Architecture](./backup-and-restore-architecture.md) owns archive cut modes,
state selection, skipped sections and the restore guarantees, and [Backup And
Restore](./backup-and-restore.md) the operator-visible statements, reports and limits.

Resource plans carry concrete pinned version numbers from the committed Models. Node binding
loads exactly those versions from its local resource store; it never resolves `LATEST` or reads
the catalog during execution. A rebinding is a validated model mutation that produces another
schedule and plan. [Resource Versions And Bindings](./resource-versions.md) owns upload completion,
pinning, rebinding, TLS refresh, and load failures.

## Failures, Guarantees, And Limits

Validation and schedule planning fail before an invalid candidate activates. A typed planning
report identifies the domain and failing part of the revision, with node, route, or reference
detail beneath it. Local binding can still fail when a pinned file is unavailable, a VM program
cannot compile against installed capabilities, a declared route cadence cannot be parsed for its
task, a module cannot prepare, or a connector cannot open. Materialized-state dependencies also
bind to the local relay state at this boundary. A local failure to build or change a domain's
execution is reported as `RuntimeError::BuildDomainExecution`, naming the domain, with the
`ExecutionBuildError` step that failed and that step's own report beneath it, as
[Errors And Diagnostics](./errors-and-diagnostics.md#absence-validation-and-planning) describes.
The runtime records the whole chain as the domain's instantiation error, and an ingestor's start
report as its transient error. It does not mark a failed cluster revision applied. Source and emitter
delivery failures retain their existing ACK,
retry, and message-error classification. Diagnostics identify operations and fields without
including sensitive payload values.

The principal guarantees are one current plan shape, one complete typed revision per committed
domain schedule, no Model interpretation by production data-plane tasks, coherent node-local
routing publication, exact branch and schema identities, and retries of unsuccessful revision
applications. These do not make all cluster nodes switch simultaneously, persist in-flight
messages, guarantee an external source or emitter is available, or turn a connector's delivery
contract into exactly-once end-to-end delivery. Engine-specific resource, batch, expression,
and concurrency limits remain in their owning chapters.

Operators observe the committed schedule, domain lifecycle, applied node state, and typed
diagnostics through the existing control-plane and session surfaces. Runtime metrics and events
report node or connector failures without exposing payloads. A recovery expansion reports when
local entity replacement needed a wider rebuild. The execution plan itself is an internal,
reconstructible artifact, so it is not an independently persisted or user-editable graph.

## Task Dependency Binding

Installation publishes bound message-error plans inside the domain routing snapshot. A failed
record retains that snapshot while executing its prepared route. Entity state identity and
checkpoint primary/executors/replicas publish through one stable assignment slot before reset callbacks;
WASM states retain that slot across schedule replacements. Task startup also binds connector
status, accounting, metric, freeze and domain-clock dependencies. Their runtime registries serve
registration, teardown and observers; recurring operations use the retained capabilities. See
[Data-Plane Concurrency](./data-plane-concurrency.md#retained-task-dependencies).

## Replication Routing

State construction publishes resolved replication routes to the installed state and that existing
assignment slot. Schedule replacement, branch eviction and entity or domain teardown retire those
routes before removing registry entries. Frame handlers read the immutable route publication;
announcers and entity catch-up tasks retain their route or lifecycle and never revisit execution
or identity registries. See [Data-Plane Concurrency](./data-plane-concurrency.md#checkpoint-replication).
