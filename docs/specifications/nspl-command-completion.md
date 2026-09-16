# NSPL command completion

Status: implemented. This document defines the current command-completion contract.

## Required outcome

A successful NSPL command has finished its declared effect. Once its response arrives, the next
command can observe and use that effect immediately, including through a different session or
another live node. The caller needs no delay, readiness request, retry, or intervening `DESCRIBE`.
For example, a successful resource upload can be followed directly by creation of a resource
consumer, and successful creation of a hash map can be followed directly by `LOOKUP`.

"Immediately" describes the state after the response, not the duration of execution. Validation,
durable publication, distribution, initialization, quiescence, and activation may take time. The
command remains pending during that work. `OK` means completion; it never means that the cluster
has merely accepted responsibility for completing the work later.

Statements inside an open transaction have a different declared effect: validation and durable
staging. Their success confirms that effect alone. `COMMIT` has the full completion guarantee and
returns success only when every live node has completed its required part of the transaction.

Execution belongs to the cluster after admission. A session disappearing changes who is waiting
for the answer; it does not undo or cancel the admitted command. Concurrent sessions remain able
to make progress within the conflict rules below.

## Completion and visibility

For a persistent command, success requires all of the following:

1. Every validation applicable to the command has passed against its execution inputs.
2. Its authoritative effects and recovery progress have been durably committed.
3. Every live node has installed the affected authoritative state and the local views needed to

serve subsequent commands correctly.

1. Every affected runtime responsibility has reached the state required by the command. This

includes required file installation, compilation, initialization, routing, source startup,

retirement, and completion of ownership handoff.

1. Any command-owned pause or gate that must be released for the resulting state has been

released. A deliberately stopped domain is complete in its stopped state.

1. The outcome is durably recorded, and subsequent observations on live nodes can report that

outcome and its effects without waiting for command application.

A quorum establishes durability. It is insufficient to establish completion on all live nodes.
Publishing a schedule, announcing a revision, spawning a task, enqueueing a connector request, or
installing bytes on one node is not by itself a completion acknowledgement.

Each command waits for its own effects and dependencies. Unrelated later revisions, resource
replica reports, transaction bookkeeping, or work in another domain cannot move its target
indefinitely. A newer applied revision satisfies the target only when it includes the required
effects; a larger revision number from a different responsibility is insufficient.

Completion applies equally to a stopped domain's configuration. Definitions, resource bindings,
compiled artifacts needed by available operations, and queryable lookups must be usable without
starting ingestion. An empty graph also requires authoritative visibility; it does not require
invented runtime tasks. A successful no-op confirms the existing effect and cannot bypass an
unfinished operation on which it depends.

After success, reads and dependent commands use the completed configuration or a later valid
configuration. A concurrent command may legitimately alter or remove an effect afterward. This
guarantee does not reserve the state against subsequent writes or guarantee continued availability
after a later node, network, or external-system failure. Such failures must be reported honestly;
stale local state must not appear as a successful observation of the completed command.

`SHOW`, `DESCRIBE`, completion suggestions, domain lists, and other observations read coherent
state. They do not materialize missing effects as a prerequisite to producing an otherwise
successful answer. During an overlapping mutation, an observation may describe committed desired
configuration and its applying status. It must distinguish that from active execution. A command
that needs an unfinished dependency receives an explicit dependency-in-progress conflict rather
than a misleading missing-resource, missing-model, or successful-no-op result. The original caller
does not encounter this condition after its successful response.

## Meaning of ready for each command

Every node acknowledges only the responsibilities it actually has. Single-owner processors
remain single-owner; cluster-wide completion does not instantiate them everywhere. All nodes
install the definitions, routing information, and other local state needed for their own role.

| Command or effect | Required state when success is returned |
| ---| --- |
| `CREATE DOMAIN` | The domain and its stopped lifecycle state are available on every live node. Domain selection and a following domain-owned command work immediately. |
| `CREATE USER` | Every live node that authenticates sessions can use the committed credentials. |
| `CREATE RESOURCE` | The domain-owned resource name is registered everywhere. It may correctly have no uploaded versions. |
| `UPLOAD RESOURCE` | The assigned version is durably published; every live node has verified and atomically installed its complete archive and manifest; local resource resolution can use it. Any refresh required by an existing binding's version-selection contract is also complete. |
| Configuration `CREATE`, `ALTER`, and `DROP` | The complete candidate graph has passed validation, the affected definitions and schedule are visible everywhere, required local artifacts are usable, and affected running execution has switched to the result. Dropped execution and routes have retired. Unchanged execution retains its state. |
| Hash map creation or alteration of its dependencies | Required resource records have decoded and the selected lookup is installed wherever it is queryable or executable. A direct query works even while the domain is stopped. File existence alone is insufficient. |
| VHOST, endpoint, and listener changes | Routing and TLS configuration required by the change are installed on every live listener node. Active listener bindings are usable; retired bindings cannot admit new work. A stopped domain still enforces its stopped admission behavior. |
| `START` | The committed clock generation and authority are installed, processing paths and required state replicas are prepared, and every scheduled source and listener has reached its startup boundary. Paced execution does not wait for a future tick or a first payload. |
| `STOP` | All nodes observe stopped state, clock authority is revoked as required, and the domain's execution and intake have stopped according to the existing stop policy. Remaining shared server listeners enforce that policy. Success cannot precede remote stopping. |
| Placement changes and `RELOCATE` | The resulting ownership, state transfer, routes, and destination execution are active; former owners are fenced or retired; command-owned handoff gates are released. |
| `CORDON NODE` and `UNCORDON NODE` | Every relevant observer and scheduling decision uses the committed eligibility. Existing valid owners are preserved by the placement policy; a cordon is not a drain. |
| `DRAIN NODE` | The node is cordoned, all required moves have completed, and no work that the drain contract moves remains assigned to it. Cluster-wide listeners retain their existing node-presence contract. A partial drain returns failure with completed and pending moves identified. |
| `DROP NODE` | Membership removal and resulting schedules have been applied by the remaining live nodes. Work affected by removal has completed its required activation or reports failure. Existing quorum and live-node-removal restrictions still apply. |
| `CREATE SUBSCRIPTION` | Validation, the session receiver, filtering, and owner-visible subscription interest are installed, so a subsequent admitted batch can reach the subscription. Data delivery remains asynchronous. |
| `DELETE SUBSCRIPTION` | The session subscription is stopped and cannot enqueue further deliveries. Already queued events retain the existing delivery semantics. Other subscriptions sharing relay interest continue normally. |
| `USE`, read-only commands, `BEGIN`, staged statements, and `REVERT` | The specific selection, observation, transaction creation, validated append, or discard is complete. These operations do not trigger unrelated runtime activation. |

Resource distribution and use are distinct responsibilities. Upload verifies the resource as a
directory archive. A consuming command validates and initializes its codec, lookup, TLS bundle,
inference model, or guest. Upload does not compile every file as every possible consumer. Explicit
versions remain explicit, and omitted-version selection follows the consuming model's contract;
this change introduces no new automatic reload behavior.

Connector startup is complete at its actual protocol boundary. A subscribed source must have
completed the required connection, authentication, subscription acknowledgement, and assignment
steps that make it able to receive data. MQTT requires successful subscription establishment or
confirmed persistent-session resumption; an outbound WebSocket with signaling requires the
declared transition to accepting data. Binding a passive UDP listener completes its local startup
without waiting for a sender. Polling sources must have valid initialized execution and their
cadence armed; completion does not wait for a sample or a future poll.

An unused named client remains configuration only. A used database pool honors its declared
minimum and maximum; a zero minimum does not create a requirement to fill the pool. Initialization
errors belong to the initiating command. Errors from a later payload operation retain their
connector error and retry semantics. Nervix does not create external topics, queues, tables,
buckets, or other external entities to make activation succeed.

Readiness does not require a first record, a new branch, a nonempty materialized relay, a generator
output, a completed flush window, or delivery of an emitted record. Future branch-local work and
`REQUIRED WAIT` remain demand-driven. Required restoration or transfer of existing state is part
of activation; waiting for new streaming data to populate that state is not. Existing data-plane
acknowledgement, delivery, sampling, and backpressure contracts continue to govern that work.

## Live nodes, membership changes, and recovery

The completion set contains every authenticated live node in the cluster's current topology,
including followers, nonowners, cordoned nodes, and live learners. It is not limited to voters,
nodes selected for placement, nodes that answered a request, or nodes that already report ready.

The coordinator evaluates this set against the current topology and availability decision before
recording completion. A node joining before that decision is included. A removed node, or a node
declared unavailable by the existing availability policy, ceases to be required only once that
decision and any resulting ownership changes are effective. A slow transfer, unknown health,
probe-capacity exhaustion, or one timed-out readiness request does not remove a live node. Loss of
quorum cannot be bypassed by shrinking the completion set, and an empty set cannot certify success.

Each acknowledgement identifies the current node incarnation, the affected responsibility and
effect generation, and the applicable topology or ownership context. An acknowledgement from a
previous process with the same node name, from a previous assignment, or for different resource
content cannot satisfy a current operation. Resource readiness includes the expected content
digest. Restart invalidates process-local readiness even when files or control-plane records
survive on disk. Failure reports are matched to the same current obligations; an obsolete
assignment's error cannot fail a replacement that has completed successfully.

A node joining or rejoining after completion installs completed authoritative state and the local
effects needed for a service before admitting requests to that service. It may expose health and
recovery diagnostics during catch-up, but it cannot serve stale successful NSPL observations or
accept data through an incompletely installed route. A peer excluded as unavailable must be fenced
from stale execution and must pass this same admission rule on return. Configured server listeners
remain present across cluster events; recovery gates their affected service when necessary.

Node health and an individual command's applying state are separate. An unrelated slow operation
must not mark the entire node unavailable or prevent healthy domains and administrative reads from
being served. Service admission during recovery is scoped to the state that service requires.

Preparation and full completion are separate facts. Nodes may need to confirm that receivers,
state, and clocks are installed before producers start. This coordination must finish before the
final startup acknowledgements; it must not create a cycle in which every node waits for another
node to declare complete before either can start. Preparation alone never satisfies the command
completion condition.

Leader changes preserve admitted work and durable progress. A replacement leader verifies the
current incarnations and obligations, resumes unfinished application, and obtains completion
evidence before returning success. Background reconciliation uses the same completion conditions
as execution requested by a connected client.

## Execution ownership and outcomes

Persistent commands have an internal applying lifecycle separate from their session and from
durable publication. The cluster retains the semantic operation, execution identity, authoritative
effect boundaries, and progress needed to finish admitted work. Runtime payloads, buffered records,
data-plane acknowledgements, and suspended messages remain in memory under their existing rules.
Recovery preserves existing secret-handling rules: user creation retains the derived credential,
and progress inspection never exposes passwords or sensitive configuration or resource contents.

| Execution condition | Command behavior |
| ---| --- |
| Validation or admission rejected | Return a failure identifying the rejected operation. No effects of that rejected command are applied. |
| Admitted and applying | Keep the command response pending while validation obligations, durable effects, and activation complete. Progress and diagnostic observations may report applying. |
| Completed successfully | Record the completed outcome and return success with the command's normal useful output. |
| Definitive execution failure | Record and return failure, including whether no effects, some effects, or all authoritative effects were committed and what remains unusable. |
| Transport lost or caller stops waiting | Detach the waiter. Execution continues and retains its eventual outcome. |

The success response is produced by the command's completion decision. An asynchronous error event
is supplementary reporting and cannot substitute for a failed command result. Resource install,
runtime initialization, TLS activation, handoff, and pause-release failures all participate in
that decision.

Before an authoritative effect is committed, failed preparation releases temporary resources and
any reversible command-owned holds. After an effect is committed, failure does not imply rollback.
The result identifies committed effects and the actual remaining state. Any compensation required
by the existing operation contract must itself complete before the command claims restoration.
Session loss never initiates compensation. A failed effect stays safely gated until it can be
reconciled or explicitly corrected; unrelated execution continues.

Recording a terminal failure releases that command's mutation admission ownership. Any safety
gate still needed belongs to the failed effect and remains visible in its diagnostics. Corrective
commands can acquire mutation ownership and replace or retire that effect; a failed command cannot
retain an invisible lock that prevents its own repair.

A definitive failure is a terminal outcome. Later repair of desired state does not rewrite that
outcome into success. Transient retry or reconciliation while an admitted command can still
complete leaves it applying. An interrupted application resumes from durable effect boundaries;
it does not repeat completed mutations, allocate another upload version, or establish a second
domain-clock generation for the same start operation.
Resolved choices whose semantics are fixed by execution, including the committed start mapping
and resource-version selection, remain bound to that operation. Recovery can adapt ownership to a
changed membership without replaying the semantic mutation or recomputing its committed choices.

Invalid configuration, missing required external entities discovered during initialization,
content-digest mismatches, exceeded per-resource limits, definitively rejected credentials, and
invalid TLS or code artifacts are definitive failures. Temporary transport interruptions and
leadership loss are retryable. Exhausting a temporary execution budget is distinct from a resource
violating its declared limit. Quiescence and drain deadlines retain their operation-specific
failure semantics and must report any committed prefix or unsuccessful restoration.

Every persistent request has a stable execution reference before submission. Transaction and
upload operations retain their existing owning identities. Each transaction append additionally
has a request identity and expected position; correcting a rejected append uses a new request
identity at the unchanged position. Ordinary persistent commands require the equivalent execution
identity. The reference is bound to the authenticated owner, domain where applicable, and semantic
request. Reuse for different content fails. Duplicate submission of the same request joins the
existing execution or retrieves its retained result.

The session and upload interfaces support resuming and inspecting an execution by its reference.
Inspection reports applying progress or the retained terminal result; it does not return an
execution-success acknowledgement for unfinished work. Existing transaction attach and upload
retry serve this purpose for their respective operations. This adds no new NSPL statement syntax.
Clients must match the outstanding operation, not infer its completion from an unrelated change
in a transaction's state or count.

Ordinary command outcomes are retained for 15 minutes after completion. Transaction outcomes keep
their configured tombstone retention, and upload identity records retain their existing resource
lifetime. Applying operations do not expire. After an outcome has expired, a resume request
reports that the outcome is unavailable and does not submit a new command. Clients never convert
an uncertain outcome into a fresh execution identity automatically. A retained success describes
the original completion; later availability changes are handled by service admission and normal
error reporting.

Execution and waiting consume bounded administrative execution and memory budgets. Capacity
exhaustion rejects new admission explicitly; it does not discard admitted work or invent success.
Durable control-plane progress, rather than retained per-request payload copies, supports recovery.

### Timeouts and disconnection

There is no implicit command-wide deadline that converts incomplete application into a successful
response. A connected caller without its own deadline keeps waiting through transient application
delays. Individual network, connection, quiescence, drain, and startup attempts retain physical
timeouts and their semantic error classification. A retryable attempt timeout leaves the command
applying; a definitive operation failure returns a failure. Domain pacing never scales these waits.

A caller-supplied deadline or cancellation stops that caller's wait. It produces a timeout or
transport outcome with the execution reference and known disposition, not a terminal assertion
that the command failed or was reverted. The client can resume waiting for the same operation.
Where admission itself is uncertain, the result states that uncertainty rather than asserting
that no effect occurred.

For uploads, the cluster cannot continue bytes the client never supplied. An interrupted incomplete
body is not an admitted complete upload. The client can retry that upload identity and its bytes.
Once the complete verified input is durably owned by the cluster and admitted for publication and
distribution, disconnecting the upload request does not cancel installation or distribution. A
recovering leader uses available durable content; it must report missing content honestly if a
failure makes that input unavailable.

Subscriptions retain their session-local lifetime and stop when their session ends. Existing clean
close behavior for an idle `OPEN` transaction is retained. A disconnect during an admitted
persistent command, transaction append, or `COMMIT` does not revert that operation; cleanup must
distinguish this case from closing an idle open transaction.

## Transactions

`BEGIN` continues to require one existing selected domain. Queueable statements and prohibited
transaction content remain as documented: model mutations, domain configuration and lifecycle,
and resource registration are eligible; uploads, observations, subscriptions, domain and user
creation, and node administration remain outside transactions.

Each staged statement validates against the ordered candidate produced by the existing prefix.
Success means its validated semantic content is durably appended and available to the next staged
statement and transaction-aware completion. No live graph replacement, resource publication,
listener activation, source startup, lifecycle transition, or quiescence occurs during staging.
Validation may inspect existing resources and compile and test code without installing it into
active execution. Rejected statements leave the prefix and pending count unchanged.

Statement-local validity is mandatory at admission. Cross-model completeness may remain
provisional while one consecutive atomic model-mutation run is being assembled, so coordinated
schema and codec changes remain expressible. A non-model statement closes that run; a later run
cannot repair an invalid earlier execution boundary. `COMMIT` validates every complete execution
boundary against current authoritative inputs, rather than trusting the queue-time snapshot.

On commit, each consecutive model-mutation run retains its atomic candidate-graph validation and
authoritative publication. Other eligible statements remain individual ordered steps. Each step
must pass its applicable validations before effects and complete its required application before
the next step executes. A sequence containing `STOP` followed by `START` therefore completes the
stop before executing the start. The transaction as a whole does not become an all-or-nothing
data-plane operation; failures can preserve a previously completed prefix.

Durable effect progress and completed application progress are distinct. The transaction stays
`COMMITTING` while any step is applying, including after the last authoritative effect has been
committed. `COMMITTED` is recorded only after all step application, final lifecycle state, and
command-owned gate release have completed across the live-node set. Read-only observation of
`COMMITTED`, as well as the `COMMIT` response, carries this meaning.

Recovery resumes application of an already committed step before advancing to a subsequent step.
A definitive failure records the affected statement or atomic run, the error, and the committed
prefix. Required recovery of a command-owned pause remains part of execution, not an error event
that may arrive after successful completion.

Attaching to an in-progress commit identifies that commit and resumes waiting for its terminal
result when the outstanding request is `COMMIT`. A successful attach itself does not satisfy the
commit request. Repeating `COMMIT` for the same execution joins it; it does not restart its effects.
Owner authentication, transaction takeover, retained outcomes, and leader routing remain enforced.

Successful `COMMIT` output retains the aggregate actually executed quiesce level and existing
relocation summary. It does not repeat queued statement outputs. Queue-time quiesce output remains
predictive validation output. An empty commit completes transaction bookkeeping without forcing
unrelated runtime activation.

## Concurrency and service responsiveness

There is no cluster-wide command lock, global commit queue, or global runtime-completion wait.
Independent commands and commits in different domains can validate and apply concurrently.
An upload's bulk distribution must not occupy the progress path for unrelated administrative work.

Conflicting graph, lifecycle, and placement operations in one domain are ordered by ownership of
that domain's mutation. A conflicting new request is rejected with an explicit in-progress
conflict, preserving the current rejection behavior. A committing transaction retains that
domain ownership through its ordered steps and required recovery. Leadership changes preserve
effective exclusion; a stale leader cannot continue to publish a conflicting operation.

This ownership does not lock every session or every resource in the domain. Reads, diagnostics,
independent resource distribution, and validation of other open transactions can proceed against
the applicable coherent state. Their later commit revalidates current inputs. Operations that
span membership or several domains exclude only actual conflicting changes for their duration;
normal consensus ordering alone does not justify holding an unrelated session until activation.

Within one session, commands preserve submitted order. A later command cannot overtake a pending
command and observe its partial effects. While execution waits, the transport must continue
handling keepalives, disconnects, bounded progress, and subscription delivery. A blocking
subscription must not deadlock a drain by requiring the same transport loop that is waiting for
the drain result. Command responses and management traffic must retain bounded progress under
subscription and bulk load.

## Public interface

Resource upload success reports the assigned version and upload identity after every live node has
installed and verified the same digest. Resource descriptions expose identity, checksum, contents,
publication metadata, and per-incarnation installation or failure diagnostics. Upload itself is the
completion operation; callers do not perform a second readiness operation.

Command responses, the Rust client, the CLI, and browser upload and session flows all await the same
terminal result. Progress displays may show validation, transfer, and application while pending.
They report success and advance a sequential script only after the final result. Success wording
describes a completed action, including a started domain and an uploaded version.

Existing data-plane HTTP acceptance responses, asynchronous subscription events, per-message errors,
and connector delivery acknowledgements retain their own contracts. They are separate from NSPL
administrative-command completion.

Persisted and protocol data has one current shape. Incompatible or incomplete stored state fails
clearly and must be recreated.

## Acceptance criteria

Scenarios that are not inherently topology-specific cover one-node and three-node clusters.
Three-node runtime cases use the repository's randomized stable placement unless production
placement, drain, or failover is the behavior under test. Exercise the public session protocol and
client interfaces, with browser-specific behavior covered through the normal browser suite.

| Scenario | Required evidence |
| ---| --- |
| Upload completes before immediate follower use | Delay one live replica using a controlled installation gate. The upload remains pending until that replica installs. After success, create a resource consumer and use it immediately through each node, with no inspection command between actions. |
| Upload failures reach the caller | A remote checksum, quota, installation, or required binding-refresh failure produces a failed upload result with its assigned version and disposition when applicable. |
| Resource identity survives interrupted waiting | Disconnect after full admission, then resume the same identity through another leader. Observe one assigned version and the correct terminal result after distribution. A changed digest fails. |
| Incomplete uploads do not invent content | Interrupt before the declared complete body. No completed upload is reported; retrying the identity with the complete body can finish. |
| Definitions are immediately observable | Create a domain, user, or resource and immediately select, authenticate, or describe through another node. Test a stopped domain and an empty graph. |
| Lookup initialization is part of creation | After successful hash map creation, immediately query a known key from each node while the domain is stopped. Malformed resource records fail the creating command. |
| Runtime creation and alteration complete | Immediately use newly created or altered routes after success. Exercise dynamic changes, entity pause, domain pause, and existing concrete branches while preserving unaffected state. |
| Runtime removal completes | After successful `DROP`, a new observation and a new attempted use see the removal on every relevant node without a cleanup delay. |
| START waits for source readiness | Hold a remote source before its subscription acknowledgement or listener activation. Once released and `START` succeeds, immediately send through the external source or every configured listener. |
| STOP waits for remote stopping | Hold remote teardown. Release it and require immediate stopped observation and stopped intake following success, including when this is the last running domain. |
| TLS activation is complete | Immediately establish a new TLS connection through each affected listener after the changing command succeeds; a failed required certificate installation fails that command. |
| Transaction staging only validates | While a transaction is open, another session observes the unchanged live configuration. Ordered preflight simulates lifecycle, resource, placement, and atomic model-run steps against one captured planning snapshot. An invalid operation leaves the staged prefix unchanged; only an unfinished final model run may remain provisionally incomplete. |
| COMMIT waits beyond durable progress | Hold activation after the last authoritative step is committed. Observe `COMMITTING`, release the hold, and immediately use the resulting graph after success. Repeat for a model run, `START`, `STOP`, and pause release. |
| COMMIT failures remain failures | Inject a remote activation or release failure after durable effects. The initiating and reconnecting clients receive the same failed outcome and committed-prefix information. |
| Recovery resumes application | Change the leader after a step is committed but before activation completes. The successor completes that step without repeating its effects, then advances. A reconnecting `COMMIT` waiter waits for the terminal result. |
| Disconnection does not revert execution | End both session transports during standalone application and commit. Independent observations later show the completed effect; resuming the execution reports its outcome. Cover admitted staging separately from idle open-transaction cleanup. |
| Caller deadlines only detach | Expire a caller deadline while a controlled application gate is held. Release the gate and resume the same reference to obtain completion. |
| Readiness belongs to an incarnation | Restart a pending or previously prepared node under the same name. Completion requires the new process's installation and activation, including digest verification for resources. |
| Membership changes preserve the guarantee | Join a node during application, lose a node under the actual availability policy, and rejoin after success. Completion uses the effective live set; returning service cannot expose stale state. |
| Independent operations make progress | Hold an upload or commit applying in one domain. Reads, health, subscriptions, and a commit in another domain complete while the hold remains. Same-domain conflicting execution receives the specified conflict. |
| Later unrelated revisions do not move the target | Continuously update an unrelated domain and resource reports while one command finishes. Its completion depends only on its required effects. |
| Transport stays active during a long command | Keep a session command pending while exercising keepalives and blocking subscription delivery. Control responses and drain work make progress without a transport-induced deadlock. |
| Node administration completes | Immediately observe eligibility after cordon changes, destination operation after relocation or drain, and effective remaining membership and ownership after node removal. |
| No-op and session commands complete their own effect | Immediately use an existing valid object after a no-op, publish after subscription creation, and recreate a subscription after deletion under the current session delivery contract. |
| Streaming remains asynchronous | Start an empty branched graph with generators, windows, and `REQUIRED WAIT`; command completion does not require inventing branches or data. Existing cadence, delivery, and error-policy scenarios continue to pass. |

Hold-and-release scenarios use explicit progress gates and bounded waits for named events. They
must prove response ordering without relying on a short silence window or scheduler timing. The
first dependent action after success is executed once. A test helper must not insert hidden
readiness polling or materialization to make it pass.

Dedicated `DESCRIBE RESOURCE` scenarios assert current metadata and per-node diagnostics directly.
Resource descriptions used only as synchronization after uploads are removed from consumer setup.
The same audit covers readiness waits after model changes and `START`, including helper-level
source-subscription waits. Condition-based waits remain appropriate for actual streaming output,
branch appearance, failover detection, and node recovery initiated after an already completed
command. Assertions for eliminated output and protocol forms are deleted rather than replaced
with historical-shape rejection tests.
