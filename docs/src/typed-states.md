# Typed States And Validation Boundaries

*Plexus, the Nervix graph definition language.*

Client configuration declarations distinguish required, optional, defaulted and address-scheme-dependent keys, Nervix and driver
ownership, typed value rules, TLS certificate/identity file roles and delegated property namespaces.
`All` is an explicit driver namespace, not an empty-prefix sentinel. The vocabulary schema owns
key presence, duplicates, type checks and TLS pairing; rendered mount values cross that same
boundary again before storage. A driver's other names for one property are declared pairs, each
naming the one key the schema accepts, and a prefix the driver strips is declared with the
driver's own properties under it. An empty table and an absent prefix mean the driver reads every
property under one name. The schema refuses a declared other name and a key under the prefix with
`DriverAlias` and `DriverPrefix` before any declared or delegated key is read, so exact-key
duplicate detection also covers the driver's names. Driver-specific validation stays in connector engines called by
the composition root. Confidential inspection is an explicit projection of complete entries;
stored configuration and archive serialization do not consume that projection.
Defaulted keys carry their declared value in their presence variant. The effective-entry projection
adds it only when the authored key is absent; an invalid explicit value still fails validation.
`DESCRIBE CLIENT` distinguishes these effective defaults from authored values, with the same
confidential projection, while canonical configuration preserves omission.
Scheme-dependent presence distinguishes a key required by an address scheme from an optional key
permitted only with that scheme. OTEL TLS file keys use the latter state for HTTPS, and RabbitMQ
TLS file keys use it for `amqps`. The same vocabulary validation runs on authored and rendered
entries, deferring an unresolved address template until rendering. Percent-encoded header pairs
retain their encoded client configuration;
validation and the connector use one decoder into ordered named pairs, without feeding a decoded
value back through that decoder. OTEL's field rules own exact types and literal value constraints;
the registry establishes them before planning, and the connector verifies the prepared schema.
ClickHouse's rendered CONFIG validation requires a positive request deadline and HTTPS trust or
refuses TLS file keys on HTTP. The case-preserving `ClickHouseTableName` is constructed only from
one or two nonempty locator parts; the connector receives that validated name and quotes its parts
for SQL without reparsing a Plexus statement.

A direct-value expression's inference refusal carries its declared section and destination key
separately from the VM's typed compile cause. The registry associates that cause's operation span
with the lowered mapping before reporting the refusal; a null-only mapping is refused at admission.
[Errors And Diagnostics](errors-and-diagnostics.md#registry-validation-and-planning) owns the report.

The MongoDB engine resolves a required nonempty database from its explicit key or the URL path
into one private configuration value. It consumes the driver's optional default rather than
carrying a second database identity. Collection names are validated vocabulary values that retain
their original UTF-8 spelling; the resolved database, separator and collection cross the complete
namespace-budget check before the emitter opens. Missing database configuration and a refused
namespace return typed errors, never an empty database or a substituted collection.
Its BSON field-name predicate rejects NUL at admission. Its private TLS plan distinguishes native
driver TLS options from a complete split-file identity awaiting preparation; a partial identity
is a configuration error. Preparation returns native options together with their private combined
PEM guard. One shared client owner retains both driver and guard until its last handle ends.

Nervix represents absence and distinct semantic states in its types. A zero, empty string,
empty collection, maximum integer, or all-zero digest is a value in its own right; it does not
stand for a missing value or a different state. The owner of each state decides when a value is
required, validates it there, and passes the resulting typed value to the next layer. This keeps
the registry, execution plans, runtime, clients, and diagnostics from assigning different meanings
to the same raw value.

A backup cut's active phase is a typed state. Its completed phase measurements are an ordered
sequence of typed phases and durations; an empty sequence means the first phase is still active.
These task-owned measurements are volatile diagnostics and do not enter stored configuration,
archive records or the interconnect's terminal capture reply. [Backup And Restore Architecture](backup-and-restore-architecture.md#observability)
owns their reporting and attribution limits.

This chapter describes the landed typed-state architecture across those boundaries. The
[Cluster Interconnect](./interconnect.md) chapter owns node-to-node wire forms, membership, relay
delivery, and transport failures. [Domain Clock](./domain-clock.md) owns clock installation,
authority, and time bounds. [Shutdown And Recovery](./shutdown.md) owns drain, handoff, and restart
semantics. The state rules here apply to those systems without replacing their detailed contracts.

[Execution Plans](./execution-plans.md) follows the validated values into a complete installed
revision.

Opening a WebSockets ingest metadata builder validates the frame-label byte capacity derived from
the group's row bound and the vocabulary's frame labels. An estimate that exceeds the address space
is a typed error before allocation; the ingest group adds the opening operation's context and
retains the cause for its general-error policy.

A paced poll message carries a typed metadata variant. HTTP polling supplies its response status
and scheduled poll instant in that variant, while sources with no integration fields carry the
headers-only variant. The host copies the variant into a matching quiesce buffer and Arrow builder;
neither a missing status nor a default timestamp can be mistaken for a valid HTTP response.

The Pulsar emitter's running publishing choice keeps related state together: the `NO ACK` variant
owns its pending receipt futures and an optional first driver failure; the `ACK` variant owns its
confirmation settings. An empty receipt collection means there are no outstanding receipts,
and an absent failure means none has been observed. These are task-owned execution values,
never persisted state or substitutes for a missing publishing mode. The
[connector contract](./connector-contract.md#finishing-an-emitter) owns their completion semantics.

The Kafka emitter installs its effective native queue byte capacity as `NonZeroU64` after librdkafka
validates the client configuration. Its confirmation window's on-demand settlement returns
`Some(EmitterRecordId)` for an answered record and `None` for an empty window. A refused enqueue
keeps the same record until admission succeeds or its wait fails. The optional admission deadline
is absent while outstanding confirmation answers can provide progress, and is established only
when a full native queue has no current-window answer left to await. These values are private,
volatile execution state; the [connector contract](./connector-contract.md) owns their bounds.

A forced-recovery destination that has not installed its execution schedule yet retains that
absence as an installation wait, rather than inferring that its checkpoints are missing. The
installation publication and application sequence establish the execution before its fingerprint
and node are validated. Expiry carries the typed domain and node in an installation deadline
context; an installed identity mismatch remains a validation failure. The detailed recovery
outcomes stay in [Shutdown And Recovery](./shutdown.md#when-the-former-owner-is-gone).

## A State Has One Owner

Settings use the same construction boundary. A `SettingDefinition` declares its sealed class,
semantic value, name, one default, optional derivation and validation against typed context.
`Setting<D>` validates explicit and default values before construction, retains the complete
typed validation report on failure, and provides no mutable access around that boundary.
`Prerequisites`, `ClusterParameters` and `NodeCapabilities` classify the fields; omitting the
class is a compile-time error. [Node Settings](node-settings.md) owns their operational meaning,
carriers, defaults, change procedures and source ratchet.

Redis subscription plans carry a nonzero bounded message capacity and a validated idle deadline.
The source's absent subscription means it needs a resume. An installed subscription owns its
bounded receiver and exact reader task; close and suspend join that task before replacing it.
Queued messages precede its terminal read failure. These handles, queued payloads and parsing
state remain volatile and are never part of a persisted Model.

Running MQTT acknowledgement state distinguishes automatic acknowledgement from manual
acknowledgement with its bounded QoS 0 capacity. Each queued QoS 0 outcome owns its permit until
the source takes it; acknowledged packets use their separate reserved capacity. Overflow is an
ordinary admission outcome that drops and counts one publish, rather than a read failure or a
missing connection. The queue, permits and host-supplied counter handle remain volatile.

Text is parsed into a semantic Model, registry validation resolves its references and contracts,
and planning produces an execution plan. Each stage passes forward a value whose type expresses
what has already been established. Runtime code executes the plan; it does not reinterpret a Model
or infer a state from a display label. Shared identities live in the vocabulary so every layer
uses the same meaning for a domain, branch, node, timestamp, or schema fingerprint.

### Source-owned identity and installation contracts

Gate these reviewable contracts with `cfg_attr(nervix_lint, ...)`:

```rust,ignore
#[cfg_attr(nervix_lint, nervix::identity(node, reason = "destination cluster member"))]
destination: ClusterNodeName,

#[cfg_attr(nervix_lint, nervix::state(installed, reason = "constructed before branch admission"))]
branches: OnceLock<NamedBranches>,
```

`identity(node, ...)` marks shared vocabulary identity types, and declares an identity requirement
on an exact field, parameter, type alias or function return. The compiler resolves aliases and
normalizes types. References, arrays/slices, Option, Vec, Box and shared Arc wrappers carry their
typed identity; Result checks its success type. A tuple or unrelated container containing a typed
node does not make another string member an identity. Identity types must belong to the vocabulary.
The current vocabulary includes cluster node names, process-incarnation identities, graph node
references and domain node references. Ordinary labels and external pass-through strings carry no
identity requirement merely because their identifier contains `node`.

`state(installed, ...)` describes a field's installation lifetime and forbids a resolved RwLock
anywhere in its normalized representation, including aliases and outer wrappers. Immutable
construction, one-time initialization or an appropriate publication primitive preserves the owning
lifetime. A field containing `RwLock<Option<name or shared handle>>` without a state contract emits
`nervix::state_contract` for review. Its type cannot establish whether the value is installed once
or replaced repeatedly. A genuinely replaceable field declares `state(mutable, ...)` with the
replacement's actual owner and reason. The annotation proves neither a runtime lifetime nor a
publication ordering; affected runtime corrections still need their owner tests and models.

These are ordinary compiler diagnostics in the complete architecture gate, without numeric
allowances or a filename-based identity policy. The [developer guide](./developing-nervix.md#compiler-architecture-gate)
describes qualification and evidence.

The representation depends on the question being asked:

| Question | Representation | Boundary obligation |
| --- | --- | --- |
| May the value be absent? | `Option<T>` | Keep `None` distinct until the owner either permits absence or reports a missing required value. |
| Which semantic state exists? | An enum variant with the data of that state | Match the variant; do not reconstruct the state from a string, count, or collection shape. |
| Is a present value valid? | A validated type or a fallible conversion | Reject invalid input where the owner has enough context to explain it. |
| Can a physical format encode the state? | A private wire, storage, Arrow, or atomic encoding | Decode at the boundary and expose typed operations to callers. |

Consensus replication replies have explicit initial-probe, ordered-stream and finished phases.
Leading empty requests confirm a log boundary over management capacity. The initial-probe owner
retains the last confirmed preceding log identity for this input. A first data request whose
preceding identity differs requires an empty management check; a request at the beginning of the
log has no preceding identity to verify. Only a complete match transfers the original data and
remaining input to one ordered stream. Empty commit updates then remain in that stream's order.
A failed or rejected prefix check ends its input. These phases and their confirmation are volatile
protocol state and are never persisted or reused across stream generations.

A pure conversion refusal is an ordinary typed outcome, even when it implements
`std::error::Error` for a parsing trait. Its owner states that contract at the exact error type
with reason-bearing `nervix::error_boundary(outcome, ...)` metadata. A contextual operation failure
returns a report; a missing value does not silently turn one into a success. The resolved compiler
gate and report-carrier obligations are described in [Errors And Diagnostics](errors-and-diagnostics.md).

The outbound WebSocket source distinguishes a peer Close carrying its code and reason, a Close
without a status, an ended stream and a local close-handshake timeout as typed failure states.
No fabricated close code stands for an absent status. The source removes the exact transport
before its bounded closing wait, so cancellation leaves it needing a new resume. The
[connector contract](./connector-contract.md#ingestor-boundary) owns the retry and payload lifetime
effects.

WebSockets upgrade fields cross from literal semantic Models into `WebsocketsUpgradeHeaders`,
whose private representation enforces HTTP bounds and WebSocket protocol-name restrictions before
an execution plan receives it. `WebsocketDataFrame` carries either text or binary payload, so a
text tag cannot accompany invalid UTF-8. Each message retains its connection's immutable response
header snapshot; the quiesce buffer retains the same handle beside the typed frame kind.

The validated WebSockets client configuration carries the handshake capacity as `NonZeroU16` and
message/frame limits as `NonZeroU32`. Decimal CONFIG values cross this boundary once; zero,
overflow and an unschedulable ping-derived idle deadline are typed configuration refusals. The
effective idle duration is already established before planning. A handshake attempt owns its
bounded queue until signaling succeeds; only the completed queue moves into ordinary source
reads. Dropping or failing the negotiating future drops that exact queue, so its frames cannot
become data for a replacement connection. After a completed queue drains, its absent channel is
represented by `None`, and steady reads use only task-owned connection state.

Zero work outstanding, an initial sequence value, an empty payload, and a genuinely empty result
remain ordinary values. A documented scalar function may return zero for a particular input;
that result is part of the function contract. Arrow values under a null or failure validity mask
also do not encode absence in the numeric lane: the mask does. An `Option` or enum is needed when
the *state* is absent or different, not merely because a literal looks special.

The guest protocol keeps an absent branch key and an absent source token distinct from present
values. Empty application-state bytes remain present inside the Rust SDK's snapshot envelope.
Reset acceptance, reset refusal, an unusable snapshot envelope and rejected application state have
distinct typed ABI verdicts. Protocol decoding checks the complete size-prefixed header before
identifier access and verifies offsets and counts before constructing owned values.

Entity presentation distinguishes a written option from its declared effective default. Inspection
shows both, while canonical Plexus emits only written options, preserving optional Model fields
through a complete round trip. This is a display projection, never a stored shape or runtime plan.

The Sentry event Model keeps an omitted `PLATFORM` as `None`, separately from an explicitly
written string, including an empty string or `other`. Its serialized field is required and holds
either that string or an explicit null. Planning resolves the declared default into a required
platform string; connector preparation uses it only when the codec's event omits the field.
The registry's `NERVIX QOS2 SEND` archive signature identifies the current Model layout, including
the required branch partition count, Sentry platform, an ingestor's `TIMESTAMP AT metadata.<field>`
source, an SQS QUEUE emitter's `SEND` request shape, and the session and quality of service an MQTT
TOPIC ingestor writes beside its delivery mode. Consensus's `raft_qos_send_` keyspaces and current
state metadata encoding identify its stored shapes. An unrecognized signature, namespace or
encoding fails with the owning storage error and requires the stored data to be recreated.

## Absence And Distinct States

**Installed resource manifests.** The resource store returns an optional manifest: a filesystem
`NotFound` means the local copy is absent, while other filesystem and decoding failures keep their
typed reports. Replica reconciliation records an absent copy as pending while no live source is visible;
an installation has not failed merely because source discovery has not converged. A real read or
metadata failure remains distinct, and a matching local manifest proves the copy ready.
Recovery of an unpublished upload consumes the same absence outcome: without the admitting
node's installed copy, the admitted archive is unavailable. Its read failures retain their causes,
while an executor refusal or cancellation leaves that upload applying for another reconciliation.

**MQTT size admission.** A source packet is either an admitted publish with independently owned
bytes or an oversized outcome with an optional prepared acknowledgement. Absence means that QoS
requires none or the client acknowledges automatically. A discard is explicitly deferred for
its ordinary prefix, unrequested with its deadline, or requested with its confirmation target.
The following read starts its optional keep-alive deadline after the host acknowledges the
ordinary prefix; that original deadline then survives cancellation. The connection retains at
most one discard and clears it only when it returns the size outcome after confirmation. See
[MQTT source connections](./connector-contract.md#mqtt-source-connections).

**Iceberg catalog identity.** The namespace is required configuration, represented by one shared
validated name type from parsing through connector opening. Dot-separated levels are nonempty and
retain case. No absent namespace is filled from the Nervix domain, and no layer invents another
namespace key. A commit-size limit is the same positive whole-byte value that bounds a batch;
decoding validates that value rather than storing text for the runtime to parse.

The Core NATS emitter's buffer is either empty or pending with a nonzero record count and its
connection expectation. A pending write that began connected requires that same successful
connection; one queued while disconnected requires the next connection. The state is consumed
only once the driver drains its buffer, and a mismatched connection becomes a typed failure
instead of certifying discarded bytes. A deadline leaves the pending state intact. This is
volatile connector state, never a stored shape or a separate representation of the host's rows.

**MQTT input retirement.** A retiring input keeps the previous typed plan and an optional
replacement plan. Absence means the declaration is removed; stopping an execution whose input
is unchanged creates no retirement. A connector session catalog has a private constructor that
validates each configured endpoint and effective client identity once and borrows its immutable
plan. It represents each broker session once even if an invalid explicit identity is repeated.
Cleanup failure at a preceding endpoint is reported separately from failure to prepare the
replacement. A failed preceding configuration supplies no preservation proof: the explicit
replacement must prepare all of its sessions. An absent `client_id` selects the domain-qualified
default, with an instance suffix when needed.

**Pulsar input retirement.** The same optional replacement plan distinguishes dropping an input
from replacing it. A changed owned source requires a comparison of resolved broker address,
qualified topic and subscription name; ownership and consumer policy are not subscription
identity. A failed resolution or invalid service address leaves that comparison undecided, reports
its typed failure and preserves the subscription. It never supplies a default identity or treats
unknown state as permission to delete broker state.

**Consensus append input.** `None` from the caller's request stream means that input has ended;
the adapter returns an empty answer stream and opens no transport. `Some` carries the first typed
append request, retained exactly once while setup runs. A membership, admission or transport error
after obtaining that request remains a failure naming its peer; it is not inferred from ended
input. The caller's retry and cancellation gate runs before this distinction is resolved.

**Consensus log chunks.** An unread chunk, a buffered chunk owning decoded entries and their
working set, the reservation and the store's Commands turn, and an exhausted chunk boundary are
distinct states. Taking the last entry replaces the buffered state with the exhausted boundary and
drops its iterator allocation, its charge and then its turn before returning. The exhausted state
carries the need to yield before the next read without retaining memory on behalf of an empty
chunk. A consumer need not poll again to release that memory.

**Runtime checkpoint exchange.** A state synchronization answer uses `None` only when the
requester already holds the owner's current revision. `Some` carries a typed revision, byte length
and digest; zero-length checkpoint bytes remain a present checkpoint. The subsequent bulk request
names that exact revision. The receiver verifies the declared and received lengths and digest
before it constructs an installable state entry, so a partial stream is a transfer failure rather
than a different absence or an empty checkpoint. A handoff carries the same descriptions beside
their typed placements and prepares state only after each selected stream verifies.

**Node trace export.** A tracing guard either has no trace export or owns its provider and resolver
publication together. The publication carries `Option<DnsResolver>`: absence means startup has not
installed the node resolver, and presence carries that same resolver's shared handle. A closed
publication before installation is a distinct connection failure, not a choice of another resolver.
The lazy connector waits through the publication primitive; its export connection timeout starts
only after installation, preserving early startup spans.

**Branching.** A validated branch declaration is either unbranched or carries its named branch
and resolved schema together. The schema supplies the branch fields and their sensitivity, so a
runtime plan cannot pair a present branch schema with an independently missing sensitivity set.
Unbranched execution has no branch key; a concrete key has a nonempty declared shape. An empty
field list or synthetic root identifier does not select a branch. A concrete key holds only finite
floats, because the VM turns a non-finite float result into a row error and a non-finite float has
no canonical text to key a branch by. A record's fields are not held to that rule: a payload number
rounding past an `F32` field's range, an Avro float or a producer's Arrow batch can bring a NaN or
an infinity in, and every JSON rendering of a record, such as a materialized report, a hash map
answer or a hash map key, writes one as the string `NaN`, `Infinity` or `-Infinity`. A key read from a stored checkpoint or a peer that holds one
fails with `BranchKeyError::NonFiniteFloat`, and one holding a datetime that is not RFC 3339 text
fails with `BranchKeyError::RemoteFieldValue`; both name the field. A concrete key is a key of a
branch schema when it holds exactly the schema's fields, each with a value of exactly its declared
type; a missing or undeclared field and a value of another type are distinct
`BranchKeyShapeError` states naming the field, never a value converted to fit. The subscription
Row encoder holds each key to its relay's branch with that one check, and a restore holds every
archived key it installs to the branching its restored entity declares before staging it; see
[Backup And Restore](./backup-and-restore.md#archived-branch-keys). Materialized-state reads use the
incoming concrete branch, or the actual unbranched state, and reporting keeps the optional branch
identity until the display boundary. The structural ASCII graph projection is domain-free; a
serialized graph retains its real typed domain.

An emitter's buffered Arrow carrier keeps its typed source relay and optional concrete branch key.
The relay's declared branch name is fixed, so the pair identifies the exact source branch even if
another relay uses the same key fields and values. Payload assembly compares the pair before
combining carriers, and unbranched absence remains `None` throughout buffering and packing. Each
buffered row is one of three states: pending, carried by a batch payload or prepared request the
emitter retains, or resolved. A retained payload's members are therefore neither packed or prepared
again nor mistaken for resolved rows, which a delivered flag could not express.

A client emitter's consumer, delivery identity and ACK attempt are separate typed identities.
The optional concrete branch remains an internal `BranchKey`; the client sees only its opaque
fingerprint, never its key values. An attempt is pending or assigned to one consumer with one
reference and deadline. Retry, timeout or detach makes that reference stale before another
attempt becomes live. Confirmed ACK, retry and rejection results have bounded retention for
idempotence and expire independently of the delivery identity. An absent consumer is not encoded
as an empty consumer id, and an unbranched batch has no synthetic branch fingerprint.

The Rust client's desired producer and consumer handles have distinct active, interrupted,
restoring, reopen-required and closed states. An attachment belongs to one session exchange; a
replacement receives a fresh request-derived identity. The producer's unresolved sent batch has
an unknown outcome, rather than a missing outcome defaulted to not admitted. A consumer reports
the interruption before delivery from a replacement attachment. Its prior delivery reference is
expired, while a settlement sent before losing its answer is separately uncertain. The domain
`START` generation and endpoint contract fingerprint are required values on every successful
consumer open, so absence cannot be mistaken for a matching contract.

**Expression scopes and errors.** The VM frontend receives a scope policy that says whether a
bare field may be read, written, both, or neither. A generated or set-only route reports an
unavailable `message` or `input` scope during lowering, rather than inventing a namespace that
later fails lookup. Message errors carry a `MessageErrorOperation` variant through evaluation and
route handling. Its display text is derived from the variant; changing a label cannot select a
different operation. Structured errors identify the operation and affected fields without
exposing sensitive payload values.

**Progress.** A node's applied schedule revision is optional before its first successful
application. Once applied, every `u64` revision, including the maximum, is a real revision that
suppresses equal or older publications. The revision is recorded under the lock that serializes
schedule application, so the decision and its recorded state stay together. Consensus byte-based
snapshot retention likewise distinguishes no request from a request made while there was no
completed snapshot, and from one made at completed index zero. The trigger holds the optional
completed index that existed when the request was made; it suppresses duplicate requests for that
same completed snapshot without confusing its absence with index zero.

**Remote acknowledgements and admission.** A routing position is open with its generation and
optional correlation, or permanently closed. A correlation owns either a delivery's record rows
or an admission response channel. Pending rows distinguish waiting for admission from admitted
silence; ordered parked progress retains its required-wait guard. The wire number is an opaque
position/generation/row encoding, validated behind this owner with the registrar process identity.
No numeric value means missing, and an exhausted generation never wraps into a live identity.
An authenticated peer is starting, awaiting membership with its bound epoch, live with that epoch,
or ended. The admission choice's private atomic byte exposes only pending, admitted and cancelled;
one compare-and-exchange chooses an irreversible verdict. Borrowed admitted work and released
transport permits have separate lifetimes. [Cluster Interconnect](./interconnect.md#bounded-correlation-and-peer-owners)
owns their delivery guarantees and bounds.

**Receive decoding admission.** `RemoteRequestFailure` distinguishes a full memory budget,
an input above that budget, a closed budget, a full execution queue and a closed execution pool.
The wire projection retains the traffic class and applicable byte or queue dimensions; callers
recognize capacity by the variant. These refusals judge admission before inspecting the input,
while `InvalidPayload` represents validation or deserialization failure. A nested decoding job
owns its encoded bytes together with their independent reservation until the job exits, even if
the enclosing request ends. [Cluster Interconnect](./interconnect.md#traffic-and-resource-isolation)
owns receive admission and its stream responses.

**Client ingestors.** An ingestor's input is either a transport, which carries its source and the
codec that decodes it, or a client source, which carries the schema its batches hold and its
producer policy. There is no optional codec beside an optional schema, so an ingestor cannot claim
both or neither. A producer's admission is `Open` or `Suspended`; a batch's outcome is one of four
variants, each carrying its own typed cause, so a completed batch has no cause and a refused one no
failure; and a producer's end is one typed reason. A producer's identity is the request identity
that opened it, and a forwarded producer's link key is never reused within its process, so neither
has a reserved value meaning none. The client ingestor endpoint's published counts are genuine
zero counts, never markers.

**Sources and emitters.** A source or an emitter is addressed by the connector that serves it and the
entity it consumes, and its Model states both in its shape: the union holds one variant per
connector, that variant holds one variant per entity, and each entity has its own Model with only
the fields its statement writes. A Kafka topic source therefore cannot hold a Pub/Sub channel, and
an OpenTelemetry emitter is a logs, a traces or a metrics emitter, so only a metrics emitter carries a
metric and no signal value sits beside mappings that might contradict it. The kind that names a
pair is read from the Model and never stored beside it. An ingestor alteration is nested the same
way: its head selects one connector and entity, and the operations of that entity set a source of
that entity's own Model, so `ALTER KAFKA TOPIC INGESTOR` cannot carry a Redis channel and no kind
field sits beside the source it sets. Only a transport's operations include the codec that decodes
its payloads, so a client producer alteration cannot hold one. A stored ingestor of another pair is
refused by comparing the two kinds. The names an external system owns, such as a Kafka topic or a NATS subject, are distinct
types from Nervix names: each is checked against its entity's own rule once, when it is parsed or
decoded, and keeps its text exactly as written, case included, so a connector sends what the
statement says. What every source or emitter of a pair
declares, whatever its statement says, is declared once on that entity's Model in the vocabulary:
whether its messages carry headers, the typed `metadata` namespace they expose, whether it runs on
every node, what its body references, and for an emitter whether it writes headers, needs a codec,
requires batching and which publishing modes it accepts. A client Model names the connector it
configures as a typed kind, so a source or an emitter is matched with its client by comparing kinds
rather than labels. A session producer's source is no transport, so the transport union holds no
Model of it; the kind union still names it, because capability decisions ask about every source.

Direct-value entities declare their mapped field types in the same vocabulary. Codec and native
bodies have no direct-mapping contract; column mappings and named protocol fields have distinct
contracts. The registry infers each mapping's type and checks it without coercion before accepting
the candidate graph. It also establishes required batching and schedulable transport delivery
durations there. Planning consumes those guarantees, including the presence of a required batch,
and an invalid alteration leaves the accepted configuration intact.

MySQL table names validate their one- or two-part path at construction and decoding, keeping the
original case. Their parts are borrowed from that validated name when the connector quotes the
SQL destination; it does not reinterpret the whole path as one identifier. Pooled-client
configuration has one reserved-key declaration, shared by offline MySQL URL inspection and the
host's resolved-configuration refusal. Pool capacity remains the client's typed `POOL SIZE`
bounds, with no competing URL setting.

The MySQL shared pool and each borrowed connection retain an `Option<ConfiguredDeadline>`:
absence means the client omitted an operation bound, while a present value has already passed
the shared schedulable-deadline validation. Checkout, validation and insert failures keep typed
reports, including `DeadlineElapsed`, beneath their host contexts; a rendered reason is never
the pool-borrow error's state. Paired identity-file presence is established by the client schema,
then checked again when the driver options are constructed. The connector extends the parsed
URL's TLS options rather than reconstructing their policy from the presence of a CA file.

Postgres table names validate the same one- or two-part path at construction and decoding, and the
connector quotes the parts it borrows from the validated name in both its insert and its catalog
lookup. A Postgres address's TLS policy is the typed `PostgresSslMode`, read through the declared
`sslmode` option that schema validation applies too; a spelling the option does not declare, or the
option written twice, is refused rather than defaulted to a policy. A Postgres conflict clause's
rules belong to `PostgresConflictAction`, whose typed refusal names a missing `DO UPDATE` target, an
empty target column or an update without a mapped column outside its target.

RabbitMQ's consumer enum distinguishes automatic acknowledgement, sequential acknowledgement
and parallel acknowledgement. The parallel variant owns a `NonZeroU16` prefetch count and batch
timeout; both acknowledged variants own an ACK timeout and retry policy. Zero cannot accidentally
request AMQP's unlimited prefetch. The
composed Plexus parser rejects a count outside `1..=65535` instead of truncating it. Runtime
composition passes either automatic consumption or that exact nonzero count to the engine plan.
The engine does not recover the mode from a missing timeout or a numeric marker. The host's
settlement contract separately distinguishes batch commit boundaries from independent per-delivery
ACK/NACK boundaries.

Each emitter destination also owns its closed publishing enum: a Kafka topic can hold its own
`NO ACK` or acknowledged policy, while an HTTP request can hold only its request acknowledgement
policy. Capability metadata comes from those enum declarations. Shared inspection borrows the
policy; it stores no second mode. Plexus parses a complete `SET TO` body using its declared entity,
and ALTER carries that declared kind. The vocabulary checks the stored kind and every replacement
before atomically publishing an alteration, including manually constructed Models. Typed failures
name both entities. External locators preserve case in entity-specific validated types, including
one SQS queue-name type shared by sources and destinations. An Iceberg storage-client reference
retains absence of a resolved backend until the decision layer resolves the actual configured
client; no default backend fills that boundary.

**Endpoint availability.** Gossip publication carries an optional typed interconnect endpoint.
Parsing a present advertised endpoint checks its host and port syntax. Membership admission
requires that validated interconnect endpoint; an incomplete publication cannot become a
reachable member through an empty address. Other service advertisements, including the console,
may be absent independently. Absence says the service is unavailable; it does not invent a reason.
Peer selection and transport handling follow the [interconnect contract](./interconnect.md).

**Startup voter evidence.** The reconciliation task owns a process-local set of typed node names
actually observed live, including observations made while following. An absent voter in that set
means no live observation; Chitchat's current dead verdict does not supply one. A first relayed
heartbeat can therefore leave a voter unobserved even though gossip knows its identity. Expiring the
ten-second startup grace ends the observation requirement without declaring that voter live, and a
leadership change retains the original deadline. The set is discarded after expiry. See
[Whole-Cluster Restart Keeps Ownership](./shutdown.md#whole-cluster-restart-keeps-ownership).

## Identity Across Scheduling And Recovery

Schema-bound runtime state is keyed by a computed `SchemaFingerprint` supplied by the committed
schedule. Branch aggregate and Kafka offset state that do not depend on a schema identify that
independence explicitly. No fingerprint byte pattern means “missing” or “independent”; a computed
fingerprint is accepted for its identity even if its bytes happen to be zero. Scheduling must
publish the required fingerprint before schema-bound state can be loaded or replicated. Missing
required identity is an error, not a default digest.

The runtime Kafka offset key remains schema-independent. A backup archive also records the
ingestor's scheduled fingerprint with those offsets, so restore applies them only to the same
ingestor contract after publishing its target schedule.

The Kafka checkpoint also carries an optional applied-start generation. Absence means no explicit
start has been applied; generation zero is an ordinary present value. The marker and reset positions
publish together under assignment authority. Repeated or delayed starts cannot replace offsets
already acknowledged in the same or a later generation. Native checkpoint loading, replication,
archive encoding and restore preserve the marker exactly; restore never infers it from the domain's
latest start record.
The native root has a required, verified format signature, so arbitrary bytes cannot supply an
apparent absent generation when a checkpoint uses a different representation.

Kafka replica catch-up describes revision state as `Current` or `Advanced(revision)`, with typed
remote-operation failures for refusal. An advanced revision selects a bulk native checkpoint;
its header must name a revision newer than the replica's request and no earlier than the described
revision. Length, digest and the current archived shape are verified before a replacement table
is published through the retained replica assignment token. A cancelled or invalid transfer
leaves the installed state intact. Every successful synchronization returns the held revision,
including a `Current` answer, after validating replica authority. It is acknowledged again so
lost progress reports recover without another transfer; a promoted or replaced assignment rejects
reports through a retained token.

The runtime checks a stored state's schema identity against the current scheduled identity before
accepting it. WASM guest state additionally uses its generation for the concrete branch. A state
from another schema or generation cannot become current merely because it has a later revision.
Its required unit is a typed `Single`, `Partition(p)` or `Instance(i)` identity in the schedule,
state placement, storage key, remote envelope and archive section. A checkpoint for one unit cannot
be read as the state of another unit of the same entity.
This identity check is separate from the snapshot and handoff durability rules in
[WASM State And Recovery](./wasm-state.md), [Consensus Storage And Replication](./consensus-storage-and-replication.md),
and [Shutdown And Recovery](./shutdown.md). Those chapters define when state is saved, transferred,
and recovered; this chapter defines how the current state is identified.

Nervix keeps one current stored and wire shape. Producers and consumers of a changed identity are
updated together. A previously stored shape that cannot supply required identity fails to load
clearly and must be recreated; it is not defaulted into the current state. Tests construct the
current shape and assert its behavior.

## Archive Preparation Admission

Archive preparation reads the first physical tar header into the backup crate's `ManifestHeader`.
That type carries the regular manifest entry's validated identity and bounded encoded length.
Restore uses its length to admit manifest decoding before allocating owned archive values. The
full archive reader uses the same boundary and then verifies the manifest and every section; see
[Restore Intake And Preparation](./backup-and-restore-architecture.md#restore-intake-and-preparation).

## Archived Counts

Native `usize` and `NonZeroUsize` counts use the vocabulary's `CountAsU64` archive adapter. Every
count-bearing archived field selects it explicitly, so its stored width is 64 bits independently
of the archive's pointer width. Zero remains valid for an ordinary count; a nonzero count uses the
archive's validated `NonZeroU64` representation. Encoding preserves every bit of a supported
native count. Decoding converts once with `usize::try_from` and returns `ArchivedCountError` if the
receiving target cannot represent the value. No narrowing cast, clamp, or default supplies a count.

This contract covers relay capacity, transaction positions and operation numbers, queue limits,
application progress and outcomes, plan and report counts, topology counts, WASM inspection totals
and omitted-entry counts, and histogram delayed-removal bucket indices. The owning stored
namespaces and frame signatures identify the current count-bearing shape before decoding, and the
interconnect's wire fingerprint fences it between nodes. Unrecognized stored state fails clearly
and must be recreated. Complete equality properties exercise the production representations at
their range boundaries; see [Property Testing And Fuzzing](./property-testing-and-fuzzing.md).

## Atomic States On The Data Plane

The bounded executor's worker has `Selected`, `Executing` and `Idle` states. `Selected` owns its
next admitted callback and charge; `Executing` means that ownership is with the synchronous
callback; `Idle` has returned the class's active place. Selection and the idle transition share
the class queue fence. If a runtime drops a worker before its next callback, its destructor returns
the active place and closes queued submissions with `PoolClosed`, releasing their charges outside
the fence. Ordinary completion publishes `Idle` before its result, so destruction cannot return
that place twice. The separate queued-cancellation guard removes its exact entry only while it
remains queued; a selected callback instead retains its charge and receives the cancellation
signal. [Execution Primitives](./execution-primitives.md#executor-admission-and-draining) owns the
full admission and draining contract.

An ACK root's ownership handoff can be tracking a bounded number of active shares or be complete.
`REQUIRED WAIT` can leave a tracking root temporarily idle without completing it. Attachments,
wait releases, and final completion race, so the ACK owner stores the state in one atomic word and
changes it with compare-and-swap transitions. A reserved word value is private to that owner;
callers observe typed tracking, idle, and completion outcomes rather than doing arithmetic on the
encoding. Completion cannot be reactivated. This preserves the contentionless ACK path and the
root tracker's accounting without adding a lock. The delivery and handoff boundaries themselves
are described in [Data-Plane Concurrency](./data-plane-concurrency.md) and
[Shutdown And Recovery](./shutdown.md). [Execution Primitives](./execution-primitives.md) owns which
backend supplies the atomic; selecting a modeled atomic does not validate the owner's encoding or
prove its transitions without the corresponding production-owner check.

A source instance owns starting, ready and retired states in a private atomic byte. Its handle alone
interprets starting=0, ready=1 and retired=2. Compare-and-swap permits readiness changes only before
retirement; a retired source cannot become ready or change a replacement instance. This relaxed
scalar publishes no payload or other location. Buffered message-error workers separately own
`Prepared`, `Running` and `Ended` lifecycle states; a fallible binding prepares the bounded queue,
and successful running publication starts its worker once. An ended worker is never restarted.

The domain clock's atomic maximum has a different meaning. Its initial `i64::MIN` is the
documented identity for a maximum computation, and zero elapsed time is the value of an elapsed
duration clamped at its physical anchor. Neither encodes an absent clock. The
[Domain Clock](./domain-clock.md) chapter defines clock absence, installation states, authority
fencing, and overflow handling.

An entity-gate operation owns pending, held, released and failed outcomes. A held operation has
closed its admission scope and requested its force flush. Ownership capture also requires a freeze
owned by that exact coordination identity. The exact operation's drain-status query publishes it
only after observing no affected admitted work or flush obligations. The operation's registry guard
keeps its hold alive through that observation and publication. Request cancellation leaves the
receiver-owned engagement and its original lease intact; retries await the same operation and
renew no lease. The preparation deadline separately bounds engagement, drain and capture.

A domain schedule's planning snapshot keeps its captured consensus cordons with two distinct
identity sets: all live voters and eligible placement destinations. Termination of an already
cordoned voter leaves both sets unchanged; losing or restarting that voter changes its live
identity and invalidates the plan. Schedule and transaction publication validate the composed
destination set and retain the independent consensus topology fence.
The snapshot also retains the terminating live voters independently for destination diagnostics.
After membership and liveness, relocation reports that process state before a committed cordon.
Its terminating set is a captured diagnostic fact, while publication continues to fence the
composed destination identities; termination of an already cordoned former owner still changes
no eligible destination.

## External Encodings And Conversion Failure

The emitter connector state owns its stop drain and returns one typed answer: finished with its
publish report and finish outcome, failed with the typed connector/runtime cause, or elapsed
without an answer within the host bound. A connector reporting its own finish-deadline failure
remains a failed finish with that cause. It does not become an absent answer because a host timer
used the same instant. [Connector Crates And The Connector Contract](connector-contract.md) owns
the finish contract and [Shutdown And Recovery](shutdown.md) owns the deadline and response grace.

The stopping-node drain action carries a required remaining `Duration` budget. Its sender deducts
elapsed request time before each leader redirect; the receiving leader owns the typed budget for
the handoffs it performs. Preparation and activation deadlines are derived from that one value,
and exhaustion produces a failed drain outcome. The stopping-node answer distinguishes a failure
while its coordinator still leads from an interrupted drain whose coordinator no longer leads.
The latter routes the remaining moves through the next leader without resetting the budget or
repeating committed moves. Classification consumes the typed command disposition and consensus
leadership observation rather than matching diagnostic text. The interconnect verifies the archived
action
and fences its current representation with the wire fingerprint. [Shutdown And Recovery](./shutdown.md)
owns the lifecycle and deadline policy.

At an external boundary, a protocol may require a raw tag, signature, null lane, or optional
scalar. The adapter owns that encoding and turns it into a typed internal state. In the other
direction it encodes the typed state once. A format signature or syslog boundary marker can remain
raw on the wire, while callers see the state the format describes. Arrow batches remain the
data-plane payload throughout this conversion; codecs use typed builders and column values, and a
single addressed message is a view into a batch.

A relay batch carries its rows' ingestion watermarks the same way, in two Arrow buffers of
Unix-nanosecond timestamps beside the payload. A kernel reads them as `i64` lanes; a row addressed
on its own, the interconnect wire, and a materialized-state snapshot receive typed timestamps,
converted once where the row leaves the batch.

A conversion failure never becomes another valid payload value. The MongoDB emitter returns a typed
per-record failure if a value cannot be represented in BSON, including an unsigned value above
BSON's signed range. It does not publish that record with a replacement BSON null or a changed
conflict key. An actual null remains null, and other valid records in the batch can proceed under
the connector's record outcome contract. See [Connector Crates And The Connector Contract](./connector-contract.md)
for the host's publish and acknowledgement boundaries.

The MongoDB sink also distinguishes each failed write's document answers: delivered, rejected with
the server's definitive refusal code, or unresolved. An insert's implicit applied-document account
and a verbose bulk write's explicit applied indexes are separate request variants. The unsatisfied
write concern remains an optional failure beside those answers; it never replaces the documents'
individual outcomes. Repeated refusals leave that document unresolved. The connector contract owns
how these answers narrow the host's retry.

VM integer count and position operands retain their signedness and full magnitude. A large
unsigned input does not narrow to the largest signed integer; an unsupported result size reports
a per-row error before allocating it. UUID-v7 construction reports an unsupported pre-epoch time
as a row error rather than substituting the epoch. Exact schema types and Arrow validity still
govern the rest of the expression. The public function results are documented in
[Expression Functions](./filter-map-functions.md).

A submitted client batch is validated as a whole before any row is admitted: a stream that is not
exactly one uncompressed record batch of the ingestor's canonical Arrow schema, with valid columns
and within the row and byte limits, is refused with its typed defect. No value is cast, widened,
coerced, or defaulted to make a batch fit, and no subset of a malformed batch is admitted.
The shared Arrow IPC boundary checks canonical message framing, declared bodies and column buffer
ranges before invoking Arrow's reader. A malformed frame reports `ArrowBodyError::Framing` for a
relay or snapshot body and `ClientBatchError::Malformed` for a client batch; neither gains a partial
row from it.

Session replies carry typed command purpose and outcomes. An upload failure can carry an optional
assigned nonzero resource version; before assignment, the version is absent. Diagnostic spans can
be absent, while a present span beginning at offset zero is still present. The web console uses
the typed outcome for domain-selection dispatch instead of matching reply message text. The
FlatBuffers encoding preserves these optional fields and typed variants across the session edge;
[Client Session Protocol](./client-session-protocol.md#verification-before-reading) defines how a
receiver keeps an absent optional value distinct from a present zero.

The shared C binding gives clock observations their own `nx_clock_event_kind` and installations
their own `nx_clock_state`. Only `NX_CLOCK_PACED` has a mapping for `nx_clock_event_paced`; tick and
end-reason accessors likewise require their corresponding event kinds. An interruption or a refused
restoration carries a domain but no invented generation or end reason. A mismatched accessor returns
`NX_ERROR_TYPE` without changing its outputs, so absence cannot look like a zero generation or
timestamp. The paced and tick accessors allow omitted output pointers for fields a host does not
need.

The clock of a followed domain, `nx_domain_clock`, always has a generation and a state, so those
accessors cannot fail. A domain the session does not follow reads as a NULL clock rather than a
stopped one, a clock without an accepted tick answers `false` from `nx_domain_clock_tick` rather
than a zero tick, and an unpaced clock reports no admission window rather than an unbounded one.
Only a paced clock has a mapping for `nx_domain_clock_paced`, and a stopped or uninstalled clock has
no logical time, so its projections return `NX_ERROR_TYPE` instead of a fabricated instant.

Completion replies likewise carry a `SuggestionStatus` variant for ready, missing, stale, or failed
context and an optional continuation. The server resolves typed semantic references from one
committed configuration read with the requesting session's ordered transaction prefix applied;
a missing or mismatched transaction binding reports stale context instead of silently discarding
queued changes. Each candidate carries an explicit UTF-8 byte-range edit. The CLI and web console
apply the edit after checking the cursor boundary, and the web console converts browser UTF-16
selection offsets at its edge. A continuation is bound to the input, domain, revision, and
candidate set, so a changed context cannot silently reuse a page.

Structured-control lookups use a separate typed choice boundary. A request names its semantic
target and carries each dependency as a `ChoiceValue`; a placement choice therefore depends on a
domain-pace variant rather than on the text `PACED`. An internal-schema, branch, or relay choice
depends on a typed domain reference and returns a kind-qualified Model reference, and a relay-field
choice depends on the domain and relay references and returns a typed field reference. Results keep
the same typed union for enum variants and domain, resource, model, or field references, with
label, detail, and group held separately as presentation. The FlatBuffers discriminant selects
behavior. A missing typed dependency is `MissingContext`, and a page cursor binds the dependencies,
revision, candidate values, and presentation so changed form state is `StaleContext` rather than a
silently retargeted page. The browser keeps missing prerequisites, stale context, empty results,
loading, and failures as separate choice states. A missing prerequisite carries a hint; stale
context offers a fresh lookup; only lookup and transport failures are alerts.

Incomplete schema, branch, relay, and subscription form values stay in browser drafts. The
completed conversion creates the current schema, branch, or relay Model, with field order,
optionality, sensitivity, wire format and mode intact, or the current subscription client
statement. A relay draft's branching starts unselected, a state distinct from unbranched execution,
so a completed relay is never unbranched by omission; its materialized state is absent until
`LAST BY TIMESTAMP` is chosen, because a relay without one is itself valid. A schema, branch, or
relay selection retained after its captured domain changes is explicitly invalid until reselected;
no empty name or fabricated Model stands for a missing selection.

Junction and reingestor drafts use the same boundary. A junction's branch is unselected until the
operator chooses unbranched execution or a named branch; a reingestor route separately chooses
preserve, unbranched, or a named outgoing branch. A materialized dependency remains incomplete
until its relay and absence policy are selected. Ordered input, dependency, assignment, and route
drafts become the existing semantic Model only when every required choice and expression builds.
Changing the captured domain or a dependent reference invalidates selected references while
keeping the operator's draft text visible for correction.

### Diagnostic evidence states

The diagnostic owner distinguishes active cycles, potential order and overload. Potential records
carry `Unreviewed` or `Reviewed(TriageProof)`, with a bounded explicit basis, reason and retained
regression reference. New source/lifetime context revokes a review. `Live`, `Ended` and `Unrecorded`
lock instances preserve different observations; absent construction or acquisition context stays
absent. Reused source sites never replace run-local instance identity.

`ProcessRecord` requires its compile-time/runtime diagnostic selection, and an artifact requires
whole-process or selected scope. A selected export cannot qualify a source process whose other
findings it omitted. The current version-3 wire conversion checks every required field, bound,
identity and review once; unsupported versions fail from their header without another shape or a
default. [Data-Plane Concurrency](./data-plane-concurrency.md#diagnostic-deadlock-detection) owns the
current representation, qualification policy and coverage limits.

## Validation And Failure Boundaries

Configuration-backup round-trip validation names an unpinned resource binding with its own typed
refusal, rather than using a resource name itself as the error. Its exact outcome classification
applies to that validity check. Generic resource-version mapping still forwards the callback's
actual error type and does not classify every callback failure as a refusal.

Native connection options validate every timeout at setup: one millisecond through 24 hours.
The backup command wait has its own required duration, distinct from an optional per-domain
quiesce override and the ordinary request and retry bounds. Backup execution identity projects
the typed scope, resource inclusion and capture options together with the selected domain; the
local output destination remains a client download concern. Recovery may change that destination
while the server continues to validate the original semantic request and execution reference.

The registry rejects unresolved or contradictory contracts before a graph becomes active. It
resolves branch names, schemas, fields, and sensitivity as one branch state. Scheduling supplies
the required runtime-state identity. Runtime owners enforce transitions that depend on concurrent
execution or recovery. Connectors and the session edge validate external representations when
they decode or publish them. A caller does not compensate for a failed lookup, absent required
field, type mismatch, or conversion by supplying a default zero, empty value, or null.

The Iceberg emitter validates its configured location into a URL before creating its storage factory.
The initialized operator belongs to that storage root: a later object path must match its scheme,
host, port and container identity before its relative key reaches the operator. Table metadata
refreshes retain the first table's initialized `FileIO`; they do not install a different storage
owner alongside refreshed metadata. Invalid object references fail at this boundary.

Generated compiler findings carry a required execution-context field. An explicit null means that
analysis has not established a source contract; it is an unknown effect, not a cold classification.
The report decoder requires that field even though its value is optional. Complete compiler,
configuration and worktree identities are also required before generated evidence is reusable.

A vocabulary type that validates its value when it is parsed or constructed validates it again when
it is decoded, from JSON and from the archive alike. Every name type, a command execution reference,
a resource upload identity, a JSON path, connection-pool bounds, and an emitter's message and size
limits decode through the rule that constructs them, and accept only a value that rule produces
unchanged. Stored or received data therefore cannot hold a name with an upper-case letter, a path
past its step limit, a minimum above its maximum, or a size that is not a whole number of its unit;
such data fails to decode with a typed error and is never normalized into a valid value. A timestamp
is its signed Unix nanoseconds, and every conversion into one goes through them, so two spellings of
one instant, such as an offset and its UTC equivalent, or a leap second and the second after it, are
one timestamp. Every duration Nervix reads from text, whether a Plexus literal, a domain-clock period
or skew, a Model's timeout, interval, retention or TTL, a window aggregate's delay, or a node's
command-line option, is read by one guarded parser in the vocabulary. It refuses text whose spans
could add up to the most seconds a duration holds with a typed error; the grammar library it wraps
would panic on such text instead of failing, and Clippy rejects every other way of reaching that
library's parser.

Archive validation establishes the archived structure, then each vocabulary value is checked when
it is read back. A refusal in a list can therefore follow values that were already constructed.
The archive reader owns each completed value and frees it on refusal, together with the incomplete
list or fixed array and any box or shared pointer allocation. No decoded fragment crosses the
storage or transport boundary, and a stored record of an invalid current shape still requires
recreation.

The current archive readback inventory for containers with a semantic value that may refuse its
archived representation is:

| Boundary | Fallible contents inside archived containers |
| --- | --- |
| Interconnect | `ApplicationCompletionPeersResponse.peers`; `RelayMetadata.acks`; `PrepareOwnershipHandoffStateRequest.checkpoints`; the `relays` and `affected_entities` of `EntityGateRequest` and `EntityDrainStatusRequest`; the `emitter_publishing` lists of `DomainDrainStatusEnvelope` and `EntityDrainStatusEnvelope`; `DescribeRelayRequest.bindings`; ownership handoff checkpoint responses. These contain typed node, relay, emitter, model, or domain names. |
| Consensus replication and persistence | `AppendEntriesRecord.entries`, including membership configurations and nodes; durable batches of Raft entries and consensus state; the `Vec` and `Box` descendants of `ConsensusCommand`, domain schedules, command execution, transaction plans and reports, and stored models. They contain typed names, references, limits, and operation ranges. |
| Registry | Stored `Model` variants, including schema fields, nested `ParseAsType` boxes, processor routes and inputs, connector configuration, and recursive expression vectors and boxes. Their typed names and other checked vocabulary values may refuse after earlier members decode. |
| Runtime state identity | `StoredHandoffPreparation.checkpoints` and `StoredForcedRecoveryPreparation.checkpoints` contain placement envelopes with checked domain and model names. |
| Runtime window snapshot | `WindowDelayedRemovalSection.removals` contains `CountAsU64` buckets. A stored count above `usize::MAX` is refused on a narrower host. |

There is no archived shared pointer field at these Nervix boundaries today; names share an `Arc`
only in memory, and archive as text. The shared pointer reader is covered by the same dependency
fix and by a refusal test. The deduplicator, Kafka offset, materialized identity, branch LRU and
other runtime state snapshot containers, together with backup sections and deadlock evidence, first
deserialize primitive or raw string wire values, then validate their meaning after the complete
wire value owns its allocations. Their malformed input tests still check that every archive decode
frees its allocations.

Constant integer division prepares a `SignedDivisor` or `UnsignedDivisor` at the kernel boundary.
Its unsigned magnitude is `NonZeroU64`, and its private reciprocal state distinguishes a power-of-two
shift from a multiply-high reciprocal. A zero input produces no prepared divisor; the numeric
kernel reports failed lanes with the existing failure mask. Datetime callers already hold validated
positive strides, so they can assert that preparation succeeds. These per-call values are execution
artifacts and are never persisted; [VM Functions](./vm-functions.md#checked-buffer-kernels) owns their
arithmetic and failure contracts.

One in-memory domain activation plan resolves each relay's compiled schema, branch retention and
materialized-state presence; each codec's schema and wire definition; and each endpoint's VHOST and
signaling reference. A missing reference is a typed planning failure before installation. The
same plan shape feeds running and passive builds. Passive builds retain the planned materialized
relay identities and endpoint routes, while admission remains stopped. A server-side listener stays
bound on every live node independently of graph placement or domain leadership.

Materialized origination is an exclusive task capability: moving it transfers the mutable branch
selections, while reads and snapshot installation remain independently retainable. Read publications
carry an immutable row with its Arrow schema and watermarks. Branch membership and an ended row
publication are distinct states; absence in live state owns the answer even when storage retains an
earlier checkpoint. Recreating a branch allocates a fresh publication. Installation validates
assignment capability, branch generation, captured fence and revision before replacing any row;
the current branch lifecycle and revision cannot move backwards.

A generator's optional retained remote generation keeps its selected owner, placement, complete
sealed envelope and admitted snapshot together. Reuse follows a fresh description and equality of
all those identity fields. Equal revision counters do not establish equal contents across owner
restart or restore; changed digests, fences or branch generations require replacement. A failed
read or transition to local routing cannot obtain a cached remote success. An admitted read retains
its immutable view and admission until it ends, independently of cache release. This read cache cannot
install state or weaken the installation boundary's monotonic checks.

Server endpoint configuration and source availability are distinct states. The immutable route
table contains configured definitions; a bound source lifetime contains an optional prepared intake.
Source ending publishes absence through that lifetime before removing its route binding. A retained
request or WebSocket cannot interpret absence as a replacement source with the same node identity.
A lease loaded before ending may complete; later admission sees absence. Domain replacement and
teardown end the domain's lifetimes and replace all of its route definitions together, preserving
other domains. Unbind names the exact binding allocation so a preceding source's close cannot end
its replacement.

Endpoint request policy is either unacknowledged or a typed acknowledged plan carrying positive
capacity, physical timeout and retry delay. The decision boundary validates these durations and
the reject-only quiesce contract once. A WebSocket ACK declaration requires a signaling reference;
composition verifies its prepared outbound encoder before binding intake. Request outcomes retain
the distinction between local input refusal, temporary non-admission, downstream failure and an
interrupted lifetime. Only the public response boundary projects those states onto the shared
client outcome vocabulary. Exact lifetime ending is a cancellation event distinct from absent
intake; held replies observe the event while later requests observe absence.

A second in-memory decision, the domain's entrypoint plans, resolves every ingestor's source,
client, codec and routes and every reingestor's inputs, node filter and routes. It records how a
route's records get their branch key as one of three states: unbranched, keeping the incoming key,
or constructing a new key with a lowered program. The two branched states carry the branch and the
retention of the relay the route writes, so the branch a route declares and the branch its
entrypoint retains are one value, and a route whose declaration disagrees with its relay fails
planning. An ingestor's transport class is read from the one source it declares rather than stored
beside it. A Kafka ingestor's offsets are either a consumer group or domain offsets together with
their placement. The planner lowers every filter, route and branch construction before a node binds
them, so a bound route always carries its compiled program and a running task has no
missing-program or undeclared-branch state to check.

The Kafka source's assignment knowledge distinguishes unknown, cleared and installed partitions.
Moving an assignment operation into an admitted job leaves its source's knowledge unknown until
the job returns the confirmed state. Cancellation or failure cannot leave a cached assignment
claiming that a native operation the caller did not observe completed successfully. The next
operation therefore reinstalls or clears it rather than trusting an unconfirmed cache.

The owner reports a semantic typed error; contextual propagation uses `error-stack`. A
per-record conversion error is reported at the record boundary without formatting a fresh
message on every hot-path operation. Diagnostics contain the relevant identity, operation, and
field names, while sensitive payload values stay out of errors and logs. A truly optional value
continues as `Option` until its consumer decides whether absence is valid. A label or rendered
string is only a presentation of the state and never an input to execution.

Replicated command admission distinguishes a reference that has expired from one bound to a
different owner, domain, transaction position, or content in its typed conflict result. The
session carries that distinction into the public command disposition; rendering its message does
not choose the disposition.

Error-route branch validation carries the node, source route, error relay, and both branch
declarations as typed data. Direct emitter `VALUES` validation identifies a sensitive external
target by name and requires explicit leakage; neither error needs the source payload value.

## Restore Installation Authority

`RestoreLifecycle` is a closed `Stopped`/`Resume` policy in the semantic restore Model. Text omitting
`RESUME` selects `Stopped` at the language boundary. The restore plan carries distinct initial
stopped state and activation state. Both require the archived start generation and latest start
point; paced activation also requires the archived mapping. Required report and `DomainInfo`
generation fields are validated at their wire boundary, including the valid generation zero.
Materialized archive descriptors and row identities are archive-owned types. Their raw schema
fingerprint binds once to the archived start generation for native storage, independently of the
installation authority; payload values stay exact-schema Arrow columns. Typed branch identities,
watermark ordering, counts and framing are validated before those values reach the runtime, and
each record identity's branch must be a key of the restored relay's branching.

A captured archive section carries its publication key, content kind and staged artifact as named fields.
Guest capture retains a selected checkpoint handle until its scheduled generation is checked;
its admitted storage job records the size for later admission, and opening that handle returns
the checkpoint revision with the reader for those exact bytes.

Deduplicator and window capture likewise distinguish a live publication from an unopened stored
checkpoint in the captured-state union. A stored selection carries its immutable view and inspected
size, and opening it returns its actual revision with an admitted immutable artifact. Only a
validated keyspace or window reader exposes counts, boundaries or columns. The control plane
selects the scheduled owner and schema before opening; it receives typed conversion results rather
than native bytes. A window's live and stored readers retain their own required lifetime and metadata.

A materialized capture carries either current rows or stored checkpoints as distinct variants.
A captured materialized checkpoint is a selected immutable source. Opening it produces a reader
whose complete header is required; there is no optional or defaulted generation metadata. Each
bounded group retains its scalar identities and original Arrow bytes, and completion checks the
whole declared group and row count. Running and paused domains capture current rows; stopped
domains capture stored checkpoints. An empty generation still has its descriptor and
zero group/row counts. RESTORE resets the stored ownership fence to zero explicitly.

A restored domain has a replicated `Pending` installation from creation and an `Installing`
authority once its state generation is admitted. Neither state permits `START`, direct or transactional. Completion of the
exact current generation removes the installation; terminal command failure does not. Authority
carries leader identity and term, execution reference, mutation lease revision and generation.
Checkpoint revision remains guest history and cannot grant installation authority. Validation at
the applied-state boundary holds its read guard across the storage mutation and handle clearing.
The state store validates authority-bound receipts, checkpoint headers and ordered chunks before
synchronizing the complete namespace. One small active-generation record then selects that set
atomically and is synchronized before completion. It retains both authority and inventory for
exact retry and stale-attempt rejection. Cancellation after selection preserves the closed start
gate until the same authority completes durability and cleanup.

Runtime storage has distinct initial and restored namespaces; restore installation generation is
separate from guest-state generation and checkpoint revision. A current checkpoint is either an
inline checkpoint or a segmented checkpoint with required revision, length and digest. A read
uses one database view for namespace selection and payload data. A queued checkpoint job retains
its selected namespace and validates that it remains current before writing. A storage format
marker other than the current one fails explicitly, and so does a missing marker beside nonempty
checkpoint storage; either requires recreation rather than inventing a namespace for the stored
keys.

A branch-aggregate metric checkpoint revision identifies one exact encoded payload. Capture
retains its elapsed fields once, so another descriptor, stream or persistence request cannot choose
different bytes for that revision. Initial, replica and persisted recovery preserve the received
bytes with their revision. A dirty metric update selects the next revision; a snapshot mutex
serializes capture and installation at that placement's cold boundary.

Active chunk maintenance validates the placement and fixed revision/offset coordinates before
forming a chunk-set cursor. A set remains referenced only when the same view's selected header is
segmented and names that exact revision. A missing or inline header is an ordinary unreferenced
outcome; malformed coordinates or a malformed bounded header remain typed storage failures.
Segmented headers have only their fixed archived root, so a size check bounds the caller's copy
before decoding. The required current storage shape makes larger valid values inline; Fjall may
still read/cache those values internally. No second persisted liveness field or default is added.

Restore reclamation receives a borrowed `RestoreStateRetention` from one locked applied revision.
Absence of an applied log means catch-up is unknown and retains all generations. A generation
ahead of that log is also retained. Once applied, only the exact generation of an applying restore
requires unpublished storage; terminal, expired or absent execution records do not. Publication
identity remains a separate node-store protection, and reclamation never converts an incomplete
installation into a completed one. Key/value usage is reconstructed from current namespace keys,
including partial chunks without a receipt, rather than a defaulted persisted counter.
Namespace cursors use the vocabulary's canonical `decode` entry point for the required domain
name. It requires lowercase stored text and retains the typed `NameError` beneath the storage
format failure.

## Qualification Evidence

The integration harness's startup owner classifies a failed attempt after its cleanup, using the
task's joined terminal outcome when that reveals an initialization failure the readiness probe
could not observe. A listener bind failure names the addresses to replace; a readiness timeout
whose requested stop ended cleanly names all listeners. Cleanup shares the absolute startup budget,
and only the ending of the exact launched task permits a new launch. A task still stopping at the
budget boundary stays owned with its storage and listeners. See [Integration Test Lifecycle](./integration-test-lifecycle.md)
for its occupied-listener regression and endpoint observation.

The [typed states qualification ledger](https://github.com/nervix-io/nervix/blob/main/tests/typed-states-qualification-ledger.md)
maps each confirmed finding to its owning change, current source path, unit test, public Cucumber
scenario, and local validation command. It records the checks on the landed implementation rather
than claiming a new behavior change in this documentation chapter.

The focused public selection passed **217 scenarios and 2,784 steps** across branch and scope
rules, materialized state, error routes, MongoDB emission, VM functions, clock boundaries,
membership and recovery, session protocol, and the browser console. The applicable runtime
scenarios include one-node and three-node examples; ownership and recovery scenarios use the
production sticky scheduler. Owner tests passed **2,399 tests** across Models, VM, consensus,
interconnect, MongoDB, Client Wire, client core, browser, and server. `just test-shuttle` passed
**70 distinct checks under two schedules** for ACK and state-recovery concurrency. The same
qualification passed `just validate` and `just ratchet`.

These results establish the listed semantic paths and their tested failure boundaries. The
ledger distinguishes source-audited findings from demonstrated runtime behavior and records
which values remain legitimate zeros, empty content, Arrow masked lanes, and private encodings.

## Recurring Task Observations

The paced simulation applications represent endpoint-open intent as `CurrentGeneration` or
`FollowingStart`. Only the latter permits a bounded retry of `DomainStopped` after observing a
later paced generation; initial and contract-change opens retain the stopped-domain refusal.
The opened producer's generation and its paced clock still own permission to plan readings.

Healthy connector status is absence of a failure publication. TaskStatus represents task-only,
connector-only and simultaneous failures as distinct states, each retaining its error and optional
retry. The writer's identity is independent of the retry kind and diagnostic text. Each writer
clears its own state in the current atomic publication. Selection retains an active task retry
first, an active connector retry next, and a task error without a retry after that, so a pending
retry remains visible to drains. Operator inspection reads both owners' errors and the selected
retry from one immutable publication. Retained observations are immutable.

A Syslog stream source separately represents its listener's existence and the listening socket's
presence. An absent socket after accept failure requires rebind, while the same listener still
owns established connection tasks and queued frames; source close ends that retained state.
The testing capability shares one named listener-fault request between the connector and host:
the selected bound address and its acknowledgement belong to that same armed request.

Domain-clock publication carries lifecycle pause, generation and start
point beside the installation. Entity assignment absence invalidates a retained checkpoint reader;
a present assignment carries its state identity and, once the entity is placed, its checkpoint
owners: an optional primary, executors and replicas together. An assignment without checkpoint
owners is an installed identity awaiting placement, and an absent primary represents a placed
assignment without one primary owner. Replication routes
retain that same slot and an optional state intake; ending an intake permanently fences its exact
route before the routing publication withdraws it. Assignment absence and ended intake are distinct
states: identity removal invalidates every reader, while state retirement may leave the entity's
assignment available for storage-backed synchronization. Force-flush
readiness is privately encoded as idle=0, available=1 and closed=2. Only its owning type writes or
decodes that byte; the coordinator remains the authority for generation and claim state. The hint
has no cross-location data-publication contract.

Connector execution identity is a nested connector/entity variant. An ingestor plan's Kafka topic
leaf can only compose the Kafka topic ingestor, and OTEL logs, traces and metrics have distinct
emitter plans, configurations and concrete types. `IngestorConnector::KIND` and
`EmitterLifecycle::kind()` expose the vocabulary identity; composition asserts the declared kind
at compile time. Shared private encoding or endpoint intake state does not add an entity selector
to those public configurations. A new entity adds a typed variant and implementation.
[Connector and entity kinds](./connector-contract.md#connector-and-entity-kinds) owns the head,
reference, implementation declaration and composition contract.

OTEL field rules carry their exact type with the constraint that requires it: a histogram count
cannot be declared with a floating-point descriptor. Required, optional and required-for-DELTA
presence are distinct vocabulary states. Opening an OTEL client validates its required positive
deadline before constructing settings; the installed settings hold a duration with no absent state.
