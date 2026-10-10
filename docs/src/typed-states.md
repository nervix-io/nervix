# Typed States And Validation Boundaries

Nervix represents absence and distinct semantic states in its types. A zero, empty string,
empty collection, maximum integer, or all-zero digest is a value in its own right; it does not
stand for a missing value or a different state. The owner of each state decides when a value is
required, validates it there, and passes the resulting typed value to the next layer. This keeps
the registry, execution plans, runtime, clients, and diagnostics from assigning different meanings
to the same raw value.

This chapter describes the landed typed-state architecture across those boundaries. The
[Cluster Interconnect](./interconnect.md) chapter owns node-to-node wire forms, membership, relay
delivery, and transport failures. [Domain Clock](./domain-clock.md) owns clock installation,
authority, and time bounds. [Shutdown And Recovery](./shutdown.md) owns drain, handoff, and restart
semantics. The state rules here apply to those systems without replacing their detailed contracts.

[Execution Plans](./execution-plans.md) follows the validated values into a complete installed
revision.

## A State Has One Owner

Text is parsed into a semantic Model, registry validation resolves its references and contracts,
and planning produces an execution plan. Each stage passes forward a value whose type expresses
what has already been established. Runtime code executes the plan; it does not reinterpret a Model
or infer a state from a display label. Shared identities live in the vocabulary so every layer
uses the same meaning for a domain, branch, node, timestamp, or schema fingerprint.

The representation depends on the question being asked:

| Question | Representation | Boundary obligation |
| --- | --- | --- |
| May the value be absent? | `Option<T>` | Keep `None` distinct until the owner either permits absence or reports a missing required value. |
| Which semantic state exists? | An enum variant with the data of that state | Match the variant; do not reconstruct the state from a string, count, or collection shape. |
| Is a present value valid? | A validated type or a fallible conversion | Reject invalid input where the owner has enough context to explain it. |
| Can a physical format encode the state? | A private wire, storage, Arrow, or atomic encoding | Decode at the boundary and expose typed operations to callers. |

A pure conversion refusal is an ordinary typed outcome, even when it implements
`std::error::Error` for a parsing trait. Its owner states that contract at the exact error type
with reason-bearing `nervix::error_boundary(outcome, ...)` metadata. A contextual operation failure
returns a report; a missing value does not silently turn one into a success. The resolved compiler
gate and report-carrier obligations are described in [Errors And Diagnostics](errors-and-diagnostics.md).

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

## Absence And Distinct States

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

**Client ingestors.** An ingestor's input is either a transport, which carries its source and the
codec that decodes it, or a client source, which carries the schema its batches hold and its
producer policy. There is no optional codec beside an optional schema, so an ingestor cannot claim
both or neither. A producer's admission is `Open` or `Suspended`; a batch's outcome is one of four
variants, each carrying its own typed cause, so a completed batch has no cause and a refused one no
failure; and a producer's end is one typed reason. A producer's identity is the request identity
that opened it, and a forwarded producer's link key is never reused within its process, so neither
has a reserved value meaning none. The client ingestor endpoint's published counts are genuine
zero counts, never markers.

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
[Backup And Restore](./backup-and-restore.md).

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

An ACK root's ownership handoff can be tracking a bounded number of active shares or be complete.
`REQUIRED WAIT` can leave a tracking root temporarily idle without completing it. Attachments,
wait releases, and final completion race, so the ACK owner stores the state in one atomic word and
changes it with compare-and-swap transitions. A reserved word value is private to that owner;
callers observe typed tracking, idle, and completion outcomes rather than doing arithmetic on the
encoding. Completion cannot be reactivated. This preserves the contentionless ACK path and the
root tracker's accounting without adding a lock. The delivery and handoff boundaries themselves
are described in [Data-Plane Concurrency](./data-plane-concurrency.md) and
[Shutdown And Recovery](./shutdown.md).

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

## External Encodings And Conversion Failure

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

A conversion failure never becomes another valid payload value. The MongoDB sink returns a typed
per-record failure if a value cannot be represented in BSON, including an unsigned value above
BSON's signed range. It does not publish that record with a replacement BSON null or a changed
conflict key. An actual null remains null, and other valid records in the batch can proceed under
the connector's record outcome contract. See [Connector Crates And The Connector Contract](./connector-contract.md)
for the host's publish and acknowledgement boundaries.

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
findings it omitted. The current version-2 wire conversion checks every required field, bound,
identity and review once; unsupported versions fail from their header without another shape or a
default. [Data-Plane Concurrency](./data-plane-concurrency.md#diagnostic-deadlock-detection) owns the
current representation, qualification policy and coverage limits.

## Validation And Failure Boundaries

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
one timestamp. Every duration Nervix reads from text, whether an NSPL literal, a domain-clock period
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

Server endpoint configuration and source availability are distinct states. The immutable route
table contains configured definitions; a bound source lifetime contains an optional prepared intake.
Source ending publishes absence through that lifetime before removing its route binding. A retained
request or WebSocket cannot interpret absence as a replacement source with the same node identity.
A lease loaded before ending may complete; later admission sees absence. Domain replacement and
teardown end the domain's lifetimes and replace all of its route definitions together, preserving
other domains. Unbind names the exact binding allocation so a preceding source's close cannot end
its replacement.

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

A materialized capture carries either current rows or stored checkpoints as distinct variants.
A captured materialized checkpoint is a selected immutable source. Opening it produces a reader
whose complete header is required; there is no optional or defaulted generation metadata. Each
bounded group retains its scalar identities and original Arrow bytes, and completion checks the
whole declared group and row count. Running and paused domains capture current rows; stopped
domains capture stored checkpoints. An empty generation still has its descriptor and
zero group/row counts. RESTORE resets the stored ownership fence to zero explicitly.

A restored domain has a replicated `Pending` installation from creation and an `Installing`
authority once its state generation is admitted. Neither state permits `START`. Completion of the
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
its selected namespace and validates that it remains current before writing. The storage format
marker is required for nonempty checkpoint storage; corruption or absence fails explicitly and
requires recreation rather than inventing a namespace for the stored keys.

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

Healthy connector status is absence of a failure; a failure contains its safe error and an optional
retry in one publication. Domain-clock publication carries lifecycle pause, generation and start
point beside the installation. Entity assignment absence invalidates a retained checkpoint reader;
a present assignment contains its state identity, optional primary, executors and replicas
together. An absent primary represents an assignment without one primary owner. Replication routes
retain that same slot and an optional state intake; ending an intake permanently fences its exact
route before the routing publication withdraws it. Assignment absence and ended intake are distinct
states: identity removal invalidates every reader, while state retirement may leave the entity's
assignment available for storage-backed synchronization. Force-flush
readiness is privately encoded as idle=0, available=1 and closed=2. Only its owning type writes or
decodes that byte; the coordinator remains the authority for generation and claim state. The hint
has no cross-location data-publication contract.
