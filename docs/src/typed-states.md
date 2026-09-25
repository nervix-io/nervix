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

Zero work outstanding, an initial sequence value, an empty payload, and a genuinely empty result
remain ordinary values. A documented scalar function may return zero for a particular input;
that result is part of the function contract. Arrow values under a null or failure validity mask
also do not encode absence in the numeric lane: the mask does. An `Option` or enum is needed when
the *state* is absent or different, not merely because a literal looks special.

## Absence And Distinct States

**Branching.** A validated branch declaration is either unbranched or carries its named branch
and resolved schema together. The schema supplies the branch fields and their sensitivity, so a
runtime plan cannot pair a present branch schema with an independently missing sensitivity set.
Unbranched execution has no branch key; a concrete key has a nonempty declared shape. An empty
field list or synthetic root identifier does not select a branch. Materialized-state reads use the
incoming concrete branch, or the actual unbranched state, and reporting keeps the optional branch
identity until the display boundary. The structural ASCII graph projection is domain-free; a
serialized graph retains its real typed domain.

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

**Endpoint availability.** Gossip publication carries an optional typed interconnect endpoint.
Parsing a present advertised endpoint checks its host and port syntax. Membership admission
requires that validated interconnect endpoint; an incomplete publication cannot become a
reachable member through an empty address. Other service advertisements, including the console,
may be absent independently. Absence says the service is unavailable; it does not invent a reason.
Peer selection and transport handling follow the [interconnect contract](./interconnect.md).

## Identity Across Scheduling And Recovery

Schema-bound runtime state is keyed by a computed `SchemaFingerprint` supplied by the committed
schedule. Branch aggregate and Kafka offset state that do not depend on a schema identify that
independence explicitly. No fingerprint byte pattern means “missing” or “independent”; a computed
fingerprint is accepted for its identity even if its bytes happen to be zero. Scheduling must
publish the required fingerprint before schema-bound state can be loaded or replicated. Missing
required identity is an error, not a default digest.

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

The domain clock's atomic maximum has a different meaning. Its initial `i64::MIN` is the
documented identity for a maximum computation, and zero elapsed time is the value of an elapsed
duration clamped at its physical anchor. Neither encodes an absent clock. The
[Domain Clock](./domain-clock.md) chapter defines clock absence, installation states, authority
fencing, and overflow handling.

## External Encodings And Conversion Failure

At an external boundary, a protocol may require a raw tag, signature, null lane, or optional
scalar. The adapter owns that encoding and turns it into a typed internal state. In the other
direction it encodes the typed state once. A format signature or syslog boundary marker can remain
raw on the wire, while callers see the state the format describes. Arrow batches remain the
data-plane payload throughout this conversion; codecs use typed builders and column values, and a
single addressed message is a view into a batch.

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

Session replies carry typed command purpose and outcomes. An upload failure can carry an optional
assigned nonzero resource version; before assignment, the version is absent. Diagnostic spans can
be absent, while a present span beginning at offset zero is still present. The web console uses
the typed outcome for domain-selection dispatch instead of matching reply message text. The
FlatBuffers encoding preserves these optional fields and typed variants across the session edge.

## Validation And Failure Boundaries

The registry rejects unresolved or contradictory contracts before a graph becomes active. It
resolves branch names, schemas, fields, and sensitivity as one branch state. Scheduling supplies
the required runtime-state identity. Runtime owners enforce transitions that depend on concurrent
execution or recovery. Connectors and the session edge validate external representations when
they decode or publish them. A caller does not compensate for a failed lookup, absent required
field, type mismatch, or conversion by supplying a default zero, empty value, or null.

The owner reports a semantic typed error; contextual propagation uses `error-stack`. A
per-record conversion error is reported at the record boundary without formatting a fresh
message on every hot-path operation. Diagnostics contain the relevant identity, operation, and
field names, while sensitive payload values stay out of errors and logs. A truly optional value
continues as `Option` until its consumer decides whether absence is valid. A label or rendered
string is only a presentation of the state and never an input to execution.

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
