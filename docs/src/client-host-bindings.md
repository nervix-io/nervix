# Client Host Bindings

*Plexus, the Nervix graph definition language.*

The Python, Ruby and JVM libraries and the C ABI share the Rust client's session through
`nervix-client-host`. This chapter owns their architecture: runtime and thread ownership, call
cancellation, host lifetimes, failure projection, data exchange, packaging and qualification.
Changes to a binding crate, the host runtime, a conformance probe or its ledger keep this chapter
current. The [Client Session Protocol](client-session-protocol.md) owns the exchange with a node;
the [Client Implementation Manual](client-implementation-manual.md#using-the-c-abi) owns normative
host requirements.

Application APIs and executed examples belong in the [Python](client-library-python.md),
[Ruby](client-library-ruby.md), [JVM](client-library-jvm.md) and
[C and C++](client-library-c.md) guides. The [conformance ledger](https://github.com/nervix-io/nervix/blob/main/tests/client-conformance-ledger.md)
records exactly what each probe and host suite proves.

## Ownership By Layer

Bindings and their shared host runtime are edges. They depend inward on the Rust client,
vocabulary and verified wire views, and carry no server implementation.

| Owner | Responsibility |
| --- | --- |
| `nervix-client-core` | Session state machine, correlation, redirects, retries, execution identity, subscription and clock restoration, endpoint credit, outcomes and settlements, Arrow IPC encoding and decoding |
| `crates/client-host` | A session's runtime; blocking entry with cancellation; prepared executions; host-neutral events, clocks, fields, batches and endpoint handles; failure classification and whole-report rendering |
| `crates/client-ffi` | The C header and exports, opaque C handles, pointer/length checks and explicit retain/release ownership |
| `crates/client-python` | pyo3 classes, exceptions and enums, GIL release, Python-owned views and Arrow PyCapsule exchange |
| `crates/client-ruby` | magnus classes over rb-sys, GVL release and interrupt handling, collector-visible ownership and immutable buffer views |
| `crates/client-jvm` | JNI exports, Java classes and exceptions, Cleaner-owned handles, native leases and direct buffers |
| `tests/client_conformance` and the conformance ledger | External host probes, language-level suites, exact test inventories and evidence boundaries |

A binding maps host values into the shared types and projects the result back. It never retries a
request, chooses a server, restores a subscription or implements acknowledgement state itself.
Python, Ruby and Java call the shared Rust owner directly through their host's Rust binding
library. The C ABI serves C and C++ only. An independent protocol implementation, such as the
Go or TypeScript qualification client, is a separate client and owns its own conformance claims.

## Threads, Calls And Cancellation

Each connected session owns a native runtime with two worker threads. The runtime is obtained
through `nervix_primitives::unmodeled::runtime` under the permission for
`crates/client-host/src/session.rs`; host entry and background shutdown are outside model
executions. The Rust client's production-owner checks qualify its protocol separately.

A blocking operation runs its future on the calling host thread while the runtime drives the
exchange. The library never invokes application code on its workers. Python releases the GIL and
Ruby releases the GVL while waiting. Java takes native leases on every input before releasing its
handle monitor; no Java monitor spans the wait. Calls from several application threads may share
a session; the Rust client still serializes its commands. Mutable field drafts and builders use
their host's exclusive-access rules, and an endpoint open captures a complete field snapshot.

A thread already driving a Tokio runtime cannot enter this blocking API; C ABI entry from that
context aborts the process. A Rust application uses the async Rust client or a separate blocking
thread. A prepared command also runs on a 512 KiB host stack in the unoptimized build, covered by
the Rust client's `a_prepared_command_completes_on_a_host_thread_with_half_a_java_stack` test.

Every wait accepts an optional cancellation token. Another host thread can trigger it. Its
optional physical deadline bounds that call independently of connection, request and retry
deadlines in `ConnectOptions`. The host token accepts at most one day and checks monotonic-clock
overflow. A triggered token wins over a simultaneously ready result; cancellation and deadline
expiry have distinct failure kinds. A Ruby interrupt cancels the current call's child token,
leaving sibling calls and their shared parent token intact.

Cancelling a waiter never rolls back admitted work. Prepare a durable command once and execute
the same handle again to recover an uncertain, cancelled or expired attempt. Session-local
subscription and clock commands have no durable outcome identity: a repeat can be refused after
the first took effect, so the application inspects its state or events. Cancelling a producer
outcome wait leaves its submission with the producer; releasing a delivery never settles it.
The endpoint distinctions are specified in
[Client Ingestors And Emitters](client-io-architecture.md#completion-attempts-and-replay).

## Host Lifetimes And Views

Session, producer, consumer and delivery handles share the runtime they need. Releasing the
session object does not invalidate an admitted call or a surviving endpoint. The final shared
owner releases the client inside its runtime and uses background runtime shutdown, so collection
does not block. Immutable outcomes, events, clocks and batches keep their own data alive.
Explicit endpoint close/end waits for the server-side operation; releasing local storage is a
separate ownership action.

| Host | Ownership and borrowed data |
| --- | --- |
| Python | Python objects own Rust handles. Session, producer, consumer and delivery context managers release their share at block exit; admitted calls retain theirs. A frame or IPC `memoryview` keeps its exporting object alive, including after other references are collected. |
| Ruby | magnus objects mark every Ruby owner they retain. Collection releases native ownership without waiting. Frame and IPC `IO::Buffer` roots are read-only, frozen and locked; derived slices retain the root and its native owner. `get_string` creates a binary copy. |
| JVM | Each `AutoCloseable` object has Cleaner fallback. Closing it ends new access through all Java references to that object. Independent leases keep admitted blocking calls alive. Short calls defer native release through reentrant close and refuse nested access. A read-only direct `ByteBuffer` retains its native owner through the root, slices and duplicates, even after explicit handle close. |
| C and C++ | The caller releases each owned handle exactly once with its header-declared function. Borrowed pointers require a live owning reference. C++ uses an owner such as `std::unique_ptr` with the matching deleter. |

Python, Ruby and Java applications retain ordinary language objects and views, without exposing
the C ABI's retain/release operations. They still decide when to settle deliveries, release
submission interest and close endpoints. Collection supplies memory safety, not a delivery
decision or a prompt server-side close.

### The C ABI's Explicit Ownership

`crates/client-ffi/include/nervix_client.h` is the complete C contract. An object returned to the
caller is owned; an accessor's pointer/length pair is borrowed. Strings are byte sequences with
lengths, can contain NUL and are not terminated. A draft `nx_fields` or `nx_batch_builder` is used
from one thread at a time; other concurrent-use permissions come from the header.

Events, clock events, domain clocks, deliveries and batches expose matching `retain`/`release`
pairs. Each retained reference has one release, on any thread the header permits. Other owned
handles use their matching `free`, including sessions, executions, errors, options, outcomes,
inspections and drafts. An independently obtained schema or inspection outlives the outcome it
was read from. A borrowed error message ends with its error; a borrowed frame, IPC stream or
value ends with its owning reference. Freeing a session leaves its endpoint-held runtime alive.

An `nx_connect_options` draft carries copied credentials, TLS requirement, CA PEM, seeds and
connection/request/retry/backup waits. Connect reads its snapshot; keep the draft unmodified
during that call and free it afterwards. An HTTPS start requires HTTPS on every seed and
redirect. [Manual B-1 through B-16](client-implementation-manual.md#using-the-c-abi) specify the
application's recovery, cancellation, ownership and settlement duties.

## Failure Projection

`nervix_client_host::Failure` is the reporting boundary. It classifies typed report contexts and retains
the full cause chain as the display message. Backup failures inspect the typed download cause;
received malformed batches become protocol failures, while malformed application batches are
invalid arguments. A host branches on the kind, typed open refusal, event cause or disposition,
never on rendered text. An uncertain command failure retains its execution reference.

| Shared kind | C `nx_error_kind` | Python and Ruby subclass | JVM subclass |
| --- | --- | --- | --- |
| `InvalidArgument` | `NX_ERROR_INVALID_ARGUMENT` | `InvalidArgumentError` | `InvalidArgumentException` |
| `Connect` | `NX_ERROR_CONNECT` | `ConnectError` | `ConnectException` |
| `Transport` | `NX_ERROR_TRANSPORT` | `TransportError` | `TransportException` |
| `Uncertain` | `NX_ERROR_UNCERTAIN` | `UncertainOutcomeError` | `UncertainOutcomeException` |
| `Rejected` | `NX_ERROR_REJECTED` | `RejectedError` | `RejectedException` |
| `Deadline` | `NX_ERROR_DEADLINE` | `DeadlineError` | `DeadlineException` |
| `Cancelled` | `NX_ERROR_CANCELLED` | `CancelledError` | `CancelledException` |
| `Overflow` | `NX_ERROR_OVERFLOW` | `EventOverflowError` | `EventOverflowException` |
| `Protocol` | `NX_ERROR_PROTOCOL` | `ProtocolError` | `ProtocolException` |
| `Type` | `NX_ERROR_TYPE` | `TypeMismatchError` | `TypeMismatchException` |
| `Closed` | `NX_ERROR_CLOSED` | `ClosedError` | `ClosedException` |
| `Interrupted` | `NX_ERROR_INTERRUPTED` | `ConsumerInterruptedError` | `ConsumerInterruptedException` |
| `ReopenRequired` | `NX_ERROR_REOPEN_REQUIRED` | `ReopenRequiredError` | `ReopenRequiredException` |

Python and Ruby subclasses inherit `ClientError`; Java subclasses inherit `ClientException`.
They expose the classified kind, complete message, optional execution reference and optional
endpoint-open refusal. C calls that return `nx_error *` return NULL on success; a failure object
is read and freed by the caller. A successfully received command can still have a failed
`Outcome` disposition, which remains distinct from a failed host operation.

Subscription and clock events keep their semantic kinds. A restoration failure carries a typed
cause, message and retry wait. A removed or redefined subscription generation ends; a retryable
refusal remains interrupted until restoration succeeds or the application deletes it. The
bounded session event queue's overflow is a failure of the wait, distinct from a subscription's
consumer-overflow event. Endpoint interruption, reopening and uncertain settlement retain the
Rust client's own meanings.

A followed domain clock is an immutable snapshot of the latest state the session received.
After an attach, the host can read the generation and mapping from its reply before any tick;
subsequent observations replace that snapshot in receive order, so a read is never behind an
event already taken. A domain not followed has no clock. The shared owner projects logical time,
physical waits and admission with the Rust client's arithmetic. A refused attach is a failed
command disposition with the server's message; endpoint `OpenRefusal` describes producer and
consumer opens instead.

## Columnar Data And Arrow Exchange

Row events retain the verified FlatBuffers frame, already checked against their subscription's
schema. Bulk access copies a column level at a time; string and bytes accessors can borrow a
single value. Constructing an Arrow batch from a Row event happens only when requested and
preserves nulls and redaction, with sensitive fields nullable. Subscription list values remain
available in the verified frame; the C Row scalar accessors do not flatten lists.

Endpoint fields preserve written order, exact scalar widths, every list level, nullability and
sensitivity. Builders copy each supplied buffer before returning, so callers can immediately
reuse it. Consumers decode and cache a delivered batch on first batch access, validating schema
and row count. The one batch encoding is canonical Arrow IPC; an imported stream must match that
contract. [Client Ingestors And Emitters](client-io-architecture.md) owns its transport, credit
and acknowledgement boundaries.

| Host boundary | Exchange |
| --- | --- |
| C and C++ | Bulk typed buffers and pointer/length views; `nx_producer_submit_ipc` copies input bytes, and batch/delivery IPC accessors borrow retained bytes |
| Python | Typed values and buffer views, plus Arrow PyCapsules for batches and Row events; producer submission imports compatible Arrow data from pyarrow or polars under the exact declared schema |
| Ruby | Typed column arrays and immutable IPC buffers; an Arrow-capable library can exchange canonical IPC bytes |
| JVM | Bulk Java arrays and read-only direct IPC/frame buffers; Arrow Java exchanges canonical IPC |

PyCapsules share Arrow array buffers across the language boundary; this does not remove IPC
encoding or transport copies. Ruby and Java bulk arrays copy, while their retained native buffer
views borrow. Widths, float bits, signed DATETIME nanoseconds, embedded NULs and non-UTF-8 bytes
survive their declared mappings; Java uses a `long`'s bits for unsigned 64-bit values. No binding
introduces row-map payloads, implicit numeric conversions or per-cell callbacks from Rust.

## Typed Inspection

`Outcome::state_inspection()` supplies the existing owned `Inspection`. Its state is the
verified `StateInspection`, and its cached JSON is that envelope's lossless serialization.
Bindings retain this owner for page/refusal, entity, complete counts, unit identity, ordered
replicas, captured origin, section and opaque continuation access. They add no second inspection
representation and derive no typed value from display text. A `SHOW WASM PROCESSOR ... STATE` page
uses the same page and unit views, and its units have no sections; the bindings have no typed WASM
views, so its pinned module and each unit's guest-state facts are read from the lossless JSON.

Python and Ruby metadata objects share one inspection owner. JVM unit and section handles lease
the same inspection with immutable checked indices, so closing a parent does not invalidate a
child. `InspectionFrame` retains exactly one verified Row frame and its schema. Its construction
checks schema conformance, clones the shared backing bytes and does not retain the whole page.
Schema handles returned by a frame may outlive that frame.

The Python buffer exporter, Ruby read-only `IO::Buffer` exporter and JVM direct-buffer lease
already used for event bytes also retain inspection frames. Derived slices preserve that owner
after all parent objects close or are collected. These are the session's exact Row/Cell bytes,
with integer widths, float bits, timestamps, nested values, branch identity, null and redaction
preserved. They are distinct from the Arrow IPC exposed by batches.

Refusals retain their typed reason and have no page counts, units or continuation. Empty pages
remain successful pages. Unit classification is `Single`, `Partition` or `Instance`; its optional
index preserves the current unit identity. JVM unsigned 64-bit metadata stays in Java `long`
bits. A continuation remains opaque to every binding; server selection and fencing rules own it.

The native owner tests compare complete JSON, exact frame bytes and schema metadata while testing
independent close/GC lifetimes. The host's registered bounded inspection property checks the
complete envelope and shared-frame projection. The one-/three-node public scenarios in
`native_binding_inspection.feature` cover empty pages, refusal, sensitive branched pages,
continuation and views retained after session close. Normal pytest, minitest and JUnit suites
also exercise the public classes; their exact member and test inventory is checked by
`just validate-client-suites`.

## Packaging And Build

Artifacts are native to the target that loads them. Canonical recipes build them through the
configured Rust compiler wrapper and place generated files under the configured build root;
`target/client-conformance` is the default. They do not publish to package registries.

| Artifact | Toolchain and preparation |
| --- | --- |
| C ABI | Rust `cdylib`, its checked-in header, and C11/C++17 compilers for the external probes and application suites |
| Python wheel | `just build-client-python`, maturin and pyo3's abi3 for CPython 3.12+; ships typed stubs and `py.typed`, with conformance environments for Arrow integration and bare-wheel loading |
| Ruby gem | `just build-client-ruby`, CRuby 3.3+, headers/shared library and libclang for rb-sys; builds against the loading interpreter's minor ABI, which the loader checks |
| JVM jar | `just build-client-jvm`, JDK 21+ and `javac --release 21`; bundles the target JNI library and Java API, with no preview feature required |

The JVM loader extracts its bundled native library into a private temporary directory.
`-Dio.nervix.client.library=/absolute/path/to/library` selects an explicit file; a jar with no
bundled library uses `System.loadLibrary`. Ruby packaging checks both the extracted archive and a
fresh installed gem. The Python paced driver uses the same package through a release wheel under
`target/paced-simulation/python`; it defines no separate ABI.
[Developing Nervix](developing-nervix.md#prerequisites) owns tool versions and preparation commands.

## Qualification And Observability

The ledger inventories every probe, including the independent Go and TypeScript clients. Every
new binding or probe supplies one-/three-node examples for the three core outlines: typed rows
and operation outcomes; attached clocks and generations; and producer/consumer batch exchange.
The remaining conformance outlines qualify additional surfaces such as upload identity,
restoration causes and C connection policy; their actual runtime rows define their scope.
The `client-conformance` PR label selects the cross-language lane.

Host suites separately exercise each published API in its own framework: pytest, JUnit 5,
minitest, and the C/C++ registered native tests. Their boundary and cluster tiers, public-member
reference checks, exact inventory and guide includes are specified in the ledger. Prepared
artifacts precede test execution; tests never invoke a compiler or package manager. A probe's
report is not coverage of every host API, and a public-member reference is not a behavioral
assertion.

Every host boundary tier runs in ordinary CI, and the Python cluster examples also run in
ordinary scenario shards. The `client-conformance` lane runs every host's cluster tier and the
cross-language probes. Prepared boundary and packaging checks pass before any cluster starts.

Registered Bolero batch and field-description properties live with the actual conversion owner:
the shared host's Arrow interface and the C, Ruby and JNI boundaries. A new binding adds its
batch property with matching register, corpus and domain bounds. Runtime protocol models stay
with the Rust client's production owners. Deloxide does not observe the separate host processes;
server diagnostic-image evidence concerns the server processes, not host memory ownership.

Applications observe dispositions, execution references, typed failures, interruption/restoration
events, overflow, endpoint state, credit and submission outcomes. Conformance probes print the
common report oracle and finish with `PASS` after their required cleanup. Host suite results are
emitted after teardown and checked against exact inventory identities. Server metrics and logs
remain server observations; the bindings add no independent
retry counters or outcome inference. Logs, measurements and qualification reports are recorded
on the owning ClickUp task rather than committed as architecture text.

## Guarantees And Limits

All four binding crates reuse one Rust session state machine and one host failure classifier.
Native host objects and borrowed views keep their storage alive under the ownership rules above.
They preserve the Rust client's correlation, recovery, typed values and explicit uncertainty.

The surface is blocking. Asyncio, Ruby fiber-scheduler and Java virtual-thread adapters are not
provided. GC timing is not a close deadline. Cancellation is not rollback, restoration is not
durable replay, and retaining delivery bytes does not preserve settlement rights across a gap.
Session limits and exact endpoint grants still bound frames, queued events, rows and credit;
the host adds no alternate admission or recovery policy.

Transaction status/inspection, structured choice lookup, server notices and leadership
observation are outside their shared host surface. Read the
Rust library and ledger for the exact supported operations rather than assuming binding parity
with every Rust method. The independent Go and TypeScript probes qualify their recorded subset;
they are not separately maintained application SDKs.
