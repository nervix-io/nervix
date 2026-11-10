# Client Host Bindings

*Plexus, the Nervix graph definition language.*

The Python, Ruby and JVM libraries and the C ABI share the Rust client's session through
`nervix-client-host`. This chapter owns their architecture: runtime and thread ownership, call
cancellation, host lifetimes, failure projection, data exchange, packaging and qualification, and
the language-level suites and guides that hold each library to its documented behaviour. Changes
to a binding crate, the host runtime, a conformance probe or its ledger, a host suite or its
inventory, `validate-client-suites` or the shape of a host guide keep this chapter current. The
[Client Session Protocol](client-session-protocol.md) owns the exchange with a node; the
[Client Implementation Manual](client-implementation-manual.md#using-the-c-abi) owns normative host
requirements.

Application APIs and executed examples belong in the [Python](client-library-python.md),
[Ruby](client-library-ruby.md), [JVM](client-library-jvm.md) and
[C and C++](client-library-c.md) guides, which the [Client Libraries](client-libraries.md) index
introduces. The [conformance ledger](https://github.com/nervix-io/nervix/blob/main/tests/client-conformance-ledger.md)
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
| `tests/client_conformance` and the conformance ledger | External host probes, language-level suites, exact test inventories and evidence boundaries, as [Host Suites And Guides](#host-suites-and-guides) describes |

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

`Outcome::entity_labels()` supplies an independently retained `EntityLabelsInspection` for a
domain metadata read. Every native binding preserves the exact map, existing domain/node identity,
captured policy revision and limits, and explicit committed/staged view. A committed view has its
applied revision; a staged view has transaction id, accepted position and planning-basis bytes.
Absent variant fields remain absent, and zero-valued revisions or positions remain present.
Pointer/length accessors preserve empty values and embedded NUL bytes. No binding derives this
typed result from the display message. The registered host suites exercise exact values, staged
inspection and revert, and views retained after their outcome is released.

`Outcome::state_inspection()` supplies the existing owned `Inspection`. Its state is the
verified `StateInspection`, and its cached JSON is that envelope's lossless serialization.
Bindings retain this owner for page/refusal, entity, complete counts, unit identity, ordered
replicas, captured origin, section and opaque continuation access. They add no second inspection
representation and derive no typed value from display text.

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
minitest, and the C/C++ registered native tests. [Host Suites And Guides](#host-suites-and-guides)
owns their tiers, results, completeness check, guides and placement in CI. A probe's report is not
coverage of every host API, and a public-member reference is not a behavioral assertion.

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

## Host Suites And Guides

A probe holds every runtime to one report. A host suite holds one library to the behaviour its
guide documents, in its host's own idiom and test framework, and asserts values and failures
rather than report lines. The guide teaches that behaviour to an application developer, and every
example it shows is a test of the suite. Every binding host has both: the Python package, the JVM
library, the Ruby gem, and the C ABI from C11 and from C++17.

| Owner | Responsibility |
| --- | --- |
| `tests/client_conformance/<host>/suite` | One suite per host beside its probe: pytest for `python`, JUnit 5 for `java`, minitest for `ruby`, and the registered test tables of checked C11 and C++17 programs for `c` and `cpp`, each split into a `boundary` and a `cluster` tier |
| `docs/src/client-library-{python,jvm,ruby,c}.md` | One guide per host language under the [Client Libraries](client-libraries.md) index; C and C++ share one |
| `tests/client-suites-inventory.toml` | Every suite test by host, tier and stable identity |
| `scripts/client_suites.py` and its host readers | `just validate-client-suites`, which holds each suite to its binding's public surface, the inventory to the suite sources, the Python stub to the package and every guide to the shared shape; and `check-run`, which judges one tier's results against the inventory |
| `tests/features/runtime/client_suites.feature` and `tests/common/client_suites.rs` | The cluster tier: one outline per host on one and three nodes, the runner's environment, declared scenario actions and the run's verdict against the inventory |

### Two Tiers

The boundary tier needs no cluster. It covers construction, enumerations, builders and readers at
their bounds, Arrow exchange, typed failures from invalid arguments, a connect cancelled from
another thread or ended by its deadline against a listener that never answers, and lifetimes under
explicit collection. `just test-client-<host>-api` runs one host's boundary tier, for `python`,
`jvm`, `ruby`, `c` and `cpp`, and `just test` runs all of them: Python's and the JVM's through
their prepared-run recipes, Ruby's through the `host_suite` test of `nervix-client-ruby`, and C's
and C++'s through the `native_suites` test of `nervix-client-ffi`, which runs each native program
both ordinarily and under AddressSanitizer and UndefinedBehaviorSanitizer.

The cluster tier runs as one outline per host in `client_suites.feature`, on one- and three-node
clusters whose session listener serves HTTPS, and for the JVM, Ruby, C and C++ also HTTP. Its
graph is the conformance graph's shape, and a suite creates what a test changes itself. The
scenario starts the host's runner on a follower when the cluster has one, with the probe's
environment and what a suite needs beside it: the leader's listener to relay a session through,
every node as a seed, the fixture certificate authority and a backup directory. C and C++ run
their sanitized programs. A running suite can ask its scenario for a declared testing-build
action and wait for its numbered confirmation; [Client
Suites](integration-test-lifecycle.md#client-suites) owns that exchange and the run's bounds.
`just test-client-<host>-cluster` runs one host's rows, for `python`, `jvm`, `ruby`, and `c`
for both native hosts, and `just test-client-conformance` runs them beside the probes.

In CI, the ordinary `tests` job runs every boundary tier from prepared artifacts, and the ordinary
scenario shards also run Python's cluster rows from the wheel their archive carries. The JVM,
Ruby, C and C++ rows need toolchains and artifacts that only the `client-conformance` job
prepares, so they run under that label beside the probes. Every runner that starts host
clusters first runs the prepared boundary tiers, so an artifact that fails its boundary tier stops
the run before a cluster starts.

### Results And The Inventory

A runner prints `NERVIX-SUITE <outcome> <identity>` once a test's teardown ends, where the outcome
is `passed`, `failed` or `skipped` and the identity is the test's stable name: a pytest node id
relative to the suite, such as `boundary/test_packaging.py::test_the_package_loads_without_pyarrow_or_polars`,
a JUnit `class#method`, a minitest `Class#method`, or a C or C++ test function. The suite owns the
adapter that prints it: the `Results` plugin of the pytest `conftest.py`, a registered JUnit
`TestExecutionListener`, a minitest reporter, and the native programs' test tables. A line on
standard output serves every framework without another dependency: minitest writes no
machine-readable report without an extra gem, and a C program has no reporter.

`scripts/client_suites.py check-run` judges a boundary run and the scenario step judges a cluster
run by the same rule. Every test the inventory registers for that host's tier ran once and passed,
nothing was skipped, nothing ran that the inventory does not register, and the runner exited
successfully. A test cannot disappear silently, pass by being skipped or run unregistered. Every
identity names one test, so parametrized pytest tests are refused.

### Completeness

`just validate-client-suites`, part of `just validate` and `just validate-ci`, reads each binding's
public surface from its source and names every member that no test of that host references:

- Python: pyo3 classes, methods, getters, renames, enumeration members and registered exceptions,
  with references resolved by a mypy plugin over the typed suite.
- JVM: every public type, constructor, method, record component and enumeration constant of
  `io.nervix.client`, with references resolved from the Java compiler's trees.
- Ruby: every class and method the magnus extension registers, against Prism-parsed constant and
  typed-receiver references.
- C and C++: every function, typedef and enumerator of `nervix_client.h`, against Clang-resolved
  references from both programs.

There is no allowance list: a member a suite cannot reach is a defect in the member or in the
suite. The same check holds the inventory to the suite sources, so every registered identity
exists and every test is registered; holds the Python stub to the package; and holds every guide
to its shape. A reference shows that a test uses a member, not that it asserts all of its
behaviour.

### The Guides

A guide is written for an application developer who has not read the protocol chapter: it
explains how to write the code, not how the binding is built. Every guide has the same sections in
the same order, which `validate-client-suites` checks: Install, Connect, Execute Plexus, Complete A
Statement, Subscribe And Read Rows, Follow A Domain Clock, Publish Through A Client Ingestor,
Consume A Client Emitter, Back Up And Restore, Errors, Threads, Cancellation And Deadlines,
Lifetimes And Memory, Arrow Interop, What The Library Does Not Expose, and A Complete Application.
A developer who knows one guide finds the same answer at the same place in the next. The C and
C++ guide shows a C++ example beside the C one wherever ownership differs.

Every code block of a guide is one mdBook include of an anchored test of that host's suite, and
the check verifies that the file is a source of that suite and that it holds the anchor. A
documented call that stops working therefore fails a test, and a guide never shows code its suite
does not run. A behaviour a suite cannot demonstrate is a documentation defect, fixed in the
change that finds it. The guides own host how-to. The manual keeps each host's normative rules and
links its guide, this chapter keeps the internals, and the [Rust Client Library](client-library.md)
keeps the Rust API, with no host section.

### Typing And Packaging

The wheel ships the hand-written stub `crates/client-python/nervix_client.pyi` as
`nervix_client/__init__.pyi` beside a `py.typed` marker, so mypy and pyright check an application
against the package's real surface instead of `Any`. `validate-client-suites` holds every class,
member, parameter name, keyword-only marker and default of the stub to the pyo3 source, and type
checks the suite and the paced simulation driver against it with `mypy --strict`. The JVM's types
are the jar's own Java types, C and C++ read the declarations of `nervix_client.h`, and the Ruby
gem ships no type signatures.

Each host checks its artifact as an application loads it, before its other tests rely on it:

| Host | Packaging check |
| --- | --- |
| Python | Boundary tests load the wheel from `python-bare`, an environment holding the package and nothing else, so it imports without pyarrow or polars, and find its stub and typed marker |
| JVM | The cluster tier's first test starts a fresh JVM under `-Xcheck:jni` that loads the jar without JUnit, an explicit native-library path or a second application library |
| Ruby | Each tier's runner first loads the gem from the fresh `GEM_HOME` its build installed it into, checking its ABI and version, and runs no other test when that fails |
| C and C++ | `just build-client-c-suites` compiles and links the header from C11 and from C++17 with `-Wall -Wextra -Werror -pthread`, ordinary and sanitized, before any test runs |

Installation stays in the recipes: no test runs Cargo, a compiler or a package manager, and
`scripts/scenario_shards.py` packs the wheel the ordinary shards install for Python's rows.

### What Each Instrument Proves

| Instrument | Proves | Leaves to the others |
| --- | --- | --- |
| Host suite | One library behaves as its guide documents, in its host's idiom: values and typed failures, context managers, try-with-resources and blocks, `__repr__`, `toString` and `inspect`, equality and hashing, optional values, threads and cancellation from another thread, lifetimes under collection, packaging, and a reference to every public member | How another runtime reads the protocol, conversions of generated values, and whether a referenced member's whole behaviour is asserted |
| Conformance probe | Every runtime prints one expected report on one and three nodes: typed rows and operation outcomes for every runtime, the independent Go and TypeScript clients included, and attached clocks, generations and producer and consumer exchange for every binding host | The library surface and host idioms beyond that report |
| Bolero property | A conversion its owner performs keeps every bounded generated value: the shared host's Arrow interface, the C, JNI and Ruby column and field boundaries, and the typed inspection views | Behaviour against a cluster |
| Binding owner test | What a live cluster cannot provoke on demand, such as a refused clock restoration, a domain-removed clock end or a settlement whose answer was lost | That an application reaches it through the published API in a separate process |

### Limits Of The Suites

A suite observes only what its graph, its own statements and the declared testing-build actions
provoke. The one declared fault refuses the next subscription opening on every node, so every
suite reads a refused subscription restoration live; a refused clock restoration and a
domain-removed clock end are binding owner-test evidence. A passing suite qualifies its host on the revision it ran; C and C++
sanitizers judge the host programs' use of the ABI while the Rust library stays uninstrumented,
and Deloxide does not observe the host processes. The suites run the libraries the repository
builds; no package registry or asynchronous adapter is involved.

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
