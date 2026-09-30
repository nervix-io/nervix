# Client wire cross-language conformance ledger

This is the executable ledger of [Client Wire 14](https://app.clickup.com/t/86bc1ahyr), which
[Clock Attach 05](https://app.clickup.com/t/86bc8azn1) extended with the domain clock events the
binding exposes, [Clock Attach 05A](https://app.clickup.com/t/86bc95p44) with the clock an attach
reports, and [Client I/O 04](https://app.clickup.com/t/86bc8bj89) with the producers and consumers
of client endpoints. It records which runtimes speak the finalized session protocol, whether each
does so as an independent native client or through the shared Rust binding, the commands that build
and run each one, and what the qualification found. It does not promise a separately maintained
production SDK for any language: the probes are qualification clients, and the binding is the
supported way for Python, JVM, Ruby, C and C++ hosts to reuse the Rust session.

## Paths

| Runtime | Path | Transport | Probe |
| --- | --- | --- | --- |
| C ABI, in process | Shared Rust binding | Native gRPC | `tests/common/client_conformance/c_abi_probe.rs` |
| C (C11) | Shared Rust binding | Native gRPC | `tests/client_conformance/c/probe.c` |
| C++ (C++17) | Shared Rust binding, owned through `std::unique_ptr` | Native gRPC | `tests/client_conformance/cpp/probe.cpp` |
| CPython 3.12+ | Shared Rust binding through `ctypes` | Native gRPC | `tests/client_conformance/python/probe.py` |
| Java 22+ | Shared Rust binding through the Foreign Function and Memory API | Native gRPC | `tests/client_conformance/java/Probe.java` |
| Ruby 3.3+ | Shared Rust binding through Fiddle | Native gRPC | `tests/client_conformance/ruby/probe.rb` |
| Go 1.25+ | Independent native client, `flatc --go` and `google.golang.org/grpc` | Native gRPC | `tests/client_conformance/go/` |
| Node.js 24 | Independent native client, `flatc --ts` and the built-in `WebSocket` | Binary WebSocket | `tests/client_conformance/node/probe.ts` |
| Bun 1.4 | The same TypeScript client, unchanged | Binary WebSocket | `tests/client_conformance/node/probe.ts` |

The shared Rust binding is `crates/client-ffi`, whose contract is
`crates/client-ffi/include/nervix_client.h`. Every binding host drives the Rust client's own state
machine, so leader retry, request correlation, reconnection, execution identity and subscription
restoration are the ones `nervix-client-core` tests, and no host reinterprets an uncertain outcome.
The Go and TypeScript clients implement the protocol themselves: they correlate replies by request
identity, follow a leader redirect with the same execution reference, and fail rather than report
success when an outcome is unknown. Other JVM languages reach the binding through the same Foreign
Function and Memory API as Java; Kotlin and Swift code generation from the schema was checked, and
neither is probed.

## Build and run

`just test-client-conformance` builds every artifact below into `target/client-conformance` and runs
every probe against one- and three-node clusters. Its argument selects runtimes by tag, for example
`just test-client-conformance '@client_probe_python or @client_probe_bun'`. The in-process probe
runs in the ordinary scenario suite. The CI job `client-conformance` runs the whole ledger.

| Runtime | Build | Run |
| --- | --- | --- |
| Shared binding | `cargo build --package nervix-client-ffi` | Loaded from `target/debug/libnervix_client_ffi.so` |
| C | `cc -std=c11 -Wall -Wextra -Werror -pthread -I crates/client-ffi/include tests/client_conformance/c/probe.c -L target/debug -lnervix_client_ffi -Wl,-rpath,target/debug -o target/client-conformance/c-probe` | `target/client-conformance/c-probe` |
| C++ | `c++ -std=c++17 -Wall -Wextra -Werror -pthread -I crates/client-ffi/include tests/client_conformance/cpp/probe.cpp -L target/debug -lnervix_client_ffi -Wl,-rpath,target/debug -o target/client-conformance/cpp-probe` | `target/client-conformance/cpp-probe` |
| Python | None | `python3 tests/client_conformance/python/probe.py` |
| Java | None; the launcher compiles the source file | `java --enable-native-access=ALL-UNNAMED tests/client_conformance/java/Probe.java` |
| Ruby | None | `ruby tests/client_conformance/ruby/probe.rb` |
| Go | `flatc --go` into a copy of `tests/client_conformance/go`, then `go build` | `target/client-conformance/go-probe`, or `go-probe corpus <dir>` |
| Node.js | `flatc --ts`, `npm ci`, then `esbuild probe.ts --bundle --platform=node --format=esm` | `node target/client-conformance/node/probe.mjs`, or with `corpus <dir>` |
| Bun | The Node.js bundle; Bun is the release the probe's `package-lock.json` pins | `target/client-conformance/node-src/node_modules/.bin/bun target/client-conformance/node/probe.mjs` |

A binding probe reads `NERVIX_CLIENT_LIBRARY`, and every probe reads its target from
`NERVIX_PROBE_GRPC_URI`, `NERVIX_PROBE_WEBSOCKET_URI`, `NERVIX_PROBE_USERNAME`,
`NERVIX_PROBE_PASSWORD` and `NERVIX_PROBE_DOMAIN`, and its subscription from `NERVIX_PROBE_RELAY`,
`NERVIX_PROBE_SUBSCRIPTION` and `NERVIX_PROBE_ROWS`. Run with the `clock` argument, a binding probe
follows the domain's clock instead and reads no subscription. Run with the `io` argument, it
publishes to the ingestor `NERVIX_PROBE_INGESTOR` names, consumes the emitter
`NERVIX_PROBE_EMITTER` names, and reads no subscription either.

## What every probe proves

`A <runtime> client round-trips an operation, typed rows, an error and a closure` runs each runtime
against a one- and a three-node cluster, starting on `node-1`, which is a follower in most
three-node runs. Every probe prints the same report and the scenario holds all of them to one
expected report:

- An operation: `SHOW CREATE RELAY` completes.
- An error: a malformed statement fails with one diagnostic at the exact source span.
- The schema a subscription announces: every field's name, type, nullability and sensitivity, the
  declared branch and its key fields.
- Typed rows from two interleaved concrete branches: every integer width at both extremes, 64-bit
  values on both sides of the JavaScript safe-integer boundary, an absent and a present-zero optional
  value, multi-byte text with an embedded NUL, bytes that are not UTF-8, empty text and bytes,
  `-0.0`, the smallest subnormal and the largest finite floats by their bits, the extreme DATETIME
  nanoseconds, and a sensitive field that is always redacted. The ingestor passes integer, text,
  bytes, datetime, optional and sensitive fields through `coalesce`, `concat`, hex encode/decode,
  and `nullif` calls before the client reads them, so the report covers function output through
  the final transport as well as the transport's scalar encoding.
- A closure: deleting the subscription completes.

`A <runtime> client reads the running domain clock it attached to before its ticks and keeps its
generations apart` runs every binding host, the C ABI in process, C, C++, Python, Java and Ruby,
against a one- and a three-node cluster. The scenario starts a paced domain at a logical origin with
a time rate and holds the clock authority's first progress, which proves the owning node and that
every live node has installed the generation. The probe then attaches through a TCP forwarder in
front of `node-1`, which does not own the clock in a three-node cluster, and every probe prints the
same report:

- The attachment: `ATTACH DOMAIN CLOCK;` completes through `nx_session_execute`.
- The clock the attach reported, read with `nx_session_domain_clock` before any event: the domain,
  the generation, the paced state, and the period, skew, logical origin and time rate the scenario
  started the clock with, the rate by its bits.
- The projections of that clock at its own UTC anchor: the logical time there is the origin, one
  period's wait at the scenario's rate, the admission window of the first tick center alone, and
  an event at the skew's edge admitted where one nanosecond past it is refused.
- The first tick, read from its `NX_CLOCK_EVENT_TICK` event and held to the clock the attach
  reported: the same generation, a boundary of the logical origin plus one period for every id
  before it, and a serving node's logical reading that never precedes the origin. A read taken
  after it holds that tick or a newer one of the same generation.
- After the scenario runs `STOP` and a new `START` at another origin and rate, the paced state of
  the new generation from its `NX_CLOCK_EVENT_STATE` event, then that generation's first tick. A
  tick of the old generation after its stopped state, or of the new one before its paced state,
  fails the probe, and so does a read older than a state the probe took. The stopped state and a
  state reporting the new generation uninstalled can be coalesced away, so they are checked, not
  printed.
- After the scenario stops the forwarder, the interruption, then the paced state the restored
  attachment reports, then the first tick after it, each held to the same rules.
- The detachment: `DETACH DOMAIN CLOCK;` completes.

Tick ids, the authority's UTC observation and the UTC anchor depend on when the scenario ran, so
they are checked rather than printed.

`A <runtime> client publishes typed batches through a client ingestor and acknowledges their output
through a client emitter` runs every binding host, the C ABI in process, C, C++, Python, Java and
Ruby, against a one- and a three-node cluster, through a TCP forwarder in front of `node-1`. The
client ingestor branches its rows by tenant, and the client emitter inherits every field, adds an
`echo` of the identifier and leaks the sensitive one, under a sequential window of one attempt.
Every probe prints the same report:

- The opens: a producer and a consumer whose expected fields differ from the endpoint's only in one
  field's sensitivity are both refused as a schema mismatch, read with `nx_error_open_refusal`.
  Then the opened consumer's and producer's generation, state, admission, window, ACK timeout, grant
  and batch limits, and every field of both schemas with its nullability and sensitivity, a list
  type by its levels.
- The waits: a read of no output ends by its deadline, and one cancelled from another thread by its
  token.
- A batch of the wrong schema: built for the consumer's schema, it is refused as the host's
  argument before anything is sent, and the same batch submitted as the stream other Arrow tooling
  would write is refused by the server as an invalid batch of another schema.
- The typed batch, built column by column: every integer width at both extremes, 64-bit values on
  both sides of the JavaScript safe-integer boundary, `-0.0`, the smallest subnormal and the largest
  finite floats by their bits, multi-byte text, text with an embedded NUL, bytes that are not UTF-8,
  empty and null text and bytes, the extreme DATETIME nanoseconds, an absent and a present-zero
  optional value, a `VEC<STRING>` holding an empty list and an empty string, an `ARRAY<I16, 2, 2>`
  at the `I16` extremes, and a nullable `VEC<ARRAY<DATETIME, 2>>` that is null, empty, and at the
  extremes. Its submission stays unresolved while its output waits, and the output rows are read
  from the delivery one level at a time.
- The settlements: a retry comes back with the same identity and a new reference, the first
  reference then settles as stale, and the retained one acknowledges; the submission completes.
  A rejected delivery finishes its submission as a processing failure the application rejected.
- The credit: two outstanding submissions take the producer's credit, so a third waits and ends by
  its deadline without being submitted; once both are acknowledged it is submitted and completes.
- The session loss: while one delivery is held unacknowledged, the scenario stops the forwarder.
  The consumer's read reports the interruption, the held delivery can no longer be settled, its
  submission's outcome is unknown because the session was lost, and a producer closed during the
  reconnect stays closed. When the forwarder returns, the consumer receives the held batch again
  under the same identity and the restored producer publishes one more batch, which completes.
- The closure: both handles close, and the endpoints report no producer, no consumer, and no
  outstanding or retained batch.

Beyond the shared report, each kind of probe checks what only it can reach:

| Probe | Checks |
| --- | --- |
| Binding hosts | Rows survive on a retained reference after the first is released, and read identically after collections, allocation churn, and release on another thread. Every column is copied in one call, and every string and bytes value borrowed from the frame equals its copy. A wait cancelled from another thread reports `NX_ERROR_CANCELLED`, an expired deadline `NX_ERROR_DEADLINE`, and a cancelled command keeps its execution reference. A clock wait of a session that follows no clock ends the same two ways, and the clock the attach reported and the first tick event read identically on a retained reference after the first is released on another thread. |
| Binding hosts, producers and consumers | Every buffer a host passes a batch builder is overwritten as soon as the call returns, and the rows the consumer reads are still the ones the host built. The stream a host passes `nx_producer_submit_ipc` is overwritten or released as soon as the call returns, and the server still refuses it for its schema rather than as malformed. A delivery's batch borrows the stream the delivery carried. A delivery and its batch retained before the first reference is released on another thread read the same stream and rows, and the retained delivery settles the attempt. A submission's pending list names the one waiting submission, and a wait for its outcome ends by its deadline while its output is unacknowledged. |
| Python | The frame is a `memoryview` whose buffer keeps the event alive; it stays readable after every other reference is dropped and collected. |
| Java | References retained into automatic arenas are released by the collector while other references to the same event are read. A view of a released event throws instead of reading freed memory. |
| Ruby | References nothing reaches are released by the collector's free function while retained references are read. |
| Go, TypeScript | A request that omits the required subscription type is rejected rather than defaulted, and cancelling an identity that is not in flight reports `NotInFlight`. The TypeScript client reads every 64-bit value as a `BigInt`. |

## Batch buffers

What the binding copies on the way from a host's buffers to the session and back:

| Path | Copies the binding makes |
| --- | --- |
| A batch a host builds | Each builder call copies the buffer it receives before it returns. Finishing assembles the Arrow arrays from those copies without another, and submitting writes the canonical stream once. |
| A stream a host writes | `nx_producer_submit_ipc` copies the stream once before it returns. |
| A delivery's stream | None: `nx_delivery_ipc`, and `nx_batch_ipc` of its batch, borrow the stream the reply carried until the last reference is released. |
| A delivery's columns | `nx_delivery_batch` decodes the stream once, and each read copies one whole level of a column into the host's buffer. |

## The corpus

`crates/client-wire/conformance` holds frames the Rust encoder writes and `corpus.report`, the report
of them. `just test-client-wire` checks that the checked-in bytes are exactly what the encoder writes
and that decoding them prints exactly the report; `just update-client-wire-corpus` regenerates both
for review. The corpus covers what the live relay does not: fixed and variable lists, nested lists,
a NaN with a payload, infinity, a nullable and a sensitive branch key field, command outcomes with
diagnostics, a leader redirect with and without a known leader, an unknown outcome, a rejection, a
subscription ending, and client requests with a present-zero and an absent optional position, a
preview fingerprint, and the largest request identity. It also covers the domain clock attachment:
the attach and detach requests, a paced clock attached at the largest generation with the extreme
signed timestamps and a time rate read by its bits, clock frames in the stopped, uninstalled and
unpaced states, every refusal of an attach and a detach, and the end of an attachment with its
typed reason. A tick frame carries its generation, id, boundary, authority UTC observation, and
serving node logical reading through the Rust, Go, and TypeScript readers. Producers are covered by
the open, submit, and close requests with a batch carried as opaque bytes, an opened producer with
its description under a parallel window, a refused open, a closed producer, a submission of
each of the four outcome classes with a typed cause, and the admission-change and end frames of a
producer.

`A <runtime> client reads every frame of the conformance corpus the Rust encoder wrote` holds the Go
reader and the TypeScript reader on Node.js and Bun to the same report. Together with the live
scenarios, where the Go and TypeScript clients write every request the Rust server reads, this is
the independent interoperation with Rust in both encoding directions.

## Findings

| Finding | Resolution |
| --- | --- |
| `Fingerprint` was a struct holding a fixed-length byte array, which `flatc` cannot generate for Go, Kotlin, Swift or Dart. | `Fingerprint` is a table holding a byte vector of exactly 32 bytes. The Rust reader refuses any other length, and the corpus readers check it. |
| JavaScriptCore, which Bun runs on, canonicalizes a NaN it materializes as a Number, and V8 keeps the payload; the generated `value()` of a float cell returns a Number. | A JavaScript client that needs exact float bits reads the scalar's bits, as the TypeScript probe does. |
| The Go and TypeScript FlatBuffers runtimes have no verifier. | Their probes check each frame's identifier and every union discriminant and required value they read. The Rust verifier remains the only full structural verification. |
| Dart code generation rejects optional scalars, which the schema uses for every required enum. | Dart is not a target runtime. |
| Under load, the outputs of two batches a producer had outstanding at once sometimes arrived in the other order, with one- and three-node clusters alike. The node refuses a batch as `Busy` when it cannot reserve memory or a worker to validate it, and the client sends that batch again after the backoff, behind the batch submitted after it. | Nothing orders batches outstanding at once, and [Ordering And Backpressure](../docs/src/client-session-protocol.md#ordering-and-backpressure) and P-5 of the implementation manual now say so. The probes check that each of the two outputs keeps its rows, in either order. |
| In three-node runs, a consumer whose emitter executed on another node sometimes saw its attachment end right after an acknowledgement or across a reconnect. The serving and owning ends of the consumer stream select each receive against heartbeats and settlements, and a receive dropped while its frame was still decoding lost that frame, so a delivery's start or chunk went missing and the serving node ended the stream. | Duplex receives are cancel-safe: a receive keeps the decoding it started, and the next receive finishes that frame first. `a_receive_abandoned_while_its_frame_decodes_leaves_the_frame_to_the_next_receive` and `a_handler_read_abandoned_while_its_frame_decodes_leaves_the_frame_to_the_next_read` in `nervix-interconnect` hold both ends to it. |

## Limits

- The binding's column access to a subscription's Row events covers scalar, string and bytes
  fields. A list field reports its shape, and its values are read from the borrowed frame with
  generated code. A producer's or consumer's batch reads and writes every field, a list one level at
  a time.
- The loss of an acknowledgement's answer cannot be timed against a live cluster, so the probes do
  not cut a session between the server applying a settlement and its answer. The binding's unit
  test `a_consumer_reads_and_settles_a_delivery_and_a_lost_confirmation_is_uncertain` does, and the
  settlement reports `NX_ERROR_UNCERTAIN`.
- The probes connect over plaintext. TLS selection is the Rust client's and is not reimplemented by
  the binding.
- C and C++ consume the binding. No independent C or C++ reader is maintained.
- Only the Rust client restores subscriptions and domain clock attachments after reconnection. The
  Go and TypeScript clients show that the protocol is implementable, not that they recover; a
  binding host inherits the Rust client's recovery.
- An attach refusal reaches a binding host as a failed disposition and the server's message, not
  as a typed refusal; `nx_session_domain_clock` tells whether the session follows the clock after
  it.
- The Go and TypeScript clients read the attach reply and the clock frames only from the corpus;
  no live probe of theirs follows a domain clock.
