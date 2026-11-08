# C And C++ Client Library

*Plexus, the Nervix graph definition language.*

The Nervix C ABI runs the [Rust Client Library](client-library.md) in your process. C and C++
programs use the same session, execution recovery, subscriptions, clocks and client endpoints as
other Nervix clients. Include `nervix_client.h` and link the shared binding. The
[Client Implementation Manual](client-implementation-manual.md#using-the-shared-binding) gives
the normative B-1 to B-16 host rules.

Every example below comes from the C11 or C++17 application suite under
`tests/client_conformance/{c,cpp}/suite`. The common cases compile and execute in both languages;
C++ also tests typed `std::unique_ptr` owners. `suite_check` checks and releases a returned error,
`HOLD` registers an owned reference, and `suite_drop` releases it. Their implementations and setup
are part of the executable suite. Replace these helpers with your application's error and ownership
handling when using an excerpt.

## Install

Build from the Nervix repository today. `just build-client-c-suites` runs
`cargo build --package nervix-client-ffi` through the repository's build configuration and compiles
the example programs. The public header is `crates/client-ffi/include/nervix_client.h`; the native
library is `target/debug/libnervix_client_ffi.so` on Linux, or the platform's shared-library form.
Your compiler needs the header's directory on its include path and the library's directory on its
link and runtime loader paths, with `nervix_client_ffi` as the library name.

The supported example floors are C11 and C++17. The host programs use POSIX threads for their
threaded examples. The recipe builds ordinary programs and programs with AddressSanitizer and
UndefinedBehaviorSanitizer; `just test-client-c-api` and `just test-client-cpp-api` run both boundary
tiers. These development checks require a compiler with both sanitizers. The header itself has no
Arrow, Tokio or C++ dependency.

## Connect

Create options, copy in credentials and a PEM certificate authority, then connect to a session URI
and domain. HTTPS uses TLS; `NX_TLS_REQUIRED` rejects a plaintext address, while
`NX_TLS_PREFERRED` follows the address's scheme. A session first opened over HTTPS keeps that
requirement for every seed and redirect. Credentials and certificate bytes are copied; keep the
options unmodified during the connect call and release them once it returns (B-4).

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:connect_options}}
```

The four timeout selectors bound one connection, one request, the whole retrying call, and waiting
for a backup archive. Their values are milliseconds, validated from 1 ms through 24 hours. Add
further session URLs with `nx_connect_options_add_seed`. The suite supplies its fixture credentials,
certificate path and server URLs through environment variables.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:connect}}
```

C++ transfers the returned handle into its typed owner; the aliases are shown in
[Lifetimes And Memory](#lifetimes-and-memory).

```cpp
{{#include ../../tests/client_conformance/cpp/suite/main.cpp:cpp_connect}}
```

## Execute Plexus

Prepare a durable logical command once. If a wait ends as cancelled, expired or uncertain, retain
that `nx_execution` and execute it again to learn the command's outcome (B-1). A new execution is a
new logical command. Inspect `nx_outcome_disposition` as well as the call's error: a successful call
can return a failed command with diagnostics. Execution references are borrowed UTF-8 bytes.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:execute}}
```

Cancellation ends a wait; it does not roll back a transaction the server already admitted. The
suite holds an admitted commit behind an unsettled delivery, cancels its wait, then recovers it
through the same execution and checks the resulting configuration.

## Complete A Statement

`nx_session_suggest` takes the input and cursor, then returns replacement ranges and candidates.
Follow its continuation until the page is complete, preserving the requested input and cursor.
Use pointer and length accessors for the candidate text and release each `nx_suggestions` object.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:completion}}
```

The continuation borrows its page. Keep that page until the next request returns; the binding
copies the token during the call. This test reads every two-candidate page with a 64-page guard.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:completion_pages}}
```

## Subscribe And Read Rows

Create a subscription with a session-local statement. Its outcome carries the subscription's name,
generation and schema. Read `nx_event` objects until a rows event for the subscription arrives.
Each event is owned; any borrowed frame, string or value requires a live event reference (B-2,
B-3). Read strings as byte spans, including embedded NULs, and prefer whole-column accessors (B-4,
B-6). A null data buffer asks the variable-length accessor for its required size.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:columns}}
```

An interrupted event names a subscription gap: rows between sessions are unavailable. A restoration
failure carries a typed cause, message and retry wait; an ended event is the end of that generation.
A consumer overflow also ends delivery of that subscription on the session. Handle every kind under
[B-7](client-implementation-manual.md#using-the-shared-binding); accessor calls for another event
kind fail with `NX_ERROR_TYPE`.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:subscription_gap}}
```

Read the cause, borrowed UTF-8 message and retry wait from a temporary restoration refusal.
The wait is in nanoseconds. Keep reading: the same subscription can deliver rows after a later
opening succeeds. This case observes that recovery through the live cluster.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:restoration_failed}}
```

## Follow A Domain Clock

The shared C binding reads the same events and attached clocks for C and C++ hosts; its
header is `crates/client-ffi/include/nervix_client.h`. `ATTACH DOMAIN CLOCK;` and
`DETACH DOMAIN CLOCK;` run through `nx_session_prepare` and `nx_session_execute` like any statement,
for the session's selected domain, and fail with `NX_ERROR_INVALID_ARGUMENT` when none is selected.
`nx_session_next_clock_event` waits for the next event, bounded by the same `nx_cancel` tokens and
deadlines as every blocking call, and hands out an `nx_clock_event` reference. While a followed
clock waits to be restored it opens a session; when none can open, it fails with `NX_ERROR_CLOSED`
if no server is known and otherwise with the connection, transport or deadline failure of that
attempt, and the next call tries again. The events are the Rust client's, coalesced the same way:

- `nx_clock_event_kind_of` tells an `NX_CLOCK_EVENT_STATE`, `NX_CLOCK_EVENT_TICK`,
  `NX_CLOCK_EVENT_ENDED`, `NX_CLOCK_EVENT_INTERRUPTED` or `NX_CLOCK_EVENT_RESTORATION_FAILED` event
  apart, and `nx_clock_event_domain` borrows the name of the domain it concerns. A restoration
  failure reports that the session refused to attach an interrupted clock again, which it tries
  again after the growing wait the Rust client reports. `nx_clock_event_restoration_cause` reads its
  cause, `NX_RESTORATION_IN_TRANSACTION`, `NX_RESTORATION_REJECTED` or `NX_RESTORATION_FAILED`. The
  event also carries the refusal message as borrowed UTF-8 bytes and that wait in nanoseconds, read
  with `nx_clock_event_restoration_message` and `nx_clock_event_restoration_retry_after`.
- `nx_clock_event_generation` reads the `START` generation of a state or tick event,
  `nx_clock_event_state` the installation state of a state event, `nx_clock_event_paced` the period,
  skew, logical origin, UTC anchor and time rate of a paced one, `nx_clock_event_tick` the id,
  logical boundary, authority UTC observation and serving node's logical reading of a tick, and
  `nx_clock_event_end_reason` why the server ended an attachment. Each fails with `NX_ERROR_TYPE`
  for an event whose kind does not carry what it reads.
- Instants are signed nanoseconds since the Unix epoch, the period and skew unsigned nanoseconds,
  and the time rate a `double`. Every field is written to an out-parameter the host provides, so
  reading an event allocates nothing on the host's side.
- `nx_clock_event_retain` and `nx_clock_event_release` count references the way `nx_event_retain`
  and `nx_event_release` do, and a reference may be released on any thread.

For C and C++ hosts, `nx_connect_options_new` starts with the Rust client's defaults. The host
may set basic credentials, a TLS requirement, a copied CA certificate, seed URLs and the four
deadlines before it calls `nx_session_connect`. The session copies the options, so the host may
release them after the call. The Rust client validates endpoint origins and the 1 ms to 24 hour
deadline bounds when connecting, and an HTTPS initial endpoint requires HTTPS for every later
server. A subscription's `NX_EVENT_RESTORATION_FAILED` carries its refusal message and retry wait
through `nx_event_restoration_message` and `nx_event_restoration_retry_after`, with the same
borrowed-byte and nanosecond rules as the clock event. A wrong event kind returns `NX_ERROR_TYPE`.

`nx_session_domain_clock` reads `domain_clock` for a domain the session follows as an
`nx_domain_clock` reference, and writes NULL for a domain whose clock the session does not follow.
It never blocks. Right after `ATTACH DOMAIN CLOCK;` completes it is the clock the attach reply
carried, so a host that attaches to a running paced clock reads its `START` generation and
committed mapping before it uses the first tick. Every later observation and accepted tick replaces
it in the order the session received them, so a read is never older than an event the host has
already taken, including after a redirect moves the attachment to another session without an
interruption. Once the session holding the attachment ends, which `NX_CLOCK_EVENT_INTERRUPTED`
reports, it keeps the clock that session reported last, without a tick, until the restored
attachment reports the clock again. A read never
changes once taken, stays valid after the session is freed, and is counted with
`nx_domain_clock_retain` and `nx_domain_clock_release` like an event:

- `nx_domain_clock_domain`, `nx_domain_clock_generation` and `nx_domain_clock_state` read the
  domain, the generation and the installation state. `nx_domain_clock_paced` reads the period,
  skew, logical origin, UTC anchor and time rate of a paced clock and fails with `NX_ERROR_TYPE` for
  any other state, and `nx_domain_clock_tick` says whether the session holds an accepted tick of the
  clock's generation and reads it when it does.
- `nx_domain_clock_logical_time_at`, `nx_domain_clock_wall_duration_until`,
  `nx_domain_clock_admission_window` and `nx_domain_clock_admits` answer with the arithmetic of
  `AttachedDomainClock`: the logical time at a UTC instant, rounded down; the physical wait until a
  logical target, rounded up; the oldest and newest tick centers a `TIMESTAMP AT` ingestor admits
  events around; and whether it admits a given event time. An unpaced clock reads UTC and admits
  every timestamp. A stopped or uninstalled clock fails with `NX_ERROR_TYPE`, and a result outside
  the timestamp range with `NX_ERROR_INVALID_ARGUMENT`.

An attach the server refuses completes as failed with the server's reason, and
`nx_session_domain_clock` still reads the earlier attachment's clock when the refusal is that the
session already follows it. An attach whose session was lost is sent again on the next session the
client opens. An attach cancelled or expired after its request was sent can still attach the
session: executing the same prepared execution again sends another session-local request, which
may be refused as already attached even when the first succeeded. `nx_session_domain_clock` tells
whether the session follows the clock.

Read the attached clock immediately after attach and before using a tick (B-11). A retained clock
is an immutable snapshot, so acquire another after a state event or session restoration.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:clock_read}}
```

Use the snapshot's projection methods for their exact rounding and admission bounds instead of
copying the mapping arithmetic.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:clock_project}}
```

## Publish Through A Client Ingestor

The shared C binding exposes the same producers and consumers to C and C++ hosts,
through `crates/client-ffi/include/nervix_client.h`. Every call forwards to the Rust handles
above, so credit, retries of temporary refusals, restoration, outcomes and settlements behave
exactly as described; the binding keeps no acknowledgement or reconnect state of its own.

- **Opening.** A host describes the fields it expects with `nx_fields`: `nx_fields_add` starts a
  field with its name, outermost type, nullability and sensitivity, and `nx_fields_element` adds
  the element of each list around it, so `ARRAY<I16, 2, 2>` is a `FIXED_LIST` of 2, a `FIXED_LIST`
  of 2, then `I16`. `nx_session_open_ingestor` and `nx_session_subscribe_emitter` open a producer
  or a consumer on a domain named explicitly, with the credit the host asks for, and copy the
  fields. A refusal is `NX_ERROR_REJECTED`, and `nx_error_open_refusal` reads its
  `nx_open_refusal`, such as `NX_OPEN_SCHEMA_MISMATCH`.
- **Description.** `nx_producer_schema`, `nx_consumer_schema`, `*_generation`, `*_contract`,
  `*_grant` and `*_policy` read what the open established: the schema with every field's
  nullability, sensitivity and type levels, the `START` generation, the contract digest, the
  granted credit and batch limits, and the acknowledgement window, ACK timeout and backoff.
  `nx_producer_state` and `nx_consumer_state` read the connection, `nx_producer_admission` the
  admission, and `*_reopen_reason` why a handle needs a new open.
- **Submitting.** A host builds a batch from its own buffers with `nx_batch_builder_new` for the
  producer's schema: one `nx_batch_builder_states` call per nullable column, one
  `nx_batch_builder_offsets` call per variable-length list level, and one `nx_batch_builder_fixed`
  or `nx_batch_builder_varlen` call for the innermost values. Every call copies its buffers before
  it returns, and nothing is converted. `nx_producer_submit` waits for credit, checks the batch
  against the producer's schema and row limit, and returns the submission's identity;
  `nx_producer_rejoin` waits for its outcome and takes it, and `nx_submission_outcome_*` read its
  class, typed cause and message. `nx_producer_pending` lists what the producer holds and
  `nx_producer_release` lets a submission go. A host that writes canonical Arrow IPC itself submits
  it with `nx_producer_submit_ipc`, which copies it and leaves its checking to the server.
- **Consuming.** `nx_consumer_next` returns a reference-counted `nx_delivery`: its identity,
  reference, source relay, branch fingerprint, member count and execution time, its stream,
  borrowed with `nx_delivery_ipc`, and its batch, which `nx_delivery_batch` decodes the first time
  it is asked for and then shares. A batch is read one level of a column at a time:
  `nx_batch_states` for the rows, `nx_batch_offsets` for each variable-length list level, and
  `nx_batch_fixed` or `nx_batch_varlen` for the values, each in one call. `nx_delivery_ack`,
  `nx_delivery_retry` and `nx_delivery_reject` settle the attempt and return the server's
  `nx_settlement`. Releasing a delivery settles nothing.
- **Interruption.** After a session ends, the next `nx_consumer_next` returns
  `NX_ERROR_INTERRUPTED` once, a delivery read before the gap fails to settle with
  `NX_ERROR_REJECTED`, a submission sent before it is `NX_UNCERTAINTY_SESSION_LOST`, and a
  settlement whose answer the session lost is `NX_ERROR_UNCERTAIN`. The next calls restore the
  handles on a new session while their contract holds. A handle closed while it waits to be
  restored returns at once and is never restored.
- **Ownership.** Producers, consumers, deliveries and batches may be used from several threads at
  once, and deliveries and batches are released on any thread; a field list and a builder are used
  from one thread at a time. A producer, a consumer and a delivery keep their session running until
  they are released, and a borrowed stream or value stays valid until the reference it was read from
  is released.

Describe every field and list level exactly, including sensitivity and nullability. An open in a
transaction is refused. Read `nx_error_open_refusal` for its typed reason (B-12). Builders copy
each column buffer before returning, so the application can reuse it immediately (B-13).

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:build_submit}}
```

A submission owns credit until its outcome is taken with `nx_producer_rejoin` or released with
`nx_producer_release`. Cancelling a rejoin leaves that submission pending; use its original
identity to wait again. Inspect Completed, NotAdmitted, ProcessingFailed and OutcomeUnknown as
different classes, with their typed causes (B-14).

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:pending_rejoin}}
```

## Consume A Client Emitter

Read a delivery from an open consumer, inspect its identity and metadata, and obtain a retained
`nx_batch` to read its columns. Keep the delivery reference while borrowing its IPC stream.
Acknowledge, retry or reject each attempt explicitly; releasing an object settles nothing (B-15).
The returned `nx_settlement` says whether the server accepted that settlement.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:retry_delivery}}
```

A lost session interrupts a consumer once. A delivery from before that gap cannot be settled on
the replacement session. A producer can restore while its contract holds; a changed contract needs
a new explicit open with fields and credit checked again (B-16).

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:reopen}}
```

## Back Up And Restore

A backup statement names the local archive file. Keep its prepared execution if the call is
cancelled or uncertain; running that execution again recovers the outcome and downloads the archive
while the server retains it. `nx_outcome_backup` reports the byte length and BLAKE3 digest (B-8).

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:backup}}
```

Restore streams the file the statement names. The outcome reports dry-run status, restored domains,
resources, models and whether a step failed. Keep both the archive and the original execution until
its outcome is known; recovery streams the same archive again (B-10).

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:restore}}
```

## Errors

A fallible call returns NULL on success or an owned `nx_error *`. Read `nx_error_kind_of` and
`nx_error_message`, then release it with `nx_error_free`. Out-parameters are written only on
success. Branch on the typed kind and typed refusal or uncertainty accessors; message text is for
people. `nx_error_execution_reference` identifies the execution to retain after an uncertain,
cancelled or expired command. A server command failure is an outcome disposition with diagnostics,
which includes byte spans when parsing or validation failed.

The [Execute Plexus](#execute-plexus) example checks cancellation and the execution reference; the
[Publish](#publish-through-a-client-ingestor) and [Consume](#consume-a-client-emitter) examples
check endpoint and settlement results. No exception crosses the C ABI. C++ callers can turn an
error into an application exception after copying its message and releasing its owner.

## Threads, Cancellation And Deadlines

Calls that wait block the calling thread and take an optional `nx_cancel`. Trigger it from another
host thread, or create it with a millisecond deadline. Keep the token alive until every call using
it has returned. Cancellation does not consume a pending producer outcome or an unread delivery.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:cancel_connect}}
```

B-5 forbids calling the binding from a thread driving a Tokio runtime. Use ordinary application
threads for blocking calls. Sessions, producers, consumers, deliveries and batches may be shared;
field lists and builders are used from one thread at a time. Never release a handle while another
thread still uses that same owned reference. An independently retained event, clock, delivery or
batch reference may be released on any host thread.

## Lifetimes And Memory

Own every returned object and release each reference once with its matching function (B-2). A
borrowed byte span ends when its owner is released. Retain an event before letting another owner go
if its frame or values must remain live (B-3).

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:retain_event}}
```

An outcome and a copied schema can outlive the host's session reference. Producers, consumers and
deliveries keep the underlying session running until they too are released.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:lifetimes}}
```

In C++, use a distinct `std::unique_ptr` owner with the matching custom deleter. `free` functions
and reference-counted `release` functions remain different APIs; the deleter must match the type.

```cpp
{{#include ../../tests/client_conformance/cpp/suite/main.cpp:cpp_owners}}
```

The owner releases its handle when a scope exits, including during exception unwinding.

```cpp
{{#include ../../tests/client_conformance/cpp/suite/main.cpp:cpp_exception}}
```

## Arrow Interop

A client endpoint exchanges one canonical Arrow IPC stream per batch: schema, one record batch,
and an end marker, uncompressed, without dictionaries or schema metadata. Its fields must match the
endpoint exactly. Build it with `nx_batch_builder`, or submit your own stream with
`nx_producer_submit_ipc`, which copies it. `nx_delivery_ipc` and `nx_batch_ipc` borrow the received
stream; `nx_delivery_batch` decodes it once and returns an independently retained batch. Use bulk
states, offsets, fixed-width and variable-length column reads. Width, signedness, nested list levels,
nullability and sensitivity remain part of the schema.

The C++ form moves a retained batch owner into a host thread and reads it after releasing the
original owner. The move transfers one reference; it does not copy the payload.

```cpp
{{#include ../../tests/client_conformance/cpp/suite/main.cpp:cpp_batch}}
```

## What The Library Does Not Expose

The C ABI has no callbacks, host event loop, Arrow C Data interface or C++ class library. It exposes
typed column copies and canonical IPC streams. It does not expose the cluster's internal transport,
Raft operations, host retry policy or graph execution state. The shared Rust client owns redirects,
reconnection and restoration. The host owns its handles, threads and the decision to act on an
uncertain application effect.

## A Complete Application

The full suite files are runnable programs with setup, bounded waits, teardown and named test
tables. This example opens a producer and consumer before publishing, drops the host's session
reference, reads and acknowledges a batch, then takes the producer's outcome and closes both
endpoints. Reading before waiting for the producer prevents its acknowledged path from waiting on
a consumer the application has not yet run.

```c
{{#include ../../tests/client_conformance/c/suite/contract.h:application}}
```

The C++ application uses a typed custom deleter for each endpoint, delivery, batch and outcome.
`suite_take` transfers an already owned fixture handle into its `std::unique_ptr`; each `.get()`
borrows that handle for a call, and `.reset()` releases its one reference.

```cpp
{{#include ../../tests/client_conformance/cpp/suite/main.cpp:cpp_application}}
```

C explicitly registers and drops each owned handle. The C++ program compiles the same cases with
its `std::unique_ptr` cleanup owner and tests typed owners and cross-thread transfers separately.
`just test-client-c-cluster` runs both programs against one- and three-node Nervix clusters over
HTTP and HTTPS; `just validate-client-suites` checks the inventory, every public header member and
all guide includes.
