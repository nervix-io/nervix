# JVM Client Library

*Plexus, the Nervix graph definition language.*

Java applications use `io.nervix.client.Nervix`, the Java 21 library over Nervix's shared Rust
client. A session executes Plexus, reads relay rows, follows a domain clock, and opens producers
and consumers. The Rust client owns reconnection and recovery; the application owns its handles
and decides what to do with outcomes.

The examples are included from the library's JUnit suite. Its `Cluster` fixture supplies a server,
domain, credentials, certificate authority and test graph; replace those values with your own.
`Cluster.deadline()` creates a cancellation token with a 120-second test deadline. The
[Client Implementation Manual](client-implementation-manual.md#using-the-jvm-library) states the
normative Java ownership, threading and unsigned-width rules.

## Install

Build the jar from the Nervix repository with `just build-client-jvm`. The artifact is
`target/client-conformance/java/nervix-client.jar`. Add it to your application's classpath. The
jar contains its build target's native library and loads it itself; use a jar built for your
operating system and architecture. No Maven or Gradle dependency is published today. Java 21 or
newer is required.

`just test-client-jvm-api` runs the cluster-free JUnit tier with `-Xcheck:jni`;
`just test-client-jvm-cluster` runs the cluster tier on one and three nodes, over TLS and plaintext.
A fresh JVM's packaging check loads the jar without JUnit, an explicit native-library path or a second application library.

## Connect

Use a session URI, an existing domain and credentials. TLS listeners use `https://`. Pass the
certificate authority's PEM bytes to `caCertificate`: the client loads no system roots, so every
TLS connection needs that explicit authority. `REQUIRED` refuses a plaintext URI;
`PREFERRED` also supports a plaintext listener. Both requirements verify the peer when using TLS.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:connect}}
```

`ConnectOptions` is a handle: close it after connecting. The session retains the configuration it
needs. Seeds let it reach the cluster when the initial address is unavailable. Connection, request,
retry and backup-wait timeouts are seconds; cancellation deadlines can bound an individual call.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:seeds}}
```

A wrong password, an untrusted listener and a cluster no address reaches raise `ConnectException`.
Inspect its message for the complete reported cause; credentials are not included.

## Execute Plexus

Prepare a statement to obtain its execution reference, then execute it. A server answer is an
`Outcome`; inspect `disposition()` before using its result fields. `COMPLETED` and `FAILED` are
server outcomes, while a failed host call raises a `ClientException`. A recorded mutation names
its execution reference; a read or client-local result can leave that field `null`.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:execute}}
```

Diagnostics carry a message and a two-element source span. Completion and diagnostic spans are
UTF-8 byte offsets, so convert them before indexing a Java string containing non-ASCII characters.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:diagnostics}}
```

A cancelled or expired wait does not prove that the command had no effect. Keep its prepared
`Execution` and execute that object again to recover the same operation. A new execution reference
could apply the statement again. The exception's `executionReference()` identifies the operation.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:recover_execution}}
```

Other dispositions include leadership or transaction changes, an unknown outcome, reference
conflict or expiry, and a stale preview. Follow the
[command completion contract](command-completion.md) for their recovery rules.

## Complete A Statement

`suggest` returns a `Suggestions` page. Check its status, then apply each candidate's replacement
at its start/end byte range. A lookup can also report missing or stale context or a failed lookup.
Pass `continuation()` back with the same input, cursor and page size to fetch the next page.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:complete}}
```

## Subscribe And Read Rows

Create a subscription with Plexus and retain the returned name and generation. Its schema describes
row fields and branch-key fields separately, with every list level, nullability and sensitivity.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:subscribe}}
```

Read `nextEvent` with a deadline. `ROWS` carries typed columns, a batch and a frame. States distinguish
values, nulls and redaction; redacted bytes are not a usable value. Fixed-width bytes use native byte
order. Strings are UTF-8 and bytes can contain NUL. Read nested columns through `Batch` one level at
a time, with offsets into the next level.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:read_rows}}
```

An `INTERRUPTED` event reports a session gap; the client restores its desired subscription in the
new server session. Generation numbers are scoped to that session and can repeat after restoration.
Use the handle the new event carries. Restoration sees subsequent rows and does not replay the gap.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/RecoveryTests.java:subscription_interrupted}}
```

`ENDED` is terminal for that generation. Its reason distinguishes a removed relay from a changed
relay. Delete a subscription with `DELETE SUBSCRIPTION <name>;`; creating that name again obtains
a new generation. `RESTORATION_FAILED` carries a typed cause, message and retry wait; read it only
for that event kind. `DELIVERY_LOST`, `ROWS_SKIPPED` and `CONSUMER_OVERFLOW` report delivery gaps.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/RecoveryTests.java:subscription_ended}}
```

## Follow A Domain Clock

Attach to the active domain's clock explicitly. `domainClock` returns an immutable snapshot, or
`null` when the session has no attachment. The mapping is available before the application consumes
its first tick.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClockTests.java:attach_clock}}
```

A paced snapshot projects UTC nanoseconds into domain logical time, calculates a physical wait,
and checks the admission window. Use the current generation's mapping when pacing an application.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClockTests.java:project_clock}}
```

`nextClockEvent` yields state, tick, interruption, restoration failure and end events. A tick's
boundary belongs to its generation; obtain a fresh snapshot after the clock changes.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClockTests.java:clock_ticks}}
```

A STOP followed by START creates a new generation and can change the origin and rate. Do not
combine its ticks with the preceding generation.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClockTests.java:clock_generations}}
```

A session gap preserves the last snapshot while the session restores its attachment. Detach
explicitly when the application no longer follows it.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClockTests.java:detach_clock}}
```

## Publish Through A Client Ingestor

Open a producer against a `CLIENT PRODUCER INGESTOR` with its exact `Fields`, including sensitivity,
nullability and every list level. The negotiated description contains the grant, limits, contract,
attachment, ACK window and retry policy. The fields fixture in the suite declares the complete schema.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:open_producer}}
```

Build a batch by copying whole column levels through `BatchBuilder`, or import canonical IPC with
`Batch.fromIpc`. Submitting a batch returns an identity. `pending()` lists the identities still held
and includes an outcome once it is known. `submitIpc` submits bytes produced by other Arrow tooling.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:submit}}
```

`rejoin` waits for the retained result and returns credit. Branch on `SubmissionInfo.result()`:
`COMPLETED`, `NOT_ADMITTED` with its refusal and optional defect, `PROCESSING_FAILED` with its failure,
or `OUTCOME_UNKNOWN` with its uncertainty. A failure or unknown outcome can already have effects.
Java represents causes absent from that outcome class as `null`.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:rejoin}}
```

`release` gives up interest without undoing publication. It returns the known terminal `Submission`,
or `null` if that result was not available. Rejoin a submission whose wait was cancelled; resubmitting
its payload would create a different publication. A full credit window waits for credit and can be
cancelled before another identity is accepted.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:release}}
```

Admission can be suspended during quiescence. A session gap restores a producer only while its
domain generation and contract match. `REOPEN_REQUIRED` requires an explicit new open; inspect
`reopen()` for the reason. The client does not replay an unknown batch on its own.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/RecoveryTests.java:reopen}}
```

## Consume A Client Emitter

Open a consumer with the emitter's exact output schema and bounded credit. `next` returns a
`Delivery` containing its identity, attempt reference, relay, branch fingerprint, member count and
execution instant. Read columns through `batch()`, or exchange its IPC. `description()` exposes the
negotiated contract and policy.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:consume}}
```

Settle each attempt explicitly. `ack` confirms successful application handling; `retry` requests
another attempt with the same identity and a new reference; `reject` invokes the emitter's message
error policy. Closing or collecting a delivery settles nothing.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:retry}}
```

An interrupted consumer raises `ConsumerInterruptedException`. A delivery read before that gap can
no longer settle its attempt; the restored consumer can redeliver the same identity. Deduplicate
application effects by delivery identity. Use `end` with a token to wait for the attachment to end;
`close` releases the local handle.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/RecoveryTests.java:consumer_interrupted}}
```

## Back Up And Restore

A backup writes its archive locally and returns its byte count and digest. Retain the prepared
execution to recover an interrupted wait to the same operation.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:backup}}
```

A restore can plan with `DRY RUN` or install under another domain name. Its report states whether
it was a dry run, its domain/resource/model counts and whether it failed. Check the disposition as
well as the report.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:restore}}
```

## Errors

Every classified failure derives from `ClientException`. Its `kind()`, optional execution reference
and optional open refusal let the application branch on typed information. Its message preserves
the complete reported cause; do not parse it to choose recovery.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:errors}}
```

| Failure | Application response |
| --- | --- |
| `InvalidArgumentException`, `TypeMismatchException` | Correct the argument or read the member appropriate to the value's kind. Malformed local IPC is an argument failure. |
| `ConnectException`, `TransportException` | Inspect connectivity, credentials and TLS; preserve the execution identity when one is present. |
| `UncertainOutcomeException` | Recover the same prepared execution; do not infer whether it had effects. |
| `RejectedException` | Inspect `openRefusal()` where present, or the rejected operation's message. |
| `DeadlineException`, `CancelledException` | The wait ended; retain the identity of any operation already accepted. |
| `OverflowException`, `ProtocolException` | Handle the overflow or invalid server data without inventing missing values. |
| `ClosedException` | Obtain a live handle before calling it. |
| `ConsumerInterruptedException`, `ReopenRequiredException` | Restore the desired attachment or explicitly open one with the current contract. |

## Threads, Cancellation And Deadlines

Several Java threads can share a session. No Java monitor is held while a native call waits, so
another thread can execute a command or trigger its cancellation token. A field builder or batch
builder is used from one thread at a time. Token deadlines are finite, nonnegative seconds.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:threads}}
```

A deadline against a silent listener still ends the wait. Cancellation may happen before or after
admission, so its exception's execution reference matters when recovering a command. This test
holds replies after another session observes the command's effect, then cancels and recovers its
original execution.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/RecoveryTests.java:cancellation_after_admission}}
```

## Lifetimes And Memory

Close every `AutoCloseable` handle with try-with-resources. `close` is idempotent, and later calls
on the same Java handle raise `ClosedException`. Cleaner supplies fallback release for an unreachable
handle. Producers, consumers and deliveries retain the shared session they need, independently of
the Java `Session` handle.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/RecoveryTests.java:owner_lifetime}}
```

A read-only direct frame or IPC buffer retains its native owner through slice and duplicate chains.
It remains readable after the originating handle closes and is collected. Arrays returned by bulk
readers are Java-owned copies. Java records use their normal component equality and hashing;
array-valued components use Java array identity, so compare their contents explicitly when needed.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:frame_lifetime}}
```

Unsigned 64-bit identities, counts, offsets and durations retain their bits in `long`. Use
`Long.compareUnsigned` and `Long.toUnsignedString`; fixed-width column buffers use native byte order.

## Arrow Interop

The library exchanges canonical Arrow IPC bytes. `Batch.fromIpc` validates the exact Arrow schema, one batch and
row count; `ipc` copies a stream and `ipcBuffer` exposes a retained direct view. Arrow Java is an
optional application dependency and is not required to load the client jar. The suite round-trips
through the client's own reader without an Arrow Java dependency.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:arrow_ipc}}
```

## What The Library Does Not Expose

The JVM library runs the shared session state machine; it does not expose its native pointers,
spawn callbacks on Rust runtime threads, add a Java retry policy or convert arbitrary Java objects
into rows. It is a blocking API used from application threads. A binding's typed outcomes and
optional fields are described by the [implementation manual](client-implementation-manual.md#using-the-jvm-library).

## A Complete Application

This end-to-end suite test opens both endpoint sides, submits a columnar batch, reads and settles
its delivery, then observes the completed publication. The scenario supplies the graph described
in [Client Ingestors](ingestors.md#client-ingestors) and
[Client Emitters](emitters.md#client-emitters); provision that graph before running the application.
The earlier connection and schema examples supply the `Cluster` fixture's values in a real program.

```java
{{#include ../../tests/client_conformance/java/suite/cluster/ClusterTests.java:application}}
```
