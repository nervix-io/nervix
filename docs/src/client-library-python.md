# Python Client Library

*Plexus, the Nervix graph definition language.*

`nervix_client` is the Python package of the Nervix client. It runs the
[Rust Client Library](client-library.md) inside your CPython process, so a Python program connects
to a cluster, runs Plexus, reads relay rows, follows a domain clock, and publishes to and consumes
from client endpoints with the same recovery as every other Nervix client. This chapter tells you how
to write that program. Every example below is a test of the package's own suite,
`tests/client_conformance/python/suite`, which runs against a real cluster, so an example that stops
working fails a test.

The rules a host must follow are stated normatively in the
[Client Implementation Manual](client-implementation-manual.md#using-the-python-package) as N-1 to
N-5; this chapter links them where they apply.

## Install

The package is built from the Nervix repository today; it is not published to a package index. In a
checkout, `just build-client-python` builds an abi3 wheel with maturin into
`target/client-conformance/wheels/` and installs it into a CPython environment beside pyarrow and
polars. Install the same wheel into your own environment with `pip install` or `uv pip install`.

- **Python.** CPython 3.12 or newer. One wheel serves every CPython release from 3.12 on.
- **Dependencies.** None. pyarrow and polars are optional: the package exchanges Arrow data with them
  through the Arrow PyCapsule interface when you use them, and imports neither.
- **Typing.** The wheel ships `nervix_client.pyi` and a `py.typed` marker, so mypy and pyright check
  every call against the package's real surface.

## Connect

A `Session` opens a session on one server and keeps it open: it follows the leader, reconnects after
a lost connection and restores what it held. Pass the server's session URI, the domain the session
works in, and the user's credentials. A listener that serves TLS is named with `https://`; pass the
certificate authority its certificate chains to as PEM bytes when it is not one your system trusts.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_connect.py:connect}}
```

Leaving the `with` block releases the session; see [Lifetimes And Memory](#lifetimes-and-memory).

`tls=Tls.REQUIRED` refuses any server or seed that is not `https://` before anything is sent, and
`tls=Tls.PREFERRED` uses TLS whenever the address asks for it. A session that starts on an
`https://` server never falls back to plaintext, whichever you pass.

```python
{{#include ../../tests/client_conformance/python/suite/boundary/test_connecting.py:tls_required}}
```

`seeds` lists further servers the session may reach, tried when the server does not answer and used
again when the session reconnects. Every timeout is in seconds: `connect_timeout` bounds opening one
connection, `request_timeout` one request, `retry_timeout` a call with all of its retries and
redirects, and `backup_wait_timeout` a backup's wait for its archive.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_connect.py:seeds}}
```

An option the session cannot use, such as a username without a password, a seed that is not a URL
or a negative timeout, raises `InvalidArgumentError` and sends nothing. Opening a session tries the
server and then each seed once, within `retry_timeout`. A server no connection reaches, a
certificate the session does not trust, a TLS requirement a plaintext address cannot meet and a
wrong password, which the server refuses before the session starts, raise `ConnectError`, whose
message never repeats the password.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_connect.py:wrong_password}}
```

Every call that waits, the constructor included, takes an optional `cancel` token; see
[Threads, Cancellation And Deadlines](#threads-cancellation-and-deadlines).

## Execute Plexus

`Session.prepare` captures a statement and a fresh execution identity before anything is sent, and
`Session.execute` runs it. The `Outcome` says what became of it: its `disposition`, the server's
`message`, and the `execution_reference` the command ran under.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_execute.py:execute}}
```

A command the server refused is an outcome, not an exception: its disposition is `FAILED`, and each
`Diagnostic` names its message and the `(start, end)` byte span of the source it concerns.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_execute.py:diagnostics}}
```

A call that ends before you learn the outcome, because its token was cancelled, its deadline passed
or its session was lost, raises an exception that carries the command's `execution_reference`. The
command may already have run. Execute the same `Execution` again: the session sends it under the same
identity, so the cluster runs it once and answers its one outcome. Preparing the statement again
would make a second command.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_execute.py:recover_cancelled}}
```

An outcome also carries what its command opened: the `subscription` and `schema` of a
`CREATE SUBSCRIPTION`, the archive summary of a `BACKUP` and the report of a `RESTORE`. Each is `None`
for a command that opened none.

`UPLOAD RESOURCE <name> VERSION '<directory>';` streams a local directory as a new version of a
resource the domain declares. The session serves it itself, under the upload identity its
`Execution` fixed, so executing the same execution again sends the same archive under the same
identity and completes without installing a second version.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_execute.py:upload}}
```

An upload the cluster refused is a `FAILED` outcome whose `upload_failure` says why. It is `None` for
a completed upload and for every other command. `IDENTITY_CONFLICT` means the identity already names
another archive: keep the original bytes to recover that upload, and prepare the statement again
for a different one.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_execute.py:upload_conflict}}
```

## Inspect Materialized State

`outcome.state_inspection` returns an independently retained inspection, or `None` when the
command returned no inspection. Check `rejection` against `InspectionRejection` before using
page fields. A refusal has no counts or units; a successful empty page has zero counts.
`json` preserves the complete typed envelope.

Complete `counts` cover the selected cut. Units carry their own counts, owner, ordered
replicas, captured origin, `InspectionUnitKind` and `index`. `InspectionStage` distinguishes live and stored
state. Sections expose schemas, omitted rows and positional watermark columns. Resume the opaque
`continuation` with `AFTER` and the same selection; `STALE_CONTINUATION` requires a new read.

Each `InspectionFrame` retains one verified Row frame and schema. Its `frame` memoryview and `memoryview(frame)` are read-only. They and their derived slices keep the native frame alive after parent objects are collected.
These are exact session Row/Cell bytes, including types, nulls and redaction; they are not Arrow IPC.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_inspection.py:inspection}}
```


## Complete A Statement

`Session.suggest` asks the cluster what may follow the cursor in a partial statement, from the
session's domain, so the suggestions include the names that exist there. A page's `status` says
whether it could be computed. Each `Suggestion` has a `value`, a `kind` and the edit that applies it:
replace the bytes of `span` with `replacement`.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_completion.py:suggest}}
```

A page holds at most `page_size` candidates. Its `continuation` is the token of the next page, and
`None` on the last.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_completion.py:suggest_pages}}
```

## Subscribe And Read Rows

A subscription is a statement: `CREATE SUBSCRIPTION <name> TO <relay>;` opens it, and its outcome
holds its `Subscription` handle and the `Schema` of its rows, with the relay's fields, its branch and
the branch key's fields.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:subscribe}}
```

`Session.next_event` waits for the next event of any subscription the session holds. A `ROWS` event
reads each column as Python values: an `int`, `float`, `bool`, `str` or `bytes`, the nanoseconds
since the Unix epoch for a `DATETIME`, a `list` for a list, `None` for a null, and `REDACTED` for a
value the server withheld because its field is sensitive. `states` tells the three apart, and the
branch key is read from its own `Part`.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:read_rows}}
```

To read many rows without a Python object per value, read a fixed-width column as native-order bytes
with `fixed`, a string or bytes column as offsets and data with `varlen`, or the whole event as
`frame`, a `memoryview` of the verified frame it was decoded from.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:read_bulk}}
```

The other events report what happened to a subscription. `INTERRUPTED` says its session was lost and
the rows published until it is restored are not delivered; the session opens it again as a new
generation on its next session, without your doing anything. A generation is numbered within its
session, so the restored subscription keeps its name but can carry the number it had before.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:interrupted}}
```

`ENDED` is the last event of a generation that ended for good, and its `end_reason` says why:
`RELAY_REMOVED` when its relay or its domain was removed, and `RELAY_CHANGED` when its relay was
redefined with another schema. The server ends a generation its session holds when that happens;
subscribe again to read the relay under its current definition.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:ended}}
```

A subscription whose relay was removed or redefined while its session was lost ends the same way:
the new session finds it so when it opens the subscription again, and reports `ENDED` instead of
opening it.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:ended_while_lost}}
```

`RESTORATION_FAILED` says the new session refused for now to open the subscription again, for
example because the node it reached has not caught up with the cluster yet. Its
`restoration_failure` carries the `RestorationCause`, the server's message and how long the session
waits before it tries again; the subscription stays and the session keeps trying. The suite asks
its scenario to refuse one restoration, checks those details, then reads rows after a later attempt
succeeds:

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:restoration_failed}}
```

`DELIVERY_LOST`, `ROWS_SKIPPED` and `CONSUMER_OVERFLOW` report gaps with their row counts in
`row_count`. A reader
handles each kind of event:

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:handle_events}}
```

`DELETE SUBSCRIPTION <name>;` closes a subscription and frees its name.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:delete}}
```

## Follow A Domain Clock

`ATTACH DOMAIN CLOCK;` follows the clock of the session's domain. `Session.domain_clock` reads the
clock the session holds without waiting, and right after the attach it is the clock the attach
reported, so you read a running clock's generation and mapping before its first tick.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_clocks.py:attach}}
```

A paced clock maps UTC to logical time at its `rate` from its `anchor`. Project with the clock instead
of computing yourself: `logical_time_at` reads the logical time at a UTC instant, `wall_duration_until`
the UTC nanoseconds until a logical instant, and `admission_window` and `admits` the event times a
`TIMESTAMP AT` ingestor admits.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_clocks.py:project}}
```

`Session.next_clock_event` waits for the next event about the clocks the session follows. A `TICK`
event's `Tick` names its generation, its number from one and the logical boundary it ends.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_clocks.py:ticks}}
```

`STOP` and `START` begin a new generation with a new mapping, reported by a `STATE` event. Read the
mapping of the generation an event names; a tick of one generation never follows the state of
another.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_clocks.py:generations}}
```

A lost session reports `INTERRUPTED`. The session keeps the last clock it read, without a tick,
attaches again on its next session, and reports the clock again with a `STATE` event.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_clocks.py:clock_interrupted}}
```

When the new session refuses for now to attach the clock again, a `RESTORATION_FAILED` event carries
the same `RestorationFailure` a subscription's does, with `IN_TRANSACTION`, `REJECTED` or `FAILED` as
its cause, and the session tries again after the wait it names. A clock whose domain was removed
ends with `ENDED`, and its `end_reason` is `DOMAIN_REMOVED`.

`DETACH DOMAIN CLOCK;` stops following it.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_clocks.py:detach}}
```

To pace a driver by the clock, read `domain_clock` before you publish, wait `wall_duration_until`
the next boundary, and stamp each event with a time `admits` accepts; the
[paced simulation driver](paced-simulation-drivers.md) does exactly this.

## Publish Through A Client Ingestor

A producer publishes typed batches to a client ingestor. Describe the fields you expect the ingestor
to take, one level of each type at a time: a list is followed by its element, and a fixed-size list
names its length.

```python
{{#include ../../tests/client_conformance/python/suite/boundary/test_descriptions.py:describe_fields}}
```

`Session.open_ingestor` opens a producer with the credit you ask for, in batches and bytes. The
producer's `grant` is the credit the server granted and the largest batch it takes, its `policy` how
batches are acknowledged and retried, and `state` and `admission` whether it is attached and whether
its batches are admitted.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:open_producer}}
```

An open the cluster cannot serve raises `RejectedError`, and its `open_refusal` says why.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:open_refused}}
```

Build a batch with a `BatchBuilder` for the producer's `schema`, one level of a column at a time:
`states` for a nullable column, `offsets` for each list level, and `fixed` or `varlen` for the values.
Every call copies what it is given. `Producer.submit` waits for credit and returns the submission's
id, and `pending` lists what the producer holds.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:submit_built}}
```

`rejoin` waits for a submission's outcome and takes it. A batch completes once the graph has handled
it, which for an ingestor whose output a client emitter carries means once that output was
acknowledged.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:rejoin}}
```

A program that writes canonical Arrow IPC itself submits the bytes; the server checks them.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:submit_ipc}}
```

Every outcome has a `result`, and the cause its class carries: why a batch was `NOT_ADMITTED`, and
the defect of an invalid one; why an admitted batch's processing failed; and why no outcome could be
established. Reading a cause the class does not carry raises `TypeMismatchError` (N-4).

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:outcome_classes}}
```

`release` lets a submission go: the producer stops holding it, and a later `rejoin` of it is refused.
A submission beyond the granted credit waits for an earlier outcome, so pass a deadline to bound the
wait.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:release}}
```

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:credit}}
```

A batch submitted before its session was lost has an unknown outcome: the session restores the
producer, never sends the batch again, and leaves the decision to you.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:outcome_unknown}}
```

When the ingestor's contract changes, for example its acknowledgement timeout, the producer has to
be opened again: `reopen` says why, and `submit` raises `ReopenRequiredError` until you do.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_producers.py:reopen}}
```

## Consume A Client Emitter

`Session.subscribe_emitter` opens a consumer on a client emitter, and `Consumer.next` waits for the
next `Delivery`. A delivery carries the emitter's output: its `identity`, which every attempt of the
same output keeps, the `reference` this attempt is settled by, the relay it came from, the fingerprint
of its branch, its row count and the domain's logical time when it was produced, and its `batch`.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_consumers.py:delivery}}
```

A batch reads a column as Python values with `column`, or one level of its type at a time.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_consumers.py:delivery_levels}}
```

Settle every delivery: `ack` confirms it, `retry` asks for it again after the emitter's backoff, and
`reject` hands it to the emitter's message error policy. Each returns the server's `Settlement`. A
retried output comes back with its identity and a new reference, so make your effect idempotent by
identity. A delivery you never settle is delivered again once the emitter's ACK timeout passes.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_consumers.py:retry}}
```

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_consumers.py:reject}}
```

After a lost session the next `next` raises `ConsumerInterruptedError` once: output may have been
missed, and the consumer is restored. A delivery read before the gap can no longer be settled, and is
delivered again.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_consumers.py:consumer_interrupted}}
```

## Back Up And Restore

`BACKUP DOMAIN <name> TO '<file>';` runs through `execute` and writes the archive to the local file:
the outcome's `backup` holds its exact size and its 32-byte BLAKE3 digest, which the client checked
every downloaded byte against, and the file is complete when the call returns.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_backup.py:backup}}
```

`RESTORE ... FROM '<file>';` streams the local archive to the cluster. A `DRY RUN` verifies and plans
it and changes nothing; the outcome's `restore` counts the domains, models and resource versions and
says whether a step failed. A backup or restore whose call ended before its outcome is recovered like
any command, by executing the same execution again. See [Backup And Restore](backup-and-restore.md)
for the statements.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_backup.py:restore}}
```

## Errors

Every failure raises one subclass of `ClientError`, named after its `kind`, a `FailureKind`. Branch on
the class or the kind, never on the message, which is the whole chain of the failure's report (N-3).

```python
{{#include ../../tests/client_conformance/python/suite/boundary/test_enumerations.py:enumerations}}
```

| Class | Kind | What it means, and what to do |
| --- | --- | --- |
| `InvalidArgumentError` | `INVALID_ARGUMENT` | An argument cannot be used and nothing was sent. Fix the call. |
| `ConnectError` | `CONNECT` | The session could not be opened, or reached no server. Check the address, TLS and certificate authority; try again later. |
| `TransportError` | `TRANSPORT` | The session failed while the call used it. Execute the same execution again. |
| `UncertainOutcomeError` | `UNCERTAIN` | The outcome is not known. Execute the same execution again to learn it. |
| `RejectedError` | `REJECTED` | The server refused the call. An open's refusal is in `open_refusal`. |
| `DeadlineError` | `DEADLINE` | The call's deadline passed; admitted work is not rolled back. Execute the same execution again to learn the outcome. |
| `CancelledError` | `CANCELLED` | The call's token was triggered; admitted work is not rolled back. Execute the same execution again to learn the outcome. |
| `EventOverflowError` | `OVERFLOW` | The session's events overflowed what it buffers. Read events promptly. |
| `ProtocolError` | `PROTOCOL` | The server answered outside the session protocol. |
| `TypeMismatchError` | `TYPE` | A value was read as a type it does not have, or a field its object's kind does not carry. Check the kind first. |
| `ClosedError` | `CLOSED` | The session or the endpoint ended, or its `with` block released it. Open a new one. |
| `ConsumerInterruptedError` | `INTERRUPTED` | The consumer's session was lost: output may have been missed, and the consumer is restored. Keep reading. |
| `ReopenRequiredError` | `REOPEN_REQUIRED` | The endpoint changed and has to be opened again; `reopen` says why. |

`execution_reference` names the command an uncertain, cancelled or timed-out call concerns, and is
`None` otherwise; `open_refusal` is the `OpenRefusal` of a refused open, and `None` otherwise.

## Threads, Cancellation And Deadlines

Call the package from any thread, and use one session from several threads at once (N-2). Every call
that waits releases the GIL until it returns, so other Python threads keep running, and the package
never calls Python from a thread of its own; its session runs on two threads of its own runtime.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_execute.py:threads}}
```

A `Cancel` token bounds the calls it is passed to. Trigger it from any thread to end every call
waiting on it with `CancelledError`, or create it with a `deadline` in seconds, after which it ends
them with `DeadlineError`. Cancelling a wait never rolls back admitted work.

```python
{{#include ../../tests/client_conformance/python/suite/boundary/test_cancellation.py:cancel_from_another_thread}}
```

```python
{{#include ../../tests/client_conformance/python/suite/boundary/test_cancellation.py:connect_deadline}}
```

A `Fields` list and a `BatchBuilder` are built by one thread at a time; every other object may be
used from several threads at once.

## Lifetimes And Memory

Python owns every object the package returns (N-1). An object releases its share of the session when
Python collects it, or when its `with` block ends: a session, a producer, a consumer and a delivery
are context managers, and every call on one after its block ends raises `ClosedError`. Leaving a
producer's or a consumer's block closes it without waiting for the server; call `close` to wait. An
exception raised inside a block is never suppressed.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_lifetimes.py:with_blocks}}
```

A producer, a consumer and a delivery keep their session running until they are released too, so a
session object can be collected while they work.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_lifetimes.py:outlives_session}}
```

A `memoryview` of an event's frame, a batch's stream or a delivery's stream borrows the bytes without
a copy and keeps the object alive, and its bytes valid, for as long as the view exists, a released
delivery's included.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_subscriptions.py:view_keeps_event}}
```

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_lifetimes.py:delivery_view}}
```

Releasing or collecting a delivery settles nothing: settle it before you let it go, and before you
release its consumer, whose attachment it settles on.

## Arrow Interop

A `Batch` exports itself through the Arrow PyCapsule interface, so pyarrow and polars read it without
a copy.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_arrow.py:to_arrow}}
```

`Producer.submit` also takes Arrow data that exports itself through the interface, such as a pyarrow
record batch or table, and `Batch.from_arrow` imports it for a schema. Arrow data must
have the producer's field names in order, exactly its types, `timestamp[ns, tz=+00:00]` for a
`DATETIME`, and no null in a required field or a list element; anything else raises
`InvalidArgumentError` before a batch is sent (N-5).

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_arrow.py:from_arrow}}
```

polars holds strings, bytes and lists in Arrow types of its own, such as `string_view` and
`large_list`, and the producer takes no type but its own. Read a polars data frame with pyarrow
and cast it to the endpoint's schema before you submit it.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_arrow.py:from_polars}}
```

An event's `rows` returns its rows as one batch. Its export marks every sensitive field nullable and
reads a withheld value as a null, so read `states` to tell a withheld value from a null.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_arrow.py:event_rows}}
```

## What The Library Does Not Expose

The package exposes sessions, Plexus execution with resource uploads, completion, subscriptions,
domain clocks, client endpoints, typed state inspection, backup and restore. It does not expose these parts of the Rust
client:

- the typed transaction status, transaction attach and transaction inspection; `BEGIN`, `COMMIT` and
  `REVERT` still run through `execute`, and the session keeps its transaction across a reconnect;
- the typed domain list, structured choice lookups, server notices and the leadership view;
- uploads that report their progress or run under an upload identity you choose; `UPLOAD RESOURCE`
  runs through `execute` under the identity its `Execution` fixed;
- `DESCRIBE BACKUP`, which reads an archive without a server.

There is no asyncio interface: every call blocks its thread with the GIL released, so run a call on
another thread, for example with `asyncio.to_thread`, to wait for it from a coroutine.

## A Complete Application

A complete program opens one session, a consumer and a producer, publishes a batch, handles its
output, and takes the batch's outcome, closing each object when its block ends.

```python
{{#include ../../tests/client_conformance/python/suite/cluster/test_application.py:application}}
```

The [paced simulation driver](paced-simulation-drivers.md),
`examples/paced-simulation/python/paced_simulation.py`, is a longer application on the same package:
it follows a paced domain clock, paces its producer by the clock's projections, runs its consumers
beside the producer on one session, and reopens its endpoints when their contracts change.
