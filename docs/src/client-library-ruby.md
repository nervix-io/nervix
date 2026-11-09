# Ruby Client Library

*Plexus, the Nervix graph definition language.*

The `nervix-client` gem runs the [Rust Client Library](client-library.md) inside your CRuby
process. A Ruby program connects to Nervix, executes Plexus, reads subscriptions and domain
clocks, publishes through client ingestors, and consumes client emitters with the Rust client's
recovery. The [Client Implementation Manual](client-implementation-manual.md#using-the-ruby-gem)
states the host rules R-1 to R-5.

Every example is included from `tests/client_conformance/ruby/suite`, a minitest suite over the
gem. Cluster examples use the real graph in `tests/features/runtime/client_suites.feature`.
`NX` is `Nervix::Client`; `cluster` supplies the listener URI, credentials, domain and fixture
certificate authority. Helpers such as `completed`, `producer`, `consumer` and `build` are defined
in the suite's `support.rb`; assertions show the values the example checks.

## Install

The gem is built from the Nervix repository today and is not published to a gem registry.
`just build-client-ruby` builds the native extension, packages it into
`target/client-conformance/nervix-client-*.gem`, and extracts it under
`target/client-conformance/ruby`. Install that archive with `gem install` in your application's
gem environment, then `require 'nervix_client'`.

CRuby **3.3 or newer** is required, with its matching development headers when building. The
native gem targets the building interpreter's ABI; use the same ABI when loading it. It has no
runtime gem dependencies. The suite loads the archive installed into a fresh `GEM_HOME` before
opening its first cluster session:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/support.rb:packaging}}
```

## Connect

`Session.connect` opens and maintains a session, following leader redirects and restoring its
subscriptions and endpoints after a lost connection. Credentials are optional only when the
listener permits anonymous access. For a TLS listener use `https://`, and pass its certificate
authority as PEM bytes. The client loads no system roots, so every TLS connection needs that
explicit authority.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:connect}}
```

`tls: NX::Tls::REQUIRED` refuses plaintext addresses, including seeds; `PREFERRED` follows the
address's scheme. A session initially opened over TLS never falls back to plaintext. `seeds`
supplies alternate server URIs. Timeouts are seconds: `connect_timeout` bounds one connection,
`request_timeout` one request, `retry_timeout` the call's retries and redirects, and
`backup_wait_timeout` the wait for an archive. Optional connection keywords accept `nil`.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:seeds}}
```

Use the block form to release the session reference when the block returns or raises:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:session_block}}
```

## Execute Plexus

`prepare` fixes an execution identity before sending a statement. `execute` returns its `Outcome`,
including the disposition, message, diagnostics and execution reference. A server refusal is a
`FAILED` outcome; an interrupted host call raises a typed exception (R-4).

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:execute}}
```

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:diagnostics}}
```

An exception does not roll back an admitted command. Keep its `Execution` and execute that same
object again to recover the outcome under the same identity. Preparing again would create another
command.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:recover}}
```

An outcome also carries the subscription and schema a command opened, its backup or restore
report, and a typed `upload_failure` for a refused `UPLOAD RESOURCE`. Attributes absent from that
outcome are `nil`. Upload identity is fixed by the execution; recovery must send the same archive.

### Inspect Materialized State

`outcome.state_inspection` returns an independently retained inspection, or `nil` when the
command returned no inspection. Check `rejection` against `InspectionRejection` before using
page fields. A refusal has no counts or units; a successful empty page has zero counts.
`json` preserves the complete typed envelope.

Complete `counts` cover the selected cut. Units carry their own counts, owner, ordered
replicas, captured origin, `InspectionUnitKind` and `index`. `InspectionStage` distinguishes live and stored
state. Sections expose schemas, omitted rows and positional watermark columns. Resume the opaque
`continuation` with `AFTER` and the same selection; `STALE_CONTINUATION` requires a new read.

Each `InspectionFrame` retains one verified Row frame and schema. Its `frame` is a read-only `IO::Buffer`. It and its derived slices keep the native frame alive after parent objects are closed or collected.
These are exact session Row/Cell bytes, including types, nulls and redaction; they are not Arrow IPC.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/inspection_test.rb:inspection}}
```


## Complete A Statement

`suggest` takes statement text, a cursor measured in **UTF-8 bytes**, a page size and an optional
continuation. Check the page's status, then replace the suggestion's byte span with its replacement.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:completion}}
```

Follow `continuation` until it is `nil`:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:completion_pages}}
```

## Subscribe And Read Rows

Execute `CREATE SUBSCRIPTION` to open a relay subscription. Its outcome holds a `Subscription`
and a schema describing the rows and branch key.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/subscriptions_test.rb:subscribe}}
```

`next_event` reads the next event of any subscription the session holds. Branch on its `kind`.
For `ROWS`, columns contain Ruby integers, floats, booleans, strings, binary strings and signed
nanoseconds for timestamps. Null is `nil`; a withheld sensitive value is `NX::REDACTED`. `states`
distinguishes them. The branch key has its own `Part`.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/subscriptions_test.rb:rows}}
```

Bulk reads return native-order fixed bytes, or offsets and variable data. `frame` borrows the
verified frame as a read-only `IO::Buffer`; `get_string` makes a binary copy (R-3).

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/subscriptions_test.rb:row_buffers}}
```

`INTERRUPTED` reports a lost session. Restoration opens a new generation without your resubmitting
the statement. `RESTORATION_FAILED` reports a temporary refusal with its typed cause, message and
retry wait; keep the subscription while the client retries. This example's transport relay and
scenario action inject that refusal before the included recovery checks:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/subscriptions_test.rb:subscription_restoration}}
```

`ENDED` is permanent for that generation, for example after the relay is removed. Read attributes
only for the variant that carries them; inappropriate reads raise `TypeMismatchError`.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/subscriptions_test.rb:subscription_ended}}
```

`DELIVERY_LOST`, `ROWS_SKIPPED` and `CONSUMER_OVERFLOW` report gaps with their row counts.
Execute `DELETE SUBSCRIPTION` to release its name:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/subscriptions_test.rb:delete_subscription}}
```

## Follow A Domain Clock

Execute `ATTACH DOMAIN CLOCK` in the domain to follow it. `domain_clock(domain)` immediately reads
its current snapshot, including the paced mapping before its first tick.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/clocks_test.rb:clock_attach}}
```

Use the snapshot's projections instead of reconstructing logical time from wall time:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/clocks_test.rb:clock_project}}
```

`next_clock_event` waits for a state, tick, interruption, end or restoration failure. A tick belongs
to the generation and paced mapping it names:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/clocks_test.rb:clock_tick}}
```

`STOP` and `START` establish a new mapping. `DETACH DOMAIN CLOCK` ends the attachment. Snapshots
already returned remain usable after the session closes.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/clocks_test.rb:clock_generations}}
```

## Publish Through A Client Ingestor

Build a `Fields` draft with complete types, list levels, fixed lengths, nullability and sensitivity.
An endpoint open takes an immutable snapshot before waiting, so later edits cannot change that open
(R-2).

```ruby
{{#include ../../tests/client_conformance/ruby/suite/boundary/columns_test.rb:fields}}
```

`open_ingestor` validates those fields against the endpoint and requests bounded batch and byte
credit. A producer exposes its schema, generation, contract, grant, policy, state and reopen reason.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:open_producer}}
```

An open refusal is a `RejectedError` with a typed `open_refusal` (R-4).

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:open_refusal}}
```

`BatchBuilder` copies every input and validates the complete batch at `finish`. Supply presence
states, offsets and fixed or variable data for each type level; integer widths and floating-point
bits are preserved.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/boundary/columns_test.rb:build_batch}}
```

`submit` returns a submission identity. `pending` reports retained submissions, and `rejoin` waits
for and takes their outcome. A batch beyond the grant waits for credit, so supply a deadline.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:submit}}
```

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:rejoin}}
```

`release` gives up interest in a submission, returning an outcome only if it is already known.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:release}}
```

After session loss an admitted submission may be `OUTCOME_UNKNOWN`. The client restores the
producer but never resends that batch; decide your recovery using its typed uncertainty.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:outcome_unknown}}
```

Changing an endpoint's contract requires a new open. Inspect `reopen` before replacing the handle:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:reopen}}
```

## Consume A Client Emitter

`subscribe_emitter` opens a consumer with expected output fields and bounded credit. Its `next`
waits for a delivery, which carries the stable output identity, the current settlement reference,
relay, branch fingerprint, row count, execution time, decoded batch and Arrow IPC.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:consume}}
```

Settle every delivery. `ack` confirms it, `retry` requests another attempt after the emitter's
backoff, and `reject` applies its message error policy. A retry preserves identity but changes
reference; make application effects idempotent by identity.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:retry}}
```

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:reject}}
```

Session loss interrupts a consumer once, invalidates earlier delivery references and restores the
open consumer. Continue reading; settle the new attempt, not a reference from the lost session.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:consumer_interrupted}}
```

## Back Up And Restore

Execute `BACKUP DOMAIN` to download an archive to a local file. A completed outcome carries its
exact byte count and 32-byte BLAKE3 digest, checked while the client received it.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:backup}}
```

`RESTORE` uploads the local archive. `DRY RUN` verifies and plans without changing the cluster.
Its report counts domains, models and resource versions and identifies failed steps. Recover a
cancelled backup or restore with the same `Execution`; see [Backup And Restore](backup-and-restore.md).

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:restore}}
```

## Errors

All host failures inherit `ClientError`. Branch on their subclass or `kind`, never the display
message. `execution_reference` identifies a command interrupted before its outcome was learned;
`open_refusal` identifies an endpoint refusal. Other calls leave these attributes `nil` (R-4).

```ruby
{{#include ../../tests/client_conformance/ruby/suite/boundary/connection_test.rb:typed_error}}
```

| Class | Meaning and response |
| --- | --- |
| `InvalidArgumentError` | Fix the argument; the invalid call sent nothing. |
| `ConnectError` | Check the listener, credentials, TLS, CA and seeds. |
| `TransportError`, `UncertainOutcomeError` | Recover an admitted command with its original execution. |
| `RejectedError` | Inspect the typed refusal when opening an endpoint. |
| `CancelledError`, `DeadlineError` | The host wait ended; admitted work may still complete. |
| `EventOverflowError` | Read events promptly; the session's event buffer overflowed. |
| `ProtocolError` | The server answered outside the session protocol. |
| `TypeMismatchError` | Check the object's variant or column type before reading. |
| `ClosedError` | Open a new session or endpoint. |
| `ConsumerInterruptedError` | Continue reading the restored consumer; earlier references expired. |
| `ReopenRequiredError` | Inspect `reopen` and open the endpoint under its current contract. |

Enum constants are ordinary integer values. Immutable grants and policies compare and hash by
value and have an `inspect` that names their fields:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:value_objects}}
```

## Threads, Cancellation And Deadlines

Calls that wait release the GVL. Several Ruby threads can use one session, and another thread can
trigger a `Cancel`; library workers never call Ruby (R-2, R-5).

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/commands_test.rb:threads}}
```

`Cancel.new(deadline: seconds)` bounds a call; `trigger` cancels calls sharing the token. A Ruby
thread interrupt ends only that call, without triggering its sibling calls' token. Cancellation
ends a wait and never rolls back admitted work.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/boundary/connection_test.rb:cancel_thread}}
```

## Lifetimes And Memory

The collector releases native ownership once, without waiting. Keep normal Ruby references to
objects in use (R-1). `Session#close` releases its reference; admitted calls, endpoints and
deliveries retain the runtime they need even after that Ruby session object is collected.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/lifetimes_test.rb:session_lifetime}}
```

The session and endpoint block forms return the block's value and close on normal return or
exception. Endpoint closure waits for the server under the open's token and deadline (R-5).
Explicit `ensure` with fresh close tokens is useful when the body has its own shorter deadline.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:endpoint_block}}
```

Frame and IPC buffers, including derived slices, keep their native owner alive through collection.
They are read-only. Use `get_string` when an independent binary copy is preferable (R-3).

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/subscriptions_test.rb:event_lifetime}}
```

Collection of a delivery acknowledges nothing. Settle it before releasing it; retain its consumer
while you are processing its output.

## Arrow Interop

The gem exchanges canonical Arrow IPC streams as binary strings or `IO::Buffer`. Decode one with
`Batch.decode(schema, ipc, rows)`; read typed arrays with `column` or bulk levels with `states`,
`offsets`, `fixed` and `varlen`. It has no Ruby Arrow gem dependency or Arrow C Data adapter.

```ruby
{{#include ../../tests/client_conformance/ruby/suite/boundary/columns_test.rb:ipc}}
```

`submit_ipc` sends a stream for the server to validate against the producer schema:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/endpoints_test.rb:submit_ipc}}
```

## What The Library Does Not Expose

The gem exposes sessions, Plexus execution and resource uploads, completion, subscriptions, clocks,
client endpoints, typed state inspection, backup and restore. It does not expose typed transaction inspection, structured
choice lookups, the leadership view, typed server notices, custom upload identities or upload
progress, or offline `DESCRIBE BACKUP`. Transaction statements still run through `execute`.

There is no asynchronous Ruby API: a waiting call blocks its calling thread with the GVL released.
Use Ruby threads to run calls concurrently. The gem targets CRuby's native extension ABI.

## A Complete Application

Open one session and both endpoints, publish a batch, settle its output, and take the submission
outcome. `ensure` releases each handle even when processing raises:

```ruby
{{#include ../../tests/client_conformance/ruby/suite/cluster/lifetimes_test.rb:application}}
```
