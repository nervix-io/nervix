#  Rust Client Library

The workspace includes `nervix-client-core`, a native Rust client library built on the same session
gRPC API used by `nervix-cli`. It is the reference implementation of the [Client Implementation
Manual](./client-implementation-manual.md), and [Client Session
Protocol](./client-session-protocol.md) explains the protocol behavior it relies on.

Capabilities:

- `Client::connect(...)` and `Client::connect_with_options(...)`
- `Client::execute(...)`, and `Client::prepare_execution(...)` with `Client::execute_prepared(...)`
- `Client::transaction_status()`, `Client::attach_transaction(...)` and
  `Client::inspect_transaction(...)`
- `Client::list_domains()`, `Client::domain()` and `Client::set_domain(...)`
- `Client::subscribe(...)`, `Client::unsubscribe(...)` and `Client::next_subscription()`
- `Client::attach_domain_clock(...)`, `Client::detach_domain_clock(...)`,
  `Client::domain_clock(...)` and `Client::next_domain_clock_event()`
- `Client::open_ingestor(...)`, with `Producer::send(...)`, `Producer::submit(...)`,
  `Producer::rejoin(...)`, `Producer::pending_submissions()`, `Producer::release(...)` and
  `Producer::close()`
- `Client::upload_resource_from_directory(...)`
- `Client::download_backup(...)`
- `Client::restore(...)` and `Client::restore_with_reference(...)`
- `Client::next_server_event()`, `Client::next_domain_list()` and `Client::leadership()`
- `Client::suggest(...)` behind the `autocomplete` feature
- `Client::lookup_choices(...)`

Suggestion pages report `Ready`, `MissingContext`, `StaleContext`, or `LookupFailed`. Every
candidate carries a UTF-8 byte text edit against the full input and a continuation requests the
next bounded page. The shared C binding exposes the same result through `nx_session_suggest`,
`nx_suggestions_at`, and `nx_suggestions_continuation`; its header is
`crates/client-ffi/include/nervix_client.h`.

Structured choice pages carry typed enum variants or domain, resource, and model references beside
separate presentation metadata. The request includes typed dependencies, search, and a
revision-fenced page cursor. `Client::lookup_choices(...)` retries this read-only request across a
session reconnect and returns its `Ready`, `MissingContext`, `StaleContext`, or `LookupFailed`
status without interpreting labels.

Every outcome carries a typed `CommandDisposition`: `Completed`, `Failed`, `NotLeader` with the
leader's endpoints when discovery knows them, `TransactionDetached`, `TransactionTakenOver`,
`OutcomeUnknown` with its cause, `ExecutionReferenceConflict`, `ExecutionReferenceExpired`, or
`PreviewStale`. `CommandOutcome::succeeded()` is true for `Completed`.

Each `execute` call creates one stable execution reference and retains it through leader redirects,
transaction reattachment, and transport reconnects. A successful outcome means the command's
administrative effect is usable on the current live-node set. If the caller cancels its future, the
cluster still owns any already-admitted effect; the client sends no cancellation and discards the
reply when it arrives. To submit the same logical request again, prepare it once with
`prepare_execution`, which fixes its execution reference, domain, and upload identity, and pass the
same `ExecutionHandle` to `execute_prepared` after an uncertain, cancelled, or timed-out attempt.

Every call is bounded by `ConnectOptions`: `connect_timeout` for each connection attempt (10
seconds by default), `request_timeout` for each request (120 seconds), and `retry_timeout` for the
call with all of its retries and redirects (120 seconds). A call whose deadline passes before a
command's outcome is known fails with `ClientError::UncertainCommand`, which names the execution
reference. An `https://` server is verified only against `ca_certificate_pem`: the client loads no
system roots, so a TLS connection needs the certificate authority that signed the server's
certificate.

Native connections resolve a hostname through Hickory. By default the client loads the host's
resolver configuration and hosts file once at connection setup. `ConnectOptions::dns` can name a
different `DnsConfiguration` or an already loaded `DnsResolver` shared with the caller's runtime.
The same resolver is reused for initial connections, seeds, redirects, and reconnects. Each new
connection uses its current cached DNS answer, tries its addresses in order, and keeps the URL's
hostname for HTTP/2 authority and TLS verification. The connection timeout includes lookup and
all connection work; a failed DNS configuration returns `LoadDnsConfiguration`, and a failed lookup
stays in the connection error's cause chain. DNS does not change execution identities,
subscriptions, transactions, or cancellation.

Minimal example:

```rust
use nervix_client_core::{
    Client, ConnectOptions, DomainName, SubscriptionEvent, SubscriptionRequest,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ConnectOptions::default().with_basic_auth("default", "nervix");
    let domain = DomainName::parse("default")?;
    let client =
        Client::connect_with_options("http://127.0.0.1:47391", Some(domain), options).await?;

    let result = client.execute("SHOW CLUSTER STATUS;").await?;
    println!("{}", result.message);

    let request = SubscriptionRequest::new("sampled_orders", "orders")
        .dropping()
        .with_batch_sample_rate("0.1")
        .with_where_clause(
            nervix_nspl::parse_expression("input.tenant = \"acme\"")?
        );
    client.subscribe(&request).await?;

    if let SubscriptionEvent::Rows(rows) = client.next_subscription().await? {
        for line in rows.display_lines()? {
            println!("{line}");
        }
    }
    client.unsubscribe("sampled_orders").await?;
    Ok(())
}
```

A subscription delivers typed rows against the schema its subscribe reply announced.
`SubscriptionRowsEvent::display_lines()` renders each row as one JSON object, prefixed with its
concrete branch key as `key=<object> payload=<object>` on a branched relay; a sensitive field reads
`"<masked>"`. The other subscription events report rows a dropping subscription could not deliver,
rows it skipped, and the end of the subscription with its reason: `RelayChanged` when the relay was
redefined, so the announced schema no longer describes its rows, or `RelayRemoved` when the relay
or its domain no longer exists. The end is the last event of that subscription; subscribe again to
keep reading a redefined relay. See [Sessions](sessions.md#subscription-lifecycle).

A subscription the server acknowledged outlives its session. When the session ends,
`next_subscription()` reports `Interrupted`, the gap before the subscription opens again, and the
client opens it again as a new generation on its next session. When that session refuses it, for
example because its relay no longer exists, `next_subscription()` reports `RestorationFailed` with
the server's message and the wait before the next attempt; the client keeps trying on that session,
after a wait that starts at one second and doubles up to thirty seconds, until the subscription
opens, the session ends, or the subscription is deleted. `Client::subscription_lifecycle(&name)`
reads the state a subscription is in. `subscribe` and `unsubscribe` reopen a closed session like
every other call. Deleting a subscription that no open session holds, because its session ended or
the current session refused to open it again, completes without a request and releases the name.

## Backing Up

`execute` runs `BACKUP CLUSTER TO '<file>';` and `BACKUP DOMAIN [<name>] TO '<file>';` as one
command and then downloads the archive the backup assembled into the named file. The download is
not bounded by the client's retry deadline; each frame must arrive within the request timeout. The
client writes a private file beside the destination and moves it over the destination only once
the archive's size and BLAKE3 digest match the backup's summary, so the file never holds a partial
archive, and on Unix only its owner may read it. A download that fails in transport starts again
from the first byte while the server retains the archive.

The outcome's `backup` field carries the summary: the archive's size and digest, the capture time,
the instant the server stops retaining it, whether resource bytes are included, the number of
users, and each domain's revision, section count and bytes. A download that still fails returns
`ClientError::BackupDownload` with the backup's execution reference and a typed
`BackupDownloadError`. Running the same `ExecutionHandle` again returns the recorded outcome and
downloads the archive again, and `Client::download_backup(reference, summary, destination)`
downloads a summary's archive directly. A download that receives the whole archive releases it on
the server, and a later download of it is refused.

`execute` refuses `DESCRIBE BACKUP`, which `nervix-cli` serves from a local file without a server;
the `nervix-backup` crate's `describe_archive` reads and verifies an archive for other Rust
programs. The C binding runs `BACKUP` through `nx_session_execute` the same way and reports the
archive's size and digest through `nx_outcome_backup`. See [Backup And Restore](backup-and-restore.md).

## Restoring

`execute` runs `RESTORE CLUSTER FROM '<file>' ...;` and `RESTORE DOMAIN <name> ... FROM '<file>'
...;` by streaming the named local archive to the leader on the session service's restore stream,
beside the session; it refuses both inside a transaction. `Client::restore(restore, on_progress)`
runs a parsed `Restore` the same way and reports each number of archive bytes it hands to the
transport to `on_progress`, and `Client::restore_with_reference(restore, reference, on_progress)`
runs it under an execution reference the caller chose. The client reads the file once to declare
its size and BLAKE3 digest, and sends it in 256 KiB chunks.

A restore is not bounded by the client's retry deadline: each frame must reach the transport within
the request timeout, and so must the reply once the last frame was sent. After a redirect, a lost
connection, or an answer that the restore still applies, the client streams the archive again under
the same execution reference, which joins the restore or returns its recorded outcome, so a leader
change resumes a restore instead of repeating it. An error that may hide an admitted restore is
`ClientError::UncertainCommand` with the restore's execution reference, and running the restore
again under that reference recovers its outcome.

The outcome's `restore` field carries the typed `RestoreReport`: the mode, the archive's size and
digest, the capture time, what the users step did, each restored domain, and each step's outcome.
A stream the leader refused before the restore ran is a failed outcome whose message names the
typed failure. The C binding runs `RESTORE` through `nx_session_execute` and reports the report's
counts, whether it was a dry run, and whether a step failed through `nx_outcome_restore`.

## Following A Domain Clock

`execute` routes `ATTACH DOMAIN CLOCK;` and `DETACH DOMAIN CLOCK;` the way it routes `USE`: it sends
them as typed attach and detach requests for the selected domain and never as commands. Without a
selected domain they fail with `ClientError::NoActiveDomain`, and while a transaction is active they
are refused like every other client-local statement. `Client::attach_domain_clock(domain)` and
`Client::detach_domain_clock(domain)` send the same requests for a domain named explicitly and
return the typed `DomainClockAttachOutcome` and `DomainClockDetachOutcome`, whose dispositions tell
an attachment from each refusal.

Once attached, `Client::domain_clock(&domain)` returns an `AttachedDomainClock` holding the newest
clock and accepted tick the session received. `latest_tick()` gives the tick id, logical boundary,
authority UTC observation, and serving node's logical reading; `frontier()` gives that boundary.
It answers with the same vocabulary arithmetic the cluster runs,
`DomainClockState::logical_time_at` and `DomainAdmissionWindow::reached`, for a UTC instant the
caller supplies:

```rust
use nervix_client_core::{DomainClockEvent, DomainName, Timestamp};

let domain = DomainName::parse("simulation")?;
client.set_domain(Some(domain.clone())).await;
client.execute("ATTACH DOMAIN CLOCK;").await?;

let clock = client.domain_clock(&domain).expect("the session follows this clock");
let now = Timestamp::now();
let logical_now = clock.logical_time_at(now)?;
if let Some(frontier) = clock.frontier() {
    println!("latest accepted tick boundary: {frontier}");
}
if let Some(window) = clock.admission_window(now)? {
    // The newest center the ingestor has reached, admitted within SKEW on either side.
    let occurred_at = window.latest_center();
    println!("{logical_now}: send events stamped {occurred_at}");
}

match client.next_domain_clock_event().await? {
    DomainClockEvent::Observed(observed) => println!("{}", observed.clock),
    DomainClockEvent::Ticked(ticked) => println!("tick {}", ticked.tick.tick_id),
    DomainClockEvent::Ended(ended) => println!("ended because {}", ended.reason),
    DomainClockEvent::Interrupted(gap) => println!("{} is attached again", gap.domain),
    DomainClockEvent::RestorationFailed(failure) => {
        println!("{}: {}; retrying in {:?}", failure.domain, failure.message, failure.retry_after)
    }
}
```

`logical_time_at` reads an unpaced clock as UTC itself and rounds a paced projection down;
`wall_duration_until(now, target)` returns the physical wait until a logical instant, rounded up so
waiting it never arrives early, and zero once the instant is reached; `admission_window` returns
`None` for an unpaced clock, whose ingestors admit every timestamp. A stopped or uninstalled clock
answers every question with a typed `DomainClockReadError`. The answers hold for the caller's UTC: a
host whose UTC is offset from the cluster's receives answers shifted by that offset multiplied by
the rate.

`Client::next_domain_clock_event()` reports what the session receives after each attach reply:
`Observed` with a changed clock, `Ticked` with the newest accepted progress, `Ended` when the server
ended the attachment because the domain no longer exists on the serving node, and `Interrupted`
when the session holding an attachment ended. Events are coalesced per domain: an unread state
arrives before a tick of its generation, while older unread ticks are replaced by the newest one.
After a reconnect, the client attaches every followed
clock again on the new session before any other request, and the clock that attachment reports
follows the interruption as an `Observed` event; changes in between are not reported. A new session
that answers the attach with the domain not found ends the attachment with `Ended`, as the server
would. A node that is still starting answers that attach only once it has installed the cluster's
committed domains, so a restart never ends an attachment. When the new session refuses that attach
for any other reason, the event is `RestorationFailed` with the server's message and the wait
before the client sends the attach again on the same session; the wait starts at one second and
doubles up to thirty seconds. An attach the session leaves unanswered past `request_timeout` ends
that session, and the next one attaches the clock again. Waiting for the next event reopens a
closed session when a followed clock waits for it. A detach, or an end, stops following the domain, and
`domain_clock` returns `None` for it afterwards.

### Through The Shared C Binding

The shared C binding reads the same events for C, C++, Python, JVM and Ruby hosts; its header is
`crates/client-ffi/include/nervix_client.h`. `ATTACH DOMAIN CLOCK;` and `DETACH DOMAIN CLOCK;` run
through `nx_session_prepare` and `nx_session_execute` like any statement, for the session's selected
domain. `nx_session_next_clock_event` waits for the next event, bounded by the same `nx_cancel`
tokens and deadlines as every blocking call, and hands out an `nx_clock_event` reference. The events
are the Rust client's, coalesced the same way:

- `nx_clock_event_kind_of` tells an `NX_CLOCK_EVENT_STATE`, `NX_CLOCK_EVENT_TICK`,
  `NX_CLOCK_EVENT_ENDED`, `NX_CLOCK_EVENT_INTERRUPTED` or `NX_CLOCK_EVENT_RESTORATION_FAILED` event
  apart, and `nx_clock_event_domain` borrows the name of the domain it concerns. A restoration
  failure reports that the session refused to attach an interrupted clock again, or did not answer;
  the session tries again after the growing wait the Rust client reports.
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

The binding exposes the events, not `AttachedDomainClock`: a host projects logical time, physical
waits and admission windows from the paced fields itself. The outcome of an attach carries its
disposition and message, not the clock, so a host that attaches to a running clock paces on the
ticks' logical readings until the next state event reports the committed mapping.

## Events Across Reconnects

`next_subscription()`, `next_domain_clock_event()`, and `next_server_event()` read streams that
belong to the client rather than to one session, so a reader keeps calling them across reconnects:

- When a session ends, `next_subscription()` reports `Interrupted` for every subscription that
  session held, reopens a session, and opens each of them again as a new generation. With nothing
  to restore it waits for the next session the client opens, for example for its next command, and
  delivers the events of subscriptions opened there.
- `next_domain_clock_event()` reopens a session while a followed clock waits to be attached again,
  as described above.
- `next_server_event()` never opens a session itself. Notices end with the session that delivered
  them, including the ones not read yet, and the stream continues with the notices of the next
  session.

A failed read leaves its stream open. `ClientError::EventOverflow` from `next_server_event()` means
notices arrived faster than they were read: the client dropped the ones it held, and the next read
returns the notices that arrived after that gap. An error from reopening a session, such as
`ClientError::RetryDeadline` or `ClientError::ConnectServer`, leaves subscriptions and clocks
waiting to be restored, and the next read tries again. Only `ClientError::SessionClosed` ends a
stream: the session ended and the client knows no server to open another on, as happens to a
client built with `Client::from_channel` that has not been redirected.

## Producers

`Client::open_ingestor(domain, ingestor, expected_fields, limits)` attaches a producer to a
[client ingestor](ingestors.md#client-ingestors) of the domain named explicitly, on the session's
current exchange. `expected_fields` must be exactly the ingestor's input schema, including each
field's optionality and sensitivity, and `limits` asks for the batches and bytes the producer may
have outstanding. A refusal is `ClientError::ProducerRefused` with its typed
`ClientProducerRefusal`, and leaves nothing attached. The returned `Producer` reports what the open
established through `description()`: the schema, the `START` generation, the endpoint contract and
attachment, the ingestor's policy, and the granted credit.

```rust
use std::num::{NonZeroU32, NonZeroU64};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use nervix_client_core::{ClientProducerLimits, DomainName, IngestorName, ProducerOutcome};

let producer = client
    .open_ingestor(
        DomainName::parse("shop")?,
        IngestorName::parse("orders_in")?,
        expected_fields,
        ClientProducerLimits {
            batches: NonZeroU32::try_from(16)?,
            bytes: NonZeroU64::try_from(8 * 1024 * 1024)?,
        },
    )
    .await?;
let rows = RecordBatch::try_new(
    std::sync::Arc::new(producer.arrow_schema()),
    vec![
        std::sync::Arc::new(StringArray::from(vec!["o-1"])),
        std::sync::Arc::new(Int64Array::from(vec![1200])),
    ],
)?;
match producer.send(producer.batch(&rows)?).await? {
    ProducerOutcome::Completed => println!("delivered"),
    ProducerOutcome::NotAdmitted { refusal, .. } => println!("not admitted: {refusal:?}"),
    ProducerOutcome::ProcessingFailed { failure, .. } => println!("failed: {failure:?}"),
    ProducerOutcome::OutcomeUnknown { cause, .. } => println!("replay decision: {cause:?}"),
}
producer.close().await?;
```

`Producer::batch` and `ProducerBatch::from_record_batch`, behind the `arrow` feature, write a record
batch as the canonical stream the server accepts after checking it against the producer's schema and
row limit; `ProducerBatch::from_arrow_ipc` takes a stream the application already wrote.

`send` waits for credit, submits the batch, and returns its terminal outcome. `submit` returns once
the producer holds the batch, and `rejoin` waits for that submission's outcome. A submission keeps
its share of the credit until the application observes its outcome, so a producer whose application
stops reading outcomes stops being granted room for new batches. Cancelling a `send` or `rejoin`
future never loses the batch or its outcome: `pending_submissions()` lists every submission the
producer holds with its outcome once it has one, `rejoin` resumes the wait without sending the batch
again, and `release` lets go of a submission and its credit.

The client sends a batch again only when the server refused it temporarily, as `Suspended` or
`Busy`, after the ingestor's declared backoff, and it waits while the producer's admission is
suspended. It never replays a batch that failed or whose outcome is unknown. A batch that was sent
when the session ended is reported as `OutcomeUnknown` with `SubmissionUncertainty::SessionLost`,
and one still waiting to be sent as not admitted. `Producer::admission()` tells whether batches are
admitted now and `Producer::end()` how the producer ended: `Closed`, `Ended` with the server's
reason, or `SessionLost`. This release restores no producer across a reconnect; the application
opens another one, and `open_ingestor` reopens a lost session before it sends the open. Dropping a
producer closes it without waiting.

## Transaction Handles And Attach

`CommandOutcome::transaction` describes the session's transaction binding. Its `TransactionStatus`
holds the transaction id, the domain the transaction is bound to, its `TransactionLifecycle`
(`Open`, `Committing`, `Committed`, `Failed` with the failing operation and its error, `Reverted`, or
`Expired`), and its accepted, applied and pending operation counts. Treat the id as the durable
transaction handle:

```rust
client.execute("CREATE DOMAIN production;").await?;
client.set_domain(Some(DomainName::parse("production")?)).await;

let begun = client.execute("BEGIN;").await?;
let transaction_id = begun
    .transaction
    .as_ref()
    .expect("BEGIN returns transaction status")
    .transaction_id()
    .to_string();

client.execute("CREATE SCHEMA notification (user_id I64);").await?;

// A different Client authenticated as the same user can take over the transaction.
let attached = recovered_client.attach_transaction(transaction_id).await?;
if !attached.succeeded() {
    eprintln!("attach outcome: {}", attached.message);
}
```

`BEGIN` requires an already-existing selected domain and binds the transaction to it. Attaching
adopts that domain, so `recovered_client` follows the transaction's domain without an explicit
`set_domain`.

While a transaction is open, `execute` first preflights a queueable statement against the
replicated prefix. An unsuccessful preflight leaves the transaction `Open` with the same pending
count, so callers may correct the command and continue using the same handle. A queued model
mutation's message reports the effective quiesce level of its consecutive atomic model run at the
current prefix. A later mutation in that run may escalate the level or cancel the net change. An
exact append retry returns the originally admitted result without refreshing preflight or the
transaction's activity timestamp.

The client automatically attaches its active transaction after a leader redirect or transport
reconnect before retrying a command. It retains each append's execution reference and expected
position and only treats the exact recorded append as completion. A pending `COMMIT` waits for the
outcome recorded under its own reference, repeating the commit after an interruption; it completes
from that exact terminal outcome rather than from a coincidental progress count, and the call fails
as uncertain if `retry_timeout` passes first. A recovered committed operation returns a successful
`CommandOutcome` containing the retained aggregate quiesce output. A direct `COMMIT` outcome has no
per-statement `statements`; its message contains only the maximum quiesce level actually executed.
If a peer briefly has no leader address while an election converges, the client waits and retries
within `retry_timeout` instead of returning the transient `NotLeader` outcome. An `OutcomeUnknown`
outcome means the command may have been admitted and its result is not known yet, for example
because leadership moved while it applied; the client retries it with the same execution reference
within the same deadline, which recovers the recorded outcome.

An explicit attach by another session takes over the binding. Attach to a retained tombstone
returns an unsuccessful command outcome whose transaction status names `Committed`, `Failed`,
`Reverted`, or `Expired`; when commit produced aggregate output, `diagnostics` carries that one
aggregate rather than the individual statement results. Attach to an id removed after retention
returns an unknown-id outcome. `transaction_status()` keeps the
latest structured status so an interactive caller can render `Open` and `Committing` differently.

## Inspecting A Transaction

[Transaction Quiescence And Impact Inspection](./transaction-quiescence.md) defines the report's
operation contributions, effective step scopes, actual engagement, and retained topology. This
section describes how the Rust client receives it and fences a later commit.

`DESCRIBE TRANSACTION` answers twice: rendered in the outcome's `message`, as `TEXT` by default or as
one JSON document with `FORMAT JSON`, and typed in `CommandOutcome::inspection`. The typed
`TransactionInspection` holds the inspected transaction's status, the selected operation if the
statement named one, and the whole `TransactionImpactReport`, whichever format rendered the message:

```rust
let described = client.execute("DESCRIBE TRANSACTION OPERATION 2;").await?;
if let Some(inspection) = &described.inspection {
    println!(
        "{} operation(s) in {} execution step(s)",
        inspection.report.operations().len(),
        inspection.report.execution_steps().len()
    );
}
```

The rendered message and `CommandOutcome::inspection` describe the same complete value. The server
does not paginate or truncate a large report, so callers that inspect transactions with many
operations or large affected graphs should budget for one response proportional to the expanded
report. A successful result always includes all operations, execution steps, topology, and recorded
outcomes.

`CommandOutcome::transaction` keeps describing this session's own binding, so inspecting another
transaction by id changes neither `transaction_status()` nor the selected domain. An inspection
consumes no queue position: the next queued statement receives the operation number it would have
had. An inspection of the attached transaction refreshes the identified preview used by `COMMIT`
only when its report covers the attached transaction's current accepted-operation position.
Inspecting another transaction, or receiving an older position, cannot replace that preview. A
stale-preview refusal leaves the previously reviewed basis in place; inspect the attached
transaction again before retrying `COMMIT`. A refused inspection is an unsuccessful outcome whose
message names why nothing was read.

`DESCRIBE RESOURCE <name>` is typed the same way: `CommandOutcome::resource` holds the
`ResourceDescription` beside the printed text, with the highest completed version as an `Option`,
every published version with its entries under their exact paths, and the models bound to each
version.

`DESCRIBE WASM PROCESSOR <name>` is typed too: `CommandOutcome::wasm_state` holds the
`WasmStateInspection` beside the text or JSON rendering, with the pinned module binding, the default
guest-state generation, the latest reset with its request reference, scope, phase, and reason, the
recorded rejected-state recoveries, and each current branch's checkpoint stage, revisions, and
replica counts under its opaque fingerprint. `RESET WASM PROCESSOR ... STATE` is an ordinary
command: its success arrives only once the new lifetime is usable, and a retry the client makes
after an uncertain outcome reuses the same `execution_reference`, so it recovers the original
reset's outcome rather than starting another. See
[WASM State And Recovery](./wasm-state.md#observability).

`Client::inspect_transaction(target, operation)` also returns the typed `InspectionOutcome` from
the API. It follows leader redirects and reconnects with the session client's normal request-ID
dispatch. A successful read of the attached transaction refreshes the same commit preview; a
rejected read changes no binding or preview.
