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
- `Client::upload_resource_from_directory(...)`
- `Client::download_backup(...)`
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
follows the interruption as an `Observed` event; changes in between are not reported. Waiting for
the next event reopens a closed session when a followed clock waits for it. A detach, or an end,
stops following the domain, and `domain_clock` returns `None` for it afterwards.

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
