#  Rust Client Library

The workspace includes `nervix-client-core`, a native Rust client library built on the same session
gRPC API used by `nervix-cli`.

Capabilities:

- `Client::connect(...)` and `Client::connect_with_options(...)`
- `Client::execute(...)`
- `Client::transaction_status()`, `Client::attach_transaction(...)` and
  `Client::inspect_transaction(...)`
- `Client::list_domains()`, `Client::domain()` and `Client::set_domain(...)`
- `Client::subscribe(...)`, `Client::unsubscribe(...)` and `Client::next_subscription()`
- `Client::upload_resource_from_directory(...)`
- `Client::next_server_event()`, `Client::next_domain_list()` and `Client::leadership()`
- `Client::suggest(...)` behind the `autocomplete` feature

Suggestion pages report `Ready`, `MissingContext`, `StaleContext`, or `LookupFailed`. Every
candidate carries a UTF-8 byte text edit against the full input and a continuation requests the
next bounded page. The shared C binding exposes the same result through `nx_session_suggest`,
`nx_suggestions_at`, and `nx_suggestions_continuation`; its header is
`crates/client-ffi/include/nervix_client.h`.

Every outcome carries a typed `CommandDisposition`: `Completed`, `Failed`, `NotLeader` with the
leader's endpoints when discovery knows them, `TransactionDetached`, `TransactionTakenOver`,
`OutcomeUnknown` with its cause, `ExecutionReferenceConflict`, `ExecutionReferenceExpired`, or
`PreviewStale`. `CommandOutcome::succeeded()` is true for `Completed`.

Each `execute` call creates one stable execution reference and retains it through leader redirects,
transaction reattachment, and transport reconnects. A successful outcome means the command's
administrative effect is usable on the current live-node set. If the caller cancels its future, the
cluster still owns any already-admitted effect; submitting the same logical request requires
retaining its execution identity at the protocol boundary.

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
position and only treats the exact recorded append as completion. A pending `COMMIT` remains
pending while attached status is `Committing`; it completes from the exact retained terminal
outcome rather than from a coincidental progress count. A recovered committed operation returns a successful
`CommandOutcome` containing the retained aggregate quiesce output. A direct `COMMIT` outcome has no
per-statement `statements`; its message contains only the maximum quiesce level actually executed.
If a peer briefly has no leader address while an election converges, the client retries that
bounded interval instead of returning the transient `NotLeader` outcome. An `OutcomeUnknown`
outcome means the command was durably admitted and its result is not known yet, for example
because leadership moved while it applied; the client retries it with the same execution reference
for the same bounded interval, which recovers the recorded outcome.

An explicit attach by another session takes over the binding. Attach to a retained tombstone
returns an unsuccessful command outcome whose transaction status names `Committed`, `Failed`,
`Reverted`, or `Expired`; when commit produced aggregate output, `diagnostics` carries that one
aggregate rather than the individual statement results. Attach to an id removed after retention
returns an unknown-id outcome. `transaction_status()` keeps the
latest structured status so an interactive caller can render `Open` and `Committing` differently.

## Inspecting A Transaction

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
