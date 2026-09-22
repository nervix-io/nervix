#  Rust Client Library

The workspace includes `nervix-client-core`, a native Rust client library built on the same session gRPC API used by `nervix-cli`.

Capabilities:

- `Client::connect(...)`
- `Client::execute(...)`
- `Client::transaction_status()` and `Client::attach_transaction(...)`
- `Client::subscribe(...)`
- `Client::unsubscribe(...)`
- `Client::next_subscription()`
- `Client::suggest(...)` behind the `autocomplete` feature

Each `execute` call creates one stable execution reference and retains it through leader redirects,
transaction reattachment, and transport reconnects. A successful outcome means the command's
administrative effect is usable on the current live-node set. If the caller cancels its future, the
cluster still owns any already-admitted effect; submitting the same logical request requires
retaining its execution identity at the protocol boundary.

Minimal example:

```rust
use nervix_client_core::{Client, ConnectOptions, SubscriptionRequest};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ConnectOptions::default().with_basic_auth("default", "nervix");
    let client =
        Client::connect_with_options("http://127.0.0.1:47391", "default", options).await?;

    let result = client.execute("SHOW CLUSTER STATUS;").await?;
    println!("{}", result.message);

    let request = SubscriptionRequest::new("sampled_orders", "orders")
        .dropping()
        .with_batch_sample_rate("0.1")
        .with_where_clause(
            nervix_nspl::parse_expression("input.tenant = \"acme\"")?
        );
    client.subscribe(&request).await?;

    let event = client.next_subscription().await?;
    println!("{}", event.payload);
    client.unsubscribe("sampled_orders").await?;
    Ok(())
}
```

## Transaction Handles And Attach

`CommandOutcome::transaction` replaces the former boolean transaction flag. Its
`TransactionStatus` contains the transaction id, the domain the transaction is bound to,
`Open`/`Committing`/finished state, pending and completed counts, total statement count, and any
failure error and one-based failing statement. Treat the id as the durable transaction handle:

```rust
client.execute("CREATE DOMAIN production;").await?;
client.set_domain("production").await;

let begun = client.execute("BEGIN;").await?;
let transaction_id = begun
    .transaction
    .as_ref()
    .expect("BEGIN returns transaction status")
    .id
    .clone();

client.execute("CREATE SCHEMA notification (user_id I64);").await?;

// A different Client authenticated as the same user can take over the transaction.
let attached = recovered_client.attach_transaction(transaction_id).await?;
if !attached.success {
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
per-statement `results`; its message contains only the maximum quiesce level actually executed.
If a peer briefly has no leader address while an election converges, the client retries that
bounded interval instead of returning the transient `not-a-leader` outcome.

An explicit attach by another session takes over the binding. Attach to a retained tombstone
returns an unsuccessful command outcome whose transaction status names `Committed`, `Failed`,
`Reverted`, or `Expired`; when commit produced aggregate output, `results` contains that one
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
had, and the preview the client fences `COMMIT` with stays the one its last accepted append
reported. A refused inspection is an unsuccessful outcome whose message names why nothing was read.
