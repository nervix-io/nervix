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
`TransactionStatus` contains the transaction id, `Open`/`Committing`/finished state, pending and
completed counts, total statement count, and any failure error and one-based failing statement.
Treat the id as the durable transaction handle:

```rust
let begun = client.execute("BEGIN;").await?;
let transaction_id = begun
    .transaction
    .as_ref()
    .expect("BEGIN returns transaction status")
    .id
    .clone();

client.execute("CREATE DOMAIN production;").await?;

// A different Client authenticated as the same user can take over the transaction.
let attached = recovered_client.attach_transaction(transaction_id).await?;
if !attached.success {
    eprintln!("attach outcome: {}", attached.message);
}
```

The client automatically attaches its active transaction after a leader redirect or transport
reconnect before retrying a command. It compares the attached status with the status it last saw:
if replicated queue or commit progress already records the operation, it returns that state instead
of repeating the operation. A recovered committed operation is returned as a successful aggregate
`CommandOutcome` with its retained per-statement `results`.

An explicit attach by another session takes over the binding. Attach to a retained tombstone
returns an unsuccessful command outcome whose transaction status names `Committed`, `Failed`,
`Reverted`, or `Expired`; its `results` contain the retained per-statement commit results. Attach to
an id removed after retention returns an unknown-id outcome. `transaction_status()` keeps the
latest structured status so an interactive caller can render `Open` and `Committing` differently.
