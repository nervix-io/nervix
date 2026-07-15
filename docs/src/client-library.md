#  Rust Client Library

The workspace includes `nervix-client-core`, a native Rust client library built on the same session gRPC API used by `nervix-cli`.

Capabilities:

- `Client::connect(...)`
- `Client::execute(...)`
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
        .with_filter_map("SET orders.normalized = lower(orders.tenant) UNSET orders.raw WHERE orders.tenant = \"acme\"");
    client.subscribe(&request).await?;

    let event = client.next_subscription().await?;
    println!("{}", event.payload);
    client.unsubscribe("sampled_orders").await?;
    Ok(())
}
```
