use nervix_client_core::{Client, SubscriptionRequest};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("http://127.0.0.1:47391", "default").await?;

    let result = client.execute("SHOW CLUSTER STATUS;").await?;
    println!("{}", result.message);

    let request = SubscriptionRequest::new("orders")
        .with_filter_map("SET normalized = lower(tenant) UNSET raw WHERE tenant = \"acme\"");
    let result = client.subscribe(&request).await?;
    println!("{}", result.message);

    let event = client.next_subscription().await?;
    println!(
        "subscription [{}] from [{}]: {}",
        event.subscription, event.relay, event.payload
    );

    Ok(())
}
