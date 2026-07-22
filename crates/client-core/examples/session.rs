use nervix_client_core::{Client, SubscriptionRequest};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("http://127.0.0.1:47391", "default").await?;

    let result = client.execute("SHOW CLUSTER STATUS;").await?;
    println!("{}", result.message);

    let where_clause = nervix_nspl::parse_expression("input.tenant = 'acme'").map_err(|error| {
        std::io::Error::other(format!("invalid subscription expression: {error:?}"))
    })?;
    let request = SubscriptionRequest::new("acme_orders", "orders").with_where_clause(where_clause);
    let result = client.subscribe(&request).await?;
    println!("{}", result.message);

    let event = client.next_subscription().await?;
    println!(
        "subscription [{}] from [{}]: {}",
        event.subscription, event.relay, event.payload
    );

    Ok(())
}
