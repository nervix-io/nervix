//! Connects to a local server, runs one statement, opens a subscription and prints the rows of
//! its first batch.

#[cfg(not(feature = "shuttle"))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use nervix_client_core::{Client, DomainName, SubscriptionEvent, SubscriptionRequest};

    let domain = DomainName::try_from("default")?;
    let client = Client::connect("http://127.0.0.1:47391", Some(domain)).await?;

    let result = client.execute("SHOW CLUSTER STATUS;").await?;
    println!("{}", result.message);

    let where_clause = nervix_nspl::parse_expression("input.tenant = 'acme'").map_err(|error| {
        std::io::Error::other(format!("invalid subscription expression: {error:?}"))
    })?;
    let request = SubscriptionRequest::new("acme_orders", "orders").with_where_clause(where_clause);
    let result = client.subscribe(&request).await?;
    println!("{}", result.message);

    let SubscriptionEvent::Rows(event) = client.next_subscription().await? else {
        return Ok(());
    };
    let lines = event
        .display_lines()
        .map_err(|report| report.current_context().clone())?;
    for line in lines {
        println!(
            "subscription [{}] from [{}]: {line}",
            event.rows.subscription().name,
            event.relay
        );
    }

    Ok(())
}

/// A Shuttle build replaces the client's synchronization with models that only run inside a
/// Shuttle test, so the example talks to a server only in a production build.
#[cfg(feature = "shuttle")]
fn main() {}
