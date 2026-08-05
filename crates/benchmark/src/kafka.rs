use std::{io, sync::Arc as StdArc, time::Duration};

use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    error::RDKafkaErrorCode,
};

const METADATA_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn provision_topics(
    bootstrap_servers: &str,
    input_topic: &str,
    output_topic: &str,
    partitions: u32,
    timeout: Duration,
) -> io::Result<()> {
    let partitions = i32::try_from(partitions)
        .map_err(|_| io::Error::other("Kafka partition count exceeds i32"))?;
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .map_err(io::Error::other)?;
    let topics = [
        NewTopic::new(input_topic, partitions, TopicReplication::Fixed(1)),
        NewTopic::new(output_topic, partitions, TopicReplication::Fixed(1)),
    ];
    let deadline = tokio::time::Instant::now() + timeout;
    let results = tokio::time::timeout(timeout, admin.create_topics(&topics, &AdminOptions::new()))
        .await
        .map_err(|_| io::Error::other("timed out creating Kafka benchmark topics"))?
        .map_err(io::Error::other)?;
    for result in results {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic, code)) => {
                return Err(io::Error::other(format!(
                    "failed to create Kafka topic '{topic}': {code:?}"
                )));
            }
        }
    }

    let expected = usize::try_from(partitions)
        .map_err(|_| io::Error::other("Kafka partition count exceeds usize"))?;
    let admin = StdArc::new(admin);
    for topic in [input_topic, output_topic] {
        loop {
            tokio::task::consume_budget().await;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::other(format!(
                    "Kafka topic '{topic}' did not reach {expected} partitions before timeout"
                )));
            }
            let request_timeout = remaining.min(METADATA_ATTEMPT_TIMEOUT);
            let admin = StdArc::clone(&admin);
            let topic_name = topic.to_string();
            let observed = tokio::task::spawn_blocking(move || {
                admin
                    .inner()
                    .fetch_metadata(Some(&topic_name), request_timeout)
                    .ok()
                    .and_then(|metadata| {
                        metadata
                            .topics()
                            .iter()
                            .find(|metadata| metadata.name() == topic_name)
                            .map(|metadata| metadata.partitions().len())
                    })
            })
            .await
            .map_err(|error| io::Error::other(format!("Kafka metadata task failed: {error}")))?;
            if observed == Some(expected) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::other(format!(
                    "Kafka topic '{topic}' did not reach {expected} partitions before timeout; \
                     observed {observed:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}
