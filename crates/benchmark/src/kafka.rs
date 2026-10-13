use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_primitives::sync::StdArc;
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication, TopicResult},
    client::DefaultClientContext,
    error::RDKafkaErrorCode,
};
use thiserror::Error;

const METADATA_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Why the benchmark's Kafka topics were not provisioned. The Kafka client's own error, when there
/// is one, is the frame beneath.
#[derive(Debug, Error)]
pub enum TopicProvisionError {
    #[error("{partitions} Kafka partitions are more than a Kafka topic can have")]
    PartitionCount { partitions: u32 },
    #[error("failed to create a Kafka admin client for '{bootstrap_servers}'")]
    AdminClient { bootstrap_servers: String },
    #[error("creating the Kafka benchmark topics did not finish within {timeout:?}")]
    CreateTimeout { timeout: Duration },
    #[error("failed to create the Kafka benchmark topics")]
    CreateTopics,
    #[error("Kafka refused to create topic '{topic}' with {code:?}")]
    CreateTopic {
        topic: String,
        code: RDKafkaErrorCode,
    },
    #[error("the metadata check of Kafka topic '{topic}' did not run to completion")]
    MetadataCheck { topic: String },
    #[error(
        "Kafka topic '{topic}' did not reach {expected} partitions within {timeout:?}; observed \
         {observed:?}"
    )]
    PartitionsNotReady {
        topic: String,
        expected: usize,
        observed: Option<usize>,
        timeout: Duration,
    },
}

pub async fn provision_topics(
    bootstrap_servers: &str,
    input_topic: &str,
    output_topic: &str,
    partitions: u32,
    timeout: Duration,
) -> error_stack::Result<(), TopicProvisionError> {
    let Ok(partition_count) = i32::try_from(partitions) else {
        return Err(Report::new(TopicProvisionError::PartitionCount {
            partitions,
        }));
    };
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .change_context_lazy(|| TopicProvisionError::AdminClient {
            bootstrap_servers: bootstrap_servers.to_string(),
        })?;
    let topics = [
        NewTopic::new(input_topic, partition_count, TopicReplication::Fixed(1)),
        NewTopic::new(output_topic, partition_count, TopicReplication::Fixed(1)),
    ];
    let deadline = nervix_primitives::time::Instant::now() + timeout;
    let created = nervix_primitives::time::timeout(
        timeout,
        admin.create_topics(&topics, &AdminOptions::new()),
    )
    .await;
    let Ok(created) = created else {
        return Err(Report::new(TopicProvisionError::CreateTimeout { timeout }));
    };
    let results = created.change_context(TopicProvisionError::CreateTopics)?;
    ensure_topics_created(results)?;

    let expected = usize::try_from(partition_count)
        .assured("the partition count was converted from a non-negative u32 above");
    let admin = StdArc::new(admin);
    for topic in [input_topic, output_topic] {
        loop {
            nervix_primitives::task::consume_budget().await;
            let remaining =
                deadline.saturating_duration_since(nervix_primitives::time::Instant::now());
            if remaining.is_zero() {
                return Err(Report::new(TopicProvisionError::PartitionsNotReady {
                    topic: topic.to_string(),
                    expected,
                    observed: None,
                    timeout,
                }));
            }
            let request_timeout = remaining.min(METADATA_ATTEMPT_TIMEOUT);
            let admin = StdArc::clone(&admin);
            let topic_name = topic.to_string();
            let observed = nervix_primitives::task::spawn_blocking(move || {
                let Ok(metadata) = admin
                    .inner()
                    .fetch_metadata(Some(&topic_name), request_timeout)
                else {
                    return None;
                };
                metadata
                    .topics()
                    .iter()
                    .find(|metadata| metadata.name() == topic_name)
                    .map(|metadata| metadata.partitions().len())
            })
            .await
            .change_context_lazy(|| TopicProvisionError::MetadataCheck {
                topic: topic.to_string(),
            })?;
            if observed == Some(expected) {
                break;
            }
            if nervix_primitives::time::Instant::now() >= deadline {
                return Err(Report::new(TopicProvisionError::PartitionsNotReady {
                    topic: topic.to_string(),
                    expected,
                    observed,
                    timeout,
                }));
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

/// Accepts the creation result of every benchmark topic: Kafka created it now, or it already
/// existed.
fn ensure_topics_created(
    results: Vec<TopicResult>,
) -> error_stack::Result<(), TopicProvisionError> {
    for result in results {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic, code)) => {
                return Err(Report::new(TopicProvisionError::CreateTopic {
                    topic,
                    code,
                }));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::OptionExt as _;
    use rdkafka::error::RDKafkaErrorCode;

    use super::{TopicProvisionError, ensure_topics_created, provision_topics};

    #[nervix_primitives::test]
    async fn a_partition_count_kafka_cannot_hold_is_refused_before_connecting() {
        let error = provision_topics(
            "127.0.0.1:1",
            "input",
            "output",
            u32::MAX,
            Duration::from_secs(1),
        )
        .await
        .expect_err("no topic can have u32::MAX partitions");

        assert!(
            matches!(
                error.current_context(),
                TopicProvisionError::PartitionCount {
                    partitions: u32::MAX
                }
            ),
            "{error:?}"
        );
    }

    #[nervix_primitives::test]
    async fn topics_a_broker_never_creates_end_at_the_provisioning_timeout() {
        let timeout = Duration::from_millis(200);
        let provisioned = provision_topics("127.0.0.1:1", "input", "output", 1, timeout).await;
        let error = provisioned
            .err()
            .assured("nothing listens on port 1, so no broker creates the topics");

        assert!(
            matches!(
                error.current_context(),
                TopicProvisionError::CreateTimeout { timeout: waited } if waited == &timeout
            ),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "creating the Kafka benchmark topics did not finish within 200ms"
        );
    }

    #[nervix_primitives::test]
    async fn a_bootstrap_address_the_kafka_client_cannot_hold_is_refused() {
        let error = provision_topics("broker\0", "input", "output", 1, Duration::from_secs(1))
            .await
            .expect_err("a bootstrap address with a NUL byte cannot configure a client");

        assert!(
            matches!(
                error.current_context(),
                TopicProvisionError::AdminClient { bootstrap_servers }
                    if bootstrap_servers == "broker\0"
            ),
            "{error:?}"
        );
    }

    #[test]
    fn a_topic_that_already_exists_counts_as_created_and_a_refusal_names_its_topic() {
        ensure_topics_created(vec![
            Ok("input".to_string()),
            Err(("output".to_string(), RDKafkaErrorCode::TopicAlreadyExists)),
        ])
        .expect("an existing topic is a created topic");

        let error = ensure_topics_created(vec![
            Ok("input".to_string()),
            Err(("output".to_string(), RDKafkaErrorCode::PolicyViolation)),
        ])
        .expect_err("a topic Kafka refused is not created");

        assert!(
            matches!(
                error.current_context(),
                TopicProvisionError::CreateTopic {
                    topic,
                    code: RDKafkaErrorCode::PolicyViolation
                } if topic == "output"
            ),
            "{error:?}"
        );
    }
}
