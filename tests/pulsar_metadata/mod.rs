//! Public Pulsar metadata fixtures.
//!
//! Layer: test harness.
//! - **Owns.** Explicit producer metadata and independent broker observations for the public
//!   metadata scenarios.
//! - **Depends on.** The scenario world and the Pulsar driver.
//! - **Must not know.** The Nervix ingestor, its metadata visitor or its acknowledgement state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public Pulsar metadata fixtures")
)]

use std::time::Duration;

use cucumber::{gherkin::Step, when};
use futures_util::StreamExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use pulsar::{ConsumerOptions, Pulsar, TokioExecutor, consumer::InitialPosition};

use super::{ScenarioWorld, docstring, expand_placeholders};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataRecord {
    payload: String,
    key: Option<String>,
    ordering_key: Option<Vec<u8>>,
    event_time: Option<u64>,
}

#[when(expr = "these Pulsar metadata records are published to topic {string}")]
async fn publish_records(world: &mut ScenarioWorld, topic: String, #[step] step: &Step) {
    let topic = expand_placeholders(world, &topic);
    let topic = format!("persistent://public/default/{topic}");
    let source = expand_placeholders(world, docstring(step));
    let records: Vec<MetadataRecord> =
        serde_json::from_str(&source).assured("the fixture lists current Pulsar messages");
    assert!((1..=16).contains(&records.len()), "the fixture is bounded");
    let addr = expand_placeholders(world, "{{pulsar_addr}}");
    let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor)
        .build()
        .await
        .assured("the scenario broker accepts its fixture client");
    let mut observer = client
        .consumer()
        .with_topic(&topic)
        .with_subscription(format!("metadata_observer_{}", world.test_id))
        .with_options(
            ConsumerOptions::default()
                .with_initial_position(InitialPosition::Earliest)
                .with_ack_receipt_enabled(true),
        )
        .build::<Vec<u8>>()
        .await
        .assured("the fixture owns its independent observer subscription");
    let producer_name = format!("metadata_producer_{}", world.test_id);
    let mut producer = client
        .producer()
        .with_topic(&topic)
        .with_name(&producer_name)
        .build()
        .await
        .assured("the fixture topic was explicitly provisioned");
    for (index, record) in records.into_iter().enumerate() {
        nervix_primitives::task::consume_budget().await;
        let message = pulsar::producer::Message {
            payload: record.payload.clone().into_bytes(),
            properties: [("Case".to_owned(), "kept".to_owned())].into(),
            partition_key: record.key,
            ordering_key: record.ordering_key,
            event_time: record.event_time,
            ..Default::default()
        };
        let receipt = producer
            .send_non_blocking(message)
            .await
            .assured("the fixture message is enqueued")
            .await
            .assured("the broker confirms the fixture message");
        let observed = nervix_primitives::time::timeout(Duration::from_secs(30), observer.next())
            .await
            .assured("the independent broker observation is bounded")
            .verified("the observer remains open")
            .assured("the broker sends a current Pulsar message");
        assert_eq!(observed.payload.data, record.payload.as_bytes());
        let observed_id = observed.message_id();
        let receipt_id = receipt
            .message_id
            .as_ref()
            .verified("the broker receipt identifies the stored fixture message");
        assert_eq!(observed_id.ledger_id, receipt_id.ledger_id);
        assert_eq!(observed_id.entry_id, receipt_id.entry_id);
        assert_eq!(
            observed_id.partition.unwrap_or(-1),
            receipt_id.partition.unwrap_or(-1)
        );
        assert_eq!(
            observed_id.batch_index.unwrap_or(-1),
            receipt_id.batch_index.unwrap_or(-1)
        );
        let id = observed.message_id();
        let mut message_id = format!(
            "{}:{}:{}",
            id.ledger_id,
            id.entry_id,
            id.partition.unwrap_or(-1)
        );
        if let Some(batch_index) = id.batch_index {
            message_id.push_str(&format!(":{batch_index}"));
        }
        let number = index + 1;
        world
            .placeholders
            .insert(format!("pulsar_message_id_{number}"), message_id);
        let nanos = i64::try_from(observed.metadata().publish_time)
            .assured("fixture time is within the signed millisecond range")
            .checked_mul(1_000_000)
            .assured("fixture time is within the signed nanosecond range");
        let publish_time = nervix_models::Timestamp::from_unix_nanos(nanos)
            .as_datetime()
            .to_rfc3339();
        world
            .placeholders
            .insert(format!("pulsar_publish_time_{number}"), publish_time);
        world.placeholders.insert(
            format!("pulsar_sequence_id_{number}"),
            observed.metadata().sequence_id.to_string(),
        );
        observer
            .ack(&observed)
            .await
            .assured("the independent observer settles its own message");
    }
    observer
        .close()
        .await
        .assured("the independent observer closes");
    producer
        .close()
        .await
        .assured("the fixture producer closes");
    world
        .placeholders
        .insert("pulsar_producer_name".to_owned(), producer_name);
}
