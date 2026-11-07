//! Independent broker observations of emitter keys and destination partitions.
//!
//! Layer: test harness.
//! - **Owns.** Exact byte-key and partition assertions on messages read from real brokers.
//! - **Depends on.** The scenario world, its broker observer and maintained base64 decoding.
//! - **Must not know.** Nervix envelope evaluation, connector packing or producer routing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the independent public broker observations")
)]

use super::*;
use crate::common::dependencies::PULSAR_ADMIN_ADDR;

#[given(expr = "Pulsar key {string} routes among {int} partitions as {string}")]
fn pulsar_key_partition(
    world: &mut ScenarioWorld,
    key: String,
    partitions: usize,
    placeholder: String,
) {
    assert!(partitions > 0, "the destination has at least one partition");
    let partition =
        pulsar::routing_policy::RoutingPolicy::compute_partition_index_for_key(&key, partitions);
    world
        .placeholders
        .insert(placeholder, partition.to_string());
}

#[given(expr = "Pulsar topic {string} exists with {int} partitions")]
async fn partitioned_pulsar_topic(world: &mut ScenarioWorld, topic: String, partitions: u16) {
    let topic = expand_placeholders(world, &topic);
    let admin = world
        .dependencies
        .endpoints()
        .get(PULSAR_ADMIN_ADDR)
        .assured("the Pulsar fixture publishes its admin address");
    let mut url = url::Url::parse(admin).assured("the admin address is a URL");
    url.path_segments_mut()
        .assured("the admin URL has a host")
        .clear()
        .extend([
            "admin",
            "v2",
            "persistent",
            "public",
            "default",
            &topic,
            "partitions",
        ]);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .assured("the fixture client has a bounded request");
    client
        .put(url.clone())
        .json(&partitions)
        .send()
        .await
        .assured("the partition fixture reaches the broker")
        .error_for_status()
        .assured("the topic is explicitly provisioned");
    let observed: serde_json::Value = client
        .get(url)
        .send()
        .await
        .assured("the broker answers its partition metadata")
        .error_for_status()
        .assured("the topic metadata exists")
        .json()
        .await
        .assured("the admin reply is JSON");
    assert_eq!(observed["partitions"].as_u64(), Some(u64::from(partitions)));
}

/// A complete payload and its exact external key, including an absent key and non-UTF-8 bytes.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct KeyedMessage {
    payload: Vec<u8>,
    key: Option<Vec<u8>>,
}

impl BrokerMessageExpectation for KeyedMessage {
    fn delivered(message: &BrokerMessage) -> Self {
        Self {
            payload: message.bytes.clone(),
            key: message.key.clone(),
        }
    }

    fn describe(&self) -> String {
        format!(
            "{} with key {:?}",
            describe_broker_payload(&self.payload),
            self.key
        )
    }
}

#[then(expr = "within {string} the observed broker receives exactly these byte-keyed messages")]
async fn byte_keyed_messages(world: &mut ScenarioWorld, duration: String, #[step] step: &Step) {
    let table = step.table.as_ref().assured("the step lists its messages");
    let mut rows = table.rows.iter();
    assert_eq!(
        rows.next()
            .assured("the table names its columns")
            .as_slice(),
        ["payload", "key base64"]
    );
    let mut expected = BTreeMap::new();
    for row in rows {
        let [payload, key] = row.as_slice() else {
            panic!("each message names its payload and base64 key");
        };
        let key = match key.as_str() {
            "none" => None,
            _ => Some(
                BASE64_STANDARD
                    .decode(key)
                    .assured("the expected key is base64"),
            ),
        };
        let message = KeyedMessage {
            payload: expand_placeholders(world, payload).into_bytes(),
            key,
        };
        let count = expected.entry(message).or_insert(0_usize);
        *count = count
            .checked_add(1)
            .assured("the table's rows are held in memory");
    }
    receive_exactly_these_broker_messages(world, &duration, expected).await;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PartitionedMessage {
    message: KeyedMessage,
    partition: Option<i32>,
}

impl BrokerMessageExpectation for PartitionedMessage {
    fn delivered(message: &BrokerMessage) -> Self {
        Self {
            message: KeyedMessage::delivered(message),
            partition: message.partition,
        }
    }

    fn describe(&self) -> String {
        format!(
            "{} in partition {:?}",
            self.message.describe(),
            self.partition
        )
    }
}

#[then(expr = "within {string} the observed broker receives exactly these partitioned messages")]
async fn partitioned_messages(world: &mut ScenarioWorld, duration: String, #[step] step: &Step) {
    let table = step.table.as_ref().assured("the step lists its messages");
    let mut rows = table.rows.iter();
    assert_eq!(
        rows.next()
            .assured("the table names its columns")
            .as_slice(),
        ["payload", "key base64", "partition"]
    );
    let mut expected = BTreeMap::new();
    for row in rows {
        let [payload, key, partition] = row.as_slice() else {
            panic!("each message names its payload, byte key and partition");
        };
        let message = PartitionedMessage {
            message: KeyedMessage {
                payload: expand_placeholders(world, payload).into_bytes(),
                key: match key.as_str() {
                    "none" => None,
                    _ => Some(
                        BASE64_STANDARD
                            .decode(key)
                            .assured("the expected key is base64"),
                    ),
                },
            },
            partition: Some(
                expand_placeholders(world, partition)
                    .parse()
                    .assured("the expected partition is an integer"),
            ),
        };
        let count = expected.entry(message).or_insert(0_usize);
        *count = count
            .checked_add(1)
            .assured("the table's rows are held in memory");
    }
    receive_exactly_these_broker_messages(world, &duration, expected).await;
}
