//! Public Kafka record metadata probes.
//!
//! Layer: test harness.
//! - **Owns.** Producing explicitly keyed, timestamped and null-valued Kafka records.
//! - **Depends on.** The public scenario cluster and the Kafka producer driver.
//! - **Must not know.** Ingestor internals, metadata projection or acknowledgement state.

use nervix_connector_kafka::testing_rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use nervix_models::Timestamp;

use super::*;

/// The same client and queue limits as the Kafka scenario publishers.
const PUBLISH_LIMIT: Duration = Duration::from_secs(5);
/// Observe a delivery after the client's five-second limit can finish it.
const PUBLISH_OBSERVATION: Duration = Duration::from_secs(6);

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KafkaMetadataRecord {
    key: Option<Vec<u8>>,
    payload: Option<String>,
    #[serde(deserialize_with = "record_timestamp")]
    timestamp: Timestamp,
}

fn record_timestamp<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Timestamp, D::Error> {
    let source = <String as serde::Deserialize>::deserialize(deserializer)?;
    source.parse().map_err(serde::de::Error::custom)
}

impl KafkaMetadataRecord {
    pub(crate) async fn publish(
        &self,
        mut config: ClientConfig,
        topic: &str,
    ) -> std::io::Result<()> {
        let timeout_ms = PUBLISH_LIMIT.as_millis().to_string();
        let producer: FutureProducer = config
            .set("delivery.timeout.ms", &timeout_ms)
            .set("request.timeout.ms", &timeout_ms)
            .create()
            .map_err(std::io::Error::other)?;
        let mut record = FutureRecord::<[u8], str>::to(topic)
            .partition(0)
            .timestamp(self.timestamp.unix_nanos().div_euclid(1_000_000));
        if let Some(key) = &self.key {
            record = record.key(key);
        }
        if let Some(payload) = &self.payload {
            record = record.payload(payload);
        }
        let delivered = nervix_primitives::time::timeout(
            PUBLISH_OBSERVATION,
            producer.send(record, PUBLISH_LIMIT),
        )
        .await
        .map_err(std::io::Error::other)?;
        delivered.map_err(|(error, _)| std::io::Error::other(error))?;
        Ok(())
    }
}

#[when(expr = "these Kafka records are published to topic {string}")]
async fn publish_records(world: &mut ScenarioWorld, topic: String, #[step] step: &Step) {
    let topic = expand_placeholders(world, &topic);
    let source = expand_placeholders(world, docstring(step));
    let records: Vec<KafkaMetadataRecord> =
        serde_json::from_str(&source).assured("the fixture lists current Kafka records");
    for record in records {
        world
            .cluster()
            .publish_kafka_metadata_record(&topic, &record)
            .await
            .assured("the broker confirms the fixture record");
    }
}
