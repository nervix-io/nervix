//! The complete metadata of a Kafka record through its production message owner.
//!
//! Layer: test harness.
//! - **Owns.** Exact assertions on timestamp kinds, nullable keys and null payloads.
//! - **Depends on.** The Kafka message owner and the connector's typed metadata capture.
//! - **Must not know.** Consumer scheduling, host projections or registry state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests the production Kafka message metadata boundary"
    )
)]

use nervix_connector::{RetainedIngestMetadata, RetainedIngestMetadataValue as Value};
use rdkafka::{Timestamp as KafkaTimestamp, message::OwnedMessage};

use super::*;

#[test]
fn record_metadata_keeps_binary_keys_both_timestamp_kinds_and_tombstones() {
    for timestamp in [
        KafkaTimestamp::NotAvailable,
        KafkaTimestamp::CreateTime(1_234),
        KafkaTimestamp::LogAppendTime(5_678),
        KafkaTimestamp::CreateTime(i64::MIN / 1_000_000),
        KafkaTimestamp::LogAppendTime(i64::MAX / 1_000_000),
        KafkaTimestamp::CreateTime(0),
        KafkaTimestamp::LogAppendTime(-2),
        KafkaTimestamp::CreateTime(-1),
        KafkaTimestamp::LogAppendTime(-1),
    ] {
        for key in [None, Some(Vec::new()), Some(vec![0, 255])] {
            for payload in [None, Some(Vec::new()), Some(b"{}".to_vec())] {
                let source = OwnedMessage::new(
                    payload.clone(),
                    key.clone(),
                    "Orders".to_string(),
                    timestamp,
                    3,
                    42,
                    None,
                );
                let message = KafkaTopicIngestorMessage::from_message(source)
                    .assured("the fixture offset and timestamps fit their representations");
                let key_value = match &key {
                    Some(key) => Value::Bytes(key.clone()),
                    None => Value::Absent,
                };
                let time_value = match timestamp.to_millis() {
                    None => Value::Absent,
                    Some(millis) => Value::Datetime(Timestamp::from_unix_nanos(
                        millis
                            .checked_mul(1_000_000)
                            .assured("fixture milliseconds fit"),
                    )),
                };
                let kind_value = match timestamp {
                    KafkaTimestamp::NotAvailable
                    | KafkaTimestamp::CreateTime(-1)
                    | KafkaTimestamp::LogAppendTime(-1) => Value::Absent,
                    KafkaTimestamp::CreateTime(_) => Value::String("create_time".to_string()),
                    KafkaTimestamp::LogAppendTime(_) => {
                        Value::String("log_append_time".to_string())
                    }
                };
                let row = message.metadata();
                let retained = RetainedIngestMetadata::capture(row.fields);
                assert_eq!(
                    retained.values(),
                    [
                        Value::String("Orders".to_string()),
                        Value::I32(3),
                        Value::I64(42),
                        key_value,
                        time_value,
                        kind_value,
                        Value::Bool(payload.is_none()),
                    ],
                    "key {key:?}, timestamp {timestamp:?}, payload {payload:?}"
                );
                assert_eq!(RetainedIngestMetadata::capture(&retained), retained);
                let expected_payload = match &payload {
                    Some(payload) => payload.as_slice(),
                    None => &[],
                };
                assert_eq!(message.payload(), expected_payload);
                assert_eq!(
                    row.position,
                    Some(IngestTransportPosition::Kafka {
                        topic: "Orders",
                        partition: 3,
                        offset: 42,
                    })
                );
            }
        }
    }
}

#[test]
fn a_present_timestamp_outside_the_nanosecond_range_is_a_typed_refusal() {
    for millis in [i64::MIN, i64::MAX] {
        let source = OwnedMessage::new(
            Some(b"{}".to_vec()),
            None,
            "Orders".to_string(),
            KafkaTimestamp::CreateTime(millis),
            3,
            42,
            None,
        );
        let Err(error) = KafkaTopicIngestorMessage::from_message(source) else {
            panic!("an out-of-range Kafka timestamp cannot become a typed metadata instant");
        };
        assert!(error.contains::<nervix_models::TimestampError>());
    }
}

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedBytes {
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=16))]
    bytes: Vec<u8>,
}

#[derive(Debug, bolero::TypeGenerator)]
enum GeneratedTimestamp {
    Unavailable,
    Create(i64),
    LogAppend(i64),
}

impl GeneratedTimestamp {
    fn raw(&self) -> KafkaTimestamp {
        match self {
            Self::Unavailable => KafkaTimestamp::NotAvailable,
            Self::Create(nanos) => KafkaTimestamp::CreateTime(Self::millis(*nanos)),
            Self::LogAppend(nanos) => KafkaTimestamp::LogAppendTime(Self::millis(*nanos)),
        }
    }

    fn millis(nanos: i64) -> i64 {
        // Generate valid whole milliseconds, excluding Kafka's external unavailable sentinel
        // from the domain of present timestamps.
        let millis = nanos / 1_000_000;
        if millis == -1 { 0 } else { millis }
    }
}

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedRecord {
    topic: u8,
    #[generator(0_i32..=i32::MAX)]
    partition: i32,
    #[generator(0_i64..i64::MAX)]
    offset: i64,
    timestamp: GeneratedTimestamp,
    key: Option<GeneratedBytes>,
    payload: Option<GeneratedBytes>,
}

impl GeneratedRecord {
    fn assert_metadata_exact(&self) {
        let topic = format!("Topic-{}", self.topic);
        let partition = self.partition;
        let offset = self.offset;
        let key = self.key.as_ref().map(|key| key.bytes.clone());
        let payload = self.payload.as_ref().map(|payload| payload.bytes.clone());
        let timestamp = self.timestamp.raw();
        let key_value = match &key {
            Some(key) => Value::Bytes(key.clone()),
            None => Value::Absent,
        };
        let time_value = match timestamp.to_millis() {
            Some(millis) => Value::Datetime(Timestamp::from_unix_nanos(
                millis
                    .checked_mul(1_000_000)
                    .assured("generated milliseconds came from a signed nanosecond timestamp"),
            )),
            None => Value::Absent,
        };
        let kind_value = if timestamp.to_millis().is_none() {
            Value::Absent
        } else {
            match timestamp {
                KafkaTimestamp::NotAvailable => Value::Absent,
                KafkaTimestamp::CreateTime(_) => Value::String("create_time".to_string()),
                KafkaTimestamp::LogAppendTime(_) => Value::String("log_append_time".to_string()),
            }
        };
        let expected = RetainedIngestMetadata::from_values(vec![
            Value::String(topic.clone()),
            Value::I32(partition),
            Value::I64(offset),
            key_value,
            time_value,
            kind_value,
            Value::Bool(payload.is_none()),
        ]);
        let source = OwnedMessage::new(
            payload.clone(),
            key,
            topic.clone(),
            timestamp,
            partition,
            offset,
            None,
        );
        let message = KafkaTopicIngestorMessage::from_message(source)
            .assured("generated timestamps and offsets fit the message's representations");
        let row = message.metadata();
        let retained = RetainedIngestMetadata::capture(row.fields);
        assert_eq!(retained, expected);
        assert_eq!(RetainedIngestMetadata::capture(&retained), expected);
        let expected_payload = match &payload {
            Some(payload) => payload.as_slice(),
            None => &[],
        };
        assert_eq!(message.payload(), expected_payload);
        assert_eq!(
            row.position,
            Some(IngestTransportPosition::Kafka {
                topic: &topic,
                partition,
                offset,
            })
        );
        assert_eq!(
            message.position().offset,
            offset
                .checked_add(1)
                .assured("the generated offset has a representable successor")
        );
    }
}

#[test]
fn bolero_kafka_record_metadata_preserves_every_field() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .with_type::<GeneratedRecord>()
        .for_each(GeneratedRecord::assert_metadata_exact);
}
