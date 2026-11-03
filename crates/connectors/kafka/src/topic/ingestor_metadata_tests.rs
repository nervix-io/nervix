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

use meticulous::ResultExt as _;
use nervix_connector::{RetainedIngestMetadata, RetainedIngestMetadataValue as Value};
use nervix_models::IngestorMetadataType;
use rdkafka::{Timestamp as KafkaTimestamp, message::OwnedMessage};

use super::*;

/// The last whole millisecond a signed count of nanoseconds holds, in 2262-04-11.
const LAST_HELD_MILLIS: i64 = i64::MAX / 1_000_000;
/// The first whole millisecond a signed count of nanoseconds holds, in 1677-09-21.
const FIRST_HELD_MILLIS: i64 = i64::MIN / 1_000_000;
/// The first whole millisecond after every instant a `DATETIME` holds.
const AFTER_THE_LAST_MILLIS: i64 = LAST_HELD_MILLIS + 1;
/// The last whole millisecond before every instant a `DATETIME` holds.
const BEFORE_THE_FIRST_MILLIS: i64 = FIRST_HELD_MILLIS - 1;
/// How far past [`AFTER_THE_LAST_MILLIS`] Kafka's signed milliseconds reach.
const LATER_EXCESS_MAX: i64 = i64::MAX - AFTER_THE_LAST_MILLIS;
/// How far before [`BEFORE_THE_FIRST_MILLIS`] Kafka's signed milliseconds reach.
const EARLIER_EXCESS_MAX: i64 = BEFORE_THE_FIRST_MILLIS - i64::MIN;

const _: () = assert!(LAST_HELD_MILLIS.checked_mul(1_000_000).is_some());
const _: () = assert!(FIRST_HELD_MILLIS.checked_mul(1_000_000).is_some());
const _: () = assert!(AFTER_THE_LAST_MILLIS.checked_mul(1_000_000).is_none());
const _: () = assert!(BEFORE_THE_FIRST_MILLIS.checked_mul(1_000_000).is_none());

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

/// The driver's variant of a present timestamp kind, and the kind its metadata reads as.
struct PresentKind {
    stamp: fn(i64) -> KafkaTimestamp,
    metadata: &'static str,
}

const PRESENT_KINDS: [PresentKind; 2] = [
    PresentKind {
        stamp: KafkaTimestamp::CreateTime,
        metadata: "create_time",
    },
    PresentKind {
        stamp: KafkaTimestamp::LogAppendTime,
        metadata: "log_append_time",
    },
];

#[test]
fn a_present_timestamp_no_datetime_holds_is_handed_over_unrepresentable_with_its_kind() {
    for (millis, written) in [
        (AFTER_THE_LAST_MILLIS, "9223372036855 milliseconds after"),
        // Microseconds written where Kafka specifies milliseconds.
        (1_767_225_600_000_000, "1767225600000000 milliseconds after"),
        (i64::MAX, "9223372036854775807 milliseconds after"),
        (BEFORE_THE_FIRST_MILLIS, "9223372036855 milliseconds before"),
        (i64::MIN, "9223372036854775808 milliseconds before"),
    ] {
        for kind in &PRESENT_KINDS {
            let source = OwnedMessage::new(
                Some(b"{}".to_vec()),
                None,
                "Orders".to_string(),
                (kind.stamp)(millis),
                3,
                42,
                None,
            );
            let message = KafkaTopicIngestorMessage::from_message(source)
                .assured("offset 42 has a next offset");
            let retained = RetainedIngestMetadata::capture(message.metadata().fields);
            assert_eq!(
                retained.values(),
                [
                    Value::String("Orders".to_string()),
                    Value::I32(3),
                    Value::I64(42),
                    Value::Absent,
                    Value::Unrepresentable {
                        ty: IngestorMetadataType::Datetime,
                        value: format!("the Kafka timestamp {written} the Unix epoch"),
                    },
                    Value::String(kind.metadata.to_string()),
                    Value::Bool(false),
                ],
                "{millis} milliseconds of kind {}",
                kind.metadata
            );
            assert_eq!(RetainedIngestMetadata::capture(&retained), retained);
        }
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
    Create(GeneratedMillis),
    LogAppend(GeneratedMillis),
}

impl GeneratedTimestamp {
    fn raw(&self) -> KafkaTimestamp {
        match self {
            Self::Unavailable => KafkaTimestamp::NotAvailable,
            Self::Create(millis) => KafkaTimestamp::CreateTime(millis.millis()),
            Self::LogAppend(millis) => KafkaTimestamp::LogAppendTime(millis.millis()),
        }
    }

    /// The value `metadata.timestamp` reads for this timestamp.
    fn expected_time(&self) -> Value {
        match self {
            Self::Unavailable => Value::Absent,
            Self::Create(millis) | Self::LogAppend(millis) => millis.expected(),
        }
    }

    /// The value `metadata.timestamp_type` reads for this timestamp.
    fn expected_kind(&self) -> Value {
        match self {
            Self::Unavailable => Value::Absent,
            Self::Create(_) => Value::String("create_time".to_string()),
            Self::LogAppend(_) => Value::String("log_append_time".to_string()),
        }
    }
}

/// The milliseconds of a present timestamp: whole milliseconds a signed nanosecond `DATETIME`
/// holds, or a count Kafka's signed milliseconds reach past either end of those instants.
#[derive(Debug, bolero::TypeGenerator)]
enum GeneratedMillis {
    /// The whole milliseconds of this many signed nanoseconds.
    Held(i64),
    /// This many milliseconds after [`AFTER_THE_LAST_MILLIS`].
    Later(#[generator(0_i64..=LATER_EXCESS_MAX)] i64),
    /// This many milliseconds before [`BEFORE_THE_FIRST_MILLIS`].
    Earlier(#[generator(0_i64..=EARLIER_EXCESS_MAX)] i64),
}

impl GeneratedMillis {
    fn millis(&self) -> i64 {
        match self {
            Self::Held(nanos) => {
                // Generate valid whole milliseconds, excluding Kafka's external unavailable
                // sentinel from the domain of present timestamps.
                let millis = nanos / 1_000_000;
                if millis == -1 { 0 } else { millis }
            }
            Self::Later(excess) => AFTER_THE_LAST_MILLIS
                .checked_add(*excess)
                .assured("the excess is bounded by LATER_EXCESS_MAX"),
            Self::Earlier(excess) => BEFORE_THE_FIRST_MILLIS
                .checked_sub(*excess)
                .assured("the excess is bounded by EARLIER_EXCESS_MAX"),
        }
    }

    /// The value these milliseconds read as: the `DATETIME` they name, or a description of the
    /// milliseconds no `DATETIME` holds.
    fn expected(&self) -> Value {
        let millis = self.millis();
        match self {
            Self::Held(_) => Value::Datetime(Timestamp::from_unix_nanos(
                millis
                    .checked_mul(1_000_000)
                    .assured("held milliseconds came from a signed nanosecond timestamp"),
            )),
            Self::Later(_) => Value::Unrepresentable {
                ty: IngestorMetadataType::Datetime,
                value: format!("the Kafka timestamp {millis} milliseconds after the Unix epoch"),
            },
            Self::Earlier(_) => Value::Unrepresentable {
                ty: IngestorMetadataType::Datetime,
                value: format!(
                    "the Kafka timestamp {} milliseconds before the Unix epoch",
                    millis.unsigned_abs()
                ),
            },
        }
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
        let time_value = self.timestamp.expected_time();
        let kind_value = self.timestamp.expected_kind();
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
            .assured("a generated offset below i64::MAX has a next offset");
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
