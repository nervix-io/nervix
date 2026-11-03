//! Pulsar messages through the production metadata owner.
//!
//! Layer: test harness.
//! - **Owns.** Complete metadata and header oracles, absence boundaries and checked refusals.
//! - **Depends on.** The received-message owner, driver messages and retained metadata contract.
//! - **Must not know.** Registry state, runtime projections or consumer scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the received-message boundary")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    RetainedIngestHeaders, RetainedIngestMetadata, RetainedIngestMetadataValue as Value,
};
use nervix_models::{Timestamp, TimestampError};
use pulsar::{
    Payload,
    consumer::data::MessageData,
    message::proto::{KeyValue, MessageMetadata},
};

use super::*;

const MAX_MILLIS: u64 = 9_223_372_036_854;

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedInstant {
    #[generator(0_u64..=MAX_MILLIS)]
    millis: u64,
}

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedIndex {
    #[generator(0_i32..=i32::MAX)]
    value: i32,
}

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedText {
    #[generator(bolero::generator::produce_with::<String>().len(0_usize..=8))]
    text: String,
}

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedBytes {
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=8))]
    bytes: Vec<u8>,
}

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedPayload {
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=16))]
    bytes: Vec<u8>,
}

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedMessage {
    ledger: u64,
    entry: u64,
    partition: Option<GeneratedIndex>,
    batch_index: Option<GeneratedIndex>,
    #[generator(0_u64..=MAX_MILLIS)]
    publish_millis: u64,
    event_millis: Option<GeneratedInstant>,
    #[generator(0_i64..=i64::MAX)]
    sequence: i64,
    redelivery_count: Option<u32>,
    key: Option<GeneratedText>,
    ordering_key: Option<GeneratedBytes>,
    producer: GeneratedText,
    payload: GeneratedPayload,
}

impl GeneratedMessage {
    fn source(&self) -> PulsarMessage<Vec<u8>> {
        let key = self.key.as_ref().map(|key| key.text.clone());
        let ordering_key = self.ordering_key.as_ref().map(|key| key.bytes.clone());
        PulsarMessage::new(
            "persistent://tenant/namespace/Orders",
            MessageData {
                id: MessageIdData {
                    ledger_id: self.ledger,
                    entry_id: self.entry,
                    partition: self.partition.as_ref().map(|index| index.value),
                    batch_index: self.batch_index.as_ref().map(|index| index.value),
                    ..Default::default()
                },
                batch_size: None,
            },
            Payload {
                metadata: MessageMetadata {
                    producer_name: self.producer.text.clone(),
                    sequence_id: u64::try_from(self.sequence)
                        .assured("generated sequence is nonnegative"),
                    publish_time: self.publish_millis,
                    event_time: self.event_millis.as_ref().map(|instant| instant.millis),
                    partition_key: key,
                    ordering_key,
                    properties: vec![
                        KeyValue {
                            key: "Case".into(),
                            value: "first".into(),
                        },
                        KeyValue {
                            key: "case".into(),
                            value: "lower".into(),
                        },
                        KeyValue {
                            key: "Case".into(),
                            value: "last".into(),
                        },
                    ],
                    ..Default::default()
                },
                data: self.payload.bytes.clone(),
            },
            self.redelivery_count,
        )
    }

    fn assert_metadata_exact(&self) {
        let source = self.source();
        let id = source.message_id().clone();
        let mut identity = format!(
            "{}:{}:{}",
            self.ledger,
            self.entry,
            id.partition.unwrap_or(-1)
        );
        if let Some(batch_index) = id.batch_index {
            identity.push_str(&format!(":{batch_index}"));
        }
        let message = PulsarTopicIngestorMessage::try_from(source)
            .assured("the generated current message fits every declared metadata type");
        let key = match &self.key {
            Some(key) => Value::String(key.text.clone()),
            None => Value::Absent,
        };
        let ordering_key = match &self.ordering_key {
            Some(key) => Value::Bytes(key.bytes.clone()),
            None => Value::Absent,
        };
        let event_time = match &self.event_millis {
            Some(instant) => Value::Datetime(Self::instant(instant.millis)),
            None => Value::Absent,
        };
        let redelivery_count = match self.redelivery_count {
            Some(count) => Value::I64(i64::from(count)),
            None => Value::I64(0),
        };
        let row = message.metadata();
        let retained = RetainedIngestMetadata::capture(row.fields);
        assert_eq!(
            retained.values(),
            [
                Value::String("persistent://tenant/namespace/Orders".into()),
                Value::String(identity),
                Value::Datetime(Self::instant(self.publish_millis)),
                event_time,
                key,
                ordering_key,
                Value::String(self.producer.text.clone()),
                Value::I64(self.sequence),
                redelivery_count,
            ]
        );
        assert_eq!(RetainedIngestMetadata::capture(&retained), retained);
        let headers = RetainedIngestHeaders::capture(message.headers());
        let mut visited = Vec::new();
        headers.visit(&mut |name, value| visited.push((name.to_owned(), value.to_owned())));
        assert_eq!(
            visited,
            [
                ("Case".into(), "first".into()),
                ("case".into(), "lower".into()),
                ("Case".into(), "last".into())
            ]
        );
        assert_eq!(message.payload(), self.payload.bytes.as_slice());
        assert_eq!(message.position().message_id, id);
        assert_eq!(
            message.position().topic,
            "persistent://tenant/namespace/Orders"
        );
    }

    fn instant(millis: u64) -> Timestamp {
        let millis = i64::try_from(millis).assured("the generated milliseconds fit I64");
        Timestamp::from_unix_nanos(
            millis
                .checked_mul(1_000_000)
                .assured("the generated milliseconds fit nanoseconds"),
        )
    }
}

#[test]
fn metadata_preserves_absence_empty_values_zero_and_representation_boundaries() {
    for event_millis in [None, Some(0), Some(MAX_MILLIS)] {
        for redelivery_count in [None, Some(0), Some(1), Some(u32::MAX)] {
            for key in [None, Some(""), Some("é中🦀")] {
                for ordering_key in [None, Some(Vec::new()), Some(vec![0, 255])] {
                    GeneratedMessage {
                        ledger: u64::MAX,
                        entry: u64::MAX,
                        partition: Some(GeneratedIndex { value: i32::MAX }),
                        batch_index: Some(GeneratedIndex { value: i32::MAX }),
                        publish_millis: MAX_MILLIS,
                        event_millis: event_millis.map(|millis| GeneratedInstant { millis }),
                        sequence: i64::MAX,
                        redelivery_count,
                        key: key.map(|text| GeneratedText {
                            text: text.to_owned(),
                        }),
                        ordering_key: ordering_key.map(|bytes| GeneratedBytes { bytes }),
                        producer: GeneratedText {
                            text: "é中🦀".to_owned(),
                        },
                        payload: GeneratedPayload {
                            bytes: vec![255; 16],
                        },
                    }
                    .assert_metadata_exact();
                }
            }
        }
    }
}

#[test]
fn present_values_outside_the_declared_range_preserve_their_typed_failure() {
    let generated = GeneratedMessage {
        ledger: 1,
        entry: 2,
        partition: None,
        batch_index: None,
        publish_millis: 1,
        event_millis: None,
        sequence: 0,
        redelivery_count: None,
        key: None,
        ordering_key: None,
        producer: GeneratedText {
            text: String::new(),
        },
        payload: GeneratedPayload { bytes: Vec::new() },
    };
    for millis in [MAX_MILLIS + 1, u64::MAX] {
        for event_time in [false, true] {
            let mut source = generated.source();
            if event_time {
                source.payload.metadata.event_time = Some(millis);
            } else {
                source.payload.metadata.publish_time = millis;
            }
            let Err(error) = PulsarTopicIngestorMessage::try_from(source) else {
                panic!("a present unsupported instant cannot become missing metadata");
            };
            assert!(error.contains::<TimestampError>());
        }
    }
    let mut source = generated.source();
    source.payload.metadata.sequence_id = u64::MAX;
    let Err(error) = PulsarTopicIngestorMessage::try_from(source) else {
        panic!("an unsupported sequence cannot become missing metadata");
    };
    assert!(error.contains::<std::num::TryFromIntError>());
}

#[test]
fn bolero_pulsar_message_metadata_preserves_every_field() {
    bolero::check!()
        .with_type::<GeneratedMessage>()
        .for_each(|message| message.assert_metadata_exact());
}
