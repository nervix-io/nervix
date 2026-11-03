//! Complete AMQP property and wire oracles over the publisher's production conversion.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current property inputs and complete conversion and wire assertions.
//! - **Depends on.** The publisher conversion, connector records and the AMQP wire codec.
//! - **Must not know.** Graph execution, host retries or broker connection state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns AMQP property evidence")
)]

use lapin::{
    BasicProperties,
    protocol::basic::{gen_properties, parse_properties},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    EmitterRecord, EmitterRecordId, PropertyValue, RecordEnvelope, RecordProperty,
};
use nervix_models::Timestamp;

use super::{RabbitMqMessage, properties::RabbitMqPropertyError};

const NAMES: [&str; 10] = [
    "content_type",
    "content_encoding",
    "message_id",
    "correlation_id",
    "reply_to",
    "expiration",
    "priority",
    "type",
    "app_id",
    "user_id",
];

fn record(envelope: RecordEnvelope) -> EmitterRecord {
    EmitterRecord::new(
        EmitterRecordId::new(0),
        None,
        vec![1, 2, 3],
        vec![("trace".to_owned(), "value".to_owned())],
        Timestamp::from_unix_nanos(0),
    )
    .with_envelope(envelope)
}

fn read_property(properties: &BasicProperties, name: &str) -> Option<PropertyValue> {
    if name == "priority" {
        return properties.priority().map(PropertyValue::U8);
    }
    let value = match name {
        "content_type" => properties.content_type(),
        "content_encoding" => properties.content_encoding(),
        "message_id" => properties.message_id(),
        "correlation_id" => properties.correlation_id(),
        "reply_to" => properties.reply_to(),
        "expiration" => properties.expiration(),
        "type" => properties.kind(),
        "app_id" => properties.app_id(),
        "user_id" => properties.user_id(),
        _ => unreachable!("the oracle enumerates every declared RabbitMQ property"),
    };
    value
        .as_ref()
        .map(|value| PropertyValue::String(value.to_string()))
}

#[test]
fn bolero_amqp_properties_preserve_every_declared_value_through_the_wire() {
    bolero::check!()
        .with_type::<[u8; 64]>()
        .with_iterations(256)
        .for_each(|data| {
            let mut envelope = RecordEnvelope {
                persistent: data[10] & 1 != 0,
                ..Default::default()
            };
            if data[11] & 1 != 0 {
                let seconds = u64::from_le_bytes(
                    data[44..52].try_into().assured("the slice has eight bytes"),
                ) % (u64::try_from(i64::MAX / 1_000_000_000)
                    .assured("the timestamp bound is nonnegative")
                    + 1);
                let nanos = i64::try_from(seconds)
                    .assured("the bounded seconds fit i64")
                    .checked_mul(1_000_000_000)
                    .assured("the bounded seconds fit signed nanoseconds");
                envelope.timestamp = Some(Timestamp::from_unix_nanos(nanos));
            }
            for (index, name) in NAMES.iter().copied().enumerate() {
                if data[index] & 1 == 0 {
                    continue;
                }
                let value = if name == "priority" {
                    PropertyValue::U8(data[16])
                } else if name == "expiration" {
                    PropertyValue::String(u16::from_le_bytes([data[17], data[18]]).to_string())
                } else {
                    let length = usize::from(data[20 + index]);
                    let text = if data[32 + index] & 1 == 0 {
                        "x".repeat(length)
                    } else {
                        "é".repeat(length / 2)
                    };
                    PropertyValue::String(text)
                };
                envelope.properties.push(RecordProperty { name, value });
            }
            let record = record(envelope);
            let properties = RabbitMqMessage::properties(&record)
                .assured("every generated property is representable");
            let serialized = gen_properties(&properties)(Vec::new().into())
                .assured("the AMQP serializer writes the complete properties");
            let bytes = serialized.write;
            let (remaining, decoded) = parse_properties(bytes.as_slice())
                .assured("the AMQP parser reads the emitted properties");
            assert!(remaining.is_empty());
            assert_eq!(
                decoded, properties,
                "every AMQP field and presence bit survives"
            );
            assert_eq!(
                decoded.delivery_mode(),
                &record.envelope.persistent.then_some(2)
            );
            let restored_time = decoded.timestamp().map(|seconds| {
                let nanos = i64::try_from(seconds)
                    .assured("the generated timestamp fits i64")
                    .checked_mul(1_000_000_000)
                    .assured("the generated timestamp fits signed nanoseconds");
                Timestamp::from_unix_nanos(nanos)
            });
            assert_eq!(restored_time, record.envelope.timestamp);
            for name in NAMES {
                // The generated set has at most the ten declared keys; this independent oracle
                // scans only that fixed bound.
                let expected = record
                    .envelope
                    .properties
                    .iter()
                    .find(|property| property.name == name);
                let expected = expected.map(|property| property.value.clone());
                assert_eq!(read_property(&decoded, name), expected, "{name}");
            }
            let expected_headers =
                lapin::types::FieldTable::from(std::collections::BTreeMap::from([(
                    "trace".into(),
                    lapin::types::AMQPValue::LongString("value".into()),
                )]));
            assert_eq!(decoded.headers(), &Some(expected_headers));
            assert!(decoded.cluster_id().is_none());
        });
}

#[test]
fn timestamp_zero_is_present_and_unrepresentable_instants_refuse_the_record() {
    for nanos in [0, i64::MAX - i64::MAX % 1_000_000_000] {
        let record = record(RecordEnvelope {
            timestamp: Some(Timestamp::from_unix_nanos(nanos)),
            ..Default::default()
        });
        let seconds =
            u64::try_from(nanos / 1_000_000_000).assured("the valid instant is nonnegative");
        assert_eq!(
            RabbitMqMessage::properties(&record)
                .assured("the whole-second boundary is representable")
                .timestamp(),
            &Some(seconds)
        );
    }
    for nanos in [-1, 1, i64::MIN, i64::MAX] {
        let record = record(RecordEnvelope {
            timestamp: Some(Timestamp::from_unix_nanos(nanos)),
            ..Default::default()
        });
        let error = RabbitMqMessage::properties(&record)
            .expect_err("an unrepresentable instant is refused");
        assert!(matches!(
            error.current_context(),
            RabbitMqPropertyError::TimestampUnrepresentable
        ));
    }
}

#[test]
fn short_string_limits_count_utf8_bytes_and_preserve_the_driver_cause() {
    for text in ["x".repeat(255), format!("{}x", "é".repeat(127))] {
        let record = record(RecordEnvelope {
            properties: vec![RecordProperty {
                name: "content_type",
                value: PropertyValue::String(text.clone()),
            }],
            ..Default::default()
        });
        let properties =
            RabbitMqMessage::properties(&record).assured("255 UTF-8 bytes fit a short string");
        assert_eq!(
            properties
                .content_type()
                .as_ref()
                .map(|value| value.as_str()),
            Some(text.as_str())
        );
    }
    for text in ["x".repeat(256), "é".repeat(128)] {
        let record = record(RecordEnvelope {
            properties: vec![RecordProperty {
                name: "content_type",
                value: PropertyValue::String(text),
            }],
            ..Default::default()
        });
        let error = RabbitMqMessage::properties(&record).expect_err("256 UTF-8 bytes do not fit");
        assert!(matches!(
            error.current_context(),
            RabbitMqPropertyError::StringTooLong {
                name: "content_type",
                bytes: 256
            }
        ));
        assert!(error.contains::<lapin::types::ShortStringError>());
    }
}

#[test]
fn properties_and_last_header_writes_keep_distinct_fields() {
    let mut record = record(RecordEnvelope {
        properties: vec![RecordProperty {
            name: "content_type",
            value: PropertyValue::String("application/json".to_owned()),
        }],
        ..Default::default()
    });
    record.headers = vec![
        ("content_type".to_owned(), "first".to_owned()),
        ("content_type".to_owned(), "second".to_owned()),
    ];
    let properties = RabbitMqMessage::properties(&record).assured("the fields are representable");
    assert_eq!(
        properties
            .content_type()
            .as_ref()
            .map(|value| value.as_str()),
        Some("application/json")
    );
    let expected = lapin::types::FieldTable::from(std::collections::BTreeMap::from([(
        "content_type".into(),
        lapin::types::AMQPValue::LongString("second".into()),
    )]));
    assert_eq!(properties.headers(), &Some(expected));
}

#[test]
fn a_mistyped_or_unmapped_property_is_a_typed_record_refusal() {
    for name in NAMES {
        let record = record(RecordEnvelope {
            properties: vec![RecordProperty {
                name,
                value: PropertyValue::Bool(false),
            }],
            ..Default::default()
        });
        let error = RabbitMqMessage::properties(&record)
            .expect_err("every declared property requires its exact type");
        assert!(
            matches!(error.current_context(), RabbitMqPropertyError::InvalidType { name: actual, .. } if actual == &name)
        );
    }
    let record = record(RecordEnvelope {
        properties: vec![RecordProperty {
            name: "unrecognized",
            value: PropertyValue::String("value".to_owned()),
        }],
        ..Default::default()
    });
    let error =
        RabbitMqMessage::properties(&record).expect_err("a key without an AMQP mapping is refused");
    assert!(matches!(
        error.current_context(),
        RabbitMqPropertyError::UnmappedProperty {
            name: "unrecognized"
        }
    ));
}
