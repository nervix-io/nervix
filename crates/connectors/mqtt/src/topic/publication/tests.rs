//! Layer: test harness, outside product execution.
//! - **Owns.** Complete MQTT publication property, encoding and field-boundary assertions.
//! - **Depends on.** The production property owner, shared record values and the driver's codec.
//! - **Must not know.** Host retries, graph state, connections or runtime scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the MQTT publication-property boundary")
)]

use bytes::BytesMut;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{PropertyValue, RecordProperty};
use rumqttc::{Publish, PublishProperties, QoS, mqttbytes::v5::Packet};

use super::MqttPublishProperties;
use crate::topic::{MqttPublishFraming, MqttPublishingMode, MqttRecordError};

#[derive(Debug, bolero::TypeGenerator)]
enum FieldText {
    Empty,
    Short(#[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=128))] Vec<u8>),
    WireLimit,
}

impl FieldText {
    fn text(&self) -> String {
        match self {
            Self::Empty => String::new(),
            Self::Short(bytes) => {
                let mut text = String::new();
                for byte in bytes {
                    let character = match byte % 4 {
                        0 => 'a',
                        1 => 'é',
                        2 => '水',
                        _ => '/',
                    };
                    text.push(character);
                }
                text
            }
            Self::WireLimit => "x".repeat(usize::from(u16::MAX)),
        }
    }

    fn binary(&self) -> Vec<u8> {
        match self {
            Self::Empty => Vec::new(),
            Self::Short(bytes) => bytes.clone(),
            Self::WireLimit => vec![0xff; usize::from(u16::MAX)],
        }
    }
}

#[derive(Debug, bolero::TypeGenerator)]
struct PropertyCase {
    content_type: Option<FieldText>,
    message_expiry: Option<u32>,
    response_topic: Option<u16>,
    correlation_data: Option<FieldText>,
    #[generator(bolero::generator::produce_with::<Vec<(FieldText, FieldText)>>().len(0_usize..=16))]
    user_properties: Vec<(FieldText, FieldText)>,
    qos: u8,
    retain: bool,
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=256))]
    payload: Vec<u8>,
}

impl PropertyCase {
    fn expected(&self) -> Option<PublishProperties> {
        let mut user_properties = Vec::new();
        // Repeat each key deliberately; ordering and duplicates are part of the contract.
        for (key, value) in &self.user_properties {
            let key = key.text();
            user_properties.push((key.clone(), value.text()));
            user_properties.push((key, "second".to_owned()));
        }
        let properties = PublishProperties {
            content_type: self.content_type.as_ref().map(FieldText::text),
            message_expiry_interval: self.message_expiry,
            response_topic: self
                .response_topic
                .map(|number| format!("responses/{number}")),
            correlation_data: self
                .correlation_data
                .as_ref()
                .map(|value| value.binary().into()),
            user_properties,
            ..Default::default()
        };
        if properties == PublishProperties::default() {
            None
        } else {
            Some(properties)
        }
    }
}

fn record_properties(properties: &PublishProperties) -> Vec<RecordProperty> {
    let mut records = Vec::new();
    if let Some(value) = &properties.content_type {
        records.push(RecordProperty {
            name: "content_type",
            value: PropertyValue::String(value.clone()),
        });
    }
    if let Some(value) = properties.message_expiry_interval {
        records.push(RecordProperty {
            name: "message_expiry",
            value: PropertyValue::U32(value),
        });
    }
    if let Some(value) = &properties.response_topic {
        records.push(RecordProperty {
            name: "response_topic",
            value: PropertyValue::String(value.clone()),
        });
    }
    if let Some(value) = &properties.correlation_data {
        records.push(RecordProperty {
            name: "correlation_data",
            value: PropertyValue::Bytes(value.to_vec()),
        });
    }
    records
}

#[test]
fn bolero_publish_properties_preserve_the_complete_packet() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .with_type::<PropertyCase>()
        .for_each(|case| {
            let expected = case.expected();
            let (records, headers) = match &expected {
                Some(properties) => (
                    record_properties(properties),
                    properties.user_properties.clone(),
                ),
                None => (Vec::new(), Vec::new()),
            };
            let prepared = MqttPublishProperties::new(records, headers)
                .assured("the generator produces current bounded MQTT properties");
            assert_eq!(prepared.properties, expected);
            let (mode, qos) = match case.qos % 3 {
                0 => (MqttPublishingMode::Qos0, QoS::AtMostOnce),
                1 => (
                    MqttPublishingMode::Qos1(super::super::tests::confirmed()),
                    QoS::AtLeastOnce,
                ),
                _ => (
                    MqttPublishingMode::Qos2(super::super::tests::confirmed()),
                    QoS::ExactlyOnce,
                ),
            };
            let topic = super::super::tests::topic("properties/Case");
            let packet_bytes = MqttPublishFraming::new(&topic, mode)
                .packet_bytes(case.payload.len(), prepared.encoded_bytes())
                .assured("the bounded generated packet fits MQTT's Remaining Length");
            let mut expected_packet =
                Publish::new(topic.as_str(), qos, case.payload.clone(), expected);
            expected_packet.retain = case.retain;
            if qos != QoS::AtMostOnce {
                expected_packet.pkid = 1;
            }
            let mut encoded = BytesMut::new();
            expected_packet
                .write(&mut encoded)
                .assured("all generated fields fit the wire");
            assert_eq!(packet_bytes, encoded.len());
            let decoded = Packet::read(&mut encoded, None).assured("the packet is complete");
            assert!(encoded.is_empty());
            assert_eq!(decoded, Packet::Publish(expected_packet));
        });
}

#[test]
fn packet_measure_includes_every_property_length_width() {
    // A user property with the one-byte key "k" carries six framing bytes and its value.
    const FIELD_FRAMING: usize = 6;
    let full_field = usize::from(u16::MAX) + FIELD_FRAMING;
    for property_bytes in [127_usize, 128, 16_383, 16_384, 2_097_151, 2_097_152] {
        let mut remaining = property_bytes;
        let mut headers = Vec::new();
        while remaining > full_field {
            headers.push(("k".to_owned(), "x".repeat(usize::from(u16::MAX))));
            remaining = remaining
                .checked_sub(full_field)
                .assured("the loop leaves a positive remainder");
        }
        let value_bytes = remaining
            .checked_sub(FIELD_FRAMING)
            .assured("each chosen boundary leaves room for its last field's framing");
        headers.push(("k".to_owned(), "x".repeat(value_bytes)));
        let prepared = MqttPublishProperties::new(Vec::new(), headers)
            .assured("every field fits its two-byte wire prefix");
        assert_eq!(prepared.encoded_bytes(), property_bytes);
        let topic = super::super::tests::topic("property-lengths");
        let measured = MqttPublishFraming::new(&topic, MqttPublishingMode::Qos0)
            .packet_bytes(1, prepared.encoded_bytes())
            .assured("the bounded property section fits the packet");
        let expected = Publish::new(
            topic.as_str(),
            QoS::AtMostOnce,
            vec![0xff],
            prepared.properties,
        );
        let mut encoded = BytesMut::new();
        expected
            .write(&mut encoded)
            .assured("the driver writes every valid field");
        assert_eq!(measured, encoded.len());
        assert_eq!(
            Packet::read(&mut encoded, None).assured("the full packet decodes"),
            Packet::Publish(expected)
        );
    }
}

#[test]
fn string_and_binary_field_bounds_are_checked_before_encoding() {
    let bytes = usize::from(u16::MAX) + 1;
    for (property, field) in [
        (
            RecordProperty {
                name: "content_type",
                value: PropertyValue::String("é".repeat(bytes / 2)),
            },
            "content_type",
        ),
        (
            RecordProperty {
                name: "response_topic",
                value: PropertyValue::String("x".repeat(bytes)),
            },
            "response_topic",
        ),
        (
            RecordProperty {
                name: "correlation_data",
                value: PropertyValue::Bytes(vec![0xff; bytes]),
            },
            "correlation_data",
        ),
    ] {
        let Err(error) = MqttPublishProperties::new(vec![property], Vec::new()) else {
            panic!("a field longer than a two-byte length prefix must be rejected");
        };
        assert_eq!(
            error.current_context(),
            &MqttRecordError::PropertyLength { field, bytes }
        );
        assert!(error.contains::<std::num::TryFromIntError>());
    }
    for (headers, field) in [
        (
            vec![("x".repeat(65_536), "value".to_owned())],
            "user property name",
        ),
        (
            vec![("key".to_owned(), "x".repeat(65_536))],
            "user property value",
        ),
    ] {
        let Err(error) = MqttPublishProperties::new(Vec::new(), headers) else {
            panic!("a user property field must fit its length prefix");
        };
        assert_eq!(
            error.current_context(),
            &MqttRecordError::PropertyLength {
                field,
                bytes: 65_536
            }
        );
    }
}

#[test]
fn invalid_text_and_response_topics_are_safe_per_record_errors() {
    for (name, field) in [
        ("content_type", "content_type"),
        ("response_topic", "response_topic"),
    ] {
        let Err(error) = MqttPublishProperties::new(
            vec![RecordProperty {
                name,
                value: PropertyValue::String("confidential\0text".to_owned()),
            }],
            Vec::new(),
        ) else {
            panic!("MQTT strings contain no NUL");
        };
        assert_eq!(
            error.current_context(),
            &MqttRecordError::PropertyNul { field }
        );
        assert!(!format!("{error:?}").contains("confidential"));
    }
    for value in ["", "responses/+", "responses/#"] {
        let Err(error) = MqttPublishProperties::new(
            vec![RecordProperty {
                name: "response_topic",
                value: PropertyValue::String(value.to_owned()),
            }],
            Vec::new(),
        ) else {
            panic!("a response topic names one nonempty literal topic");
        };
        assert_eq!(error.current_context(), &MqttRecordError::ResponseTopic);
        assert!(error.contains::<nervix_models::ExternalNameError>());
    }
    for (headers, field) in [
        (
            vec![("confidential\0key".to_owned(), "value".to_owned())],
            "user property name",
        ),
        (
            vec![("key".to_owned(), "confidential\0value".to_owned())],
            "user property value",
        ),
    ] {
        let Err(error) = MqttPublishProperties::new(Vec::new(), headers) else {
            panic!("MQTT user property strings contain no NUL");
        };
        assert_eq!(
            error.current_context(),
            &MqttRecordError::PropertyNul { field }
        );
        assert!(!format!("{error:?}").contains("confidential"));
    }
}

#[test]
fn a_property_outside_the_declared_typed_contract_is_rejected() {
    for property in [
        RecordProperty {
            name: "message_expiry",
            value: PropertyValue::I64(600),
        },
        RecordProperty {
            name: "missing",
            value: PropertyValue::String("confidential".to_owned()),
        },
    ] {
        let name = property.name;
        let actual = property.value.ty();
        let Err(error) = MqttPublishProperties::new(vec![property], Vec::new()) else {
            panic!("the connector accepts only its declared property types");
        };
        assert_eq!(
            error.current_context(),
            &MqttRecordError::PropertyType { name, actual }
        );
        assert!(!format!("{error:?}").contains("confidential"));
    }
}
