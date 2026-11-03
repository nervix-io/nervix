//! The row and headers of delivered publishes, read through the driver's own packet decoder.
//!
//! Layer: test harness.
//! - **Owns.** The cases of one delivered publish's metadata row and headers, and the bounded
//!   property over them.
//! - **Depends on.** The vocabulary's declaration and the driver's packet encoder and decoder.
//! - **Must not know.** Connections, sessions or the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests the production MQTT message metadata boundary"
    )
)]

use std::num::NonZeroU32;

use bytes::{Bytes, BytesMut};
use meticulous::ResultExt as _;
use nervix_connector::{
    IngestMessageHeaders as _, IngestorMessage as _, RetainedIngestHeaders, RetainedIngestMetadata,
    RetainedIngestMetadataValue as Value,
};
use nervix_models::{ClientConfigEntry, IngestorEntityKind, MqttClientConfig, MqttIngestorEntity};
use rumqttc::{AckMode, Packet, Publish, PublishProperties, QoS};

use super::super::{MqttTopicIngestorMessage, driver::IngestorPacket};

/// The bound the driver admits a complete publish within when its client declares no
/// `max_packet_size`, which every generated publish fits.
fn default_admission() -> NonZeroU32 {
    let client = [ClientConfigEntry {
        key: "addr".to_string(),
        value: "mqtt://127.0.0.1:1883".to_string(),
    }];
    MqttClientConfig::parse(&client)
        .assured("an address alone is a valid MQTT client configuration")
        .max_packet_size()
}

/// The publish `published` as an ingestor receives it: through the client's decoder and the
/// driver's admission, which copies the packet's bytes out of its frame.
fn delivered(published: Publish) -> MqttTopicIngestorMessage {
    let mut wire = BytesMut::new();
    Packet::Publish(published)
        .write(&mut wire, None)
        .assured("a generated publish is a legal packet");
    let packet = Packet::read(&mut wire, None).assured("the decoder reads what the encoder wrote");
    let Packet::Publish(publish) = packet else {
        panic!("the decoder read a publish as {packet:?}");
    };
    let IngestorPacket::Message(admitted) =
        IngestorPacket::classify(publish, AckMode::Manual, default_admission())
    else {
        panic!("the driver admits a publish within its packet bound");
    };
    MqttTopicIngestorMessage::new(admitted)
}

/// What a message hands the host for its row, in the declared order.
fn row(message: &MqttTopicIngestorMessage) -> Vec<Value> {
    RetainedIngestMetadata::capture(message.metadata().fields)
        .values()
        .to_vec()
}

/// What a message hands the host for its headers, in the order it visits them.
fn headers(message: &MqttTopicIngestorMessage) -> Vec<(String, String)> {
    let retained = RetainedIngestHeaders::capture(message.headers());
    let mut pairs = Vec::new();
    retained.visit(&mut |name, value| pairs.push((name.to_string(), value.to_string())));
    pairs
}

/// Every value holds its field's declared type, field by field in declared order, or is absent.
fn assert_fits_the_declaration(row: &[Value]) {
    let declared = IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic)
        .capabilities()
        .metadata;
    assert_eq!(row.len(), declared.fields().len());
    for (value, field) in row.iter().zip(declared.fields()) {
        if let Some(ty) = value.ty() {
            assert_eq!(ty, field.ty(), "metadata.{}", field.name());
        }
    }
}

fn expected_row(publish: &Publish, topic: &str) -> Vec<Value> {
    let properties = publish.properties.clone().unwrap_or_default();
    let content_type = match properties.content_type {
        Some(content_type) => Value::String(content_type),
        None => Value::Absent,
    };
    let response_topic = match properties.response_topic {
        Some(response_topic) => Value::String(response_topic),
        None => Value::Absent,
    };
    let correlation_data = match properties.correlation_data {
        Some(correlation_data) => Value::Bytes(correlation_data.to_vec()),
        None => Value::Absent,
    };
    let message_expiry = match properties.message_expiry_interval {
        Some(message_expiry) => Value::U32(message_expiry),
        None => Value::Absent,
    };
    let qos = match publish.qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    };
    vec![
        Value::String(topic.to_string()),
        Value::Bool(publish.retain),
        Value::U8(qos),
        Value::Bool(publish.dup),
        content_type,
        response_topic,
        correlation_data,
        message_expiry,
    ]
}

fn publish(topic: &str, qos: QoS, properties: Option<PublishProperties>) -> Publish {
    let mut publish = Publish::new(topic, qos, b"{}".to_vec(), properties);
    if qos != QoS::AtMostOnce {
        publish.pkid = 7;
    }
    publish
}

#[test]
fn a_delivered_publish_hands_over_every_declared_field_of_its_declared_type() {
    let mut published = publish(
        "sensors/kitchen/temperature",
        QoS::ExactlyOnce,
        Some(PublishProperties {
            content_type: Some("application/json".to_string()),
            response_topic: Some("replies/kitchen".to_string()),
            correlation_data: Some(Bytes::from_static(&[0, 159, 146, 150])),
            message_expiry_interval: Some(u32::MAX),
            ..PublishProperties::default()
        }),
    );
    published.retain = true;
    published.dup = true;

    let message = delivered(published);
    let row = row(&message);

    assert_eq!(
        row,
        [
            Value::String("sensors/kitchen/temperature".to_string()),
            Value::Bool(true),
            Value::U8(2),
            Value::Bool(true),
            Value::String("application/json".to_string()),
            Value::String("replies/kitchen".to_string()),
            Value::Bytes(vec![0, 159, 146, 150]),
            Value::U32(u32::MAX),
        ]
    );
    assert_fits_the_declaration(&row);
}

/// The guarantee `metadata.topic` borrows its text under: a topic that is not UTF-8 ends at the
/// client's decoder, before any message exists.
#[test]
fn a_publish_whose_topic_is_not_utf8_never_reaches_a_message() {
    // PUBLISH at QoS 0: a two-byte topic of two invalid UTF-8 bytes and an empty property section.
    let mut wire = BytesMut::from(&[0x30, 0x05, 0x00, 0x02, 0xff, 0xfe, 0x00][..]);

    assert!(Packet::read(&mut wire, None).is_err());
}

#[test]
fn a_publish_without_properties_reads_each_of_them_as_absent_and_carries_no_header() {
    let message = delivered(publish("sensors/hall", QoS::AtMostOnce, None));

    assert_eq!(
        row(&message),
        [
            Value::String("sensors/hall".to_string()),
            Value::Bool(false),
            Value::U8(0),
            Value::Bool(false),
            Value::Absent,
            Value::Absent,
            Value::Absent,
            Value::Absent,
        ]
    );
    assert!(headers(&message).is_empty());
}

#[test]
fn a_property_set_empty_is_a_value_and_one_left_unset_is_not() {
    let message = delivered(publish(
        "sensors/hall",
        QoS::AtLeastOnce,
        Some(PublishProperties {
            content_type: Some(String::new()),
            correlation_data: Some(Bytes::new()),
            message_expiry_interval: Some(0),
            ..PublishProperties::default()
        }),
    ));

    assert_eq!(
        row(&message)[4..],
        [
            Value::String(String::new()),
            Value::Absent,
            Value::Bytes(Vec::new()),
            Value::U32(0),
        ]
    );
}

#[test]
fn user_properties_keep_their_written_order_and_repeats_as_headers() {
    let user_properties = vec![
        ("route".to_string(), "second".to_string()),
        ("tenant".to_string(), "acme".to_string()),
        ("route".to_string(), "first".to_string()),
        ("Route".to_string(), String::new()),
    ];
    let message = delivered(publish(
        "sensors/hall",
        QoS::AtLeastOnce,
        Some(PublishProperties {
            user_properties: user_properties.clone(),
            ..PublishProperties::default()
        }),
    ));

    assert_eq!(headers(&message), user_properties);
}

/// One publish a broker can deliver, drawn across every flag, each property's presence and the
/// values its type allows: a text property or user property may be empty or multibyte, a repeated
/// user property name is common, and the expiry reaches both ends of its range.
#[derive(Debug, bolero::TypeGenerator)]
struct PublishCase {
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(1_usize..=32))]
    topic: Vec<u8>,
    qos: u8,
    retain: bool,
    duplicate: bool,
    carries_properties: bool,
    content_type: Option<String>,
    response_topic: Option<String>,
    correlation_data: Option<Vec<u8>>,
    message_expiry: Option<u32>,
    user_properties: Vec<(u8, String)>,
}

/// The characters of a generated topic, none of them a wildcard, which a publish may not name.
const TOPIC_CHARACTERS: &[u8; 8] = b"abc/XYZ-";
/// Few enough names that a generated publish often repeats one.
const USER_PROPERTY_NAMES: [&str; 3] = ["route", "Route", "tenant"];

impl PublishCase {
    fn topic(&self) -> String {
        let mut topic = String::with_capacity(self.topic.len());
        for byte in &self.topic {
            let index = usize::from(*byte) % TOPIC_CHARACTERS.len();
            topic.push(char::from(TOPIC_CHARACTERS[index]));
        }
        topic
    }

    fn qos(&self) -> QoS {
        match self.qos % 3 {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        }
    }

    /// The text a property may hold: the driver refuses a NUL in an MQTT string and nothing else
    /// that is valid UTF-8.
    fn text(value: &str) -> String {
        value.replace('\0', "")
    }

    fn properties(&self) -> Option<PublishProperties> {
        if !self.carries_properties {
            return None;
        }
        let mut user_properties = Vec::with_capacity(self.user_properties.len());
        for (name, value) in &self.user_properties {
            let name = USER_PROPERTY_NAMES[usize::from(*name) % USER_PROPERTY_NAMES.len()];
            user_properties.push((name.to_string(), Self::text(value)));
        }
        Some(PublishProperties {
            content_type: self.content_type.as_deref().map(Self::text),
            response_topic: self.response_topic.as_deref().map(Self::text),
            correlation_data: self.correlation_data.clone().map(Bytes::from),
            message_expiry_interval: self.message_expiry,
            user_properties,
            ..PublishProperties::default()
        })
    }

    fn publish(&self) -> Publish {
        let mut publish = publish(&self.topic(), self.qos(), self.properties());
        publish.retain = self.retain;
        // A QoS 0 publish cannot be a duplicate: the decoder refuses that flag on it.
        publish.dup = self.duplicate && self.qos() != QoS::AtMostOnce;
        publish
    }
}

/// Whatever a broker delivers, a publish hands over exactly one value of its declared type for
/// each declared field, in declared order, absent exactly where its packet carries none, and its
/// user properties in their written order with their repeats; the row and the headers survive the
/// copy a quiesce buffer retains.
#[test]
fn bolero_delivered_publishes_hand_over_their_declared_row_and_user_properties() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<PublishCase>()
        .for_each(|case| {
            let published = case.publish();
            let topic = case.topic();
            let expected = expected_row(&published, &topic);
            let user_properties = match published.properties.as_ref() {
                Some(properties) => properties.user_properties.clone(),
                None => Vec::new(),
            };

            let message = delivered(published);

            let row = row(&message);
            assert_eq!(row, expected);
            assert_fits_the_declaration(&row);
            assert_eq!(headers(&message), user_properties);
            let retained = RetainedIngestMetadata::capture(message.metadata().fields);
            assert_eq!(RetainedIngestMetadata::capture(&retained), retained);
        });
}
