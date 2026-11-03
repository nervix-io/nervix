//! Public MQTT publish proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** Publishes a scenario sends with the flags and MQTT 5 properties a data table names.
//! - **Depends on.** The scenario's MQTT endpoint and the shared fixture publisher.
//! - **Must not know.** The MQTT ingestor's subscription, its metadata row or any product state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use cucumber::gherkin::Table;
use rumqttc::{PublishOptions, PublishProperties, QoS};

use super::*;

/// One publish a scenario sends: its payload, and the flags and properties its table sets.
struct PublishedMessage {
    payload: String,
    options: PublishOptions,
}

impl PublishedMessage {
    /// Reads a two-column table of names and values. `payload`, `qos` and `retain` name the
    /// publish, `user_property` adds a `name=value` user property in table order, and any other
    /// row names an MQTT 5 property. A property the table leaves out stays unset, as a publisher
    /// leaves it, and a table that sets none publishes a packet without properties.
    fn from_table(world: &ScenarioWorld, table: &Table) -> Self {
        let mut payload = None;
        let mut qos = QoS::AtMostOnce;
        let mut retain = false;
        let mut properties = PublishProperties::default();
        for row in &table.rows {
            let [name, value] = row.as_slice() else {
                panic!("each row names one publish setting and its value: {row:?}");
            };
            let value = expand_placeholders(world, value);
            match name.as_str() {
                "payload" => payload = Some(value),
                "qos" => qos = Self::qos(&value),
                "retain" => {
                    retain = value
                        .parse()
                        .unwrap_or_else(|error| panic!("retain '{value}' is no bool: {error}"));
                }
                _ => Self::set_property(&mut properties, name, value),
            }
        }
        let payload = payload.unwrap_or_else(|| panic!("the table names no payload row"));
        let options = PublishOptions::new(qos).retain(retain);
        let options = if properties == PublishProperties::default() {
            options
        } else {
            options.properties(properties)
        };
        Self { payload, options }
    }

    fn qos(value: &str) -> QoS {
        match value {
            "0" => QoS::AtMostOnce,
            "1" => QoS::AtLeastOnce,
            "2" => QoS::ExactlyOnce,
            other => panic!("unsupported MQTT QoS '{other}'"),
        }
    }

    fn set_property(properties: &mut PublishProperties, name: &str, value: String) {
        match name {
            "content_type" => properties.content_type = Some(value),
            "response_topic" => properties.response_topic = Some(value),
            "correlation_data" => properties.correlation_data = Some(value.into_bytes().into()),
            "message_expiry" => {
                let seconds = value
                    .parse()
                    .unwrap_or_else(|error| panic!("expiry '{value}' is no u32: {error}"));
                properties.message_expiry_interval = Some(seconds);
            }
            "user_property" => {
                let (key, value) = value
                    .split_once('=')
                    .unwrap_or_else(|| panic!("user property '{value}' is not name=value"));
                properties
                    .user_properties
                    .push((key.to_string(), value.to_string()));
            }
            other => panic!("unsupported MQTT publish setting '{other}'"),
        }
    }
}

#[when(expr = "an MQTT message is published to topic {string} with these MQTT 5 properties")]
async fn when_mqtt_message_is_published_with_properties(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let table = step
        .table
        .as_ref()
        .expect("the step names the publish in a table");
    let topic = expand_placeholders(world, &topic);
    let message = PublishedMessage::from_table(world, table);
    wait_for_mqtt_ingestors_ready(world).await;
    world.last_publish_at = Some(Instant::now());
    world
        .cluster()
        .publish_mqtt_with_options(&topic, &message.payload, message.options)
        .await
        .expect("failed to publish the MQTT message");
}
