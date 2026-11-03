//! What one delivered publish hands the ingestor host.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The values of the metadata `MQTT TOPIC` declares for a delivered publish, read from
//!   its packet in the declared order, and the headers its MQTT 5 user properties become.
//! - **Depends on.** The connector contract's metadata and header traits, the vocabulary's quality
//!   of service and the driver's publish packet.
//! - **Must not know.** Connections, sessions, acknowledgement, or the columns the host builds.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use meticulous::ResultExt as _;
use nervix_connector::{IngestMessageHeaders, IngestMessageMetadata, IngestMetadataValues};
use nervix_models::MqttQos;
use rumqttc::{Publish, QoS};

use super::MqttTopicIngestorMessage;

impl MqttTopicIngestorMessage {
    /// The delivered packet.
    fn packet(&self) -> &Publish {
        self.position.publish()
    }

    /// The topic the broker delivered the publish on, which a wildcard filter does not name.
    fn topic(&self) -> &str {
        std::str::from_utf8(&self.packet().topic)
            .assured("the client's packet decoder refuses a publish whose topic is not UTF-8")
    }

    /// The quality of service of the delivered packet: the lower of what its publisher sent and
    /// what the subscription was granted.
    fn delivered_qos(&self) -> MqttQos {
        match self.packet().qos {
            QoS::AtMostOnce => MqttQos::Qos0,
            QoS::AtLeastOnce => MqttQos::Qos1,
            QoS::ExactlyOnce => MqttQos::Qos2,
        }
    }
}

/// The values of the metadata `MQTT TOPIC` declares, in its order: the topic, the retain, quality
/// of service and duplicate flags, then the MQTT 5 properties, each absent where the packet carries
/// none.
impl IngestMessageMetadata for MqttTopicIngestorMessage {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        let packet = self.packet();
        values.string(self.topic());
        values.bool(packet.retain);
        values.u8(self.delivered_qos().level());
        values.bool(packet.dup);
        let Some(properties) = packet.properties.as_ref() else {
            values.absent();
            values.absent();
            values.absent();
            values.absent();
            return;
        };
        match &properties.content_type {
            Some(content_type) => values.string(content_type),
            None => values.absent(),
        }
        match &properties.response_topic {
            Some(response_topic) => values.string(response_topic),
            None => values.absent(),
        }
        match &properties.correlation_data {
            Some(correlation_data) => values.bytes(correlation_data),
            None => values.absent(),
        }
        match properties.message_expiry_interval {
            Some(message_expiry) => values.u32(message_expiry),
            None => values.absent(),
        }
    }
}

/// A publish's MQTT 5 user properties, in the order its packet carries them, each name's repeats in
/// their places.
impl IngestMessageHeaders for MqttTopicIngestorMessage {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        let Some(properties) = self.packet().properties.as_ref() else {
            return;
        };
        for (name, value) in &properties.user_properties {
            visit(name, value);
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "delivered_publish_tests.rs"]
mod tests;
