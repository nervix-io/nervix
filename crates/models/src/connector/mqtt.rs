//! The MQTT connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The MQTT client Model, the topic source and sink Models with their session,
//!   quality-of-service and delivery modes, the topic filter a source subscribes to with the rules
//!   MQTT holds it to, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The MQTT driver, its configuration keys, or how a topic filter is
//!   subscribed or published to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use super::{
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    PositionClass, RetryPolicy, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SourceBody, SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind, TopicName,
};

configured_client!(Mqtt: unpooled CreateClientMqtt => MqttConfigEntry);

/// The longest topic filter MQTT carries, in bytes.
const MQTT_TOPIC_FILTER_MAX: usize = 65_535;

external_names! {
    /// An MQTT topic filter: 1 to 65,535 bytes without the NUL character, whose levels are joined by
    /// `/` and may be empty. `+` stands for one level and `#` for every remaining level, each only as
    /// a whole level, and `#` only as the last one, as MQTT itself requires.
    pub struct MqttTopicFilter => mqtt_topic_filter_rule;
}

fn mqtt_topic_filter_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, MQTT_TOPIC_FILTER_MAX)?;
    if text.contains('\0') {
        return Err(ExternalNameError::InvalidChar { ch: '\0' });
    }
    let mut levels = text.split('/').peekable();
    while let Some(level) = levels.next() {
        if level.len() > 1
            && let Some(wildcard) = level.chars().find(|ch| matches!(ch, '+' | '#'))
        {
            return Err(ExternalNameError::WildcardInsideLevel { wildcard });
        }
        if level == "#" && levels.peek().is_some() {
            return Err(ExternalNameError::WildcardBeforeLastLevel { wildcard: '#' });
        }
    }
    Ok(())
}

/// One topic filter an MQTT client subscribes to, kept exactly as written: MQTT topics are
/// case-sensitive and routinely hold `/`, `.`, `+` or `#`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MqttTopicSource {
    pub client: ClientName,
    pub topic: MqttTopicFilter,
    pub instances: NonZeroU64,
    pub mode: MqttIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for MqttTopicSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: false,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Mqtt),
        position_class: PositionClass::BrokerRedelivered,
    };
}

impl SourceBody for MqttTopicSource {
    fn reference(&self) -> SourceReference<'_> {
        SourceReference::Client(&self.client)
    }

    fn quiesce(&self) -> &IngestQuiesceMode {
        &self.quiesce
    }

    fn quiesce_mut(&mut self) -> &mut IngestQuiesceMode {
        &mut self.quiesce
    }

    /// The broker holds messages for a suspended subscriber only when its session is persistent
    /// and its messages are delivered at least once.
    fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        match quiesce {
            IngestQuiesceMode::Suspend => {
                self.mode.session() == MqttSession::Persistent
                    && self.mode.qos() == MqttQos::AtLeastOnce
            }
            IngestQuiesceMode::Buffer { .. } | IngestQuiesceMode::Drop => true,
            IngestQuiesceMode::EndpointBuffer { .. } | IngestQuiesceMode::Reject { .. } => false,
        }
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum MqttSession {
    Clean,
    Persistent,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum MqttQos {
    AtMostOnce,
    AtLeastOnce,
}

impl MqttQos {
    pub const fn level(self) -> u8 {
        match self {
            Self::AtMostOnce => 0,
            Self::AtLeastOnce => 1,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MqttIngestMode {
    NoAck {
        session: MqttSession,
        qos: MqttQos,
    },
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
    AckParallel {
        max: NonZeroU64,
        batch_timeout: String,
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

impl MqttIngestMode {
    pub const fn session(&self) -> MqttSession {
        match self {
            Self::NoAck { session, .. } => *session,
            Self::AckSequential { .. } | Self::AckParallel { .. } => MqttSession::Persistent,
        }
    }

    pub const fn qos(&self) -> MqttQos {
        match self {
            Self::NoAck { qos, .. } => *qos,
            Self::AckSequential { .. } | Self::AckParallel { .. } => MqttQos::AtLeastOnce,
        }
    }

    pub const fn is_ack(&self) -> bool {
        match self {
            Self::AckSequential { .. } | Self::AckParallel { .. } => true,
            Self::NoAck { .. } => false,
        }
    }

    pub fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => IngestAcknowledgement::Parallel {
                max: *max,
                batch_timeout,
                timeout,
                retry: retry_policy,
            },
            Self::AckSequential {
                timeout,
                retry_policy,
            } => IngestAcknowledgement::Sequential {
                timeout,
                retry: retry_policy,
            },
            Self::NoAck { .. } => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Mqtt = "MQTT";
    /// What an MQTT source reads.
    pub enum MqttSource;
    /// The alterations of the ingestors that read an MQTT entity.
    pub enum MqttIngestorAlteration;
    /// The entities an MQTT source reads.
    pub enum MqttSourceEntity {
        Topic(MqttTopicSource) = "TOPIC", "MQTT";
    }
}

/// One topic an MQTT client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MqttTopicSink {
    pub client: ClientName,
    pub topic: TopicName,
}

impl SinkEntity for MqttTopicSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        requires_codec: true,
        batch_requirement: EmitterBatchRequirement::Optional,
        requires_batch_transformation: false,
        batch_size_maximum: None,
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(
            mode,
            EmitterPublishingMode::MqttQos0 { .. }
                | EmitterPublishingMode::MqttQos1 { .. }
                | EmitterPublishingMode::MqttQos2 { .. }
        )
    }
}

impl SinkBody for MqttTopicSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Mqtt,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Mqtt = "MQTT";
    /// What an MQTT sink writes.
    pub enum MqttSink;
    /// The entities an MQTT sink writes.
    pub enum MqttSinkEntity {
        Topic(MqttTopicSink) = "TOPIC";
    }
}
