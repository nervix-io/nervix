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
    AcknowledgementConfirmation, ChoiceRequirement, ClientName, ConnectorKind, DeclaredDelivery,
    DeclaredQuiesce, EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm,
    EmitterPublishingModeKind, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration, OptionForm, OptionPresence,
    PositionClass, RetryPolicy, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SourceBody, SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind, TopicName,
};

configured_client!(Mqtt: unpooled, config Nervix, CreateClientMqtt => MqttConfigEntry);

/// The longest topic filter MQTT carries, in bytes.
const MQTT_TOPIC_FILTER_MAX: usize = 65_535;

external_names! {
    /// An MQTT topic filter: 1 to 65,535 bytes without the NUL character, whose levels are joined by
    /// `/` and may be empty. `+` stands for one level and `#` for every remaining level, each only as
    /// a whole level, and `#` only as the last one, as MQTT itself requires.
    pub struct MqttTopicFilter => mqtt_topic_filter_rule, label "mqtt_topic_filter",
        example "devices/+/orders";
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

/// The keywords of the option that chooses a subscriber's session.
const SESSION_OPTION: &str = "SESSION";
/// The keywords of the option that chooses the quality of service a subscriber receives at.
const QOS_OPTION: &str = "QOS";

/// The session a broker keeps a subscriber's messages for while the subscriber is away: a
/// persistent one, whose messages are delivered at least once. Suspending a subscriber and
/// acknowledging its messages both rely on it.
const BROKER_KEPT_SESSION: &[ChoiceRequirement] = &[
    ChoiceRequirement {
        option: SESSION_OPTION,
        choice: MqttSession::Persistent.keyword(),
    },
    ChoiceRequirement {
        option: QOS_OPTION,
        choice: MqttQos::AtLeastOnce.spelling(),
    },
];

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
        acknowledgement_confirmation: AcknowledgementConfirmation::Queued,
        delivery: &[
            DeclaredDelivery::NO_ACK,
            DeclaredDelivery::ACK_SEQUENTIAL.requiring(BROKER_KEPT_SESSION),
            DeclaredDelivery::ACK_PARALLEL.requiring(BROKER_KEPT_SESSION),
        ],
        quiesce: &[
            DeclaredQuiesce::SUSPEND.requiring(BROKER_KEPT_SESSION),
            DeclaredQuiesce::BUFFER,
            DeclaredQuiesce::DROP,
        ],
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(MqttTopicFilter::DECLARATION),
        }),
        options: &[
            OptionDeclaration::INSTANCES,
            OptionDeclaration {
                keywords: SESSION_OPTION,
                presence: OptionPresence::Optional {
                    default: Some(MqttSession::Clean.keyword()),
                },
                form: OptionForm::Choice(&[
                    OptionChoice {
                        name: MqttSession::Clean.keyword(),
                        value: None,
                        example: "SESSION CLEAN",
                    },
                    OptionChoice {
                        name: MqttSession::Persistent.keyword(),
                        value: None,
                        example: "SESSION PERSISTENT",
                    },
                ]),
            },
            OptionDeclaration {
                keywords: QOS_OPTION,
                presence: OptionPresence::Optional {
                    default: Some(MqttQos::AtMostOnce.spelling()),
                },
                form: OptionForm::Choice(&[
                    OptionChoice {
                        name: MqttQos::AtMostOnce.spelling(),
                        value: None,
                        example: "QOS 0",
                    },
                    OptionChoice {
                        name: MqttQos::AtLeastOnce.spelling(),
                        value: None,
                        example: "QOS 1",
                    },
                ]),
            },
        ],
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

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }

    /// The session and the quality of service this source subscribes with, which suspending it
    /// and acknowledging its messages require.
    fn chosen(&self, option: &str) -> Option<&'static str> {
        match option {
            SESSION_OPTION => Some(self.mode.session().keyword()),
            QOS_OPTION => Some(self.mode.qos().spelling()),
            _ => None,
        }
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

impl MqttSession {
    /// The keyword a statement chooses this session with after `SESSION`.
    pub const fn keyword(self) -> &'static str {
        match self {
            Self::Clean => "CLEAN",
            Self::Persistent => "PERSISTENT",
        }
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

    /// The literal a statement chooses this quality of service with after `QOS`.
    pub const fn spelling(self) -> &'static str {
        match self {
            Self::AtMostOnce => "0",
            Self::AtLeastOnce => "1",
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
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: &[
            EmitterPublishingModeKind::MqttQos0,
            EmitterPublishingModeKind::MqttQos1,
            EmitterPublishingModeKind::MqttQos2,
        ],
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::Unquoted {
                label: "topic_name",
            },
        }),
        options: &[],
        envelope: &[],
        properties: &[],
    };
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
