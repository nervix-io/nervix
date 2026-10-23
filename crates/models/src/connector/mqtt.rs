//! The MQTT connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The MQTT client Model, the topic source and emitter Models with their session,
//!   quality-of-service and delivery modes, the topic filter a source subscribes to with the rules
//!   MQTT holds it to, and what each entity declares.
//! - **Depends on.** The shared source and emitter declarations and the names a body references.
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
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeclaredBounds, DeclaredBuffer, DeclaredWait,
        WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_emitters, connector_ingestors},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ChoiceRequirement, ClientName, ConnectorKind, DeclaredDelivery,
    DeclaredQuiesce, EmitterBatchLegality, EmitterBodyForm, EmitterClientReference,
    EmitterDeclaration, EmitterEntityBody, EmitterEntityCapabilities, EmitterReference,
    EmitterReferenceKind, IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration,
    IngestorEntityBody, IngestorEntityCapabilities, IngestorMetadataScope, IngestorReference,
    IngestorReferenceKind, LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration,
    OptionForm, OptionPresence, PositionClass, RetryPolicy,
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
pub struct MqttTopicIngestor {
    pub client: ClientName,
    pub topic: MqttTopicFilter,
    pub instances: NonZeroU64,
    pub mode: MqttIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// The largest packet an MQTT TOPIC ingestor's client receives, in bytes, which every `CONNECT` it
/// sends advertises as its Maximum Packet Size, so MQTT 5 obliges the broker to discard a larger
/// publish for this subscriber instead of sending it.
///
/// A policy input: the ten KiB the driver applied, unannounced, before Nervix declared the bound,
/// so declaring it refuses nothing an ingestor received before.
pub const MQTT_SOURCE_MAXIMUM_PACKET_BYTES: u64 = 10 * 1024;

/// The largest value MQTT's Four Byte Integer, which carries the Maximum Packet Size, can hold.
const MQTT_FOUR_BYTE_INTEGER_MAX: u64 = 0xFFFF_FFFF;

const _: () = assert!(
    MQTT_SOURCE_MAXIMUM_PACKET_BYTES > 0
        && MQTT_SOURCE_MAXIMUM_PACKET_BYTES <= MQTT_FOUR_BYTE_INTEGER_MAX,
    "MQTT forbids a Maximum Packet Size of zero, and carries it as a Four Byte Integer"
);

/// What the connector of a MQTT TOPIC ingestor holds and how long it waits.
const MQTT_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "each incoming packet, whose bound every CONNECT advertises as its Maximum \
                    Packet Size",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Bytes(MQTT_SOURCE_MAXIMUM_PACKET_BYTES),
                overflow: BufferOverflow::PeerDiscards,
            },
        },
        DeclaredBuffer {
            holds: "SUBSCRIBE and PUBACK requests for the connection",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Driver("1024 requests"),
                overflow: BufferOverflow::Backpressure,
            },
        },
    ],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, TCP, TLS and CONNECT",
            bound: WaitBound::Driver("5 s"),
        },
        DeclaredWait {
            wait: "SUBACK",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "a silent connection",
            bound: WaitBound::Driver("keep-alive of 60 s, or the broker's"),
        },
    ],
    driver_limits: &[
        "A packet above the advertised Maximum Packet Size that a broker sends anyway ends the \
         connection with DISCONNECT Packet too large.",
    ],
};

/// What the connector of a MQTT TOPIC emitter holds and how long it waits.
const MQTT_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "publishes queued for the driver",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("1 under QOS 0, ACK PARALLEL MAX under QOS 1 and 2"),
            overflow: BufferOverflow::FailPublish,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, TCP, TLS and CONNECT",
            bound: WaitBound::Driver("5 s"),
        },
        DeclaredWait {
            wait: "answers to QOS 1 and 2 publishes",
            bound: WaitBound::Clause("ACK TIMEOUT"),
        },
        DeclaredWait {
            wait: "writes to the connection",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &["Packets above the broker's maximum packet size are rejected."],
};

impl IngestorDeclaration for MqttTopicIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        headers: None,
        metadata: IngestorMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Mqtt),
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
        bounds: MQTT_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for MqttTopicIngestor {
    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Client(&self.client)
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

connector_ingestors! {
    connector Mqtt = "MQTT";
    /// What an MQTT source reads.
    pub enum MqttIngestor;
    /// The alterations of the ingestors that read an MQTT entity.
    pub enum MqttTopicIngestorAlteration;
    /// The entities an MQTT source reads.
    pub enum MqttIngestorEntity {
        Topic(MqttTopicIngestor) = "TOPIC", "MQTT";
    }
}

/// One topic an MQTT client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MqttTopicEmitter {
    pub mode: MqttTopicPublishingMode,
    pub client: ClientName,
    pub topic: MqttTopicName,
}

impl EmitterDeclaration for MqttTopicEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::Mqtt),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: MqttTopicPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(MqttTopicName::DECLARATION),
        }),
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: MQTT_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for MqttTopicEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Mqtt,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Mqtt = "MQTT";
    /// What an MQTT emitter writes.
    pub enum MqttEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum MqttTopicEmitterMode;
    /// The entities an MQTT emitter writes.
    pub enum MqttEmitterEntity {
        Topic(MqttTopicEmitter, MqttTopicPublishingMode) = "TOPIC";
    }
}

super::publishing::emitter_modes! {
    pub enum MqttTopicPublishingMode {
        Qos0 { retry_policy: crate::RetryPolicy } => MqttQos0,
        Qos1 { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => MqttQos1,
        Qos2 { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => MqttQos2,
    }
}

super::external_name::external_names! {
    /// An MQTT publication topic, without subscription wildcards.
    pub struct MqttTopicName => mqtt_publication_topic, label "mqtt_topic", example "devices/Orders";
}

fn mqtt_publication_topic(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, MQTT_TOPIC_FILTER_MAX)?;
    if let Some(ch) = text.chars().find(|ch| matches!(ch, '\0' | '+' | '#')) {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    Ok(())
}
