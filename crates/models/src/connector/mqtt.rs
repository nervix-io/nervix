//! The MQTT connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The MQTT client Model, the topic source and emitter Models with their session,
//!   quality-of-service and delivery modes, the topic filter a source subscribes to with the rules
//!   MQTT holds it to, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality, and the
//!   typed values of a configuration that satisfies them.
//! - **Depends on.** The shared source and emitter declarations, the shared client configuration
//!   schema and the names a body references.
//! - **Must not know.** The MQTT driver, or how a topic filter is
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
    AcknowledgementConfirmation, ChoiceRequirement, ClientConfigKey, ClientConfigSchema,
    ClientConfigValue, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration, IngestorEntityBody,
    IngestorEntityCapabilities, IngestorMetadataDeclaration, IngestorReference,
    IngestorReferenceKind, LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration,
    OptionForm, OptionPresence, PositionClass, RetryPolicy,
};

mod config;
#[cfg(test)]
mod config_tests;

pub use config::{
    MqttBroker, MqttClientConfig, MqttClientConfigError, MqttPassword, MqttTlsFiles,
    MqttTlsIdentity, MqttTransport,
};

configured_client!(Mqtt: unpooled, config CLIENT_CONFIG, CreateClientMqtt => MqttConfigEntry);

/// The broker every connection dials. A connection reads only its scheme, host and port.
const ADDR: ClientConfigKey =
    ClientConfigKey::required("addr", ClientConfigValue::HostPortUrl(&["mqtt", "mqtts"]));
/// The identity template every connection renders: its `{{instance}}` placeholder renders the
/// index of the ingestor instance that connects, and an emitter connects as instance 0.
const CLIENT_ID: ClientConfigKey =
    ClientConfigKey::optional("client_id", ClientConfigValue::String);
/// The keep-alive every `CONNECT` asks for, in whole seconds written as milliseconds: 60 seconds
/// where omitted.
const KEEP_ALIVE: ClientConfigKey =
    ClientConfigKey::defaulted("keep_alive_ms", ClientConfigValue::Milliseconds, "60000");
/// The largest complete publish an ingestor admits, its topic, properties and framing included:
/// 10,240 bytes where omitted. Emitters receive no publish.
const MAX_PACKET_SIZE: ClientConfigKey = ClientConfigKey::defaulted(
    "max_packet_size",
    ClientConfigValue::PositiveInteger,
    "10240",
)
.read_by_ingestors();
/// The password every `CONNECT` carries, which public inspection redacts.
const PASSWORD: ClientConfigKey =
    ClientConfigKey::optional("password", ClientConfigValue::String).secret();
/// The username every `CONNECT` carries.
const USERNAME: ClientConfigKey = ClientConfigKey::optional("username", ClientConfigValue::String);

/// The current keys this connector reads, shared by validation, completion and inspection.
///
/// A connection reads only the scheme, host and port of `addr`, and reads the TLS files only for
/// `mqtts`, where its trust has no other root than `tls_ca_file`. Credentials, the keep-alive and
/// the admission bound are keys of their own.
const CLIENT_CONFIG: ClientConfigSchema = ClientConfigSchema::new(
    &[
        ADDR,
        CLIENT_ID,
        KEEP_ALIVE,
        MAX_PACKET_SIZE,
        PASSWORD,
        ClientConfigKey::TLS_CA.exactly_for_scheme(ADDR.name, "mqtts"),
        ClientConfigKey::TLS_CERT.only_for_scheme(ADDR.name, "mqtts"),
        ClientConfigKey::TLS_KEY.only_for_scheme(ADDR.name, "mqtts"),
        USERNAME,
    ],
    &[],
    &[],
);

/// The longest topic filter MQTT carries, in bytes.
const MQTT_TOPIC_FILTER_MAX: usize = 65_535;

/// The first level of an MQTT shared subscription. Every topic ingestor subscribes through the
/// shared subscription `$share/<domain>~<ingestor>/<filter>`, which Nervix forms around the filter.
pub const MQTT_SHARED_SUBSCRIPTION_LEVEL: &str = "$share";

external_names! {
    /// An MQTT topic filter: 1 to 65,535 bytes without the NUL character, whose levels are joined by
    /// `/` and may be empty. `+` stands for one level and `#` for every remaining level, each only as
    /// a whole level, and `#` only as the last one, as MQTT itself requires. Its first level is not
    /// `$share`: the source subscribes through a shared subscription Nervix forms around the filter,
    /// within which a filter naming its own would match nothing. It is subscribed to exactly as
    /// written, letter case included.
    pub struct MqttTopicFilter => mqtt_topic_filter_rule, label "mqtt_topic_filter",
        example "devices/+/orders";
}

fn mqtt_topic_filter_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, MQTT_TOPIC_FILTER_MAX)?;
    if text.contains('\0') {
        return Err(ExternalNameError::InvalidChar { ch: '\0' });
    }
    let mut levels = text.split('/').peekable();
    if levels.peek() == Some(&MQTT_SHARED_SUBSCRIPTION_LEVEL) {
        return Err(ExternalNameError::ReservedFirstLevel {
            level: MQTT_SHARED_SUBSCRIPTION_LEVEL,
        });
    }
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

/// Extra acknowledged publishes advertised to the broker and the separate QoS 0 read-ahead quota.
/// Automatic acknowledgement uses this quota alone, with backpressure instead of overflow drops.
pub const MQTT_SOURCE_READ_AHEAD_HEADROOM: u64 = 64;

/// MQTT's largest packet: a 28-bit Remaining Length plus the five-byte fixed header.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "MQTT 5 encodes Remaining Length in at most four seven-bit groups and its fixed \
                  header in at most five bytes"
    )
)]
pub const MQTT_MAXIMUM_PACKET_BYTES: u64 = (1 << 28) + 4;

/// The largest value MQTT's Four Byte Integer, which carries the Maximum Packet Size, can hold.
const MQTT_FOUR_BYTE_INTEGER_MAX: u64 = 0xFFFF_FFFF;

const _: () = assert!(
    MQTT_MAXIMUM_PACKET_BYTES <= MQTT_FOUR_BYTE_INTEGER_MAX,
    "MQTT carries the Maximum Packet Size as a Four Byte Integer, so every admission bound up to \
     its largest packet fits thirty-two bits"
);

/// One interval of the keep-alive a connection keeps to, which bounds a wait for a write.
const KEEP_ALIVE_INTERVAL: WaitBound = WaitBound::ClientKey {
    key: KEEP_ALIVE.name,
    rule: "one interval of it, or of the broker's Server Keep Alive; 60 s when omitted",
};

/// How long a connection that hears nothing from its broker lasts: it pings after one interval
/// without traffic and ends when the following interval passes without an answer.
const SILENT_CONNECTION: WaitBound = WaitBound::ClientKey {
    key: KEEP_ALIVE.name,
    rule: "two intervals of it, pinging after the first, or of the broker's Server Keep Alive; 60 \
           s when omitted",
};

/// What the connector of a MQTT TOPIC ingestor holds and how long it waits.
const MQTT_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "each raw MQTT frame, whose protocol bound every CONNECT advertises as its \
                    Maximum Packet Size; one packet is decoded before admission",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Bytes(MQTT_MAXIMUM_PACKET_BYTES),
                overflow: BufferOverflow::PeerDiscards,
            },
        },
        DeclaredBuffer {
            holds: "each admitted publish including its packet headers, copied out of raw framing",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("max_packet_size, 10240 bytes when omitted"),
                overflow: BufferOverflow::DropAndCount,
            },
        },
        DeclaredBuffer {
            holds: "ACK-mode QoS 1/2 outcomes in the packet-ordered read-ahead queue",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting(
                    "the Receive Maximum every CONNECT advertises, ACK PARALLEL MAX or 1 under \
                     ACK SEQUENTIAL plus 64 up to 65,535",
                ),
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "ACK-mode QoS 0 outcomes alongside the reserved acknowledged capacity in the \
                    same packet-ordered read-ahead queue",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: MQTT_SOURCE_READ_AHEAD_HEADROOM,
                    unit: "outcome",
                },
                overflow: BufferOverflow::DropAndCount,
            },
        },
        DeclaredBuffer {
            holds: "NO ACK outcomes in the packet-ordered read-ahead queue",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: MQTT_SOURCE_READ_AHEAD_HEADROOM,
                    unit: "outcome",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "the next classified packet outcome held for room in read-ahead",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 1,
                    unit: "outcome",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "a deferred or in-progress size rejection retained across reads",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 1,
                    unit: "discard",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "SUBSCRIBE and PUBACK requests for the connection",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("the Receive Maximum, or 1 under NO ACK"),
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
            wait: "ordinary and oversized-discard acknowledgements written to the connection",
            bound: KEEP_ALIVE_INTERVAL,
        },
        DeclaredWait {
            wait: "a silent connection",
            bound: SILENT_CONNECTION,
        },
    ],
    driver_limits: &[
        "A publish above the admission bound is acknowledged before it is counted as oversized; \
         its topic, payload and properties never enter read-ahead. The raw framing bound is \
         MQTT's protocol maximum, separate from the admission bound.",
        "ACK-mode QoS 0 overflow is counted immediately and never consumes an acknowledged \
         outcome's reserved capacity. An oversized QoS 0 publish refused here counts as overflow \
         only. NO ACK retains backpressure.",
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
        DeclaredWait {
            wait: "a silent connection",
            bound: SILENT_CONNECTION,
        },
    ],
    driver_limits: &["Packets above the broker's maximum packet size are rejected."],
};

impl IngestorDeclaration for MqttTopicIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // An MQTT packet carries no transport header map, and the source reads no MQTT 5 user
        // property as one.
        headers: None,
        metadata: IngestorMetadataDeclaration::NONE,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Mqtt),
        // A persistent session's broker delivers again every publish a lost connection did not
        // acknowledge, and an acknowledgement is complete once its PUBACK is written: the broker
        // answers none.
        position_class: PositionClass::BrokerRedelivered,
        acknowledgement_confirmation: AcknowledgementConfirmation::Written,
        delivery: &[
            DeclaredDelivery::NO_ACK,
            DeclaredDelivery::ACK_SEQUENTIAL.requiring(BROKER_KEPT_SESSION),
            DeclaredDelivery::ACK_PARALLEL.requiring(BROKER_KEPT_SESSION),
        ],
        // Suspending disconnects while the broker keeps the session's publishes, which only a
        // persistent session at QoS 1 holds; buffering and dropping stay connected.
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
    strum::EnumIter,
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
    strum::EnumIter,
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

    /// How many of its publishes may wait on their acknowledgements at once: one under
    /// `ACK SEQUENTIAL` and `MAX` under `ACK PARALLEL`. `NO ACK` acknowledges nothing, so none wait.
    pub const fn acknowledgement_window(&self) -> Option<NonZeroU64> {
        match self {
            Self::NoAck { .. } => None,
            Self::AckSequential { .. } => Some(NonZeroU64::MIN),
            Self::AckParallel { max, .. } => Some(*max),
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
        // Each broker declares its Maximum Packet Size when the client connects, and the emitter
        // rejects a larger publish before handing it to the client.
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: MqttTopicPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(MqttTopicName::DECLARATION),
        }),
        // A publish carries its topic and payload; the emitter sets no retain flag and no MQTT 5
        // property of its own.
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
    /// An MQTT publication topic: 1 to 65,535 bytes without NUL or the `+` and `#` subscription
    /// wildcards, since a publish names one literal topic. It is published to exactly as written,
    /// letter case included.
    pub struct MqttTopicName => mqtt_publication_topic, label "mqtt_topic", example "devices/Orders";
}

fn mqtt_publication_topic(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, MQTT_TOPIC_FILTER_MAX)?;
    if let Some(ch) = text.chars().find(|ch| matches!(ch, '\0' | '+' | '#')) {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    Ok(())
}

#[cfg(test)]
#[path = "mqtt_tests.rs"]
mod tests;
