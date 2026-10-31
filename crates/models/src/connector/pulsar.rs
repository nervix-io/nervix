//! The Pulsar connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Pulsar client Model, the topic source and emitter Models with their delivery
//!   mode, the topic and subscription names a source reads with the rules Pulsar holds them to,
//!   who owns a source's subscription, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared source and emitter declarations and the names a body references.
//! - **Must not know.** The Pulsar driver, or how a topic is consumed or
//!   produced to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeclaredBounds, DeclaredBuffer, DeclaredWait,
        WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_emitters, connector_ingestors},
    external_name::{ExternalNameError, external_names, refuse_control_characters, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration,
    IngestorEntityBody, IngestorEntityCapabilities, IngestorMetadataDeclaration, IngestorReference,
    IngestorReferenceKind, KafkaIngestMode, LocatorDeclaration, LocatorName, OptionChoice,
    OptionDeclaration, OptionForm, OptionPresence, OptionValue, PositionClass,
};

configured_client!(Pulsar: unpooled, config CLIENT_CONFIG, CreateClientPulsar => PulsarConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::Url(&["pulsar", "pulsar+ssl"])),
            K::optional("namespace", V::String),
            K::optional("tls_allow_insecure_connection", V::Boolean),
            K::TLS_CA,
            K::optional("tls_hostname_verification_enabled", V::Boolean),
        ],
        &[],
        &[],
    )
};

/// The longest topic Nervix consumes, in bytes.
const PULSAR_TOPIC_MAX: usize = 1024;
/// The longest subscription Nervix consumes through, in bytes.
const PULSAR_SUBSCRIPTION_MAX: usize = 255;
/// The domains a qualified topic may name, which Pulsar defines.
const PULSAR_TOPIC_DOMAINS: [&str; 2] = ["persistent", "non-persistent"];
/// The levels a qualified topic names after its domain: its tenant, its namespace and itself.
const PULSAR_QUALIFIED_TOPIC_LEVELS: [&str; 3] = ["tenant", "namespace", "topic"];

external_names! {
    /// A Pulsar topic: a local name, which the connector places in its client's namespace, or a
    /// qualified `persistent://tenant/namespace/topic` or `non-persistent://tenant/namespace/topic`;
    /// at most 1,024 bytes without whitespace or control characters, and with no empty level.
    pub struct PulsarTopicName => pulsar_topic_rule, label "pulsar_topic", example "orders";
    /// A Pulsar subscription: 1 to 255 bytes without control characters.
    pub struct PulsarSubscriptionName => pulsar_subscription_rule, label "pulsar_subscription",
        example "readers";
}

fn pulsar_topic_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, PULSAR_TOPIC_MAX)?;
    if let Some(ch) = text
        .chars()
        .find(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    let Some((domain, path)) = text.split_once("://") else {
        // The connector places a local name in its client's namespace, so a local name is one
        // level.
        if text.contains('/') {
            return Err(ExternalNameError::LevelsWithoutDomain {
                separator: '/',
                known: &PULSAR_TOPIC_DOMAINS,
            });
        }
        return Ok(());
    };
    // The scan reads the two domains Pulsar defines, a fixed array.
    if !PULSAR_TOPIC_DOMAINS.contains(&domain) {
        return Err(ExternalNameError::UnknownDomain {
            domain: domain.to_string(),
            known: &PULSAR_TOPIC_DOMAINS,
        });
    }
    if path.split('/').any(str::is_empty) {
        return Err(ExternalNameError::EmptyLevel { separator: '/' });
    }
    let levels = path.split('/').count();
    if levels != PULSAR_QUALIFIED_TOPIC_LEVELS.len() {
        return Err(ExternalNameError::QualifiedLevels {
            levels,
            expected: &PULSAR_QUALIFIED_TOPIC_LEVELS,
        });
    }
    Ok(())
}

fn pulsar_subscription_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, PULSAR_SUBSCRIPTION_MAX)?;
    refuse_control_characters(text)
}

/// One topic a Pulsar client consumes through a subscription.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PulsarTopicIngestor {
    pub client: ClientName,
    pub topic: PulsarTopicName,
    pub subscription: PulsarSubscription,
    pub instances: NonZeroU64,
    pub mode: PulsarIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// The durable subscription a Pulsar source reads through, and who owns it on the broker.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PulsarSubscription {
    pub name: PulsarSubscriptionName,
    pub ownership: PulsarSubscriptionOwnership,
}

/// Who owns a Pulsar source's subscription, which its statement declares after the
/// subscription's name.
///
/// The broker cannot say later who created a subscription, so the statement states it rather than
/// Nervix inferring it when the subscription is first opened.
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
pub enum PulsarSubscriptionOwnership {
    /// `OWNED BY INGESTOR`: the ingestor owns the subscription. Joining it creates it when it is
    /// absent, and dropping the ingestor or its domain removes it.
    Ingestor,
    /// `PROVISIONED`: an operator provisioned the subscription. Every open verifies that it exists,
    /// and Nervix never creates or removes it.
    Provisioned,
}

impl PulsarSubscriptionOwnership {
    /// The words a statement writes after the subscription's name to declare this ownership.
    pub const fn keywords(self) -> &'static str {
        match self {
            Self::Ingestor => "OWNED BY INGESTOR",
            Self::Provisioned => "PROVISIONED",
        }
    }
}

/// A Pulsar subscription acknowledges exactly as a Kafka consumer does.
pub type PulsarIngestMode = KafkaIngestMode;

/// What the connector of a PULSAR TOPIC ingestor holds and how long it waits.
const PULSAR_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "messages each partition consumer receives ahead of the host",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Driver("a receiver queue of 1000 messages"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "name resolution",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "connection: TCP, TLS and CONNECT",
            bound: WaitBound::Driver("10 s per address, 12 retries"),
        },
        DeclaredWait {
            wait: "topic lookup and subscribe",
            bound: WaitBound::Driver("30 s per attempt, retried without limit"),
        },
        DeclaredWait {
            wait: "an acknowledgement under ACK",
            bound: WaitBound::Driver("30 s operation timeout"),
        },
        DeclaredWait {
            wait: "close",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &["Inbound frames have no size limit."],
};

/// What the connector of a PULSAR TOPIC emitter holds and how long it waits.
const PULSAR_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "receipts held under NO ACK",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 100,
                    unit: "receipts",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "messages the driver queues for its connection",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Driver("100 messages"),
                overflow: BufferOverflow::FailPublish,
            },
        },
        DeclaredBuffer {
            holds: "records awaiting confirmation under ACK",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("ACK PARALLEL MAX"),
                overflow: BufferOverflow::Backpressure,
            },
        },
    ],
    waits: &[
        DeclaredWait {
            wait: "name resolution",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "connection",
            bound: WaitBound::Driver("10 s per address"),
        },
        DeclaredWait {
            wait: "topic lookup, producer creation and receipts",
            bound: WaitBound::Driver(
                "the operation timeout: ACK TIMEOUT under ACK, 30 s under NO ACK",
            ),
        },
    ],
    driver_limits: &["Messages above the broker's announced maximum are rejected."],
};

impl IngestorDeclaration for PulsarTopicIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // A Pulsar message carries its properties as a list, in the order the producer wrote them.
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        metadata: IngestorMetadataDeclaration::NONE,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Pulsar),
        position_class: PositionClass::Replayable,
        // In an `ACK` mode; `MODE NO ACK` acknowledges without waiting for the broker.
        acknowledgement_confirmation: AcknowledgementConfirmation::Answered,
        delivery: &[
            DeclaredDelivery::NO_ACK,
            DeclaredDelivery::ACK_SEQUENTIAL,
            DeclaredDelivery::ACK_PARALLEL,
        ],
        quiesce: &[DeclaredQuiesce::SUSPEND],
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(PulsarTopicName::DECLARATION),
        }),
        options: &[
            OptionDeclaration {
                keywords: "SUBSCRIPTION",
                presence: OptionPresence::Required,
                // The subscription's name comes first and its ownership follows it, as each
                // example writes the option in full.
                form: OptionForm::Choice(&[
                    OptionChoice {
                        name: PulsarSubscriptionOwnership::Ingestor.keywords(),
                        value: Some(OptionValue::Name(PulsarSubscriptionName::DECLARATION)),
                        example: "SUBSCRIPTION 'readers' OWNED BY INGESTOR",
                    },
                    OptionChoice {
                        name: PulsarSubscriptionOwnership::Provisioned.keywords(),
                        value: Some(OptionValue::Name(PulsarSubscriptionName::DECLARATION)),
                        example: "SUBSCRIPTION 'readers' PROVISIONED",
                    },
                ]),
            },
            OptionDeclaration::INSTANCES,
        ],
        bounds: PULSAR_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for PulsarTopicIngestor {
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
}

connector_ingestors! {
    connector Pulsar = "PULSAR";
    /// What a Pulsar source reads.
    pub enum PulsarIngestor;
    /// The alterations of the ingestors that read a Pulsar entity.
    pub enum PulsarTopicIngestorAlteration;
    /// The entities a Pulsar source reads.
    pub enum PulsarIngestorEntity {
        Topic(PulsarTopicIngestor) = "TOPIC", "Pulsar";
    }
}

/// One topic a Pulsar client produces to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PulsarTopicEmitter {
    pub mode: PulsarTopicPublishingMode,
    pub client: ClientName,
    pub topic: PulsarTopicName,
}

impl EmitterDeclaration for PulsarTopicEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        // The producer keeps a message's properties in a map: one value per name, the last written.
        headers: Some(HeaderModel::exact(HeaderLayout::LastWins)),
        reference: EmitterReferenceKind::Client(ConnectorKind::Pulsar),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: PulsarTopicPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(PulsarTopicName::DECLARATION),
        }),
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: PULSAR_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for PulsarTopicEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Pulsar,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Pulsar = "PULSAR";
    /// What a Pulsar emitter writes.
    pub enum PulsarEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum PulsarTopicEmitterMode;
    /// The entities a Pulsar emitter writes.
    pub enum PulsarEmitterEntity {
        Topic(PulsarTopicEmitter, PulsarTopicPublishingMode) = "TOPIC";
    }
}

super::publishing::emitter_modes! {
    pub enum PulsarTopicPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
        Ack { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => BrokerAck,
    }
}
