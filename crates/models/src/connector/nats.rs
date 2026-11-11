//! The NATS connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The NATS client Model, subject and stream ingestor and emitter Models with their
//!   delivery modes, validated subject and queue group names, stream and durable consumer names,
//!   and each entity's declarations. The subject rule owns NATS 2.11 token syntax and Nervix's
//!   stricter limits; the JetStream name rule owns Nervix's implementation of nats-server 2.11's
//!   stream and consumer name rules.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared ingestor and emitter declarations and the names a body references.
//! - **Must not know.** The NATS driver, or how a subject is subscribed or
//!   published to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

mod subject;

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeadlineKey, DeclaredBounds, DeclaredBuffer,
        DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_emitters, connector_ingestors},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    EnvelopeClauseKind, HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode,
    IngestorDeclaration, IngestorEntityBody, IngestorEntityCapabilities,
    IngestorMetadataDeclaration, IngestorMetadataField, IngestorMetadataType, IngestorReference,
    IngestorReferenceKind, LocatorDeclaration, LocatorName, OptionDeclaration, OptionForm,
    OptionPresence, OptionValue, PositionClass, RetryPolicy,
};

configured_client!(Nats: unpooled, config CLIENT_CONFIG, CreateClientNats => NatsConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigFile as F, ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::required("addr", V::DriverAddress),
            K::optional("credentials_file", V::File(F::MountedFile)).secret(),
            K::optional("nkey_seed", V::String).secret(),
            K::TIMEOUT,
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
            K::optional("token", V::String).secret(),
        ],
        &[],
        &[],
    )
    .with_mutually_exclusive(&["credentials_file", "nkey_seed", "token"])
};

/// The longest queue group Nervix joins, in bytes.
const NATS_QUEUE_GROUP_MAX: usize = 255;
/// The longest stream or consumer name a JetStream server admits, in bytes.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "nats-server refuses a stream or consumer name longer than its JSMaxNameLen of \
                  255 bytes"
    )
)]
const NATS_JETSTREAM_NAME_MAX: usize = 255;

external_names! {
    /// A NATS subject a queue subscription listens on: at most 1,024 bytes of tokens joined by `.`,
    /// none of them empty, without whitespace or control characters. `*` stands for one token and
    /// `>` for every remaining token, each only as a whole token, and `>` only as the last one.
    /// The subject is subscribed to exactly as written, letter case included.
    pub struct NatsSubjectName => nats_subject_rule, label "nats_subject",
        example "orders.created";
    /// A NATS queue group: 1 to 255 bytes without whitespace or control characters, joined
    /// exactly as written.
    pub struct NatsQueueGroupName => nats_queue_group_rule, label "nats_queue_group",
        example "workers";
    /// A JetStream stream an operator provisioned: 1 to 255 bytes without whitespace, control
    /// characters, `.`, `*`, `>` or a path separator, read exactly as written.
    pub struct NatsStreamName => nats_jetstream_name_rule, label "nats_stream",
        example "orders";
    /// A durable JetStream consumer an operator provisioned on its stream, held to the rules of a
    /// stream name.
    pub struct NatsConsumerName => nats_jetstream_name_rule, label "nats_consumer",
        example "nervix";
}

/// The first whitespace or control character of `text`, which neither a subject nor a queue group
/// may hold.
fn refuse_whitespace(text: &str) -> Result<(), ExternalNameError> {
    match text
        .chars()
        .find(|ch| ch.is_whitespace() || ch.is_control())
    {
        Some(ch) => Err(ExternalNameError::InvalidChar { ch }),
        None => Ok(()),
    }
}

fn nats_subject_rule(text: &str) -> Result<(), ExternalNameError> {
    subject::SubscriptionPattern::parse(text).map(|_| ())
}

fn nats_queue_group_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, NATS_QUEUE_GROUP_MAX)?;
    refuse_whitespace(text)
}

/// The rule nats-server 2.11 holds a JetStream stream or consumer name to: 1 to 255 bytes, its
/// `JSMaxNameLen`, with no subject separator or wildcard, because the name is a subject token of
/// the JetStream API, and no path separator, because it names a directory of the server's store.
/// Like every NATS name here, it also holds no whitespace or control character, which the server
/// refuses only in part. Each refusal is a typed [`ExternalNameError`].
///
/// This is Nervix's one implementation of that rule: the vocabulary cannot depend on the NATS
/// client, whose own check refuses only part of it, and the connector's tests hold this rule to
/// that check.
fn nats_jetstream_name_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, NATS_JETSTREAM_NAME_MAX)?;
    refuse_whitespace(text)?;
    if let Some(ch) = text
        .chars()
        .find(|ch| matches!(ch, '.' | '*' | '>' | '/' | '\\'))
    {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    Ok(())
}

/// One subject a NATS client subscribes to as a member of a queue group.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NatsSubjectIngestor {
    pub client: ClientName,
    pub subject: NatsSubjectName,
    pub queue_group: NatsQueueGroupName,
    pub instances: crate::IngestorInstances,
    pub mode: NatsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a NATS SUBJECT ingestor holds and how long it waits.
const NATS_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "messages the subscription holds ahead of the host",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Driver("65,536 messages"),
            overflow: BufferOverflow::DropUncounted,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "name resolution",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "connection",
            bound: WaitBound::Driver("5 s per address"),
        },
        DeclaredWait {
            wait: "the subscription's flush",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "a silent connection",
            bound: WaitBound::Driver(
                "a ping every 60 s; the connection ends after 2 go unanswered",
            ),
        },
    ],
    driver_limits: &[],
};

/// What the connector of a NATS SUBJECT emitter holds and how long it waits.
const NATS_SUBJECT_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "commands queued for the connection",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Driver("2048 commands"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "name resolution",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "connection",
            bound: WaitBound::Driver("5 s per address"),
        },
        DeclaredWait {
            wait: "a core publish and its flush to the client's socket, not a server round trip",
            bound: WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Unbounded,
            },
        },
    ],
    driver_limits: &["Messages above the server's max_payload are rejected."],
};

/// What a JetStream emitter holds while it awaits publication acknowledgements.
const NATS_STREAM_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "commands queued for the connection",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Driver("2048 commands"),
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "PubAck answers awaiting settlement",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("ACK PARALLEL MAX, 1 under ACK SEQUENTIAL"),
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
            bound: WaitBound::Driver("5 s per address"),
        },
        DeclaredWait {
            wait: "the stream lookup",
            bound: WaitBound::Clause("ACK TIMEOUT"),
        },
        DeclaredWait {
            wait: "a JetStream acknowledgement",
            bound: WaitBound::Clause("ACK TIMEOUT"),
        },
    ],
    driver_limits: &["Messages above the server's max_payload are rejected."],
};

impl IngestorDeclaration for NatsSubjectIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        external_partitions: crate::IngestorExternalPartitions::None,
        events: crate::DeclaredEvents::NONE,
        instancing: crate::IngestorInstancingCapability::PerPartition,
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        payload: crate::IngestorPayloadDeclaration::BYTES,
        metadata: IngestorMetadataDeclaration::new(&[
            IngestorMetadataField::new("subject", IngestorMetadataType::String),
            IngestorMetadataField::new("reply", IngestorMetadataType::String),
        ]),
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Nats),
        // Core NATS hands each message to one member of the queue group and retains nothing for it,
        // so a subscriber has no position to resume from and nothing to acknowledge.
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        // Core NATS keeps no backlog for a subscriber, so intake cannot suspend: a quiesced member
        // keeps reading its share and buffers or drops it.
        quiesce: &[DeclaredQuiesce::BUFFER, DeclaredQuiesce::DROP],
        locator: Some(LocatorDeclaration {
            keywords: "SUBJECT",
            name: LocatorName::External(NatsSubjectName::DECLARATION),
        }),
        options: &[
            OptionDeclaration {
                keywords: "QUEUE GROUP",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Name(NatsQueueGroupName::DECLARATION),
                    example: "QUEUE GROUP 'workers'",
                },
            },
            OptionDeclaration::INSTANCES.required(),
        ],
        bounds: NATS_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for NatsSubjectIngestor {
    fn instances(&self) -> Option<crate::IngestorInstances> {
        Some(self.instances)
    }

    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Client(&self.client)
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NatsIngestMode {
    NoAck,
}

impl NatsIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

/// One change an `ALTER NATS SUBJECT INGESTOR` writes to the body of the ingestor it changes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NatsSubjectIngestorChange {
    /// `SET SUBJECT '<subject>'`.
    Subject(NatsSubjectName),
    /// `SET QUEUE GROUP '<group>'`.
    QueueGroup(NatsQueueGroupName),
    /// `SET MODE <mode>`.
    Mode(NatsIngestMode),
}

impl crate::IngestorEntityChange for NatsSubjectIngestorChange {
    type Entity = NatsSubjectIngestor;

    fn apply_to(
        &self,
        entity: &mut NatsSubjectIngestor,
    ) -> error_stack::Result<(), crate::AlterIngestorError> {
        match self {
            Self::Subject(subject) => entity.subject = subject.clone(),
            Self::QueueGroup(group) => entity.queue_group = group.clone(),
            Self::Mode(mode) => entity.mode = mode.clone(),
        }
        Ok(())
    }

    fn to_canonical_plexus(&self) -> error_stack::Result<String, crate::CanonicalPlexusError> {
        let text = match self {
            Self::Subject(subject) => {
                format!("SET SUBJECT {}", crate::string_literal(subject.as_str()))
            }
            Self::QueueGroup(group) => {
                format!("SET QUEUE GROUP {}", crate::string_literal(group.as_str()))
            }
            Self::Mode(mode) => {
                format!("SET MODE {}", mode.acknowledgement().to_canonical_plexus())
            }
        };
        Ok(text)
    }
}

/// One durable pull consumer of a JetStream stream, both of which an operator provisioned.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NatsStreamIngestor {
    pub client: ClientName,
    pub stream: NatsStreamName,
    pub consumer: NatsConsumerName,
    pub mode: NatsStreamIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a NATS STREAM ingestor holds and how long it waits.
const NATS_STREAM_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "deliveries a pull request brought ahead of the host",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("ACK PARALLEL MAX, 1 under ACK SEQUENTIAL"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "name resolution",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "connection",
            bound: WaitBound::Driver("5 s per address"),
        },
        DeclaredWait {
            wait: "the consumer lookup",
            bound: WaitBound::Driver("5 s request timeout"),
        },
        DeclaredWait {
            wait: "the answer to an acknowledgement or a negative acknowledgement",
            bound: WaitBound::Driver("10 s request timeout"),
        },
        DeclaredWait {
            wait: "a pull request, and a silent connection",
            bound: WaitBound::Driver(
                "a ping every 60 s; the connection ends after 2 go unanswered",
            ),
        },
    ],
    driver_limits: &[
        "A pull request asks for at most the consumer's MaxRequestBatch and waits at most its \
         MaxRequestExpires, where its operator set them.",
    ],
};

impl IngestorDeclaration for NatsStreamIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        external_partitions: crate::IngestorExternalPartitions::None,
        events: crate::DeclaredEvents::NONE,
        instancing: crate::IngestorInstancingCapability::Single,
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        payload: crate::IngestorPayloadDeclaration::BYTES,
        metadata: IngestorMetadataDeclaration::new(&[
            IngestorMetadataField::new("subject", IngestorMetadataType::String),
            IngestorMetadataField::new("reply", IngestorMetadataType::String),
            IngestorMetadataField::new("stream", IngestorMetadataType::String),
            IngestorMetadataField::new("consumer", IngestorMetadataType::String),
            IngestorMetadataField::new("sequence", IngestorMetadataType::U64),
            IngestorMetadataField::new("timestamp", IngestorMetadataType::Datetime),
            IngestorMetadataField::new("delivered", IngestorMetadataType::U64),
        ]),
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Nats),
        // A delivery is acknowledged on a subject of its own, and the consumer delivers again what a
        // lost connection held unacknowledged once the consumer's AckWait passes.
        position_class: PositionClass::BrokerRedelivered,
        // Each acknowledgement is a request the server answers once it recorded it.
        acknowledgement_confirmation: AcknowledgementConfirmation::Answered,
        delivery: &[
            DeclaredDelivery::ACK_SEQUENTIAL,
            DeclaredDelivery::ACK_PARALLEL,
        ],
        // The stream keeps every message the consumer has not delivered, so a suspended ingestor
        // loses nothing. An ingestor that buffers or drops while quiesced keeps pulling and
        // acknowledges each delivery once the host buffered or dropped it.
        quiesce: &[
            DeclaredQuiesce::SUSPEND,
            DeclaredQuiesce::BUFFER,
            DeclaredQuiesce::DROP,
        ],
        locator: Some(LocatorDeclaration {
            keywords: "STREAM",
            name: LocatorName::External(NatsStreamName::DECLARATION),
        }),
        options: &[OptionDeclaration {
            keywords: "CONSUMER",
            presence: OptionPresence::Required,
            form: OptionForm::Value {
                value: OptionValue::Name(NatsConsumerName::DECLARATION),
                example: "CONSUMER 'nervix'",
            },
        }],
        bounds: NATS_STREAM_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for NatsStreamIngestor {
    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Client(&self.client)
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
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

/// How a NATS STREAM ingestor settles each delivery with its consumer.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NatsStreamIngestMode {
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

impl NatsStreamIngestMode {
    pub fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::AckSequential {
                timeout,
                retry_policy,
            } => IngestAcknowledgement::Sequential {
                timeout,
                retry: retry_policy,
            },
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
        }
    }
}

/// One change an `ALTER NATS STREAM INGESTOR` writes to the body of the ingestor it changes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NatsStreamIngestorChange {
    /// `SET STREAM '<stream>'`.
    Stream(NatsStreamName),
    /// `SET CONSUMER '<consumer>'`.
    Consumer(NatsConsumerName),
    /// `SET MODE <mode>`.
    Mode(NatsStreamIngestMode),
}

impl crate::IngestorEntityChange for NatsStreamIngestorChange {
    type Entity = NatsStreamIngestor;

    fn apply_to(
        &self,
        entity: &mut NatsStreamIngestor,
    ) -> error_stack::Result<(), crate::AlterIngestorError> {
        match self {
            Self::Stream(stream) => entity.stream = stream.clone(),
            Self::Consumer(consumer) => entity.consumer = consumer.clone(),
            Self::Mode(mode) => entity.mode = mode.clone(),
        }
        Ok(())
    }

    fn to_canonical_plexus(&self) -> error_stack::Result<String, crate::CanonicalPlexusError> {
        let text = match self {
            Self::Stream(stream) => {
                format!("SET STREAM {}", crate::string_literal(stream.as_str()))
            }
            Self::Consumer(consumer) => {
                format!("SET CONSUMER {}", crate::string_literal(consumer.as_str()))
            }
            Self::Mode(mode) => {
                format!("SET MODE {}", mode.acknowledgement().to_canonical_plexus())
            }
        };
        Ok(text)
    }
}

connector_ingestors! {
    connector Nats = "NATS";
    /// What a NATS ingestor reads.
    pub enum NatsIngestor;
    /// The alterations of the ingestors that read a NATS entity.
    pub enum NatsSubjectIngestorAlteration;
    /// The entities a NATS ingestor reads.
    pub enum NatsIngestorEntity {
        Subject(NatsSubjectIngestor => NatsSubjectIngestorChange) = "SUBJECT", "NATS";
        Stream(NatsStreamIngestor => NatsStreamIngestorChange) = "STREAM", "NATS JetStream";
    }
}

/// One subject a NATS client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NatsSubjectEmitter {
    pub mode: NatsSubjectPublishingMode,
    pub client: ClientName,
    pub subject: NatsPublishSubjectName,
}

impl EmitterDeclaration for NatsSubjectEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        events: crate::DeclaredEvents::NONE,
        instancing: crate::EmitterInstancingCapability::Instances,
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        reference: EmitterReferenceKind::Client(ConnectorKind::Nats),
        body: EmitterBodyForm::Codec,
        payload: super::EmitterPayloadFormat::Codec,
        // The NATS protocol sets no message size of its own: each server announces its
        // max_payload, and the client refuses a message above it before writing.
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: NatsSubjectPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "SUBJECT",
            name: LocatorName::External(NatsPublishSubjectName::DECLARATION),
        }),
        // A core publication can name the subject its receiver should reply to.
        options: &[],
        envelope: &[EnvelopeClauseKind::ReplyTo],
        properties: &[],
        bounds: NATS_SUBJECT_EMITTER_BOUNDS,
    };
}

/// A publication to one operator-provisioned JetStream stream and one literal subject it captures.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NatsStreamEmitter {
    pub client: ClientName,
    pub stream: NatsStreamName,
    pub subject: NatsPublishSubjectName,
    pub mode: NatsStreamPublishingMode,
}

impl EmitterDeclaration for NatsStreamEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        events: crate::DeclaredEvents::NONE,
        instancing: crate::EmitterInstancingCapability::Instances,
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        reference: EmitterReferenceKind::Client(ConnectorKind::Nats),
        body: EmitterBodyForm::Codec,
        payload: super::EmitterPayloadFormat::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: NatsStreamPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "STREAM",
            name: LocatorName::External(NatsStreamName::DECLARATION),
        }),
        options: &[OptionDeclaration {
            keywords: "SUBJECT",
            presence: OptionPresence::Required,
            form: OptionForm::Value {
                value: OptionValue::Name(NatsPublishSubjectName::DECLARATION),
                example: "SUBJECT 'orders.created'",
            },
        }],
        envelope: &[EnvelopeClauseKind::MessageId],
        properties: &[],
        bounds: NATS_STREAM_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for NatsStreamEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Nats,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

impl EmitterEntityBody for NatsSubjectEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Nats,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

/// One change an `ALTER NATS SUBJECT EMITTER` writes to the body of the emitter it changes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NatsSubjectEmitterChange {
    /// `SET SUBJECT '<name>'`.
    Subject(NatsPublishSubjectName),
    /// `SET MODE <mode>`.
    Mode(NatsSubjectPublishingMode),
}

impl crate::EmitterEntityChange for NatsSubjectEmitterChange {
    type Entity = NatsSubjectEmitter;

    fn apply_to(
        &self,
        entity: &mut NatsSubjectEmitter,
    ) -> error_stack::Result<(), crate::AlterEmitterError> {
        match self {
            Self::Subject(name) => entity.subject = name.clone(),
            Self::Mode(mode) => entity.mode = mode.clone(),
        }
        Ok(())
    }

    fn to_canonical_plexus(&self) -> error_stack::Result<String, crate::CanonicalPlexusError> {
        let text = match self {
            Self::Subject(name) => format!("SET SUBJECT {}", crate::string_literal(name.as_str())),
            Self::Mode(mode) => format!("SET MODE {}", mode.publishing().to_canonical_plexus()),
        };
        Ok(text)
    }
}

/// One change an `ALTER NATS STREAM EMITTER` writes to its destination body.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NatsStreamEmitterChange {
    /// `SET STREAM '<name>'`.
    Stream(NatsStreamName),
    /// `SET SUBJECT '<name>'`.
    Subject(NatsPublishSubjectName),
    /// `SET MODE <mode>`.
    Mode(NatsStreamPublishingMode),
}

impl crate::EmitterEntityChange for NatsStreamEmitterChange {
    type Entity = NatsStreamEmitter;

    fn apply_to(
        &self,
        entity: &mut NatsStreamEmitter,
    ) -> error_stack::Result<(), crate::AlterEmitterError> {
        match self {
            Self::Stream(name) => entity.stream = name.clone(),
            Self::Subject(name) => entity.subject = name.clone(),
            Self::Mode(mode) => entity.mode = mode.clone(),
        }
        Ok(())
    }

    fn to_canonical_plexus(&self) -> error_stack::Result<String, crate::CanonicalPlexusError> {
        let text = match self {
            Self::Stream(name) => format!("SET STREAM {}", crate::string_literal(name.as_str())),
            Self::Subject(name) => format!("SET SUBJECT {}", crate::string_literal(name.as_str())),
            Self::Mode(mode) => format!("SET MODE {}", mode.publishing().to_canonical_plexus()),
        };
        Ok(text)
    }
}

connector_emitters! {
    connector Nats = "NATS";
    /// What a NATS emitter writes.
    pub enum NatsEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum NatsEmitterMode;
    /// One change to the body of one of this connector's emitter entities.
    pub enum NatsEmitterChange;
    /// The entities a NATS emitter writes.
    pub enum NatsEmitterEntity {
        Subject(NatsSubjectEmitter, NatsSubjectPublishingMode, NatsSubjectEmitterChange) = "SUBJECT";
        Stream(NatsStreamEmitter, NatsStreamPublishingMode, NatsStreamEmitterChange) = "STREAM";
    }
}

super::publishing::emitter_modes! {
    pub enum NatsSubjectPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
    }
}

super::publishing::emitter_modes! {
    pub enum NatsStreamPublishingMode {
        Ack { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => BrokerAck,
    }
}

super::external_name::external_names! {
    /// A NATS publication subject: a subject a subscription could name, without the `*` and `>`
    /// wildcards, since a message is published to one literal subject. It is published to exactly
    /// as written, letter case included.
    pub struct NatsPublishSubjectName => nats_publication_subject,
        label "nats_publish_subject", example "orders.Created";
}

fn nats_publication_subject(text: &str) -> Result<(), ExternalNameError> {
    subject::SubscriptionPattern::parse(text)?.require_literal()
}

#[cfg(test)]
#[path = "nats_tests.rs"]
mod tests;
