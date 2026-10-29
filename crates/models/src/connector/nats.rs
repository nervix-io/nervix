//! The NATS connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The NATS client Model, the subject source and emitter Models with their delivery
//!   mode, the subject and queue group names a source subscribes with and the rules NATS holds
//!   them to, the stream source Model with the stream and durable consumer names it reads and the
//!   rules JetStream holds them to, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared source and emitter declarations and the names a body references.
//! - **Must not know.** The NATS driver, or how a subject is subscribed or
//!   published to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

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
    HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration,
    IngestorEntityBody, IngestorEntityCapabilities, IngestorMetadataScope, IngestorReference,
    IngestorReferenceKind, LocatorDeclaration, LocatorName, OptionDeclaration, OptionForm,
    OptionPresence, OptionValue, PositionClass, RetryPolicy,
};

configured_client!(Nats: unpooled, config CLIENT_CONFIG, CreateClientNats => NatsConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::DriverAddress),
            K::TIMEOUT,
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
        ],
        &[],
        &[],
    )
};

/// The longest subject Nervix subscribes to, in bytes.
const NATS_SUBJECT_MAX: usize = 1024;
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
    require_length(text, NATS_SUBJECT_MAX)?;
    refuse_whitespace(text)?;
    let mut tokens = text.split('.').peekable();
    while let Some(token) = tokens.next() {
        if token.is_empty() {
            return Err(ExternalNameError::EmptyLevel { separator: '.' });
        }
        if token.len() > 1
            && let Some(wildcard) = token.chars().find(|ch| matches!(ch, '*' | '>'))
        {
            return Err(ExternalNameError::WildcardInsideLevel { wildcard });
        }
        if token == ">" && tokens.peek().is_some() {
            return Err(ExternalNameError::WildcardBeforeLastLevel { wildcard: '>' });
        }
    }
    Ok(())
}

fn nats_queue_group_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, NATS_QUEUE_GROUP_MAX)?;
    refuse_whitespace(text)
}

/// The rule a JetStream server holds a stream or consumer name to: it names a subject token of the
/// JetStream API and a directory of the server's store, so it holds no subject separator or
/// wildcard and no path separator.
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
    pub instances: NonZeroU64,
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
const NATS_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
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
        DeclaredWait {
            wait: "a JetStream acknowledgement",
            bound: WaitBound::Clause("ACK TIMEOUT"),
        },
    ],
    driver_limits: &["Messages above the server's max_payload are rejected."],
};

impl IngestorDeclaration for NatsSubjectIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // A NATS message's header map keeps every value of a name together under it, and its names
        // in no order.
        headers: Some(HeaderModel::exact(HeaderLayout::RepeatsByName)),
        // A message also carries the subject it was published to and its reply subject, but the
        // declaration projects neither as a typed field, so programs read its payload and headers.
        metadata: IngestorMetadataScope::Headers,
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
        // A delivery carries the header map it was published with: every value of a name together
        // under it, and its names in no order.
        headers: Some(HeaderModel::exact(HeaderLayout::RepeatsByName)),
        // A delivery also carries its stream and consumer sequences, its timestamp and how often it
        // was delivered, but the declaration projects none of them as a typed field, so programs
        // read its payload and headers.
        metadata: IngestorMetadataScope::Headers,
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
        // The stream keeps every message the consumer has not delivered, so a suspended source
        // loses nothing. A source that buffers or drops while quiesced keeps pulling and
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

/// How a NATS STREAM source settles each delivery with its consumer.
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

connector_ingestors! {
    connector Nats = "NATS";
    /// What a NATS source reads.
    pub enum NatsIngestor;
    /// The alterations of the ingestors that read a NATS entity.
    pub enum NatsSubjectIngestorAlteration;
    /// The entities a NATS source reads.
    pub enum NatsIngestorEntity {
        Subject(NatsSubjectIngestor) = "SUBJECT", "NATS";
        Stream(NatsStreamIngestor) = "STREAM", "NATS JetStream";
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
        // A NATS message's header map keeps every value of a name together under it, and its names
        // in no order.
        headers: Some(HeaderModel::exact(HeaderLayout::RepeatsByName)),
        reference: EmitterReferenceKind::Client(ConnectorKind::Nats),
        body: EmitterBodyForm::Codec,
        // The NATS protocol sets no message size of its own: each server announces its
        // max_payload, and the client refuses a message above it before writing.
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: NatsSubjectPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "SUBJECT",
            name: LocatorName::External(NatsPublishSubjectName::DECLARATION),
        }),
        // A publication carries the subject, the written headers and the payload; the emitter sets
        // no reply subject or JetStream message id of its own.
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: NATS_EMITTER_BOUNDS,
    };
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

connector_emitters! {
    connector Nats = "NATS";
    /// What a NATS emitter writes.
    pub enum NatsEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum NatsSubjectEmitterMode;
    /// The entities a NATS emitter writes.
    pub enum NatsEmitterEntity {
        Subject(NatsSubjectEmitter, NatsSubjectPublishingMode) = "SUBJECT";
    }
}

super::publishing::emitter_modes! {
    pub enum NatsSubjectPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
        JetStream { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => NatsJetStream,
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
    nats_subject_rule(text)?;
    if let Some(ch) = text.chars().find(|ch| matches!(ch, '*' | '>')) {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    Ok(())
}

#[cfg(test)]
#[path = "nats_tests.rs"]
mod tests;
