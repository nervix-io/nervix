//! The NATS connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The NATS client Model, the subject source and sink Models with their delivery
//!   mode, the subject and queue group names a source subscribes with and the rules NATS holds
//!   them to, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The NATS driver, its configuration keys, or how a subject is subscribed or
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
        BufferBound, BufferCapacity, BufferOverflow, DeclaredBounds, DeclaredBuffer, DeclaredWait,
        WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm, EmitterPublishingModeKind,
    HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    LocatorDeclaration, LocatorName, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
    PositionClass, SinkBody, SinkClientReference, SinkEntity, SinkReference, SinkReferenceKind,
    SourceBody, SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind,
    SubjectName,
};

configured_client!(Nats: unpooled, config NervixWithAddressQuery, CreateClientNats => NatsConfigEntry);

/// The longest subject Nervix subscribes to, in bytes.
const NATS_SUBJECT_MAX: usize = 1024;
/// The longest queue group Nervix joins, in bytes.
const NATS_QUEUE_GROUP_MAX: usize = 255;

external_names! {
    /// A NATS subject a queue subscription listens on: at most 1,024 bytes of tokens joined by `.`,
    /// none of them empty, without whitespace or control characters. `*` stands for one token and
    /// `>` for every remaining token, each only as a whole token, and `>` only as the last one.
    pub struct NatsSubjectName => nats_subject_rule, label "nats_subject",
        example "orders.created";
    /// A NATS queue group: 1 to 255 bytes without whitespace or control characters.
    pub struct NatsQueueGroupName => nats_queue_group_rule, label "nats_queue_group",
        example "workers";
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

/// One subject a NATS client subscribes to as a member of a queue group.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NatsSubjectSource {
    pub client: ClientName,
    pub subject: NatsSubjectName,
    pub queue_group: NatsQueueGroupName,
    pub instances: NonZeroU64,
    pub mode: NatsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a NATS SUBJECT ingestor holds and how long it waits.
const NATS_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
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
const NATS_SINK_BOUNDS: DeclaredBounds = DeclaredBounds {
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
            wait: "a core publish and its flush",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "a JetStream acknowledgement",
            bound: WaitBound::Clause("ACK TIMEOUT"),
        },
    ],
    driver_limits: &["Messages above the server's max_payload are rejected."],
};

impl SourceEntity for NatsSubjectSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        // A NATS message's header map keeps every value of a name together under it, and its names
        // in no order.
        headers: Some(HeaderModel::exact(HeaderLayout::RepeatsByName)),
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Nats),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
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
        bounds: NATS_SOURCE_BOUNDS,
    };
}

impl SourceBody for NatsSubjectSource {
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

connector_sources! {
    connector Nats = "NATS";
    /// What a NATS source reads.
    pub enum NatsSource;
    /// The alterations of the ingestors that read a NATS entity.
    pub enum NatsIngestorAlteration;
    /// The entities a NATS source reads.
    pub enum NatsSourceEntity {
        Subject(NatsSubjectSource) = "SUBJECT", "NATS";
    }
}

/// One subject a NATS client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NatsSubjectSink {
    pub client: ClientName,
    pub subject: SubjectName,
}

impl SinkEntity for NatsSubjectSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        // A NATS message's header map keeps every value of a name together under it, and its names
        // in no order.
        headers: Some(HeaderModel::exact(HeaderLayout::RepeatsByName)),
        reference: SinkReferenceKind::Client(ConnectorKind::Nats),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: &[
            EmitterPublishingModeKind::NoAck,
            EmitterPublishingModeKind::NatsJetStream,
        ],
        locator: Some(LocatorDeclaration {
            keywords: "SUBJECT",
            name: LocatorName::Unquoted {
                label: "subject_name",
            },
        }),
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: NATS_SINK_BOUNDS,
    };
}

impl SinkBody for NatsSubjectSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Nats,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Nats = "NATS";
    /// What a NATS sink writes.
    pub enum NatsSink;
    /// The entities a NATS sink writes.
    pub enum NatsSinkEntity {
        Subject(NatsSubjectSink) = "SUBJECT";
    }
}
