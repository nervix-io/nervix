//! The RabbitMQ connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The RabbitMQ client Model, the queue source and sink Models with their delivery
//!   mode, the queue name a source consumes with the rule AMQP holds it to, and what each entity
//!   declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The AMQP driver, its configuration keys, or how a queue is consumed or
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
        BufferBound, BufferCapacity, BufferOverflow, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
        DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm, HeaderLayout, HeaderModel,
    IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities, LocatorDeclaration,
    LocatorName, OptionDeclaration, PositionClass, RetryPolicy, SinkBody, SinkClientReference,
    SinkEntity, SinkReference, SinkReferenceKind, SourceBody, SourceEntity, SourceMetadataScope,
    SourceReference, SourceReferenceKind,
};

configured_client!(RabbitMq: unpooled, config NervixWithAddressQuery, CreateClientRabbitMq => RabbitMqConfigEntry);

/// The longest queue name AMQP 0-9-1 carries, in bytes.
const RABBITMQ_QUEUE_MAX: usize = 255;

external_names! {
    /// A RabbitMQ queue: 1 to 255 bytes, the length AMQP 0-9-1 carries a queue name in.
    pub struct RabbitMqQueueName => rabbitmq_queue_rule, label "rabbitmq_queue", example "orders";
}

fn rabbitmq_queue_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, RABBITMQ_QUEUE_MAX)
}

/// One queue a RabbitMQ client consumes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RabbitMqQueueSource {
    pub client: ClientName,
    pub queue: RabbitMqQueueName,
    pub instances: NonZeroU64,
    pub mode: RabbitMqIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a RABBITMQ QUEUE ingestor holds and how long it waits.
const RABBITMQ_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "deliveries the broker sends ahead of their acknowledgement",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Items {
                count: 1,
                unit: "delivery",
            },
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, address attempts, TLS, the AMQP handshake, \
                   channel open and consume",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "a silent connection",
            bound: WaitBound::Driver("twice the negotiated heartbeat"),
        },
    ],
    driver_limits: &["A frame above the negotiated frame_max ends the connection."],
};

/// What the connector of a RABBITMQ QUEUE emitter holds and how long it waits.
const RABBITMQ_SINK_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "publishes awaiting confirmation",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("ACK PARALLEL MAX, 1 under ACK SEQUENTIAL"),
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "unroutable messages the broker returns under NO ACK",
            bound: BufferBound::Unbounded,
        },
    ],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, address attempts, TLS, the AMQP handshake, \
                   channel open, confirm select and the queue check",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "a publish",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "a confirmation under ACK",
            bound: WaitBound::Clause("ACK TIMEOUT"),
        },
        DeclaredWait {
            wait: "the round trip that ends a publish under NO ACK",
            bound: WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Unbounded,
            },
        },
    ],
    driver_limits: &["A frame above the negotiated frame_max ends the connection."],
};

impl SourceEntity for RabbitMqQueueSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        // An AMQP field table holds one value per name, in ascending order of name.
        headers: Some(HeaderModel::exact(HeaderLayout::SortedByName)),
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::RabbitMq),
        position_class: PositionClass::BrokerRedelivered,
        acknowledgement_confirmation: AcknowledgementConfirmation::Written,
        delivery: &[DeclaredDelivery::ACK_SEQUENTIAL],
        quiesce: &[DeclaredQuiesce::SUSPEND],
        locator: Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(RabbitMqQueueName::DECLARATION),
        }),
        options: &[OptionDeclaration::INSTANCES],
        bounds: RABBITMQ_SOURCE_BOUNDS,
    };
}

impl SourceBody for RabbitMqQueueSource {
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
pub enum RabbitMqIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

impl RabbitMqIngestMode {
    pub fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::AckSequential {
                timeout,
                retry_policy,
            } => IngestAcknowledgement::Sequential {
                timeout,
                retry: retry_policy,
            },
        }
    }
}

connector_sources! {
    connector RabbitMq = "RABBITMQ";
    /// What a RabbitMQ source reads.
    pub enum RabbitMqSource;
    /// The alterations of the ingestors that read a RabbitMQ entity.
    pub enum RabbitMqIngestorAlteration;
    /// The entities a RabbitMQ source reads.
    pub enum RabbitMqSourceEntity {
        Queue(RabbitMqQueueSource) = "QUEUE", "RabbitMQ";
    }
}

/// One queue a RabbitMQ client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RabbitMqQueueSink {
    pub mode: RabbitMqQueuePublishingMode,
    pub client: ClientName,
    pub queue: RabbitMqQueueName,
}

impl SinkEntity for RabbitMqQueueSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        // An AMQP field table holds one value per name, in ascending order of name.
        headers: Some(HeaderModel::exact(HeaderLayout::SortedByName)),
        reference: SinkReferenceKind::Client(ConnectorKind::RabbitMq),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: RabbitMqQueuePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(RabbitMqQueueName::DECLARATION),
        }),
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: RABBITMQ_SINK_BOUNDS,
    };
}

impl SinkBody for RabbitMqQueueSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::RabbitMq,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector RabbitMq = "RABBITMQ";
    /// What a RabbitMQ sink writes.
    pub enum RabbitMqSink;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum RabbitMqEmitterMode;
    /// The entities a RabbitMQ sink writes.
    pub enum RabbitMqSinkEntity {
        Queue(RabbitMqQueueSink, RabbitMqQueuePublishingMode) = "QUEUE";
    }
}

super::publishing::emitter_modes! {
    pub enum RabbitMqQueuePublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
        Ack { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => BrokerAck,
    }
}
