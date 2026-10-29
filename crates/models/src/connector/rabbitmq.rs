//! The RabbitMQ connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The RabbitMQ client Model, the queue source and emitter Models and the exchange
//!   emitter Model with their delivery modes, the queue and exchange names with the rule AMQP holds
//!   them to, the routing key an exchange emitter computes for every record, and what each entity
//!   declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared source and emitter declarations and the names a body references.
//! - **Must not know.** The AMQP driver, or how a queue is consumed or a queue or an exchange is
//!   published to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::{NonZeroU16, NonZeroU64};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
        DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    declaration::{OptionForm, OptionPresence, OptionValue},
    entity_union::{connector_emitters, connector_ingestors},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    EnvelopeClauseKind, Expression, HeaderLayout, HeaderModel, IngestAcknowledgement,
    IngestQuiesceMode, IngestorDeclaration, IngestorEntityBody, IngestorEntityCapabilities,
    IngestorMetadataScope, IngestorReference, IngestorReferenceKind, LocatorDeclaration,
    LocatorName, OptionDeclaration, PositionClass, RetryPolicy,
};

configured_client!(RabbitMq: unpooled, config CLIENT_CONFIG, CreateClientRabbitMq => RabbitMqConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::DriverAddress),
            K::CONNECT_TIMEOUT,
            K::TIMEOUT,
            K::TLS_CA.only_for_scheme("addr", "amqps"),
            K::TLS_CERT.only_for_scheme("addr", "amqps"),
            K::TLS_KEY.only_for_scheme("addr", "amqps"),
        ],
        &[],
        &[],
    )
};

/// The longest queue or exchange name AMQP 0-9-1 carries, in bytes.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "AMQP 0-9-1 encodes queue and exchange names as one-byte-length short strings"
    )
)]
const RABBITMQ_NAME_MAX: usize = 255;

external_names! {
    /// A RabbitMQ queue: 1 to 255 bytes, the length AMQP 0-9-1 carries a queue name in.
    pub struct RabbitMqQueueName => rabbitmq_name_rule, label "rabbitmq_queue", example "orders";
    /// A RabbitMQ exchange: 1 to 255 bytes, the length AMQP 0-9-1 carries an exchange name in. The
    /// default exchange, whose name is empty, is the one a QUEUE emitter publishes through.
    pub struct RabbitMqExchangeName => rabbitmq_name_rule, label "rabbitmq_exchange", example "events";
}

fn rabbitmq_name_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, RABBITMQ_NAME_MAX)
}

/// One queue a RabbitMQ client consumes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RabbitMqQueueIngestor {
    pub client: ClientName,
    pub queue: RabbitMqQueueName,
    pub instances: NonZeroU64,
    pub mode: RabbitMqIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a RABBITMQ QUEUE ingestor holds and how long it waits.
const RABBITMQ_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "Lapin's existing consumer delivery queue; broker prefetch bounds it in \
                    acknowledged modes, but automatic acknowledgements do not apply that bound",
            bound: BufferBound::Unbounded,
        },
        DeclaredBuffer {
            holds: "deliveries gathered for one host read",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting(
                    "1 under NO ACK and ACK SEQUENTIAL; ACK PARALLEL MAX otherwise, at most 65,535",
                ),
                overflow: BufferOverflow::Backpressure,
            },
        },
    ],
    waits: &[
        DeclaredWait {
            wait: "gathering further deliveries after the first under ACK PARALLEL; cancellation \
                   hands over every delivery already gathered",
            bound: WaitBound::Clause("BATCH TIMEOUT"),
        },
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
    driver_limits: &[
        "Consumer prefetch is 1 under ACK SEQUENTIAL and ACK PARALLEL MAX under ACK PARALLEL, at \
         most 65,535; the broker holds back deliveries beyond that window.",
        "NO ACK has no prefetch bound; auto-acknowledged deliveries can accumulate in Lapin's \
         existing consumer queue while the host is not reading.",
        "A frame above the negotiated frame_max ends the connection.",
    ],
};

/// What the connector of a RabbitMQ emitter holds between its writes, whichever entity it
/// publishes to.
const RABBITMQ_EMITTER_BUFFERS: &[DeclaredBuffer] = &[
    DeclaredBuffer {
        holds: "publishes awaiting confirmation",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("ACK PARALLEL MAX, 1 under ACK SEQUENTIAL"),
            overflow: BufferOverflow::Backpressure,
        },
    },
    DeclaredBuffer {
        holds: "messages the broker returns unrouted under MANDATORY, until the write that \
                published them reads them",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("the messages of one write"),
            overflow: BufferOverflow::Backpressure,
        },
    },
];

/// The waits of a RabbitMQ emitter after its connection opens, whichever entity it publishes to.
const PUBLISH_WAIT: DeclaredWait = DeclaredWait {
    wait: "a publish",
    bound: WaitBound::Unbounded,
};
const CONFIRMATION_WAIT: DeclaredWait = DeclaredWait {
    wait: "a confirmation under ACK",
    bound: WaitBound::Clause("ACK TIMEOUT"),
};
const ROUND_TRIP_WAIT: DeclaredWait = DeclaredWait {
    wait: "the round trip that ends a publish under NO ACK",
    bound: WaitBound::Key {
        key: DeadlineKey::Timeout,
        omitted: OmittedDeadline::Unbounded,
    },
};

/// What the connector of a RABBITMQ QUEUE emitter holds and how long it waits.
const RABBITMQ_QUEUE_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: RABBITMQ_EMITTER_BUFFERS,
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, address attempts, TLS, the AMQP handshake, \
                   channel open, confirm select and the queue check",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        PUBLISH_WAIT,
        CONFIRMATION_WAIT,
        ROUND_TRIP_WAIT,
    ],
    driver_limits: &["A frame above the negotiated frame_max ends the connection."],
};

/// What the connector of a RABBITMQ EXCHANGE emitter holds and how long it waits.
const RABBITMQ_EXCHANGE_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: RABBITMQ_EMITTER_BUFFERS,
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, address attempts, TLS, the AMQP handshake, \
                   channel open, confirm select and the exchange check",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        PUBLISH_WAIT,
        CONFIRMATION_WAIT,
        ROUND_TRIP_WAIT,
    ],
    driver_limits: &[
        "A frame above the negotiated frame_max ends the connection.",
        "A routing key longer than the 255 bytes an AMQP short string carries is not sent; its \
         record is rejected.",
    ],
};

impl IngestorDeclaration for RabbitMqQueueIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // An AMQP field table holds one value per name, in ascending order of name.
        headers: Some(HeaderModel::exact(HeaderLayout::SortedByName)),
        metadata: IngestorMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::RabbitMq),
        position_class: PositionClass::BrokerRedelivered,
        acknowledgement_confirmation: AcknowledgementConfirmation::Written,
        delivery: &[
            DeclaredDelivery::NO_ACK,
            DeclaredDelivery::ACK_SEQUENTIAL,
            DeclaredDelivery::ACK_PARALLEL,
        ],
        quiesce: &[DeclaredQuiesce::SUSPEND],
        locator: Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(RabbitMqQueueName::DECLARATION),
        }),
        options: &[OptionDeclaration::INSTANCES],
        bounds: RABBITMQ_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for RabbitMqQueueIngestor {
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
pub enum RabbitMqIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
    NoAck,
    AckParallel {
        /// AMQP's nonzero, 16-bit consumer prefetch count.
        max: NonZeroU16,
        batch_timeout: String,
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

impl RabbitMqIngestMode {
    pub fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
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
                max: (*max).into(),
                batch_timeout,
                timeout,
                retry: retry_policy,
            },
        }
    }
}

connector_ingestors! {
    connector RabbitMq = "RABBITMQ";
    /// What a RabbitMQ source reads.
    pub enum RabbitMqIngestor;
    /// The alterations of the ingestors that read a RabbitMQ entity.
    pub enum RabbitMqQueueIngestorAlteration;
    /// The entities a RabbitMQ source reads.
    pub enum RabbitMqIngestorEntity {
        Queue(RabbitMqQueueIngestor) = "QUEUE", "RabbitMQ";
    }
}

/// One queue a RabbitMQ client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RabbitMqQueueEmitter {
    pub mode: RabbitMqQueuePublishingMode,
    pub client: ClientName,
    pub queue: RabbitMqQueueName,
}

impl EmitterDeclaration for RabbitMqQueueEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        // An AMQP field table holds one value per name, in ascending order of name.
        headers: Some(HeaderModel::exact(HeaderLayout::SortedByName)),
        reference: EmitterReferenceKind::Client(ConnectorKind::RabbitMq),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: RabbitMqQueuePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(RabbitMqQueueName::DECLARATION),
        }),
        options: &[],
        envelope: &[EnvelopeClauseKind::Mandatory],
        properties: &[],
        bounds: RABBITMQ_QUEUE_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for RabbitMqQueueEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::RabbitMq,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

/// One exchange a RabbitMQ client publishes to, and the key the exchange routes each record by.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RabbitMqExchangeEmitter {
    pub mode: RabbitMqExchangePublishingMode,
    pub client: ClientName,
    pub exchange: RabbitMqExchangeName,
    /// `ROUTING KEY <expression>`: the exact, non-null, non-sensitive `STRING` each record is
    /// routed by, which the host computes for every record it publishes.
    pub routing_key: Expression,
}

impl EmitterDeclaration for RabbitMqExchangeEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        // An AMQP field table holds one value per name, in ascending order of name.
        headers: Some(HeaderModel::exact(HeaderLayout::SortedByName)),
        reference: EmitterReferenceKind::Client(ConnectorKind::RabbitMq),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: RabbitMqExchangePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "EXCHANGE",
            name: LocatorName::External(RabbitMqExchangeName::DECLARATION),
        }),
        options: &[OptionDeclaration {
            keywords: "ROUTING KEY",
            presence: OptionPresence::Required,
            form: OptionForm::Value {
                value: OptionValue::Expression,
                example: "ROUTING KEY input.category",
            },
        }],
        envelope: &[EnvelopeClauseKind::Mandatory],
        properties: &[],
        bounds: RABBITMQ_EXCHANGE_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for RabbitMqExchangeEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::RabbitMq,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }

    fn routing_key(&self) -> Option<&Expression> {
        Some(&self.routing_key)
    }
}

connector_emitters! {
    connector RabbitMq = "RABBITMQ";
    /// What a RabbitMQ emitter writes.
    pub enum RabbitMqEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum RabbitMqEmitterMode;
    /// The entities a RabbitMQ emitter writes.
    pub enum RabbitMqEmitterEntity {
        Queue(RabbitMqQueueEmitter, RabbitMqQueuePublishingMode) = "QUEUE";
        Exchange(RabbitMqExchangeEmitter, RabbitMqExchangePublishingMode) = "EXCHANGE";
    }
}

super::publishing::emitter_modes! {
    pub enum RabbitMqQueuePublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
        Ack { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => BrokerAck,
    }
}

super::publishing::emitter_modes! {
    pub enum RabbitMqExchangePublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
        Ack { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => BrokerAck,
    }
}

#[cfg(test)]
mod tests;
