//! The Kafka connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Kafka client Model, the topic source and sink Models with their offset and
//!   delivery modes, the topic and consumer group names a source reads with the rules Kafka holds
//!   them to, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The Kafka driver, its configuration keys, or how a topic is consumed or
//!   produced to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{num::NonZeroU64, time::Duration};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeclaredBounds, DeclaredBuffer, DeclaredWait,
        WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
    external_name::{ExternalNameError, external_names, refuse_control_characters, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm, HeaderLayout, HeaderModel,
    IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities, LocatorDeclaration,
    LocatorName, OptionChoice, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
    PositionClass, RetryPolicy, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SinkReferenceKind, SourceBody, SourceEntity, SourceMetadataScope, SourceReference,
    SourceReferenceKind,
};

configured_client!(Kafka: unpooled, config Driver, CreateClientKafka => KafkaConfigEntry);

/// The longest topic Kafka admits, in bytes.
const KAFKA_TOPIC_MAX: usize = 249;
/// The longest consumer group Nervix joins, in bytes. Kafka bounds a group only by the length of
/// its protocol string, so this bound is Nervix's own.
const KAFKA_CONSUMER_GROUP_MAX: usize = 255;

external_names! {
    /// A Kafka topic: 1 to 249 ASCII letters, digits, `.`, `_` and `-`, and neither `.` nor `..`,
    /// as the broker itself requires.
    pub struct KafkaTopicName => kafka_topic_rule, label "kafka_topic", example "orders";
    /// A Kafka consumer group: 1 to 255 bytes without control characters.
    pub struct KafkaConsumerGroupName => kafka_consumer_group_rule, label "kafka_consumer_group",
        example "readers";
}

fn kafka_topic_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, KAFKA_TOPIC_MAX)?;
    let refused = text
        .chars()
        .find(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')));
    if let Some(ch) = refused {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    match text {
        "." => Err(ExternalNameError::Reserved { name: "." }),
        ".." => Err(ExternalNameError::Reserved { name: ".." }),
        _ => Ok(()),
    }
}

fn kafka_consumer_group_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, KAFKA_CONSUMER_GROUP_MAX)?;
    refuse_control_characters(text)
}

/// One topic a Kafka client consumes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct KafkaTopicSource {
    pub client: ClientName,
    pub topic: KafkaTopicName,
    pub offset_mode: KafkaOffsetMode,
    pub instances: NonZeroU64,
    pub mode: KafkaIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a KAFKA TOPIC ingestor holds and how long it waits.
const KAFKA_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "messages librdkafka prefetches",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Driver(
                    "queued.min.messages 100000 per partition, queued.max.messages.kbytes 65536",
                ),
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "admitted synchronous control jobs per node",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 1024,
                    unit: "pending jobs",
                },
                overflow: BufferOverflow::RefuseRequest,
            },
        },
    ],
    waits: &[
        DeclaredWait {
            wait: "name resolution",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "connection setup: TCP, TLS and SASL",
            bound: WaitBound::Driver("socket.connection.setup.timeout.ms, 30 s"),
        },
        DeclaredWait {
            wait: "a fetch",
            bound: WaitBound::Driver("socket.timeout.ms, 60 s"),
        },
        DeclaredWait {
            wait: "a consumer-group commit under ACK",
            bound: WaitBound::Fixed(Duration::from_secs(30)),
        },
        DeclaredWait {
            wait: "metadata, partition inspection, offset lookups, watermarks and seeks including \
                   executor admission",
            bound: WaitBound::Driver("socket.timeout.ms, 60 s per control job"),
        },
        DeclaredWait {
            wait: "subscribe, assign and unsubscribe",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "destroying a consumer that joined its group, for its LeaveGroup answer",
            bound: WaitBound::Driver("5 s per LeaveGroup request"),
        },
    ],
    driver_limits: &[
        "Every librdkafka key passes through, so a client can change each driver default above.",
        "Connector control execution has two workers per node by default. A commit still \
         unanswered after its caller's 30 s deadline retains its worker and the source's one \
         in-flight commit until the native call ends. Native subscribe, assign and unsubscribe \
         have no cancellable deadline; their caller waits socket.timeout.ms at most.",
        "A consumer whose last handle is gone is destroyed by one connector control job, which \
         waits for a place rather than being refused; no Nervix key changes librdkafka's 5 s \
         LeaveGroup timeout.",
        "receive.message.max.bytes 100000000",
    ],
};

/// What the connector of a KAFKA TOPIC emitter holds and how long it waits.
const KAFKA_SINK_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "records the producer queues under ACK",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Driver(
                    "queue.buffering.max.messages 100000, queue.buffering.max.kbytes 1048576",
                ),
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "records the producer queues under NO ACK",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Driver(
                    "queue.buffering.max.messages 100000, queue.buffering.max.kbytes 1048576",
                ),
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
            wait: "a produce request",
            bound: WaitBound::Driver("request.timeout.ms, 30 s"),
        },
        DeclaredWait {
            wait: "delivery under NO ACK",
            bound: WaitBound::Driver("message.timeout.ms, 300 s"),
        },
        DeclaredWait {
            wait: "delivery under ACK",
            bound: WaitBound::Clause("ACK TIMEOUT"),
        },
        DeclaredWait {
            wait: "destroying a producer, for its polling thread and the reports of the records \
                   its purge failed",
            bound: WaitBound::Driver("100 ms poll, then a 500 ms flush"),
        },
    ],
    driver_limits: &[
        "Every librdkafka key passes through, so a client can change each driver default above.",
        "message.max.bytes 1000000",
        "A record whose payload exceeds the whole queue.buffering.max.kbytes is rejected through \
         ON MESSAGE ERROR.",
        "A producer whose last handle is gone is destroyed by one connector control job, which \
         waits for a place rather than being refused; no Nervix key changes rust-rdkafka's 100 ms \
         poll or 500 ms flush, and librdkafka then joins its threads, a broker thread still \
         resolving a name included.",
    ],
};

impl SourceEntity for KafkaTopicSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        // A Kafka record carries its headers as a list, in the order written, repeats included.
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        metadata: SourceMetadataScope::Kafka,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Kafka),
        position_class: PositionClass::Replayable,
        acknowledgement_confirmation: AcknowledgementConfirmation::Answered,
        delivery: &[
            DeclaredDelivery::NO_ACK,
            DeclaredDelivery::ACK_SEQUENTIAL,
            DeclaredDelivery::ACK_PARALLEL,
        ],
        quiesce: &[DeclaredQuiesce::SUSPEND],
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(KafkaTopicName::DECLARATION),
        }),
        options: &[
            OptionDeclaration {
                keywords: "OFFSET BY",
                presence: OptionPresence::Required,
                form: OptionForm::Choice(&[
                    OptionChoice {
                        name: "DOMAIN",
                        value: None,
                        example: "OFFSET BY DOMAIN",
                    },
                    OptionChoice {
                        name: "CONSUMER GROUP",
                        value: Some(OptionValue::Name(KafkaConsumerGroupName::DECLARATION)),
                        example: "OFFSET BY CONSUMER GROUP 'readers'",
                    },
                ]),
            },
            OptionDeclaration::INSTANCES,
        ],
        bounds: KAFKA_SOURCE_BOUNDS,
    };
}

impl SourceBody for KafkaTopicSource {
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
pub enum KafkaOffsetMode {
    ConsumerGroup(KafkaConsumerGroupName),
    Domain,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum KafkaIngestMode {
    AckParallel {
        max: NonZeroU64,
        batch_timeout: String,
        timeout: String,
        retry_policy: RetryPolicy,
    },
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
    NoAck,
}

impl KafkaIngestMode {
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
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Kafka = "KAFKA";
    /// What a Kafka source reads.
    pub enum KafkaSource;
    /// The alterations of the ingestors that read a Kafka entity.
    pub enum KafkaIngestorAlteration;
    /// The entities a Kafka source reads.
    pub enum KafkaSourceEntity {
        Topic(KafkaTopicSource) = "TOPIC", "Kafka";
    }
}

/// One topic a Kafka client produces to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct KafkaTopicSink {
    pub mode: KafkaTopicPublishingMode,
    pub client: ClientName,
    pub topic: KafkaTopicName,
}

impl SinkEntity for KafkaTopicSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        // A Kafka record carries its headers as a list, in the order written, repeats included.
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        reference: SinkReferenceKind::Client(ConnectorKind::Kafka),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: KafkaTopicPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(KafkaTopicName::DECLARATION),
        }),
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: KAFKA_SINK_BOUNDS,
    };
}

impl SinkBody for KafkaTopicSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Kafka,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Kafka = "KAFKA";
    /// What a Kafka sink writes.
    pub enum KafkaSink;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum KafkaEmitterMode;
    /// The entities a Kafka sink writes.
    pub enum KafkaSinkEntity {
        Topic(KafkaTopicSink, KafkaTopicPublishingMode) = "TOPIC";
    }
}

super::publishing::emitter_modes! {
    pub enum KafkaTopicPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
        Ack { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => BrokerAck,
    }
}
