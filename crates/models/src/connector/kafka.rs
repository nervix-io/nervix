//! The Kafka connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Kafka client Model, the topic source and emitter Models with their offset and
//!   delivery modes, the topic and consumer group names a source reads with the rules Kafka holds
//!   them to, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality, and the
//!   one name a client accepts for each librdkafka property.
//! - **Depends on.** The shared source and emitter declarations and the names a body references.
//! - **Must not know.** The Kafka driver, or how a topic is consumed or
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
    entity_union::{connector_emitters, connector_ingestors},
    external_name::{ExternalNameError, external_names, refuse_control_characters, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration,
    IngestorEntityBody, IngestorEntityCapabilities, IngestorMetadataDeclaration,
    IngestorMetadataField, IngestorMetadataType, IngestorReference, IngestorReferenceKind,
    LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration, OptionForm, OptionPresence,
    OptionValue, PositionClass, RetryPolicy,
};

configured_client!(Kafka: unpooled, config CLIENT_CONFIG, CreateClientKafka => KafkaConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace, ClientConfigSchema, ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional("auto.offset.reset", V::String).driver(),
            K::required("bootstrap.servers", V::String).driver(),
            K::optional("client.id", V::String).driver(),
            K::optional("delivery.timeout.ms", V::UnsignedInteger).driver(),
            K::optional("fetch.wait.max.ms", V::UnsignedInteger).driver(),
            K::optional("heartbeat.interval.ms", V::PositiveInteger).driver(),
            K::optional("linger.ms", V::UnsignedInteger).driver(),
            K::optional("message.max.bytes", V::PositiveInteger).driver(),
            K::optional("queue.buffering.max.messages", V::UnsignedInteger).driver(),
            K::optional("sasl.mechanism", V::String).driver(),
            K::optional("sasl.oauthbearer.config", V::String)
                .secret()
                .driver(),
            K::optional("sasl.password", V::String).secret().driver(),
            K::optional("sasl.username", V::String).secret().driver(),
            K::optional("security.protocol", V::String).driver(),
            K::optional("session.timeout.ms", V::PositiveInteger).driver(),
            K::optional("socket.timeout.ms", V::Milliseconds).driver(),
            K::optional(
                "ssl.ca.location",
                V::File(crate::ClientConfigFile::MountedFile),
            )
            .driver(),
            K::optional(
                "ssl.certificate.location",
                V::File(crate::ClientConfigFile::MountedFile),
            )
            .driver(),
            K::optional(
                "ssl.key.location",
                V::File(crate::ClientConfigFile::MountedFile),
            )
            .driver(),
            K::optional("ssl.key.password", V::String).secret().driver(),
            K::optional("ssl.key.pem", V::String).secret().driver(),
        ],
        &[ClientConfigNamespace::All],
        &[
            "allow.auto.create.topics",
            "delivery.report.only.error",
            "enable.auto.commit",
            "enable.partition.eof",
            "group.id",
        ],
    )
    .with_driver_aliases(LIBRDKAFKA_ALIASES, Some(LIBRDKAFKA_TOPIC_PREFIX))
};

/// The other names librdkafka reads for its properties, each with the name a Kafka client accepts
/// instead: the Apache Kafka Java client's, which librdkafka reads as an alias. Sorted by the name
/// refused, as read from the property table of librdkafka 2.12.1, which the Kafka connector links
/// and whose own name lookup its tests hold these pairs to.
const LIBRDKAFKA_ALIASES: &[crate::ClientConfigAlias] = {
    use crate::ClientConfigAlias as A;
    &[
        A {
            alias: "compression.codec",
            accepted: "compression.type",
        },
        A {
            alias: "fetch.message.max.bytes",
            accepted: "max.partition.fetch.bytes",
        },
        A {
            alias: "max.in.flight",
            accepted: "max.in.flight.requests.per.connection",
        },
        A {
            alias: "message.send.max.retries",
            accepted: "retries",
        },
        A {
            alias: "message.timeout.ms",
            accepted: "delivery.timeout.ms",
        },
        A {
            alias: "metadata.broker.list",
            accepted: "bootstrap.servers",
        },
        A {
            alias: "queue.buffering.max.ms",
            accepted: "linger.ms",
        },
        A {
            alias: "request.required.acks",
            accepted: "acks",
        },
        A {
            alias: "sasl.mechanisms",
            accepted: "sasl.mechanism",
        },
        A {
            alias: "sasl.oauthbearer.client.id",
            accepted: "sasl.oauthbearer.client.credentials.client.id",
        },
        A {
            alias: "sasl.oauthbearer.client.secret",
            accepted: "sasl.oauthbearer.client.credentials.client.secret",
        },
    ]
};

/// librdkafka reads a key that names none of its global properties as a property of the default
/// topic configuration, after stripping a leading `topic.`, so that spelling would be a second
/// name for each default-topic property. Its own global properties whose names begin with the
/// prefix, as read from librdkafka 2.12.1's property table, stay what they are.
const LIBRDKAFKA_TOPIC_PREFIX: crate::ClientConfigStrippedPrefix =
    crate::ClientConfigStrippedPrefix {
        prefix: "topic.",
        properties: &[
            "topic.blacklist",
            "topic.metadata.propagation.max.ms",
            "topic.metadata.refresh.fast.cnt",
            "topic.metadata.refresh.fast.interval.ms",
            "topic.metadata.refresh.interval.ms",
            "topic.metadata.refresh.sparse",
        ],
    };

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
pub struct KafkaTopicIngestor {
    pub client: ClientName,
    pub topic: KafkaTopicName,
    pub offset_mode: KafkaOffsetMode,
    pub instances: NonZeroU64,
    pub mode: KafkaIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a KAFKA TOPIC ingestor holds and how long it waits.
const KAFKA_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "a record conversion report after a gathered prefix",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 1,
                    unit: "report per ingestor instance",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
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
const KAFKA_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
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
            bound: WaitBound::Driver("delivery.timeout.ms, 300 s"),
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

/// A Kafka record's position, key, timestamp and null-value state.
const KAFKA_TOPIC_METADATA: IngestorMetadataDeclaration = IngestorMetadataDeclaration::new(&[
    IngestorMetadataField::new("topic", IngestorMetadataType::String),
    IngestorMetadataField::new("partition", IngestorMetadataType::I32),
    IngestorMetadataField::new("offset", IngestorMetadataType::I64),
    IngestorMetadataField::new("key", IngestorMetadataType::Bytes),
    IngestorMetadataField::new("timestamp", IngestorMetadataType::Datetime),
    IngestorMetadataField::new("timestamp_type", IngestorMetadataType::String),
    IngestorMetadataField::new("tombstone", IngestorMetadataType::Bool),
]);

impl IngestorDeclaration for KafkaTopicIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // A Kafka record carries its headers as a list, in the order written, repeats included.
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        metadata: KAFKA_TOPIC_METADATA,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Kafka),
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
        bounds: KAFKA_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for KafkaTopicIngestor {
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

connector_ingestors! {
    connector Kafka = "KAFKA";
    /// What a Kafka source reads.
    pub enum KafkaIngestor;
    /// The alterations of the ingestors that read a Kafka entity.
    pub enum KafkaTopicIngestorAlteration;
    /// The entities a Kafka source reads.
    pub enum KafkaIngestorEntity {
        Topic(KafkaTopicIngestor) = "TOPIC", "Kafka";
    }
}

/// One topic a Kafka client produces to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct KafkaTopicEmitter {
    pub mode: KafkaTopicPublishingMode,
    pub client: ClientName,
    pub topic: KafkaTopicName,
}

impl EmitterDeclaration for KafkaTopicEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        // A Kafka record carries its headers as a list, in the order written, repeats included.
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        reference: EmitterReferenceKind::Client(ConnectorKind::Kafka),
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
        bounds: KAFKA_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for KafkaTopicEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Kafka,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Kafka = "KAFKA";
    /// What a Kafka emitter writes.
    pub enum KafkaEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum KafkaTopicEmitterMode;
    /// The entities a Kafka emitter writes.
    pub enum KafkaEmitterEntity {
        Topic(KafkaTopicEmitter, KafkaTopicPublishingMode) = "TOPIC";
    }
}

super::publishing::emitter_modes! {
    pub enum KafkaTopicPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
        Ack { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => BrokerAck,
    }
}

#[cfg(test)]
#[path = "kafka_tests.rs"]
mod tests;
