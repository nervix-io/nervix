//! The Kafka connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Kafka client Model, the topic ingestor and emitter Models with their offset and
//!   delivery modes, a consumer group's start position and its alteration, the topic and consumer
//!   group names an ingestor reads with the rules Kafka holds them to, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality, and the
//!   one name a client accepts for each librdkafka property.
//! - **Depends on.** The shared ingestor and emitter declarations and the names a body references.
//! - **Must not know.** The Kafka driver, or how a topic is consumed or
//!   produced to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{num::NonZeroU64, time::Duration};

use error_stack::Report;
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
    EnvelopeClauseKind, HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode,
    IngestStartPosition, IngestorDeclaration, IngestorEntityBody, IngestorEntityCapabilities,
    IngestorMetadataDeclaration, IngestorMetadataField, IngestorMetadataType, IngestorReference,
    IngestorReferenceKind, LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration,
    OptionForm, OptionPresence, OptionValue, PositionClass, RetryPolicy,
};

configured_client!(Kafka: unpooled, config CLIENT_CONFIG, CreateClientKafka => KafkaConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace, ClientConfigSchema, ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
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
            "auto.offset.reset",
            "delivery.report.only.error",
            "enable.auto.commit",
            "enable.partition.eof",
            "group.id",
        ],
    )
    .with_statement_keys(&[crate::ClientConfigStatementKey {
        key: "auto.offset.reset",
        clause: "START AT on the consumer group of the KAFKA TOPIC ingestor",
    }])
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

impl KafkaTopicName {
    /// Transfers the validated spelling into a fixed string allocation.
    pub fn into_boxed_str(self) -> Box<str> {
        self.0.into_boxed_str()
    }
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
    pub instances: crate::IngestorInstances,
    pub mode: KafkaIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a KAFKA TOPIC ingestor holds and how long it waits.
const KAFKA_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "an inherited consumer group's unadmitted native partition position",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 1,
                    unit: "position per inherited group reader",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
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
            wait: "a consumer-group offset commit",
            bound: WaitBound::Fixed(Duration::from_secs(30)),
        },
        DeclaredWait {
            wait: "metadata, partition inspection, offset lookups, watermarks and seeks including \
                   executor admission",
            bound: WaitBound::Driver("socket.timeout.ms, 60 s per control job"),
        },
        DeclaredWait {
            wait: "subscribe, assign, pause and unsubscribe",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "destroying a consumer that joined its group, for its LeaveGroup answer",
            bound: WaitBound::Driver("5 s per LeaveGroup request"),
        },
    ],
    driver_limits: &[
        "Kafka clients accept one name per property and refuse Nervix-owned keys. The ingestor \
         supplies group.id, auto.offset.reset and enable.auto.commit, and disables \
         enable.partition.eof and allow.auto.create.topics.",
        "An inherited consumer-group reader disables automatic offset storage and stores only \
         positions in its committed external partition set. It pauses and rewinds an unadmitted \
         partition through connector control execution, retaining its position until that work \
         finishes. The broker confirms that exact unread starting offset before the reader can \
         retire, so even a subsequent START AT LATEST join cannot skip it; replacing the reader \
         after a topology commit admits the partition.",
        "Connector control execution has two workers per node by default. A commit still \
         unanswered after its caller's 30 s deadline retains its worker and the ingestor's one \
         in-flight commit until the native call ends. Native subscribe, assign, pause and \
         unsubscribe have no cancellable deadline; their caller waits socket.timeout.ms at most.",
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
            holds: "PARTITION FROM BRANCH native topic metadata response",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Driver("receive.message.max.bytes 100000000"),
                overflow: BufferOverflow::FailPublish,
            },
        },
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
            wait: "PARTITION FROM BRANCH topic metadata, including connector executor admission",
            bound: WaitBound::Driver("socket.timeout.ms, 60 s; maximum 300 s"),
        },
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
            wait: "a failed publish under ACK, for the reports of the records its purge gave up",
            bound: WaitBound::Clause("ACK TIMEOUT"),
        },
        DeclaredWait {
            wait: "destroying a producer, for its polling thread and the reports of the records \
                   its purge failed",
            bound: WaitBound::Driver("100 ms poll, then a 500 ms flush"),
        },
    ],
    driver_limits: &[
        "Kafka clients accept one name per property and refuse Nervix-owned keys. MODE ACK sets \
         delivery.timeout.ms from ACK TIMEOUT.",
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
/// The external partition coordinate, filled from the connector's native position.
const PARTITION_METADATA: IngestorMetadataField =
    IngestorMetadataField::new("partition", IngestorMetadataType::I32);

const KAFKA_TOPIC_METADATA: IngestorMetadataDeclaration = IngestorMetadataDeclaration::new(&[
    IngestorMetadataField::new("topic", IngestorMetadataType::String),
    PARTITION_METADATA,
    IngestorMetadataField::new("offset", IngestorMetadataType::I64),
    IngestorMetadataField::new("key", IngestorMetadataType::Bytes),
    IngestorMetadataField::new("timestamp", IngestorMetadataType::Datetime),
    IngestorMetadataField::new("timestamp_type", IngestorMetadataType::String),
    IngestorMetadataField::new("tombstone", IngestorMetadataType::Bool),
]);

impl IngestorDeclaration for KafkaTopicIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        external_partitions: crate::IngestorExternalPartitions::Enumerated(PARTITION_METADATA),
        events: crate::DeclaredEvents::NONE,
        instancing: crate::IngestorInstancingCapability::PerPartition,
        // A Kafka record carries its headers as a list, in the order written, repeats included.
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        metadata: KAFKA_TOPIC_METADATA,
        payload: crate::IngestorPayloadDeclaration::BYTES,
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
                // A consumer group's name comes before its start position, as each example writes
                // the option in full.
                form: OptionForm::Choice(&[
                    OptionChoice {
                        name: "DOMAIN",
                        value: None,
                        example: "OFFSET BY DOMAIN",
                    },
                    OptionChoice {
                        name: "CONSUMER GROUP START AT EARLIEST",
                        value: Some(OptionValue::Name(KafkaConsumerGroupName::DECLARATION)),
                        example: "OFFSET BY CONSUMER GROUP 'readers' START AT EARLIEST",
                    },
                    OptionChoice {
                        name: "CONSUMER GROUP START AT LATEST",
                        value: Some(OptionValue::Name(KafkaConsumerGroupName::DECLARATION)),
                        example: "OFFSET BY CONSUMER GROUP 'readers' START AT LATEST",
                    },
                ]),
            },
            OptionDeclaration::INSTANCES,
        ],
        bounds: KAFKA_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for KafkaTopicIngestor {
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
pub enum KafkaOffsetMode {
    /// `CONSUMER GROUP '<group>' START AT EARLIEST|LATEST`: the group's offsets on the broker own
    /// the positions, and a partition without a usable committed offset starts at `start_at`.
    ConsumerGroup {
        group: KafkaConsumerGroupName,
        start_at: IngestStartPosition,
    },
    /// `DOMAIN`: the domain's offset records own the positions, and the domain's start point
    /// places every partition, so this mode has no start position of its own.
    Domain,
}

impl KafkaOffsetMode {
    fn set_start_at(
        &mut self,
        position: IngestStartPosition,
    ) -> error_stack::Result<(), crate::AlterIngestorError> {
        match self {
            Self::ConsumerGroup { start_at, .. } => *start_at = position,
            Self::Domain => {
                return Err(Report::new(
                    crate::AlterIngestorError::DomainKafkaStartPosition,
                ));
            }
        }
        Ok(())
    }
}

/// One change an `ALTER KAFKA TOPIC INGESTOR` writes to the body of the ingestor it changes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum KafkaTopicIngestorChange {
    /// `SET TOPIC '<topic>'`.
    Topic(KafkaTopicName),
    /// `SET OFFSET BY DOMAIN | CONSUMER GROUP '<group>' START AT EARLIEST|LATEST`.
    OffsetBy(KafkaOffsetMode),
    /// `SET START AT EARLIEST|LATEST`: where a consumer group starts, its group kept.
    StartAt(IngestStartPosition),
    /// `SET MODE <mode>`.
    Mode(KafkaIngestMode),
}

impl crate::IngestorEntityChange for KafkaTopicIngestorChange {
    type Entity = KafkaTopicIngestor;

    fn apply_to(
        &self,
        entity: &mut KafkaTopicIngestor,
    ) -> error_stack::Result<(), crate::AlterIngestorError> {
        match self {
            Self::Topic(topic) => entity.topic = topic.clone(),
            Self::OffsetBy(offset_mode) => entity.offset_mode = offset_mode.clone(),
            Self::StartAt(position) => entity.offset_mode.set_start_at(*position)?,
            Self::Mode(mode) => entity.mode = mode.clone(),
        }
        Ok(())
    }

    fn to_canonical_plexus(&self) -> error_stack::Result<String, crate::CanonicalPlexusError> {
        let text = match self {
            Self::Topic(topic) => format!("SET TOPIC {}", crate::string_literal(topic.as_str())),
            Self::OffsetBy(offset_mode) => {
                format!("SET OFFSET BY {}", offset_mode.to_canonical_plexus())
            }
            Self::StartAt(position) => format!("SET START AT {position}"),
            Self::Mode(mode) => {
                format!("SET MODE {}", mode.acknowledgement().to_canonical_plexus())
            }
        };
        Ok(text)
    }
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
    /// What a Kafka ingestor reads.
    pub enum KafkaIngestor;
    /// The alterations of the ingestors that read a Kafka entity.
    pub enum KafkaTopicIngestorAlteration;
    /// The entities a Kafka ingestor reads.
    pub enum KafkaIngestorEntity {
        Topic(KafkaTopicIngestor => KafkaTopicIngestorChange) = "TOPIC", "Kafka";
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
        events: crate::DeclaredEvents::NONE,
        instancing: crate::EmitterInstancingCapability::Instances,
        // A Kafka record carries its headers as a list, in the order written, repeats included.
        headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
        reference: EmitterReferenceKind::Client(ConnectorKind::Kafka),
        body: EmitterBodyForm::Codec,
        payload: super::EmitterPayloadFormat::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: KafkaTopicPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(KafkaTopicName::DECLARATION),
        }),
        options: &[],
        // A Kafka record carries its create time beside its key, headers and payload. It has no
        // persistence, routing or retention flag, no delayed delivery, no reply target and no
        // per-record message or deduplication identity, and its headers carry what properties
        // would.
        envelope: &[
            EnvelopeClauseKind::Key,
            EnvelopeClauseKind::PartitionFromBranch,
            EnvelopeClauseKind::Timestamp,
        ],
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

/// One change an `ALTER KAFKA TOPIC EMITTER` writes to the body of the emitter it changes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum KafkaTopicEmitterChange {
    /// `SET TOPIC '<name>'`.
    Topic(KafkaTopicName),
    /// `SET MODE <mode>`.
    Mode(KafkaTopicPublishingMode),
}

impl crate::EmitterEntityChange for KafkaTopicEmitterChange {
    type Entity = KafkaTopicEmitter;

    fn apply_to(
        &self,
        entity: &mut KafkaTopicEmitter,
    ) -> error_stack::Result<(), crate::AlterEmitterError> {
        match self {
            Self::Topic(name) => entity.topic = name.clone(),
            Self::Mode(mode) => entity.mode = mode.clone(),
        }
        Ok(())
    }

    fn to_canonical_plexus(&self) -> error_stack::Result<String, crate::CanonicalPlexusError> {
        let text = match self {
            Self::Topic(name) => format!("SET TOPIC {}", crate::string_literal(name.as_str())),
            Self::Mode(mode) => format!("SET MODE {}", mode.publishing().to_canonical_plexus()),
        };
        Ok(text)
    }
}

connector_emitters! {
    connector Kafka = "KAFKA";
    /// What a Kafka emitter writes.
    pub enum KafkaEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum KafkaTopicEmitterMode;
    /// One change to the body of one of this connector's emitter entities.
    pub enum KafkaEmitterChange;
    /// The entities a Kafka emitter writes.
    pub enum KafkaEmitterEntity {
        Topic(KafkaTopicEmitter, KafkaTopicPublishingMode, KafkaTopicEmitterChange) = "TOPIC";
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
