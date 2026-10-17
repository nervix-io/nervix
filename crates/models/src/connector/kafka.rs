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

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
    external_name::{ExternalNameError, external_names, refuse_control_characters, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm, EmitterPublishingModeKind,
    IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities, LocatorDeclaration,
    LocatorName, OptionChoice, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
    PositionClass, RetryPolicy, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SinkReferenceKind, SourceBody, SourceEntity, SourceMetadataScope, SourceReference,
    SourceReferenceKind, TopicName,
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

impl SourceEntity for KafkaTopicSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
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
    pub client: ClientName,
    pub topic: TopicName,
}

impl SinkEntity for KafkaTopicSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: true,
        reference: SinkReferenceKind::Client(ConnectorKind::Kafka),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: &[
            EmitterPublishingModeKind::NoAck,
            EmitterPublishingModeKind::BrokerAck,
        ],
        locator: Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::Unquoted {
                label: "topic_name",
            },
        }),
        options: &[],
        envelope: &[],
        properties: &[],
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
    /// The entities a Kafka sink writes.
    pub enum KafkaSinkEntity {
        Topic(KafkaTopicSink) = "TOPIC";
    }
}
