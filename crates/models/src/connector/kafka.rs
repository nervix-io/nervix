//! The Kafka connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Kafka client Model, the topic source and sink Models with their offset and
//!   delivery modes, and what each entity declares.
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
};
use crate::{
    ClientName, ConnectorKind, ConsumerGroupName, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    PositionClass, RetryPolicy, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SourceBody, SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind, TopicName,
};

configured_client!(Kafka: unpooled CreateClientKafka => KafkaConfigEntry);

/// One topic a Kafka client consumes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct KafkaTopicSource {
    pub client: ClientName,
    pub topic: TopicName,
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

    fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        matches!(quiesce, IngestQuiesceMode::Suspend)
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum KafkaOffsetMode {
    ConsumerGroup(ConsumerGroupName),
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
    NoAckParallel,
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
            Self::NoAckParallel => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Kafka = "KAFKA";
    /// What a Kafka source reads.
    pub enum KafkaSource;
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
        requires_codec: true,
        batch_requirement: EmitterBatchRequirement::Optional,
        requires_batch_transformation: false,
        batch_size_maximum: None,
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(
            mode,
            EmitterPublishingMode::NoAck { .. } | EmitterPublishingMode::BrokerAck { .. }
        )
    }
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
