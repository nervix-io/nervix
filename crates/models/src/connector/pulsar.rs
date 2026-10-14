//! The Pulsar connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Pulsar client Model, the topic source and sink Models with their delivery
//!   mode, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The Pulsar driver, its configuration keys, or how a topic is consumed or
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
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    KafkaIngestMode, PulsarSubscriptionName, SinkBody, SinkClientReference, SinkEntity,
    SinkReference, SourceBody, SourceEntity, SourceMetadataScope, SourceReference,
    SourceReferenceKind, TopicName,
};

configured_client!(Pulsar: unpooled CreateClientPulsar => PulsarConfigEntry);

/// One topic a Pulsar client consumes through a subscription.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PulsarTopicSource {
    pub client: ClientName,
    pub topic: TopicName,
    pub subscription: PulsarSubscriptionName,
    pub instances: NonZeroU64,
    pub mode: PulsarIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// A Pulsar subscription acknowledges exactly as a Kafka consumer does.
pub type PulsarIngestMode = KafkaIngestMode;

impl SourceEntity for PulsarTopicSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Pulsar),
    };
}

impl SourceBody for PulsarTopicSource {
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

connector_sources! {
    connector Pulsar = "PULSAR";
    /// What a Pulsar source reads.
    pub enum PulsarSource;
    /// The entities a Pulsar source reads.
    pub enum PulsarSourceEntity {
        Topic(PulsarTopicSource) = "TOPIC", "Pulsar";
    }
}

/// One topic a Pulsar client produces to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PulsarTopicSink {
    pub client: ClientName,
    pub topic: TopicName,
}

impl SinkEntity for PulsarTopicSink {
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

impl SinkBody for PulsarTopicSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Pulsar,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Pulsar = "PULSAR";
    /// What a Pulsar sink writes.
    pub enum PulsarSink;
    /// The entities a Pulsar sink writes.
    pub enum PulsarSinkEntity {
        Topic(PulsarTopicSink) = "TOPIC";
    }
}
