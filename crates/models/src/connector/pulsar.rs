//! The Pulsar connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Pulsar client Model, the topic source and sink Models with their delivery
//!   mode, the topic and subscription names a source reads with the rules Pulsar holds them to,
//!   and what each entity declares.
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
    external_name::{ExternalNameError, external_names, refuse_control_characters, require_length},
};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    KafkaIngestMode, SinkBody, SinkClientReference, SinkEntity, SinkReference, SourceBody,
    SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind, TopicName,
};

configured_client!(Pulsar: unpooled CreateClientPulsar => PulsarConfigEntry);

/// The longest topic Nervix consumes, in bytes.
const PULSAR_TOPIC_MAX: usize = 1024;
/// The longest subscription Nervix consumes through, in bytes.
const PULSAR_SUBSCRIPTION_MAX: usize = 255;
/// The domains a qualified topic may name, which Pulsar defines.
const PULSAR_TOPIC_DOMAINS: [&str; 2] = ["persistent", "non-persistent"];
/// The levels a qualified topic names after its domain: its tenant, its namespace and itself.
const PULSAR_QUALIFIED_TOPIC_LEVELS: [&str; 3] = ["tenant", "namespace", "topic"];

external_names! {
    /// A Pulsar topic: a local name, which the connector places in its client's namespace, or a
    /// qualified `persistent://tenant/namespace/topic` or `non-persistent://tenant/namespace/topic`;
    /// at most 1,024 bytes without whitespace or control characters, and with no empty level.
    pub struct PulsarTopicName => pulsar_topic_rule;
    /// A Pulsar subscription: 1 to 255 bytes without control characters.
    pub struct PulsarSubscriptionName => pulsar_subscription_rule;
}

fn pulsar_topic_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, PULSAR_TOPIC_MAX)?;
    if let Some(ch) = text
        .chars()
        .find(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    let Some((domain, path)) = text.split_once("://") else {
        // The connector places a local name in its client's namespace, so a local name is one
        // level.
        if text.contains('/') {
            return Err(ExternalNameError::LevelsWithoutDomain {
                separator: '/',
                known: &PULSAR_TOPIC_DOMAINS,
            });
        }
        return Ok(());
    };
    // The scan reads the two domains Pulsar defines, a fixed array.
    if !PULSAR_TOPIC_DOMAINS.contains(&domain) {
        return Err(ExternalNameError::UnknownDomain {
            domain: domain.to_string(),
            known: &PULSAR_TOPIC_DOMAINS,
        });
    }
    if path.split('/').any(str::is_empty) {
        return Err(ExternalNameError::EmptyLevel { separator: '/' });
    }
    let levels = path.split('/').count();
    if levels != PULSAR_QUALIFIED_TOPIC_LEVELS.len() {
        return Err(ExternalNameError::QualifiedLevels {
            levels,
            expected: &PULSAR_QUALIFIED_TOPIC_LEVELS,
        });
    }
    Ok(())
}

fn pulsar_subscription_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, PULSAR_SUBSCRIPTION_MAX)?;
    refuse_control_characters(text)
}

/// One topic a Pulsar client consumes through a subscription.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PulsarTopicSource {
    pub client: ClientName,
    pub topic: PulsarTopicName,
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
    /// The alterations of the ingestors that read a Pulsar entity.
    pub enum PulsarIngestorAlteration;
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
