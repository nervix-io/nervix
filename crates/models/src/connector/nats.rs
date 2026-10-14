//! The NATS connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The NATS client Model, the subject source and sink Models with their delivery
//!   mode, and what each entity declares.
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
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    QueueGroupName, SinkBody, SinkClientReference, SinkEntity, SinkReference, SourceBody,
    SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind, SubjectName,
};

configured_client!(Nats: unpooled CreateClientNats => NatsConfigEntry);

/// One subject a NATS client subscribes to as a member of a queue group.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NatsSubjectSource {
    pub client: ClientName,
    pub subject: SubjectName,
    pub queue_group: QueueGroupName,
    pub instances: NonZeroU64,
    pub mode: NatsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for NatsSubjectSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Nats),
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

    fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        matches!(
            quiesce,
            IngestQuiesceMode::Buffer { .. } | IngestQuiesceMode::Drop
        )
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NatsIngestMode {
    NoAckSequential,
}

impl NatsIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAckSequential => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Nats = "NATS";
    /// What a NATS source reads.
    pub enum NatsSource;
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
        writes_headers: true,
        requires_codec: true,
        batch_requirement: EmitterBatchRequirement::Optional,
        requires_batch_transformation: false,
        batch_size_maximum: None,
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(
            mode,
            EmitterPublishingMode::NoAck { .. } | EmitterPublishingMode::NatsJetStream { .. }
        )
    }
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
