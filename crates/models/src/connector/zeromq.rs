//! The ZeroMQ connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The ZeroMQ client Model, the pull source and push sink Models with their delivery
//!   mode, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The ZeroMQ driver, its configuration keys, or how a socket is bound,
//!   connected, read or written.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    PositionClass, SinkBody, SinkClientReference, SinkEntity, SinkReference, SourceBody,
    SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind,
};

configured_client!(ZeroMq: unpooled CreateClientZeroMq => ZeroMqConfigEntry);

/// The pull socket of a ZeroMQ client, which the client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ZeroMqPullSource {
    pub client: ClientName,
    pub mode: ZeroMqIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for ZeroMqPullSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: false,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::ZeroMq),
        position_class: PositionClass::None,
    };
}

impl SourceBody for ZeroMqPullSource {
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
            IngestQuiesceMode::Suspend | IngestQuiesceMode::Buffer { .. } | IngestQuiesceMode::Drop
        )
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ZeroMqIngestMode {
    NoAckSequential,
}

impl ZeroMqIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAckSequential => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector ZeroMq = "ZEROMQ";
    /// What a ZeroMQ source reads.
    pub enum ZeroMqSource;
    /// The entities a ZeroMQ source reads.
    pub enum ZeroMqSourceEntity {
        Pull(ZeroMqPullSource) = "PULL", "ZeroMQ";
    }
}

/// The push socket of a ZeroMQ client, which the client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ZeroMqPushSink {
    pub client: ClientName,
}

impl SinkEntity for ZeroMqPushSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        requires_codec: true,
        batch_requirement: EmitterBatchRequirement::Optional,
        requires_batch_transformation: false,
        batch_size_maximum: None,
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(mode, EmitterPublishingMode::NoAck { .. })
    }
}

impl SinkBody for ZeroMqPushSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::ZeroMq,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector ZeroMq = "ZEROMQ";
    /// What a ZeroMQ sink writes.
    pub enum ZeroMqSink;
    /// The entities a ZeroMQ sink writes.
    pub enum ZeroMqSinkEntity {
        Push(ZeroMqPushSink) = "PUSH";
    }
}
