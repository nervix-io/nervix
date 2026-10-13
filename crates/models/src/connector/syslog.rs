//! The syslog connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The syslog client Model, the listener source and collector sink Models, and what
//!   each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** Syslog framing, its configuration keys, or how a listener is bound or a
//!   collector is written to.

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
    SinkBody, SinkEntity, SinkReference, SourceBody, SourceEntity, SourceMetadataScope,
    SourceReference, SourceReferenceKind,
};

configured_client!(Syslog: unpooled CreateClientSyslog => SyslogConfigEntry);

/// The listener a syslog client's configuration binds, which every node of the cluster serves.
/// Its delivery mode is always `NO_ACK SEQUENTIAL`, so the Model carries none.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyslogListenerSource {
    pub client: ClientName,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for SyslogListenerSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: false,
        metadata: SourceMetadataScope::Syslog,
        executes_on_every_cluster_node: true,
        reference: SourceReferenceKind::Client(ConnectorKind::Syslog),
    };
}

impl SourceBody for SyslogListenerSource {
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
        IngestAcknowledgement::Unacknowledged
    }
}

connector_sources! {
    connector Syslog = "SYSLOG";
    /// What a syslog source reads.
    pub enum SyslogSource;
    /// The entities a syslog source reads.
    pub enum SyslogSourceEntity {
        Listener(SyslogListenerSource) = "LISTENER", "Syslog";
    }
}

/// The collector a syslog client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyslogCollectorSink {
    pub client: ClientName,
}

impl SinkEntity for SyslogCollectorSink {
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

impl SinkBody for SyslogCollectorSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client {
            client: &self.client,
            connector: ConnectorKind::Syslog,
        }
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Syslog = "SYSLOG";
    /// What a syslog sink writes.
    pub enum SyslogSink;
    /// The entities a syslog sink writes.
    pub enum SyslogSinkEntity {
        Collector(SyslogCollectorSink) = "COLLECTOR";
    }
}
