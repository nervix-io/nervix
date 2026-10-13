//! The HTTP connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The HTTP client Model, the polled request source and the request sink Models, and
//!   what each entity declares.
//! - **Depends on.** The shared source and sink declarations, expressions, the domain clock's
//!   period and the names a body references.
//! - **Must not know.** The HTTP client library, its configuration keys, or how a request is
//!   built, sent or classified.

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
    ClientName, ConnectorKind, DomainClockPeriod, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, Expression, IngestAcknowledgement, IngestQuiesceMode,
    IngestSourceCapabilities, SinkBody, SinkEntity, SinkReference, SourceBody, SourceEntity,
    SourceMetadataScope, SourceReference, SourceReferenceKind,
};

configured_client!(Http: unpooled CreateClientHttp => HttpConfigEntry);

/// The request an HTTP client sends on a cadence of domain time, whose response is the payload.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HttpRequestSource {
    pub client: ClientName,
    pub every: DomainClockPeriod,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for HttpRequestSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Http),
    };
}

impl SourceBody for HttpRequestSource {
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
            IngestQuiesceMode::Suspend | IngestQuiesceMode::Buffer { .. }
        )
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        IngestAcknowledgement::Unacknowledged
    }
}

connector_sources! {
    connector Http = "HTTP";
    /// What an HTTP source reads.
    pub enum HttpSource;
    /// The entities an HTTP source reads.
    pub enum HttpSourceEntity {
        Request(HttpRequestSource) = "REQUEST", "HTTP polling";
    }
}

/// The request an HTTP client sends for every published payload, with the method and path each
/// one computes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HttpRequestSink {
    pub client: ClientName,
    pub method: Expression,
    pub path: Expression,
}

impl SinkEntity for HttpRequestSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: true,
        requires_codec: false,
        batch_requirement: EmitterBatchRequirement::Optional,
        requires_batch_transformation: false,
        batch_size_maximum: None,
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(mode, EmitterPublishingMode::RequestAck { .. })
    }
}

impl SinkBody for HttpRequestSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client {
            client: &self.client,
            connector: ConnectorKind::Http,
        }
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Http = "HTTP";
    /// What an HTTP sink writes.
    pub enum HttpSink;
    /// The entities an HTTP sink writes.
    pub enum HttpSinkEntity {
        Request(HttpRequestSink) = "REQUEST";
    }
}
