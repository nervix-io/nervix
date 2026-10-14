//! The vocabulary of the endpoints a cluster serves, as a source reads them.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The endpoint source Model with its delivery mode, and what it declares.
//! - **Depends on.** The shared source declarations and the endpoint name a body references.
//! - **Must not know.** How an endpoint is published, routed to or served, or which protocol a
//!   request reached it over.
//!
//! An endpoint is an HTTP endpoint or a WebSockets endpoint, and only its own Model says which.
//! A source that reads one therefore names the `ENDPOINT` connector and the endpoint itself.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::entity_union::connector_sources;
use crate::{
    EndpointName, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    PositionClass, SourceBody, SourceEntity, SourceMetadataScope, SourceReference,
    SourceReferenceKind,
};

/// One endpoint this cluster serves, whose requests or messages are the payloads. Every node of
/// the cluster serves it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct EndpointEndpointSource {
    pub endpoint: EndpointName,
    pub mode: EndpointIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for EndpointEndpointSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: true,
        reference: SourceReferenceKind::Endpoint,
        position_class: PositionClass::None,
    };
}

impl SourceBody for EndpointEndpointSource {
    fn reference(&self) -> SourceReference<'_> {
        SourceReference::Endpoint(&self.endpoint)
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
            IngestQuiesceMode::EndpointBuffer { .. } | IngestQuiesceMode::Reject { .. }
        )
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EndpointIngestMode {
    NoAckSequential,
}

impl EndpointIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAckSequential => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Endpoint = "ENDPOINT";
    /// What an endpoint source reads.
    pub enum EndpointSource;
    /// The entities an endpoint source reads.
    pub enum EndpointSourceEntity {
        Endpoint(EndpointEndpointSource) = "ENDPOINT", "Endpoint";
    }
}
