//! The Prometheus connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Prometheus client Model, the query source Model with its delivery mode, and
//!   what it declares.
//! - **Depends on.** The shared source declarations, the domain clock's period and the names a
//!   body references.
//! - **Must not know.** The Prometheus HTTP API, its configuration keys, or how a query is
//!   evaluated on its cadence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{configured_client::configured_client, entity_union::connector_sources};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DomainClockPeriod,
    IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities, PositionClass, SourceBody,
    SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind,
};

configured_client!(Prometheus: unpooled CreateClientPrometheus => PrometheusConfigEntry);

/// One PromQL query a Prometheus client evaluates on a cadence of domain time.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PrometheusQuerySource {
    pub client: ClientName,
    pub query: String,
    pub every: DomainClockPeriod,
    pub mode: PrometheusIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for PrometheusQuerySource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: false,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Prometheus),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
    };
}

impl SourceBody for PrometheusQuerySource {
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
        self.mode.acknowledgement()
    }
}

/// How a query's results are delivered. A result is read again on the next evaluation, so none
/// waits on an acknowledgement.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PrometheusIngestMode {
    NoAck,
}

impl PrometheusIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Prometheus = "PROMETHEUS";
    /// What a Prometheus source reads.
    pub enum PrometheusSource;
    /// The alterations of the ingestors that read a Prometheus entity.
    pub enum PrometheusIngestorAlteration;
    /// The entities a Prometheus source reads.
    pub enum PrometheusSourceEntity {
        Query(PrometheusQuerySource) = "QUERY", "Prometheus";
    }
}
