//! The ingestor host's retained interaction counters.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One registration of the ingestor host's existing counter children.
//! - **Depends on.** Vocabulary identities and the node metric registry.
//! - **Must not know.** Connector drivers, intake lifetimes, decoding policy or acknowledgements.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the infrastructure boundary declared above")
)]

use nervix_models::{ClusterNodeName, DomainName, IngestorName, Unit};
use prometheus::IntCounter;

use super::RuntimeMetrics;

/// Native children kept by value, with the same ownership and recording cost as individual fields.
#[derive(Debug, Clone)]
pub(crate) struct IngestorInteractionMetrics {
    /// Unacknowledged payloads refused because the extension workers had no room to unfold them.
    pub(crate) unfolding_refused: IntCounter,
    /// Positions discarded after a reconnect because their broker redelivers them.
    pub(crate) transport_redeliveries: IntCounter,
    /// Payloads dropped because they exceed the client's message-size bound.
    pub(crate) oversized_payloads: IntCounter,
    /// Invalid UTF-8 header values skipped by polls and the host's endpoint bindings.
    pub(crate) header_values_dropped: IntCounter,
}

impl RuntimeMetrics {
    /// Decode rejection has its own shared policy owner, which takes its child by value.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves the ingestor host's children once at startup"
        )
    )]
    pub(crate) fn register_ingestor_interactions(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        unit: Unit,
        physical_node: Option<&ClusterNodeName>,
    ) -> (IngestorInteractionMetrics, IntCounter) {
        let interactions = IngestorInteractionMetrics {
            unfolding_refused: self.ingestor_unfolding_refused(
                domain,
                ingestor,
                unit,
                physical_node,
            ),
            transport_redeliveries: self.ingestor_transport_redeliveries(
                domain,
                ingestor,
                unit,
                physical_node,
            ),
            oversized_payloads: self.ingestor_oversized_payloads(
                domain,
                ingestor,
                unit,
                physical_node,
            ),
            header_values_dropped: self.ingestor_header_values_dropped(
                domain,
                ingestor,
                unit,
                physical_node,
            ),
        };
        let decode_rejections =
            self.ingestor_decode_rejections(domain, ingestor, unit, physical_node);
        (interactions, decode_rejections)
    }
}
