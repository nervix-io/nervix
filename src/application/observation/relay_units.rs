//! Observing partitioned runtime nodes and materialized relays at their unit owners.
//!
//! Layer: control plane.
//!
//! - **Owns.** Gathering DESCRIBE metrics and WASM checkpoint facts from every partition owner,
//!   and a materialized relay's state from wherever each of its units is held.
//! - **Depends on.** The schedule's unit placements, the runtime's local and remote materialized
//!   reads, and the describe requests its parent module sends.
//! - **Must not know.** How a unit's owner routes, buffers or applies its batches.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use nervix_interconnect::DescribeMetricsEnvelope;
use nervix_models::{DomainName, ModelKind, ModelName, ScheduledNode};

use super::{super::session_service::SessionServiceImpl, ObservationError};

impl SessionServiceImpl {
    /// Metrics and checkpoint facts gathered once per distinct owner. Each owner reports only the
    /// units it executes, including when it also keeps replicas of sibling units.
    pub(super) async fn describe_partitioned_runtime(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: ModelName,
        node: &ScheduledNode,
    ) -> error_stack::Result<DescribeMetricsEnvelope, ObservationError> {
        let metric_kind = kind.as_str().to_ascii_uppercase();
        let local_node_id = self.inner.consensus.local_node_id();
        let mut described = DescribeMetricsEnvelope {
            metrics: Vec::new(),
            checkpoints: Vec::new(),
        };
        for owner in node.execution_nodes() {
            let owned = if &owner == local_node_id {
                self.local_runtime_describe(domain, kind, identifier.clone(), &metric_kind)
            } else {
                self.remote_runtime_describe(domain, kind, identifier.clone(), &owner)
                    .await?
            };
            described.metrics.extend(owned.metrics);
            described.checkpoints.extend(owned.checkpoints);
        }
        Ok(described)
    }
}
