//! Observing a relay one unit at a time, wherever the schedule places each unit.
//!
//! Layer: control plane.
//!
//! - **Owns.** Gathering the DESCRIBE metrics of a relay divided into partitions from every node
//!   that owns one of its partitions, and a materialized relay's state from wherever each of its
//!   units is held.
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
use nervix_models::{DomainName, ModelKind, ModelName, RelayName, ScheduledNode};

use super::{super::session_service::SessionServiceImpl, ObservationError};
use crate::runtime::{BranchKey, MaterializedReadError, MaterializedRecordReport};

impl SessionServiceImpl {
    /// The metrics of a relay divided into partitions, gathered from each node that owns one of
    /// its partitions: every owner records only the partitions it owns, and each line names its
    /// partition.
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

    /// The materialized records of every unit of `relay`, in the canonical order of their keys.
    /// Each unit reports its own keys: from this node's copy when it holds one or runs the unit,
    /// otherwise from the node the schedule places the unit's owner on.
    pub(super) async fn materialized_state_of_every_unit(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        relay_node: &ScheduledNode,
    ) -> error_stack::Result<Vec<MaterializedRecordReport>, MaterializedReadError> {
        let local_node_id = self.inner.consensus.local_node_id();
        let mut entries = Vec::new();
        for unit in relay_node.units.keys() {
            let local = self
                .inner
                .runtime
                .local_materialized_stream_state(domain, relay, *unit)
                .await?;
            if !local.is_empty() || relay_node.unit_executes_on(*unit, local_node_id) {
                entries.extend(local);
                continue;
            }
            let Some(owner) = relay_node.unit_execution_node(*unit) else {
                continue;
            };
            let remote = self
                .inner
                .runtime
                .remote_materialized_stream_state(owner, domain, relay, *unit)
                .await?;
            entries.extend(remote);
        }
        entries.sort_by(|left, right| BranchKey::canonical_order(&left.branch, &right.branch));
        Ok(entries)
    }
}
