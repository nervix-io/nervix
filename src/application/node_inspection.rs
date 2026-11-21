//! Cluster node inspection from the answering node's local observations.
//!
//! Layer: control plane.
//! - **Owns.** Capturing observed membership and committed facts, applying a node selector and
//!   reporting the answer in its requested presentation.
//! - **Depends on.** The consensus observer, cluster availability and the pure selector decision.
//! - **Must not know.** Transport framing, the membership planner or placement decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "observes cluster nodes for a session command")
)]

use std::collections::{BTreeMap, BTreeSet};

use error_stack::{Report, ResultExt as _};
use nervix_consensus::{CommittedNodeObservations, GossipState};
use nervix_execution::{CpuClass, MemoryClass};
use nervix_models::{
    ClusterNodeName, CommittedNodeRecord, InspectionFormat, NodeInspection, NodeInspectionRow,
    ShowNodes,
};
use thiserror::Error;

use super::{
    SessionServiceImpl,
    cluster_description::{property_lines, tag_lines},
    command_result::{CommandDiagnostic, CommandResult},
    model_mutation::{command_error, command_ok},
};
use crate::registry::node_selector::NodeSelector;

/// What one answering node observes for a preview.
struct NodeObservations {
    names: BTreeSet<ClusterNodeName>,
    live: BTreeSet<ClusterNodeName>,
    voters: BTreeSet<ClusterNodeName>,
    cordoned: BTreeSet<ClusterNodeName>,
    terminating: BTreeSet<ClusterNodeName>,
    records: BTreeMap<ClusterNodeName, CommittedNodeRecord>,
}

#[derive(Debug, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "a node inspection can refuse the request without altering the cluster"
    )
)]
enum NodeInspectionError {
    #[error("node inspection could not enter bulk execution")]
    Admission,
    #[error("node inspection was cancelled")]
    Cancelled,
    #[error("could not compile the node selector")]
    Selector,
    #[error("could not evaluate the node selector")]
    Evaluation,
}

impl SessionServiceImpl {
    pub(super) async fn show_nodes(&self, show: ShowNodes) -> CommandResult {
        match NodeObservations::inspect(self, show).await {
            Ok(result) => result,
            Err(error) => command_error(format!("{error:#}")),
        }
    }
}

impl NodeObservations {
    async fn inspect(
        service: &SessionServiceImpl,
        show: ShowNodes,
    ) -> Result<CommandResult, Report<NodeInspectionError>> {
        let executor = service.inner.runtime.executor();
        let reservation = executor
            .try_reserve(
                MemoryClass::Bulk,
                executor.limits().bulk_chunk_bytes.as_u64(),
            )
            .change_context(NodeInspectionError::Admission)?;
        let consensus = service.inner.consensus.clone();
        let availability = service.inner.cluster.availability_state().await;
        executor
            .run_cpu(CpuClass::Bulk, reservation, move |_, cancellation| {
                let selector = show
                    .selector
                    .as_ref()
                    .map(NodeSelector::compile)
                    .transpose()
                    .change_context(NodeInspectionError::Selector)?;
                if cancellation.is_cancelled() {
                    return Err(Report::new(NodeInspectionError::Cancelled));
                }
                let observations =
                    Self::capture(availability, consensus.committed_node_observations());
                observations.render(selector.as_ref(), show.format)
            })
            .await
            .change_context(NodeInspectionError::Admission)?
    }

    fn capture(availability: GossipState, committed: CommittedNodeObservations) -> Self {
        let CommittedNodeObservations {
            members: mut names,
            voters,
            cordoned,
            records,
        } = committed;
        let live = availability.live_node_ids();
        let terminating = availability
            .latest_nodes_by_id()
            .into_iter()
            .filter(|(name, run)| run.terminating && live.contains(name))
            .map(|(name, _)| name)
            .collect();
        names.extend(live.iter().cloned());
        names.extend(records.keys().cloned());
        Self {
            names,
            live,
            voters,
            cordoned,
            terminating,
            records,
        }
    }

    fn render(
        mut self,
        selector: Option<&NodeSelector>,
        format: InspectionFormat,
    ) -> Result<CommandResult, Report<NodeInspectionError>> {
        let mut diagnostics = Vec::new();
        let names = if let Some(selector) = selector {
            let selection = selector
                .evaluate(&self.records)
                .change_context(NodeInspectionError::Evaluation)?;
            for failure in selection.failures {
                for error in failure.errors {
                    diagnostics.push(CommandDiagnostic::unlocated(format!(
                        "node selector failed on '{}': {}",
                        failure.node, error.reason
                    )));
                }
            }
            selection.matches
        } else {
            self.names
        };
        let mut nodes = Vec::with_capacity(names.len());
        for name in names {
            let record = self.records.remove(&name).map(Into::into);
            nodes.push(NodeInspectionRow {
                live: self.live.contains(&name),
                voter: self.voters.contains(&name),
                cordoned: self.cordoned.contains(&name),
                terminating: self.terminating.contains(&name),
                name,
                record,
            });
        }
        let inspection = NodeInspection { nodes };
        let message = match format {
            InspectionFormat::Json => nervix_client_wire::node_inspection_json(&inspection),
            InspectionFormat::Text => Self::text(&inspection),
        };
        let mut result = command_ok(message);
        result.diagnostics = diagnostics;
        result.node_inspection = Some(Box::new(inspection));
        Ok(result)
    }

    fn text(inspection: &NodeInspection) -> String {
        if inspection.nodes.is_empty() {
            return "no matching nodes".to_string();
        }
        let mut lines = Vec::new();
        for node in &inspection.nodes {
            lines.push(format!(
                "{} live={} voter={} cordoned={} terminating={}",
                node.name, node.live, node.voter, node.cordoned, node.terminating
            ));
            let Some(record) = &node.record else {
                lines.push("  record: not announced".to_string());
                continue;
            };
            lines.push(format!("  record: revision {}", record.revision));
            if let Some(count) = record.cpu_count {
                lines.push(format!("  cpu.count: {count}"));
            }
            if let Some(limit) = record.memory_limit {
                lines.push(format!("  memory.limit: {limit}"));
            }
            lines.push("  properties:".to_string());
            lines.extend(property_lines(&record.properties));
            lines.extend(tag_lines(&record.tags));
        }
        lines.join("\n")
    }
}
