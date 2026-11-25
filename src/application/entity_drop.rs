//! What the nodes discarded when they destroyed the entities a committed change dropped, as the
//! change's coordinator collects it and the statement's answer reports it.
//!
//! Layer: control plane.
//! - **Owns.** Answering the coordinator's drop report request only while it leads, collecting
//!   every live node's report of a change's runtime revision, and the answer's text for each
//!   dropped entity.
//! - **Depends on.** The drop report wire contract, consensus leadership, cluster liveness and the
//!   runtime's drop ledger.
//! - **Must not know.** How a node destroys an entity or what each kind holds.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::{BTreeMap, BTreeSet};

use error_stack::Report;
use nervix_interconnect::{
    HandlerRegistrationError, RemoteOperationFailure, RemoteOperationSubject, Transport,
    entity_drop::{DropReportAnswer, DropReportRequest},
};
use nervix_models::{ClusterNodeName, DiscardedWork, DiscardedWorkKind, DomainName, NodeRef};
use nervix_primitives::time::{Instant, sleep};

use crate::application::session_service::SessionServiceImpl;

fn not_from_the_leader(domain: &DomainName) -> RemoteOperationFailure {
    RemoteOperationFailure::failed(
        RemoteOperationSubject::domain(domain),
        "the drop report was asked by a node that is not the current leader".to_string(),
    )
}

/// What the live nodes discarded for the entities one change dropped, and the nodes whose
/// discards are not known.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(in crate::application) struct DropReport {
    /// Every entity's discarded work by kind, summed over the nodes that reported.
    discarded: BTreeMap<NodeRef, BTreeMap<DiscardedWorkKind, u64>>,
    /// The nodes that did not report: they left, did not answer in time, or restarted after
    /// applying the change.
    unreported: BTreeSet<ClusterNodeName>,
}

impl DropReport {
    /// Adds what one node discarded.
    fn add(&mut self, discarded: Vec<DiscardedWork>) {
        for work in discarded {
            let entity = NodeRef::new(work.entity_kind, work.entity);
            let total = self
                .discarded
                .entry(entity)
                .or_default()
                .entry(work.kind)
                .or_default();
            *total = nervix_primitives::expect_lint!(
                nervix::clamped_arithmetic,
                "an operator reads the sum of the counts the nodes reported, and a sum past \
                 u64::MAX reads as the most the answer can say",
                total.saturating_add(work.items)
            );
        }
    }

    /// The answer's clause for `entity`, which the change dropped.
    pub(in crate::application) fn entity_text(&self, entity: &NodeRef) -> String {
        let name = format!("{} '{}'", entity.kind.as_str(), entity.identifier.as_str());
        let mut items = Vec::new();
        if let Some(kinds) = self.discarded.get(entity) {
            for (kind, count) in kinds {
                items.push(format!("{} ({count})", kind.describe()));
            }
        }
        let discarded = if items.is_empty() {
            "nothing".to_string()
        } else {
            items.join(", ")
        };
        let mut text = format!("{name} discarded {discarded}, state removed");
        if !self.unreported.is_empty() {
            let nodes = self
                .unreported
                .iter()
                .map(ClusterNodeName::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            text.push_str(&format!(" (not reported by {nodes})"));
        }
        text
    }
}

impl SessionServiceImpl {
    pub(in crate::application) fn register_entity_drop_handlers(
        &self,
        interconnect: &Transport,
    ) -> Result<(), Report<HandlerRegistrationError>> {
        let service = self.clone();
        interconnect.register_handler::<DropReportRequest, _, _>(move |context, request| {
            let service = service.clone();
            let peer = context.peer_node_id().clone();
            async move { service.handle_drop_report_request(&peer, request).await }
        })?;
        Ok(())
    }

    /// Answers what this node discarded for the request's domain from its runtime revision on,
    /// only to the current leader.
    pub(super) async fn handle_drop_report_request(
        &self,
        peer: &ClusterNodeName,
        request: DropReportRequest,
    ) -> Result<DropReportAnswer, RemoteOperationFailure> {
        if self.inner.consensus.current_leader().await.as_ref() != Some(peer) {
            return Err(not_from_the_leader(&request.domain));
        }
        Ok(self
            .inner
            .runtime
            .drop_report(&request.domain, request.revision))
    }

    /// Collects what every live node discarded for `domain` when it applied the runtime
    /// `revision` that dropped entities. A node that has not applied it yet is asked again until
    /// the domain drain timeout passes; one that does not report by then, has left or restarted
    /// since is named as not reporting.
    pub(in crate::application) async fn collect_drop_report(
        &self,
        domain: &DomainName,
        revision: u64,
    ) -> DropReport {
        let mut report = DropReport::default();
        let Ok(coordination) = self.inner.interconnect.next_coordination_identity() else {
            report.unreported.extend(self.cut_live_nodes().await);
            return report;
        };
        let deadline = Instant::now() + self.cluster_parameters().domain_drain_timeout();
        let mut waiting = self.cut_live_nodes().await;
        loop {
            nervix_primitives::task::consume_budget().await;
            let live = self.cut_live_nodes().await;
            let mut still_waiting = BTreeSet::new();
            for node in waiting {
                nervix_primitives::task::consume_budget().await;
                if !live.contains(&node) {
                    report.unreported.insert(node);
                    continue;
                }
                let request = DropReportRequest {
                    coordination: coordination.clone(),
                    domain: domain.clone(),
                    revision,
                };
                let answer = if &node == self.inner.consensus.local_node_id() {
                    self.handle_drop_report_request(&node, request).await
                } else {
                    match self.inner.interconnect.request(&node, request).await {
                        Ok(answer) => answer,
                        Err(_) => {
                            still_waiting.insert(node);
                            continue;
                        }
                    }
                };
                match answer {
                    Ok(DropReportAnswer::Reported(discarded)) => report.add(discarded),
                    Ok(DropReportAnswer::Unknown) => {
                        report.unreported.insert(node);
                    }
                    Ok(DropReportAnswer::Pending) | Err(_) => {
                        still_waiting.insert(node);
                    }
                }
            }
            if still_waiting.is_empty() {
                return report;
            }
            if Instant::now() >= deadline {
                report.unreported.extend(still_waiting);
                return report;
            }
            waiting = still_waiting;
            sleep(self.cluster_parameters().domain_drain_poll_interval()).await;
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{ModelKind, ModelName};

    use super::*;

    fn named(name: &str) -> ModelName {
        ModelName::parse(name).assured("the test entity name is valid")
    }

    fn node(name: &str) -> ClusterNodeName {
        ClusterNodeName::parse(name).assured("the test node name is valid")
    }

    #[test]
    fn sums_each_kind_over_the_nodes_and_names_what_did_not_report() {
        let mut report = DropReport::default();
        let emitter = NodeRef::new(ModelKind::Emitter, named("published"));
        report.add(vec![DiscardedWork {
            entity_kind: ModelKind::Emitter,
            entity: named("published"),
            kind: DiscardedWorkKind::UndeliveredRecords,
            items: 2,
        }]);
        report.add(vec![
            DiscardedWork {
                entity_kind: ModelKind::Emitter,
                entity: named("published"),
                kind: DiscardedWorkKind::UndeliveredRecords,
                items: 3,
            },
            DiscardedWork {
                entity_kind: ModelKind::Emitter,
                entity: named("published"),
                kind: DiscardedWorkKind::UnprocessedMessages,
                items: 1,
            },
        ]);
        assert_eq!(
            report.entity_text(&emitter),
            "emitter 'published' discarded undelivered records (5), unprocessed messages (1), \
             state removed"
        );

        report.unreported.insert(node("node-3"));
        let deduplicator = NodeRef::new(ModelKind::Deduplicator, named("dedup_txns"));
        assert_eq!(
            report.entity_text(&deduplicator),
            "deduplicator 'dedup_txns' discarded nothing, state removed (not reported by node-3)"
        );
    }
}
