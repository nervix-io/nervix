//! Node-scoped holds and queries of the application progress a revision barrier reads from other
//! nodes.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The hold of every revision-query reply a node answers and of every progress report
//!   a node sends, until a scenario releases it, and the scenario's own revision queries, which a
//!   chosen node sends as its barriers do.
//! - **Depends on.** The shared command-pause registry, vocabulary node identities and the
//!   interconnect's progress shapes.
//! - **Must not know.** What a barrier waits for, its participants or its deadline.

use error_stack::Report;
use nervix_interconnect::ApplicationProgress;
use nervix_models::ClusterNodeName;
use nervix_primitives::sync::{mpsc, oneshot};

use super::{CommandPausePoint, FaultInjection};

/// A scenario's request that one node query another node's application progress over the
/// interconnect, as a revision barrier on that node queries it. A node asked for its own progress
/// answers what its revision-query handler answers.
pub(crate) struct ApplicationProgressQuery {
    pub(crate) target: ClusterNodeName,
    pub(crate) response:
        oneshot::Sender<error_stack::Result<ApplicationProgress, ApplicationProgressQueryError>>,
}

/// Why a scenario's application progress query has no answer.
#[derive(Debug, thiserror::Error)]
pub enum ApplicationProgressQueryError {
    #[error("node '{}' answers no application progress queries", .node.as_str())]
    QuerierUnavailable { node: ClusterNodeName },
    #[error("node '{}' stopped answering application progress queries", .node.as_str())]
    QuerierStopped { node: ClusterNodeName },
    #[error("node '{}' dropped its application progress query", .node.as_str())]
    ResponseDropped { node: ClusterNodeName },
    #[error(
        "node '{}' could not query the application progress of node '{}'",
        .querier.as_str(),
        .target.as_str()
    )]
    Query {
        querier: ClusterNodeName,
        target: ClusterNodeName,
    },
}

impl FaultInjection {
    /// Holds every revision-query reply from this node until the scenario releases it. The node
    /// keeps recording and reporting its progress while the replies retain their sampled values.
    pub fn pause_application_revision_responses_from(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ApplicationRevisionResponse(node_id));
    }

    pub async fn wait_for_application_revision_response_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ApplicationRevisionResponse(
            node_id.clone(),
        ))
        .await;
    }

    pub fn release_application_revision_responses_from(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::ApplicationRevisionResponse(
            node_id.clone(),
        ));
    }

    pub(crate) async fn pause_application_revision_response_if_armed(
        &self,
        node_id: &ClusterNodeName,
    ) {
        let point = CommandPausePoint::ApplicationRevisionResponse(node_id.clone());
        let Some(pause) = self
            .inner
            .command_pauses
            .get(&point)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
    }

    /// Holds every application progress report this node would send until the scenario releases
    /// it. The node keeps recording its progress meanwhile, so a released report carries what the
    /// node reached by then.
    pub fn pause_application_progress_reports_from(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ApplicationProgressReport(node_id));
    }

    pub async fn wait_for_application_progress_report_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ApplicationProgressReport(
            node_id.clone(),
        ))
        .await;
    }

    pub fn release_application_progress_reports_from(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::ApplicationProgressReport(
            node_id.clone(),
        ));
    }

    pub(crate) async fn pause_application_progress_report_if_armed(
        &self,
        node_id: &ClusterNodeName,
    ) {
        let point = CommandPausePoint::ApplicationProgressReport(node_id.clone());
        let Some(pause) = self
            .inner
            .command_pauses
            .get(&point)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
    }

    /// Registers `node` to answer the scenario's application progress queries. A restarted node
    /// registers again and replaces its predecessor's registration.
    pub(crate) fn register_application_progress_queries(
        &self,
        node: ClusterNodeName,
    ) -> mpsc::Receiver<ApplicationProgressQuery> {
        let (sender, receiver) = mpsc::channel(1);
        self.inner.application_progress_queries.insert(node, sender);
        receiver
    }

    /// Asks `querier` to query the application progress of `target` as its revision barriers do,
    /// and returns the answer.
    pub async fn query_application_progress(
        &self,
        querier: &ClusterNodeName,
        target: ClusterNodeName,
    ) -> error_stack::Result<ApplicationProgress, ApplicationProgressQueryError> {
        let sender = match self.inner.application_progress_queries.get(querier) {
            Some(sender) => sender.clone(),
            None => {
                return Err(Report::new(
                    ApplicationProgressQueryError::QuerierUnavailable {
                        node: querier.clone(),
                    },
                ));
            }
        };
        let (response, receiver) = oneshot::channel();
        let query = ApplicationProgressQuery { target, response };
        if sender.send(query).await.is_err() {
            return Err(Report::new(ApplicationProgressQueryError::QuerierStopped {
                node: querier.clone(),
            }));
        }
        match receiver.await {
            Ok(answer) => answer,
            Err(_) => Err(Report::new(
                ApplicationProgressQueryError::ResponseDropped {
                    node: querier.clone(),
                },
            )),
        }
    }
}
