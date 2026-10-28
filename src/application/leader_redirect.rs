//! Where a request that needs the cluster leader goes instead.
//!
//! Layer: control plane.
//!
//! - **Owns.** Locating a node, such as the current leader, with the client endpoints discovery
//!   established for it, and the not-leader result a request that needs the leader is refused
//!   with.
//! - **Depends on.** Consensus for the leader and for proposal failures, and cluster gossip for
//!   the endpoints each node advertises.
//! - **Must not know.** How a redirect travels to a client.
//!
//! A redirect never guesses. While no leader is known it names none, and while discovery has not
//! established one of the leader's endpoints that endpoint stays absent.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use error_stack::Report;
use nervix_consensus::ConsensusError;
use nervix_models::ClusterNodeName;

use super::{
    command_result::{
        CommandDiagnostic, CommandDisposition, CommandResult, LeaderRedirect, NodeLocation,
    },
    model_mutation::command_error,
    session_service::SessionServiceImpl,
};

impl SessionServiceImpl {
    /// A node's location, with the endpoints discovery has established for it.
    pub(in crate::application) async fn node_location(
        &self,
        node: ClusterNodeName,
    ) -> NodeLocation {
        let advertised = self.inner.cluster.live_node(&node).await;
        let Some(advertised) = advertised else {
            return NodeLocation {
                node,
                grpc_url: None,
                web_console_url: None,
            };
        };
        NodeLocation {
            node,
            grpc_url: advertised.client_url,
            web_console_url: advertised.console_url,
        }
    }

    /// Where a request that needs the leader goes instead, while `leader` leads.
    pub(in crate::application) async fn redirect_to_leader(
        &self,
        leader: Option<ClusterNodeName>,
    ) -> LeaderRedirect {
        let leader = match leader {
            Some(leader) => Some(self.node_location(leader).await),
            None => None,
        };
        LeaderRedirect { leader }
    }

    /// Refuses a command that needs the leader. Nothing of it was admitted. The diagnostic spans
    /// the command's whole source text, or has no location when the refused request carried none.
    pub(in crate::application) async fn not_leader_response(
        &self,
        query: &str,
        leader: Option<ClusterNodeName>,
    ) -> CommandResult {
        let message = match leader.as_ref() {
            Some(leader) => format!("retry this command on leader '{leader}'"),
            None => "retry this command on the current leader".to_string(),
        };
        let span = if query.is_empty() {
            None
        } else {
            Some(0..query.len())
        };
        let redirect = self.redirect_to_leader(leader).await;
        CommandResult {
            diagnostics: vec![CommandDiagnostic { message, span }],
            ..CommandResult::new(
                CommandDisposition::NotLeader(redirect),
                "not-a-leader".to_string(),
            )
        }
    }

    /// The result of a command whose proposal failed: a not-leader redirect when this node lost
    /// leadership, and a failure carrying `message` otherwise.
    pub(in crate::application) async fn consensus_error_response(
        &self,
        error: &ConsensusError,
        message: String,
    ) -> CommandResult {
        match error {
            ConsensusError::LeadershipLost { leader_id } => {
                self.not_leader_response("", leader_id.clone()).await
            }
            _ => command_error(message),
        }
    }

    /// Renders a proposal report at the session boundary while classification stays typed.
    pub(in crate::application) async fn consensus_report_response(
        &self,
        report: &Report<ConsensusError>,
        operation: String,
    ) -> CommandResult {
        let message = format!("{operation}: {}", ConsensusError::report_message(report));
        self.consensus_error_response(report.current_context(), message)
            .await
    }
}
