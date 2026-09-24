//! Where a request that needs the cluster leader goes instead.
//!
//! Layer: control plane.
//!
//! - **Owns.** Locating the current leader with the client endpoints discovery established for
//!   it, and the not-leader result a request that needs the leader is refused with.
//! - **Depends on.** Consensus for the leader and for proposal failures, and cluster gossip for
//!   the endpoints each node advertises.
//! - **Must not know.** How a redirect travels to a client.
//!
//! A redirect never guesses. While no leader is known it names none, and while discovery has not
//! established one of the leader's endpoints that endpoint stays absent.

use nervix_consensus::ConsensusError;
use nervix_models::ClusterNodeName;

use super::{
    command_result::{
        CommandDiagnostic, CommandDisposition, CommandResult, LeaderLocation, LeaderRedirect,
    },
    model_mutation::command_error,
    session_service::SessionServiceImpl,
};

impl SessionServiceImpl {
    /// The leader's location, with the endpoints discovery has established for it.
    pub(in crate::application) async fn leader_location(
        &self,
        leader: ClusterNodeName,
    ) -> LeaderLocation {
        let advertised = self.inner.cluster.live_node(&leader).await;
        let Some(node) = advertised else {
            return LeaderLocation {
                node: leader,
                grpc_url: None,
                web_console_url: None,
            };
        };
        LeaderLocation {
            node: leader,
            grpc_url: node.client_url,
            web_console_url: node.console_url,
        }
    }

    /// Where a request that needs the leader goes instead, while `leader` leads.
    pub(in crate::application) async fn redirect_to_leader(
        &self,
        leader: Option<ClusterNodeName>,
    ) -> LeaderRedirect {
        let leader = match leader {
            Some(leader) => Some(self.leader_location(leader).await),
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
}
