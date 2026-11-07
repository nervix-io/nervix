//! A controlled pause after an explicit transaction COMMIT attempt fails.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** A one-shot pause between a failed commit attempt and its reply.
//! - **Depends on.** The shared command pause owner and typed node identity.
//! - **Must not know.** Transaction recovery policy or session transport state.

use nervix_models::ClusterNodeName;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    pub fn pause_explicit_commit_failure(&self, node: ClusterNodeName, transaction_id: String) {
        self.arm_command_pause(CommandPausePoint::ExplicitCommitFailure {
            node,
            transaction_id,
        });
    }

    pub async fn wait_for_explicit_commit_failure(
        &self,
        node: &ClusterNodeName,
        transaction_id: &str,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::ExplicitCommitFailure {
            node: node.clone(),
            transaction_id: transaction_id.to_string(),
        })
        .await;
    }

    pub fn release_explicit_commit_failure(&self, node: &ClusterNodeName, transaction_id: &str) {
        self.release_command_pause(&CommandPausePoint::ExplicitCommitFailure {
            node: node.clone(),
            transaction_id: transaction_id.to_string(),
        });
    }

    pub(crate) async fn pause_explicit_commit_failure_if_armed(
        &self,
        node: &ClusterNodeName,
        transaction_id: &str,
    ) {
        self.pause_command_if_armed(CommandPausePoint::ExplicitCommitFailure {
            node: node.clone(),
            transaction_id: transaction_id.to_string(),
        })
        .await;
    }
}
