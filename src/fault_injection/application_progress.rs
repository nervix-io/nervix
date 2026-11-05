//! Node-scoped holds of the application progress a revision barrier reads from other nodes.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The hold of every revision-query reply a node answers, until a scenario releases it.
//! - **Depends on.** The shared command-pause registry and vocabulary node identities.
//! - **Must not know.** What a barrier waits for, its participants or its deadline.

use nervix_models::ClusterNodeName;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Holds every revision-query reply from this node until the scenario releases it. Local
    /// revision publication and gossip continue while the replies retain their sampled values.
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
}
