//! Holding one subscription's opening reply before it announces its schema.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Arming, observing, releasing and reaching the node's one-shot subscription reply seam.
//! - **Depends on.** Typed node names and the shared pause registry and its lifecycle.
//! - **Must not know.** Subscription contents, client restoration, transport framing or graph state.

use nervix_models::ClusterNodeName;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Holds one opened subscription before its reply announces its schema.
    pub fn pause_subscription_response_delivery_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::SubscriptionResponseDelivery(node_id));
    }

    pub async fn wait_for_subscription_response_delivery_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::SubscriptionResponseDelivery(
            node_id.clone(),
        ))
        .await;
    }

    pub fn release_subscription_response_delivery_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::SubscriptionResponseDelivery(
            node_id.clone(),
        ));
    }

    pub(crate) async fn pause_subscription_response_delivery_if_armed(
        &self,
        node_id: &ClusterNodeName,
    ) {
        self.pause_command_if_armed(CommandPausePoint::SubscriptionResponseDelivery(
            node_id.clone(),
        ))
        .await;
    }
}
