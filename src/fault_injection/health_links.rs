//! The application health responses and gossip links a scenario fails, blocks or pauses.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The scenario controls of the health responses between two nodes and of the gossip
//!   one node exchanges, and the releases of those controls.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** The failure detector, membership or what a health probe answers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns scenario health and gossip link controls")
)]

use super::*;

impl FaultInjection {
    pub fn arm_health_response_pause(
        &self,
        probing_node: ClusterNodeName,
        responding_node: ClusterNodeName,
    ) {
        self.inner.health_response_pauses.insert(
            HealthResponsePauseKey {
                probing_node,
                responding_node,
            },
            Arc::new(TestPause::default()),
        );
    }

    pub fn fail_health_responses_from(&self, responding_node: ClusterNodeName) {
        self.inner
            .failed_health_responders
            .insert(responding_node, ());
    }

    pub fn fail_health_responses_between(
        &self,
        probing_node: ClusterNodeName,
        responding_node: ClusterNodeName,
    ) {
        self.inner.failed_health_links.insert(
            HealthResponsePauseKey {
                probing_node,
                responding_node,
            },
            (),
        );
    }

    pub fn restore_health_responses_between(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: &ClusterNodeName,
    ) {
        self.inner
            .failed_health_links
            .remove(&HealthResponsePauseKey {
                probing_node: probing_node.clone(),
                responding_node: responding_node.clone(),
            });
    }

    pub fn block_gossip_for_node(&self, node: ClusterNodeName, send_delay: Duration) {
        self.inner.blocked_gossip_nodes.insert(node, send_delay);
    }

    pub fn restore_gossip_for_node(&self, node: &ClusterNodeName) {
        self.inner.blocked_gossip_nodes.remove(node);
    }

    pub(crate) fn gossip_exchange_is_blocked(
        &self,
        sending_node: &ClusterNodeName,
        receiving_node: &ClusterNodeName,
    ) -> bool {
        self.inner.blocked_gossip_nodes.contains_key(sending_node)
            || self.inner.blocked_gossip_nodes.contains_key(receiving_node)
            || self.startup_voter_gossip_exchange_is_blocked(sending_node, receiving_node)
    }

    pub(crate) fn gossip_send_delay(&self, destination: &ClusterNodeName) -> Option<Duration> {
        self.inner
            .blocked_gossip_nodes
            .get(destination)
            .map(|delay| *delay.value())
    }

    pub async fn wait_for_health_response_pause(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: &ClusterNodeName,
    ) {
        let pause = self.health_response_pause(probing_node, responding_node);
        pause.wait_until_reached().await;
    }

    pub fn release_health_response_pause(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: &ClusterNodeName,
    ) {
        let pause = self.health_response_pause(probing_node, responding_node);
        pause.release();
    }

    pub fn release_all_health_responses(&self) {
        for pause in &self.inner.health_response_pauses {
            pause.release();
        }
        self.inner.health_response_pauses.clear();
        self.inner.failed_health_responders.clear();
    }
}
