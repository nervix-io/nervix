//! Consensus membership and stream controls exposed to the fault harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Membership interruption, follower answer delays, append controls and observations
//!   from running nodes' consensus probes.
//! - **Depends on.** The harness's shared probe registry and vocabulary node identities.
//! - **Must not know.** Plexus, graph placement, runtime state or Raft retry policy.

use std::time::Duration;

use meticulous::OptionExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

impl FaultInjection {
    /// Interrupt a promotion on whichever current node leads, after its joint entry applies.
    pub async fn interrupt_voter_promotion(&self, joining: ClusterNodeName) {
        for probe in self.consensus_test_probes() {
            nervix_primitives::task::consume_budget().await;
            probe.interrupt_voter_promotion(joining.clone()).await;
        }
    }

    pub async fn interrupted_membership(
        &self,
        joining: &ClusterNodeName,
    ) -> Option<nervix_consensus::InterruptedMembership> {
        for probe in self.consensus_test_probes() {
            nervix_primitives::task::consume_budget().await;
            if let Some(witness) = probe.interrupted_membership(joining).await {
                return Some(witness);
            }
        }
        None
    }

    pub fn set_consensus_heartbeat_response_delay(&self, node: &ClusterNodeName, delay: Duration) {
        let state = self.inner.consensus_probes.entry(node.clone()).or_default();
        state.test_probe.set_heartbeat_response_delay(delay);
    }

    pub fn delayed_consensus_heartbeat_requests(&self, node: &ClusterNodeName) -> u64 {
        let state = self
            .inner
            .consensus_probes
            .get(node)
            .verified("the harness registered this node before observing delayed heartbeats");
        state.test_probe.delayed_heartbeat_requests()
    }

    /// Arm one data submission on the future leader, before transferring leadership to it.
    pub async fn fail_next_append_submission(
        &self,
        node: &ClusterNodeName,
        target: ClusterNodeName,
    ) {
        let test_probe = {
            let state = self.inner.consensus_probes.entry(node.clone()).or_default();
            state.test_probe.clone()
        };
        test_probe.fail_next_append_submission(target).await;
    }

    pub async fn append_submission_failure(
        &self,
        node: &ClusterNodeName,
    ) -> Option<nervix_consensus::AppendSubmissionFailure> {
        let test_probe = {
            let state = self.inner.consensus_probes.get(node)?;
            state.test_probe.clone()
        };
        test_probe.append_submission_failure().await
    }

    pub fn consensus_append_stream_attempt_count(
        &self,
        node: &ClusterNodeName,
        target: &ClusterNodeName,
    ) -> u64 {
        let state = self
            .inner
            .consensus_probes
            .get(node)
            .verified("the harness registered this node before observing consensus");
        let _running_probe = state
            .probe
            .as_ref()
            .verified("the harness started this node before observing consensus");
        state.test_probe.append_stream_attempt_count(target)
    }
}
