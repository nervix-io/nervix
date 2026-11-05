//! Consensus heartbeat controls and stream observations exposed to the fault harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Follower answer delays and per-peer append setup observations from consensus probes.
//! - **Depends on.** The harness's shared probe registry and vocabulary node identities.
//! - **Must not know.** Plexus, graph placement, runtime state or Raft retry policy.

use std::time::Duration;

use meticulous::OptionExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

impl FaultInjection {
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
