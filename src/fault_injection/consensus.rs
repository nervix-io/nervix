//! Consensus stream observations exposed to the fault harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Read-only per-peer append setup counts from a running node's consensus probe.
//! - **Depends on.** The harness's shared probe registry and vocabulary node identities.
//! - **Must not know.** NSPL, graph placement, runtime state or Raft retry policy.

use meticulous::OptionExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

impl FaultInjection {
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
