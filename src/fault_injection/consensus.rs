//! Consensus stream controls and observations exposed to the fault harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Per-peer append controls and observations from a running node's consensus probe.
//! - **Depends on.** The harness's shared probe registry and vocabulary node identities.
//! - **Must not know.** Plexus, graph placement, runtime state or Raft retry policy.

use meticulous::OptionExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

impl FaultInjection {
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
