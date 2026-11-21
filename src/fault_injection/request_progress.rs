//! Withheld progress reports of the requests a node's sessions serve.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Which nodes withhold the progress reports their sessions send.
//! - **Depends on.** Vocabulary node identities and the shared test injector.
//! - **Must not know.** Session transports, the requests a session serves or any client.

#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "owns bounded testing-only progress report controls")
)]

use nervix_models::ClusterNodeName;

use super::FaultInjection;

impl FaultInjection {
    /// Withholds every progress report the sessions on `node_id` send. A client waiting on a
    /// request that node serves then hears silence until the request is answered or the reports
    /// are restored, as it would from a node that stopped answering.
    pub fn withhold_request_progress_on(&self, node_id: ClusterNodeName) {
        self.inner.withheld_request_progress.insert(node_id, ());
    }

    /// Lets the sessions on `node_id` send their progress reports again.
    pub fn restore_request_progress_on(&self, node_id: &ClusterNodeName) {
        self.inner.withheld_request_progress.remove(node_id);
    }

    /// Whether the sessions on `node_id` withhold their progress reports.
    pub(crate) fn request_progress_withheld_on(&self, node_id: &ClusterNodeName) -> bool {
        self.inner.withheld_request_progress.contains_key(node_id)
    }
}
