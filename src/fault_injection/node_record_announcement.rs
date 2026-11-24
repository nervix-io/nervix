//! A node that holds back the announcement of its record, for public scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Which nodes hold back the announcement of their record, so a restarted node keeps
//!   its previous run's committed record, as when its announcement is slow to commit.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** Consensus, record contents or what a record decides.

use nervix_models::ClusterNodeName;

use super::FaultInjection;

impl FaultInjection {
    /// Makes `node` hold back the announcement of its record until
    /// [`Self::release_node_record_announcement`]. A scenario arms the hold while the node is
    /// stopped, so the node's next run keeps the record of its previous run committed.
    pub fn hold_node_record_announcement(&self, node: ClusterNodeName) {
        self.inner.held_node_record_announcements.insert(node, ());
    }

    /// Lets `node` announce its record again on its next attempt.
    pub fn release_node_record_announcement(&self, node: &ClusterNodeName) {
        self.inner.held_node_record_announcements.remove(node);
    }

    /// Whether `node` holds back the announcement of its record.
    pub(crate) fn holds_node_record_announcement(&self, node: &ClusterNodeName) -> bool {
        self.inner.held_node_record_announcements.contains_key(node)
    }
}
