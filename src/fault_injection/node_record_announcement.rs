//! A node that holds back the announcement of its record, for public scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Which nodes hold back the announcement of their record, so a restarted node keeps
//!   its previous run's committed record, as when its announcement is slow to commit.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** Consensus, record contents or what a record decides.

use ahash::RandomState;
use nervix_models::ClusterNodeName;
use nervix_primitives::collections::DashMap;

use super::FaultInjection;

/// The nodes that hold back the announcement of their record.
#[derive(Debug, Default)]
pub(super) struct HeldRecordAnnouncements(DashMap<ClusterNodeName, (), RandomState>);

impl FaultInjection {
    /// Makes `node` hold back the announcement of its record until
    /// [`Self::release_node_record_announcement`]. A scenario arms the hold while the node is
    /// stopped, so the node's next run keeps the record of its previous run committed.
    pub fn hold_node_record_announcement(&self, node: ClusterNodeName) {
        self.inner.held_node_record_announcements.0.insert(node, ());
    }

    /// Lets `node` announce its record again on its next attempt.
    pub fn release_node_record_announcement(&self, node: &ClusterNodeName) {
        self.inner.held_node_record_announcements.0.remove(node);
    }

    /// Whether `node` holds back the announcement of its record.
    pub(crate) fn holds_node_record_announcement(&self, node: &ClusterNodeName) -> bool {
        self.inner
            .held_node_record_announcements
            .0
            .contains_key(node)
    }
}
