//! Fault controls at the start of a resource upload.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** One node's refusal of its next resource upload start, made the way a leader that
//!   could not confirm its lease refuses a write before proposing it, and the observation that the
//!   refusal happened.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** How an upload installs, how a refusal is answered, or how a client sends an
//!   upload again.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns the test refusal of an upload start and its observation"
    )
)]

use nervix_models::ClusterNodeName;

use super::FaultInjection;

/// Where one node's refusal of a resource upload start stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResourceUploadStartRefusal {
    /// The node's next resource upload start is refused.
    Armed,
    /// The node refused an upload start, which proposed nothing and assigned no version.
    Refused,
}

impl FaultInjection {
    /// Refuses the next resource upload start on `node_id` before the node proposes it, as a
    /// leader whose lease lapsed refuses a write. Only the first upload start after the refusal was
    /// armed meets it.
    pub fn refuse_next_resource_upload_start_on(&self, node_id: ClusterNodeName) {
        self.inner
            .refused_resource_upload_starts
            .insert(node_id, ResourceUploadStartRefusal::Armed);
    }

    /// Whether `node_id` refused a resource upload start: `None` when no refusal is armed there,
    /// and `Some(false)` while the armed refusal waits for an upload start.
    pub fn refused_resource_upload_start(&self, node_id: &ClusterNodeName) -> Option<bool> {
        let refusal = self.inner.refused_resource_upload_starts.get(node_id)?;
        let refused = *refusal == ResourceUploadStartRefusal::Refused;
        Some(refused)
    }

    /// Whether the resource upload start `node_id` is about to propose is refused, recording that
    /// the armed refusal was delivered.
    pub(crate) fn refuses_resource_upload_start(&self, node_id: &ClusterNodeName) -> bool {
        let Some(mut refusal) = self.inner.refused_resource_upload_starts.get_mut(node_id) else {
            return false;
        };
        if *refusal != ResourceUploadStartRefusal::Armed {
            return false;
        }
        *refusal = ResourceUploadStartRefusal::Refused;
        true
    }
}
