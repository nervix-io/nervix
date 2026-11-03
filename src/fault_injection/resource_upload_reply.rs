//! Fault controls at the reply of an upload call that installed a resource version.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** One node's loss of the reply to its next upload call that installs a version, and
//!   the observation that the loss happened.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** How an upload installs, how its reply is encoded, or how a client
//!   recovers the upload's outcome.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns the test loss of an installed upload's reply and its observation"
    )
)]

use nervix_models::ClusterNodeName;

use super::FaultInjection;

/// Where one node's loss of an installed upload's reply stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResourceUploadReplyLoss {
    /// The reply to the node's next upload call that installs a version is lost.
    Armed,
    /// The node installed a version and lost the reply to the call that installed it.
    Lost,
}

impl FaultInjection {
    /// Loses the reply to the next upload call on `node_id` that installs a version. The call
    /// ends in transport once the installation completed, as a connection that broke before the
    /// reply reached the client ends it, so the uploading client learns nothing of the
    /// installation it caused.
    pub fn lose_next_installed_resource_upload_reply_on(&self, node_id: ClusterNodeName) {
        self.inner
            .lost_resource_upload_replies
            .insert(node_id, ResourceUploadReplyLoss::Armed);
    }

    /// Whether `node_id` lost the reply to an upload call that installed a version: `None` when no
    /// loss is armed there, and `Some(false)` while the armed loss waits for an installing call.
    pub fn lost_installed_resource_upload_reply(&self, node_id: &ClusterNodeName) -> Option<bool> {
        let loss = self.inner.lost_resource_upload_replies.get(node_id)?;
        let lost = *loss == ResourceUploadReplyLoss::Lost;
        Some(lost)
    }

    /// Whether the reply to an upload call with which `node_id` installed a version is lost,
    /// recording that the armed loss was delivered. Only the first installing call after the
    /// loss was armed loses its reply.
    pub(crate) fn loses_installed_resource_upload_reply(&self, node_id: &ClusterNodeName) -> bool {
        let Some(mut loss) = self.inner.lost_resource_upload_replies.get_mut(node_id) else {
            return false;
        };
        if *loss != ResourceUploadReplyLoss::Armed {
            return false;
        }
        *loss = ResourceUploadReplyLoss::Lost;
        true
    }
}
