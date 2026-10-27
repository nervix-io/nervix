//! Fault controls at the publication of an uploaded resource version.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** One node's refusal of its next uploaded-version publication as a lost-leadership
//!   refusal, the hold that keeps the refused upload's reply back until the node's reconciliation
//!   takes the upload up, and the observation of both.
//! - **Depends on.** Vocabulary identities, the boundary's watch channel and the shared test
//!   injector.
//! - **Must not know.** How an upload installs, how its recovery decides, consensus or the store.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test publication controls and their observations"
    )
)]

use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;
use nervix_primitives::sync::watch;

use super::FaultInjection;

/// Where one node's refusal of an uploaded-version publication stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResourcePublicationRefusal {
    /// The node's next publication of an uploaded version is refused.
    Armed,
    /// A publication was refused, and the reply to its upload waits for the node's reconciliation.
    Refused,
    /// The node's reconciliation took the upload whose publication it refused up.
    TakenUp,
}

impl FaultInjection {
    /// Refuses the next publication of an uploaded version on `node_id`, after the node installed
    /// its own copy, as a leader that cannot confirm its leadership refuses a write. The reply to
    /// that upload waits until the node's reconciliation takes the upload up, so the uploading
    /// client sends it again only after the reconciliation decided about it.
    pub fn refuse_next_resource_publication_on(&self, node_id: ClusterNodeName) {
        let (refusal, _) = watch::channel(ResourcePublicationRefusal::Armed);
        self.inner
            .refused_resource_publications
            .insert(node_id, refusal);
    }

    /// Whether the reconciliation of `node_id` took up the upload whose publication the node
    /// refused: `None` when no refusal is armed there, and `Some(false)` while the refusal is
    /// armed or the refused upload has not been taken up yet.
    pub fn refused_resource_publication_taken_up(&self, node_id: &ClusterNodeName) -> Option<bool> {
        let refusal = self.inner.refused_resource_publications.get(node_id)?;
        let taken_up = *refusal.borrow() == ResourcePublicationRefusal::TakenUp;
        Some(taken_up)
    }

    /// Whether the publication of an uploaded version `node_id` is about to make is refused,
    /// recording that the armed refusal was delivered.
    pub(crate) fn refuses_resource_publication(&self, node_id: &ClusterNodeName) -> bool {
        let Some(refusal) = self.inner.refused_resource_publications.get(node_id) else {
            return false;
        };
        refusal.send_if_modified(|state| {
            if *state != ResourcePublicationRefusal::Armed {
                return false;
            }
            *state = ResourcePublicationRefusal::Refused;
            true
        })
    }

    /// Records that the reconciliation of `node_id` took an applying upload up, which releases the
    /// reply of an upload whose publication the node refused.
    pub(crate) fn take_up_refused_resource_upload(&self, node_id: &ClusterNodeName) {
        let Some(refusal) = self.inner.refused_resource_publications.get(node_id) else {
            return;
        };
        refusal.send_if_modified(|state| {
            if *state != ResourcePublicationRefusal::Refused {
                return false;
            }
            *state = ResourcePublicationRefusal::TakenUp;
            true
        });
    }

    /// Holds the reply to an upload `node_id` installed while the publication it refused waits
    /// for the node's reconciliation. Every other reply returns at once.
    pub(crate) async fn hold_refused_resource_upload_reply(&self, node_id: &ClusterNodeName) {
        let mut refusal = {
            let Some(armed) = self.inner.refused_resource_publications.get(node_id) else {
                return;
            };
            armed.subscribe()
        };
        refusal
            .wait_for(|state| *state != ResourcePublicationRefusal::Refused)
            .await
            .assured("the armed refusal keeps its sender for the rest of the scenario");
    }
}
