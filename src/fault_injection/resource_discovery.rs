//! Scenario-controlled visibility of a resource's replication sources.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: a node-scoped empty source view and observation that reconciliation consumed it.
//! Depends on: the existing command-pause registry and typed node identities.
//! Must not know: resource bytes, transport behavior, upload outcomes or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "scenario-owned resource source visibility")
)]

use nervix_models::ClusterNodeName;
use nervix_primitives::sync::atomic::Ordering;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Hide otherwise usable sources until the scenario explicitly restores their visibility.
    pub fn defer_resource_source_discovery_on(&self, node: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ResourceSourceDiscovery(node));
    }

    /// Observe that an unready local copy met the deferred source view.
    pub async fn wait_for_deferred_resource_source_discovery(&self, node: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ResourceSourceDiscovery(node.clone()))
            .await;
    }

    /// Restore the real source view; the next ordinary reconciliation may transfer the archive.
    pub fn resume_resource_source_discovery_on(&self, node: &ClusterNodeName) {
        let point = CommandPausePoint::ResourceSourceDiscovery(node.clone());
        let pause = self.command_pause(&point);
        self.inner.command_pauses.remove(&point);
        pause.release();
    }

    pub(crate) fn resource_source_discovery_is_deferred(&self, node: &ClusterNodeName) -> bool {
        let point = CommandPausePoint::ResourceSourceDiscovery(node.clone());
        let Some(pause) = self
            .inner
            .command_pauses
            .get(&point)
            .map(|pause| pause.value().clone())
        else {
            return false;
        };
        if !pause.claimed.swap(true, Ordering::AcqRel) {
            pause.reach();
        }
        true
    }
}
