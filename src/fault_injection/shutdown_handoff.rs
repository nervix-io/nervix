//! Controlled boundaries of a stopping node's drain and of a committed handoff's activation, for
//! public shutdown scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Whether a stopping node drains its admitted work at all, the node-scoped pause a
//!   stopping node takes before it asks for its work to move, and the domain-scoped pause a
//!   coordinator takes between committing a planned handoff and asking its destination to
//!   activate.
//! - **Depends on.** The shared one-shot pause owner and vocabulary identities.
//! - **Must not know.** Plexus syntax, drain planning or handoff policy.

use nervix_models::{ClusterNodeName, DomainName};
use nervix_primitives::sync::atomic::Ordering;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Makes every node that starts from now on skip draining its admitted work when it shuts
    /// down, until [`Self::run_shutdown_drain`]. The scenario harness skips the drain unless a
    /// scenario enables graceful shutdown drain.
    pub fn skip_shutdown_drain(&self) {
        self.inner
            .shutdown_drain_skipped
            .store(true, Ordering::Release);
    }

    /// Makes every node that starts from now on drain its admitted work when it shuts down, as a
    /// node does unless a test asked it to skip the drain.
    pub fn run_shutdown_drain(&self) {
        self.inner
            .shutdown_drain_skipped
            .store(false, Ordering::Release);
    }

    /// Whether a node starting now drains its admitted work when it shuts down.
    pub(crate) fn runs_shutdown_drain(&self) -> bool {
        !self.inner.shutdown_drain_skipped.load(Ordering::Acquire)
    }

    /// Holds the next graceful drain of `node_id`, once, after the node advertised that it is
    /// terminating and closed its intake but before it asks for its work to move.
    pub fn pause_graceful_drain_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::GracefulDrain(node_id));
    }

    pub async fn wait_for_graceful_drain_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::GracefulDrain(node_id.clone()))
            .await;
    }

    pub fn release_graceful_drain_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::GracefulDrain(node_id.clone()));
    }

    pub(crate) async fn pause_graceful_drain_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::GracefulDrain(node_id.clone()))
            .await;
    }

    /// Holds the coordinator of the next planned handoff in `domain`, once, after its schedule is
    /// committed and before it asks the destination to activate.
    pub fn pause_ownership_handoff_activation(&self, domain: DomainName) {
        self.arm_command_pause(CommandPausePoint::OwnershipHandoffActivation(domain));
    }

    pub async fn wait_for_ownership_handoff_activation_pause(&self, domain: &DomainName) {
        self.wait_for_command_pause(&CommandPausePoint::OwnershipHandoffActivation(
            domain.clone(),
        ))
        .await;
    }

    pub fn release_ownership_handoff_activation_pause(&self, domain: &DomainName) {
        self.release_command_pause(&CommandPausePoint::OwnershipHandoffActivation(
            domain.clone(),
        ));
    }

    pub(crate) async fn pause_ownership_handoff_activation_if_armed(&self, domain: &DomainName) {
        self.pause_command_if_armed(CommandPausePoint::OwnershipHandoffActivation(
            domain.clone(),
        ))
        .await;
    }
}
