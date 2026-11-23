//! Controlled boundaries of a START that places a stopped domain's units under its node rules, for
//! public placement scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Node- and domain-scoped barriers where a START has found a unit with no admitted node
//!   and is about to wait, where the ownership handoff of a move it planned has begun and it is
//!   about to check its planning inputs, where it checked them and is about to publish the moved
//!   schedule, and where it could not move its misplaced units yet and is about to look again.
//! - **Depends on.** The shared one-shot pause owner and vocabulary identities.
//! - **Must not know.** Node rules, schedules, ownership handoffs or how a START answers.

use nervix_models::{ClusterNodeName, DomainName};

use super::{CommandPausePoint, FaultInjection};

/// Where a START placing a stopped domain's units is held: after it decided and before it waits,
/// or between the steps of a move of misplaced units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StartPlacementBoundary {
    /// Some unit has no node its rules admit, and the START is about to wait for the cluster to
    /// change.
    Waiting,
    /// The ownership handoff of a move of misplaced units has begun, and the START is about to
    /// check that the planning inputs it planned the move from have not changed.
    Moving,
    /// The START found its planning inputs unchanged and is about to publish the moved schedule,
    /// which consensus checks against the same inputs again.
    Publishing,
    /// A move of misplaced units could not begin or publish yet, and the START is about to wait
    /// before it looks again.
    Deferred,
}

impl FaultInjection {
    pub fn pause_start_placement(
        &self,
        node: ClusterNodeName,
        domain: DomainName,
        boundary: StartPlacementBoundary,
    ) {
        self.arm_command_pause(CommandPausePoint::StartPlacement {
            node,
            domain,
            boundary,
        });
    }

    pub async fn wait_for_start_placement_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: StartPlacementBoundary,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::StartPlacement {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        })
        .await;
    }

    pub fn release_start_placement_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: StartPlacementBoundary,
    ) {
        self.release_command_pause(&CommandPausePoint::StartPlacement {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        });
    }

    pub(crate) async fn pause_start_placement_if_armed(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: StartPlacementBoundary,
    ) {
        self.pause_command_if_armed(CommandPausePoint::StartPlacement {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        })
        .await;
    }
}
