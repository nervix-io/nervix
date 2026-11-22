//! Scenario barriers around a materialized relay's rows on their way to another unit or to disk.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** One-shot pauses before the unit holding a moved key encodes the row it releases,
//!   before the unit claiming the key decodes the row it was handed, and before a relay's checkpoint
//!   writer seals rows that changed.
//! - **Depends on.** Typed relay identities and the shared command pause owner.
//! - **Must not know.** Partition transitions, claim retries, checkpoint cadence or how a row is
//!   encoded.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns scenario barriers around a materialized relay's moved and checkpointed rows"
    )
)]

use nervix_models::{DomainName, DomainNodeRef, ModelKind, ModelName, NodeRef};

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Holds the next unit of `relay` asked to release a moved key's row, before it encodes the
    /// row it releases.
    pub fn pause_materialized_row_release(&self, relay: DomainNodeRef) {
        self.arm_command_pause(CommandPausePoint::MaterializedRowRelease(relay));
    }

    pub async fn wait_for_materialized_row_release(&self, relay: &DomainNodeRef) {
        self.wait_for_command_pause(&CommandPausePoint::MaterializedRowRelease(relay.clone()))
            .await;
    }

    pub fn release_materialized_row_release(&self, relay: &DomainNodeRef) {
        self.release_command_pause(&CommandPausePoint::MaterializedRowRelease(relay.clone()));
    }

    pub(crate) async fn pause_materialized_row_release_if_armed(
        &self,
        domain: &DomainName,
        relay: &NodeRef,
    ) {
        self.pause_command_if_armed(CommandPausePoint::MaterializedRowRelease(
            relay.in_domain(domain),
        ))
        .await;
    }

    /// Holds the next unit of `relay` that claims a moved key, once its holder has answered with
    /// the key's row and before it decodes that row.
    pub fn pause_materialized_row_take(&self, relay: DomainNodeRef) {
        self.arm_command_pause(CommandPausePoint::MaterializedRowTake(relay));
    }

    pub async fn wait_for_materialized_row_take(&self, relay: &DomainNodeRef) {
        self.wait_for_command_pause(&CommandPausePoint::MaterializedRowTake(relay.clone()))
            .await;
    }

    pub fn release_materialized_row_take(&self, relay: &DomainNodeRef) {
        self.release_command_pause(&CommandPausePoint::MaterializedRowTake(relay.clone()));
    }

    pub(crate) async fn pause_materialized_row_take_if_armed(
        &self,
        domain: &DomainName,
        relay: &NodeRef,
    ) {
        self.pause_command_if_armed(CommandPausePoint::MaterializedRowTake(
            relay.in_domain(domain),
        ))
        .await;
    }

    /// Holds the next checkpoint `relay`'s writer makes of rows that changed, before it seals them
    /// into its staging files.
    pub fn pause_materialized_checkpoint(&self, relay: DomainNodeRef) {
        self.arm_command_pause(CommandPausePoint::MaterializedCheckpointSeal(relay));
    }

    pub async fn wait_for_materialized_checkpoint(&self, relay: &DomainNodeRef) {
        self.wait_for_command_pause(&CommandPausePoint::MaterializedCheckpointSeal(
            relay.clone(),
        ))
        .await;
    }

    pub fn release_materialized_checkpoint(&self, relay: &DomainNodeRef) {
        self.release_command_pause(&CommandPausePoint::MaterializedCheckpointSeal(
            relay.clone(),
        ));
    }

    pub(crate) async fn pause_materialized_checkpoint_if_armed(
        &self,
        domain: &DomainName,
        relay: &ModelName,
    ) {
        self.pause_command_if_armed(CommandPausePoint::MaterializedCheckpointSeal(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        ))
        .await;
    }
}
