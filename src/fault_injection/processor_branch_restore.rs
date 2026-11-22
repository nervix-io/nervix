//! A scenario barrier inside a processor task's restore of its branches.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** A one-shot pause after a processor task's restore has read its branches' lifecycle
//!   and before it opens any of their retained state.
//! - **Depends on.** Typed processor identities and the shared command pause owner.
//! - **Must not know.** How a restore builds, installs or retries its branches, or what their
//!   retained state holds.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns a scenario barrier inside a processor task's restore of its branches"
    )
)]

use nervix_models::{DomainName, DomainNodeRef, NodeRef};

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Holds a processor task's next restore of its branches after it has read their lifecycle and
    /// before it opens any of their retained state, so a scenario can fill the node's workers first.
    pub fn pause_processor_branch_restore(&self, processor: DomainNodeRef) {
        self.arm_command_pause(CommandPausePoint::ProcessorBranchRestore(processor));
    }

    pub async fn wait_for_processor_branch_restore(&self, processor: &DomainNodeRef) {
        self.wait_for_command_pause(&CommandPausePoint::ProcessorBranchRestore(
            processor.clone(),
        ))
        .await;
    }

    pub fn release_processor_branch_restore(&self, processor: &DomainNodeRef) {
        self.release_command_pause(&CommandPausePoint::ProcessorBranchRestore(
            processor.clone(),
        ));
    }

    pub(crate) async fn pause_processor_branch_restore_if_armed(
        &self,
        domain: &DomainName,
        processor: &NodeRef,
    ) {
        self.pause_command_if_armed(CommandPausePoint::ProcessorBranchRestore(
            processor.in_domain(domain),
        ))
        .await;
    }
}
