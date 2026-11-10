//! A processor's completed flush whose accounting obligation a scenario holds open.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The one-shot observation and release of one processor's flush completion.
//! - **Depends on.** Vocabulary identities and the shared reached/release barrier.
//! - **Must not know.** Drain decisions, branch contents or force-flush generation internals.

#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "owns a testing-only processor completion barrier")
)]

use nervix_models::DomainNodeRef;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Holds one completed processor flush before its obligation is settled.
    pub fn pause_processor_flush_completion(&self, processor: DomainNodeRef) {
        self.arm_command_pause(CommandPausePoint::ProcessorFlushCompletion(processor));
    }

    pub async fn wait_for_processor_flush_completion_pause(&self, processor: &DomainNodeRef) {
        self.wait_for_command_pause(&CommandPausePoint::ProcessorFlushCompletion(
            processor.clone(),
        ))
        .await;
    }

    pub fn release_processor_flush_completion_pause(&self, processor: &DomainNodeRef) {
        self.release_command_pause(&CommandPausePoint::ProcessorFlushCompletion(
            processor.clone(),
        ));
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario holds one processor flush completion before its accounting \
                      settles",
            key = "one test processor",
            bound = "one keyed read and at most one removal; no guard survives the one-shot test \
                     wait"
        )
    )]
    pub(crate) async fn pause_processor_flush_completion_if_armed(&self, processor: DomainNodeRef) {
        self.pause_command_if_armed(CommandPausePoint::ProcessorFlushCompletion(processor))
            .await;
    }
}
