//! Scenario barriers around the first poll of a real processor's accepted input.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Test pauses before and after one input future is first polled.
//! - **Depends on.** Typed processor identities and the existing scenario pause owner.
//! - **Must not know.** VM programs, admission policy or processor state.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns scenario barriers around accepted processor input"
    )
)]

use std::{future::Future, task::Poll};

use nervix_models::DomainNodeRef;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    pub fn pause_accepted_processor_input(&self, processor: DomainNodeRef) {
        self.arm_command_pause(CommandPausePoint::ProcessorInputBeforePoll(
            processor.clone(),
        ));
        self.arm_command_pause(CommandPausePoint::ProcessorInputAfterPoll(processor));
    }

    pub async fn wait_for_accepted_processor_input(&self, processor: &DomainNodeRef, polled: bool) {
        self.wait_for_command_pause(&input_point(processor, polled))
            .await;
    }

    pub fn release_accepted_processor_input(&self, processor: &DomainNodeRef, polled: bool) {
        self.release_command_pause(&input_point(processor, polled));
    }

    /// A scenario fills a class between these two barriers. Polling once while it is full makes
    /// both immediate refusal and waiting deterministic; the scenario can release the class
    /// before resuming this future without racing the attempt against that release.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one scenario selects the first poll of an accepted processor input",
            key = "one processor's before-poll and after-poll CommandPausePoints",
            bound = "one keyed presence read and at most two one-shot pause claims; no map guard \
                     crosses an await; scenario teardown releases both pauses"
        )
    )]
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the scenario barrier polls its caller-owned input future \
                                   once, then resumes that same future after the scenario \
                                   releases the barrier")
    )]
    pub(crate) async fn run_accepted_processor_input<F: Future>(
        &self,
        processor: DomainNodeRef,
        future: F,
    ) -> F::Output {
        let armed = self
            .inner
            .command_pauses
            .contains_key(&input_point(&processor, false));
        let mut future = std::pin::pin!(future);
        if !armed {
            return future.await;
        }
        self.pause_command_if_armed(input_point(&processor, false))
            .await;
        let first = futures_util::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
        self.pause_command_if_armed(input_point(&processor, true))
            .await;
        match first {
            Poll::Ready(output) => output,
            Poll::Pending => future.await,
        }
    }
}

fn input_point(processor: &DomainNodeRef, polled: bool) -> CommandPausePoint {
    if polled {
        CommandPausePoint::ProcessorInputAfterPoll(processor.clone())
    } else {
        CommandPausePoint::ProcessorInputBeforePoll(processor.clone())
    }
}
