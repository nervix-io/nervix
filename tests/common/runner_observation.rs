//! Bounded observations a scenario keeps independent of its runner task.
//!
//! Layer: test harness.
//! - **Owns.** An observation task, cancellation when its step ends, and returning its result or
//!   original panic to that step.
//! - **Depends on.** A bounded observation future and the binary's runtime.
//! - **Must not know.** The observed product, its protocol, or its deadline policy.

use std::{future::Future, panic::resume_unwind};

use meticulous::ResultExt as _;
use nervix_primitives::task::AbortOnDropHandle;

/// Starts polling `observation` on a runtime worker before returning its owned future.
///
/// The caller captures the observation's deadline before handing it here. A stalled runner delays
/// learning the result, rather than polling the observation or restarting its budget. Dropping the
/// returned future aborts the task and releases the session or observer the observation owns.
pub(crate) fn observe_off_the_runner<F>(observation: F) -> impl Future<Output = F::Output> + Send
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let task = AbortOnDropHandle::new(nervix_primitives::task::spawn(observation));
    async move {
        match task.await {
            Ok(output) => output,
            Err(failure) => {
                let panic = failure.try_into_panic().assured(
                    "the step owns the observation's only handle, so nothing cancelled it",
                );
                resume_unwind(panic)
            }
        }
    }
}
