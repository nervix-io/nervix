//! Synchronous work a scenario step hands to a blocking task instead of running it on the runner
//! task.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Running one synchronous job a step needs, such as reading, verifying or rewriting a
//!   backup archive or inspecting a stopped node's database, on the runtime's blocking pool, and
//!   handing its result or its panic back to the step that awaits it.
//! - **Depends on.** The binary's runtime and its blocking pool.
//! - **Must not know.** What a job reads or asserts, or which scenario runs it.
//!
//! Cucumber polls every running scenario, and the suite watchdog around them, from one runner task.
//! A step that does seconds of file, encoding or database work inline stops every scenario of the
//! run for that long, and the watchdog cannot end such a stall. A job handed here keeps its step
//! waiting while the runner task goes on polling the others.

use std::panic::resume_unwind;

use meticulous::ResultExt as _;
use nervix_primitives::task::AbortOnDropHandle;

/// Runs `job` on a blocking task and returns its result once it ends.
///
/// The job starts when this is called, before the returned future is first polled. A job that
/// panics fails the step that awaits it with its own panic, so its assertion message is the step's
/// failure. Dropping the future while the job runs detaches it: a blocking job cannot be aborted,
/// so it runs to its own end with whatever it owns.
pub(crate) fn off_the_runner<T, J>(job: J) -> impl Future<Output = T> + Send
where
    T: Send + 'static,
    J: FnOnce() -> T + Send + 'static,
{
    let task = AbortOnDropHandle::new(nervix_primitives::task::spawn_blocking(job));
    async move {
        match task.await {
            Ok(output) => output,
            Err(failure) => {
                let panic = failure
                    .try_into_panic()
                    .assured("the future owns the job's only handle, so nothing cancelled it");
                resume_unwind(panic)
            }
        }
    }
}
