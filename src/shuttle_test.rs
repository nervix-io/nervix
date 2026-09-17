//! Shuttle runner policy for server concurrency models.
//!
//! Layer: test harness.
//! - **Owns.** Server model stack sizing, failed-schedule persistence and schedule replay.
//! - **Depends on.** Shuttle's schedulers and runner.
//! - **Must not know.** Product configuration or runtime state outside the model under test.

use std::path::PathBuf;

use shuttle::{
    Config, FailurePersistence, Runner,
    scheduler::{DfsScheduler, PctScheduler, RandomScheduler, ReplayScheduler, Scheduler},
};

// Shuttle executes each modeled thread on a coroutine stack. The server test binary's allocator
// instrumentation and debug frames can exceed Shuttle's 60 KiB default before the model reaches
// its first scheduling point, which the guard page reports as SIGSEGV rather than a Rust panic.
const SERVER_MODEL_STACK_SIZE: usize = 1_048_576;

pub(crate) fn check_dfs<F>(invariant: F, max_iterations: Option<usize>)
where
    F: Fn() + Send + Sync + 'static,
{
    check_with_scheduler(invariant, DfsScheduler::new(max_iterations, false));
}

pub(crate) fn check_pct<F>(invariant: F, iterations: usize, depth: usize)
where
    F: Fn() + Send + Sync + 'static,
{
    check_with_scheduler(invariant, PctScheduler::new(depth, iterations));
}

pub(crate) fn check_random<F>(invariant: F, iterations: usize)
where
    F: Fn() + Send + Sync + 'static,
{
    check_with_scheduler(invariant, RandomScheduler::new(iterations));
}

fn check_with_scheduler<F, S>(invariant: F, scheduler: S)
where
    F: Fn() + Send + Sync + 'static,
    S: Scheduler + 'static,
{
    let mut config = Config::new();
    config.stack_size = SERVER_MODEL_STACK_SIZE;

    if let Some(schedule) = std::env::var_os("SHUTTLE_TRACE_FILE") {
        let scheduler = match ReplayScheduler::new_from_file(&schedule) {
            Ok(scheduler) => scheduler,
            Err(error) => panic!(
                "cannot load Shuttle schedule {}: {error}",
                PathBuf::from(schedule).display()
            ),
        };
        Runner::new(scheduler, config).run(invariant);
        return;
    }

    if let Some(trace_directory) = std::env::var_os("SHUTTLE_TRACE_DIR") {
        let trace_directory = PathBuf::from(trace_directory);
        if let Err(error) = std::fs::create_dir_all(&trace_directory) {
            panic!(
                "cannot create Shuttle failure directory {}: {error}",
                trace_directory.display()
            );
        }
        config.failure_persistence = FailurePersistence::File(Some(trace_directory));
    }

    Runner::new(scheduler, config).run(invariant);
}
