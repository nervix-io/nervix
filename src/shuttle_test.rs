//! Shuttle runner policy for server concurrency models.
//!
//! Layer: test harness.
//! - **Owns.** Server model stack sizing, exploration budgets, failed-schedule persistence and
//!   schedule replay.
//! - **Depends on.** Shuttle's schedulers and runner.
//! - **Must not know.** Product configuration or runtime state outside the model under test.

use std::path::PathBuf;

use shuttle::{
    Config, FailurePersistence, Runner,
    scheduler::{DfsScheduler, PctScheduler, RandomScheduler, ReplayScheduler},
};

// Shuttle executes each modeled thread on a coroutine stack. The server test binary's allocator
// instrumentation and debug frames can exceed Shuttle's 60 KiB default before the model reaches
// its first scheduling point, which the guard page reports as SIGSEGV rather than a Rust panic.
const SERVER_MODEL_STACK_SIZE: usize = 1_048_576;

// The randomized exploration budget of one check, matching the execution crate's checks.
const RANDOM_ITERATIONS: usize = 100;
const PCT_ITERATIONS: usize = 100;
const PCT_DEPTH: usize = 3;

/// How one check runs: replaying the schedule `SHUTTLE_TRACE_FILE` names, or exploring new
/// schedules and persisting a failing one under `SHUTTLE_TRACE_DIR` when that is set.
enum ModelRun {
    Replay {
        scheduler: ReplayScheduler,
        config: Config,
    },
    Explore {
        config: Config,
    },
}

impl ModelRun {
    fn from_environment() -> Self {
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
            return Self::Replay { scheduler, config };
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

        Self::Explore { config }
    }
}

pub(crate) fn check_dfs<F>(invariant: F, max_iterations: Option<usize>)
where
    F: Fn() + Send + Sync + 'static,
{
    match ModelRun::from_environment() {
        ModelRun::Replay { scheduler, config } => {
            Runner::new(scheduler, config).run(invariant);
        }
        ModelRun::Explore { config } => {
            Runner::new(DfsScheduler::new(max_iterations, false), config).run(invariant);
        }
    }
}

/// Explores `invariant` under Shuttle's random scheduler and then under its probabilistic
/// concurrency testing scheduler.
pub(crate) fn check_random_and_pct(invariant: fn()) {
    match ModelRun::from_environment() {
        ModelRun::Replay { scheduler, config } => {
            Runner::new(scheduler, config).run(invariant);
        }
        ModelRun::Explore { config } => {
            Runner::new(RandomScheduler::new(RANDOM_ITERATIONS), config.clone()).run(invariant);
            Runner::new(PctScheduler::new(PCT_DEPTH, PCT_ITERATIONS), config).run(invariant);
        }
    }
}
