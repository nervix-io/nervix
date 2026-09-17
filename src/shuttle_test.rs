//! Shuttle runner policy for server concurrency models.
//!
//! Layer: test harness.
//! - **Owns.** Server model stack sizing, the schedulers and budgets a model is explored under,
//!   failed-schedule persistence and schedule replay.
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

/// Schedules the random scheduler runs for one invariant, each choosing a random runnable thread
/// at every scheduling point.
const RANDOM_ITERATIONS: usize = 1_000;
/// Schedules the probabilistic concurrency testing scheduler runs for one invariant.
const PCT_ITERATIONS: usize = 1_000;
/// One more than the priority changes a PCT schedule makes. A PCT schedule runs the
/// highest-priority runnable thread and moves the running thread to the lowest priority at each
/// change and whenever it yields, so a depth of three reaches an ordering that needs two
/// preemptions at chosen points.
const PCT_DEPTH: usize = 3;
/// Schedules a bounded depth-first search runs for one invariant. The search varies the latest
/// scheduling decisions of its first schedule first, so the bound explores that schedule's tail and
/// the random and PCT schedules explore the rest.
const DFS_ITERATIONS: usize = 1_000;

/// Explore `invariant` under random schedules, under probabilistic concurrency testing and under a
/// bounded depth-first search, or replay the schedule `SHUTTLE_TRACE_FILE` names instead.
///
/// The depth-first search ignores yields and tries the lowest-numbered runnable thread first. A
/// model therefore spawns a thread that spins until another thread finishes after that thread, or
/// the search never leaves the spin.
pub(crate) fn check_interleavings(invariant: fn()) {
    if let Some(replay) = persisted_schedule() {
        Runner::new(replay, model_config()).run(invariant);
        return;
    }

    let config = exploration_config();
    Runner::new(RandomScheduler::new(RANDOM_ITERATIONS), config.clone()).run(invariant);
    Runner::new(PctScheduler::new(PCT_DEPTH, PCT_ITERATIONS), config.clone()).run(invariant);
    Runner::new(DfsScheduler::new(Some(DFS_ITERATIONS), false), config).run(invariant);
}

pub(crate) fn check_dfs<F>(invariant: F, max_iterations: Option<usize>)
where
    F: Fn() + Send + Sync + 'static,
{
    if let Some(replay) = persisted_schedule() {
        Runner::new(replay, model_config()).run(invariant);
        return;
    }

    Runner::new(
        DfsScheduler::new(max_iterations, false),
        exploration_config(),
    )
    .run(invariant);
}

fn model_config() -> Config {
    let mut config = Config::new();
    config.stack_size = SERVER_MODEL_STACK_SIZE;
    config
}

/// The schedule `SHUTTLE_TRACE_FILE` names, when a failure is being replayed.
fn persisted_schedule() -> Option<ReplayScheduler> {
    let schedule = std::env::var_os("SHUTTLE_TRACE_FILE")?;
    match ReplayScheduler::new_from_file(&schedule) {
        Ok(replay) => Some(replay),
        Err(error) => panic!(
            "cannot load Shuttle schedule {}: {error}",
            PathBuf::from(schedule).display()
        ),
    }
}

/// The model configuration, persisting a failing schedule under `SHUTTLE_TRACE_DIR` when it is
/// set.
fn exploration_config() -> Config {
    let mut config = model_config();
    let Some(trace_directory) = std::env::var_os("SHUTTLE_TRACE_DIR") else {
        return config;
    };
    let trace_directory = PathBuf::from(trace_directory);
    if let Err(error) = std::fs::create_dir_all(&trace_directory) {
        panic!(
            "cannot create Shuttle failure directory {}: {error}",
            trace_directory.display()
        );
    }
    config.failure_persistence = FailurePersistence::File(Some(trace_directory));
    config
}
