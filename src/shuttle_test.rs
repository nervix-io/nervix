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
    scheduler::{DfsScheduler, PctScheduler, RandomScheduler, ReplayScheduler, Scheduler},
};

// Shuttle executes each modeled thread on a coroutine stack. The server test binary's allocator
// instrumentation and debug frames can exceed Shuttle's 60 KiB default before the model reaches
// its first scheduling point, which the guard page reports as SIGSEGV rather than a Rust panic.
const SERVER_MODEL_STACK_SIZE: usize = 1_048_576;

/// Schedules the random scheduler runs when `check_interleavings` explores one invariant, each
/// choosing a random runnable thread at every scheduling point.
const INTERLEAVING_RANDOM_ITERATIONS: usize = 1_000;
/// Schedules the probabilistic concurrency testing scheduler runs when `check_interleavings`
/// explores one invariant.
const INTERLEAVING_PCT_ITERATIONS: usize = 1_000;
/// One more than the priority changes a PCT schedule of `check_interleavings` makes. A PCT schedule
/// runs the highest-priority runnable thread and moves the running thread to the lowest priority
/// at each change and whenever it yields, so a depth of three reaches an ordering that needs two
/// preemptions at chosen points.
const INTERLEAVING_PCT_DEPTH: usize = 3;
/// Schedules the bounded depth-first search of `check_interleavings` runs for one invariant. The
/// search varies the latest scheduling decisions of its first schedule first, so the bound
/// explores that schedule's tail and the random and PCT schedules explore the rest.
const INTERLEAVING_DFS_ITERATIONS: usize = 1_000;

/// Explore `invariant` under random schedules, under probabilistic concurrency testing and under a
/// bounded depth-first search. A schedule `SHUTTLE_TRACE_FILE` names replays once for each of them.
///
/// The depth-first search ignores yields and tries the lowest-numbered runnable thread first. A
/// model therefore spawns a thread that spins until another thread finishes after that thread, or
/// the search never leaves the spin.
pub(crate) fn check_interleavings(invariant: fn()) {
    check_random(invariant, INTERLEAVING_RANDOM_ITERATIONS);
    check_pct(
        invariant,
        INTERLEAVING_PCT_ITERATIONS,
        INTERLEAVING_PCT_DEPTH,
    );
    check_dfs(invariant, Some(INTERLEAVING_DFS_ITERATIONS));
}

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
