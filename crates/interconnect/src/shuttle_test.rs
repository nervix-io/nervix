//! Shuttle runner policy for interconnect concurrency checks.
//!
//! Layer: test harness.
//!
//! - **Owns.** The random and PCT exploration every interconnect check runs under, failed-schedule
//!   persistence, and schedule replay.
//! - **Depends on.** Shuttle's schedulers and runner.
//! - **Must not know.** The protocol a check holds to its invariant.

use std::path::PathBuf;

use shuttle::{
    Config, FailurePersistence, Runner,
    scheduler::{PctScheduler, RandomScheduler, ReplayScheduler},
};

const RANDOM_ITERATIONS: usize = 100;
const PCT_ITERATIONS: usize = 100;
const PCT_DEPTH: usize = 3;

/// Explore `invariant` under Shuttle's random scheduler and then its PCT scheduler.
///
/// `SHUTTLE_TRACE_DIR` persists a failing schedule into that directory, and `SHUTTLE_TRACE_FILE`
/// replays one persisted schedule instead of exploring.
pub(crate) fn check_random_and_pct<F>(invariant: F)
where
    F: Fn() + Clone + Send + Sync + 'static,
{
    let mut config = Config::new();

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

    Runner::new(RandomScheduler::new(RANDOM_ITERATIONS), config.clone()).run(invariant.clone());
    Runner::new(PctScheduler::new(PCT_DEPTH, PCT_ITERATIONS), config).run(invariant);
}
