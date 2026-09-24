//! Shuttle runner policy for interconnect concurrency checks.
//!
//! Layer: test harness.
//!
//! - **Owns.** The random and PCT exploration every interconnect check runs under, failed-schedule
//!   persistence, and schedule replay.
//! - **Depends on.** Shuttle's schedulers and runner.
//! - **Must not know.** The protocol a check holds to its invariant.

use std::{
    path::PathBuf,
    sync::{
        Arc as StdArc,
        atomic::{AtomicUsize, Ordering},
    },
};

use shuttle::{
    Config, FailurePersistence, MaxSteps, Runner,
    scheduler::{
        PctScheduler, RandomScheduler, ReplayScheduler, UncontrolledNondeterminismCheckScheduler,
    },
};

const RANDOM_ITERATIONS: usize = 100;
const PCT_ITERATIONS: usize = 100;
const PCT_DEPTH: usize = 3;
const NONDETERMINISM_ITERATIONS: usize = 100;
const MAX_SCHEDULE_STEPS: usize = 10_000;

fn measured_invariant<F>(
    invariant: F,
    highest_steps: StdArc<AtomicUsize>,
) -> impl Fn() + Send + Sync + 'static
where
    F: Fn() + Send + Sync + 'static,
{
    move || {
        invariant();
        highest_steps.fetch_max(shuttle::current::context_switches(), Ordering::Relaxed);
        if std::env::var_os("SHUTTLE_FORCE_FAILURE").is_some() {
            panic!("forced Shuttle schedule replay verification");
        }
    }
}

/// Explore `invariant` under Shuttle's random scheduler and then its PCT scheduler.
///
/// `SHUTTLE_TRACE_DIR` persists a failing schedule into that directory, and `SHUTTLE_TRACE_FILE`
/// replays one persisted schedule instead of exploring.
pub(crate) fn check_random_and_pct<F>(invariant: F)
where
    F: Fn() + Clone + Send + Sync + 'static,
{
    let mut config = Config::new();
    config.max_steps = MaxSteps::FailAfter(MAX_SCHEDULE_STEPS);
    let highest_steps = StdArc::new(AtomicUsize::new(0));

    if let Some(schedule) = std::env::var_os("SHUTTLE_TRACE_FILE") {
        let scheduler = match ReplayScheduler::new_from_file(&schedule) {
            Ok(scheduler) => scheduler,
            Err(error) => panic!(
                "cannot load Shuttle schedule {}: {error}",
                PathBuf::from(schedule).display()
            ),
        };
        Runner::new(scheduler, config)
            .run(measured_invariant(invariant, StdArc::clone(&highest_steps)));
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

    if std::env::var_os("SHUTTLE_CHECK_NONDETERMINISM").is_some() {
        let scheduler = UncontrolledNondeterminismCheckScheduler::new(RandomScheduler::new(
            NONDETERMINISM_ITERATIONS,
        ));
        Runner::new(scheduler, config)
            .run(measured_invariant(invariant, StdArc::clone(&highest_steps)));
    } else {
        Runner::new(RandomScheduler::new(RANDOM_ITERATIONS), config.clone()).run(
            measured_invariant(invariant.clone(), StdArc::clone(&highest_steps)),
        );
        Runner::new(PctScheduler::new(PCT_DEPTH, PCT_ITERATIONS), config)
            .run(measured_invariant(invariant, StdArc::clone(&highest_steps)));
    }

    if std::env::var_os("SHUTTLE_REPORT_STEPS").is_some() {
        eprintln!(
            "Shuttle maximum steps: {}",
            highest_steps.load(Ordering::Relaxed)
        );
    }
}
