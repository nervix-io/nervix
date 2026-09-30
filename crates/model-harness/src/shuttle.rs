//! Shuttle checks of production owners.
//!
//! A check drives a production owner from a few Shuttle threads and tasks, and Shuttle runs it once
//! for every schedule its scheduler chooses. [`check_random_and_pct`] explores an invariant under
//! 100 random schedules and 100 probabilistic concurrency testing schedules of depth three;
//! [`check_interleavings`] explores it under 1,000 of each and a depth-first search bounded at 1,000
//! schedules; [`check_random`], [`check_pct`] and [`check_dfs`] explore it under one scheduler with
//! the check's own bounds. Every schedule may take at most 10,000 steps, and one that needs more
//! fails its check rather than ending the search early. A passing random or PCT search is evidence
//! for the schedules it explored, not an exhaustive proof; a depth-first search that finishes below
//! its bound explored every schedule.
//!
//! The environment selects the other runs of the same check. `SHUTTLE_CHECK_NONDETERMINISM` explores
//! 100 random schedules and runs each twice, failing when the second run diverges from the first,
//! which uncovers nondeterminism the scheduler does not control. `SHUTTLE_TRACE_DIR` persists a
//! failing schedule there, and `SHUTTLE_TRACE_FILE` replays one persisted schedule instead of
//! exploring. `SHUTTLE_FORCE_FAILURE` fails every schedule after its invariant held, to prove
//! persistence and replay without breaking a protocol, and `SHUTTLE_REPORT_STEPS` reports the most
//! steps a schedule took.
//!
//! A search that finishes prints one record naming what it explored:
//! `nervix-model-harness: shuttle check explored to completion: <searches> (step limit <n>)`.
//! `just test-shuttle` accepts nothing else as a completed check. A search that ran fewer schedules
//! than it declares fails, and a replay announces the schedule it replays before it starts, which
//! completes nothing.

use std::{env, path::PathBuf};

use nervix_primitives::{
    sync::Arc,
    unmodeled::sync::atomic::{AtomicUsize, Ordering},
};
use shuttle::{
    Config, FailurePersistence, MaxSteps, Runner,
    scheduler::{
        DfsScheduler, PctScheduler, RandomScheduler, ReplayScheduler, Scheduler,
        UncontrolledNondeterminismCheckScheduler,
    },
};

/// The stack of each modeled thread. A test binary's allocator instrumentation and debug frames
/// can exceed Shuttle's 60 KiB default before a model reaches its first scheduling point, which the
/// guard page reports as a segmentation fault rather than a panic.
const MODEL_STACK_SIZE: usize = 1_048_576;
/// The most steps one schedule may take. A schedule that needs more fails its check.
const MAX_SCHEDULE_STEPS: usize = 10_000;
/// The random schedules the uncontrolled-nondeterminism check explores.
const NONDETERMINISM_ITERATIONS: usize = 100;
/// The runs of the uncontrolled-nondeterminism check: every random schedule runs once to record its
/// decisions and once more to compare them.
const NONDETERMINISM_EXECUTIONS: usize = NONDETERMINISM_ITERATIONS * 2;
/// The schedules each scheduler of [`check_random_and_pct`] explores.
const RANDOM_AND_PCT_ITERATIONS: usize = 100;
/// The schedules each scheduler of [`check_interleavings`] explores, and the bound of its search.
const INTERLEAVING_ITERATIONS: usize = 1_000;
/// One more than the priority changes a PCT schedule makes. A PCT schedule runs the
/// highest-priority runnable thread and moves the running thread to the lowest priority at each
/// change and whenever it yields, so a depth of three reaches an ordering that needs two
/// preemptions at chosen points.
const PCT_DEPTH: usize = 3;

/// How one search of a check chooses its schedules.
#[derive(Debug, Clone, Copy)]
enum Search {
    Random { iterations: usize },
    Pct { iterations: usize, depth: usize },
    Dfs { max_iterations: Option<usize> },
}

impl Search {
    /// Run `invariant` under this search and describe what it explored, or fail the check when the
    /// search ended before it explored what it declares.
    fn explore<F>(self, config: &Config, invariant: &Invariant<F>) -> String
    where
        F: Fn() + Send + Sync + 'static,
    {
        match self {
            Self::Random { iterations } => {
                let executions = Runner::new(RandomScheduler::new(iterations), config.clone())
                    .run(invariant.run());
                require_every_schedule("random", executions, iterations);
                format!("random {executions} of {iterations} schedules")
            }
            Self::Pct { iterations, depth } => {
                let executions = Runner::new(PctScheduler::new(depth, iterations), config.clone())
                    .run(invariant.run());
                require_every_schedule("PCT", executions, iterations);
                format!("PCT {executions} of {iterations} schedules at depth {depth}")
            }
            Self::Dfs { max_iterations } => {
                let executions =
                    Runner::new(DfsScheduler::new(max_iterations, false), config.clone())
                        .run(invariant.run());
                match max_iterations {
                    Some(bound) if executions >= bound => {
                        format!("depth-first {executions} schedules, bounded at {bound}")
                    }
                    Some(_) | None => format!("depth-first {executions} schedules, every one"),
                }
            }
        }
    }
}

/// Fail the check when a search ran fewer schedules than it declares: an exploration that did not
/// finish is not evidence.
fn require_every_schedule(search: &str, executions: usize, declared: usize) {
    assert!(
        executions == declared,
        "the {search} search of a shuttle check ran {executions} of its {declared} schedules; an \
         exploration that does not finish is a failure"
    );
}

/// The invariant of one check, shared by every search of it, and the step statistic the searches
/// report. The statistic spans every schedule the runner starts and is read after they end, where
/// no modeled atomic can exist, so it is a real one.
struct Invariant<F> {
    body: Arc<F>,
    highest_steps: Arc<AtomicUsize>,
}

impl<F> Invariant<F>
where
    F: Fn() + Send + Sync + 'static,
{
    fn new(body: F) -> Self {
        Self {
            body: Arc::new(body),
            highest_steps: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// One schedule of the check: the invariant, then the step statistic, then the forced failure
    /// that proves persistence and replay when the environment asks for it.
    fn run(&self) -> impl Fn() + Send + Sync + 'static {
        let body = Arc::clone(&self.body);
        let highest_steps = Arc::clone(&self.highest_steps);
        move || {
            body();
            highest_steps.fetch_max(shuttle::current::context_switches(), Ordering::Relaxed);
            if env::var_os("SHUTTLE_FORCE_FAILURE").is_some() {
                panic!("forced Shuttle schedule replay verification");
            }
        }
    }

    fn report_steps(&self) {
        if env::var_os("SHUTTLE_REPORT_STEPS").is_some() {
            eprintln!(
                "Shuttle maximum steps: {}",
                self.highest_steps.load(Ordering::Relaxed)
            );
        }
    }
}

/// Explore `invariant` under `searches`, or under the run the environment selects instead.
fn explore<F>(searches: &[Search], invariant: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let invariant = Invariant::new(invariant);
    let mut config = Config::new();
    config.stack_size = MODEL_STACK_SIZE;
    config.max_steps = MaxSteps::FailAfter(MAX_SCHEDULE_STEPS);

    if let Some(schedule) = env::var_os("SHUTTLE_TRACE_FILE") {
        let schedule = PathBuf::from(schedule);
        let scheduler = match ReplayScheduler::new_from_file(&schedule) {
            Ok(scheduler) => scheduler,
            Err(error) => panic!(
                "cannot load Shuttle schedule {}: {error}",
                schedule.display()
            ),
        };
        eprintln!(
            "nervix-model-harness: shuttle check replays the schedule in {}; a replay completes \
             no exploration",
            schedule.display()
        );
        Runner::new(scheduler, config).run(invariant.run());
        invariant.report_steps();
        return;
    }

    if let Some(trace_directory) = env::var_os("SHUTTLE_TRACE_DIR") {
        let trace_directory = PathBuf::from(trace_directory);
        if let Err(error) = std::fs::create_dir_all(&trace_directory) {
            panic!(
                "cannot create Shuttle failure directory {}: {error}",
                trace_directory.display()
            );
        }
        config.failure_persistence = FailurePersistence::File(Some(trace_directory));
    }

    let explored = if env::var_os("SHUTTLE_CHECK_NONDETERMINISM").is_some() {
        let scheduler = UncontrolledNondeterminismCheckScheduler::new(RandomScheduler::new(
            NONDETERMINISM_ITERATIONS,
        ));
        let executions = Runner::new(scheduler, config).run(invariant.run());
        require_every_schedule("nondeterminism", executions, NONDETERMINISM_EXECUTIONS);
        vec![format!(
            "nondeterminism check of {NONDETERMINISM_ITERATIONS} random schedules, each run twice"
        )]
    } else {
        let mut explored = Vec::new();
        for search in searches {
            explored.push(search.explore(&config, &invariant));
        }
        explored
    };
    invariant.report_steps();
    eprintln!(
        "nervix-model-harness: shuttle check explored to completion: {} (step limit \
         {MAX_SCHEDULE_STEPS})",
        explored.join(", ")
    );
}

/// Explore `invariant` under 100 random schedules and 100 PCT schedules of depth three.
pub fn check_random_and_pct<F>(invariant: F)
where
    F: Fn() + Send + Sync + 'static,
{
    explore(
        &[
            Search::Random {
                iterations: RANDOM_AND_PCT_ITERATIONS,
            },
            Search::Pct {
                iterations: RANDOM_AND_PCT_ITERATIONS,
                depth: PCT_DEPTH,
            },
        ],
        invariant,
    );
}

/// Explore `invariant` under 1,000 random schedules, 1,000 PCT schedules of depth three, and a
/// depth-first search bounded at 1,000 schedules.
///
/// The depth-first search varies the latest scheduling decisions of its first schedule first, so
/// the bound explores that schedule's tail and the random and PCT schedules explore the rest. It
/// ignores yields and tries the lowest-numbered runnable thread first, so a model spawns a thread
/// that spins until another thread finishes after that thread, or the search never leaves the spin.
pub fn check_interleavings<F>(invariant: F)
where
    F: Fn() + Send + Sync + 'static,
{
    explore(
        &[
            Search::Random {
                iterations: INTERLEAVING_ITERATIONS,
            },
            Search::Pct {
                iterations: INTERLEAVING_ITERATIONS,
                depth: PCT_DEPTH,
            },
            Search::Dfs {
                max_iterations: Some(INTERLEAVING_ITERATIONS),
            },
        ],
        invariant,
    );
}

/// Explore `invariant` under `iterations` random schedules.
pub fn check_random<F>(invariant: F, iterations: usize)
where
    F: Fn() + Send + Sync + 'static,
{
    explore(&[Search::Random { iterations }], invariant);
}

/// Explore `invariant` under `iterations` PCT schedules of `depth`.
pub fn check_pct<F>(invariant: F, iterations: usize, depth: usize)
where
    F: Fn() + Send + Sync + 'static,
{
    explore(&[Search::Pct { iterations, depth }], invariant);
}

/// Explore `invariant` under a depth-first search of at most `max_iterations` schedules, or of every
/// schedule.
pub fn check_dfs<F>(invariant: F, max_iterations: Option<usize>)
where
    F: Fn() + Send + Sync + 'static,
{
    explore(&[Search::Dfs { max_iterations }], invariant);
}
