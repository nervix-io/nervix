//! The one budget the whole scenario suite runs within, and what it does when that budget ends.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The suite's execution budget and the reserve it leaves the workflow job, the
//!   registry of the clusters a run has live, the diagnostic a suite timeout prints, the bounded
//!   cleanup that timeout drives, the bounded teardown that follows every run, and the status the
//!   process ends with.
//! - **Depends on.** The active-scenario registry, the stop a running node publishes, the
//!   whole-cluster cleanup budget, the phase deadline, Tokio's timers, and the stop of the suite's
//!   test dependencies, which it is handed as a future.
//! - **Must not know.** What a scenario does, what a node is, or how a cluster is configured.
//!
//! # The advertised worst case
//!
//! A scenario run ends within [`SUITE_BUDGET`] of the moment the suite started, whatever any
//! scenario body or after hook is doing. Cucumber's own fail-fast is not that guarantee: it stops
//! scheduling further scenarios and leaves the scenarios already running exactly where they are.
//! The budget is a clock instead, so a step, a diagnostic or a node stop that never returns ends
//! the run at the same instant as one that returns at once.
//!
//! When the budget expires the watchdog reads the registries before it changes anything, so the
//! diagnostic is the state the run was actually in: every scenario still active, which attempt of
//! it is running, the phase it is in, how long it has been in that phase, and the nodes it holds.
//! It then asks every live node to stop and waits one [`WATCHDOG_CLEANUP_WINDOW`] for all of them
//! together, and finally drops the run, which kills the child processes the scenario worlds
//! started and leaves the nodes they still hold to their keepers. Those nodes end with the
//! runtime when the process drops it.
//!
//! # Why the budget is the length it is
//!
//! Each shard of the suite runs inside one workflow job whose own [`WORKFLOW_JOB_LIMIT`] kills
//! everything it owns wherever it is, so the logs such a job leaves end mid-scenario without the
//! suite's own diagnostic. The budget therefore has to leave that job enough time to finish the
//! work around the suite and to upload the diagnostics the suite just produced:
//! [`SLOWEST_JOB_WORK_BEFORE_SUITE`] before the scenario binary starts and
//! [`SUITE_CLEANUP_RESERVE`] after its budget expires. What is left is the budget, and a healthy
//! suite finishes inside it with [`SUITE_SLACK`] to spare.

use std::{
    collections::BTreeMap, fmt, future::Future, io::Write as _, str::FromStr, time::Duration,
};

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::DurationTextError;
use nervix_primitives::sync::{
    StdArc,
    blocking::{LazyLock, Mutex},
};
use nervix_recovery::Reported as _;
use nervix_test_environment::TeardownFailures;

use super::{
    cluster_teardown::CLUSTER_TEARDOWN_BUDGET,
    phase_deadline::{BeforeDeadline, PhaseDeadline},
    scenario_phase::{ActiveScenario, ScenarioIdentity},
};

/// The `timeout-minutes` of the workflow job that runs one shard of the scenario suite. A policy
/// input: keep it in step with the `scenarios` job in `.github/workflows/check.yaml`, which is the
/// emergency guard outside this budget rather than the mechanism that ends a wedged run.
pub(crate) const WORKFLOW_JOB_LIMIT: Duration = Duration::from_secs(115 * 60);
/// A shard's work before the suite starts: its setup, and the download and restore of the archive
/// the `scenarios-build` job packed, which holds about 1.4 GiB of compressed executables. A shard
/// compiles nothing. Before sharding, the same job's setup steps took up to five minutes on Boat in
/// Check 41010408001. Fifteen minutes funds twice that setup with the archive's transfer and
/// restore beside it.
const SLOWEST_JOB_WORK_BEFORE_SUITE: Duration = Duration::from_secs(15 * 60);
/// What the job keeps for itself once the suite budget has expired: the bounded cleanup the
/// watchdog drives, the dependency containers the suite then stops, and the artifact upload that
/// follows. Cleanup, dependency stop and runtime shutdown can consume four minutes together.
/// Retain eleven further minutes for coverage export and uploads on a slow runner; those steps
/// share this reserve after a successful run too, where they took 0.7 to 1.9 minutes in the
/// unsharded scenarios jobs of 22 Check runs of 21-27 October 2026. A policy input.
const SUITE_CLEANUP_RESERVE: Duration = Duration::from_secs(15 * 60);
/// The one budget a whole scenario run has: what the job limit leaves once the work before the
/// suite and the reserve after it are both paid for.
pub(crate) const SUITE_BUDGET: Duration =
    match WORKFLOW_JOB_LIMIT.checked_sub(SLOWEST_JOB_WORK_BEFORE_SUITE) {
        Some(after_setup) => match after_setup.checked_sub(SUITE_CLEANUP_RESERVE) {
            Some(budget) => budget,
            None => panic!("the workflow job limit must cover the suite's cleanup reserve"),
        },
        None => panic!("the workflow job limit must cover the work that precedes the suite"),
    };
/// The longest a healthy shard of three runs. Unsharded on Boat, Check 41010408001 ran 4406
/// attempts, 240,698 slot-seconds of work, in 85.5 minutes at 97.8% utilization of its 48 run
/// slots, and its longest attempt ran 37.5 minutes. A third of that work fills 48 slots for 28.5
/// minutes, so a shard is bounded by its longest attempt instead: 40 minutes funds that attempt and
/// the work queued before it starts. One run measures one runner, so the measuring runner's speed
/// cannot be assumed. Fewer shards make each one longer and need this re-derived.
const SLOWEST_HEALTHY_SUITE: Duration = Duration::from_secs(40 * 60);
/// A policy input funding a runner at half the measured throughput. Across 17 runs of 21-26
/// October on one inventory, the slowest runners ran every scenario 1.7 times as long as the
/// fastest.
const SUITE_HEADROOM_FACTOR: u32 = 2;
/// Additional time beyond the scaled healthy ceiling for retries and the last active attempts.
const SUITE_SLACK: Duration = Duration::from_secs(5 * 60);
const _: () = assert!(
    match SLOWEST_HEALTHY_SUITE.checked_mul(SUITE_HEADROOM_FACTOR) {
        Some(scaled) => match scaled.checked_add(SUITE_SLACK) {
            Some(bound) => bound.as_nanos() <= SUITE_BUDGET.as_nanos(),
            None => false,
        },
        None => false,
    },
    "the suite budget must outlast the slowest healthy suite by its slack"
);
/// The one window every live node has to end within once the watchdog has asked all of them to
/// stop. It is the whole-cluster cleanup budget a scenario's own teardown spends, spent here for
/// every live cluster at once rather than once per cluster.
const WATCHDOG_CLEANUP_WINDOW: Duration = CLUSTER_TEARDOWN_BUDGET;
const _: () = assert!(
    WATCHDOG_CLEANUP_WINDOW.as_nanos() < SUITE_CLEANUP_RESERVE.as_nanos(),
    "the cleanup the watchdog waits for must end well inside the reserve the job keeps"
);
/// How often the cleanup checks whether every node it asked to stop has ended.
const CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// How long the suite waits for its test dependencies to stop before it leaves them to the
/// runner.
///
/// Stopping containers is the runner's job too, and it does it when the job ends. Waiting here
/// without a bound is how a run that has already produced its whole result still loses it to the
/// workflow's own timeout, which cancels the job instead of reporting that result. A policy input.
pub(crate) const DEPENDENCY_SHUTDOWN_BUDGET: Duration = Duration::from_secs(2 * 60);
/// How long dropping the runtime may wait for the blocking tasks a scenario left behind.
///
/// Dropping a multi-threaded runtime waits for every blocking task without a bound, and a scenario
/// that left one parked in a driver that never returns holds the whole process there. This gives
/// them a window and then abandons them: the process is ending, so a task that outlives it costs
/// nothing. A policy input.
pub(crate) const RUNTIME_SHUTDOWN_BUDGET: Duration = Duration::from_secs(60);
const _: () = assert!(
    match DEPENDENCY_SHUTDOWN_BUDGET.checked_add(RUNTIME_SHUTDOWN_BUDGET) {
        Some(teardown) => teardown.as_nanos() < SUITE_CLEANUP_RESERVE.as_nanos(),
        None => false,
    },
    "the suite's bounded teardown must finish inside the reserve the job keeps for it"
);
/// The status the process ends with when the suite budget expired.
///
/// It is the status `timeout(1)` reports, and it is neither the 101 a panic ends with nor the 1 a
/// failed run reports, so a wedged suite is told apart from a failing one by the exit status
/// alone.
pub(crate) const SUITE_TIMEOUT_EXIT_STATUS: i32 = 124;
/// What a Rust process ends with when it unwinds out of `main`.
const PANIC_EXIT_STATUS: i32 = 101;
const _: () = assert!(
    SUITE_TIMEOUT_EXIT_STATUS != 0
        && SUITE_TIMEOUT_EXIT_STATUS != 1
        && SUITE_TIMEOUT_EXIT_STATUS != PANIC_EXIT_STATUS,
    "a suite timeout must be told apart from a passing run, a failing run and a panic"
);

/// The environment variable a run sets to give the suite a budget of its own.
pub(crate) const SUITE_BUDGET_ENV: &str = "NERVIX_TEST_SUITE_BUDGET";

/// The budget one scenario run is given, so a run that must end sooner than the suite's own budget
/// can say so without rebuilding the harness.
#[derive(Clone, Copy, Debug, clap::Args)]
pub(crate) struct SuiteWatchdogArgs {
    /// How long the whole scenario run may take before the watchdog ends it and reports every
    /// scenario still active.
    #[arg(
        long = "suite-budget",
        env = SUITE_BUDGET_ENV,
        default_value_t = SuiteBudgetText(SUITE_BUDGET),
        value_name = "DURATION"
    )]
    suite_budget: SuiteBudgetText,
}

impl SuiteWatchdogArgs {
    /// The watchdog this run uses: the budget the run was given, and the suite's own cleanup
    /// window, which is a property of how long a cluster takes to stop rather than of the run.
    pub(crate) fn watchdog(self) -> SuiteWatchdog {
        SuiteWatchdog::new(self.suite_budget.0, WATCHDOG_CLEANUP_WINDOW)
    }
}

/// A suite budget as its option spells it: read through the guarded duration parser, and written
/// the way that parser reads it back, so the option's help shows the policy default.
#[derive(Clone, Copy, Debug)]
struct SuiteBudgetText(Duration);

impl FromStr for SuiteBudgetText {
    type Err = Report<DurationTextError>;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let budget = nervix_models::parse_duration_text(text)?;
        Ok(Self(budget))
    }
}

impl fmt::Display for SuiteBudgetText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&humantime::format_duration(self.0), formatter)
    }
}

/// The stop one running node published, which the watchdog asks for without owning the node.
///
/// A node belongs to the scenario world that started it, and that world belongs to the run the
/// watchdog is bounding. The registry therefore holds the stop the node published rather than the
/// node, so the watchdog can end a cluster without reaching into a scenario that is still using
/// it.
pub(crate) trait NodeStop: Send + Sync {
    /// Ask this node to stop, returning without waiting for it.
    fn request_stop(&self);
}

/// One node of a live cluster, as the registry holds it.
struct LiveNodeEntry {
    name: String,
    stop: StdArc<dyn NodeStop>,
}

/// One cluster of live nodes, as the registry holds it.
struct LiveClusterEntry {
    scenario: ScenarioIdentity,
    nodes: BTreeMap<u64, LiveNodeEntry>,
}

/// Every cluster the suite has live, and the registrations the next cluster and node take.
#[derive(Default)]
struct LiveClusters {
    /// Keyed by the registration that owns each entry. A scenario name repeats across outline
    /// examples and retries, so the key is the registration rather than anything a feature file
    /// supplies.
    entries: BTreeMap<u64, LiveClusterEntry>,
    next_cluster: u64,
    next_node: u64,
}

impl LiveClusters {
    /// Publishes `entry` under a registration no earlier cluster of the run took.
    fn register_cluster(&mut self, entry: LiveClusterEntry) -> u64 {
        let registration = self.next_cluster;
        self.next_cluster = registration
            .checked_add(1)
            .assured("a suite builds far fewer than 2^64 clusters");
        self.entries.insert(registration, entry);
        registration
    }

    /// Publishes `node` in `cluster`, if that cluster is still registered, under a registration no
    /// earlier node of the run took.
    fn register_node(&mut self, cluster: u64, node: LiveNodeEntry) -> u64 {
        let registration = self.next_node;
        self.next_node = registration
            .checked_add(1)
            .assured("a suite starts far fewer than 2^64 nodes");
        if let Some(cluster) = self.entries.get_mut(&cluster) {
            cluster.nodes.insert(registration, node);
        }
        registration
    }
}

static LIVE_CLUSTERS: LazyLock<Mutex<LiveClusters>> =
    LazyLock::new(|| Mutex::new(LiveClusters::default()));

/// One cluster's entry in the live-cluster registry.
///
/// The entry exists for exactly as long as the registration does: a cluster appears when it is
/// built and disappears when the scenario world holding it is dropped.
#[derive(Debug)]
pub(crate) struct LiveClusterRegistration {
    registration: u64,
}

impl LiveClusterRegistration {
    /// Publishes a cluster the scenario `scenario` is building.
    pub(crate) fn start(scenario: ScenarioIdentity) -> Self {
        let entry = LiveClusterEntry {
            scenario,
            nodes: BTreeMap::new(),
        };
        let registration = LIVE_CLUSTERS.lock().register_cluster(entry);
        Self { registration }
    }

    /// The handle this cluster's nodes publish themselves through.
    pub(crate) fn handle(&self) -> LiveClusterHandle {
        LiveClusterHandle {
            cluster: self.registration,
        }
    }
}

impl Drop for LiveClusterRegistration {
    fn drop(&mut self) {
        LIVE_CLUSTERS.lock().entries.remove(&self.registration);
    }
}

/// The handle a cluster gives each of its nodes, so a node can publish itself for as long as its
/// task runs.
#[derive(Clone, Debug)]
pub(crate) struct LiveClusterHandle {
    cluster: u64,
}

impl LiveClusterHandle {
    /// Publishes a node whose task has just been spawned.
    ///
    /// The returned registration belongs to that task: the node leaves the registry when the task
    /// ends, whether it returned, failed or panicked, or ended with the runtime. That is what lets
    /// the watchdog tell a cluster that stopped from one that is still running without owning
    /// either.
    pub(crate) fn node_started(
        &self,
        name: &str,
        stop: StdArc<dyn NodeStop>,
    ) -> LiveNodeRegistration {
        let node = LiveNodeEntry {
            name: name.to_string(),
            stop,
        };
        let registration = LIVE_CLUSTERS.lock().register_node(self.cluster, node);
        LiveNodeRegistration {
            cluster: self.cluster,
            node: registration,
        }
    }
}

/// One node's entry in the live-cluster registry, owned by that node's own task.
#[derive(Debug)]
pub(crate) struct LiveNodeRegistration {
    cluster: u64,
    node: u64,
}

impl Drop for LiveNodeRegistration {
    fn drop(&mut self) {
        let mut clusters = LIVE_CLUSTERS.lock();
        if let Some(cluster) = clusters.entries.get_mut(&self.cluster) {
            cluster.nodes.remove(&self.node);
        }
    }
}

/// One cluster the suite has live, and the nodes of it that are still running.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LiveCluster {
    pub(crate) scenario: ScenarioIdentity,
    pub(crate) nodes: Vec<String>,
}

impl LiveCluster {
    /// Every cluster with a node still running, oldest registration first.
    ///
    /// A cluster whose nodes have all ended is not live: its registration outlives its nodes
    /// because the scenario world still holds it, and reporting it would name a cluster that has
    /// nothing left to stop.
    pub(crate) fn live() -> Vec<Self> {
        let clusters = LIVE_CLUSTERS.lock();
        let mut live = Vec::new();
        for cluster in clusters.entries.values() {
            if cluster.nodes.is_empty() {
                continue;
            }
            let mut nodes = Vec::new();
            for node in cluster.nodes.values() {
                nodes.push(node.name.clone());
            }
            live.push(Self {
                scenario: cluster.scenario.clone(),
                nodes,
            });
        }
        live
    }
}

impl fmt::Display for LiveCluster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} nodes=[{}]",
            self.scenario,
            self.nodes.join(", ")
        )
    }
}

/// Asks every live node to stop, and reports how many were asked.
///
/// The stops are collected before any of them is asked, so the registry lock is never held while a
/// node is deciding what a stop request means for it.
fn request_stop_of_every_live_node() -> usize {
    let clusters = LIVE_CLUSTERS.lock();
    let mut stops = Vec::new();
    for cluster in clusters.entries.values() {
        for node in cluster.nodes.values() {
            stops.push(node.stop.clone());
        }
    }
    drop(clusters);

    let asked = stops.len();
    for stop in stops {
        stop.request_stop();
    }
    asked
}

/// What one active scenario was doing when the suite budget expired.
#[derive(Clone, Debug)]
pub(crate) struct StalledScenario {
    pub(crate) active: ActiveScenario,
    /// The nodes of every cluster this scenario has live, named as its clusters hold them.
    pub(crate) nodes: Vec<String>,
}

impl StalledScenario {
    /// Reads every scenario still queued or running together with the clusters it holds.
    ///
    /// An attempt that has finished is not part of what the run was doing: Cucumber keeps the
    /// world of a failed attempt for its report, so its registration stays in the registry long
    /// after the attempt and its retry have ended, but it holds no permit and no cluster.
    fn snapshot(live: &[LiveCluster]) -> Vec<Self> {
        let mut stalled = Vec::new();
        for active in ActiveScenario::unfinished() {
            let mut nodes = Vec::new();
            for cluster in live {
                if cluster.scenario == active.identity {
                    nodes.extend(cluster.nodes.iter().cloned());
                }
            }
            stalled.push(Self { active, nodes });
        }
        stalled
    }
}

impl fmt::Display for StalledScenario {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} nodes=[{}]",
            self.active,
            self.nodes.join(", ")
        )
    }
}

/// The state the suite budget expired in, read before the watchdog changed anything.
#[derive(Clone, Debug)]
pub(crate) struct SuiteStall {
    /// The budget that expired.
    pub(crate) budget: Duration,
    /// Every scenario that had started and not yet finished.
    pub(crate) scenarios: Vec<StalledScenario>,
    /// The live clusters no active scenario claims. A cluster outliving the scenario that started
    /// it is a leak, and naming it is the only record such a cluster has.
    pub(crate) unclaimed: Vec<LiveCluster>,
}

impl SuiteStall {
    fn capture(budget: Duration) -> Self {
        let live = LiveCluster::live();
        let scenarios = StalledScenario::snapshot(&live);
        let mut unclaimed = Vec::new();
        for cluster in live {
            let claimed = scenarios
                .iter()
                .any(|stalled| stalled.active.identity == cluster.scenario);
            if !claimed {
                unclaimed.push(cluster);
            }
        }
        Self {
            budget,
            scenarios,
            unclaimed,
        }
    }
}

impl fmt::Display for SuiteStall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "suite timeout: the {:?} suite budget expired with {} scenario(s) active",
            self.budget,
            self.scenarios.len()
        )?;
        for scenario in &self.scenarios {
            writeln!(formatter, "  suite timeout active: {scenario}")?;
        }
        for cluster in &self.unclaimed {
            writeln!(
                formatter,
                "  suite timeout unclaimed cluster: {cluster}; its scenario had already left the \
                 registry"
            )?;
        }
        Ok(())
    }
}

/// What the bounded cleanup a suite timeout drives did.
#[derive(Clone, Debug)]
pub(crate) struct WatchdogCleanup {
    /// The one window every live node had to end within.
    pub(crate) window: Duration,
    /// How long the cleanup actually took.
    pub(crate) elapsed: Duration,
    /// How many nodes were asked to stop.
    pub(crate) asked: usize,
    /// The clusters still running when the window passed. They end with the runtime.
    pub(crate) still_live: Vec<LiveCluster>,
}

impl WatchdogCleanup {
    /// Asks every live node to stop and waits one `window` for all of them together.
    ///
    /// Every node is asked before any of them is waited for, so a suite holding several wedged
    /// clusters costs one window rather than one window per cluster.
    async fn stop_every_live_node(window: Duration) -> Self {
        let asked = request_stop_of_every_live_node();
        let deadline = PhaseDeadline::after(window);
        loop {
            nervix_primitives::task::consume_budget().await;
            let still_live = LiveCluster::live();
            if still_live.is_empty() || deadline.has_passed() {
                return Self {
                    window,
                    elapsed: deadline.elapsed(),
                    asked,
                    still_live,
                };
            }
            deadline.pause(CLEANUP_POLL_INTERVAL).await;
        }
    }

    /// Whether the window passed with a node still running.
    pub(crate) fn was_forced(&self) -> bool {
        !self.still_live.is_empty()
    }
}

impl fmt::Display for WatchdogCleanup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "suite timeout cleanup: asked {} node(s) to stop and waited {:?} of a {:?} window",
            self.asked, self.elapsed, self.window
        )?;
        if self.still_live.is_empty() {
            return formatter.write_str("; every node ended itself");
        }
        for cluster in &self.still_live {
            write!(formatter, "; still running at the window: {cluster}")?;
        }
        Ok(())
    }
}

/// A scenario run the suite budget ended: the state it expired in, and the cleanup that followed.
#[derive(Clone, Debug)]
pub(crate) struct SuiteTimeout {
    pub(crate) stall: SuiteStall,
    pub(crate) cleanup: WatchdogCleanup,
}

impl fmt::Display for SuiteTimeout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}{}", self.stall, self.cleanup)
    }
}

/// How a scenario run ended.
#[derive(Debug)]
pub(crate) enum SuiteRun<T> {
    /// The run finished inside its budget, with whatever it produced.
    Completed(T),
    /// The budget expired first, and the watchdog ended the run.
    TimedOut(SuiteTimeout),
}

/// The one budget a whole scenario run has, and the bounded cleanup that follows it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SuiteWatchdog {
    budget: Duration,
    cleanup_window: Duration,
}

impl SuiteWatchdog {
    /// A watchdog whose budget and cleanup window are both given, so the path a timeout takes can
    /// be run without spending a suite-length budget on it.
    pub(crate) fn new(budget: Duration, cleanup_window: Duration) -> Self {
        Self {
            budget,
            cleanup_window,
        }
    }

    /// The budget this run was given.
    pub(crate) fn budget(self) -> Duration {
        self.budget
    }

    /// Runs `run` until it finishes or the budget expires, whichever comes first.
    ///
    /// The budget is passed into the wait rather than wrapped around it: a timeout wrapped around
    /// the run would drop it at expiry, and the registries a diagnostic reads live in the worlds
    /// that run owns. So the run is held, read, and asked to stop, and only then dropped.
    #[allow(
        dead_code,
        reason = "harness regressions use this entry point; scenarios report at expiry"
    )]
    pub(crate) async fn bound<F>(self, run: F) -> SuiteRun<F::Output>
    where
        F: Future,
    {
        self.bound_with_timeout_report(run, || {}).await
    }

    /// As [`Self::bound`], but captures a report at the expiry instant, before cleanup changes
    /// the active scenario registry or spends its own window.
    pub(crate) async fn bound_with_timeout_report<F>(
        self,
        run: F,
        report_timeout: impl FnOnce(),
    ) -> SuiteRun<F::Output>
    where
        F: Future,
    {
        // Boxed so the run can be dropped where the watchdog decides rather than wherever this
        // frame happens to end: `pin!` would yield a reference, and dropping a reference leaves
        // the run it points at exactly where it was.
        let mut run = Box::pin(run);
        let budget = PhaseDeadline::after(self.budget);
        let bounded = budget.bound(&mut run).await;
        if let BeforeDeadline::Finished(output) = bounded {
            return SuiteRun::Completed(output);
        }

        // Read before anything is asked to stop, so the diagnostic is the state the budget expired
        // in rather than the state the cleanup left behind. Printed before the cleanup too: the
        // job's own timeout is the guard outside this one, and a process it kills has still said
        // what it was doing.
        let stall = SuiteStall::capture(self.budget);
        eprint!("{stall}");
        report_timeout();
        flush_process_output();

        let cleanup = WatchdogCleanup::stop_every_live_node(self.cleanup_window).await;
        // Dropping the run ends every scenario future the suite still held, which kills the child
        // processes those scenarios started and hands the nodes they still hold to keepers. A
        // node the window could not stop ends when the process drops the runtime.
        drop(run);
        SuiteRun::TimedOut(SuiteTimeout { stall, cleanup })
    }
}

/// What stopping the suite's test dependencies did.
///
/// The run's own result is already known by the time this happens, so the only thing at stake is
/// whether the process ends in time to report that result itself.
#[derive(Debug)]
pub(crate) enum SuiteTeardown {
    /// The dependencies stopped, and every one of them was removed.
    Stopped,
    /// The dependencies stopped, and these failed to stop or to be removed. Each keeps its report
    /// until the teardown is printed.
    Failed(TeardownFailures),
    /// The budget passed with the stop still running, so the suite stopped waiting for it and
    /// left the containers to the runner that owns them.
    Abandoned(Duration),
}

impl SuiteTeardown {
    /// Stops the suite's dependencies within [`DEPENDENCY_SHUTDOWN_BUDGET`].
    ///
    /// The stop is a future this is handed rather than something it knows how to perform: what a
    /// dependency is belongs to whoever started it, and what a bounded ending is belongs here.
    pub(crate) async fn bounded<Stop>(stop: Stop) -> Self
    where
        Stop: Future<Output = Result<(), TeardownFailures>>,
    {
        let deadline = PhaseDeadline::after(DEPENDENCY_SHUTDOWN_BUDGET);
        match deadline.bound(stop).await {
            BeforeDeadline::Finished(Ok(())) => Self::Stopped,
            BeforeDeadline::Finished(Err(failures)) => Self::Failed(failures),
            BeforeDeadline::Passed => Self::Abandoned(DEPENDENCY_SHUTDOWN_BUDGET),
        }
    }

    /// Whether the teardown both finished and had nothing to report.
    pub(crate) fn is_clean(&self) -> bool {
        match self {
            Self::Stopped => true,
            Self::Failed(_) | Self::Abandoned(_) => false,
        }
    }
}

impl fmt::Display for SuiteTeardown {
    /// One line, whatever the teardown met, so the report the process ends with stays one report.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => {
                formatter.write_str("suite dependency teardown stopped every dependency")
            }
            Self::Failed(failures) => {
                write!(formatter, "suite dependency teardown failed: {failures}")
            }
            Self::Abandoned(budget) => write!(
                formatter,
                "suite dependency teardown did not finish within {budget:?} and was left to the \
                 runner"
            ),
        }
    }
}

/// How a whole scenario run ended, and the status the process reports it with.
#[derive(Debug)]
pub(crate) enum SuiteOutcome {
    /// Every scenario ran and none of them failed.
    Passed,
    /// The run finished, and the failures it counted are named here.
    Failed(String),
    /// The suite budget expired before the run finished.
    TimedOut(SuiteTimeout),
}

impl SuiteOutcome {
    /// Ends the process the way this outcome is reported.
    ///
    /// A timeout ends the process rather than unwinding out of `main`: the run was ended by a
    /// clock and not by an assertion, and a panic's status would be read as a failed scenario.
    pub(crate) fn end_process(self) {
        match self {
            Self::Passed => {}
            Self::Failed(message) => panic!("{message}"),
            Self::TimedOut(timeout) => {
                // The state the budget expired in was printed as it was read, before the cleanup
                // that followed could change any of it, so only that cleanup's outcome is new
                // here.
                eprintln!("{}", timeout.cleanup);
                // Nothing runs after this, so what the run has written is flushed here or not at
                // all.
                flush_process_output();
                std::process::exit(SUITE_TIMEOUT_EXIT_STATUS);
            }
        }
    }
}

/// Writes out what the run has printed so far, so a diagnostic survives an ending that runs no
/// destructors.
fn flush_process_output() {
    std::io::stdout()
        .flush()
        .reported("flushing the suite's standard output");
    std::io::stderr()
        .flush()
        .reported("flushing the suite's standard error");
}
