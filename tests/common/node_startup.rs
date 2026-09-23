//! The one bounded budget in which the test harness starts a node.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The absolute per-node startup budget, the attempt limit spent inside it, the slice
//!   a failed attempt's cleanup may take, the pause before the next launch, the classification
//!   that decides whether an attempt may be retried, and the exhaustion diagnostic that carries
//!   every attempt.
//! - **Depends on.** The startup failure, task state and terminal outcomes of the node-liveness
//!   owner, the phase deadline, the harness status timing policy, and the application error an
//!   in-process node returns.
//! - **Must not know.** How a node is configured, launched or torn down, scenario state, or the
//!   product's shutdown deadline.
//!
//! # The advertised worst case
//!
//! One node reaches readiness or fails within [`NODE_STARTUP_BUDGET`], whatever happens inside it.
//! The budget starts before the first launch, and every readiness probe, every cleanup after a
//! failed attempt and every pause between attempts receives only the time left before it. A retry
//! therefore cannot restart the budget, and the product's shutdown watchdog, which is minutes
//! long, is never reached once per retry: a node that never became ready has no drain to finish,
//! so its cleanup is a short slice of what remains and ends in an abort.
//!
//! The budget pays for [`FULL_LENGTH_ATTEMPTS`] attempts at their full length. The further launch
//! [`NODE_START_ATTEMPTS`] allows is reached when an earlier attempt failed fast, which is what a
//! bound address does, and it receives whatever the earlier ones left.
//!
//! A cluster is built one node at a time, so its worst case is [`cluster_startup_budget`]: the
//! per-node budget repeated for every node. A three-node cluster is therefore bounded by three
//! times that budget, where eight launches, each waiting thirty seconds for readiness and then up
//! to five minutes for the product's shutdown watchdog, once bounded a single node by forty-four
//! minutes.
//!
//! What a healthy node actually needs is [`SLOWEST_HEALTHY_NODE_STARTUP`], which one attempt
//! outlasts. The budget bounds what the harness waits for, not what the operating system
//! schedules: a runner that stops running the harness's own tasks for longer than an attempt
//! delays the check that ends it, and the measured startups include such a runner.

use std::{fmt, io};

use error_stack::Report;
use nervix_models::ClusterNodeName;
use nervix_server::application::AppError;
use thiserror::Error;
use tokio::time::Duration;
use triomphe::Arc;

use super::{
    node_liveness::{
        NodeStartupError, NodeStartupFailure, NodeTaskState, NodeTaskTerminalOutcome,
        NodeTaskWaitOutcome,
    },
    phase_deadline::PhaseDeadline,
    status_request::{STATUS_REQUEST_TIMEOUT, STATUS_REQUESTS_PER_STARTUP},
};

/// How long one attempt may wait for the node it launched to answer a readiness probe. A policy
/// input.
pub(crate) const ATTEMPT_READINESS_BUDGET: Duration = Duration::from_secs(36);
/// The slowest a healthy node took from its launch to its first accepted readiness probe while
/// the whole scenario suite ran at the CI concurrency factor of two scenarios per CPU: 24.2s,
/// against a 2.0s median and a 4.3s 99th percentile over the 3,465 startups the run measured. A
/// policy input: measure it again when the suite, its concurrency or node startup changes.
const SLOWEST_HEALTHY_NODE_STARTUP: Duration = Duration::from_millis(24_211);
/// The slice of what remains that cleaning up after a failed attempt may take before the harness
/// stops waiting for the node and aborts its task. A policy input.
const RETRY_CLEANUP_SLICE: Duration = Duration::from_secs(5);
/// How long the harness waits after cleaning up before it launches again, so an address or a peer
/// that was busy has a moment to free up. A policy input.
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
/// How many launches the budget pays for at their full length, so readiness that never arrives is
/// waited for again before the harness gives up. A policy input.
pub(crate) const FULL_LENGTH_ATTEMPTS: u32 = 2;
/// How many times the harness launches one node before it gives up. A policy input: the launches
/// beyond [`FULL_LENGTH_ATTEMPTS`] are what a launch that fails at once leaves room for.
pub(crate) const NODE_START_ATTEMPTS: u32 = 3;
/// The longest a node startup may take before the harness stops advertising a bounded suite. A
/// policy input: the ceiling the derived budget is measured against.
const NODE_STARTUP_CEILING: Duration = Duration::from_secs(90);
/// One attempt and the preparation the next one needs: the readiness it waits for, the cleanup
/// that ends it, and the pause before the following launch.
const ATTEMPT_BUDGET: Duration = match RETRY_CLEANUP_SLICE.checked_add(RETRY_BACKOFF) {
    Some(preparation) => match ATTEMPT_READINESS_BUDGET.checked_add(preparation) {
        Some(budget) => budget,
        None => panic!("one startup attempt and its retry preparation must fit in Duration"),
    },
    None => panic!("retry cleanup and its backoff must fit in Duration"),
};
/// The one absolute budget a node's whole startup has, retries included: the worst case every
/// caller is entitled to advertise.
pub(crate) const NODE_STARTUP_BUDGET: Duration =
    match ATTEMPT_BUDGET.checked_mul(FULL_LENGTH_ATTEMPTS) {
        Some(budget) => budget,
        None => panic!("the full-length startup attempts must fit in Duration"),
    };
const _: () = assert!(
    SLOWEST_HEALTHY_NODE_STARTUP.as_nanos() < ATTEMPT_READINESS_BUDGET.as_nanos(),
    "one attempt must outlast the slowest healthy node startup the harness has measured"
);
const _: () = assert!(
    match STATUS_REQUEST_TIMEOUT.checked_mul(STATUS_REQUESTS_PER_STARTUP) {
        Some(requests) => requests.as_nanos() <= ATTEMPT_READINESS_BUDGET.as_nanos(),
        None => false,
    },
    "a node startup attempt must outlast its stalled readiness requests"
);
const _: () = assert!(
    RETRY_CLEANUP_SLICE.as_nanos() < ATTEMPT_READINESS_BUDGET.as_nanos(),
    "cleaning up after an attempt must stay short beside the attempt it ends"
);
const _: () = assert!(
    FULL_LENGTH_ATTEMPTS >= 2,
    "a budget that pays for one attempt needs no retry policy"
);
const _: () = assert!(
    NODE_START_ATTEMPTS > FULL_LENGTH_ATTEMPTS,
    "the attempt limit must admit the launch a fast failure leaves budget for"
);
const _: () = assert!(
    NODE_STARTUP_BUDGET.as_nanos() <= NODE_STARTUP_CEILING.as_nanos(),
    "the derived startup budget must stay under the ceiling the harness advertises"
);

/// The longest a cluster of `node_count` nodes may take to start. The harness builds a cluster one
/// node at a time, so the cluster's worst case is the per-node budget repeated.
pub(crate) const fn cluster_startup_budget(node_count: u32) -> Duration {
    match NODE_STARTUP_BUDGET.checked_mul(node_count) {
        Some(budget) => budget,
        None => {
            panic!("a test cluster's node count and per-node startup budget must fit in Duration")
        }
    }
}

/// A node the harness can launch, poll for readiness and clean up between attempts.
///
/// The startup owner drives these steps and decides nothing about what a node is; the implementor
/// owns the node's configuration, its listeners and its task.
pub(crate) trait StartableNode {
    /// Launch the node. Everything read here is the harness's own configuration, so a failure ends
    /// the startup rather than retrying it.
    fn launch(&mut self) -> io::Result<()>;

    /// Poll the launched node until it answers a readiness probe, its task terminates, or
    /// `readiness` passes.
    async fn readiness(
        &mut self,
        attempt: u32,
        readiness: PhaseDeadline,
    ) -> error_stack::Result<(), NodeStartupError>;

    /// Stop the node this attempt launched within `cleanup`, aborting and joining its task when
    /// that slice ends first.
    async fn clean_up(&mut self, cleanup: PhaseDeadline) -> AttemptCleanup;

    /// Move the node to freshly allocated ports, so a launch that lost a port race does not
    /// repeat it.
    fn move_to_fresh_ports(&mut self) -> io::Result<()>;
}

/// Whether the harness may launch a node again after an attempt failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::Display)]
pub(crate) enum StartupRetry {
    /// A fresh launch can plausibly produce a different outcome: an address another process held,
    /// a peer that was not listening yet, or readiness that did not arrive in time.
    #[strum(serialize = "transient")]
    Transient,
    /// The node decided this outcome from its configuration, its stored state or its own code, so
    /// the next launch repeats it.
    #[strum(serialize = "terminal")]
    Terminal,
}

/// Why one launch attempt did not produce a ready node.
#[derive(Debug)]
pub(crate) enum AttemptFailure {
    /// The harness could not launch the node at all.
    Launch(io::Error),
    /// The launched node did not reach readiness.
    NotReady(Report<NodeStartupError>),
}

impl AttemptFailure {
    /// Whether launching the node again could plausibly produce a different outcome.
    pub(crate) fn retry(&self) -> StartupRetry {
        match self {
            // Launching reads the harness's own configuration: the addresses it must parse, the
            // test certificate authority, the node's database directory. None of that changes
            // because the harness tried again.
            Self::Launch(_) => StartupRetry::Terminal,
            Self::NotReady(error) => Self::readiness_retry(error.current_context()),
        }
    }

    fn readiness_retry(error: &NodeStartupError) -> StartupRetry {
        match error.failure {
            // Readiness that did not arrive in time is the resource pressure this budget exists
            // for.
            NodeStartupFailure::DeadlineExpired => StartupRetry::Transient,
            NodeStartupFailure::TaskTerminated => match &error.task_state {
                NodeTaskState::Terminal(outcome) => Self::terminal_retry(outcome),
                // A task reported as unstarted or still running did not terminate, so the harness
                // has no ending to classify and must not launch over it.
                NodeTaskState::NotStarted | NodeTaskState::Running => StartupRetry::Terminal,
            },
        }
    }

    fn terminal_retry(outcome: &NodeTaskTerminalOutcome) -> StartupRetry {
        match outcome {
            NodeTaskTerminalOutcome::ApplicationError(error) => {
                Self::application_retry(error.current_context())
            }
            // Nothing asked the node to stop while it was starting, so a clean exit, a panic and a
            // cancellation are all the node's own ending.
            NodeTaskTerminalOutcome::CleanApplicationExit
            | NodeTaskTerminalOutcome::Panic(_)
            | NodeTaskTerminalOutcome::Cancellation(_) => StartupRetry::Terminal,
        }
    }

    fn application_retry(error: &AppError) -> StartupRetry {
        match error {
            // The harness allocates ports from a pool it shares with every concurrent scenario and
            // with sibling worktrees running the same suite, so an address another process took
            // first is the one application error a fresh allocation resolves.
            AppError::BindGrpcListenAddress
            | AppError::BindHttpListenAddress
            | AppError::BindHttpsListenAddress
            | AppError::BindObservabilityListenAddress
            | AppError::BindWebConsoleListenAddress
            | AppError::BindWebConsoleHttpsListenAddress
            // The interconnect transport binds its own listener while it starts.
            | AppError::StartInterconnect => StartupRetry::Transient,
            // Every other application error is the node's decision about its configuration, its
            // stored state or its peers, and it holds for the next launch too.
            _ => StartupRetry::Terminal,
        }
    }
}

impl fmt::Display for AttemptFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Launch(error) => write!(formatter, "the node could not be launched: {error}"),
            Self::NotReady(error) => write!(formatter, "{error:#}"),
        }
    }
}

/// What the harness observed while stopping the node a failed attempt left behind.
#[derive(Debug)]
pub(crate) enum AttemptCleanup {
    /// The attempt failed before it spawned a node task, so there was nothing to stop.
    NothingLaunched,
    /// The node task ended inside the cleanup slice.
    Stopped(Arc<NodeTaskTerminalOutcome>),
    /// The cleanup slice ended first, so the harness aborted the task and joined it.
    AbortedAtDeadline(Arc<NodeTaskTerminalOutcome>),
}

impl From<NodeTaskWaitOutcome> for AttemptCleanup {
    fn from(outcome: NodeTaskWaitOutcome) -> Self {
        match outcome {
            NodeTaskWaitOutcome::NotStarted => Self::NothingLaunched,
            NodeTaskWaitOutcome::AlreadyObserved(outcome)
            | NodeTaskWaitOutcome::Joined(outcome) => Self::Stopped(outcome),
            NodeTaskWaitOutcome::AbortedAtDeadline(outcome) => Self::AbortedAtDeadline(outcome),
        }
    }
}

impl fmt::Display for AttemptCleanup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NothingLaunched => formatter.write_str("nothing was launched"),
            Self::Stopped(outcome) => write!(formatter, "stopped ({outcome})"),
            Self::AbortedAtDeadline(outcome) => {
                write!(formatter, "aborted at the cleanup deadline ({outcome})")
            }
        }
    }
}

/// One launch that did not produce a ready node, and what the harness did about it.
#[derive(Debug)]
pub(crate) struct FailedAttempt {
    pub(crate) attempt: u32,
    /// How much of the startup budget had been spent when this attempt was cleaned up.
    pub(crate) elapsed: Duration,
    pub(crate) failure: AttemptFailure,
    pub(crate) retry: StartupRetry,
    pub(crate) cleanup: AttemptCleanup,
}

impl fmt::Display for FailedAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            attempt,
            elapsed,
            failure,
            retry,
            cleanup,
        } = self;
        write!(
            formatter,
            "attempt {attempt}/{NODE_START_ATTEMPTS} [{retry}] failed {elapsed:?} into the \
             budget: {failure}; cleanup: {cleanup}"
        )
    }
}

/// Every attempt one node's startup spent, in the order they ran.
#[derive(Debug)]
pub(crate) struct StartupAttempts {
    pub(crate) spent: Vec<FailedAttempt>,
}

impl fmt::Display for StartupAttempts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.spent.is_empty() {
            return formatter.write_str("no attempt ran");
        }
        let mut separated = false;
        for attempt in &self.spent {
            if separated {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{attempt}")?;
            separated = true;
        }
        Ok(())
    }
}

/// What ended a node's startup before it became ready.
#[derive(Debug, Error)]
pub(crate) enum StartupEnd {
    #[error("the last attempt failed for a reason a fresh launch repeats")]
    TerminalFailure,
    #[error("every launch the attempt limit allows was spent")]
    AttemptsSpent,
    #[error("the startup budget ran out before another launch could start")]
    BudgetSpent,
    #[error("fresh ports for the next launch could not be allocated: {0}")]
    PortsUnavailable(io::Error),
}

/// Why a node never became ready inside the one budget its startup had.
#[derive(Debug, Error)]
#[error(
    "node '{node}' did not become ready within its {budget:?} startup budget: {end} after \
     {elapsed:?}; {attempts}"
)]
pub(crate) struct NodeStartupExhausted {
    pub(crate) node: ClusterNodeName,
    pub(crate) budget: Duration,
    pub(crate) elapsed: Duration,
    pub(crate) end: StartupEnd,
    pub(crate) attempts: StartupAttempts,
}

/// One node's startup: the single absolute budget it has, and every attempt spent inside it.
pub(crate) struct NodeStartup<'node> {
    node: &'node ClusterNodeName,
    budget: PhaseDeadline,
    attempts: Vec<FailedAttempt>,
}

impl<'node> NodeStartup<'node> {
    /// Starts `target` inside `budget`, launching it again only while that budget and the attempt
    /// limit both allow it.
    ///
    /// `budget` is the node's whole startup: it is started before the first launch, and every
    /// readiness probe, cleanup and pause receives only the time left before it.
    pub(crate) async fn start(
        node: &'node ClusterNodeName,
        target: &mut impl StartableNode,
        budget: PhaseDeadline,
    ) -> Result<(), Report<NodeStartupExhausted>> {
        let startup = Self {
            node,
            budget,
            attempts: Vec::new(),
        };
        startup.run(target).await
    }

    async fn run(
        mut self,
        target: &mut impl StartableNode,
    ) -> Result<(), Report<NodeStartupExhausted>> {
        for attempt in 1..=NODE_START_ATTEMPTS {
            tokio::task::consume_budget().await;
            if self.budget.has_passed() {
                return Err(self.exhausted(StartupEnd::BudgetSpent));
            }

            let failure = match self.attempt(target, attempt).await {
                Ok(()) => {
                    self.report_ready(attempt);
                    return Ok(());
                }
                Err(failure) => failure,
            };
            let retry = failure.retry();
            let cleanup = target
                .clean_up(self.budget.nested(RETRY_CLEANUP_SLICE))
                .await;
            self.record(attempt, failure, retry, cleanup);

            if let StartupRetry::Terminal = retry {
                return Err(self.exhausted(StartupEnd::TerminalFailure));
            }
            if attempt == NODE_START_ATTEMPTS {
                break;
            }
            if let Err(error) = target.move_to_fresh_ports() {
                return Err(self.exhausted(StartupEnd::PortsUnavailable(error)));
            }
            self.budget.pause(RETRY_BACKOFF).await;
        }
        Err(self.exhausted(StartupEnd::AttemptsSpent))
    }

    async fn attempt(
        &self,
        target: &mut impl StartableNode,
        attempt: u32,
    ) -> Result<(), AttemptFailure> {
        if let Err(error) = target.launch() {
            return Err(AttemptFailure::Launch(error));
        }
        let readiness = self.budget.nested(ATTEMPT_READINESS_BUDGET);
        match target.readiness(attempt, readiness).await {
            Ok(()) => Ok(()),
            Err(error) => Err(AttemptFailure::NotReady(error)),
        }
    }

    /// Reports the attempt that produced a ready node and what the whole startup cost by then, so a
    /// run's startup times can be read from its output beside the attempts it spent.
    fn report_ready(&self, attempt: u32) {
        eprintln!(
            "node '{}' startup: ready after {:?} on attempt {attempt}/{NODE_START_ATTEMPTS}; {:?} \
             of its {:?} budget left",
            self.node,
            self.budget.elapsed(),
            self.budget.remaining(),
            self.budget.budget()
        );
    }

    /// Retains a failed attempt and reports the transition, so a run that ends in exhaustion is
    /// already legible while it is still going.
    fn record(
        &mut self,
        attempt: u32,
        failure: AttemptFailure,
        retry: StartupRetry,
        cleanup: AttemptCleanup,
    ) {
        let record = FailedAttempt {
            attempt,
            elapsed: self.budget.elapsed(),
            failure,
            retry,
            cleanup,
        };
        eprintln!(
            "node '{}' startup {record}; {:?} of its {:?} budget left",
            self.node,
            self.budget.remaining(),
            self.budget.budget()
        );
        self.attempts.push(record);
    }

    fn exhausted(self, end: StartupEnd) -> Report<NodeStartupExhausted> {
        Report::new(NodeStartupExhausted {
            node: self.node.clone(),
            budget: self.budget.budget(),
            elapsed: self.budget.elapsed(),
            end,
            attempts: StartupAttempts {
                spent: self.attempts,
            },
        })
    }
}
