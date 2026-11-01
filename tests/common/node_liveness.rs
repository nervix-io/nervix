//! Liveness observation for a Nervix node run by the in-process test harness.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Readiness probe outcomes, the bounded history of the outcomes one readiness wait
//!   saw, the single owned node task, its terminal outcome, and the diagnostic produced when
//!   startup does not reach readiness.
//! - **Depends on.** The bounded status request used for readiness, the application error returned
//!   by the in-process server, Tokio task ownership, and the phase deadline of a startup attempt.
//! - **Must not know.** Production retry policy, production session internals, or scenario state.

use std::{collections::VecDeque, fmt, future::Future, time::Duration};

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_client_wire::{CommandDisposition, Diagnostic};
use nervix_models::ClusterNodeName;
use nervix_primitives::{sync::Arc, task::JoinHandle, time::timeout};
use nervix_server::application::AppError;
use thiserror::Error;

use super::{
    phase_deadline::PhaseDeadline,
    status_request::{StatusEndpoint, StatusRequestEnding, StatusRequestError},
};

/// How many runs of probes a readiness history keeps from the start of its wait, and how many of
/// the latest runs it keeps after those. Policy inputs: a startup normally passes through two or
/// three kinds of answer, so the bounds only shorten the history of a node whose answers keep
/// changing.
const READINESS_HISTORY_FIRST_RUNS: usize = 8;
const READINESS_HISTORY_LATEST_RUNS: usize = 8;

/// What a node answered a readiness probe's status command with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseKind {
    Completed,
    NotLeader,
    Failed,
    /// Any other disposition, which a status command is not expected to produce.
    Other,
}

impl ResponseKind {
    fn of(disposition: &CommandDisposition) -> Self {
        match disposition {
            CommandDisposition::Completed { .. } => Self::Completed,
            CommandDisposition::NotLeader(_) => Self::NotLeader,
            CommandDisposition::Failed => Self::Failed,
            CommandDisposition::TransactionDetached { .. }
            | CommandDisposition::TransactionTakenOver { .. }
            | CommandDisposition::OutcomeUnknown(_)
            | CommandDisposition::ExecutionReferenceConflict(_)
            | CommandDisposition::ExecutionReferenceExpired
            | CommandDisposition::PreviewStale { .. } => Self::Other,
        }
    }
}

/// Everything one readiness probe can observe.
#[derive(Debug)]
pub(crate) enum ReadinessProbeOutcome {
    Ready {
        response_kind: ResponseKind,
    },
    /// The status request ended without a command result, including the operation its deadline
    /// interrupted when the deadline passed.
    RequestFailed(Report<StatusRequestError>),
    UnsuccessfulResponse {
        response_kind: ResponseKind,
        message: String,
        diagnostics: Vec<Diagnostic>,
    },
}

impl ReadinessProbeOutcome {
    /// Waits only for public status observation. Scenarios that examine an initializing node
    /// must be able to observe it before its live policy and routes can be installed.
    pub(crate) async fn probe_observation(endpoint: &StatusEndpoint, phase: PhaseDeadline) -> Self {
        match Self::probe(endpoint, phase).await {
            Self::UnsuccessfulResponse {
                response_kind: ResponseKind::Completed,
                ..
            } => Self::Ready {
                response_kind: ResponseKind::Completed,
            },
            outcome => outcome,
        }
    }

    /// Asks the node for cluster status within `phase`. Public observation starts before
    /// initialization completes, so a successful reply must publish completed initialization.
    pub(crate) async fn probe(endpoint: &StatusEndpoint, phase: PhaseDeadline) -> Self {
        let outcome = match endpoint.request(phase).await {
            Ok(outcome) => outcome,
            Err(error) => return Self::RequestFailed(error),
        };
        let response_kind = ResponseKind::of(&outcome.disposition);
        match response_kind {
            ResponseKind::Completed
                if outcome
                    .message
                    .lines()
                    .any(|line| line == "node.initialization=ready") =>
            {
                Self::Ready { response_kind }
            }
            _ => Self::UnsuccessfulResponse {
                response_kind,
                message: outcome.message,
                diagnostics: outcome.diagnostics,
            },
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    /// What kind of answer the probe got.
    pub(crate) fn kind(&self) -> ReadinessProbeKind {
        match self {
            Self::Ready { response_kind } => ReadinessProbeKind::Ready(*response_kind),
            Self::RequestFailed(error) => {
                ReadinessProbeKind::RequestEnded(error.current_context().ending())
            }
            Self::UnsuccessfulResponse { response_kind, .. } => {
                ReadinessProbeKind::UnsuccessfulResponse(*response_kind)
            }
        }
    }
}

/// What kind of answer one readiness probe got, without the details that differ between probes of
/// the same kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadinessProbeKind {
    Ready(ResponseKind),
    /// The status request ended without a command result, where its ending says.
    RequestEnded(StatusRequestEnding),
    UnsuccessfulResponse(ResponseKind),
}

impl fmt::Display for ReadinessProbeKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready(response_kind) => write!(formatter, "ready response ({response_kind:?})"),
            Self::RequestEnded(ending) => write!(formatter, "status request: {ending}"),
            Self::UnsuccessfulResponse(response_kind) => {
                write!(formatter, "non-ready response ({response_kind:?})")
            }
        }
    }
}

/// One run of consecutive readiness probes that got the same kind of answer.
#[derive(Clone, Copy, Debug)]
struct ReadinessRun {
    kind: ReadinessProbeKind,
    probes: u64,
    /// When the run's first probe ended, measured from the start of the wait's phase.
    first_at: Duration,
    /// When the run's last probe ended, measured the same way.
    last_at: Duration,
}

impl fmt::Display for ReadinessRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:.1?}–{:.1?} {} ×{}",
            self.first_at, self.last_at, self.kind, self.probes
        )
    }
}

/// The runs a readiness history dropped between its first and its latest runs.
#[derive(Clone, Copy, Debug, Default)]
struct OmittedRuns {
    runs: u64,
    probes: u64,
}

/// The readiness probes of one wait, as runs of consecutive probes that got the same kind of
/// answer, with when each run began and ended, and the last probe's outcome in full.
///
/// It keeps the first runs of the wait and the latest ones and counts the runs it dropped between
/// them, so it stays bounded however long the wait lasts and however often the answers change.
#[derive(Debug)]
pub(crate) struct ReadinessHistory {
    first: Vec<ReadinessRun>,
    latest: VecDeque<ReadinessRun>,
    omitted: OmittedRuns,
    last: LastReadinessOutcome,
}

impl ReadinessHistory {
    pub(crate) fn new() -> Self {
        Self {
            first: Vec::with_capacity(READINESS_HISTORY_FIRST_RUNS),
            latest: VecDeque::with_capacity(READINESS_HISTORY_LATEST_RUNS),
            omitted: OmittedRuns::default(),
            last: LastReadinessOutcome::NoCompletedProbe,
        }
    }

    /// Records one probe's outcome. `at` is when the probe ended, measured from the start of the
    /// wait's phase.
    pub(crate) fn record(&mut self, at: Duration, outcome: ReadinessProbeOutcome) {
        let kind = outcome.kind();
        self.last = LastReadinessOutcome::Observed(outcome);
        let current = match self.latest.back_mut() {
            Some(run) => Some(run),
            None => self.first.last_mut(),
        };
        if let Some(run) = current
            && run.kind == kind
        {
            run.probes = run
                .probes
                .checked_add(1)
                .assured("a wait ends long before it could make u64::MAX probes");
            run.last_at = at;
            return;
        }
        let run = ReadinessRun {
            kind,
            probes: 1,
            first_at: at,
            last_at: at,
        };
        if self.first.len() < READINESS_HISTORY_FIRST_RUNS {
            self.first.push(run);
            return;
        }
        if self.latest.len() == READINESS_HISTORY_LATEST_RUNS
            && let Some(dropped) = self.latest.pop_front()
        {
            self.omitted.runs = self
                .omitted
                .runs
                .checked_add(1)
                .assured("a wait ends long before it could make u64::MAX probes");
            self.omitted.probes = self
                .omitted
                .probes
                .checked_add(dropped.probes)
                .assured("a wait ends long before it could make u64::MAX probes");
        }
        self.latest.push_back(run);
    }

    /// The outcome of the last probe recorded, in full.
    pub(crate) fn last(&self) -> &LastReadinessOutcome {
        &self.last
    }
}

impl fmt::Display for ReadinessHistory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.first.is_empty() {
            return formatter.write_str("no readiness probe completed");
        }
        let mut separator = "";
        for run in &self.first {
            write!(formatter, "{separator}{run}")?;
            separator = "; ";
        }
        if self.omitted.runs > 0 {
            write!(
                formatter,
                "; {} runs of {} probes omitted",
                self.omitted.runs, self.omitted.probes
            )?;
        }
        for run in &self.latest {
            write!(formatter, "; {run}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ReadinessProbeOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready { response_kind } => {
                write!(formatter, "ready response ({response_kind:?})")
            }
            Self::RequestFailed(error) => write!(formatter, "status request failed: {error:#}"),
            Self::UnsuccessfulResponse {
                response_kind,
                message,
                diagnostics,
            } => write!(
                formatter,
                "non-ready response ({response_kind:?}): {message}; {} diagnostic(s)",
                diagnostics.len()
            ),
        }
    }
}

/// The one terminal result obtained by joining a spawned in-process node task.
#[derive(Debug)]
pub(crate) enum NodeTaskTerminalOutcome {
    CleanApplicationExit,
    ApplicationError(Report<AppError>),
    Panic(nervix_primitives::task::JoinError),
    Cancellation(nervix_primitives::task::JoinError),
}

impl NodeTaskTerminalOutcome {
    fn from_join(
        result: Result<Result<(), Report<AppError>>, nervix_primitives::task::JoinError>,
    ) -> Self {
        match result {
            Ok(Ok(())) => Self::CleanApplicationExit,
            Ok(Err(error)) => Self::ApplicationError(error),
            Err(error) if error.is_panic() => Self::Panic(error),
            Err(error) => Self::Cancellation(error),
        }
    }
}

impl fmt::Display for NodeTaskTerminalOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CleanApplicationExit => formatter.write_str("clean application exit"),
            Self::ApplicationError(error) => write!(formatter, "application error: {error:?}"),
            Self::Panic(error) => write!(formatter, "panic: {error}"),
            Self::Cancellation(error) => write!(formatter, "cancellation: {error}"),
        }
    }
}

/// The current state a startup diagnostic can report without inferring it from an error string.
#[derive(Clone, Debug)]
pub(crate) enum NodeTaskState {
    NotStarted,
    Running,
    /// The task outlived its owner's wait and was handed to a keeper, which waits for its end.
    LeftStopping,
    Terminal(Arc<NodeTaskTerminalOutcome>),
}

impl fmt::Display for NodeTaskState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted => formatter.write_str("not started"),
            Self::Running => formatter.write_str("running"),
            Self::LeftStopping => formatter.write_str("left stopping"),
            Self::Terminal(outcome) => write!(formatter, "terminal ({outcome})"),
        }
    }
}

/// The result of waiting for a node task within a deadline.
#[derive(Debug)]
pub(crate) enum NodeTaskWaitOutcome {
    NotStarted,
    AlreadyObserved(Arc<NodeTaskTerminalOutcome>),
    Joined(Arc<NodeTaskTerminalOutcome>),
    /// The deadline passed while the task was still running. Its owner still holds it.
    StillRunning,
    /// The task had already been handed to a keeper, which waits for its end.
    LeftStopping,
}

/// The sole owner of a node's join handle and the outcome obtained from that handle.
///
/// A node's task is the future the node was started as, not the node: the node spawns the tasks
/// that hold its databases, its listeners and its peers, and they outlive that future when it is
/// dropped. Aborting the task would therefore leave a node running that nothing owns and nothing
/// can join. This owner never aborts. A node ends by its own shutdown, and a node that has not
/// ended when a wait's deadline passes stays owned, here or by the keeper it is handed to.
#[derive(Debug)]
pub(crate) enum OwnedNodeTask {
    NotStarted,
    Running(JoinHandle<Result<(), Report<AppError>>>),
    /// The running task was handed to a keeper, which waits for its end.
    LeftStopping,
    Terminal(Arc<NodeTaskTerminalOutcome>),
}

impl OwnedNodeTask {
    pub(crate) fn not_started() -> Self {
        Self::NotStarted
    }

    pub(crate) fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = Result<(), Report<AppError>>> + Send + 'static,
    {
        Self::Running(nervix_primitives::task::spawn(future))
    }

    pub(crate) fn is_running(&self) -> bool {
        matches!(self, Self::Running(_))
    }

    pub(crate) fn application_error(&self) -> Option<&Report<AppError>> {
        match self {
            Self::Terminal(outcome) => match outcome.as_ref() {
                NodeTaskTerminalOutcome::ApplicationError(error) => Some(error),
                NodeTaskTerminalOutcome::CleanApplicationExit
                | NodeTaskTerminalOutcome::Panic(_)
                | NodeTaskTerminalOutcome::Cancellation(_) => None,
            },
            Self::NotStarted | Self::Running(_) | Self::LeftStopping => None,
        }
    }

    pub(crate) fn state(&self) -> NodeTaskState {
        match self {
            Self::NotStarted => NodeTaskState::NotStarted,
            Self::Running(_) => NodeTaskState::Running,
            Self::LeftStopping => NodeTaskState::LeftStopping,
            Self::Terminal(outcome) => NodeTaskState::Terminal(outcome.clone()),
        }
    }

    /// Join a task that has finished and retain its typed result for later teardown.
    pub(crate) async fn inspect(&mut self) -> NodeTaskState {
        let Self::Running(task) = self else {
            return self.state();
        };
        if !task.is_finished() {
            return NodeTaskState::Running;
        }
        // The handle stays with this owner across the join, so a caller that stops polling here
        // leaves a task that is still owned.
        let outcome = Arc::new(NodeTaskTerminalOutcome::from_join(task.await));
        *self = Self::Terminal(outcome.clone());
        NodeTaskState::Terminal(outcome)
    }

    /// Wait for the owned task until `deadline`, and leave it running with this owner when the
    /// deadline passes first.
    ///
    /// The join handle never leaves the owner while it waits, so a wait that is cancelled from
    /// outside, like one whose deadline passes, leaves a task that is still owned.
    pub(crate) async fn wait(&mut self, deadline: PhaseDeadline) -> NodeTaskWaitOutcome {
        let task = match self {
            Self::NotStarted => return NodeTaskWaitOutcome::NotStarted,
            Self::LeftStopping => return NodeTaskWaitOutcome::LeftStopping,
            Self::Terminal(outcome) => {
                return NodeTaskWaitOutcome::AlreadyObserved(outcome.clone());
            }
            Self::Running(task) => task,
        };
        let joined = match timeout(deadline.remaining(), task).await {
            Ok(joined) => joined,
            Err(_) => return NodeTaskWaitOutcome::StillRunning,
        };
        let outcome = Arc::new(NodeTaskTerminalOutcome::from_join(joined));
        *self = Self::Terminal(outcome.clone());
        NodeTaskWaitOutcome::Joined(outcome)
    }

    /// Hand a task that is still running to a keeper, which waits for the node to end and then
    /// runs `ended` with how it ended. `ended` gives back what the node held until then: its
    /// storage, its ports and its registrations.
    ///
    /// Returns whether a running task was handed over. A task in any other state has nothing left
    /// to wait for, and `ended` is dropped unrun.
    pub(crate) fn keep_until_ended<Ended>(&mut self, ended: Ended) -> bool
    where
        Ended: FnOnce(&NodeTaskTerminalOutcome) + Send + 'static,
    {
        let previous = std::mem::replace(self, Self::LeftStopping);
        let task = match previous {
            Self::Running(task) => task,
            state @ (Self::NotStarted | Self::LeftStopping | Self::Terminal(_)) => {
                *self = state;
                return false;
            }
        };
        // The keeper is the task's owner from here on. Nothing joins the keeper: it ends when the
        // node does, or with the runtime that runs them both.
        nervix_primitives::task::spawn(async move {
            let outcome = NodeTaskTerminalOutcome::from_join(task.await);
            ended(&outcome);
        });
        true
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeStartupFailure {
    DeadlineExpired,
    TaskTerminated,
}

impl fmt::Display for NodeStartupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExpired => formatter.write_str("startup deadline expired"),
            Self::TaskTerminated => formatter.write_str("node task terminated before readiness"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum LastReadinessOutcome {
    NoCompletedProbe,
    Observed(ReadinessProbeOutcome),
}

impl fmt::Display for LastReadinessOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoCompletedProbe => formatter.write_str("no readiness probe completed"),
            Self::Observed(outcome) => outcome.fmt(formatter),
        }
    }
}

/// The semantic startup failure returned by the in-process node harness.
#[derive(Debug, Error)]
#[error(
    "node '{node}' startup attempt {attempt} failed after {elapsed:?}: {failure}; task state: \
     {task_state}; last readiness outcome: {last_readiness}"
)]
pub(crate) struct NodeStartupError {
    pub(crate) node: ClusterNodeName,
    pub(crate) attempt: u32,
    pub(crate) elapsed: Duration,
    pub(crate) failure: NodeStartupFailure,
    pub(crate) task_state: NodeTaskState,
    pub(crate) last_readiness: LastReadinessOutcome,
}

impl NodeStartupError {
    fn report(
        node: &ClusterNodeName,
        attempt: u32,
        deadline: PhaseDeadline,
        failure: NodeStartupFailure,
        task_state: NodeTaskState,
        last_readiness: LastReadinessOutcome,
    ) -> Report<Self> {
        Report::new(Self {
            node: node.clone(),
            attempt,
            elapsed: deadline.elapsed(),
            failure,
            task_state,
            last_readiness,
        })
    }
}

impl OwnedNodeTask {
    /// Poll readiness until it succeeds, the task terminates, or the startup deadline passes.
    ///
    /// Every probe receives `deadline` and must finish by it, so a probe that never replies ends
    /// the wait at the deadline and becomes the last readiness outcome the failure reports.
    pub(crate) async fn wait_until_ready<P, Probe>(
        &mut self,
        node: &ClusterNodeName,
        attempt: u32,
        deadline: PhaseDeadline,
        poll_interval: Duration,
        mut probe: P,
    ) -> error_stack::Result<(), NodeStartupError>
    where
        P: FnMut(PhaseDeadline) -> Probe,
        Probe: Future<Output = ReadinessProbeOutcome>,
    {
        let mut last_readiness = LastReadinessOutcome::NoCompletedProbe;
        loop {
            nervix_primitives::task::consume_budget().await;
            let task_state = self.inspect().await;
            if !matches!(task_state, NodeTaskState::Running) {
                return Err(NodeStartupError::report(
                    node,
                    attempt,
                    deadline,
                    NodeStartupFailure::TaskTerminated,
                    task_state,
                    last_readiness,
                ));
            }
            if deadline.has_passed() {
                return Err(NodeStartupError::report(
                    node,
                    attempt,
                    deadline,
                    NodeStartupFailure::DeadlineExpired,
                    task_state,
                    last_readiness,
                ));
            }

            let outcome = probe(deadline).await;
            let ready = outcome.is_ready();
            last_readiness = LastReadinessOutcome::Observed(outcome);

            let task_state = self.inspect().await;
            match task_state {
                NodeTaskState::Running if ready => return Ok(()),
                NodeTaskState::Running => deadline.pause(poll_interval).await,
                NodeTaskState::NotStarted
                | NodeTaskState::LeftStopping
                | NodeTaskState::Terminal(_) => {
                    return Err(NodeStartupError::report(
                        node,
                        attempt,
                        deadline,
                        NodeStartupFailure::TaskTerminated,
                        task_state,
                        last_readiness,
                    ));
                }
            }
        }
    }
}
