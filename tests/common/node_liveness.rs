//! Liveness observation for a Nervix node run by the in-process test harness.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Readiness probe outcomes, the single owned node task, its terminal outcome, and the
//!   diagnostic produced when startup does not reach readiness.
//! - **Depends on.** The bounded status request used for readiness, the application error returned
//!   by the in-process server, Tokio task ownership, and the phase deadline of a startup attempt.
//! - **Must not know.** Production retry policy, production session internals, or scenario state.

use std::{fmt, future::Future, time::Duration};

use error_stack::Report;
use nervix_client_wire::{CommandDisposition, Diagnostic};
use nervix_models::ClusterNodeName;
use nervix_server::application::AppError;
use thiserror::Error;
use tokio::{task::JoinHandle, time::timeout};
use triomphe::Arc;

use super::{
    phase_deadline::PhaseDeadline,
    status_request::{StatusEndpoint, StatusRequestError},
};

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
    /// Asks the node for its cluster status within `phase`. A node that is not the leader is ready
    /// too, because it answered an authenticated command.
    pub(crate) async fn probe(endpoint: &StatusEndpoint, phase: PhaseDeadline) -> Self {
        let outcome = match endpoint.request(phase).await {
            Ok(outcome) => outcome,
            Err(error) => return Self::RequestFailed(error),
        };
        let response_kind = ResponseKind::of(&outcome.disposition);
        match response_kind {
            ResponseKind::Completed | ResponseKind::NotLeader => Self::Ready { response_kind },
            ResponseKind::Failed | ResponseKind::Other => Self::UnsuccessfulResponse {
                response_kind,
                message: outcome.message,
                diagnostics: outcome.diagnostics,
            },
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
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
    Panic(tokio::task::JoinError),
    Cancellation(tokio::task::JoinError),
}

impl NodeTaskTerminalOutcome {
    fn from_join(result: Result<Result<(), Report<AppError>>, tokio::task::JoinError>) -> Self {
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
    Terminal(Arc<NodeTaskTerminalOutcome>),
}

impl fmt::Display for NodeTaskState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted => formatter.write_str("not started"),
            Self::Running => formatter.write_str("running"),
            Self::Terminal(outcome) => write!(formatter, "terminal ({outcome})"),
        }
    }
}

/// The result of consuming a node task during orderly or forced teardown.
#[derive(Debug)]
pub(crate) enum NodeTaskWaitOutcome {
    NotStarted,
    AlreadyObserved(Arc<NodeTaskTerminalOutcome>),
    Joined(Arc<NodeTaskTerminalOutcome>),
    AbortedAtDeadline(Arc<NodeTaskTerminalOutcome>),
}

/// The sole owner of a node's join handle and the outcome obtained from that handle.
#[derive(Debug)]
pub(crate) enum OwnedNodeTask {
    NotStarted,
    Running(JoinHandle<Result<(), Report<AppError>>>),
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
        Self::Running(tokio::spawn(future))
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
            Self::NotStarted | Self::Running(_) => None,
        }
    }

    pub(crate) fn state(&self) -> NodeTaskState {
        match self {
            Self::NotStarted => NodeTaskState::NotStarted,
            Self::Running(_) => NodeTaskState::Running,
            Self::Terminal(outcome) => NodeTaskState::Terminal(outcome.clone()),
        }
    }

    /// Join a task that has finished and retain its typed result for later teardown.
    pub(crate) async fn inspect(&mut self) -> NodeTaskState {
        let finished = match self {
            Self::Running(task) => task.is_finished(),
            Self::NotStarted | Self::Terminal(_) => false,
        };
        if !finished {
            return self.state();
        }

        let previous = std::mem::replace(self, Self::NotStarted);
        let task = match previous {
            Self::Running(task) => task,
            state => {
                *self = state;
                return self.state();
            }
        };
        let outcome = Arc::new(NodeTaskTerminalOutcome::from_join(task.await));
        *self = Self::Terminal(outcome.clone());
        NodeTaskState::Terminal(outcome)
    }

    /// Wait for the owned task once, aborting and then joining it if `deadline` passes.
    ///
    /// The deadline is a value this call receives rather than a timeout wrapped around it: the
    /// wait takes the join handle out of the owner, so a wait cancelled from outside would drop
    /// that handle and leave the task running with nothing left that could abort or join it.
    pub(crate) async fn wait(&mut self, deadline: PhaseDeadline) -> NodeTaskWaitOutcome {
        let previous = std::mem::replace(self, Self::NotStarted);
        let mut task = match previous {
            Self::NotStarted => return NodeTaskWaitOutcome::NotStarted,
            Self::Terminal(outcome) => {
                *self = Self::Terminal(outcome.clone());
                return NodeTaskWaitOutcome::AlreadyObserved(outcome);
            }
            Self::Running(task) => task,
        };

        let (outcome, expired) = match timeout(deadline.remaining(), &mut task).await {
            Ok(result) => (Arc::new(NodeTaskTerminalOutcome::from_join(result)), false),
            Err(_) => {
                task.abort();
                let result = task.await;
                (Arc::new(NodeTaskTerminalOutcome::from_join(result)), true)
            }
        };
        *self = Self::Terminal(outcome.clone());
        if expired {
            NodeTaskWaitOutcome::AbortedAtDeadline(outcome)
        } else {
            NodeTaskWaitOutcome::Joined(outcome)
        }
    }

    /// Abort the still-running task when an async join is impossible, such as from `Drop`.
    pub(crate) fn abort(&mut self) {
        let previous = std::mem::replace(self, Self::NotStarted);
        if let Self::Running(task) = previous {
            task.abort();
        }
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
            tokio::task::consume_budget().await;
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
                NodeTaskState::NotStarted | NodeTaskState::Terminal(_) => {
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
