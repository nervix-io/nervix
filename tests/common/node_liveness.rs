//! Liveness observation for a Nervix node run by the in-process test harness.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Readiness probe outcomes, the single owned node task, its terminal outcome, and the
//!   diagnostic produced when startup does not reach readiness.
//! - **Depends on.** The session client used for readiness, the application error returned by the
//!   in-process server, Tokio task ownership, and wall-clock test deadlines.
//! - **Must not know.** Production retry policy, production session internals, or scenario state.

use std::{
    fmt,
    future::Future,
    io,
    time::{Duration, Instant},
};

use error_stack::Report;
use nervix_client_core::{ClientError, CommandOutcomeKind, Diagnostic};
use nervix_models::ClusterNodeName;
use nervix_server::application::AppError;
use thiserror::Error;
use tokio::{task::JoinHandle, time::timeout};
use triomphe::Arc;

/// A failure that prevented the readiness probe from creating its authenticated client session.
#[derive(Debug, Error)]
pub(crate) enum ReadinessConnectionFailure {
    #[error("failed to prepare client connection options")]
    Options(#[source] io::Error),
    #[error("failed to create the client session")]
    Session(#[source] ClientError),
}

/// Everything one readiness probe can observe.
#[derive(Debug)]
pub(crate) enum ReadinessProbeOutcome {
    Ready {
        response_kind: CommandOutcomeKind,
    },
    ConnectionFailed(ReadinessConnectionFailure),
    CommandFailed(ClientError),
    UnsuccessfulResponse {
        response_kind: CommandOutcomeKind,
        message: String,
        diagnostics: Vec<Diagnostic>,
    },
}

impl ReadinessProbeOutcome {
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
            Self::ConnectionFailed(error) => {
                write!(formatter, "connection/session creation failed: {error}")
            }
            Self::CommandFailed(error) => {
                write!(formatter, "readiness command failed: {error}")
            }
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

    /// Wait for the owned task once, aborting and then joining it if the deadline expires.
    pub(crate) async fn wait(&mut self, deadline: Duration) -> NodeTaskWaitOutcome {
        let previous = std::mem::replace(self, Self::NotStarted);
        let mut task = match previous {
            Self::NotStarted => return NodeTaskWaitOutcome::NotStarted,
            Self::Terminal(outcome) => {
                *self = Self::Terminal(outcome.clone());
                return NodeTaskWaitOutcome::AlreadyObserved(outcome);
            }
            Self::Running(task) => task,
        };

        let (outcome, expired) = match timeout(deadline, &mut task).await {
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
    pub(crate) attempt: usize,
    pub(crate) elapsed: Duration,
    pub(crate) failure: NodeStartupFailure,
    pub(crate) task_state: NodeTaskState,
    pub(crate) last_readiness: LastReadinessOutcome,
}

impl NodeStartupError {
    fn report(
        node: &ClusterNodeName,
        attempt: usize,
        started_at: Instant,
        failure: NodeStartupFailure,
        task_state: NodeTaskState,
        last_readiness: LastReadinessOutcome,
    ) -> Report<Self> {
        Report::new(Self {
            node: node.clone(),
            attempt,
            elapsed: started_at.elapsed(),
            failure,
            task_state,
            last_readiness,
        })
    }
}

impl OwnedNodeTask {
    /// Poll readiness until it succeeds, the task terminates, or the startup deadline expires.
    pub(crate) async fn wait_until_ready<P, Probe>(
        &mut self,
        node: &ClusterNodeName,
        attempt: usize,
        startup_timeout: Duration,
        poll_interval: Duration,
        mut probe: P,
    ) -> error_stack::Result<(), NodeStartupError>
    where
        P: FnMut() -> Probe,
        Probe: Future<Output = ReadinessProbeOutcome>,
    {
        let started_at = Instant::now();
        let mut last_readiness = LastReadinessOutcome::NoCompletedProbe;
        let polling = async {
            loop {
                tokio::task::consume_budget().await;
                let task_state = self.inspect().await;
                if !matches!(task_state, NodeTaskState::Running) {
                    return Err((NodeStartupFailure::TaskTerminated, task_state));
                }

                let outcome = probe().await;
                let ready = outcome.is_ready();
                last_readiness = LastReadinessOutcome::Observed(outcome);

                let task_state = self.inspect().await;
                match task_state {
                    NodeTaskState::Running if ready => return Ok(()),
                    NodeTaskState::Running => tokio::time::sleep(poll_interval).await,
                    NodeTaskState::NotStarted | NodeTaskState::Terminal(_) => {
                        return Err((NodeStartupFailure::TaskTerminated, task_state));
                    }
                }
            }
        };

        match timeout(startup_timeout, polling).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err((failure, task_state))) => Err(NodeStartupError::report(
                node,
                attempt,
                started_at,
                failure,
                task_state,
                last_readiness,
            )),
            Err(_) => {
                let task_state = self.inspect().await;
                let failure = match task_state {
                    NodeTaskState::Running => NodeStartupFailure::DeadlineExpired,
                    NodeTaskState::NotStarted | NodeTaskState::Terminal(_) => {
                        NodeStartupFailure::TaskTerminated
                    }
                };
                Err(NodeStartupError::report(
                    node,
                    attempt,
                    started_at,
                    failure,
                    task_state,
                    last_readiness,
                ))
            }
        }
    }
}
