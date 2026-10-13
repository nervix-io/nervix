//! The record of how the stop of one in-process node went.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** When a node was first asked to stop and when its task ended, where a node that
//!   outlived a wait stood in its own shutdown, read from that node's shutdown coordinator, and the
//!   transition a stop that a scenario waited for prints.
//! - **Depends on.** The product's shutdown coordinator and its phases, the outcome of the node
//!   task, and Tokio's monotonic clock.
//! - **Must not know.** Which scenario stopped the node, what the node holds, or why one of its
//!   phases is slow.

use std::{fmt, time::Duration};

use nervix_primitives::{
    sync::{Arc, watch},
    time::Instant,
};
use nervix_server::application::{ShutdownCoordinator, ShutdownOutcome, ShutdownPhase};

use super::node_liveness::NodeTaskTerminalOutcome;

/// How a stopping node's own shutdown deadline stood when the harness read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProductShutdownDeadline {
    /// No stop request has reached the node, so it has no deadline yet.
    NotRequested,
    /// The node's deadline is still this far ahead.
    Ahead(Duration),
    /// The node's deadline has passed. A server process is ended there by its deadline
    /// supervision; an in-process node has none, and goes on stopping.
    Passed,
}

/// Where a node's own shutdown stood when the harness read it: the phase its coordinator had
/// reached, and its product deadline.
///
/// A node that outlives a harness wait is named with this, so a reader can tell a drain that is
/// still inside its deadline from a terminal teardown that has outlasted it without a debugger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NodeStopProgress {
    pub(crate) phase: ShutdownPhase,
    pub(crate) deadline: ProductShutdownDeadline,
}

impl NodeStopProgress {
    /// Reads the phase and the deadline from a node's coordinator. Reading changes nothing in the
    /// node.
    pub(crate) fn observe(shutdown: &ShutdownCoordinator) -> Self {
        let deadline = match shutdown.request() {
            None => ProductShutdownDeadline::NotRequested,
            Some(request) => {
                let deadline = request.deadline();
                if deadline.has_passed() {
                    ProductShutdownDeadline::Passed
                } else {
                    ProductShutdownDeadline::Ahead(deadline.remaining())
                }
            }
        };
        Self {
            phase: shutdown.phase(),
            deadline,
        }
    }
}

impl fmt::Display for NodeStopProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "in shutdown phase {:?}, ", self.phase)?;
        match self.deadline {
            ProductShutdownDeadline::NotRequested => {
                formatter.write_str("with no stop request accepted")
            }
            ProductShutdownDeadline::Ahead(remaining) => {
                write!(formatter, "{remaining:?} before its own shutdown deadline")
            }
            ProductShutdownDeadline::Passed => {
                formatter.write_str("past its own shutdown deadline")
            }
        }
    }
}

/// The half of a [`NodeStopClock`] the node's task holds. Dropping it records when the task ended,
/// whether the task returned, failed or panicked.
#[derive(Debug)]
pub(crate) struct NodeEndRecorder {
    ended_at: watch::Sender<Option<Instant>>,
}

impl Drop for NodeEndRecorder {
    fn drop(&mut self) {
        self.ended_at.send_replace(Some(Instant::now()));
    }
}

/// When one launch of a node was first asked to stop, and when its task ended.
///
/// The task records its own ending, so the time a stop took is the node's and not the harness's:
/// a cluster's stops are requested together and awaited in turn, and a node that had ended before
/// its turn came is still reported with the moment it ended.
#[derive(Debug)]
pub(crate) struct NodeStopClock {
    requested_at: Option<Instant>,
    ended_at: watch::Receiver<Option<Instant>>,
}

impl NodeStopClock {
    /// The clock of a node whose task `recorder` goes to. The task owns the recorder for as long
    /// as it runs.
    pub(crate) fn start() -> (Self, NodeEndRecorder) {
        let (ended_at, ended) = watch::channel(None);
        let clock = Self {
            requested_at: None,
            ended_at: ended,
        };
        (clock, NodeEndRecorder { ended_at })
    }

    /// Records the first stop request made of this launch. A later request changes nothing, as it
    /// changes nothing in the node.
    pub(crate) fn stop_requested(&mut self) {
        if self.requested_at.is_none() {
            self.requested_at = Some(Instant::now());
        }
    }

    /// How long the node has been stopping: from its first stop request to the ending of its task,
    /// or to now while the task still runs. `None` when the harness never asked this launch to
    /// stop, as for a node that ended by itself.
    pub(crate) fn stopping_for(&self) -> Option<Duration> {
        let requested_at = self.requested_at?;
        let ended_at = *self.ended_at.borrow();
        let until = match ended_at {
            Some(ended_at) => ended_at,
            None => Instant::now(),
        };
        match until.checked_duration_since(requested_at) {
            Some(stopping_for) => Some(stopping_for),
            // The task had already ended when the stop was requested, so the stop took no time.
            None => Some(Duration::ZERO),
        }
    }
}

/// How a stop that a scenario waited for ended.
#[derive(Debug)]
pub(crate) enum NodeStopEnding {
    /// The node's task ended inside the watchdog, with the outcome of each shutdown phase when the
    /// node's coordinator reported one.
    Ended {
        task: Arc<NodeTaskTerminalOutcome>,
        shutdown: Option<ShutdownOutcome>,
    },
    /// The watchdog passed with the node still stopping, in the state its coordinator was read in.
    StillStopping(NodeStopProgress),
    /// The node had already been left stopping under a keeper, which holds its coordinator and
    /// waits for it to end, so the wait had nothing left to read.
    LeftToKeeper,
}

/// One stop a scenario requested and waited for: the transition the harness prints as it happens,
/// so a run whose stops are slowing down is legible while it is still going.
#[derive(Debug)]
pub(crate) struct NodeStopRecord {
    pub(crate) node: String,
    /// How long the node took to end, or had been stopping when its watchdog passed, counted from
    /// the first stop request the harness made of it. `None` when the harness made none.
    pub(crate) stopping_for: Option<Duration>,
    /// The watchdog the wait ran under.
    pub(crate) watchdog: Duration,
    pub(crate) ending: NodeStopEnding,
}

impl fmt::Display for NodeStopRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            node,
            stopping_for,
            watchdog,
            ending,
        } = self;
        write!(formatter, "node '{node}' stop: ")?;
        match ending {
            NodeStopEnding::Ended { task, shutdown } => {
                match stopping_for {
                    Some(stopping_for) => {
                        write!(formatter, "ended {stopping_for:?} after its stop request")?;
                    }
                    None => formatter.write_str("ended without a stop request from the harness")?,
                }
                write!(formatter, ", inside its {watchdog:?} watchdog: {task}")?;
                match shutdown {
                    Some(outcome) => write!(
                        formatter,
                        "; shutdown phases: stop admission {:?}, drain support {:?}, terminal \
                         teardown {:?}",
                        outcome.stop_admission, outcome.drain_support, outcome.terminal_teardown
                    ),
                    None => formatter.write_str("; no shutdown outcome was reported"),
                }
            }
            NodeStopEnding::StillStopping(progress) => {
                Self::write_still_stopping(formatter, *stopping_for)?;
                write!(
                    formatter,
                    ", at the end of its {watchdog:?} watchdog, {progress}"
                )
            }
            NodeStopEnding::LeftToKeeper => {
                Self::write_still_stopping(formatter, *stopping_for)?;
                formatter
                    .write_str("; it had already been left to a keeper, which waits for it to end")
            }
        }
    }
}

impl NodeStopRecord {
    fn write_still_stopping(
        formatter: &mut fmt::Formatter<'_>,
        stopping_for: Option<Duration>,
    ) -> fmt::Result {
        match stopping_for {
            Some(stopping_for) => {
                write!(
                    formatter,
                    "still stopping {stopping_for:?} after its stop request"
                )
            }
            None => formatter.write_str("still running without a stop request from the harness"),
        }
    }
}
