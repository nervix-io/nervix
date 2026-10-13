//! The bounded cleanup of the cluster one scenario started.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The one deadline a whole cluster stops within, asking every node to stop before
//!   awaiting any of them, leaving whatever is still stopping when that deadline passes to end
//!   itself, the release of what the ended nodes held, and the record of what each node did.
//! - **Depends on.** The owned node task and the phase deadline.
//! - **Must not know.** What a node is, how it was configured, or what it holds.

use std::{fmt, time::Duration};

use futures_util::future::join_all;
use meticulous::OptionExt as _;

use super::{
    node_liveness::{NodeTaskTerminalOutcome, NodeTaskWaitOutcome, OwnedNodeTask},
    phase_deadline::PhaseDeadline,
};

/// The slowest a whole cluster stopped itself after cleanup asked it to, rounded up, over 163
/// cleanups of the cluster scenarios, most of them run beside a second full suite on the same
/// machine: a median of 0.15 seconds, 1.6 at the ninetieth percentile and 12.2 at the slowest,
/// which was a graceful-shutdown scenario whose nodes drain. A policy input: measure it again when
/// the suite or its concurrency changes.
const SLOWEST_HEALTHY_CLUSTER_STOP: Duration = Duration::from_secs(15);
/// How many times the slowest healthy stop cleanup waits before it stops waiting for a cluster, so
/// a runner slower than the measuring one still sees its own nodes stop. A policy input.
const CLUSTER_STOP_HEADROOM: u32 = 4;
const _: () = assert!(
    CLUSTER_STOP_HEADROOM >= 2,
    "a cleanup budget must leave the slowest healthy stop its headroom"
);
/// The one deadline every node of one cluster stops within during scenario cleanup.
///
/// Cleanup runs after the scenario's assertions, so this bounds the harness and not the product:
/// a scenario that asserts on shutdown, drain or deadline expiry stops its nodes itself, within
/// the product deadlines it configured. It is a whole-cluster budget rather than a per-node one,
/// so cleanup takes as long for a three-node cluster as for a single node.
pub(crate) const CLUSTER_TEARDOWN_BUDGET: Duration =
    match SLOWEST_HEALTHY_CLUSTER_STOP.checked_mul(CLUSTER_STOP_HEADROOM) {
        Some(budget) => budget,
        None => panic!("the scenario cleanup budget must fit in Duration"),
    };
const _: () = assert!(
    CLUSTER_TEARDOWN_BUDGET.as_nanos() > SLOWEST_HEALTHY_CLUSTER_STOP.as_nanos(),
    "a cluster that stops as slowly as the slowest healthy one must still stop itself"
);

/// A node one cluster teardown stops.
///
/// The teardown owns the order: every node is asked to stop before any node is awaited, every
/// node's task is awaited under the one cluster deadline, and what a node holds goes back only
/// once its own task has ended.
pub(crate) trait TeardownNode {
    /// How this node is named in the teardown record.
    fn node_name(&self) -> String;

    /// Ask this node to stop, returning without waiting for it.
    fn request_stop(&mut self);

    /// The node's single owned task. The teardown awaits it through this one handle, which keeps
    /// the task whether the wait ends, is cancelled, or runs out of time.
    fn owned_task(&mut self) -> &mut OwnedNodeTask;

    /// Give back the harness state this node holds: its port leases, the fault injection it
    /// registered, and anything else the next scenario must not find taken. The teardown calls it
    /// once per node whose task has ended, whatever ended it.
    fn release(&mut self);

    /// Leave this node, which was still stopping at the cleanup deadline, to end itself. The node
    /// hands its task, and everything that task still uses, to a keeper that outlives the
    /// cluster and gives them back once the task has ended. The teardown calls it instead of
    /// [`Self::release`], so nothing a running node uses is taken from under it.
    fn leave_stopping(&mut self);
}

/// What the teardown did with one node.
#[derive(Debug)]
pub(crate) struct NodeTeardown {
    pub(crate) node: String,
    pub(crate) stop: NodeTaskWaitOutcome,
}

impl NodeTeardown {
    /// Whether the cluster deadline passed with this node still stopping, so the teardown left it
    /// to end itself with what it holds.
    pub(crate) fn is_still_stopping(&self) -> bool {
        match self.stop {
            NodeTaskWaitOutcome::StillRunning | NodeTaskWaitOutcome::LeftStopping => true,
            NodeTaskWaitOutcome::NotStarted
            | NodeTaskWaitOutcome::AlreadyObserved(_)
            | NodeTaskWaitOutcome::Joined(_) => false,
        }
    }

    /// The node's own failure, when its task ended in one. A node that returned an application
    /// error reports it to the scenario that asked for it; a node that panicked has no other
    /// witness than this record.
    pub(crate) fn panic(&self) -> Option<&NodeTaskTerminalOutcome> {
        let outcome = match &self.stop {
            NodeTaskWaitOutcome::AlreadyObserved(outcome)
            | NodeTaskWaitOutcome::Joined(outcome) => outcome.as_ref(),
            NodeTaskWaitOutcome::NotStarted
            | NodeTaskWaitOutcome::StillRunning
            | NodeTaskWaitOutcome::LeftStopping => return None,
        };
        match outcome {
            NodeTaskTerminalOutcome::Panic(_) => Some(outcome),
            NodeTaskTerminalOutcome::CleanApplicationExit
            | NodeTaskTerminalOutcome::ApplicationError(_)
            | NodeTaskTerminalOutcome::Cancellation(_) => None,
        }
    }
}

impl fmt::Display for NodeTeardown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "node {:?} ", self.node)?;
        match &self.stop {
            NodeTaskWaitOutcome::NotStarted => formatter.write_str("was never started"),
            NodeTaskWaitOutcome::AlreadyObserved(outcome) => {
                write!(formatter, "had already ended with {outcome}")
            }
            NodeTaskWaitOutcome::Joined(outcome) => write!(formatter, "ended with {outcome}"),
            NodeTaskWaitOutcome::StillRunning => formatter.write_str(
                "was still stopping at the cleanup deadline and keeps its storage and ports until \
                 it ends",
            ),
            NodeTaskWaitOutcome::LeftStopping => formatter.write_str(
                "had already been left stopping and keeps its storage and ports until it ends",
            ),
        }
    }
}

/// What one scenario's cluster teardown did.
#[derive(Debug)]
pub(crate) struct ClusterTeardown {
    /// The one deadline every node of the cluster had to stop within.
    pub(crate) budget: Duration,
    /// How long the whole cleanup took, including the releases that followed the stops.
    pub(crate) elapsed: Duration,
    /// What became of each node, in the order the cluster held them.
    pub(crate) nodes: Vec<NodeTeardown>,
}

impl ClusterTeardown {
    /// Stops every node of one cluster within one `budget` and gives back what each of them holds.
    ///
    /// Every node is asked to stop before any node is awaited, and the waits then run together
    /// under one deadline, so a cluster of three wedged nodes costs one budget rather than three.
    /// A node still stopping when that deadline passes is not taken apart: its task is the future
    /// it was started as, and the tasks it spawned would outlive an abort with its databases and
    /// its listeners. It is left to end itself and keeps what it holds until it has. A node whose
    /// task ended gives back what it held once every wait is over.
    pub(crate) async fn stop_all<'node, Node>(
        nodes: impl IntoIterator<Item = &'node mut Node>,
        budget: Duration,
    ) -> Self
    where
        Node: TeardownNode + 'node,
    {
        let mut nodes = nodes.into_iter().collect::<Vec<_>>();
        for node in &mut nodes {
            node.request_stop();
        }

        let deadline = PhaseDeadline::after(budget);
        let stops = nodes.iter_mut().map(|node| async {
            let name = node.node_name();
            let stop = node.owned_task().wait(deadline).await;
            NodeTeardown { node: name, stop }
        });
        let stopped = join_all(stops).await;

        for (node, teardown) in nodes.iter_mut().zip(&stopped) {
            if teardown.is_still_stopping() {
                node.leave_stopping();
            } else {
                node.release();
            }
        }

        Self {
            budget,
            elapsed: deadline.elapsed(),
            nodes: stopped,
        }
    }

    /// The nodes that were still stopping at the cleanup deadline, which the caller reports as
    /// unfinished cleanup.
    pub(crate) fn still_stopping(&self) -> impl Iterator<Item = &NodeTeardown> {
        self.nodes.iter().filter(|node| node.is_still_stopping())
    }

    /// Whether the cleanup deadline passed with any node still stopping.
    pub(crate) fn left_nodes_stopping(&self) -> bool {
        self.nodes.iter().any(NodeTeardown::is_still_stopping)
    }

    /// The nodes whose task panicked, which no other record names.
    pub(crate) fn panics(&self) -> impl Iterator<Item = &NodeTeardown> {
        self.nodes.iter().filter(|node| node.panic().is_some())
    }

    /// The record of every node that was still stopping or whose task panicked, for a caller whose
    /// own failure this cleanup followed. `None` when every node ended without either.
    pub(crate) fn unclean(&self) -> Option<String> {
        let records = self
            .still_stopping()
            .chain(self.panics())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if records.is_empty() {
            return None;
        }
        Some(records.join("; "))
    }
}

impl fmt::Display for ClusterTeardown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let still_stopping = self.still_stopping().count();
        let asked = self.nodes.len();
        let stopped = asked
            .checked_sub(still_stopping)
            .assured("the nodes still stopping are among the nodes asked to stop");
        write!(
            formatter,
            "stopped {stopped} of {asked} node(s) in {:?} of a {:?} budget, {still_stopping} \
             still stopping",
            self.elapsed, self.budget
        )
    }
}
