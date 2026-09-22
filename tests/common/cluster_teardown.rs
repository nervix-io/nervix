//! The bounded cleanup of the cluster one scenario started.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The one deadline a whole cluster stops within, asking every node to stop before
//!   awaiting any of them, aborting and joining whatever is still running when that deadline
//!   passes, the release that follows, and the record of what each node did.
//! - **Depends on.** The owned node task and the phase deadline.
//! - **Must not know.** What a node is, how it was configured, or what it holds.

use std::fmt;

use futures_util::future::join_all;
use tokio::time::Duration;

use super::{
    node_liveness::{NodeTaskTerminalOutcome, NodeTaskWaitOutcome, OwnedNodeTask},
    phase_deadline::PhaseDeadline,
};

/// The slowest a whole cluster stopped itself after cleanup asked it to, rounded up, over the
/// cleanups of the cluster scenarios run at the CI concurrency factor of two scenarios per CPU
/// beside a second full suite on the same machine: a median of 0.2 seconds, 1.6 at the ninetieth
/// percentile and 10.6 at the slowest. A policy input: measure it again when the suite or its
/// concurrency changes.
const SLOWEST_HEALTHY_CLUSTER_STOP: Duration = Duration::from_secs(12);
/// How many times the slowest healthy stop cleanup waits before it treats a cluster as wedged and
/// takes its tasks apart, so a runner slower than the measuring one still stops its own nodes.
/// A policy input.
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
/// node's task is awaited under the one cluster deadline, and a node gives back what it holds only
/// once its own task has ended.
pub(crate) trait TeardownNode {
    /// How this node is named in the teardown record.
    fn node_name(&self) -> String;

    /// Ask this node to stop, returning without waiting for it.
    fn request_stop(&mut self);

    /// The node's single owned task. The teardown awaits it, and aborts and joins it, through this
    /// one handle, so no cancelled wait can leave the task running with nothing left to join it.
    fn owned_task(&mut self) -> &mut OwnedNodeTask;

    /// Give back the harness state this node holds: its port leases, the fault injection it
    /// registered, and anything else the next scenario must not find taken. The teardown calls it
    /// once per node, after that node's task has ended, whatever ended it.
    fn release(&mut self);
}

/// What the teardown did with one node.
#[derive(Debug)]
pub(crate) struct NodeTeardown {
    pub(crate) node: String,
    pub(crate) stop: NodeTaskWaitOutcome,
}

impl NodeTeardown {
    /// Whether the cluster deadline passed with this node still running, so the teardown aborted
    /// and joined its task instead of letting the node end itself.
    pub(crate) fn was_forced(&self) -> bool {
        matches!(self.stop, NodeTaskWaitOutcome::AbortedAtDeadline(_))
    }

    /// The node's own failure, when its task ended in one. A node that returned an application
    /// error reports it to the scenario that asked for it; a node that panicked has no other
    /// witness than this record.
    pub(crate) fn panic(&self) -> Option<&NodeTaskTerminalOutcome> {
        let outcome = match &self.stop {
            NodeTaskWaitOutcome::AlreadyObserved(outcome)
            | NodeTaskWaitOutcome::Joined(outcome)
            | NodeTaskWaitOutcome::AbortedAtDeadline(outcome) => outcome.as_ref(),
            NodeTaskWaitOutcome::NotStarted => return None,
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
            NodeTaskWaitOutcome::AbortedAtDeadline(outcome) => write!(
                formatter,
                "was still running at the cleanup deadline and was aborted and joined, leaving \
                 {outcome}"
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
    /// A node still running when that deadline passes has its task aborted and joined by the same
    /// wait that owns it: the deadline is passed into the wait rather than wrapped around it,
    /// because a wait cancelled from outside would drop the join handle it had taken and leave the
    /// node's task running with nothing left that could abort or join it. The releases that follow
    /// are unconditional, and they run once every task has ended.
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

        for node in &mut nodes {
            node.release();
        }

        Self {
            budget,
            elapsed: deadline.elapsed(),
            nodes: stopped,
        }
    }

    /// The nodes the cleanup deadline forced, which its caller reports as forced cleanup.
    pub(crate) fn forced(&self) -> impl Iterator<Item = &NodeTeardown> {
        self.nodes.iter().filter(|node| node.was_forced())
    }

    /// Whether the cleanup deadline passed with any node still running.
    pub(crate) fn was_forced(&self) -> bool {
        self.nodes.iter().any(NodeTeardown::was_forced)
    }

    /// The nodes whose task panicked, which no other record names.
    pub(crate) fn panics(&self) -> impl Iterator<Item = &NodeTeardown> {
        self.nodes.iter().filter(|node| node.panic().is_some())
    }
}

impl fmt::Display for ClusterTeardown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let forced = self.forced().count();
        write!(
            formatter,
            "stopped {} node(s) in {:?} of a {:?} budget, {forced} forced",
            self.nodes.len(),
            self.elapsed,
            self.budget
        )
    }
}
