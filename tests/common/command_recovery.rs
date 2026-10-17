//! Learning the outcome of a scenario command whose leader moved before the outcome was known.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Sending a command again, under the execution reference its first attempt carried,
//!   to the nodes of a scenario cluster until the leader answers it with a known outcome, and the
//!   bound on how long that may take.
//! - **Depends on.** The harness's raw session, the gRPC endpoints of the cluster's nodes, and the
//!   harness's phase deadline.
//! - **Must not know.** Which step sent the command, what the command does, or which node a
//!   scenario expects to lead.
//!
//! The client session protocol names a command's effects by an execution reference that outlives
//! the session carrying it, and answers every repetition of the reference from one durable record.
//! A command whose leader lost leadership after admitting it, or that reached a node that no longer
//! leads, is therefore recovered by sending it again with the same reference to the current leader.
//! The leader joins the admitted execution, returns its recorded outcome, or admits the command if
//! nothing admitted it before, so the command takes effect once.
//!
//! Recovery asks each node in turn instead of reading which node leads. A follower refuses the
//! command before admitting anything, a stopped node refuses the connection, and a node that still
//! believes it leads answers with another unknown outcome, so the node that does lead answers with
//! the outcome whichever node a scenario last saw leading. That also leaves recovery nothing to
//! borrow from the scenario, so a step's background task recovers its commands the same way.

use std::{collections::BTreeMap, time::Duration};

use nervix_client_wire::CommandOutcome;

use super::{
    phase_deadline::PhaseDeadline,
    raw_session::{SessionScope, TestSession, outcome_awaits_leader, outcome_not_leader},
};

/// How long recovery may go on asking the nodes for one command's outcome. A test cluster elects a
/// leader within its two-second election timeout; the rest is slack for a loaded machine.
const UNKNOWN_OUTCOME_RECOVERY: Duration = Duration::from_secs(60);

/// How long recovery waits, after no node it reached knew the outcome, before it asks again. An
/// election runs for at least this long, so asking sooner only adds sessions.
const LEADER_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The gRPC endpoint of every node of a scenario cluster, by node id.
#[derive(Clone, Debug)]
pub(crate) struct LeaderRoutes {
    endpoints: BTreeMap<String, String>,
}

/// A raw session and the node it is open on.
pub(crate) struct NodeSession {
    pub(crate) node: String,
    pub(crate) session: TestSession,
}

/// What recovering one command learned.
pub(crate) struct LearnedOutcome {
    /// The outcome the leader knew, or the last unknown one when the recovery bound passed first.
    pub(crate) outcome: CommandOutcome,
    /// The session whose node gave that outcome, when any node answered the repeated command.
    pub(crate) answered_on: Option<NodeSession>,
}

impl LearnedOutcome {
    fn awaits_leader(&self) -> bool {
        outcome_awaits_leader(&self.outcome)
    }
}

impl LeaderRoutes {
    pub(crate) fn new(endpoints: BTreeMap<String, String>) -> Self {
        Self { endpoints }
    }

    /// Learns the outcome of `command`, whose attempt under `reference` on a session of `scope`
    /// returned `first`, by sending it again under the same reference, on a session of the same
    /// scope, to each node in turn until one answers with an outcome only it could know.
    ///
    /// A node that cannot be reached, does not lead, or does not know the outcome yet leaves it to
    /// the next node. When no node knew it, recovery asks again after [`LEADER_POLL_INTERVAL`], and
    /// it starts no round once [`UNKNOWN_OUTCOME_RECOVERY`] has passed. A repeated command that
    /// joins an execution still applying waits for its reply as any request of a session does, so a
    /// long command is never cut short. A known outcome returns as it is, success or failure, so a
    /// command that failed still fails.
    pub(crate) async fn learn_outcome(
        &self,
        scope: &SessionScope,
        command: &str,
        reference: &str,
        first: CommandOutcome,
    ) -> LearnedOutcome {
        let deadline = PhaseDeadline::after(UNKNOWN_OUTCOME_RECOVERY);
        let mut learned = LearnedOutcome {
            outcome: first,
            answered_on: None,
        };
        while learned.awaits_leader() && !deadline.has_passed() {
            nervix_primitives::task::consume_budget().await;
            for (node, endpoint) in &self.endpoints {
                nervix_primitives::task::consume_budget().await;
                let Ok(mut session) = scope.open_session(endpoint).await else {
                    continue;
                };
                let Ok(outcome) = session
                    .run_command_result_with_reference(command, reference)
                    .await
                else {
                    continue;
                };
                if outcome_not_leader(&outcome) {
                    continue;
                }
                learned = LearnedOutcome {
                    outcome,
                    answered_on: Some(NodeSession {
                        node: node.clone(),
                        session,
                    }),
                };
                if !learned.awaits_leader() {
                    return learned;
                }
            }
            deadline.pause(LEADER_POLL_INTERVAL).await;
        }
        learned
    }
}
