//! Learning the outcome of a scenario command whose leader moved before the outcome was known.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Sending a command again, under the execution reference its first attempt carried,
//!   to the nodes of a scenario cluster until the leader answers it with a known outcome, and the
//!   bound on how long that may take. It also owns the node any other request goes to next when a
//!   node answered it without deciding it.
//! - **Depends on.** The harness's raw session, the gRPC endpoints of the cluster's nodes, the
//!   leader a client wire redirect names, and the harness's phase deadline.
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
//!
//! A request that is not a command, such as a transaction attach, an upload or a restore stream,
//! follows the client protocol's redirects instead: a node that names the leader sends the
//! request there next, and any other answer that decides nothing sends it to the next node in
//! node order, so a request that no node redirects still reaches every node in turn.

use std::{collections::BTreeMap, ops::Bound, time::Duration};

use meticulous::OptionExt as _;
use nervix_client_wire::{CommandOutcome, NodeEndpoints};

use super::{
    command_probe::{CommandProbe, ProbeReply},
    phase_deadline::PhaseDeadline,
    raw_session::{PreparedCommand, TestSession, outcome_not_leader},
};

/// How long recovery may go on asking the nodes for one command's outcome, and a step for the
/// answer to an attach or an upload that nodes keep answering without deciding it. A test cluster
/// elects a leader within its two-second election timeout; the rest is slack for a loaded machine.
pub(crate) const UNKNOWN_OUTCOME_RECOVERY: Duration = Duration::from_secs(60);

/// How long recovery waits, after no node it reached knew the outcome, before it asks again. An
/// election runs for at least this long, so asking sooner only adds sessions.
pub(crate) const LEADER_POLL_INTERVAL: Duration = Duration::from_millis(100);

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

impl LeaderRoutes {
    pub(crate) fn new(endpoints: BTreeMap<String, String>) -> Self {
        Self { endpoints }
    }

    /// The gRPC endpoint of `node`, when it is a node of this cluster.
    pub(crate) fn endpoint(&self, node: &str) -> Option<&str> {
        let endpoint = self.endpoints.get(node)?;
        Some(endpoint.as_str())
    }

    /// The node a request goes to after `answered` answered it without deciding it.
    ///
    /// A redirect that names `leader` sends the request to that node when it is a node of this
    /// cluster, as the client protocol requires. Any other answer that decides nothing, a redirect
    /// that names no leader or one this cluster does not have, an unknown outcome or a call that
    /// ended without an answer, sends it to the node after `answered` in node order, wrapping
    /// around, so one node that knows no leader cannot hold the request.
    pub(crate) fn after(&self, answered: &str, leader: Option<&NodeEndpoints>) -> &str {
        if let Some(leader) = leader
            && let Some((node, _)) = self.endpoints.get_key_value(leader.node.as_str())
        {
            return node;
        }
        let later = self
            .endpoints
            .range::<str, _>((Bound::Excluded(answered), Bound::Unbounded))
            .next();
        let (node, _) = match later {
            Some(later) => later,
            None => self
                .endpoints
                .first_key_value()
                .assured("a scenario cluster's routes name at least the node that answered"),
        };
        node
    }

    /// Repeats the original request and transaction position until the outcome is known, or an
    /// armed probe has observed its boundary and needs the unresolved outcome itself.
    ///
    /// A node that cannot be reached, does not lead, or does not know the outcome yet leaves it to
    /// the next node. When no node knew it, recovery asks again after [`LEADER_POLL_INTERVAL`], and
    /// it starts no round once [`UNKNOWN_OUTCOME_RECOVERY`] has passed. A repeated command that
    /// joins an execution still applying waits for its reply as any request of a session does, so a
    /// long command is never cut short. A known outcome returns as it is, success or failure, so a
    /// command that failed still fails.
    pub(crate) async fn learn_outcome(
        &self,
        command: &PreparedCommand,
        first: CommandOutcome,
        mut probe: Option<&mut CommandProbe>,
    ) -> LearnedOutcome {
        let deadline = PhaseDeadline::after(UNKNOWN_OUTCOME_RECOVERY);
        let mut learned = LearnedOutcome {
            outcome: first,
            answered_on: None,
        };
        // One exchange per node bounds recovery's retained state. Reusing a binding exchange also
        // avoids closing an uncertain BEGIN merely because its reply carried no transaction status.
        let mut sessions = BTreeMap::new();
        let mut last_answered_node = None;
        while command.awaits_recovery(&learned.outcome) && !deadline.has_passed() {
            nervix_primitives::task::consume_budget().await;
            for (node, endpoint) in &self.endpoints {
                nervix_primitives::task::consume_budget().await;
                let mut session = match sessions.remove(node) {
                    Some(session) => session,
                    None => match command.open_for_recovery(endpoint).await {
                        Ok(session) => session,
                        Err(_) => continue,
                    },
                };
                match command.reattach(&mut session).await {
                    Ok(true) => {}
                    Ok(false) | Err(_) => {
                        if command.transfers_transaction_binding() && !session.is_ended() {
                            sessions.insert(node.clone(), session);
                        }
                        continue;
                    }
                }
                let attempted = match probe.as_deref_mut() {
                    Some(probe) => probe.observe(command.run(&mut session)).await,
                    None => command
                        .run(&mut session)
                        .await
                        .map(ProbeReply::BeforeBoundary),
                };
                let (outcome, observed) = match attempted {
                    Ok(ProbeReply::AfterBoundary(outcome)) => (outcome, true),
                    Ok(ProbeReply::BeforeBoundary(outcome)) => (outcome, false),
                    Err(error) => {
                        tracing::info!(node, reference = %command.reference(), %error, "command recovery transport failed");
                        if command.transfers_transaction_binding() && !session.is_ended() {
                            sessions.insert(node.clone(), session);
                        }
                        continue;
                    }
                };
                tracing::info!(node, reference = %command.reference(), position = ?command.expected_position(), disposition = ?outcome.disposition, message = %outcome.message, "command recovery reply");
                if !observed && outcome_not_leader(&outcome) {
                    if command.transfers_transaction_binding() {
                        sessions.insert(node.clone(), session);
                    }
                    continue;
                }
                if observed || !command.awaits_recovery(&outcome) {
                    return LearnedOutcome {
                        outcome,
                        answered_on: Some(NodeSession {
                            node: node.clone(),
                            session,
                        }),
                    };
                }
                learned.outcome = outcome;
                last_answered_node = Some(node.clone());
                if !command.transfers_transaction_binding() {
                    sessions.clear();
                }
                sessions.insert(node.clone(), session);
            }
            deadline.pause(LEADER_POLL_INTERVAL).await;
        }
        learned.answered_on = last_answered_node.and_then(|node| {
            sessions
                .remove(&node)
                .map(|session| NodeSession { node, session })
        });
        learned
    }
}
