//! Whether a node that is joining the cluster may stay in it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Following one run's join from its first outbound handshake, through the clock skew
//!   to the peers it completes a handshake with and the leader's verdict on the run, to the end of
//!   its join, and the typed refusal that stops a node the cluster cannot run beside while it is
//!   still joining.
//! - **Depends on.** The transport's join progress and prerequisite record, the cluster's gossip
//!   view, the leader's verdicts it publishes there and the clock probe, and the consensus
//!   membership this node has applied.
//! - **Must not know.** How a handshake judges two records, how the leader judges a run or admits
//!   a member, or how the node stops.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::{collections::BTreeSet, time::Duration};

use nervix_consensus::Observer;
use nervix_interconnect::{JoinProgress, JoinRefusal, Transport};
use nervix_models::{
    AdmissionVerdict, ClusterNodeIdentity, ClusterNodeName, SettingDefinition as _,
};
use nervix_primitives::{
    sync::{Arc, CancellationToken},
    time::sleep,
};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::cluster::{ClockSkew, ClockSkewTolerance, ClusterHandle};

/// Why a joining node stops instead of joining the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JoinRefusedError {
    /// This node and a member of the cluster cannot run together.
    #[error("{0}")]
    Prerequisite(JoinRefusal),
    /// The physical clock of a peer this node completed a handshake with is too far from its own.
    #[error(
        "the physical clock of node '{peer}' is beyond this node's {name} of {tolerance:?}: {skew}",
        name = ClockSkewTolerance::NAME
    )]
    ClockSkew {
        peer: ClusterNodeName,
        skew: ClockSkew,
        tolerance: Duration,
    },
}

/// How one run's join ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) enum JoinOutcome {
    /// The join ended with the node in the cluster.
    Admitted,
    /// The cluster cannot run beside this node; it stops.
    Refused(JoinRefusedError),
    /// The node began stopping before its join ended.
    Stopping,
}

/// What one run joins as, by the membership it recovered from durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum JoinStanding {
    /// A member of the membership it recovered. Its return changes neither the voters nor the
    /// quorum, so its join ends once the leader has accepted its run beside every live voter and
    /// a peer's clock has answered within tolerance.
    RecoveredMember,
    /// Outside the membership it recovered, or with none recovered. Its join ends once the
    /// membership it has applied admits it, because from then on the cluster may count it among
    /// its voters.
    Newcomer,
}

/// How the first outbound handshake of this run ended.
enum FirstHandshake {
    Joined { through: ClusterNodeName },
    Refused(JoinRefusal),
    Stopping,
}

/// What a joining node sees between two rounds of clock probes.
#[derive(Debug, Default)]
struct JoinView {
    /// Whether the membership this node has applied holds it.
    admitted: bool,
    /// The first live member of that membership whose record this node cannot run beside.
    incompatible_member: Option<JoinRefusal>,
    /// The newest leader's verdict on this run, once that leader has judged it.
    verdict: Option<AdmissionVerdict>,
    /// The live peers this node has completed a handshake with.
    connected: BTreeSet<ClusterNodeName>,
}

/// What a joining node does next.
#[derive(Debug, PartialEq, Eq)]
enum JoinStep {
    /// The join ended.
    Ended(JoinOutcome),
    /// Probe the clocks of these peers, in this order.
    Probe(Vec<ClusterNodeName>),
}

/// What one answered clock probe did to a run's join.
#[derive(Debug, PartialEq, Eq)]
enum ProbeAnswer {
    /// The peer's clock is within the tolerance, and the join goes on.
    Within,
    /// The answer ended the join.
    Ended(JoinOutcome),
}

/// The decisions of one run's join, from what the node has seen since its first outbound
/// handshake completed.
struct JoinJudge {
    standing: JoinStanding,
    /// The peer that handshake completed with; its clock is probed first.
    through: ClusterNodeName,
    clock_skew_tolerance: Duration,
    /// The peers whose clock answered within the tolerance.
    within: BTreeSet<ClusterNodeName>,
}

impl JoinJudge {
    fn new(
        standing: JoinStanding,
        through: ClusterNodeName,
        clock_skew_tolerance: Duration,
    ) -> Self {
        Self {
            standing,
            through,
            clock_skew_tolerance,
            within: BTreeSet::new(),
        }
    }

    /// What to do after seeing `view`. A newcomer's join ends once it is admitted, or once a live
    /// member it cannot run beside refuses it. Any joining node's join ends once the leader has
    /// refused its run, naming the voter it cannot run beside, and a recovered member's once the
    /// leader has accepted its run and a peer's clock has answered within the tolerance. Otherwise
    /// every peer whose clock has not answered within the tolerance is probed, the one this node
    /// joined through first.
    fn step(&self, view: JoinView) -> JoinStep {
        if self.standing == JoinStanding::Newcomer {
            if view.admitted {
                return JoinStep::Ended(JoinOutcome::Admitted);
            }
            if let Some(refusal) = view.incompatible_member {
                return JoinStep::Ended(JoinOutcome::Refused(JoinRefusedError::Prerequisite(
                    refusal,
                )));
            }
        }
        match view.verdict {
            Some(AdmissionVerdict::Refused { voter, mismatch }) => {
                // The leader judged from the voter's side; the refusal reads from this node's.
                let refusal = JoinRefusal {
                    peer: voter,
                    mismatch: mismatch.reversed(),
                };
                return JoinStep::Ended(JoinOutcome::Refused(JoinRefusedError::Prerequisite(
                    refusal,
                )));
            }
            Some(AdmissionVerdict::Accepted)
                if self.standing == JoinStanding::RecoveredMember && !self.within.is_empty() =>
            {
                info!("the leader accepted this node's return beside every live voter");
                return JoinStep::Ended(JoinOutcome::Admitted);
            }
            Some(AdmissionVerdict::Accepted) | None => {}
        }
        let mut peers = Vec::new();
        if !self.within.contains(&self.through) {
            peers.push(self.through.clone());
        }
        for peer in view.connected {
            if peer != self.through && !self.within.contains(&peer) {
                peers.push(peer);
            }
        }
        JoinStep::Probe(peers)
    }

    /// Records the skew one answered probe of `peer` proved, with whether the membership this node
    /// has applied held it once the answer arrived, and returns whether the answer ended the join.
    fn record(&mut self, peer: ClusterNodeName, skew: ClockSkew, admitted: bool) -> ProbeAnswer {
        if skew.at_least() <= self.clock_skew_tolerance {
            if peer == self.through {
                info!(
                    peer = %peer,
                    %skew,
                    "joined the cluster through a peer whose clock is within tolerance"
                );
            } else {
                debug!(peer = %peer, %skew, "a peer's clock is within tolerance");
            }
            self.within.insert(peer);
            return ProbeAnswer::Within;
        }
        let refusal = JoinRefusedError::ClockSkew {
            peer,
            skew,
            tolerance: self.clock_skew_tolerance,
        };
        // Refusing is correct only while the node is joining: once admitted, it may already be a
        // voter the cluster's quorum counts.
        if self.standing == JoinStanding::Newcomer && admitted {
            warn!(
                refusal = %refusal,
                "a peer's clock proved beyond tolerance only after the membership admitted this \
                 node, which stays in the cluster"
            );
            return ProbeAnswer::Ended(JoinOutcome::Admitted);
        }
        ProbeAnswer::Ended(JoinOutcome::Refused(refusal))
    }
}

/// Follows the join of a node that starts with peers to reach: a bootstrap host or the members it
/// recovered. A node that bootstraps a cluster alone joins nothing, and is never supervised, so a
/// newcomer it later dials can never stop it.
pub(in crate::application) struct JoinSupervision {
    pub(in crate::application) transport: Transport,
    pub(in crate::application) cluster: Arc<ClusterHandle>,
    pub(in crate::application) observer: Observer,
    pub(in crate::application) local_node: ClusterNodeName,
    pub(in crate::application) standing: JoinStanding,
    pub(in crate::application) clock_skew_tolerance: Duration,
    /// How long to wait before probing an unanswered peer's clock again.
    pub(in crate::application) probe_retry_interval: Duration,
}

impl JoinSupervision {
    /// Follows this run's join until it ends with the node in the cluster, refused, or stopping.
    /// A peer that does not answer its clock probe is probed again in the next round, a gossip
    /// interval later or sooner when the topology or the live nodes change. A change of the
    /// leader's verdicts changes a live node's gossip state, so it wakes the next round too.
    pub(in crate::application) async fn run(self, stopping: CancellationToken) -> JoinOutcome {
        let through = match self.first_outbound_handshake(&stopping).await {
            FirstHandshake::Joined { through } => through,
            FirstHandshake::Refused(refusal) => {
                return JoinOutcome::Refused(JoinRefusedError::Prerequisite(refusal));
            }
            FirstHandshake::Stopping => return JoinOutcome::Stopping,
        };
        let run = self.cluster.local_node_identity().await;
        let mut judge = JoinJudge::new(self.standing, through, self.clock_skew_tolerance);
        let mut topology = self.observer.subscribe_topology();
        let mut live_nodes = self.cluster.subscribe_live_node_states().await;
        loop {
            nervix_primitives::task::consume_budget().await;
            let view = self.view(&run).await;
            let peers = match judge.step(view) {
                JoinStep::Ended(outcome) => return outcome,
                JoinStep::Probe(peers) => peers,
            };
            // A clock answering within the tolerance may be all a verdict the node already holds
            // waits for, so a round that saw one is judged again at once.
            let mut answered_within = false;
            for peer in peers {
                nervix_primitives::task::consume_budget().await;
                let probe = nervix_primitives::select! {
                    _ = stopping.cancelled() => return JoinOutcome::Stopping,
                    probe = self.cluster.probe_clock_skew(&peer) => probe,
                };
                let skew = match probe {
                    Ok(skew) => skew,
                    Err(error) => {
                        debug!(
                            peer = %peer,
                            error = %format!("{error:#}"),
                            "a peer did not answer its clock probe; it is probed again later"
                        );
                        continue;
                    }
                };
                // The leader may have admitted this node while the probe was in flight, so the
                // membership is read once the answer is here.
                let admitted = self.is_member().await;
                match judge.record(peer, skew, admitted) {
                    ProbeAnswer::Within => answered_within = true,
                    ProbeAnswer::Ended(outcome) => return outcome,
                }
            }
            if answered_within {
                continue;
            }
            nervix_primitives::select! {
                _ = stopping.cancelled() => return JoinOutcome::Stopping,
                changed = topology.changed() => {
                    if !changed {
                        return JoinOutcome::Stopping;
                    }
                }
                changed = live_nodes.changed() => {
                    if changed.is_err() {
                        return JoinOutcome::Stopping;
                    }
                }
                () = sleep(self.probe_retry_interval) => {}
            }
        }
    }

    /// Waits for the first outbound handshake of this run to complete or to be refused.
    async fn first_outbound_handshake(&self, stopping: &CancellationToken) -> FirstHandshake {
        let mut progress = self.transport.join_progress();
        loop {
            nervix_primitives::task::consume_budget().await;
            let current = progress.borrow_and_update().clone();
            match current {
                JoinProgress::Joining => {}
                JoinProgress::Joined { through } => return FirstHandshake::Joined { through },
                JoinProgress::Refused(refusal) => return FirstHandshake::Refused(refusal),
            }
            nervix_primitives::select! {
                _ = stopping.cancelled() => return FirstHandshake::Stopping,
                changed = progress.changed() => {
                    if changed.is_err() {
                        // The transport that publishes the progress has stopped with the node.
                        return FirstHandshake::Stopping;
                    }
                }
            }
        }
    }

    /// Whether the membership this node has applied holds it.
    async fn is_member(&self) -> bool {
        self.observer
            .membership_nodes()
            .await
            .contains_key(&self.local_node)
    }

    /// What this node sees now: whether it is admitted, the first live member whose record it
    /// cannot run beside, the newest leader's verdict on `run`, and the live peers it has completed
    /// a handshake with. A live node outside the membership this node has applied, such as another
    /// joining node or one that was itself refused and has not left gossip yet, never refuses it.
    async fn view(&self, run: &ClusterNodeIdentity) -> JoinView {
        let membership = self.observer.membership_nodes().await;
        let local = self.transport.local_prerequisites();
        let gossip = self.cluster.gossip_state().await;
        let verdict = self.cluster.admission_verdict_of(run).await;
        let mut view = JoinView {
            admitted: membership.contains_key(&self.local_node),
            verdict,
            ..JoinView::default()
        };
        for node in gossip.live_nodes {
            if node.node_id == self.local_node {
                continue;
            }
            if view.incompatible_member.is_none()
                && membership.contains_key(&node.node_id)
                && let Some(record) = &node.prerequisites
                && let Err(mismatch) = local.verify_peer(record)
            {
                view.incompatible_member = Some(JoinRefusal {
                    peer: node.node_id.clone(),
                    mismatch,
                });
            }
            if self.transport.is_connected_to(&node.node_id) {
                view.connected.insert(node.node_id);
            }
        }
        view
    }
}

#[cfg(test)]
#[path = "join_supervision_tests.rs"]
mod tests;
