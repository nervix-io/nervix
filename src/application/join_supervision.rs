//! Whether a node that is joining the cluster may stay in it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Following one run's join from its first outbound handshake, through the clock skew
//!   to every peer it completes a handshake with, to its admission to the membership, and the typed
//!   refusal that stops a node the cluster cannot run beside.
//! - **Depends on.** The transport's join progress and prerequisite record, the cluster's gossip
//!   view and clock probe, and the consensus membership this node has applied.
//! - **Must not know.** How a handshake judges two records, how the leader admits a member, or how
//!   the node stops.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use nervix_consensus::Observer;
use nervix_interconnect::{JoinProgress, JoinRefusal, Transport};
use nervix_models::{ClusterNodeName, SettingDefinition as _};
use nervix_primitives::{
    sync::{Arc, CancellationToken},
    time::sleep,
};
use thiserror::Error;
use tracing::{debug, info};

use crate::cluster::{ClockSkew, ClockSkewTolerance, ClusterHandle};

/// Why a joining node stops instead of joining the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JoinRefusedError {
    /// This node and a member of the cluster cannot run together.
    #[error("{0}")]
    Prerequisite(JoinRefusal),
    /// The physical clock of the node this node joined through is too far from its own.
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
    /// The node is a member of the membership it has applied.
    Admitted,
    /// The cluster cannot run beside this node; it stops.
    Refused(JoinRefusedError),
    /// The node began stopping before its join ended.
    Stopping,
}

/// How the first outbound handshake of this run ended.
enum FirstHandshake {
    Joined { through: ClusterNodeName },
    Refused(JoinRefusal),
    Stopping,
}

/// How the clock skew to the peer this node joined through was judged.
enum SkewJudgment {
    Within,
    Beyond(JoinRefusedError),
    Stopping,
}

/// Follows the join of a node that starts with peers to reach: a bootstrap host or the members it
/// recovered. A node that bootstraps a cluster alone joins nothing, and is never supervised, so a
/// newcomer it later dials can never stop it.
pub(in crate::application) struct JoinSupervision {
    pub(in crate::application) transport: Transport,
    pub(in crate::application) cluster: Arc<ClusterHandle>,
    pub(in crate::application) observer: Observer,
    pub(in crate::application) local_node: ClusterNodeName,
    pub(in crate::application) clock_skew_tolerance: Duration,
    /// How long to wait before probing an unanswered peer's clock again.
    pub(in crate::application) probe_retry_interval: Duration,
}

impl JoinSupervision {
    /// Follows this run's join until the node is admitted, refused, or stopping.
    pub(in crate::application) async fn run(self, stopping: CancellationToken) -> JoinOutcome {
        let through = match self.first_outbound_handshake(&stopping).await {
            FirstHandshake::Joined { through } => through,
            FirstHandshake::Refused(refusal) => {
                return JoinOutcome::Refused(JoinRefusedError::Prerequisite(refusal));
            }
            FirstHandshake::Stopping => return JoinOutcome::Stopping,
        };
        match self.judge_clock_skew(&through, &stopping).await {
            SkewJudgment::Within => {}
            SkewJudgment::Beyond(refusal) => return JoinOutcome::Refused(refusal),
            SkewJudgment::Stopping => return JoinOutcome::Stopping,
        }
        self.until_admitted(through, &stopping).await
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

    /// Probes the clock of the peer this node joined through until it answers, and refuses the
    /// join when the skew one probe proves exceeds this node's tolerance.
    async fn judge_clock_skew(
        &self,
        through: &ClusterNodeName,
        stopping: &CancellationToken,
    ) -> SkewJudgment {
        loop {
            nervix_primitives::task::consume_budget().await;
            let probe = nervix_primitives::select! {
                _ = stopping.cancelled() => return SkewJudgment::Stopping,
                probe = self.cluster.probe_clock_skew(through) => probe,
            };
            match probe {
                Ok(skew) => match self.skew_judgment(through, skew) {
                    SkewJudgment::Within => {
                        info!(
                            peer = %through,
                            %skew,
                            "joined the cluster through a peer whose clock is within tolerance"
                        );
                        return SkewJudgment::Within;
                    }
                    beyond => return beyond,
                },
                Err(error) => debug!(
                    peer = %through,
                    error = %format!("{error:#}"),
                    "the node this node joined through did not answer its clock probe"
                ),
            }
            nervix_primitives::select! {
                _ = stopping.cancelled() => return SkewJudgment::Stopping,
                () = sleep(self.probe_retry_interval) => {}
            }
        }
    }

    /// Whether the skew one probe of `peer` proved is within this node's tolerance.
    fn skew_judgment(&self, peer: &ClusterNodeName, skew: ClockSkew) -> SkewJudgment {
        if skew.at_least() > self.clock_skew_tolerance {
            return SkewJudgment::Beyond(JoinRefusedError::ClockSkew {
                peer: peer.clone(),
                skew,
                tolerance: self.clock_skew_tolerance,
            });
        }
        SkewJudgment::Within
    }

    /// While this node is outside the membership it has applied, refuses the join as soon as a
    /// live member publishes a record this node cannot run beside, or a peer it completed a
    /// handshake with proves a clock beyond its tolerance. Peers are probed once each; one that
    /// does not answer is probed again a gossip interval later.
    async fn until_admitted(
        &self,
        through: ClusterNodeName,
        stopping: &CancellationToken,
    ) -> JoinOutcome {
        let mut topology = self.observer.subscribe_topology();
        let mut live_nodes = self.cluster.subscribe_live_node_states().await;
        let mut probed = BTreeSet::from([through]);
        loop {
            nervix_primitives::task::consume_budget().await;
            let membership = self.observer.membership_nodes().await;
            if membership.contains_key(&self.local_node) {
                return JoinOutcome::Admitted;
            }
            if let Some(refusal) = self.incompatible_live_member(&membership).await {
                return JoinOutcome::Refused(JoinRefusedError::Prerequisite(refusal));
            }
            if let Some(refusal) = self.probe_connected_peers(&mut probed, stopping).await {
                return refusal;
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

    /// The first live member, by the membership this node has applied, whose published record
    /// this node cannot run beside. A live node outside that membership, such as another joining
    /// node or one that was itself refused and has not left gossip yet, never refuses this one.
    async fn incompatible_live_member(
        &self,
        membership: &BTreeMap<ClusterNodeName, String>,
    ) -> Option<JoinRefusal> {
        let local = self.transport.local_prerequisites();
        let gossip = self.cluster.gossip_state().await;
        for node in gossip.live_nodes {
            if node.node_id == self.local_node || !membership.contains_key(&node.node_id) {
                continue;
            }
            let Some(record) = node.prerequisites else {
                continue;
            };
            if let Err(mismatch) = local.verify_peer(&record) {
                return Some(JoinRefusal {
                    peer: node.node_id,
                    mismatch,
                });
            }
        }
        None
    }

    /// Probes the clock of every live peer this node has completed a handshake with and has not
    /// probed yet, and refuses the join at the first one beyond its tolerance.
    async fn probe_connected_peers(
        &self,
        probed: &mut BTreeSet<ClusterNodeName>,
        stopping: &CancellationToken,
    ) -> Option<JoinOutcome> {
        let gossip = self.cluster.gossip_state().await;
        for node in gossip.live_nodes {
            nervix_primitives::task::consume_budget().await;
            let peer = node.node_id;
            if peer == self.local_node
                || probed.contains(&peer)
                || !self.transport.is_connected_to(&peer)
            {
                continue;
            }
            let probe = nervix_primitives::select! {
                _ = stopping.cancelled() => return Some(JoinOutcome::Stopping),
                probe = self.cluster.probe_clock_skew(&peer) => probe,
            };
            match probe {
                Ok(skew) => match self.skew_judgment(&peer, skew) {
                    SkewJudgment::Within => {
                        debug!(peer = %peer, %skew, "a peer's clock is within tolerance");
                        probed.insert(peer);
                    }
                    SkewJudgment::Beyond(refusal) => return Some(JoinOutcome::Refused(refusal)),
                    SkewJudgment::Stopping => return Some(JoinOutcome::Stopping),
                },
                Err(error) => debug!(
                    peer = %peer,
                    error = %format!("{error:#}"),
                    "a peer did not answer its clock probe; it is probed again later"
                ),
            }
        }
        None
    }
}
