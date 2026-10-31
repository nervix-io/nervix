//! What a node knows about Raft leadership when it cannot propose as the leader.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The observation a leadership refusal carries, and the classification of a write Raft
//!   refused from the leader it names and the tenure this node observed around the refusal.
//! - **Depends on.** The vocabulary's node names and this crate's leader tenure.
//! - **Must not know.** How a refusal is answered, retried or worded for a client.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::fmt;

use nervix_models::ClusterNodeName;

use crate::LeaderTenure;

/// What this node knew about Raft leadership when it could not propose as the leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipObservation {
    /// Another node leads, and a redirect can name it.
    Leader(ClusterNodeName),
    /// No leader is known, as during an election.
    NoLeader,
    /// This node still leads `term`, but no quorum acknowledged it within its lease, so Raft
    /// refused the write before appending it. Nothing of the write took effect, and the same node
    /// may still lead when the refusal is reported.
    UnconfirmedLease { term: u64 },
}

impl LeadershipObservation {
    /// The leader a redirect can name: another node that leads.
    pub fn leader(&self) -> Option<&ClusterNodeName> {
        match self {
            Self::Leader(leader) => Some(leader),
            Self::NoLeader | Self::UnconfirmedLease { .. } => None,
        }
    }

    /// What a write that Raft refused by forwarding it says about leadership.
    ///
    /// Raft names the leader it forwards to when it knows one. A leader whose lease lapsed forwards
    /// to no one while it keeps leading its term, and so does a node that knows no leader, such as
    /// during an election. The two are told apart by the tenure this node observed when it proposed
    /// the write and after the refusal: only a node that led the same term at both observations led
    /// when Raft refused, because a node that stops leading can lead again only in a later term.
    pub(crate) fn of_refused_write(
        named: Option<ClusterNodeName>,
        local: &ClusterNodeName,
        proposed_under: Option<&LeaderTenure>,
        refused_under: Option<&LeaderTenure>,
    ) -> Self {
        if let Some(leader) = named {
            return Self::Leader(leader);
        }
        let (Some(proposed_under), Some(refused_under)) = (proposed_under, refused_under) else {
            return Self::NoLeader;
        };
        if proposed_under.leader_id() != local || proposed_under != refused_under {
            return Self::NoLeader;
        }
        Self::UnconfirmedLease {
            term: proposed_under.term(),
        }
    }
}

impl From<Option<ClusterNodeName>> for LeadershipObservation {
    /// The leader a node observes outside a refused write: the node that leads, or none.
    fn from(leader: Option<ClusterNodeName>) -> Self {
        match leader {
            Some(leader) => Self::Leader(leader),
            None => Self::NoLeader,
        }
    }
}

impl fmt::Display for LeadershipObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leader(leader) => write!(formatter, "node '{leader}' leads"),
            Self::NoLeader => formatter.write_str("no leader is known"),
            Self::UnconfirmedLease { term } => write!(
                formatter,
                "this node leads term {term} but could not confirm its lease"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    fn node(name: &str) -> ClusterNodeName {
        ClusterNodeName::parse(name).assured("the test node name is valid")
    }

    fn tenure(leader: &str, term: u64) -> LeaderTenure {
        LeaderTenure {
            leader_id: node(leader),
            term,
        }
    }

    #[test]
    fn a_named_leader_is_the_observation_whatever_the_tenure() {
        let local = node("node-1");
        let led = tenure("node-1", 3);
        let observed = LeadershipObservation::of_refused_write(
            Some(node("node-2")),
            &local,
            Some(&led),
            Some(&led),
        );
        assert_eq!(observed, LeadershipObservation::Leader(node("node-2")));
        assert_eq!(observed.leader(), Some(&node("node-2")));
    }

    #[test]
    fn a_leader_that_keeps_its_term_could_not_confirm_its_lease() {
        let local = node("node-1");
        let led = tenure("node-1", 3);
        let observed =
            LeadershipObservation::of_refused_write(None, &local, Some(&led), Some(&led));
        assert_eq!(
            observed,
            LeadershipObservation::UnconfirmedLease { term: 3 }
        );
        assert_eq!(observed.leader(), None);
        assert_eq!(
            observed.to_string(),
            "this node leads term 3 but could not confirm its lease"
        );
    }

    #[test]
    fn a_refusal_without_a_leader_after_the_tenure_changed_knows_no_leader() {
        let local = node("node-1");
        let proposed_under = tenure("node-1", 3);
        for refused_under in [
            Some(tenure("node-1", 4)),
            Some(tenure("node-2", 4)),
            Some(tenure("node-2", 3)),
            None,
        ] {
            let observed = LeadershipObservation::of_refused_write(
                None,
                &local,
                Some(&proposed_under),
                refused_under.as_ref(),
            );
            assert_eq!(
                observed,
                LeadershipObservation::NoLeader,
                "refused under {refused_under:?}"
            );
        }
    }

    #[test]
    fn a_node_that_did_not_lead_when_it_proposed_knows_no_leader() {
        let local = node("node-1");
        let followed = tenure("node-2", 3);
        for proposed_under in [Some(&followed), None] {
            let observed = LeadershipObservation::of_refused_write(
                None,
                &local,
                proposed_under,
                Some(&followed),
            );
            assert_eq!(observed, LeadershipObservation::NoLeader);
        }
    }

    #[test]
    fn an_observed_leader_converts_into_its_observation() {
        assert_eq!(
            LeadershipObservation::from(Some(node("node-2"))),
            LeadershipObservation::Leader(node("node-2"))
        );
        assert_eq!(
            LeadershipObservation::from(None),
            LeadershipObservation::NoLeader
        );
        assert_eq!(
            LeadershipObservation::Leader(node("node-2")).to_string(),
            "node 'node-2' leads"
        );
        assert_eq!(
            LeadershipObservation::NoLeader.to_string(),
            "no leader is known"
        );
    }
}
