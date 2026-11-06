//! The changes of its live set a node reports: a node joining, leaving, or becoming a new
//! incarnation.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The live set the membership task last reported, by node name with the incarnation
//!   that node is live as, and the membership changes a newer live set differs from it by.
//! - **Depends on.** Chitchat's live-node states and the vocabulary's node identities.
//! - **Must not know.** Sessions, notices, or what members publish under their gossip keys.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "gossip membership and management health coordinate topology outside record \
                  processing"
    )
)]

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt,
};

use chitchat::{ChitchatId, NodeState};
use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName};
use tracing::info;

use super::cluster_node_identity;

/// One change in which nodes this node sees live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MembershipChange {
    node: ClusterNodeName,
    transition: MembershipTransition,
}

/// How a node's place in the live set changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MembershipTransition {
    /// The node became live, as this incarnation.
    Joined(ClusterNodeIncarnation),
    /// The node is no longer live.
    Left,
    /// The node is live as this incarnation instead of the one it was live as.
    Restarted(ClusterNodeIncarnation),
}

impl MembershipChange {
    /// Writes the change to this node's log, the record that does not depend on any session.
    pub(super) fn record(&self) {
        match self.transition {
            MembershipTransition::Joined(incarnation) => {
                info!(node = %self.node, %incarnation, "cluster node joined");
            }
            MembershipTransition::Left => {
                info!(node = %self.node, "cluster node left");
            }
            MembershipTransition::Restarted(incarnation) => {
                info!(
                    node = %self.node,
                    %incarnation,
                    "cluster node restarted as a new incarnation"
                );
            }
        }
    }
}

/// The display text a session receives for the change.
impl fmt::Display for MembershipChange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.transition {
            MembershipTransition::Joined(incarnation) => write!(
                formatter,
                "node '{}' joined (incarnation {incarnation})",
                self.node
            ),
            MembershipTransition::Left => write!(formatter, "node '{}' left", self.node),
            MembershipTransition::Restarted(incarnation) => write!(
                formatter,
                "node '{}' restarted (incarnation {incarnation})",
                self.node
            ),
        }
    }
}

/// The live set the membership task last reported. It is the task's own state rather than
/// Chitchat's, which wakes the task for every change of any member's published keys.
pub(super) struct LiveMembers {
    members: BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
}

impl LiveMembers {
    /// The live set of a node that has just started: its own run, which it never reports joining.
    pub(super) fn of_local_run(local: &ClusterNodeIdentity) -> Self {
        Self {
            members: BTreeMap::from([(local.node_id().clone(), local.incarnation())]),
        }
    }

    /// Takes the live set of `nodes` as the current one and returns how it differs from the
    /// previous one: nothing when only the members' published keys changed.
    pub(super) fn observe(
        &mut self,
        nodes: &BTreeMap<ChitchatId, NodeState>,
    ) -> Vec<MembershipChange> {
        let current = live_incarnations(nodes);
        let mut changes = Vec::new();
        for (node, incarnation) in &current {
            let transition = match self.members.get(node) {
                None => MembershipTransition::Joined(*incarnation),
                Some(previous) if previous == incarnation => continue,
                Some(_) => MembershipTransition::Restarted(*incarnation),
            };
            changes.push(MembershipChange {
                node: node.clone(),
                transition,
            });
        }
        for node in self.members.keys() {
            if current.contains_key(node) {
                continue;
            }
            changes.push(MembershipChange {
                node: node.clone(),
                transition: MembershipTransition::Left,
            });
        }
        self.members = current;
        changes
    }
}

/// The incarnation each node of `nodes` is live as: the highest one Chitchat lists live for it,
/// since a restarted node's earlier incarnation stays listed until the failure detector removes it.
pub(super) fn live_incarnations(
    nodes: &BTreeMap<ChitchatId, NodeState>,
) -> BTreeMap<ClusterNodeName, ClusterNodeIncarnation> {
    let mut live = BTreeMap::new();
    for chitchat_id in nodes.keys() {
        let Some(identity) = cluster_node_identity(chitchat_id) else {
            continue;
        };
        match live.entry(identity.node_id().clone()) {
            Entry::Vacant(entry) => {
                entry.insert(identity.incarnation());
            }
            Entry::Occupied(mut entry) => {
                if identity.incarnation() > *entry.get() {
                    entry.insert(identity.incarnation());
                }
            }
        }
    }
    live
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use meticulous::ResultExt as _;

    use super::*;

    fn name(node: &str) -> ClusterNodeName {
        ClusterNodeName::parse(node).assured("the test node names are valid")
    }

    fn run(node: &str, incarnation: u64) -> ChitchatId {
        ChitchatId {
            node_id: node.to_string(),
            generation_id: incarnation,
            gossip_advertise_addr: SocketAddr::from(([127, 0, 0, 1], 47392)),
        }
    }

    /// The live states Chitchat publishes for `runs`, each with the keys `keys` sets.
    fn live(runs: &[(&str, u64)], keys: &[(&str, &str)]) -> BTreeMap<ChitchatId, NodeState> {
        let mut nodes = BTreeMap::new();
        for (node, incarnation) in runs {
            let mut state = NodeState::for_test();
            for (key, value) in keys {
                state.set(*key, *value);
            }
            nodes.insert(run(node, *incarnation), state);
        }
        nodes
    }

    fn texts(changes: &[MembershipChange]) -> Vec<String> {
        changes.iter().map(MembershipChange::to_string).collect()
    }

    fn members_of(local: &str, incarnation: u64) -> LiveMembers {
        LiveMembers::of_local_run(&ClusterNodeIdentity::new(
            name(local),
            ClusterNodeIncarnation::new(incarnation),
        ))
    }

    #[test]
    fn the_local_run_is_never_reported_joining() {
        let mut members = members_of("node-1", 7);

        let changes = members.observe(&live(&[("node-1", 7)], &[]));

        assert_eq!(changes, Vec::new());
    }

    #[test]
    fn changed_published_keys_report_no_membership_change() {
        let mut members = members_of("node-1", 7);
        members.observe(&live(&[("node-1", 7), ("node-2", 9)], &[]));

        let relay_created = members.observe(&live(
            &[("node-1", 7), ("node-2", 9)],
            &[("runtime_revision_ready", "12")],
        ));
        let subscription_opened = members.observe(&live(
            &[("node-1", 7), ("node-2", 9)],
            &[
                ("runtime_revision_ready", "12"),
                ("subscription_interest:tenant/orders", "1"),
            ],
        ));

        assert_eq!(relay_created, Vec::new());
        assert_eq!(subscription_opened, Vec::new());
    }

    #[test]
    fn a_join_a_leave_and_a_return_are_each_reported_once() {
        let mut members = members_of("node-1", 7);

        let joined = members.observe(&live(&[("node-1", 7), ("node-3", 11)], &[]));
        let left = members.observe(&live(&[("node-1", 7)], &[]));
        let still_gone = members.observe(&live(&[("node-1", 7)], &[]));
        let returned = members.observe(&live(&[("node-1", 7), ("node-3", 15)], &[]));

        assert_eq!(texts(&joined), ["node 'node-3' joined (incarnation 11)"]);
        assert_eq!(texts(&left), ["node 'node-3' left"]);
        assert_eq!(still_gone, Vec::new());
        assert_eq!(texts(&returned), ["node 'node-3' joined (incarnation 15)"]);
    }

    #[test]
    fn a_new_incarnation_listed_beside_the_old_one_is_one_restart() {
        let mut members = members_of("node-1", 7);
        members.observe(&live(&[("node-1", 7), ("node-2", 9)], &[]));

        let restarted =
            members.observe(&live(&[("node-1", 7), ("node-2", 9), ("node-2", 13)], &[]));
        let old_run_removed = members.observe(&live(&[("node-1", 7), ("node-2", 13)], &[]));

        assert_eq!(
            texts(&restarted),
            ["node 'node-2' restarted (incarnation 13)"]
        );
        assert_eq!(old_run_removed, Vec::new());
    }

    #[test]
    fn simultaneous_changes_are_each_reported() {
        let mut members = members_of("node-1", 7);
        members.observe(&live(&[("node-1", 7), ("node-2", 9)], &[]));

        let changes = members.observe(&live(&[("node-1", 7), ("node-3", 4)], &[]));

        assert_eq!(
            texts(&changes),
            ["node 'node-3' joined (incarnation 4)", "node 'node-2' left"]
        );
    }

    #[test]
    fn live_incarnations_name_the_highest_run_of_each_node() {
        let nodes = live(&[("node-2", 13), ("node-2", 9), ("node-1", 7)], &[]);

        let incarnations = live_incarnations(&nodes);

        assert_eq!(
            incarnations,
            BTreeMap::from([
                (name("node-1"), ClusterNodeIncarnation::new(7)),
                (name("node-2"), ClusterNodeIncarnation::new(13)),
            ])
        );
    }
}
