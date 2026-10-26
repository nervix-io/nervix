//! The description of the cluster a client asks for with `DESCRIBE CLUSTER`.
//!
//! Layer: control plane.
//!
//! - **Owns.** Rendering every live node's published prerequisites, the current leader, and how
//!   each node's timings differ from the leader's, as printable lines.
//! - **Depends on.** The cluster's gossip view and the consensus observer's leader.
//! - **Must not know.** How a node publishes its record, or how two records are judged.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeMap;

use nervix_consensus::{GossipNode, Observer};
use nervix_models::{ClusterNodeName, NodePrerequisites, Prerequisite, timing_text};

use crate::cluster;

/// The lines `DESCRIBE CLUSTER` answers with: the leader, which nodes' timings differ from the
/// leader's, and every live node's newest run with the prerequisites it published.
pub(in crate::application) async fn describe_cluster(
    cluster: &cluster::ClusterHandle,
    consensus: &Observer,
) -> String {
    let leader = consensus.current_leader().await;
    let gossip = cluster.gossip_state().await;
    let nodes = gossip.latest_nodes_by_id();
    let leader_record = match &leader {
        Some(leader) => match nodes.get(leader) {
            Some(node) => node.prerequisites.clone(),
            None => None,
        },
        None => None,
    };
    let description = ClusterDescription {
        leader,
        leader_record,
        nodes,
    };
    let mut lines = description.lines();
    lines.extend(super::cluster_parameters::describe_parameters(cluster, consensus).await);
    lines.join("\n")
}

/// One view of the cluster, as the lines of `DESCRIBE CLUSTER` render it.
struct ClusterDescription {
    leader: Option<ClusterNodeName>,
    leader_record: Option<NodePrerequisites>,
    nodes: BTreeMap<ClusterNodeName, GossipNode>,
}

impl ClusterDescription {
    fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        match &self.leader {
            Some(leader) => lines.push(format!("leader: {leader}")),
            None => lines.push("leader: none".to_string()),
        }
        lines.push(format!("timing drift: {}", self.drift_summary()));
        for (node_id, node) in &self.nodes {
            lines.extend(self.node_lines(node_id, node));
        }
        lines
    }

    /// The nodes whose timings differ from the leader's, `none`, or why nothing was compared.
    fn drift_summary(&self) -> String {
        let Some(leader_record) = &self.leader_record else {
            return "unknown, no leader record is published".to_string();
        };
        let mut drifting = Vec::new();
        for (node_id, node) in &self.nodes {
            let Some(record) = &node.prerequisites else {
                continue;
            };
            if !record.timings.differing(&leader_record.timings).is_empty() {
                drifting.push(node_id.to_string());
            }
        }
        if drifting.is_empty() {
            return "none".to_string();
        }
        drifting.join(", ")
    }

    fn node_lines(&self, node_id: &ClusterNodeName, node: &GossipNode) -> Vec<String> {
        let is_leader = self.leader.as_ref() == Some(node_id);
        let mut lines = Vec::new();
        if is_leader {
            lines.push(format!("node: {node_id} (leader)"));
        } else {
            lines.push(format!("node: {node_id}"));
        }
        lines.push(format!("  incarnation: {}", node.incarnation));
        let Some(record) = &node.prerequisites else {
            lines.push("  prerequisites: not published".to_string());
            return lines;
        };
        for described in record.described() {
            let drift = self.timing_drift(is_leader, record, described.prerequisite);
            match drift {
                Some(leader_value) => lines.push(format!(
                    "  {}: {}, the leader's {leader_value}",
                    described.prerequisite, described.value
                )),
                None => lines.push(format!("  {}: {}", described.prerequisite, described.value)),
            }
        }
        lines
    }

    /// The leader's value of `prerequisite` when it is a timing this non-leader record holds
    /// differently.
    fn timing_drift(
        &self,
        is_leader: bool,
        record: &NodePrerequisites,
        prerequisite: Prerequisite,
    ) -> Option<String> {
        if is_leader {
            return None;
        }
        let leader_record = self.leader_record.as_ref()?;
        for timing in record.timings.differing(&leader_record.timings) {
            if Prerequisite::from(timing) == prerequisite {
                return Some(timing_text(leader_record.timings.get(timing)));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nervix_models::{
        ClusterNodeIncarnation, ConsensusFloors, PrerequisiteTimings, RaftLogDurability,
        TrustRootFingerprint, WireContractFingerprint,
    };

    use super::*;

    fn name(node: &str) -> ClusterNodeName {
        ClusterNodeName::parse(node).expect("a valid node name")
    }

    fn record(heartbeat_millis: u64) -> NodePrerequisites {
        NodePrerequisites {
            cluster_identity: "cluster".to_string(),
            trust_root: TrustRootFingerprint::from_digest([1; 32]),
            wire_contract: WireContractFingerprint::from_digest([2; 32]),
            consensus_floors: ConsensusFloors {
                largest_command_bytes: 1 << 20,
                largest_management_event_bytes: 64 << 10,
            },
            durability: RaftLogDurability::FsyncEveryAppend,
            timings: PrerequisiteTimings {
                raft_heartbeat_interval: Duration::from_millis(heartbeat_millis),
                gossip_interval: Duration::from_millis(500),
                clock_skew_tolerance: Duration::from_millis(500),
            },
        }
    }

    fn node(node: &str, incarnation: u64, record: Option<NodePrerequisites>) -> GossipNode {
        GossipNode {
            parameters: None,
            node_id: name(node),
            incarnation: ClusterNodeIncarnation::new(incarnation),
            terminating: false,
            client_url: None,
            console_url: None,
            interconnect_endpoint: None,
            prerequisites: record,
        }
    }

    #[test]
    fn a_node_whose_timing_differs_from_the_leader_is_marked_with_both_values() {
        let description = ClusterDescription {
            leader: Some(name("node-1")),
            leader_record: Some(record(250)),
            nodes: BTreeMap::from([
                (name("node-1"), node("node-1", 7, Some(record(250)))),
                (name("node-2"), node("node-2", 8, Some(record(400)))),
                (name("node-3"), node("node-3", 9, None)),
            ]),
        };
        let lines = description.lines();
        assert_eq!(lines[0], "leader: node-1");
        assert_eq!(lines[1], "timing drift: node-2");
        assert!(lines.contains(&"node: node-1 (leader)".to_string()));
        assert!(lines.contains(&"  raft-heartbeat-interval: 250ms".to_string()));
        assert!(
            lines.contains(&"  raft-heartbeat-interval: 400ms, the leader's 250ms".to_string())
        );
        assert!(lines.contains(&"  cluster identity: cluster".to_string()));
        assert!(lines.contains(&"  durability promise: fsync every append".to_string()));
        assert!(lines.contains(&"  prerequisites: not published".to_string()));
    }

    #[test]
    fn without_a_leader_record_no_drift_is_claimed() {
        let description = ClusterDescription {
            leader: None,
            leader_record: None,
            nodes: BTreeMap::from([(name("node-1"), node("node-1", 1, Some(record(250))))]),
        };
        let lines = description.lines();
        assert_eq!(lines[0], "leader: none");
        assert_eq!(
            lines[1],
            "timing drift: unknown, no leader record is published"
        );
        assert!(lines.contains(&"  raft-heartbeat-interval: 250ms".to_string()));
    }

    #[test]
    fn matching_timings_report_no_drift() {
        let description = ClusterDescription {
            leader: Some(name("node-1")),
            leader_record: Some(record(250)),
            nodes: BTreeMap::from([
                (name("node-1"), node("node-1", 1, Some(record(250)))),
                (name("node-2"), node("node-2", 2, Some(record(250)))),
            ]),
        };
        assert_eq!(description.lines()[1], "timing drift: none");
    }
}
