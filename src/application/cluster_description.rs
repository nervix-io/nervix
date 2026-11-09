//! The description of the cluster a client asks for with `DESCRIBE CLUSTER`.
//!
//! Layer: control plane.
//!
//! - **Owns.** Rendering the public identity, prerequisites, leader/timing drift, committed
//!   live/departed capacity claims and effective receive bounds as printable lines.
//! - **Depends on.** The cluster gossip view and coherent public consensus observations.
//! - **Must not know.** Private keys, record publication or how two records are judged.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeMap;

use nervix_consensus::{ClusterIdentity, GossipNode, Observer};
use nervix_models::{
    ClusterNodeName, NodeCapabilityRecord, NodePrerequisites, PeerReceiveBounds, Prerequisite,
    timing_text,
};

use crate::cluster;

/// The public identity, leader, timing drift and each live node's published prerequisites.
pub(in crate::application) async fn describe_cluster(
    cluster: &cluster::ClusterHandle,
    consensus: &Observer,
    interconnect: &nervix_interconnect::Transport,
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
        identity: consensus.cluster_identity(),
        leader,
        leader_record,
        nodes,
        capabilities: consensus.node_capabilities(),
    };
    let mut lines = description.lines();
    for (peer, record) in interconnect.peer_capability_records() {
        lines.push(format!(
            "adopted peer receive bounds: node={peer} incarnation={} {}",
            record.identity.incarnation(),
            bounds_line(record.capabilities.receive)
        ));
    }
    lines.extend(super::cluster_parameters::describe_parameters(cluster, consensus).await);
    lines.join("\n")
}

/// One view of the cluster, as the lines of `DESCRIBE CLUSTER` render it.
struct ClusterDescription {
    identity: Option<ClusterIdentity>,
    leader: Option<ClusterNodeName>,
    leader_record: Option<NodePrerequisites>,
    nodes: BTreeMap<ClusterNodeName, GossipNode>,
    capabilities: BTreeMap<ClusterNodeName, NodeCapabilityRecord>,
}

impl ClusterDescription {
    fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        match &self.leader {
            Some(leader) => lines.push(format!("leader: {leader}")),
            None => lines.push("leader: none".to_string()),
        }
        if let Some(identity) = &self.identity {
            lines.push(format!("cluster uid: {}", identity.cluster_uid));
            lines.push(format!("cluster key: {}", identity.kid));
            lines.push(format!(
                "cluster created: {}",
                identity.created_at.to_rfc3339()
            ));
        }
        lines.push(format!("timing drift: {}", self.drift_summary()));
        for (node_id, node) in &self.nodes {
            lines.extend(self.node_lines(node_id, node));
        }
        for (node_id, record) in &self.capabilities {
            if !self.nodes.contains_key(node_id) {
                lines.push(format!("node: {node_id} (departed)"));
                lines.push(format!("  incarnation: {}", record.identity.incarnation()));
                lines.push(capability_line(record));
            }
        }
        let effective = self
            .capabilities
            .iter()
            .filter_map(|(name, record)| {
                let live = self.nodes.get(name)?;
                if live.incarnation == record.identity.incarnation() {
                    Some(record.capabilities.peer_receive_bounds())
                } else {
                    None
                }
            })
            .reduce(PeerReceiveBounds::intersection);
        match effective {
            Some(bounds) => lines.push(format!(
                "cluster-effective receive bounds: {}",
                bounds_line(bounds)
            )),
            None => lines.push(
                "cluster-effective receive bounds: awaiting committed live claims".to_string(),
            ),
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
        match self.capabilities.get(node_id) {
            Some(record) if record.identity.incarnation() == node.incarnation => {
                lines.push(capability_line(record))
            }
            Some(record) => lines.push(format!(
                "  capabilities: awaiting this incarnation; retained {}",
                record.identity
            )),
            None => lines.push("  capabilities: awaiting committed claim".to_string()),
        }
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

fn bounds_line(bounds: PeerReceiveBounds) -> String {
    format!(
        "relay-batch-bytes={} relay-decoded-bytes={} bulk-chunk-bytes={} bulk-window-bytes={}",
        bounds.relay_batch_bytes,
        bounds.relay_decoded_bytes,
        bounds.bulk_chunk_bytes,
        bounds.bulk_window_bytes
    )
}

fn capability_line(record: &NodeCapabilityRecord) -> String {
    let capabilities = record.capabilities;
    let cpu = match capabilities.cpu_count {
        Some(count) => count.to_string(),
        None => "unknown".to_string(),
    };
    let memory = match capabilities.memory_bytes {
        Some(bytes) => bytes.to_string(),
        None => "unknown".to_string(),
    };
    format!(
        "  capabilities: committed {} cpu-count={cpu} memory-bytes={memory} {}",
        record.identity,
        bounds_line(capabilities.receive)
    )
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
            capabilities: BTreeMap::new(),
            identity: None,
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
            capabilities: BTreeMap::new(),
            identity: None,
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
            capabilities: BTreeMap::new(),
            identity: None,
            leader: Some(name("node-1")),
            leader_record: Some(record(250)),
            nodes: BTreeMap::from([
                (name("node-1"), node("node-1", 1, Some(record(250)))),
                (name("node-2"), node("node-2", 2, Some(record(250)))),
            ]),
        };
        assert_eq!(description.lines()[1], "timing drift: none");
    }

    #[test]
    fn node_capabilities_keep_departed_claims_and_only_current_live_claims_set_effective_bounds() {
        use std::num::NonZeroU64;

        use nervix_models::{ClusterNodeIdentity, NodeCapabilities};

        let claim = |node: &str, incarnation, relay, chunk| {
            let mut receive = nervix_interconnect::PeerReceiveSettings::default().bounds();
            receive.relay_batch_bytes = NonZeroU64::new(relay).expect("positive relay bound");
            receive.bulk_chunk_bytes = NonZeroU64::new(chunk).expect("positive chunk bound");
            NodeCapabilityRecord {
                identity: ClusterNodeIdentity::new(
                    name(node),
                    ClusterNodeIncarnation::new(incarnation),
                ),
                capabilities: NodeCapabilities {
                    cpu_count: None,
                    memory_bytes: NonZeroU64::new(8 << 30),
                    receive,
                },
            }
        };
        let first = claim("node-1", 1, 2048, 16384);
        let second = claim("node-2", 2, 4096, 8192);
        let stale = claim("node-3", 2, 1, 1);
        let departed = claim("node-4", 4, 1, 1);
        let description = ClusterDescription {
            identity: None,
            leader: None,
            leader_record: None,
            nodes: BTreeMap::from([
                (name("node-1"), node("node-1", 1, None)),
                (name("node-2"), node("node-2", 2, None)),
                (name("node-3"), node("node-3", 3, None)),
            ]),
            capabilities: [first.clone(), second.clone(), stale, departed.clone()]
                .into_iter()
                .map(|record| (record.identity.node_id().clone(), record))
                .collect(),
        };
        let lines = description.lines();
        assert!(lines.contains(&capability_line(&first)));
        assert!(capability_line(&first).contains("cpu-count=unknown memory-bytes=8589934592"));
        assert!(lines.contains(&"node: node-4 (departed)".to_string()));
        assert!(lines.contains(&capability_line(&departed)));
        let effective = first
            .capabilities
            .receive
            .intersection(second.capabilities.receive);
        assert_eq!(effective.relay_batch_bytes.get(), 2048);
        assert_eq!(effective.bulk_chunk_bytes.get(), 8192);
        assert!(lines.contains(&format!(
            "cluster-effective receive bounds: {}",
            bounds_line(effective)
        )));
        assert!(
            lines.iter().any(
                |line| line.starts_with("  capabilities: awaiting this incarnation; retained ")
            )
        );
    }
}
