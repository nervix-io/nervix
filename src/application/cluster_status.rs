//! The cluster and schedule report a client asks for.
//!
//! Layer: control plane.
//!
//! - **Owns.** Rendering membership, leadership and every domain schedule as printable lines.
//! - **Depends on.** The cluster handle and the consensus observer.
//! - **Must not know.** How membership or a schedule is decided.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeSet;

use nervix_consensus::{GossipState, Observer};
use nervix_models::{ClusterNodeName, ScheduledUnit};

use crate::cluster;

pub(in crate::application) async fn render_cluster_status(
    cluster: &cluster::ClusterHandle,
    consensus: &Observer,
) -> String {
    let gossip = cluster.gossip_state().await;
    let mut lines = Vec::new();

    lines.push("[chitchat]".to_string());
    lines.extend(cluster.status_lines().await);
    lines.push(String::new());
    lines.push("[raft]".to_string());
    lines.extend(consensus.status_lines().await);
    lines.push(String::new());
    lines.push("[interconnect]".to_string());
    lines.extend(cluster.interconnect_status_section());
    lines.push(String::new());
    lines.push("[domains]".to_string());
    lines.extend(consensus.domain_status_lines().await);
    lines.push(String::new());
    lines.push("[schedule]".to_string());
    lines.extend(render_cluster_schedule_lines(
        &consensus.current_schedule().await,
    ));
    lines.push(String::new());
    lines.push("[warnings]".to_string());

    let membership = consensus.membership_nodes().await;
    let raft_ids = membership.keys().cloned().collect::<BTreeSet<_>>();
    let availability = cluster.availability_state().await;
    let health_unavailable = cluster.peer_health_snapshot().unavailable_nodes();
    lines.extend(render_membership_warnings(
        &gossip,
        &availability,
        &raft_ids,
        &health_unavailable,
    ));

    lines.join("\n")
}

fn render_membership_warnings(
    gossip: &GossipState,
    availability: &GossipState,
    raft_ids: &BTreeSet<ClusterNodeName>,
    health_unavailable: &BTreeSet<ClusterNodeName>,
) -> Vec<String> {
    let gossip_ids = gossip
        .live_nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    let unavailable = &availability.dead_node_ids;
    let mut lines = Vec::new();
    let mut warned = false;
    for missing in gossip_ids.difference(raft_ids) {
        warned = true;
        lines.push(format!(
            "- gossip node '{missing}' is not present in raft membership"
        ));
    }
    for missing in raft_ids.difference(&gossip_ids) {
        warned = true;
        lines.push(format!(
            "- raft member '{missing}' is not currently visible in gossip"
        ));
    }
    for dead in unavailable.intersection(raft_ids) {
        warned = true;
        let source = if health_unavailable.contains(dead) {
            "application health"
        } else {
            "chitchat"
        };
        lines.push(format!(
            "- raft member '{dead}' is marked unavailable by {source}"
        ));
    }
    if !warned {
        lines.push("- none".to_string());
    }

    lines
}

fn render_cluster_schedule_lines(schedule: &nervix_models::ClusterSchedule) -> Vec<String> {
    if schedule.domains.is_empty() {
        return vec!["- none".to_string()];
    }

    let mut lines = Vec::new();
    for domain in schedule.domains.values() {
        if domain.nodes.is_empty() {
            lines.push(format!("- domain={} nodes=none", domain.domain.as_str()));
            continue;
        }

        // One line per unit: a node without partitions keeps its one line, and each partition of
        // a partitioned relay names its own owner and replicas.
        for node in domain.nodes.values() {
            for (unit, scheduled) in &node.units {
                let owner = match node.unit_execution_node(*unit) {
                    Some(owner) => owner.as_str(),
                    None => "-",
                };
                lines.push(format!(
                    "- domain={} kind={} name={}{} owner={owner} replicas={}{}",
                    domain.domain.as_str(),
                    node.kind().as_str(),
                    node.identifier.as_str(),
                    unit.status_field(),
                    format_schedule_status_replicas(scheduled),
                    format_ownership_transition(scheduled)
                ));
            }
        }
    }
    lines
}

fn format_schedule_status_replicas(unit: &ScheduledUnit) -> String {
    let replicas = unit.replica_nodes();
    if replicas.is_empty() {
        "-".to_string()
    } else {
        replicas
            .iter()
            .map(|node| node.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn format_ownership_transition(unit: &ScheduledUnit) -> String {
    let Some(transition) = unit.ownership_transition.as_ref() else {
        return String::new();
    };
    let mut rendered = format!(
        " transition_from={} state_recovery={}",
        transition.source,
        transition.state_recovery.as_ref()
    );
    if !transition.resets.is_empty() {
        rendered.push_str(" resets=");
        rendered.push_str(
            &transition
                .resets
                .iter()
                .map(|reset| format!("{}:{}", reset.component.as_ref(), reset.cause.as_ref()))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    rendered
}

#[cfg(test)]
mod tests {
    use nervix_consensus::GossipNode;
    use nervix_models::{ClusterNodeIncarnation, NodeEndpoint};

    use super::*;

    fn peer() -> GossipNode {
        GossipNode {
            node_id: ClusterNodeName::parse("node-3").expect("valid peer name"),
            incarnation: ClusterNodeIncarnation::new(7),
            terminating: false,
            client_url: None,
            console_url: None,
            interconnect_endpoint: Some(NodeEndpoint::new("127.0.0.1", 33015)),
            prerequisites: None,
        }
    }

    #[test]
    fn membership_warnings_preserve_a_health_retained_peer() {
        let peer = peer();
        let membership = BTreeSet::from([peer.node_id.clone()]);
        let gossip = GossipState {
            live_nodes: Vec::new(),
            dead_node_ids: membership.clone(),
            dead_node_identities: BTreeSet::from([peer.identity()]),
        };
        let availability = GossipState {
            live_nodes: vec![peer],
            dead_node_ids: BTreeSet::new(),
            dead_node_identities: gossip.dead_node_identities.clone(),
        };
        assert_eq!(
            render_membership_warnings(&gossip, &availability, &membership, &BTreeSet::new()),
            vec!["- raft member 'node-3' is not currently visible in gossip"]
        );
    }

    #[test]
    fn membership_warnings_report_each_effective_unavailability_owner() {
        let peer = peer();
        let membership = BTreeSet::from([peer.node_id.clone()]);
        let gossip = GossipState {
            live_nodes: Vec::new(),
            dead_node_ids: membership.clone(),
            dead_node_identities: BTreeSet::from([peer.identity()]),
        };
        for (health_unavailable, owner) in [
            (BTreeSet::new(), "chitchat"),
            (membership.clone(), "application health"),
        ] {
            assert_eq!(
                render_membership_warnings(&gossip, &gossip, &membership, &health_unavailable),
                vec![
                    "- raft member 'node-3' is not currently visible in gossip".to_string(),
                    format!("- raft member 'node-3' is marked unavailable by {owner}"),
                ]
            );
        }
    }

    #[test]
    fn membership_warnings_report_health_exclusion_of_a_gossip_live_peer() {
        let peer = peer();
        let membership = BTreeSet::from([peer.node_id.clone()]);
        let gossip = GossipState {
            live_nodes: vec![peer.clone()],
            dead_node_ids: BTreeSet::new(),
            dead_node_identities: BTreeSet::new(),
        };
        let availability = GossipState {
            live_nodes: vec![peer],
            dead_node_ids: membership.clone(),
            dead_node_identities: BTreeSet::new(),
        };
        assert_eq!(
            render_membership_warnings(&gossip, &availability, &membership, &membership),
            vec!["- raft member 'node-3' is marked unavailable by application health"]
        );
    }
}
