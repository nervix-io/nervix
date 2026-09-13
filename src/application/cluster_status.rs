//! The cluster and schedule report a client asks for.
//!
//! Layer: control plane.
//!
//! - **Owns.** Rendering membership, leadership and every domain schedule as printable lines.
//! - **Depends on.** The cluster handle and the consensus observer.
//! - **Must not know.** How membership or a schedule is decided.

use nervix_consensus::Observer;
use nervix_models::ScheduledNode;

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

    let gossip_ids = gossip
        .live_nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let membership = consensus.membership_nodes().await;
    let raft_ids = membership
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let health_unavailable = cluster.peer_health_snapshot().unavailable_nodes();
    let mut unavailable = gossip.dead_node_ids.clone();
    unavailable.extend(health_unavailable.iter().cloned());

    let mut warned = false;
    for missing in gossip_ids.difference(&raft_ids) {
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
    for dead in unavailable.intersection(&raft_ids) {
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

    lines.join("\n")
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

        for node in domain.nodes.values() {
            let owner = match node.execution_node() {
                Some(owner) => owner.as_str(),
                None => "-",
            };
            lines.push(format!(
                "- domain={} kind={} name={} owner={owner} replicas={}{}",
                domain.domain.as_str(),
                node.kind().as_str(),
                node.identifier.as_str(),
                format_schedule_status_replicas(node),
                format_ownership_transition(node)
            ));
        }
    }
    lines
}

fn format_schedule_status_replicas(node: &ScheduledNode) -> String {
    let replicas = node.replica_nodes();
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

fn format_ownership_transition(node: &ScheduledNode) -> String {
    let Some(transition) = node.ownership_transition.as_ref() else {
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
