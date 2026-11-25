//! Public status observations used by in-process cluster waits.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** One-pass cluster status parsing and committed-uniform voter readiness.
//! - **Depends on.** Standard collections and the current public status text.
//! - **Must not know.** Scenario state, node lifecycle or leadership-transfer policy.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub(super) struct ClusterStatus {
    pub(super) raw: String,
    pub(super) current_leader: Option<String>,
    pub(super) current_term: Option<u64>,
    pub(super) raft_state: Option<String>,
    pub(super) last_applied: Option<u64>,
    pub(super) membership: BTreeMap<String, String>,
    pub(super) membership_joint: Option<bool>,
    pub(super) membership_committed: Option<bool>,
    pub(super) interconnect: BTreeMap<String, String>,
    /// The other nodes this node's gossip calls live, once the status lists them.
    pub(super) live_gossip_nodes: Option<BTreeSet<String>>,
    /// The other nodes of which this node's gossip calls a run live that has not announced its
    /// stop. A node that restarted is listed by its new run beside its terminating earlier run
    /// until the earlier one leaves, so it is here once the gossip knows the new run.
    pub(super) running_gossip_nodes: BTreeSet<String>,
}

impl ClusterStatus {
    pub(super) fn parse(raw: String) -> Self {
        let mut current_leader = None;
        let mut current_term = None;
        let mut raft_state = None;
        let mut last_applied = None;
        let mut membership = BTreeMap::new();
        let mut membership_joint = None;
        let mut membership_committed = None;
        let mut interconnect = BTreeMap::new();
        let mut live_gossip_nodes = BTreeSet::new();
        let mut running_gossip_nodes = BTreeSet::new();
        // The node whose listed run the indented advertisements below describe.
        let mut listed_gossip_node: Option<String> = None;
        let mut lists_live_gossip = false;
        let mut in_membership = false;
        let mut in_interconnect = false;
        let mut in_live_gossip = false;

        for line in raw.lines() {
            if line == "live_nodes:" {
                in_live_gossip = true;
                lists_live_gossip = true;
                continue;
            }
            if in_live_gossip {
                if let Some(node_id) = line.strip_prefix("- node_id: ") {
                    let node_id = node_id.trim().to_string();
                    live_gossip_nodes.insert(node_id.clone());
                    listed_gossip_node = Some(node_id);
                    continue;
                }
                // A listed node's other advertisements are indented beneath its name.
                if line.starts_with("  ") {
                    if line.trim() == "terminating: false"
                        && let Some(node_id) = &listed_gossip_node
                    {
                        running_gossip_nodes.insert(node_id.clone());
                    }
                    continue;
                }
                in_live_gossip = false;
                listed_gossip_node = None;
            }

            if let Some(value) = line.strip_prefix("raft.current_leader: ") {
                let trimmed = value.trim();
                if trimmed != "(none)" {
                    current_leader = Some(trimmed.to_string());
                }
            }

            if let Some(value) = line.strip_prefix("raft.current_term: ") {
                current_term = value.trim().parse::<u64>().ok();
            }

            if let Some(value) = line.strip_prefix("raft.state: ") {
                raft_state = Some(value.trim().to_string());
            }

            if let Some(value) = line.strip_prefix("raft.last_applied: ") {
                let trimmed = value.trim();
                if trimmed != "(none)" {
                    last_applied = trimmed.parse::<u64>().ok();
                }
            }

            if let Some(value) = line.strip_prefix("raft.membership_joint: ") {
                membership_joint = value.trim().parse().ok();
            }
            if let Some(value) = line.strip_prefix("raft.membership_committed: ") {
                membership_committed = value.trim().parse().ok();
            }

            if line == "raft.membership:" {
                in_membership = true;
                in_interconnect = false;
                continue;
            }

            if line == "[interconnect]" {
                in_interconnect = true;
                in_membership = false;
                continue;
            }

            if in_membership {
                if !line.starts_with("- ") {
                    in_membership = false;
                } else if let Some((node_id, role)) = parse_membership_line(line) {
                    membership.insert(node_id, role);
                }
            }

            if in_interconnect {
                if !line.starts_with("- ") {
                    in_interconnect = false;
                } else if let Some((node_id, status)) = parse_interconnect_line(line) {
                    interconnect.insert(node_id, status);
                }
            }
        }

        Self {
            raw,
            current_leader,
            current_term,
            raft_state,
            last_applied,
            membership,
            membership_joint,
            membership_committed,
            interconnect,
            live_gossip_nodes: lists_live_gossip.then_some(live_gossip_nodes),
            running_gossip_nodes,
        }
    }
}

impl ClusterStatus {
    fn membership_is_ready(&self) -> bool {
        self.membership_joint == Some(false) && self.membership_committed == Some(true)
    }

    pub(super) fn has_ready_voters(&self, expected: &BTreeSet<String>) -> bool {
        if !self.membership_is_ready() {
            return false;
        }
        let mut observed = BTreeSet::new();
        for (node, role) in &self.membership {
            if role == "voter" {
                observed.insert(node.clone());
            }
        }
        &observed == expected
    }

    /// Whether a committed uniform membership holds `node_id` as a voter.
    pub(super) fn is_ready_voter(&self, node_id: &str) -> bool {
        if !self.membership_is_ready() {
            return false;
        }
        match self.membership.get(node_id) {
            Some(role) => role == "voter",
            None => false,
        }
    }

    /// Whether this node's gossip, as the status lists it, no longer calls `node_id` live. A
    /// status that lists no live gossip proves nothing, so it never reads as a departure.
    pub(super) fn has_left_live_gossip(&self, node_id: &str) -> bool {
        match &self.live_gossip_nodes {
            Some(live) => !live.contains(node_id),
            None => false,
        }
    }

    /// Whether this node's gossip, as the status lists it, calls a run of `node_id` live that has
    /// not announced its stop. Planning reads a node's newest live run, so once this holds, this
    /// node no longer plans against an earlier run of `node_id` that is terminating.
    pub(super) fn lists_running_gossip_node(&self, node_id: &str) -> bool {
        self.running_gossip_nodes.contains(node_id)
    }
}

fn parse_membership_line(line: &str) -> Option<(String, String)> {
    let item = line.strip_prefix("- ")?;
    let (node_id, rest) = item.split_once(" [")?;
    let (role, _) = rest.split_once(']')?;
    Some((node_id.to_string(), role.to_string()))
}

fn parse_interconnect_line(line: &str) -> Option<(String, String)> {
    let item = line.strip_prefix("- ")?;
    let (node_id, rest) = item.split_once(": ")?;
    let (_, status) = rest.split_once("status=")?;
    Some((node_id.to_string(), status.to_string()))
}

#[cfg(test)]
mod membership_status_tests {
    use super::{BTreeSet, ClusterStatus};

    #[rstest::rstest]
    #[case(true, true, false)]
    #[case(true, false, false)]
    #[case(false, false, false)]
    #[case(false, true, true)]
    fn voter_readiness_requires_a_committed_uniform_membership(
        #[case] joint: bool,
        #[case] committed: bool,
        #[case] ready: bool,
    ) {
        let expected = BTreeSet::from([
            "node-1".to_string(),
            "node-2".to_string(),
            "node-3".to_string(),
        ]);
        let raw = format!(
            "[raft]\nraft.state: Leader\nraft.current_leader: node-1\nraft.current_term: \
             7\nraft.last_applied: 42\nraft.membership_joint: {joint}\nraft.membership_committed: \
             {committed}\nraft.membership:\n- node-1 [voter] 127.0.0.1:41001\n- node-2 [voter] \
             127.0.0.1:41002\n- node-3 [voter] 127.0.0.1:41003\n\n[interconnect]\n- node-2: \
             addr=127.0.0.1:41002 status=connected"
        );
        let status = ClusterStatus::parse(raw.clone());
        assert_eq!(status.raw, raw);
        assert_eq!(status.current_leader.as_deref(), Some("node-1"));
        assert_eq!(status.current_term, Some(7));
        assert_eq!(status.raft_state.as_deref(), Some("Leader"));
        assert_eq!(status.last_applied, Some(42));
        assert_eq!(
            status.interconnect.get("node-2").map(String::as_str),
            Some("connected")
        );
        assert_eq!(status.has_ready_voters(&expected), ready);
        assert_eq!(status.is_ready_voter("node-3"), ready);
    }

    #[test]
    fn live_gossip_lists_only_the_other_live_nodes() {
        let raw = "[chitchat]\ncluster_id: cucumber\nseed_nodes: (none)\nself:\n- node_id: \
                   node-1\n  gossip_addr: 127.0.0.1:41001\n  terminating: false\nlive_nodes:\n- \
                   node_id: node-2\n  gossip_addr: 127.0.0.1:41002\n  terminating: true\n- \
                   node_id: node-3\n  gossip_addr: 127.0.0.1:41003\n\n[raft]\nraft.membership:\n- \
                   node-1 [voter] 127.0.0.1:41001\n- node-2 [learner] 127.0.0.1:41002"
            .to_string();
        let status = ClusterStatus::parse(raw);

        assert_eq!(
            status.live_gossip_nodes,
            Some(BTreeSet::from(["node-2".to_string(), "node-3".to_string()]))
        );
        assert!(!status.has_left_live_gossip("node-2"));
        assert!(status.has_left_live_gossip("node-4"));
        assert_eq!(
            status.membership.get("node-2").map(String::as_str),
            Some("learner")
        );
    }

    #[test]
    fn a_restarted_node_runs_once_gossip_lists_its_new_run_beside_the_terminating_one() {
        let raw = "[chitchat]\ncluster_id: cucumber\nseed_nodes: (none)\nself:\n- node_id: \
                   node-1\n  gossip_addr: 127.0.0.1:41001\n  terminating: false\nlive_nodes:\n- \
                   node_id: node-2\n  gossip_addr: 127.0.0.1:41002\n  terminating: true\n- \
                   node_id: node-3\n  gossip_addr: 127.0.0.1:41003\n  terminating: true\n- \
                   node_id: node-2\n  gossip_addr: 127.0.0.1:41002\n  terminating: \
                   false\n\n[raft]\nraft.state: Leader"
            .to_string();
        let status = ClusterStatus::parse(raw);

        assert!(status.lists_running_gossip_node("node-2"));
        assert!(!status.lists_running_gossip_node("node-3"));
        // Its own run is listed apart from the live nodes, as `self`.
        assert!(!status.lists_running_gossip_node("node-1"));
        assert_eq!(
            status.live_gossip_nodes,
            Some(BTreeSet::from(["node-2".to_string(), "node-3".to_string()]))
        );
    }

    #[test]
    fn a_status_without_live_gossip_proves_no_departure() {
        let empty = ClusterStatus::parse("live_nodes:\n\n[raft]".to_string());
        assert_eq!(empty.live_gossip_nodes, Some(BTreeSet::new()));
        assert!(empty.has_left_live_gossip("node-2"));

        let unlisted = ClusterStatus::parse("[raft]\nraft.state: Leader".to_string());
        assert_eq!(unlisted.live_gossip_nodes, None);
        assert!(!unlisted.has_left_live_gossip("node-2"));
    }
}
