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
        let mut in_membership = false;
        let mut in_interconnect = false;

        for line in raw.lines() {
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
