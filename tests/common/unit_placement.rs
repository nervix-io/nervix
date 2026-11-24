//! Where a node reports that a scheduled unit runs: the node that owns it and the nodes that hold
//! replicas of its state, as `SHOW CLUSTER STATUS` schedule lines and `DESCRIBE` outputs render
//! them.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Reading a unit's owner and state replicas from that text, and telling a unit a node
//!   owns from one no node owns.
//! - **Depends on.** The public status and `DESCRIBE` text only.
//! - **Must not know.** Scenario state, or how a scenario reads a node's output.
//!
//! Both outputs write `-` where a unit's owner would be when no node owns the unit, for example
//! after its owner stopped and before failover places it again, and `-` for an empty replica list.
//! That `-` names no node, so it is read here as no owner and no replica. A step waiting for a unit
//! to move off a node therefore keeps waiting until another node owns it.

/// What `SHOW CLUSTER STATUS` and `DESCRIBE` write for an owner, or a replica list, naming no node.
const NO_NODE: &str = "-";

/// Where a `SHOW CLUSTER STATUS` output says one scheduled unit runs.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ScheduledPlacement<'a> {
    /// The node that owns the unit, absent while no node does.
    pub(crate) owner: Option<&'a str>,
    /// The nodes that hold a replica of the unit's state, in the order the status lists them.
    pub(crate) replicas: Vec<&'a str>,
}

impl<'a> ScheduledPlacement<'a> {
    /// The placement `status` lists for the unit of `kind` `name` in `domain`, if it lists that
    /// unit.
    pub(crate) fn from_status(
        status: &'a str,
        domain: &str,
        kind: &str,
        name: &str,
    ) -> Option<Self> {
        for line in status.lines() {
            let Some(line) = line.trim().strip_prefix("- ") else {
                continue;
            };
            let mut line_domain = None;
            let mut line_kind = None;
            let mut line_name = None;
            let mut owner = None;
            let mut replicas = None;
            for field in line.split_whitespace() {
                if let Some(value) = field.strip_prefix("domain=") {
                    line_domain = Some(value);
                } else if let Some(value) = field.strip_prefix("kind=") {
                    line_kind = Some(value);
                } else if let Some(value) = field.strip_prefix("name=") {
                    line_name = Some(value);
                } else if let Some(value) = field.strip_prefix("owner=") {
                    owner = Some(value);
                } else if let Some(value) = field.strip_prefix("replicas=") {
                    replicas = Some(value);
                }
            }
            if line_domain == Some(domain)
                && line_kind == Some(kind)
                && line_name == Some(name)
                && let Some(owner) = owner
            {
                let replicas = match replicas {
                    Some(replicas) => replica_nodes(replicas),
                    None => Vec::new(),
                };
                return Some(Self {
                    owner: owner_node(owner),
                    replicas,
                });
            }
        }
        None
    }

    /// Whether a node other than `node` owns the unit. A unit no node owns has not moved to
    /// another node yet.
    pub(crate) fn is_owned_by_node_other_than(&self, node: &str) -> bool {
        match self.owner {
            Some(owner) => owner != node,
            None => false,
        }
    }
}

/// The node a `DESCRIBE` output names as the owner of its unit, absent when the output names no
/// node or has no owner line.
pub(crate) fn described_owner(output: &str) -> Option<&str> {
    let owner = output
        .lines()
        .find_map(|line| line.trim().strip_prefix("owner: "))?;
    owner_node(owner.trim())
}

/// The nodes a `DESCRIBE` output names as holding replicas of its unit's state.
pub(crate) fn described_replicas(output: &str) -> Vec<&str> {
    let replicas = output
        .lines()
        .find_map(|line| line.trim().strip_prefix("replicas: "));
    match replicas {
        Some(replicas) => replica_nodes(replicas),
        None => Vec::new(),
    }
}

/// The node an owner written as `owner` names, absent for `-`.
fn owner_node(owner: &str) -> Option<&str> {
    if owner == NO_NODE { None } else { Some(owner) }
}

/// The nodes a replica list written as `replicas` names, none for `-`. `SHOW CLUSTER STATUS`
/// separates them with a comma, and `DESCRIBE` with a comma and a space.
fn replica_nodes(replicas: &str) -> Vec<&str> {
    replicas
        .split(',')
        .map(str::trim)
        .filter(|replica| !replica.is_empty() && *replica != NO_NODE)
        .collect()
}
