//! Where a node reports that a scheduled unit runs: the node that owns it and the nodes that hold
//! replicas of its state, as `SHOW CLUSTER STATUS` schedule lines and `DESCRIBE` outputs render
//! them.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Reading a unit's owner and state replicas from that text.
//! - **Depends on.** The public status and `DESCRIBE` text only.
//! - **Must not know.** Scenario state, or how a scenario reads a node's output.

/// Where a `SHOW CLUSTER STATUS` output says one scheduled unit runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScheduledPlacement<'a> {
    /// The owner the status names for the unit.
    pub(crate) owner: &'a str,
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
        status.lines().map(str::trim).find_map(|line| {
            let line = line.strip_prefix("- ")?;
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

            if line_domain == Some(domain) && line_kind == Some(kind) && line_name == Some(name) {
                let replica_nodes = replicas
                    .filter(|value| *value != "-")
                    .map(|value| {
                        value
                            .split(',')
                            .map(str::trim)
                            .filter(|replica| !replica.is_empty())
                            .collect()
                    })
                    .unwrap_or_default();
                owner.map(|owner| Self {
                    owner,
                    replicas: replica_nodes,
                })
            } else {
                None
            }
        })
    }

    /// Whether the status names an owner other than `node`.
    pub(crate) fn is_owned_by_node_other_than(&self, node: &str) -> bool {
        self.owner != node
    }
}

/// The owner a `DESCRIBE` output names for its unit, if it has an owner line.
pub(crate) fn described_owner(output: &str) -> Option<&str> {
    output
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("owner: ").map(str::trim))
}

/// The nodes a `DESCRIBE` output names as holding replicas of its unit's state.
pub(crate) fn described_replicas(output: &str) -> Vec<&str> {
    output
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("replicas: ").map(str::trim))
        .map(|replicas| {
            replicas
                .split(',')
                .map(str::trim)
                .filter(|replica| !replica.is_empty() && *replica != "-")
                .collect()
        })
        .unwrap_or_default()
}
