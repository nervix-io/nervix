//! The runtime revisions the nodes of a cluster applied, and the first node to apply a newer one.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** What each node incarnation answered for the newest runtime revision it applied, and
//!   the comparison that finds the first node whose answer moved on from a recording of them.
//! - **Depends on.** The application progress record a node keeps for its own incarnation.
//! - **Must not know.** How a scenario asks a node, or what a newer revision released.
//!
//! A node applies a runtime revision whenever the cluster's schedule or domains change. It applied
//! a revision once it prepared it, and also once it failed to prepare it: the failed attempt had
//! already started doing what the revision asked of the node. A node that answers under another
//! incarnation restarted, which is a disruption of its own.

use std::{collections::BTreeMap, fmt};

use nervix_interconnect::ApplicationProgress;
use nervix_models::ClusterNodeIdentity;

/// The newest runtime revision one node incarnation applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeRuntimeApplication {
    identity: ClusterNodeIdentity,
    /// Absent until the incarnation prepared, or failed to prepare, a runtime revision.
    newest: Option<u64>,
}

impl NodeRuntimeApplication {
    /// What `progress` says its incarnation applied: the newer of the runtime revision it prepared
    /// and the one it failed to prepare.
    pub(crate) fn of(progress: &ApplicationProgress) -> Self {
        let failed = progress
            .runtime_failure
            .as_ref()
            .map(|failure| failure.revision);
        Self {
            identity: progress.identity.clone(),
            newest: progress.runtime_prepared.max(failed),
        }
    }
}

impl fmt::Display for NodeRuntimeApplication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.newest {
            Some(revision) => write!(formatter, "{} at revision {revision}", self.identity),
            None => write!(formatter, "{} at no revision", self.identity),
        }
    }
}

/// What the nodes of a cluster answered at one moment, by node name. A node that did not answer is
/// absent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RuntimeApplications {
    nodes: BTreeMap<String, NodeRuntimeApplication>,
}

impl RuntimeApplications {
    /// Records what `node` answered.
    pub(crate) fn record(&mut self, node: String, application: NodeRuntimeApplication) {
        self.nodes.insert(node, application);
    }

    /// The newest runtime revision any node that answered had applied.
    fn newest(&self) -> Option<u64> {
        let mut newest = None;
        for application in self.nodes.values() {
            newest = newest.max(application.newest);
        }
        newest
    }

    /// The first node, by name, whose answer moved on from `recorded`: one that answers under
    /// another incarnation, or applied a newer runtime revision than `recorded` holds for it. A node
    /// that did not answer the recording is compared with the newest revision any node had applied
    /// then.
    pub(crate) fn moved_on_from(&self, recorded: &Self) -> Option<NewerApplication> {
        for (node, current) in &self.nodes {
            let recorded_revision = match recorded.nodes.get(node) {
                Some(before) if before.identity != current.identity => {
                    return Some(NewerApplication::Restarted {
                        recorded: before.identity.clone(),
                        current: current.identity.clone(),
                    });
                }
                Some(before) => before.newest,
                None => recorded.newest(),
            };
            if let Some(applied) = current.newest
                && current.newest > recorded_revision
            {
                return Some(NewerApplication::Applied {
                    node: current.identity.clone(),
                    recorded: recorded_revision,
                    applied,
                });
            }
        }
        None
    }
}

impl fmt::Display for RuntimeApplications {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nodes.is_empty() {
            return formatter.write_str("no node answered");
        }
        let mut separator = "";
        for application in self.nodes.values() {
            write!(formatter, "{separator}{application}")?;
            separator = ", ";
        }
        Ok(())
    }
}

/// A node whose answer moved on from a recording.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NewerApplication {
    /// The node applied a runtime revision newer than the one recorded for it, or newer than any
    /// node had applied when it did not answer the recording.
    Applied {
        node: ClusterNodeIdentity,
        recorded: Option<u64>,
        applied: u64,
    },
    /// The node answers under an incarnation other than the recorded one.
    Restarted {
        recorded: ClusterNodeIdentity,
        current: ClusterNodeIdentity,
    },
}

impl fmt::Display for NewerApplication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Applied {
                node,
                recorded: Some(recorded),
                applied,
            } => write!(
                formatter,
                "{node} applied runtime revision {applied} after revision {recorded}"
            ),
            Self::Applied {
                node,
                recorded: None,
                applied,
            } => write!(
                formatter,
                "{node} applied runtime revision {applied} after none"
            ),
            Self::Restarted { recorded, current } => {
                write!(formatter, "{recorded} restarted as {current}")
            }
        }
    }
}
