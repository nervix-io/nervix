//! Pure selector decisions over committed node facts.
//!
//! Layer: decisions.
//! - **Owns.** Turning a selector expression into a typed plan and deciding which records match.
//! - **Depends on.** The node record vocabulary and the VM's node predicate engine.
//! - **Must not know.** Membership, clocks, observation routing or the caller's placement policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "decides selector matches from typed inputs")
)]

use std::collections::{BTreeMap, BTreeSet};

use error_stack::Report;
use nervix_models::{ClusterNodeName, CommittedNodeRecord, Expression};
use nervix_vm::{NodePredicate, NodePredicateError, RuntimeError, SideError};

#[derive(Debug, Clone)]
pub(crate) struct NodeSelector(NodePredicate);

#[derive(Debug)]
pub(crate) struct NodeSelection {
    pub(crate) matches: BTreeSet<ClusterNodeName>,
    pub(crate) failures: Vec<NodeSelectionFailure>,
}

#[derive(Debug, Clone)]
pub(crate) struct NodeSelectionFailure {
    #[cfg_attr(
        nervix_lint,
        nervix::identity(node, reason = "the node whose selector evaluation failed")
    )]
    pub(crate) node: ClusterNodeName,
    pub(crate) errors: Vec<SideError>,
}

impl NodeSelector {
    pub(crate) fn compile(expression: &Expression) -> Result<Self, Report<NodePredicateError>> {
        Ok(Self(NodePredicate::compile(expression)?))
    }

    pub(crate) fn evaluate(
        &self,
        records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
    ) -> Result<NodeSelection, Report<RuntimeError>> {
        let result = self.0.evaluate(records)?;
        let mut selected = result.selected_rows.iter().peekable();
        let mut matches = BTreeSet::new();
        let mut failures = Vec::new();
        for (row, node) in records.keys().enumerate() {
            let errors = result.input_errors.row(row);
            if !errors.is_empty() {
                failures.push(NodeSelectionFailure {
                    node: node.clone(),
                    errors: errors.to_vec(),
                });
            }
            if selected.peek() == Some(&row) {
                selected.next();
                matches.insert(node.clone());
            }
        }
        Ok(NodeSelection { matches, failures })
    }
}
