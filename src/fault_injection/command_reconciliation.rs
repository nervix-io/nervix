//! A request's ownership of the command it finalizes in a controlled recovery scenario.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Deferring one node's reconciliation of one execution before it takes that execution's guard.
//! - **Depends on.** Vocabulary identities and the shared command pause lifetime.
//! - **Must not know.** Command effects, expected outcomes or how a client retries.

#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "owns a testing-only command reconciliation hold")
)]

use ahash::HashSet;
use nervix_models::{ClusterNodeName, CommandExecutionReference};
use nervix_recovery::Discarded as _;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Defers this execution without waiting inside the reconciliation loop. Other nodes and
    /// executions remain free to reconcile; explicit release or scenario cleanup ends the hold.
    pub fn defer_command_reconciliation_on(
        &self,
        node: ClusterNodeName,
        reference: CommandExecutionReference,
    ) {
        self.arm_command_pause(CommandPausePoint::Reconciliation { node, reference });
    }

    /// Lets the next reconciliation pass reconsider this exact execution.
    pub fn resume_command_reconciliation_on(
        &self,
        node: &ClusterNodeName,
        reference: &CommandExecutionReference,
    ) {
        let point = CommandPausePoint::Reconciliation {
            node: node.clone(),
            reference: reference.clone(),
        };
        let pause = self.command_pause(&point);
        self.inner
            .command_pauses
            .remove(&point)
            .discarded("the scenario ends this exact reconciliation hold");
        pause.release();
    }

    /// Captures one pass's holds without retaining a map guard while execution proceeds.
    pub(crate) fn deferred_command_reconciliations(
        &self,
        node: &ClusterNodeName,
    ) -> HashSet<CommandExecutionReference> {
        let mut deferred = HashSet::default();
        for pause in &self.inner.command_pauses {
            if let CommandPausePoint::Reconciliation {
                node: selected_node,
                reference,
            } = pause.key()
                && selected_node == node
            {
                deferred.insert(reference.clone());
            }
        }
        deferred
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{ClusterNodeName, CommandExecutionReference};

    use super::FaultInjection;

    #[test]
    fn a_reconciliation_hold_selects_one_node_and_execution_and_ends_with_its_owner() {
        let faults = FaultInjection::default();
        let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
        let other_node =
            ClusterNodeName::parse("node-2").assured("the other fixture node name is valid");
        let reference = CommandExecutionReference::parse("018bcfe5-6800-7000-8000-000000000001")
            .assured("the fixture execution reference is UUIDv7");
        let other_reference =
            CommandExecutionReference::parse("018bcfe5-6800-7000-8000-000000000002")
                .assured("the other fixture execution reference is UUIDv7");
        faults.defer_command_reconciliation_on(node.clone(), reference.clone());
        let selected = faults.deferred_command_reconciliations(&node);
        assert!(
            faults
                .deferred_command_reconciliations(&other_node)
                .is_empty()
        );
        assert!(!selected.contains(&other_reference));
        assert_eq!(selected.len(), 1);
        assert!(selected.contains(&reference));
        faults.resume_command_reconciliation_on(&node, &reference);
        assert!(selected.contains(&reference));
        assert!(faults.deferred_command_reconciliations(&node).is_empty());

        faults.defer_command_reconciliation_on(node.clone(), reference.clone());
        assert!(
            faults
                .deferred_command_reconciliations(&node)
                .contains(&reference)
        );
        faults.release_all_command_pauses();
        assert!(faults.deferred_command_reconciliations(&node).is_empty());
    }
}
