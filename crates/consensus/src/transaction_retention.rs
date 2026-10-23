//! Reclamation eligibility for finished transactions and their retained reports.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The pure retention decision over one coherent replicated state.
//! - **Depends on.** Current transaction states, mutation owners and command execution records.
//! - **Must not know.** Sessions, runtime tasks, transport retries or reconciliation cadence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_models::Timestamp;

use crate::{CommandExecution, DomainMutationOwner, StateMachineData, TransactionState};

impl StateMachineData {
    /// Aged tombstones that no applying execution can resume through. Both the leader's cleanup
    /// query and replicated removal use this decision; removal rechecks any intervening admission.
    pub(crate) fn finished_transaction_removals(&self, finished_before: Timestamp) -> Vec<String> {
        let targets = self.command_executions.applying_transaction_targets();
        let mut removals = Vec::new();
        for (id, transaction) in self.transactions.iter() {
            let TransactionState::Finished(finished) = &transaction.state else {
                continue;
            };
            if finished.finished_at > finished_before || targets.contains(id.as_str()) {
                continue;
            }
            // Internal planning refresh attempts share the command's durable mutation owner;
            // their generated transaction names do not select their retention lifetime.
            if let DomainMutationOwner::Command(reference) = transaction.mutation_owner()
                && self
                    .command_executions
                    .get(reference)
                    .is_some_and(CommandExecution::is_applying)
            {
                continue;
            }
            removals.push(id.clone());
        }
        removals
    }
}
