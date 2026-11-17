//! Fenced recovery of a domain pause that no committing transaction owns.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The conditional proposal and its atomic lease, transaction and restore-gate checks.
//! - **Depends on.** The applied consensus records, typed mutation authority and domain status.
//! - **Must not know.** Sessions, runtime alteration guards, graph planning or intake installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns conditional replicated pause recovery")
)]

use error_stack::Report;
use nervix_models::{DomainName, DomainStatus};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    AppliedConsensusCommand, ConsensusCommand, ConsensusConflict, ConsensusError,
    ConsensusResponse, DomainMutationLease, Proposer, StateMachineChanges, StateMachineData,
    TransactionState, restore, validate_domain_mutation,
};

/// What the applied revision decided, independently of an earlier coordinator's observation.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum OrphanedPauseRecovery {
    Resumed,
    Unchanged,
    TransactionActive { transaction: String },
}

impl Proposer {
    pub async fn recover_orphaned_model_pause(
        &self,
        domain_id: DomainName,
        mutation: &DomainMutationLease,
    ) -> Result<OrphanedPauseRecovery, Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::RecoverOrphanedModelPause {
                domain_id,
                mutation: Box::new(mutation.clone()),
            })
            .await?;
        match response.data {
            ConsensusResponse::OrphanedPauseRecovery(recovery) => Ok(recovery),
            ConsensusResponse::Conflict(cause) => Err(Report::new(ConsensusError::Conflict(cause))),
            ConsensusResponse::Applied | ConsensusResponse::Transaction(_) => {
                Err(Report::new(ConsensusError::UnexpectedResponse))
            }
        }
    }
}

impl StateMachineData {
    pub(super) fn recover_orphaned_model_pause(
        &mut self,
        domain: &DomainName,
        mutation: &DomainMutationLease,
    ) -> AppliedConsensusCommand {
        // A stale lease is refused even when the domain no longer has a pause to release.
        if let Err(error) = validate_domain_mutation(self, domain, Some(mutation)) {
            // This is the fixed replicated outcome boundary; local report frames stay here.
            return AppliedConsensusCommand::conflict(ConsensusConflict::DomainMutation {
                cause: Box::new(error.current_context().clone()),
            });
        }
        if let Err(error) = restore::validate_restored_domain_start(self, domain) {
            return AppliedConsensusCommand::conflict(ConsensusConflict::RestoreInstallation {
                cause: Box::new(error.current_context().clone()),
            });
        }
        for transaction in self.transactions.values() {
            if transaction.domain == *domain
                && matches!(transaction.state, TransactionState::Committing(_))
            {
                return AppliedConsensusCommand::pause_recovery(
                    OrphanedPauseRecovery::TransactionActive {
                        transaction: transaction.id.clone(),
                    },
                    StateMachineChanges::default(),
                );
            }
        }
        if self
            .domains
            .get(domain)
            .is_some_and(|state| state.status == DomainStatus::Paused)
            && let Some(reason) = self.licensing_stop()
        {
            return AppliedConsensusCommand::conflict(ConsensusConflict::LicensingStop { reason });
        }
        let mut changes = StateMachineChanges::default();
        let recovery = if let Some(state) = self.domains.get_mut(domain)
            && let DomainStatus::Paused = state.status
        {
            state.status = DomainStatus::Running;
            changes.domains_changed = true;
            OrphanedPauseRecovery::Resumed
        } else {
            OrphanedPauseRecovery::Unchanged
        };
        AppliedConsensusCommand::pause_recovery(recovery, changes)
    }
}

impl AppliedConsensusCommand {
    fn pause_recovery(recovery: OrphanedPauseRecovery, changes: StateMachineChanges) -> Self {
        Self {
            response: ConsensusResponse::OrphanedPauseRecovery(recovery),
            schedule_changed: changes.schedule_changed,
            domains_changed: changes.domains_changed,
            resources_changed: changes.resources_changed,
            transactions_changed: changes.transactions_changed,
        }
    }
}

#[cfg(test)]
mod tests;
