//! Identities attached to runtime-state snapshots awaiting handoff or forced recovery.
//!
//! Layer: data plane.
//!
//! - **Owns.** The exact preparation that may supply one pending runtime-state snapshot.
//! - **Depends on.** Shared coordination identities and persisted runtime-state entries.
//! - **Must not know.** Schedule planning, interconnect transport, or state activation policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use nervix_interconnect::{ForcedOwnershipRecoveryRound, PrepareForcedOwnershipRecoveryRequest};
use nervix_primitives::collections::dash_map::Entry;

impl<'a> ForcedRuntimeStateRecoveryTransition<'a> {
    pub(super) fn from_request(
        request: &'a PrepareForcedOwnershipRecoveryRequest,
        entity: &'a DomainNodeRef,
    ) -> Self {
        Self {
            operation_id: &request.operation_id,
            source: &request.source,
            destination: &request.destination,
            destination_incarnation: request.destination_incarnation,
            entity,
            unit: request.unit,
            target_schedule_fingerprint: request.target_schedule_fingerprint,
        }
    }
}

use super::*;

#[derive(Debug)]
pub(in crate::runtime) struct PreparedRuntimeStateSnapshot {
    pub(super) preparation: RuntimeStatePreparationIdentity,
    pub(super) snapshot: PersistedRuntimeStateEntry,
}

#[derive(Debug)]
pub(super) enum RuntimeStatePreparationIdentity {
    OwnershipHandoff {
        coordination: CoordinationIdentity,
        operation_id: String,
    },
    ForcedRecovery,
}

impl RuntimeStatePreparationIdentity {
    pub(super) fn is_ownership_handoff(
        &self,
        coordination: &CoordinationIdentity,
        operation_id: &str,
    ) -> bool {
        match self {
            Self::OwnershipHandoff {
                coordination: prepared_coordination,
                operation_id: prepared_operation_id,
            } => prepared_coordination == coordination && prepared_operation_id == operation_id,
            Self::ForcedRecovery => false,
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct PreparedForcedRuntimeStateRecovery {
    pub(super) recovery: ForcedRuntimeStateRecoveryIdentity,
    pub(super) destination_incarnation: ClusterNodeIncarnation,
    pub(super) target_schedule_fingerprint: [u8; 32],
    pub(super) checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
}

impl PreparedForcedRuntimeStateRecovery {
    fn matches(&self, transition: &ForcedRuntimeStateRecoveryTransition<'_>) -> bool {
        self.recovery.matches(transition)
            && self.destination_incarnation == transition.destination_incarnation
            && self.target_schedule_fingerprint == transition.target_schedule_fingerprint
    }
}

impl Runtime {
    pub(super) fn stage_forced_recovery_preparation(
        &self,
        round: &ForcedOwnershipRecoveryRound,
        transition: &ForcedRuntimeStateRecoveryTransition<'_>,
        checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
    ) -> OwnershipHandoffResult<()> {
        let key = DomainNodeUnitRef::new(transition.entity.clone(), transition.unit);
        let store = self.inner.state_store.as_deref();
        let encoded = store
            .map(|store| {
                RuntimeStateStore::encode_forced_recovery_preparation(transition, &checkpoints)
                    .map(|encoded| (store, encoded))
            })
            .transpose()
            .change_context(OwnershipHandoffError::Persistence)?;
        let entry = self
            .inner
            .prepared_forced_runtime_state_recoveries
            .entry(key.clone());
        // The discard records its round before it takes the same unit slot. A prepare that
        // acquired the slot first either observes that fence or is removed by the discard.
        if *self.inner.rejected_forced_recovery_round.borrow() >= forced_recovery_round_order(round)
        {
            return Err(OwnershipHandoffError::participant(
                "the forced recovery preparation round has ended",
            ));
        }
        let written = encoded
            .map(|(store, encoded)| {
                store
                    .write_forced_recovery_preparation(transition, encoded)
                    .map(|written| written.then_some(store))
            })
            .transpose()
            .change_context(OwnershipHandoffError::Persistence)?
            .flatten();
        let prepared = PreparedForcedRuntimeStateRecovery {
            recovery: transition.identity(),
            destination_incarnation: transition.destination_incarnation,
            target_schedule_fingerprint: transition.target_schedule_fingerprint,
            checkpoints,
        };
        match entry {
            Entry::Occupied(mut entry) => {
                entry.insert(prepared);
            }
            Entry::Vacant(entry) => {
                entry.insert(prepared);
            }
        }
        if let Some(store) = written
            && let Err(error) = store.synchronize_forced_recovery()
        {
            self.inner
                .prepared_forced_runtime_state_recoveries
                .remove_if(&key, |_, current| current.matches(transition));
            return Err(error.change_context(OwnershipHandoffError::Persistence));
        }
        Ok(())
    }

    pub(crate) async fn discard_forced_recovery_preparation(
        &self,
        request: PrepareForcedOwnershipRecoveryRequest,
    ) -> OwnershipHandoffResult<()> {
        self.record_rejected_forced_recovery_round(&request.round);
        let runtime = self.clone();
        let executor = self.executor();
        let charge = executor
            .try_reserve(nervix_execution::MemoryClass::Bulk, 1)
            .change_context(RuntimePersistenceError::StorageAdmission)
            .change_context(OwnershipHandoffError::Persistence)?;
        executor
            .run_storage_with(
                nervix_execution::StorageClass::Filesystem,
                nervix_execution::QueueAdmission::WaitForPlace,
                charge,
                move |_charge, cancellation| {
                    cancellation
                        .check()
                        .change_context(RuntimePersistenceError::Cancelled)?;
                    runtime.discard_forced_recovery_unit(&request)
                },
            )
            .await
            .change_context(RuntimePersistenceError::StorageExecution)
            .change_context(OwnershipHandoffError::Persistence)?
            .change_context(OwnershipHandoffError::Persistence)
    }

    pub(super) fn record_rejected_forced_recovery_round(
        &self,
        round: &ForcedOwnershipRecoveryRound,
    ) {
        let rejected = forced_recovery_round_order(round);
        self.inner
            .rejected_forced_recovery_round
            .send_if_modified(|latest| {
                if *latest >= rejected {
                    return false;
                }
                *latest = rejected;
                true
            });
    }

    pub(super) fn discard_forced_recovery_unit(
        &self,
        request: &PrepareForcedOwnershipRecoveryRequest,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let entity = request.entity.in_domain(&request.domain);
        let transition = ForcedRuntimeStateRecoveryTransition::from_request(request, &entity);
        let key = DomainNodeUnitRef::new(entity.clone(), request.unit);
        let entry = self
            .inner
            .prepared_forced_runtime_state_recoveries
            .entry(key);
        let store = self.inner.state_store.as_deref();
        let synchronize = store
            .map(|store| store.delete_forced_recovery_preparation(&transition))
            .transpose()?
            .unwrap_or(false);
        match entry {
            Entry::Occupied(entry) if entry.get().matches(&transition) => {
                entry.remove();
            }
            other => drop(other),
        };
        if synchronize && let Some(store) = store {
            store.synchronize_forced_recovery()?;
        }
        Ok(())
    }
}

fn forced_recovery_round_order(round: &ForcedOwnershipRecoveryRound) -> (u64, u64) {
    (round.leader_term, round.coordination.sequence())
}

pub(super) struct ForcedRecoveryCheckpoint {
    pub(super) snapshot: Option<PersistedRuntimeStateEntry>,
    pub(super) reset_cause: OwnershipStateResetCause,
}
