//! Identities attached to runtime-state snapshots awaiting handoff or forced recovery.
//!
//! Layer: data plane.
//!
//! - **Owns.** The exact preparation that may supply one pending runtime-state snapshot.
//! - **Depends on.** Shared coordination identities and persisted runtime-state entries.
//! - **Must not know.** Schedule planning, interconnect transport, or state activation policy.

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
    pub(super) operation_id: String,
    pub(super) destination_incarnation: ClusterNodeIncarnation,
    pub(super) target_schedule_fingerprint: [u8; 32],
    pub(super) checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
}

pub(super) struct ForcedRecoveryCheckpoint {
    pub(super) snapshot: Option<PersistedRuntimeStateEntry>,
    pub(super) reset_cause: OwnershipStateResetCause,
}
