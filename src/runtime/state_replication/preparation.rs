//! Runtime-state snapshots awaiting handoff or forced recovery.
//!
//! Layer: data plane.
//!
//! - **Owns.** Pending runtime-state snapshots and forced-recovery preparation.
//! - **Depends on.** Persisted runtime-state entries.
//! - **Must not know.** Schedule planning, interconnect transport, or activation policy.

use super::*;

#[derive(Debug)]
pub(in crate::runtime) struct PreparedRuntimeStateSnapshot {
    pub(super) operation_id: String,
    pub(super) snapshot: PersistedRuntimeStateEntry,
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
