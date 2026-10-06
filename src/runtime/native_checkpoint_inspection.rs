//! Read-only inspection of selected native checkpoint generations.
//!
//! Layer: data plane.
//! - **Owns.** Opening a stopped checkpoint store and decoding its current native metadata.
//! - **Depends on.** The production generation reader and native checkpoint codecs.
//! - **Must not know.** Archive records, restore installation, or test assertions.

#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "inspection opens a stopped checkpoint store once")
)]

use std::path::Path;

use error_stack::{Report, ResultExt as _};
use nervix_execution::Executor;
use nervix_models::{DomainName, ModelKind, ModelName};

pub use super::backup_state::{BackupBranchLifecycleEntry, BackupStateCaptureError};
use super::{
    backup_state::{decode_backup_branch_lifecycle, decode_backup_kafka_offsets},
    state_store::{DEFAULT_RESTORE_STAGING_MAX_BYTES, RuntimeStateKind, RuntimeStateStore},
};

/// Decoded metadata from one checkpoint in the selected complete generation.
pub enum NativeCheckpointInspection {
    BranchLifecycle {
        owner_kind: ModelKind,
        entity: ModelName,
        revision: u64,
        branches: Vec<BackupBranchLifecycleEntry>,
    },
    KafkaOffsets {
        entity: ModelName,
        revision: u64,
        offsets: Vec<(String, i32, i64)>,
    },
}

/// Every node using this database must have stopped before inspection. The caller owns the
/// decoded observations; their allocations are separate from a running restore's working memory.
pub fn read_native_checkpoint_values(
    path: &Path,
    domain: &DomainName,
) -> error_stack::Result<Vec<NativeCheckpointInspection>, BackupStateCaptureError> {
    let database = fjall::Database::builder(path)
        .open()
        .map_err(Report::new)
        .change_context(BackupStateCaptureError::Storage)?;
    let store = RuntimeStateStore::from_database(
        database,
        Executor::default(),
        DEFAULT_RESTORE_STAGING_MAX_BYTES,
    )
    .change_context(BackupStateCaptureError::Storage)?;
    let entries = store
        .snapshot_backup_domain(
            domain,
            &[RuntimeStateKind::BranchLru, RuntimeStateKind::KafkaOffset],
        )
        .change_context(BackupStateCaptureError::Storage)?;
    let mut observations = Vec::new();
    for (placement, snapshot) in entries.checkpoints {
        let observation = match placement.state.kind() {
            RuntimeStateKind::BranchLru => NativeCheckpointInspection::BranchLifecycle {
                branches: decode_backup_branch_lifecycle(&snapshot.payload, &placement.identifier)?,
                owner_kind: placement.kind,
                entity: placement.identifier,
                revision: snapshot.lsm,
            },
            RuntimeStateKind::KafkaOffset => NativeCheckpointInspection::KafkaOffsets {
                offsets: decode_backup_kafka_offsets(&snapshot.payload)?,
                entity: placement.identifier,
                revision: snapshot.lsm,
            },
            _ => continue,
        };
        observations.push(observation);
    }
    Ok(observations)
}
