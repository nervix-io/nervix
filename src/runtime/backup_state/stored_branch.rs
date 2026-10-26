//! One selected stored branch checkpoint staged and admitted before archive conversion.
//!
//! Layer: data plane.
//! - **Owns.** A cut's unopened checkpoint selection, bounded cancellable staging and its revision.
//! - **Depends on.** The checkpoint reader, staging quota, executor and typed capture failures.
//! - **Must not know.** Archive paths, configuration Models or restore installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns selected stored branch capture")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "one backup cut reads one selected immutable checkpoint"
    )
)]

use std::io::Read as _;

use error_stack::{Report, ResultExt as _};
use nervix_execution::MemoryClass;

use super::{BackupStateCaptureError, ListedCheckpoint, Runtime};
use crate::runtime::{
    SnapshotStagingError, snapshot_staging::MappedArtifact, window_archive::CapturedWindowState,
};

const CHUNK_BYTES: usize = super::super::RESTORE_STATE_CHUNK_BYTES;
// A retained stored chunk, its Fjall read, the transfer block and the writer's buffer, plus
// bounded hashing and filesystem metadata. Inline value copies have their own metadata charge.
const STAGING_BYTES: u64 = 4 * 64 * 1024 + 8 * 1024;

pub(crate) struct StoredBranchCheckpoint {
    checkpoint: ListedCheckpoint,
    stored_bytes: u64,
}

pub(in crate::runtime) struct OpenedBranchCheckpoint {
    pub(in crate::runtime) revision: u64,
    pub(in crate::runtime) view: MappedArtifact,
}

impl StoredBranchCheckpoint {
    pub(super) fn new(
        checkpoint: ListedCheckpoint,
    ) -> error_stack::Result<Self, BackupStateCaptureError> {
        let stored_bytes = checkpoint.stored_bytes().change_context_lazy(|| {
            BackupStateCaptureError::StateSize {
                entity: checkpoint.placement.identifier.clone(),
            }
        })?;
        Ok(Self {
            checkpoint,
            stored_bytes,
        })
    }

    pub(crate) async fn open_keyspace(
        self,
        runtime: &Runtime,
    ) -> error_stack::Result<crate::runtime::CapturedDeduplicatorKeyspace, BackupStateCaptureError>
    {
        let entity = self.checkpoint.placement.identifier.clone();
        let opened = self.open(runtime).await?;
        crate::runtime::CapturedDeduplicatorKeyspace::stored(
            runtime.executor(),
            opened.revision,
            opened.view,
        )
        .await
        .change_context(BackupStateCaptureError::Keyspace { entity })
    }

    pub(crate) async fn open_window(
        self,
        runtime: &Runtime,
        input: &nervix_primitives::sync::StdArc<arrow_schema::Schema>,
        arguments: &nervix_primitives::sync::StdArc<arrow_schema::Schema>,
    ) -> error_stack::Result<Option<CapturedWindowState>, BackupStateCaptureError> {
        let entity = self.checkpoint.placement.identifier.clone();
        let opened = self.open(runtime).await?;
        CapturedWindowState::stored(
            runtime.executor(),
            opened.revision,
            opened.view,
            input,
            arguments,
        )
        .await
        .change_context(BackupStateCaptureError::Window { entity })
    }

    /// The caller has selected the current scheduled owner and schema before calling this.
    /// Only this checkpoint is read. Its view, rather than decoded rows or keys, survives staging.
    pub(in crate::runtime) async fn open(
        self,
        runtime: &Runtime,
    ) -> error_stack::Result<OpenedBranchCheckpoint, BackupStateCaptureError> {
        let entity = self.checkpoint.placement.identifier.clone();
        let length = self.stored_bytes;
        let conversion_bytes = length.checked_mul(3).ok_or_else(|| {
            Report::new(BackupStateCaptureError::Admission {
                entity: entity.clone(),
            })
        })?;
        // ListedCheckpoint also permits inline values. Admission precedes opening either form,
        // covering the Fjall value, aligned stored header and decoded inline payload together.
        let conversion = runtime
            .executor()
            .try_reserve(MemoryClass::RestoreMetadata, conversion_bytes.max(1))
            .change_context_lazy(|| BackupStateCaptureError::Admission {
                entity: entity.clone(),
            })?;
        let working = runtime
            .executor()
            .try_reserve(MemoryClass::Bulk, STAGING_BYTES)
            .change_context_lazy(|| BackupStateCaptureError::Admission {
                entity: entity.clone(),
            })?;
        let writer = runtime
            .try_stage_artifact(length)
            .await
            .change_context(BackupStateCaptureError::Storage)?;
        let (artifact, revision) = writer
            .encode_artifact_with_result(working, move |output, cancellation| {
                let _conversion = conversion;
                cancellation
                    .check()
                    .change_context(SnapshotStagingError::Cancelled)?;
                let (revision, mut reader) = self
                    .checkpoint
                    .open_with_revision()
                    .change_context(SnapshotStagingError::Read)?;
                let mut block = [0_u8; CHUNK_BYTES];
                loop {
                    cancellation
                        .check()
                        .change_context(SnapshotStagingError::Cancelled)?;
                    let count = reader
                        .read(&mut block)
                        .map_err(Report::new)
                        .change_context(SnapshotStagingError::Read)?;
                    if count == 0 {
                        break;
                    }
                    output
                        .write_all(&block[..count])
                        .map_err(Report::new)
                        .change_context(SnapshotStagingError::Write)?;
                }
                Ok(revision)
            })
            .await
            .change_context(BackupStateCaptureError::Storage)?;
        let view = artifact
            .into_mapped()
            .await
            .map_err(|error| match error.current_context() {
                SnapshotStagingError::Admission => {
                    error.change_context(BackupStateCaptureError::Admission { entity })
                }
                _ => error.change_context(BackupStateCaptureError::Storage),
            })?;
        Ok(OpenedBranchCheckpoint { revision, view })
    }
}
