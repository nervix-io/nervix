//! Converting admitted archive metadata into quota-owned native checkpoint files.
//!
//! Layer: control plane.
//! - **Owns.** Passing immutable archived values to the current native streaming encoder.
//! - **Depends on.** The verified description's retained charge, native codecs, and file staging.
//! - **Must not know.** Publication authority, state-store keys, or remote transfer framing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use error_stack::{Report, ResultExt as _};
use nervix_backup::DescribedRuntimeState;
use nervix_execution::{AdmissionError, Executor, MemoryClass, Reservation};
use nervix_models::DomainName;
use nervix_primitives::sync::Arc;

use super::{RestoreRefusal, VerifiedArchive};
use crate::runtime::{
    BackupBranchLifecycleEntry, RESTORE_STATE_WORKING_BYTES, Runtime, SnapshotStagingError,
    StagedArtifact, StagedSnapshotWriter, write_restored_branch_lifecycle,
    write_restored_kafka_offsets,
};

impl VerifiedArchive {
    /// Native state and its serializer scratch have their own restore-metadata charges. Keep
    /// the restore bulk allowance as a floor and cover the output buffer at any configured size.
    pub(in crate::application::restore) async fn reserve_native_checkpoint_encoding(
        executor: &Executor,
    ) -> error_stack::Result<Reservation, AdmissionError> {
        executor
            .reserve(
                MemoryClass::Bulk,
                Self::native_checkpoint_encoding_bytes(executor),
            )
            .await
    }

    /// The bulk charge [`Self::reserve_native_checkpoint_encoding`] takes.
    pub(in crate::application::restore) fn native_checkpoint_encoding_bytes(
        executor: &Executor,
    ) -> u64 {
        let buffer = StagedSnapshotWriter::encoding_buffer_bytes(executor);
        RESTORE_STATE_WORKING_BYTES.max(buffer)
    }

    /// The description and its reservation travel into the admitted job together. Conversion
    /// materializes one native entry at a time; only serializer resolvers depend on entry count.
    pub(in crate::application) async fn stage_native_checkpoint(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        index: usize,
    ) -> Result<Arc<StagedArtifact>, Report<RestoreRefusal>> {
        let domain_index = self
            .data
            .contents
            .description
            .domains
            .iter()
            .position(|described| &described.capture.domain == domain)
            .ok_or_else(|| Report::new(RestoreRefusal::InvalidArchive))?;
        let archived = self.data.contents.description.domains[domain_index]
            .state
            .get(index)
            .ok_or_else(|| Report::new(RestoreRefusal::InvalidArchive))?;
        let record = match archived {
            DescribedRuntimeState::BranchLifecycle { record, .. }
            | DescribedRuntimeState::KafkaOffsets { record, .. } => record,
            DescribedRuntimeState::Inferencer { .. }
            | DescribedRuntimeState::Wasm { .. }
            | DescribedRuntimeState::Materialized { .. }
            | DescribedRuntimeState::Deduplicator { .. }
            | DescribedRuntimeState::Window { .. } => {
                return Err(Report::new(RestoreRefusal::InvalidArchive));
            }
        };
        // Native representations include normalized typed fields and textual datetimes. This
        // upper bound reserves disk before encoding; the artifact records the exact result size.
        let maximum = record
            .length
            .checked_mul(8)
            .ok_or_else(|| Report::new(RestoreRefusal::MetadataAdmission))?;
        let maximum = maximum
            .checked_add(RESTORE_STATE_WORKING_BYTES)
            .ok_or_else(|| Report::new(RestoreRefusal::MetadataAdmission))?;
        let charge = Self::reserve_native_checkpoint_encoding(runtime.executor())
            .await
            .change_context(RestoreRefusal::MetadataAdmission)?;
        let writer = runtime
            .stage_artifact(maximum)
            .await
            .change_context(RestoreRefusal::Unreadable)?;
        let data = self.data.clone();
        writer
            .encode_artifact(charge, move |output, cancellation| {
                match &data.contents.description.domains[domain_index].state[index] {
                    DescribedRuntimeState::BranchLifecycle { lifecycle, .. } => {
                        write_restored_branch_lifecycle(
                            lifecycle
                                .branches
                                .iter()
                                .map(|entry| BackupBranchLifecycleEntry {
                                    key: entry
                                        .key
                                        .clone()
                                        .map_fields(nervix_backup::StateField::into_remote),
                                    last_ingestion: entry.last_ingestion,
                                    incarnation: entry.incarnation,
                                }),
                            &lifecycle.entity,
                            output,
                            cancellation,
                        )
                        .change_context(SnapshotStagingError::Encode)
                    }
                    DescribedRuntimeState::KafkaOffsets { offsets, .. } => {
                        write_restored_kafka_offsets(
                            offsets.applied_start_generation,
                            offsets.offsets.iter().map(|entry| {
                                (entry.topic.clone(), entry.partition, entry.next_offset)
                            }),
                            output,
                            cancellation,
                        )
                        .change_context(SnapshotStagingError::Encode)
                    }
                    DescribedRuntimeState::Inferencer { .. }
                    | DescribedRuntimeState::Wasm { .. }
                    | DescribedRuntimeState::Materialized { .. }
                    | DescribedRuntimeState::Deduplicator { .. }
                    | DescribedRuntimeState::Window { .. } => {
                        Err(Report::new(SnapshotStagingError::Encode))
                    }
                }
            })
            .await
            .map(Arc::new)
            .change_context(RestoreRefusal::Unreadable)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use ubyte::ByteUnit;

    use super::*;

    // Archived metadata, regrouped lifecycles and deduplicator keyspaces share this admission.
    #[rstest::rstest]
    #[case::large_chunk(ByteUnit::Mebibyte(4), 4 * 1024 * 1024)]
    #[case::within_the_floor(ByteUnit::Kibibyte(256), 2 * 1024 * 1024)]
    #[case::default_chunk(ByteUnit::Kibibyte(64), 2 * 1024 * 1024)]
    #[nervix_primitives::test]
    async fn native_checkpoint_restore_charges_staging_buffer(
        #[case] chunk: ByteUnit,
        #[case] expected: u64,
    ) {
        let executor = StagedSnapshotWriter::encoding_test_executor(chunk);
        let charge = VerifiedArchive::reserve_native_checkpoint_encoding(&executor)
            .await
            .assured("the test bulk budget fits the native restore encoder");
        StagedSnapshotWriter::assert_staging_encoding_charge(&executor, charge, expected).await;
    }
}
