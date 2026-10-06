//! Converting admitted archive metadata into quota-owned native checkpoint files.
//!
//! Layer: control plane.
//! - **Owns.** Passing immutable archived values to the current native streaming encoder.
//! - **Depends on.** The verified description's retained charge, native codecs, and file staging.
//! - **Must not know.** Publication authority, state-store keys, or remote transfer framing.

use error_stack::{Report, ResultExt as _};
use nervix_backup::DescribedRuntimeState;
use nervix_execution::MemoryClass;
use nervix_models::DomainName;
use nervix_primitives::sync::Arc;

use super::{RestoreRefusal, VerifiedArchive};
use crate::runtime::{
    BackupBranchLifecycleEntry, RESTORE_STATE_WORKING_BYTES, Runtime, SnapshotStagingError,
    StagedArtifact, write_restored_branch_lifecycle, write_restored_kafka_offsets,
};

impl VerifiedArchive {
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
            DescribedRuntimeState::Wasm { .. } | DescribedRuntimeState::Materialized { .. } => {
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
        let charge = runtime
            .executor()
            .reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES)
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
                                    key: entry.key.clone().map(|fields| {
                                        fields
                                            .into_iter()
                                            .map(|field| field.into_remote())
                                            .collect()
                                    }),
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
                            offsets.offsets.iter().map(|entry| {
                                (entry.topic.clone(), entry.partition, entry.next_offset)
                            }),
                            output,
                            cancellation,
                        )
                        .change_context(SnapshotStagingError::Encode)
                    }
                    DescribedRuntimeState::Wasm { .. }
                    | DescribedRuntimeState::Materialized { .. } => {
                        Err(Report::new(SnapshotStagingError::Encode))
                    }
                }
            })
            .await
            .map(Arc::new)
            .change_context(RestoreRefusal::Unreadable)
    }
}
