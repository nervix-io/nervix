//! Layer: data plane.
//! Owns: frozen inference publications and bounded tensor projection at a backup cut.
//! Depends on: native tensor generations, the snapshot engine, admitted staging and raw archive values.
//! Must not know: Models, archive paths, graph scheduling or restore placement decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "projects a frozen inference generation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "a backup cut stages a frozen generation once")
)]

use std::io::Write;

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{Cancellation, Executor, MemoryClass, Reservation};
use nervix_primitives::sync::StdArc;

use super::{
    RESTORE_STATE_CHUNK_BYTES, Runtime, SnapshotStagingError, StagedArtifact, StagedSnapshotWriter,
    backup_state::stored_branch::StoredBranchCheckpoint,
    inferencer_state::{InferencerGeneration, ReplicatedInferencerState},
    published_generation::Generation,
};
use crate::inferencer_state_snapshot::{BranchSnapshot, Snapshot, TensorSnapshot};

pub(crate) struct CapturedInferencer {
    revision: u64,
    source: Source,
    charge: Option<Reservation>,
}

enum Source {
    Published(StdArc<Generation<Option<InferencerGeneration>>>),
    Stored(BranchSnapshot<'static>),
}

pub(crate) struct InferencerArchiveMetadata {
    pub(crate) revision: u64,
    pub(crate) incarnation: u64,
    pub(crate) shapes: Vec<Vec<u64>>,
}

impl CapturedInferencer {
    /// The immutable native publication retained by this cut, when it came from a live owner.
    pub(super) fn published_generation(&self) -> Option<&InferencerGeneration> {
        match &self.source {
            Source::Published(generation) => generation.value.as_ref(),
            Source::Stored(_) => None,
        }
    }

    pub(super) fn revision(&self) -> u64 {
        self.revision
    }

    pub(super) fn published(state: &ReplicatedInferencerState) -> Option<Self> {
        let generation = state.generations.load();
        generation.value.as_ref()?;
        Some(Self {
            revision: generation.revision,
            source: Source::Published(generation),
            charge: None,
        })
    }

    pub(crate) async fn stored(
        checkpoint: StoredBranchCheckpoint,
        runtime: &Runtime,
        limit: u64,
    ) -> Result<Option<Self>, Report<SnapshotStagingError>> {
        crate::inferencer_state_snapshot::check_bound(
            limit,
            runtime.executor().limits().snapshot_section_bytes.as_u64(),
        )
        .change_context(SnapshotStagingError::Read)?;
        let opened = checkpoint
            .open(runtime)
            .await
            .change_context(SnapshotStagingError::Read)?;
        let length = u64::try_from(opened.view.length())
            .map_err(Report::new)
            .change_context(SnapshotStagingError::Read)?;
        crate::inferencer_state_snapshot::check_bound(length, limit)
            .change_context(SnapshotStagingError::Read)?;
        let working = limit
            .checked_mul(2)
            .ok_or_else(|| Report::new(SnapshotStagingError::Admission))?;
        let charge = runtime
            .executor()
            .try_reserve(MemoryClass::Bulk, working.max(1))
            .change_context(SnapshotStagingError::Admission)?;
        let (snapshot, charge) = opened
            .view
            .inspect(charge, move |bytes, charge, cancellation| {
                let snapshot = Snapshot::decode(bytes.to_vec(), limit, cancellation)
                    .change_context(SnapshotStagingError::Read)?;
                if snapshot.revision != opened.revision {
                    return Err(Report::new(SnapshotStagingError::Read));
                }
                Ok::<_, Report<SnapshotStagingError>>((snapshot, charge))
            })
            .await??;
        let Some(branch) = snapshot.branch else {
            return Ok(None);
        };
        let (retained, excess) = charge
            .split(limit)
            .change_context(SnapshotStagingError::Admission)?;
        drop(excess);
        Ok(Some(Self {
            revision: snapshot.revision,
            source: Source::Stored(branch),
            charge: Some(retained),
        }))
    }

    /// Only one branch is projected at a time. Staging's writer bounds and checks every write.
    pub(crate) async fn stage(
        mut self,
        runtime: &Runtime,
        raw_bytes: u64,
        snapshot_bytes: u64,
    ) -> Result<(StagedArtifact, InferencerArchiveMetadata), Report<SnapshotStagingError>> {
        let charge = self
            .reserve_staging(runtime.executor(), snapshot_bytes)
            .await?;
        let writer = runtime.try_stage_artifact(raw_bytes).await?;
        writer
            .encode_artifact_with_result(charge, move |output, cancellation| {
                self.write(output, cancellation)
            })
            .await
    }

    async fn reserve_staging(
        &mut self,
        executor: &Executor,
        snapshot_bytes: u64,
    ) -> Result<Reservation, Report<SnapshotStagingError>> {
        let tensors_working = snapshot_bytes
            .checked_mul(2)
            .ok_or_else(|| Report::new(SnapshotStagingError::Admission))?;
        let segment_bytes: u64 = RESTORE_STATE_CHUNK_BYTES.arch_into();
        // Allow two fixed blocks for native and raw tensor work beside the configured writer.
        let scratch = segment_bytes
            .checked_mul(2)
            .verified("two persisted segments fit in u64");
        let output_bytes = StagedSnapshotWriter::encoding_buffer_bytes(executor).max(segment_bytes);
        let buffers = scratch
            .checked_add(output_bytes)
            .verified("the fixed blocks and the validated u32 bulk chunk fit in u64");
        let working = tensors_working
            .checked_add(buffers)
            .ok_or_else(|| Report::new(SnapshotStagingError::Admission))?;
        match self.charge.take() {
            Some(mut charge) => {
                charge
                    .grow_to(working)
                    .change_context(SnapshotStagingError::Admission)?;
                Ok(charge)
            }
            None => executor
                .reserve(MemoryClass::Bulk, working)
                .await
                .change_context(SnapshotStagingError::Admission),
        }
    }

    fn write(
        &self,
        output: &mut dyn Write,
        cancellation: &Cancellation,
    ) -> Result<InferencerArchiveMetadata, Report<SnapshotStagingError>> {
        let (incarnation, tensors) = match &self.source {
            Source::Published(_) => {
                let state = self
                    .published_generation()
                    .ok_or_else(|| Report::new(SnapshotStagingError::Encode))?;
                (
                    state.incarnation,
                    state
                        .generation
                        .snapshot(cancellation)
                        .change_context(SnapshotStagingError::Encode)?,
                )
            }
            Source::Stored(branch) => (
                branch.incarnation,
                branch
                    .tensors
                    .iter()
                    .map(|tensor| TensorSnapshot {
                        shape: tensor.shape.clone(),
                        values: std::borrow::Cow::Borrowed(tensor.values.as_ref()),
                    })
                    .collect(),
            ),
        };
        let mut shapes = Vec::with_capacity(tensors.len());
        for tensor in tensors {
            for chunk in tensor.values.chunks(16 * 1024) {
                cancellation
                    .check()
                    .change_context(SnapshotStagingError::Cancelled)?;
                nervix_backup::write_tensor_values(output, chunk)
                    .change_context(SnapshotStagingError::Encode)?;
            }
            shapes.push(tensor.shape);
        }
        Ok(InferencerArchiveMetadata {
            revision: self.revision(),
            incarnation,
            shapes,
        })
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

    #[rstest::rstest]
    #[case::large_chunk(ByteUnit::Mebibyte(4), 4 * 1024 * 1024 + 130 * 1024)]
    #[case::small_chunk(ByteUnit::Kibibyte(32), 194 * 1024)]
    #[nervix_primitives::test]
    async fn inferencer_capture_grows_the_retained_charge_for_the_staging_buffer(
        #[case] chunk: ByteUnit,
        #[case] expected: u64,
    ) {
        let executor = StagedSnapshotWriter::encoding_test_executor(chunk);
        let snapshot_bytes = 1024;
        let charge = executor
            .try_reserve(MemoryClass::Bulk, snapshot_bytes)
            .assured("the stored tensor snapshot retains its native charge");
        let mut captured = CapturedInferencer {
            revision: 1,
            source: Source::Stored(BranchSnapshot {
                incarnation: 1,
                tensors: vec![TensorSnapshot {
                    shape: vec![1],
                    values: std::borrow::Cow::Owned(vec![1.0_f32]),
                }],
            }),
            charge: Some(charge),
        };
        let charge = captured
            .reserve_staging(&executor, snapshot_bytes)
            .await
            .assured("the test bulk budget fits the tensor encoder");
        StagedSnapshotWriter::assert_staging_encoding_charge(&executor, charge, expected).await;
    }
}
