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

use error_stack::{Report, ResultExt as _};
use nervix_execution::{Cancellation, MemoryClass, Reservation};
use nervix_primitives::sync::StdArc;

use super::{
    Runtime, SnapshotStagingError, StagedArtifact,
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
        let tensors_working = snapshot_bytes
            .checked_mul(2)
            .ok_or_else(|| Report::new(SnapshotStagingError::Admission))?;
        let working = tensors_working
            .checked_add(3 * 64 * 1024)
            .ok_or_else(|| Report::new(SnapshotStagingError::Admission))?;
        let charge = match self.charge.take() {
            Some(mut charge) => {
                charge
                    .grow_to(working)
                    .change_context(SnapshotStagingError::Admission)?;
                charge
            }
            None => runtime
                .executor()
                .reserve(MemoryClass::Bulk, working)
                .await
                .change_context(SnapshotStagingError::Admission)?,
        };
        let writer = runtime.try_stage_artifact(raw_bytes).await?;
        writer
            .encode_artifact_with_result(charge, move |output, cancellation| {
                self.write(output, cancellation)
            })
            .await
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
