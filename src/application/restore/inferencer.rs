//! Layer: control plane.
//! Owns: validating archived inference identity and wrapping exact tensor bits for installation.
//! Depends on: archive records, validated schedule plans, the snapshot engine and admitted staging.
//! Must not know: database keys, native model execution, client framing or consensus activation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "adapts archived tensors to the current installation contract"
    )
)]

use std::io::{BufReader, Read, Seek, SeekFrom};

use error_stack::{Report, ResultExt as _};
use nervix_backup::{BranchLifecycleRecord, DescribedSection, InferencerStateDescriptor};
use nervix_execution::MemoryClass;
use nervix_models::{DomainSchedule, ModelKind, NodeRef};

use super::{prepare::VerifiedArchive, steps::remote_branch_key};
use crate::{
    inferencer_state_snapshot::{BranchSnapshot, Snapshot, TensorSnapshot},
    registry::InferencerArchivePlan,
    runtime::{BranchKey, Runtime, SnapshotStagingError, StagedArtifact},
};

#[derive(Debug, thiserror::Error)]
pub(super) enum InferencerRestoreError {
    #[error("the archived inferencer is absent from the restored schedule")]
    NotScheduled,
    #[error("the archived inferencer unit is absent from the restored schedule")]
    Unit,
    #[error("the archived inferencer schema fingerprint differs from the restored schedule")]
    Schema,
    #[error("the archived inferencer model fingerprint differs from the restored schedule")]
    Model,
    #[error("the archived inferencer tensors differ from the declared STATE shapes")]
    Shapes,
    #[error("the archived inferencer branch fingerprint differs from its typed key")]
    Branch,
    #[error("the archived inferencer incarnation differs from its branch lifecycle")]
    Incarnation,
    #[error("the archived inferencer tensor section is invalid")]
    Values,
    #[error("inferencer restore conversion could not be admitted")]
    Admission,
    #[error("the inferencer restore checkpoint could not be staged")]
    Staging,
}

pub(super) async fn prepare_inferencer_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &InferencerStateDescriptor,
    values: &DescribedSection,
    schedule: Option<&DomainSchedule>,
    lifecycles: &[&BranchLifecycleRecord],
) -> Result<StagedArtifact, Report<InferencerRestoreError>> {
    let reference = NodeRef::new(ModelKind::Inferencer, descriptor.entity.clone());
    let schedule = schedule.ok_or_else(|| Report::new(InferencerRestoreError::NotScheduled))?;
    let node = schedule
        .nodes
        .get(&reference)
        .ok_or_else(|| Report::new(InferencerRestoreError::NotScheduled))?;
    // A branch's partitions place an archived key's state in the unit the restored count gives the
    // key, whichever unit archived it: a holding unit a shrinking count dropped included.
    if !node.units.contains_key(&descriptor.unit) && node.partition_branch().is_none() {
        return Err(Report::new(InferencerRestoreError::Unit));
    }
    if node.schema_fingerprint != descriptor.schema {
        return Err(Report::new(InferencerRestoreError::Schema));
    }
    let plan = InferencerArchivePlan::of_scheduled(node)
        .ok_or_else(|| Report::new(InferencerRestoreError::Model))?;
    if plan.model != descriptor.model {
        return Err(Report::new(InferencerRestoreError::Model));
    }
    if plan.shapes() != descriptor.shapes.as_slice()
        || plan.bytes_per_branch != descriptor.byte_length
    {
        return Err(Report::new(InferencerRestoreError::Shapes));
    }
    let branch = BranchKey::from_remote_key(remote_branch_key(descriptor.branch.as_ref()))
        .change_context(InferencerRestoreError::Branch)?;
    if branch.as_ref().map(BranchKey::fingerprint) != descriptor.branch_fingerprint {
        return Err(Report::new(InferencerRestoreError::Branch));
    }
    let lifetime_matches = lifecycles.iter().any(|lifecycle| {
        lifecycle.owner_kind == ModelKind::Inferencer
            && lifecycle.entity == descriptor.entity
            && lifecycle.unit == descriptor.unit
            && lifecycle.schema == descriptor.schema
            && lifecycle.branches.iter().any(|entry| {
                entry.key == descriptor.branch && entry.incarnation == descriptor.incarnation
            })
    });
    if !lifetime_matches {
        return Err(Report::new(InferencerRestoreError::Incarnation));
    }
    descriptor
        .validate(values.path.as_str())
        .change_context(InferencerRestoreError::Values)?;
    if values.length != descriptor.byte_length || values.digest != descriptor.digest {
        return Err(Report::new(InferencerRestoreError::Values));
    }
    let maximum = plan.snapshot_bytes;
    crate::inferencer_state_snapshot::check_bound(
        maximum,
        runtime.executor().limits().snapshot_section_bytes.as_u64(),
    )
    .change_context(InferencerRestoreError::Shapes)?;
    let input = archive.artifact();
    if values
        .offset
        .checked_add(values.length)
        .is_none_or(|end| end > input.length())
    {
        return Err(Report::new(InferencerRestoreError::Values));
    }
    let offset = values.offset;
    let length = values.length;
    let tensors_working = maximum
        .checked_mul(2)
        .ok_or_else(|| Report::new(InferencerRestoreError::Admission))?;
    let working = tensors_working
        .checked_add(2 * 64 * 1024)
        .ok_or_else(|| Report::new(InferencerRestoreError::Admission))?;
    let charge = runtime
        .executor()
        .reserve(MemoryClass::Bulk, working)
        .await
        .change_context(InferencerRestoreError::Admission)?;
    let writer = runtime
        .try_stage_artifact(maximum)
        .await
        .change_context(InferencerRestoreError::Staging)?;
    let descriptor = descriptor.clone();
    let path = values.path.to_string();
    writer
        .encode_artifact(charge, move |output, cancellation| {
            cancellation
                .check()
                .change_context(SnapshotStagingError::Cancelled)?;
            let mut file =
                std::fs::File::open(input.path()).change_context(SnapshotStagingError::Read)?;
            file.seek(SeekFrom::Start(offset))
                .change_context(SnapshotStagingError::Read)?;
            let checked = TensorSectionReader {
                input: file.take(length),
                cancellation,
            };
            let mut reader = BufReader::with_capacity(TENSOR_READ_BLOCK_BYTES, checked);
            let decoded = nervix_backup::read_tensor_values(
                &path,
                &mut reader,
                &descriptor,
                descriptor.byte_length,
            );
            cancellation
                .check()
                .change_context(SnapshotStagingError::Cancelled)?;
            let tensors = decoded.change_context(SnapshotStagingError::Encode)?;
            let tensors = descriptor
                .shapes
                .into_iter()
                .zip(tensors)
                .map(|(shape, values)| TensorSnapshot {
                    shape,
                    values: std::borrow::Cow::Owned(values),
                })
                .collect();
            let payload = Snapshot {
                revision: descriptor.revision,
                branch: Some(BranchSnapshot {
                    incarnation: descriptor.incarnation,
                    tensors,
                }),
            }
            .encode(maximum, cancellation)
            .change_context(SnapshotStagingError::Encode)?;
            output
                .write_all(&payload)
                .change_context(SnapshotStagingError::Write)
        })
        .await
        .change_context(InferencerRestoreError::Staging)
}

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "a storage read block bounds buffering and the interval between cancellation \
                  checks"
    )
)]
const TENSOR_READ_BLOCK_BYTES: usize = 64 * 1024;

/// Buffer fills check cancellation between fixed-size storage reads, including during a large tensor.
struct TensorSectionReader<'a> {
    input: std::io::Take<std::fs::File>,
    cancellation: &'a nervix_execution::Cancellation,
}

impl Read for TensorSectionReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        self.cancellation.check().map_err(std::io::Error::other)?;
        self.input.read(output)
    }
}
