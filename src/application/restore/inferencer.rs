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
use nervix_execution::{Cancellation, Executor, MemoryClass, Reservation};
use nervix_models::{DomainSchedule, ModelKind, NodeRef};

use super::{prepare::VerifiedArchive, steps::remote_branch_key};
use crate::{
    inferencer_state_snapshot::{BranchSnapshot, Snapshot, TensorSnapshot},
    registry::InferencerArchivePlan,
    runtime::{BranchKey, Runtime, SnapshotStagingError, StagedArtifact, StagedSnapshotWriter},
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
    if !node.units.contains_key(&descriptor.unit) && node.partition_parametrizer().is_none() {
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
    let branch = BranchKey::from_remote_key(remote_branch_key(&descriptor.branch))
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
    crate::inferencer_state_snapshot::check_bound(length, maximum)
        .change_context(InferencerRestoreError::Shapes)?;
    let chunk_bytes = usize::try_from(
        StagedSnapshotWriter::encoding_buffer_bytes(runtime.executor()).min(length),
    )
    .change_context(InferencerRestoreError::Admission)?;
    let charge = VerifiedArchive::reserve_inferencer_encoding(runtime.executor(), maximum).await?;
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
            let decoded = nervix_backup::read_tensor_values(
                &path,
                &mut TensorSectionReader::buffered(file.take(length), chunk_bytes, cancellation),
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

impl VerifiedArchive {
    async fn reserve_inferencer_encoding(
        executor: &Executor,
        snapshot_bytes: u64,
    ) -> error_stack::Result<Reservation, InferencerRestoreError> {
        // The decoded tensors overlap either the bounded read buffer or the native frame.
        // Both fit within the snapshot footprint, and the staging writer has its own buffer.
        let tensors_working = crate::inferencer_state_snapshot::working_bytes(snapshot_bytes)
            .change_context(InferencerRestoreError::Admission)?;
        let working = tensors_working
            .checked_add(StagedSnapshotWriter::encoding_buffer_bytes(executor))
            .ok_or_else(|| Report::new(InferencerRestoreError::Admission))?;
        executor
            .reserve(MemoryClass::Bulk, working)
            .await
            .change_context(InferencerRestoreError::Admission)
    }
}

/// Checks cancellation before each bounded read, including when the buffered reader bypasses its
/// buffer to read a single F32 directly.
struct TensorSectionReader<'a, R> {
    input: R,
    chunk_bytes: usize,
    cancellation: &'a Cancellation,
}

impl<'a, R: Read> TensorSectionReader<'a, R> {
    fn buffered(input: R, chunk_bytes: usize, cancellation: &'a Cancellation) -> BufReader<Self> {
        BufReader::with_capacity(
            chunk_bytes,
            Self {
                input,
                chunk_bytes,
                cancellation,
            },
        )
    }
}

impl<R: Read> Read for TensorSectionReader<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        self.cancellation.check().map_err(std::io::Error::other)?;
        let length = output.len().min(self.chunk_bytes);
        self.input.read(&mut output[..length])
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_execution::StorageClass;
    use ubyte::ByteUnit;

    use super::*;

    struct Reads<'a> {
        input: &'a [u8],
        requested: Vec<usize>,
    }

    impl Read for Reads<'_> {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            self.requested.push(output.len());
            self.input.read(output)
        }
    }

    #[rstest::rstest]
    #[case::smaller_than_one_value(3)]
    #[case::several_values_with_trailing_bytes(9)]
    #[case::one_complete_tensor(20)]
    #[nervix_primitives::test]
    async fn inferencer_restore_keeps_every_read_within_the_bulk_chunk(#[case] chunk_bytes: usize) {
        let executor = StagedSnapshotWriter::encoding_test_executor(ByteUnit::Byte(
            u64::try_from(chunk_bytes).assured("the test's chunk fits"),
        ));
        let chunk_bytes = usize::try_from(StagedSnapshotWriter::encoding_buffer_bytes(&executor))
            .assured("the test's chunk fits");
        let bytes = *b"0123456789abcdefghij";
        let requested = executor
            .run_storage(
                StorageClass::Filesystem,
                executor.charge_nothing(MemoryClass::Bulk),
                move |_, cancellation| {
                    let mut reader = TensorSectionReader::buffered(
                        Reads {
                            input: &bytes,
                            requested: Vec::new(),
                        },
                        chunk_bytes,
                        cancellation,
                    );
                    let mut output = [0; 20];
                    for value in output.as_chunks_mut::<{ size_of::<f32>() }>().0 {
                        reader.read_exact(value).assured("the next F32 fits");
                    }
                    assert_eq!(output, bytes);
                    reader.into_inner().input.requested
                },
            )
            .await
            .assured("the storage job ran");
        assert_eq!(requested.iter().copied().max(), Some(chunk_bytes));
    }

    #[rstest::rstest]
    #[case::large_chunk(ByteUnit::Mebibyte(4), 4 * 1024 * 1024 + 2 * 1024)]
    #[case::small_chunk(ByteUnit::Kibibyte(32), 34 * 1024)]
    #[nervix_primitives::test]
    async fn inferencer_restore_charges_staging_buffer_beside_the_reader(
        #[case] chunk: ByteUnit,
        #[case] expected: u64,
    ) {
        let executor = StagedSnapshotWriter::encoding_test_executor(chunk);
        let charge = VerifiedArchive::reserve_inferencer_encoding(&executor, 1024)
            .await
            .assured("the test bulk budget fits the tensor restore");
        StagedSnapshotWriter::assert_staging_encoding_charge(&executor, charge, expected).await;
    }
}
