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
use nervix_execution::{Cancellation, Executor, MemoryClass, Reservation};
use nervix_primitives::sync::StdArc;

use super::{
    Runtime, SnapshotStagingError, StagedArtifact, StagedSnapshotWriter,
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

    /// The stored inferencer checkpoint, read and charged through `executor`, which may draw from
    /// the capture's allowances.
    pub(crate) async fn stored(
        checkpoint: StoredBranchCheckpoint,
        runtime: &Runtime,
        executor: &Executor,
        limit: u64,
    ) -> Result<Option<Self>, Report<SnapshotStagingError>> {
        crate::inferencer_state_snapshot::check_bound(
            limit,
            executor.limits().snapshot_section_bytes.as_u64(),
        )
        .change_context(SnapshotStagingError::Read)?;
        let opened = checkpoint
            .open(runtime, executor)
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
        let charge = executor
            .reserve(MemoryClass::Bulk, working.max(1))
            .await
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
    /// The staging working set is charged through `executor`, which may draw from the capture's
    /// allowances.
    pub(crate) async fn stage(
        mut self,
        runtime: &Runtime,
        executor: &Executor,
        raw_bytes: u64,
        snapshot_bytes: u64,
    ) -> Result<(StagedArtifact, InferencerArchiveMetadata), Report<SnapshotStagingError>> {
        crate::inferencer_state_snapshot::check_bound(raw_bytes, snapshot_bytes)
            .change_context(SnapshotStagingError::Admission)?;
        let (charge, retained) = self.reserve_staging(executor, snapshot_bytes).await?;
        let chunk_bytes =
            usize::try_from(StagedSnapshotWriter::encoding_buffer_bytes(executor).min(raw_bytes))
                .change_context(SnapshotStagingError::Admission)?;
        let writer = runtime.try_stage_artifact(raw_bytes).await?;
        writer
            .encode_artifact_with_result(charge, move |output, cancellation| {
                let _retained = retained;
                self.write(output, cancellation, chunk_bytes)
            })
            .await
    }

    /// The staging working set: the capture's retained charge, which it keeps, and a charge for
    /// the rest of the set, drawn through `executor`. The retained charge is not grown: it may
    /// come from another budget than the one the rest is drawn from, and growing it would refuse
    /// rather than wait.
    async fn reserve_staging(
        &mut self,
        executor: &Executor,
        snapshot_bytes: u64,
    ) -> Result<(Reservation, Option<Reservation>), Report<SnapshotStagingError>> {
        // The native projection and its raw encoding buffer each fit within the snapshot
        // footprint. The buffer cannot outgrow the section even with a larger chunk setting.
        let tensors_working = crate::inferencer_state_snapshot::working_bytes(snapshot_bytes)
            .change_context(SnapshotStagingError::Admission)?;
        let working = tensors_working
            .checked_add(StagedSnapshotWriter::encoding_buffer_bytes(executor))
            .ok_or_else(|| Report::new(SnapshotStagingError::Admission))?;
        let retained = self.charge.take();
        let held = match &retained {
            Some(retained) => retained.bytes(),
            None => 0,
        };
        let charge = executor
            .reserve(
                MemoryClass::Bulk,
                nervix_primitives::expect_lint!(
                    nervix::clamped_arithmetic,
                    "a retained charge that already covers the working set leaves nothing to add",
                    working.saturating_sub(held)
                )
                .max(1),
            )
            .await
            .change_context(SnapshotStagingError::Admission)?;
        Ok((charge, retained))
    }

    fn write(
        &self,
        output: &mut dyn Write,
        cancellation: &Cancellation,
        chunk_bytes: usize,
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
        // A float is encoded whole. The staging writer also splits bytes when its chunk is
        // smaller than one F32.
        let values_per_chunk = (chunk_bytes / size_of::<f32>()).max(1);
        let mut buffer = vec![[0; size_of::<f32>()]; values_per_chunk];
        for tensor in tensors {
            for chunk in tensor.values.chunks(values_per_chunk) {
                cancellation
                    .check()
                    .change_context(SnapshotStagingError::Cancelled)?;
                nervix_backup::write_tensor_values(output, chunk, &mut buffer)
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
    use nervix_execution::StorageClass;
    use ubyte::ByteUnit;

    use super::*;

    #[derive(Default)]
    struct Writes(Vec<Vec<u8>>);

    impl Write for Writes {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.push(bytes.to_vec());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[rstest::rstest]
    #[case::one_value(4, &[4, 4, 4, 4, 4])]
    #[case::unaligned(9, &[8, 8, 4])]
    #[case::whole_tensor(20, &[20])]
    #[nervix_primitives::test]
    async fn inferencer_archive_writes_whole_values_up_to_the_bulk_chunk(
        #[case] chunk_bytes: u64,
        #[case] block_lengths: &[usize],
    ) {
        let executor = StagedSnapshotWriter::encoding_test_executor(ByteUnit::Byte(chunk_bytes));
        let chunk_bytes = usize::try_from(StagedSnapshotWriter::encoding_buffer_bytes(&executor))
            .assured("the test's chunk fits");
        let bits: [u32; 5] = [0, 0x8000_0000, 0x7fc0_1234, 0x7f80_0000, 0xff80_0000];
        let output = executor
            .run_storage(
                StorageClass::Filesystem,
                executor.charge_nothing(MemoryClass::Bulk),
                move |_, cancellation| {
                    let captured = CapturedInferencer {
                        revision: 1,
                        source: Source::Stored(BranchSnapshot {
                            incarnation: 1,
                            tensors: vec![TensorSnapshot {
                                shape: vec![5],
                                values: std::borrow::Cow::Owned(
                                    bits.into_iter().map(f32::from_bits).collect(),
                                ),
                            }],
                        }),
                        charge: None,
                    };
                    let mut output = Writes::default();
                    let metadata = captured
                        .write(&mut output, cancellation, chunk_bytes)
                        .assured("the owned tensor can be encoded");
                    assert_eq!(metadata.shapes, vec![vec![5]]);
                    output.0
                },
            )
            .await
            .assured("the storage job ran");
        assert_eq!(
            output.iter().map(Vec::len).collect::<Vec<_>>(),
            block_lengths
        );
        assert_eq!(
            output.into_iter().flatten().collect::<Vec<_>>(),
            bits.into_iter()
                .flat_map(u32::to_le_bytes)
                .collect::<Vec<_>>()
        );
    }

    #[rstest::rstest]
    #[case::large_chunk(ByteUnit::Mebibyte(4), 4 * 1024 * 1024 + 2 * 1024)]
    #[case::small_chunk(ByteUnit::Kibibyte(32), 34 * 1024)]
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
        let (charge, retained) = captured
            .reserve_staging(&executor, snapshot_bytes)
            .await
            .assured("the test bulk budget fits the tensor encoder");
        let charge = match retained {
            Some(retained) => charge
                .merge(retained)
                .assured("both charges come from the test's bulk class"),
            None => charge,
        };
        StagedSnapshotWriter::assert_staging_encoding_charge(&executor, charge, expected).await;
    }
}
