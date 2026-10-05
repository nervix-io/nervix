//! Bounded reads of a captured materialized checkpoint for archive conversion.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One immutable checkpoint view, complete generation metadata and ordered groups.
//! - **Depends on.** The current native container codec, typed scalar identities and executor.
//! - **Must not know.** Archive records, Models, placement decisions or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "backup reads a selected immutable checkpoint generation"
    )
)]

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_execution::{ChargedBytes, CpuClass, Executor, MemoryClass, Reservation};
use nervix_models::{RemoteRuntimeField, RemoteRuntimeRecordMetadata};

use super::{
    MaterializedSnapshotError, SealedRecordIdentities, SealedSectionKind, SealedSnapshotHeader,
    SealedSnapshotSummary, SealedSource, decode_rkyv,
};

pub(crate) struct CapturedMaterializedCheckpoint {
    source: SealedSource<'static>,
}

pub(crate) struct MaterializedCheckpointReader {
    source: SealedSource<'static>,
    header: SealedSnapshotHeader,
    next_group: u32,
    records: u64,
    _working: Reservation,
}

pub(crate) struct MaterializedCheckpointGroup {
    pub(crate) identities: Vec<(Option<Vec<RemoteRuntimeField>>, RemoteRuntimeRecordMetadata)>,
    pub(crate) columns: ChargedBytes,
    pub(crate) _metadata: Reservation,
}

impl CapturedMaterializedCheckpoint {
    pub(in crate::runtime) fn new(
        executor: Executor,
        reader: crate::runtime::state_store::checkpoint_reader::CheckpointReader,
    ) -> Self {
        Self {
            source: SealedSource::stored(executor, reader),
        }
    }

    pub(crate) async fn open(
        self,
        executor: &Executor,
    ) -> Result<MaterializedCheckpointReader, Report<MaterializedSnapshotError>> {
        let working = executor
            .reserve(
                MemoryClass::Bulk,
                crate::runtime::RESTORE_STATE_WORKING_BYTES,
            )
            .await
            .change_context(MaterializedSnapshotError::Admission)?;
        let mut source = self.source;
        let header = source.take_header(executor).await?;
        let _layout = header.metadata_layout()?;
        Ok(MaterializedCheckpointReader {
            source,
            header,
            next_group: 0,
            records: 0,
            _working: working,
        })
    }
}

impl MaterializedCheckpointReader {
    pub(crate) fn summary(&self) -> SealedSnapshotSummary {
        SealedSnapshotSummary {
            revision: self.header.revision,
            fence: self.header.fence,
            branch_generation: self.header.branch_generation,
            records: self.header.records,
            groups: self.header.groups,
        }
    }

    pub(crate) async fn next_group(
        &mut self,
        executor: &Executor,
        identity_limit: u64,
        columns_limit: u64,
    ) -> Result<Option<MaterializedCheckpointGroup>, Report<MaterializedSnapshotError>> {
        if self.next_group == self.header.groups {
            if self.records != self.header.records {
                return Err(MaterializedSnapshotError::decoding(
                    "materialized record count mismatch",
                ));
            }
            self.source.finish()?;
            return Ok(None);
        }
        let bytes = self
            .source
            .take_section(SealedSectionKind::RecordIdentities, identity_limit)
            .await?;
        let metadata_bytes = u64::try_from(bytes.len())
            .verified("bounded identity bytes fit")
            .checked_mul(8)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
        let metadata = executor
            .reserve(MemoryClass::Bulk, metadata_bytes.max(1))
            .await
            .change_context(MaterializedSnapshotError::Admission)?;
        let identities =
            decode_rkyv::<SealedRecordIdentities>(executor, bytes, identity_limit).await?;
        if identities.identities.is_empty() {
            return Err(MaterializedSnapshotError::decoding(
                "materialized group has no identities",
            ));
        }
        let count = u64::try_from(identities.identities.len()).verified("bounded row counts fit");
        self.records = self.records.checked_add(count).ok_or_else(|| {
            MaterializedSnapshotError::decoding("materialized record count overflow")
        })?;
        if self.records > self.header.records {
            return Err(MaterializedSnapshotError::decoding(
                "materialized records exceed their declared count",
            ));
        }
        let (identities, metadata) = executor
            .run_cpu(CpuClass::Bulk, metadata, move |metadata, cancellation| {
                let mut projected = Vec::with_capacity(identities.identities.len());
                for identity in identities.identities {
                    cancellation
                        .check()
                        .change_context(MaterializedSnapshotError::Execution)?;
                    projected.push((identity.branch, identity.watermarks));
                }
                Ok::<_, Report<MaterializedSnapshotError>>((projected, metadata))
            })
            .await
            .change_context(MaterializedSnapshotError::Execution)??;
        let columns = self
            .source
            .take_section(SealedSectionKind::RecordColumns, columns_limit)
            .await?;
        self.next_group += 1;
        Ok(Some(MaterializedCheckpointGroup {
            identities,
            columns,
            _metadata: metadata,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use meticulous::OptionExt as _;
    use nervix_models::Timestamp;

    use super::*;
    use crate::runtime::{
        BranchKey, materialized_columns_frame, materialized_container_header,
        materialized_identity_section, state_store::checkpoint_reader::CheckpointReader,
    };

    fn container(groups: u32) -> Vec<u8> {
        let mut bytes = materialized_container_header(123, 7, u64::MAX, u64::from(groups), groups)
            .assured("the current native header encodes");
        for group in 0..groups {
            let branch = crate::runtime::string_branch_key("tenant", &format!("tenant-{group}"));
            let identities = vec![(
                BranchKey::to_remote_key(&branch),
                RemoteRuntimeRecordMetadata {
                    ingested_at_low_watermark: Timestamp::from_unix_nanos(-123),
                    ingested_at_high_watermark: Timestamp::from_unix_nanos(456),
                },
            )];
            bytes.extend(materialized_identity_section(identities).assured("identities encode"));
            bytes.extend(materialized_columns_frame(513).assured("the column frame encodes"));
            bytes.extend(vec![u8::try_from(group).assured("three groups fit"); 513]);
        }
        bytes
    }

    async fn open(
        executor: &Executor,
        bytes: Vec<u8>,
    ) -> Result<MaterializedCheckpointReader, Report<MaterializedSnapshotError>> {
        CapturedMaterializedCheckpoint::new(
            executor.clone(),
            CheckpointReader::Inline(Cursor::new(bytes)),
        )
        .open(executor)
        .await
    }

    #[nervix_primitives::test]
    async fn stored_groups_preserve_complete_metadata_order_and_opaque_bytes() {
        let executor = Executor::default();
        for groups in [0, 1, 3] {
            let mut reader = open(&executor, container(groups))
                .await
                .assured("the stored generation opens");
            assert_eq!(
                reader.summary(),
                SealedSnapshotSummary {
                    revision: 123,
                    fence: 7,
                    branch_generation: u64::MAX,
                    records: u64::from(groups),
                    groups,
                }
            );
            for group in 0..groups {
                let row = reader
                    .next_group(&executor, 1024 * 1024, 8 * 1024 * 1024)
                    .await
                    .assured("the stored group reads")
                    .assured("the group exists");
                let branch =
                    crate::runtime::string_branch_key("tenant", &format!("tenant-{group}"));
                assert_eq!(
                    row.identities,
                    vec![(
                        BranchKey::to_remote_key(&branch),
                        RemoteRuntimeRecordMetadata {
                            ingested_at_low_watermark: Timestamp::from_unix_nanos(-123),
                            ingested_at_high_watermark: Timestamp::from_unix_nanos(456),
                        }
                    )]
                );
                assert_eq!(
                    row.columns.as_ref(),
                    vec![u8::try_from(group).assured("three groups fit"); 513]
                );
            }
            assert!(
                reader
                    .next_group(&executor, 1024 * 1024, 8 * 1024 * 1024)
                    .await
                    .assured("the complete generation ends")
                    .is_none()
            );
        }
    }

    #[nervix_primitives::test]
    async fn stored_group_limits_and_complete_counts_are_checked() {
        let executor = Executor::default();
        let mut reader = open(&executor, container(1))
            .await
            .assured("the header opens");
        let error = reader
            .next_group(&executor, 1, 8 * 1024 * 1024)
            .await
            .err()
            .assured("identity framing exceeds its bound");
        assert!(matches!(
            error.current_context(),
            MaterializedSnapshotError::RecordTooLarge { limit: 1, .. }
        ));
        let mut reader = open(&executor, container(1))
            .await
            .assured("the header opens");
        let error = reader
            .next_group(&executor, 1024 * 1024, 512)
            .await
            .err()
            .assured("columns exceed their bound");
        assert!(matches!(
            error.current_context(),
            MaterializedSnapshotError::SectionTooLarge { limit: 512, .. }
        ));
        let mut bytes = container(0);
        bytes.push(1);
        let mut reader = open(&executor, bytes).await.assured("the header opens");
        assert!(
            reader
                .next_group(&executor, 1024 * 1024, 8 * 1024 * 1024)
                .await
                .is_err()
        );
        let mut bytes =
            materialized_container_header(1, 2, 3, 2, 1).assured("a current header encodes");
        let group = container(1);
        let header = materialized_container_header(123, 7, u64::MAX, 1, 1)
            .assured("the current header encodes");
        bytes.extend_from_slice(&group[header.len()..]);
        let mut reader = open(&executor, bytes)
            .await
            .assured("the declared count is structurally valid");
        drop(
            reader
                .next_group(&executor, 1024 * 1024, 8 * 1024 * 1024)
                .await
                .assured("one complete group reads"),
        );
        assert!(
            reader
                .next_group(&executor, 1024 * 1024, 8 * 1024 * 1024)
                .await
                .is_err()
        );
    }
}
