//! An admitted index of current columnar checkpoint groups, with bounded selective decoding.
//!
//! Layer: data plane.
//! - **Owns.** Validating frames and row identities, indexed column ranges and charged row views
//!   decoded only from groups overlapping the requested rows.
//! - **Depends on.** The owning native container codec, Arrow schemas and admitted file views.
//! - **Must not know.** Archive paths, Models, scheduling or restore installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns bounded indexed columnar checkpoint reads")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "one capture reads selected immutable window column groups"
    )
)]

use std::ops::Range;

use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_execution::{Executor, MemoryClass, Reservation};
use nervix_primitives::sync::{Arc, StdArc};

use super::{
    MaterializedSnapshotError, SEALED_SNAPSHOT_MAGIC, SealedRecordIdentities, SealedSectionKind,
    SealedSnapshotHeader, SealedSnapshotSummary, SliceCursor, decode_aligned_rkyv,
};
use crate::{
    runtime::{BranchKey, snapshot_staging::MappedArtifact},
    runtime_schema::{RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow},
};

pub(in crate::runtime) struct ColumnarCheckpointIndex {
    pub(in crate::runtime) summary: SealedSnapshotSummary,
    pub(in crate::runtime) identities: Vec<CheckpointRowIdentity>,
    groups: Vec<ColumnGroup>,
    _memory: Reservation,
}

pub(in crate::runtime) struct CheckpointRowIdentity {
    pub(in crate::runtime) branch: Option<BranchKey>,
    pub(in crate::runtime) metadata: RuntimeRecordMetadata,
}

struct ColumnGroup {
    rows: Range<usize>,
    columns: Range<usize>,
}

pub(in crate::runtime) struct CheckpointRows {
    pub(in crate::runtime) rows: Vec<RuntimeRow>,
    _memory: Reservation,
}

impl ColumnarCheckpointIndex {
    pub(in crate::runtime) async fn open(
        executor: &Executor,
        view: &MappedArtifact,
        range: Range<usize>,
    ) -> error_stack::Result<Self, MaterializedSnapshotError> {
        let limits = executor.limits();
        let header_limit = limits.snapshot_header_bytes.as_u64();
        let identity_limit = limits.snapshot_record_bytes.as_u64();
        let column_limit = limits.snapshot_section_bytes.as_u64();
        let working = executor
            .try_reserve(
                MemoryClass::Bulk,
                header_limit
                    .checked_mul(4)
                    .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?,
            )
            .change_context(MaterializedSnapshotError::Admission)?;
        let inspecting = executor.clone();
        view.inspect(working, move |bytes, _working, cancellation| {
            let payload = bytes.get(range.clone()).ok_or_else(|| {
                MaterializedSnapshotError::decoding("window column container is truncated")
            })?;
            let mut cursor = SliceCursor { payload, offset: 0 };
            if cursor.take(SEALED_SNAPSHOT_MAGIC.len(), "magic")? != SEALED_SNAPSHOT_MAGIC {
                return Err(Report::new(MaterializedSnapshotError::NotASnapshot));
            }
            let length = u64::from(cursor.take_u32("header length")?);
            if length > header_limit {
                return Err(Report::new(MaterializedSnapshotError::HeaderTooLarge {
                    size: length,
                    limit: header_limit,
                }));
            }
            let length = usize::try_from(length).map_err(MaterializedSnapshotError::decoding)?;
            let header =
                decode_aligned_rkyv::<SealedSnapshotHeader>(cursor.take(length, "header")?)?;
            let rows =
                usize::try_from(header.records).map_err(MaterializedSnapshotError::decoding)?;
            let count =
                usize::try_from(header.groups).map_err(MaterializedSnapshotError::decoding)?;
            // Typed identities and group ranges survive this inspection; variable branch fields
            // are additionally admitted from each bounded identity section before decoding it.
            let identity_bytes = rows
                .checked_mul(std::mem::size_of::<CheckpointRowIdentity>())
                .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
            let group_bytes = count
                .checked_mul(std::mem::size_of::<ColumnGroup>())
                .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
            let metadata = identity_bytes
                .checked_add(group_bytes)
                .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
            let mut memory = inspecting
                .try_reserve(
                    MemoryClass::RestoreMetadata,
                    u64::try_from(metadata)
                        .map_err(MaterializedSnapshotError::decoding)?
                        .max(1),
                )
                .change_context(MaterializedSnapshotError::Admission)?;
            let mut identities = Vec::with_capacity(rows);
            let mut groups = Vec::with_capacity(count);
            for _ in 0..header.groups {
                cancellation
                    .check()
                    .change_context(MaterializedSnapshotError::Execution)?;
                let identity_range =
                    cursor.section_range(SealedSectionKind::RecordIdentities, identity_limit)?;
                let identity = &payload[identity_range];
                let extra = u64::try_from(identity.len())
                    .verified("a bounded identity section fits")
                    .checked_mul(8)
                    .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
                let admitted = memory
                    .bytes()
                    .checked_add(extra)
                    .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
                memory
                    .grow_to(admitted)
                    .change_context(MaterializedSnapshotError::Admission)?;
                let decoded = decode_aligned_rkyv::<SealedRecordIdentities>(identity)?;
                if decoded.identities.is_empty() {
                    return Err(MaterializedSnapshotError::decoding(
                        "a window column group has no identities",
                    ));
                }
                let first = identities.len();
                for identity in decoded.identities {
                    cancellation
                        .check()
                        .change_context(MaterializedSnapshotError::Execution)?;
                    if identities.len() == rows {
                        return Err(MaterializedSnapshotError::decoding(
                            "window rows exceed the declared count",
                        ));
                    }
                    identities.push(CheckpointRowIdentity {
                        branch: BranchKey::from_remote_key(identity.branch).change_context(
                            MaterializedSnapshotError::Decode {
                                reason: "invalid branch identity".to_string(),
                            },
                        )?,
                        metadata: RuntimeRecordMetadata::from_remote(identity.watermarks),
                    });
                }
                let body = cursor.section_range(SealedSectionKind::RecordColumns, column_limit)?;
                let start = range.start.checked_add(body.start).ok_or_else(|| {
                    MaterializedSnapshotError::decoding("column position overflow")
                })?;
                let end = range.start.checked_add(body.end).ok_or_else(|| {
                    MaterializedSnapshotError::decoding("column position overflow")
                })?;
                groups.push(ColumnGroup {
                    rows: first..identities.len(),
                    columns: start..end,
                });
            }
            if identities.len() != rows || cursor.offset != payload.len() {
                return Err(MaterializedSnapshotError::decoding(
                    "window column container count or trailing bytes mismatch",
                ));
            }
            Ok::<_, Report<MaterializedSnapshotError>>(Self {
                summary: SealedSnapshotSummary {
                    revision: header.revision,
                    fence: header.fence,
                    branch_generation: header.branch_generation,
                    records: header.records,
                    groups: header.groups,
                },
                identities,
                groups,
                _memory: memory,
            })
        })
        .await
        .change_context(MaterializedSnapshotError::Execution)?
    }

    pub(in crate::runtime) fn group_ranges(&self) -> impl Iterator<Item = Range<usize>> + '_ {
        self.groups.iter().map(|group| group.rows.clone())
    }

    pub(in crate::runtime) async fn read_rows(
        &self,
        executor: &Executor,
        view: &MappedArtifact,
        schema: &StdArc<ArrowSchema>,
        range: Range<usize>,
    ) -> error_stack::Result<CheckpointRows, MaterializedSnapshotError> {
        if range.start > range.end || range.end > self.identities.len() {
            return Err(MaterializedSnapshotError::decoding(
                "a requested row range exceeds its window",
            ));
        }
        // Groups are the wire's ordered sequence, with consecutive disjoint row ranges; binary
        // search reaches only the native groups intersecting this requested archive group.
        let first = self
            .groups
            .partition_point(|group| group.rows.end <= range.start);
        let last = self
            .groups
            .partition_point(|group| group.rows.start < range.end);
        let mut retained = 0_u64;
        for group in &self.groups[first..last] {
            let bytes =
                u64::try_from(group.columns.len()).verified("a bounded columns section fits");
            retained = retained
                .checked_add(
                    bytes
                        .checked_mul(4)
                        .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?,
                )
                .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        }
        let row_bytes = range
            .len()
            .checked_mul(std::mem::size_of::<RuntimeRow>())
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        retained = retained
            .checked_add(u64::try_from(row_bytes).map_err(MaterializedSnapshotError::decoding)?)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let memory = executor
            .try_reserve(MemoryClass::RestoreMetadata, retained.max(1))
            .change_context(MaterializedSnapshotError::Admission)?;
        let mut rows = Vec::with_capacity(range.len());
        for group in &self.groups[first..last] {
            nervix_primitives::task::consume_budget().await;
            let bytes = view
                .copy_section(
                    group.columns.clone(),
                    executor.limits().snapshot_section_bytes.as_u64(),
                )
                .await
                .change_context(MaterializedSnapshotError::Execution)?;
            let batch = RuntimeRecordBatch::decode_arrow_snapshot_section(
                executor,
                StdArc::clone(schema),
                bytes,
            )
            .await
            .change_context(MaterializedSnapshotError::Decode {
                reason: "invalid window columns".to_string(),
            })?;
            if batch.batch().num_rows() != group.rows.len() {
                return Err(MaterializedSnapshotError::decoding(
                    "window columns and identities have different row counts",
                ));
            }
            let batch = Arc::new(batch);
            let start = range.start.max(group.rows.start);
            let end = range.end.min(group.rows.end);
            for index in start..end {
                rows.push(
                    RuntimeRow::new(
                        batch.clone(),
                        index - group.rows.start,
                        self.identities[index].metadata.clone(),
                    )
                    .map_err(MaterializedSnapshotError::decoding)?,
                );
            }
        }
        Ok(CheckpointRows {
            rows,
            _memory: memory,
        })
    }
}
