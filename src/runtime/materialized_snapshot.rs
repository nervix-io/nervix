//! The sealed columnar form one relay's materialized records are captured and restored in.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The sealed snapshot container, the immutable generation captured under the
//!   ownership barrier, encoding that generation into a header and bounded Arrow sections, and
//!   decoding a sealed snapshot back into rows.
//! - **Depends on.** The Arrow body codec, the executor that admits and charges the work, and the
//!   vocabulary's branch keys, watermarks and timestamps.
//! - **Must not know.** Who asked for a snapshot, where a sealed snapshot is stored, or how its
//!   bytes reach another node.
//!
//! Records travel as Arrow columns and nothing else. Branch keys, revisions, fences and watermarks
//! are scalar identities and stay scalar; they are described beside the columns rather than folded
//! into them. A snapshot larger than one section limit becomes more sections, never one larger
//! section, so a receiver never decodes a whole snapshot as a single value.

use std::{
    io::{Read as _, Write as _},
    ops::Range,
};

use arch_into::ArchInto as _;
use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{
    BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass, StorageClass,
};
use nervix_models::{RemoteRuntimeField, RemoteRuntimeRecordMetadata};
use nervix_primitives::sync::{Arc, StdArc};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

#[cfg(test)]
use super::snapshot_staging::StagedArtifactReader;
use super::{
    BranchKey,
    snapshot_staging::{SnapshotStaging, StagedArtifact, StagedSnapshot},
};
use crate::runtime_schema::{
    ArrowBodyError, RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow,
};

/// The first bytes of every sealed runtime snapshot. A file that does not start with them is not
/// one of ours, and is refused before any length it declares is believed.
const SEALED_SNAPSHOT_MAGIC: [u8; 8] = *b"NVXSNAPS";

/// How many bytes every declared length occupies, in the container header and in each section.
const LENGTH_PREFIX_BYTES: usize = 4;

/// The prefix each section carries: one byte of kind and its declared length.
const SECTION_FRAME_BYTES: usize = 1 + LENGTH_PREFIX_BYTES;

/// The prefix the container carries: the magic and the header length that follows it.
const CONTAINER_FRAME_BYTES: usize = SEALED_SNAPSHOT_MAGIC.len() + LENGTH_PREFIX_BYTES;

/// What one record's identity costs beside the bytes of its branch key: two watermarks, the option
/// discriminant, and the relative pointers rkyv writes for the vector holding it.
const IDENTITY_OVERHEAD_BYTES: u64 = 96;

/// Why a materialized relay snapshot could not be sealed or opened.
#[derive(Debug, Error)]
pub(crate) enum MaterializedSnapshotError {
    #[error("materialized snapshot memory admission was refused")]
    Admission,
    #[error("materialized row views exceed the 8 MiB metadata limit")]
    MetadataTooLarge,
    #[error("the snapshot could not be admitted for execution")]
    Execution,
    #[error("failed to encode the materialized relay snapshot: {reason}")]
    Encode { reason: String },
    #[error("the sealed snapshot does not begin with a Nervix snapshot header")]
    NotASnapshot,
    #[error("the sealed snapshot ends inside its {section}")]
    Truncated { section: &'static str },
    #[error("a sealed snapshot header of {size} bytes exceeds the {limit} byte limit")]
    HeaderTooLarge { size: u64, limit: u64 },
    #[error("a sealed snapshot record of {size} bytes exceeds the {limit} byte limit")]
    RecordTooLarge { size: u64, limit: u64 },
    #[error("a sealed snapshot Arrow section of {size} bytes exceeds the {limit} byte limit")]
    SectionTooLarge { size: u64, limit: u64 },
    #[error("the sealed snapshot carries section kind {kind} where {expected} was declared")]
    UnexpectedSection { kind: u8, expected: &'static str },
    #[error(
        "the sealed snapshot describes {declared} records where its columns carry {actual} rows"
    )]
    LengthMismatch { declared: usize, actual: usize },
    #[error(
        "the ownership assignment changed from fence {captured} to {current} while the snapshot          was being sealed"
    )]
    OwnershipChanged { captured: u64, current: u64 },
    #[error("failed to decode the materialized relay snapshot: {reason}")]
    Decode { reason: String },
}

impl MaterializedSnapshotError {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the typed snapshot error conversion")
    )]
    fn encoding(error: impl ToString) -> Report<Self> {
        Report::new(Self::Encode {
            reason: error.to_string(),
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the typed snapshot error conversion")
    )]
    fn decoding(error: impl ToString) -> Report<Self> {
        Report::new(Self::Decode {
            reason: error.to_string(),
        })
    }

    fn arrow(error: Report<ArrowBodyError>, failure: Self) -> Report<Self> {
        let context = match error.current_context() {
            ArrowBodyError::Admission => Self::Admission,
            ArrowBodyError::Execution | ArrowBodyError::Cancelled => Self::Execution,
            _ => failure,
        };
        error.change_context(context)
    }
}

macro_rules! declare_sealed_section_kinds {
    ($($(#[$doc:meta])* $Kind:ident = $tag:literal,)+) => {
        /// The kind of one section in a sealed snapshot, and the byte it occupies on disk. Each
        /// kind is bounded by its own limit, so no section can be enlarged by claiming to be a
        /// different one.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr, strum::IntoStaticStr)]
        #[strum(serialize_all = "snake_case")]
        #[repr(u8)]
        enum SealedSectionKind {
            $($(#[$doc])* $Kind = $tag,)+
        }

        impl From<SealedSectionKind> for u8 {
            fn from(kind: SealedSectionKind) -> Self {
                match kind {
                    $(SealedSectionKind::$Kind => $tag,)+
                }
            }
        }
    };
}

declare_sealed_section_kinds! {
    /// The scalar identity of one group of records: their branch keys and watermarks.
    RecordIdentities = 1,
    /// The Arrow columns of one group of records.
    RecordColumns = 2,
}

/// What one sealed snapshot states about itself before any of its sections are read.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct SealedSnapshotHeader {
    /// The state revision every section in this snapshot was captured at.
    revision: u64,
    /// The ownership assignment that captured it. A snapshot sealed under a superseded assignment
    /// is refused rather than installed.
    fence: u64,
    /// The branch lifecycle generation the capture observed, so an evicted and reappearing branch
    /// cannot be restored from a snapshot taken before it left.
    branch_generation: u64,
    /// How many records the sections together carry.
    records: u64,
    /// How many groups follow, each an identity record and the Arrow section it describes.
    groups: u32,
}

/// Validated capacities and the charge for the row views retained during installation.
struct MaterializedRecordLayout {
    records: usize,
    groups: usize,
    metadata_bytes: u64,
}

impl SealedSnapshotHeader {
    fn metadata_layout(
        &self,
    ) -> Result<MaterializedRecordLayout, Report<MaterializedSnapshotError>> {
        if (self.records == 0) != (self.groups == 0) || u64::from(self.groups) > self.records {
            return Err(MaterializedSnapshotError::decoding(
                "invalid materialized group count",
            ));
        }
        let records = usize::try_from(self.records)
            .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
        let groups = usize::try_from(self.groups)
            .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
        let metadata_bytes = self
            .records
            .checked_mul(
                u64::try_from(
                    std::mem::size_of::<RestoredMaterializedRecord>()
                        + std::mem::size_of::<nervix_execution::Reservation>()
                        + 2 * (std::mem::size_of::<Option<BranchKey>>() + 1),
                )
                .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?,
            )
            .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
        if metadata_bytes > 8 * 1024 * 1024 {
            return Err(Report::new(MaterializedSnapshotError::MetadataTooLarge));
        }
        Ok(MaterializedRecordLayout {
            records,
            groups,
            metadata_bytes,
        })
    }
}

/// The scalar identity of the records one Arrow section carries, in that section's row order.
#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
struct SealedRecordIdentities {
    identities: Vec<SealedRecordIdentity>,
}

/// One record's typed concrete branch identity and the watermarks it was materialized with. An
/// absent branch is the unbranched record, never every branch.
#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
struct SealedRecordIdentity {
    branch: Option<Vec<RemoteRuntimeField>>,
    watermarks: RemoteRuntimeRecordMetadata,
}

/// One relay's materialized records exactly as they stood at one revision.
///
/// Capturing shares the carrier columns rather than copying them, so the barrier that produces a
/// generation holds only long enough to clone row views and read the revision, fence and branch
/// generation together. Everything after that reads an immutable value.
#[derive(Debug, Clone)]
pub(crate) struct MaterializedGeneration {
    revision: u64,
    fence: u64,
    branch_generation: u64,
    schema: StdArc<ArrowSchema>,
    records: Arc<Vec<MaterializedGenerationRecord>>,
}

/// One captured record: its branch identity and the shared row view holding its columns.
#[derive(Debug, Clone)]
pub(crate) struct MaterializedGenerationRecord {
    pub(crate) branch: Option<BranchKey>,
    pub(crate) row: RuntimeRow,
}

/// One sealed snapshot's bytes together with what a receiver checks them against.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct SealedMaterializedSnapshot {
    pub(in crate::runtime) descriptor: SealedSnapshotDescriptor,
    pub(in crate::runtime) artifact: Arc<StagedArtifact>,
}

/// What a sealed snapshot supplies about itself before a byte of it is transferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(in crate::runtime) struct SealedSnapshotDescriptor {
    pub(in crate::runtime) length: u64,
    pub(in crate::runtime) digest: [u8; 32],
    pub(in crate::runtime) revision: u64,
    pub(in crate::runtime) fence: u64,
    pub(in crate::runtime) branch_generation: u64,
}

/// What a sealed container states about itself, read without decoding a single column.
///
/// Structural validation stops here: it proves the magic, the header and every section frame are
/// consistent and within their limits. Turning sections into rows is a separate step that needs
/// the schema and the bulk budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct SealedSnapshotSummary {
    pub(in crate::runtime) revision: u64,
    pub(in crate::runtime) fence: u64,
    pub(in crate::runtime) branch_generation: u64,
    pub(in crate::runtime) records: u64,
}

/// The container an empty generation seals into: one header and no sections.
///
/// It carries no columns, so it needs no schema. This is what a destination is handed when an
/// entity moves with no materialized records behind it.
pub(in crate::runtime) fn empty_sealed_container()
-> Result<Vec<u8>, Report<MaterializedSnapshotError>> {
    let header = rkyv::to_bytes::<rkyv::rancor::Error>(&SealedSnapshotHeader {
        revision: 0,
        fence: 0,
        branch_generation: 0,
        records: 0,
        groups: 0,
    })
    .map_err(MaterializedSnapshotError::encoding)?;
    let header_length = u32::try_from(header.len())
        .map_err(|_| MaterializedSnapshotError::encoding("the snapshot header is unaddressable"))?;
    let capacity = CONTAINER_FRAME_BYTES
        .checked_add(header.len())
        .assured("a bounded header plus its fixed frame fits an address");
    let mut container = Vec::with_capacity(capacity);
    container.extend_from_slice(&SEALED_SNAPSHOT_MAGIC);
    container.extend_from_slice(&header_length.to_le_bytes());
    container.extend_from_slice(&header);
    Ok(container)
}

/// Validate a sealed container's frame and read what it declares, without decoding its columns.
///
/// The header is bounded by its own limit and every section frame is checked against the bytes
/// that remain, so a truncated, overstated or foreign payload is refused here rather than by the
/// decoder that would otherwise allocate for it.
pub(in crate::runtime) fn inspect_sealed_container(
    payload: &[u8],
    header_limit: u64,
) -> Result<SealedSnapshotSummary, Report<MaterializedSnapshotError>> {
    let mut cursor = SliceCursor { payload, offset: 0 };
    if cursor.take(SEALED_SNAPSHOT_MAGIC.len(), "magic")? != SEALED_SNAPSHOT_MAGIC {
        return Err(Report::new(MaterializedSnapshotError::NotASnapshot));
    }
    let header_bytes = u64::from(cursor.take_u32("header length")?);
    if header_bytes > header_limit {
        return Err(Report::new(MaterializedSnapshotError::HeaderTooLarge {
            size: header_bytes,
            limit: header_limit,
        }));
    }
    let header_bytes = usize::try_from(header_bytes)
        .map_err(|_| Report::new(MaterializedSnapshotError::Truncated { section: "header" }))?;
    let header = decode_aligned_rkyv::<SealedSnapshotHeader>(cursor.take(header_bytes, "header")?)?;
    for _ in 0..header.groups {
        for expected in [
            SealedSectionKind::RecordIdentities,
            SealedSectionKind::RecordColumns,
        ] {
            let kind = cursor.take(1, "section kind")?;
            let Some(&kind) = kind.first() else {
                return Err(Report::new(MaterializedSnapshotError::Truncated {
                    section: "section kind",
                }));
            };
            if SealedSectionKind::from_repr(kind) != Some(expected) {
                return Err(Report::new(MaterializedSnapshotError::UnexpectedSection {
                    kind,
                    expected: expected.into(),
                }));
            }
            let length = usize::try_from(cursor.take_u32("section length")?).map_err(|_| {
                Report::new(MaterializedSnapshotError::Truncated {
                    section: "section body",
                })
            })?;
            cursor.take(length, "section body")?;
        }
    }
    if cursor.offset != payload.len() {
        return Err(Report::new(MaterializedSnapshotError::Truncated {
            section: "section body",
        }));
    }
    Ok(SealedSnapshotSummary {
        revision: header.revision,
        fence: header.fence,
        branch_generation: header.branch_generation,
        records: header.records,
    })
}

/// A forward-only position in a container held whole in memory, used by the frame validation that
/// reads nothing beyond the bounded header.
struct SliceCursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> SliceCursor<'a> {
    fn take(
        &mut self,
        length: usize,
        section: &'static str,
    ) -> Result<&'a [u8], Report<MaterializedSnapshotError>> {
        let truncated = || Report::new(MaterializedSnapshotError::Truncated { section });
        let end = self.offset.checked_add(length).ok_or_else(truncated)?;
        let slice = self.payload.get(self.offset..end).ok_or_else(truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn take_u32(
        &mut self,
        section: &'static str,
    ) -> Result<u32, Report<MaterializedSnapshotError>> {
        let bytes: [u8; LENGTH_PREFIX_BYTES] = self
            .take(LENGTH_PREFIX_BYTES, section)?
            .try_into()
            .map_err(|_| Report::new(MaterializedSnapshotError::Truncated { section }))?;
        Ok(u32::from_le_bytes(bytes))
    }
}

impl SealedMaterializedSnapshot {
    /// The handoff metadata boundary accepts an admitted resident entry. Large checkpoint
    /// transfer and persistence use the artifact directly, independently of this memory bound.
    pub(in crate::runtime) async fn into_persisted_entry(
        self,
        executor: &Executor,
    ) -> Result<super::PersistedRuntimeStateEntry, Report<MaterializedSnapshotError>> {
        let size = self
            .descriptor
            .length
            .checked_mul(2)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let _charge = executor
            .try_reserve(MemoryClass::Bulk, size.max(1))
            .change_context(MaterializedSnapshotError::Admission)?;
        let mut reader = self
            .artifact
            .open_reader()
            .await
            .change_context(MaterializedSnapshotError::Execution)?;
        let mut payload = Vec::with_capacity(
            usize::try_from(self.descriptor.length).map_err(MaterializedSnapshotError::encoding)?,
        );
        while let Some(chunk) = reader
            .next_chunk(64 * 1024)
            .await
            .change_context(MaterializedSnapshotError::Execution)?
        {
            payload.extend_from_slice(&chunk);
        }
        Ok(super::PersistedRuntimeStateEntry {
            lsm: self.descriptor.revision,
            payload,
        })
    }
}

impl MaterializedGeneration {
    pub(in crate::runtime) fn new(
        revision: u64,
        fence: u64,
        branch_generation: u64,
        schema: StdArc<ArrowSchema>,
        records: Vec<MaterializedGenerationRecord>,
    ) -> Self {
        Self {
            revision,
            fence,
            branch_generation,
            schema,
            records: Arc::new(records),
        }
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn fence(&self) -> u64 {
        self.fence
    }

    pub(crate) fn records(&self) -> &[MaterializedGenerationRecord] {
        &self.records
    }

    /// Seal this generation into one container: a header, then a bounded identity record and a
    /// bounded Arrow section for each group of records.
    ///
    /// The encoding runs on the bulk workers and holds no mutation lock, so updates and deletions
    /// continue against the live state while this immutable generation is written out.
    pub(in crate::runtime) async fn seal(
        &self,
        executor: &Executor,
        staging: &SnapshotStaging,
    ) -> Result<SealedMaterializedSnapshot, Report<MaterializedSnapshotError>> {
        let groups = self.groups(executor);
        let count = u32::try_from(groups.len())
            .map_err(|_| MaterializedSnapshotError::encoding("too many snapshot groups"))?;
        let header = materialized_container_header(
            self.revision,
            self.fence,
            self.branch_generation,
            self.records.len().arch_into(),
            count,
        )?;
        let header = executor
            .charge_owned(MemoryClass::Bulk, header)
            .await
            .change_context(MaterializedSnapshotError::Admission)?;
        let mut pieces = vec![stage_sealed_piece(staging, header).await?];
        for group in &groups {
            nervix_primitives::task::consume_budget().await;
            for section in [
                self.seal_identities(executor, group).await?,
                self.seal_columns(executor, group).await?,
            ] {
                let frame = section_frame(section.kind, section.bytes.len())?;
                let frame = executor
                    .charge_owned(MemoryClass::Bulk, frame)
                    .await
                    .change_context(MaterializedSnapshotError::Admission)?;
                pieces.push(stage_sealed_piece(staging, frame).await?);
                pieces.push(stage_sealed_piece(staging, section.bytes).await?);
            }
        }
        let length = pieces
            .iter()
            .try_fold(0_u64, |total, piece| total.checked_add(piece.length()))
            .ok_or_else(|| {
                MaterializedSnapshotError::encoding("snapshot length is unaddressable")
            })?;
        let mut writer = staging
            .try_stage(length)
            .await
            .change_context(MaterializedSnapshotError::Execution)?;
        for piece in pieces {
            let mut reader = piece
                .open_reader()
                .await
                .change_context(MaterializedSnapshotError::Execution)?;
            while let Some(chunk) = reader
                .next_chunk(64 * 1024)
                .await
                .change_context(MaterializedSnapshotError::Execution)?
            {
                writer
                    .write_chunk(chunk)
                    .await
                    .change_context(MaterializedSnapshotError::Execution)?;
            }
        }
        let artifact = Arc::new(
            writer
                .finish_artifact()
                .await
                .change_context(MaterializedSnapshotError::Execution)?,
        );
        Ok(SealedMaterializedSnapshot {
            descriptor: SealedSnapshotDescriptor {
                length: artifact.length(),
                digest: artifact.digest(),
                revision: self.revision,
                fence: self.fence,
                branch_generation: self.branch_generation,
            },
            artifact,
        })
    }

    /// The bounded resident container a window checkpoint nests inside its own admitted payload.
    /// Materialized relay persistence and transfer call `seal`, which retains a file instead.
    pub(in crate::runtime) async fn encode_resident_container(
        &self,
        executor: &Executor,
    ) -> Result<ChargedBytes, Report<MaterializedSnapshotError>> {
        let groups = self.groups(executor);
        let mut sections = Vec::new();
        for group in &groups {
            nervix_primitives::task::consume_budget().await;
            sections.push(self.seal_identities(executor, group).await?);
            sections.push(self.seal_columns(executor, group).await?);
        }
        let groups = u32::try_from(groups.len())
            .map_err(|_| MaterializedSnapshotError::encoding("too many snapshot groups"))?;
        encode_resident_container(
            executor,
            SealedSnapshotHeader {
                revision: self.revision,
                fence: self.fence,
                branch_generation: self.branch_generation,
                records: self.records.len().arch_into(),
                groups,
            },
            sections,
        )
        .await
    }

    /// Split the records into groups that each fit one Arrow section and one identity record.
    ///
    /// Grouping is by measured payload bytes rather than by a row-count guess, so one enormous
    /// record occupies a group of its own instead of pushing a section past its limit.
    fn groups(&self, executor: &Executor) -> Vec<Range<usize>> {
        self.bounded_groups(
            executor.limits().snapshot_record_bytes.as_u64(),
            executor.limits().snapshot_section_bytes.as_u64(),
        )
    }

    pub(crate) fn branch_generation(&self) -> u64 {
        self.branch_generation
    }

    pub(crate) async fn encode_columns(
        &self,
        executor: &Executor,
        group: &Range<usize>,
    ) -> Result<ChargedBytes, Report<MaterializedSnapshotError>> {
        Ok(self.seal_columns(executor, group).await?.bytes)
    }

    /// Bound both scalar identities and columns independently before either conversion allocates.
    pub(crate) fn bounded_groups(
        &self,
        identity_limit: u64,
        section_limit: u64,
    ) -> Vec<Range<usize>> {
        let mut groups = Vec::new();
        let mut start = 0;
        let mut columns = 0_u64;
        let mut identities = 0_u64;
        for (index, record) in self.records.iter().enumerate() {
            let record_columns = record.row.one_row_batch().estimated_bytes();
            let record_identity = estimated_identity_bytes(record.branch.as_ref());
            // A total that does not fit a u64 is past every limit there is, so it closes the
            // group exactly as an ordinary overrun does.
            let next = match (
                columns.checked_add(record_columns),
                identities.checked_add(record_identity),
            ) {
                (Some(next_columns), Some(next_identities)) => {
                    Some((next_columns, next_identities))
                }
                _ => None,
            };
            let overruns = match next {
                Some((next_columns, next_identities)) => {
                    next_columns > section_limit || next_identities > identity_limit
                }
                None => true,
            };
            if index > start && overruns {
                groups.push(start..index);
                start = index;
                columns = record_columns;
                identities = record_identity;
                continue;
            }
            match next {
                Some((next_columns, next_identities)) => {
                    columns = next_columns;
                    identities = next_identities;
                }
                None => {
                    columns = section_limit;
                    identities = identity_limit;
                }
            }
        }
        if start < self.records.len() {
            groups.push(start..self.records.len());
        }
        groups
    }

    async fn seal_identities(
        &self,
        executor: &Executor,
        group: &Range<usize>,
    ) -> Result<SealedSection, Report<MaterializedSnapshotError>> {
        let limit = executor.limits().snapshot_record_bytes.as_u64();
        let estimate = self.records[group.clone()]
            .iter()
            .try_fold(0_u64, |bytes, record| {
                bytes.checked_add(estimated_identity_bytes(record.branch.as_ref()))
            });
        let size = estimate.ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        if size > limit {
            return Err(Report::new(MaterializedSnapshotError::RecordTooLarge {
                size,
                limit,
            }));
        }
        // Remote identities, the aligned serialization, and the bounded output overlap in one
        // job. Admit all three before converting a branch key or allocating their vectors.
        let bytes = limit
            .checked_mul(3)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let reservation = executor
            .try_reserve(MemoryClass::Bulk, bytes.max(1))
            .change_context(MaterializedSnapshotError::Admission)?;
        let records = Arc::clone(&self.records);
        let group = group.clone();
        let bytes = executor
            .run_cpu(CpuClass::Bulk, reservation, move |charge, cancellation| {
                let mut identities = Vec::with_capacity(group.len());
                for record in &records[group] {
                    cancellation
                        .check()
                        .change_context(MaterializedSnapshotError::Execution)?;
                    identities.push(SealedRecordIdentity {
                        branch: BranchKey::to_remote_key(&record.branch),
                        watermarks: record.row.metadata().to_remote(),
                    });
                }
                let encoded =
                    rkyv::to_bytes::<rkyv::rancor::Error>(&SealedRecordIdentities { identities })
                        .map_err(MaterializedSnapshotError::encoding)?;
                let size = encoded.len().arch_into();
                if size > limit {
                    return Err(Report::new(MaterializedSnapshotError::RecordTooLarge {
                        size,
                        limit,
                    }));
                }
                let mut buffer = BudgetedBuffer::with_limit(charge, limit);
                buffer
                    .write_all(&encoded)
                    .map_err(MaterializedSnapshotError::encoding)?;
                Ok(ChargedBytes::from_buffer(buffer))
            })
            .await
            .change_context(MaterializedSnapshotError::Execution)??;
        Ok(SealedSection {
            kind: SealedSectionKind::RecordIdentities,
            bytes,
        })
    }

    async fn seal_columns(
        &self,
        executor: &Executor,
        group: &Range<usize>,
    ) -> Result<SealedSection, Report<MaterializedSnapshotError>> {
        let payload = self.records[group.clone()]
            .iter()
            .try_fold(0_u64, |bytes, record| {
                bytes.checked_add(record.row.one_row_batch().estimated_bytes())
            })
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let limit = executor.limits().snapshot_section_bytes.as_u64();
        if payload > limit {
            return Err(Report::new(MaterializedSnapshotError::SectionTooLarge {
                size: payload,
                limit,
            }));
        }
        let row_bytes = u64::try_from(group.len())
            .map_err(|_| Report::new(MaterializedSnapshotError::Admission))?
            .checked_mul(256)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let schema_bytes = u64::try_from(self.schema.fields().len())
            .map_err(|_| Report::new(MaterializedSnapshotError::Admission))?
            .checked_mul(256)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let projection_bytes = payload
            .checked_mul(2)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let projection_bytes = projection_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let projection_bytes = projection_bytes
            .checked_add(schema_bytes)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let encoded_estimate = payload
            .checked_add(64 * 1024)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let schema = StdArc::clone(&self.schema);
        let records = Arc::clone(&self.records);
        let group = group.clone();
        let bytes = RuntimeRecordBatch::encode_arrow_snapshot_projection(
            executor,
            projection_bytes,
            encoded_estimate,
            move || {
                RuntimeRecordBatch::from_rows(
                    schema,
                    records[group].iter().map(|record| &record.row),
                )
            },
        )
        .await
        .map_err(|error| {
            MaterializedSnapshotError::arrow(
                error,
                MaterializedSnapshotError::Encode {
                    reason: "the Arrow section could not be written".to_string(),
                },
            )
        })?;
        Ok(SealedSection {
            kind: SealedSectionKind::RecordColumns,
            bytes,
        })
    }
}

/// Native framing for a generation assembled from bounded external column sections.
pub(crate) fn materialized_container_header(
    revision: u64,
    fence: u64,
    branch_generation: u64,
    records: u64,
    groups: u32,
) -> Result<Vec<u8>, Report<MaterializedSnapshotError>> {
    let header = rkyv::to_bytes::<rkyv::rancor::Error>(&SealedSnapshotHeader {
        revision,
        fence,
        branch_generation,
        records,
        groups,
    })
    .map_err(MaterializedSnapshotError::encoding)?;
    let length = u32::try_from(header.len()).map_err(MaterializedSnapshotError::encoding)?;
    let mut bytes = SEALED_SNAPSHOT_MAGIC.to_vec();
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&header);
    Ok(bytes)
}

/// Convert bounded typed identities into their native record frame, without any payload rows.
pub(crate) fn materialized_identity_section(
    identities: Vec<(Option<Vec<RemoteRuntimeField>>, RemoteRuntimeRecordMetadata)>,
) -> Result<Vec<u8>, Report<MaterializedSnapshotError>> {
    let identities = identities
        .into_iter()
        .map(|(branch, watermarks)| SealedRecordIdentity { branch, watermarks })
        .collect();
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&SealedRecordIdentities { identities })
        .map_err(MaterializedSnapshotError::encoding)?;
    let mut bytes = section_frame(SealedSectionKind::RecordIdentities, encoded.len())?;
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
}

pub(crate) fn materialized_columns_frame(
    length: usize,
) -> Result<Vec<u8>, Report<MaterializedSnapshotError>> {
    section_frame(SealedSectionKind::RecordColumns, length)
}

fn section_frame(
    kind: SealedSectionKind,
    length: usize,
) -> Result<Vec<u8>, Report<MaterializedSnapshotError>> {
    let length = u32::try_from(length).map_err(MaterializedSnapshotError::encoding)?;
    let mut bytes = vec![kind.into()];
    bytes.extend_from_slice(&length.to_le_bytes());
    Ok(bytes)
}

/// One encoded section waiting to be written into a container.
struct SealedSection {
    kind: SealedSectionKind,
    bytes: ChargedBytes,
}

/// One record restored from a sealed snapshot, ready to be installed under the ownership barrier.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct RestoredMaterializedRecord {
    pub(crate) branch: Option<BranchKey>,
    pub(crate) row: RuntimeRow,
}

/// Everything a sealed snapshot restores into: the records, and the revision, fence and branch
/// generation they belong to.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct RestoredMaterializedSnapshot {
    pub(in crate::runtime) revision: u64,
    pub(in crate::runtime) fence: u64,
    pub(in crate::runtime) branch_generation: u64,
    pub(in crate::runtime) records: Vec<RestoredMaterializedRecord>,
    _metadata_charge: Arc<nervix_execution::Reservation>,
    _columns_charge: Arc<Vec<nervix_execution::Reservation>>,
}

impl RestoredMaterializedSnapshot {
    #[cfg(test)]
    pub(in crate::runtime) fn from_captured_generation(
        executor: &Executor,
        generation: MaterializedGeneration,
    ) -> Self {
        use meticulous::ResultExt as _;
        let bytes = generation
            .records
            .len()
            .checked_mul(std::mem::size_of::<RestoredMaterializedRecord>())
            .assured("a bounded test generation fits");
        let charge = executor
            .try_reserve(
                MemoryClass::Relay,
                u64::try_from(bytes)
                    .verified("a bounded test generation fits")
                    .max(1),
            )
            .assured("a bounded test generation is admitted");
        Self {
            revision: generation.revision,
            fence: generation.fence,
            branch_generation: generation.branch_generation,
            records: generation
                .records
                .iter()
                .map(|record| RestoredMaterializedRecord {
                    branch: record.branch.clone(),
                    row: record.row.clone(),
                })
                .collect(),
            _metadata_charge: Arc::new(charge),
            _columns_charge: Arc::new(Vec::new()),
        }
    }

    /// Apply the relay's one-record-per-concrete-branch contract after decoding the shared
    /// columnar container. A window's retained-row container permits repeated keys instead.
    pub(in crate::runtime) async fn open_relay(
        executor: &Executor,
        schema: &StdArc<ArrowSchema>,
        source: SealedSource<'_>,
    ) -> Result<Self, Report<MaterializedSnapshotError>> {
        let snapshot = Self::open(executor, schema, source).await?;
        let charge = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(MaterializedSnapshotError::Admission)?;
        executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                let mut branches = ahash::HashSet::with_capacity_and_hasher(
                    snapshot.records.len(),
                    ahash::RandomState::default(),
                );
                for record in &snapshot.records {
                    cancellation
                        .check()
                        .change_context(MaterializedSnapshotError::Execution)?;
                    if record.branch.is_none() && snapshot.records.len() != 1 {
                        return Err(MaterializedSnapshotError::decoding(
                            "an unbranched materialized relay contains one record",
                        ));
                    }
                    if !branches.insert(record.branch.clone()) {
                        return Err(MaterializedSnapshotError::decoding(
                            "duplicate materialized branch identity",
                        ));
                    }
                }
                Ok(snapshot)
            })
            .await
            .change_context(MaterializedSnapshotError::Execution)?
    }

    /// Open a sealed snapshot against the schema installed here.
    ///
    /// Every length the container declares is checked against the limit for the section it names
    /// and against the bytes that actually remain, so a truncated or overstated snapshot is
    /// refused before it allocates anything.
    pub(in crate::runtime) async fn open(
        executor: &Executor,
        schema: &StdArc<ArrowSchema>,
        source: SealedSource<'_>,
    ) -> Result<Self, Report<MaterializedSnapshotError>> {
        let mut cursor = source;
        let header = cursor.take_header(executor).await?;
        let identity_limit = executor.limits().snapshot_record_bytes.as_u64();
        let section_limit = executor.limits().snapshot_section_bytes.as_u64();
        let layout = header.metadata_layout()?;
        // Admit the row views and uniqueness table before either grows. The columns become
        // materialized runtime state; these arrays are transient until installation consumes them.
        let mut metadata_charge = executor
            .reserve(MemoryClass::Relay, layout.metadata_bytes.max(1))
            .await
            .change_context(MaterializedSnapshotError::Admission)?;
        let mut records = Vec::with_capacity(layout.records);
        let mut column_charges = Vec::with_capacity(layout.groups);
        for _ in 0..header.groups {
            nervix_primitives::task::consume_budget().await;
            let identities = cursor
                .take_section(SealedSectionKind::RecordIdentities, identity_limit)
                .await?;
            let identity_bytes = u64::try_from(identities.len())
                .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?
                .checked_mul(8)
                .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
            let identity_bytes = metadata_charge
                .bytes()
                .checked_add(identity_bytes)
                .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
            if identity_bytes > 8 * 1024 * 1024 {
                return Err(Report::new(MaterializedSnapshotError::MetadataTooLarge));
            }
            metadata_charge
                .grow_to(identity_bytes)
                .map_err(|error| error.change_context(MaterializedSnapshotError::Admission))?;
            let identities =
                decode_rkyv::<SealedRecordIdentities>(executor, identities, identity_limit).await?;
            let columns = cursor
                .take_section(SealedSectionKind::RecordColumns, section_limit)
                .await?;
            // Decoded carrier columns remain retained until installation consumes this snapshot.
            // Charge that cumulative materialization separately from one section's bulk scratch.
            // Refuse when full: waiting while retaining preceding groups cannot free this charge.
            let retained_bytes = u64::try_from(columns.len())
                .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?
                .checked_mul(2)
                .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?
                .min(section_limit)
                .max(1);
            column_charges.push(
                executor
                    .try_reserve(MemoryClass::Relay, retained_bytes)
                    .map_err(|error| error.change_context(MaterializedSnapshotError::Admission))?,
            );
            let batch = RuntimeRecordBatch::decode_arrow_snapshot_section(
                executor,
                StdArc::clone(schema),
                columns,
            )
            .await
            .map_err(|error| {
                MaterializedSnapshotError::arrow(
                    error,
                    MaterializedSnapshotError::Decode {
                        reason: "the Arrow section could not be read".to_string(),
                    },
                )
            })?;
            let batch = Arc::new(batch);
            if identities.identities.len() != batch.batch().num_rows() {
                return Err(Report::new(MaterializedSnapshotError::LengthMismatch {
                    declared: identities.identities.len(),
                    actual: batch.batch().num_rows(),
                }));
            }
            let record_count = records
                .len()
                .checked_add(identities.identities.len())
                .ok_or_else(|| {
                    MaterializedSnapshotError::decoding(
                        "materialized record count is unaddressable",
                    )
                })?;
            if record_count > layout.records {
                return Err(MaterializedSnapshotError::decoding(
                    "materialized records exceed their declared count",
                ));
            }
            for (row, identity) in identities.identities.into_iter().enumerate() {
                let branch = BranchKey::from_remote_key(identity.branch).change_context(
                    MaterializedSnapshotError::Decode {
                        reason: "a record identity carries an invalid branch key".to_string(),
                    },
                )?;
                let metadata = RuntimeRecordMetadata::from_remote(identity.watermarks);
                let row = RuntimeRow::new(batch.clone(), row, metadata)
                    .map_err(MaterializedSnapshotError::decoding)?;
                records.push(RestoredMaterializedRecord { branch, row });
            }
        }
        let restored: u64 = records.len().arch_into();
        if restored != header.records {
            return Err(Report::new(MaterializedSnapshotError::LengthMismatch {
                declared: usize::try_from(header.records).unwrap_or(usize::MAX),
                actual: records.len(),
            }));
        }
        cursor.finish()?;
        Ok(Self {
            revision: header.revision,
            fence: header.fence,
            branch_generation: header.branch_generation,
            records,
            _metadata_charge: Arc::new(metadata_charge),
            _columns_charge: Arc::new(column_charges),
        })
    }
}

/// Where a sealed container's bytes come from while it is opened.
///
/// A container that already sits in memory under one charge is read in place. A container that
/// arrived over the interconnect is read from the file it was staged into, one bounded section at
/// a time, so a snapshot larger than the node's transfer-memory budget opens without ever being
/// held whole.
pub(in crate::runtime) enum SealedSource<'a> {
    Memory {
        sealed: ChargedBytes,
        offset: usize,
    },
    Borrowed {
        bytes: &'a [u8],
        offset: usize,
        executor: &'a Executor,
    },
    Staged(StagedSnapshot),
    #[cfg(test)]
    Artifact {
        _artifact: Arc<StagedArtifact>,
        reader: StagedArtifactReader,
    },
    Stored {
        reader: Option<super::state_store::checkpoint_reader::CheckpointReader>,
        executor: Executor,
    },
}

impl<'a> SealedSource<'a> {
    /// Validate and admit the container header before reading any row metadata or columns.
    async fn take_header(
        &mut self,
        executor: &Executor,
    ) -> Result<SealedSnapshotHeader, Report<MaterializedSnapshotError>> {
        let magic = self
            .take(SEALED_SNAPSHOT_MAGIC.len().arch_into(), "magic")
            .await?;
        if magic.as_ref() != SEALED_SNAPSHOT_MAGIC {
            return Err(Report::new(MaterializedSnapshotError::NotASnapshot));
        }
        let header_bytes = u64::from(self.take_u32("header length").await?);
        let header_limit = executor.limits().snapshot_header_bytes.as_u64();
        if header_bytes > header_limit {
            return Err(Report::new(MaterializedSnapshotError::HeaderTooLarge {
                size: header_bytes,
                limit: header_limit,
            }));
        }
        let header = self.take(header_bytes, "header").await?;
        decode_rkyv::<SealedSnapshotHeader>(executor, header, header_limit).await
    }

    fn finish(&self) -> Result<(), Report<MaterializedSnapshotError>> {
        let complete = match self {
            Self::Memory { sealed, offset } => sealed.len() == *offset,
            Self::Borrowed { bytes, offset, .. } => bytes.len() == *offset,
            Self::Staged(staged) => staged.remaining() == 0,
            #[cfg(test)]
            Self::Artifact { reader, .. } => reader.remaining() == 0,
            Self::Stored { reader, .. } => reader
                .as_ref()
                .is_some_and(|reader| reader.remaining() == 0),
        };
        if !complete {
            return Err(MaterializedSnapshotError::decoding(
                "sealed snapshot has trailing bytes",
            ));
        }
        Ok(())
    }

    pub(in crate::runtime) fn memory(sealed: ChargedBytes) -> Self {
        Self::Memory { sealed, offset: 0 }
    }

    /// Open an already resident snapshot one bounded section at a time, charging each copied
    /// section only while its decoder reads it. A persisted window container can exceed the bulk
    /// budget without holding the whole container under that budget during restore.
    pub(in crate::runtime) fn borrowed(executor: &'a Executor, bytes: &'a [u8]) -> Self {
        Self::Borrowed {
            bytes,
            offset: 0,
            executor,
        }
    }

    pub(in crate::runtime) fn staged(staged: StagedSnapshot) -> Self {
        Self::Staged(staged)
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn artifact(
        artifact: Arc<StagedArtifact>,
    ) -> Result<Self, Report<MaterializedSnapshotError>> {
        let reader = artifact
            .open_reader()
            .await
            .change_context(MaterializedSnapshotError::Execution)?;
        Ok(Self::Artifact {
            _artifact: artifact,
            reader,
        })
    }

    pub(in crate::runtime) fn stored(
        executor: Executor,
        reader: super::state_store::checkpoint_reader::CheckpointReader,
    ) -> Self {
        Self::Stored {
            reader: Some(reader),
            executor,
        }
    }

    /// Read the next `length` bytes, naming the part of the container they belong to so a
    /// truncated snapshot reports where it ended instead of which arithmetic failed.
    async fn take(
        &mut self,
        length: u64,
        section: &'static str,
    ) -> Result<ChargedBytes, Report<MaterializedSnapshotError>> {
        let truncated = || Report::new(MaterializedSnapshotError::Truncated { section });
        match self {
            Self::Stored { reader, executor } => {
                let working = length
                    .checked_add(super::RESTORE_STATE_WORKING_BYTES)
                    .ok_or_else(truncated)?;
                let charge = executor
                    .reserve(MemoryClass::Bulk, working)
                    .await
                    .change_context(MaterializedSnapshotError::Admission)?;
                let mut source = reader
                    .take()
                    .assured("the selected checkpoint is held between storage reads");
                let result = executor
                    .run_storage(
                        StorageClass::Filesystem,
                        charge,
                        move |charge, cancellation| {
                            let result = (|| {
                                cancellation
                                    .check()
                                    .change_context(MaterializedSnapshotError::Execution)?;
                                let capacity = usize::try_from(length)
                                    .map_err(MaterializedSnapshotError::decoding)?;
                                let mut bytes = vec![0; capacity];
                                source
                                    .read_exact(&mut bytes)
                                    .map_err(MaterializedSnapshotError::decoding)?;
                                Ok::<_, Report<MaterializedSnapshotError>>(
                                    ChargedBytes::from_owned(bytes, charge),
                                )
                            })();
                            (source, result)
                        },
                    )
                    .await
                    .change_context(MaterializedSnapshotError::Execution)?;
                *reader = Some(result.0);
                result.1
            }
            Self::Memory { sealed, offset } => {
                let length = usize::try_from(length).map_err(|_| truncated())?;
                let end = offset.checked_add(length).ok_or_else(truncated)?;
                let slice = sealed.slice(*offset, end).ok_or_else(truncated)?;
                *offset = end;
                Ok(slice)
            }
            Self::Borrowed {
                bytes,
                offset,
                executor,
            } => {
                let length = usize::try_from(length).map_err(|_| truncated())?;
                let end = offset.checked_add(length).ok_or_else(truncated)?;
                let slice = bytes.get(*offset..end).ok_or_else(truncated)?;
                *offset = end;
                executor
                    .charge_owned(MemoryClass::Bulk, slice.to_vec())
                    .await
                    .change_context(MaterializedSnapshotError::Admission)
            }
            #[cfg(test)]
            Self::Artifact { reader, .. } => reader
                .read(length)
                .await
                .change_context(MaterializedSnapshotError::Truncated { section }),
            Self::Staged(staged) => staged
                .read(length)
                .await
                .change_context(MaterializedSnapshotError::Truncated { section }),
        }
    }

    async fn take_u32(
        &mut self,
        section: &'static str,
    ) -> Result<u32, Report<MaterializedSnapshotError>> {
        let bytes = self.take(LENGTH_PREFIX_BYTES.arch_into(), section).await?;
        let bytes: [u8; LENGTH_PREFIX_BYTES] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| Report::new(MaterializedSnapshotError::Truncated { section }))?;
        Ok(u32::from_le_bytes(bytes))
    }

    /// Read the next section, refusing one that names another kind or overstates its limit.
    async fn take_section(
        &mut self,
        expected: SealedSectionKind,
        limit: u64,
    ) -> Result<ChargedBytes, Report<MaterializedSnapshotError>> {
        let kind = self.take(1, "section kind").await?;
        let Some(&kind) = kind.as_ref().first() else {
            return Err(Report::new(MaterializedSnapshotError::Truncated {
                section: "section kind",
            }));
        };
        if SealedSectionKind::from_repr(kind) != Some(expected) {
            return Err(Report::new(MaterializedSnapshotError::UnexpectedSection {
                kind,
                expected: expected.into(),
            }));
        }
        let length = u64::from(self.take_u32("section length").await?);
        if length > limit {
            return Err(Report::new(match expected {
                SealedSectionKind::RecordIdentities => MaterializedSnapshotError::RecordTooLarge {
                    size: length,
                    limit,
                },
                SealedSectionKind::RecordColumns => MaterializedSnapshotError::SectionTooLarge {
                    size: length,
                    limit,
                },
            }));
        }
        self.take(length, "section body").await
    }
}

/// Retain one bounded encoded piece on quota-owned disk, releasing its bulk charge before the
/// next group is encoded. Concatenation holds one 64 KiB chunk regardless of total snapshot size.
async fn stage_sealed_piece(
    staging: &SnapshotStaging,
    bytes: ChargedBytes,
) -> Result<StagedArtifact, Report<MaterializedSnapshotError>> {
    let mut writer = staging
        .try_stage(bytes.len().arch_into())
        .await
        .change_context(MaterializedSnapshotError::Execution)?;
    writer
        .write_chunk(bytes)
        .await
        .change_context(MaterializedSnapshotError::Execution)?;
    writer
        .finish_artifact()
        .await
        .change_context(MaterializedSnapshotError::Execution)
}

/// Encode the bounded resident container nested in a window checkpoint. Its caller admits the
/// complete window payload; relay persistence and transfer use staged files.
async fn encode_resident_container(
    executor: &Executor,
    header: SealedSnapshotHeader,
    sections: Vec<SealedSection>,
) -> Result<ChargedBytes, Report<MaterializedSnapshotError>> {
    let header_limit = executor.limits().snapshot_header_bytes.as_u64();
    let header = encode_rkyv(executor, header, header_limit).await?;
    let unaddressable =
        || MaterializedSnapshotError::encoding("the sealed snapshot exceeds an addressable size");
    let header_length = u32::try_from(header.len()).map_err(|_| unaddressable())?;
    let container_frame: u64 = CONTAINER_FRAME_BYTES.arch_into();
    let section_frame: u64 = SECTION_FRAME_BYTES.arch_into();
    let mut length = container_frame
        .checked_add(header.len().arch_into())
        .ok_or_else(unaddressable)?;
    for section in &sections {
        let section_bytes: u64 = section.bytes.len().arch_into();
        let Some(framed) = length.checked_add(section_frame) else {
            return Err(unaddressable());
        };
        let Some(next) = framed.checked_add(section_bytes) else {
            return Err(unaddressable());
        };
        length = next;
    }
    let reservation = executor
        .try_reserve(MemoryClass::Bulk, length)
        .change_context(MaterializedSnapshotError::Admission)?;
    let bytes = executor
        .run_cpu(CpuClass::Bulk, reservation, move |charge, cancellation| {
            cancellation
                .check()
                .change_context(MaterializedSnapshotError::Execution)?;
            let mut buffer = BudgetedBuffer::with_limit(charge, length);
            buffer
                .write_all(&SEALED_SNAPSHOT_MAGIC)
                .map_err(MaterializedSnapshotError::encoding)?;
            buffer
                .write_all(&header_length.to_le_bytes())
                .map_err(MaterializedSnapshotError::encoding)?;
            buffer
                .write_all(header.as_ref())
                .map_err(MaterializedSnapshotError::encoding)?;
            for section in &sections {
                cancellation
                    .check()
                    .change_context(MaterializedSnapshotError::Execution)?;
                let section_length = u32::try_from(section.bytes.len()).map_err(|_| {
                    MaterializedSnapshotError::encoding("a section is unaddressable")
                })?;
                buffer
                    .write_all(&[u8::from(section.kind)])
                    .map_err(MaterializedSnapshotError::encoding)?;
                buffer
                    .write_all(&section_length.to_le_bytes())
                    .map_err(MaterializedSnapshotError::encoding)?;
                buffer
                    .write_all(section.bytes.as_ref())
                    .map_err(MaterializedSnapshotError::encoding)?;
            }
            Ok::<_, Report<MaterializedSnapshotError>>(ChargedBytes::from_buffer(buffer))
        })
        .await
        .change_context(MaterializedSnapshotError::Execution)??;
    Ok(bytes)
}

async fn encode_rkyv<T>(
    executor: &Executor,
    value: T,
    limit: u64,
) -> Result<ChargedBytes, Report<MaterializedSnapshotError>>
where
    T: for<'a> RkyvSerialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        > + Send
        + 'static,
{
    let reservation = executor
        .reserve(MemoryClass::Bulk, limit)
        .await
        .change_context(MaterializedSnapshotError::Admission)?;
    executor
        .run_cpu(CpuClass::Bulk, reservation, move |charge, cancellation| {
            cancellation
                .check()
                .change_context(MaterializedSnapshotError::Execution)?;
            let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&value)
                .map_err(MaterializedSnapshotError::encoding)?;
            let encoded_bytes: u64 = encoded.len().arch_into();
            if encoded_bytes > limit {
                return Err(Report::new(MaterializedSnapshotError::RecordTooLarge {
                    size: encoded_bytes,
                    limit,
                }));
            }
            let mut buffer = BudgetedBuffer::with_limit(charge, limit);
            buffer
                .write_all(&encoded)
                .map_err(MaterializedSnapshotError::encoding)?;
            Ok(ChargedBytes::from_buffer(buffer))
        })
        .await
        .change_context(MaterializedSnapshotError::Execution)?
}

async fn decode_rkyv<T>(
    executor: &Executor,
    bytes: ChargedBytes,
    limit: u64,
) -> Result<T, Report<MaterializedSnapshotError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + RkyvDeserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let reservation = executor
        .reserve(MemoryClass::Bulk, limit)
        .await
        .change_context(MaterializedSnapshotError::Admission)?;
    executor
        .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
            cancellation
                .check()
                .change_context(MaterializedSnapshotError::Execution)?;
            decode_aligned_rkyv(bytes.as_ref())
        })
        .await
        .change_context(MaterializedSnapshotError::Execution)?
}

/// Read an archived value out of a container.
///
/// A section starts wherever the framing put it, and rkyv reads its relative pointers from an
/// eight-byte aligned base, so the bytes are moved into an aligned buffer first. Every caller has
/// already bounded the slice by the limit for the section it belongs to, so the copy is bounded
/// too.
pub(in crate::runtime) fn decode_aligned_rkyv<T>(
    bytes: &[u8],
) -> Result<T, Report<MaterializedSnapshotError>>
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + RkyvDeserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
        .map_err(MaterializedSnapshotError::decoding)
}

/// What one record's identity costs in its group's identity record. A branch key's rendered form
/// bounds the field names and scalar values rkyv writes for it, so the estimate follows the key
/// rather than assuming a fixed size per branch. The encoder still enforces the record limit, so
/// an underestimate fails the seal rather than producing an oversized record.
fn estimated_identity_bytes(branch: Option<&BranchKey>) -> u64 {
    let Some(branch) = branch else {
        return IDENTITY_OVERHEAD_BYTES;
    };
    let rendered: u64 = branch.as_str().len().arch_into();
    let fields = rendered
        .checked_mul(2)
        .assured("a branch key renders from a bounded batch, far below half of u64::MAX");
    fields
        .checked_add(IDENTITY_OVERHEAD_BYTES)
        .assured("a branch key renders from a bounded batch, far below u64::MAX")
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use meticulous::ResultExt as _;
    use nervix_execution::{ExecutionConfig, MemoryBudgets, OperationLimits};
    use nervix_models::FieldName;
    use ubyte::ByteUnit;

    use super::*;
    use crate::{
        runtime::snapshot_staging::{SnapshotStaging, SnapshotStagingLimits},
        runtime_schema::{RuntimeValue, test_runtime_row},
    };

    /// One row of payload in the snapshot under test. Large enough that a handful of records
    /// exceed several section limits, small enough that no single record does.
    const RECORD_BYTES: usize = 512 * 1024;

    /// How many records the snapshot carries. Their columns alone exceed eight mebibytes and the
    /// bulk transfer budget the narrow executor allows.
    const RECORDS: usize = 80;

    fn generated_schema_type(
        arbitrary: &mut nervix_arbitrary::Arbitrary<'_>,
        depth: u8,
    ) -> nervix_models::ParseAsType {
        use nervix_models::ParseAsType as T;
        let scalar_types = [
            T::U8,
            T::U16,
            T::U32,
            T::U64,
            T::I8,
            T::I16,
            T::I32,
            T::I64,
            T::F32,
            T::F64,
            T::Bool,
            T::String,
            T::Datetime,
            T::Bytes,
        ];
        let choice = usize::from(arbitrary.entropy().byte())
            % if depth == 0 {
                scalar_types.len()
            } else {
                scalar_types.len() + 2
            };
        match choice {
            14 => T::Vec {
                element: Box::new(generated_schema_type(arbitrary, depth - 1)),
            },
            15 => T::Array {
                element: Box::new(generated_schema_type(arbitrary, depth - 1)),
                len: NonZeroU32::new(u32::from(arbitrary.entropy().byte() % 4) + 1)
                    .assured("generated fixed-size lists contain one to four elements"),
            },
            index => scalar_types[index].clone(),
        }
    }

    fn generated_columns(
        arbitrary: &mut nervix_arbitrary::Arbitrary<'_>,
        ty: &nervix_models::ParseAsType,
        valid: &[bool],
    ) -> arrow_array::ArrayRef {
        use arrow_array::*;
        use nervix_models::ParseAsType as T;
        macro_rules! primitive {
            ($array:ty, $value:expr) => {
                StdArc::new(<$array>::from(
                    valid
                        .iter()
                        .map(|valid| valid.then(|| $value))
                        .collect::<Vec<_>>(),
                ))
            };
        }
        match ty {
            T::U8 => primitive!(UInt8Array, arbitrary.entropy().byte()),
            T::I8 => primitive!(Int8Array, i8::from_le_bytes([arbitrary.entropy().byte()])),
            T::U16 => primitive!(
                UInt16Array,
                u16::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()))
            ),
            T::I16 => primitive!(
                Int16Array,
                i16::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()))
            ),
            T::U32 => primitive!(
                UInt32Array,
                u32::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()))
            ),
            T::I32 => primitive!(
                Int32Array,
                i32::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()))
            ),
            T::U64 => primitive!(UInt64Array, arbitrary.entropy().any_u64()),
            T::I64 => primitive!(Int64Array, arbitrary.entropy().any_i64()),
            T::Bool => primitive!(BooleanArray, arbitrary.entropy().flag()),
            T::String => StdArc::new(StringArray::from_iter(
                valid.iter().map(|valid| valid.then(|| arbitrary.string())),
            )),
            T::Datetime => StdArc::new(
                TimestampNanosecondArray::from(
                    valid
                        .iter()
                        .map(|valid| valid.then(|| arbitrary.entropy().any_i64()))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            T::F32 => primitive!(
                Float32Array,
                f32::from_bits(u32::from_le_bytes(std::array::from_fn(|_| arbitrary
                    .entropy()
                    .byte())))
            ),
            T::F64 => primitive!(Float64Array, f64::from_bits(arbitrary.entropy().any_u64())),
            T::Bytes => {
                StdArc::new(BinaryArray::from_iter(valid.iter().map(|valid| {
                    valid.then(|| arbitrary.entropy().any_u64().to_le_bytes())
                })))
            }
            T::Array { element, len } => {
                let child_count = valid
                    .len()
                    .checked_mul(usize::try_from(len.get()).verified("generated array length fits"))
                    .assured("bounded generated array fits");
                let child = generated_columns(arbitrary, element, &vec![true; child_count]);
                let arrow_schema::DataType::FixedSizeList(field, length) = ty.arrow_data_type()
                else {
                    unreachable!("a fixed-array type has a fixed-list carrier")
                };
                StdArc::new(
                    FixedSizeListArray::try_new(
                        field,
                        length,
                        child,
                        Some(arrow_buffer::NullBuffer::from(valid.to_vec())),
                    )
                    .assured("generated fixed-list columns match their type"),
                )
            }
            T::Vec { element } => {
                let mut offsets = vec![0_i32];
                let mut count = 0_usize;
                for present in valid {
                    let length = if *present {
                        usize::from(arbitrary.entropy().byte() % 4)
                    } else {
                        0
                    };
                    count = count
                        .checked_add(length)
                        .assured("bounded vector count fits");
                    offsets.push(i32::try_from(count).verified("bounded vector offsets fit"));
                }
                let child = generated_columns(arbitrary, element, &vec![true; count]);
                let arrow_schema::DataType::List(field) = ty.arrow_data_type() else {
                    unreachable!("a vector type has a list carrier")
                };
                StdArc::new(
                    ListArray::try_new(
                        field,
                        arrow_buffer::OffsetBuffer::new(offsets.into()),
                        child,
                        Some(arrow_buffer::NullBuffer::from(valid.to_vec())),
                    )
                    .assured("generated list columns match their type"),
                )
            }
        }
    }

    #[test]
    fn bolero_materialized_archive_columns_preserve_the_exact_schema_and_rows() {
        use nervix_arbitrary::{Arbitrary, Domain};
        use nervix_backup::{
            ArchiveRecord, MaterializedIdentitiesRecord, MaterializedRecordIdentity,
            MaterializedRelayDescriptor, StateField,
        };
        use nervix_models::{
            CreateSchema, ParseAsType, SchemaField, SchemaFingerprint, SchemaName,
        };
        bolero::check!()
            .with_iterations(128)
            .with_max_len(4096)
            .for_each(|input| {
                let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .assured("property runtime opens");
                let executor =
                    Executor::new(ExecutionConfig::default()).assured("default bounds are valid");
                let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
                let scalar = generated_schema_type(&mut arbitrary, 3);
                let schema_model = CreateSchema {
                    name: SchemaName::parse("event").assured("schema name is valid"),
                    fields: vec![
                        SchemaField {
                            name: FieldName::parse("nullable").assured("field is valid"),
                            ty: scalar,
                            optional: true,
                            sensitive: arbitrary.entropy().flag(),
                        },
                        SchemaField {
                            name: FieldName::parse("secret").assured("field is valid"),
                            ty: ParseAsType::String,
                            optional: arbitrary.entropy().flag(),
                            sensitive: true,
                        },
                        SchemaField {
                            name: FieldName::parse("nested").assured("field is valid"),
                            ty: ParseAsType::Vec {
                                element: Box::new(ParseAsType::Array {
                                    element: Box::new(ParseAsType::I64),
                                    len: NonZeroU32::new(2).assured("array length is positive"),
                                }),
                            },
                            optional: arbitrary.entropy().flag(),
                            sensitive: arbitrary.entropy().flag(),
                        },
                        SchemaField {
                            name: FieldName::parse("bytes").assured("field is valid"),
                            ty: ParseAsType::Bytes,
                            optional: arbitrary.entropy().flag(),
                            sensitive: arbitrary.entropy().flag(),
                        },
                    ],
                };
                let compiled = crate::runtime_schema::compile_schema(&schema_model);
                let columns = schema_model
                    .fields
                    .iter()
                    .map(|field| {
                        let valid = (0..2)
                            .map(|_| !field.optional || arbitrary.entropy().flag())
                            .collect::<Vec<_>>();
                        generated_columns(&mut arbitrary, &field.ty, &valid)
                    })
                    .collect::<Vec<_>>();
                let arrow_schema = compiled.arrow_schema().clone();
                let arrow = arrow_array::RecordBatch::try_new(arrow_schema.clone(), columns)
                    .assured("generated Arrow columns match their exact schema");
                let batch = Arc::new(
                    RuntimeRecordBatch::from_record_batch(arrow_schema, arrow)
                        .assured("generated payload remains columnar"),
                );
                let low = arbitrary.entropy().any_i64();
                let high = arbitrary.entropy().any_i64();
                let metadata = RuntimeRecordMetadata::from_remote(RemoteRuntimeRecordMetadata {
                    ingested_at_low_watermark: nervix_models::Timestamp::from_unix_nanos(
                        low.min(high),
                    ),
                    ingested_at_high_watermark: nervix_models::Timestamp::from_unix_nanos(
                        low.max(high),
                    ),
                });
                let records = (0..2)
                    .map(|index| MaterializedGenerationRecord {
                        branch: Some(tenant_branch(if index == 0 { "alpha" } else { "beta" })),
                        row: RuntimeRow::new(batch.clone(), index, metadata.clone())
                            .assured("row exists"),
                    })
                    .collect();
                let generation = MaterializedGeneration::new(
                    arbitrary.entropy().any_u64(),
                    arbitrary.entropy().any_u64(),
                    arbitrary.entropy().any_u64(),
                    batch.schema(),
                    records,
                );
                let descriptor = MaterializedRelayDescriptor {
                    domain: nervix_models::DomainName::parse("prod").assured("domain is valid"),
                    entity: nervix_models::ModelName::parse("state").assured("relay is valid"),
                    schema: SchemaFingerprint::from_digest(std::array::from_fn(|_| {
                        arbitrary.entropy().byte()
                    })),
                    revision: generation.revision(),
                    fence: generation.fence(),
                    branch_generation: generation.branch_generation(),
                    record_count: 2,
                    groups: 1,
                };
                let descriptor = MaterializedRelayDescriptor::decode(
                    "descriptor.rkyv",
                    &descriptor.encode().assured("descriptor encodes"),
                )
                .assured("descriptor validates");
                let identities = MaterializedIdentitiesRecord {
                    domain: descriptor.domain.clone(),
                    entity: descriptor.entity.clone(),
                    group: 0,
                    identities: generation
                        .records()
                        .iter()
                        .map(|record| MaterializedRecordIdentity {
                            branch: BranchKey::to_remote_key(&record.branch).map(|fields| {
                                fields.into_iter().map(StateField::from_remote).collect()
                            }),
                            watermarks: record.row.metadata().to_remote(),
                        })
                        .collect(),
                };
                let identities = MaterializedIdentitiesRecord::decode(
                    "identities.rkyv",
                    &identities.encode().assured("identities encode"),
                )
                .assured("identities validate");
                runtime.block_on(async {
                    let columns = generation
                        .encode_columns(&executor, &(0..2))
                        .await
                        .assured("exact columns encode");
                    let decoded = RuntimeRecordBatch::decode_arrow_snapshot_section(
                        &executor,
                        batch.schema(),
                        columns.clone(),
                    )
                    .await
                    .assured("exact columns validate");
                    assert_eq!(decoded.schema(), batch.schema());
                    assert_eq!(
                        decoded
                            .encode_arrow_snapshot_section(&executor)
                            .await
                            .assured("restored columns encode")
                            .as_ref(),
                        columns.as_ref()
                    );
                    let mut bytes = materialized_container_header(
                        descriptor.revision,
                        descriptor.fence,
                        descriptor.branch_generation,
                        descriptor.record_count,
                        descriptor.groups,
                    )
                    .assured("native header encodes");
                    bytes.extend(
                        materialized_identity_section(
                            identities
                                .identities
                                .into_iter()
                                .map(|identity| {
                                    (
                                        identity.branch.map(|fields| {
                                            fields
                                                .into_iter()
                                                .map(StateField::into_remote)
                                                .collect()
                                        }),
                                        identity.watermarks,
                                    )
                                })
                                .collect(),
                        )
                        .assured("native identities encode"),
                    );
                    bytes.extend(
                        materialized_columns_frame(columns.len()).assured("column frame encodes"),
                    );
                    bytes.extend_from_slice(columns.as_ref());
                    drop(columns);
                    let restored = RestoredMaterializedSnapshot::open_relay(
                        &executor,
                        &batch.schema(),
                        SealedSource::memory(
                            executor
                                .charge_owned(MemoryClass::Bulk, bytes)
                                .await
                                .assured("bounded container is charged"),
                        ),
                    )
                    .await
                    .assured("native generation opens");
                    assert_eq!(
                        (
                            restored.revision,
                            restored.fence,
                            restored.branch_generation
                        ),
                        (
                            descriptor.revision,
                            descriptor.fence,
                            descriptor.branch_generation
                        )
                    );
                    assert_eq!(restored.records.len(), generation.records().len());
                    for (expected, actual) in generation.records().iter().zip(restored.records) {
                        assert_eq!(actual.branch, expected.branch);
                        assert_eq!(
                            actual.row.metadata().to_remote(),
                            expected.row.metadata().to_remote()
                        );
                        let actual_columns = actual
                            .row
                            .one_row_batch()
                            .encode_arrow_snapshot_section(&executor)
                            .await
                            .assured("native restored row encodes");
                        let expected_columns = expected
                            .row
                            .one_row_batch()
                            .encode_arrow_snapshot_section(&executor)
                            .await
                            .assured("captured row encodes");
                        assert_eq!(actual_columns.as_ref(), expected_columns.as_ref());
                    }
                });
            });
    }

    /// A node whose snapshot sections are far smaller than the snapshot under test, so a snapshot
    /// that seals and moves proves it did so as many bounded sections and many bounded chunks
    /// rather than as one large one.
    fn narrow_executor() -> Executor {
        Executor::new(ExecutionConfig {
            budgets: MemoryBudgets {
                bulk: ByteUnit::Mebibyte(32),
                ..MemoryBudgets::default()
            },
            limits: OperationLimits {
                snapshot_section_bytes: ByteUnit::Mebibyte(1),
                snapshot_record_bytes: ByteUnit::Kibibyte(256),
                snapshot_header_bytes: ByteUnit::Kibibyte(64),
                bulk_chunk_bytes: ByteUnit::Kibibyte(64),
                decoder_depth: NonZeroU32::new(64).assured("sixty-four is not zero"),
                ..OperationLimits::default()
            },
            ..ExecutionConfig::default()
        })
        .assured("a bulk budget above two sections plus a header and a chunk is consistent")
    }

    fn test_index(index: usize) -> i64 {
        i64::try_from(index).assured("the test builds fewer records than an i64 counts")
    }

    fn tenant_branch(tenant: &str) -> BranchKey {
        BranchKey::from_fields([(
            FieldName::try_from("tenant".to_string())
                .assured("the test branch field name satisfies the field grammar"),
            RuntimeValue::String(tenant.to_string()),
        )])
        .assured("a one-field branch key is well formed")
    }

    fn wide_generation() -> MaterializedGeneration {
        let records = (0..RECORDS)
            .map(|index| {
                let row = test_runtime_row([
                    ("index".to_string(), RuntimeValue::I64(test_index(index))),
                    (
                        "payload".to_string(),
                        RuntimeValue::String("p".repeat(RECORD_BYTES)),
                    ),
                ]);
                MaterializedGenerationRecord {
                    branch: Some(tenant_branch(&format!("tenant-{index:02}"))),
                    row,
                }
            })
            .collect::<Vec<_>>();
        let schema = records[0].row.arrow_schema();
        MaterializedGeneration::new(7, 3, 5, schema, records)
    }

    #[nervix_primitives::test]
    async fn admitted_handoff_preserves_the_generation_and_validates_its_frames() {
        let executor = narrow_executor();
        let root = tempfile::tempdir().assured("handoff staging directory opens");
        let staging = SnapshotStaging::new(
            root.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let row = test_runtime_row([("value".to_string(), RuntimeValue::I64(42))]);
        let schema = row.arrow_schema();
        let metadata = row.metadata().to_remote();
        let branch = Some(tenant_branch("alpha"));
        let sealed = MaterializedGeneration::new(
            7,
            3,
            5,
            schema.clone(),
            vec![MaterializedGenerationRecord {
                branch: branch.clone(),
                row,
            }],
        )
        .seal(&executor, &staging)
        .await
        .assured("the bounded generation seals");
        let descriptor = sealed.descriptor;
        let entry = sealed
            .into_persisted_entry(&executor)
            .await
            .assured("the handoff admits its resident entry");
        assert_eq!(entry.lsm, 7);
        assert_eq!(
            u64::try_from(entry.payload.len()).verified("the admitted payload length fits"),
            descriptor.length
        );
        assert_eq!(*blake3::hash(&entry.payload).as_bytes(), descriptor.digest);
        let header_limit = executor.limits().snapshot_header_bytes.as_u64();
        let summary = inspect_sealed_container(&entry.payload, header_limit)
            .assured("the complete handoff frame validates without decoding columns");
        assert_eq!(
            summary,
            SealedSnapshotSummary {
                revision: 7,
                fence: 3,
                branch_generation: 5,
                records: 1,
            }
        );
        let restored = RestoredMaterializedSnapshot::open_relay(
            &executor,
            &schema,
            SealedSource::borrowed(&executor, &entry.payload),
        )
        .await
        .assured("the handoff opens against the installed schema");
        assert_eq!(restored.records[0].branch, branch);
        assert_eq!(restored.records[0].row.metadata().to_remote(), metadata);
        assert_eq!(
            restored.records[0].row.value_at(0).assured("value loads"),
            Some(RuntimeValue::I64(42))
        );
        for length in [0, 8, 11, 12, entry.payload.len() - 1] {
            let error = inspect_sealed_container(&entry.payload[..length], header_limit)
                .expect_err("a truncated frame cannot be admitted for handoff");
            assert!(matches!(
                error.current_context(),
                MaterializedSnapshotError::Truncated { .. }
            ));
        }
        let mut bytes = entry.payload.clone();
        bytes[0] ^= 1;
        assert!(matches!(
            inspect_sealed_container(&bytes, header_limit)
                .expect_err("a foreign container has no Nervix header")
                .current_context(),
            MaterializedSnapshotError::NotASnapshot
        ));
        let mut bytes = entry.payload.clone();
        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            inspect_sealed_container(&bytes, header_limit)
                .expect_err("header admission precedes reading its declared bytes")
                .current_context(),
            MaterializedSnapshotError::HeaderTooLarge { .. }
        ));
        let header_length: usize = u32::from_le_bytes(
            entry.payload[8..12]
                .try_into()
                .assured("the header length is four bytes"),
        )
        .arch_into();
        let first_section = CONTAINER_FRAME_BYTES + header_length;
        let mut bytes = entry.payload.clone();
        bytes[first_section] = u8::from(SealedSectionKind::RecordColumns);
        assert!(matches!(
            inspect_sealed_container(&bytes, header_limit)
                .expect_err("each group begins with its identities")
                .current_context(),
            MaterializedSnapshotError::UnexpectedSection { .. }
        ));
        let mut bytes = entry.payload.clone();
        bytes[first_section + 1..first_section + SECTION_FRAME_BYTES]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            inspect_sealed_container(&bytes, header_limit)
                .expect_err("a section cannot overstate the bytes that remain")
                .current_context(),
            MaterializedSnapshotError::Truncated { .. }
        ));
        let mut bytes = entry.payload.clone();
        bytes.push(17);
        assert!(matches!(
            inspect_sealed_container(&bytes, header_limit)
                .expect_err("a handoff consumes the complete container")
                .current_context(),
            MaterializedSnapshotError::Truncated { .. }
        ));
    }

    #[nervix_primitives::test]
    async fn current_materialized_containers_validate_counts_keys_and_full_consumption() {
        let executor = Executor::default();
        let rows = (0..2)
            .map(|index| test_runtime_row(vec![("value".to_string(), RuntimeValue::I64(index))]))
            .collect::<Vec<_>>();
        let schema = rows[0].arrow_schema();
        let batch = RuntimeRecordBatch::from_rows(schema.clone(), rows.iter())
            .assured("two columnar rows have the same schema");
        let columns = batch
            .encode_arrow_snapshot_section(&executor)
            .await
            .assured("bounded fixture columns encode");
        let alpha = Some(tenant_branch("alpha"));
        let beta = Some(tenant_branch("beta"));
        for (records, groups, keys, trailing, reason) in [
            (0, 1, vec![], false, "group count"),
            (2, 0, vec![], false, "group count"),
            (1, 2, vec![], false, "group count"),
            (u64::MAX, 1, vec![], false, "metadata"),
            (2, 1, vec![alpha.clone(), alpha.clone()], false, "duplicate"),
            (2, 1, vec![None, beta.clone()], false, "unbranched"),
            (2, 1, vec![alpha.clone()], false, "describes"),
            (
                1,
                1,
                vec![alpha.clone(), beta.clone()],
                false,
                "declared count",
            ),
            (3, 1, vec![alpha.clone(), beta.clone()], false, "describes"),
            (2, 1, vec![alpha, beta], true, "trailing"),
        ] {
            let mut bytes = materialized_container_header(7, 3, 5, records, groups)
                .assured("a current header encodes");
            if !keys.is_empty() {
                bytes.extend(
                    materialized_identity_section(
                        keys.into_iter()
                            .map(|key| {
                                (
                                    BranchKey::to_remote_key(&key),
                                    rows[0].metadata().to_remote(),
                                )
                            })
                            .collect(),
                    )
                    .assured("current scalar identities encode"),
                );
                bytes.extend(
                    materialized_columns_frame(columns.len()).assured("column frame encodes"),
                );
                bytes.extend_from_slice(columns.as_ref());
            }
            if trailing {
                bytes.push(17);
            }
            let error = RestoredMaterializedSnapshot::open_relay(
                &executor,
                &schema,
                SealedSource::borrowed(&executor, &bytes),
            )
            .await
            .expect_err("invalid current snapshot must fail before installation");
            assert!(format!("{error:?}").contains(reason), "{error:?}");
        }
        let empty = materialized_container_header(0, 0, 0, 0, 0)
            .assured("an empty current generation encodes");
        let restored = RestoredMaterializedSnapshot::open_relay(
            &executor,
            &schema,
            SealedSource::borrowed(&executor, &empty),
        )
        .await
        .assured("an empty current generation consumes its complete container");
        assert!(restored.records.is_empty());
    }

    #[nervix_primitives::test]
    async fn a_snapshot_larger_than_the_transfer_budget_moves_through_bounded_chunks() {
        let executor = narrow_executor();
        let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
        let staging = SnapshotStaging::new(
            staging_root.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let generation = wide_generation();
        let schema = generation.records()[0].row.arrow_schema();
        let sealed = generation
            .seal(&executor, &staging)
            .await
            .assured("a generation of bounded records seals into bounded sections");
        assert!(
            sealed.descriptor.length > ByteUnit::Mebibyte(32).as_u64(),
            "the sealed snapshot is {} bytes, which does not exercise a large transfer",
            sealed.descriptor.length
        );
        let four_sections = executor
            .limits()
            .snapshot_section_bytes
            .as_u64()
            .checked_mul(4)
            .assured("four mebibyte-sized sections fit a u64");
        assert!(
            sealed.descriptor.length > four_sections,
            "a snapshot that fits a few sections does not exercise a sectioned transfer"
        );
        let error = sealed
            .clone()
            .into_persisted_entry(&executor)
            .await
            .expect_err("resident handoff refuses a generation above its memory budget");
        assert!(matches!(
            error.current_context(),
            MaterializedSnapshotError::Admission
        ));

        // Move it exactly as a transfer does: bounded chunks into staging, then a length and
        // digest check, then one bounded section read at a time.
        let mut writer = staging
            .stage(sealed.descriptor.length)
            .await
            .assured("the node's staging quota admits one snapshot");
        let mut reader = sealed
            .artifact
            .open_reader()
            .await
            .assured("sealed artifact opens");
        while let Some(chunk) = reader
            .next_chunk(executor.limits().bulk_chunk_bytes.as_u64())
            .await
            .assured("one bounded sealed chunk reads")
        {
            writer
                .write_chunk(chunk)
                .await
                .assured("a staged chunk within the declared length is accepted");
        }
        let staged = writer
            .finish(sealed.descriptor.digest)
            .await
            .assured("a complete transfer matches the length and digest it declared");

        let restored = RestoredMaterializedSnapshot::open_relay(
            &executor,
            &schema,
            SealedSource::staged(staged),
        )
        .await
        .assured("a staged snapshot opens one bounded section at a time");

        assert_eq!(restored.revision, 7);
        assert_eq!(restored.fence, 3);
        assert_eq!(restored.branch_generation, 5);
        assert_eq!(restored.records.len(), RECORDS);
        for (index, record) in restored.records.iter().enumerate() {
            assert_eq!(
                record.branch,
                Some(tenant_branch(&format!("tenant-{index:02}")))
            );
            assert_eq!(
                record
                    .row
                    .value_at(0)
                    .assured("the restored index column loads"),
                Some(RuntimeValue::I64(test_index(index)))
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_truncated_transfer_is_refused_before_anything_reads_it() {
        let executor = narrow_executor();
        let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
        let staging = SnapshotStaging::new(
            staging_root.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let sealed = wide_generation()
            .seal(&executor, &staging)
            .await
            .assured("a generation of bounded records seals into bounded sections");
        let mut writer = staging
            .stage(sealed.descriptor.length)
            .await
            .assured("the node's staging quota admits one snapshot");
        let mut reader = sealed
            .artifact
            .open_reader()
            .await
            .assured("sealed artifact opens");
        let short = reader
            .next_chunk(64 * 1024)
            .await
            .assured("a bounded chunk reads")
            .assured("the snapshot contains one chunk");
        writer
            .write_chunk(short)
            .await
            .assured("a chunk within the declared length is accepted");

        assert!(writer.finish(sealed.descriptor.digest).await.is_err());
    }

    #[nervix_primitives::test]
    async fn a_corrupted_transfer_is_refused_before_anything_reads_it() {
        let executor = narrow_executor();
        let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
        let staging = SnapshotStaging::new(
            staging_root.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let sealed = wide_generation()
            .seal(&executor, &staging)
            .await
            .assured("a generation of bounded records seals into bounded sections");
        let mut writer = staging
            .stage(sealed.descriptor.length)
            .await
            .assured("the node's staging quota admits one snapshot");
        let mut reader = sealed
            .artifact
            .open_reader()
            .await
            .assured("sealed artifact opens");
        while let Some(chunk) = reader
            .next_chunk(64 * 1024)
            .await
            .assured("a bounded chunk reads")
        {
            writer
                .write_chunk(chunk)
                .await
                .assured("a bounded chunk is accepted");
        }

        assert!(writer.finish([0; 32]).await.is_err());
    }
}
