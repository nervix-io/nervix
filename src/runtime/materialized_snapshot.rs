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

use std::{io::Write as _, ops::Range, sync::Arc as StdArc};

use arch_into::ArchInto as _;
use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass};
use nervix_models::{RemoteRuntimeField, RemoteRuntimeRecordMetadata};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;
use triomphe::Arc;

use super::{BranchKey, snapshot_staging::StagedSnapshot};
use crate::runtime_schema::{RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow};

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
pub(in crate::runtime) enum MaterializedSnapshotError {
    #[error("the node has no bulk capacity to seal or open this snapshot")]
    Admission,
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
        "the sealed snapshot declares schema fingerprint {declared} where {expected} is installed"
    )]
    SchemaFingerprintMismatch { declared: String, expected: String },
    #[error(
        "the ownership assignment changed from fence {captured} to {current} while the snapshot          was being sealed"
    )]
    OwnershipChanged { captured: u64, current: u64 },
    #[error("failed to decode the materialized relay snapshot: {reason}")]
    Decode { reason: String },
}

impl MaterializedSnapshotError {
    fn encoding(error: impl ToString) -> Report<Self> {
        Report::new(Self::Encode {
            reason: error.to_string(),
        })
    }

    fn decoding(error: impl ToString) -> Report<Self> {
        Report::new(Self::Decode {
            reason: error.to_string(),
        })
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
    /// The schema the Arrow sections declare.
    schema_fingerprint: [u8; 32],
    /// How many records the sections together carry.
    records: u64,
    /// How many groups follow, each an identity record and the Arrow section it describes.
    groups: u32,
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
pub(in crate::runtime) struct MaterializedGeneration {
    revision: u64,
    fence: u64,
    branch_generation: u64,
    schema_fingerprint: [u8; 32],
    schema: StdArc<ArrowSchema>,
    records: Arc<Vec<MaterializedGenerationRecord>>,
}

/// One captured record: its branch identity and the shared row view holding its columns.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct MaterializedGenerationRecord {
    pub(in crate::runtime) branch: Option<BranchKey>,
    pub(in crate::runtime) row: RuntimeRow,
}

/// One sealed snapshot's bytes together with what a receiver checks them against.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct SealedMaterializedSnapshot {
    pub(in crate::runtime) descriptor: SealedSnapshotDescriptor,
    pub(in crate::runtime) bytes: ChargedBytes,
}

/// What a sealed snapshot supplies about itself before a byte of it is transferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(in crate::runtime) struct SealedSnapshotDescriptor {
    pub(in crate::runtime) length: u64,
    pub(in crate::runtime) digest: [u8; 32],
    pub(in crate::runtime) schema_fingerprint: [u8; 32],
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
    pub(in crate::runtime) schema_fingerprint: [u8; 32],
    pub(in crate::runtime) records: u64,
}

/// The container an empty generation seals into: one header and no sections.
///
/// It carries no columns, so it needs no schema. This is what a destination is handed when an
/// entity moves with no materialized records behind it.
pub(in crate::runtime) fn empty_sealed_container(
    schema_fingerprint: [u8; 32],
) -> Result<Vec<u8>, Report<MaterializedSnapshotError>> {
    let header = rkyv::to_bytes::<rkyv::rancor::Error>(&SealedSnapshotHeader {
        revision: 0,
        fence: 0,
        branch_generation: 0,
        schema_fingerprint,
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
        schema_fingerprint: header.schema_fingerprint,
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
    /// The entry this sealed snapshot is stored and carried as. The payload is the sealed
    /// container itself: header, identity records and Arrow sections, exactly as it was written.
    pub(in crate::runtime) fn into_persisted_entry(self) -> super::PersistedRuntimeStateEntry {
        super::PersistedRuntimeStateEntry {
            lsm: self.descriptor.revision,
            schema_fingerprint: self.descriptor.schema_fingerprint,
            payload: self.bytes.as_ref().to_vec(),
        }
    }
}

impl MaterializedGeneration {
    pub(in crate::runtime) fn new(
        revision: u64,
        fence: u64,
        branch_generation: u64,
        schema_fingerprint: [u8; 32],
        schema: StdArc<ArrowSchema>,
        records: Vec<MaterializedGenerationRecord>,
    ) -> Self {
        Self {
            revision,
            fence,
            branch_generation,
            schema_fingerprint,
            schema,
            records: Arc::new(records),
        }
    }

    pub(in crate::runtime) fn revision(&self) -> u64 {
        self.revision
    }

    pub(in crate::runtime) fn fence(&self) -> u64 {
        self.fence
    }

    pub(in crate::runtime) fn records(&self) -> &[MaterializedGenerationRecord] {
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
    ) -> Result<SealedMaterializedSnapshot, Report<MaterializedSnapshotError>> {
        let groups = self.groups(executor);
        let mut sections = Vec::new();
        for group in &groups {
            tokio::task::consume_budget().await;
            sections.push(self.seal_identities(executor, group).await?);
            sections.push(self.seal_columns(executor, group).await?);
        }
        let groups = u32::try_from(groups.len())
            .map_err(|_| MaterializedSnapshotError::encoding("too many snapshot groups"))?;
        let header = SealedSnapshotHeader {
            revision: self.revision,
            fence: self.fence,
            branch_generation: self.branch_generation,
            schema_fingerprint: self.schema_fingerprint,
            records: self.records.len().arch_into(),
            groups,
        };
        seal_container(executor, header, sections).await
    }

    /// Split the records into groups that each fit one Arrow section and one identity record.
    ///
    /// Grouping is by measured payload bytes rather than by a row-count guess, so one enormous
    /// record occupies a group of its own instead of pushing a section past its limit.
    fn groups(&self, executor: &Executor) -> Vec<Range<usize>> {
        let section_limit = executor.limits().snapshot_section_bytes.as_u64();
        let identity_limit = executor.limits().snapshot_record_bytes.as_u64();
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
        let identities = self.records[group.clone()]
            .iter()
            .map(|record| SealedRecordIdentity {
                branch: BranchKey::to_remote_key(&record.branch),
                watermarks: record.row.metadata().to_remote(),
            })
            .collect::<Vec<_>>();
        let limit = executor.limits().snapshot_record_bytes.as_u64();
        let bytes = encode_rkyv(executor, SealedRecordIdentities { identities }, limit).await?;
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
        // Projection, not reconstruction: the batch reuses the carrier columns these rows already
        // share whenever a group is exactly one carrier batch.
        let batch = RuntimeRecordBatch::from_rows(
            StdArc::clone(&self.schema),
            self.records[group.clone()].iter().map(|record| &record.row),
        )
        .map_err(MaterializedSnapshotError::encoding)?;
        let bytes = batch
            .encode_arrow_snapshot_section(executor)
            .await
            .change_context(MaterializedSnapshotError::Encode {
                reason: "the Arrow section could not be written".to_string(),
            })?;
        Ok(SealedSection {
            kind: SealedSectionKind::RecordColumns,
            bytes,
        })
    }
}

/// One encoded section waiting to be written into a container.
struct SealedSection {
    kind: SealedSectionKind,
    bytes: ChargedBytes,
}

/// One record restored from a sealed snapshot, ready to be installed under the ownership barrier.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct RestoredMaterializedRecord {
    pub(in crate::runtime) branch: Option<BranchKey>,
    pub(in crate::runtime) row: RuntimeRow,
}

/// Everything a sealed snapshot restores into: the records, and the revision, fence and branch
/// generation they belong to.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct RestoredMaterializedSnapshot {
    pub(in crate::runtime) revision: u64,
    pub(in crate::runtime) fence: u64,
    pub(in crate::runtime) branch_generation: u64,
    pub(in crate::runtime) records: Vec<RestoredMaterializedRecord>,
}

impl RestoredMaterializedSnapshot {
    /// Open a sealed snapshot against the schema installed here.
    ///
    /// Every length the container declares is checked against the limit for the section it names
    /// and against the bytes that actually remain, so a truncated or overstated snapshot is
    /// refused before it allocates anything.
    pub(in crate::runtime) async fn open(
        executor: &Executor,
        schema: &StdArc<ArrowSchema>,
        schema_fingerprint: [u8; 32],
        source: SealedSource,
    ) -> Result<Self, Report<MaterializedSnapshotError>> {
        let mut cursor = source;
        let magic = cursor
            .take(SEALED_SNAPSHOT_MAGIC.len().arch_into(), "magic")
            .await?;
        if magic.as_ref() != SEALED_SNAPSHOT_MAGIC {
            return Err(Report::new(MaterializedSnapshotError::NotASnapshot));
        }
        let header_bytes = u64::from(cursor.take_u32("header length").await?);
        let header_limit = executor.limits().snapshot_header_bytes.as_u64();
        if header_bytes > header_limit {
            return Err(Report::new(MaterializedSnapshotError::HeaderTooLarge {
                size: header_bytes,
                limit: header_limit,
            }));
        }
        let header = cursor.take(header_bytes, "header").await?;
        let header = decode_rkyv::<SealedSnapshotHeader>(executor, header, header_limit).await?;
        if header.schema_fingerprint != schema_fingerprint {
            return Err(Report::new(
                MaterializedSnapshotError::SchemaFingerprintMismatch {
                    declared: encode_hex(&header.schema_fingerprint),
                    expected: encode_hex(&schema_fingerprint),
                },
            ));
        }
        let identity_limit = executor.limits().snapshot_record_bytes.as_u64();
        let section_limit = executor.limits().snapshot_section_bytes.as_u64();
        let mut records = Vec::new();
        for _ in 0..header.groups {
            tokio::task::consume_budget().await;
            let identities = cursor
                .take_section(SealedSectionKind::RecordIdentities, identity_limit)
                .await?;
            let identities =
                decode_rkyv::<SealedRecordIdentities>(executor, identities, identity_limit).await?;
            let columns = cursor
                .take_section(SealedSectionKind::RecordColumns, section_limit)
                .await?;
            let batch = RuntimeRecordBatch::decode_arrow_snapshot_section(
                executor,
                StdArc::clone(schema),
                columns,
            )
            .await
            .change_context(MaterializedSnapshotError::Decode {
                reason: "the Arrow section could not be read".to_string(),
            })?;
            let batch = Arc::new(batch);
            if identities.identities.len() != batch.batch().num_rows() {
                return Err(Report::new(MaterializedSnapshotError::LengthMismatch {
                    declared: identities.identities.len(),
                    actual: batch.batch().num_rows(),
                }));
            }
            for (row, identity) in identities.identities.into_iter().enumerate() {
                let branch = BranchKey::from_remote_key(identity.branch)
                    .map_err(MaterializedSnapshotError::decoding)?;
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
        Ok(Self {
            revision: header.revision,
            fence: header.fence,
            branch_generation: header.branch_generation,
            records,
        })
    }
}

/// Where a sealed container's bytes come from while it is opened.
///
/// A container that already sits in memory under one charge is read in place. A container that
/// arrived over the interconnect is read from the file it was staged into, one bounded section at
/// a time, so a snapshot larger than the node's transfer-memory budget opens without ever being
/// held whole.
pub(in crate::runtime) enum SealedSource {
    Memory { sealed: ChargedBytes, offset: usize },
    Staged(StagedSnapshot),
}

impl SealedSource {
    pub(in crate::runtime) fn memory(sealed: ChargedBytes) -> Self {
        Self::Memory { sealed, offset: 0 }
    }

    pub(in crate::runtime) fn staged(staged: StagedSnapshot) -> Self {
        Self::Staged(staged)
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
            Self::Memory { sealed, offset } => {
                let length = usize::try_from(length).map_err(|_| truncated())?;
                let end = offset.checked_add(length).ok_or_else(truncated)?;
                let slice = sealed.slice(*offset, end).ok_or_else(truncated)?;
                *offset = end;
                Ok(slice)
            }
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

/// Write the header and every section into one buffer, measuring and digesting it as it is written.
async fn seal_container(
    executor: &Executor,
    header: SealedSnapshotHeader,
    sections: Vec<SealedSection>,
) -> Result<SealedMaterializedSnapshot, Report<MaterializedSnapshotError>> {
    let header_limit = executor.limits().snapshot_header_bytes.as_u64();
    let schema_fingerprint = header.schema_fingerprint;
    let revision = header.revision;
    let fence = header.fence;
    let branch_generation = header.branch_generation;
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
        .reserve(MemoryClass::Bulk, length)
        .await
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
    Ok(SealedMaterializedSnapshot {
        descriptor: SealedSnapshotDescriptor {
            length: bytes.len().arch_into(),
            digest: *blake3::hash(bytes.as_ref()).as_bytes(),
            schema_fingerprint,
            revision,
            fence,
            branch_generation,
        },
        bytes,
    })
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
fn decode_aligned_rkyv<T>(bytes: &[u8]) -> Result<T, Report<MaterializedSnapshotError>>
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

fn encode_hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
    const RECORDS: usize = 20;

    /// A node whose snapshot sections are far smaller than the snapshot under test, so a snapshot
    /// that seals and moves proves it did so as many bounded sections and many bounded chunks
    /// rather than as one large one.
    fn narrow_executor() -> Executor {
        Executor::new(ExecutionConfig {
            budgets: MemoryBudgets {
                bulk: ByteUnit::Mebibyte(64),
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
        MaterializedGeneration::new(7, 3, 5, [9; 32], schema, records)
    }

    #[tokio::test]
    async fn a_snapshot_larger_than_the_transfer_budget_moves_through_bounded_chunks() {
        let executor = narrow_executor();
        let generation = wide_generation();
        let schema = generation.records()[0].row.arrow_schema();
        let sealed = generation
            .seal(&executor)
            .await
            .assured("a generation of bounded records seals into bounded sections");
        assert!(
            sealed.descriptor.length > ByteUnit::Mebibyte(8).as_u64(),
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

        // Move it exactly as a transfer does: bounded chunks into staging, then a length and
        // digest check, then one bounded section read at a time.
        let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
        let staging = SnapshotStaging::new(
            staging_root.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let mut writer = staging
            .stage(sealed.descriptor.length)
            .await
            .assured("the node's staging quota admits one snapshot");
        let chunk_bytes = usize::try_from(executor.limits().bulk_chunk_bytes.as_u64())
            .assured("a configured chunk size fits an address");
        let mut offset = 0;
        while offset < sealed.bytes.len() {
            let end = offset
                .checked_add(chunk_bytes)
                .assured("the offset walks a buffer that already fits an address")
                .min(sealed.bytes.len());
            let chunk = sealed
                .bytes
                .slice(offset, end)
                .assured("the window lies inside the sealed snapshot");
            writer
                .write_chunk(chunk)
                .await
                .assured("a staged chunk within the declared length is accepted");
            offset = end;
        }
        let staged = writer
            .finish(sealed.descriptor.digest)
            .await
            .assured("a complete transfer matches the length and digest it declared");

        let restored = RestoredMaterializedSnapshot::open(
            &executor,
            &schema,
            [9; 32],
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

    #[tokio::test]
    async fn a_truncated_transfer_is_refused_before_anything_reads_it() {
        let executor = narrow_executor();
        let sealed = wide_generation()
            .seal(&executor)
            .await
            .assured("a generation of bounded records seals into bounded sections");
        let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
        let staging = SnapshotStaging::new(
            staging_root.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let mut writer = staging
            .stage(sealed.descriptor.length)
            .await
            .assured("the node's staging quota admits one snapshot");
        let short = sealed
            .bytes
            .slice(0, sealed.bytes.len() / 2)
            .assured("half of the sealed snapshot lies inside it");
        writer
            .write_chunk(short)
            .await
            .assured("a chunk within the declared length is accepted");

        assert!(writer.finish(sealed.descriptor.digest).await.is_err());
    }

    #[tokio::test]
    async fn a_corrupted_transfer_is_refused_before_anything_reads_it() {
        let executor = narrow_executor();
        let sealed = wide_generation()
            .seal(&executor)
            .await
            .assured("a generation of bounded records seals into bounded sections");
        let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
        let staging = SnapshotStaging::new(
            staging_root.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let mut writer = staging
            .stage(sealed.descriptor.length)
            .await
            .assured("the node's staging quota admits one snapshot");
        writer
            .write_chunk(sealed.bytes.clone())
            .await
            .assured("the whole snapshot is within the declared length");

        assert!(writer.finish([0; 32]).await.is_err());
    }
}
