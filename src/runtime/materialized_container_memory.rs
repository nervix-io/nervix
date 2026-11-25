//! What each step of encoding a sealed container takes and where it takes it from, and what
//! opening one takes.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The charge of every step that encodes a sealed container, the plan a page that
//!   carries a container sets aside by and the two places those steps draw their memory from: the
//!   node's pools, or memory taken one step ahead. Also what opening a relay container of a given
//!   layout takes, by the opener's own charge rules, which a reader admits before it receives the
//!   container.
//! - **Depends on.** The sealed container's frames, header and sections, the Arrow body codec's
//!   decode charge and the executor's memory classes.
//! - **Must not know.** Who reads a container, where it is stored, or how its bytes reach another
//!   node.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use arch_into::ArchInto as _;
use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{ChargedBytes, Executor, MemoryClass, Reservation};

use super::{
    BranchKey, CONTAINER_FRAME_BYTES, MaterializedGenerationRecord, MaterializedRecordLayout,
    MaterializedSnapshotError, RestoredMaterializedRecord, RestoredMaterializedSnapshot,
    RestoredRows, SECTION_FRAME_BYTES, estimated_identity_bytes,
};
use crate::runtime_schema::RuntimeRecordBatch;

/// The row-view metadata one group's identity section of `identities` bytes adds to an opened
/// container's charge: eight bytes for each byte of the section, which bounds the branch keys and
/// watermarks it decodes into.
pub(super) fn identity_metadata_bytes(
    identities: u64,
) -> Result<u64, Report<MaterializedSnapshotError>> {
    identities
        .checked_mul(8)
        .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))
}

/// What one group's decoded Arrow columns keep while the opened container lives, for a column
/// section of `columns` bytes: twice the section, within the section bound `section_limit`, and
/// at least one byte.
pub(super) fn retained_column_bytes(
    columns: u64,
    section_limit: u64,
) -> Result<u64, Report<MaterializedSnapshotError>> {
    let retained = columns
        .checked_mul(2)
        .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
    Ok(retained.min(section_limit).max(1))
}

/// The shape of a sealed container that a reader admits memory for before it holds any of the
/// container: how many records it carries, and the exact length of each group's sections, in
/// container order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct SealedContainerLayout {
    pub(in crate::runtime) records: u64,
    pub(in crate::runtime) groups: Vec<SealedGroupLayout>,
}

/// The exact lengths of one group's record identity section and its Arrow column section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct SealedGroupLayout {
    pub(in crate::runtime) identities: u64,
    pub(in crate::runtime) columns: u64,
}

/// A resident container and its layout.
pub(in crate::runtime) struct ResidentContainer {
    pub(in crate::runtime) bytes: ChargedBytes,
    pub(in crate::runtime) layout: SealedContainerLayout,
}

impl MaterializedRecordLayout {
    /// The row views and uniqueness table `records` records in `groups` groups take, refused when
    /// the counts contradict each other or the views pass `rows`' own ceiling.
    pub(super) fn of(
        records: u64,
        groups: u32,
        rows: RestoredRows,
        relay_budget: u64,
    ) -> Result<Self, Report<MaterializedSnapshotError>> {
        if (records == 0) != (groups == 0) || u64::from(groups) > records {
            return Err(MaterializedSnapshotError::decoding(
                "invalid materialized group count",
            ));
        }
        let metadata_bytes = records
            .checked_mul(
                u64::try_from(
                    std::mem::size_of::<RestoredMaterializedRecord>()
                        + std::mem::size_of::<nervix_execution::Reservation>()
                        + 2 * (std::mem::size_of::<Option<BranchKey>>() + 1),
                )
                .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?,
            )
            .ok_or_else(|| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
        if rows.exceeds_ceiling(metadata_bytes, relay_budget) {
            return Err(Report::new(MaterializedSnapshotError::MetadataTooLarge));
        }
        let records = usize::try_from(records)
            .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
        let groups = usize::try_from(groups)
            .map_err(|_| Report::new(MaterializedSnapshotError::MetadataTooLarge))?;
        Ok(Self {
            records,
            groups,
            metadata_bytes,
        })
    }
}

/// What opening a relay container takes, by the opener's own charge rules, which a reader admits
/// before it receives the container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct RelayOpening {
    /// The scratch the container's decoders draw from, one section at a time, giving it back after
    /// each.
    pub(in crate::runtime) scratch: u64,
    /// What the opened rows keep of their sections: eight bytes of row metadata for each byte of
    /// record identities, and each group's decoded columns.
    pub(in crate::runtime) sections: u64,
    /// The opened rows' views and uniqueness table.
    pub(in crate::runtime) metadata: u64,
}

impl RelayOpening {
    /// All of it, which the reader admits as one charge.
    pub(in crate::runtime) fn need(&self) -> Result<u64, Report<MaterializedSnapshotError>> {
        let too_large = || Report::new(MaterializedSnapshotError::MetadataTooLarge);
        self.scratch
            .checked_add(self.sections)
            .ok_or_else(too_large)?
            .checked_add(self.metadata)
            .ok_or_else(too_large)
    }
}

impl SealedContainerLayout {
    /// What opening a relay container of this layout and of `length` bytes takes on `executor`:
    /// the bound on one section's decoding scratch, and the share the opened rows keep, which
    /// [`RestoredMaterializedSnapshot::open_relay_within`] draws every step of the opening from. A
    /// layout whose sections pass their bounds, or whose row views pass the relay's budget, is
    /// refused as opening the container would refuse it.
    pub(in crate::runtime) fn relay_opening(
        &self,
        executor: &Executor,
        length: u64,
    ) -> Result<RelayOpening, Report<MaterializedSnapshotError>> {
        let too_large = || Report::new(MaterializedSnapshotError::MetadataTooLarge);
        let limits = executor.limits();
        let identity_limit = limits.snapshot_record_bytes.as_u64();
        let section_limit = limits.snapshot_section_bytes.as_u64();
        let relay_budget = executor.memory(MemoryClass::Relay).capacity_bytes;
        let rows = RestoredRows::Relay;
        let groups = u32::try_from(self.groups.len()).map_err(|_| too_large())?;
        let layout = MaterializedRecordLayout::of(self.records, groups, rows, relay_budget)?;
        let metadata = layout.metadata_bytes.max(1);
        let mut kept_metadata = metadata;
        let mut sections = 0_u64;
        for group in &self.groups {
            if group.identities > identity_limit {
                return Err(Report::new(MaterializedSnapshotError::RecordTooLarge {
                    size: group.identities,
                    limit: identity_limit,
                }));
            }
            if group.columns > section_limit {
                return Err(Report::new(MaterializedSnapshotError::SectionTooLarge {
                    size: group.columns,
                    limit: section_limit,
                }));
            }
            let identities = identity_metadata_bytes(group.identities)?;
            kept_metadata = kept_metadata.checked_add(identities).ok_or_else(too_large)?;
            if rows.exceeds_ceiling(kept_metadata, relay_budget) {
                return Err(too_large());
            }
            sections = sections
                .checked_add(identities)
                .ok_or_else(too_large)?
                .checked_add(retained_column_bytes(group.columns, section_limit)?)
                .ok_or_else(too_large)?;
        }
        Ok(RelayOpening {
            scratch: RestoredMaterializedSnapshot::decode_scratch_bound(executor, length),
            sections,
            metadata,
        })
    }
}

/// Fills groups of records that each fit one Arrow section and one identity record, by each
/// record's measured payload and estimated identity bytes, so one enormous record occupies a
/// group of its own instead of pushing a section past its limit.
#[derive(Debug, Clone, Copy)]
pub(super) struct GroupFill {
    identity_limit: u64,
    section_limit: u64,
    /// The open group's column bytes and identity bytes.
    columns: u64,
    identities: u64,
    /// Whether a group is open.
    open: bool,
}

impl GroupFill {
    pub(super) fn new(identity_limit: u64, section_limit: u64) -> Self {
        Self {
            identity_limit,
            section_limit,
            columns: 0,
            identities: 0,
            open: false,
        }
    }

    /// Adds `record` and whether it closes the open group and starts the next one.
    pub(super) fn add(&mut self, record: &MaterializedGenerationRecord) -> bool {
        let record_columns = record.row.one_row_batch().estimated_bytes();
        let record_identity = estimated_identity_bytes(record.branch.as_ref());
        // A total that does not fit a u64 is past every limit there is, so it closes the group
        // exactly as an ordinary overrun does.
        let next = match (
            self.columns.checked_add(record_columns),
            self.identities.checked_add(record_identity),
        ) {
            (Some(next_columns), Some(next_identities)) => Some((next_columns, next_identities)),
            _ => None,
        };
        let overruns = match next {
            Some((next_columns, next_identities)) => {
                next_columns > self.section_limit || next_identities > self.identity_limit
            }
            None => true,
        };
        if self.open && overruns {
            self.columns = record_columns;
            self.identities = record_identity;
            return true;
        }
        self.open = true;
        match next {
            Some((next_columns, next_identities)) => {
                self.columns = next_columns;
                self.identities = next_identities;
            }
            None => {
                self.columns = self.section_limit;
                self.identities = self.identity_limit;
            }
        }
        false
    }
}

/// The job that seals one group's identities: the remote identities, their aligned serialization
/// and the bounded output overlap in it, each within the identity record bound `limit`.
pub(super) fn identity_job_bytes(limit: u64) -> Result<u64, Report<MaterializedSnapshotError>> {
    limit
        .checked_mul(3)
        .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))
}

/// What sealing one group's columns charges: the projection of its rows while it exists, and the
/// section it is encoded into from the start.
#[derive(Debug, Clone, Copy)]
pub(super) struct ColumnCharge {
    projection: u64,
    encoded: u64,
}

impl ColumnCharge {
    /// The charge for a group of `rows` rows whose columns measure `payload` bytes, in a schema of
    /// `fields` fields: the projection takes twice the payload, as its builders grow, and room for
    /// each row's and each field's offsets and validity; the section starts at the payload and the
    /// IPC stream's framing.
    pub(super) fn of(
        payload: u64,
        rows: u64,
        fields: u64,
    ) -> Result<Self, Report<MaterializedSnapshotError>> {
        let too_large = || Report::new(MaterializedSnapshotError::Admission);
        let row_bytes = rows.checked_mul(256).ok_or_else(too_large)?;
        let schema_bytes = fields.checked_mul(256).ok_or_else(too_large)?;
        let projection = payload
            .checked_mul(2)
            .ok_or_else(too_large)?
            .checked_add(row_bytes)
            .ok_or_else(too_large)?
            .checked_add(schema_bytes)
            .ok_or_else(too_large)?;
        let encoded = payload.checked_add(64 * 1024).ok_or_else(too_large)?;
        Ok(Self {
            projection,
            encoded,
        })
    }
}

/// Where encoding a resident container takes the memory each of its steps spends.
pub(super) enum EncodingCharges {
    /// Each step takes its memory from the bulk pool as it reaches it.
    Pools,
    /// Every step draws on memory a page set aside for its container as it selected the records:
    /// what is left of it, absent only once a step failed.
    SetAside(Option<Reservation>),
}

impl EncodingCharges {
    /// Takes `bytes` for one step: from the bulk pool, waiting for room there, or from what the page
    /// set aside.
    pub(super) async fn take(
        &mut self,
        executor: &Executor,
        bytes: u64,
    ) -> Result<Reservation, Report<MaterializedSnapshotError>> {
        match self {
            Self::Pools => executor
                .reserve(MemoryClass::Bulk, bytes.max(1))
                .await
                .change_context(MaterializedSnapshotError::Admission),
            Self::SetAside(memory) => {
                let left = memory
                    .take()
                    .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
                let (taken, rest) = left
                    .split(bytes)
                    .change_context(MaterializedSnapshotError::Admission)?;
                *memory = Some(rest);
                Ok(taken)
            }
        }
    }

    /// Gives back what a step only borrowed: to the pool, or to what the page set aside.
    pub(super) fn give_back(
        &mut self,
        charge: Reservation,
    ) -> Result<(), Report<MaterializedSnapshotError>> {
        match self {
            Self::Pools => {
                drop(charge);
                Ok(())
            }
            Self::SetAside(memory) => {
                let left = memory
                    .take()
                    .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
                let merged = left
                    .merge(charge)
                    .change_context(MaterializedSnapshotError::Admission)?;
                *memory = Some(merged);
                Ok(())
            }
        }
    }

    /// Encodes one group's columns, projected by `project`, under `charge`.
    pub(super) async fn encode_columns<E: error_stack::Context>(
        &mut self,
        executor: &Executor,
        charge: ColumnCharge,
        project: impl FnMut() -> error_stack::Result<RuntimeRecordBatch, E> + Send + 'static,
    ) -> Result<ChargedBytes, Report<MaterializedSnapshotError>> {
        let unwritable = |error| {
            MaterializedSnapshotError::arrow(
                error,
                MaterializedSnapshotError::Encode {
                    reason: "the Arrow section could not be written".to_string(),
                },
            )
        };
        if let Self::Pools = self {
            return RuntimeRecordBatch::encode_arrow_snapshot_projection(
                executor,
                charge.projection,
                charge.encoded,
                project,
            )
            .await
            .map_err(unwritable);
        }
        let limit = executor.limits().snapshot_section_bytes.as_u64();
        let projection = self.take(executor, charge.projection).await?;
        let encoded = self.take(executor, charge.encoded.min(limit)).await?;
        let (body, projection) = RuntimeRecordBatch::encode_arrow_snapshot_projection_within(
            executor,
            projection,
            encoded,
            project,
        )
        .await
        .map_err(unwritable)?;
        self.give_back(projection)?;
        Ok(body)
    }

    /// The container header's encoding, within the header bound `limit`. From the pool it waits
    /// for room.
    pub(super) async fn header(
        &mut self,
        executor: &Executor,
        limit: u64,
    ) -> Result<Reservation, Report<MaterializedSnapshotError>> {
        match self {
            Self::Pools => executor
                .reserve(MemoryClass::Bulk, limit)
                .await
                .change_context(MaterializedSnapshotError::Admission),
            Self::SetAside(_) => self.take(executor, limit).await,
        }
    }

    /// The container of `length` bytes. From the pool it waits for room. From what the page set
    /// aside, a container whose sections came out larger than the plan bounded them tops up from
    /// the bulk pool at once, or the encoding is refused.
    pub(super) async fn container(
        &mut self,
        executor: &Executor,
        length: u64,
    ) -> Result<Reservation, Report<MaterializedSnapshotError>> {
        let Self::SetAside(memory) = self else {
            return self.take(executor, length).await;
        };
        let left = memory
            .take()
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        let Some(lacking) = length.checked_sub(left.bytes()) else {
            let (taken, rest) = left
                .split(length)
                .change_context(MaterializedSnapshotError::Admission)?;
            *memory = Some(rest);
            return Ok(taken);
        };
        let more = executor
            .try_reserve(MemoryClass::Bulk, lacking)
            .change_context(MaterializedSnapshotError::Admission)?;
        left.merge(more)
            .change_context(MaterializedSnapshotError::Admission)
    }
}

/// What encoding the resident container of the records added so far takes, for a page that sets
/// that memory aside as it selects the records: the most the encoding holds at once, step by step
/// in the order [`super::MaterializedGeneration::encode_resident_container_within`] takes it. Each group
/// borrows its identity job and its column projection for their steps alone, keeps at most an
/// identity record bound and its column section's starting charge for the sections it writes, then
/// the header keeps at most its bound and the container takes its sections and frames at most.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResidentEncodingPlan {
    fill: GroupFill,
    fields: u64,
    identity_limit: u64,
    section_limit: u64,
    header_limit: u64,
    /// What the closed groups' sections keep, at most.
    kept: u64,
    /// The most the closed groups' steps held at once.
    peak: u64,
    /// The closed groups' framed sections in the container, at most.
    framed: u64,
    /// The open group's column payload and rows.
    payload: u64,
    rows: u64,
}

impl ResidentEncodingPlan {
    /// The plan of an empty container of `schema`'s records under `executor`'s bounds.
    pub(in crate::runtime) fn new(executor: &Executor, schema: &ArrowSchema) -> Self {
        let limits = executor.limits();
        Self {
            fill: GroupFill::new(
                limits.snapshot_record_bytes.as_u64(),
                limits.snapshot_section_bytes.as_u64(),
            ),
            fields: schema.fields().len().arch_into(),
            identity_limit: limits.snapshot_record_bytes.as_u64(),
            section_limit: limits.snapshot_section_bytes.as_u64(),
            header_limit: limits.snapshot_header_bytes.as_u64(),
            kept: 0,
            peak: 0,
            framed: 0,
            payload: 0,
            rows: 0,
        }
    }

    /// The plan with `record` added as the container's next record. A record whose columns alone
    /// pass the section bound can never be sealed and is refused.
    pub(in crate::runtime) fn with(
        &self,
        record: &MaterializedGenerationRecord,
    ) -> Result<Self, Report<MaterializedSnapshotError>> {
        let too_large = || Report::new(MaterializedSnapshotError::Admission);
        let identity = estimated_identity_bytes(record.branch.as_ref());
        if identity > self.identity_limit {
            return Err(Report::new(MaterializedSnapshotError::RecordTooLarge {
                size: identity,
                limit: self.identity_limit,
            }));
        }
        let columns = record.row.one_row_batch().estimated_bytes();
        if columns > self.section_limit {
            return Err(Report::new(MaterializedSnapshotError::SectionTooLarge {
                size: columns,
                limit: self.section_limit,
            }));
        }
        let mut next = *self;
        if next.fill.add(record) {
            next.close()?;
        }
        next.payload = next.payload.checked_add(columns).ok_or_else(too_large)?;
        next.rows = next.rows.checked_add(1).ok_or_else(too_large)?;
        Ok(next)
    }

    /// Closes the open group into what the closed groups take.
    fn close(&mut self) -> Result<(), Report<MaterializedSnapshotError>> {
        let steps = self.open_steps(self.kept)?;
        self.kept = steps.kept;
        self.peak = self.peak.max(steps.peak);
        self.framed = self
            .framed
            .checked_add(steps.framed)
            .ok_or_else(|| Report::new(MaterializedSnapshotError::Admission))?;
        self.payload = 0;
        self.rows = 0;
        Ok(())
    }

    /// The open group's steps after `kept` bytes of earlier sections: the most they hold at once,
    /// what is kept after them, and the group's framed sections in the container.
    fn open_steps(&self, kept: u64) -> Result<GroupSteps, Report<MaterializedSnapshotError>> {
        let too_large = || Report::new(MaterializedSnapshotError::Admission);
        let charge = ColumnCharge::of(self.payload, self.rows, self.fields)?;
        let encoded = charge.encoded.min(self.section_limit);
        let identities = kept
            .checked_add(identity_job_bytes(self.identity_limit)?)
            .ok_or_else(too_large)?;
        let kept = kept
            .checked_add(self.identity_limit)
            .ok_or_else(too_large)?;
        let columns = kept
            .checked_add(charge.projection)
            .ok_or_else(too_large)?
            .checked_add(encoded)
            .ok_or_else(too_large)?;
        let kept = kept.checked_add(encoded).ok_or_else(too_large)?;
        let section_frames: u64 = SECTION_FRAME_BYTES.arch_into();
        let framed = section_frames
            .checked_mul(2)
            .ok_or_else(too_large)?
            .checked_add(self.identity_limit)
            .ok_or_else(too_large)?
            .checked_add(encoded)
            .ok_or_else(too_large)?;
        Ok(GroupSteps {
            peak: identities.max(columns),
            kept,
            framed,
        })
    }

    /// What encoding the container of every record added so far takes.
    pub(in crate::runtime) fn need(&self) -> Result<u64, Report<MaterializedSnapshotError>> {
        let too_large = || Report::new(MaterializedSnapshotError::Admission);
        let mut peak = self.peak;
        let mut kept = self.kept;
        let mut framed = self.framed;
        if self.rows > 0 {
            let steps = self.open_steps(kept)?;
            peak = peak.max(steps.peak);
            kept = steps.kept;
            framed = framed.checked_add(steps.framed).ok_or_else(too_large)?;
        }
        // The header's encoding keeps at most its bound.
        peak = peak.max(kept.checked_add(self.header_limit).ok_or_else(too_large)?);
        kept = kept.checked_add(self.header_limit).ok_or_else(too_large)?;
        // The container holds the frame, the header and every framed section at once.
        let container_frame: u64 = CONTAINER_FRAME_BYTES.arch_into();
        let container = container_frame
            .checked_add(self.header_limit)
            .ok_or_else(too_large)?
            .checked_add(framed)
            .ok_or_else(too_large)?;
        Ok(peak.max(kept.checked_add(container).ok_or_else(too_large)?))
    }
}

/// What one group's encoding steps hold: the most at once, what is kept after them, and the
/// group's framed sections in the container.
struct GroupSteps {
    peak: u64,
    kept: u64,
    framed: u64,
}
