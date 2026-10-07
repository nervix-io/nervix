//! Published branch-local window state and its sealed snapshot codec.
//!
//! Layer: data plane.
//! - **Owns.** Immutable window generations, bounded Arrow and typed snapshot sections, and
//!   restoration against a concrete branch lifetime.
//! - **Depends on.** Window accumulator plans, Arrow row views, the bulk executor, and runtime
//!   state placement.
//! - **Must not know.** NSPL text, connector protocols, control-plane transactions, or ACK state.

use std::io::Write as _;

use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use nervix_checkpoint_replication::CheckpointReplication;
use nervix_execution::{BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass};
use nervix_models::Timestamp;
use nervix_primitives::sync::StdArc;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
    WindowAccumulatorPlan, WindowProcessorError, WindowProcessorState,
    branch_checkpoint_catalog::{BranchCheckpointCatalog, CatalogRegistration},
    materialized_snapshot::{
        MaterializedGeneration, MaterializedGenerationRecord, RestoredMaterializedSnapshot,
        SealedSource, decode_aligned_rkyv,
    },
    published_generation::{Generation, PublishedGenerations},
};

/// One row a published window retains as shared Arrow views of its input and arguments.
#[derive(Debug, Clone)]
pub(super) struct WindowEntrySnapshot {
    pub(super) sequence: u64,
    pub(super) timestamp: Timestamp,
    pub(super) key: Option<super::BranchKey>,
    pub(super) record: crate::runtime_schema::RuntimeRow,
    pub(super) arguments: crate::runtime_schema::RuntimeRow,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct LinearHistogramDelayedRemovalSnapshot {
    pub(super) expires_at: Timestamp,
    #[rkyv(with = nervix_models::CountAsU64)]
    pub(super) bucket: usize,
}

/// What one aggregate structure publishes beyond the rows its window retains.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) enum WindowAccumulatorSnapshot {
    /// The structure is rebuilt entirely from the retained rows.
    Retained,
    /// A linear histogram also keeps stepped rows counted until their delay expires.
    LinearHistogram {
        delayed_removals: Vec<LinearHistogramDelayedRemovalSnapshot>,
    },
}

#[derive(Debug, Clone)]
pub(super) struct WindowProcessorStateSnapshot {
    pub(super) entries: Vec<WindowEntrySnapshot>,
    pub(super) next_sequence: u64,
    pub(super) incarnation: Option<u64>,
    pub(super) accumulators: Vec<WindowAccumulatorSnapshot>,
}

/// What one window processor branch keeps beyond the branch task that processes it.
///
/// The live window belongs to that task, which changes it without a lock and publishes it here.
/// Everything outside the task reads the window it last published: the snapshot task persists it,
/// replicas and ownership handoff receive it, and the next task for the same branch restores from
/// it.
#[derive(Debug)]
pub(super) struct ReplicatedWindowProcessorState {
    pub(super) placement: RuntimeStatePlacement,
    /// Absent until the branch task first publishes, which restores as an empty window.
    pub(super) generations: PublishedGenerations<Option<WindowPublishedSnapshot>>,
    /// What each replica reported holding of the published window, and the offer of the newest
    /// published window to the replicas that lack it.
    replication: CheckpointReplication,
    /// The entry of the entity's branch checkpoint catalog that every published window is recorded
    /// in, absent for a state that only encodes or checks a checkpoint.
    catalog: Option<CatalogRegistration>,
}

#[derive(Debug, Clone)]
pub(super) enum WindowPublishedSnapshot {
    Live(WindowProcessorStateSnapshot),
    Sealed(Vec<u8>),
}

const WINDOW_SNAPSHOT_MAGIC: [u8; 8] = *b"NVXWIN64";
const WINDOW_SNAPSHOT_FRAME_BYTES: usize = WINDOW_SNAPSHOT_MAGIC.len() + 4;

#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(super) enum WindowSnapshotError {
    #[error("failed to encode the {section:?} window snapshot section")]
    Encode { section: WindowSnapshotSection },
    #[error("failed to decode the {section:?} window snapshot section")]
    Decode { section: WindowSnapshotSection },
    #[error("window snapshot is invalid: {issue}")]
    Invalid { issue: WindowSnapshotIssue },
    #[error("window snapshot exceeded the bulk memory allowance")]
    Admission,
    #[error("window snapshot bulk execution failed")]
    Execution,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum WindowSnapshotSection {
    Header,
    Input,
    Arguments,
    DelayedRemovals,
    Container,
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(super) enum WindowSnapshotIssue {
    #[error("the sealed header is invalid or truncated; recreate the stored window state")]
    Header,
    #[error("the header exceeds its bulk limit")]
    HeaderLimit,
    #[error("a declared length is unaddressable or overflows")]
    Length,
    #[error("the declared input or argument section is truncated")]
    ArrowSection,
    #[error("a typed section length is truncated")]
    TypedLength,
    #[error("a typed section exceeds its bulk limit")]
    TypedLimit,
    #[error("a typed section is truncated")]
    TypedSection,
    #[error("a typed section names no histogram demand")]
    TypedDemand,
    #[error("typed sections disagree with the declared removal count")]
    RemovalCount,
    #[error("the container carries undeclared typed sections")]
    TrailingSections,
    #[error("the Arrow sections disagree about their generation or row count")]
    Generation,
    #[error("an aggregate argument carries a branch identity")]
    ArgumentBranch,
    #[error("a retained row sequence is missing or overflows")]
    Sequence,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct WindowSnapshotHeader {
    revision: u64,
    first_sequence: Option<u64>,
    next_sequence: u64,
    incarnation: Option<u64>,
    rows: u64,
    input_bytes: u64,
    argument_bytes: u64,
    accumulators: Vec<WindowAccumulatorDescriptor>,
    typed_sections: u32,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
enum WindowAccumulatorDescriptor {
    Retained,
    LinearHistogram { delayed_removals: u64 },
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct WindowDelayedRemovalSection {
    demand: u32,
    removals: Vec<LinearHistogramDelayedRemovalSnapshot>,
}

pub(super) async fn encode_window_processor_snapshot(
    snapshot: &WindowProcessorStateSnapshot,
    revision: u64,
    executor: &Executor,
) -> Result<Vec<u8>, Report<WindowSnapshotError>> {
    let (input_schema, argument_schema) = match snapshot.entries.first() {
        Some(entry) => (
            entry.record.one_row_batch().schema(),
            entry.arguments.one_row_batch().schema(),
        ),
        None => (
            StdArc::new(ArrowSchema::empty()),
            StdArc::new(ArrowSchema::empty()),
        ),
    };
    let input_records = snapshot
        .entries
        .iter()
        .map(|entry| MaterializedGenerationRecord {
            branch: entry.key.clone(),
            row: entry.record.clone(),
        })
        .collect();
    let argument_records = snapshot
        .entries
        .iter()
        .map(|entry| MaterializedGenerationRecord {
            branch: None,
            row: entry.arguments.clone(),
        })
        .collect();
    // An empty forced-recovery checkpoint has no concrete branch lifetime. The nested Arrow
    // container requires a numeric generation, but the outer `None` makes restore return before
    // those empty sections are opened; only the outer typed incarnation selects behavior.
    let branch_generation = snapshot.incarnation.unwrap_or_default();
    let input =
        MaterializedGeneration::new(revision, 0, branch_generation, input_schema, input_records)
            .encode_resident_container(executor)
            .await
            .change_context(WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Input,
            })?;
    let arguments = MaterializedGeneration::new(
        revision,
        0,
        branch_generation,
        argument_schema,
        argument_records,
    )
    .encode_resident_container(executor)
    .await
    .change_context(WindowSnapshotError::Encode {
        section: WindowSnapshotSection::Arguments,
    })?;
    let typed_limit = executor.limits().snapshot_record_bytes.as_u64();
    let chunk_rows = usize::try_from((typed_limit / 64).max(1)).map_err(|error| {
        Report::new(WindowSnapshotError::Encode {
            section: WindowSnapshotSection::DelayedRemovals,
        })
        .attach_printable(error)
    })?;
    let mut accumulators = Vec::with_capacity(snapshot.accumulators.len());
    let mut typed_sections = Vec::new();
    for (demand, accumulator) in snapshot.accumulators.iter().enumerate() {
        nervix_primitives::task::consume_budget().await;
        match accumulator {
            WindowAccumulatorSnapshot::Retained => {
                accumulators.push(WindowAccumulatorDescriptor::Retained);
            }
            WindowAccumulatorSnapshot::LinearHistogram { delayed_removals } => {
                accumulators.push(WindowAccumulatorDescriptor::LinearHistogram {
                    delayed_removals: u64::try_from(delayed_removals.len()).map_err(|error| {
                        Report::new(WindowSnapshotError::Encode {
                            section: WindowSnapshotSection::DelayedRemovals,
                        })
                        .attach_printable(error)
                    })?,
                });
                let demand = u32::try_from(demand).map_err(|error| {
                    Report::new(WindowSnapshotError::Encode {
                        section: WindowSnapshotSection::DelayedRemovals,
                    })
                    .attach_printable(error)
                })?;
                for removals in delayed_removals.chunks(chunk_rows) {
                    nervix_primitives::task::consume_budget().await;
                    let section = WindowDelayedRemovalSection {
                        demand,
                        removals: removals.to_vec(),
                    };
                    let bytes =
                        rkyv::to_bytes::<rkyv::rancor::Error>(&section).map_err(|error| {
                            Report::new(WindowSnapshotError::Encode {
                                section: WindowSnapshotSection::DelayedRemovals,
                            })
                            .attach_printable(error)
                        })?;
                    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > typed_limit {
                        return Err(Report::new(WindowSnapshotError::Invalid {
                            issue: WindowSnapshotIssue::TypedLimit,
                        }));
                    }
                    let bytes = executor
                        .try_charge_owned(MemoryClass::Bulk, bytes.to_vec())
                        .change_context(WindowSnapshotError::Admission)?;
                    typed_sections.push(bytes);
                }
            }
        }
    }
    let header = WindowSnapshotHeader {
        revision,
        first_sequence: snapshot.entries.first().map(|entry| entry.sequence),
        next_sequence: snapshot.next_sequence,
        incarnation: snapshot.incarnation,
        rows: u64::try_from(snapshot.entries.len()).change_context(
            WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Header,
            },
        )?,
        input_bytes: u64::try_from(input.len()).change_context(WindowSnapshotError::Encode {
            section: WindowSnapshotSection::Header,
        })?,
        argument_bytes: u64::try_from(arguments.len()).change_context(
            WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Header,
            },
        )?,
        accumulators,
        typed_sections: u32::try_from(typed_sections.len()).change_context(
            WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Header,
            },
        )?,
    };
    let header = rkyv::to_bytes::<rkyv::rancor::Error>(&header).change_context(
        WindowSnapshotError::Encode {
            section: WindowSnapshotSection::Header,
        },
    )?;
    let header_limit = executor.limits().snapshot_header_bytes.as_u64();
    if u64::try_from(header.len()).unwrap_or(u64::MAX) > header_limit {
        return Err(Report::new(WindowSnapshotError::Invalid {
            issue: WindowSnapshotIssue::HeaderLimit,
        }));
    }
    let header_length =
        u32::try_from(header.len()).change_context(WindowSnapshotError::Encode {
            section: WindowSnapshotSection::Header,
        })?;
    let length_error = || {
        Report::new(WindowSnapshotError::Invalid {
            issue: WindowSnapshotIssue::Length,
        })
    };
    let mut length = WINDOW_SNAPSHOT_FRAME_BYTES
        .checked_add(header.len())
        .ok_or_else(length_error)?;
    length = length.checked_add(input.len()).ok_or_else(length_error)?;
    length = length
        .checked_add(arguments.len())
        .ok_or_else(length_error)?;
    for section in &typed_sections {
        let framed = length.checked_add(4).ok_or_else(length_error)?;
        length = framed.checked_add(section.len()).ok_or_else(length_error)?;
    }
    let length_u64 = u64::try_from(length).change_context(WindowSnapshotError::Encode {
        section: WindowSnapshotSection::Container,
    })?;
    let reservation = executor
        .try_reserve(MemoryClass::Bulk, length_u64)
        .change_context(WindowSnapshotError::Admission)?;
    let sealed = executor
        .run_cpu(CpuClass::Bulk, reservation, move |charge, cancellation| {
            cancellation
                .check()
                .change_context(WindowSnapshotError::Execution)?;
            let mut buffer = BudgetedBuffer::with_limit(charge, length_u64);
            buffer.write_all(&WINDOW_SNAPSHOT_MAGIC).change_context(
                WindowSnapshotError::Encode {
                    section: WindowSnapshotSection::Container,
                },
            )?;
            buffer
                .write_all(&header_length.to_le_bytes())
                .change_context(WindowSnapshotError::Encode {
                    section: WindowSnapshotSection::Container,
                })?;
            buffer
                .write_all(&header)
                .change_context(WindowSnapshotError::Encode {
                    section: WindowSnapshotSection::Header,
                })?;
            buffer
                .write_all(input.as_ref())
                .change_context(WindowSnapshotError::Encode {
                    section: WindowSnapshotSection::Input,
                })?;
            buffer
                .write_all(arguments.as_ref())
                .change_context(WindowSnapshotError::Encode {
                    section: WindowSnapshotSection::Arguments,
                })?;
            for section in typed_sections {
                cancellation
                    .check()
                    .change_context(WindowSnapshotError::Execution)?;
                let section_length =
                    u32::try_from(section.len()).change_context(WindowSnapshotError::Encode {
                        section: WindowSnapshotSection::DelayedRemovals,
                    })?;
                buffer
                    .write_all(&section_length.to_le_bytes())
                    .change_context(WindowSnapshotError::Encode {
                        section: WindowSnapshotSection::DelayedRemovals,
                    })?;
                buffer
                    .write_all(section.as_ref())
                    .change_context(WindowSnapshotError::Encode {
                        section: WindowSnapshotSection::DelayedRemovals,
                    })?;
            }
            Ok::<_, Report<WindowSnapshotError>>(ChargedBytes::from_buffer(buffer))
        })
        .await
        .change_context(WindowSnapshotError::Execution)??;
    Ok(sealed.as_ref().to_vec())
}

/// Which branch lifetime a decoded window snapshot must belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WindowSnapshotLifetime {
    /// The lifetime of the branch restoring the window. A snapshot of another lifetime decodes as
    /// nothing, without its Arrow sections being opened.
    Branch(u64),
    /// Whatever lifetime the snapshot names, as a backup reads a stored window.
    Recorded,
}

/// The schemas a window snapshot's two Arrow sections are laid out by.
#[derive(Debug, Clone, Copy)]
pub(super) struct WindowSnapshotSchemas<'a> {
    /// The window's input relay schema.
    pub(super) input: &'a StdArc<ArrowSchema>,
    /// The argument columns of every aggregate demand, as `WindowArgumentColumns` saves them.
    pub(super) arguments: &'a StdArc<ArrowSchema>,
}

pub(super) async fn decode_window_processor_snapshot(
    payload: &[u8],
    executor: &Executor,
    schemas: WindowSnapshotSchemas<'_>,
    lifetime: WindowSnapshotLifetime,
) -> Result<Option<WindowProcessorStateSnapshot>, Report<WindowSnapshotError>> {
    let invalid = |issue| Report::new(WindowSnapshotError::Invalid { issue });
    if payload.len() < WINDOW_SNAPSHOT_FRAME_BYTES
        || payload[..WINDOW_SNAPSHOT_MAGIC.len()] != WINDOW_SNAPSHOT_MAGIC
    {
        return Err(invalid(WindowSnapshotIssue::Header));
    }
    let header_length = u32::from_le_bytes(
        payload[WINDOW_SNAPSHOT_MAGIC.len()..WINDOW_SNAPSHOT_FRAME_BYTES]
            .try_into()
            .map_err(|_| invalid(WindowSnapshotIssue::Header))?,
    );
    let header_length =
        usize::try_from(header_length).map_err(|_| invalid(WindowSnapshotIssue::Length))?;
    if u64::try_from(header_length).unwrap_or(u64::MAX)
        > executor.limits().snapshot_header_bytes.as_u64()
    {
        return Err(invalid(WindowSnapshotIssue::HeaderLimit));
    }
    let header_end = WINDOW_SNAPSHOT_FRAME_BYTES
        .checked_add(header_length)
        .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
    let header_bytes = payload
        .get(WINDOW_SNAPSHOT_FRAME_BYTES..header_end)
        .ok_or_else(|| invalid(WindowSnapshotIssue::Header))?;
    let header = decode_aligned_rkyv::<WindowSnapshotHeader>(header_bytes).change_context(
        WindowSnapshotError::Decode {
            section: WindowSnapshotSection::Header,
        },
    )?;
    match lifetime {
        WindowSnapshotLifetime::Branch(expected) => {
            if header.incarnation != Some(expected) {
                return Ok(None);
            }
        }
        WindowSnapshotLifetime::Recorded => {
            if header.incarnation.is_none() {
                return Ok(None);
            }
        }
    }
    let input_end = header_end
        .checked_add(
            usize::try_from(header.input_bytes)
                .map_err(|_| invalid(WindowSnapshotIssue::Length))?,
        )
        .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
    let argument_end = input_end
        .checked_add(
            usize::try_from(header.argument_bytes)
                .map_err(|_| invalid(WindowSnapshotIssue::Length))?,
        )
        .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
    if argument_end > payload.len() {
        return Err(invalid(WindowSnapshotIssue::ArrowSection));
    }
    let mut delayed_removals = (0..header.accumulators.len())
        .map(|_| Vec::new())
        .collect::<Vec<Vec<LinearHistogramDelayedRemovalSnapshot>>>();
    let mut offset = argument_end;
    let typed_limit = executor.limits().snapshot_record_bytes.as_u64();
    for _ in 0..header.typed_sections {
        nervix_primitives::task::consume_budget().await;
        let length_end = offset
            .checked_add(4)
            .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
        let length_bytes: [u8; 4] = payload
            .get(offset..length_end)
            .ok_or_else(|| invalid(WindowSnapshotIssue::TypedLength))?
            .try_into()
            .map_err(|_| invalid(WindowSnapshotIssue::TypedLength))?;
        let length = usize::try_from(u32::from_le_bytes(length_bytes))
            .map_err(|_| invalid(WindowSnapshotIssue::Length))?;
        if u64::try_from(length).unwrap_or(u64::MAX) > typed_limit {
            return Err(invalid(WindowSnapshotIssue::TypedLimit));
        }
        let section_end = length_end
            .checked_add(length)
            .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
        let section_bytes = payload
            .get(length_end..section_end)
            .ok_or_else(|| invalid(WindowSnapshotIssue::TypedSection))?;
        let section = decode_aligned_rkyv::<WindowDelayedRemovalSection>(section_bytes)
            .change_context(WindowSnapshotError::Decode {
                section: WindowSnapshotSection::DelayedRemovals,
            })?;
        let demand = usize::try_from(section.demand)
            .map_err(|_| invalid(WindowSnapshotIssue::TypedDemand))?;
        let descriptor = header
            .accumulators
            .get(demand)
            .ok_or_else(|| invalid(WindowSnapshotIssue::TypedDemand))?;
        let WindowAccumulatorDescriptor::LinearHistogram {
            delayed_removals: declared,
        } = descriptor
        else {
            return Err(invalid(WindowSnapshotIssue::TypedDemand));
        };
        let removals = &mut delayed_removals[demand];
        let next = removals
            .len()
            .checked_add(section.removals.len())
            .ok_or_else(|| invalid(WindowSnapshotIssue::RemovalCount))?;
        if u64::try_from(next).unwrap_or(u64::MAX) > *declared {
            return Err(invalid(WindowSnapshotIssue::RemovalCount));
        }
        removals.extend(section.removals);
        offset = section_end;
    }
    if offset != payload.len() {
        return Err(invalid(WindowSnapshotIssue::TrailingSections));
    }
    let mut accumulators = Vec::with_capacity(header.accumulators.len());
    for (demand, descriptor) in header.accumulators.iter().enumerate() {
        match descriptor {
            WindowAccumulatorDescriptor::Retained => {
                accumulators.push(WindowAccumulatorSnapshot::Retained);
            }
            WindowAccumulatorDescriptor::LinearHistogram {
                delayed_removals: declared,
            } => {
                let removals = std::mem::take(&mut delayed_removals[demand]);
                if u64::try_from(removals.len()).unwrap_or(u64::MAX) != *declared {
                    return Err(invalid(WindowSnapshotIssue::RemovalCount));
                }
                accumulators.push(WindowAccumulatorSnapshot::LinearHistogram {
                    delayed_removals: removals,
                });
            }
        }
    }
    let input = payload
        .get(header_end..input_end)
        .ok_or_else(|| invalid(WindowSnapshotIssue::ArrowSection))?;
    let arguments = payload
        .get(input_end..argument_end)
        .ok_or_else(|| invalid(WindowSnapshotIssue::ArrowSection))?;
    let input = RestoredMaterializedSnapshot::open(
        executor,
        schemas.input,
        SealedSource::borrowed(executor, input),
    )
    .await
    .change_context(WindowSnapshotError::Decode {
        section: WindowSnapshotSection::Input,
    })?;
    let arguments = RestoredMaterializedSnapshot::open(
        executor,
        schemas.arguments,
        SealedSource::borrowed(executor, arguments),
    )
    .await
    .change_context(WindowSnapshotError::Decode {
        section: WindowSnapshotSection::Arguments,
    })?;
    if input.revision != header.revision
        || arguments.revision != header.revision
        || input.branch_generation != header.incarnation.unwrap_or_default()
        || arguments.branch_generation != header.incarnation.unwrap_or_default()
        || input.records.len() != arguments.records.len()
        || u64::try_from(input.records.len()).unwrap_or(u64::MAX) != header.rows
    {
        return Err(invalid(WindowSnapshotIssue::Generation));
    }
    let mut entries = Vec::with_capacity(input.records.len());
    for (index, (input, arguments)) in input.records.into_iter().zip(arguments.records).enumerate()
    {
        nervix_primitives::task::consume_budget().await;
        if arguments.branch.is_some() {
            return Err(invalid(WindowSnapshotIssue::ArgumentBranch));
        }
        let index = u64::try_from(index).map_err(|_| invalid(WindowSnapshotIssue::Length))?;
        let first = header
            .first_sequence
            .ok_or_else(|| invalid(WindowSnapshotIssue::Sequence))?;
        let sequence = first
            .checked_add(index)
            .ok_or_else(|| invalid(WindowSnapshotIssue::Sequence))?;
        entries.push(WindowEntrySnapshot {
            sequence,
            timestamp: input.row.metadata().ingested_at_low_watermark(),
            key: input.branch,
            record: input.row,
            arguments: arguments.row,
        });
    }
    if entries.is_empty() != header.first_sequence.is_none() {
        return Err(invalid(WindowSnapshotIssue::Sequence));
    }
    Ok(Some(WindowProcessorStateSnapshot {
        entries,
        next_sequence: header.next_sequence,
        incarnation: header.incarnation,
        accumulators,
    }))
}

impl ReplicatedWindowProcessorState {
    /// The window state of `placement`, publishing `initial` as its sealed snapshot. The snapshot
    /// is opened only when a branch restores its window from it.
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Self {
        let generations = match initial {
            Some(initial) => PublishedGenerations::restored(
                initial.lsm,
                Some(WindowPublishedSnapshot::Sealed(initial.payload)),
            ),
            None => PublishedGenerations::restored(0, None),
        };
        Self {
            placement,
            generations,
            replication: CheckpointReplication::new(),
            catalog: None,
        }
    }

    /// This state as the state of a branch this node owns, with every window it publishes recorded
    /// in `catalog`, so the entity's replicas learn of it.
    pub(super) fn cataloged(mut self, catalog: &BranchCheckpointCatalog) -> Self {
        let revision = self.generations.load().revision;
        self.catalog = Some(catalog.register(
            self.placement.branch_key.clone(),
            self.placement.state,
            revision,
        ));
        self
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.replication
    }

    /// Build the live window a branch task owns from the window published last.
    pub(super) async fn restore_state(
        &self,
        plan: &WindowAccumulatorPlan,
        input_schema: &crate::runtime_schema::CompiledSchema,
        incarnation: u64,
        executor: &Executor,
    ) -> error_stack::Result<WindowProcessorState, WindowProcessorError> {
        let published = self.generations.load();
        let Some(snapshot) = &published.value else {
            return Ok(WindowProcessorState::new(plan, incarnation));
        };
        let snapshot = match snapshot {
            WindowPublishedSnapshot::Live(snapshot) => snapshot.clone(),
            WindowPublishedSnapshot::Sealed(payload) => {
                let arguments = super::WindowArgumentColumns::snapshot_schema(plan);
                let decoded = decode_window_processor_snapshot(
                    payload,
                    executor,
                    WindowSnapshotSchemas {
                        input: &input_schema.arrow_schema(),
                        arguments: &arguments,
                    },
                    WindowSnapshotLifetime::Branch(incarnation),
                )
                .await
                .map_err(|error| {
                    Report::new(WindowProcessorError::RestoreSnapshotEntry).attach_printable(error)
                })?;
                let Some(decoded) = decoded else {
                    self.generations.mark_live_dirty();
                    return Ok(WindowProcessorState::new(plan, incarnation));
                };
                decoded
            }
        };
        if snapshot.incarnation != Some(incarnation) {
            self.generations.mark_live_dirty();
            return Ok(WindowProcessorState::new(plan, incarnation));
        }
        if snapshot
            .entries
            .iter()
            .any(|entry| entry.key != self.placement.branch_key)
        {
            return Err(Report::new(WindowProcessorError::SnapshotBranchKey));
        }
        WindowProcessorState::from_snapshot(plan, input_schema, &snapshot)
    }

    /// Publish the owning branch task's live window as the window everything else reads.
    pub(super) fn replace_state(
        &self,
        state: &WindowProcessorState,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let snapshot = state
            .to_snapshot()
            .change_context(RuntimePersistenceError::WindowSnapshot)?;
        let revision = self
            .generations
            .publish(Some(WindowPublishedSnapshot::Live(snapshot)));
        if let Some(catalog) = &self.catalog {
            catalog.record(revision);
        }
        Ok(())
    }

    /// Encode the window published last, stamped with the revision it stands at.
    pub(super) async fn latest_snapshot(
        &self,
        executor: &Executor,
    ) -> Result<PersistedRuntimeStateEntry, Report<RuntimePersistenceError>> {
        let published = self.generations.load();
        self.snapshot_of(&published, executor).await
    }

    /// Encode the window published last when its revision is after `after_lsm`. A requester that
    /// already holds that revision costs no encode.
    pub(super) async fn snapshot_after(
        &self,
        after_lsm: Option<u64>,
        executor: &Executor,
    ) -> Result<Option<PersistedRuntimeStateEntry>, Report<RuntimePersistenceError>> {
        let Some(published) = self.generations.load_after(after_lsm) else {
            return Ok(None);
        };
        Ok(Some(self.snapshot_of(&published, executor).await?))
    }

    async fn snapshot_of(
        &self,
        published: &Generation<Option<WindowPublishedSnapshot>>,
        executor: &Executor,
    ) -> Result<PersistedRuntimeStateEntry, Report<RuntimePersistenceError>> {
        let payload = match &published.value {
            Some(WindowPublishedSnapshot::Live(snapshot)) => {
                encode_window_processor_snapshot(snapshot, published.revision, executor)
                    .await
                    .change_context(RuntimePersistenceError::WindowSnapshot)?
            }
            Some(WindowPublishedSnapshot::Sealed(payload)) => payload.clone(),
            None => encode_window_processor_snapshot(
                &WindowProcessorStateSnapshot {
                    entries: Vec::new(),
                    next_sequence: 0,
                    incarnation: None,
                    accumulators: Vec::new(),
                },
                published.revision,
                executor,
            )
            .await
            .change_context(RuntimePersistenceError::WindowSnapshot)?,
        };
        Ok(PersistedRuntimeStateEntry {
            lsm: published.revision,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use nervix_models::ParseAsType;
    use nervix_primitives::sync::Arc;

    use super::*;
    use crate::{
        runtime::{WindowArgumentColumns, test_schema, window_plan},
        runtime_schema::{
            CompiledSchema, RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, RuntimeValue,
        },
    };

    /// Decodes `payload` as the branch lifetime `incarnation` of a window compiled from `plan`.
    async fn decode_for_branch(
        payload: &[u8],
        executor: &Executor,
        plan: &WindowAccumulatorPlan,
        input_schema: &CompiledSchema,
        incarnation: u64,
    ) -> Result<Option<WindowProcessorStateSnapshot>, Report<WindowSnapshotError>> {
        let arguments = WindowArgumentColumns::snapshot_schema(plan);
        decode_window_processor_snapshot(
            payload,
            executor,
            WindowSnapshotSchemas {
                input: &input_schema.arrow_schema(),
                arguments: &arguments,
            },
            WindowSnapshotLifetime::Branch(incarnation),
        )
        .await
    }

    #[test]
    fn bolero_window_archive_counts_round_trip() {
        use meticulous::ResultExt as _;
        use nervix_arbitrary::Entropy;

        bolero::check!()
            .with_iterations(256)
            .with_max_len(32)
            .for_each(|bytes: &[u8]| {
                let mut entropy = Entropy::new(bytes);
                let generated = entropy.any_u64();
                let expires_at = Timestamp::from_unix_nanos(entropy.any_i64());
                for bucket in [0, u64::from(u32::MAX) + 1, u64::MAX, generated] {
                    let bucket = usize::try_from(bucket)
                        .assured("the native test and fuzz targets address 64 bits");
                    let value = LinearHistogramDelayedRemovalSnapshot { expires_at, bucket };
                    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&value)
                        .assured("the bounded current typed snapshot section encodes");
                    let decoded = rkyv::from_bytes::<
                        LinearHistogramDelayedRemovalSnapshot,
                        rkyv::rancor::Error,
                    >(&encoded)
                    .assured("a current typed snapshot section reads back its own encoding");
                    assert_eq!(decoded, value);
                }
            });
    }

    #[nervix_primitives::test]
    async fn window_snapshot_seals_and_restores_shared_arrow_columns() {
        let plan = window_plan(
            "SET count = COUNT(input.latency)",
            ParseAsType::I64,
            &[("count", ParseAsType::I64)],
        );
        let input_schema = test_schema(&[("latency", ParseAsType::I64)]);
        let at = Timestamp::from_unix_nanos(42);
        let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(at, at);
        let record = crate::runtime_schema::test_runtime_row([(
            "latency".to_string(),
            RuntimeValue::I64(10),
        )])
        .with_metadata(metadata.clone());
        let argument_schema = WindowArgumentColumns::snapshot_schema(&plan);
        let argument_array: ArrayRef = StdArc::new(Int64Array::from(vec![Some(10)]));
        let argument_batch = RecordBatch::try_new(argument_schema.clone(), vec![argument_array])
            .expect("the argument batch matches its plan");
        let argument_batch = RuntimeRecordBatch::from_record_batch(argument_schema, argument_batch)
            .expect("the argument batch has its declared schema");
        let arguments = RuntimeRow::new(Arc::new(argument_batch), 0, metadata)
            .expect("the argument batch has one row");
        let snapshot = WindowProcessorStateSnapshot {
            entries: vec![WindowEntrySnapshot {
                sequence: 5,
                timestamp: at,
                key: None,
                record,
                arguments,
            }],
            next_sequence: 6,
            incarnation: Some(7),
            accumulators: vec![WindowAccumulatorSnapshot::Retained],
        };
        let executor = Executor::default();
        let payload = encode_window_processor_snapshot(&snapshot, 3, &executor)
            .await
            .expect("the current snapshot should seal");
        let restored = decode_for_branch(&payload, &executor, &plan, &input_schema, 7)
            .await
            .expect("the current snapshot should open")
            .expect("the incarnation matches");
        assert_eq!(restored.entries.len(), 1);
        assert_eq!(restored.entries[0].sequence, 5);
        assert_eq!(restored.entries[0].timestamp, at);
        assert_eq!(
            restored.entries[0]
                .record
                .value("latency")
                .expect("the record column should be readable"),
            Some(RuntimeValue::I64(10))
        );
        assert_eq!(
            restored.entries[0]
                .arguments
                .value("argument_0")
                .expect("the argument column should be readable"),
            Some(RuntimeValue::I64(10))
        );
        let live = WindowProcessorState::from_snapshot(&plan, &input_schema, &restored)
            .expect("the restored columns should rebuild the accumulator");
        assert_eq!(live.entries.len(), 1);
        assert_eq!(live.incarnation, 7);

        assert!(
            decode_for_branch(&payload, &executor, &plan, &input_schema, 8)
                .await
                .expect("the sealed header is valid")
                .is_none(),
            "a different branch lifetime must not decode the old Arrow sections"
        );
        let mut malformed = payload;
        malformed[0] = b'X';
        let Err(error) = decode_for_branch(&malformed, &executor, &plan, &input_schema, 7).await
        else {
            panic!("a damaged current snapshot header must fail before section decoding");
        };
        assert!(matches!(
            error.current_context(),
            WindowSnapshotError::Invalid {
                issue: WindowSnapshotIssue::Header
            }
        ));
        assert!(
            error
                .to_string()
                .contains("recreate the stored window state")
        );
    }

    #[nervix_primitives::test]
    async fn empty_window_snapshot_preserves_typed_delayed_removals() {
        let plan = window_plan(
            "SET count = COUNT(input.latency)",
            ParseAsType::I64,
            &[("count", ParseAsType::I64)],
        );
        let input_schema = test_schema(&[("latency", ParseAsType::I64)]);
        let removal = LinearHistogramDelayedRemovalSnapshot {
            expires_at: Timestamp::from_unix_nanos(500),
            bucket: 3,
        };
        let snapshot = WindowProcessorStateSnapshot {
            entries: Vec::new(),
            next_sequence: 12,
            incarnation: Some(9),
            accumulators: vec![WindowAccumulatorSnapshot::LinearHistogram {
                delayed_removals: vec![removal],
            }],
        };
        let executor = Executor::default();
        let payload = encode_window_processor_snapshot(&snapshot, 4, &executor)
            .await
            .expect("an empty window with typed state should seal");
        let restored = decode_for_branch(&payload, &executor, &plan, &input_schema, 9)
            .await
            .expect("the typed section should open")
            .expect("the branch lifetime matches");
        assert!(restored.entries.is_empty());
        assert_eq!(restored.next_sequence, 12);
        match &restored.accumulators[0] {
            WindowAccumulatorSnapshot::LinearHistogram { delayed_removals } => {
                assert_eq!(delayed_removals.len(), 1);
                assert_eq!(
                    delayed_removals[0].expires_at,
                    Timestamp::from_unix_nanos(500)
                );
                assert_eq!(delayed_removals[0].bucket, 3);
            }
            WindowAccumulatorSnapshot::Retained => {
                panic!("the typed aggregate state should survive the snapshot")
            }
        }

        let truncated = &payload[..payload.len() - 1];
        assert!(
            decode_for_branch(truncated, &executor, &plan, &input_schema, 9)
                .await
                .is_err(),
            "a truncated typed section must fail to load"
        );
    }

    /// The plan and schemas of a window that counts `input.latency`, shared by the window snapshot
    /// properties.
    struct WindowSnapshotFixture {
        plan: WindowAccumulatorPlan,
        input_schema: Arc<crate::runtime_schema::CompiledSchema>,
        argument_schema: StdArc<ArrowSchema>,
        argument_nullable: bool,
    }

    /// A generated window, the argument value each of its rows holds, and the branch lifetime and
    /// revision it is sealed at.
    struct GeneratedWindow {
        snapshot: WindowProcessorStateSnapshot,
        argument_values: Vec<Option<i64>>,
        incarnation: u64,
        revision: u64,
    }

    impl WindowSnapshotFixture {
        fn new() -> Self {
            let plan = window_plan(
                "SET count = COUNT(input.latency)",
                ParseAsType::I64,
                &[("count", ParseAsType::I64)],
            );
            let input_schema = test_schema(&[("latency", ParseAsType::I64)]);
            let argument_schema = WindowArgumentColumns::snapshot_schema(&plan);
            let argument_nullable = argument_schema.field(0).is_nullable();
            Self {
                plan,
                input_schema,
                argument_schema,
                argument_nullable,
            }
        }

        /// A window of up to three rows with consecutive sequences and typed aggregate state.
        fn generated(&self, arbitrary: &mut nervix_arbitrary::Arbitrary<'_>) -> GeneratedWindow {
            use meticulous::{OptionExt as _, ResultExt as _};

            // A window belongs to one branch, so every row it retains carries the same key.
            let key = crate::runtime::BranchKey::generated_scope(arbitrary);
            let count = arbitrary.entropy().between(0..=3);
            let first = arbitrary.entropy().boundary_biased(0..=u64::MAX - 3);
            let mut entries = Vec::new();
            let mut argument_values = Vec::new();
            for index in 0..count {
                let low = arbitrary.timestamp();
                let high = arbitrary.timestamp();
                let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(low, high);
                let latency = arbitrary.entropy().any_i64();
                let record = crate::runtime_schema::test_runtime_row([(
                    "latency".to_string(),
                    RuntimeValue::I64(latency),
                )])
                .with_metadata(metadata.clone());
                let argument = if self.argument_nullable && arbitrary.entropy().flag() {
                    None
                } else {
                    Some(arbitrary.entropy().any_i64())
                };
                argument_values.push(argument);
                let argument_array: ArrayRef = StdArc::new(Int64Array::from(vec![argument]));
                let argument_batch =
                    RecordBatch::try_new(self.argument_schema.clone(), vec![argument_array])
                        .assured("the argument column matches its plan's schema");
                let argument_batch = RuntimeRecordBatch::from_record_batch(
                    self.argument_schema.clone(),
                    argument_batch,
                )
                .assured("the argument batch has its declared schema");
                let arguments = RuntimeRow::new(Arc::new(argument_batch), 0, metadata)
                    .assured("the argument batch has one row");
                entries.push(WindowEntrySnapshot {
                    sequence: first
                        .checked_add(index)
                        .verified("the first sequence leaves room for three rows"),
                    // A window orders its rows by the time each was ingested.
                    timestamp: low,
                    key: key.clone(),
                    record,
                    arguments,
                });
            }
            let next_sequence = if count == 0 {
                arbitrary.entropy().any_u64()
            } else {
                first
                    .checked_add(count)
                    .verified("the first sequence leaves room for three rows")
            };
            let incarnation = arbitrary.entropy().any_u64();
            let accumulators = arbitrary.records(|arbitrary| {
                if arbitrary.entropy().flag() {
                    return WindowAccumulatorSnapshot::Retained;
                }
                let delayed_removals =
                    arbitrary.records(|arbitrary| LinearHistogramDelayedRemovalSnapshot {
                        expires_at: arbitrary.timestamp(),
                        bucket: usize::try_from(arbitrary.entropy().any_u64())
                            .assured("the native test and fuzz targets address 64 bits"),
                    });
                WindowAccumulatorSnapshot::LinearHistogram { delayed_removals }
            });
            let snapshot = WindowProcessorStateSnapshot {
                entries,
                next_sequence,
                incarnation: Some(incarnation),
                accumulators,
            };
            GeneratedWindow {
                snapshot,
                argument_values,
                incarnation,
                revision: arbitrary.entropy().any_u64(),
            }
        }

        async fn open(
            &self,
            payload: &[u8],
            executor: &Executor,
            incarnation: u64,
        ) -> Result<Option<WindowProcessorStateSnapshot>, Report<WindowSnapshotError>> {
            decode_for_branch(
                payload,
                executor,
                &self.plan,
                &self.input_schema,
                incarnation,
            )
            .await
        }

        /// Asserts that `payload` fails to open with the snapshot's typed failure, opens nothing
        /// for this branch lifetime, or opens a window that seals to a payload which opens and
        /// seals back to the same bytes.
        async fn assert_opens_typed_or_canonically(
            &self,
            payload: &[u8],
            executor: &Executor,
            incarnation: u64,
            revision: u64,
        ) {
            use meticulous::{OptionExt as _, ResultExt as _};

            let restored = match self.open(payload, executor, incarnation).await {
                Ok(Some(restored)) => restored,
                Ok(None) => return,
                Err(report) => {
                    assert!(
                        matches!(
                            report.current_context(),
                            WindowSnapshotError::Decode { .. }
                                | WindowSnapshotError::Invalid { .. }
                        ),
                        "{report:?}"
                    );
                    return;
                }
            };
            let sealed = encode_window_processor_snapshot(&restored, revision, executor)
                .await
                .assured("a window that opened seals again");
            let reopened = self
                .open(&sealed, executor, incarnation)
                .await
                .assured("a sealed window opens")
                .verified("it was sealed for this branch lifetime");
            let resealed = encode_window_processor_snapshot(&reopened, revision, executor)
                .await
                .assured("a reopened window seals again");
            assert_eq!(resealed, sealed);
        }
    }

    /// `payload` damaged once at a generated position: a flipped bit, a cut, appended bytes, or a
    /// four-byte little-endian length overwritten with a boundary value.
    fn damaged(arbitrary: &mut nervix_arbitrary::Arbitrary<'_>, mut payload: Vec<u8>) -> Vec<u8> {
        use meticulous::{OptionExt as _, ResultExt as _};

        let length = u64::try_from(payload.len()).assured("a payload length fits in u64");
        let position = arbitrary.entropy().up_to(length);
        let position = usize::try_from(position).verified("a position at most the payload length");
        match arbitrary.entropy().byte() % 4 {
            0 => {
                let bit = arbitrary.entropy().byte() % 8;
                if let Some(byte) = payload.get_mut(position) {
                    *byte ^= 1_u8 << bit;
                }
            }
            1 => payload.truncate(position),
            2 => {
                let added = arbitrary.entropy().count(16);
                for _ in 0..added {
                    payload.push(arbitrary.entropy().byte());
                }
            }
            _ => {
                let whole =
                    u32::try_from(payload.len()).assured("a sealed test window is far below 4 GiB");
                let value = arbitrary.entropy().pick([0, 1, whole, u32::MAX]);
                for (offset, byte) in value.to_le_bytes().into_iter().enumerate() {
                    let index = position.checked_add(offset).verified(
                        "a position at most a test payload's length is far below usize::MAX",
                    );
                    if let Some(slot) = payload.get_mut(index) {
                        *slot = byte;
                    }
                }
            }
        }
        payload
    }

    /// A sealed window restores every retained row: its sequence, its ingestion time, its branch
    /// bit for bit, its input and argument values with their metadata, and the window's next
    /// sequence, branch lifetime and every aggregate's typed state. A snapshot of another branch
    /// lifetime restores nothing.
    #[test]
    fn bolero_window_snapshots_restore_every_retained_row_and_typed_state() {
        use meticulous::{OptionExt as _, ResultExt as _};
        use nervix_arbitrary::{Arbitrary, Domain};

        let fixture = WindowSnapshotFixture::new();
        bolero::check!()
            .with_iterations(128)
            .with_max_len(2048)
            .for_each(|bytes: &[u8]| {
                let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .assured("a property runtime opens");
                let executor = Executor::default();
                let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
                let generated = fixture.generated(&mut arbitrary);
                runtime.block_on(async {
                    let payload = encode_window_processor_snapshot(
                        &generated.snapshot,
                        generated.revision,
                        &executor,
                    )
                    .await
                    .assured("a bounded generated window seals");
                    let restored = fixture
                        .open(&payload, &executor, generated.incarnation)
                        .await
                        .assured("a sealed window opens")
                        .verified("the branch lifetime is the one it was sealed for");
                    assert_window_snapshots_match(
                        &restored,
                        &generated.snapshot,
                        &generated.argument_values,
                    );
                    let other = fixture
                        .open(&payload, &executor, generated.incarnation ^ 1)
                        .await
                        .assured("a sealed window header opens");
                    assert!(other.is_none(), "another branch lifetime restores nothing");
                });
            });
    }

    /// A sealed window damaged once, and arbitrary bytes behind the snapshot's magic, either fail
    /// to open with the snapshot's typed failure, open nothing for the branch lifetime, or open a
    /// window that seals to a payload which opens and seals back to the same bytes.
    #[test]
    fn bolero_malformed_window_snapshots_fail_typed() {
        use meticulous::ResultExt as _;
        use nervix_arbitrary::{Arbitrary, Domain};

        let fixture = WindowSnapshotFixture::new();
        bolero::check!()
            .with_iterations(256)
            .with_max_len(4096)
            .for_each(|bytes: &[u8]| {
                let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .assured("a property runtime opens");
                let executor = Executor::default();
                let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
                let generated = fixture.generated(&mut arbitrary);
                runtime.block_on(async {
                    let payload = encode_window_processor_snapshot(
                        &generated.snapshot,
                        generated.revision,
                        &executor,
                    )
                    .await
                    .assured("a bounded generated window seals");
                    let payload = damaged(&mut arbitrary, payload);
                    fixture
                        .assert_opens_typed_or_canonically(
                            &payload,
                            &executor,
                            generated.incarnation,
                            generated.revision,
                        )
                        .await;

                    let mut framed = WINDOW_SNAPSHOT_MAGIC.to_vec();
                    framed.extend_from_slice(bytes);
                    fixture
                        .assert_opens_typed_or_canonically(
                            &framed,
                            &executor,
                            generated.incarnation,
                            generated.revision,
                        )
                        .await;
                });
            });
    }

    /// Asserts that `restored` holds exactly the rows, sequences and typed state of `expected`.
    fn assert_window_snapshots_match(
        restored: &WindowProcessorStateSnapshot,
        expected: &WindowProcessorStateSnapshot,
        arguments: &[Option<i64>],
    ) {
        use meticulous::ResultExt as _;

        let key_bits = |key: &Option<crate::runtime::BranchKey>| {
            rkyv::to_bytes::<rkyv::rancor::Error>(&crate::runtime::BranchKey::to_remote_key(key))
                .assured("a remote branch key archives")
                .to_vec()
        };
        assert_eq!(restored.entries.len(), expected.entries.len());
        for ((restored, expected), argument) in restored
            .entries
            .iter()
            .zip(&expected.entries)
            .zip(arguments)
        {
            assert_eq!(restored.sequence, expected.sequence);
            assert_eq!(restored.timestamp, expected.timestamp);
            assert_eq!(key_bits(&restored.key), key_bits(&expected.key));
            assert_eq!(
                restored
                    .record
                    .value("latency")
                    .assured("the restored row reads"),
                expected
                    .record
                    .value("latency")
                    .assured("the generated row reads")
            );
            for (restored, expected) in [
                (restored.record.metadata(), expected.record.metadata()),
                (restored.arguments.metadata(), expected.arguments.metadata()),
            ] {
                assert_eq!(
                    restored.ingested_at_low_watermark(),
                    expected.ingested_at_low_watermark()
                );
                assert_eq!(
                    restored.ingested_at_high_watermark(),
                    expected.ingested_at_high_watermark()
                );
            }
            assert_eq!(
                restored
                    .arguments
                    .value("argument_0")
                    .assured("the restored argument reads"),
                argument.map(RuntimeValue::I64)
            );
        }
        assert_eq!(restored.next_sequence, expected.next_sequence);
        assert_eq!(restored.incarnation, expected.incarnation);
        assert_eq!(restored.accumulators.len(), expected.accumulators.len());
        for (restored, expected) in restored.accumulators.iter().zip(&expected.accumulators) {
            match (restored, expected) {
                (WindowAccumulatorSnapshot::Retained, WindowAccumulatorSnapshot::Retained) => {}
                (
                    WindowAccumulatorSnapshot::LinearHistogram {
                        delayed_removals: restored,
                    },
                    WindowAccumulatorSnapshot::LinearHistogram {
                        delayed_removals: expected,
                    },
                ) => assert_eq!(restored, expected),
                (restored, expected) => panic!("{restored:?} restored as {expected:?}"),
            }
        }
    }
}
