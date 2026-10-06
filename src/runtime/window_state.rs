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
use meticulous::OptionExt as _;
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
        RestoredRows, SealedContainerPieces, SealedSource, decode_aligned_rkyv,
    },
    published_generation::{Generation, PublishedGenerations},
    snapshot_staging::{SnapshotStaging, StagedArtifact, StagedPieces},
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

/// The input and argument generations of consecutive retained rows, nested in a checkpoint as two
/// sealed containers of the same row order.
struct WindowGenerations {
    input: MaterializedGeneration,
    arguments: MaterializedGeneration,
}

impl WindowGenerations {
    /// The rows `entries` hold as the nested generations of a checkpoint at `revision`, numbered by
    /// `branch_generation`. Rows of no window lay out empty schemas.
    fn of(entries: &[WindowEntrySnapshot], revision: u64, branch_generation: u64) -> Self {
        let (input_schema, argument_schema) = match entries.first() {
            Some(entry) => (
                entry.record.one_row_batch().schema(),
                entry.arguments.one_row_batch().schema(),
            ),
            None => (
                StdArc::new(ArrowSchema::empty()),
                StdArc::new(ArrowSchema::empty()),
            ),
        };
        let mut input_records = Vec::with_capacity(entries.len());
        let mut argument_records = Vec::with_capacity(entries.len());
        for entry in entries {
            input_records.push(MaterializedGenerationRecord {
                branch: entry.key.clone(),
                row: entry.record.clone(),
            });
            argument_records.push(MaterializedGenerationRecord {
                branch: None,
                row: entry.arguments.clone(),
            });
        }
        Self {
            input: MaterializedGeneration::new(
                revision,
                0,
                branch_generation,
                input_schema,
                input_records,
            ),
            arguments: MaterializedGeneration::new(
                revision,
                0,
                branch_generation,
                argument_schema,
                argument_records,
            ),
        }
    }
}

/// What every aggregate keeps beyond the retained rows: its descriptor in the header, and the
/// bounded typed sections that carry each linear histogram's delayed removals.
struct WindowTypedSections {
    descriptors: Vec<WindowAccumulatorDescriptor>,
    sections: Vec<ChargedBytes>,
}

impl WindowTypedSections {
    async fn encode(
        accumulators: &[WindowAccumulatorSnapshot],
        executor: &Executor,
    ) -> Result<Self, Report<WindowSnapshotError>> {
        let typed_limit = executor.limits().snapshot_record_bytes.as_u64();
        let chunk_rows = usize::try_from((typed_limit / 64).max(1)).map_err(|error| {
            Report::new(WindowSnapshotError::Encode {
                section: WindowSnapshotSection::DelayedRemovals,
            })
            .attach_printable(error)
        })?;
        let mut descriptors = Vec::with_capacity(accumulators.len());
        let mut sections = Vec::new();
        for (demand, accumulator) in accumulators.iter().enumerate() {
            nervix_primitives::task::consume_budget().await;
            match accumulator {
                WindowAccumulatorSnapshot::Retained => {
                    descriptors.push(WindowAccumulatorDescriptor::Retained);
                }
                WindowAccumulatorSnapshot::LinearHistogram { delayed_removals } => {
                    descriptors.push(WindowAccumulatorDescriptor::LinearHistogram {
                        delayed_removals: u64::try_from(delayed_removals.len()).map_err(
                            |error| {
                                Report::new(WindowSnapshotError::Encode {
                                    section: WindowSnapshotSection::DelayedRemovals,
                                })
                                .attach_printable(error)
                            },
                        )?,
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
                        sections.push(bytes);
                    }
                }
            }
        }
        Ok(Self {
            descriptors,
            sections,
        })
    }

    fn count(&self) -> Result<u32, Report<WindowSnapshotError>> {
        u32::try_from(self.sections.len()).change_context(WindowSnapshotError::Encode {
            section: WindowSnapshotSection::Header,
        })
    }
}

impl WindowSnapshotHeader {
    /// The header's encoding, refused beyond the node's snapshot header limit.
    fn encode(&self, executor: &Executor) -> Result<Vec<u8>, Report<WindowSnapshotError>> {
        let header = rkyv::to_bytes::<rkyv::rancor::Error>(self).change_context(
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
        Ok(header.to_vec())
    }

    /// The magic, the header's length and the header that open every window checkpoint.
    fn framed(&self, executor: &Executor) -> Result<Vec<u8>, Report<WindowSnapshotError>> {
        let header = self.encode(executor)?;
        let header_length =
            u32::try_from(header.len()).change_context(WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Header,
            })?;
        let mut framed = Vec::with_capacity(
            WINDOW_SNAPSHOT_FRAME_BYTES
                .checked_add(header.len())
                .assured("a header within its bounded limit fits beside its fixed frame"),
        );
        framed.extend_from_slice(&WINDOW_SNAPSHOT_MAGIC);
        framed.extend_from_slice(&header_length.to_le_bytes());
        framed.extend_from_slice(&header);
        Ok(framed)
    }
}

impl WindowProcessorStateSnapshot {
    /// Whether the retained rows' input and argument columns, as their Arrow slices measure them,
    /// occupy more than `limit` bytes. A total past u64::MAX is past every limit.
    fn retains_more_than(&self, limit: u64) -> bool {
        let mut bytes = 0_u64;
        for entry in &self.entries {
            let input = entry.record.one_row_batch().estimated_bytes();
            let arguments = entry.arguments.one_row_batch().estimated_bytes();
            let Some(row_bytes) = input.checked_add(arguments) else {
                return true;
            };
            let Some(total) = bytes.checked_add(row_bytes) else {
                return true;
            };
            if total > limit {
                return true;
            }
            bytes = total;
        }
        false
    }
}

/// Encode a window checkpoint in memory, for a window small enough to hold whole under the bulk
/// budget: replica synchronization, ownership handoff, and periodic persistence of a window whose
/// rows fit one snapshot section. A larger window is sealed in pieces by
/// [`seal_window_processor_snapshot`] into the same container.
pub(super) async fn encode_window_processor_snapshot(
    snapshot: &WindowProcessorStateSnapshot,
    revision: u64,
    executor: &Executor,
) -> Result<Vec<u8>, Report<WindowSnapshotError>> {
    // A window whose rows alone exceed the whole bulk budget can never be held whole, so it is
    // refused before any section is encoded for nothing.
    let bulk_capacity = executor.snapshot().bulk_memory.capacity_bytes;
    if snapshot.retains_more_than(bulk_capacity) {
        return Err(Report::new(WindowSnapshotError::Admission));
    }
    // An empty forced-recovery checkpoint has no concrete branch lifetime. The nested Arrow
    // container requires a numeric generation, but the outer `None` makes restore return before
    // those empty sections are opened; only the outer typed incarnation selects behavior.
    let branch_generation = snapshot.incarnation.unwrap_or_default();
    let generations = WindowGenerations::of(&snapshot.entries, revision, branch_generation);
    let input = generations
        .input
        .encode_resident_container(executor)
        .await
        .change_context(WindowSnapshotError::Encode {
            section: WindowSnapshotSection::Input,
        })?;
    let arguments = generations
        .arguments
        .encode_resident_container(executor)
        .await
        .change_context(WindowSnapshotError::Encode {
            section: WindowSnapshotSection::Arguments,
        })?;
    let typed = WindowTypedSections::encode(&snapshot.accumulators, executor).await?;
    let typed_sections = typed.count()?;
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
        accumulators: typed.descriptors,
        typed_sections,
    };
    let framed = header.framed(executor)?;
    let length_error = || {
        Report::new(WindowSnapshotError::Invalid {
            issue: WindowSnapshotIssue::Length,
        })
    };
    let mut length = framed.len();
    length = length.checked_add(input.len()).ok_or_else(length_error)?;
    length = length
        .checked_add(arguments.len())
        .ok_or_else(length_error)?;
    for section in &typed.sections {
        let prefixed = length.checked_add(4).ok_or_else(length_error)?;
        length = prefixed
            .checked_add(section.len())
            .ok_or_else(length_error)?;
    }
    let length_u64 = u64::try_from(length).change_context(WindowSnapshotError::Encode {
        section: WindowSnapshotSection::Container,
    })?;
    let reservation = executor
        .try_reserve(MemoryClass::Bulk, length_u64)
        .change_context(WindowSnapshotError::Admission)?;
    let typed_sections = typed.sections;
    let sealed = executor
        .run_cpu(CpuClass::Bulk, reservation, move |charge, cancellation| {
            cancellation
                .check()
                .change_context(WindowSnapshotError::Execution)?;
            let mut buffer = BudgetedBuffer::with_limit(charge, length_u64);
            buffer
                .write_all(&framed)
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

/// A window checkpoint staged group by group on quota-owned disk.
///
/// The nested input and argument containers grow as their groups are sealed, each group's bulk
/// charge ending once it is staged, so a window larger than the bulk budget never exists whole in
/// memory. `finish` places the window header, which counts the rows and the containers' bytes,
/// ahead of them, and the typed delayed-removal sections behind them, in the same container
/// [`encode_window_processor_snapshot`] writes.
pub(super) struct WindowCheckpointPieces {
    staging: SnapshotStaging,
    revision: u64,
    incarnation: Option<u64>,
    first_sequence: Option<u64>,
    rows: u64,
    input: SealedContainerPieces,
    arguments: SealedContainerPieces,
}

impl WindowCheckpointPieces {
    pub(super) fn new(staging: &SnapshotStaging, revision: u64, incarnation: Option<u64>) -> Self {
        // As in the resident encoding, only the outer typed incarnation selects behavior; an empty
        // window without a lifetime still numbers its nested containers.
        let branch_generation = incarnation.unwrap_or_default();
        Self {
            staging: staging.clone(),
            revision,
            incarnation,
            first_sequence: None,
            rows: 0,
            input: SealedContainerPieces::new(staging, revision, 0, branch_generation),
            arguments: SealedContainerPieces::new(staging, revision, 0, branch_generation),
        }
    }

    /// Count `rows` more retained rows, the first of them admitted at `first_sequence`.
    fn count_rows(
        &mut self,
        first_sequence: u64,
        rows: usize,
    ) -> Result<(), Report<WindowSnapshotError>> {
        if self.first_sequence.is_none() {
            self.first_sequence = Some(first_sequence);
        }
        let rows = u64::try_from(rows).change_context(WindowSnapshotError::Encode {
            section: WindowSnapshotSection::Header,
        })?;
        self.rows = self.rows.checked_add(rows).ok_or_else(|| {
            Report::new(WindowSnapshotError::Invalid {
                issue: WindowSnapshotIssue::Length,
            })
        })?;
        Ok(())
    }

    /// The input and argument generations of `entries` in this checkpoint.
    fn generations_of(&self, entries: &[WindowEntrySnapshot]) -> WindowGenerations {
        let branch_generation = self.incarnation.unwrap_or_default();
        WindowGenerations::of(entries, self.revision, branch_generation)
    }

    /// `records` of `schema` as one nested generation of this checkpoint, for a caller that seals a
    /// window's input rows and argument columns separately.
    pub(super) fn nested_generation(
        &self,
        schema: StdArc<ArrowSchema>,
        records: Vec<MaterializedGenerationRecord>,
    ) -> MaterializedGeneration {
        let branch_generation = self.incarnation.unwrap_or_default();
        MaterializedGeneration::new(self.revision, 0, branch_generation, schema, records)
    }

    /// Seal `entries`, the next retained rows in admission order, in bounded groups.
    pub(super) async fn append_entries(
        &mut self,
        executor: &Executor,
        entries: &[WindowEntrySnapshot],
    ) -> Result<(), Report<WindowSnapshotError>> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        self.count_rows(first.sequence, entries.len())?;
        let generations = self.generations_of(entries);
        self.input
            .append_generation(executor, &generations.input)
            .await
            .change_context(WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Input,
            })?;
        self.arguments
            .append_generation(executor, &generations.arguments)
            .await
            .change_context(WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Arguments,
            })
    }

    /// Seal the input rows `input` holds, the next retained rows from `first_sequence`, whose Arrow
    /// columns `columns` already holds as one section. The section is kept as it is whenever the
    /// rows' identities fit one identity record.
    pub(super) async fn append_encoded_input(
        &mut self,
        executor: &Executor,
        first_sequence: u64,
        input: &MaterializedGeneration,
        columns: ChargedBytes,
    ) -> Result<(), Report<WindowSnapshotError>> {
        if input.records().is_empty() {
            return Ok(());
        }
        self.count_rows(first_sequence, input.records().len())?;
        self.input
            .append_encoded(executor, input, columns)
            .await
            .change_context(WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Input,
            })
    }

    /// Seal the argument columns `arguments` holds, row-aligned with the input rows sealed last,
    /// whose Arrow columns `columns` already holds as one section.
    pub(super) async fn append_encoded_arguments(
        &mut self,
        executor: &Executor,
        arguments: &MaterializedGeneration,
        columns: ChargedBytes,
    ) -> Result<(), Report<WindowSnapshotError>> {
        self.arguments
            .append_encoded(executor, arguments, columns)
            .await
            .change_context(WindowSnapshotError::Encode {
                section: WindowSnapshotSection::Arguments,
            })
    }

    /// The finished checkpoint in one quota-owned artifact, whose window resumes at
    /// `next_sequence` with `accumulators`.
    pub(super) async fn finish(
        self,
        executor: &Executor,
        next_sequence: u64,
        accumulators: &[WindowAccumulatorSnapshot],
    ) -> Result<StagedArtifact, Report<WindowSnapshotError>> {
        let input =
            self.input
                .finish(executor)
                .await
                .change_context(WindowSnapshotError::Encode {
                    section: WindowSnapshotSection::Input,
                })?;
        let arguments =
            self.arguments
                .finish(executor)
                .await
                .change_context(WindowSnapshotError::Encode {
                    section: WindowSnapshotSection::Arguments,
                })?;
        let typed = WindowTypedSections::encode(accumulators, executor).await?;
        let header = WindowSnapshotHeader {
            revision: self.revision,
            first_sequence: self.first_sequence,
            next_sequence,
            incarnation: self.incarnation,
            rows: self.rows,
            input_bytes: input.length(),
            argument_bytes: arguments.length(),
            typed_sections: typed.count()?,
            accumulators: typed.descriptors,
        };
        let framed = header.framed(executor)?;
        let framed = executor
            .charge_owned(MemoryClass::Bulk, framed)
            .await
            .change_context(WindowSnapshotError::Admission)?;
        let mut pieces = StagedPieces::new(self.staging);
        pieces
            .stage(framed)
            .await
            .change_context(WindowSnapshotError::Execution)?;
        pieces.extend(input);
        pieces.extend(arguments);
        for section in typed.sections {
            nervix_primitives::task::consume_budget().await;
            let section_length =
                u32::try_from(section.len()).change_context(WindowSnapshotError::Encode {
                    section: WindowSnapshotSection::DelayedRemovals,
                })?;
            let prefix = executor
                .charge_owned(MemoryClass::Bulk, section_length.to_le_bytes().to_vec())
                .await
                .change_context(WindowSnapshotError::Admission)?;
            pieces
                .stage(prefix)
                .await
                .change_context(WindowSnapshotError::Execution)?;
            pieces
                .stage(section)
                .await
                .change_context(WindowSnapshotError::Execution)?;
        }
        pieces
            .concatenate()
            .await
            .change_context(WindowSnapshotError::Execution)
    }
}

/// Seal a window checkpoint in bounded pieces on quota-owned disk: the same container
/// [`encode_window_processor_snapshot`] writes in memory, for a window too large to hold whole.
pub(super) async fn seal_window_processor_snapshot(
    snapshot: &WindowProcessorStateSnapshot,
    revision: u64,
    executor: &Executor,
    staging: &SnapshotStaging,
) -> Result<StagedArtifact, Report<WindowSnapshotError>> {
    let mut pieces = WindowCheckpointPieces::new(staging, revision, snapshot.incarnation);
    pieces.append_entries(executor, &snapshot.entries).await?;
    pieces
        .finish(executor, snapshot.next_sequence, &snapshot.accumulators)
        .await
}

/// How one published window is written to the state store.
pub(super) enum WindowPersistence {
    /// A checkpoint small enough to encode in memory, written as one value.
    Resident(PersistedRuntimeStateEntry),
    /// A checkpoint sealed in bounded pieces on quota-owned disk, published in segments.
    Sealed {
        revision: u64,
        artifact: StagedArtifact,
    },
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
        RestoredRows::Window,
    )
    .await
    .change_context(WindowSnapshotError::Decode {
        section: WindowSnapshotSection::Input,
    })?;
    let arguments = RestoredMaterializedSnapshot::open(
        executor,
        schemas.arguments,
        SealedSource::borrowed(executor, arguments),
        RestoredRows::Window,
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
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let generations = match initial {
            Some(initial) => PublishedGenerations::restored(
                initial.lsm,
                Some(WindowPublishedSnapshot::Sealed(initial.payload)),
            ),
            None => PublishedGenerations::restored(0, None),
        };
        Ok(Self {
            placement,
            generations,
            replication: CheckpointReplication::new(),
            catalog: None,
        })
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

    /// What persisting the window published after `after_lsm` writes, or nothing when that window
    /// is already persisted.
    ///
    /// A live window whose retained rows exceed one snapshot section is sealed in bounded pieces on
    /// quota-owned disk instead of being encoded in memory, so the windows the runtime persists are
    /// bounded by its staging quota rather than by the bulk budget.
    pub(super) async fn persistence_after(
        &self,
        after_lsm: u64,
        executor: &Executor,
        staging: &SnapshotStaging,
    ) -> Result<Option<WindowPersistence>, Report<RuntimePersistenceError>> {
        let Some(published) = self.generations.load_after(Some(after_lsm)) else {
            return Ok(None);
        };
        let section_limit = executor.limits().snapshot_section_bytes.as_u64();
        if let Some(WindowPublishedSnapshot::Live(snapshot)) = &published.value
            && snapshot.retains_more_than(section_limit)
        {
            let artifact =
                seal_window_processor_snapshot(snapshot, published.revision, executor, staging)
                    .await
                    .change_context(RuntimePersistenceError::WindowSnapshot)?;
            return Ok(Some(WindowPersistence::Sealed {
                revision: published.revision,
                artifact,
            }));
        }
        let entry = self.snapshot_of(&published, executor).await?;
        Ok(Some(WindowPersistence::Resident(entry)))
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

        /// A window of `rows` unbranched rows sharing one input and one argument batch, admitted at
        /// consecutive sequences from 100 in branch lifetime `incarnation`.
        fn window_of(&self, rows: usize, incarnation: u64) -> WindowProcessorStateSnapshot {
            use meticulous::ResultExt as _;

            let count = i64::try_from(rows).assured("a test window's row count fits i64");
            let latencies: ArrayRef = StdArc::new(Int64Array::from_iter_values(0..count));
            let input_schema = self.input_schema.arrow_schema();
            let input = RecordBatch::try_new(StdArc::clone(&input_schema), vec![latencies])
                .assured("the latency column matches the input schema");
            let input = Arc::new(
                RuntimeRecordBatch::from_record_batch(input_schema, input)
                    .assured("the input batch has its declared schema"),
            );
            let values: ArrayRef = StdArc::new(Int64Array::from_iter_values(0..count));
            let arguments = RecordBatch::try_new(self.argument_schema.clone(), vec![values])
                .assured("the argument column matches its plan's schema");
            let arguments = Arc::new(
                RuntimeRecordBatch::from_record_batch(self.argument_schema.clone(), arguments)
                    .assured("the argument batch has its declared schema"),
            );
            let mut entries = Vec::with_capacity(rows);
            for row in 0..rows {
                let offset = u64::try_from(row).assured("a test row index fits u64");
                let at = Timestamp::from_unix_nanos(
                    i64::try_from(row).assured("a test row index fits i64"),
                );
                let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(at, at);
                entries.push(WindowEntrySnapshot {
                    sequence: 100_u64
                        .checked_add(offset)
                        .assured("a test window stays far below u64::MAX"),
                    timestamp: at,
                    key: None,
                    record: RuntimeRow::new(Arc::clone(&input), row, metadata.clone())
                        .assured("the row is inside its input batch"),
                    arguments: RuntimeRow::new(Arc::clone(&arguments), row, metadata)
                        .assured("the row is inside its argument batch"),
                });
            }
            let rows = u64::try_from(rows).assured("a test window's row count fits u64");
            WindowProcessorStateSnapshot {
                entries,
                next_sequence: 100_u64
                    .checked_add(rows)
                    .assured("a test window stays far below u64::MAX"),
                incarnation: Some(incarnation),
                accumulators: vec![WindowAccumulatorSnapshot::Retained],
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

    /// A window sealed in pieces on quota-owned disk is the same container, byte for byte, as its
    /// resident encoding, so either one restores the window the other does.
    #[test]
    fn bolero_sealed_window_checkpoints_match_their_resident_encoding() {
        use meticulous::ResultExt as _;
        use nervix_arbitrary::{Arbitrary, Domain};

        let fixture = WindowSnapshotFixture::new();
        bolero::check!()
            .with_iterations(64)
            .with_max_len(2048)
            .for_each(|bytes: &[u8]| {
                let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .assured("a property runtime opens");
                let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
                let executor = Executor::default();
                let directory = tempfile::tempdir().assured("the staging directory opens");
                let staging = SnapshotStaging::new(
                    directory.path().to_path_buf(),
                    executor.clone(),
                    crate::runtime::snapshot_staging::SnapshotStagingLimits::default(),
                );
                let generated = fixture.generated(&mut arbitrary);
                runtime.block_on(async {
                    let resident = encode_window_processor_snapshot(
                        &generated.snapshot,
                        generated.revision,
                        &executor,
                    )
                    .await
                    .assured("a bounded generated window encodes in memory");
                    let sealed = seal_window_processor_snapshot(
                        &generated.snapshot,
                        generated.revision,
                        &executor,
                        &staging,
                    )
                    .await
                    .assured("a bounded generated window seals in pieces");
                    let sealed_bytes =
                        std::fs::read(sealed.path()).assured("the sealed checkpoint reads");
                    assert_eq!(
                        sealed.length(),
                        u64::try_from(sealed_bytes.len()).assured("fits")
                    );
                    assert_eq!(sealed.digest(), *blake3::hash(&sealed_bytes).as_bytes());
                    assert_eq!(
                        sealed_bytes, resident,
                        "the sealed pieces concatenate into the resident container"
                    );
                });
                assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
            });
    }

    /// An executor whose snapshot sections and identity records are small, so a test window
    /// counts as one too large to persist in memory and seals in several groups.
    fn narrow_executor() -> Executor {
        use meticulous::ResultExt as _;

        Executor::new(nervix_execution::ExecutionConfig {
            limits: nervix_execution::OperationLimits {
                snapshot_section_bytes: ubyte::ByteUnit::Byte(4096),
                snapshot_record_bytes: ubyte::ByteUnit::Byte(512),
                ..nervix_execution::OperationLimits::default()
            },
            ..nervix_execution::ExecutionConfig::default()
        })
        .assured("narrow snapshot limits within the default bulk budget are valid")
    }

    /// A published window persists in memory while its rows fit one snapshot section, and is
    /// sealed in pieces for segmented publication once they do not; both persist the window it
    /// published.
    #[nervix_primitives::test]
    async fn a_window_beyond_one_section_persists_through_sealed_pieces() {
        use meticulous::{OptionExt as _, ResultExt as _};

        let fixture = WindowSnapshotFixture::new();
        let executor = narrow_executor();
        let directory = tempfile::tempdir().assured("the staging directory opens");
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            crate::runtime::snapshot_staging::SnapshotStagingLimits::default(),
        );
        let small = fixture.window_of(1, 9);
        let large = fixture.window_of(300, 9);
        let placement = RuntimeStatePlacement {
            domain: nervix_models::DomainName::parse("test").assured("a valid domain name"),
            state: super::super::RuntimeState::WindowProcessor {
                schema: nervix_models::SchemaFingerprint::from_digest([3; 32]),
            },
            kind: nervix_models::ModelKind::WindowProcessor,
            identifier: nervix_models::ModelName::parse("latency_window")
                .assured("a valid model name"),
            branch_key: None,
        };
        let state = ReplicatedWindowProcessorState::new(placement, None)
            .assured("an empty window state has nothing to decode");
        assert!(
            state
                .persistence_after(0, &executor, &staging)
                .await
                .assured("an unpublished window needs no persistence")
                .is_none()
        );
        let small_revision = state
            .generations
            .publish(Some(WindowPublishedSnapshot::Live(small.clone())));
        let resident = state
            .persistence_after(0, &executor, &staging)
            .await
            .assured("a small window persists")
            .verified("a newer window was published");
        let WindowPersistence::Resident(entry) = resident else {
            panic!("a window within one section persists in memory");
        };
        assert_eq!(entry.lsm, small_revision);
        assert_eq!(
            entry.payload,
            encode_window_processor_snapshot(&small, small_revision, &executor)
                .await
                .assured("the small window encodes")
        );
        let large_revision = state
            .generations
            .publish(Some(WindowPublishedSnapshot::Live(large.clone())));
        let sealed = state
            .persistence_after(small_revision, &executor, &staging)
            .await
            .assured("a large window persists")
            .verified("a newer window was published");
        let WindowPersistence::Sealed { revision, artifact } = sealed else {
            panic!("a window beyond one section seals in pieces");
        };
        assert_eq!(revision, large_revision);
        let payload = std::fs::read(artifact.path()).assured("the sealed window reads");
        assert_eq!(
            payload,
            encode_window_processor_snapshot(&large, large_revision, &executor)
                .await
                .assured("the large window still fits the bulk budget in memory"),
            "the window sealed in several groups is its resident container"
        );
        let restored = fixture
            .open(&payload, &executor, 9)
            .await
            .assured("the sealed window opens")
            .verified("the sealed window names its branch lifetime");
        assert_eq!(restored.entries.len(), 300);
        assert_eq!(restored.next_sequence, large.next_sequence);
        assert!(
            state
                .persistence_after(large_revision, &executor, &staging)
                .await
                .assured("a persisted window needs nothing more")
                .is_none()
        );
    }

    /// A window may retain more rows than one relay's row-view metadata ceiling, and a branch
    /// reopens all of them, charged to the relay memory class while it does.
    #[nervix_primitives::test]
    async fn a_window_beyond_one_relays_row_view_ceiling_reopens() {
        use meticulous::{OptionExt as _, ResultExt as _};

        let fixture = WindowSnapshotFixture::new();
        let executor = Executor::default();
        let window = fixture.window_of(100_000, 9);
        let payload = encode_window_processor_snapshot(&window, 5, &executor)
            .await
            .assured("a window of small rows encodes in memory");
        let restored = fixture
            .open(&payload, &executor, 9)
            .await
            .assured("every retained row reopens")
            .verified("the window names its branch lifetime");
        assert_eq!(restored.entries.len(), 100_000);
        assert_eq!(restored.entries[99_999].sequence, 100_099);
        drop(restored);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }

    /// A window whose rows alone exceed the whole bulk budget is refused by the resident encoding
    /// before it encodes a section for nothing.
    #[nervix_primitives::test]
    async fn the_resident_encoding_refuses_a_window_beyond_the_bulk_budget_first() {
        use meticulous::ResultExt as _;

        let fixture = WindowSnapshotFixture::new();
        let executor = Executor::new(nervix_execution::ExecutionConfig {
            budgets: nervix_execution::MemoryBudgets {
                bulk: ubyte::ByteUnit::Kibibyte(512),
                ..nervix_execution::MemoryBudgets::default()
            },
            limits: nervix_execution::OperationLimits {
                snapshot_section_bytes: ubyte::ByteUnit::Kibibyte(64),
                snapshot_record_bytes: ubyte::ByteUnit::Kibibyte(16),
                snapshot_header_bytes: ubyte::ByteUnit::Kibibyte(16),
                bulk_chunk_bytes: ubyte::ByteUnit::Kibibyte(16),
                ..nervix_execution::OperationLimits::default()
            },
            ..nervix_execution::ExecutionConfig::default()
        })
        .assured("a small bulk budget above two sections is valid");
        let window = fixture.window_of(40_000, 9);
        let granted = executor.snapshot().bulk_memory.granted;
        let error = encode_window_processor_snapshot(&window, 1, &executor)
            .await
            .expect_err("a window larger than the whole bulk budget is refused");
        assert!(matches!(
            error.current_context(),
            WindowSnapshotError::Admission
        ));
        assert_eq!(
            executor.snapshot().bulk_memory.granted,
            granted,
            "no section was encoded for a window that can never be held whole"
        );
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
