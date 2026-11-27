//! Bounded reads of the rows that reorderer and correlator branch tasks hold.
//!
//! Layer: data plane.
//! - **Owns.** The inspection request a processor branch task answers between two of its batches,
//!   the immutable bounded copy it answers with, the unit task's selection of branch tasks, and the
//!   owner page that gathers one execution unit's copies into Arrow columns.
//! - **Depends on.** Processor branch tasks and their command channels, the unit task's branch
//!   registry, typed branch selections, row views over shared Arrow batches and the bounded
//!   executor.
//! - **Must not know.** Models, Plexus, sessions, Row frames or presentation formats.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        observer,
        reason = "operator inspections copy task-owned buffers on explicit request"
    )
)]

use std::collections::BTreeSet;

use arrow_array::{TimestampNanosecondArray, UInt32Array};
use arrow_schema::FieldRef;
use error_stack::ResultExt as _;
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass, Reservation};
use nervix_models::{ProcessorStateSection, StateInspectionBranchSelection, StateInspectionCounts};

use super::*;
use crate::runtime_schema::batch_payload_bytes;

/// What one branch task copies of the rows it holds: the rows after the first `skip` of them, in
/// the order the task lists them, at most `max_rows` rows of at most `max_bytes` payload bytes.
#[derive(Debug, Clone, Copy)]
pub(super) struct BranchStateRead {
    pub(super) skip: u64,
    pub(super) max_rows: usize,
    pub(super) max_bytes: u64,
}

/// Why the rows a processor branch holds could not be read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BranchStateReadError {
    #[error("the processor branch task ended or is draining and answers no inspection")]
    Ended,
    #[error("the processor has not restored its branches")]
    Unrestored,
    #[error("the processor holds no state that an inspection of its kind reads")]
    Unsupported,
    #[error("a held row could not be addressed")]
    Row,
    #[error("a held row's order key does not have the type its ordering expression produces")]
    OrderKey,
}

/// A branch task's answer, taken between two of its batches: the complete counts of everything the
/// branch holds and an immutable copy of the selected rows. Row views share the task's Arrow
/// batches; nothing the task keeps mutable is borrowed.
#[derive(Debug)]
pub(super) struct BranchStateCopy {
    /// The lifetime of the branch the task runs, which a recreated branch starts afresh.
    pub(super) incarnation: u64,
    /// The events the task has handled. Everything a branch holds changes only inside one of them,
    /// so equal events of one lifetime name one exact state.
    pub(super) events: u64,
    pub(super) held: HeldBranchRows,
}

/// Everything one branch holds, counted completely, and the rows of it a read copied.
#[derive(Debug)]
pub(super) struct HeldBranchRows {
    pub(super) rows: u64,
    pub(super) bytes: u64,
    /// The payload bytes of the copied rows, charged against the read's byte bound.
    pub(super) copied_bytes: u64,
    pub(super) kind: HeldRowsKind,
}

#[derive(Debug)]
pub(super) enum HeldRowsKind {
    Reorderer {
        /// The order-key fields the reorderer's key program produces, in `BY` order.
        key_fields: Vec<FieldRef>,
        rows: Vec<HeldReordererRow>,
    },
    Correlator {
        /// How many of the branch's held rows are pending on the left side; the rest are right.
        left: u64,
        rows: Vec<HeldCorrelatorRow>,
    },
}

impl HeldRowsKind {
    fn copied(&self) -> usize {
        match self {
            Self::Reorderer { rows, .. } => rows.len(),
            Self::Correlator { rows, .. } => rows.len(),
        }
    }

    /// What admitting each copied row takes, in the order the rows were copied.
    fn row_costs(&self) -> Vec<HeldRowCost> {
        let mut costs = Vec::with_capacity(self.copied());
        match self {
            Self::Reorderer { rows, .. } => {
                // The route the row waits on, and its received and release times.
                let fixed = std::mem::size_of::<u32>()
                    .checked_add(std::mem::size_of::<i64>())
                    .assured("fixed column widths are tiny");
                let fixed = fixed
                    .checked_add(std::mem::size_of::<i64>())
                    .assured("fixed column widths are tiny");
                let fixed = u64::try_from(fixed).assured("fixed column widths fit u64");
                let key_part = u64::try_from(std::mem::size_of::<u64>())
                    .assured("a fixed column width fits u64");
                for row in rows {
                    let columns = held_row_bytes(&row.row);
                    let mut appended = fixed;
                    for part in &row.key {
                        appended = appended
                            .checked_add(key_part)
                            .assured("a resident row's key is far below u64::MAX bytes");
                        appended = appended
                            .checked_add(part.value_bytes())
                            .assured("a resident row's key is far below u64::MAX bytes");
                    }
                    costs.push(HeldRowCost::new(columns, appended));
                }
            }
            Self::Correlator { rows, .. } => {
                // The row's received and timeout times.
                let appended = std::mem::size_of::<i64>()
                    .checked_add(std::mem::size_of::<i64>())
                    .assured("fixed column widths are tiny");
                let appended = u64::try_from(appended).assured("fixed column widths fit u64");
                for row in rows {
                    costs.push(HeldRowCost::new(held_row_bytes(&row.row), appended));
                }
            }
        }
        costs
    }

    /// Keeps only the first `rows` copied rows.
    fn truncate(&mut self, rows: usize) {
        match self {
            Self::Reorderer { rows: held, .. } => held.truncate(rows),
            Self::Correlator { rows: held, .. } => held.truncate(rows),
        }
    }
}

/// What admitting one copied row to a page takes: the bytes of its gathered columns, and the bytes
/// of the body that carries it to another node.
struct HeldRowCost {
    columns: u64,
    body: u64,
}

impl HeldRowCost {
    /// A row whose payload takes `columns` bytes and whose appended columns take `appended`.
    fn new(columns: u64, appended: u64) -> Self {
        Self {
            columns,
            body: columns
                .checked_add(appended)
                .assured("a resident row is far below u64::MAX bytes"),
        }
    }
}

/// One row a reorderer holds back on one output route.
#[derive(Debug)]
pub(super) struct HeldReordererRow {
    pub(super) row: RuntimeRow,
    /// The route the row waits on, numbered from zero in declaration order.
    pub(super) output: u32,
    pub(super) key: Vec<ReorderKeyPart>,
    pub(super) received_at: Timestamp,
    /// When the route releases its held rows: `MAX TIME` after the first of them arrived, or the
    /// route's flush deadline when that comes first.
    pub(super) release_at: Timestamp,
}

/// One row a correlator holds waiting for its match.
#[derive(Debug)]
pub(super) struct HeldCorrelatorRow {
    pub(super) side: CorrelatorSide,
    pub(super) row: RuntimeRow,
    pub(super) received_at: Timestamp,
    pub(super) timeout_at: Timestamp,
}

/// How much of a branch's held rows one read still skips and copies, consumed in the order the
/// branch lists its rows.
pub(super) struct HeldRowsBudget {
    skip: u64,
    rows: usize,
    bytes: u64,
    copied_bytes: u64,
    full: bool,
}

impl HeldRowsBudget {
    pub(super) fn new(read: BranchStateRead) -> Self {
        Self {
            skip: read.skip,
            rows: read.max_rows,
            bytes: read.max_bytes,
            copied_bytes: 0,
            full: read.max_rows == 0,
        }
    }

    /// Skips up to `available` leading rows, returning how many of them the read skips.
    pub(super) fn skip(&mut self, available: usize) -> usize {
        let available_rows = u64::try_from(available).assured("an addressable row count fits u64");
        let skipped = self.skip.min(available_rows);
        self.skip = self
            .skip
            .checked_sub(skipped)
            .verified("skipped is the smaller of skip and the available rows");
        usize::try_from(skipped).assured("a skip never exceeds the addressable rows it skips")
    }

    /// Whether the next row, of `bytes` payload bytes, is copied. Once a row does not fit, the read
    /// copies nothing more, so the copied rows stay one contiguous run of the branch's order.
    pub(super) fn admit(&mut self, bytes: u64) -> bool {
        if self.full {
            return false;
        }
        let Some(total) = self.copied_bytes.checked_add(bytes) else {
            self.full = true;
            return false;
        };
        if total > self.bytes {
            self.full = true;
            return false;
        }
        self.copied_bytes = total;
        self.rows = self
            .rows
            .checked_sub(1)
            .verified("a read with no rows left is full and returned above");
        self.full = self.rows == 0;
        true
    }

    pub(super) fn is_full(&self) -> bool {
        self.full
    }
}

/// The logical Arrow payload bytes of one row view, as the row carrier measures payload.
pub(super) fn held_row_bytes(row: &RuntimeRow) -> u64 {
    batch_payload_bytes(&row.batch().batch().slice(row.row_index(), 1))
}

impl RelayProcessorNode {
    /// Copies the rows this processor holds in one branch. The branch task calls it between two
    /// batches, so the copy is never a partial batch.
    pub(super) fn copy_held_rows(
        &self,
        read: BranchStateRead,
        now: Timestamp,
    ) -> error_stack::Result<HeldBranchRows, BranchStateReadError> {
        let mut budget = HeldRowsBudget::new(read);
        match &self.operation {
            RelayProcessorOperationNode::Reorderer {
                output_routes,
                max_time,
                compiled_program,
                output_buffers,
                ..
            } => {
                let key_fields = match compiled_program.as_deref() {
                    Some(program) => {
                        let fields = program.program.output_schema.fields();
                        let end = program
                            .key_column_offset
                            .checked_add(program.key_count)
                            .ok_or_else(|| Report::new(BranchStateReadError::OrderKey))?;
                        fields
                            .get(program.key_column_offset..end)
                            .ok_or_else(|| Report::new(BranchStateReadError::OrderKey))?
                            .to_vec()
                    }
                    None => Vec::new(),
                };
                let mut rows = 0_u64;
                let mut bytes = 0_u64;
                for buffer in output_buffers {
                    rows = rows
                        .checked_add(buffer.held_rows())
                        .assured("rows a node holds in memory are fewer than 2^64");
                    bytes = bytes
                        .checked_add(buffer.estimated_bytes())
                        .assured("bytes a node holds in memory are fewer than 2^64");
                }
                let mut copied = Vec::new();
                for (index, buffer) in output_buffers.iter().enumerate() {
                    if budget.is_full() {
                        break;
                    }
                    let Some(first_received_at) = buffer.first_received_at() else {
                        continue;
                    };
                    let route = output_routes
                        .routes
                        .get(index)
                        .ok_or_else(|| Report::new(BranchStateReadError::Row))?;
                    let release_at = reorderer_release_at(first_received_at, *max_time, route, now);
                    let output =
                        u32::try_from(index).map_err(|_| Report::new(BranchStateReadError::Row))?;
                    buffer.copy_held(output, release_at, &mut budget, &mut copied)?;
                }
                Ok(HeldBranchRows {
                    rows,
                    bytes,
                    copied_bytes: budget.copied_bytes,
                    kind: HeldRowsKind::Reorderer {
                        key_fields,
                        rows: copied,
                    },
                })
            }
            RelayProcessorOperationNode::Correlator {
                max_time, state, ..
            } => {
                let left = u64::try_from(state.pending_left.len())
                    .assured("an addressable count fits u64");
                let right = u64::try_from(state.pending_right.len())
                    .assured("an addressable count fits u64");
                let rows = left
                    .checked_add(right)
                    .assured("rows a node holds in memory are fewer than 2^64");
                // A pending record is a view of the batch it arrived in and keeps that batch alive,
                // so the branch holds those batches' payload bytes, each counted once however many
                // pending records share it, as a reorderer counts the batches its routes hold. The
                // pass visits each pending record once without copying it.
                let mut batches = HashSet::default();
                let mut bytes = 0_u64;
                for pending in state.pending_left.iter().chain(&state.pending_right) {
                    let batch = pending.message.record.batch();
                    if batches.insert(Arc::as_ptr(batch).addr()) {
                        bytes = bytes
                            .checked_add(batch.estimated_bytes())
                            .assured("bytes a node holds in memory are fewer than 2^64");
                    }
                }
                let mut copied = Vec::new();
                for (side, pending) in [
                    (CorrelatorSide::Left, &state.pending_left),
                    (CorrelatorSide::Right, &state.pending_right),
                ] {
                    let skipped = budget.skip(pending.len());
                    for entry in &pending[skipped..] {
                        let row = entry.message.record.clone();
                        if !budget.admit(held_row_bytes(&row)) {
                            break;
                        }
                        copied.push(HeldCorrelatorRow {
                            side,
                            row,
                            received_at: entry.received_at,
                            timeout_at: checked_add_duration_to_timestamp(
                                entry.received_at,
                                *max_time,
                            ),
                        });
                    }
                }
                Ok(HeldBranchRows {
                    rows,
                    bytes,
                    copied_bytes: budget.copied_bytes,
                    kind: HeldRowsKind::Correlator { left, rows: copied },
                })
            }
            RelayProcessorOperationNode::Deduplicator { .. }
            | RelayProcessorOperationNode::WindowProcessor { .. }
            | RelayProcessorOperationNode::Junction { .. }
            | RelayProcessorOperationNode::Inferencer { .. }
            | RelayProcessorOperationNode::WasmProcessor { .. } => {
                Err(Report::new(BranchStateReadError::Unsupported))
            }
        }
    }
}

/// When one reorderer route releases what it holds: `MAX TIME` after its first held row arrived,
/// or its flush deadline when that comes first. An immediate flush deadline is physical and due
/// within its minimum timeout, so its release is the current domain time.
fn reorderer_release_at(
    first_received_at: Timestamp,
    max_time: Duration,
    route: &RelayProcessorOutputNode,
    now: Timestamp,
) -> Timestamp {
    let by_max_time = checked_add_duration_to_timestamp(first_received_at, max_time);
    match route.flush_timer.deadline() {
        Some(BranchBufferDeadline::Logical(deadline)) => by_max_time.min(deadline.due_at()),
        Some(BranchBufferDeadline::Physical(_)) => by_max_time.min(now),
        None => by_max_time,
    }
}

impl BranchRuntime {
    /// Copies the rows `processor` holds in this branch, for the inspection request the branch
    /// task is answering between two batches.
    pub(super) fn copy_processor_held_rows(
        &self,
        processor: &ModelName,
        read: BranchStateRead,
        now: Timestamp,
    ) -> error_stack::Result<HeldBranchRows, BranchStateReadError> {
        self.processors
            .get(processor)
            .ok_or_else(|| Report::new(BranchStateReadError::Unsupported))?
            .copy_held_rows(read, now)
    }
}

/// Asks one branch task for the rows it holds, and waits for its answer.
///
/// The task serves this request from its command arm, which it prefers to its next batch, so the
/// answer is taken between two batches. A task that ended, or that is draining after it stopped
/// serving commands, drops the request: the read is then refused as [`BranchStateReadError::Ended`]
/// rather than left waiting. The caller bounds the whole wait with the inspection read deadline.
pub(super) async fn read_branch_state(
    task: &ProcessorBranchTask,
    read: BranchStateRead,
) -> error_stack::Result<BranchStateCopy, BranchStateReadError> {
    let (response, answer) = oneshot::channel();
    task.commands
        .send(ProcessorBranchCommand::Inspect { read, response })
        .await
        .map_err(|_| Report::new(BranchStateReadError::Ended))?;
    answer
        .await
        .map_err(|_| Report::new(BranchStateReadError::Ended))?
}

/// The events a branch task has handled that can change what it holds: ticks, batches,
/// materialized retries and force flushes. An inspection names the exact state it copied by this
/// count.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct HandledEvents {
    count: u64,
}

impl HandledEvents {
    /// Counts one more handled event.
    pub(super) fn record(&mut self) {
        self.count = self
            .count
            .checked_add(1)
            .assured("a branch handles fewer than 2^64 events");
    }
}

/// Answers one inspection with what the branch holds now. The branch task calls this between two
/// batches; a requester that already withdrew, because its deadline elapsed, receives nothing.
pub(super) fn answer_branch_state(
    response: oneshot::Sender<error_stack::Result<BranchStateCopy, BranchStateReadError>>,
    incarnation: u64,
    events: HandledEvents,
    held: error_stack::Result<HeldBranchRows, BranchStateReadError>,
) {
    let answer = match held {
        Ok(held) => Ok(BranchStateCopy {
            incarnation,
            events: events.count,
            held,
        }),
        Err(error) => Err(error),
    };
    response
        .send(answer)
        .means_peer_left("processor state inspection requester");
}

/// The branch tasks one inspection reads, in canonical branch order.
pub(super) struct SelectedStateBranches {
    pub(super) branches: Vec<SelectedStateBranch>,
}

pub(super) struct SelectedStateBranch {
    pub(super) key: Option<BranchKey>,
    /// The key's live slot in the unit's registry. Releasing a key replaces its registry entry
    /// rather than changing the slot, so a slot selected live stays live.
    slot: Arc<ProcessorBranchSlot>,
}

impl SelectedStateBranch {
    /// The branch task the selection reads.
    pub(super) fn task(&self) -> &ProcessorBranchTask {
        self.slot.live().assured(
            "a selection keeps only live slots, and a registry slot never changes in place",
        )
    }
}

impl SelectedStateBranches {
    /// Selects the branch tasks of one unit an inspection reads. The unit task answers this from
    /// its own registry without waiting for any branch, so input dispatch is not held behind the
    /// branches' answers.
    ///
    /// A read of one concrete branch compares fingerprints over the unit's live branches: the
    /// fingerprint is the only identity the request carries, and the visit is the one a read of all
    /// branches makes, bounded by the processor's branch count.
    ///
    /// A key this unit released during a partition transition is not read here: its state moves
    /// to the unit that claims it, which reports the key once it installs that state, and this unit
    /// keeps its copy only until that unit asks it to forget the key.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the branch registry's iterator walks its own index map of \
                                   live and released instances, once per inspection and bounded \
                                   by the unit's branches, and acquires nothing")
    )]
    pub(super) fn select(
        instances: &ProcessorBranches,
        selection: StateInspectionBranchSelection,
    ) -> Self {
        let mut branches = Vec::new();
        for (key, slot) in instances.instances() {
            if slot.live().is_none() {
                continue;
            }
            let selected = match selection {
                StateInspectionBranchSelection::All => true,
                StateInspectionBranchSelection::Unbranched => key.is_none(),
                StateInspectionBranchSelection::Branch(wanted) => match key {
                    Some(key) => key.fingerprint() == wanted,
                    None => false,
                },
            };
            if selected {
                branches.push(SelectedStateBranch {
                    key: key.clone(),
                    slot: slot.clone(),
                });
            }
        }
        branches.sort_by(|left, right| BranchKey::canonical_order(&left.key, &right.key));
        Self { branches }
    }

    /// Answers a unit's branch selection for one inspection. The branches a restore has not
    /// installed hold rows no task can copy yet, so while a restore is pending the inspection is
    /// refused rather than reporting an incomplete state as complete.
    pub(super) fn answer(
        response: oneshot::Sender<error_stack::Result<Self, BranchStateReadError>>,
        instances: &ProcessorBranches,
        selection: StateInspectionBranchSelection,
        restore_pending: bool,
    ) {
        let answer = if restore_pending {
            Err(Report::new(BranchStateReadError::Unrestored))
        } else {
            Ok(Self::select(instances, selection))
        };
        response
            .send(answer)
            .means_peer_left("processor state inspection requester");
    }
}

/// Why one unit's owner page could not be read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProcessorStatePageError {
    #[error("the processor's unit has no task on this node")]
    Unavailable,
    #[error("a branch of the processor could not be read")]
    Branch,
    #[error("the processor has not restored its branches")]
    Unrestored,
    #[error("the continuation position does not belong to this state")]
    Position,
    #[error("the page could not obtain bounded execution or memory")]
    Admission,
    #[error("a held row's columns could not be gathered")]
    Columns,
}

/// One owner-side read of a processor unit's state page: the unit, what it selects, where it
/// resumes and the bounds of what it returns.
pub(crate) struct ProcessorStatePageRead<'a> {
    pub(crate) domain: &'a DomainName,
    pub(crate) processor: &'a NodeRef,
    pub(crate) unit: Unit,
    pub(crate) selection: ProcessorStateSelection,
    pub(crate) after: Option<(Option<BranchKeyFingerprint>, u64)>,
    pub(crate) bounds: ProcessorStatePageBounds,
}

/// Effective owner-side bounds after the coordinator and local capability checks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProcessorStatePageBounds {
    pub(crate) rows: usize,
    pub(crate) bytes: u64,
    /// What admitting the page's rows also sets aside for the reply that carries them.
    pub(crate) reply: PageReply,
}

/// How an owner page reaches its reader, which decides what admitting its rows also sets aside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageReply {
    /// This node presents the page itself and writes its rows as Row frames: nothing is set aside.
    Local,
    /// Another node reads the page, which travels as a descriptor and native Arrow bodies. The
    /// page sets aside `descriptor` for the descriptor's encoding and, when it can return rows,
    /// `framing` for each section body's Arrow framing; every row it admits sets the bytes of the
    /// body that carries it aside as well.
    Remote { descriptor: u64, framing: u64 },
}

impl PageReply {
    /// The eight-byte little-endian length that frames a page stream's descriptor.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "a page stream frames its descriptor with an eight-byte little-endian length"
        )
    )]
    pub(crate) const DESCRIPTOR_LENGTH_BYTES: u64 = 8;

    /// The charge of the job that computes one body's digest: the least a bounded job carries.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "a bounded executor job carries a charge of at least one byte"
        )
    )]
    pub(crate) const DIGEST_JOB_BYTES: u64 = 1;

    /// The reply to another node under `executor`'s bounds: what its descriptor takes and each
    /// body's framing.
    pub(crate) fn remote(executor: &nervix_execution::Executor) -> Self {
        Self::Remote {
            descriptor: Self::descriptor_share(executor),
            framing: Self::body_framing(executor),
        }
    }

    /// What a page another node reads sets aside ahead for its descriptor under `executor`'s
    /// bounds: the largest descriptor a bulk page stream carries, the length that frames it, and
    /// the job that computes each body's digest, one body at a time.
    pub(crate) fn descriptor_share(executor: &nervix_execution::Executor) -> u64 {
        let descriptor = nervix_interconnect::PoolClass::Bulk.payload_limit(executor);
        let framed = descriptor
            .checked_add(Self::DESCRIPTOR_LENGTH_BYTES)
            .assured("the bulk payload bound leaves room for its length");
        framed
            .checked_add(Self::DIGEST_JOB_BYTES)
            .assured("the bulk payload bound leaves room for its digest job")
    }

    /// The most one Arrow body's framing, its schema, record batch header and buffer padding, adds
    /// to its rows' bytes under `executor`'s bounds: the bound on one sealed snapshot header.
    pub(crate) fn body_framing(executor: &nervix_execution::Executor) -> u64 {
        executor.limits().snapshot_header_bytes.as_u64()
    }

    /// The bulk share an owner page of at most `max_rows` rows and `max_bytes` bytes starts its
    /// read from. A page that returns no row starts from the least share, since everything it
    /// takes is required. Any other starts from the room the class has free now
    /// ([`Executor::memory`]), up to the most such a page takes by the charges its rows take: each
    /// row's columns twice over, the selected views and their gathered buffers, and for a reply to
    /// another node the body that carries the row, beside what the reply sets aside ahead. A page
    /// that requires more runs again within a larger share, and one whose optional rows find no
    /// room in its share ends before them.
    pub(crate) fn initial_share(self, executor: &Executor, max_rows: u64, max_bytes: u64) -> u64 {
        if max_rows == 0 {
            return 1;
        }
        let free = executor.memory(MemoryClass::Bulk).available_bytes();
        // A page bound past what `u64` counts is past the class, whose free room alone then bounds
        // the share.
        let Some(most) = self.most_bytes(max_bytes) else {
            return free.max(1);
        };
        most.min(free).max(1)
    }

    /// The most an owner page of at most `max_bytes` bytes takes by the charges its rows take, or
    /// `None` past what `u64` counts: its rows' columns twice over and, for a reply to another
    /// node, the bodies that carry them beside what the reply sets aside ahead.
    fn most_bytes(self, max_bytes: u64) -> Option<u64> {
        let columns = max_bytes.checked_mul(2)?;
        let (bodies, ahead) = match self {
            Self::Local => (0, 0),
            Self::Remote {
                descriptor,
                framing,
            } => (max_bytes, descriptor.checked_add(framing)?),
        };
        columns.checked_add(bodies)?.checked_add(ahead)
    }
}

/// The ordinary branch selector and, only during repartitioning, copies selected elsewhere.
pub(crate) struct ProcessorStateSelection {
    pub(crate) branch: StateInspectionBranchSelection,
    #[cfg_attr(
        nervix_lint,
        expect(
            nervix::branch_inversion,
            reason = "processor or generator bookkeeping independently tracks branches; use branch-owned parts or the partition table in https://app.clickup.com/t/86bd1z1bj"
        )
    )]
    pub(crate) partition_exclusions: Option<BTreeSet<BranchKeyFingerprint>>,
}

impl From<StateInspectionBranchSelection> for ProcessorStateSelection {
    fn from(branch: StateInspectionBranchSelection) -> Self {
        Self {
            branch,
            partition_exclusions: None,
        }
    }
}

/// One execution unit's page of the rows its reorderer or correlator branches hold.
pub(crate) struct ProcessorStatePage {
    pub(crate) partition_catalog: nervix_models::StateInspectionKeyCatalog,
    /// The exact cut the page was read from: every selected branch's identity, lifetime and handled
    /// events.
    pub(crate) fingerprint: [u8; 32],
    /// The membership high watermark of the branches read: the latest lifetime any of them began,
    /// or zero when no branch was selected.
    pub(crate) generation: u64,
    pub(crate) counts: StateInspectionCounts,
    pub(crate) offset: u64,
    pub(crate) last_branch: Option<BranchKeyFingerprint>,
    pub(crate) more: bool,
    /// The returned rows in the unit's row order, as runs of consecutive rows that share one
    /// section and one branch. A section's columns hold its rows in the order its runs list them.
    pub(crate) runs: Vec<ProcessorStateRun>,
    pub(crate) sections: Vec<ProcessorStateSectionColumns>,
    /// A published-state reader retains admission for its newly built page through projection.
    pub(crate) memory: Option<ProcessorStatePageMemory>,
}

/// Keeps the page's allocation admission and decoded publication storage alive through encoding.
pub(crate) struct ProcessorStatePageMemory {
    pub(super) _reservation: nervix_execution::Reservation,
    pub(super) _windows: Vec<super::window_state::AdmittedWindowSnapshot>,
    /// What a page read for another node set aside for its reply; absent for a page that set
    /// nothing aside.
    pub(super) reply: Option<ReplyMemory>,
}

impl ProcessorStatePageMemory {
    /// Takes what the page set aside for its reply to another node.
    pub(crate) fn take_reply(&mut self) -> Option<ReplyMemory> {
        self.reply.take()
    }
}

/// What a page read for another node set aside for its reply when it admitted its rows, so that
/// encoding the reply never waits for room the page itself holds.
pub(crate) struct ReplyMemory {
    /// Encodes the reply's descriptor.
    pub(crate) descriptor: nervix_execution::Reservation,
    /// Encodes the bodies: every admitted row's bytes, and each section body's framing.
    pub(crate) bodies: nervix_execution::Reservation,
    /// The most one body's Arrow framing adds to its rows' bytes.
    pub(crate) framing: u64,
}

/// One section of a unit page: how many rows of it the unit holds, and the returned rows as Arrow
/// columns, payload first and then the columns the section appends.
pub(crate) struct ProcessorStateSectionColumns {
    pub(crate) section: ProcessorStateSection,
    /// Every row of this section the unit holds, including the rows the page omits.
    pub(crate) rows: u64,
    /// The returned rows' payload columns.
    pub(crate) payload: Vec<ArrayRef>,
    /// The returned rows' appended columns: a reorderer's output, its order keys, the received time
    /// and the release deadline; a correlator side's received time and timeout deadline.
    pub(crate) appended: Vec<ArrayRef>,
    /// A reorderer's order-key fields as its key program types them; empty for a correlator side.
    pub(crate) key_fields: Vec<FieldRef>,
}

/// A run of consecutive returned rows that belong to one section and one branch.
pub(crate) struct ProcessorStateRun {
    pub(crate) section: ProcessorStateSection,
    pub(crate) branch: Option<BranchKey>,
    pub(crate) rows: usize,
}

/// The columns of a page, and the runs that order its returned rows.
struct GatheredSections {
    runs: Vec<ProcessorStateRun>,
    sections: Vec<ProcessorStateSectionColumns>,
}

/// One selected branch's answer, kept beside the key it was selected by.
struct CopiedBranch {
    key: Option<BranchKey>,
    held: HeldBranchRows,
}

impl Runtime {
    /// Reads one page of the rows a local reorderer or correlator unit holds.
    ///
    /// The unit task names the selected branch tasks; each branch task then copies what it holds
    /// between two of its batches, after skipping the rows earlier pages returned, within the rows
    /// and bytes still left. Every selected branch is asked even once the page is full, because the
    /// counts are complete and the page names the exact state of every branch. The caller bounds the
    /// whole read with the inspection deadline.
    pub(crate) async fn local_processor_state_page(
        &self,
        executor: &Executor,
        read: ProcessorStatePageRead<'_>,
    ) -> error_stack::Result<ProcessorStatePage, ProcessorStatePageError> {
        if let ModelKind::Inferencer = read.processor.kind {
            return self.local_inferencer_state_page(executor, read).await;
        }
        if let ModelKind::Deduplicator | ModelKind::WindowProcessor = read.processor.kind {
            return self
                .local_published_processor_state_page(executor, read)
                .await;
        }
        let ProcessorStatePageRead {
            domain,
            processor,
            unit,
            selection,
            after,
            bounds,
        } = read;
        if selection.partition_exclusions.is_some() {
            return Err(Report::new(ProcessorStatePageError::Columns));
        }
        let commands = {
            let execution = self
                .inner
                .executions
                .get(domain)
                .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
            execution
                .node_tasks
                .get(&NodeUnitRef::new(processor.clone(), unit))
                .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?
                .commands
                .clone()
        };
        let (response, selected) = oneshot::channel();
        commands
            .send(ProcessorNodeCommand::SelectStateBranches {
                selection: selection.branch,
                response,
            })
            .await
            .map_err(|_| Report::new(ProcessorStatePageError::Unavailable))?;
        let selected = match selected.await {
            Ok(Ok(selected)) => selected,
            Ok(Err(error)) => {
                let context = match error.current_context() {
                    BranchStateReadError::Unrestored => ProcessorStatePageError::Unrestored,
                    _ => ProcessorStatePageError::Branch,
                };
                return Err(error.change_context(context));
            }
            Err(_) => return Err(Report::new(ProcessorStatePageError::Unavailable)),
        };

        let start = match after {
            Some((_, offset)) => offset,
            None => 0,
        };
        let mut skip = start;
        let mut rows_left = bounds.rows;
        let mut bytes_left = bounds.bytes;
        let mut counts = StateInspectionCounts::default();
        let mut previous_branch = None;
        let mut digest = nervix_crypto::digest::Sha256::new();
        digest.update(b"nervix/state-inspection/processor-branches");
        let mut generation = 0_u64;
        let mut copies = Vec::with_capacity(selected.branches.len());
        for branch in selected.branches {
            nervix_primitives::task::consume_budget().await;
            let read = BranchStateRead {
                skip,
                max_rows: rows_left,
                max_bytes: bytes_left,
            };
            let copy = read_branch_state(branch.task(), read)
                .await
                .change_context(ProcessorStatePageError::Branch)?;
            let fingerprint = branch.key.as_ref().map(BranchKey::fingerprint);
            match fingerprint {
                Some(fingerprint) => {
                    digest.update(&[1]);
                    digest.update(fingerprint.fingerprint());
                }
                None => digest.update(&[0]),
            }
            digest.update(&copy.incarnation.to_be_bytes());
            digest.update(&copy.events.to_be_bytes());
            generation = generation.max(copy.incarnation);
            let held = copy.held;
            counts.rows = counts
                .rows
                .checked_add(held.rows)
                .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
            counts.bytes = counts
                .bytes
                .checked_add(held.bytes)
                .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
            counts.branches = counts
                .branches
                .checked_add(u64::from(fingerprint.is_some()))
                .assured("branches a node holds in memory are fewer than 2^64");
            if skip > 0 && skip <= held.rows {
                previous_branch = fingerprint;
            }
            let skipped = skip.min(held.rows);
            skip = skip
                .checked_sub(skipped)
                .verified("skipped is the smaller of skip and the branch's rows");
            let copied = held.kind.copied();
            if copied > 0 {
                // A branch copies within the rows and bytes it was asked for, so an answer beyond
                // them breaks the read contract rather than the page bounds.
                rows_left = rows_left
                    .checked_sub(copied)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Branch))?;
                bytes_left = bytes_left
                    .checked_sub(held.copied_bytes)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Branch))?;
            }
            copies.push(CopiedBranch {
                key: branch.key,
                held,
            });
        }
        if start > counts.rows || after.is_some_and(|(expected, _)| expected != previous_branch) {
            return Err(Report::new(ProcessorStatePageError::Position));
        }
        let copied = CopiedPage {
            copies,
            start,
            counts,
            generation,
            digest,
        };
        self.held_rows_page(
            executor,
            processor.kind,
            bounds.reply,
            bounds.rows != 0,
            copied,
        )
        .await
    }

    /// The page of the rows `copied` holds for a `kind` unit: admitted to the bulk memory the
    /// page holds, those it keeps gathered into columns, and what it set aside for `reply`. A read
    /// that `returns_rows` admits rows; a count-only read admits none.
    async fn held_rows_page(
        &self,
        executor: &Executor,
        kind: ModelKind,
        reply: PageReply,
        returns_rows: bool,
        copied: CopiedPage,
    ) -> error_stack::Result<ProcessorStatePage, ProcessorStatePageError> {
        let CopiedPage {
            copies,
            start,
            counts,
            generation,
            digest,
        } = copied;
        let sections = held_row_sections(kind)?;
        let memory = HeldRowsMemory::new(executor, reply, sections, returns_rows)?;
        let held = self
            .processor_state_sections(executor, kind, copies, memory)
            .await?;
        let offset = start
            .checked_add(held.admitted.returned)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Position))?;
        let (rows_memory, reply) = held.memory.finish()?;
        let gathered = held.gathered;
        Ok(ProcessorStatePage {
            partition_catalog: nervix_models::StateInspectionKeyCatalog::Unrequested,
            fingerprint: digest.finish(),
            generation,
            counts,
            offset,
            last_branch: held.admitted.last_branch,
            more: offset < counts.rows,
            runs: gathered.runs,
            sections: gathered.sections,
            memory: Some(ProcessorStatePageMemory {
                _reservation: rows_memory,
                _windows: Vec::new(),
                reply,
            }),
        })
    }

    /// Admits the copied rows to `memory` and gathers those it keeps into Arrow columns per
    /// section, on the bulk executor, before any column is built.
    async fn processor_state_sections(
        &self,
        executor: &Executor,
        kind: ModelKind,
        mut copies: Vec<CopiedBranch>,
        memory: HeldRowsMemory,
    ) -> error_stack::Result<HeldSections, ProcessorStatePageError> {
        let HeldRowsMemory {
            memory: charge,
            reply,
            bodies,
        } = memory;
        executor
            .run_cpu(CpuClass::Bulk, charge, move |charge, cancellation| {
                cancellation
                    .check()
                    .change_context(ProcessorStatePageError::Admission)?;
                let mut memory = HeldRowsMemory {
                    memory: charge,
                    reply,
                    bodies,
                };
                let admitted = memory.admit_copies(&mut copies, cancellation)?;
                let gathered = match kind {
                    ModelKind::Reorderer => reorderer_sections(copies)?,
                    ModelKind::Correlator => correlator_sections(copies)?,
                    _ => return Err(Report::new(ProcessorStatePageError::Columns)),
                };
                Ok(HeldSections {
                    gathered,
                    memory,
                    admitted,
                })
            })
            .await
            .change_context(ProcessorStatePageError::Admission)?
    }
}

/// What reading a unit's selected branches came to before their rows are admitted to a page:
/// every branch's copied rows, the offset the page starts at, the complete counts, the newest
/// incarnation and the fingerprint so far.
struct CopiedPage {
    copies: Vec<CopiedBranch>,
    start: u64,
    counts: StateInspectionCounts,
    generation: u64,
    digest: nervix_crypto::digest::Sha256,
}

/// The columns of a page of held rows, the memory that holds them, and what admitting its rows
/// left it with.
struct HeldSections {
    gathered: GatheredSections,
    memory: HeldRowsMemory,
    admitted: AdmittedRows,
}

/// How many sections a page of `kind` carries its held rows in: a reorderer's pending rows, or a
/// correlator's left and right rows.
fn held_row_sections(kind: ModelKind) -> error_stack::Result<u64, ProcessorStatePageError> {
    let sections = match kind {
        ModelKind::Reorderer => [ProcessorStateSection::Pending].len(),
        ModelKind::Correlator => [ProcessorStateSection::Left, ProcessorStateSection::Right].len(),
        _ => return Err(Report::new(ProcessorStatePageError::Columns)),
    };
    Ok(u64::try_from(sections).assured("a section count fits u64"))
}

/// The bulk memory of a page of held rows: the bytes of the rows' gathered columns and, for a page
/// another node reads, what encoding its reply takes, set aside as the rows are admitted.
struct HeldRowsMemory {
    memory: Reservation,
    reply: PageReply,
    /// The bytes set aside so far for the bodies of a reply to another node.
    bodies: u64,
}

/// What admitting the copied rows left a page with.
struct AdmittedRows {
    returned: u64,
    last_branch: Option<BranchKeyFingerprint>,
}

impl HeldRowsMemory {
    /// The memory of a page whose rows fall into `sections` sections. A page another node reads
    /// sets aside, before any row, the bound on its descriptor and, when it can return rows, the
    /// framing of each section's body.
    fn new(
        executor: &Executor,
        reply: PageReply,
        sections: u64,
        returns_rows: bool,
    ) -> error_stack::Result<Self, ProcessorStatePageError> {
        let mut page = Self {
            memory: executor.charge_nothing(MemoryClass::Bulk),
            reply,
            bodies: 0,
        };
        if let PageReply::Remote {
            descriptor,
            framing,
        } = reply
        {
            page.reserve(descriptor)?;
            if returns_rows {
                let framing = framing
                    .checked_mul(sections)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
                page.reserve(framing)?;
                page.bodies = framing;
            }
        }
        Ok(page)
    }

    /// Takes `bytes` more, as a charge the page requires: drawn from the page's allowance, a share
    /// without room for it runs the read again within a larger one.
    fn reserve(&mut self, bytes: u64) -> error_stack::Result<(), ProcessorStatePageError> {
        let total = self
            .memory
            .bytes()
            .checked_add(bytes)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        self.memory
            .grow_to(total)
            .change_context(ProcessorStatePageError::Admission)
    }

    /// Admits the copied rows in page order while the page's share has room for each now, and
    /// keeps only those. Every row after the page's first is optional, so the page ends before the
    /// first row the share has no room for, with its complete counts. The first row is required: a
    /// share without room for it runs the read again within a larger one.
    fn admit_copies(
        &mut self,
        copies: &mut [CopiedBranch],
        cancellation: &Cancellation,
    ) -> error_stack::Result<AdmittedRows, ProcessorStatePageError> {
        let mut admitted = AdmittedRows {
            returned: 0,
            last_branch: None,
        };
        let mut full = false;
        for copy in copies.iter_mut() {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            let mut kept = 0_usize;
            if !full {
                for cost in copy.held.kind.row_costs() {
                    if !self.admit(cost, admitted.returned == 0)? {
                        full = true;
                        break;
                    }
                    kept = kept
                        .checked_add(1)
                        .assured("a page keeps no more rows than were copied");
                    admitted.returned = admitted
                        .returned
                        .checked_add(1)
                        .assured("a page returns fewer rows than its unit holds");
                }
            }
            copy.held.kind.truncate(kept);
            if kept > 0 {
                admitted.last_branch = copy.key.as_ref().map(BranchKey::fingerprint);
            }
        }
        Ok(admitted)
    }

    /// Admits one row whose columns, and for another node its body, take `cost`. A page's `first`
    /// row is required; every later row joins only while the share has room for it now.
    fn admit(
        &mut self,
        cost: HeldRowCost,
        first: bool,
    ) -> error_stack::Result<bool, ProcessorStatePageError> {
        let body = match self.reply {
            PageReply::Local => 0,
            PageReply::Remote { .. } => cost.body,
        };
        let charge = cost
            .columns
            .checked_add(body)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        let target = self
            .memory
            .bytes()
            .checked_add(charge)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        if first {
            self.memory
                .grow_to(target)
                .change_context(ProcessorStatePageError::Admission)?;
        } else if self.memory.try_grow_to(target).is_err() {
            return Ok(false);
        }
        self.bodies = self
            .bodies
            .checked_add(body)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
        Ok(true)
    }

    /// The page's memory split into what its rows hold and, for a reply to another node, what it
    /// set aside for encoding the reply.
    fn finish(
        self,
    ) -> error_stack::Result<(Reservation, Option<ReplyMemory>), ProcessorStatePageError> {
        match self.reply {
            PageReply::Local => Ok((self.memory, None)),
            PageReply::Remote {
                descriptor,
                framing,
            } => {
                let (descriptor, rest) = self
                    .memory
                    .split(descriptor)
                    .change_context(ProcessorStatePageError::Admission)?;
                let (bodies, rest) = rest
                    .split(self.bodies)
                    .change_context(ProcessorStatePageError::Admission)?;
                let reply = ReplyMemory {
                    descriptor,
                    bodies,
                    framing,
                };
                Ok((rest, Some(reply)))
            }
        }
    }
}

/// Appends one returned row to the run it continues, or opens the next run.
pub(super) fn extend_runs(
    runs: &mut Vec<ProcessorStateRun>,
    section: ProcessorStateSection,
    branch: &Option<BranchKey>,
) {
    if let Some(last) = runs.last_mut()
        && last.section == section
        && last.branch.as_ref().map(BranchKey::fingerprint)
            == branch.as_ref().map(BranchKey::fingerprint)
    {
        last.rows = last
            .rows
            .checked_add(1)
            .assured("a run counts rows of one bounded page");
        return;
    }
    runs.push(ProcessorStateRun {
        section,
        branch: branch.clone(),
        rows: 1,
    });
}

fn reorderer_sections(
    copies: Vec<CopiedBranch>,
) -> error_stack::Result<GatheredSections, ProcessorStatePageError> {
    let mut held_rows = 0_u64;
    let mut key_fields = Vec::new();
    let mut runs = Vec::new();
    let mut rows = Vec::<&HeldReordererRow>::new();
    for copy in &copies {
        held_rows = held_rows
            .checked_add(copy.held.rows)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
        let HeldRowsKind::Reorderer {
            key_fields: fields,
            rows: held,
        } = &copy.held.kind
        else {
            return Err(Report::new(ProcessorStatePageError::Columns));
        };
        if key_fields.is_empty() {
            key_fields.clone_from(fields);
        }
        for row in held {
            extend_runs(&mut runs, ProcessorStateSection::Pending, &copy.key);
            rows.push(row);
        }
    }
    let payload = payload_columns(rows.iter().map(|row| &row.row))?;
    // The output route, every order key, the received time and the release deadline.
    let appended_columns = key_fields
        .len()
        .checked_add(3)
        .assured("a key program's fields are far fewer than usize::MAX");
    let mut appended = Vec::with_capacity(appended_columns);
    if !rows.is_empty() {
        let output: ArrayRef = StdArc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.output),
        ));
        appended.push(output);
        for (index, field) in key_fields.iter().enumerate() {
            let mut parts = Vec::with_capacity(rows.len());
            for row in &rows {
                let Some(part) = row.key.get(index) else {
                    return Err(Report::new(ProcessorStatePageError::Columns));
                };
                parts.push(part);
            }
            let column =
                ReorderKeyPart::typed_column(field.data_type(), parts.into_iter(), rows.len())
                    .change_context(ProcessorStatePageError::Columns)?;
            appended.push(column);
        }
        appended.push(timestamp_column(rows.iter().map(|row| row.received_at)));
        appended.push(timestamp_column(rows.iter().map(|row| row.release_at)));
    }
    Ok(GatheredSections {
        runs,
        sections: vec![ProcessorStateSectionColumns {
            section: ProcessorStateSection::Pending,
            rows: held_rows,
            payload,
            appended,
            key_fields,
        }],
    })
}

fn correlator_sections(
    copies: Vec<CopiedBranch>,
) -> error_stack::Result<GatheredSections, ProcessorStatePageError> {
    let mut left_rows = 0_u64;
    let mut right_rows = 0_u64;
    let mut runs = Vec::new();
    let mut left = Vec::<&HeldCorrelatorRow>::new();
    let mut right = Vec::<&HeldCorrelatorRow>::new();
    for copy in &copies {
        let HeldRowsKind::Correlator {
            left: held_left,
            rows: held,
        } = &copy.held.kind
        else {
            return Err(Report::new(ProcessorStatePageError::Columns));
        };
        let held_right = copy
            .held
            .rows
            .checked_sub(*held_left)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
        left_rows = left_rows
            .checked_add(*held_left)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
        right_rows = right_rows
            .checked_add(held_right)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
        for row in held {
            match row.side {
                CorrelatorSide::Left => {
                    extend_runs(&mut runs, ProcessorStateSection::Left, &copy.key);
                    left.push(row);
                }
                CorrelatorSide::Right => {
                    extend_runs(&mut runs, ProcessorStateSection::Right, &copy.key);
                    right.push(row);
                }
            }
        }
    }
    let mut sections = Vec::with_capacity(ProcessorStateSection::FIXED_SECTION_COUNT);
    for (section, held_rows, rows) in [
        (ProcessorStateSection::Left, left_rows, left),
        (ProcessorStateSection::Right, right_rows, right),
    ] {
        let payload = payload_columns(rows.iter().map(|row| &row.row))?;
        let appended = if rows.is_empty() {
            Vec::new()
        } else {
            vec![
                timestamp_column(rows.iter().map(|row| row.received_at)),
                timestamp_column(rows.iter().map(|row| row.timeout_at)),
            ]
        };
        sections.push(ProcessorStateSectionColumns {
            section,
            rows: held_rows,
            payload,
            appended,
            key_fields: Vec::new(),
        });
    }
    Ok(GatheredSections { runs, sections })
}

/// Takes the selected rows' payload values column by column from the shared batches they live in.
/// Only the selected values are copied; the source batches are read, never rebuilt.
pub(super) fn payload_columns<'a>(
    rows: impl Iterator<Item = &'a RuntimeRow>,
) -> error_stack::Result<Vec<ArrayRef>, ProcessorStatePageError> {
    let rows = rows.collect::<Vec<_>>();
    let Some(first) = rows.first() else {
        return Ok(Vec::new());
    };
    let columns = first.batch().batch().num_columns();
    let indices = rows
        .iter()
        .enumerate()
        .map(|(source, row)| (source, row.row_index()))
        .collect::<Vec<_>>();
    let mut gathered = Vec::with_capacity(columns);
    for column in 0..columns {
        let mut sources = Vec::with_capacity(rows.len());
        for row in &rows {
            let batch = row.batch().batch();
            if batch.num_columns() != columns {
                return Err(Report::new(ProcessorStatePageError::Columns));
            }
            sources.push(batch.column(column).as_ref());
        }
        let values = arrow_select::interleave::interleave(&sources, &indices)
            .change_context(ProcessorStatePageError::Columns)?;
        gathered.push(values);
    }
    Ok(gathered)
}

/// A datetime column: nanoseconds in UTC, exactly as a Plexus `DATETIME` is carried.
pub(super) fn timestamp_column(values: impl Iterator<Item = Timestamp>) -> ArrayRef {
    let column = TimestampNanosecondArray::from_iter_values(values.map(Timestamp::unix_nanos))
        .with_timezone("+00:00");
    StdArc::new(column)
}

#[cfg(test)]
#[path = "processor_state_inspection_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "processor_state_inspection_shuttle_tests.rs"]
mod shuttle_tests;
