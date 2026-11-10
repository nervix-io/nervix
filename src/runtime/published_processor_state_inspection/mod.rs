//! Bounded inspection of immutable deduplicator and window publications.
//!
//! Layer: data plane.
//! - **Owns.** Capturing one generation per branch, retained-row paging, complete counts and
//!   Arrow sections for published keys, window inputs, headers and accumulator facts.
//! - **Depends on.** Prepared processor plans, published generations, native window decoding,
//!   the production window charge and the bulk executor.
//! - **Must not know.** Models, Plexus, Row frames, sessions or presentation formats.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns published state observation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        observer,
        reason = "explicit operator reads retain immutable publications"
    )
)]

use std::collections::BTreeSet;

use arrow_array::{StringArray, TimestampNanosecondArray, UInt32Array};
use arrow_schema::{FieldRef, Schema as ArrowSchema};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Cancellation, CpuClass, MemoryClass, Reservation};
use nervix_models::{ProcessorStateSection, StateInspectionBranchSelection, StateInspectionCounts};
use nervix_primitives::sync::{Arc, StdArc};

use super::{
    BranchKey, DomainName, NodeRef, NodeUnitRef, RelayMessage, RelayProcessorOperationTemplate,
    ReorderKeyPart, Runtime, RuntimeStateKind, Timestamp, Unit, WindowAccumulatorPlan,
    WindowAggregateStorageKind, WindowArgumentColumns, checked_add_duration_to_timestamp,
    deduplicator::PublishedDeduplicatorKey,
    processor_state_inspection::{
        ProcessorStatePage, ProcessorStatePageBounds, ProcessorStatePageError,
        ProcessorStatePageMemory, ProcessorStateRun, ProcessorStateSectionColumns,
        ProcessorStateSelection, extend_runs, held_row_bytes, payload_columns, timestamp_column,
    },
    published_generation::Generation,
    window_state::{
        AdmittedWindowSnapshot, WindowAccumulatorSnapshot, WindowProcessorStateSnapshot,
        WindowPublishedSnapshot, WindowSnapshotLifetime, WindowSnapshotSchemas,
        decode_window_processor_layout, layout::WindowCheckpointLayout,
    },
    window_state_charge::WindowStateCharge,
};
use crate::runtime_ack::AckSet;

mod capture;
mod columns;
mod page;
mod window;

#[cfg(test)]
mod tests;

type ReadResult<T> = error_stack::Result<T, ProcessorStatePageError>;

/// The typed projection of the installed processor plan used by an observation.
enum ReadPlan {
    Keys {
        fields: Vec<FieldRef>,
        max_time: std::time::Duration,
    },
    Window {
        input: StdArc<ArrowSchema>,
        arguments: StdArc<ArrowSchema>,
        plan: WindowAccumulatorPlan,
        width: Option<std::time::Duration>,
    },
}

struct CapturedBranch {
    key: Option<BranchKey>,
    registration: [u8; 16],
    revision: u64,
    state: CapturedState,
}

enum CapturedState {
    Keys(StdArc<Generation<Vec<PublishedDeduplicatorKey>>>),
    Window(StdArc<Generation<Option<WindowPublishedSnapshot>>>),
}

struct OpenBranch {
    key: Option<BranchKey>,
    registration: [u8; 16],
    revision: u64,
    state: OpenState,
}

enum OpenState {
    Keys(StdArc<Generation<Vec<PublishedDeduplicatorKey>>>),
    Window(WindowRead),
}

enum WindowRead {
    Published(StdArc<Generation<Option<WindowPublishedSnapshot>>>),
    Decoded(Box<AdmittedWindowSnapshot>),
}

impl WindowRead {
    fn snapshot(&self) -> ReadResult<Option<&WindowProcessorStateSnapshot>> {
        match self {
            Self::Decoded(decoded) => Ok(Some(&decoded.snapshot)),
            Self::Published(generation) => match &generation.value {
                Some(WindowPublishedSnapshot::Live(snapshot)) => Ok(Some(snapshot)),
                None => Ok(None),
                Some(WindowPublishedSnapshot::Sealed(_)) => {
                    Err(Report::new(ProcessorStatePageError::Columns))
                }
            },
        }
    }
}

fn selected(selection: StateInspectionBranchSelection, key: &Option<BranchKey>) -> bool {
    match selection {
        StateInspectionBranchSelection::All => true,
        StateInspectionBranchSelection::Unbranched => key.is_none(),
        StateInspectionBranchSelection::Branch(wanted) => {
            key.as_ref().is_some_and(|key| key.fingerprint() == wanted)
        }
    }
}

fn reserve_item<T>(memory: &mut Reservation) -> ReadResult<()> {
    let bytes =
        u64::try_from(std::mem::size_of::<T>()).assured("a resident object's size fits u64");
    // Cover geometric Vec capacity and bounded scratch copies before a push allocates them.
    let bytes = bytes
        .checked_mul(4)
        .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
    reserve_bytes(memory, bytes)
}

fn reserve_bytes(memory: &mut Reservation, bytes: u64) -> ReadResult<()> {
    let total = memory
        .bytes()
        .checked_add(bytes)
        .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
    memory
        .grow_to(total)
        .change_context(ProcessorStatePageError::Admission)
}

/// The published window and prepared accumulator plan of one selected branch.
#[derive(Clone, Copy)]
struct WindowBranch<'a> {
    key: &'a Option<BranchKey>,
    revision: u64,
    snapshot: &'a WindowProcessorStateSnapshot,
    plan: &'a WindowAccumulatorPlan,
    width: Option<std::time::Duration>,
}

struct KeyRow {
    key: Arc<super::DeduplicatorKey>,
    seen_at: Timestamp,
}

struct WindowRow {
    entry: super::window_state::WindowEntrySnapshot,
}

struct HeaderRow {
    revision: u64,
    rows: u64,
    first: Option<u64>,
    next: u64,
    incarnation: Option<u64>,
    due: Option<Timestamp>,
    bytes: u64,
}

struct StructureRow {
    index: u32,
    kind: String,
    count: u64,
}

/// One unit's bounded page, counting metadata separately from the retained records it describes.
struct PublishedPage {
    skip: u64,
    rows_left: usize,
    bytes_left: u64,
    returned: u64,
    previous_branch: Option<nervix_models::BranchKeyFingerprint>,
    last_branch: Option<nervix_models::BranchKeyFingerprint>,
    counts: StateInspectionCounts,
    generation: u64,
    digest: nervix_crypto::digest::Sha256,
    keys: Vec<KeyRow>,
    windows: Vec<WindowRow>,
    headers: Vec<HeaderRow>,
    structures: Vec<StructureRow>,
    runs: Vec<ProcessorStateRun>,
    metadata_runs: Vec<ProcessorStateRun>,
    memory: Reservation,
}
