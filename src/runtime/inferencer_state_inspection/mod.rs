//! Layer: data plane.
//! Owns: bounded Arrow pages over immutable inference tensor generations and captured progress.
//! Depends on: installed plans, publication and replication handles, typed selections and bulk admission.
//! Must not know: Models, Plexus, Row frames, sessions, rendering or mutable ONNX execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns native inference inspection pages")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        observer,
        reason = "operator reads retain existing immutable inference generations"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "reads installed inference state plans only")
)]

use std::collections::BTreeSet;

use arrow_array::builder::{Float32Builder, ListBuilder, StringBuilder, UInt64Builder};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Cancellation, CpuClass, MemoryClass, Reservation};
use nervix_models::{
    BranchKeyFingerprint, ClusterNodeName, DomainName, ModelKind, NodeRef, NodeUnitRef,
    ProcessorStateSection, StateInspectionBranchSelection, StateInspectionCounts, Unit,
};
use nervix_primitives::sync::StdArc;

use super::{
    BranchKey, RelayProcessorOperationTemplate, Runtime, RuntimeStateKind,
    inferencer_state::{InferencerGeneration, ReplicatedInferencerState},
    processor_state_inspection::{
        ProcessorStatePage, ProcessorStatePageBounds, ProcessorStatePageError,
        ProcessorStatePageMemory, ProcessorStateRun, ProcessorStateSectionColumns,
        ProcessorStateSelection, extend_runs,
    },
    published_generation::Generation,
};
use crate::registry::{InferencerStatePlan, InferencerStatePortPlan};

mod capture;
mod columns;
mod page;

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
mod tests;

type ReadResult<T> = error_stack::Result<T, ProcessorStatePageError>;

#[derive(Clone)]
struct CapturedBranch {
    key: Option<BranchKey>,
    registration: [u8; 16],
    generation: StdArc<Generation<Option<InferencerGeneration>>>,
    persisted: u64,
    confirmed: u64,
    replicas: u64,
}

#[derive(Clone, Copy, strum::AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
enum PersistenceStage {
    Published,
    Persisted,
    ReplicaConfirmed,
}

impl CapturedBranch {
    fn stage(&self) -> PersistenceStage {
        if self.persisted < self.generation.revision {
            PersistenceStage::Published
        } else if self.replicas != 0 && self.confirmed == self.replicas {
            PersistenceStage::ReplicaConfirmed
        } else {
            PersistenceStage::Persisted
        }
    }

    fn value(&self) -> ReadResult<&InferencerGeneration> {
        self.generation
            .value
            .as_ref()
            .ok_or_else(|| Report::new(ProcessorStatePageError::Unrestored))
    }
}

fn reserve(memory: &mut Reservation, bytes: u64) -> ReadResult<()> {
    let total = memory
        .bytes()
        .checked_add(bytes)
        .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
    memory
        .grow_to(total)
        .change_context(ProcessorStatePageError::Admission)
}

fn checked_add(left: u64, right: u64) -> ReadResult<u64> {
    left.checked_add(right)
        .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))
}
