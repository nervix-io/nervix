//! Layer: data plane.
//! Owns: typed failure contexts for capturing, validating, and activating runtime state handoffs,
//! and the rejection a peer receives for one.
//! May depend on: runtime state persistence errors, the model vocabulary, the interconnect's
//! handoff failure envelope, and `error-stack` reports.
//! Must not know: schedules, consensus transactions, NSPL, or edge protocols.

use error_stack::Report;
use nervix_interconnect::OwnershipHandoffFailure;
use nervix_models::{ModelKind, ModelName};
use thiserror::Error;

use super::RuntimePersistenceError;

pub(crate) type OwnershipHandoffResult<T> = Result<T, Report<OwnershipHandoffError>>;

#[derive(Debug, Error)]
pub(crate) enum OwnershipHandoffError {
    #[error("ownership handoff checkpoint failed: {0}")]
    Checkpoint(String),
    #[error("ownership handoff participant validation failed: {0}")]
    Participant(String),
    #[error("ownership handoff schedule validation failed: {0}")]
    Schedule(String),
    #[error("ownership handoff state validation failed: {0}")]
    State(String),
    #[error("ownership handoff transport failed: {0}")]
    Transport(String),
    #[error("ownership handoff deadline elapsed: {0}")]
    Deadline(String),
    #[error("ownership handoff WASM restore failed for wasm processor '{}'", .processor.as_str())]
    WasmRestore { processor: ModelName },
    #[error("ownership handoff checkpoint failed for wasm processor '{}'", .processor.as_str())]
    WasmCheckpoint { processor: ModelName },
    #[error(
        "ownership handoff cannot place the runtime state of {} '{}'",
        .kind.as_str(),
        .identifier.as_str()
    )]
    StatePlacement {
        kind: ModelKind,
        identifier: ModelName,
    },
    #[error(transparent)]
    Persistence(#[from] RuntimePersistenceError),
}

impl OwnershipHandoffError {
    pub(in crate::runtime) fn checkpoint(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Checkpoint(reason.into()))
    }

    pub(crate) fn participant(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Participant(reason.into()))
    }

    pub(crate) fn schedule(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Schedule(reason.into()))
    }

    pub(in crate::runtime) fn state(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::State(reason.into()))
    }

    pub(crate) fn transport(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Transport(reason.into()))
    }

    pub(crate) fn deadline(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Deadline(reason.into()))
    }

    /// The rejection a peer receives for `report`, carrying every context the report accumulated
    /// instead of only its outermost one.
    pub(crate) fn remote_rejection(report: &Report<Self>) -> OwnershipHandoffFailure {
        OwnershipHandoffFailure::rejected(format!("{report:#}"))
    }

    pub(crate) fn persistence(error: RuntimePersistenceError) -> Report<Self> {
        Report::new(Self::Persistence(error))
    }
}
