//! Layer: data plane.
//! Owns: typed failure contexts for capturing, validating, and activating runtime state handoffs,
//! and the rejection a peer receives for one.
//! May depend on: the model vocabulary, the interconnect's handoff failure envelope, and
//! `error-stack` reports, which keep a runtime state persistence failure beneath them.
//! Must not know: schedules, consensus transactions, NSPL, or edge protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::Report;
use nervix_interconnect::OwnershipHandoffFailure;
use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName, NodeRef};
use thiserror::Error;

pub(crate) type OwnershipHandoffResult<T> = Result<T, Report<OwnershipHandoffError>>;

#[derive(Debug, Error)]
pub(crate) enum OwnershipHandoffError {
    #[error("ownership handoff checkpoint failed: {0}")]
    Checkpoint(String),
    #[error("ownership handoff participant validation failed: {0}")]
    Participant(String),
    /// The destination's own process incarnation advertised that it is terminating, so it is not a
    /// new placement destination, whatever its coordinator's view still shows.
    #[error("destination node '{node}' is terminating")]
    DestinationTerminating { node: ClusterNodeName },
    #[error("ownership handoff schedule validation failed: {0}")]
    Schedule(String),
    #[error("ownership handoff state validation failed: {0}")]
    State(String),
    #[error("ownership handoff transport failed: {0}")]
    Transport(String),
    #[error("ownership handoff deadline elapsed: {0}")]
    Deadline(String),
    #[error(
        "forced recovery of {} '{}' in domain '{}' timed out waiting for its local schedule installation",
        .node.kind.as_str(), .node.identifier.as_str(), .domain.as_str()
    )]
    ScheduleInstallationDeadline { domain: DomainName, node: NodeRef },
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
    #[error(
        "{} '{}' has not restored the branches its lifecycle checkpoint names yet",
        .kind.as_str(),
        .identifier.as_str()
    )]
    BranchesUnrestored {
        kind: ModelKind,
        identifier: ModelName,
    },
    /// The node's runtime state storage failed; its `RuntimePersistenceError` report is beneath.
    #[error("ownership handoff state storage failed")]
    Persistence,
}

impl OwnershipHandoffError {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the typed checkpoint error conversion")
    )]
    pub(in crate::runtime) fn checkpoint(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Checkpoint(reason.into()))
    }

    pub(crate) fn participant(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Participant(reason.into()))
    }

    pub(crate) fn schedule(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Schedule(reason.into()))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the typed state error conversion")
    )]
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
}
