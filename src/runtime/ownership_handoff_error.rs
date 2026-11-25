//! Layer: data plane.
//! Owns: typed failure contexts for capturing, validating, and activating runtime state handoffs,
//! and the rejection a peer receives for one.
//! May depend on: the model vocabulary, the interconnect's handoff failure envelope, and
//! `error-stack` reports, which keep a runtime state persistence failure beneath them.
//! Must not know: schedules, consensus transactions, Plexus, or edge protocols.

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
    /// A forced recovery's budget passed before every live node it waited for answered. The
    /// recovery fails and every replica keeps the state it holds.
    #[error("the forced-recovery budget passed before every live replica answered")]
    BudgetPassed,
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
    #[error("ingestor instance layout state could not be prepared")]
    IngestorUnitLayout,
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

    /// Whether the failure is an operation budget passing while the nodes it waited for lived:
    /// the operation fails, and nothing they hold is reset because of it.
    pub(crate) const fn is_budget_expiry(&self) -> bool {
        matches!(
            self,
            Self::Deadline(_) | Self::ScheduleInstallationDeadline { .. } | Self::BudgetPassed
        )
    }

    /// The rejection a peer receives for `report`, carrying every context the report accumulated
    /// instead of only its outermost one. A terminating destination's refusal and a passed budget
    /// stay typed, so the coordinator can plan the move onto another node, or fail the operation
    /// without resetting what live replicas hold.
    pub(crate) fn remote_rejection(report: &Report<Self>) -> OwnershipHandoffFailure {
        let context = report.current_context();
        if let Self::DestinationTerminating { node } = context {
            return OwnershipHandoffFailure::DestinationTerminating { node: node.clone() };
        }
        if context.is_budget_expiry() {
            return OwnershipHandoffFailure::BudgetPassed;
        }
        OwnershipHandoffFailure::rejected(format!("{report:#}"))
    }

    /// The report of a rejection a peer answered with: the inverse of [`Self::remote_rejection`].
    pub(crate) fn of_remote_rejection(failure: OwnershipHandoffFailure) -> Report<Self> {
        match failure {
            OwnershipHandoffFailure::DestinationTerminating { node } => {
                Report::new(Self::DestinationTerminating { node })
            }
            OwnershipHandoffFailure::BudgetPassed => Report::new(Self::BudgetPassed),
            OwnershipHandoffFailure::Rejected { reason } => Self::participant(reason),
        }
    }
}
