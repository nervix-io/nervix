//! The identities transaction inspection and preview-fenced commits share.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The transaction an inspection reads, the status and report it answers with, and
//!   the identity of the preview a commit expects to apply.
//! - **Depends on.** The impact report identities.
//! - **Must not know.** How a transaction is inspected, planned, persisted, transported or
//!   presented.

use error_stack::Report;
use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, IntoStaticStr};
use thiserror::Error;

use super::{
    ImpactPlanningBasis, TransactionImpactReport, TransactionOperationNumber, TransactionPosition,
};
use crate::DomainName;

/// Which transaction an inspection reads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionInspectionTarget {
    /// The transaction bound to the inspecting session.
    Attached,
    /// A transaction named by identity. Inspecting it never attaches it, adopts its domain,
    /// touches its activity time or changes its queue position.
    Transaction { transaction_id: String },
}

/// One side-effect-free read of a transaction's impact report.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionInspectionRequest {
    pub target: TransactionInspectionTarget,
    /// The accepted operation the reader focuses on. Absent reads the whole transaction.
    pub operation: Option<TransactionOperationNumber>,
}

/// Where a replicated transaction is in its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionLifecycle {
    Open,
    Committing,
    Committed,
    Failed {
        /// The operation whose execution step failed.
        failing_operation: TransactionOperationNumber,
        error: String,
    },
    Reverted,
    Expired,
}

impl TransactionLifecycle {
    /// Whether the transaction can still change: it is open or committing.
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Open | Self::Committing)
    }
}

/// A replicated transaction as the serving leader records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionStatus {
    transaction_id: String,
    domain: DomainName,
    lifecycle: TransactionLifecycle,
    accepted_operations: TransactionPosition,
    applied_operations: usize,
}

impl TransactionStatus {
    pub fn new(
        transaction_id: String,
        domain: DomainName,
        lifecycle: TransactionLifecycle,
        accepted_operations: TransactionPosition,
        applied_operations: usize,
    ) -> Result<Self, Report<TransactionStatusError>> {
        if applied_operations > accepted_operations.accepted_operations() {
            return Err(Report::new(
                TransactionStatusError::AppliedOperationsExceedAccepted {
                    applied: applied_operations,
                    accepted: accepted_operations.accepted_operations(),
                },
            ));
        }
        Ok(Self {
            transaction_id,
            domain,
            lifecycle,
            accepted_operations,
            applied_operations,
        })
    }

    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    pub fn domain(&self) -> &DomainName {
        &self.domain
    }

    pub const fn lifecycle(&self) -> &TransactionLifecycle {
        &self.lifecycle
    }

    /// Operations accepted into the transaction, which is also the position the next append
    /// expects.
    pub const fn accepted_operations(&self) -> TransactionPosition {
        self.accepted_operations
    }

    /// Accepted operations whose execution steps have applied.
    pub const fn applied_operations(&self) -> usize {
        self.applied_operations
    }

    /// Accepted operations still waiting to apply. A finished transaction has none.
    pub fn pending_operations(&self) -> usize {
        if !self.lifecycle.is_active() {
            return 0;
        }
        self.accepted_operations
            .accepted_operations()
            .checked_sub(self.applied_operations)
            .verified("construction rejects applied operations above accepted operations")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransactionStatusError {
    #[error("{applied} applied operations exceed {accepted} accepted operations")]
    AppliedOperationsExceedAccepted { applied: usize, accepted: usize },
}

/// A transaction's impact report, read without attaching or changing the transaction.
///
/// The report always describes the whole transaction. `operation` names the accepted operation
/// the reader asked about, so a presentation can lead with that operation's contribution and the
/// execution step containing it without the report itself losing topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionInspection {
    pub transaction: TransactionStatus,
    /// The inspected operation, when the request selected one.
    pub operation: Option<TransactionOperationNumber>,
    pub report: TransactionImpactReport,
}

/// Why an inspection was refused.
///
/// Every variant means nothing was read and nothing changed. A refusal is never a partial report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, AsRefStr, IntoStaticStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionInspectionRejection {
    /// The request read the attached transaction, and the session has none.
    NoAttachedTransaction,
    /// No transaction has that identity, or its retention already reclaimed it.
    TransactionNotFound,
    /// The transaction belongs to another user.
    NotOwner,
    /// The selected operation number is past the operations the transaction accepted.
    OperationNotFound,
    /// The transaction exists, but no coherent report can be read for it right now.
    ReportUnavailable,
}

impl TransactionInspectionRejection {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// The whole-transaction preview a commit expects to apply.
///
/// The identity covers the transaction, the accepted-operation position the preview described and
/// the planning basis it was planned from, so a commit can refuse a preview that no longer
/// describes the transaction before any of its effects apply.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct TransactionPreviewIdentity {
    pub transaction_id: String,
    pub position: TransactionPosition,
    pub planning_basis: ImpactPlanningBasis,
}

/// The stable operation number and whole-transaction preview returned by an accepted append.
///
/// An exact retry returns this same value, allowing a client to advance its queue position and
/// commit basis from the one durable admission result.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionOperationAdmission {
    pub operation: TransactionOperationNumber,
    pub preview: TransactionPreviewIdentity,
}
