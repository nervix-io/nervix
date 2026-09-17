//! The identities transaction inspection and preview-fenced commits share.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The transaction an inspection reads and the identity of the preview a commit
//!   expects to apply.
//! - **Depends on.** The impact report identities.
//! - **Must not know.** How a transaction is inspected, planned, persisted, transported or
//!   presented.

use super::{ImpactPlanningBasis, TransactionPosition};

/// Which transaction an inspection reads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionInspectionTarget {
    /// The transaction bound to the inspecting session.
    Attached,
    /// A transaction named by identity. Inspecting it never attaches it, adopts its domain,
    /// touches its activity time or changes its queue position.
    Transaction { transaction_id: String },
}

/// The whole-transaction preview a commit expects to apply.
///
/// The identity covers the transaction, the accepted-operation position the preview described and
/// the planning basis it was planned from, so a commit can refuse a preview that no longer
/// describes the transaction before any of its effects apply.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionPreviewIdentity {
    pub transaction_id: String,
    pub position: TransactionPosition,
    pub planning_basis: ImpactPlanningBasis,
}
