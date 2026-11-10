//! Typed refusals of the shared licensing decision contract.
//!
//! Layer: vocabulary.
//! - **Owns.** The semantic field and failure identities a decision verifier returns.
//! - **Depends on.** Serialization and typed error declarations.
//! - **Must not know.** Verification, cryptography, consensus, Models or network requests.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum BindingField {
    Activation,
    Cluster,
    Kind,
    Key,
    Generation,
    Expiry,
    Creation,
    Request,
    License,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum RevisionField {
    Decision,
    Conditions,
    IssuedAt,
    ReusedDecision,
    ReusedConditions,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum TimingField {
    RefreshOrder,
    ValidUntil,
    Term,
    Ephemeral,
    Creation,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Error,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum DecisionRejection {
    #[error("the decision envelope or signing authorization is invalid")]
    Envelope,
    #[error("decision protocol {received} is unsupported")]
    Protocol { received: u8 },
    #[error("the decision names a different licensing service")]
    Service,
    #[error("the decision does not match its committed binding: {field:?}")]
    Binding { field: BindingField },
    #[error("the decision violates monotone revision identity: {field:?}")]
    Revision { field: RevisionField },
    #[error("the decision timing is invalid: {field:?}")]
    Timing { field: TimingField },
    #[error("the decision conditions, status or acknowledged transitions are invalid")]
    Content,
}
