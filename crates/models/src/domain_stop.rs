//! Why a committed stop ended a domain.
//!
//! Layer: vocabulary.
//! - **Owns.** The cause a domain's committed stop records: an operator's stop or the licensing
//!   stop gate's.
//! - **Depends on.** The licensing stop reason and serialization.
//! - **Must not know.** Consensus application, lifecycle execution or policy evaluation.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Why a domain stopped, as the stop that committed it records it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum DomainStopCause {
    /// An operator's `STOP`, a transaction's `STOP` step, or the rollback of a `START` whose
    /// application failed.
    Operator,
    /// The licensing stop gate closed while the domain ran (specification §10.7).
    Licensing(crate::LicensingStopReason),
}
