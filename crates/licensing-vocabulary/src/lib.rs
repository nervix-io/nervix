//! Shared licensing activation, quota and capability vocabulary.
//!
//! Layer: vocabulary.
//! - **Owns.** Activation kinds, quota values and their reduction arithmetic, capability names,
//!   decision rejections and service refusal codes.
//! - **Depends on.** Serialization and primitive value types.
//! - **Must not know.** Policies, cryptography, Models, consensus or network I/O.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns shared licensing words")
)]

mod problem;
pub use problem::ProblemCode;

mod decision;
pub use decision::{BindingField, DecisionRejection, RevisionField, TimingField};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// The lifetime class of an activation, shared by statements and signed documents.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ActivationKind {
    Persistent,
    Ephemeral,
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Quota {
    Bounded { value: u64 },
    Unlimited,
}

/// The catalog capabilities: features a license switches on, each enabled or disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CapabilityName {
    /// Changes nothing. A license enables it so that a cluster shows a verified decision reached it.
    Dummy,
}

impl CapabilityName {
    pub const ALL: [Self; 1] = [Self::Dummy];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dummy => "licensing.dummy",
        }
    }
}

impl Quota {
    pub const fn admits(self, usage: u64) -> bool {
        match self {
            Self::Unlimited => true,
            Self::Bounded { value } => usage <= value,
        }
    }

    pub const fn is_reduction_from(self, previous: Self) -> bool {
        match (previous, self) {
            (Self::Unlimited, Self::Bounded { .. }) => true,
            (Self::Bounded { value: previous }, Self::Bounded { value }) => value < previous,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_capability_name_is_its_catalog_spelling() {
        let spellings: Vec<&str> = CapabilityName::ALL
            .into_iter()
            .map(CapabilityName::as_str)
            .collect();
        assert_eq!(spellings, ["licensing.dummy"]);
    }
}
