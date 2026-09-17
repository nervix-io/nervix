//! Durable exclusion for mutations that change one domain's control-plane state.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The replicated mutation owner, its recovery fence, and the pure admission decision.
//! - **Depends on.** Vocabulary identities used by commands and transactions.
//! - **Must not know.** Sessions, runtime activation, scheduling policy, or mutation execution.

use std::fmt;

use nervix_models::{CommandExecutionReference, DomainName};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The durable execution whose work may mutate a domain.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum DomainMutationOwner {
    Command(CommandExecutionReference),
    Transaction(String),
}

impl DomainMutationOwner {
    pub fn command(reference: CommandExecutionReference) -> Self {
        Self::Command(reference)
    }

    pub fn transaction(id: String) -> Self {
        Self::Transaction(id)
    }

    pub fn is_transaction(&self) -> bool {
        matches!(self, Self::Transaction(_))
    }
}

impl fmt::Display for DomainMutationOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Command(reference) => write!(formatter, "command '{reference}'"),
            Self::Transaction(id) => write!(formatter, "transaction '{id}'"),
        }
    }
}

/// The committed log position that distinguishes one ownership acquisition from every later one.
#[derive(
    Debug,
    Clone,
    Copy,
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
pub struct DomainMutationRecoveryFence(u64);

impl DomainMutationRecoveryFence {
    pub(crate) fn at_revision(revision: u64) -> Self {
        Self(revision)
    }

    pub fn revision(self) -> u64 {
        self.0
    }
}

/// Authority to publish mutations for one domain until the owning execution reaches a terminal
/// outcome.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainMutationLease {
    owner: DomainMutationOwner,
    recovery_fence: DomainMutationRecoveryFence,
}

impl DomainMutationLease {
    fn new(owner: DomainMutationOwner, recovery_fence: DomainMutationRecoveryFence) -> Self {
        Self {
            owner,
            recovery_fence,
        }
    }

    pub fn owner(&self) -> &DomainMutationOwner {
        &self.owner
    }

    pub fn recovery_fence(&self) -> DomainMutationRecoveryFence {
        self.recovery_fence
    }
}

/// The complete result of admitting one owner against the current durable lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DomainMutationAdmission {
    Acquired(DomainMutationLease),
    Joined(DomainMutationLease),
    Conflict(DomainMutationLease),
}

/// A replicated mutation request that does not hold the domain's authoritative lease.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum DomainMutationError {
    #[error("domain '{domain}' mutation is owned by {owner}")]
    Conflict {
        domain: DomainName,
        owner: DomainMutationOwner,
    },
    #[error("domain '{domain}' mutation lease for {owner} is no longer authoritative")]
    FenceLost {
        domain: DomainName,
        owner: DomainMutationOwner,
    },
}

impl DomainMutationAdmission {
    pub(crate) fn decide(
        current: Option<&DomainMutationLease>,
        requested_owner: &DomainMutationOwner,
        recovery_fence: DomainMutationRecoveryFence,
    ) -> Self {
        match current {
            None => Self::Acquired(DomainMutationLease::new(
                requested_owner.clone(),
                recovery_fence,
            )),
            Some(current) if current.owner() == requested_owner => Self::Joined(current.clone()),
            Some(current) => Self::Conflict(current.clone()),
        }
    }

    pub(crate) fn lease(&self) -> &DomainMutationLease {
        match self {
            Self::Acquired(lease) | Self::Joined(lease) | Self::Conflict(lease) => lease,
        }
    }

    pub(crate) fn into_admitted(self) -> Option<DomainMutationLease> {
        match self {
            Self::Acquired(lease) | Self::Joined(lease) => Some(lease),
            Self::Conflict(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    fn command_owner(reference: &str) -> DomainMutationOwner {
        DomainMutationOwner::command(
            CommandExecutionReference::parse(reference)
                .assured("the test command reference is an accepted literal"),
        )
    }

    #[test]
    fn admission_acquires_joins_and_fences_conflicting_owners() {
        let first_owner = command_owner("request-1");
        let second_owner = command_owner("request-2");
        let first_fence = DomainMutationRecoveryFence::at_revision(11);
        let later_fence = DomainMutationRecoveryFence::at_revision(19);

        let acquired = DomainMutationAdmission::decide(None, &first_owner, first_fence);
        assert!(matches!(acquired, DomainMutationAdmission::Acquired(_)));
        let lease = acquired
            .into_admitted()
            .assured("an acquisition returns its new lease");

        let joined = DomainMutationAdmission::decide(Some(&lease), &first_owner, later_fence);
        assert!(matches!(joined, DomainMutationAdmission::Joined(_)));
        assert_eq!(joined.lease().recovery_fence(), first_fence);

        let conflict = DomainMutationAdmission::decide(Some(&lease), &second_owner, later_fence);
        assert!(matches!(conflict, DomainMutationAdmission::Conflict(_)));
        assert_eq!(conflict.lease(), &lease);
    }
}
