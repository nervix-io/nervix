//! Shared licensing quota values.
//!
//! Layer: vocabulary.
//! - **Owns.** The bounded and unlimited quota value and its arithmetic.
//! - **Depends on.** Serialization and primitive value types.
//! - **Must not know.** Policies, cryptography, Models, consensus or network I/O.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns shared licensing words")
)]

use error_stack::Report;
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Quota {
    Bounded { value: u64 },
    Unlimited,
}

/// The cluster quotas whose usage has an authoritative consensus owner.
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
pub enum QuotaName {
    #[serde(rename = "cluster.nodes")]
    ClusterNodes,
    #[serde(rename = "cluster.domains")]
    ClusterDomains,
}

impl QuotaName {
    pub const ALL: [Self; 2] = [Self::ClusterNodes, Self::ClusterDomains];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClusterNodes => "cluster.nodes",
            Self::ClusterDomains => "cluster.domains",
        }
    }
}

impl std::fmt::Display for QuotaName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// An immutable policy constraint captured for one admission and retained by its durable effect.
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
pub struct QuotaConstraint {
    pub name: QuotaName,
    pub quota: Quota,
}

impl QuotaConstraint {
    /// Free policy has no replacement grants: every newly occupied slot uses the same quota.
    /// An unlimited quota admits without summing counts it does not constrain.
    pub fn admit(
        self,
        current_usage: u64,
        requested: u64,
        _replacement: bool,
    ) -> Result<Self, Report<LicensingRefusal>> {
        match self.quota {
            Quota::Unlimited => Ok(self),
            Quota::Bounded { value } => {
                if current_usage
                    .checked_add(requested)
                    .is_some_and(|total| total <= value)
                {
                    Ok(self)
                } else {
                    Err(Report::new(LicensingRefusal {
                        condition: self.name,
                        limit: value,
                        current_usage,
                        requested,
                    }))
                }
            }
        }
    }
}

/// A quota owner preserves this cause while adding its own admission context.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Error,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[error(
    "licensing quota '{condition}' refused usage: limit {limit}, current {current_usage}, \
     requested {requested} (over_limit)"
)]
pub struct LicensingRefusal {
    pub condition: QuotaName,
    pub limit: u64,
    pub current_usage: u64,
    pub requested: u64,
}

impl LicensingRefusal {
    pub const fn state_label(self) -> &'static str {
        "over_limit"
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
    fn quota_admission_keeps_unlimited_zero_last_slot_and_overflow_distinct() {
        assert!(Quota::Unlimited.admits(u64::MAX));
        let constraint = |quota| QuotaConstraint {
            name: QuotaName::ClusterDomains,
            quota,
        };
        assert!(
            constraint(Quota::Unlimited)
                .admit(u64::MAX, u64::MAX, false)
                .is_ok()
        );
        assert!(
            constraint(Quota::Bounded { value: 0 })
                .admit(0, 0, false)
                .is_ok()
        );
        let refused = constraint(Quota::Bounded { value: 0 })
            .admit(0, 1, false)
            .unwrap_err();
        assert_eq!(
            *refused.current_context(),
            LicensingRefusal {
                condition: QuotaName::ClusterDomains,
                limit: 0,
                current_usage: 0,
                requested: 1
            }
        );
        let last = constraint(Quota::Bounded { value: 2 });
        assert!(matches!(last.admit(1, 1, false), Ok(value) if value == last));
        assert!(matches!(last.admit(1, 1, true), Ok(value) if value == last));
        assert!(last.admit(2, 1, true).is_err());
        assert!(
            constraint(Quota::Bounded { value: u64::MAX })
                .admit(u64::MAX, 1, false)
                .is_err()
        );
    }
}
