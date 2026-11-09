//! Pure derivation of the current cluster licensing policy.
//!
//! Layer: decisions.
//! - **Owns.** Free conditions and permission queries from a coherent committed observation.
//! - **Depends on.** Public identity/record facts and the shared condition catalog and quota words.
//! - **Must not know.** Publication, locks, async tasks, clocks, metrics or service connectivity.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns pure licensing decisions")
)]

use meticulous::OptionExt as _;
use nervix_consensus::LicensingSnapshot;
use nervix_licensing_protocol::conditions::{Capability, FREE_POLICY, UsageConditions};
use nervix_licensing_vocabulary::{LicensingRefusal, Quota, QuotaConstraint, QuotaName};
use nervix_models::Timestamp;

/// The current catalog contains no capabilities. A future catalog entry supplies its own name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityName {}

/// Current free mode has no processing-stop reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicensingPolicy {
    pub snapshot: LicensingSnapshot,
    pub conditions: UsageConditions,
}

/// Revision and usage must travel with identity/record: separate observations could mix cuts.
pub fn evaluate_policy(snapshot: LicensingSnapshot) -> LicensingPolicy {
    LicensingPolicy {
        snapshot,
        conditions: FREE_POLICY.usage(),
    }
}

pub fn next_deadline(_policy: &LicensingPolicy) -> Option<Timestamp> {
    None
}

impl LicensingPolicy {
    pub fn state_name(&self) -> &'static str {
        match self
            .snapshot
            .initialized
            .as_ref()
            .map(|value| value.record.mode)
        {
            None | Some(nervix_consensus::LicensingMode::Free) => "free",
        }
    }

    pub fn capability(&self, name: CapabilityName) -> Capability {
        match name {}
    }

    pub fn quota(&self, name: QuotaName) -> Quota {
        *self
            .conditions
            .quotas
            .get(name.as_str())
            .and_then(|value| value.known())
            .verified("the embedded free policy supplies every current cluster quota")
    }

    pub fn admit(
        &self,
        name: QuotaName,
        current_usage: u64,
        requested: u64,
        replacement: bool,
    ) -> Result<QuotaConstraint, LicensingRefusal> {
        QuotaConstraint {
            name,
            quota: self.quota(name),
        }
        .admit(current_usage, requested, replacement)
    }

    pub fn processing_stop(&self) -> Option<StopReason> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_policy_admits_unlimited_usage_without_a_stop_or_deadline() {
        let policy = evaluate_policy(LicensingSnapshot {
            initialized: None,
            revision: None,
            usage: nervix_licensing_protocol::impact::UsageSnapshot {
                cluster_nodes: u64::MAX,
                cluster_domains: u64::MAX,
            },
        });
        assert_eq!(policy.state_name(), "free");
        assert_eq!(policy.processing_stop(), None);
        assert_eq!(next_deadline(&policy), None);
        for name in QuotaName::ALL {
            assert_eq!(policy.quota(name), Quota::Unlimited);
            for replacement in [false, true] {
                assert_eq!(
                    policy.admit(name, u64::MAX, 1, replacement),
                    Ok(QuotaConstraint {
                        name,
                        quota: Quota::Unlimited
                    })
                );
            }
        }
    }
}
