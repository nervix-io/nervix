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

use meticulous::{OptionExt as _, ResultExt as _};
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
    pub state: PolicyState,
}

/// Revision and usage must travel with identity/record: separate observations could mix cuts.
pub fn evaluate_policy(snapshot: LicensingSnapshot) -> LicensingPolicy {
    let mut policy = LicensingPolicy {
        snapshot,
        conditions: FREE_POLICY.usage(),
        state: PolicyState::Free,
    };
    if let Some(initialized) = &policy.snapshot.initialized
        && let nervix_consensus::LicensingMode::Activated(_) = &initialized.record.mode
    {
        match &initialized.verification {
            Some(Ok(verified)) => {
                let decision = &verified.decision;
                let now = initialized.record.licensing_time.into_datetime();
                policy.conditions = if now >= decision.timing.term_ends_at
                    || decision.status
                        != nervix_licensing_protocol::documents::DecisionStatus::Active
                {
                    FREE_POLICY.usage()
                } else {
                    decision.conditions.usage.clone()
                };
                policy.state = if decision
                    .timing
                    .valid_until
                    .is_some_and(|until| now >= until)
                {
                    PolicyState::Lapsed
                } else if now >= decision.timing.overdue_at {
                    PolicyState::LicensedOverdue
                } else if now >= decision.timing.refresh_after {
                    PolicyState::LicensedDue
                } else {
                    PolicyState::LicensedFresh
                };
            }
            Some(Err(_)) | None => policy.state = PolicyState::Degraded,
        }
    }
    policy
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyState {
    Free,
    LicensedFresh,
    LicensedDue,
    LicensedOverdue,
    Lapsed,
    Degraded,
}

pub fn next_deadline(policy: &LicensingPolicy) -> Option<Timestamp> {
    let initialized = policy.snapshot.initialized.as_ref()?;
    let time = initialized.record.licensing_time;
    let binding_expiry = match &initialized.record.mode {
        nervix_consensus::LicensingMode::Activated(activation) => {
            activation.binding.ephemeral_expires_at
        }
        nervix_consensus::LicensingMode::Free => None,
    };
    let signed = match &initialized.verification {
        Some(Ok(verified)) => {
            let timing = &verified.decision.timing;
            [
                Some(timing.refresh_after),
                Some(timing.overdue_at),
                timing.valid_until,
                Some(timing.term_ends_at),
            ]
            .map(|at| {
                at.map(|at| {
                    Timestamp::try_from(at)
                        .assured("verification admitted the timestamp coordinate")
                })
            })
        }
        _ => [None; 4],
    };
    signed
        .into_iter()
        .flatten()
        .chain(binding_expiry)
        .filter(|at| *at > time)
        .min()
}

impl LicensingPolicy {
    pub fn state_name(&self) -> &'static str {
        match self.state {
            PolicyState::Free => "free",
            PolicyState::LicensedFresh => "licensed_fresh",
            PolicyState::LicensedDue => "licensed_due",
            PolicyState::LicensedOverdue => "licensed_overdue",
            PolicyState::Lapsed => "lapsed",
            PolicyState::Degraded => "degraded",
        }
    }

    pub fn display_state(&self) -> &'static str {
        match self.state {
            PolicyState::LicensedFresh => "licensed fresh",
            PolicyState::LicensedDue => "licensed due",
            PolicyState::LicensedOverdue => "licensed overdue",
            PolicyState::Lapsed => "lapsed freeze",
            _ => self.state_name(),
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
    ) -> error_stack::Result<QuotaConstraint, LicensingRefusal> {
        if self.state == PolicyState::Lapsed && !replacement && requested > 0 {
            return Err(error_stack::Report::new(LicensingRefusal {
                state: nervix_licensing_vocabulary::AdmissionState::Lapsed,
                condition: name,
                limit: current_usage,
                current_usage,
                requested,
            }));
        }
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
                    policy
                        .admit(name, u64::MAX, 1, replacement)
                        .assured("an unlimited quota admits any usage"),
                    QuotaConstraint {
                        name,
                        quota: Quota::Unlimited
                    }
                );
            }
        }
    }
}
