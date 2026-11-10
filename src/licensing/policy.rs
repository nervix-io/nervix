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
use nervix_licensing_vocabulary::{
    AdmissionState, LicensingRefusal, Quota, QuotaConstraint, QuotaName,
};
use nervix_models::{LicensingStopReason, Timestamp};

/// The current catalog contains no capabilities. A future catalog entry supplies its own name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityName {}

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
        // The stop gate derives from the committed binding alone, so it holds whatever the
        // decision's verification: an ephemeral cluster never falls back to the free policy.
        if let Some(reason) = initialized.record.processing_stop() {
            policy.state = PolicyState::Stopped { reason };
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
    /// The licensing stop gate is closed: processing stops and no new usage is admitted.
    Stopped {
        reason: LicensingStopReason,
    },
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
            PolicyState::Stopped { .. } => "stopped",
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
        // A closed stop gate admits nothing, replacements included (specification §13.2).
        let refused = match self.state {
            PolicyState::Stopped { .. } => Some(AdmissionState::Stopped),
            PolicyState::Lapsed if !replacement && requested > 0 => Some(AdmissionState::Lapsed),
            _ => None,
        };
        if let Some(state) = refused {
            return Err(error_stack::Report::new(LicensingRefusal {
                state,
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

    pub fn processing_stop(&self) -> Option<LicensingStopReason> {
        match self.state {
            PolicyState::Stopped { reason } => Some(reason),
            _ => None,
        }
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

    #[test]
    fn a_closed_stop_gate_admits_nothing_and_reports_its_reason() {
        let mut policy = evaluate_policy(LicensingSnapshot {
            initialized: None,
            revision: None,
            usage: nervix_licensing_protocol::impact::UsageSnapshot {
                cluster_nodes: 3,
                cluster_domains: 2,
            },
        });
        let reason = LicensingStopReason::EphemeralExpired {
            expired_at: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
        };
        policy.state = PolicyState::Stopped { reason };
        assert_eq!(policy.state_name(), "stopped");
        assert_eq!(policy.display_state(), "stopped");
        assert_eq!(policy.processing_stop(), Some(reason));
        for name in QuotaName::ALL {
            // Unlimited conditions, replacements and empty requests are refused alike.
            for (replacement, requested) in [(false, 1), (true, 1), (false, 0)] {
                let Err(refusal) = policy.admit(name, 2, requested, replacement) else {
                    panic!("a closed gate admits no usage");
                };
                assert_eq!(
                    *refusal.current_context(),
                    LicensingRefusal {
                        state: AdmissionState::Stopped,
                        condition: name,
                        limit: 2,
                        current_usage: 2,
                        requested,
                    }
                );
            }
        }
    }
}
