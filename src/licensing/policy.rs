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

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::LicensingSnapshot;
use nervix_licensing_protocol::conditions::{FREE_POLICY, UsageConditions};
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
    grants: BTreeMap<QuotaName, QuotaGrant>,
}

/// Revision and usage must travel with identity/record: separate observations could mix cuts.
pub fn evaluate_policy(snapshot: LicensingSnapshot) -> LicensingPolicy {
    let mut policy = LicensingPolicy {
        snapshot,
        conditions: FREE_POLICY.usage(),
        state: PolicyState::Free,
        grants: BTreeMap::new(),
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
    if let Some(initialized) = &policy.snapshot.initialized
        && matches!(
            initialized.record.mode,
            nervix_consensus::LicensingMode::Released(_)
        )
    {
        policy.state = PolicyState::Releasing;
    }
    policy.refresh_grants();
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
    Releasing,
}

pub fn next_deadline(policy: &LicensingPolicy) -> Option<Timestamp> {
    let initialized = policy.snapshot.initialized.as_ref()?;
    let time = initialized.record.licensing_time;
    if let nervix_consensus::LicensingMode::Released(release) = &initialized.record.mode {
        return release
            .transitions
            .values()
            .iter()
            .map(|transition| {
                Timestamp::try_from(transition.deadline)
                    .assured("committed grant deadline fits Timestamp")
            })
            .filter(|at| *at > time)
            .min();
    }
    let binding_expiry = match &initialized.record.mode {
        nervix_consensus::LicensingMode::Activated(activation) => {
            activation.binding.ephemeral_expires_at
        }
        nervix_consensus::LicensingMode::Free { .. }
        | nervix_consensus::LicensingMode::Released(_) => None,
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
    let grant = policy.grants.values().map(|grant| grant.until).min();
    signed
        .into_iter()
        .flatten()
        .chain(binding_expiry)
        .chain(grant)
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
            PolicyState::Releasing => "releasing",
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

    pub fn capability(&self, name: CapabilityName) -> CapabilityVerdict {
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
        .admit_with_grant(
            current_usage,
            requested,
            replacement,
            self.grants.get(&name).map(|grant| grant.existing_allowance),
        )
    }

    pub fn processing_stop(&self) -> Option<StopReason> {
        None
    }
}

/// A temporary allowance protects recovery of existing usage, separately from the target limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaGrant {
    pub existing_allowance: u64,
    pub until: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaVerdict {
    Unlimited,
    Bounded {
        limit: u64,
        grant: Option<QuotaGrant>,
    },
}

/// The current catalog has no capability names; this is the contract for future catalog entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityVerdict {
    Enabled,
    Disabled,
    PermittedUntil { deadline: Timestamp },
}

impl LicensingPolicy {
    pub fn transitions(
        &self,
    ) -> Vec<(
        nervix_licensing_protocol::documents::Transition,
        Timestamp,
        bool,
    )> {
        let Some(initialized) = &self.snapshot.initialized else {
            return Vec::new();
        };
        match &initialized.record.mode {
            nervix_consensus::LicensingMode::Activated(activation) => activation
                .transitions
                .values()
                .map(|entry| {
                    let until = match activation.binding.ephemeral_expires_at {
                        Some(expiry) => entry.until().min(expiry),
                        None => entry.until(),
                    };
                    (entry.transition.clone(), until, entry.acknowledged())
                })
                .collect(),
            nervix_consensus::LicensingMode::Released(release) => release
                .transitions
                .values()
                .iter()
                .map(|transition| {
                    (
                        transition.clone(),
                        Timestamp::try_from(transition.deadline)
                            .assured("validated release deadline fits Timestamp"),
                        true,
                    )
                })
                .collect(),
            nervix_consensus::LicensingMode::Free { .. } => Vec::new(),
        }
    }

    pub fn active_transition_count(&self) -> u64 {
        if self.processing_stop().is_some() {
            return 0;
        }
        let Some(initialized) = &self.snapshot.initialized else {
            return 0;
        };
        u64::try_from(
            self.transitions()
                .iter()
                .filter(|(transition, until, _)| {
                    transition.started_at <= initialized.record.licensing_time.into_datetime()
                        && *until > initialized.record.licensing_time
                })
                .count(),
        )
        .assured("bounded transition count fits u64")
    }

    pub(super) fn refresh_grants(&mut self) {
        self.grants.clear();
        if self.processing_stop().is_some() {
            return;
        }
        let Some(initialized) = &self.snapshot.initialized else {
            return;
        };
        let now = initialized.record.licensing_time;
        let transitions = self.transitions();
        let effective = transitions
            .iter()
            .map(|(transition, until, _)| {
                let mut value = transition.clone();
                value.deadline = until.into_datetime();
                value
            })
            .collect::<Vec<_>>();
        for name in QuotaName::ALL {
            let target = self.quota(name);
            let allowance = nervix_licensing_protocol::impact::EffectiveUsage {
                conditions: &self.conditions,
                transitions: &effective,
            }
            .quota(name.as_str(), now.into_datetime())
            .assured("committed grants have checked allowances");
            if let (Quota::Bounded { value: target }, Quota::Bounded { value: allowance }) =
                (target, allowance)
                && allowance > target
                && let Some(until) = transitions
                    .iter()
                    .filter(|(transition, until, _)| {
                        *until > now
                            && transition.started_at <= now.into_datetime()
                            && transition
                                .affected
                                .iter()
                                .any(|entry| entry.condition == name.as_str())
                    })
                    .map(|(_, until, _)| *until)
                    .min()
            {
                self.grants.insert(
                    name,
                    QuotaGrant {
                        existing_allowance: allowance,
                        until,
                    },
                );
            }
        }
    }

    pub fn is_node_replacement(&self, node: &nervix_models::ClusterNodeName) -> bool {
        let Some(initialized) = &self.snapshot.initialized else {
            return false;
        };
        let nervix_consensus::LicensingMode::Activated(activation) = &initialized.record.mode
        else {
            return false;
        };
        activation.transitions.values().any(|entry| {
            entry.until() > initialized.record.licensing_time
                && entry
                    .transition
                    .affected
                    .iter()
                    .any(|affected| affected.condition == QuotaName::ClusterNodes.as_str())
                && entry
                    .replacement_nodes
                    .as_ref()
                    .is_some_and(|nodes| nodes.contains(node))
        })
    }

    pub fn quota_verdict(&self, name: QuotaName) -> QuotaVerdict {
        match self.quota(name) {
            Quota::Unlimited => QuotaVerdict::Unlimited,
            Quota::Bounded { value } => QuotaVerdict::Bounded {
                limit: value,
                grant: self.grants.get(&name).copied(),
            },
        }
    }

    pub fn quota_state(&self, name: QuotaName) -> &'static str {
        let usage = match name {
            QuotaName::ClusterNodes => self.snapshot.usage.cluster_nodes,
            QuotaName::ClusterDomains => self.snapshot.usage.cluster_domains,
        };
        if self.quota(name).admits(usage) {
            "within_limit"
        } else if self.effective_quota(name).admits(usage) {
            "within_grace"
        } else {
            "over_limit"
        }
    }

    pub fn effective_quota(&self, name: QuotaName) -> Quota {
        match self.quota_verdict(name) {
            QuotaVerdict::Unlimited => Quota::Unlimited,
            QuotaVerdict::Bounded { limit, grant } => Quota::Bounded {
                value: match grant {
                    Some(grant) => grant.existing_allowance.max(limit),
                    None => limit,
                },
            },
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
}
