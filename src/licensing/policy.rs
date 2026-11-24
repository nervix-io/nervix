//! Pure derivation of the current cluster licensing policy.
//!
//! Layer: decisions.
//! - **Owns.** Free conditions and permission queries from a coherent committed observation.
//! - **Depends on.** Public identity/record facts, the shared condition catalog and capability names.
//! - **Must not know.** Publication, locks, async tasks, clocks, metrics or service connectivity.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns pure licensing decisions")
)]

use meticulous::ResultExt as _;
use nervix_consensus::LicensingSnapshot;
use nervix_licensing_protocol::conditions::{Capability, FREE_POLICY, UsageConditions};
use nervix_licensing_vocabulary::CapabilityName;
use nervix_models::{LicensingStopReason, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicensingPolicy {
    pub snapshot: LicensingSnapshot,
    pub conditions: UsageConditions,
    pub state: PolicyState,
}

/// The revision must travel with identity/record: separate observations could mix cuts.
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
    if let Some(initialized) = &policy.snapshot.initialized
        && matches!(
            initialized.record.mode,
            nervix_consensus::LicensingMode::Released(_)
        )
    {
        policy.state = PolicyState::Releasing;
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
    /// The licensing stop gate is closed: processing stops and no domain may start.
    Stopped {
        reason: LicensingStopReason,
    },
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
    // A grace period's end is committed on time, so its owner sees the grace end.
    let grace = policy
        .transitions()
        .into_iter()
        .map(|(_, until, _)| until)
        .filter(|until| *until > time)
        .min();
    // The licensing stop is forced a fixed time after the expiry, in committed licensing time, so
    // licensing time is committed at that instant for the stop's owner to see it pass.
    let forced_stop = match binding_expiry {
        Some(expiry) => expiry
            .checked_add(nervix_models::LICENSING_STOP_ENFORCE_AFTER)
            .ok(),
        None => None,
    };
    signed
        .into_iter()
        .flatten()
        .chain(binding_expiry)
        .chain(forced_stop)
        .chain(grace)
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
        let state = self.conditions.capability(name.as_str()).verified(
            "the embedded free policy holds known values, and verification refuses an extension \
             value for a catalog name",
        );
        match state {
            Capability::Enabled => CapabilityVerdict::Enabled,
            Capability::Disabled => CapabilityVerdict::Disabled,
        }
    }

    pub fn processing_stop(&self) -> Option<LicensingStopReason> {
        match self.state {
            PolicyState::Stopped { reason } => Some(reason),
            _ => None,
        }
    }
}

/// A capability's state for its owner. A capability whose removal has a grace period stays
/// permitted until that deadline; no current capability has one, so `licensing.dummy` is only ever
/// enabled or disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityVerdict {
    Enabled,
    Disabled,
    PermittedUntil { deadline: Timestamp },
}

impl CapabilityVerdict {
    /// The state `DESCRIBE LICENSE` reports; a permitted capability also reports its deadline.
    pub fn state_name(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::PermittedUntil { .. } => "permitted",
        }
    }

    pub fn permitted_until(self) -> Option<Timestamp> {
        match self {
            Self::PermittedUntil { deadline } => Some(deadline),
            Self::Enabled | Self::Disabled => None,
        }
    }
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
}

#[cfg(test)]
mod tests {

    use super::*;

    fn free_policy() -> LicensingPolicy {
        evaluate_policy(LicensingSnapshot {
            initialized: None,
            revision: None,
        })
    }

    #[test]
    fn the_free_policy_has_no_stop_or_deadline() {
        let policy = free_policy();
        assert_eq!(policy.state_name(), "free");
        assert_eq!(policy.processing_stop(), None);
        assert_eq!(next_deadline(&policy), None);
        assert!(policy.conditions.quotas.is_empty());
    }

    #[test]
    fn the_free_policy_disables_every_capability_and_applied_conditions_enable_or_leave_one_out() {
        let mut policy = free_policy();
        for name in CapabilityName::ALL {
            let verdict = policy.capability(name);
            assert_eq!(verdict, CapabilityVerdict::Disabled);
            assert_eq!(verdict.state_name(), "disabled");
            assert_eq!(verdict.permitted_until(), None);
            policy.conditions.capabilities.insert(
                name.as_str().to_owned(),
                nervix_licensing_protocol::conditions::ConditionValue::Known(Capability::Enabled),
            );
            assert_eq!(policy.capability(name), CapabilityVerdict::Enabled);
            policy.conditions.capabilities.remove(name.as_str());
            assert_eq!(
                policy.capability(name),
                CapabilityVerdict::Disabled,
                "a capability the applied conditions leave out is disabled"
            );
        }
        let deadline = Timestamp::from_unix_nanos(1_700_000_000_000_000_000);
        let permitted = CapabilityVerdict::PermittedUntil { deadline };
        assert_eq!(permitted.state_name(), "permitted");
        assert_eq!(permitted.permitted_until(), Some(deadline));
    }

    #[test]
    fn a_closed_stop_gate_reports_its_reason() {
        let mut policy = free_policy();
        let reason = LicensingStopReason::EphemeralExpired {
            expired_at: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
        };
        policy.state = PolicyState::Stopped { reason };
        assert_eq!(policy.state_name(), "stopped");
        assert_eq!(policy.display_state(), "stopped");
        assert_eq!(policy.processing_stop(), Some(reason));
    }
}
