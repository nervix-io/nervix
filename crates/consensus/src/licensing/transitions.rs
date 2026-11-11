//! Committed transition identities, signed acknowledgements and receipt-time impact.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Grant history and the immutable offline bound of an unacknowledged grant.
//! - **Depends on.** Verified decisions, replicated usage and shared impact arithmetic.
//! - **Must not know.** Physical clocks, HTTP, admission owners or graph execution.

use std::collections::{BTreeMap, BTreeSet};

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_licensing_protocol::{
    conditions::{FREE_POLICY, UsageConditions},
    document::DocumentLimits,
    documents::{DecisionStatus, Transition, TransitionReason, TransitionTrigger},
    impact::{
        EffectiveUsage, ImpactContext, ImpactTarget, TransitionPolicy, UsageSnapshot, assess_impact,
    },
};
use nervix_models::Timestamp;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{LicenseDecisionError, LicensingEnvironment, LicensingMode, LicensingRecord};
use crate::ClusterIdentity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransitionAuthorization {
    Pending { valid_until: Option<Timestamp> },
    Acknowledged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedTransition {
    pub transition: Transition,
    /// A pending bound is captured from the introducing decision and cannot renew on refresh.
    pub(super) authorization: TransitionAuthorization,
    pub replacement_nodes: Option<BTreeSet<nervix_models::ClusterNodeName>>,
}

impl CommittedTransition {
    pub fn acknowledged(&self) -> bool {
        matches!(self.authorization, TransitionAuthorization::Acknowledged)
    }

    pub fn until(&self) -> Timestamp {
        let deadline = Timestamp::try_from(self.transition.deadline)
            .assured("committed transition deadlines were validated on admission");
        match self.authorization {
            TransitionAuthorization::Pending {
                valid_until: Some(bound),
            } => deadline.min(bound),
            _ => deadline,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct CommittedLicenseTransitions(
    #[rkyv(with = super::archive::Json)] BTreeMap<Uuid, CommittedTransition>,
);

impl CommittedLicenseTransitions {
    pub(super) fn insert_pending(
        &mut self,
        transition: Transition,
        valid_until: Option<Timestamp>,
    ) {
        self.0
            .entry(transition.transition_id)
            .or_insert(CommittedTransition {
                transition,
                authorization: TransitionAuthorization::Pending { valid_until },
                replacement_nodes: None,
            });
    }
    pub fn values(&self) -> impl Iterator<Item = &CommittedTransition> {
        self.0.values()
    }

    pub fn reported(&self) -> Vec<Transition> {
        self.values()
            .map(|entry| entry.transition.clone())
            .collect()
    }

    pub(super) fn revisions(&self) -> impl Iterator<Item = u64> + '_ {
        self.values().flat_map(|entry| {
            [
                entry.transition.from_conditions_revision,
                entry.transition.to_conditions_revision,
            ]
        })
    }

    pub(super) fn effective_values(&self) -> Vec<Transition> {
        self.values()
            .map(|entry| {
                let mut transition = entry.transition.clone();
                transition.deadline = entry.until().into_datetime();
                transition
            })
            .collect()
    }

    fn bounded(self, limits: DocumentLimits) -> error_stack::Result<Self, LicenseDecisionError> {
        if !self.validate() {
            return Err(Report::new(LicenseDecisionError::Metadata));
        }
        let written = nervix_bounded_write::BoundedWriter::write_with(limits.bytes, |writer| {
            serde_json::to_writer(writer, &self)
        })
        .change_context(LicenseDecisionError::Bounds)?;
        if matches!(written, nervix_bounded_write::BoundedWrite::Complete(_)) {
            Ok(self)
        } else {
            Err(Report::new(LicenseDecisionError::Bounds))
        }
    }

    pub(super) fn acknowledge(
        held: Option<Self>,
        signed: &[Transition],
        limits: DocumentLimits,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let mut committed = held.unwrap_or_default();
        for transition in signed {
            let replacement_nodes = committed
                .0
                .get(&transition.transition_id)
                .and_then(|entry| entry.replacement_nodes.clone());
            committed.0.insert(
                transition.transition_id,
                CommittedTransition {
                    transition: transition.clone(),
                    authorization: TransitionAuthorization::Acknowledged,
                    replacement_nodes,
                },
            );
        }
        committed.bounded(limits)
    }

    fn improve(
        &mut self,
        target: &UsageConditions,
        usage: UsageSnapshot,
    ) -> error_stack::Result<(), LicenseDecisionError> {
        for entry in self.0.values_mut() {
            entry.transition.affected.retain(|affected| {
                let count = match affected.condition.as_str() {
                    nervix_licensing_protocol::conditions::CLUSTER_NODES => usage.cluster_nodes,
                    nervix_licensing_protocol::conditions::CLUSTER_DOMAINS => usage.cluster_domains,
                    _ => return true,
                };
                !target.quota(&affected.condition).is_ok_and(|quota| {
                    quota != affected.target
                        && !quota.is_reduction_from(affected.target)
                        && quota.admits(count)
                })
            });
        }
        self.0
            .retain(|_, entry| !entry.transition.affected.is_empty());
        Ok(())
    }

    pub(super) fn protect_nodes(&mut self, nodes: &BTreeSet<nervix_models::ClusterNodeName>) {
        for entry in self.0.values_mut() {
            if entry.replacement_nodes.is_none() {
                entry.replacement_nodes = Some(nodes.clone());
            }
        }
    }

    pub(super) fn validate(&self) -> bool {
        self.0.iter().all(|(id, entry)| {
            let transition = &entry.transition;
            *id == transition.transition_id
                && transition.deadline >= transition.started_at
                && !transition.affected.is_empty()
                && transition
                    .affected
                    .iter()
                    .all(|affected| affected.additional_allowance().is_ok())
                && Timestamp::try_from(transition.started_at).is_ok()
                && Timestamp::try_from(transition.deadline).is_ok()
        })
    }
}

impl LicensingRecord {
    /// Called in the same state-machine apply as the accepted decision, using that cut's usage.
    pub(super) fn assess_decision(
        mut self,
        previous: &Self,
        identity: &ClusterIdentity,
        usage: UsageSnapshot,
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let LicensingMode::Activated(activation) = &self.mode else {
            return Ok(self);
        };
        let Some(Ok(verified)) = self.verify(identity, environment) else {
            return Ok(self);
        };
        let decision = &verified.decision;
        let (old_revision, old_usage) = match previous.verify(identity, environment) {
            Some(Ok(verified)) => {
                let old = &verified.decision;
                let usage = if old.status == DecisionStatus::Active
                    && previous.licensing_time.into_datetime() < old.timing.term_ends_at
                {
                    old.conditions.usage.clone()
                } else {
                    FREE_POLICY.usage()
                };
                (old.conditions.conditions_revision, usage)
            }
            Some(Err(_)) | None => (0, FREE_POLICY.usage()),
        };
        let target = if decision.status == DecisionStatus::Active
            && self.licensing_time.into_datetime() < decision.timing.term_ends_at
        {
            decision.conditions.usage.clone()
        } else {
            FREE_POLICY.usage()
        };
        let held = match &previous.mode {
            LicensingMode::Activated(held) => held.transitions.effective_values(),
            _ => Vec::new(),
        };
        let generated = assess_impact(
            EffectiveUsage {
                conditions: &old_usage,
                transitions: &held,
            },
            if decision.status == DecisionStatus::Expired {
                ImpactTarget::Expired
            } else {
                ImpactTarget::Usage(&target)
            },
            usage,
            TransitionPolicy::from_operating(&decision.conditions.operating)
                .change_context(LicenseDecisionError::Metadata)?,
            ImpactContext {
                from_conditions_revision: old_revision,
                to_conditions_revision: decision.conditions.conditions_revision,
                started_at: activation.decision.received_at.into_datetime(),
                trigger: TransitionTrigger::Decision {
                    decision_id: decision.decision_id,
                },
                reason: TransitionReason::PlanChanged,
            },
            environment.limits().bytes,
        )
        .change_context(LicenseDecisionError::Metadata)?;
        let LicensingMode::Activated(activation) = &mut self.mode else {
            unreachable!("the accepted mode was checked above");
        };
        if decision.status == DecisionStatus::Expired {
            activation.transitions = CommittedLicenseTransitions::default();
        } else {
            activation.transitions.improve(&target, usage)?;
        }
        let valid_until = decision
            .timing
            .valid_until
            .map(Timestamp::try_from)
            .transpose()
            .change_context(LicenseDecisionError::Timestamp)?;
        for transition in generated {
            activation
                .transitions
                .insert_pending(transition, valid_until);
        }
        activation.transitions = activation
            .transitions
            .clone()
            .bounded(environment.limits())?;
        Ok(self)
    }

    pub(super) fn assess_term_tick(
        &mut self,
        identity: &ClusterIdentity,
        previous_time: Timestamp,
        usage: UsageSnapshot,
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<(), LicenseDecisionError> {
        let Some(Ok(verified)) = self.verify(identity, environment) else {
            return Ok(());
        };
        let decision = &verified.decision;
        let now = self.licensing_time.into_datetime();
        if previous_time.into_datetime() >= decision.timing.term_ends_at
            || now < decision.timing.term_ends_at
        {
            return Ok(());
        }
        let LicensingMode::Activated(activation) = &mut self.mode else {
            return Ok(());
        };
        let target = FREE_POLICY.usage();
        let held = activation.transitions.effective_values();
        let generated = assess_impact(
            EffectiveUsage {
                conditions: &decision.conditions.usage,
                transitions: &held,
            },
            ImpactTarget::Usage(&target),
            usage,
            TransitionPolicy::from_operating(&decision.conditions.operating)
                .change_context(LicenseDecisionError::Metadata)?,
            ImpactContext {
                from_conditions_revision: decision.conditions.conditions_revision,
                to_conditions_revision: decision.conditions.conditions_revision,
                started_at: now,
                trigger: TransitionTrigger::Tick {
                    decision_id: decision.decision_id,
                },
                reason: TransitionReason::TermEnded,
            },
            environment.limits().bytes,
        )
        .change_context(LicenseDecisionError::Metadata)?;
        activation.transitions.improve(&target, usage)?;
        let valid_until = decision
            .timing
            .valid_until
            .map(Timestamp::try_from)
            .transpose()
            .change_context(LicenseDecisionError::Timestamp)?;
        for transition in generated {
            activation
                .transitions
                .insert_pending(transition, valid_until);
        }
        activation.transitions = activation
            .transitions
            .clone()
            .bounded(environment.limits())?;
        Ok(())
    }
}
