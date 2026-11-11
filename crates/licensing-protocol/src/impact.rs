//! Pure impact arithmetic for current catalog quotas.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Reduction detection, deterministic transition identity and checked grace deadlines.
//! - **Depends on.** Effective usage supplied by the caller, the catalog and signed operating values.
//! - **Must not know.** Runtime admission, clocks, graphs, consensus mutation or networking.

use std::num::NonZeroUsize;

use chrono::{DateTime, TimeDelta, Utc};
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    conditions::{
        CAPABILITY_GRACE, CLUSTER_DOMAINS, CLUSTER_NODES, ConditionDuration, OperatingConditions,
        QUOTA_GRACE, Quota, UsageConditions,
    },
    document::encode_json,
    documents::{AffectedQuota, Transition, TransitionReason, TransitionTrigger},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UsageSnapshot {
    pub cluster_nodes: u64,
    pub cluster_domains: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransitionPolicy {
    pub quota_reduction_grace_seconds: u64,
    pub capability_removal_grace_seconds: u64,
}

impl TransitionPolicy {
    pub fn from_operating(operating: &OperatingConditions) -> Result<Self, Report<ImpactError>> {
        let quota = operating
            .duration(QUOTA_GRACE)
            .change_context(ImpactError::Conditions)?;
        let capability = operating
            .duration(CAPABILITY_GRACE)
            .change_context(ImpactError::Conditions)?;
        let (
            ConditionDuration::Bounded { seconds: quota },
            ConditionDuration::Bounded {
                seconds: capability,
            },
        ) = (quota, capability)
        else {
            return Err(Report::new(ImpactError::UnboundedGrace));
        };
        Ok(Self {
            quota_reduction_grace_seconds: quota,
            capability_removal_grace_seconds: capability,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ImpactContext {
    pub from_conditions_revision: u64,
    pub to_conditions_revision: u64,
    pub started_at: DateTime<Utc>,
    pub trigger: TransitionTrigger,
    pub reason: TransitionReason,
}

pub enum ImpactTarget<'a> {
    Usage(&'a UsageConditions),
    Expired,
}

/// The target already committed, together with the grants still held by its owner. Each grant
/// covers only its additional amount; the original deadline is never rewritten by assessment.
pub struct EffectiveUsage<'a> {
    pub conditions: &'a UsageConditions,
    pub transitions: &'a [Transition],
}

impl EffectiveUsage<'_> {
    pub fn quota(&self, name: &str, at: DateTime<Utc>) -> Result<Quota, Report<ImpactError>> {
        let base = self
            .conditions
            .quota(name)
            .change_context(ImpactError::Conditions)?;
        let Quota::Bounded { value: base } = base else {
            return Ok(Quota::Unlimited);
        };
        let mut allowance = base;
        let mut ceiling = base;
        for transition in self.transitions {
            if transition.started_at > at || transition.deadline <= at {
                continue;
            }
            for affected in &transition.affected {
                if affected.condition != name {
                    continue;
                }
                let additional = affected.additional_allowance()?;
                // The final sum is capped by a u64 usage ceiling. Saturating the intermediate
                // sum is exact for that cap, including overlapping signed full-width grants.
                #[cfg_attr(
                    nervix_lint,
                    expect(
                        nervix::clamped_arithmetic,
                        reason = "the final u64 usage ceiling caps this sum, so intermediate \
                                  saturation preserves the exact capped allowance"
                    )
                )]
                let capped = allowance.saturating_add(additional);
                allowance = capped;
                ceiling = ceiling.max(affected.usage);
            }
        }
        Ok(Quota::Bounded {
            value: allowance.min(ceiling),
        })
    }
}

impl AffectedQuota {
    /// The part newly protected by this reduction, excluding earlier transitions' grants.
    pub fn additional_allowance(&self) -> Result<u64, Report<ImpactError>> {
        let Quota::Bounded { value: target } = self.target else {
            return Err(Report::new(ImpactError::Allowance));
        };
        let previous = match self.previous {
            Quota::Unlimited => self.usage,
            Quota::Bounded { value } => value.min(self.usage),
        };
        match previous.checked_sub(target) {
            Some(amount) if amount > 0 => Ok(amount),
            _ => Err(Report::new(ImpactError::Allowance)),
        }
    }
}

#[derive(Debug, Error)]
pub enum ImpactError {
    #[error("impact assessment requires complete current usage conditions")]
    Conditions,
    #[error("transition grace must be bounded")]
    UnboundedGrace,
    #[error("the grace deadline exceeds the supported UTC range")]
    Deadline,
    #[error("the transition identity exceeds its encoding bound")]
    Encoding,
    #[error("a transition's additional allowance is invalid or exceeds its numeric bound")]
    Allowance,
}

/// Existing grants are retained by the caller. Only the newly affected amount below the previous
/// target receives a new deadline. Repeating a decision revision yields no transition; a tick or
/// release may change the target without inventing another signed conditions revision.
pub fn assess_impact(
    current: EffectiveUsage<'_>,
    target: ImpactTarget<'_>,
    usage: UsageSnapshot,
    policy: TransitionPolicy,
    context: ImpactContext,
    limit: NonZeroUsize,
) -> Result<Vec<Transition>, Report<ImpactError>> {
    let ImpactTarget::Usage(target) = target else {
        return Ok(Vec::new());
    };
    if context.from_conditions_revision == context.to_conditions_revision
        && matches!(context.trigger, TransitionTrigger::Decision { .. })
    {
        return Ok(Vec::new());
    }
    let mut affected = Vec::new();
    for (condition, usage) in [
        (CLUSTER_NODES, usage.cluster_nodes),
        (CLUSTER_DOMAINS, usage.cluster_domains),
    ] {
        let effective = current.quota(condition, context.started_at)?;
        let previous = current
            .conditions
            .quota(condition)
            .change_context(ImpactError::Conditions)?;
        let target = target
            .quota(condition)
            .change_context(ImpactError::Conditions)?;
        if target.is_reduction_from(effective)
            && target.is_reduction_from(previous)
            && !target.admits(usage)
        {
            let entry = AffectedQuota {
                condition: condition.to_owned(),
                previous,
                target,
                usage,
            };
            entry.additional_allowance()?;
            affected.push(entry);
        }
    }
    if affected.is_empty() {
        return Ok(Vec::new());
    }
    let seconds = i64::try_from(policy.quota_reduction_grace_seconds)
        .change_context(ImpactError::Deadline)?;
    let Some(grace) = TimeDelta::try_seconds(seconds) else {
        return Err(Report::new(ImpactError::Deadline));
    };
    let Some(deadline) = context.started_at.checked_add_signed(grace) else {
        return Err(Report::new(ImpactError::Deadline));
    };
    #[derive(Serialize)]
    struct TransitionIdentity<'a> {
        context: &'a ImpactContext,
        deadline: DateTime<Utc>,
        affected: &'a [AffectedQuota],
    }
    let identity = TransitionIdentity {
        context: &context,
        deadline,
        affected: &affected,
    };
    let bytes = encode_json(&identity, limit).change_context(ImpactError::Encoding)?;
    let digest = nervix_crypto::digest::sha256(&bytes);
    let uuid_bytes = digest[..16]
        .try_into()
        .assured("a SHA-256 digest contains at least sixteen bytes");
    // UUIDv8 reserves application-defined identity bits. Identical committed inputs on every
    // node yield the same transition; neither randomness nor a physical clock is consulted.
    let transition_id = Uuid::new_v8(uuid_bytes);
    Ok(vec![Transition {
        transition_id,
        from_conditions_revision: context.from_conditions_revision,
        to_conditions_revision: context.to_conditions_revision,
        started_at: context.started_at,
        deadline,
        trigger: context.trigger,
        reason: context.reason,
        affected,
    }])
}
