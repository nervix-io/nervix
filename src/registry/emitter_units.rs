//! The instances a validated emitter runs as.
//!
//! Layer: decisions.
//! - **Owns.** Checking an emitter's `INSTANCES` declaration against its entity's capability and
//!   the branch its input relays share, and the resulting unit set and corresponding relays.
//! - **Depends on.** Semantic Models, shared branch resolution and registry errors.
//! - **Must not know.** Tasks, locks, connector drivers or node-local execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::num::{NonZeroU16, NonZeroU64};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    CreateEmitter, DomainName, EmitterInstances, Model, ModelIndex, ModelKind, ModelName, NodeRef,
    RelayName, ResolvedBranching, SingleInstanceRefusal, Unit,
};
use thiserror::Error;

use super::{error::RegistryError, validation::branching::resolved_branch_selection};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum EmitterInstancingError {
    #[error(transparent)]
    SingleInstance(SingleInstanceRefusal),
    #[error("INSTANCES {count} cannot be represented as execution units on this target")]
    UnaddressableCount { count: NonZeroU64 },
    #[error("INSTANCES PER PARTITION requires input relays branched by one branch")]
    MissingPartitions,
    #[error(
        "INSTANCES requires every input relay to share one branch or all to be unbranched; input \
         relay '{relay}' differs"
    )]
    DifferentBranch { relay: RelayName },
}

/// The complete unit declaration of an emitter after capability and input validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::registry) enum EmitterUnits {
    /// One `single` unit, as every emitter without an `INSTANCES` clause runs.
    Single,
    /// `INSTANCES n` with n above one: instance `i` serves the input partitions `p` with
    /// `p mod n = i`.
    Instances(NonZeroU64),
    /// `INSTANCES PER PARTITION`: instance `p` serves partition `p` of every input relay, which
    /// share one branch of `count` partitions.
    PerPartition {
        count: NonZeroU16,
        relays: Vec<RelayName>,
    },
}

impl EmitterUnits {
    pub(in crate::registry) fn decide(
        domain: &DomainName,
        emitter: &CreateEmitter,
        models: &ModelIndex,
    ) -> Result<Self, Report<RegistryError>> {
        let context = || RegistryError::EmitterInstancing {
            domain: domain.clone(),
            emitter: emitter.name.clone(),
        };
        let refusal = |issue| Report::new(issue).change_context(context());
        if emitter.instances.is_one() {
            return Ok(Self::Single);
        }
        let entity = emitter.connector.kind();
        if let Some(refused) = entity.capabilities().instancing.refusal(entity) {
            return Err(refusal(EmitterInstancingError::SingleInstance(refused)));
        }
        let branching = Self::shared_input_branching(domain, emitter, models)?;
        match emitter.instances {
            EmitterInstances::Count(count) => {
                let capacity = usize::try_from(count.get())
                    .change_context(EmitterInstancingError::UnaddressableCount { count })
                    .change_context_lazy(context)?;
                std::alloc::Layout::array::<(Unit, nervix_models::ScheduledUnit)>(capacity)
                    .change_context(EmitterInstancingError::UnaddressableCount { count })
                    .change_context_lazy(context)?;
                let last = count.get().checked_sub(1).verified(
                    "a nonzero count has a last instance index",
                );
                Unit::try_instance(last)
                    .change_context(EmitterInstancingError::UnaddressableCount { count })
                    .change_context_lazy(context)?;
                Ok(Self::Instances(count))
            }
            EmitterInstances::PerPartition => {
                let Some(partitions) = branching.partitions() else {
                    return Err(refusal(EmitterInstancingError::MissingPartitions));
                };
                let count = partitions.count();
                if count.get() == 1 {
                    return Ok(Self::Single);
                }
                Ok(Self::PerPartition {
                    count,
                    relays: emitter.from.relays().to_vec(),
                })
            }
        }
    }

    /// The branch every input relay of an instanced emitter shares, exactly named or absent from
    /// all of them, so partition `p` names the same keys on every input.
    fn shared_input_branching(
        domain: &DomainName,
        emitter: &CreateEmitter,
        models: &ModelIndex,
    ) -> Result<ResolvedBranching, Report<RegistryError>> {
        let identifier = ModelName::from(&emitter.name);
        let mut shared: Option<ResolvedBranching> = None;
        for relay in emitter.from.relays() {
            let Some(Model::Relay(model)) = models.get(&NodeRef::new(ModelKind::Relay, relay))
            else {
                // Reference validation reports a missing input relay with its own error.
                continue;
            };
            let resolved = resolved_branch_selection(domain, &identifier, models, &model.branching)?;
            match &shared {
                None => shared = Some(resolved),
                Some(expected) => {
                    if expected.branch() != resolved.branch() {
                        return Err(Report::new(EmitterInstancingError::DifferentBranch {
                            relay: relay.clone(),
                        })
                        .change_context(RegistryError::EmitterInstancing {
                            domain: domain.clone(),
                            emitter: emitter.name.clone(),
                        }));
                    }
                }
            }
        }
        Ok(shared.unwrap_or_else(ResolvedBranching::unbranched))
    }

    pub(in crate::registry) fn unit_layout(&self) -> super::unit_placement::UnitLayout {
        use super::unit_placement::UnitLayout;
        match self {
            Self::Single => UnitLayout::Single,
            Self::Instances(count) => UnitLayout::Instances(*count),
            Self::PerPartition { count, .. } => UnitLayout::Instances(NonZeroU64::from(*count)),
        }
    }
}

#[cfg(test)]
mod tests;
