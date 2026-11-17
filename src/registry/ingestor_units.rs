//! The intake units a validated ingestor declares.
//!
//! Layer: decisions.
//! - **Owns.** Capability, offset-mode and partition-structure validation, and the resulting unit
//!   set and corresponding relay units.
//! - **Depends on.** Semantic Models, shared branch resolution and registry errors.
//! - **Must not know.** Tasks, locks, connector drivers or node-local execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::{
    collections::BTreeSet,
    num::{NonZeroU16, NonZeroU64},
};

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    BranchName, BranchSelection, CreateIngestor, DomainName, IngestorEntity, IngestorEntityKind,
    IngestorInstances, IngestorInstancingCapability, KafkaIngestor, KafkaOffsetMode, ModelIndex,
    ModelName, RelayName, Unit,
};
use thiserror::Error;

use super::{
    error::RegistryError,
    validation::branching::{branch_model, resolved_branch_selection},
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum IngestorInstancingError {
    #[error("INSTANCES {count} cannot be represented as execution units on this target")]
    UnaddressableCount { count: NonZeroU64 },
    #[error("installed per-partition intake has {count} units, above the partition bound")]
    InstalledPartitionUnitCount { count: usize },
    #[error("{entity} does not support the declared instancing form")]
    Unsupported { entity: IngestorEntityKind },
    #[error("{entity} INSTANCES PER PARTITION requires OFFSET BY DOMAIN")]
    KafkaOffsets { entity: IngestorEntityKind },
    #[error(
        "INSTANCES PER PARTITION requires constructing routes to one branch partition structure"
    )]
    MissingConstruction,
    #[error("Pulsar {kind} INSTANCES PER PARTITION requires an inherited branch")]
    PulsarPartitionTopics {
        kind: nervix_models::PulsarSubscriptionType,
    },
    #[error(
        "INSTANCES PER PARTITION requires constructing routes to one branch partition structure; \
         route to '{relay}' differs"
    )]
    DifferentStructure { relay: RelayName },
}

/// The complete unit declaration after reference and structural validation.
#[derive(Debug, Clone)]
pub(in crate::registry) enum IngestorUnits {
    Single,
    Instances(NonZeroU64),
    PerPartition {
        count: Option<NonZeroU16>,
        relays: Vec<RelayName>,
        branches: Vec<BranchName>,
    },
}

impl IngestorUnits {
    pub(in crate::registry) fn decide(
        domain: &DomainName,
        ingestor: &CreateIngestor,
        models: &ModelIndex,
    ) -> Result<Self, Report<RegistryError>> {
        let entity = ingestor.input.entity_kind();
        let capability = entity.capabilities().instancing;
        let context = || RegistryError::IngestorInstancing {
            domain: domain.clone(),
            ingestor: ingestor.name.clone(),
        };
        let refusal = |issue| Report::new(issue).change_context(context());
        let Some(instances) = ingestor.input.instances() else {
            return if capability == IngestorInstancingCapability::Single {
                Ok(Self::Single)
            } else {
                Err(refusal(IngestorInstancingError::Unsupported { entity }))
            };
        };
        if !capability.permits(instances) {
            return Err(refusal(IngestorInstancingError::Unsupported { entity }));
        }
        match instances {
            IngestorInstances::Count(count) => {
                let capacity = usize::try_from(count.get())
                    .change_context(IngestorInstancingError::UnaddressableCount { count })
                    .change_context_lazy(context)?;
                std::alloc::Layout::array::<(Unit, nervix_models::ScheduledUnit)>(capacity)
                    .change_context(IngestorInstancingError::UnaddressableCount { count })
                    .change_context_lazy(context)?;
                return Ok(Self::Instances(count));
            }
            IngestorInstances::PerPartition => {}
        }
        if let Some(IngestorEntity::Kafka(KafkaIngestor::Topic(source))) =
            ingestor.input.transport_entity()
            && !matches!(source.offset_mode, KafkaOffsetMode::Domain)
        {
            return Err(refusal(IngestorInstancingError::KafkaOffsets { entity }));
        }
        let identifier = ModelName::from(&ingestor.name);
        let mut structure = None;
        let mut relays = BTreeSet::new();
        let mut branches = BTreeSet::new();
        for route in ingestor.output_routes.outputs() {
            let Some(branch) = route.branch.as_ref().and_then(|branch| branch.branch()) else {
                continue;
            };
            let resolved = resolved_branch_selection(
                domain,
                &identifier,
                models,
                &BranchSelection::BranchedBy {
                    branch: branch.clone(),
                },
            )?;
            let fields = resolved.fields().to_vec();
            let declaration = branch_model(domain, &identifier, models, branch)?.partitions;
            if let Some((expected_fields, expected_declaration, _)) = &structure {
                if *expected_fields != fields || *expected_declaration != declaration {
                    return Err(refusal(IngestorInstancingError::DifferentStructure {
                        relay: route.relay.clone(),
                    }));
                }
            } else {
                structure = Some((
                    fields,
                    declaration,
                    resolved.partitions().map(|value| value.count()),
                ));
            }
            branches.insert(branch.clone());
            relays.insert(route.relay.clone());
        }
        let Some((_, declaration, count)) = structure else {
            return Err(refusal(IngestorInstancingError::MissingConstruction));
        };
        if let Some(IngestorEntity::Pulsar(nervix_models::PulsarIngestor::Topic(source))) =
            ingestor.input.transport_entity()
            && source.subscription.kind.reads_through_one_consumer()
            && !declaration.is_inherited()
        {
            return Err(refusal(IngestorInstancingError::PulsarPartitionTopics {
                kind: source.subscription.kind,
            }));
        }
        Ok(Self::PerPartition {
            count,
            relays: relays.into_iter().collect(),
            branches: branches.into_iter().collect(),
        })
    }

    /// Retains the installed units while a changed topology is unresolved, so their native
    /// position states remain available to the eventual unit-layout transfer.
    pub(in crate::registry) fn resolve_topology(
        &mut self,
        domain: &DomainName,
        ingestor: &nervix_models::IngestorName,
        topology: &nervix_models::PartitionTopology,
        existing: Option<&nervix_models::ScheduledNode>,
        restored: Option<NonZeroU16>,
    ) -> Result<(), Report<RegistryError>> {
        let Self::PerPartition {
            count, branches, ..
        } = self
        else {
            return Ok(());
        };
        let mut observed = None;
        let mut complete = true;
        for branch in branches {
            let Some(layout) = topology.branches.get(branch) else {
                complete = false;
                break;
            };
            let found = layout.partitions().count();
            if observed.is_some_and(|expected| expected != found) {
                complete = false;
                break;
            }
            observed = Some(found);
        }
        if complete {
            *count = observed;
            return Ok(());
        }
        if count.is_some() {
            return Ok(());
        }
        if let Some(installed) = restored {
            *count = Some(installed);
            return Ok(());
        }
        if let Some(existing) = existing
            && matches!(existing.config.as_ref(), nervix_models::Model::Ingestor(model)
                if model.input.instances() == Some(IngestorInstances::PerPartition))
        {
            let installed = u16::try_from(existing.units.len())
                .change_context(IngestorInstancingError::InstalledPartitionUnitCount {
                    count: existing.units.len(),
                })
                .change_context(RegistryError::IngestorInstancing {
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                })?;
            *count = NonZeroU16::new(installed);
        }
        Ok(())
    }

    pub(in crate::registry) fn unit_layout(&self) -> super::unit_placement::UnitLayout {
        use super::unit_placement::UnitLayout;
        match self {
            Self::Single => UnitLayout::Single,
            Self::Instances(count) => UnitLayout::Instances(*count),
            Self::PerPartition {
                count: Some(count), ..
            } => UnitLayout::Instances(NonZeroU64::from(*count)),
            Self::PerPartition { count: None, .. } => UnitLayout::AwaitingTopology,
        }
    }
}

#[cfg(test)]
mod tests;
