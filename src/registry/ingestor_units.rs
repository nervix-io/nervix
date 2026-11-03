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

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BranchSelection, CreateIngestor, DomainName, IngestorEntity, IngestorEntityKind,
    IngestorInstances, IngestorInstancingCapability, KafkaIngestor, KafkaOffsetMode, ModelIndex,
    ModelKind, ModelName, NodeRef, NodeUnitRef, RelayName, Unit,
};
use thiserror::Error;

use super::{error::RegistryError, validation::branching::resolved_branch_selection};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum IngestorInstancingError {
    #[error("{entity} does not support the declared instancing form")]
    Unsupported { entity: IngestorEntityKind },
    #[error("{entity} INSTANCES PER PARTITION requires OFFSET BY DOMAIN")]
    KafkaOffsets { entity: IngestorEntityKind },
    #[error(
        "INSTANCES PER PARTITION requires constructing routes to one branch partition structure"
    )]
    MissingConstruction,
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
        count: NonZeroU16,
        relays: Vec<RelayName>,
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
        let refusal = |issue| {
            Report::new(issue).change_context(RegistryError::IngestorInstancing {
                domain: domain.clone(),
                ingestor: ingestor.name.clone(),
            })
        };
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
            IngestorInstances::Count(count) => return Ok(Self::Instances(count)),
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
            let partitions = resolved
                .partitions()
                .assured("an explicitly named branch resolves as branched");
            if let Some((expected_fields, expected_partitions)) = &structure {
                if *expected_fields != fields || *expected_partitions != partitions {
                    return Err(refusal(IngestorInstancingError::DifferentStructure {
                        relay: route.relay.clone(),
                    }));
                }
            } else {
                structure = Some((fields, partitions));
            }
            relays.insert(route.relay.clone());
        }
        let Some((_, partitions)) = structure else {
            return Err(refusal(IngestorInstancingError::MissingConstruction));
        };
        Ok(Self::PerPartition {
            count: partitions.count(),
            relays: relays.into_iter().collect(),
        })
    }

    pub(in crate::registry) fn units(&self) -> Vec<Unit> {
        let count = match self {
            Self::Single => return vec![Unit::Single],
            Self::Instances(count) => count.get(),
            Self::PerPartition { count, .. } => u64::from(count.get()),
        };
        (0..count).map(|index| Unit::Instance(index)).collect()
    }

    /// The exact relay units with which this source instance must move.
    pub(in crate::registry) fn colocated_relays(&self, unit: Unit) -> Vec<NodeUnitRef> {
        let (Self::PerPartition { count, relays }, Unit::Instance(index)) = (self, unit) else {
            return Vec::new();
        };
        if index >= u64::from(count.get()) {
            return Vec::new();
        }
        let relay_unit = if count.get() == 1 {
            Unit::Single
        } else {
            Unit::Partition(u32::try_from(index).assured("a constructed partition index fits u32"))
        };
        relays
            .iter()
            .map(|relay| NodeUnitRef::new(NodeRef::new(ModelKind::Relay, relay), relay_unit))
            .collect()
    }
}

#[cfg(test)]
mod tests;
