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

use std::{collections::BTreeSet, num::NonZeroU16};

use error_stack::Report;
use meticulous::OptionExt as _;
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
        "Kafka domain-offset instance layout cannot change in place; explicitly recreate the \
         ingestor before changing its unit count"
    )]
    KafkaLayoutChange,
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

/// Offset tables are unit-owned. A count change requires an explicit source lifetime boundary,
/// rather than assigning the same broker partition to a unit that never committed its position.
pub(in crate::registry) fn validate_unit_layout_transition(
    domain: &DomainName,
    current: &super::graph::ActiveGraph,
    candidate: &super::graph::ActiveGraph,
) -> Result<(), Report<RegistryError>> {
    let keeps_domain_offsets = |node: &super::graph::ActiveNode| {
        matches!(node.config.as_ref(), nervix_models::Model::Ingestor(ingestor)
        if matches!(ingestor.input.transport_entity(), Some(IngestorEntity::Kafka(KafkaIngestor::Topic(source))) if source.offset_mode == KafkaOffsetMode::Domain))
    };
    for before in current
        .graph
        .node_weights()
        .filter(|node| keeps_domain_offsets(node))
    {
        let Some(after) = candidate.node(before.kind, &before.identifier) else {
            continue;
        };
        if keeps_domain_offsets(after) && before.placement_units() != after.placement_units() {
            return Err(
                Report::new(IngestorInstancingError::KafkaLayoutChange).change_context(
                    RegistryError::IngestorInstancing {
                        domain: domain.clone(),
                        ingestor: nervix_models::IngestorName::from(&before.identifier),
                    },
                ),
            );
        }
    }
    Ok(())
}

/// The complete unit declaration after reference and structural validation.
#[derive(Debug, Clone)]
pub(in crate::registry) enum IngestorUnits {
    Single,
    Instances(NonZeroU16),
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
            Self::Instances(count) | Self::PerPartition { count, .. } => count.get(),
        };
        (0..count)
            .map(|index| Unit::Instance(u32::from(index)))
            .collect()
    }

    /// The exact relay units with which this source instance must move.
    pub(in crate::registry) fn colocated_relays(&self, unit: Unit) -> Vec<NodeUnitRef> {
        let (Self::PerPartition { count, relays }, Unit::Instance(index)) = (self, unit) else {
            return Vec::new();
        };
        let relay_unit = if count.get() == 1 {
            Unit::Single
        } else {
            Unit::Partition(index)
        };
        relays
            .iter()
            .map(|relay| NodeUnitRef::new(NodeRef::new(ModelKind::Relay, relay), relay_unit))
            .collect()
    }
}

#[cfg(test)]
mod tests;
