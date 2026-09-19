//! Atomic resource-rebind planning within an ordered transaction model run.
//!
//! Layer: decisions.
//!
//! - **Owns.** Selection, version resolution and model transitions for one rebind operation.
//! - **Depends on.** Captured resource uploads, the simulated model prefix and registry mutations.
//! - **Must not know.** Consensus, persistence, sessions, Tokio or runtime execution.

use std::collections::BTreeSet;

use error_stack::Report;
use nervix_models::{
    CanonicalImpactSet, DomainName, DropModel, ImpactAttribution, ModelIndex, NodeRef,
    OperationImpactReason, RebindResource, RebindResourceSelection, ResourceBindingImpact,
    ResourceName, ResourceUploads, TransactionOperation, TransactionOperationNumber,
};

use super::{PlannedResourceRebind, TransactionPlanningError, model_contribution};
use crate::registry::RegistryMutation;

pub(super) fn plan_resource_rebind(
    domain: &DomainName,
    resources: &BTreeSet<ResourceName>,
    resource_uploads: &ResourceUploads,
    prefix_models: &mut ModelIndex,
    rebind: &RebindResource,
    operation: TransactionOperationNumber,
) -> Result<PlannedResourceRebind, Report<TransactionPlanningError>> {
    if !resources.contains(&rebind.resource) {
        return Err(Report::new(TransactionPlanningError::ResourceNotFound {
            resource: rebind.resource.clone(),
        }));
    }
    let resolved = resource_uploads
        .resolve_completed_version(domain, &rebind.resource, rebind.version)
        .map_err(|error| {
            let resolution = error.current_context().clone();
            error.change_context(TransactionPlanningError::ResourceVersion {
                operation,
                error: resolution,
            })
        })?;
    let selected: BTreeSet<NodeRef> = match &rebind.selection {
        RebindResourceSelection::Members(members) => members.iter().cloned().collect(),
        RebindResourceSelection::All => prefix_models
            .iter()
            .filter_map(|(node, model)| {
                model
                    .resource_version(&rebind.resource)
                    .map(|_| node.clone())
            })
            .collect(),
    };
    let before = prefix_models.clone();
    let mut reasons = Vec::with_capacity(selected.len());
    let mut bindings = Vec::with_capacity(selected.len());
    let mut mutations = Vec::new();

    for node in selected {
        let model = prefix_models.get(&node).ok_or_else(|| {
            Report::new(TransactionPlanningError::RebindMemberNotFound {
                domain: domain.clone(),
                node: node.clone(),
            })
        })?;
        let rebound = model
            .rebind_resource(&rebind.resource, resolved.version)
            .ok_or_else(|| {
                Report::new(TransactionPlanningError::RebindMemberDoesNotBind {
                    node: node.clone(),
                    resource: rebind.resource.clone(),
                })
            })?;
        reasons.push(OperationImpactReason::ResourceRebinding {
            node: node.clone(),
            resource: rebind.resource.clone(),
            from_version: rebound.previous_version,
            to_version: resolved.version,
        });
        bindings.push(ResourceBindingImpact {
            node: node.clone(),
            resource: rebind.resource.clone(),
            requested: rebind.version,
            version: resolved.version,
            attribution: ImpactAttribution::single(operation),
        });
        if rebound.previous_version == resolved.version {
            continue;
        }
        mutations.push(RegistryMutation::Drop(DropModel {
            kind: node.kind,
            name: node.identifier.clone(),
        }));
        mutations.push(RegistryMutation::Create(Box::new(rebound.model.clone())));
        prefix_models.insert(rebound.model);
    }

    let mut contribution = model_contribution(&before, prefix_models, operation);
    contribution.reasons = reasons;
    contribution.effects.resource_bindings = CanonicalImpactSet::new(bindings.clone());
    Ok(PlannedResourceRebind {
        operation: TransactionOperation::RebindResource {
            domain: domain.clone(),
            resource: rebind.resource.clone(),
            requested: rebind.version,
            version: resolved.version,
        },
        contribution,
        mutations,
        bindings,
    })
}
