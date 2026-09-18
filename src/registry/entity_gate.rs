//! Relay admission boundaries for an entity-scoped pause.
//!
//! Layer: decisions.
//!
//! - **Owns.** Pure derivation of the relay gates required by a scheduled entity scope.
//! - **Depends on.** Scheduled Models and their typed node identities.
//! - **Must not know.** Runtime tasks, gate leases, cluster coordination or persistence.

use std::collections::BTreeSet;

use ahash::HashSet;
use meticulous::OptionExt as _;
use nervix_models::{
    ConcreteBranchCoverage, DomainSchedule, ImpactGateBoundary, ImpactNodeCoverage, Model,
    ModelKind, NodeRef, RelayName, ScheduledNode,
};

use super::{graph::is_schedulable_model, validation::branching::model_branch_selection};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct EntityGatePlan {
    affected_entities: Vec<NodeRef>,
    relays: Vec<RelayName>,
}

impl EntityGatePlan {
    pub(crate) fn for_model_change(
        current: Option<&DomainSchedule>,
        affected_entities: impl IntoIterator<Item = NodeRef>,
        changed_entities: impl IntoIterator<Item = NodeRef>,
    ) -> Self {
        let affected_entities = BTreeSet::from_iter(affected_entities);
        let affected = affected_entities.iter().cloned().collect::<Vec<_>>();
        let changed = BTreeSet::from_iter(changed_entities);
        let relays = match current {
            Some(schedule) => model_change_relays_for_schedule(schedule, &affected, &changed),
            None => Vec::new(),
        };
        Self {
            affected_entities: affected_entities.into_iter().collect(),
            relays,
        }
    }

    pub(crate) fn for_ownership_handoff(
        current: Option<&DomainSchedule>,
        desired: Option<&DomainSchedule>,
        moved_entities: impl IntoIterator<Item = NodeRef>,
    ) -> Self {
        let mut affected = BTreeSet::from_iter(moved_entities);
        loop {
            let prior_len = affected.len();
            for schedule in [current, desired].into_iter().flatten() {
                for group in &schedule.placement_groups {
                    if group.members.iter().any(|member| affected.contains(member)) {
                        affected.extend(group.members.iter().cloned());
                    }
                }
            }
            if affected.len() == prior_len {
                break;
            }
        }
        let affected_entities = affected.iter().cloned().collect::<Vec<_>>();
        let relays = match current {
            Some(schedule) => {
                ownership_handoff_relays_for_schedule(schedule, affected_entities.as_slice())
            }
            None => Vec::new(),
        };
        Self {
            affected_entities: affected.into_iter().collect(),
            relays,
        }
    }

    pub(crate) fn affected_entities(&self) -> &[NodeRef] {
        &self.affected_entities
    }

    pub(crate) fn relays(&self) -> &[RelayName] {
        &self.relays
    }
}

pub(crate) fn scheduled_impact_coverage(node: &ScheduledNode) -> ImpactNodeCoverage {
    let identity = node.identity();
    if !is_schedulable_model(node.config.as_ref()) {
        return ImpactNodeCoverage::configuration(identity);
    }
    let declared_branch = match node.config.as_ref() {
        Model::Relay(relay) => relay.branching.branch(),
        model => model_branch_selection(model).and_then(|selection| selection.branch_ref()),
    };
    let branches = match declared_branch {
        Some(branch) => ConcreteBranchCoverage::AllOfBranch {
            branch: branch.clone(),
        },
        None if node.resolved_branching.is_some()
            && matches!(node.kind(), ModelKind::Emitter | ModelKind::Reingestor) =>
        {
            ConcreteBranchCoverage::All
        }
        None => ConcreteBranchCoverage::Unbranched,
    };
    ImpactNodeCoverage::execution(identity, branches)
}

pub(crate) fn gate_boundary(
    schedule: &DomainSchedule,
    relay: &RelayName,
) -> Option<ImpactGateBoundary> {
    let node = schedule
        .nodes
        .get(&NodeRef::new(ModelKind::Relay, relay.clone()))?;
    let coverage = scheduled_impact_coverage(node);
    Some(ImpactGateBoundary {
        relay: relay.clone(),
        branches: coverage
            .branches
            .assured("a scheduled relay is an execution node with explicit branch coverage"),
    })
}

pub(crate) fn entity_pause_relays_for_schedule(
    schedule: &DomainSchedule,
    affected_entities: &[NodeRef],
) -> Vec<RelayName> {
    let mut relays = Vec::new();
    for entity in affected_entities {
        if entity.kind == ModelKind::Relay {
            relays.push(RelayName::from(&entity.identifier));
            continue;
        }
        let Some(node) = schedule.nodes.get(entity) else {
            continue;
        };
        relays.extend(entity_input_relays(node.config.as_ref()));
    }
    relays.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    relays.dedup();
    relays
}

pub(crate) fn ownership_handoff_relays_for_schedule(
    schedule: &DomainSchedule,
    affected_entities: &[NodeRef],
) -> Vec<RelayName> {
    let mut relays = entity_pause_relays_for_schedule(schedule, affected_entities);
    let affected = affected_entities.iter().cloned().collect::<HashSet<_>>();
    relays.retain(|relay| {
        let mut has_producer = false;
        let mut has_unaffected_producer = false;
        for node in schedule.nodes.values() {
            let produces_relay = node
                .config
                .output_routes()
                .is_some_and(|outputs| outputs.relays().any(|output| output == relay));
            if !produces_relay {
                continue;
            }

            has_producer = true;
            if !has_unaffected_producer {
                has_unaffected_producer = !affected.contains(&node.identity());
            }
        }

        !has_producer || has_unaffected_producer
    });
    relays
}

fn model_change_relays_for_schedule(
    schedule: &DomainSchedule,
    affected_entities: &[NodeRef],
    changed_entities: &BTreeSet<NodeRef>,
) -> Vec<RelayName> {
    let mut relays = Vec::new();
    for entity in affected_entities {
        if changed_entities.contains(entity) && entity.kind == ModelKind::Relay {
            relays.push(RelayName::from(&entity.identifier));
        }
        if entity.kind == ModelKind::Relay {
            continue;
        }
        let Some(node) = schedule.nodes.get(entity) else {
            continue;
        };
        relays.extend(entity_input_relays(node.config.as_ref()));
    }
    relays.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    relays.dedup();

    let affected = affected_entities.iter().cloned().collect::<HashSet<_>>();
    relays.retain(|relay| relay_has_external_producer(schedule, relay, &affected));
    relays
}

fn relay_has_external_producer(
    schedule: &DomainSchedule,
    relay: &RelayName,
    affected_entities: &HashSet<NodeRef>,
) -> bool {
    let mut has_producer = false;
    for node in schedule.nodes.values() {
        let produces_relay = node
            .config
            .output_routes()
            .is_some_and(|outputs| outputs.relays().any(|output| output == relay));
        if !produces_relay {
            continue;
        }
        has_producer = true;
        if !affected_entities.contains(&node.identity()) {
            return true;
        }
    }
    !has_producer
}

fn entity_input_relays(model: &Model) -> Vec<RelayName> {
    match model {
        Model::Emitter(emitter) => emitter.from.relays().to_vec(),
        Model::Reingestor(reingestor) => reingestor.from.relays().to_vec(),
        Model::Generator(generator) => vec![generator.materialized_relay.clone()],
        Model::Inferencer(inferencer) => inferencer.from.relays().to_vec(),
        Model::WasmProcessor(processor) => processor.from.relays().to_vec(),
        Model::Junction(junction) => junction.from.relays().to_vec(),
        Model::Deduplicator(deduplicator) => deduplicator.from.relays().to_vec(),
        Model::Correlator(correlator) => correlator
            .left
            .relays()
            .iter()
            .chain(correlator.right.relays())
            .cloned()
            .collect(),
        Model::Reorderer(reorderer) => reorderer.from.relays().to_vec(),
        Model::WindowProcessor(processor) => processor.from.relays().to_vec(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        DomainName, ModelName, PlacementGroupSchedule, ScheduledNode, SchemaFingerprint,
    };

    use super::*;
    use crate::registry::test_fixtures::{ingestor, junction, named, relay};

    #[test]
    fn model_scope_gates_only_external_admission_without_widening_to_ingestors() {
        let changed = NodeRef::new(ModelKind::Junction, named::<ModelName>("changed"));
        let middle = NodeRef::new(ModelKind::Relay, named::<ModelName>("middle"));
        let downstream = NodeRef::new(ModelKind::Junction, named::<ModelName>("downstream"));
        let output = NodeRef::new(ModelKind::Relay, named::<ModelName>("output"));
        let source = NodeRef::new(ModelKind::Ingestor, named::<ModelName>("source"));
        let shared_output_source = NodeRef::new(
            ModelKind::Ingestor,
            named::<ModelName>("shared_output_source"),
        );
        let schedule = DomainSchedule::new(
            named::<DomainName>("default"),
            [
                ScheduledNode::new(
                    relay("input", "event_schema"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    relay("middle", "event_schema"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    relay("output", "event_schema"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    ingestor("source", "input", "codec", "client"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    ingestor("shared_output_source", "output", "codec", "client"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    junction("changed", &["input"], "middle"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    junction("downstream", &["middle"], "output"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
            ],
            Vec::new(),
        );

        let plan = EntityGatePlan::for_model_change(
            Some(&schedule),
            [
                changed.clone(),
                middle.clone(),
                downstream.clone(),
                output.clone(),
            ],
            [changed.clone()],
        );

        assert_eq!(
            plan.affected_entities()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([changed, middle, downstream, output])
        );
        assert!(!plan.affected_entities().contains(&source));
        assert!(!plan.affected_entities().contains(&shared_output_source));
        assert_eq!(plan.relays(), &[named::<RelayName>("input")]);
    }

    #[test]
    fn ownership_scope_expands_every_transitive_required_placement_group() {
        let first = NodeRef::new(ModelKind::Junction, named::<ModelName>("first"));
        let second = NodeRef::new(ModelKind::Junction, named::<ModelName>("second"));
        let third = NodeRef::new(ModelKind::Junction, named::<ModelName>("third"));
        let schedule = DomainSchedule::new(
            named::<DomainName>("default"),
            [
                ScheduledNode::new(
                    relay("input", "event_schema"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    relay("middle", "event_schema"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    relay("output", "event_schema"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    relay("terminal", "event_schema"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    junction("first", &["input"], "middle"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    junction("second", &["middle"], "output"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
                ScheduledNode::new(
                    junction("third", &["output"], "terminal"),
                    SchemaFingerprint::from_digest([1; 32]),
                ),
            ],
            vec![
                PlacementGroupSchedule {
                    members: vec![first.clone(), second.clone()],
                    primary_node: None,
                },
                PlacementGroupSchedule {
                    members: vec![second.clone(), third.clone()],
                    primary_node: None,
                },
            ],
        );

        let plan = EntityGatePlan::for_ownership_handoff(
            Some(&schedule),
            Some(&schedule),
            [first.clone()],
        );

        assert_eq!(plan.affected_entities(), &[first, second, third]);
        assert_eq!(plan.relays(), &[named::<RelayName>("input")]);
    }
}
