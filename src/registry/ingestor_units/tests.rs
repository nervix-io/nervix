//! Instance placement decisions exercised from semantic inputs.
//!
//! Layer: test harness.
//! - **Owns.** Pure graph, unit-group, placement and unit-layout invariants.
//! - **Depends on.** The production registry decisions and typed Model fixtures.
//! - **Must not know.** Runtime tasks, storage or connector drivers.

use std::collections::BTreeSet;

use nervix_models::{BranchPartitions, IngestorInput, Model, PlacementPolicy};
use nonzero_ext::nonzero;

use super::*;
use crate::registry::{
    domain_state::DomainState, test_fixtures::*, unit_placement::UnitPlacementGroups,
};

fn graph(instances: IngestorInstances, branched: bool) -> super::super::graph::ActiveGraph {
    let mut source = ingestor_statement(
        "source",
        "events",
        "json",
        "broker",
        if branched { &["value"] } else { &[] },
    );
    let IngestorInput::Transport(input) = &mut source.input else {
        unreachable!("the fixture reads Kafka")
    };
    let IngestorEntity::Kafka(KafkaIngestor::Topic(input)) = &mut input.source else {
        unreachable!("the fixture reads Kafka")
    };
    input.instances = instances;
    input.offset_mode = KafkaOffsetMode::Domain;
    let mut models = vec![
        wire_schema("event_wire"),
        schema("row"),
        codec("json", "row"),
        client_model("broker"),
        Model::Ingestor(source),
    ];
    if branched {
        let mut branch = branch_for_relay("events", "row");
        let Model::Branch(declaration) = &mut branch else {
            unreachable!("the fixture declares a branch")
        };
        declaration.partitions = BranchPartitions::Direct(nonzero!(4u16));
        models.extend([branch, relay_branched_by_relay_branch("events", "row")]);
    } else {
        models.push(relay("events", "row"));
    }
    DomainState::build(&named("instances"), &models.into_iter().collect())
        .expect("the source graph validates")
        .graph
}

#[test]
fn one_instance_exposes_its_single_execution_and_several_instances_keep_distinct_placements() {
    let owner = named::<nervix_models::ClusterNodeName>("node-1");
    let single_graph = graph(IngestorInstances::ONE, false);
    let mut schedule = single_graph.schedule_for_domain(
        &named("instances"),
        std::slice::from_ref(&owner),
        0,
        PlacementPolicy::Neutral,
    );
    let identity = NodeRef::new(ModelKind::Ingestor, named::<ModelName>("source"));
    let source = schedule
        .nodes
        .get_mut(&identity)
        .expect("the source is scheduled");
    assert_eq!(
        source.units.keys().copied().collect::<Vec<_>>(),
        [Unit::Instance(0)]
    );
    assert_eq!(source.assigned_single_node(), Some(&owner));
    assert_eq!(source.primary_node(), Some(&owner));
    assert_eq!(
        source.single_unit(),
        source.unit(Unit::Instance(0)).unwrap()
    );
    source.single_unit_mut().primary_node = Some(named("node-2"));
    assert_eq!(
        source.sole_unit().unwrap().primary_node,
        Some(named("node-2"))
    );

    let multiple_graph = graph(IngestorInstances::Count(nonzero!(3u16)), false);
    let schedule = multiple_graph.schedule_for_domain(
        &named("instances"),
        &[owner],
        0,
        PlacementPolicy::Neutral,
    );
    let source = &schedule.nodes[&identity];
    assert!(source.sole_unit().is_none());
    assert!(source.assigned_single_node().is_none());
    assert_eq!(source.units.len(), 3);
}

#[test]
fn independent_instances_spread_primaries_and_keep_their_own_replicas() {
    let graph = graph(IngestorInstances::Count(nonzero!(3u16)), false);
    let nodes = [named("node-1"), named("node-2"), named("node-3")];
    let schedule =
        graph.schedule_for_domain(&named("instances"), &nodes, 1, PlacementPolicy::Neutral);
    let source = &schedule.nodes[&NodeRef::new(ModelKind::Ingestor, named::<ModelName>("source"))];
    assert_eq!(
        source.units.keys().copied().collect::<Vec<_>>(),
        (0..3).map(Unit::Instance).collect::<Vec<_>>()
    );
    assert_eq!(
        source
            .units
            .values()
            .filter_map(|unit| unit.primary_node.clone())
            .collect::<BTreeSet<_>>(),
        nodes.into_iter().collect()
    );
    for unit in source.units.values() {
        assert_eq!(unit.assigned_nodes.len(), 2);
        assert_eq!(unit.assigned_nodes.first(), unit.primary_node.as_ref());
    }
}

#[test]
fn per_partition_groups_bind_only_the_corresponding_instance_and_relay_unit() {
    let graph = graph(IngestorInstances::PerPartition, true);
    let groups = UnitPlacementGroups::decide(&graph, &[]);
    assert_eq!(groups.groups.len(), 4);
    for index in 0..4 {
        let source = NodeUnitRef::new(
            NodeRef::new(ModelKind::Ingestor, named::<ModelName>("source")),
            Unit::Instance(index),
        );
        let relay = NodeUnitRef::new(
            NodeRef::new(ModelKind::Relay, named::<ModelName>("events")),
            Unit::Partition(index),
        );
        assert_eq!(
            groups.groups[groups.by_member[&source]]
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            [source, relay].into_iter().collect()
        );
    }
    let schedule = graph.schedule_for_domain(
        &named("instances"),
        &[named("node-1"), named("node-2"), named("node-3")],
        1,
        PlacementPolicy::Neutral,
    );
    assert_eq!(schedule.placement_groups.len(), 4);
    for group in &schedule.placement_groups {
        for member in &group.members {
            assert_eq!(
                schedule.nodes[&member.node]
                    .unit(member.unit)
                    .expect("every group member is scheduled")
                    .primary_node,
                group.primary_node
            );
        }
    }
    assert_eq!(
        schedule
            .placement_groups
            .iter()
            .filter_map(|group| group.primary_node.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
}

#[test]
fn whole_entity_colocation_joins_every_declared_unit() {
    let graph = graph(IngestorInstances::Count(nonzero!(3u16)), false);
    let groups = UnitPlacementGroups::decide(
        &graph,
        &[vec![
            NodeRef::new(ModelKind::Ingestor, named::<ModelName>("source")),
            NodeRef::new(ModelKind::Relay, named::<ModelName>("events")),
        ]],
    );
    assert_eq!(groups.groups.len(), 1);
    assert_eq!(groups.groups[0].len(), 4);
}

#[test]
fn kafka_domain_offsets_keep_their_unit_layout_until_explicit_recreation() {
    let before = graph(IngestorInstances::Count(nonzero!(3u16)), false);
    let unchanged = graph(IngestorInstances::Count(nonzero!(3u16)), false);
    validate_unit_layout_transition(&named("instances"), &before, &unchanged)
        .expect("the same units retain their offsets");
    let resized = graph(IngestorInstances::Count(nonzero!(4u16)), false);
    let error = validate_unit_layout_transition(&named("instances"), &before, &resized)
        .expect_err("a count change needs a new offset lifetime");
    assert!(matches!(
        error.downcast_ref::<IngestorInstancingError>(),
        Some(IngestorInstancingError::KafkaLayoutChange)
    ));
}
