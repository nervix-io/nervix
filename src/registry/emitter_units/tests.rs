//! Emitter instance decisions exercised from semantic inputs.
//!
//! Layer: test harness.
//! - **Owns.** Emitter unit layouts, refusals, placement groups and partition affinity.
//! - **Depends on.** The production registry decisions and typed Model fixtures.
//! - **Must not know.** Runtime tasks, storage or connector drivers.

use std::collections::BTreeSet;

use meticulous::ResultExt as _;
use nervix_models::{
    BranchPartitions, ClusterNodeName, EmitterEntity, EmitterEntityKind, MqttEmitterEntity,
    NodeUnitRef, ProcessorInputs, RetryPolicy, SingleEmitterReason,
};
use nonzero_ext::nonzero;

use super::*;
use crate::registry::{
    domain_state::DomainState,
    graph::ActiveGraph,
    test_fixtures::*,
    unit_placement::{HopClass, UnitCorrespondence, UnitLayout, UnitPlacementGroups},
};

fn domain() -> DomainName {
    named("instances")
}

fn instance(index: u64) -> Unit {
    Unit::try_instance(index).assured("the fixture index fits the execution-unit range")
}

fn partition(index: u64) -> Unit {
    Unit::partition(u32::try_from(index).assured("the fixture partitions fit u32"))
}

fn emitter_ref() -> NodeRef {
    NodeRef::new(ModelKind::Emitter, named::<ModelName>("sink"))
}

fn relay_ref(relay: &str) -> NodeRef {
    NodeRef::new(ModelKind::Relay, named::<ModelName>(relay))
}

/// A branch of `partitions` partitions on relay `relay`, with the relay branched by it.
fn partitioned_relay(relay: &str, partitions: u16) -> [Model; 2] {
    let mut branch = branch_for_relay(relay, "row");
    let Model::Branch(declaration) = &mut branch else {
        unreachable!("the fixture declares a branch")
    };
    declaration.partitions = BranchPartitions::Direct(
        std::num::NonZeroU16::new(partitions).assured("the fixture counts are nonzero"),
    );
    [branch, relay_branched_by_relay_branch(relay, "row")]
}

fn sink(instances: EmitterInstances, inputs: &[&str]) -> CreateEmitter {
    let Model::Emitter(mut sink) = emitter("sink", inputs[0], "json", "broker") else {
        unreachable!("the emitter helper builds an emitter")
    };
    sink.instances = instances;
    sink.from = ProcessorInputs::new(
        inputs
            .iter()
            .map(|relay| named::<RelayName>(relay))
            .collect(),
        Vec::new(),
    );
    sink
}

fn models(relays: Vec<Model>, sink: CreateEmitter) -> ModelIndex {
    let mut models = vec![
        schema("row"),
        wire_schema("event_wire"),
        codec("json", "row"),
        client_model("broker"),
    ];
    models.extend(relays);
    models.push(Model::Emitter(sink));
    models.into_iter().collect()
}

fn graph(relays: Vec<Model>, instances: EmitterInstances) -> ActiveGraph {
    DomainState::build(&domain(), &models(relays, sink(instances, &["events"])))
        .expect("the emitter graph validates")
        .graph
}

fn refusal(relays: Vec<Model>, sink: CreateEmitter) -> Report<RegistryError> {
    let models = models(relays, sink.clone());
    let error = EmitterUnits::decide(&domain(), &sink, &models)
        .expect_err("the declaration is refused before any unit exists");
    assert!(matches!(
        error.current_context(),
        RegistryError::EmitterInstancing { emitter, .. } if emitter.as_str() == "sink"
    ));
    error
}

#[test]
fn an_emitter_without_instances_and_one_over_a_single_partition_run_as_their_single_unit() {
    let default = graph(partitioned_relay("events", 4).into(), EmitterInstances::ONE);
    let node = default
        .node(ModelKind::Emitter, &named::<ModelName>("sink"))
        .expect("the emitter is active");
    assert_eq!(node.emitter_units, Some(EmitterUnits::Single));
    assert_eq!(UnitLayout::of(node), UnitLayout::Single);

    let one_partition = graph(
        partitioned_relay("events", 1).into(),
        EmitterInstances::PerPartition,
    );
    let node = one_partition
        .node(ModelKind::Emitter, &named::<ModelName>("sink"))
        .expect("the emitter is active");
    assert_eq!(node.emitter_units, Some(EmitterUnits::Single));
    let nodes = [named::<ClusterNodeName>("node-1")];
    let schedule = one_partition.schedule_for_domain(
        &domain(),
        &nodes,
        0,
        None,
        &crate::registry::uniform_capabilities(&nodes),
    );
    assert_eq!(
        schedule.nodes[&emitter_ref()]
            .units
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [Unit::SINGLE]
    );
}

#[test]
fn a_counted_emitter_is_a_fiber_whose_instances_serve_partitions_modulo_their_count() {
    let graph = graph(
        partitioned_relay("events", 4).into(),
        EmitterInstances::Count(nonzero!(3u64)),
    );
    let relay = graph
        .node(ModelKind::Relay, &named::<ModelName>("events"))
        .expect("the relay is active");
    let emitter = graph
        .node(ModelKind::Emitter, &named::<ModelName>("sink"))
        .expect("the emitter is active");
    assert_eq!(
        emitter.emitter_units,
        Some(EmitterUnits::Instances(nonzero!(3u64)))
    );
    let correspondence = UnitCorrespondence::decide(relay, emitter);
    assert_eq!(correspondence.class, HopClass::Fiber);
    for (relay_unit, served_by) in [(0, 0), (1, 1), (2, 2), (3, 0)] {
        assert_eq!(
            correspondence
                .partners(true, partition(relay_unit))
                .collect::<Vec<_>>(),
            [instance(served_by)]
        );
    }
    let served = [
        vec![partition(0), partition(3)],
        vec![partition(1)],
        vec![partition(2)],
    ];
    for (index, partitions) in (0_u64..).zip(served) {
        assert_eq!(
            correspondence
                .partners(false, instance(index))
                .collect::<Vec<_>>(),
            partitions
        );
    }
}

#[test]
fn a_counted_emitter_over_an_unpartitioned_input_serves_it_from_its_first_instance() {
    let graph = graph(
        vec![relay("events", "row")],
        EmitterInstances::Count(nonzero!(2u64)),
    );
    let relay = graph
        .node(ModelKind::Relay, &named::<ModelName>("events"))
        .expect("the relay is active");
    let emitter = graph
        .node(ModelKind::Emitter, &named::<ModelName>("sink"))
        .expect("the emitter is active");
    let correspondence = UnitCorrespondence::decide(relay, emitter);
    assert_eq!(correspondence.class, HopClass::Fiber);
    assert_eq!(
        correspondence
            .partners(false, instance(0))
            .collect::<Vec<_>>(),
        [Unit::SINGLE]
    );
    assert_eq!(correspondence.partners(false, instance(1)).count(), 0);
    let nodes = [named::<ClusterNodeName>("node-1"), named("node-2")];
    let schedule = graph.schedule_for_domain(
        &domain(),
        &nodes,
        0,
        None,
        &crate::registry::uniform_capabilities(&nodes),
    );
    assert_eq!(
        schedule.nodes[&emitter_ref()]
            .units
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [instance(0), instance(1)]
    );
}

#[test]
fn counted_instances_prefer_the_owner_of_a_partition_they_serve() {
    let graph = graph(
        partitioned_relay("events", 4).into(),
        EmitterInstances::Count(nonzero!(2u64)),
    );
    let nodes = [
        named::<ClusterNodeName>("node-1"),
        named("node-2"),
        named("node-3"),
    ];
    let schedule = graph.schedule_for_domain(
        &domain(),
        &nodes,
        0,
        None,
        &crate::registry::uniform_capabilities(&nodes),
    );
    let relay = &schedule.nodes[&relay_ref("events")];
    let emitter = &schedule.nodes[&emitter_ref()];
    assert_eq!(emitter.units.len(), 2);
    for index in 0..2_u64 {
        let owner = emitter
            .unit(instance(index))
            .expect("every declared instance is scheduled")
            .primary_node
            .clone()
            .expect("a scheduled instance has an owner");
        let mut served_owners = BTreeSet::new();
        for relay_unit in (0..4_u64).filter(|relay_unit| relay_unit % 2 == index) {
            let partition_owner = relay
                .unit(partition(relay_unit))
                .expect("every relay partition is scheduled")
                .primary_node
                .clone()
                .expect("a scheduled partition has an owner");
            served_owners.insert(partition_owner);
        }
        assert!(
            served_owners.contains(&owner),
            "instance {index} on {owner} owns none of its partitions {served_owners:?}"
        );
    }
}

#[test]
fn per_partition_groups_bind_each_instance_to_its_partition_of_every_input() {
    let mut relays = Vec::from(partitioned_relay("events", 4));
    relays.push(relay_branched_like("more_events", "row", "events"));
    let models = models(
        relays,
        sink(EmitterInstances::PerPartition, &["events", "more_events"]),
    );
    let graph = DomainState::build(&domain(), &models)
        .expect("inputs sharing one branch validate")
        .graph;
    let emitter = graph
        .node(ModelKind::Emitter, &named::<ModelName>("sink"))
        .expect("the emitter is active");
    assert_eq!(
        emitter.emitter_units,
        Some(EmitterUnits::PerPartition {
            count: nonzero!(4u16),
            relays: vec![named("events"), named("more_events")],
        })
    );
    for input in ["events", "more_events"] {
        let relay = graph
            .node(ModelKind::Relay, &named::<ModelName>(input))
            .expect("the input relay is active");
        assert_eq!(
            UnitCorrespondence::decide(relay, emitter).class,
            HopClass::Identity
        );
    }
    let groups = UnitPlacementGroups::decide(&graph, &graph.placement.effective());
    assert_eq!(groups.groups.len(), 4);
    for index in 0..4 {
        let member = NodeUnitRef::new(emitter_ref(), instance(index));
        assert_eq!(
            groups.groups[groups.by_member[&member]]
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            [
                member,
                NodeUnitRef::new(relay_ref("events"), partition(index)),
                NodeUnitRef::new(relay_ref("more_events"), partition(index)),
            ]
            .into_iter()
            .collect()
        );
    }
    let nodes = [
        named::<ClusterNodeName>("node-1"),
        named("node-2"),
        named("node-3"),
    ];
    let schedule = graph.schedule_for_domain(
        &domain(),
        &nodes,
        1,
        None,
        &crate::registry::uniform_capabilities(&nodes),
    );
    for index in 0..4 {
        let owner = &schedule.nodes[&emitter_ref()]
            .unit(instance(index))
            .expect("every instance is scheduled")
            .primary_node;
        for input in ["events", "more_events"] {
            assert_eq!(
                &schedule.nodes[&relay_ref(input)]
                    .unit(partition(index))
                    .expect("every partition is scheduled")
                    .primary_node,
                owner
            );
        }
    }
}

#[test]
fn an_entity_that_runs_once_refuses_instances_with_its_declared_reason() {
    let mut mqtt = sink(EmitterInstances::Count(nonzero!(2u64)), &["events"]);
    *mqtt.connector = EmitterEntity::Mqtt(nervix_models::MqttEmitter::Topic(
        nervix_models::MqttTopicEmitter {
            mode: nervix_models::MqttTopicPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named("broker"),
            topic: nervix_models::MqttTopicName::try_from("devices/orders")
                .expect("the fixture topic is valid"),
            qos: nervix_models::MqttQos::Qos0,
        },
    ));
    let error = refusal(partitioned_relay("events", 4).into(), mqtt);
    let Some(EmitterInstancingError::SingleInstance(refused)) =
        error.downcast_ref::<EmitterInstancingError>()
    else {
        panic!("the refusal names the entity's capability: {error:#}")
    };
    assert_eq!(
        *refused,
        SingleInstanceRefusal {
            entity: EmitterEntityKind::Mqtt(MqttEmitterEntity::Topic),
            reason: SingleEmitterReason::OneClientIdentifier,
        }
    );
    assert!(format!("{error:#}").contains(
        "MQTT TOPIC emitters publish through one client identifier, so they refuse INSTANCES"
    ));
}

#[test]
fn per_partition_needs_partitioned_inputs_and_instances_need_one_shared_branch() {
    let unbranched = refusal(
        vec![relay("events", "row")],
        sink(EmitterInstances::PerPartition, &["events"]),
    );
    assert!(matches!(
        unbranched.downcast_ref::<EmitterInstancingError>(),
        Some(EmitterInstancingError::MissingPartitions)
    ));

    let mut relays = Vec::from(partitioned_relay("events", 4));
    relays.push(relay("unbranched", "row"));
    let mixed = refusal(
        relays,
        sink(
            EmitterInstances::Count(nonzero!(2u64)),
            &["events", "unbranched"],
        ),
    );
    assert!(matches!(
        mixed.downcast_ref::<EmitterInstancingError>(),
        Some(EmitterInstancingError::DifferentBranch { relay }) if relay.as_str() == "unbranched"
    ));
}

#[test]
fn an_unaddressable_instance_count_is_refused_before_unit_materialization() {
    let error = refusal(
        partitioned_relay("events", 4).into(),
        sink(EmitterInstances::Count(NonZeroU64::MAX), &["events"]),
    );
    assert!(matches!(
        error.downcast_ref::<EmitterInstancingError>(),
        Some(EmitterInstancingError::UnaddressableCount { count }) if *count == NonZeroU64::MAX
    ));
    assert!(
        error.contains::<std::alloc::LayoutError>()
            || error.contains::<std::num::TryFromIntError>()
    );
}
