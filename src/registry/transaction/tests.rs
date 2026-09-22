//! Transaction-planner tests.
//!
//! Test harness outside the product layer order.

use std::collections::{BTreeMap, BTreeSet};

use nervix_models::{
    AckMode, AlterJunction, AlterProcessorOperation, AlterRelay, AlterRelayOperation, AlterSchema,
    AlterSchemaOperation, BranchSelection, ClusterNodeName, ConcreteBranchCoverage, CreateResource,
    CreateStatement, CreateVhost, DomainConfig, DomainPace, DomainStartPoint, DropModel, FieldName,
    ImpactEdgeKind, ImpactPlanningBasis, Model, ModelKind, OutputBranch, ParseAsType, ResourceId,
    ResourceName, ResourceUpload, ResourceUploadIdentity, ResourceUploadKey, ResourceUploadState,
    SchemaField, Statement, UserName, VhostTlsResource,
};
use nonzero_ext::nonzero;

use super::*;
use crate::registry::test_fixtures::{
    client_model, codec, ingestor, junction, named, relay, schema, wire_schema,
};

fn domain_state(status: DomainStatus) -> ControlDomainState {
    ControlDomainState {
        id: named("default"),
        config: DomainConfig {
            pace: DomainPace::Unpaced,
            placement: PlacementPolicy::Neutral,
        },
        status,
        start_version: 0,
        last_start: DomainStartPoint::Resume,
        clock: None,
    }
}

pub(super) fn snapshot(
    status: DomainStatus,
    models: impl IntoIterator<Item = Model>,
) -> TransactionPlanningSnapshot {
    TransactionPlanningSnapshot {
        domain: domain_state(status),
        models: models.into_iter().collect(),
        resources: BTreeSet::new(),
        resource_uploads: ResourceUploads::default(),
        schedule: None,
        basis: ImpactPlanningBasis::new([7; 32]),
    }
}

pub(super) fn scheduled_snapshot(
    status: DomainStatus,
    models: impl IntoIterator<Item = Model>,
) -> TransactionPlanningSnapshot {
    let domain = named("default");
    let models = models.into_iter().collect::<ModelIndex>();
    let graph = crate::registry::domain_state::DomainState::build(&domain, &models)
        .assured("the transaction test graph is valid")
        .graph;
    let node = ClusterNodeName::parse("node-a")
        .assured("the scheduler fixture node is an identifier-shaped literal");
    let schedule = graph.schedule_for_domain(&domain, &[node], 0, PlacementPolicy::Neutral);
    TransactionPlanningSnapshot {
        domain: domain_state(status),
        models,
        resources: BTreeSet::new(),
        resource_uploads: ResourceUploads::default(),
        schedule: Some(schedule),
        basis: ImpactPlanningBasis::new([7; 32]),
    }
}

fn unbranched_junction(name: &str, input: &str, output: &str) -> Model {
    let mut model = junction(name, &[input], output);
    let Model::Junction(junction) = &mut model else {
        unreachable!("the junction fixture constructs a junction model");
    };
    junction.branched_by = BranchSelection::unbranched();
    for route in &mut junction.output_routes.routes {
        route.branch = Some(OutputBranch::Unbranched);
    }
    model
}

pub(super) fn node_ref(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(kind, named::<nervix_models::ModelName>(name))
}

pub(super) fn preserve_schedule(
    _graph: Option<ActiveGraph>,
    _placement: PlacementPolicy,
    current: Option<&DomainSchedule>,
    _attribution: &ImpactAttribution,
) -> TransactionScheduleDecision {
    TransactionScheduleDecision {
        schedule: current.cloned(),
        ownership_moves: CanonicalImpactSet::default(),
    }
}

fn add_note() -> Statement {
    Statement::AlterSchema(AlterSchema {
        schema: named("events"),
        operations: vec![AlterSchemaOperation::AddField {
            field: SchemaField {
                name: FieldName::parse("note")
                    .assured("the fixture field is an identifier-shaped literal"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            },
        }],
    })
}

fn drop_note() -> Statement {
    Statement::AlterSchema(AlterSchema {
        schema: named("events"),
        operations: vec![AlterSchemaOperation::DropField {
            field: named("note"),
        }],
    })
}

#[test]
fn an_incomplete_model_run_cannot_be_repaired_after_a_resource_boundary() {
    let statements = vec![
        Statement::Create(CreateStatement::new(
            Box::new(relay("events", "missing_schema").into()),
            false,
        )),
        Statement::CreateResource(CreateStatement::new(
            CreateResource {
                identifier: ResourceName::parse("weights")
                    .assured("the resource fixture is an identifier-shaped literal"),
            },
            false,
        )),
        Statement::Create(CreateStatement::new(
            Box::new(schema("missing_schema").into()),
            false,
        )),
    ];

    let error = Registry::plan_transaction(
        snapshot(DomainStatus::Running, []),
        &statements,
        0,
        true,
        preserve_schedule,
    )
    .expect_err("a later run cannot repair the invalid earlier run");
    assert!(matches!(
        error.current_context(),
        TransactionPlanningError::ModelPreflight { operation, .. }
            if operation.get() == 1
    ));
}

#[test]
fn drop_and_recreate_classifies_the_model_run_from_base_to_final() {
    let mut replacement = schema("events");
    let Model::Schema(replacement_schema) = &mut replacement else {
        unreachable!("the schema fixture constructs a schema model");
    };
    replacement_schema.fields.push(SchemaField {
        name: named("note"),
        ty: ParseAsType::String,
        optional: true,
        sensitive: false,
    });
    let statements = vec![
        Statement::Drop(DropModel {
            kind: ModelKind::Schema,
            name: named("events"),
        }),
        Statement::Create(CreateStatement::new(Box::new(replacement.into()), false)),
    ];

    let plan = Registry::plan_transaction(
        snapshot(DomainStatus::Running, [schema("events")]),
        &statements,
        0,
        false,
        preserve_schedule,
    )
    .assured("the final replacement schema is valid");
    let report = plan
        .report()
        .assured("the complete plan has a valid report");
    assert_eq!(report.execution_steps().len(), 1);
    assert_eq!(report.summary().level(), QuiesceLevel::DomainPause);
    let changes = report.execution_steps()[0]
        .planned()
        .effects
        .changed_configuration
        .as_slice();
    assert_eq!(changes.len(), 1);
    assert!(matches!(
        changes[0].transition,
        ConfigurationTransition::Changed { .. }
    ));
    assert!(matches!(
        report.operations()[0]
            .contribution
            .changed_configuration
            .as_slice()[0]
            .transition,
        ConfigurationTransition::Dropped { .. }
    ));
    assert!(matches!(
        report.operations()[1]
            .contribution
            .changed_configuration
            .as_slice()[0]
            .transition,
        ConfigurationTransition::Created { .. }
    ));
}

#[test]
fn cancelling_alters_make_the_atomic_run_a_noop() {
    let plan = Registry::plan_transaction(
        snapshot(DomainStatus::Running, [schema("events")]),
        &[add_note(), drop_note()],
        0,
        false,
        preserve_schedule,
    )
    .assured("the two valid alterations cancel");

    let step = plan
        .first_step()
        .verified("the two alterations form one model run");
    assert_eq!(step.impact.planned().pause.level(), QuiesceLevel::Dynamic);
    assert!(
        step.impact
            .planned()
            .effects
            .changed_configuration
            .is_empty()
    );
    let PlannedTransactionStepKind::Models { plan } = &step.kind else {
        unreachable!("the alterations produce a complete model plan");
    };
    let Some(planned) = &plan.planned else {
        unreachable!("the alterations produce a complete model plan");
    };
    assert!(planned.is_noop());
}

#[test]
fn ownership_moves_raise_a_running_step_to_entity_pause() {
    let moved = relay("events", "event_schema").node_ref();
    let source = nervix_models::ClusterNodeName::parse("node-a")
        .assured("the source node fixture is an identifier-shaped literal");
    let destination = nervix_models::ClusterNodeName::parse("node-b")
        .assured("the destination node fixture is an identifier-shaped literal");
    let statement = Statement::Create(CreateStatement::new(
        Box::new(relay("events", "event_schema").into()),
        false,
    ));
    let plan = Registry::plan_transaction(
        snapshot(DomainStatus::Running, [schema("event_schema")]),
        &[statement],
        0,
        false,
        move |_graph, _placement, current, attribution| TransactionScheduleDecision {
            schedule: current.cloned(),
            ownership_moves: CanonicalImpactSet::new([OwnershipMoveImpact {
                node: nervix_models::ImpactNodeCoverage::all_executions(moved.clone()),
                source: source.clone(),
                destination: destination.clone(),
                attribution: attribution.clone(),
            }]),
        },
    )
    .assured("the model creation and captured schedule decision are valid");

    let report = plan
        .report()
        .assured("the complete plan has a valid report");
    assert_eq!(report.summary().level(), QuiesceLevel::EntityPause);
    assert_eq!(
        report.execution_steps()[0]
            .planned()
            .effects
            .ownership_moves
            .len(),
        1
    );
}

#[test]
fn stopped_schedule_move_keeps_the_execution_gate_without_reporting_a_pause() {
    let domain = named("default");
    let moved = node_ref(ModelKind::Relay, "events");
    let expected_moved = moved.clone();
    let source = ClusterNodeName::parse("node-a")
        .assured("the source node fixture is an identifier-shaped literal");
    let destination = ClusterNodeName::parse("node-b")
        .assured("the destination node fixture is an identifier-shaped literal");
    let statement = Statement::AlterRelay(AlterRelay {
        relay: named("events"),
        operations: vec![AlterRelayOperation::SetCapacity {
            capacity: nonzero!(32usize),
        }],
    });
    let plan = Registry::plan_transaction(
        scheduled_snapshot(
            DomainStatus::Stopped,
            [schema("event_schema"), relay("events", "event_schema")],
        ),
        &[statement],
        0,
        false,
        move |graph, placement, _current, attribution| TransactionScheduleDecision {
            schedule: graph.map(|graph| {
                graph.schedule_for_domain(&domain, std::slice::from_ref(&destination), 0, placement)
            }),
            ownership_moves: CanonicalImpactSet::new([OwnershipMoveImpact {
                node: ImpactNodeCoverage::all_executions(moved.clone()),
                source: source.clone(),
                destination: destination.clone(),
                attribution: attribution.clone(),
            }]),
        },
    )
    .assured("the stopped relay change has a captured ownership transition");
    let step = plan
        .first_step()
        .verified("the relay alteration produces one execution step");
    assert_eq!(step.impact.planned().pause, PauseRequirement::NoPause);
    let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
        unreachable!("the relay alteration produces a model step");
    };
    assert_eq!(
        model_plan.ownership_gate.affected_entities(),
        std::slice::from_ref(&expected_moved)
    );
    assert_eq!(model_plan.ownership_gate.relays(), &[named("events")]);

    let ownership_transition_ids =
        BTreeMap::from([(step.impact.operations().first(), "transition-1".to_string())]);
    let commit = plan.commit_plan(
        "tx".to_string(),
        &ownership_transition_ids,
        &BTreeMap::new(),
    );
    let TransactionCommitStepKind::Models { schedule, .. } = &commit.steps[0].kind else {
        unreachable!("the relay alteration commits one model step");
    };
    let schedule = schedule
        .as_deref()
        .verified("the scheduled relay alteration retains its target schedule");
    let transition = schedule
        .nodes
        .get(&expected_moved)
        .and_then(|node| node.ownership_transition.as_ref())
        .verified("the admitted target schedule freezes its ownership transition");
    assert_eq!(transition.source.as_str(), "node-a");
    assert_eq!(transition.destination.as_str(), "node-b");
    assert_eq!(transition.state_recovery.as_ref(), "complete");
    assert_eq!(transition.id, "transition-1");
}

#[test]
fn entity_pause_report_and_execution_share_the_exact_affected_graph() {
    let domain = named("default");
    let cluster_node = ClusterNodeName::parse("node-a")
        .assured("the scheduler fixture node is an identifier-shaped literal");
    let models = [
        schema("event_schema"),
        wire_schema("event_wire"),
        codec("event_codec", "event_schema"),
        client_model("input_client"),
        client_model("disjoint_client"),
        relay("input", "event_schema"),
        relay("middle", "event_schema"),
        relay("output", "event_schema"),
        relay("disjoint_input", "event_schema"),
        relay("disjoint_output", "event_schema"),
        ingestor("input_ingestor", "input", "event_codec", "input_client"),
        ingestor(
            "disjoint_ingestor",
            "disjoint_input",
            "event_codec",
            "disjoint_client",
        ),
        unbranched_junction("changed", "input", "middle"),
        unbranched_junction("downstream", "middle", "output"),
        unbranched_junction("disjoint", "disjoint_input", "disjoint_output"),
    ];
    let statements = [
        Statement::AlterJunction(AlterJunction {
            junction: named("changed"),
            operations: vec![AlterProcessorOperation::SetMode {
                mode: AckMode::Detached,
            }],
        }),
        Statement::AlterRelay(AlterRelay {
            relay: named("disjoint_input"),
            operations: vec![AlterRelayOperation::SetCapacity {
                capacity: nonzero!(32usize),
            }],
        }),
    ];
    let plan = Registry::plan_transaction(
        scheduled_snapshot(DomainStatus::Running, models),
        &statements,
        0,
        false,
        move |graph, placement, _current, _attribution| TransactionScheduleDecision {
            schedule: graph.map(|graph| {
                graph.schedule_for_domain(
                    &domain,
                    std::slice::from_ref(&cluster_node),
                    0,
                    placement,
                )
            }),
            ownership_moves: CanonicalImpactSet::default(),
        },
    )
    .assured("the junction mode change has a complete scheduled plan");
    let step = plan
        .first_step()
        .verified("the single alteration produces one execution step");
    assert!(step.impact.planned().completeness.is_complete());
    let PauseRequirement::Subgraph { scope } = &step.impact.planned().pause else {
        unreachable!("changing junction acknowledgement mode requires an entity pause");
    };
    let expected_nodes = BTreeSet::from([
        node_ref(ModelKind::Junction, "changed"),
        node_ref(ModelKind::Relay, "middle"),
        node_ref(ModelKind::Junction, "downstream"),
        node_ref(ModelKind::Relay, "output"),
    ]);
    let paused_nodes = scope
        .nodes()
        .iter()
        .map(|node| {
            assert_eq!(
                node.coverage.branches,
                Some(ConcreteBranchCoverage::Unbranched)
            );
            node.coverage.node.clone()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(paused_nodes, expected_nodes);
    assert!(!paused_nodes.contains(&node_ref(ModelKind::Ingestor, "input_ingestor")));
    assert!(!paused_nodes.contains(&node_ref(ModelKind::Ingestor, "disjoint_ingestor")));
    assert!(!paused_nodes.contains(&node_ref(ModelKind::Relay, "disjoint_input")));
    let expected_relays = BTreeSet::from([named("input")]);
    let gate_relays = scope
        .gate_boundaries()
        .iter()
        .map(|gate| gate.boundary.relay.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(gate_relays, expected_relays);

    let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
        unreachable!("the alteration produces a model step");
    };
    assert_eq!(
        model_plan
            .model_gate
            .affected_entities()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_nodes
    );
    assert_eq!(
        model_plan
            .model_gate
            .relays()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_relays
    );

    let effects = &step.impact.planned().effects;
    let topology_nodes = effects
        .topology
        .before
        .nodes
        .as_slice()
        .iter()
        .map(|node| node.coverage.node.clone())
        .collect::<BTreeSet<_>>();
    assert!(topology_nodes.contains(&node_ref(ModelKind::Relay, "input")));
    assert!(!topology_nodes.contains(&node_ref(ModelKind::Junction, "disjoint")));
    let input_edge_kinds = effects
        .topology
        .before
        .edges
        .as_slice()
        .iter()
        .filter(|edge| {
            edge.source.node == node_ref(ModelKind::Relay, "input")
                && edge.target.node == node_ref(ModelKind::Junction, "changed")
        })
        .map(|edge| edge.kind)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        input_edge_kinds,
        BTreeSet::from([
            ImpactEdgeKind::ConfigurationDependency,
            ImpactEdgeKind::Dataflow,
        ])
    );
    let force_flushed_nodes = effects
        .force_flushes
        .as_slice()
        .iter()
        .map(|flush| flush.node.node.clone())
        .collect::<BTreeSet<_>>();
    assert!(force_flushed_nodes.contains(&node_ref(ModelKind::Junction, "disjoint")));
}

#[test]
fn schedule_reassignment_raises_a_dynamic_change_to_an_entity_pause() {
    let domain = named("default");
    let destination = ClusterNodeName::parse("node-b")
        .assured("the scheduler fixture node is an identifier-shaped literal");
    let statement = Statement::AlterRelay(AlterRelay {
        relay: named("events"),
        operations: vec![AlterRelayOperation::SetCapacity {
            capacity: nonzero!(32usize),
        }],
    });
    let plan = Registry::plan_transaction(
        scheduled_snapshot(
            DomainStatus::Running,
            [schema("event_schema"), relay("events", "event_schema")],
        ),
        &[statement],
        0,
        false,
        move |graph, placement, _current, _attribution| TransactionScheduleDecision {
            schedule: graph.map(|graph| {
                graph.schedule_for_domain(&domain, std::slice::from_ref(&destination), 0, placement)
            }),
            ownership_moves: CanonicalImpactSet::default(),
        },
    )
    .assured("the dynamic relay change has a captured reassignment");
    let step = plan
        .first_step()
        .verified("the relay alteration produces one execution step");
    let PauseRequirement::Subgraph { scope } = &step.impact.planned().pause else {
        unreachable!("a scheduled entity swap requires an entity pause");
    };
    let relay = node_ref(ModelKind::Relay, "events");
    assert_eq!(
        scope
            .nodes()
            .iter()
            .map(|node| node.coverage.node.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([relay.clone()])
    );
    assert_eq!(
        scope
            .gate_boundaries()
            .iter()
            .map(|gate| gate.boundary.relay.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([named("events")])
    );
    let PlannedTransactionStepKind::Models { plan } = &step.kind else {
        unreachable!("the relay alteration produces a model step");
    };
    assert_eq!(plan.model_gate.affected_entities(), &[relay]);
}

#[test]
fn schedule_rebuild_raises_entity_creation_to_a_domain_pause() {
    let domain = named("default");
    let cluster_node = ClusterNodeName::parse("node-a")
        .assured("the scheduler fixture node is an identifier-shaped literal");
    let statement = Statement::Create(CreateStatement::new(
        Box::new(relay("events", "event_schema").into()),
        false,
    ));
    let plan = Registry::plan_transaction(
        scheduled_snapshot(DomainStatus::Running, [schema("event_schema")]),
        &[statement],
        0,
        false,
        move |graph, placement, _current, _attribution| TransactionScheduleDecision {
            schedule: graph.map(|graph| {
                graph.schedule_for_domain(
                    &domain,
                    std::slice::from_ref(&cluster_node),
                    0,
                    placement,
                )
            }),
            ownership_moves: CanonicalImpactSet::default(),
        },
    )
    .assured("the relay creation has a captured schedule rebuild");
    let step = plan
        .first_step()
        .verified("the relay creation produces one execution step");
    assert_eq!(
        step.impact.planned().pause,
        PauseRequirement::Domain {
            domain: named("default")
        }
    );
    assert!(
        step.impact
            .planned()
            .effects
            .rebuilds
            .as_slice()
            .iter()
            .any(|rebuild| rebuild.node.node == node_ref(ModelKind::Relay, "events"))
    );
    let PlannedTransactionStepKind::Models { plan } = &step.kind else {
        unreachable!("the relay creation produces a model step");
    };
    assert!(plan.model_gate.affected_entities().is_empty());
}

#[test]
fn drop_recreate_retains_both_sides_of_a_rewired_graph() {
    let domain = named("default");
    let cluster_node = ClusterNodeName::parse("node-a")
        .assured("the scheduler fixture node is an identifier-shaped literal");
    let replacement = unbranched_junction("changed", "input", "new_output");
    let statements = [
        Statement::Drop(DropModel {
            kind: ModelKind::Junction,
            name: named("changed"),
        }),
        Statement::Create(CreateStatement::new(Box::new(replacement.into()), false)),
    ];
    let plan = Registry::plan_transaction(
        scheduled_snapshot(
            DomainStatus::Running,
            [
                schema("event_schema"),
                relay("input", "event_schema"),
                relay("old_output", "event_schema"),
                relay("new_output", "event_schema"),
                unbranched_junction("changed", "input", "old_output"),
            ],
        ),
        &statements,
        0,
        false,
        move |graph, placement, _current, _attribution| TransactionScheduleDecision {
            schedule: graph.map(|graph| {
                graph.schedule_for_domain(
                    &domain,
                    std::slice::from_ref(&cluster_node),
                    0,
                    placement,
                )
            }),
            ownership_moves: CanonicalImpactSet::default(),
        },
    )
    .assured("the junction replacement leaves a valid graph");
    let effects = &plan
        .first_step()
        .verified("the replacement produces one execution step")
        .impact
        .planned()
        .effects;
    let changed = node_ref(ModelKind::Junction, "changed");
    let old_output = node_ref(ModelKind::Relay, "old_output");
    let new_output = node_ref(ModelKind::Relay, "new_output");
    assert!(effects.topology.before.edges.as_slice().iter().any(|edge| {
        edge.source.node == changed
            && edge.target.node == old_output
            && edge.kind == ImpactEdgeKind::Dataflow
            && edge.attribution.operations()
                == [
                    TransactionOperationNumber::from_index(0)
                        .assured("the first fixture operation is addressable"),
                    TransactionOperationNumber::from_index(1)
                        .assured("the second fixture operation is addressable"),
                ]
    }));
    assert!(effects.topology.after.edges.as_slice().iter().any(|edge| {
        edge.source.node == changed
            && edge.target.node == new_output
            && edge.kind == ImpactEdgeKind::Dataflow
    }));
    assert!(
        !effects
            .topology
            .before
            .nodes
            .as_slice()
            .iter()
            .any(|node| { node.coverage.node == new_output })
    );
    assert!(
        !effects
            .topology
            .after
            .nodes
            .as_slice()
            .iter()
            .any(|node| { node.coverage.node == old_output })
    );
}

#[test]
fn lifecycle_steps_change_the_effective_pause_of_later_model_runs() {
    let plan = Registry::plan_transaction(
        snapshot(DomainStatus::Stopped, [schema("events")]),
        &[
            add_note(),
            Statement::StartDomain(nervix_models::StartDomain {
                start: DomainStartPoint::Resume,
            }),
            drop_note(),
        ],
        0,
        false,
        preserve_schedule,
    )
    .assured("the ordered lifecycle and model runs are valid");

    assert_eq!(plan.steps().len(), 3);
    assert_eq!(
        plan.steps()[0].impact.planned().pause.level(),
        QuiesceLevel::Dynamic
    );
    assert_eq!(
        plan.steps()[2].impact.planned().pause.level(),
        QuiesceLevel::DomainPause
    );
}

#[test]
fn if_not_exists_uses_the_ordered_prefix_and_keeps_operation_positions() {
    let statements = [
        Statement::Create(CreateStatement::new(
            Box::new(schema("events").into()),
            false,
        )),
        Statement::Create(CreateStatement::new(
            Box::new(schema("events").into()),
            true,
        )),
    ];
    let plan = Registry::plan_transaction(
        snapshot(DomainStatus::Running, []),
        &statements,
        0,
        false,
        preserve_schedule,
    )
    .assured("the second create is an ordered no-op");

    let step = plan.first_step().verified("the creates form one model run");
    assert_eq!(step.impact.operations().first().get(), 1);
    assert_eq!(step.impact.operations().last().get(), 2);
    let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
        unreachable!("the creates produce a model plan");
    };
    assert_eq!(
        &model_plan.no_op_operations,
        &BTreeSet::from([TransactionOperationNumber::from_index(1)
            .assured("the second fixture operation is addressable")])
    );
}

/// The outcome of one fixture upload of the `tls_bundle` resource.
pub(super) enum FixtureUploadOutcome {
    Completed,
    Applying,
}

pub(super) fn tls_bundle_uploads(uploads: &[(u64, FixtureUploadOutcome)]) -> ResourceUploads {
    fixture_uploads("tls_bundle", uploads)
}

pub(super) fn fixture_uploads(
    resource: &str,
    uploads: &[(u64, FixtureUploadOutcome)],
) -> ResourceUploads {
    let domain = named::<DomainName>("default");
    let mut records = Vec::new();
    for (version, outcome) in uploads {
        let root_checksum = format!("root-{version}");
        let state = match outcome {
            FixtureUploadOutcome::Completed => ResourceUploadState::Completed {
                root_checksum,
                outcome_revision: *version,
            },
            FixtureUploadOutcome::Applying => ResourceUploadState::Applying { root_checksum },
        };
        records.push(ResourceUpload {
            key: ResourceUploadKey::new(
                UserName::parse("default")
                    .assured("the fixture owner is an identifier-shaped literal"),
                domain.clone(),
                named(resource),
                ResourceUploadIdentity::parse(format!("upload-{version}"))
                    .assured("fixture upload identities use accepted characters"),
            ),
            version: *version,
            state,
        });
    }
    ResourceUploads::try_from_uploads(records)
        .assured("fixture uploads have unique identities and versions")
}

fn create_tls_vhost(version: RequestedResourceVersion, if_not_exists: bool) -> Statement {
    Statement::Create(CreateStatement::new(
        Box::new(Model::Vhost(CreateVhost {
            name: named("edge"),
            hostnames: vec!["edge.example.com".to_string()],
            tls: Some(VhostTlsResource {
                resource: named("tls_bundle"),
                version,
            }),
        })),
        if_not_exists,
    ))
}

pub(super) fn stored_tls_vhost(name: &str, version: u64) -> Model {
    Model::Vhost(CreateVhost {
        name: named(name),
        hostnames: vec![format!("{name}.example.com")],
        tls: Some(VhostTlsResource {
            resource: named("tls_bundle"),
            version,
        }),
    })
}

fn operation(number: usize) -> TransactionOperationNumber {
    TransactionOperationNumber::from_index(
        number
            .checked_sub(1)
            .assured("fixture operation numbers are one-based"),
    )
    .assured("fixture operation numbers are addressable")
}

#[test]
fn latest_binds_the_highest_version_completed_in_the_captured_uploads() {
    let mut snapshot = snapshot(DomainStatus::Stopped, []);
    snapshot.resource_uploads = tls_bundle_uploads(&[
        (1, FixtureUploadOutcome::Completed),
        (2, FixtureUploadOutcome::Completed),
        (3, FixtureUploadOutcome::Applying),
    ]);
    let statements = vec![create_tls_vhost(RequestedResourceVersion::Latest, false)];

    let plan = Registry::plan_transaction(snapshot, &statements, 0, false, preserve_schedule)
        .assured("the latest completed version is bindable");

    let binding = ResourceBindingImpact {
        node: node_ref(ModelKind::Vhost, "edge"),
        resource: named("tls_bundle"),
        requested: RequestedResourceVersion::Latest,
        version: 2,
        attribution: ImpactAttribution::single(operation(1)),
    };
    let step = plan.first_step().verified("the create forms one model run");
    assert_eq!(
        step.impact.planned().effects.resource_bindings.as_slice(),
        std::slice::from_ref(&binding)
    );
    let report = plan
        .report()
        .assured("a plan from the first operation has a whole-transaction report");
    assert_eq!(
        report.operations()[0]
            .contribution
            .resource_bindings
            .as_slice(),
        std::slice::from_ref(&binding)
    );
    let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
        unreachable!("the create produces a model plan");
    };
    let planned = model_plan
        .planned
        .as_ref()
        .verified("a complete model run carries its planned mutations");
    let [Model::Vhost(vhost)] = planned.changed_models().as_slice() else {
        panic!("the plan persists exactly the created VHOST");
    };
    let tls = vhost.tls.as_ref().verified("the created VHOST binds TLS");
    assert_eq!(tls.version, 2);
}

#[test]
fn an_incomplete_version_fails_the_operation_that_names_it() {
    let mut snapshot = snapshot(DomainStatus::Stopped, []);
    snapshot.resource_uploads = tls_bundle_uploads(&[
        (1, FixtureUploadOutcome::Completed),
        (2, FixtureUploadOutcome::Applying),
    ]);
    let statements = vec![
        Statement::Create(CreateStatement::new(
            Box::new(schema("events").into()),
            false,
        )),
        create_tls_vhost(RequestedResourceVersion::Number(2), false),
    ];

    let error = Registry::plan_transaction(snapshot, &statements, 0, false, preserve_schedule)
        .expect_err("an applying version is not bindable");

    assert!(matches!(
        error.current_context(),
        TransactionPlanningError::ResourceVersion {
            operation,
            error: ResourceVersionResolutionError::NotCompleted(id),
        } if operation.get() == 2
            && id == &ResourceId::new(named("default"), named("tls_bundle"), 2)
    ));
    assert!(
        error
            .to_string()
            .contains("resource 'tls_bundle@2' is not a completed version in domain 'default'")
    );
}

#[test]
fn an_existing_model_skipped_by_if_not_exists_resolves_no_version() {
    let existing: Model = Model::Vhost(CreateVhost {
        name: named("edge"),
        hostnames: vec!["edge.example.com".to_string()],
        tls: Some(VhostTlsResource {
            resource: named("tls_bundle"),
            version: 1,
        }),
    });
    let statements = vec![create_tls_vhost(RequestedResourceVersion::Latest, true)];

    let plan = Registry::plan_transaction(
        snapshot(DomainStatus::Stopped, [existing]),
        &statements,
        0,
        false,
        preserve_schedule,
    )
    .assured("an IF NOT EXISTS no-op does not need a completed version");

    let step = plan.first_step().verified("the create forms one model run");
    assert!(step.impact.planned().effects.resource_bindings.is_empty());
    let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
        unreachable!("the create produces a model plan");
    };
    assert!(model_plan.no_op_operations.contains(&operation(1)));
}

#[test]
fn a_binding_dropped_later_in_the_run_is_not_reported_by_its_step() {
    let mut snapshot = snapshot(DomainStatus::Stopped, []);
    snapshot.resource_uploads = tls_bundle_uploads(&[(1, FixtureUploadOutcome::Completed)]);
    let statements = vec![
        create_tls_vhost(RequestedResourceVersion::Latest, false),
        Statement::Drop(DropModel {
            kind: ModelKind::Vhost,
            name: named("edge"),
        }),
    ];

    let plan = Registry::plan_transaction(snapshot, &statements, 0, false, preserve_schedule)
        .assured("creating and dropping a VHOST is a valid run");

    let step = plan
        .first_step()
        .verified("both statements form one model run");
    assert!(step.impact.planned().effects.resource_bindings.is_empty());
    let report = plan
        .report()
        .assured("a plan from the first operation has a whole-transaction report");
    assert_eq!(
        report.operations()[0].contribution.resource_bindings.len(),
        1,
        "the create still reports the version it resolved"
    );
}
