use nervix_arbitrary::{Arbitrary, Domain};
use nonzero_ext::nonzero;

use crate::{
    AckMode, BranchKeyFingerprint, BranchSelection, ClusterNodeName, ClusterSchedule,
    CommandExecutionReference, CreateIngestor, CreateJunction, CreateRelay, CreateSchema,
    CreateWasmProcessor, DomainName, DomainSchedule, EndpointIngestMode, FlushPolicy,
    GeneralErrorPolicy, HttpEndpointIngestor, HttpIngestor, IngestQuiesceMode, IngestorEntity,
    KafkaPartitionSchedule, Model, OwnershipStateComponent, OwnershipStateRecoveryOutcome,
    OwnershipTransition, ParseAsType, ProcessorInputs, ProcessorOutput, ProcessorOutputs,
    RelayBranching, ResolvedBranching, ScheduledNode, SchemaField, SchemaFingerprint,
    SyslogIngestMode, SyslogIngestor, SyslogListenerIngestor, Unit, WasmProcessorLimits,
    WasmStateResetReason, WasmStateResetScope,
};

#[test]
fn bolero_scheduled_units_round_trip_through_current_storage_shape() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let owner = named::<ClusterNodeName>("node-a");
            let config = if arbitrary.entropy().flag() {
                Model::WasmProcessor(CreateWasmProcessor {
                    name: named("guest"),
                    from: ProcessorInputs::single(named("input")),
                    output_routes: ProcessorOutputs::single(named("output")),
                    branched_by: BranchSelection::unbranched(),
                    resource: named("guest_bundle"),
                    resource_version: u64::from(arbitrary.entropy().byte()).max(1),
                    file: "processors/guest.wasm".to_string(),
                    limits: WasmProcessorLimits {
                        max_fuel: nonzero!(1_000_000u64),
                        max_memory_bytes: nonzero!(67_108_864u64),
                    },
                    global_error_policy: GeneralErrorPolicy::Log,
                    rejected_state_policy: Default::default(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                })
            } else {
                Model::Schema(CreateSchema {
                    name: named("orders"),
                    fields: Vec::new(),
                })
            };
            let mut node = ScheduledNode::new(
                config,
                SchemaFingerprint::from_digest([arbitrary.entropy().byte(); 32]),
            )
            .placed_on(Some(owner.clone()), vec![owner.clone()]);
            if matches!(node.config.as_ref(), Model::WasmProcessor(_)) {
                if arbitrary.entropy().flag() {
                    node.begin_wasm_state_generation();
                }
                if arbitrary.entropy().flag() {
                    node.single_unit_mut().ownership_transition = Some(OwnershipTransition {
                        id: "unit-handoff".to_string(),
                        source: named("node-b"),
                        destination: owner.clone(),
                        state_recovery: OwnershipStateRecoveryOutcome::Complete,
                        resets: Vec::new(),
                    });
                }
            }
            let base = node.single_unit().clone();
            let ordinal = u32::from(arbitrary.entropy().byte());
            for unit in [Unit::Partition(ordinal), Unit::Instance(ordinal)] {
                if arbitrary.entropy().flag() {
                    let mut placement = base.clone();
                    placement.primary_node = Some(named("node-b"));
                    placement.assigned_nodes = vec![named("node-b")];
                    if let Some(transition) = placement.ownership_transition.as_mut() {
                        transition.source = owner.clone();
                        transition.destination = named("node-b");
                    }
                    if arbitrary.entropy().flag()
                        && let Some(generations) = placement.wasm_state_generations.as_mut()
                    {
                        generations.begin_every_branch();
                    }
                    node.units.insert(unit, placement);
                }
            }
            let schedule = DomainSchedule::new(domain("prod"), [node], Vec::new());
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&schedule)
                .expect("a current schedule archives");
            let decoded = rkyv::from_bytes::<DomainSchedule, rkyv::rancor::Error>(&archived)
                .expect("a current schedule reads from its archive");
            assert_eq!(decoded, schedule);
        });
}

fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).expect("valid name")
}

fn domain(raw: &str) -> DomainName {
    DomainName::try_from(raw).expect("valid domain")
}

#[test]
fn cluster_schedule_returns_matching_domain() {
    let alpha = DomainSchedule::new(domain("alpha"), Vec::new(), Vec::new());
    let beta = DomainSchedule::new(domain("beta"), Vec::new(), Vec::new());
    let schedule = ClusterSchedule::from_iter([alpha.clone(), beta]);

    assert_eq!(schedule.domain(&domain("alpha")), Some(&alpha));
    assert_eq!(schedule.domain(&domain("gamma")), None);
}

#[test]
fn scheduled_node_assignment_checks_exact_node_id() {
    let node = ScheduledNode::new(
        Model::Schema(CreateSchema {
            name: named("orders"),
            fields: vec![SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }),
        SchemaFingerprint::from_digest([1; 32]),
    )
    .with_resolved_branching(Some(ResolvedBranching::unbranched()))
    .placed_on(
        Some(named::<ClusterNodeName>("node-a")),
        vec![named::<ClusterNodeName>("node-a")],
    );

    assert!(node.is_assigned_to(&named::<ClusterNodeName>("node-a")));
    assert!(!node.is_assigned_to(&named::<ClusterNodeName>("node-b")));
    assert!(
        !node
            .clone()
            .placed_on(Some(named::<ClusterNodeName>("node-a")), Vec::new())
            .is_assigned_to(&named::<ClusterNodeName>("node-a"))
    );
}

#[test]
fn scheduled_node_single_assignment_only_when_exactly_one_node_is_present() {
    let node = ScheduledNode::new(
        Model::Schema(CreateSchema {
            name: named("orders"),
            fields: vec![SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }),
        SchemaFingerprint::from_digest([1; 32]),
    )
    .placed_on(
        Some(named::<ClusterNodeName>("node-a")),
        vec![named::<ClusterNodeName>("node-a")],
    );

    assert_eq!(node.assigned_single_node(), Some(&named("node-a")));
    assert_eq!(
        node.clone()
            .placed_on(
                Some(named::<ClusterNodeName>("node-a")),
                vec![
                    named::<ClusterNodeName>("node-a"),
                    named::<ClusterNodeName>("node-b")
                ]
            )
            .assigned_single_node(),
        None
    );
    assert_eq!(
        node.placed_on(Some(named::<ClusterNodeName>("node-a")), Vec::new())
            .assigned_single_node(),
        None
    );
}

#[test]
fn scheduled_node_exposes_primary_and_replicas() {
    let node = ScheduledNode::new(
        Model::Schema(CreateSchema {
            name: named("orders"),
            fields: vec![SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }),
        SchemaFingerprint::from_digest([1; 32]),
    )
    .placed_on(
        Some(named::<ClusterNodeName>("node-a")),
        vec![
            named::<ClusterNodeName>("node-a"),
            named::<ClusterNodeName>("node-b"),
            named::<ClusterNodeName>("node-c"),
        ],
    );

    assert_eq!(node.primary_node(), Some(&named("node-a")));
    assert_eq!(
        node.replica_nodes(),
        vec![&named("node-b"), &named("node-c")]
    );
    assert!(node.is_primary_on(&named::<ClusterNodeName>("node-a")));
    assert!(!node.is_primary_on(&named::<ClusterNodeName>("node-b")));
}

#[test]
fn scheduled_node_execution_uses_primary_except_for_server_listener_ingestors() {
    let replicated_junction = ScheduledNode::new(
        Model::Junction(CreateJunction {
            name: named("orders_merge"),
            from: ProcessorInputs::new(
                vec![named("orders_in_a"), named("orders_in_b")],
                Vec::new(),
            ),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                named("orders_out"),
                FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
            )]),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }),
        SchemaFingerprint::from_digest([1; 32]),
    )
    .placed_on(
        Some(named::<ClusterNodeName>("node-a")),
        vec![
            named::<ClusterNodeName>("node-a"),
            named::<ClusterNodeName>("node-b"),
        ],
    );
    let endpoint_ingestor = ScheduledNode::new(
        Model::Ingestor(CreateIngestor {
            name: named("orders_http"),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                named("orders_out"),
                FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
            )]),
            input: crate::IngestorInput::Transport(crate::TransportIngestorInput {
                source: IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
                    endpoint: named("public_http"),
                    mode: EndpointIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::EndpointBuffer {
                        max_size: "1MiB".to_string(),
                    },
                })),
                codec: named("codec"),
            }),
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,

            filter_where: None,
        }),
        SchemaFingerprint::from_digest([1; 32]),
    )
    .placed_on(
        Some(named::<ClusterNodeName>("node-a")),
        vec![
            named::<ClusterNodeName>("node-a"),
            named::<ClusterNodeName>("node-b"),
        ],
    );
    let syslog_ingestor = ScheduledNode::new(
        Model::Ingestor(CreateIngestor {
            name: named("orders_syslog"),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                named("orders_out"),
                FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
            )]),
            input: crate::IngestorInput::Transport(crate::TransportIngestorInput {
                source: IngestorEntity::Syslog(SyslogIngestor::Listener(SyslogListenerIngestor {
                    client: named("syslog_listener"),
                    mode: SyslogIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                codec: named("codec"),
            }),
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        }),
        SchemaFingerprint::from_digest([1; 32]),
    )
    .placed_on(
        Some(named::<ClusterNodeName>("node-a")),
        vec![
            named::<ClusterNodeName>("node-a"),
            named::<ClusterNodeName>("node-b"),
        ],
    );

    let both_nodes = [
        named::<ClusterNodeName>("node-a"),
        named::<ClusterNodeName>("node-b"),
    ];
    let unprimaried_junction = replicated_junction
        .clone()
        .placed_on(None, both_nodes.to_vec());
    let unplaced_junction = replicated_junction.clone().placed_on(None, Vec::new());

    assert_eq!(replicated_junction.execution_node(), Some(&named("node-a")));
    assert!(replicated_junction.executes_on(&named::<ClusterNodeName>("node-a")));
    assert!(!replicated_junction.executes_on(&named::<ClusterNodeName>("node-b")));
    assert_eq!(
        replicated_junction.execution_nodes(),
        [named::<ClusterNodeName>("node-a")]
    );

    assert!(unprimaried_junction.executes_on(&named::<ClusterNodeName>("node-a")));
    assert!(unprimaried_junction.executes_on(&named::<ClusterNodeName>("node-b")));
    assert_eq!(unprimaried_junction.execution_nodes(), both_nodes);
    assert!(unplaced_junction.execution_nodes().is_empty());

    assert_eq!(endpoint_ingestor.execution_node(), None);
    assert!(endpoint_ingestor.executes_on(&named::<ClusterNodeName>("node-a")));
    assert!(endpoint_ingestor.executes_on(&named::<ClusterNodeName>("node-b")));
    assert_eq!(endpoint_ingestor.execution_nodes(), both_nodes);

    assert_eq!(syslog_ingestor.execution_node(), None);
    assert!(syslog_ingestor.executes_on(&named::<ClusterNodeName>("node-a")));
    assert!(syslog_ingestor.executes_on(&named::<ClusterNodeName>("node-b")));
    assert_eq!(syslog_ingestor.execution_nodes(), both_nodes);
}

#[test]
fn kafka_partition_schedule_assigns_partitions_round_robin_by_instance() {
    let schedule = KafkaPartitionSchedule::new(nonzero!(2u64), vec![3, 1, 2, 0], 7);

    assert_eq!(schedule.observed_partitions, vec![0, 1, 2, 3]);
    assert_eq!(schedule.rebalance_epoch, 7);
    assert_eq!(schedule.instance_assignments, vec![vec![0, 2], vec![1, 3]]);
}

fn placed_schema(raw: &str, primary: &str, assigned: &[&str]) -> ScheduledNode {
    let assigned = assigned
        .iter()
        .map(|node| named::<ClusterNodeName>(node))
        .collect();
    ScheduledNode::new(
        Model::Schema(CreateSchema {
            name: named(raw),
            fields: Vec::new(),
        }),
        SchemaFingerprint::from_digest([1; 32]),
    )
    .placed_on(Some(named(primary)), assigned)
}

#[test]
fn a_domain_schedule_names_the_units_a_node_is_the_primary_of_in_schedule_order() {
    let schedule = DomainSchedule::new(
        domain("alpha"),
        vec![
            placed_schema("orders", "node-b", &["node-b", "node-a"]),
            placed_schema("payments", "node-a", &["node-a", "node-b"]),
            placed_schema("refunds", "node-b", &["node-b"]),
        ],
        Vec::new(),
    );

    let primaries = schedule.primary_units_on(&named("node-b"));
    let names = primaries
        .iter()
        .map(|unit| (unit.node.identifier.as_str(), unit.unit))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![("orders", Unit::Single), ("refunds", Unit::Single)]
    );
    assert!(schedule.primary_units_on(&named("node-c")).is_empty());
}

#[test]
fn every_ownership_component_but_branch_aggregated_metrics_is_archived() {
    assert!(!OwnershipStateComponent::BranchAggregated.is_archived());
    for component in [
        OwnershipStateComponent::BranchLifecycle,
        OwnershipStateComponent::Deduplicator,
        OwnershipStateComponent::KafkaOffsets,
        OwnershipStateComponent::MaterializedRelay,
        OwnershipStateComponent::WasmProcessor,
        OwnershipStateComponent::WindowProcessor,
    ] {
        assert!(component.is_archived(), "{component:?}");
    }
}

#[test]
fn a_relay_scheduled_as_partitions_takes_no_guest_state_lifetime_or_reset() {
    let owner = named::<ClusterNodeName>("node-a");
    let mut relay = ScheduledNode::new(
        Model::Relay(CreateRelay {
            name: named("tenant_events"),
            schema: named("event"),
            buffer: nonzero!(1usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        }),
        SchemaFingerprint::from_digest([1; 32]),
    )
    .placed_on(Some(owner.clone()), vec![owner.clone()]);
    let single = relay
        .units
        .shift_remove(&Unit::Single)
        .expect("the fixture starts with one unit");
    relay.units.insert(Unit::Partition(0), single.clone());
    relay.units.insert(Unit::Partition(1), single);
    let before = relay.clone();

    // Failover and drain begin a new guest-state lifetime for every node they move, so these run
    // for each partition of a relay too, which holds no guest state of any unit.
    relay.begin_wasm_state_generation();
    relay.begin_wasm_branch_state_generation(BranchKeyFingerprint::new([7; 32]));
    let request = CommandExecutionReference::parse("reset-tenant-events")
        .expect("the reset reference must be valid");
    let published = relay.begin_wasm_state_reset(
        request.clone(),
        WasmStateResetScope::Unbranched,
        WasmStateResetReason::Operator,
    );
    let completed = relay.complete_wasm_state_reset(&request);

    assert!(!published);
    assert!(!completed);
    assert_eq!(relay, before);
    assert_eq!(relay.wasm_state_generations(), None);
    assert_eq!(relay.wasm_state_reset(), None);
}
