//! Applying committed schedules to this node's runtime.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests of schedule application: placement-derived state, paused and stale
//!   schedules, applied revisions, standalone processor nodes and entity swaps.
//! - **Depends on.** The production schedule application path and the runtime's test fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeMap;

use nervix_models::{
    AckMode, BranchSelection, ClusterNodeName, ClusterSchedule, CreateDeduplicator, CreateJunction,
    CreateRelay, CreateSchema, DeduplicatorName, DomainConfig, DomainPace, DomainSchedule,
    DomainState, DomainStatus, ModelKind, ModelName, NodeRef, ParseAsType, ProcessorInputs,
    ProcessorOutputs, RelayBranching, RelayName, ScheduledNode, SchemaField, SchemaName,
};
use nonzero_ext::nonzero;

use super::*;

#[nervix_primitives::test]
async fn scheduled_relay_placement_does_not_create_metric_replication_state() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("events");
    let schema = named::<SchemaName>("event");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
            },
            status: DomainStatus::Running,
            start_version: 1,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
    )]));
    let schedule = ClusterSchedule::from_iter([DomainSchedule::new(
        domain.clone(),
        vec![
            ScheduledNode::new(
                nervix_models::Model::Schema(CreateSchema {
                    labels: Default::default(),
                    name: schema.clone(),
                    fields: vec![SchemaField {
                        name: named("value"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }),
                SchemaFingerprint::from_digest([1; 32]),
            ),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                labels: Default::default(),
                name: relay.clone(),
                schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
        ],
        Vec::new(),
    )]);

    runtime
        .apply_cluster_schedule(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            &schedule,
        )
        .await
        .expect("relay schedule should build");

    assert!(
        !runtime
            .inner
            .replicated_branch_aggregated_states
            .iter()
            .any(|state| state.key().kind == ModelKind::Relay),
        "relay metrics must remain volatile owner-only state"
    );
}

#[nervix_primitives::test]
async fn paused_schedule_keeps_full_execution_without_rebuilding_unchanged_graph() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let schema = named::<SchemaName>("notification");
    let relay = named::<RelayName>("notifications");
    let running = DomainState {
        id: domain.clone(),
        config: DomainConfig {
            pace: DomainPace::Unpaced,
        },
        status: DomainStatus::Running,
        start_version: 1,
        last_start: nervix_models::DomainStartPoint::Resume,
        clock: None,
    };
    runtime.sync_domains(&BTreeMap::from([(domain.clone(), running.clone())]));
    let schedule = ClusterSchedule::from_iter([DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                labels: Default::default(),
                name: schema.clone(),
                fields: vec![SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                labels: Default::default(),
                name: relay.clone(),
                schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
        ],
        Vec::new(),
    )]);
    runtime
        .apply_cluster_schedule(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            &schedule,
        )
        .await
        .expect("running schedule should build");
    let revision_before_pause = runtime
        .inner
        .executions
        .get(&domain)
        .assured("the running domain has an installed revision")
        .revision
        .clone();

    let mut paused = running;
    paused.status = DomainStatus::Paused;
    runtime.sync_domains(&BTreeMap::from([(domain.clone(), paused)]));
    runtime
        .apply_cluster_schedule(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            &schedule,
        )
        .await
        .expect("paused schedule should remain active");

    let execution = runtime
        .inner
        .executions
        .get(&domain)
        .expect("paused execution should remain");
    assert!(!execution.passive_only);
    assert!(Arc::ptr_eq(&revision_before_pause, &execution.revision));
    assert!(execution.relay_services.contains_key(&relay));
}

#[nervix_primitives::test]
async fn stale_cluster_state_cannot_replace_a_newer_runtime_schedule() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let schema = named::<SchemaName>("notification");
    let relay = named::<RelayName>("notifications");
    let domains = BTreeMap::from([(
        domain.clone(),
        DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
            },
            status: DomainStatus::Running,
            start_version: 1,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
    )]);
    let schema_node = scheduled_model(nervix_models::Model::Schema(CreateSchema {
        labels: Default::default(),
        name: schema.clone(),
        fields: vec![SchemaField {
            name: named("user_id"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    }));
    let stale_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
        domain.clone(),
        vec![schema_node.clone()],
        Vec::new(),
    )]);
    let current_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
        domain.clone(),
        vec![
            schema_node,
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                labels: Default::default(),
                name: relay.clone(),
                schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
        ],
        Vec::new(),
    )]);

    runtime
        .apply_cluster_state(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            2,
            &domains,
            &BTreeMap::new(),
            &current_schedule,
        )
        .await
        .expect("current cluster state should build");
    runtime
        .apply_cluster_state(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            1,
            &domains,
            &BTreeMap::new(),
            &stale_schedule,
        )
        .await
        .expect("stale cluster state should be ignored");

    let execution = runtime
        .inner
        .executions
        .get(&domain)
        .expect("current execution should remain");
    assert_eq!(
        execution.revision.ownership_handoff_fingerprint,
        ExecutionRevision::ownership_fingerprint(
            current_schedule
                .domains
                .get(&domain)
                .assured("the current schedule has the domain")
        )
        .assured("the current schedule has an ownership fingerprint")
    );
    assert!(execution.relay_services.contains_key(&relay));
}

#[test]
fn a_node_that_has_applied_nothing_accepts_every_revision() {
    let application = ScheduleApplication::default();

    assert!(application.advances_beyond_applied(0));
    assert!(application.advances_beyond_applied(1));
    assert!(application.advances_beyond_applied(u64::MAX));
}

#[test]
fn an_applied_revision_suppresses_equal_and_lower_revisions() {
    let mut application = ScheduleApplication::default();

    application.record_applied(0);
    assert!(!application.advances_beyond_applied(0));
    assert!(application.advances_beyond_applied(1));

    application.record_applied(7);
    assert!(!application.advances_beyond_applied(0));
    assert!(!application.advances_beyond_applied(6));
    assert!(!application.advances_beyond_applied(7));
    assert!(application.advances_beyond_applied(8));
}

#[test]
fn the_largest_applied_revision_suppresses_every_other_revision() {
    let mut application = ScheduleApplication::default();

    application.record_applied(u64::MAX);

    assert!(!application.advances_beyond_applied(u64::MAX));
    assert!(!application.advances_beyond_applied(u64::MAX - 1));
    assert!(!application.advances_beyond_applied(0));
}

#[nervix_primitives::test]
async fn the_largest_revision_still_suppresses_a_later_stale_cluster_state() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let schema = named::<SchemaName>("notification");
    let relay = named::<RelayName>("notifications");
    let domains = BTreeMap::from([(domain.clone(), unpaced_domain_state(domain.as_str()))]);
    let schema_node = scheduled_model(nervix_models::Model::Schema(CreateSchema {
        labels: Default::default(),
        name: schema.clone(),
        fields: vec![SchemaField {
            name: named("user_id"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    }));
    let stale_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
        domain.clone(),
        vec![schema_node.clone()],
        Vec::new(),
    )]);
    let current_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
        domain.clone(),
        vec![
            schema_node,
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                labels: Default::default(),
                name: relay.clone(),
                schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
        ],
        Vec::new(),
    )]);

    runtime
        .apply_cluster_state(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            u64::MAX,
            &domains,
            &BTreeMap::new(),
            &current_schedule,
        )
        .await
        .expect("current cluster state should build");
    runtime
        .apply_cluster_state(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            1,
            &domains,
            &BTreeMap::new(),
            &stale_schedule,
        )
        .await
        .expect("stale cluster state should be ignored");

    let execution = runtime
        .inner
        .executions
        .get(&domain)
        .expect("current execution should remain");
    assert_eq!(
        execution.revision.ownership_handoff_fingerprint,
        ExecutionRevision::ownership_fingerprint(
            current_schedule
                .domains
                .get(&domain)
                .assured("the current schedule has the domain")
        )
        .assured("the current schedule has an ownership fingerprint")
    );
    assert!(execution.relay_services.contains_key(&relay));
}

#[nervix_primitives::test]
async fn a_failed_application_leaves_its_revision_ready_to_apply_again() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let schema = named::<SchemaName>("notification");
    let relay = named::<RelayName>("notifications");
    let domains = BTreeMap::from([(domain.clone(), unpaced_domain_state(domain.as_str()))]);
    let relay_node = scheduled_model(nervix_models::Model::Relay(CreateRelay {
        labels: Default::default(),
        name: relay.clone(),
        schema: schema.clone(),
        buffer: nonzero!(2usize),
        branching: RelayBranching::unbranched(),
        materialized_state: None,
    }));
    let unbuildable_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
        domain.clone(),
        vec![relay_node.clone()],
        Vec::new(),
    )]);
    let buildable_schedule = ClusterSchedule::from_iter([DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                labels: Default::default(),
                name: schema,
                fields: vec![SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            relay_node,
        ],
        Vec::new(),
    )]);

    let failure = runtime
        .apply_cluster_state(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            4,
            &domains,
            &BTreeMap::new(),
            &unbuildable_schedule,
        )
        .await
        .expect_err("a relay without its schema should fail to plan");
    assert!(
        matches!(
            failure.current_context(),
            RuntimeError::PlanScheduleRevision
        ),
        "{failure:?}"
    );
    runtime
        .apply_cluster_state(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            4,
            &domains,
            &BTreeMap::new(),
            &buildable_schedule,
        )
        .await
        .expect("the revision that failed should be applied again");

    let execution = runtime
        .inner
        .executions
        .get(&domain)
        .expect("the reapplied execution should exist");
    assert!(execution.relay_services.contains_key(&relay));
}

#[nervix_primitives::test]
async fn branch_preserving_processors_build_standalone_schedule_nodes() {
    let runtime = Runtime::default();
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        unpaced_domain_state(domain.as_str()),
    )]));
    let order_schema = named::<SchemaName>("order_event");
    let order_relay = |name: &str| {
        scheduled_model(nervix_models::Model::Relay(CreateRelay {
            labels: Default::default(),
            name: named(name),
            schema: order_schema.clone(),
            buffer: nonzero!(2usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        }))
    };
    let processor_routes = |name: &str| {
        let mut routes = ProcessorOutputs::single(named(name));
        routes.routes[0].construction.inherit = Some(nervix_models::Inheritance::All);
        routes.with_flush_policy(FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        })
    };
    let schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                labels: Default::default(),
                name: order_schema.clone(),
                fields: vec![SchemaField {
                    name: named("order_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            order_relay("orders"),
            order_relay("projected_orders"),
            order_relay("left_orders"),
            order_relay("right_orders"),
            order_relay("joined_orders"),
            scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
                labels: Default::default(),
                name: named("dedup_orders"),
                from: ProcessorInputs::single(named("orders")),
                output_routes: processor_routes("projected_orders"),
                branched_by: BranchSelection::unbranched(),
                deduplicate_on: vec![expression("input.order_id")],
                max_time: "10m".to_string(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            })),
            scheduled_model(nervix_models::Model::Junction(CreateJunction {
                labels: Default::default(),
                name: named("join_orders"),
                from: ProcessorInputs::new(
                    vec![named("left_orders"), named("right_orders")],
                    Vec::new(),
                ),
                output_routes: processor_routes("joined_orders"),
                branched_by: BranchSelection::unbranched(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            })),
        ],
        Vec::new(),
    );

    runtime
        .rebuild_domain_from_schedule(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            &domain,
            Some(schedule),
            true,
        )
        .await
        .expect("standalone branch-preserving processors must build");
    runtime
        .rebuild_domain_from_schedule(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            &domain,
            None,
            true,
        )
        .await
        .expect("domain teardown must stop processor runtimes");
}

#[nervix_primitives::test]
async fn scheduled_processor_entity_swap_is_not_junction_specific() {
    let runtime = Runtime::default();
    attach_loopback_cluster(
        &runtime,
        &ClusterNodeName::parse("node-1").expect("valid name"),
    )
    .await;
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        unpaced_domain_state(domain.as_str()),
    )]));
    let event_schema = named::<SchemaName>("event");
    let processor = named::<DeduplicatorName>("deduplicate_events");
    let schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                labels: Default::default(),
                name: event_schema.clone(),
                fields: vec![SchemaField {
                    name: named("event_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                labels: Default::default(),
                name: named("events"),
                schema: event_schema.clone(),
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                labels: Default::default(),
                name: named("unique_events"),
                schema: event_schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
            scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
                labels: Default::default(),
                name: processor.clone(),
                from: ProcessorInputs::single(named("events")),
                output_routes: with_inherit_all(ProcessorOutputs::single(named("unique_events")))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                branched_by: BranchSelection::unbranched(),
                deduplicate_on: vec![expression("input.event_id")],
                max_time: "10m".to_string(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            })),
        ],
        Vec::new(),
    );

    runtime
        .rebuild_domain_from_schedule(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            &domain,
            Some(schedule.clone()),
            true,
        )
        .await
        .expect("scheduled deduplicator must build");
    let entity = NodeRef {
        kind: ModelKind::Deduplicator,
        identifier: ModelName::from(&processor),
    };
    assert_eq!(
        runtime.entity_pause_relays(
            &domain,
            &nervix_models::EntityGateTargets::Entities((std::slice::from_ref(&entity)).to_vec())
        ),
        vec![named("events")],
        "every scheduled processor swap must gate its input relays"
    );

    let mut desired = schedule;
    let nervix_models::Model::Deduplicator(config) = desired
        .nodes
        .values_mut()
        .find(|node| node.kind() == ModelKind::Deduplicator)
        .expect("schedule must contain the processor")
        .config
        .as_mut()
    else {
        panic!("scheduled processor must contain a deduplicator model");
    };
    config.mode = AckMode::Detached;

    let desired_revision = ExecutionRevision::from_schedule(&desired)
        .assured("the changed processor has a complete execution revision");
    runtime
        .swap_scheduled_nodes(
            &domain,
            desired_revision.clone(),
            &EntitySwapExecution {
                entities: vec![entity],
                reassignments: Vec::new(),
                dynamic_updates: Vec::new(),
                state_purges: BTreeMap::new(),
                gate_relays: Vec::new(),
                forced_recovery_relays: Vec::new(),
            },
        )
        .await
        .expect("non-junction scheduled processors must use the shared swap path");
    let execution = runtime
        .inner
        .executions
        .get(&domain)
        .expect("domain execution must remain installed");
    assert_eq!(
        execution.revision.ownership_handoff_fingerprint,
        desired_revision.ownership_handoff_fingerprint
    );
    assert!(execution.node_tasks.contains_key(&NodeUnitRef::new(
        NodeRef {
            kind: ModelKind::Deduplicator,
            identifier: ModelName::from(&processor),
        },
        Unit::SINGLE
    )));
    drop(execution);

    #[cfg(feature = "testing")]
    {
        let node = ClusterNodeName::parse("node-1")
            .assured("the test node is an identifier-shaped literal");
        let mut recovered = desired.clone();
        let nervix_models::Model::Deduplicator(config) = recovered
            .nodes
            .values_mut()
            .find(|node| node.kind() == ModelKind::Deduplicator)
            .assured("the test schedule contains the processor")
            .config
            .as_mut()
        else {
            panic!("scheduled processor must contain a deduplicator model");
        };
        config.mode = AckMode::Attached;
        runtime
            .inner
            .fault_injection
            .fail_next_entity_schedule_swap_on(node.clone(), domain.clone());

        let recovered_revision = ExecutionRevision::from_schedule(&recovered)
            .assured("the recovered processor has a complete execution revision");
        let delta = ExecutionDelta::between(Some(&desired), Some(&recovered));
        let application = runtime
            .apply_schedule_delta(&node, &recovered_revision, &delta, true)
            .await
            .assured("the domain rebuild recovers the injected entity swap failure");
        assert!(application.applied_incrementally);
        let expansion = application
            .recovery_expansion
            .assured("entity swap fallback reports its wider recovery scope");
        assert_eq!(expansion.domain, domain);
        assert!(
            expansion
                .reason
                .contains("injected entity-level schedule apply failure")
        );
        assert_eq!(
            expansion.scope,
            recovered.nodes.keys().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            runtime
                .inner
                .executions
                .get(&domain)
                .assured("recovery reinstalls the test domain")
                .revision
                .ownership_handoff_fingerprint,
            recovered_revision.ownership_handoff_fingerprint
        );
    }
}

#[nervix_primitives::test]
async fn scheduled_entity_swap_reinstalls_state_schema_fingerprints() {
    let runtime = Runtime::default();
    attach_loopback_cluster(
        &runtime,
        &ClusterNodeName::parse("node-1").expect("valid name"),
    )
    .await;
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        unpaced_domain_state(domain.as_str()),
    )]));
    let event_schema = named::<SchemaName>("event");
    let processor = named::<DeduplicatorName>("deduplicate_events");
    let schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                labels: Default::default(),
                name: event_schema.clone(),
                fields: vec![SchemaField {
                    name: named("event_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                labels: Default::default(),
                name: named("events"),
                schema: event_schema.clone(),
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                labels: Default::default(),
                name: named("unique_events"),
                schema: event_schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
            scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
                labels: Default::default(),
                name: processor.clone(),
                from: ProcessorInputs::single(named("events")),
                output_routes: with_inherit_all(ProcessorOutputs::single(named("unique_events")))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                branched_by: BranchSelection::unbranched(),
                deduplicate_on: vec![expression("input.event_id")],
                max_time: "10m".to_string(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            })),
        ],
        Vec::new(),
    );

    runtime
        .rebuild_domain_from_schedule(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            &domain,
            Some(schedule.clone()),
            true,
        )
        .await
        .expect("scheduled deduplicator must build");

    let mut desired = schedule;
    let processor_node = desired
        .nodes
        .values_mut()
        .find(|node| node.kind() == ModelKind::Deduplicator)
        .expect("schedule must contain the processor");
    processor_node.schema_fingerprint = SchemaFingerprint::from_digest([7; 32]);
    let nervix_models::Model::Deduplicator(config) = processor_node.config.as_mut() else {
        panic!("scheduled processor must contain a deduplicator model");
    };
    config.mode = AckMode::Detached;
    let entity = NodeRef {
        kind: ModelKind::Deduplicator,
        identifier: ModelName::from(&processor),
    };

    let desired_revision = ExecutionRevision::from_schedule(&desired)
        .assured("the changed processor has a complete execution revision");
    runtime
        .swap_scheduled_nodes(
            &domain,
            desired_revision,
            &EntitySwapExecution {
                entities: vec![entity],
                reassignments: Vec::new(),
                dynamic_updates: Vec::new(),
                state_purges: BTreeMap::new(),
                gate_relays: Vec::new(),
                forced_recovery_relays: Vec::new(),
            },
        )
        .await
        .expect("entity swap must apply");

    let installed = runtime
        .inner
        .state_identities
        .get(&DomainNodeUnitRef::single(DomainNodeRef::node_in(
            domain,
            ModelKind::Deduplicator,
            ModelName::from(&processor),
        )))
        .and_then(|slot| {
            slot.load_full()
                .map(|assignment| assignment.identity.schema_fingerprint)
        });
    assert_eq!(
        installed,
        Some(SchemaFingerprint::from_digest([7; 32])),
        "an entity swap must reinstall the schedule's state schema fingerprints so persisted \
         runtime state is not stranded under the pre-swap fingerprint"
    );
}
