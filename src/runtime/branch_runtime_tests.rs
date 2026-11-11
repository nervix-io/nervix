//! Concrete branch lifecycle, retained state and required materialized-input regressions.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Branch dispatch, state restoration, eviction and parked-input regression cases.
//! - **Depends on.** The production branch runtime and its runtime test fixtures.
//! - **Must not know.** Connector protocols, Plexus parsing or application transactions.

use nervix_interconnect::EntityGatePurpose;
use nervix_models::{IngestorName, ModelKind, ModelName, NodeRef, ParseAsType, RelayName};
use nervix_primitives::{sync::Arc, time::timeout};

use super::*;
use crate::{
    runtime_ack::{AckOutcome, AckRootTracker, AckSet},
    runtime_schema::{RuntimeValue, test_runtime_row},
};

#[nervix_primitives::test]
async fn runtime_report_chain_branch_routing_logs() {
    use crate::runtime::report_observer::ReportLogObserver;

    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let mut branch = junction_branch_template("calculate", "orders")
        .instantiate(&runtime, &domain, None, 1)
        .await
        .assured("the fixture installs its junction branch")
        .into_inner();
    let snapshot = runtime
        .bind_domain_clock(&domain)
        .assured("the fixture installs its clock")
        .snapshot()
        .assured("the fixture clock is running");
    runtime.inner.domain_routings.remove(&domain);
    branch.routing = None;
    branch.routing_snapshot = None;
    let mut logs = ReportLogObserver::new();
    logs.observe(async {
        branch
            .retry_processor_pending_materialized(&named("calculate"))
            .await;
        branch.retry_materialized_waiters(&named("profiles")).await;
        branch.tick(&snapshot).await;
        branch.force_flush(&snapshot).await;
    })
    .await;
    for message in [
        "failed to refresh routing for pending processor work",
        "failed to refresh routing for materialized-state waiters",
        "failed to refresh routing for processor tick",
        "failed to refresh routing for processor flush",
    ] {
        assert_eq!(
            logs.next(message).fields["error"],
            "domain 'default' is not instantiated"
        );
    }
}

/// A branch that read its routing before a schedule made a relay materialized here, and that
/// reconciles its materialized relays after the schedule published that routing and advanced
/// the relay-state epoch, reconciles against the routing published with that epoch. The epoch
/// it records then stands for the membership it computed.
#[nervix_primitives::test]
async fn materialized_membership_is_reconciled_against_the_routing_its_epoch_follows() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let template = junction_branch_template("wait_for_customer", "orders");
    let mut branch = template
        .instantiate(&runtime, &domain, None, 1)
        .await
        .assured("a junction branch instantiates on the test domain")
        .into_inner();
    branch
        .refresh_domain_routing()
        .assured("the test domain publishes its routing");
    let profiles = named::<RelayName>("profiles");
    let state_schema = test_schema(&[("status", ParseAsType::String)]);
    let routing = runtime
        .inner
        .domain_routings
        .get(&domain)
        .verified("the test domain publishes its routing")
        .clone();
    routing.store(StdArc::new(DomainRoutingSnapshot {
        materialized_stream_specs: [(
            profiles.clone(),
            RuntimeMaterializedRelaySpec::new(
                state_schema.arrow_schema(),
                VmSchemaSensitivity::default(),
                ResolvedBranching::unbranched(),
            ),
        )]
        .into_iter()
        .collect(),
        ..DomainRoutingSnapshot::default()
    }));
    runtime.bump_relay_state_epoch(&domain);

    branch
        .reconcile_materialized_state_membership(&profiles)
        .await;

    let published = routing.load_full();
    let reconciled_against_published = match branch.routing_snapshot.as_ref() {
        Some(snapshot) => StdArc::ptr_eq(snapshot, &published),
        None => false,
    };
    assert!(
        reconciled_against_published,
        "the branch reconciled its materialized relays against routing older than the relay-state \
         epoch it read"
    );
}

#[nervix_primitives::test]
async fn pending_materialized_batches_remain_visible_in_entity_drain_status() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let processor = named::<ModelName>("wait_for_customer");
    let input_relay = named::<RelayName>("orders");
    let template = junction_branch_template(processor.as_str(), input_relay.as_str());
    let mut branch = template
        .instantiate(&runtime, &domain, None, 1)
        .await
        .expect("junction branch should instantiate")
        .into_inner();
    branch
        .processors
        .get_mut(&processor)
        .expect("junction processor should exist")
        .pending_materialized
        .push_back(PendingMaterializedBatch::new(
            input_relay,
            quiesce_test_batch(),
        ));
    let counters =
        runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor));
    let mut gauges = BranchQuiesceGauges::new(counters.clone());

    gauges.observe(&branch, &processor);

    assert_eq!(counters.admitted_work(), 0);
    assert_eq!(counters.parked_work(), 1);
    let status = runtime.entity_drain_status(
        &domain,
        &[],
        &nervix_models::EntityGateTargets::Entities(vec![NodeRef {
            kind: ModelKind::Junction,
            identifier: processor.clone(),
        }]),
        EntityGatePurpose::ModelAlteration,
    );
    assert_eq!(status.node_work_items, 1);
    assert!(!status.is_drained());

    let handoff_status = runtime.entity_drain_status(
        &domain,
        &[],
        &nervix_models::EntityGateTargets::Entities(vec![NodeRef {
            kind: ModelKind::Junction,
            identifier: processor,
        }]),
        EntityGatePurpose::OwnershipHandoff,
    );
    assert_eq!(handoff_status.node_work_items, 0);
    assert!(handoff_status.is_drained());

    drop(gauges);
    assert_eq!(counters.outstanding_work(), 0);
}

#[test]
fn blocking_materialized_wait_is_excluded_only_from_ownership_handoff_work() {
    let counters = Arc::new(NodeQuiesceCounters::default());
    let mut work = NodeQuiesceWorkGuard::begin(counters.clone());

    assert_eq!(
        counters.outstanding_work_for(EntityGatePurpose::ModelAlteration),
        1
    );
    assert_eq!(
        counters.outstanding_work_for(EntityGatePurpose::OwnershipHandoff),
        1
    );

    work.park_for_required_materialized_state();
    assert_eq!(counters.admitted_work(), 0);
    assert_eq!(counters.parked_work(), 1);
    assert_eq!(
        counters.outstanding_work_for(EntityGatePurpose::ModelAlteration),
        1
    );
    assert_eq!(
        counters.outstanding_work_for(EntityGatePurpose::OwnershipHandoff),
        0
    );

    work.resume_from_required_materialized_state();
    assert_eq!(counters.admitted_work(), 1);
    assert_eq!(counters.parked_work(), 0);
    drop(work);
    assert_eq!(counters.outstanding_work(), 0);
}

#[nervix_primitives::test]
async fn dropping_pending_materialized_batch_nacks_its_ack_root() {
    let tracker = Arc::new(AckRootTracker::default());
    let (acks, completion) = AckSet::tracked_root(tracker.clone());
    let pending = PendingMaterializedBatch::new(
        named("orders"),
        RelayRecordBatch::single(
            test_schema(&[("value", ParseAsType::I64)]),
            None,
            test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
            acks,
        )
        .expect("pending materialized test batch should build"),
    );

    assert_eq!(tracker.outstanding(), 1);
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    drop(pending);

    assert_eq!(
        completion.wait().await,
        AckOutcome::NoAck(
            "node stopped while waiting for required materialized state at relay 'orders'"
                .to_string()
        )
    );
    assert_eq!(tracker.outstanding(), 0);
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
}

/// Every force-flush generation and every state poll re-evaluates a message parked on
/// `REQUIRED WAIT`. While its dependency stays absent, the message stays parked throughout the
/// re-evaluation: its acknowledgement never reads as active work for a drain or an ownership
/// handoff, and its root publishes no progress, so the node that admitted the message has
/// nothing new to learn. A re-evaluation that unparked the message for its read and parked it
/// again afterwards would publish the message active and then parked once more.
#[nervix_primitives::test]
async fn a_parked_message_that_still_waits_stays_parked_through_its_re_evaluation() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let profiles = named::<RelayName>("profiles");
    publish_state_identity(
        &runtime,
        &domain,
        ModelKind::Relay,
        ModelName::from(&profiles),
    );
    let processor = named::<ModelName>("wait_for_customer");
    let input_relay = named::<RelayName>("orders");
    let mut template = junction_branch_template(processor.as_str(), input_relay.as_str());
    template
        .processors
        .get_mut(&processor)
        .assured("the junction fixture declares its processor")
        .materialized_state = vec![nervix_models::MaterializedStateDependency {
        relay: profiles.clone(),
        policy: nervix_models::MaterializedStatePolicy::RequiredWait,
    }];
    let mut branch = template
        .instantiate(&runtime, &domain, None, 1)
        .await
        .assured("a junction branch instantiates on the test domain")
        .into_inner();
    let state_schema = test_schema(&[("status", ParseAsType::String)]);
    let routing = runtime
        .inner
        .domain_routings
        .get(&domain)
        .verified("the test domain publishes its routing")
        .clone();
    routing.store(StdArc::new(DomainRoutingSnapshot {
        materialized_stream_specs: [(
            profiles.clone(),
            RuntimeMaterializedRelaySpec::new(
                state_schema.arrow_schema(),
                VmSchemaSensitivity::default(),
                ResolvedBranching::unbranched(),
            ),
        )]
        .into_iter()
        .collect(),
        ..DomainRoutingSnapshot::default()
    }));
    let tracker = Arc::new(AckRootTracker::default());
    let (acks, completion) = AckSet::tracked_root(tracker.clone());
    let batch = RelayRecordBatch::single(
        test_schema(&[("value", ParseAsType::I64)]),
        None,
        test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
        acks,
    )
    .assured("the single-row fixture batch matches its schema");

    branch
        .execute_processor_input(&processor, &input_relay, batch)
        .await;
    assert!(
        branch.processor_has_pending_materialized(&processor),
        "the absent dependency parks the message"
    );
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    let parked = completion.remote_progress();
    assert!(parked.1, "the parked root publishes its park");

    branch
        .retry_processor_pending_materialized(&processor)
        .await;
    branch.retry_materialized_waiters(&profiles).await;

    assert!(
        branch.processor_has_pending_materialized(&processor),
        "the dependency is still absent, so the message is still parked"
    );
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    assert_eq!(
        completion.remote_progress(),
        parked,
        "a re-evaluation that found the dependency still absent published the message active and \
         parked it again"
    );
}

#[test]
fn required_wait_ack_does_not_block_ownership_handoff_drain_status() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let ingestor = named::<IngestorName>("orders_source");
    let tracker = Arc::new(AckRootTracker::default());
    runtime.inner.in_flight_by_ingestor.insert(
        DomainNodeUnitRef::new(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
            Unit::SINGLE,
        ),
        tracker.clone(),
    );
    let (acks, _completion) = AckSet::tracked_root(tracker);
    let _required_wait = acks.park_guard();
    let affected = [NodeRef {
        kind: ModelKind::Ingestor,
        identifier: ModelName::from(&ingestor),
    }];

    let alteration = runtime.entity_drain_status(
        &domain,
        &[],
        &nervix_models::EntityGateTargets::Entities(affected.to_vec()),
        EntityGatePurpose::ModelAlteration,
    );
    assert_eq!(alteration.outstanding_acks, 1);
    assert!(!alteration.is_drained());

    let handoff = runtime.entity_drain_status(
        &domain,
        &[],
        &nervix_models::EntityGateTargets::Entities(affected.to_vec()),
        EntityGatePurpose::OwnershipHandoff,
    );
    assert_eq!(handoff.outstanding_acks, 0);
    assert!(handoff.is_drained());
}

/// Ownership capture waits for the route's already admitted batch to be published.
#[nervix_primitives::test]
async fn ownership_handoff_flushes_the_route_before_freezing_its_state() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let ingestor = named::<IngestorName>("orders_source");
    let relay = named::<RelayName>("orders");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let counters =
        runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Ingestor, ingestor.clone()));
    let force_flush = runtime.force_flush_participant(&domain, counters.clone());
    let (branch_sender, mut branch_output) = mpsc::channel(TWO_ITEM_TEST_CHANNEL_CAPACITY);
    let (_route_sender, route_input) = mpsc::channel(TWO_ITEM_TEST_CHANNEL_CAPACITY);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let mut route_task = IngestorRouteTask {
        runtime_handle: runtime.clone(),
        domain: domain.clone(),
        ingestor: ingestor.clone(),
        template: IngestorRouteTemplate {
            branch: BranchInstanceTemplate {
                revision: ProcessorPlanRevision::new(),
                owner: NodeUnitRef::new(
                    NodeRef::new(ModelKind::Ingestor, named::<ModelName>("orders_source")),
                    Unit::SINGLE,
                ),
                root_relay: relay.clone(),
                branch: None,
                branch_ttl: None,
                branch_max_instances: None,
                error_policies: ErrorPolicies::handled_by_log(),
                relays: HashMap::default(),
                processors: HashMap::default(),
                wasm_state_reset: None,
                partition_transition: None,
            },
            ack_boundary: BranchInstanceAckBoundary::Preserve,
            flush_policy: RuntimeFlushPolicy::Each {
                interval: Duration::from_secs(3600),
                max_batch_size: u64::from(u32::MAX),
            },
        },
        branch_sender,
        pending: HashMap::default(),
        quiesce: OutputBufferQuiesceGauge::new(counters.clone()),
    };
    let domain_clock = runtime
        .bind_domain_clock(&domain)
        .expect("the route fixture has an installed domain clock");
    route_task
        .accept(
            RelayRecordBatch::single(
                schema.clone(),
                None,
                test_runtime_row([("user_id".to_string(), RuntimeValue::U32(11))]),
                AckSet::empty(),
            )
            .expect("route input batch should build"),
            &domain_clock,
        )
        .await;
    assert_eq!(counters.admitted_work(), 1);
    let coordination = nervix_models::CoordinationIdentity::new(named("coordinator"), 1, 1);
    let entity = NodeRef::new(ModelKind::Ingestor, ingestor.clone());
    let engagement = {
        let runtime = runtime.clone();
        let domain = domain.clone();
        let entity = entity.clone();
        let coordination = coordination.clone();
        nervix_primitives::task::spawn(async move {
            runtime
                .engage_entity_gate_operation(
                    &coordination,
                    &domain,
                    &[],
                    &nervix_models::EntityGateTargets::Entities(vec![entity]),
                    EntityGatePurpose::OwnershipHandoff,
                    EntityGateLease {
                        deadline: nervix_primitives::time::Instant::now() + Duration::from_secs(5),
                        reason: "route flush before capture",
                    },
                )
                .await
        })
    };
    timeout(Duration::from_secs(1), async {
        while counters.force_flush_obligations() == 0 {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("handoff should publish the flush before waiting for the route");
    let task =
        nervix_primitives::task::spawn(route_task.run(route_input, shutdown_rx, force_flush));
    let forced = timeout(Duration::from_secs(1), branch_output.recv())
        .await
        .expect("the handoff flush must publish admitted route output before freezing")
        .expect("the branch entrypoint remains open");
    assert_eq!(
        row_value(&forced.runtime_row(0).expect("one row"), "user_id"),
        Some(RuntimeValue::U32(11))
    );
    timeout(Duration::from_secs(1), engagement)
        .await
        .expect("handoff should finish after the route flush")
        .expect("engagement task should not panic")
        .expect("handoff gate should engage");
    assert!(runtime.entity_gate_operation_is_held(&coordination));
    timeout(Duration::from_secs(1), async {
        while !runtime
            .entity_gate_operation_drain_status(
                &coordination,
                &domain,
                &[],
                &nervix_models::EntityGateTargets::Entities(
                    (std::slice::from_ref(&entity)).to_vec(),
                ),
                EntityGatePurpose::OwnershipHandoff,
            )
            .expect("the exact handoff scope reports its drain")
            .is_drained()
        {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("drain status publishes the freeze after the requested flush completes");
    let freeze = OwnershipHandoffFreezeWatch::new(&runtime, entity.in_domain(&domain));
    assert!(freeze.observe().is_frozen());
    assert!(
        runtime
            .entity_drain_status(
                &domain,
                &[],
                &nervix_models::EntityGateTargets::Entities(vec![entity]),
                EntityGatePurpose::OwnershipHandoff
            )
            .is_drained()
    );
    runtime
        .release_entity_gate_operation(&coordination, &domain)
        .await
        .expect("handoff releases its exact hold");
    shutdown.send_replace(true);
    timeout(Duration::from_secs(1), task)
        .await
        .expect("route stops")
        .expect("route does not panic");
}

/// A domain force flush releases route buffers that a long logical cadence still holds, and
/// the route's obligation clears only after its output has been published.
#[nervix_primitives::test]
async fn force_flush_releases_held_ingestor_route_buffers() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let ingestor = named::<IngestorName>("orders_source");
    let relay = named::<RelayName>("orders");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let counters =
        runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Ingestor, ingestor.clone()));
    let force_flush = runtime.force_flush_participant(&domain, counters.clone());
    let (branch_sender, mut branch_output) = mpsc::channel(TWO_ITEM_TEST_CHANNEL_CAPACITY);
    let (route_sender, route_input) = mpsc::channel(TWO_ITEM_TEST_CHANNEL_CAPACITY);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let route_task = IngestorRouteTask {
        runtime_handle: runtime.clone(),
        domain: domain.clone(),
        ingestor: ingestor.clone(),
        template: IngestorRouteTemplate {
            branch: BranchInstanceTemplate {
                revision: ProcessorPlanRevision::new(),
                owner: NodeUnitRef::new(
                    NodeRef::new(ModelKind::Ingestor, named::<ModelName>("orders_source")),
                    Unit::SINGLE,
                ),
                root_relay: relay.clone(),
                branch: None,
                branch_ttl: None,
                branch_max_instances: None,
                error_policies: ErrorPolicies::handled_by_log(),
                relays: HashMap::default(),
                processors: HashMap::default(),
                wasm_state_reset: None,
                partition_transition: None,
            },
            ack_boundary: BranchInstanceAckBoundary::Preserve,
            flush_policy: RuntimeFlushPolicy::Each {
                interval: Duration::from_secs(3600),
                max_batch_size: u64::from(u32::MAX),
            },
        },
        branch_sender,
        pending: HashMap::default(),
        quiesce: OutputBufferQuiesceGauge::new(counters.clone()),
    };
    let task =
        nervix_primitives::task::spawn(route_task.run(route_input, shutdown_rx, force_flush));

    route_sender
        .send(
            RelayRecordBatch::single(
                schema.clone(),
                None,
                test_runtime_row([("user_id".to_string(), RuntimeValue::U32(11))]),
                AckSet::empty(),
            )
            .expect("route input batch should build"),
        )
        .await
        .expect("the route task should accept input");
    timeout(Duration::from_secs(1), async {
        while counters.admitted_work() != 1 {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the route output should enter its branch buffer");
    assert!(
        timeout(Duration::from_millis(20), branch_output.recv())
            .await
            .is_err(),
        "a long logical cadence must keep the route output buffered"
    );

    runtime.force_flush_domain(&domain);

    let forced = timeout(Duration::from_secs(1), branch_output.recv())
        .await
        .expect("the force flush should publish the buffered route output")
        .expect("the branch entrypoint channel should remain open");
    assert_eq!(
        row_value(
            &forced
                .runtime_row(0)
                .expect("the forced output should contain an Arrow row"),
            "user_id",
        ),
        Some(RuntimeValue::U32(11))
    );
    timeout(Duration::from_secs(1), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let pending = runtime
                .inner
                .force_flush_by_domain
                .get(&domain)
                .map(|force_flush| force_flush.pending())
                .unwrap_or_default();
            if pending == 0 {
                break;
            }
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the route obligation should clear once its output is published");
    assert_eq!(counters.admitted_work(), 0);

    shutdown.send_replace(true);
    timeout(Duration::from_secs(1), task)
        .await
        .expect("the route task should stop")
        .expect("the route task should not panic");
}
