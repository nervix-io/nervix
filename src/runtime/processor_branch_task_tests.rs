//! Layer: test harness.
//! Owns: processor branch reset, dispatch and handoff lifecycle regressions.
//! Depends on: the production branch supervisor and bounded test fixtures.
//! Must not know: external connectors, sessions or product configuration policy.

use std::time::Duration;

use ahash::HashMap;
use nervix_models::{
    CommandExecutionReference, CreateSchema, ErrorPolicies, MessageErrorPolicy, ModelKind,
    ModelName, NodeRef, ParseAsType, RelayName, SchemaField,
};
use nervix_primitives::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc, watch,
    },
    time::timeout,
};

use super::*;
use crate::{
    runtime::{
        processor_wasm_state_reset::processor_reset_target_keys,
        scheduled_node::ScheduledNodeHandoffError,
    },
    runtime_ack::AckSet,
    runtime_schema::{RuntimeValue, compile_schema, test_runtime_row},
};

fn reset_target_template(entity_kind: ModelKind, branched: bool) -> BranchInstanceTemplate {
    BranchInstanceTemplate {
        revision: ProcessorPlanRevision::new(),
        owner: NodeUnitRef::new(
            NodeRef::new(entity_kind, named::<ModelName>("counting_guest")),
            Unit::SINGLE,
        ),
        root_relay: named("counted_input_events"),
        branch: branched.then(|| named("by_tenant")),
        branch_ttl: None,
        branch_max_instances: None,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: HashMap::default(),
        processors: HashMap::default(),
        wasm_state_reset: None,
        partition_transition: None,
    }
}

#[test]
fn reset_target_keys_enforce_processor_kind_branching_and_fingerprint() {
    let processor = named::<ModelName>("counting_guest");
    let instances = BranchInstanceRegistry::new();
    let alpha =
        string_branch_key("tenant", "alpha").expect("the fixture branch key must be present");
    let alpha_scope = WasmStateResetScope::Branch(alpha.fingerprint());

    let not_wasm = reset_target_template(ModelKind::Deduplicator, false);
    let error = processor_reset_target_keys(
        &processor,
        &not_wasm,
        &instances,
        WasmStateResetScope::Unbranched,
        None,
    )
    .expect_err("a non-WASM processor cannot accept a WASM state reset");
    assert!(matches!(
        error.current_context(),
        WasmStateResetRuntimeError::NotWasmProcessor { .. }
    ));

    let unbranched = reset_target_template(ModelKind::WasmProcessor, false);
    let targets = processor_reset_target_keys(
        &processor,
        &unbranched,
        &instances,
        WasmStateResetScope::Unbranched,
        None,
    )
    .expect("an unbranched reset must select the singleton instance");
    assert_eq!(targets, vec![None]);

    let branched = reset_target_template(ModelKind::WasmProcessor, true);
    let targets = processor_reset_target_keys(&processor, &branched, &instances, alpha_scope, None)
        .expect("a published branch scope may recover without its original branch key fields");
    assert!(targets.is_empty());

    let beta = string_branch_key("tenant", "beta").expect("the fixture branch key must be present");
    let error =
        processor_reset_target_keys(&processor, &branched, &instances, alpha_scope, Some(beta))
            .expect_err("branch key fields must match the selected branch fingerprint");
    assert!(matches!(
        error.current_context(),
        WasmStateResetRuntimeError::InvalidScope { .. }
    ));
}

#[test]
fn reset_completion_rejects_a_different_fence_or_preparation() {
    let processor = named::<ModelName>("counting_guest");
    let request = CommandExecutionReference::parse("reset-alpha")
        .expect("the fixture reset reference must be valid");
    let other = CommandExecutionReference::parse("reset-beta")
        .expect("the fixture reset reference must be valid");
    let mut ready = nervix_models::WasmStateReset::publishing(
        request.clone(),
        WasmStateResetScope::Unbranched,
        nervix_models::WasmStateResetReason::Operator,
    );
    ready.mark_ready();

    let mut fence = Some(nervix_models::WasmStateReset::publishing(
        other.clone(),
        WasmStateResetScope::Unbranched,
        nervix_models::WasmStateResetReason::Operator,
    ));
    let mut prepared = None;
    let error = complete_processor_wasm_state_reset(&processor, &ready, &mut fence, &mut prepared)
        .expect_err("a ready revision cannot clear another request's fence");
    assert!(matches!(
        error.current_context(),
        WasmStateResetRuntimeError::RequestConflict { .. }
    ));

    fence = Some(nervix_models::WasmStateReset::publishing(
        request.clone(),
        WasmStateResetScope::Unbranched,
        nervix_models::WasmStateResetReason::Operator,
    ));
    prepared = Some(PreparedWasmStateReset {
        request: other,
        scope: WasmStateResetScope::Unbranched,
        published: true,
        branches: Vec::new(),
    });
    let error = complete_processor_wasm_state_reset(&processor, &ready, &mut fence, &mut prepared)
        .expect_err("a ready revision cannot clear another request's preparation");
    assert!(matches!(
        error.current_context(),
        WasmStateResetRuntimeError::RequestConflict { .. }
    ));
}

/// A processor task ended before it stopped its branches, as a shutdown grace ends one, drops
/// the handles of every branch it still holds. Each of those branches ends with its handle, so
/// none keeps running, holding the node's state store or settling acknowledgements, after the
/// processor that owned it is gone.
#[nervix_primitives::test]
async fn a_branch_task_ends_with_the_handle_its_processor_task_holds() {
    let (ended_tx, ended_rx) = nervix_primitives::sync::oneshot::channel::<()>();
    let (input, _input_rx) = mpsc::channel(1);
    let (commands, _command_rx) = mpsc::channel(1);
    let task = nervix_primitives::task::spawn(async move {
        let _ended = ended_tx;
        std::future::pending::<()>().await;
    });
    let entry = ProcessorBranchTask {
        input,
        commands,
        task: nervix_primitives::sync::blocking::Mutex::new(Some(AbortOnDropHandle::new(task))),
    };

    drop(entry);

    let ended = timeout(Duration::from_secs(5), ended_rx).await;
    assert!(
        ended.is_ok(),
        "a branch task kept running after the handle its processor task held was dropped"
    );
}

#[nervix_primitives::test]
async fn processor_branch_tasks_are_created_and_reused_per_branch_key() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    publish_state_identity(
        &runtime,
        &domain,
        ModelKind::Deduplicator,
        named::<ModelName>("dedup_users"),
    );
    let schema = Arc::new(compile_schema(&CreateSchema {
        name: named("notification"),
        fields: vec![SchemaField {
            name: named("user_id"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    }));
    let template = BranchInstanceTemplate {
        revision: ProcessorPlanRevision::new(),
        owner: NodeUnitRef::new(
            NodeRef::new(ModelKind::Deduplicator, named::<ModelName>("dedup_users")),
            Unit::SINGLE,
        ),
        root_relay: named("orders"),
        branch: None,
        branch_ttl: None,
        branch_max_instances: None,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            named("projected_orders"),
            RelayProcessorRelayTemplate {
                services: test_relay_boundary_services(),
            },
        )]
        .into_iter()
        .collect(),
        processors: [(
            named("dedup_users"),
            RelayProcessorTemplate {
                kind: ModelKind::Deduplicator,
                processor: named("dedup_users"),
                input_relays: vec![named("orders")],
                input_collect_policies: HashMap::default(),
                error_policies: ErrorPolicies::handled_by_log(),
                from_where: HashMap::default(),
                compiled_from_where: HashMap::default(),
                filter_where: None,
                compiled_filter_where: HashMap::default(),
                materialized_state: Vec::new(),
                operation: RelayProcessorOperationTemplate::Deduplicator {
                    output_routes: RelayProcessorOutputsTemplate {
                        routes: vec![RelayProcessorOutputTemplate {
                            output_relay: named("projected_orders"),
                            construction: nervix_models::RouteConstruction {
                                inherit: Some(nervix_models::Inheritance::All),
                                ..nervix_models::RouteConstruction::default()
                            },
                            flush_policy: Some(RuntimeFlushPolicy::Immediate {
                                minimum: nervix_models::DataPlaneSettings::default()
                                    .immediate_flush_minimum(),
                            }),
                            message_error_policy: MessageErrorPolicy::Log,
                            compiled_program: None,
                        }],
                    },
                    deduplicate_on: vec![expression("input.user_id")],
                    max_time: Duration::from_secs(600),
                    compiled_key_program: None,
                },
            },
        )]
        .into_iter()
        .collect(),
        wasm_state_reset: None,
        partition_transition: None,
    };
    let mut instances = ProcessorBranches::new();
    let domain_clock = runtime
        .bind_domain_clock(&domain)
        .expect("the fixture installs a running unpaced clock");
    let dequeued_work = || {
        NodeQuiesceWorkGuard::begin(runtime.node_quiesce_counters(
            &domain,
            NodeRef::new(ModelKind::Deduplicator, named::<ModelName>("dedup_users")),
        ))
    };
    let branch_batch = |user_id: i64, tenant: &str| {
        RelayRecordBatch::from_messages(
            schema.clone(),
            vec![RelayMessage {
                key: string_branch_key("tenant", tenant),
                record: test_runtime_row([("user_id".to_string(), RuntimeValue::I64(user_id))]),
                acks: AckSet::empty(),
            }],
        )
        .expect("branch batch should build")
    };

    dispatch_processor_node_input(
        ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            template: &template,
            domain_clock: &domain_clock,
        },
        &mut instances,
        named("orders"),
        branch_batch(42, "acme"),
        dequeued_work(),
    )
    .await;
    let mut states = instances.states();
    assert_eq!(states.len(), 1);
    let first = states.pop().expect("first branch task must exist");

    dispatch_processor_node_input(
        ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            template: &template,
            domain_clock: &domain_clock,
        },
        &mut instances,
        named("orders"),
        branch_batch(43, "acme"),
        dequeued_work(),
    )
    .await;
    let states = instances.states();
    assert_eq!(states.len(), 1);
    assert!(
        Arc::ptr_eq(&first, &states[0]),
        "same branch key must reuse the existing processor branch task"
    );

    dispatch_processor_node_input(
        ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            template: &template,
            domain_clock: &domain_clock,
        },
        &mut instances,
        named("orders"),
        branch_batch(7, "beta"),
        dequeued_work(),
    )
    .await;
    assert_eq!(instances.states().len(), 2);

    shutdown_all_processor_branch_instances(
        &runtime,
        &domain,
        &named::<ModelName>("dedup_users"),
        None,
        &mut instances,
    )
    .await;
    assert!(instances.states().is_empty());
}

#[nervix_primitives::test]
async fn processor_dispatch_hands_dequeued_work_into_branch_mailbox() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let processor = named::<ModelName>("route_orders");
    let input_relay = named::<RelayName>("orders");
    let template = junction_branch_template(processor.as_str(), input_relay.as_str());
    let counters =
        runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor));
    let (input_tx, mut input_rx) = mpsc::channel(1);
    let (commands, _command_rx) = mpsc::channel(1);
    let task = nervix_primitives::task::spawn(std::future::pending::<()>());
    let mut instances = ProcessorBranches::new();
    instances.insert_restored(
        None,
        Timestamp::from_unix_nanos(1),
        1,
        ProcessorBranchSlot::Live(ProcessorBranchTask {
            input: input_tx,
            commands,
            task: nervix_primitives::sync::blocking::Mutex::new(Some(AbortOnDropHandle::new(task))),
        }),
    );

    dispatch_processor_node_input(
        ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            template: &template,
            domain_clock: &runtime
                .bind_domain_clock(&domain)
                .expect("the fixture installs a running unpaced clock"),
        },
        &mut instances,
        input_relay.clone(),
        quiesce_test_batch(),
        NodeQuiesceWorkGuard::begin(counters.clone()),
    )
    .await;

    assert_eq!(counters.admitted_work(), 1);
    let queued = input_rx
        .recv()
        .await
        .expect("processor input should remain in the branch mailbox");
    assert_eq!(queued.relay, input_relay);
    assert_eq!(counters.admitted_work(), 1);
    drop(queued);
    assert_eq!(counters.admitted_work(), 0);

    let entry = instances
        .remove(&None)
        .expect("test branch task should remain registered");
    let task = entry
        .live()
        .expect("test branch task should still be live")
        .task
        .lock()
        .take()
        .expect("test branch task should still be running");
    task.abort();
    let _ = task.await;
}

#[nervix_primitives::test]
async fn accepted_processor_input_records_branch_activity_at_its_own_domain_time() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let processor = named::<ModelName>("route_orders");
    let input_relay = named::<RelayName>("orders");
    let template = junction_branch_template(processor.as_str(), input_relay.as_str());
    let counters =
        runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor));
    let (input_tx, mut input_rx) = mpsc::channel(1);
    let (commands, _command_rx) = mpsc::channel(1);
    let task = nervix_primitives::task::spawn(std::future::pending::<()>());
    let mut instances = ProcessorBranches::new();
    // A supervisor that started waiting long before this batch arrived would hold a sample
    // this old. Accepting the input must replace it with the domain time of the acceptance.
    let stale = Timestamp::from_unix_nanos(1_000_000_000);
    instances.insert_restored(
        None,
        stale,
        1,
        ProcessorBranchSlot::Live(ProcessorBranchTask {
            input: input_tx,
            commands,
            task: nervix_primitives::sync::blocking::Mutex::new(Some(AbortOnDropHandle::new(task))),
        }),
    );
    let domain_clock = runtime
        .bind_domain_clock(&domain)
        .expect("the fixture installs a running unpaced clock");
    let before = domain_clock
        .snapshot()
        .expect("the fixture installs a running unpaced clock")
        .now();

    dispatch_processor_node_input(
        ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            template: &template,
            domain_clock: &domain_clock,
        },
        &mut instances,
        input_relay.clone(),
        quiesce_test_batch(),
        NodeQuiesceWorkGuard::begin(counters.clone()),
    )
    .await;

    let mut recorded = None;
    for entry in instances.snapshot_entries() {
        if entry.key.is_none() {
            recorded = Some(entry.last_ingestion);
        }
    }
    let recorded = recorded.expect("the unbranched instance stays registered");
    assert!(
        recorded >= before,
        "accepted input must record activity at or after the acceptance, got {recorded:?}"
    );

    let queued = input_rx
        .recv()
        .await
        .expect("processor input should remain in the branch mailbox");
    drop(queued);
    let entry = instances
        .remove(&None)
        .expect("test branch task should remain registered");
    let task = entry
        .live()
        .expect("test branch task should still be live")
        .task
        .lock()
        .take()
        .expect("test branch task should still be running");
    task.abort();
    task.await
        .discarded("an aborted fixture task reports only its own cancellation");
}

#[nervix_primitives::test]
async fn detached_branch_publishes_buffered_route_output_before_it_stops() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let processor = named::<ModelName>("route_orders");
    let output = named::<RelayName>("routed_orders");
    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let mut downstream =
        RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
    let services = Arc::new(RelayBoundaryServices::new(
        fanout,
        1,
        0,
        Vec::new(),
        None,
        RelayBranchPresences::single(Arc::new(BranchPresence::new())),
        nervix_primitives::sync::Arc::new(crate::runtime_ack::AckRootTracker::default()),
    ));
    let owner = runtime
        .spawn_relay_owner_task(
            &domain,
            &output,
            Unit::SINGLE,
            services.clone(),
            RelayRetention::default(),
        )
        .expect("a whole relay schedules its single unit");
    let mut template = junction_branch_template(processor.as_str(), "orders");
    template
        .relays
        .insert(output.clone(), RelayProcessorRelayTemplate { services });
    let junction = template
        .processors
        .get_mut(&processor)
        .expect("the fixture declares its junction");
    let RelayProcessorOperationTemplate::Junction { output_routes } = &mut junction.operation
    else {
        panic!("the fixture must declare a junction");
    };
    output_routes.routes.push(RelayProcessorOutputTemplate {
        output_relay: output.clone(),
        construction: nervix_models::RouteConstruction::default(),
        flush_policy: Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(3600),
            max_batch_size: 1024 * 1024,
        }),
        message_error_policy: MessageErrorPolicy::Log,
        compiled_program: None,
    });
    let domain_clock = runtime
        .bind_domain_clock(&domain)
        .expect("the fixture installs a running unpaced clock");
    let mut branch = template
        .instantiate(&runtime, &domain, None, 1)
        .await
        .expect("the junction fixture instantiates")
        .into_inner();
    let node = branch
        .processors
        .get_mut(&processor)
        .expect("the fixture instantiates its junction");
    let RelayProcessorOperationNode::Junction { output_routes } = &mut node.operation else {
        panic!("the fixture must instantiate a junction");
    };
    let route = output_routes
        .routes
        .first_mut()
        .expect("the junction instantiates the route its template declares");
    let flush_due = route
        .enqueue(
            quiesce_test_batch(),
            &domain_clock,
            &domain_clock
                .snapshot()
                .expect("the fixture installs a running unpaced clock"),
        )
        .expect("an hour-long cadence arms its route deadline");
    assert!(
        !flush_due,
        "an hour-long cadence must hold the accepted output"
    );

    let (_input_sender, input) = mpsc::channel(1);
    let (commands, command_rx) = mpsc::channel(1);
    commands
        .send(ProcessorBranchCommand::Stop(
            ProcessorBranchStopMode::Detach,
        ))
        .await
        .expect("the stop command queues before the branch task runs");
    let (snapshot_shutdown, _) = watch::channel(false);
    timeout(
        Duration::from_secs(2),
        run_processor_branch_task(
            ProcessorRuntimeContext::new(runtime.clone(), domain.clone()),
            ProcessorBranchRunIdentity {
                processor: processor.clone(),
                incarnation: 1,
            },
            branch,
            input,
            command_rx,
            runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor)),
            ProcessorSnapshotTask {
                shutdown_tx: snapshot_shutdown,
                task: None,
                requests: None,
            },
        ),
    )
    .await
    .expect("a detached branch task stops");

    let published = timeout(Duration::from_millis(500), downstream.recv())
        .await
        .expect("detaching a branch must publish the output its route still buffers")
        .expect("the live relay owner keeps the output relay open");
    assert_eq!(published.message_count(), 1);
    owner
        .stop(Duration::from_secs(1))
        .await
        .expect("the fixture relay owner stops");
}

#[nervix_primitives::test]
async fn processor_handoff_drains_ready_batches_from_every_input() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let processor = named::<ModelName>("route_orders");
    publish_state_identity(&runtime, &domain, ModelKind::Junction, processor.clone());
    let orders = named::<RelayName>("orders");
    let returns = named::<RelayName>("returns");
    let mut template = junction_branch_template(processor.as_str(), orders.as_str());
    template
        .processors
        .get_mut(&processor)
        .expect("junction processor should exist")
        .input_relays
        .push(returns.clone());
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let orders_broadcast = RelayBroadcast::with_capacity(nonzero_capacity(2));
    let returns_broadcast = RelayBroadcast::with_capacity(nonzero_capacity(2));
    let orders_input = RelayRuntimeFanIn::new(orders_broadcast.new_receiver());
    let returns_input = RelayRuntimeFanIn::new(returns_broadcast.new_receiver());
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    let batch = |key, value| {
        RelayRecordBatch::single(
            schema.clone(),
            key,
            test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
            AckSet::empty(),
        )
        .expect("processor input batch should build")
    };
    orders_broadcast
        .broadcast(batch(acme.clone(), 1))
        .await
        .expect("orders batch should queue");
    returns_broadcast
        .broadcast(batch(beta.clone(), 2))
        .await
        .expect("returns batch should queue");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (commands, command_rx) = mpsc::channel(1);
    let (response, handoffs) = nervix_primitives::sync::oneshot::channel();
    commands
        .send(ProcessorNodeCommand::Handoff { response })
        .await
        .expect("handoff command should queue before the processor starts");
    let task = nervix_primitives::task::spawn(run_processor_node_runtime(
        ProcessorRuntimeContext::new(runtime.clone(), domain.clone()),
        template,
        vec![(orders, orders_input), (returns, returns_input)],
        shutdown_rx,
        ProcessorNodeChannels {
            commands: command_rx,
            claim_reports: None,
        },
        Vec::new(),
        Duration::from_secs(60),
    ));

    let handoffs = timeout(Duration::from_secs(2), handoffs)
        .await
        .expect("processor handoff should finish")
        .expect("processor should return handoff state");
    timeout(Duration::from_secs(2), task)
        .await
        .expect("processor supervisor should stop")
        .expect("processor supervisor should join");
    assert_eq!(handoffs.len(), 2);
    assert!(handoffs.iter().any(|handoff| handoff.key == acme));
    assert!(handoffs.iter().any(|handoff| handoff.key == beta));
    assert_eq!(
        runtime
            .node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor))
            .outstanding_work(),
        0
    );
    drop(shutdown_tx);
}

#[nervix_primitives::test]
async fn scheduled_processor_handoff_bounds_command_backpressure_and_aborts_the_task() {
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let (commands, _command_rx) = mpsc::channel(1);
    let (first_response, _first_receiver) = nervix_primitives::sync::oneshot::channel();
    commands
        .send(ProcessorNodeCommand::Handoff {
            response: first_response,
        })
        .await
        .expect("first command should fill the processor mailbox");
    let dropped = Arc::new(AtomicBool::new(false));
    let task_dropped = dropped.clone();
    let task = nervix_primitives::task::spawn(async move {
        let _dropped = Dropped(task_dropped);
        std::future::pending::<()>().await;
    });
    let scheduled = ScheduledNodeTask { commands, task };

    let error = scheduled
        .handoff_within(Duration::from_millis(10))
        .await
        .expect_err("a full command mailbox must bound handoff");

    assert!(matches!(
        error.current_context(),
        ScheduledNodeHandoffError::CommandTimeout
    ));
    assert!(dropped.load(Ordering::Acquire));
}

#[nervix_primitives::test]
async fn scheduled_processor_handoff_aborts_a_task_that_drops_its_response() {
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let (commands, mut command_rx) = mpsc::channel(1);
    let dropped = Arc::new(AtomicBool::new(false));
    let task_dropped = dropped.clone();
    let task = nervix_primitives::task::spawn(async move {
        let _dropped = Dropped(task_dropped);
        let Some(ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await else {
            panic!("scheduled processor must receive its handoff command")
        };
        drop(response);
        std::future::pending::<()>().await;
    });
    let scheduled = ScheduledNodeTask { commands, task };

    let error = scheduled
        .handoff(crate::runtime::entity_gate::task_stop_grace(
            nervix_models::DomainDrainTimeout::DEFAULT,
        ))
        .await
        .expect_err("a dropped handoff response must fail");

    assert!(matches!(
        error.current_context(),
        ScheduledNodeHandoffError::ResponseDropped
    ));
    assert!(dropped.load(Ordering::Acquire));
}

#[nervix_primitives::test]
async fn scheduled_processor_handoff_reports_an_unavailable_command_receiver() {
    let (commands, command_rx) = mpsc::channel(1);
    drop(command_rx);
    let task = nervix_primitives::task::spawn(std::future::pending::<()>());
    let scheduled = ScheduledNodeTask { commands, task };

    let error = scheduled
        .handoff_within(Duration::from_millis(10))
        .await
        .expect_err("a closed processor mailbox must fail handoff");

    assert!(matches!(
        error.current_context(),
        ScheduledNodeHandoffError::CommandUnavailable
    ));
}

#[nervix_primitives::test]
async fn scheduled_processor_handoff_bounds_the_response_wait() {
    let (commands, mut command_rx) = mpsc::channel(1);
    let task = nervix_primitives::task::spawn(async move {
        let Some(ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await else {
            panic!("scheduled processor must receive its handoff command")
        };
        let _response = response;
        std::future::pending::<()>().await;
    });
    let scheduled = ScheduledNodeTask { commands, task };

    let error = scheduled
        .handoff_within(Duration::from_millis(10))
        .await
        .expect_err("a stalled handoff response must time out");

    assert!(matches!(
        error.current_context(),
        ScheduledNodeHandoffError::ResponseTimeout
    ));
}

#[nervix_primitives::test]
async fn scheduled_processor_handoff_reports_a_failed_task_join() {
    let (commands, mut command_rx) = mpsc::channel(1);
    let task = nervix_primitives::task::spawn(async move {
        let Some(ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await else {
            panic!("scheduled processor must receive its handoff command")
        };
        response
            .send(Vec::new())
            .expect("handoff receiver must remain while the task responds");
        panic!("test processor task failure");
    });
    let scheduled = ScheduledNodeTask { commands, task };

    let error = scheduled
        .handoff_within(Duration::from_secs(1))
        .await
        .expect_err("a failed processor task must fail handoff");

    assert!(matches!(
        error.current_context(),
        ScheduledNodeHandoffError::TaskJoin
    ));
}

#[nervix_primitives::test]
async fn scheduled_processor_handoff_bounds_task_shutdown() {
    let (commands, mut command_rx) = mpsc::channel(1);
    let task = nervix_primitives::task::spawn(async move {
        let Some(ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await else {
            panic!("scheduled processor must receive its handoff command")
        };
        response
            .send(Vec::new())
            .expect("handoff receiver must remain while the task responds");
        std::future::pending::<()>().await;
    });
    let scheduled = ScheduledNodeTask { commands, task };

    let error = scheduled
        .handoff_within(Duration::from_millis(10))
        .await
        .expect_err("a processor task that does not stop must time out");

    assert!(matches!(
        error.current_context(),
        ScheduledNodeHandoffError::TaskStopTimeout
    ));
}
