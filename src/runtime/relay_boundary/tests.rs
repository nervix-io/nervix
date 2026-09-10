//! Layer: test harness.
//! Owns: focused verification of relay boundary routing, retention, and acknowledgement behavior.
//! May depend on: relay boundary internals and runtime test fixtures.
//! Must not know: production control-plane orchestration or connector protocols.

use std::sync::Arc as StdArc;

use ahash::HashMap;
use arc_swap::ArcSwapOption;
use arrow_array::Array;
use nervix_interconnect::{EntityGatePurpose, RelayPayload, RelayPayloadKind};
use nervix_models::{
    AckMode, ClusterNodeName, CreateRelay, CreateSchema, DomainName, DomainSchedule, ModelKind,
    ModelName, NodeRef, ParseAsType, RelayBranching, RelayName, RemoteAckRegistration, SchemaName,
    Timestamp,
};
use nonzero_ext::nonzero;
use tokio::{
    sync::watch,
    time::{Duration, sleep, timeout},
};
use triomphe::Arc;

use super::*;
use crate::{
    runtime_ack::{AckOutcome, AckSet},
    runtime_schema::{RuntimeValue, test_runtime_row},
};
#[tokio::test]
async fn relay_owner_buffer_remains_visible_in_entity_drain_status() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("orders");
    let services = test_relay_boundary_services();
    let _owner_receiver = services.activate_owner_buffer();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        services.fanout.clone(),
    );
    services
        .enqueue_owner_batch(
            &runtime.inner.metrics,
            &domain,
            &relay,
            None,
            &quiesce_test_batch(),
        )
        .await
        .expect("the relay owner buffer should admit the batch");

    let status = runtime.entity_drain_status(
        &domain,
        std::slice::from_ref(&relay),
        &[],
        EntityGatePurpose::ModelAlteration,
    );

    assert_eq!(status.buffered_relay_batches, 1);
    assert!(!status.is_drained());
}

#[tokio::test]
async fn relay_owner_buffer_retains_the_upstream_ack_until_fanout() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named("orders");
    let services = test_relay_boundary_services();
    let mut owner_receiver = services.activate_owner_buffer();
    let (acks, completion) = AckSet::root();
    let batch = RelayRecordBatch::single(
        test_schema(&[("value", ParseAsType::I64)]),
        None,
        test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
        acks.clone(),
    )
    .expect("relay owner ACK test batch should build");

    services
        .enqueue_owner_batch(&runtime.inner.metrics, &domain, &relay, None, &batch)
        .await
        .expect("the relay owner buffer should admit the batch");
    let completion = completion.wait();
    tokio::pin!(completion);
    acks.ack_success();
    assert!(
        timeout(Duration::from_millis(50), &mut completion)
            .await
            .is_err(),
        "owner admission must not complete the upstream ACK before fanout"
    );

    owner_receiver
        .recv()
        .await
        .expect("the owner buffer should retain the batch")
        .ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), &mut completion)
            .await
            .expect("owner fanout completion should resolve the ACK"),
        AckOutcome::Ack
    );
}

#[tokio::test]
async fn relay_dispatch_detaches_subscription_delivery_from_ack_chain() {
    let runtime = Runtime::default();
    let domain = DomainName::parse("default").expect("valid domain");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = RelayName::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("customer_id", ParseAsType::String)]);
    let registry = RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        RelayRetention::default(),
    );
    let mut subscription_rx = services.subscription_receiver();
    let mut runtime_rx = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);

    let (acks, completion) = AckSet::root();
    let batch = RelayRecordBatch::single(
        schema,
        string_branch_key("customer", "42"),
        test_runtime_row([(
            "customer_id".to_string(),
            RuntimeValue::String("42".to_string()),
        )]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");

    let subscription_batch = subscription_rx
        .recv()
        .await
        .expect("subscription should receive batch");
    assert!(subscription_batch.acks.iter().all(AckSet::is_empty));

    acks.ack_success();

    let runtime_message = runtime_rx
        .recv()
        .await
        .expect("runtime consumer should receive");
    for ack in runtime_message.acks.iter() {
        ack.ack_success();
    }

    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    drop(subscription_batch);
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn relay_dispatch_detaches_detached_runtime_consumers_from_ack_chain() {
    let runtime = Runtime::default();
    let domain = DomainName::parse("default").expect("valid domain");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = RelayName::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        RelayRetention::default(),
    );
    let mut runtime_rx = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Detached);
    let (acks, completion) = AckSet::root();
    let batch = RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");

    acks.ack_success();

    let runtime_message = runtime_rx
        .recv()
        .await
        .expect("runtime consumer should receive message");
    assert!(runtime_message.acks.iter().all(AckSet::is_empty));
    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn relay_runtime_consumer_broadcast_fans_out_to_multiple_attached_receivers() {
    let runtime = Runtime::default();
    let domain = DomainName::parse("default").expect("valid domain");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = RelayName::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        RelayRetention::default(),
    );
    let mut first_consumer = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);
    let mut second_consumer = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);

    let (acks, completion) = AckSet::root();
    let batch = RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");
    acks.ack_success();

    let first_message = first_consumer
        .recv()
        .await
        .expect("first runtime consumer should receive message");
    let second_message = second_consumer
        .recv()
        .await
        .expect("second runtime consumer should receive message");
    for ack in first_message.acks.iter().chain(second_message.acks.iter()) {
        ack.ack_success();
    }

    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn concrete_relay_reuses_branch_collapse_for_runtime_consumers() {
    let runtime = Runtime::default();
    let domain = DomainName::parse("default").expect("valid domain");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = RelayName::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = RelayRegistry::new();
    let branch_collapse = Arc::new(BranchCollapseNode::with_capacity(
        STUPID_CHANNEL_CAPACITY_REMOVE_ME,
    ));
    let mut first_fan_in = RelayRuntimeFanIn::new(
        branch_collapse.runtime_consumer_receiver_for_mode(AckMode::Attached),
    );
    let mut second_fan_in = RelayRuntimeFanIn::new(
        branch_collapse.runtime_consumer_receiver_for_mode(AckMode::Attached),
    );
    let services = Arc::new(RelayBoundaryServices::new(
        RelayBoundaryFanout::BranchCollapse(branch_collapse),
        2,
        0,
        Vec::new(),
        None,
    ));
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        RelayRetention::default(),
    );
    let mut relay_runtime = ConcreteRelayRuntime::new(ConcreteRelayRuntimeBuild {
        runtime: runtime.clone(),
        domain: domain.clone(),
        relay: relay.clone(),
        registry,
        services,
        key: Some(concrete_branch_key([(
            named("user_id"),
            RuntimeValue::U32(52),
        )])),
    });
    let (acks, completion) = AckSet::root();
    let batch = RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    relay_runtime
        .dispatch_boundary(&batch)
        .await
        .expect("concrete relay should dispatch");

    let received = timeout(Duration::from_secs(1), first_fan_in.recv())
        .await
        .expect("first fan-in should receive")
        .expect("first fan-in should stay open");
    assert_eq!(received.message_count(), 1);
    for ack in received.acks {
        ack.ack_success();
    }
    let received = timeout(Duration::from_secs(1), second_fan_in.recv())
        .await
        .expect("second fan-in should receive")
        .expect("second fan-in should stay open");
    assert_eq!(received.message_count(), 1);
    for ack in received.acks {
        ack.ack_success();
    }
    acks.ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn unbranched_relay_uses_direct_fanout_without_branch_collapse() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named("notifications");

    let fanout = runtime
        .relay_boundary_fanout_with_capacity(
            &domain,
            &relay,
            false,
            STUPID_CHANNEL_CAPACITY_REMOVE_ME,
        )
        .await;

    assert!(!fanout.uses_branch_collapse());
}

#[tokio::test]
async fn execution_builder_uses_direct_fanout_for_unbranched_relay() {
    let runtime = Runtime::default();
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        unpaced_domain_state(domain.as_str()),
    )]));
    let schema = named::<SchemaName>("notification");
    let relay = named::<RelayName>("notifications");

    runtime
        .rebuild_domain_from_schedule(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            &domain,
            Some(DomainSchedule::new(
                domain.clone(),
                vec![
                    scheduled_model(nervix_models::Model::Schema(CreateSchema {
                        name: schema.clone(),
                        fields: vec![nervix_models::SchemaField {
                            name: named("user_id"),
                            ty: ParseAsType::I64,
                            optional: false,
                            sensitive: false,
                        }],
                    })),
                    scheduled_model(nervix_models::Model::Relay(CreateRelay {
                        name: relay.clone(),
                        schema,
                        buffer: STUPID_CHANNEL_CAPACITY_REMOVE_ME,
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    })),
                ],
                Vec::new(),
            )),
            true,
        )
        .await
        .expect("unbranched relay execution should build");

    let execution = runtime
        .inner
        .executions
        .get(&domain)
        .expect("domain execution should exist");
    let services = execution
        .relay_services
        .get(&relay)
        .expect("relay services should exist");
    assert!(!services.fanout.uses_branch_collapse());
}

#[test]
fn relay_record_batches_can_be_concatenated_without_losing_metadata() {
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let left = RelayRecordBatch::single(
        schema.clone(),
        u32_branch_key("user_id", 42),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(100)),
        AckSet::empty(),
    )
    .expect("left batch should build");
    let right = RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 42),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(43))])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(200)),
        AckSet::empty(),
    )
    .expect("right batch should build");

    let concatenated = RelayRecordBatch::concat(vec![left, right]).expect("batches should concat");

    assert_eq!(concatenated.batch.batch().num_rows(), 2);
    let messages = concatenated
        .try_into_messages()
        .expect("concatenated batch should decode");
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0].record.metadata().ingested_at_low_watermark(),
        Timestamp::from_unix_nanos(100)
    );
    assert_eq!(
        messages[1].record.metadata().ingested_at_low_watermark(),
        Timestamp::from_unix_nanos(200)
    );
}

#[test]
fn relay_fanout_shares_arrow_columns_and_exposes_row_views() {
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let batch = RelayRecordBatch::from_messages(
        schema,
        vec![
            RelayMessage {
                key: u32_branch_key("user_id", 42),
                record: test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))]),
                acks: AckSet::empty(),
            },
            RelayMessage {
                key: u32_branch_key("user_id", 42),
                record: test_runtime_row([("user_id".to_string(), RuntimeValue::U32(43))]),
                acks: AckSet::empty(),
            },
        ],
    )
    .expect("relay batch should build");
    let source_column = batch.batch.batch().column(0).clone();

    let fanout = batch.into_attached_fanout(3);

    assert_eq!(fanout.len(), 3);
    for output in &fanout {
        assert!(StdArc::ptr_eq(
            &source_column,
            output.batch.batch().column(0)
        ));
    }
    let row = fanout[0]
        .runtime_row(1)
        .expect("a node may address an Arrow row view");
    assert_eq!(row_value(&row, "user_id"), Some(RuntimeValue::U32(43)));
}

#[tokio::test]
async fn owner_ingress_touches_expiring_stream_state() {
    let runtime = Runtime::default();
    let domain = DomainName::parse("default").expect("valid domain");
    let relay_id = RelayName::parse("notifications").expect("valid identifier");
    let expiring_state = runtime.expiring_stream_state(&domain, &relay_id);
    let registry = expiring_state.registry.clone();
    let services = test_relay_boundary_services();
    let (shutdown, _) = watch::channel(false);
    let mut relay_registries = HashMap::default();
    relay_registries.insert(relay_id.clone(), registry);
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let mut relay_schemas = HashMap::default();
    relay_schemas.insert(relay_id.clone(), schema.clone());
    let mut relay_services = HashMap::default();
    relay_services.insert(relay_id.clone(), services.clone());
    runtime.inner.executions.insert(
        domain.clone(),
        DomainExecution {
            schedule: DomainSchedule::new(domain.clone(), Vec::new(), Vec::new()),
            passive_only: false,
            start_version: 0,
            domain_clock: test_domain_clock(&domain),
            shutdown,
            graph: StdArc::new(ArcSwapOption::empty()),
            relay_registries,
            relay_schemas,
            relay_services,
            relay_branchings: HashMap::default(),
            relay_branching_schemas: HashMap::default(),
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            codecs: HashMap::default(),
            signaling_protocols: HashMap::default(),
            lookups: HashMap::default(),
            udfs: nervix_roto::UdfExecutor::default(),
            endpoint_routes: HashMap::default(),
            node_tasks: HashMap::default(),
            emitter_tasks: HashMap::default(),
            generator_tasks: HashMap::default(),
            reingestor_tasks: HashMap::default(),
            placement_tasks: HashMap::default(),
            relay_state_tasks: HashMap::default(),
            relay_owner_tasks: HashMap::default(),
            clients: HashMap::default(),
            tasks: Vec::new(),
        },
    );
    let batch_ipc = schema
        .batch_from_test_rows([[("user_id".to_string(), RuntimeValue::U32(42))]])
        .expect("batch should build")
        .encode_arrow_ipc(runtime.executor())
        .await
        .expect("batch ipc should serialize");

    let key = u32_branch_key("user_id", 42);
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay_id,
        expiring_state.registry.clone(),
        services,
        RelayRetention::default(),
    );
    runtime
        .handle_remote_stream(RelayPayload {
            kind: RelayPayloadKind::Ingress,
            domain: domain.clone(),
            relay: relay_id.clone(),
            key: BranchKey::to_remote_key(&key),
            batch_ipc,
            metadata: vec![
                test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(42))
                    .metadata()
                    .to_remote(),
            ],
            acks: vec![None],
            admission: Some(RemoteAckRegistration {
                ack_id: 1,
                reply_node_id: ClusterNodeName::parse("producer-node").expect("valid name"),
            }),
        })
        .await
        .expect("remote relay payload should dispatch");
    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            if runtime
                .describe_local_stream_exists(&domain, &relay_id, &key)
                .expect("stream existence should be queryable")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner should admit and observe the relay branch");
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should drain");
}

#[tokio::test]
async fn relay_owner_enforces_branch_capacity_across_batches() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = named("orders");
    let registry = RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        RelayRetention {
            branch_ttl: None,
            branch_capacity: Some(nonzero!(2usize)),
        },
    );
    let schema = test_schema(&[]);
    let keys = [
        string_branch_key("tenant", "acme"),
        string_branch_key("tenant", "globex"),
        string_branch_key("tenant", "initech"),
    ];
    for key in &keys {
        tokio::task::consume_budget().await;
        let batch = RelayRecordBatch::single(
            schema.clone(),
            key.clone(),
            test_runtime_row([]),
            AckSet::empty(),
        )
        .expect("relay batch should build");
        services
            .enqueue_owner_batch(&runtime.inner.metrics, &domain, &relay, None, &batch)
            .await
            .expect("owner should admit the batch");
    }

    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            if !registry.contains_key(&keys[0])
                && registry.contains_key(&keys[1])
                && registry.contains_key(&keys[2])
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("relay owner should evict the least-recently-used branch");
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn relay_owner_expires_branch_presence_by_ttl() {
    let fault_injection = ConfiguredFaultInjection::default();
    fault_injection.set_branch_instance_expiration_scan_interval(Duration::from_millis(5));
    let runtime = Runtime::with_persistence_and_temp_dir(
        None,
        Duration::from_secs(60),
        fault_injection,
        PathBuf::from(DEFAULT_TEMP_DIR),
    )
    .expect("runtime should build");
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = named("orders");
    let key = string_branch_key("tenant", "acme");
    let registry = RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        RelayRetention {
            branch_ttl: Some(Duration::from_millis(20)),
            branch_capacity: None,
        },
    );
    let batch = RelayRecordBatch::single(
        test_schema(&[]),
        key.clone(),
        test_runtime_row([]),
        AckSet::empty(),
    )
    .expect("relay batch should build");
    services
        .enqueue_owner_batch(&runtime.inner.metrics, &domain, &relay, None, &batch)
        .await
        .expect("owner should admit the batch");

    timeout(Duration::from_secs(1), async {
        while !registry.contains_key(&key) {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("relay owner should observe branch presence");
    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            if !registry.contains_key(&key) {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("relay owner should expire idle branch presence");
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn stop_domain_execution_preserves_expiring_relay_branch_registry() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named("notifications");
    let branch = string_branch_key("tenant", "acme");
    let expiring_state = runtime.expiring_stream_state(&domain, &relay);
    expiring_state.touch(&branch, Timestamp::from_unix_nanos(1));
    let (shutdown, _) = watch::channel(false);

    runtime
        .stop_domain_execution(
            &domain,
            DomainExecution {
                schedule: DomainSchedule::new(domain.clone(), Vec::new(), Vec::new()),
                passive_only: false,
                start_version: 0,
                domain_clock: test_domain_clock(&domain),
                shutdown,
                graph: StdArc::new(ArcSwapOption::empty()),
                relay_registries: HashMap::default(),
                relay_schemas: HashMap::default(),
                relay_services: HashMap::default(),
                relay_branchings: HashMap::default(),
                relay_branching_schemas: HashMap::default(),
                materialized_stream_specs: HashMap::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                branched_ingestors: HashMap::default(),
                branched_entrypoints: HashMap::default(),
                codecs: HashMap::default(),
                signaling_protocols: HashMap::default(),
                lookups: HashMap::default(),
                udfs: nervix_roto::UdfExecutor::default(),
                endpoint_routes: HashMap::default(),
                node_tasks: HashMap::default(),
                emitter_tasks: HashMap::default(),
                generator_tasks: HashMap::default(),
                reingestor_tasks: HashMap::default(),
                placement_tasks: HashMap::default(),
                relay_state_tasks: HashMap::default(),
                relay_owner_tasks: HashMap::default(),
                clients: HashMap::default(),
                tasks: Vec::new(),
            },
        )
        .await;

    assert!(expiring_state.contains_key(&branch));
}

#[tokio::test]
async fn relay_state_shutdown_drains_every_ready_batch() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = named::<RelayName>("materialized_orders");
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let mut assignment = runtime
        .replicated_materialized_stream_state(
            RuntimeStatePlacement {
                domain: domain.clone(),
                state: RuntimeStateKind::MaterializedRelay,
                kind: ModelKind::Relay,
                identifier: ModelName::from(&relay.clone()),
                schema_fingerprint: [0; 32],
                branch_key: None,
            },
            schema.arrow_schema(),
            None,
            Vec::new(),
            None,
        )
        .expect("materialized state should initialize");
    let state = assignment
        .originator
        .take()
        .expect("branch-local state should grant authoritative access");
    let broadcast = RelayBroadcast::with_capacity(nonzero_capacity(2));
    let receiver = RelayRuntimeFanIn::new(broadcast.new_receiver());
    let task = runtime.spawn_relay_state_task(
        &domain,
        RelayStateTaskSpec {
            relay: relay.clone(),
            state: state.clone(),
            retention: RelayRetention::default(),
            receiver,
        },
    );
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    for (key, value) in [(acme.clone(), 1), (beta.clone(), 2)] {
        tokio::task::consume_budget().await;
        broadcast
            .broadcast(
                RelayRecordBatch::single(
                    schema.clone(),
                    key,
                    test_runtime_row([("value".to_string(), RuntimeValue::I64(value))])
                        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(value)),
                    AckSet::empty(),
                )
                .expect("materialized batch should build"),
            )
            .await
            .expect("materialized batch should queue");
    }

    task.stop(Duration::from_secs(1))
        .await
        .expect("relay state task should drain before the shutdown deadline");

    assert!(
        state
            .read()
            .remote_entry(&acme)
            .expect("the acme record should be readable")
            .is_some()
    );
    assert!(
        state
            .read()
            .remote_entry(&beta)
            .expect("the beta record should be readable")
            .is_some()
    );
    assert_eq!(
        runtime
            .node_quiesce_counters(&domain, NodeRef::new(ModelKind::Relay, &relay))
            .outstanding_work(),
        0
    );
}

#[tokio::test]
async fn direct_fanout_owner_buffer_uses_configured_capacity() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named("orders");
    let schema = test_schema(&[]);
    let fanout = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(1))
        .await;
    let mut receiver = fanout.activate_owner_buffer();
    let owner_buffer = fanout.owner_buffer().expect("owner buffer must be active");

    owner_buffer
        .broadcast(
            RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("branch", "first"),
                test_runtime_row([]),
                AckSet::empty(),
            )
            .expect("first batch should build"),
        )
        .await
        .expect("first send should succeed");

    let pending_send = tokio::spawn({
        let owner_buffer = owner_buffer.clone();
        async move {
            owner_buffer
                .broadcast(
                    RelayRecordBatch::single(
                        schema,
                        string_branch_key("branch", "second"),
                        test_runtime_row([]),
                        AckSet::empty(),
                    )
                    .expect("second batch should build"),
                )
                .await
        }
    });

    sleep(Duration::from_millis(50)).await;
    assert!(
        !pending_send.is_finished(),
        "second send must wait for receiver capacity"
    );

    let first = receiver
        .recv()
        .await
        .expect("receiver should get first batch");
    assert_eq!(key_label(&first.key), r#"{"branch":"first"}"#);

    pending_send
        .await
        .expect("pending send should join")
        .expect("second send should succeed");

    let second = receiver
        .recv()
        .await
        .expect("receiver should get second batch");
    assert_eq!(key_label(&second.key), r#"{"branch":"second"}"#);
}

#[tokio::test]
async fn relay_boundary_fanout_resize_preserves_existing_owner_receiver() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named("orders");
    let schema = test_schema(&[]);
    let fanout = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(1))
        .await;
    let mut receiver = fanout.activate_owner_buffer();
    let resized = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(5))
        .await;

    let broadcast = match (&fanout, &resized) {
        (RelayBoundaryFanout::Direct(original), RelayBoundaryFanout::Direct(resized_fanout)) => {
            assert!(Arc::ptr_eq(original, resized_fanout));
            assert_eq!(resized_fanout.owner_buffer_len(), Some((0, 5)));
            assert_eq!(resized_fanout.subscriptions.capacity(), 1);
            assert_eq!(resized_fanout.attached_runtime_consumers.capacity(), 1);
            assert_eq!(resized_fanout.detached_runtime_consumers.capacity(), 1);
            resized_fanout
                .owner_buffer()
                .expect("owner buffer must remain active")
        }
        _ => panic!("unbranched relay must use direct fanout"),
    };

    broadcast
        .broadcast(
            RelayRecordBatch::single(
                schema,
                string_branch_key("branch", "after_resize"),
                test_runtime_row([]),
                AckSet::empty(),
            )
            .expect("batch should build"),
        )
        .await
        .expect("send after resize should succeed");

    let batch = receiver
        .recv()
        .await
        .expect("existing receiver should get batch after resize");
    assert_eq!(key_label(&batch.key), r#"{"branch":"after_resize"}"#);
}

#[test]
fn relay_batch_estimated_bytes_counts_arrow_payload_buffers() {
    let schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("user_id", ParseAsType::U32),
    ]);
    let batch = RelayRecordBatch::single(
        schema,
        None,
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("user_id".to_string(), RuntimeValue::U32(42)),
        ]),
        AckSet::empty(),
    )
    .expect("relay batch should build");
    let payload_bytes = batch
        .batch
        .batch()
        .columns()
        .iter()
        .map(|column| -> u64 {
            column
                .to_data()
                .get_slice_memory_size()
                .expect("test Arrow type should report its logical payload size")
                .arch_into()
        })
        .sum::<u64>();
    let allocated_bytes = batch
        .batch
        .batch()
        .columns()
        .iter()
        .map(|column| -> u64 { column.get_array_memory_size().arch_into() })
        .sum::<u64>();

    assert!(allocated_bytes > payload_bytes);
    assert_eq!(batch.estimated_bytes(), payload_bytes);
}

/// A decoded relay body is held to the payload its columns carry, not to the capacity Arrow
/// allocated while decoding them.
///
/// Every relay size limit is written in payload terms: `MAX BATCH SIZE`, the relay metrics and
/// the decoded bound a peer's body is measured against. The two numbers diverge widely for
/// string columns, so a guard that compared the allocated capacity against a payload limit
/// refused bodies the relay had itself produced, and the caller dropped the whole batch.
#[tokio::test]
async fn a_decoded_body_is_bounded_by_the_payload_it_carries() {
    use nervix_execution::{ExecutionConfig, OperationLimits};
    use ubyte::ByteUnit;

    let schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("user_id", ParseAsType::U32),
    ]);
    let batch = RelayRecordBatch::single(
        Arc::clone(&schema),
        None,
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("user_id".to_string(), RuntimeValue::U32(42)),
        ]),
        AckSet::empty(),
    )
    .expect("relay batch should build");

    let generous = Executor::default();
    let body = batch
        .batch
        .encode_arrow_ipc(&generous)
        .await
        .expect("the body should encode");
    let decoded = schema
        .decode_arrow_body(&generous, body.clone())
        .await
        .expect("the body should decode under the default limits");
    let payload = decoded.estimated_bytes();
    let allocated = decoded
        .batch()
        .columns()
        .iter()
        .map(|column| -> u64 { column.get_array_memory_size().arch_into() })
        .sum::<u64>();
    assert!(
        allocated > payload,
        "the batch must over-allocate for this bound to distinguish the two measures"
    );

    // A bound the payload fits and the allocation does not. Measured the old way this body
    // was refused; measured the way the relay sizes its own batches it is accepted.
    let midpoint = payload
        .checked_add(allocated.abs_diff(payload) / 2)
        .expect("two column sizes sum below the address space");
    let executor = Executor::new(ExecutionConfig {
        limits: OperationLimits {
            relay_decoded_bytes: ByteUnit::Byte(midpoint),
            ..OperationLimits::default()
        },
        ..ExecutionConfig::default()
    })
    .expect("a decoded bound below the default holds the default budgets");
    schema
        .decode_arrow_body(&executor, body)
        .await
        .expect("a body whose payload fits the decoded bound decodes");
}

/// A relay batch delivered to three destinations is encoded once and shared.
///
/// The body every destination carries is the same allocation, not three copies of the same
/// bytes, and it decodes back to exactly the fields, nulls and branch the source batch had.
/// Only the target relay and the acknowledgement obligations differ per destination.
#[tokio::test]
async fn a_three_destination_fanout_shares_one_encoded_body() {
    let executor = Executor::default();
    let schema = Arc::new(compile_schema(&CreateSchema {
        name: named::<SchemaName>("orders"),
        fields: vec![
            nervix_models::SchemaField {
                name: named("user_id"),
                ty: ParseAsType::U32,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("note"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: true,
            },
        ],
    }));
    let key = u32_branch_key("user_id", 7);
    // The optional field is left uninitialized, so it finalizes as a typed null and the
    // fanout has a null to preserve.
    let batch = RelayRecordBatch {
        key: key.clone(),
        keys: vec![key.clone()],
        batch: Arc::new(
            schema
                .batch_from_test_rows([[("user_id".to_string(), RuntimeValue::U32(7))]])
                .expect("the fanout test batch should build"),
        ),
        metadata: vec![
            test_runtime_row([("user_id".to_string(), RuntimeValue::U32(7))])
                .with_ingested_at_watermarks(Timestamp::from_unix_nanos(11))
                .metadata()
                .clone(),
        ],
        acks: vec![AckSet::empty()],
    };

    let admitted_before = executor.snapshot().data_cpu.admitted;
    let body = batch
        .batch
        .encode_arrow_ipc(&executor)
        .await
        .expect("the fanout body should encode");
    assert_eq!(
        executor
            .snapshot()
            .data_cpu
            .admitted
            .checked_sub(admitted_before)
            .expect("the admitted count only grows"),
        1,
        "one batch is one encode, however many destinations receive it"
    );

    let domain = domain("default");
    let consumers = ["one", "two", "three"].map(|relay| RemoteRuntimeConsumer {
        node_id: ClusterNodeName::parse(&format!("node-{relay}")).expect("valid name"),
        relay: named::<RelayName>(relay),
        mode: AckMode::Attached,
    });
    let payloads = consumers
        .iter()
        .enumerate()
        .map(|(index, consumer)| {
            routed_payload(RoutedDelivery {
                domain: &domain,
                consumer,
                batch: &batch,
                batch_ipc: body.clone(),
                acks: vec![Some(RemoteAckRegistration {
                    ack_id: index.arch_into(),
                    reply_node_id: ClusterNodeName::parse("node-source").expect("valid name"),
                })],
            })
        })
        .collect::<Vec<_>>();

    for payload in &payloads[1..] {
        assert!(
            payload
                .batch_ipc
                .shares_allocation_with(&payloads[0].batch_ipc),
            "every destination carries the one allocation the batch was encoded into"
        );
    }
    for (index, payload) in payloads.iter().enumerate() {
        assert_eq!(payload.relay, consumers[index].relay);
        assert_eq!(payload.key, BranchKey::to_remote_key(&key));
        assert_eq!(
            payload.acks,
            vec![Some(RemoteAckRegistration {
                ack_id: index.arch_into(),
                reply_node_id: ClusterNodeName::parse("node-source").expect("valid name"),
            })],
            "each destination owes its own acknowledgement"
        );
        let decoded = schema
            .decode_arrow_body(&executor, payload.batch_ipc.clone())
            .await
            .expect("every destination decodes the shared body");
        assert_eq!(decoded.batch(), batch.batch.batch());
        assert!(
            decoded.batch().column(1).is_null(0),
            "the optional field stays a typed null through the fanout"
        );
    }
}
