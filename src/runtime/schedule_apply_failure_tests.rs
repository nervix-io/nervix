//! Failures of applying a schedule to this node's runtime.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests that a failed step of a schedule swap, reassignment or processor plan
//!   binding names the step and the node it concerns beneath the domain whose execution it was
//!   building.
//! - **Depends on.** The production schedule application path and the runtime's test fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

use std::collections::BTreeMap;

use nervix_models::{
    AckMode, BranchSelection, ClusterNodeName, CodecWireFormat, CommandExecutionReference,
    CreateClientZeroMq, CreateCodec, CreateDeduplicator, CreateEmitter, CreateGenerator,
    CreateReingestor, CreateRelay, CreateSchema, DomainSchedule, EmitSink, EmitterBody,
    EmitterPublishingMode, ErrorPolicies, FlushPolicy, MaterializedRelayState, MessageErrorPolicy,
    ModelKind, ModelName, NodeRef, OutputBranch, ParseAsType, ProcessorInputs, ProcessorOutput,
    ProcessorOutputs, RelayBranching, RetryPolicy, RouteConstruction, ScheduledNode, SchemaField,
    SchemaName, WasmStateReset, WasmStateResetReason, WasmStateResetScope,
};
use nervix_primitives::{
    sync::{mpsc, watch},
    task::spawn,
};
use nonzero_ext::nonzero;

use super::*;

/// One deduplicator between two unbranched relays of one schema.
fn deduplicating_revision(domain: &DomainName) -> Arc<ExecutionRevision> {
    let event_schema = named::<SchemaName>("event");
    let schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                name: event_schema.clone(),
                fields: vec![SchemaField {
                    name: named("event_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                name: named("events"),
                schema: event_schema.clone(),
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                name: named("unique_events"),
                schema: event_schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
            scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
                name: named("deduplicate_events"),
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
    ExecutionRevision::from_schedule(&schedule)
        .assured("the deduplicating fixture schedules a complete domain revision")
}

/// A schedule change that swaps `entity` and nothing else.
fn swap_of(entity: NodeRef) -> EntitySwapExecution {
    EntitySwapExecution {
        entities: vec![entity],
        reassignments: Vec::new(),
        dynamic_updates: Vec::new(),
        state_purges: BTreeMap::new(),
        gate_relays: Vec::new(),
    }
}

/// The whole chain of a failed build of `domain`'s execution whose failed step reads `step`.
fn failed_build(domain: &DomainName, step: &str) -> String {
    format!("failed to build domain execution for '{domain}': {step}")
}

#[nervix_primitives::test]
async fn a_swap_names_the_node_its_desired_revision_does_not_plan() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let revision = test_execution_revision(&domain, Vec::new());
    let kinds = [
        ModelKind::WasmProcessor,
        ModelKind::Relay,
        ModelKind::Ingestor,
        ModelKind::Emitter,
        ModelKind::Reingestor,
        ModelKind::Generator,
        ModelKind::Deduplicator,
    ];
    for kind in kinds {
        nervix_primitives::task::consume_budget().await;
        let entity = NodeRef::new(kind, named::<ModelName>("absent"));
        let error = runtime
            .swap_scheduled_nodes(&domain, revision.clone(), &swap_of(entity))
            .await
            .expect_err("a swap of a node its revision does not plan must fail");
        let step = format!(
            "the desired execution revision has no plan for {} 'absent'",
            kind.as_str()
        );
        assert_eq!(format!("{error:#}"), failed_build(&domain, &step));
    }
}

#[nervix_primitives::test]
async fn a_swap_names_the_step_that_found_no_installed_execution() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let revision = deduplicating_revision(&domain);

    let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));
    let transition = runtime
        .swap_scheduled_nodes(&domain, revision.clone(), &swap_of(relay))
        .await
        .expect_err("a relay transition needs the installed execution");
    assert_eq!(
        format!("{transition:#}"),
        failed_build(
            &domain,
            "the domain execution is not installed while transitioning a relay"
        )
    );

    let processor = NodeRef::new(
        ModelKind::Deduplicator,
        named::<ModelName>("deduplicate_events"),
    );
    let swap = runtime
        .swap_scheduled_nodes(&domain, revision, &swap_of(processor))
        .await
        .expect_err("a processor swap needs the installed execution");
    assert_eq!(
        format!("{swap:#}"),
        failed_build(
            &domain,
            "the domain execution is not installed while swapping a processor"
        )
    );
}

#[nervix_primitives::test]
async fn a_relay_transition_names_the_part_of_the_relay_this_node_lacks() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let revision = deduplicating_revision(&domain);
    let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));

    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot::default(),
    );
    let schemaless = runtime
        .swap_scheduled_nodes(&domain, revision.clone(), &swap_of(relay.clone()))
        .await
        .expect_err("a relay without a schema on this node cannot transition");
    assert_eq!(
        format!("{schemaless:#}"),
        failed_build(&domain, "relay 'events' has no schema on this node")
    );

    let mut routing = DomainRoutingSnapshot::default();
    routing.relay_schemas.insert(
        named("events"),
        test_schema(&[("event_id", ParseAsType::I64)]),
    );
    install_test_domain_execution(&runtime, &domain, Vec::new(), routing);
    let unbounded = runtime
        .swap_scheduled_nodes(&domain, revision, &swap_of(relay))
        .await
        .expect_err("a relay without a runtime boundary on this node cannot transition");
    assert_eq!(
        format!("{unbounded:#}"),
        failed_build(
            &domain,
            "relay 'events' has no runtime boundary on this node"
        )
    );
}

#[nervix_primitives::test]
async fn a_processor_swap_names_the_processor_the_installed_revision_does_not_plan() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot::default(),
    );
    let processor = NodeRef::new(
        ModelKind::Deduplicator,
        named::<ModelName>("deduplicate_events"),
    );

    let error = runtime
        .swap_scheduled_nodes(
            &domain,
            deduplicating_revision(&domain),
            &swap_of(processor),
        )
        .await
        .expect_err("a processor the installed revision never planned has nothing to replace");
    assert_eq!(
        format!("{error:#}"),
        failed_build(
            &domain,
            "the installed execution revision has no plan for deduplicator 'deduplicate_events'"
        )
    );
}

#[nervix_primitives::test]
async fn a_reassignment_names_the_execution_or_the_node_it_cannot_find() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let local =
        ClusterNodeName::parse("node-1").assured("the test node is an identifier-shaped literal");
    let revision = test_execution_revision(&domain, Vec::new());
    let absent = NodeRef::new(ModelKind::Deduplicator, named::<ModelName>("absent"));

    let uninstalled = runtime
        .rebind_reassigned_nodes(
            &domain,
            &revision,
            std::slice::from_ref(&absent),
            Some(&local),
        )
        .await
        .expect_err("a reassignment needs the installed execution");
    assert_eq!(
        format!("{uninstalled:#}"),
        failed_build(
            &domain,
            "the domain execution is not installed while reassigning scheduled nodes"
        )
    );

    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot::default(),
    );
    let unplanned = runtime
        .rebind_reassigned_nodes(
            &domain,
            &revision,
            std::slice::from_ref(&absent),
            Some(&local),
        )
        .await
        .expect_err("a reassigned node its revision does not plan cannot be placed");
    assert_eq!(
        format!("{unplanned:#}"),
        failed_build(
            &domain,
            "the desired execution revision has no plan for deduplicator 'absent'"
        )
    );
}

#[nervix_primitives::test]
async fn processor_plans_bind_only_against_an_installed_execution() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let revision = deduplicating_revision(&domain);
    let unbound = failed_build(
        &domain,
        "the domain execution is not installed while binding processor plans",
    );

    let update = runtime
        .apply_dynamic_schedule_update(&domain, revision.clone(), &[])
        .await
        .expect_err("a dynamic update binds the plans of the installed execution");
    assert_eq!(format!("{update:#}"), unbound);

    let spec = revision
        .processors
        .processors
        .first()
        .assured("the deduplicating revision plans its deduplicator");
    let plan = runtime
        .bind_installed_processor_plan(&domain, spec)
        .await
        .expect_err("a processor plan binds against the installed execution");
    assert_eq!(format!("{plan:#}"), unbound);
}

#[nervix_primitives::test]
async fn a_materialized_relay_transition_needs_the_local_cluster_node() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let event_schema = named::<SchemaName>("event");
    let revision = test_execution_revision(
        &domain,
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                name: event_schema.clone(),
                fields: vec![SchemaField {
                    name: named("event_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            scheduled_model(nervix_models::Model::Relay(CreateRelay {
                name: named("latest_events"),
                schema: event_schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: Some(MaterializedRelayState::LastByTimestamp),
            })),
        ],
    );
    let mut routing = DomainRoutingSnapshot::default();
    routing.relay_schemas.insert(
        named("latest_events"),
        test_schema(&[("event_id", ParseAsType::I64)]),
    );
    routing
        .relay_services
        .insert(named("latest_events"), test_relay_boundary_services());
    install_test_domain_execution(&runtime, &domain, Vec::new(), routing);

    let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("latest_events"));
    let error = runtime
        .swap_scheduled_nodes(&domain, revision, &swap_of(relay))
        .await
        .expect_err("a node that does not know itself cannot place materialized relay state");
    assert_eq!(
        format!("{error:#}"),
        failed_build(
            &domain,
            "the local cluster node is not known while transitioning relay 'latest_events'"
        )
    );
}

/// Installs a running task of the WASM processor `processor` in `domain`'s execution, whose
/// commands arrive at `commands`.
fn install_wasm_processor_task(
    runtime: &Runtime,
    domain: &DomainName,
    processor: &ModelName,
    commands: mpsc::Sender<ProcessorNodeCommand>,
    task: JoinHandle<()>,
) {
    let mut execution = runtime
        .inner
        .executions
        .get_mut(domain)
        .assured("the test installed the domain's execution");
    execution.node_tasks.insert(
        NodeRef::new(ModelKind::WasmProcessor, processor.clone()),
        ScheduledNodeTask { commands, task },
    );
}

#[nervix_primitives::test]
async fn a_wasm_state_reset_keeps_the_report_of_the_task_that_did_not_apply_it() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot::default(),
    );
    let processor = named::<ModelName>("guest");
    let request = CommandExecutionReference::parse("reset-guest")
        .assured("the request reference is an identifier-shaped literal");
    let reset = [DynamicExecutionUpdate::WasmStateReset {
        processor: processor.clone(),
        reset: WasmStateReset::publishing(
            request,
            WasmStateResetScope::AllBranches,
            WasmStateResetReason::Operator,
        ),
    }];

    let (commands, closed) = mpsc::channel(1);
    drop(closed);
    install_wasm_processor_task(&runtime, &domain, &processor, commands, spawn(async {}));
    let unsent = runtime
        .apply_dynamic_model_updates(&domain, &reset)
        .await
        .expect_err("a task that reads no more commands cannot reset its state");
    assert_eq!(
        format!("{unsent:#}"),
        failed_build(
            &domain,
            "WASM processor 'guest' reset command channel closed"
        )
    );

    let (commands, mut received) = mpsc::channel(1);
    let unanswering = spawn(async move {
        let command = received.recv().await;
        drop(command);
    });
    install_wasm_processor_task(&runtime, &domain, &processor, commands, unanswering);
    let unanswered = runtime
        .apply_dynamic_model_updates(&domain, &reset)
        .await
        .expect_err("a task that drops the reset unanswered fails it");
    assert_eq!(
        format!("{unanswered:#}"),
        failed_build(
            &domain,
            "WASM processor 'guest' dropped its reset command response"
        )
    );

    let (commands, mut received) = mpsc::channel(1);
    let unrestored = processor.clone();
    let refusing = spawn(async move {
        if let Some(ProcessorNodeCommand::ApplyWasmStateReset { response, .. }) =
            received.recv().await
        {
            let refusal = Report::new(WasmStateResetRuntimeError::BranchesUnrestored {
                processor: unrestored,
            });
            response
                .send(Err(refusal))
                .assured("the update awaits the answer to its reset");
        }
    });
    install_wasm_processor_task(&runtime, &domain, &processor, commands, refusing);
    let refused = runtime
        .apply_dynamic_model_updates(&domain, &reset)
        .await
        .expect_err("a reset the task refuses fails the update");
    assert_eq!(
        format!("{refused:#}"),
        failed_build(
            &domain,
            "WASM processor 'guest' has not restored the branches its lifecycle checkpoint names \
             yet"
        )
    );
}

#[nervix_primitives::test]
async fn a_flush_change_names_the_emitter_whose_task_did_not_take_it() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot::default(),
    );
    let (commands, closed) = mpsc::channel(1);
    drop(closed);
    let (stop_signal, _) = watch::channel(None);
    runtime
        .inner
        .executions
        .get_mut(&domain)
        .assured("the test installed the domain's execution")
        .emitter_tasks
        .insert(
            NodeRef::new(ModelKind::Emitter, named::<ModelName>("audit")),
            ScheduledEmitterTask {
                commands,
                stop_signal,
                task: spawn(async {}),
            },
        );

    let flush = [DynamicExecutionUpdate::EmitterFlush {
        emitter: named("audit"),
        policy: FlushPolicy::Immediate,
    }];
    let error = runtime
        .apply_dynamic_model_updates(&domain, &flush)
        .await
        .expect_err("a task that reads no more commands cannot change its flush policy");
    assert_eq!(
        format!("{error:#}"),
        failed_build(
            &domain,
            "failed to reconfigure the flush policy of emitter 'audit': scheduled emitter task is \
             unavailable for reconfiguration"
        )
    );
}

/// The nodes of one emitter, `audit`, that publishes the relay `events` through a ZeroMQ client.
fn emitting_nodes() -> Vec<ScheduledNode> {
    vec![
        scheduled_model(nervix_models::Model::Schema(CreateSchema {
            name: named("event"),
            fields: vec![SchemaField {
                name: named("seq"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        })),
        scheduled_model(nervix_models::Model::Codec(CreateCodec {
            name: named("event_codec"),
            wire_format: CodecWireFormat::Syslog,
            schema: named("event"),
            encoding_rules: Vec::new(),
        })),
        scheduled_model(nervix_models::Model::ClientZeroMq(CreateClientZeroMq {
            name: named("sink"),
            mount: None,
            config: Vec::new(),
        })),
        scheduled_model(nervix_models::Model::Relay(CreateRelay {
            name: named("events"),
            schema: named("event"),
            buffer: nonzero!(2usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        })),
        scheduled_model(nervix_models::Model::Emitter(CreateEmitter {
            name: named("audit"),
            from: ProcessorInputs::single(named("events")),
            body: EmitterBody::Codec {
                codec: named("event_codec"),
            },
            sink: Box::new(EmitSink::ZeroMq {
                client: named("sink"),
            }),
            batch: None,
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        })),
    ]
}

#[nervix_primitives::test]
async fn an_emitter_swap_names_the_installed_part_it_cannot_find() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let revision = test_execution_revision(&domain, emitting_nodes());
    let emitter = NodeRef::new(ModelKind::Emitter, named::<ModelName>("audit"));

    let uninstalled = runtime
        .swap_scheduled_nodes(&domain, revision.clone(), &swap_of(emitter.clone()))
        .await
        .expect_err("an emitter swap needs the installed execution");
    assert_eq!(
        format!("{uninstalled:#}"),
        failed_build(
            &domain,
            "the domain execution is not installed while swapping an emitter"
        )
    );

    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot::default(),
    );
    let unplanned = runtime
        .swap_scheduled_nodes(&domain, revision, &swap_of(emitter))
        .await
        .expect_err("an emitter the installed revision never planned has nothing to replace");
    assert_eq!(
        format!("{unplanned:#}"),
        failed_build(
            &domain,
            "the installed execution revision has no plan for emitter 'audit'"
        )
    );
}

#[nervix_primitives::test]
async fn an_emitter_swap_keeps_the_task_that_did_not_stop() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_test_domain_execution(
        &runtime,
        &domain,
        emitting_nodes(),
        DomainRoutingSnapshot::default(),
    );
    let emitter = NodeRef::new(ModelKind::Emitter, named::<ModelName>("audit"));
    let (commands, closed) = mpsc::channel(1);
    drop(closed);
    let (stop_signal, _) = watch::channel(None);
    runtime
        .inner
        .executions
        .get_mut(&domain)
        .assured("the test installed the domain's execution")
        .emitter_tasks
        .insert(
            emitter.clone(),
            ScheduledEmitterTask {
                commands,
                stop_signal,
                task: spawn(async {}),
            },
        );

    let error = runtime
        .swap_scheduled_nodes(
            &domain,
            test_execution_revision(&domain, emitting_nodes()),
            &swap_of(emitter.clone()),
        )
        .await
        .expect_err("an emitter whose task does not stop cannot be swapped");
    assert_eq!(
        format!("{error:#}"),
        failed_build(
            &domain,
            "failed to stop emitter 'audit' for its swap: scheduled emitter task is unavailable \
             for stopping"
        )
    );
    let execution = runtime
        .inner
        .executions
        .get(&domain)
        .assured("a failed swap leaves the domain's execution installed");
    assert!(
        execution.emitter_tasks.contains_key(&emitter),
        "the swap keeps the task it could not stop"
    );
}

#[nervix_primitives::test]
async fn a_local_emitter_swap_names_the_input_relay_without_a_boundary() {
    let runtime = Runtime::default();
    attach_loopback_cluster(
        &runtime,
        &ClusterNodeName::parse("node-1").assured("the test node is an identifier-shaped literal"),
    )
    .await;
    let domain = domain("default");
    install_test_domain_execution(
        &runtime,
        &domain,
        emitting_nodes(),
        DomainRoutingSnapshot::default(),
    );
    let emitter = NodeRef::new(ModelKind::Emitter, named::<ModelName>("audit"));

    let error = runtime
        .swap_scheduled_nodes(
            &domain,
            test_execution_revision(&domain, emitting_nodes()),
            &swap_of(emitter),
        )
        .await
        .expect_err("an emitter cannot read a relay this node has no boundary for");
    assert_eq!(
        format!("{error:#}"),
        failed_build(
            &domain,
            "relay 'events' has no runtime boundary on this node"
        )
    );
}

/// A schema of one `tenant` text field, and an unbranched relay of it for each name in `relays`.
fn tenant_schema_and_relays(relays: &[&str]) -> Vec<ScheduledNode> {
    let mut nodes = vec![scheduled_model(nervix_models::Model::Schema(
        CreateSchema {
            name: named("payload"),
            fields: vec![SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        },
    ))];
    for relay in relays {
        nodes.push(scheduled_model(nervix_models::Model::Relay(CreateRelay {
            name: named(relay),
            schema: named("payload"),
            buffer: nonzero!(4usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        })));
    }
    nodes
}

/// The nodes of one reingestor, `repartition`, that copies `incoming` into `outgoing`.
fn reingesting_nodes() -> Vec<ScheduledNode> {
    let mut nodes = tenant_schema_and_relays(&["incoming", "outgoing"]);
    nodes.push(scheduled_model(nervix_models::Model::Reingestor(
        CreateReingestor {
            name: named("repartition"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("outgoing")))
                .with_flush_policy(FlushPolicy::Immediate)
                .with_branch(OutputBranch::Unbranched),
            mode: AckMode::Attached,
            materialized_state: Vec::new(),
            filter_where: None,
        },
    )));
    nodes
}

#[nervix_primitives::test]
async fn a_reingestor_swap_names_the_installed_part_it_cannot_find() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let revision = test_execution_revision(&domain, reingesting_nodes());
    let reingestor = NodeRef::new(ModelKind::Reingestor, named::<ModelName>("repartition"));

    let uninstalled = runtime
        .swap_scheduled_nodes(&domain, revision.clone(), &swap_of(reingestor.clone()))
        .await
        .expect_err("a reingestor swap needs the installed execution");
    assert_eq!(
        format!("{uninstalled:#}"),
        failed_build(
            &domain,
            "the domain execution is not installed while swapping a reingestor"
        )
    );

    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot::default(),
    );
    let unplanned = runtime
        .swap_scheduled_nodes(&domain, revision, &swap_of(reingestor))
        .await
        .expect_err("a reingestor the installed revision never planned has nothing to replace");
    assert_eq!(
        format!("{unplanned:#}"),
        failed_build(
            &domain,
            "the installed execution revision has no plan for reingestor 'repartition'"
        )
    );
}

#[nervix_primitives::test]
async fn a_local_reingestor_swap_names_the_input_relay_without_a_boundary() {
    let runtime = Runtime::default();
    attach_loopback_cluster(
        &runtime,
        &ClusterNodeName::parse("node-1").assured("the test node is an identifier-shaped literal"),
    )
    .await;
    let domain = domain("default");
    install_test_domain_execution(
        &runtime,
        &domain,
        reingesting_nodes(),
        DomainRoutingSnapshot::default(),
    );
    let reingestor = NodeRef::new(ModelKind::Reingestor, named::<ModelName>("repartition"));

    let error = runtime
        .swap_scheduled_nodes(
            &domain,
            test_execution_revision(&domain, reingesting_nodes()),
            &swap_of(reingestor),
        )
        .await
        .expect_err("a reingestor cannot read a relay this node has no boundary for");
    assert_eq!(
        format!("{error:#}"),
        failed_build(
            &domain,
            "relay 'incoming' has no runtime boundary on this node"
        )
    );
}

#[nervix_primitives::test]
async fn a_generator_swap_needs_the_installed_execution() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let mut nodes = tenant_schema_and_relays(&["generated"]);
    nodes.push(scheduled_model(nervix_models::Model::Relay(CreateRelay {
        name: named("notifications"),
        schema: named("payload"),
        buffer: nonzero!(4usize),
        branching: RelayBranching::unbranched(),
        materialized_state: Some(MaterializedRelayState::LastByTimestamp),
    })));
    nodes.push(scheduled_model(nervix_models::Model::Generator(
        CreateGenerator {
            name: named("synth"),
            materialized_relay: named("notifications"),
            branched_by: BranchSelection::unbranched(),
            each: "100ms".parse().assured("the fixture cadence is positive"),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                relay: named("generated"),
                construction: nervix_nspl::parse_route_construction(
                    "SET tenant = relay_state.notifications.tenant",
                )
                .assured("the fixture route is valid NSPL"),
                flush_policy: Some(FlushPolicy::Immediate),
                message_error_policy: MessageErrorPolicy::Log,
                branch: None,
            }]),
        },
    )));
    let generator = NodeRef::new(ModelKind::Generator, named::<ModelName>("synth"));

    let error = runtime
        .swap_scheduled_nodes(
            &domain,
            test_execution_revision(&domain, nodes),
            &swap_of(generator),
        )
        .await
        .expect_err("a generator swap needs the installed execution");
    assert_eq!(
        format!("{error:#}"),
        failed_build(
            &domain,
            "the domain execution is not installed while swapping a generator"
        )
    );
}
