//! A restarted processor resumes every branch and open window its checkpoints hold, or none of them
//! until a later attempt can.

use std::{collections::VecDeque, path::PathBuf, time::Duration};

use ahash::HashMap;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use nervix_models::{
    BranchName, CommandExecutionReference, ErrorPolicies, MessageErrorPolicy, ModelKind, ModelName,
    ParseAsType, RelayName, WasmStateResetReason, WasmStateResetScope,
};
use nervix_primitives::{
    publication::ArcSwap,
    sync::{Arc, StdArc, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

use super::*;
use crate::{
    runtime::{
        processor_branch_task::run_processor_node_runtime,
        window_state::{
            WindowSnapshotLifetime, WindowSnapshotSchemas, decode_window_processor_snapshot,
            encode_window_processor_snapshot,
        },
    },
    runtime_ack::{AckCompletion, AckOutcome, AckProgress, AckSet},
    runtime_schema::{
        RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, RuntimeValue, test_runtime_row,
    },
};

const WAIT: Duration = Duration::from_secs(30);

/// One branch a stopped node left behind: its lifetime and the single row of the window it had
/// open.
struct OpenBranch {
    key: Option<BranchKey>,
    incarnation: u64,
    latency: i64,
}

/// A window processor over `events`, branched by tenant, whose three-message windows count each
/// branch's latencies into `summaries`.
struct OpenWindowFixture {
    processor: ModelName,
    input_relay: RelayName,
    input_schema: Arc<CompiledSchema>,
    plan: WindowAccumulatorPlan,
    template: BranchInstanceTemplate,
}

/// A processor task the fixture started, with the channels that drive it.
struct RunningProcessor {
    commands: mpsc::Sender<ProcessorNodeCommand>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl OpenWindowFixture {
    fn new() -> Self {
        let processor = named::<ModelName>("open_windows");
        let input_relay = named::<RelayName>("events");
        let output_relay = named::<RelayName>("summaries");
        let input_schema = test_schema(&[("latency", ParseAsType::I64)]);
        let output_schema = test_schema(&[("samples", ParseAsType::I64)]);
        let mut relay_schemas = HashMap::default();
        relay_schemas.insert(input_relay.clone(), input_schema.clone());
        relay_schemas.insert(output_relay.clone(), output_schema);
        let aggregate = window_aggregate("SET samples = COUNT(input.latency)");
        let compiled = CompiledWindowAggregateProgram::compile(
            &aggregate,
            std::slice::from_ref(&input_relay),
            &output_relay,
            &relay_schemas,
            None,
        )
        .expect("the window aggregate compiles against its relays");
        let plan = WindowAccumulatorPlan::new([&compiled.route], None, None);
        let window = RelayProcessorTemplate {
            kind: ModelKind::WindowProcessor,
            processor: processor.clone(),
            input_relays: vec![input_relay.clone()],
            input_collect_policies: HashMap::default(),
            error_policies: ErrorPolicies::handled_by_log(),
            from_where: HashMap::default(),
            compiled_from_where: HashMap::default(),
            filter_where: None,
            compiled_filter_where: HashMap::default(),
            materialized_state: Vec::new(),
            operation: RelayProcessorOperationTemplate::WindowProcessor {
                output_routes: RelayProcessorOutputsTemplate {
                    routes: vec![RelayProcessorOutputTemplate {
                        output_relay: output_relay.clone(),
                        construction: nervix_models::RouteConstruction::default(),
                        flush_policy: None,
                        message_error_policy: MessageErrorPolicy::Log,
                        compiled_program: None,
                    }],
                },
                input_schema: input_schema.clone(),
                width_messages: Some(3),
                step_messages: Some(3),
                width_duration: None,
                step_duration: None,
                aggregate,
                plan: plan.clone(),
                compiled_aggregates: vec![compiled],
            },
        };
        let template = BranchInstanceTemplate {
            revision: ProcessorPlanRevision::new(),
            source_kind: ModelKind::WindowProcessor,
            source: RelayName::from(&processor),
            root_relay: input_relay.clone(),
            branch: Some(named::<BranchName>("by_tenant")),
            branch_ttl: None,
            branch_max_instances: None,
            error_policies: ErrorPolicies::handled_by_log(),
            relays: [(
                output_relay,
                RelayProcessorRelayTemplate {
                    services: test_relay_boundary_services(),
                },
            )]
            .into_iter()
            .collect(),
            processors: [(processor.clone(), window)].into_iter().collect(),
            wasm_state_reset: None,
        };
        Self {
            processor,
            input_relay,
            input_schema,
            plan,
            template,
        }
    }

    /// A runtime over a fresh state database whose admitted work goes through `executor`, with
    /// the fixture's domain installed and its processor's state identity published.
    fn runtime(&self, executor: Executor, directory: &tempfile::TempDir) -> (Runtime, DomainName) {
        let database = fjall::Database::builder(directory.path())
            .open()
            .expect("the state database opens");
        let runtime = Runtime::with_persistence_and_temp_dir(
            executor,
            None,
            Some(database),
            Duration::from_secs(60),
            ConfiguredFaultInjection::default(),
            PathBuf::from(DEFAULT_TEMP_DIR),
            DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .expect("the runtime opens its state store");
        let domain = domain("restarted");
        install_unpaced_test_domain(&runtime, &domain);
        publish_state_identity(
            &runtime,
            &domain,
            ModelKind::WindowProcessor,
            self.processor.clone(),
        );
        (runtime, domain)
    }

    fn window_placement(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        branch: &Option<BranchKey>,
    ) -> RuntimeStatePlacement {
        runtime
            .state_placement(
                domain,
                RuntimeStateKind::WindowProcessor,
                ModelKind::WindowProcessor,
                self.processor.clone(),
                branch.clone(),
            )
            .expect("the window processor has a published state identity")
    }

    /// Store what a node writes before it stops: the processor's branch lifecycle at revision
    /// `lsm`, naming every branch with its lifetime, and the window each branch has open.
    async fn store_checkpoints(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        lsm: u64,
        branches: &[OpenBranch],
    ) {
        let store = runtime
            .inner
            .state_store
            .clone()
            .expect("the runtime has a state store");
        let mut lifetimes = Vec::with_capacity(branches.len());
        for branch in branches {
            lifetimes.push(BranchInstanceSnapshotEntry {
                key: branch.key.clone(),
                last_ingestion: Timestamp::from_unix_nanos(1_000),
                incarnation: branch.incarnation,
            });
            let window = self.open_window(runtime.executor(), branch).await;
            store
                .persist_latest_snapshot(
                    &self.window_placement(runtime, domain, &branch.key),
                    1,
                    &window,
                )
                .expect("the open window checkpoint is stored");
        }
        let lifecycle =
            encode_branch_lru_snapshot(&lifetimes).expect("a current branch lifecycle encodes");
        let placement = branch_lru_placement(runtime, domain, &self.template)
            .expect("the window processor's lifecycle is placed");
        store
            .persist_latest_snapshot(&placement, lsm, &lifecycle)
            .expect("the branch lifecycle checkpoint is stored");
    }

    /// The checkpoint of the window `branch` has open: one row, opened by its lifetime.
    async fn open_window(&self, executor: &Executor, branch: &OpenBranch) -> Vec<u8> {
        let at = Timestamp::from_unix_nanos(1_000);
        let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(at, at);
        let record = test_runtime_row([("latency".to_string(), RuntimeValue::I64(branch.latency))])
            .with_metadata(metadata.clone());
        let argument_schema = WindowArgumentColumns::snapshot_schema(&self.plan);
        let argument: ArrayRef = StdArc::new(Int64Array::from(vec![Some(branch.latency)]));
        let arguments = RecordBatch::try_new(argument_schema.clone(), vec![argument])
            .expect("the argument column matches its plan");
        let arguments = RuntimeRecordBatch::from_record_batch(argument_schema, arguments)
            .expect("the argument batch has its declared schema");
        let arguments = RuntimeRow::new(Arc::new(arguments), 0, metadata)
            .expect("the argument batch has one row");
        let window = WindowProcessorStateSnapshot {
            entries: vec![WindowEntrySnapshot {
                sequence: 0,
                timestamp: at,
                key: branch.key.clone(),
                record,
                arguments,
            }],
            next_sequence: 1,
            incarnation: Some(branch.incarnation),
            accumulators: vec![WindowAccumulatorSnapshot::Retained],
        };
        encode_window_processor_snapshot(&window, 1, executor)
            .await
            .expect("a current window checkpoint seals")
    }

    /// The latencies of the rows the window `branch` last published while its lifetime is
    /// `incarnation`, or nothing when that lifetime no longer owns the window.
    async fn published_latencies(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        branch: &Option<BranchKey>,
        incarnation: u64,
    ) -> Option<Vec<i64>> {
        let state = runtime
            .replicated_window_processor_state(self.window_placement(runtime, domain, branch))
            .expect("the window state is placed");
        let published = state
            .latest_snapshot(runtime.executor())
            .await
            .expect("the published window encodes");
        let input = self.input_schema.arrow_schema();
        let arguments = WindowArgumentColumns::snapshot_schema(&self.plan);
        let window = decode_window_processor_snapshot(
            &published.payload,
            runtime.executor(),
            WindowSnapshotSchemas {
                input: &input,
                arguments: &arguments,
            },
            WindowSnapshotLifetime::Branch(incarnation),
        )
        .await
        .expect("the published window decodes")?;
        let mut latencies = Vec::with_capacity(window.entries.len());
        for entry in &window.entries {
            let latency = entry
                .record
                .value("latency")
                .expect("the retained row has its latency column");
            let Some(RuntimeValue::I64(latency)) = latency else {
                panic!("a retained latency is an I64, found {latency:?}");
            };
            latencies.push(latency);
        }
        Some(latencies)
    }

    fn input_batch(
        &self,
        branch: &Option<BranchKey>,
        latency: i64,
        acks: AckSet,
    ) -> RelayRecordBatch {
        RelayRecordBatch::single(
            self.input_schema.clone(),
            branch.clone(),
            test_runtime_row([("latency".to_string(), RuntimeValue::I64(latency))]),
            acks,
        )
        .expect("the input batch builds")
    }

    /// Start the processor's task over `input` on `runtime`, resuming `handoffs` when it has any.
    fn start(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        input: &RelayBroadcast<RelayRecordBatch>,
        handoffs: Vec<ProcessorBranchHandoff>,
    ) -> RunningProcessor {
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (commands, command_rx) = mpsc::channel(1);
        let task = nervix_primitives::task::spawn(run_processor_node_runtime(
            ProcessorRuntimeContext::new(runtime.clone(), domain.clone()),
            self.template.clone(),
            vec![(
                self.input_relay.clone(),
                RelayRuntimeFanIn::new(input.new_receiver()),
            )],
            shutdown_rx,
            command_rx,
            handoffs,
            Duration::from_secs(60),
        ));
        RunningProcessor {
            commands,
            shutdown,
            task,
        }
    }
}

impl RunningProcessor {
    async fn checkpoint(&self) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
        timeout(WAIT, ScheduledNodeTask::checkpoint_via(&self.commands))
            .await
            .expect("the processor answers its checkpoint")
    }

    /// The lifecycle the processor checkpoints once its restore has installed its branches.
    async fn restored_lifecycle(&self) -> PersistedRuntimeStateEntry {
        timeout(WAIT, async {
            loop {
                match ScheduledNodeTask::checkpoint_via(&self.commands).await {
                    Ok(lifecycle) => return lifecycle,
                    Err(error) => {
                        assert!(
                            matches!(
                                error.current_context(),
                                OwnershipHandoffError::BranchesUnrestored { .. }
                            ),
                            "only a pending restore refuses a checkpoint, found {error:?}"
                        );
                        sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        })
        .await
        .expect("a later attempt restores the processor's branches")
    }

    async fn reset_command(
        &self,
        command: impl FnOnce(
            oneshot::Sender<error_stack::Result<(), WasmStateResetRuntimeError>>,
        ) -> ProcessorNodeCommand,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        let (response, answer) = oneshot::channel();
        self.commands
            .send(command(response))
            .await
            .expect("the processor takes its command");
        timeout(WAIT, answer)
            .await
            .expect("the processor answers its command")
            .expect("the processor keeps its response")
    }

    async fn handoff(self) -> Vec<ProcessorBranchHandoff> {
        let (response, handoffs) = oneshot::channel();
        self.commands
            .send(ProcessorNodeCommand::Handoff { response })
            .await
            .expect("the processor takes its handoff");
        let handoffs = timeout(WAIT, handoffs)
            .await
            .expect("the processor hands its branches over")
            .expect("the processor keeps its response");
        timeout(WAIT, self.task)
            .await
            .expect("the processor task stops")
            .expect("the processor task joins");
        drop(self.shutdown);
        handoffs
    }

    async fn stop(self) {
        self.shutdown.send_replace(true);
        timeout(WAIT, self.task)
            .await
            .expect("the processor task stops")
            .expect("the processor task joins");
    }
}

fn branch_lifetimes(lifecycle: &PersistedRuntimeStateEntry) -> Vec<(Option<BranchKey>, u64)> {
    decode_branch_lru_snapshot(&lifecycle.payload)
        .expect("the processor's lifecycle decodes")
        .into_iter()
        .map(|entry| (entry.key, entry.incarnation))
        .collect()
}

async fn ack_outcome(mut completion: AckCompletion) -> AckOutcome {
    timeout(WAIT, async {
        loop {
            if let AckProgress::Complete(outcome) = completion.wait_for_progress().await {
                return outcome;
            }
        }
    })
    .await
    .expect("the input is settled")
}

/// On a node that has just started, a window processor task restores its branches before the
/// domain's routing is published. Every branch its lifecycle checkpoint names keeps its lifetime,
/// so each keeps its open window, and later input continues those windows.
#[nervix_primitives::test]
async fn a_restarted_window_processor_resumes_every_open_window_before_its_domain_is_routed() {
    let fixture = OpenWindowFixture::new();
    let directory = tempfile::tempdir().expect("the state directory opens");
    let (runtime, domain) = fixture.runtime(Executor::default(), &directory);
    let alpha = string_branch_key("tenant", "alpha");
    let beta = string_branch_key("tenant", "beta");
    fixture
        .store_checkpoints(
            &runtime,
            &domain,
            5,
            &[
                OpenBranch {
                    key: alpha.clone(),
                    incarnation: 3,
                    latency: 10,
                },
                OpenBranch {
                    key: beta.clone(),
                    incarnation: 4,
                    latency: 100,
                },
            ],
        )
        .await;
    // A node that has just started has not published its domain's routing yet.
    runtime.inner.domain_routings.remove(&domain);
    let input = RelayBroadcast::with_capacity(nonzero_capacity(4));
    let processor = fixture.start(&runtime, &domain, &input, Vec::new());

    let restored = processor
        .checkpoint()
        .await
        .expect("the processor checkpoints its branches");
    assert_eq!(
        (restored.lsm, branch_lifetimes(&restored)),
        (5, vec![(alpha.clone(), 3), (beta.clone(), 4)]),
        "the restarted processor must resume the branch lifecycle its checkpoint holds"
    );
    assert_eq!(
        fixture
            .published_latencies(&runtime, &domain, &alpha, 3)
            .await,
        Some(vec![10]),
        "branch alpha must keep the window it had open"
    );
    assert_eq!(
        fixture
            .published_latencies(&runtime, &domain, &beta, 4)
            .await,
        Some(vec![100]),
        "branch beta must keep the window it had open"
    );

    let mut relay_schemas = HashMap::default();
    relay_schemas.insert(fixture.input_relay.clone(), fixture.input_schema.clone());
    runtime.inner.domain_routings.insert(
        domain.clone(),
        StdArc::new(ArcSwap::from_pointee(DomainRoutingSnapshot {
            relay_schemas,
            ..DomainRoutingSnapshot::default()
        })),
    );
    input
        .broadcast(fixture.input_batch(&alpha, 20, AckSet::empty()))
        .await
        .expect("the input batch is queued");
    let continued = processor
        .checkpoint()
        .await
        .expect("the processor checkpoints its branches");
    assert_eq!(
        branch_lifetimes(&continued),
        vec![(beta.clone(), 4), (alpha.clone(), 3)],
        "input for a restored branch continues its lifetime"
    );
    assert_eq!(
        fixture
            .published_latencies(&runtime, &domain, &alpha, 3)
            .await,
        Some(vec![10, 20]),
        "input for a restored branch continues the window it had open"
    );
    processor.stop().await;
}

/// A restore the node cannot complete yet, here because decoding the open windows finds the bulk
/// CPU class full, installs no branch and stays pending. The processor takes no new input,
/// refuses the input a drain hands it, and refuses to checkpoint or reset branches it does not
/// hold. A later attempt installs every branch with the window it had open.
#[nervix_primitives::test]
async fn a_failed_restore_holds_the_processor_until_a_later_attempt_installs_every_branch() {
    let fixture = OpenWindowFixture::new();
    let directory = tempfile::tempdir().expect("the state directory opens");
    let executor = single_worker_executor();
    let (runtime, domain) = fixture.runtime(executor.clone(), &directory);
    let alpha = string_branch_key("tenant", "alpha");
    let beta = string_branch_key("tenant", "beta");
    fixture
        .store_checkpoints(
            &runtime,
            &domain,
            5,
            &[
                OpenBranch {
                    key: alpha.clone(),
                    incarnation: 3,
                    latency: 10,
                },
                OpenBranch {
                    key: beta.clone(),
                    incarnation: 4,
                    latency: 100,
                },
            ],
        )
        .await;
    let filled = FilledCpuClass::fill(&executor, nervix_execution::CpuClass::Bulk).await;
    let input = RelayBroadcast::with_capacity(nonzero_capacity(4));
    let processor = fixture.start(&runtime, &domain, &input, Vec::new());
    // The pending restore leaves this input in the relay until the checkpoint's drain delivers it.
    let (acks, completion) = AckSet::root();
    input
        .broadcast(fixture.input_batch(&alpha, 20, acks))
        .await
        .expect("the input batch is queued");

    let refused = processor.checkpoint().await.expect_err(
        "a processor that has not restored its branches has no lifecycle to checkpoint",
    );
    assert!(
        matches!(
            refused.current_context(),
            OwnershipHandoffError::BranchesUnrestored { kind, identifier }
                if *kind == ModelKind::WindowProcessor && identifier == &fixture.processor
        ),
        "the checkpoint names the unrestored processor, found {refused:?}"
    );
    assert!(
        matches!(ack_outcome(completion).await, AckOutcome::NoAck(_)),
        "input a drain hands the unrestored processor is refused rather than starting a branch"
    );
    let request =
        CommandExecutionReference::parse("reset-open-windows").expect("the reference is valid");
    let prepare = processor
        .reset_command(|response| ProcessorNodeCommand::PrepareWasmStateReset {
            preparation: WasmStateResetPreparation {
                request: request.clone(),
                scope: WasmStateResetScope::AllBranches,
                branch_key: None,
                published: false,
                reason: WasmStateResetReason::Operator,
            },
            response,
        })
        .await;
    let apply = processor
        .reset_command(|response| ProcessorNodeCommand::ApplyWasmStateReset {
            reset: nervix_models::WasmStateReset::publishing(
                request.clone(),
                WasmStateResetScope::AllBranches,
                WasmStateResetReason::Operator,
            ),
            response,
        })
        .await;
    for refusal in [prepare, apply] {
        let refusal = refusal.expect_err("an unrestored processor cannot reset branch state");
        assert!(
            matches!(
                refusal.current_context(),
                WasmStateResetRuntimeError::BranchesUnrestored { processor }
                    if processor == &fixture.processor
            ),
            "the reset names the unrestored processor, found {refusal:?}"
        );
    }

    filled.release().await;
    let restored = processor.restored_lifecycle().await;
    assert_eq!(
        (restored.lsm, branch_lifetimes(&restored)),
        (5, vec![(alpha.clone(), 3), (beta.clone(), 4)]),
        "the later attempt resumes every branch under its lifetime"
    );
    assert_eq!(
        fixture
            .published_latencies(&runtime, &domain, &alpha, 3)
            .await,
        Some(vec![10]),
        "branch alpha keeps the window it had open, without the input it refused"
    );
    assert_eq!(
        fixture
            .published_latencies(&runtime, &domain, &beta, 4)
            .await,
        Some(vec![100]),
        "branch beta keeps the window it had open"
    );
    processor.stop().await;
}

/// A processor whose restore is pending when its task is replaced hands the branches it has not
/// restored to its successor: the handed-over ones with the input they still hold, and nothing in
/// place of a lifecycle it never read, which its successor restores in its place.
#[nervix_primitives::test]
async fn a_replaced_processor_hands_over_the_branches_it_has_not_restored() {
    let fixture = OpenWindowFixture::new();
    let directory = tempfile::tempdir().expect("the state directory opens");
    let executor = single_worker_executor();
    let (runtime, domain) = fixture.runtime(executor.clone(), &directory);
    let alpha = string_branch_key("tenant", "alpha");
    let beta = string_branch_key("tenant", "beta");
    fixture
        .store_checkpoints(
            &runtime,
            &domain,
            5,
            &[
                OpenBranch {
                    key: alpha.clone(),
                    incarnation: 3,
                    latency: 10,
                },
                OpenBranch {
                    key: beta.clone(),
                    incarnation: 4,
                    latency: 100,
                },
            ],
        )
        .await;
    let filled = FilledCpuClass::fill(&executor, nervix_execution::CpuClass::Bulk).await;
    let input = RelayBroadcast::with_capacity(nonzero_capacity(4));

    let restoring = fixture.start(&runtime, &domain, &input, Vec::new());
    assert!(
        restoring.handoff().await.is_empty(),
        "a processor that never read its lifecycle hands over no branch"
    );

    let handed_over = vec![
        ProcessorBranchHandoff {
            key: alpha.clone(),
            restored_at: Timestamp::from_unix_nanos(2_000),
            incarnation: 3,
            pending_materialized: VecDeque::new(),
        },
        ProcessorBranchHandoff {
            key: beta.clone(),
            restored_at: Timestamp::from_unix_nanos(2_000),
            incarnation: 4,
            pending_materialized: VecDeque::new(),
        },
    ];
    let resuming = fixture.start(&runtime, &domain, &input, handed_over);
    let passed_on = resuming.handoff().await;
    let mut passed_on_lifetimes = Vec::with_capacity(passed_on.len());
    for handoff in &passed_on {
        passed_on_lifetimes.push((handoff.key.clone(), handoff.incarnation));
    }
    assert_eq!(
        passed_on_lifetimes,
        vec![(alpha.clone(), 3), (beta.clone(), 4)],
        "a processor passes the branches handed to it on unrestored"
    );

    filled.release().await;
    let successor = fixture.start(&runtime, &domain, &input, passed_on);
    let restored = successor.restored_lifecycle().await;
    assert_eq!(
        (restored.lsm, branch_lifetimes(&restored)),
        (5, vec![(alpha.clone(), 3), (beta.clone(), 4)]),
        "the successor resumes every handed-over branch under its lifetime"
    );
    assert_eq!(
        fixture
            .published_latencies(&runtime, &domain, &alpha, 3)
            .await,
        Some(vec![10]),
        "the successor keeps the window branch alpha had open"
    );
    successor.stop().await;
}

/// Handed-over branches resume only under the lifecycle they belong to. Without that lifecycle,
/// or when a branch began after the lifecycle revision this node holds, the restore installs
/// nothing and keeps every handed-over branch for a later attempt.
#[nervix_primitives::test]
async fn handed_over_branches_resume_only_under_the_lifecycle_they_belong_to() {
    let fixture = OpenWindowFixture::new();
    let directory = tempfile::tempdir().expect("the state directory opens");
    let (runtime, domain) = fixture.runtime(Executor::default(), &directory);
    let alpha = string_branch_key("tenant", "alpha");
    let context = ProcessorRuntimeContext::new(runtime.clone(), domain.clone());
    let mut instances = BranchInstanceRegistry::<Option<BranchKey>, ProcessorBranchTask>::new();
    let mut last_persisted_lru_lsm = 0;
    let handoff = || ProcessorBranchHandoff {
        key: alpha.clone(),
        restored_at: Timestamp::from_unix_nanos(2_000),
        incarnation: 6,
        pending_materialized: VecDeque::new(),
    };

    let mut without_lifecycle = PendingProcessorBranchRestore::new(vec![handoff()]);
    let unavailable = without_lifecycle
        .attempt(
            &context,
            &fixture.template,
            &mut instances,
            &mut last_persisted_lru_lsm,
        )
        .await
        .expect_err("handed-over branches need the lifecycle they belong to");
    assert!(matches!(
        unavailable.current_context(),
        ProcessorBranchTaskError::HandedOffLifecycleUnavailable
    ));
    assert!(
        without_lifecycle.retry_at() > Instant::now(),
        "a failed attempt waits for its backoff"
    );
    assert_eq!(without_lifecycle.into_handoffs().len(), 1);

    fixture
        .store_checkpoints(
            &runtime,
            &domain,
            5,
            &[OpenBranch {
                key: alpha.clone(),
                incarnation: 3,
                latency: 10,
            }],
        )
        .await;
    let mut after_lifecycle = PendingProcessorBranchRestore::new(vec![handoff()]);
    let after = after_lifecycle
        .attempt(
            &context,
            &fixture.template,
            &mut instances,
            &mut last_persisted_lru_lsm,
        )
        .await
        .expect_err("a branch cannot resume a lifetime its lifecycle has not reached");
    assert!(matches!(
        after.current_context(),
        ProcessorBranchTaskError::HandedOffLifetimeAfterLifecycle {
            branch,
            incarnation: 6,
            lsm: 5,
        } if branch == &alpha
    ));
    let kept = after_lifecycle.into_handoffs();
    assert_eq!(
        kept.len(),
        1,
        "a failed attempt keeps every handed-over branch"
    );
    assert_eq!(kept[0].incarnation, 6);
    assert!(
        instances.states().is_empty() && instances.version() == 0 && last_persisted_lru_lsm == 0,
        "a failed attempt installs nothing"
    );
}
