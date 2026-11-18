//! Emitter task lifecycle evidence.
//!
//! Layer: harness.
//! - **Owns.** Existing task, buffering, retry and terminal finish regression tests.
//! - **Depends on.** The production emitter task and shared runtime fixtures.
//! - **Must not know.** External connector drivers or independently reconstructed task policy.

use nervix_connector::{EmitterFinishResult, EmitterLifecycle};

use super::*;
use crate::runtime::test_fixtures::{emitter_context, input_batch};

fn task_state(context: &EmitterConnectorContext) -> EmitterTaskState {
    let mut buffer = EmitterBatchBuffer::default();
    buffer.set_flush_policy(RuntimeFlushPolicy::Immediate {
        minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum(),
    });
    EmitterTaskState::new(
        EmitterConnectorState::Unavailable {
            retry_open: false,
            reason: "test emitter is unavailable".to_string(),
        },
        buffer,
        RuntimeReconnectBackoff::default(),
        context,
    )
}

fn output_metrics(context: &EmitterConnectorContext) -> EmitterOutputMetrics {
    EmitterOutputMetrics::WithoutRelay(
        context
            .runtime
            .inner
            .metrics
            .resolve_global_node_message_metrics(
                &context.domain,
                ModelKind::Emitter,
                &ModelName::from(&context.emitter),
                None,
                "sent",
            ),
    )
}

fn batch_context<'a>(
    context: &'a EmitterConnectorContext,
    routing: &'a mut DomainRoutingCache,
    output_metrics: &'a EmitterOutputMetrics,
    node: &'a ModelName,
    source_filters: &'a HashMap<RelayName, CompiledProgramWithMaterializedInterest>,
    materialized_state: &'a [nervix_models::MaterializedStateDependency],
) -> EmitterBatchContext<'a> {
    EmitterBatchContext {
        error_routing: &context.routing,
        metrics_dirty: &context.metrics_dirty,
        runtime: &context.runtime,
        routing,
        domain_clock: &context.clock,
        domain: &context.domain,
        emitter: &context.emitter,
        node,
        output_metrics,
        interactions: &context.interactions,
        error_policies: &context.error_policies,
        source_filters,
        filter_map: None,
        ordering_group: None,
        envelope: None,
        http_requests: None,
        materialized_state,
    }
}

#[test]
fn emitter_context_reports_missing_node_dns_as_initialization_failure() {
    let context = emitter_context();
    let error = context
        .dns()
        .err()
        .assured("the fixture runtime has no DNS resolver");
    assert!(matches!(
        error.current_context(),
        EmitterRuntimeError::InitializeConnector
    ));
    assert_eq!(
        emitter_error_message(&error),
        "the node DNS resolver is not installed"
    );
}

#[test]
fn emitter_error_classification_is_explicit_for_every_context() {
    for retryable in [
        EmitterRuntimeError::ConnectorNotInitialized,
        EmitterRuntimeError::ResumeConnector,
        EmitterRuntimeError::ConnectorWaiting,
        EmitterRuntimeError::PublishBatch,
        EmitterRuntimeError::PublishStalled,
        EmitterRuntimeError::UnansweredEmitterRecords {
            unanswered: 1,
            records: 2,
        },
    ] {
        assert!(retryable.is_retryable_publish_failure());
        assert!(emitter_publish_error_is_retryable(&Report::new(retryable)));
    }
    for terminal in [
        EmitterRuntimeError::InvalidConnectorConfig,
        EmitterRuntimeError::InitializeConnector,
        EmitterRuntimeError::CloseConnector,
        EmitterRuntimeError::FlushPolicyNotInitialized,
        EmitterRuntimeError::FaultInjected,
        EmitterRuntimeError::EncodeBatch,
        EmitterRuntimeError::PreparedRowOutOfBounds {
            row: 1,
            row_count: 1,
        },
        EmitterRuntimeError::RowAlreadyPrepared { row: 0 },
        EmitterRuntimeError::UnknownEmitterRecord {
            record: 2,
            records: 2,
        },
        EmitterRuntimeError::EmitterRecordAnsweredTwice { record: 0 },
    ] {
        assert!(!terminal.is_retryable_publish_failure());
        assert!(!emitter_publish_error_is_retryable(&Report::new(terminal)));
    }

    assert_eq!(
        emitter_message_error_operation(&Report::new(EmitterRuntimeError::EncodeBatch), true),
        MessageErrorOperation::Encode
    );
    assert_eq!(
        emitter_message_error_operation(&Report::new(EmitterRuntimeError::EncodeBatch), false),
        MessageErrorOperation::Values
    );
    assert_eq!(
        emitter_message_error_operation(&Report::new(EmitterRuntimeError::PublishBatch), true),
        MessageErrorOperation::Publish
    );
}

#[test]
fn emitter_error_message_prefers_printable_context() {
    let attached =
        Report::new(EmitterRuntimeError::PublishBatch).attach_printable("specific broker failure");
    assert_eq!(emitter_error_message(&attached), "specific broker failure");

    let bare = Report::new(EmitterRuntimeError::EncodeBatch);
    assert_eq!(
        emitter_error_message(&bare),
        "failed to encode emitter batch"
    );
}

#[nervix_primitives::test]
async fn publish_success_clears_retry_state_and_releases_the_caller_batch() {
    let context = emitter_context();
    let routing = DomainRouting::new(DomainRoutingSnapshot::default());
    let mut routing = DomainRoutingCache::new(routing.shared());
    let output_metrics = output_metrics(&context);
    let node = ModelName::from(&context.emitter);
    let source_filters = HashMap::default();
    let materialized_state = Vec::new();
    let batch_context = batch_context(
        &context,
        &mut routing,
        &output_metrics,
        &node,
        &source_filters,
        &materialized_state,
    );
    let mut state = task_state(&context);
    let mut pending_batch = Some(EmitterPublishBatch::from_batch(
        input_batch(),
        Timestamp::from_unix_nanos(100),
    ));

    state
        .handle_publish_result(
            Ok(None),
            &mut pending_batch,
            &context,
            &batch_context,
            EmitterPublishOutcomeContext {
                emitter_label: "test",
                codec_route: true,
                error_report: EmitterPublishErrorReport::Publish,
            },
        )
        .await;

    assert!(pending_batch.is_none());
    assert!(!state.retry.is_active());
    assert_eq!(
        context
            .runtime
            .dataflow_node_transient_state(&context.domain, "EMITTER", &context.emitter)
            .error,
        None
    );
    let mut fresh_backoff = RuntimeReconnectBackoff::default();
    assert_eq!(
        state.backoff.take_next_delay(),
        fresh_backoff.take_next_delay(),
        "success must reset the declared retry sequence"
    );
}

#[nervix_primitives::test]
async fn retryable_caller_failure_retains_the_batch_and_defers_one_retry() {
    let context = emitter_context();
    let routing = DomainRouting::new(DomainRoutingSnapshot::default());
    let mut routing = DomainRoutingCache::new(routing.shared());
    let output_metrics = output_metrics(&context);
    let node = ModelName::from(&context.emitter);
    let source_filters = HashMap::default();
    let materialized_state = Vec::new();
    let batch_context = batch_context(
        &context,
        &mut routing,
        &output_metrics,
        &node,
        &source_filters,
        &materialized_state,
    );
    let mut state = task_state(&context);
    state.retry.clear();
    let mut pending_batch = Some(EmitterPublishBatch::from_batch(
        input_batch(),
        Timestamp::from_unix_nanos(100),
    ));
    let failure = EmitterPublishFailure::caller(
        Report::new(EmitterRuntimeError::PublishBatch)
            .attach_printable("test emitter rejected the attempt"),
    );

    state
        .handle_publish_result(
            Err(failure),
            &mut pending_batch,
            &context,
            &batch_context,
            EmitterPublishOutcomeContext {
                emitter_label: "test",
                codec_route: true,
                error_report: EmitterPublishErrorReport::Publish,
            },
        )
        .await;

    assert!(pending_batch.is_none());
    assert_eq!(state.buffer.pending().len(), 1);
    assert!(state.retry.is_active());
    assert!(state.reconnect_on_wake);
    assert_eq!(
        context
            .runtime
            .dataflow_node_transient_state(&context.domain, "EMITTER", &context.emitter)
            .error,
        Some("test emitter rejected the attempt".to_string())
    );
}

#[nervix_primitives::test]
async fn terminal_buffer_failure_drains_and_routes_every_owned_batch() {
    let context = emitter_context();
    let routing = DomainRouting::new(DomainRoutingSnapshot::default());
    let mut routing = DomainRoutingCache::new(routing.shared());
    let output_metrics = output_metrics(&context);
    let node = ModelName::from(&context.emitter);
    let source_filters = HashMap::default();
    let materialized_state = Vec::new();
    let batch_context = batch_context(
        &context,
        &mut routing,
        &output_metrics,
        &node,
        &source_filters,
        &materialized_state,
    );
    let mut state = task_state(&context);
    state
        .buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .expect("test batch must buffer");
    let failure = EmitterPublishFailure::buffer(
        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable("test encoding failed"),
    );
    let mut pending_batch = None;

    state
        .handle_publish_result(
            Err(failure),
            &mut pending_batch,
            &context,
            &batch_context,
            EmitterPublishOutcomeContext {
                emitter_label: "test",
                codec_route: true,
                error_report: EmitterPublishErrorReport::Flush,
            },
        )
        .await;

    assert!(state.buffer.is_empty());
    assert!(!state.retry.is_active());
    assert_eq!(
        context
            .runtime
            .dataflow_node_transient_state(&context.domain, "EMITTER", &context.emitter)
            .error,
        Some("test encoding failed".to_string())
    );
}

#[nervix_primitives::test]
async fn a_terminal_failure_counts_the_rows_delivered_before_it_as_sent() {
    let context = emitter_context();
    let routing = DomainRouting::new(DomainRoutingSnapshot::default());
    let mut routing = DomainRoutingCache::new(routing.shared());
    let output_metrics = output_metrics(&context);
    let node = ModelName::from(&context.emitter);
    let source_filters = HashMap::default();
    let materialized_state = Vec::new();
    let batch_context = batch_context(
        &context,
        &mut routing,
        &output_metrics,
        &node,
        &source_filters,
        &materialized_state,
    );
    let mut state = task_state(&context);
    let mut messages = Vec::with_capacity(2);
    for value in [1, 2] {
        messages.push(RelayMessage {
            key: None,
            record: test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
            acks: AckSet::empty(),
        });
    }
    let two_rows =
        RelayRecordBatch::from_messages(crate::runtime::test_fixtures::input_schema(), messages)
            .expect("the test records match the emitter input schema");
    let mut batch = EmitterPublishBatch::from_batch(two_rows, Timestamp::from_unix_nanos(100));
    batch
        .mark_delivered(0, DeliveredAcknowledgements::Host)
        .expect("an earlier attempt delivered the first row");
    state
        .buffer
        .push(&context, batch)
        .expect("test batch must buffer");
    let failure = EmitterPublishFailure::buffer(
        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable("test encoding failed"),
    );
    let mut pending_batch = None;

    state
        .handle_publish_result(
            Err(failure),
            &mut pending_batch,
            &context,
            &batch_context,
            EmitterPublishOutcomeContext {
                emitter_label: "test",
                codec_route: true,
                error_report: EmitterPublishErrorReport::Flush,
            },
        )
        .await;

    let sent = context.runtime.inner.metrics.dataflow_edge_statistics(
        &context.domain,
        &nervix_dataflow_graph::DataflowMetricRef::new(
            "EMITTER",
            context.emitter.as_str(),
            "sent",
            None::<String>,
        ),
    );
    assert_eq!(
        sent.messages_total, 1,
        "the delivered row is sent, and the row the failure routes is not"
    );
    assert!(state.buffer.is_empty());
}

#[nervix_primitives::test]
async fn emitter_context_reports_configuration_and_publish_failures() {
    let context = emitter_context();
    let mut events = context.runtime.events().subscribe();

    context.report_init_error("nats", "init failed");
    context.report_publish_error("nats", "publish failed");
    context.report_flush_error("nats", "flush failed");
    assert!(
        context
            .parse_flush_policy(&FlushPolicy::Each {
                interval: "not-a-duration".to_string(),
                max_batch_size: "1MiB".to_string()
            })
            .is_none()
    );

    let mut messages = Vec::with_capacity(4);
    for _ in 0..4 {
        nervix_primitives::task::consume_budget().await;
        let RuntimeEvent::Error(message) =
            events.recv().await.expect("error event must be emitted");
        messages.push(message);
    }
    assert!(messages[0].contains("failed to initialize nats emitter"));
    assert!(messages[1].contains("failed to publish nats message"));
    assert!(messages[2].contains("failed to flush nats rows"));
    assert!(messages[3].contains(
        "emitter 'output' in domain 'emitter_tests' has an invalid flush policy: invalid \
         flush_each 'not-a-duration'"
    ));
}

#[nervix_primitives::test]
async fn connector_status_progress_keeps_publish_and_commit_failures() {
    use meticulous::OptionExt as _;

    for kind in [EmitterRetryKind::Infrastructure, EmitterRetryKind::Commit] {
        let context = emitter_context();
        let host = context.emitter_host();
        let reason = "the broker did not answer within ACK TIMEOUT 10s";
        let backoff = Duration::from_secs(1);
        context.record_retry(reason, backoff, kind);

        host.clear_transient_error();

        let failure = context
            .status
            .snapshot()
            .assured("connection progress preserves the task's unresolved failure");
        assert_eq!(failure.error, reason);
        let retry = failure
            .retry
            .as_ref()
            .assured("record_retry supplied the pending retry");
        assert_eq!(retry.kind, kind);
        assert_eq!(retry.reconnect.backoff, backoff);

        context.status.clear();
        assert!(context.status.snapshot().is_none());
    }
}

#[nervix_primitives::test]
async fn connector_and_task_failures_are_both_described() {
    let context = emitter_context();
    let host = context.emitter_host();
    let task_reason = "mqtt client refused the record: Request channel is full";
    let connector_reason = "resolving MQTT host 'mqtt.nervix.test' failed: the name does not exist";
    let backoff = Duration::from_secs(1);

    context.record_retry(task_reason, backoff, EmitterRetryKind::Infrastructure);
    host.record_transient_error(connector_reason.to_string(), Duration::from_millis(25));

    let observed = transient_error(&context).assured("both failures remain published");
    assert!(observed.contains(task_reason), "{observed}");
    assert!(observed.contains(connector_reason), "{observed}");

    let selected = context
        .status
        .snapshot()
        .assured("the task retry is active");
    let retry = selected.retry.as_ref().assured("the task selected a retry");
    assert_eq!(retry.kind, EmitterRetryKind::Infrastructure);
    assert_eq!(retry.reconnect.backoff, backoff);

    host.clear_transient_error();
    assert_eq!(transient_error(&context).as_deref(), Some(task_reason));
    host.record_transient_error(connector_reason.to_string(), Duration::from_millis(25));
    context.status.clear();
    assert_eq!(transient_error(&context).as_deref(), Some(connector_reason));
    host.clear_transient_error();
    assert!(context.status.snapshot().is_none());
}

#[nervix_primitives::test]
async fn connector_status_failure_survives_task_success() {
    let context = emitter_context();
    let host = context.emitter_host();
    let reason = "the MQTT connection is unavailable";
    host.record_transient_error(reason.to_string(), Duration::from_millis(25));

    context.status.clear();

    assert_eq!(transient_error(&context).as_deref(), Some(reason));
    host.clear_transient_error();
    assert!(context.status.snapshot().is_none());
}

#[nervix_primitives::test]
async fn emitter_host_delegates_runtime_services_and_general_error_policy() {
    let context = emitter_context();
    let host = context.emitter_host();
    let mut events = context.runtime.events().subscribe();

    host.record_transient_error(
        "broker temporarily unavailable".to_string(),
        Duration::from_millis(25),
    );
    assert_eq!(
        context
            .runtime
            .dataflow_node_transient_state(&context.domain, "EMITTER", &context.emitter)
            .error,
        Some("broker temporarily unavailable".to_string())
    );
    assert!(
        context
            .runtime
            .dataflow_node_transient_state(&context.domain, "EMITTER", &context.emitter)
            .reconnect_backoff
            .is_some()
    );
    host.clear_transient_error();
    assert_eq!(
        context
            .runtime
            .dataflow_node_transient_state(&context.domain, "EMITTER", &context.emitter)
            .error,
        None
    );

    host.report_error("connector background error".to_string());
    let RuntimeEvent::Error(message) = events
        .recv()
        .await
        .expect("the host must publish the event");
    assert_eq!(
        message,
        "emitter error for emitter 'output' in domain 'emitter_tests': connector background error"
    );
    assert_eq!(host.staging_directory(), context.runtime.temp_dir());

    let (logged_acks, logged_completion) = AckSet::root();
    let logged_acks = EmitterRecordAcknowledgements::new(logged_acks);
    host.handle_general_error(&logged_acks, "publish failed".to_string());
    let logged_report = "emitter 'output' general error in domain 'emitter_tests': publish failed";
    let RuntimeEvent::Error(message) = events
        .recv()
        .await
        .expect("the logged general error must publish an event");
    assert_eq!(message, logged_report);
    assert_eq!(
        logged_completion.wait().await,
        AckOutcome::NoAck(logged_report.to_string())
    );

    let mut ignored_context = emitter_context();
    ignored_context.error_policies.general = GeneralErrorPolicy::Ignore;
    let (ignored_acks, ignored_completion) = AckSet::root();
    ignored_context.emitter_host().handle_general_error(
        &EmitterRecordAcknowledgements::new(ignored_acks),
        "ignored failure".to_string(),
    );
    assert_eq!(ignored_completion.wait().await, AckOutcome::Ack);
}

#[nervix_primitives::test]
async fn emitter_acknowledgement_handle_preserves_ack_lifecycle() {
    let (acks, mut completion) = AckSet::root();
    let acks = EmitterRecordAcknowledgements::new(acks);

    assert!(!acks.is_empty());
    acks.keep_alive();
    assert_eq!(completion.wait_for_progress().await, AckProgress::Alive);
    acks.acknowledge();
    assert_eq!(completion.wait().await, AckOutcome::Ack);
    assert!(EmitterRecordAcknowledgements::new(AckSet::empty()).is_empty());
}

/// An emitter whose transport client still holds records once the loop ends. It answers its
/// finish as `answer` says and reports the deadline the host finished it by.
struct HoldingEmitter {
    answer: fn() -> EmitterFinishResult,
    finished_by: watch::Sender<Option<Instant>>,
}

struct DeadlineFinishEmitter {
    overrun: bool,
}

#[async_trait::async_trait]
impl EmitterLifecycle for DeadlineFinishEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        nervix_models::EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic)
    }

    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        if self.overrun {
            nervix_primitives::time::sleep_until(
                deadline
                    + crate::runtime::entity_gate::task_stop_grace(
                        nervix_models::DomainDrainTimeout::DEFAULT,
                    )
                    + Duration::from_millis(1),
            )
            .await;
        } else {
            nervix_primitives::time::sleep_until(deadline).await;
            nervix_primitives::task::yield_now().await;
        }
        Err(Report::new(EmitterFinishError::Unsent {
            connector: "test",
            records: 3,
        }))
    }
}

#[async_trait::async_trait]
impl EmitterConnector for DeadlineFinishEmitter {
    fn lifecycle(&self) -> &dyn EmitterLifecycle {
        self
    }

    fn lifecycle_mut(&mut self) -> &mut dyn EmitterLifecycle {
        self
    }

    async fn publish_batches(
        &mut self,
        _context: &EmitterConnectorContext,
        _publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()> {
        Ok(())
    }
}

async fn stop_drain_with_emitter(
    connector: &mut EmitterConnectorState,
    deadline: Instant,
) -> EmitterStopDrainOutcome {
    let fault_injection = ConfiguredFaultInjection::default();
    let mut backoff = RuntimeReconnectBackoff::default();
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (_stop_tx, mut stop_rx) = watch::channel(Some(deadline));
    let mut control = EmitterPublishControl {
        fault_injection: &fault_injection,
        shutdown_rx: &mut shutdown_rx,
        stop_rx: &mut stop_rx,
        backoff: &mut backoff,
    };
    let context = emitter_context();
    let mut buffer = EmitterBatchBuffer::default();
    connector
        .drain_for_stop(
            "test",
            &context,
            &mut control,
            &mut buffer,
            deadline,
            EmitterPublishAttemptKind::Initial,
        )
        .await
}

#[nervix_primitives::test(start_paused = true)]
async fn a_stop_keeps_the_finish_answer_given_on_its_deadline() {
    let mut connector =
        EmitterConnectorState::opened(Box::new(DeadlineFinishEmitter { overrun: false }));
    let deadline = Instant::now() + Duration::from_secs(1);

    match stop_drain_with_emitter(&mut connector, deadline).await {
        EmitterStopDrainOutcome::Failed(error) => {
            assert_eq!(
                error.current_context(),
                &EmitterRuntimeError::FinishTransport
            );
            assert_eq!(
                emitter_error_message(&error),
                "test emitter's transport client still held 3 records when its finish deadline \
                 passed"
            );
        }
        _ => panic!("the emitter's own finish outcome must win the deadline race"),
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_stop_bounds_a_finish_that_overruns_its_deadline() {
    let mut connector =
        EmitterConnectorState::opened(Box::new(DeadlineFinishEmitter { overrun: true }));
    let deadline = Instant::now() + Duration::from_secs(1);

    assert!(matches!(
        stop_drain_with_emitter(&mut connector, deadline).await,
        EmitterStopDrainOutcome::DeadlineElapsed
    ));
}

#[async_trait::async_trait]
impl EmitterLifecycle for HoldingEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        nervix_models::EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic)
    }

    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        self.finished_by.send_replace(Some(deadline));
        (self.answer)()
    }
}

#[async_trait::async_trait]
impl EmitterConnector for HoldingEmitter {
    fn lifecycle(&self) -> &dyn EmitterLifecycle {
        self
    }

    fn lifecycle_mut(&mut self) -> &mut dyn EmitterLifecycle {
        self
    }

    async fn publish_batches(
        &mut self,
        _context: &EmitterConnectorContext,
        _publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()> {
        Ok(())
    }
}

/// The state of an emitter whose open emitter is a [`HoldingEmitter`] answering `answer`, and what
/// reports the deadline that emitter was finished by.
fn holding_task_state(
    context: &EmitterConnectorContext,
    answer: fn() -> EmitterFinishResult,
) -> (EmitterTaskState, watch::Receiver<Option<Instant>>) {
    let (finished_by, finished) = watch::channel(None);
    let state = EmitterTaskState::new(
        EmitterConnectorState::opened(Box::new(HoldingEmitter {
            answer,
            finished_by,
        })),
        EmitterBatchBuffer::default(),
        RuntimeReconnectBackoff::default(),
        context,
    );
    (state, finished)
}

fn transient_error(context: &EmitterConnectorContext) -> Option<String> {
    context
        .runtime
        .dataflow_node_transient_state(&context.domain, "EMITTER", &context.emitter)
        .error
}

#[nervix_primitives::test]
async fn teardown_finishes_the_emitter_by_the_deadline_its_stopping_execution_gave() {
    let context = emitter_context();
    let (mut state, finished) = holding_task_state(&context, || Ok(EmitterFinish::Flushed));
    let deadline = Instant::now() + Duration::from_secs(5);

    state
        .finish_after_loop(&context, "test", EmitterEnding::Teardown, Some(deadline))
        .await;

    assert_eq!(
        *finished.borrow(),
        Some(deadline),
        "teardown finishes the emitter by the deadline on the task's stop signal"
    );
    assert_eq!(
        transient_error(&context),
        None,
        "a client that sent what it held leaves no transient error"
    );
}

#[nervix_primitives::test]
async fn teardown_reports_the_records_a_finish_left_unsent() {
    let context = emitter_context();
    let mut events = context.runtime.events().subscribe();
    let (mut state, _finished) = holding_task_state(&context, || {
        Err(Report::new(EmitterFinishError::Unsent {
            connector: "test",
            records: 2,
        }))
    });

    state
        .finish_after_loop(
            &context,
            "test",
            EmitterEnding::Teardown,
            Some(Instant::now()),
        )
        .await;

    let error = transient_error(&context).expect("an unsent finish is the transient error");
    assert_eq!(
        error,
        "test emitter's transport client still held 2 records when its finish deadline passed"
    );
    let RuntimeEvent::Error(message) = events
        .recv()
        .await
        .expect("the unsent finish publishes an error event");
    assert_eq!(
        message,
        "emitter 'output' in domain 'emitter_tests' stopped before its test transport client sent \
         every record it held: test emitter's transport client still held 2 records when its \
         finish deadline passed"
    );
}

#[nervix_primitives::test]
async fn a_task_ended_without_a_stop_deadline_leaves_its_emitter_unfinished() {
    let context = emitter_context();
    let (mut state, finished) = holding_task_state(&context, || Ok(EmitterFinish::Flushed));

    state
        .finish_after_loop(&context, "test", EmitterEnding::InteractionStopped, None)
        .await;

    assert_eq!(
        *finished.borrow(),
        None,
        "nothing bounds a finish the task was given no deadline for"
    );
}

#[nervix_primitives::test]
async fn an_emitter_without_an_open_emitter_holds_nothing_to_finish() {
    let context = emitter_context();
    let mut connector = EmitterConnectorState::Unavailable {
        retry_open: false,
        reason: "test emitter is unavailable".to_string(),
    };

    let finished = connector
        .finish_transport(&context, Instant::now())
        .await
        .expect("an emitter without a client has nothing to send");

    assert_eq!(finished, EmitterFinish::NothingHeld);
}

#[test]
fn emitter_outcomes_follow_both_failure_owners_and_end_with_the_task_binding() {
    let context = emitter_context();
    let status = context.status.clone();
    let binding = context
        .interactions
        .bind_failure_source(move || status.failures().is_some());
    let observed = |expected: &str| {
        assert!(context.interactions.describe_lines().iter().any(|line| {
            line.starts_with("nervix_emitter_transient_failure{") && line.ends_with(expected)
        }));
    };
    observed("value=0");
    context.record_failure("publish failed");
    context
        .status
        .fail_connector("connection failed".to_owned(), None);
    observed("value=1");
    context.status.clear();
    observed("value=1");
    context.status.clear_connector();
    observed("value=0");
    context
        .status
        .fail_connector("connection failed".to_owned(), None);
    context.record_failure("publish failed");
    context.status.clear_connector();
    observed("value=1");
    context.status.clear();
    observed("value=0");
    context.record_failure("ending with unresolved work");
    drop(binding);
    observed("value=0");
}

#[nervix_primitives::test]
async fn emitter_outcomes_terminal_failure_counts_only_rows_routed_to_message_policy() {
    let context = emitter_context();
    let routing = DomainRouting::new(DomainRoutingSnapshot::default());
    let mut routing = DomainRoutingCache::new(routing.shared());
    let output_metrics = output_metrics(&context);
    let node = ModelName::from(&context.emitter);
    let source_filters = HashMap::default();
    let batch_context = batch_context(
        &context,
        &mut routing,
        &output_metrics,
        &node,
        &source_filters,
        &[],
    );
    let at = Timestamp::from_unix_nanos(100);
    let mut delivered = EmitterPublishBatch::from_batch(input_batch(), at);
    delivered
        .mark_delivered(0, DeliveredAcknowledgements::Host)
        .assured("fixture delivery");
    let mut rejected = EmitterPublishBatch::from_batch(input_batch(), at);
    rejected
        .mark_rejected_after_delivery(0, std::future::ready(()))
        .await
        .assured("fixture rejection settled");
    let unresolved = EmitterPublishBatch::from_batch(input_batch(), at);
    batch_context
        .handle_publish_error_batches(
            vec![delivered, rejected, unresolved],
            "terminal publish refusal".to_owned(),
            MessageErrorOperation::Publish,
        )
        .await;
    assert!(context.interactions.describe_lines().iter().any(|line| {
        line.starts_with("nervix_emitter_records_failed_total{") && line.ends_with("total=1")
    }));
}
