//! One emitter's task: the loop that receives its input and decides when it publishes.
//!
//! Layer: data plane.
//! - **Owns.** Spawning an emitter task from its start plan, the loop that handles its commands,
//!   force flushes, wakes and input batches, resolving each input batch's materialized state,
//!   filters and ordering groups, the host context its connector reports through, and the
//!   emitter's failure semantics.
//! - **Depends on.** The emitter's start plan and compiled programs, the relay interaction that
//!   delivers its input, its buffer, retry schedule and publishing, and the node's metrics, events
//!   and error policies.
//! - **Must not know.** Which sink crate the emitter publishes through, how its records are
//!   encoded or mapped, or how the registry validated it.

use error_stack::{AttachmentKind, FrameKind, ResultExt as _};
use nervix_connector::{
    SinkAcknowledgementServices, SinkAcknowledgements, SinkEventReporter, SinkGeneralErrorHandler,
    SinkHost, SinkStagingDirectory, SinkTransientErrorStatus, physical_time::actual_utc_now,
};

use super::*;

pub(in crate::runtime) struct EmitterTask;

/// What a sink connector needs to publish one emitter's output. The staging directory and the
/// event bus are read through `runtime` rather than copied in beside it, so the context carries
/// one handle to node state instead of a second view of the same values.
#[derive(Clone)]
pub(in crate::runtime) struct EmitterSinkContext {
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) emitter: EmitterName,
    pub(super) error_policies: ErrorPolicies,
    pub(super) udfs: Option<UdfExecutor>,
    /// The bound clock that resolves this emitter's explicit `FLUSH EACH` and `COMMIT EACH`
    /// cadences. Publish attempts, retry backoff, acknowledgement keepalive and stop deadlines
    /// stay on the monotonic clock and never read it.
    pub(super) clock: DomainClock,
}

#[derive(Debug, Clone)]
enum CompiledSqsFifoGroup {
    FromBranch,
    Expression(CompiledProgramWithMaterializedInterest),
}

struct EmitterBatchContext<'a> {
    runtime: &'a Runtime,
    routing: &'a mut DomainRoutingCache,
    /// Bound once with the task, so resolving a batch's materialized dependencies never re-binds
    /// the domain's execution.
    domain_clock: &'a DomainClock,
    domain: &'a DomainName,
    emitter: &'a EmitterName,
    node: &'a ModelName,
    output_metrics: &'a EmitterOutputMetrics,
    error_policies: &'a ErrorPolicies,
    source_filters: &'a HashMap<RelayName, CompiledProgramWithMaterializedInterest>,
    filter_map: Option<&'a CompiledEmitterFilterMapProgram>,
    sqs_fifo_group: Option<&'a CompiledSqsFifoGroup>,
    materialized_state: &'a [nervix_models::MaterializedStateDependency],
}

enum EmitterOutputMetrics {
    Relay(BatchMetricsHandle),
    WithoutRelay(MessageMetricsHandle),
}

impl EmitterOutputMetrics {
    fn observe(&self, report: &PublishReport) {
        match self {
            Self::Relay(metrics) => {
                metrics.observe(report.messages, report.bytes, Some(report.domain_timestamp))
            }
            Self::WithoutRelay(metrics) => {
                metrics.observe(report.messages, report.bytes, Some(report.domain_timestamp))
            }
        }
    }
}

/// A source batch whose node-wide materialized dependencies are resolved.
///
/// The dependencies are read once for the batch, so the snapshot and the execution time travel
/// with it and every emitter program for it reads exactly the same state.
struct ResolvedEmitterInput {
    batch: RelayRecordBatch,
    materialized_values: HashMap<String, RuntimeValue>,
    execution_now: Timestamp,
}

pub(in crate::runtime) type EmitterRuntimeResult<T> = Result<T, Report<EmitterRuntimeError>>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(in crate::runtime) enum EmitterRuntimeError {
    #[error("invalid emitter sink configuration")]
    InvalidSinkConfig,
    #[error("failed to initialize emitter sink")]
    InitializeSink,
    #[error("emitter sink client is not initialized")]
    SinkNotInitialized,
    #[error("emitter flush policy is not initialized")]
    FlushPolicyNotInitialized,
    #[error("emitter header count {header_count} does not match row count {row_count}")]
    HeaderCountMismatch {
        header_count: usize,
        row_count: usize,
    },
    #[error("SQS FIFO group count {group_count} does not match emitter row count {row_count}")]
    SqsGroupCountMismatch {
        group_count: usize,
        row_count: usize,
    },
    #[error("emitter delivered row {row} is outside batch with {row_count} rows")]
    DeliveryRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter delivered row {row} is outside ack set with {row_count} rows")]
    AcknowledgementRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter rejected row {row} is outside batch with {row_count} rows")]
    RejectionRowOutOfBounds { row: usize, row_count: usize },
    #[error("fault injector failed emitter publish")]
    FaultInjected,
    #[error("emitter shutdown while stalled")]
    ShutdownWhileStalled,
    #[error("the emitter could not resolve its flush cadence against the domain clock")]
    FlushTiming,
    #[error("the emitter retry deadline is outside the monotonic clock range")]
    RetryTiming,
    #[error("emitter stop deadline elapsed")]
    StopDeadlineElapsed,
    #[error("emitter final flush failed")]
    FinalFlush,
    #[error("OTEL RESOURCE attribute '{attribute}' must be a literal value or a literal array")]
    InvalidOtelResource { attribute: String },
    #[error("failed to encode emitter batch")]
    EncodeBatch,
    #[error("failed to publish emitter batch")]
    PublishBatch,
    #[error("emitter publish is stalled")]
    PublishStalled,
}

impl EmitterRuntimeError {
    pub(super) fn is_retryable_publish_failure(&self) -> bool {
        match self {
            Self::SinkNotInitialized | Self::PublishBatch | Self::PublishStalled => true,
            Self::FlushPolicyNotInitialized
            | Self::HeaderCountMismatch { .. }
            | Self::SqsGroupCountMismatch { .. }
            | Self::DeliveryRowOutOfBounds { .. }
            | Self::AcknowledgementRowOutOfBounds { .. }
            | Self::RejectionRowOutOfBounds { .. }
            | Self::InvalidSinkConfig
            | Self::InvalidOtelResource { .. }
            | Self::InitializeSink
            | Self::FaultInjected
            | Self::ShutdownWhileStalled
            | Self::StopDeadlineElapsed
            | Self::FinalFlush
            | Self::FlushTiming
            | Self::RetryTiming
            | Self::EncodeBatch => false,
        }
    }
}

pub(super) fn emitter_report(
    context: EmitterRuntimeError,
    error: impl std::fmt::Display,
) -> Report<EmitterRuntimeError> {
    Report::new(context).attach_printable(error.to_string())
}

pub(super) fn emitter_init_error(error: impl std::fmt::Display) -> Report<EmitterRuntimeError> {
    emitter_report(EmitterRuntimeError::InitializeSink, error)
}

impl EmitterSinkContext {
    pub(super) fn sink_host(&self) -> SinkHost {
        SinkHost::new(self.clone())
    }

    pub(super) fn report_init_error(&self, sink: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to initialize {sink} emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to initialize emitter sink"
        );
    }

    fn report_publish_error(&self, sink: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to publish {sink} message for emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to publish emitter message"
        );
    }

    pub(super) fn report_flush_error(&self, sink: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to flush {sink} rows for emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to flush emitter rows"
        );
    }

    /// Reads the emitter's domain execution time for a cadence decision.
    pub(super) fn execution_snapshot(&self) -> EmitterRuntimeResult<DomainExecutionSnapshot> {
        self.clock
            .snapshot()
            .change_context(EmitterRuntimeError::FlushTiming)
    }

    pub(super) fn parse_flush_policy(
        &self,
        kind: &str,
        policy: &FlushPolicy,
    ) -> Option<RuntimeFlushPolicy> {
        match Runtime::parse_runtime_node_flush_policy(&self.domain, kind, &self.emitter, policy) {
            Ok(policy) => Some(policy),
            Err(error) => {
                self.runtime.events().report_error(error.to_string());
                warn!(
                    domain = self.domain.as_str(),
                    emitter = self.emitter.as_str(),
                    error = %error,
                    "failed to parse emitter flush policy"
                );
                None
            }
        }
    }
}

impl SinkAcknowledgementServices for AckSet {
    fn acknowledge(&self) {
        self.ack_success();
    }

    fn keep_alive(&self) {
        self.ack_alive();
    }

    fn reject(&self, reason: String) {
        self.no_ack(reason);
    }

    fn is_empty(&self) -> bool {
        AckSet::is_empty(self)
    }
}

impl SinkTransientErrorStatus for EmitterSinkContext {
    fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.runtime.record_emitter_transient_error_with_backoff(
            &self.domain,
            &self.emitter,
            reason,
            retry_after,
        );
    }

    fn clear_transient_error(&self) {
        self.runtime
            .clear_emitter_transient_error(&self.domain, &self.emitter);
    }
}

impl SinkEventReporter for EmitterSinkContext {
    fn report_error(&self, message: String) {
        self.runtime.events().report_error(format!(
            "sink error for emitter '{}' in domain '{}': {message}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
    }
}

impl SinkStagingDirectory for EmitterSinkContext {
    fn staging_directory(&self) -> PathBuf {
        self.runtime.temp_dir().to_path_buf()
    }
}

impl SinkGeneralErrorHandler for EmitterSinkContext {
    fn handle_general_error(&self, acks: &SinkAcknowledgements, reason: String) {
        match self.error_policies.general {
            GeneralErrorPolicy::Ignore => acks.acknowledge(),
            GeneralErrorPolicy::Log => {
                self.runtime.events().report_error(format!(
                    "emitter '{}' general error in domain '{}': {}",
                    self.emitter.as_str(),
                    self.domain.as_str(),
                    reason
                ));
                warn!(
                    domain = self.domain.as_str(),
                    emitter = self.emitter.as_str(),
                    reason = %reason,
                    "runtime node handled general error"
                );
                acks.reject(reason);
            }
        }
    }
}

pub(super) fn emitter_error_message(error: &Report<EmitterRuntimeError>) -> String {
    error
        .frames()
        .find_map(|frame| match frame.kind() {
            FrameKind::Attachment(AttachmentKind::Printable(attachment)) => {
                Some(attachment.to_string())
            }
            FrameKind::Context(_) | FrameKind::Attachment(_) => None,
        })
        .unwrap_or_else(|| error.current_context().to_string())
}

pub(super) fn emitter_publish_error_is_retryable(error: &Report<EmitterRuntimeError>) -> bool {
    error.current_context().is_retryable_publish_failure()
}

fn emitter_message_error_operation(
    error: &Report<EmitterRuntimeError>,
    codec_route: bool,
) -> MessageErrorOperation {
    match (error.current_context(), codec_route) {
        (EmitterRuntimeError::EncodeBatch, true) => MessageErrorOperation::Encode,
        (EmitterRuntimeError::EncodeBatch, false) => MessageErrorOperation::Values,
        _ => MessageErrorOperation::Publish,
    }
}

impl EmitterTask {
    pub(in crate::runtime) fn spawn(
        runtime: &Runtime,
        build: EmitterTaskBuildDeps<'_>,
        emitter: CreateEmitter,
        plan: EmitterStartPlan,
        inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    ) -> Result<ScheduledEmitterTask, RuntimeError> {
        let EmitterTaskBuildDeps {
            domain,
            shutdown_tx,
            codecs,
            deps,
        } = build;
        let EmitterTaskDeps {
            input_schema,
            input_branching,
            materialized_relay_specs: materialized_stream_specs,
            lookups,
        } = deps;
        let codec = if let Some(codec_name) = emitter.body.codec() {
            Some(codecs.get(codec_name).cloned().ok_or_else(|| {
                RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("missing emitter codec '{}'", codec_name.as_str()),
                }
            })?)
        } else {
            None
        };
        if plan.sink.batch().is_some()
            && let Some(codec) = &codec
        {
            codec
                .check_batch_container()
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "batching emitter '{}' cannot publish through codec '{}': {}",
                        emitter.name.as_str(),
                        codec.name.as_str(),
                        error.current_context(),
                    ),
                })?;
        }
        let output_compiled_schema = match codec.as_ref() {
            Some(codec) => codec.schema(),
            None => input_schema.clone(),
        };
        let udfs = runtime.udf_executor(domain);
        let filter_map = compile_emitter_filter_map_program(
            domain,
            &emitter,
            input_schema.arrow_schema(),
            input_schema.vm_sensitivity(),
            output_compiled_schema.arrow_schema(),
            output_compiled_schema.vm_sensitivity(),
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized_stream_specs,
                available_lookups: &lookups,
                current_branching: &input_branching,
                udfs: udfs.as_ref(),
            },
        )?;
        let sqs_fifo_group = match emitter.sink.as_ref() {
            EmitSink::Sqs {
                fifo_group: Some(nervix_models::SqsFifoGroup::FromBranch),
                ..
            } => Some(CompiledSqsFifoGroup::FromBranch),
            EmitSink::Sqs {
                fifo_group: Some(nervix_models::SqsFifoGroup::Expression(_)),
                ..
            } => compile_sqs_fifo_group_program(
                domain,
                &emitter,
                input_schema.arrow_schema(),
                input_schema.vm_sensitivity(),
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &lookups,
                    current_branching: &input_branching,
                    udfs: udfs.as_ref(),
                },
            )?
            .map(CompiledSqsFifoGroup::Expression),
            _ => None,
        };
        let mut source_filters = HashMap::default();
        for source_filter in emitter.from.where_clauses() {
            let program = compile_scoped_filter_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: &ModelName::from(&emitter.name),
                },
                Some(&source_filter.where_clause),
                RuntimeVmSchema {
                    schema: input_schema.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                MessageErrorOperation::SourceWhere,
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &lookups,
                    current_branching: &input_branching,
                    udfs: udfs.as_ref(),
                },
                RuntimeFilterScope::Source {
                    namespace: "input",
                    allow_header_reads: false,
                    allow_metadata: false,
                },
            )?
            .verified(
                "a FROM WHERE clause is present here, and a present clause always compiles to a \
                 program",
            );
            source_filters.insert(source_filter.relay.clone(), program);
        }
        let task_domain = domain.clone();
        let task_emitter = emitter.name.clone();
        let task_metric_relay = if emitter.from.relays().len() == 1 {
            emitter.from.first().cloned()
        } else {
            None
        };
        let dispatcher = runtime.inner.remote_dispatcher.load();
        let physical_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        let task_input_metrics = inputs
            .iter()
            .map(|(relay, _)| {
                let metrics = runtime.inner.metrics.resolve_node_input_metrics(
                    domain,
                    ModelKind::Emitter,
                    &ModelName::from(&emitter.name),
                    relay,
                    physical_node_id,
                    None,
                );
                (relay.clone(), metrics)
            })
            .collect::<HashMap<_, _>>();
        let task_output_metrics = match &task_metric_relay {
            Some(relay) => {
                EmitterOutputMetrics::Relay(runtime.inner.metrics.resolve_node_batch_metrics(
                    NodeBatchMetricsSpec {
                        domain,
                        kind: ModelKind::Emitter,
                        node: &ModelName::from(&emitter.name),
                        relay,
                        physical_node_id,
                        direction: "sent",
                        branch_key: None,
                    },
                ))
            }
            None => EmitterOutputMetrics::WithoutRelay(
                runtime.inner.metrics.resolve_global_node_message_metrics(
                    domain,
                    ModelKind::Emitter,
                    &ModelName::from(&emitter.name),
                    physical_node_id,
                    "sent",
                ),
            ),
        };
        let task_flush_policy = emitter.flush_policy.clone();
        let task_error_policies = emitter.error_policies.clone();
        let task_materialized_state = emitter.materialized_state.clone();
        let routing_relay = inputs
            .first()
            .map(|(relay, _)| relay.clone())
            .verified("the registry validated that every emitter has at least one input");
        let fault_injection = runtime.inner.fault_injection.clone();
        let runtime = runtime.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let interaction_shutdown_rx = shutdown_tx.subscribe();
        let mut domain_work_cancel_rx = shutdown_tx.subscribe();
        let (work_cancel, mut work_cancel_rx) = watch::channel(false);
        let task_work_cancel = work_cancel.clone();
        let quiesce_counters =
            runtime.node_quiesce_counters(domain, NodeRef::new(ModelKind::Emitter, &emitter.name));
        let force_flush = runtime.force_flush_participant(domain, quiesce_counters.clone());
        let emitter_buffer_count = runtime
            .inner
            .emitter_buffers
            .entry(DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.name.clone(),
            ))
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        let buffered_messages =
            Arc::new(EmitterBufferedMessages::new(emitter_buffer_count.clone()));
        if let Err(error) = EmitterSinkStarter::check_client_config(&plan) {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "failed to resolve {} emitter client: {}",
                    plan.sink.label(),
                    emitter_error_message(&error)
                ),
            });
        }
        let input_collect_policy = Runtime::parse_runtime_node_input_collect_policy(
            domain,
            "emitter",
            &emitter.name,
            emitter.from.collect_policy.as_ref(),
        )?;
        let (commands, command_rx) = mpsc::channel(4);
        let (stop_signal, mut stop_rx) = watch::channel(None);
        let task_stop_signal = stop_signal.clone();

        let task = tokio::spawn(async move {
            let shared_routing = match runtime
                .wait_for_domain_routing(&task_domain, &routing_relay)
                .await
            {
                Ok(routing) => routing,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "emitter '{}' in domain '{}' could not bind its routing snapshot: {error}",
                        task_emitter.as_str(),
                        task_domain.as_str(),
                    ));
                    return;
                }
            };
            let mut routing = DomainRoutingCache::new(shared_routing);
            // The emitter's explicit FLUSH EACH and COMMIT EACH cadences are domain logical
            // durations, so every emitter binds the domain clock whether or not it collects input.
            let domain_clock = match runtime.bind_domain_clock(&task_domain) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "emitter '{}' in domain '{}' could not bind its domain clock: {error}",
                        task_emitter.as_str(),
                        task_domain.as_str(),
                    ));
                    return;
                }
            };
            let work_cancel_forwarder = AbortOnDropHandle::new(tokio::spawn(async move {
                if *domain_work_cancel_rx.borrow()
                    || domain_work_cancel_rx.changed().await.is_err()
                    || *domain_work_cancel_rx.borrow()
                {
                    task_work_cancel.send_replace(true);
                }
            }));
            let mut interaction_inputs = Vec::with_capacity(inputs.len());
            for (relay, receiver) in inputs {
                let input = match input_collect_policy {
                    Some(policy) => RelayInteractionInput::collecting(
                        relay,
                        receiver,
                        policy,
                        domain_clock.clone(),
                    ),
                    None => RelayInteractionInput::immediate(relay, receiver),
                };
                interaction_inputs.push(input);
            }
            let mut interaction = RelayInteraction::with_commands(
                interaction_inputs,
                interaction_shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters),
                command_rx,
            )
            .verified(
                "the registry validated this emitter's inputs, and a non-empty input list builds \
                 an interaction",
            );
            let context = EmitterSinkContext {
                runtime: runtime.clone(),
                domain: task_domain.clone(),
                emitter: task_emitter.clone(),
                error_policies: task_error_policies.clone(),
                udfs,
                clock: domain_clock.clone(),
            };
            let mut publish_backoff = RuntimeReconnectBackoff::from_policy(plan.retry_policy);
            let mut emitter_buffer =
                EmitterBatchBuffer::new(&context, &task_flush_policy, buffered_messages.clone());
            let mut sink = EmitterSinkState::open_until_cancelled(
                &plan,
                &context,
                &input_schema,
                codec.as_ref(),
                &mut work_cancel_rx,
            )
            .await;
            let mut reconnect_on_wake = sink.unavailable_reason().is_some();
            let mut retry_schedule = EmitterRetrySchedule::default();
            if let Some(reason) = sink.unavailable_reason() {
                let wait = publish_backoff.take_next_delay();
                retry_schedule.defer(
                    &context,
                    EmitterRetryDeferral {
                        wait,
                        acks: EmitterAcknowledgements::default(),
                        waiting_for_stall_clear: false,
                        reason: Some(reason),
                    },
                );
            } else {
                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
            }
            let task_emitter_node = ModelName::from(&task_emitter);
            let mut batch_context = EmitterBatchContext {
                runtime: &runtime,
                routing: &mut routing,
                domain_clock: &domain_clock,
                domain: &task_domain,
                emitter: &task_emitter,
                node: &task_emitter_node,
                output_metrics: &task_output_metrics,
                error_policies: &task_error_policies,
                source_filters: &source_filters,
                filter_map: filter_map.as_ref(),
                sqs_fifo_group: sqs_fifo_group.as_ref(),
                materialized_state: &task_materialized_state,
            };
            loop {
                tokio::task::consume_budget().await;
                let wake = retry_schedule.wake(sink.cadence_wake(&context.clock, &emitter_buffer));
                let receive_input = !retry_schedule.is_active()
                    || emitter_buffer_count.load(Ordering::Acquire) == 0;
                let work = match interaction.next_with_input(wake, receive_input).await {
                    Ok(work) => work,
                    // The emitter holds work whose cadence it cannot resolve, because the domain
                    // clock is not readable in this generation. There is no wall-clock fallback
                    // for a logical cadence, so the work stays buffered and unpublished while the
                    // acknowledgements it owns are kept alive on the physical beat.
                    Err(RelayInteractionError::WakeTiming { reason, .. }) => {
                        runtime.record_emitter_transient_error(&task_domain, &task_emitter, reason);
                        let acks = sink.pending_acks(&emitter_buffer);
                        RuntimeReconnectBackoff::wait_duration_with_ack_alive(
                            RETRY_ACK_ALIVE_EACH,
                            &mut shutdown_rx,
                            &acks,
                        )
                        .await;
                        continue;
                    }
                    Err(error) => {
                        let reason = error.to_string();
                        context.report_flush_error(plan.sink.label(), &reason);
                        runtime.handle_internal_processor_error_for_acks(
                            &task_domain,
                            ModelKind::Emitter,
                            &task_emitter,
                            &task_error_policies,
                            error.acks(),
                            reason,
                        );
                        continue;
                    }
                };
                let (input_event, mut work) = work.into_parts();
                match input_event {
                    RelayInteractionEvent::Command(EmitterTaskCommand::Reconfigure {
                        config,
                        response,
                    }) => {
                        // The new cadence replaces the old one for the batches already buffered,
                        // so a reconfiguration that cannot read the domain clock leaves them
                        // without a deadline and is recorded as the emitter's transient error.
                        if let Err(error) =
                            emitter_buffer.reconfigure(&context, &config.flush_policy)
                        {
                            let reason = emitter_error_message(&error);
                            runtime.record_emitter_transient_error(
                                &task_domain,
                                &task_emitter,
                                reason.clone(),
                            );
                            context.report_flush_error(plan.sink.label(), &reason);
                        }
                        response
                            .send(())
                            .means_peer_left("emitter reconfiguration requester");
                    }
                    RelayInteractionEvent::Command(EmitterTaskCommand::Stop {
                        deadline,
                        response,
                    }) => {
                        if response.is_closed() {
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                            continue;
                        }
                        if emitter_buffer_count.load(Ordering::Acquire) > 0
                            && let Some(reason) =
                                emitter_unavailable_reason(&sink, &fault_injection, &task_emitter)
                        {
                            runtime.record_emitter_transient_error(
                                &task_domain,
                                &task_emitter,
                                reason.clone(),
                            );
                            context.report_flush_error(plan.sink.label(), &reason);
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                            response
                                .send(Err(Report::new(EmitterRuntimeError::FinalFlush)
                                    .attach_printable(format!(
                                        "emitter final flush failed: {reason}"
                                    ))))
                                .means_peer_left("emitter stop requester");
                            continue;
                        }
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        let drained = tokio::time::timeout_at(deadline, async {
                            let report = sink
                                .flush_all(
                                    plan.sink.label(),
                                    &context,
                                    &mut control,
                                    &mut emitter_buffer,
                                )
                                .await?;
                            sink.finish_transport(deadline).await?;
                            Ok::<_, Report<EmitterRuntimeError>>(report)
                        })
                        .await;
                        let result = match drained {
                            Ok(Ok(report)) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                if let Some(report) = report.as_ref() {
                                    batch_context.observe_sent(report);
                                }
                                Ok(())
                            }
                            Ok(Err(error)) => {
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(plan.sink.label(), &reason);
                                Err(
                                    Report::new(EmitterRuntimeError::FinalFlush).attach_printable(
                                        format!("emitter final flush failed: {reason}"),
                                    ),
                                )
                            }
                            Err(_) => {
                                let reason = format!(
                                    "emitter '{}' did not drain before its configured deadline",
                                    task_emitter.as_str()
                                );
                                context.report_flush_error(plan.sink.label(), &reason);
                                Err(Report::new(EmitterRuntimeError::StopDeadlineElapsed)
                                    .attach_printable(reason))
                            }
                        };
                        let should_stop = result.is_ok();
                        if !should_stop {
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                        }
                        if response.send(result).is_ok() && should_stop {
                            break;
                        }
                        if should_stop {
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                        }
                    }
                    RelayInteractionEvent::ForceFlush(completion) => {
                        if emitter_buffer_count.load(Ordering::Acquire) > 0
                            && let Some(reason) =
                                emitter_unavailable_reason(&sink, &fault_injection, &task_emitter)
                        {
                            retry_schedule.include_acks(sink.pending_acks(&emitter_buffer));
                            if retry_schedule.is_active() {
                                runtime.record_emitter_transient_error_with_backoff(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                    publish_backoff.next_delay(),
                                );
                            } else {
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: publish_backoff.take_next_delay(),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: fault_injection
                                            .emitter_should_stall(&task_emitter),
                                        reason: Some(&reason),
                                    },
                                );
                            }
                            context.report_flush_error(plan.sink.label(), &reason);
                            completion.complete();
                            continue;
                        }
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        match sink
                            .flush_all(
                                plan.sink.label(),
                                &context,
                                &mut control,
                                &mut emitter_buffer,
                            )
                            .await
                        {
                            Ok(Some(report)) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                batch_context.observe_sent(&report);
                            }
                            Ok(None) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                            }
                            Err(error) if emitter_publish_error_is_retryable(&error) => {
                                let reason = emitter_error_message(&error);
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: emitter_retry_delay(&mut publish_backoff, &error),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: error.current_context()
                                            == &EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_flush_error(plan.sink.label(), &reason);
                            }
                            Err(error) => {
                                retry_schedule.clear();
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(plan.sink.label(), &reason);
                                let pending = emitter_buffer.drain_pending();
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batches(pending, reason, operation)
                                    .await;
                            }
                        }
                        completion.complete();
                    }
                    RelayInteractionEvent::Stopped(reason) => {
                        debug!(
                            domain = task_domain.as_str(),
                            emitter = task_emitter.as_str(),
                            ?reason,
                            "emitter relay interaction stopped"
                        );
                        if emitter_buffer_count.load(Ordering::Acquire) > 0
                            && let Some(reason) =
                                emitter_unavailable_reason(&sink, &fault_injection, &task_emitter)
                        {
                            runtime.record_emitter_transient_error(
                                &task_domain,
                                &task_emitter,
                                reason.clone(),
                            );
                            context.report_flush_error(plan.sink.label(), &reason);
                            let pending = emitter_buffer.drain_pending();
                            batch_context
                                .handle_publish_error_batches(
                                    pending,
                                    reason,
                                    MessageErrorOperation::Publish,
                                )
                                .await;
                            break;
                        }
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        match sink
                            .flush_all(
                                plan.sink.label(),
                                &context,
                                &mut control,
                                &mut emitter_buffer,
                            )
                            .await
                        {
                            Ok(Some(report)) => batch_context.observe_sent(&report),
                            Ok(None) => {}
                            Err(error) => {
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(plan.sink.label(), &reason);
                                let pending = emitter_buffer.drain_pending();
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batches(pending, reason, operation)
                                    .await;
                            }
                        }
                        break;
                    }
                    RelayInteractionEvent::Wake => {
                        let retry_was_active = retry_schedule.is_active();
                        let retry_is_due = retry_schedule.retry_is_due();
                        let stall_cleared = retry_schedule.release_if_stall_cleared(
                            fault_injection.emitter_should_stall(&task_emitter),
                        );
                        if !retry_is_due && !stall_cleared {
                            continue;
                        }
                        let retry_attempt = retry_was_active && (retry_is_due || stall_cleared);
                        if reconnect_on_wake || sink.unavailable_reason().is_some() {
                            sink = EmitterSinkState::open_until_cancelled(
                                &plan,
                                &context,
                                &input_schema,
                                codec.as_ref(),
                                &mut work_cancel_rx,
                            )
                            .await;
                            emitter_buffer.report_staged_messages(sink.staged_messages());
                            if let Some(reason) = sink.unavailable_reason() {
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: publish_backoff.take_next_delay(),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: false,
                                        reason: Some(reason),
                                    },
                                );
                                reconnect_on_wake = true;
                                continue;
                            }
                            reconnect_on_wake = false;
                            runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                        }
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        match sink
                            .flush_due(
                                plan.sink.label(),
                                &context,
                                &mut control,
                                &mut emitter_buffer,
                                retry_attempt,
                            )
                            .await
                        {
                            Ok(Some(report)) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                batch_context.observe_sent(&report);
                            }
                            Ok(None) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                            }
                            Err(error) if emitter_publish_error_is_retryable(&error) => {
                                let reason = emitter_error_message(&error);
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: emitter_retry_delay(&mut publish_backoff, &error),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: error.current_context()
                                            == &EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_flush_error(plan.sink.label(), &reason);
                            }
                            Err(error) => {
                                retry_schedule.clear();
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(plan.sink.label(), &reason);
                                let pending = emitter_buffer.drain_pending();
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batches(pending, reason, operation)
                                    .await;
                            }
                        }
                    }
                    RelayInteractionEvent::Batch {
                        relay: input_relay,
                        batch,
                    } => {
                        let delivery_observation = batch.delivery_observation(actual_utc_now());
                        let input_metrics = task_input_metrics
                            .get(&input_relay)
                            .verified("the task resolves metrics for every declared emitter input");
                        input_metrics.observe_batch(
                            batch.message_count(),
                            batch.estimated_bytes(),
                            delivery_observation.domain_timestamp,
                        );
                        runtime.mark_branch_aggregated_metrics_updated(
                            &task_domain,
                            ModelKind::Emitter,
                            &task_emitter,
                        );
                        for seconds in delivery_observation.latency_seconds {
                            input_metrics.observe_delivery_latency(
                                seconds,
                                delivery_observation.domain_timestamp,
                            );
                        }
                        let wait_for_required_state = !interaction.is_terminal_drain();
                        let publish_batch = match batch_context
                            .process(
                                &input_relay,
                                batch,
                                &mut work_cancel_rx,
                                wait_for_required_state,
                                work.as_mut(),
                            )
                            .await
                        {
                            Some(batch) => batch,
                            None => continue,
                        };

                        if interaction.is_draining() {
                            if let Err(error) =
                                emitter_buffer.retain_without_cadence(publish_batch.clone())
                            {
                                let reason = emitter_error_message(&error);
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batch(publish_batch, reason, operation)
                                    .await;
                            } else if !interaction.is_terminal_drain() {
                                retry_schedule.include_acks(publish_batch.merged_acks());
                            }
                            continue;
                        }

                        if retry_schedule.is_active()
                            || emitter_unavailable_reason(&sink, &fault_injection, &task_emitter)
                                .is_some()
                        {
                            let unavailable =
                                emitter_unavailable_reason(&sink, &fault_injection, &task_emitter);
                            if let Err(error) = emitter_buffer.push(&context, publish_batch.clone())
                            {
                                let reason = emitter_error_message(&error);
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batch(publish_batch, reason, operation)
                                    .await;
                                continue;
                            }
                            retry_schedule.include_acks(publish_batch.merged_acks());
                            if !retry_schedule.is_active() {
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: publish_backoff.take_next_delay(),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: fault_injection
                                            .emitter_should_stall(&task_emitter),
                                        reason: unavailable.as_deref(),
                                    },
                                );
                                if let Some(reason) = unavailable.as_deref() {
                                    context.report_publish_error(plan.sink.label(), reason);
                                }
                            }
                            reconnect_on_wake |= sink.unavailable_reason().is_some();
                            continue;
                        }

                        let mut pending_batch = Some(publish_batch);
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        let publish_result = sink
                            .publish_batch(
                                &context,
                                &mut control,
                                &mut emitter_buffer,
                                pending_batch
                                    .as_ref()
                                    .verified("this branch only runs while a batch is pending")
                                    .clone(),
                            )
                            .await;
                        match publish_result {
                            Ok(Some(report)) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                batch_context.observe_sent(&report);
                                pending_batch.take();
                            }
                            Ok(None) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                pending_batch.take();
                            }
                            Err(failure) if emitter_publish_error_is_retryable(&failure.error) => {
                                let EmitterPublishFailure { error, batch_owner } = failure;
                                let wait = emitter_retry_delay(&mut publish_backoff, &error);
                                if let EmitterPublishBatchOwner::Caller = batch_owner
                                    && let Some(batch) = pending_batch.take()
                                    && let Err(retain_error) =
                                        emitter_buffer.push(&context, batch.clone())
                                {
                                    retry_schedule.clear();
                                    let reason = emitter_error_message(&retain_error);
                                    let operation = emitter_message_error_operation(
                                        &retain_error,
                                        codec.is_some(),
                                    );
                                    batch_context
                                        .handle_publish_error_batch(batch, reason, operation)
                                        .await;
                                    continue;
                                }
                                if let EmitterPublishBatchOwner::Buffer
                                | EmitterPublishBatchOwner::Sink = batch_owner
                                {
                                    pending_batch.take();
                                }
                                let reason = emitter_error_message(&error);
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait,
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: error.current_context()
                                            == &EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_publish_error(plan.sink.label(), &reason);
                            }
                            Err(failure) => {
                                retry_schedule.clear();
                                let (error, failed_batches) = failure
                                    .drain_failed_batches(&mut pending_batch, &mut emitter_buffer);
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_publish_error(plan.sink.label(), &reason);
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batches(failed_batches, reason, operation)
                                    .await;
                            }
                        }
                    }
                }
            }
            drop(work_cancel_forwarder);
        });
        Ok(ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        })
    }
}

impl EmitterBatchContext<'_> {
    fn observe_sent(&self, report: &PublishReport) {
        self.output_metrics.observe(report);
        self.runtime.mark_branch_aggregated_metrics_updated(
            self.domain,
            ModelKind::Emitter,
            self.node,
        );
    }

    async fn handle_publish_error_batches(
        &self,
        batches: impl IntoIterator<Item = EmitterPublishBatch>,
        reason: String,
        operation: MessageErrorOperation,
    ) {
        for batch in batches {
            self.handle_publish_error_batch(batch, reason.clone(), operation)
                .await;
        }
    }

    async fn handle_publish_error_batch(
        &self,
        batch: EmitterPublishBatch,
        reason: String,
        operation: MessageErrorOperation,
    ) {
        let delivered = batch.delivered.clone();
        let messages = match batch.batch.try_into_messages() {
            Ok(messages) => messages,
            Err(error) => {
                let failure = *error;
                self.report_general_error(
                    failure.preserved.acks.iter(),
                    format!("{reason}; {}", failure.error),
                );
                return;
            }
        };
        for (row, message) in messages.into_iter().enumerate() {
            if delivered.get(row).copied().unwrap_or(false) {
                continue;
            }
            self.runtime
                .handle_structured_message_error(MessageErrorHandling {
                    domain: self.domain,
                    node_kind: ModelKind::Emitter,
                    node: self.node,
                    source_route: None,
                    policy: &self.error_policies.message,
                    message,
                    error: structured_message_error(
                        batch.execution_now,
                        MessageErrorCode::External,
                        reason.clone(),
                        operation,
                        None,
                        std::iter::empty(),
                    ),
                    partial_output: None,
                    materialized_state: HashMap::default(),
                    ingest_metadata: None,
                    execution_now: batch.execution_now,
                })
                .await;
        }
    }

    /// Report a failure that no single message owns, so the node-wide general error policy
    /// decides what happens to the acknowledgments the failed work was holding.
    fn report_general_error<'a>(&self, acks: impl IntoIterator<Item = &'a AckSet>, reason: String) {
        self.runtime.handle_general_error_for_acks(
            self.domain,
            ModelKind::Emitter,
            self.emitter,
            self.error_policies,
            acks,
            reason,
        );
    }

    /// Deliver the message errors a plan recorded, one for each row its program rejected.
    async fn deliver_planned_message_errors(&self, errors: Vec<PlannedMessageError>) {
        self.runtime
            .handle_planned_message_errors(
                self.domain,
                ModelKind::Emitter,
                self.emitter,
                self.error_policies,
                errors,
            )
            .await;
    }

    /// Resolve the node-wide materialized dependencies of one source batch.
    ///
    /// `None` means the batch is no longer this call's to publish: a declaration skipped it, the
    /// wait for required state ended without it, or resolution failed and its acknowledgments
    /// have already been reported.
    async fn resolve_materialized_dependencies(
        &mut self,
        input_relay: &RelayName,
        batch: RelayRecordBatch,
        wait: MaterializedBatchWaitContext<'_>,
    ) -> Option<ResolvedEmitterInput> {
        // Resolution consumes the batch, so the acknowledgments a failure would report are taken
        // while the batch still holds them.
        let dependency_error_acks = batch.acks.clone();
        let resolution = self
            .runtime
            .resolve_materialized_dependencies_for_batch(
                MaterializedDomainHandles {
                    routing: &mut *self.routing,
                    domain_clock: self.domain_clock,
                    domain: self.domain,
                },
                input_relay,
                self.materialized_state,
                batch,
                wait,
            )
            .await;
        let resolved = match resolution {
            Ok(Some(resolved)) => resolved,
            Ok(None) => return None,
            Err(error) => {
                self.runtime.handle_internal_processor_error_for_acks(
                    self.domain,
                    ModelKind::Emitter,
                    self.emitter,
                    self.error_policies,
                    dependency_error_acks.iter(),
                    format!(
                        "emitter '{}' failed to resolve materialized dependencies: {error}",
                        self.emitter.as_str()
                    ),
                );
                return None;
            }
        };
        // The node-wide dependencies are resolved once for the batch, so every emitter program
        // reads that snapshot instead of re-reading the state store per program.
        let (batch, materialized_values, execution_now) = resolved;
        Some(ResolvedEmitterInput {
            batch,
            materialized_values,
            execution_now,
        })
    }

    async fn process(
        &mut self,
        input_relay: &RelayName,
        batch: RelayRecordBatch,
        shutdown_rx: &mut watch::Receiver<bool>,
        wait_for_required_state: bool,
        quiesce_work: Option<&mut NodeQuiesceWorkGuard>,
    ) -> Option<EmitterPublishBatch> {
        let resolved = self
            .resolve_materialized_dependencies(
                input_relay,
                batch,
                MaterializedBatchWaitContext {
                    shutdown_rx,
                    wait_for_required_state,
                    quiesce_work,
                },
            )
            .await?;
        let ResolvedEmitterInput {
            batch,
            materialized_values,
            execution_now,
        } = resolved;

        let batch = self
            .filter_source_batch(input_relay, batch, &materialized_values, execution_now)
            .await?;

        // FIFO groups are evaluated over the filtered source batch and stay indexed by source
        // row, so the filter map below can hand every published row the group its own input
        // produced. A row that cannot produce one keeps its reason and is rejected at the send;
        // only a failure of the whole evaluation drops the batch here.
        let source_sqs_message_groups = match self.sqs_fifo_group {
            None => vec![Ok(None); batch.batch.batch().num_rows()],
            Some(CompiledSqsFifoGroup::FromBranch) => {
                let mut groups = Vec::with_capacity(batch.keys.len());
                for key in &batch.keys {
                    let group = match key.as_ref() {
                        Some(key) => Ok(Some(key.as_str().to_string())),
                        None => Err(SqsMessageGroupError::UnbranchedRecord),
                    };
                    groups.push(group);
                }
                groups
            }
            Some(CompiledSqsFifoGroup::Expression(program)) => {
                let evaluated = evaluate_sqs_fifo_group_program(
                    self.emitter,
                    program,
                    &batch,
                    execution_now,
                    &materialized_values,
                )
                .await;
                match evaluated {
                    Ok(groups) => groups,
                    Err(error) => {
                        let error = error.current_context();
                        self.report_general_error(error.acks.iter(), error.reason.clone());
                        return None;
                    }
                }
            }
        };

        let Some(filter_map) = self.filter_map else {
            // Without a filter map every source row publishes as it arrived, so the groups
            // already align with the batch row for row.
            let publish_batch = EmitterPublishBatch::from_batch(batch, execution_now);
            match publish_batch.with_sqs_message_groups(source_sqs_message_groups) {
                Ok(batch) => return Some(batch),
                Err(error) => {
                    self.report_general_error(
                        std::iter::empty::<&AckSet>(),
                        format!(
                            "emitter '{}' failed to build SQS FIFO group batch: {error}",
                            self.emitter.as_str()
                        ),
                    );
                    return None;
                }
            }
        };

        let planned = plan_emitter_filter_map_batch(
            self.emitter,
            filter_map,
            batch,
            execution_now,
            &materialized_values,
        )
        .await;
        let plan = match planned {
            Ok(plan) => plan,
            Err(error) => {
                self.report_general_error(error.acks.iter(), error.reason);
                return None;
            }
        };

        // The plan reports the source row of every output row in output order, so reading the
        // source groups through it keeps each published row with the group its own input
        // produced.
        let mut selected_sqs_message_groups = Vec::with_capacity(plan.source_rows.len());
        for source_row in &plan.source_rows {
            let group = match source_sqs_message_groups.get(*source_row) {
                Some(group) => group.clone(),
                None => Err(SqsMessageGroupError::SourceRowOutOfBounds { row: *source_row }),
            };
            selected_sqs_message_groups.push(group);
        }

        self.deliver_planned_message_errors(plan.message_errors)
            .await;

        // A plan that kept no row has no batch to publish, and the rows it rejected have just
        // been reported.
        let batch = plan.batch?;
        let publish_batch = match EmitterPublishBatch::new(batch, plan.headers, execution_now) {
            Ok(publish_batch) => publish_batch,
            Err(error) => {
                self.report_filtered_batch_error(error);
                return None;
            }
        };
        match publish_batch.with_sqs_message_groups(selected_sqs_message_groups) {
            Ok(batch) => Some(batch),
            Err(error) => {
                self.report_filtered_batch_error(error);
                None
            }
        }
    }

    /// Report a filtered batch whose rows, headers, and FIFO groups stopped agreeing in count.
    /// Both counts are checked while building the same batch, so either one failing is the same
    /// failure to report.
    fn report_filtered_batch_error(&self, error: Report<EmitterRuntimeError>) {
        self.report_general_error(
            std::iter::empty::<&AckSet>(),
            format!(
                "emitter '{}' failed to build filtered header batch: {}",
                self.emitter.as_str(),
                error
            ),
        );
    }

    async fn filter_source_batch(
        &self,
        input_relay: &RelayName,
        batch: RelayRecordBatch,
        side_inputs: &HashMap<String, RuntimeValue>,
        execution_now: Timestamp,
    ) -> Option<RelayRecordBatch> {
        let Some(program) = self.source_filters.get(input_relay) else {
            return Some(batch);
        };
        let plan = match plan_filter_map_messages(
            "emitter",
            self.emitter,
            MessageErrorOperation::SourceWhere,
            program,
            batch,
            execution_now,
            side_inputs,
        )
        .await
        {
            Ok(plan) => plan,
            Err(error) => {
                self.report_general_error(
                    error.acks.iter(),
                    format!("input relay '{}': {}", input_relay.as_str(), error.reason),
                );
                return None;
            }
        };
        self.deliver_planned_message_errors(plan.message_errors)
            .await;
        plan.batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_fixtures::sink_context;

    #[test]
    fn emitter_error_classification_is_explicit_for_every_context() {
        for retryable in [
            EmitterRuntimeError::SinkNotInitialized,
            EmitterRuntimeError::PublishBatch,
            EmitterRuntimeError::PublishStalled,
        ] {
            assert!(retryable.is_retryable_publish_failure());
            assert!(emitter_publish_error_is_retryable(&Report::new(retryable)));
        }
        for terminal in [
            EmitterRuntimeError::InvalidSinkConfig,
            EmitterRuntimeError::InitializeSink,
            EmitterRuntimeError::FlushPolicyNotInitialized,
            EmitterRuntimeError::FaultInjected,
            EmitterRuntimeError::EncodeBatch,
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
        let attached = Report::new(EmitterRuntimeError::PublishBatch)
            .attach_printable("specific broker failure");
        assert_eq!(emitter_error_message(&attached), "specific broker failure");

        let bare = Report::new(EmitterRuntimeError::EncodeBatch);
        assert_eq!(
            emitter_error_message(&bare),
            "failed to encode emitter batch"
        );
    }

    #[tokio::test]
    async fn sink_context_reports_configuration_and_publish_failures() {
        let context = sink_context();
        let mut events = context.runtime.events().subscribe();

        context.report_init_error("nats", "init failed");
        context.report_publish_error("nats", "publish failed");
        context.report_flush_error("nats", "flush failed");
        assert!(
            context
                .parse_flush_policy(
                    "emitter",
                    &FlushPolicy::Each {
                        interval: "not-a-duration".to_string(),
                        max_batch_size: "1MiB".to_string()
                    }
                )
                .is_none()
        );

        let mut messages = Vec::with_capacity(4);
        for _ in 0..4 {
            tokio::task::consume_budget().await;
            let RuntimeEvent::Error(message) =
                events.recv().await.expect("error event must be emitted");
            messages.push(message);
        }
        assert!(messages[0].contains("failed to initialize nats emitter"));
        assert!(messages[1].contains("failed to publish nats message"));
        assert!(messages[2].contains("failed to flush nats rows"));
        assert!(messages[3].contains("invalid flush_each 'not-a-duration'"));
    }

    #[tokio::test]
    async fn sink_host_delegates_runtime_services_and_general_error_policy() {
        let context = sink_context();
        let host = context.sink_host();
        let mut events = context.runtime.events().subscribe();

        host.record_transient_error(
            "broker temporarily unavailable".to_string(),
            Duration::from_millis(25),
        );
        assert_eq!(
            context
                .runtime
                .emitter_transient_error(&context.domain, &context.emitter),
            Some("broker temporarily unavailable".to_string())
        );
        assert!(
            context
                .runtime
                .emitter_reconnect_backoff(&context.domain, &context.emitter)
                .is_some()
        );
        host.clear_transient_error();
        assert_eq!(
            context
                .runtime
                .emitter_transient_error(&context.domain, &context.emitter),
            None
        );

        host.report_error("connector background error".to_string());
        let RuntimeEvent::Error(message) = events
            .recv()
            .await
            .expect("the host must publish the event");
        assert_eq!(
            message,
            "sink error for emitter 'output' in domain 'emitter_tests': connector background error"
        );
        assert_eq!(host.staging_directory(), context.runtime.temp_dir());

        let (logged_acks, logged_completion) = AckSet::root();
        let logged_acks = SinkAcknowledgements::new(logged_acks);
        host.handle_general_error(&logged_acks, "publish failed".to_string());
        let RuntimeEvent::Error(message) = events
            .recv()
            .await
            .expect("the logged general error must publish an event");
        assert!(message.contains("publish failed"));
        assert_eq!(
            logged_completion.wait().await,
            AckOutcome::NoAck("publish failed".to_string())
        );

        let mut ignored_context = sink_context();
        ignored_context.error_policies.general = GeneralErrorPolicy::Ignore;
        let (ignored_acks, ignored_completion) = AckSet::root();
        ignored_context.sink_host().handle_general_error(
            &SinkAcknowledgements::new(ignored_acks),
            "ignored failure".to_string(),
        );
        assert_eq!(ignored_completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn sink_acknowledgement_handle_preserves_ack_lifecycle() {
        let (acks, mut completion) = AckSet::root();
        let acks = SinkAcknowledgements::new(acks);

        assert!(!acks.is_empty());
        acks.keep_alive();
        assert_eq!(completion.wait_for_progress().await, AckProgress::Alive);
        acks.acknowledge();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        assert!(SinkAcknowledgements::new(AckSet::empty()).is_empty());
    }
}
