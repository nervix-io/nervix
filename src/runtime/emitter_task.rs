//! One emitter's task: the loop that receives its input and decides when it publishes.
//!
//! Layer: data plane.
//! - **Owns.** Spawning an emitter task from its execution plan, the loop that handles its commands,
//!   force flushes, wakes and input batches, resolving each input batch's materialized state,
//!   filters, ordering groups and HTTP request fields, the host context its connector reports
//!   through, and the emitter's failure semantics.
//! - **Depends on.** The emitter's execution and sink start plans and compiled programs, the relay interaction that
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
    ordering_group: Option<&'a CompiledOrderingGroup>,
    /// The request fields an HTTP emitter evaluates for every record its route keeps, absent for
    /// every other sink.
    http_requests: Option<&'a CompiledHttpRequestFields>,
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
    #[error("emitter ordering group count {group_count} does not match row count {row_count}")]
    OrderingGroupCountMismatch {
        group_count: usize,
        row_count: usize,
    },
    #[error("emitter route kept source row {row} outside the {row_count} rows it has groups for")]
    OrderingGroupRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter HTTP request count {request_count} does not match row count {row_count}")]
    HttpRequestCountMismatch {
        request_count: usize,
        row_count: usize,
    },
    #[error("emitter batch row {row} has no HTTP request")]
    MissingHttpRequest { row: usize },
    #[error("failed to select the ordering groups of the rows an emitter route kept")]
    SelectOrderingGroups,
    #[error("emitter delivered row {row} is outside batch with {row_count} rows")]
    DeliveryRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter delivered row {row} is outside ack set with {row_count} rows")]
    AcknowledgementRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter rejected row {row} is outside batch with {row_count} rows")]
    RejectionRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter prepared row {row} is outside batch with {row_count} rows")]
    PreparedRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter row {row} is already carried by a prepared payload or resolved")]
    RowAlreadyPrepared { row: usize },
    #[error("emitter sink answered for record {record} of a write with {records} records")]
    UnknownSinkRecord { record: usize, records: usize },
    #[error("emitter sink answered twice for record {record}")]
    SinkRecordAnsweredTwice { record: usize },
    #[error("emitter sink left {unanswered} of {records} records unanswered")]
    UnansweredSinkRecords { unanswered: usize, records: usize },
    #[error("emitter sink broke its preparation of batch {batch_index}: {violation}")]
    RowPreparation {
        batch_index: usize,
        violation: RowPreparationViolation,
    },
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
            Self::SinkNotInitialized
            | Self::PublishBatch
            | Self::PublishStalled
            | Self::UnansweredSinkRecords { .. } => true,
            Self::FlushPolicyNotInitialized
            | Self::HeaderCountMismatch { .. }
            | Self::OrderingGroupCountMismatch { .. }
            | Self::OrderingGroupRowOutOfBounds { .. }
            | Self::HttpRequestCountMismatch { .. }
            | Self::MissingHttpRequest { .. }
            | Self::SelectOrderingGroups
            | Self::DeliveryRowOutOfBounds { .. }
            | Self::AcknowledgementRowOutOfBounds { .. }
            | Self::RejectionRowOutOfBounds { .. }
            | Self::PreparedRowOutOfBounds { .. }
            | Self::RowAlreadyPrepared { .. }
            | Self::UnknownSinkRecord { .. }
            | Self::SinkRecordAnsweredTwice { .. }
            | Self::RowPreparation { .. }
            | Self::InvalidSinkConfig
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
    pub(super) fn dns(&self) -> Result<DnsResolver, Report<EmitterRuntimeError>> {
        let Some(dns) = self.runtime.dns() else {
            return Err(Report::new(EmitterRuntimeError::InitializeSink)
                .attach_printable("the node DNS resolver is not installed"));
        };
        Ok(dns.clone())
    }

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

/// The mutable state changed by an emitter loop's publish attempts.
///
/// Keeping these values together makes every event apply the same success, retry and terminal
/// failure transitions. The loop still decides when an attempt starts and whether it is a cadence,
/// force flush or newly received batch.
struct EmitterTaskState {
    sink: EmitterSinkState,
    buffer: EmitterBatchBuffer,
    retry: EmitterRetrySchedule,
    backoff: RuntimeReconnectBackoff,
    reconnect_on_wake: bool,
}

#[derive(Clone, Copy)]
enum EmitterPublishErrorReport {
    Flush,
    Publish,
}

#[derive(Clone, Copy)]
struct EmitterPublishOutcomeContext<'a> {
    sink_label: &'a str,
    codec_route: bool,
    error_report: EmitterPublishErrorReport,
}

impl EmitterPublishOutcomeContext<'_> {
    fn report_error(self, context: &EmitterSinkContext, reason: &str) {
        match self.error_report {
            EmitterPublishErrorReport::Flush => context.report_flush_error(self.sink_label, reason),
            EmitterPublishErrorReport::Publish => {
                context.report_publish_error(self.sink_label, reason)
            }
        }
    }
}

impl EmitterTaskState {
    fn new(
        sink: EmitterSinkState,
        buffer: EmitterBatchBuffer,
        mut backoff: RuntimeReconnectBackoff,
        context: &EmitterSinkContext,
    ) -> Self {
        let reconnect_on_wake = sink.unavailable_reason().is_some();
        let mut retry = EmitterRetrySchedule::default();
        if let Some(reason) = sink.unavailable_reason() {
            retry.defer(
                context,
                EmitterRetryDeferral {
                    wait: backoff.take_next_delay(),
                    acks: EmitterAcknowledgements::default(),
                    waiting_for_stall_clear: false,
                    reason: Some(reason),
                },
            );
        } else {
            context
                .runtime
                .clear_emitter_transient_error(&context.domain, &context.emitter);
        }
        Self {
            sink,
            buffer,
            retry,
            backoff,
            reconnect_on_wake,
        }
    }

    fn wake(&self, context: &EmitterSinkContext) -> RuntimeWake {
        self.retry
            .wake(self.sink.cadence_wake(&context.clock, &self.buffer))
    }

    fn receives_input(&self, buffered_messages: usize) -> bool {
        !self.retry.is_active() || buffered_messages == 0
    }

    async fn handle_publish_result(
        &mut self,
        result: EmitterPublishResult,
        pending_batch: &mut Option<EmitterPublishBatch>,
        context: &EmitterSinkContext,
        batch_context: &EmitterBatchContext<'_>,
        outcome_context: EmitterPublishOutcomeContext<'_>,
    ) {
        match result {
            Ok(report) => {
                self.backoff.reset();
                self.retry.clear();
                context
                    .runtime
                    .clear_emitter_transient_error(&context.domain, &context.emitter);
                if let Some(report) = report.as_ref() {
                    batch_context.observe_sent(report);
                }
                pending_batch.take();
            }
            Err(failure) if emitter_publish_error_is_retryable(failure.error()) => {
                let (error, batch_owner) = failure.into_parts();
                let wait = emitter_retry_delay(&mut self.backoff, &error);
                if let EmitterPublishBatchOwner::Caller = batch_owner {
                    let batch = pending_batch.take().verified(
                        "a caller-owned publish failure retains the batch passed to the attempt",
                    );
                    if let Err(retain_error) = self.buffer.push(context, batch.clone()) {
                        self.retry.clear();
                        let reason = emitter_error_message(&retain_error);
                        let operation = emitter_message_error_operation(
                            &retain_error,
                            outcome_context.codec_route,
                        );
                        batch_context
                            .handle_publish_error_batch(batch, reason, operation)
                            .await;
                        return;
                    }
                } else {
                    pending_batch.take();
                }
                let reason = emitter_error_message(&error);
                self.retry.defer(
                    context,
                    EmitterRetryDeferral {
                        wait,
                        acks: self.sink.pending_acks(&self.buffer),
                        waiting_for_stall_clear: error.current_context()
                            == &EmitterRuntimeError::PublishStalled,
                        reason: Some(&reason),
                    },
                );
                self.reconnect_on_wake = self.sink.reconnect_after(&error);
                outcome_context.report_error(context, &reason);
            }
            Err(failure) => {
                self.retry.clear();
                let (error, failed_batches) =
                    failure.drain_failed_batches(pending_batch, &mut self.buffer);
                let reason = emitter_error_message(&error);
                context.runtime.record_emitter_transient_error(
                    &context.domain,
                    &context.emitter,
                    reason.clone(),
                );
                outcome_context.report_error(context, &reason);
                let operation =
                    emitter_message_error_operation(&error, outcome_context.codec_route);
                batch_context
                    .handle_publish_error_batches(failed_batches, reason, operation)
                    .await;
            }
        }
    }
}

/// The already prepared dependencies one emitter's event loop drives.
struct EmitterTaskLoop<'a> {
    context: &'a EmitterSinkContext,
    batch_context: EmitterBatchContext<'a>,
    state: EmitterTaskState,
    interaction: RelayInteraction<EmitterTaskCommand>,
    plan: &'a EmitterStartPlan,
    input_schema: &'a CompiledSchema,
    codec: Option<&'a Arc<CompiledCodec>>,
    input_metrics: &'a HashMap<RelayName, NodeInputMetricsHandle>,
    fault_injection: &'a ConfiguredFaultInjection,
    buffered_messages: &'a AtomicUsize,
    work_cancel_rx: &'a mut watch::Receiver<bool>,
    shutdown_rx: &'a mut watch::Receiver<bool>,
    stop_rx: &'a mut watch::Receiver<Option<Instant>>,
    stop_signal: &'a watch::Sender<Option<Instant>>,
}

impl EmitterTask {
    pub(in crate::runtime) fn spawn(
        runtime: &Runtime,
        build: EmitterTaskBuildDeps<'_>,
        emitter: EmitterExecutionPlan,
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
        let codec = if let Some(codec_name) = emitter.codec.as_ref() {
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
        let compile_context = RuntimeVmCompileContext {
            available_materialized_streams: &materialized_stream_specs,
            available_lookups: &lookups,
            current_branching: &input_branching,
            udfs: udfs.as_ref(),
        };
        let filter_map = compile_emitter_filter_map_program(
            domain,
            &emitter.name,
            emitter.route.as_ref(),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: input_schema.vm_sensitivity(),
                output: output_compiled_schema.arrow_schema(),
                output_sensitivity: output_compiled_schema.vm_sensitivity(),
            },
            compile_context,
        )?;
        let http_requests = match &plan.sink {
            EmitterSinkPlan::Http(sink) => {
                let output = codec.as_ref().map(|codec| RuntimeVmSchema {
                    schema: codec.schema().arrow_schema(),
                    sensitivity: codec.schema().vm_sensitivity(),
                });
                let compiled = CompiledHttpRequestFields::compile(
                    &emitter.name,
                    sink,
                    emitter
                        .http_request
                        .as_ref()
                        .verified("the decision gives every HTTP emitter its request program"),
                    HttpRequestSchemas {
                        input: RuntimeVmSchema {
                            schema: input_schema.arrow_schema(),
                            sensitivity: input_schema.vm_sensitivity(),
                        },
                        output,
                    },
                    compile_context,
                )
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("{error:#}"),
                })?;
                Some(compiled)
            }
            _ => None,
        };
        let ordering_group = match emitter.ordering_group.as_ref() {
            None => None,
            Some(declared) => Some(CompiledOrderingGroup::compile(
                declared,
                domain,
                &emitter.name,
                RuntimeVmSchema {
                    schema: input_schema.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &lookups,
                    current_branching: &input_branching,
                    udfs: udfs.as_ref(),
                },
            )?),
        };
        let mut source_filters = HashMap::default();
        for input in &emitter.inputs {
            let Some(source_filter) = input.from_where.as_ref() else {
                continue;
            };
            let program = bind_scoped_filter_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: &ModelName::from(&emitter.name),
                },
                source_filter.program(),
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
            )?;
            source_filters.insert(input.relay.clone(), program);
        }
        let task_domain = domain.clone();
        let task_emitter = emitter.name.clone();
        let task_metric_relay = if emitter.inputs.len() == 1 {
            emitter.inputs.first().map(|input| input.relay.clone())
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
        let mut terminal_shutdown_rx = shutdown_tx.subscribe();
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
            emitter.collect_policy.as_ref(),
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
            let interaction = RelayInteraction::with_commands(
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
            let backoff = RuntimeReconnectBackoff::from_policy(plan.retry_policy);
            let buffer =
                EmitterBatchBuffer::new(&context, &task_flush_policy, buffered_messages.clone());
            let sink = EmitterSinkState::open_until_cancelled(
                &plan,
                &context,
                &input_schema,
                codec.as_ref(),
                &mut work_cancel_rx,
            )
            .await;
            let state = EmitterTaskState::new(sink, buffer, backoff, &context);
            let task_emitter_node = ModelName::from(&task_emitter);
            let batch_context = EmitterBatchContext {
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
                ordering_group: ordering_group.as_ref(),
                http_requests: http_requests.as_ref(),
                materialized_state: &task_materialized_state,
            };
            let mut task_loop = EmitterTaskLoop {
                context: &context,
                batch_context,
                state,
                interaction,
                plan: &plan,
                input_schema: &input_schema,
                codec: codec.as_ref(),
                input_metrics: &task_input_metrics,
                fault_injection: &fault_injection,
                buffered_messages: &emitter_buffer_count,
                work_cancel_rx: &mut work_cancel_rx,
                shutdown_rx: &mut shutdown_rx,
                stop_rx: &mut stop_rx,
                stop_signal: &task_stop_signal,
            };
            // The interaction observes shutdown while waiting for work, but a connector can be
            // inside an external publish attempt when terminal teardown begins. Dropping the
            // task at that boundary releases its volatile prepared requests and unresolved ACK
            // guards without reporting them as delivered or waiting for the attempt timeout.
            tokio::select! {
                biased;
                _ = super::emitter_publishing::wait_for_emitter_work_cancel(&mut terminal_shutdown_rx) => {}
                _ = task_loop.run() => {}
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

impl EmitterTaskLoop<'_> {
    async fn run(&mut self) {
        loop {
            tokio::task::consume_budget().await;
            let wake = self.state.wake(self.context);
            let receive_input = self
                .state
                .receives_input(self.buffered_messages.load(Ordering::Acquire));
            let work = match self.interaction.next_with_input(wake, receive_input).await {
                Ok(work) => work,
                // The emitter holds work whose cadence it cannot resolve, because the domain
                // clock is not readable in this generation. There is no wall-clock fallback
                // for a logical cadence, so the work stays buffered and unpublished while the
                // acknowledgements it owns are kept alive on the physical beat.
                Err(RelayInteractionError::WakeTiming { reason, .. }) => {
                    self.context.runtime.record_emitter_transient_error(
                        &self.context.domain,
                        &self.context.emitter,
                        reason,
                    );
                    let acks = self.state.sink.pending_acks(&self.state.buffer);
                    RuntimeReconnectBackoff::wait_duration_with_ack_alive(
                        RETRY_ACK_ALIVE_EACH,
                        &mut *self.shutdown_rx,
                        &acks,
                    )
                    .await;
                    continue;
                }
                Err(error) => {
                    let reason = error.to_string();
                    self.context
                        .report_flush_error(self.plan.sink.label(), &reason);
                    self.context
                        .runtime
                        .handle_internal_processor_error_for_acks(
                            &self.context.domain,
                            ModelKind::Emitter,
                            &self.context.emitter,
                            &self.context.error_policies,
                            error.acks(),
                            reason,
                        );
                    continue;
                }
            };
            let (input_event, mut work) = work.into_parts();
            match input_event {
                RelayInteractionEvent::Command(EmitterTaskCommand::Reconfigure {
                    flush_policy,
                    response,
                }) => {
                    // The new cadence replaces the old one for the batches already buffered,
                    // so a reconfiguration that cannot read the domain clock leaves them
                    // without a deadline and is recorded as the emitter's transient error.
                    if let Err(error) = self.state.buffer.reconfigure(self.context, &flush_policy) {
                        let reason = emitter_error_message(&error);
                        self.context.runtime.record_emitter_transient_error(
                            &self.context.domain,
                            &self.context.emitter,
                            reason.clone(),
                        );
                        self.context
                            .report_flush_error(self.plan.sink.label(), &reason);
                    }
                    response
                        .send(())
                        .means_peer_left("emitter reconfiguration requester");
                }
                RelayInteractionEvent::Command(EmitterTaskCommand::Stop { deadline, response }) => {
                    if response.is_closed() {
                        clear_emitter_stop_signal(self.stop_signal, deadline);
                        continue;
                    }
                    if self.buffered_messages.load(Ordering::Acquire) > 0
                        && let Some(reason) = emitter_unavailable_reason(
                            &self.state.sink,
                            self.fault_injection,
                            &self.context.emitter,
                        )
                    {
                        self.context.runtime.record_emitter_transient_error(
                            &self.context.domain,
                            &self.context.emitter,
                            reason.clone(),
                        );
                        self.context
                            .report_flush_error(self.plan.sink.label(), &reason);
                        clear_emitter_stop_signal(self.stop_signal, deadline);
                        response
                            .send(Err(Report::new(EmitterRuntimeError::FinalFlush)
                                .attach_printable(format!(
                                    "emitter final flush failed: {reason}"
                                ))))
                            .means_peer_left("emitter stop requester");
                        continue;
                    }
                    let EmitterTaskState {
                        sink,
                        buffer,
                        backoff,
                        ..
                    } = &mut self.state;
                    let mut control = EmitterPublishControl {
                        fault_injection: self.fault_injection,
                        shutdown_rx: &mut *self.shutdown_rx,
                        stop_rx: &mut *self.stop_rx,
                        backoff,
                    };
                    let drained = tokio::time::timeout_at(deadline, async {
                        let report = sink
                            .flush_all(self.plan.sink.label(), self.context, &mut control, buffer)
                            .await?;
                        sink.finish_transport(deadline).await?;
                        Ok::<_, Report<EmitterRuntimeError>>(report)
                    })
                    .await;
                    let result = match drained {
                        Ok(Ok(report)) => {
                            self.state.backoff.reset();
                            self.state.retry.clear();
                            self.context.runtime.clear_emitter_transient_error(
                                &self.context.domain,
                                &self.context.emitter,
                            );
                            if let Some(report) = report.as_ref() {
                                self.batch_context.observe_sent(report);
                            }
                            Ok(())
                        }
                        Ok(Err(error)) => {
                            let reason = emitter_error_message(&error);
                            self.context.runtime.record_emitter_transient_error(
                                &self.context.domain,
                                &self.context.emitter,
                                reason.clone(),
                            );
                            self.context
                                .report_flush_error(self.plan.sink.label(), &reason);
                            Err(Report::new(EmitterRuntimeError::FinalFlush)
                                .attach_printable(format!("emitter final flush failed: {reason}")))
                        }
                        Err(_) => {
                            let reason = format!(
                                "emitter '{}' did not drain before its configured deadline",
                                self.context.emitter.as_str()
                            );
                            self.context
                                .report_flush_error(self.plan.sink.label(), &reason);
                            Err(Report::new(EmitterRuntimeError::StopDeadlineElapsed)
                                .attach_printable(reason))
                        }
                    };
                    let should_stop = result.is_ok();
                    if !should_stop {
                        clear_emitter_stop_signal(self.stop_signal, deadline);
                    }
                    if response.send(result).is_ok() && should_stop {
                        break;
                    }
                    if should_stop {
                        clear_emitter_stop_signal(self.stop_signal, deadline);
                    }
                }
                RelayInteractionEvent::ForceFlush(completion) => {
                    if self.buffered_messages.load(Ordering::Acquire) > 0
                        && let Some(reason) = emitter_unavailable_reason(
                            &self.state.sink,
                            self.fault_injection,
                            &self.context.emitter,
                        )
                    {
                        self.state
                            .retry
                            .include_acks(self.state.sink.pending_acks(&self.state.buffer));
                        if self.state.retry.is_active() {
                            self.context
                                .runtime
                                .record_emitter_transient_error_with_backoff(
                                    &self.context.domain,
                                    &self.context.emitter,
                                    reason.clone(),
                                    self.state.backoff.next_delay(),
                                );
                        } else {
                            self.state.retry.defer(
                                self.context,
                                EmitterRetryDeferral {
                                    wait: self.state.backoff.take_next_delay(),
                                    acks: self.state.sink.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: self
                                        .fault_injection
                                        .emitter_should_stall(&self.context.emitter),
                                    reason: Some(&reason),
                                },
                            );
                        }
                        self.context
                            .report_flush_error(self.plan.sink.label(), &reason);
                        completion.complete();
                        continue;
                    }
                    let publish_result = {
                        let EmitterTaskState {
                            sink,
                            buffer,
                            backoff,
                            ..
                        } = &mut self.state;
                        let mut control = EmitterPublishControl {
                            fault_injection: self.fault_injection,
                            shutdown_rx: &mut *self.shutdown_rx,
                            stop_rx: &mut *self.stop_rx,
                            backoff,
                        };
                        sink.flush_all(self.plan.sink.label(), self.context, &mut control, buffer)
                            .await
                            .map_err(EmitterPublishFailure::buffer)
                    };
                    let mut pending_batch = None;
                    self.state
                        .handle_publish_result(
                            publish_result,
                            &mut pending_batch,
                            self.context,
                            &self.batch_context,
                            EmitterPublishOutcomeContext {
                                sink_label: self.plan.sink.label(),
                                codec_route: self.codec.is_some(),
                                error_report: EmitterPublishErrorReport::Flush,
                            },
                        )
                        .await;
                    completion.complete();
                }
                RelayInteractionEvent::Stopped(reason) => {
                    debug!(
                        domain = self.context.domain.as_str(),
                        emitter = self.context.emitter.as_str(),
                        ?reason,
                        "emitter relay interaction stopped"
                    );
                    if self.buffered_messages.load(Ordering::Acquire) > 0
                        && let Some(reason) = emitter_unavailable_reason(
                            &self.state.sink,
                            self.fault_injection,
                            &self.context.emitter,
                        )
                    {
                        self.context.runtime.record_emitter_transient_error(
                            &self.context.domain,
                            &self.context.emitter,
                            reason.clone(),
                        );
                        self.context
                            .report_flush_error(self.plan.sink.label(), &reason);
                        let pending = self.state.buffer.drain_pending();
                        self.batch_context
                            .handle_publish_error_batches(
                                pending,
                                reason,
                                MessageErrorOperation::Publish,
                            )
                            .await;
                        break;
                    }
                    let flush_result = {
                        let EmitterTaskState {
                            sink,
                            buffer,
                            backoff,
                            ..
                        } = &mut self.state;
                        let mut control = EmitterPublishControl {
                            fault_injection: self.fault_injection,
                            shutdown_rx: &mut *self.shutdown_rx,
                            stop_rx: &mut *self.stop_rx,
                            backoff,
                        };
                        sink.flush_all(self.plan.sink.label(), self.context, &mut control, buffer)
                            .await
                    };
                    match flush_result {
                        Ok(Some(report)) => self.batch_context.observe_sent(&report),
                        Ok(None) => {}
                        Err(error) => {
                            let reason = emitter_error_message(&error);
                            self.context.runtime.record_emitter_transient_error(
                                &self.context.domain,
                                &self.context.emitter,
                                reason.clone(),
                            );
                            self.context
                                .report_flush_error(self.plan.sink.label(), &reason);
                            let pending = self.state.buffer.drain_pending();
                            let operation =
                                emitter_message_error_operation(&error, self.codec.is_some());
                            self.batch_context
                                .handle_publish_error_batches(pending, reason, operation)
                                .await;
                        }
                    }
                    break;
                }
                RelayInteractionEvent::Wake => {
                    let retry_was_active = self.state.retry.is_active();
                    let retry_is_due = self.state.retry.retry_is_due();
                    let stall_cleared = self.state.retry.release_if_stall_cleared(
                        self.fault_injection
                            .emitter_should_stall(&self.context.emitter),
                    );
                    if !retry_is_due && !stall_cleared {
                        continue;
                    }
                    let retry_attempt = retry_was_active && (retry_is_due || stall_cleared);
                    if self.state.reconnect_on_wake
                        || self.state.sink.unavailable_reason().is_some()
                    {
                        self.state.sink = EmitterSinkState::open_until_cancelled(
                            self.plan,
                            self.context,
                            self.input_schema,
                            self.codec,
                            &mut *self.work_cancel_rx,
                        )
                        .await;
                        self.state
                            .buffer
                            .report_staged_messages(self.state.sink.staged_messages());
                        if let Some(reason) = self.state.sink.unavailable_reason() {
                            self.state.retry.defer(
                                self.context,
                                EmitterRetryDeferral {
                                    wait: self.state.backoff.take_next_delay(),
                                    acks: self.state.sink.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: false,
                                    reason: Some(reason),
                                },
                            );
                            self.state.reconnect_on_wake = true;
                            continue;
                        }
                        self.state.reconnect_on_wake = false;
                        self.context.runtime.clear_emitter_transient_error(
                            &self.context.domain,
                            &self.context.emitter,
                        );
                    }
                    let publish_result = {
                        let EmitterTaskState {
                            sink,
                            buffer,
                            backoff,
                            ..
                        } = &mut self.state;
                        let mut control = EmitterPublishControl {
                            fault_injection: self.fault_injection,
                            shutdown_rx: &mut *self.shutdown_rx,
                            stop_rx: &mut *self.stop_rx,
                            backoff,
                        };
                        sink.flush_due(
                            self.plan.sink.label(),
                            self.context,
                            &mut control,
                            buffer,
                            retry_attempt,
                        )
                        .await
                        .map_err(EmitterPublishFailure::buffer)
                    };
                    let mut pending_batch = None;
                    self.state
                        .handle_publish_result(
                            publish_result,
                            &mut pending_batch,
                            self.context,
                            &self.batch_context,
                            EmitterPublishOutcomeContext {
                                sink_label: self.plan.sink.label(),
                                codec_route: self.codec.is_some(),
                                error_report: EmitterPublishErrorReport::Flush,
                            },
                        )
                        .await;
                }
                RelayInteractionEvent::Batch {
                    relay: input_relay,
                    batch,
                } => {
                    let input_metrics = self
                        .input_metrics
                        .get(&input_relay)
                        .verified("the task resolves metrics for every declared emitter input");
                    input_metrics.observe_delivery(&batch.delivery_observation(actual_utc_now()));
                    self.context.runtime.mark_branch_aggregated_metrics_updated(
                        &self.context.domain,
                        ModelKind::Emitter,
                        &self.context.emitter,
                    );
                    let wait_for_required_state = !self.interaction.is_terminal_drain();
                    let publish_batch = match self
                        .batch_context
                        .process(
                            &input_relay,
                            batch,
                            &mut *self.work_cancel_rx,
                            wait_for_required_state,
                            work.as_mut(),
                        )
                        .await
                    {
                        Some(batch) => batch,
                        None => continue,
                    };

                    if self.interaction.is_draining() {
                        if let Err(error) = self
                            .state
                            .buffer
                            .retain_without_cadence(publish_batch.clone())
                        {
                            let reason = emitter_error_message(&error);
                            let operation =
                                emitter_message_error_operation(&error, self.codec.is_some());
                            self.batch_context
                                .handle_publish_error_batch(publish_batch, reason, operation)
                                .await;
                        } else if !self.interaction.is_terminal_drain() {
                            self.state.retry.include_acks(publish_batch.merged_acks());
                        }
                        continue;
                    }

                    if self.state.retry.is_active()
                        || emitter_unavailable_reason(
                            &self.state.sink,
                            self.fault_injection,
                            &self.context.emitter,
                        )
                        .is_some()
                    {
                        let unavailable = emitter_unavailable_reason(
                            &self.state.sink,
                            self.fault_injection,
                            &self.context.emitter,
                        );
                        if let Err(error) =
                            self.state.buffer.push(self.context, publish_batch.clone())
                        {
                            let reason = emitter_error_message(&error);
                            let operation =
                                emitter_message_error_operation(&error, self.codec.is_some());
                            self.batch_context
                                .handle_publish_error_batch(publish_batch, reason, operation)
                                .await;
                            continue;
                        }
                        self.state.retry.include_acks(publish_batch.merged_acks());
                        if !self.state.retry.is_active() {
                            self.state.retry.defer(
                                self.context,
                                EmitterRetryDeferral {
                                    wait: self.state.backoff.take_next_delay(),
                                    acks: self.state.sink.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: self
                                        .fault_injection
                                        .emitter_should_stall(&self.context.emitter),
                                    reason: unavailable.as_deref(),
                                },
                            );
                            if let Some(reason) = unavailable.as_deref() {
                                self.context
                                    .report_publish_error(self.plan.sink.label(), reason);
                            }
                        }
                        self.state.reconnect_on_wake |=
                            self.state.sink.unavailable_reason().is_some();
                        continue;
                    }

                    let mut pending_batch = Some(publish_batch);
                    let publish_result = {
                        let EmitterTaskState {
                            sink,
                            buffer,
                            backoff,
                            ..
                        } = &mut self.state;
                        let mut control = EmitterPublishControl {
                            fault_injection: self.fault_injection,
                            shutdown_rx: &mut *self.shutdown_rx,
                            stop_rx: &mut *self.stop_rx,
                            backoff,
                        };
                        sink.publish_batch(
                            self.context,
                            &mut control,
                            buffer,
                            pending_batch
                                .as_ref()
                                .verified("this branch only runs while a batch is pending")
                                .clone(),
                        )
                        .await
                    };
                    self.state
                        .handle_publish_result(
                            publish_result,
                            &mut pending_batch,
                            self.context,
                            &self.batch_context,
                            EmitterPublishOutcomeContext {
                                sink_label: self.plan.sink.label(),
                                codec_route: self.codec.is_some(),
                                error_report: EmitterPublishErrorReport::Publish,
                            },
                        )
                        .await;
                }
            }
        }
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
        // The rows an earlier attempt delivered are sent, and this is the last time the emitter
        // holds them, so they are counted before the rest follow the error policy.
        if let Some(report) = batch.delivered_report() {
            self.observe_sent(&report);
        }
        let execution_now = batch.execution_now();
        let resolved = batch.resolved_rows();
        let messages = match batch.into_relay_batch().try_into_messages() {
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
            if resolved.get(row).copied().unwrap_or(false) {
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
                        execution_now,
                        MessageErrorCode::External,
                        reason.clone(),
                        operation,
                        None,
                        std::iter::empty(),
                    ),
                    partial_output: None,
                    materialized_state: HashMap::default(),
                    ingest_metadata: None,
                    execution_now,
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

        // Ordering groups are evaluated over the filtered source batch and stay indexed by source
        // row, so the filter map below can hand every published row the group its own input
        // produced. A row that cannot produce one keeps its reason and is rejected at the send;
        // only a failure of the whole evaluation drops the batch here.
        let ordering_groups = match self.ordering_group {
            None => None,
            Some(ordering_group) => {
                let evaluated = ordering_group
                    .evaluate(self.emitter, &batch, execution_now, &materialized_values)
                    .await;
                match evaluated {
                    Ok(groups) => Some(groups),
                    Err(error) => {
                        let error = error.current_context();
                        self.report_general_error(error.acks.iter(), error.reason.clone());
                        return None;
                    }
                }
            }
        };

        let Some(filter_map) = self.filter_map else {
            if let Some(requests) = self.http_requests {
                // Without a route an HTTP emitter sends no body, so every source record is one
                // request.
                return self
                    .prepare_http_requests(
                        requests,
                        input_relay,
                        batch,
                        HttpRequestInput::Published,
                        &materialized_values,
                        execution_now,
                    )
                    .await;
            }
            // Without a filter map every source row publishes as it arrived, so the groups
            // already align with the batch row for row.
            let publish_batch =
                EmitterPublishBatch::from_input(input_relay.clone(), batch, execution_now);
            return self.with_ordering_groups(publish_batch, ordering_groups);
        };

        // The request fields of an HTTP emitter with a codec body read the original source record
        // of every finalized record its route keeps, and the route consumes the source batch.
        let source_records = match (self.http_requests, filter_map.codec_route) {
            (Some(_), true) => Some(SourceRecords::of(&batch)),
            (Some(_), false) | (None, _) => None,
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

        self.deliver_planned_message_errors(plan.message_errors)
            .await;

        // A plan that kept no row has no batch to publish, and the rows it rejected have just
        // been reported.
        let batch = plan.batch?;
        if let Some(requests) = self.http_requests {
            let input = match source_records {
                Some(records) => HttpRequestInput::Source {
                    records,
                    rows: plan.source_rows,
                },
                // A route without a codec only filters, so the rows it keeps are source records.
                None => HttpRequestInput::Published,
            };
            return self
                .prepare_http_requests(
                    requests,
                    input_relay,
                    batch,
                    input,
                    &materialized_values,
                    execution_now,
                )
                .await;
        }
        let publish_batch =
            match EmitterPublishBatch::new(input_relay.clone(), batch, plan.headers, execution_now)
            {
                Ok(publish_batch) => publish_batch,
                Err(error) => {
                    self.report_publish_batch_error(error);
                    return None;
                }
            };

        // The plan reports the source row of every output row in output order, so selecting the
        // source groups through it keeps each published row with the group its own input
        // produced.
        let selected_groups = match ordering_groups {
            None => None,
            Some(groups) => match groups.select(&plan.source_rows) {
                Ok(selected) => Some(selected),
                Err(error) => {
                    self.report_publish_batch_error(error);
                    return None;
                }
            },
        };
        self.with_ordering_groups(publish_batch, selected_groups)
    }

    /// Evaluates the request fields of every row `batch` publishes, delivers the message error of
    /// each row whose request cannot be sent, and buffers the rest with their requests.
    async fn prepare_http_requests(
        &self,
        requests: &CompiledHttpRequestFields,
        input_relay: &RelayName,
        batch: RelayRecordBatch,
        input: HttpRequestInput,
        materialized_values: &HashMap<String, RuntimeValue>,
        execution_now: Timestamp,
    ) -> Option<EmitterPublishBatch> {
        let prepared = requests
            .prepare(
                self.emitter,
                batch,
                input,
                materialized_values,
                execution_now,
            )
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let failure = error.current_context();
                self.report_general_error(failure.acks.iter(), failure.reason.clone());
                return None;
            }
        };
        self.deliver_planned_message_errors(prepared.message_errors)
            .await;
        let AcceptedHttpRequests { batch, requests } = prepared.accepted?;
        let publish_batch =
            match EmitterPublishBatch::new(input_relay.clone(), batch, None, execution_now) {
                Ok(publish_batch) => publish_batch,
                Err(error) => {
                    self.report_publish_batch_error(error);
                    return None;
                }
            };
        match publish_batch.with_http_requests(requests) {
            Ok(publish_batch) => Some(publish_batch),
            Err(error) => {
                self.report_publish_batch_error(error);
                None
            }
        }
    }

    /// `publish_batch` with the ordering group of each of its rows, when the emitter declares one.
    fn with_ordering_groups(
        &self,
        publish_batch: EmitterPublishBatch,
        groups: Option<OrderingGroups>,
    ) -> Option<EmitterPublishBatch> {
        let Some(groups) = groups else {
            return Some(publish_batch);
        };
        match publish_batch.with_ordering_groups(groups) {
            Ok(publish_batch) => Some(publish_batch),
            Err(error) => {
                self.report_publish_batch_error(error);
                None
            }
        }
    }

    /// Report a batch whose rows, headers, and ordering groups stopped agreeing. They are checked
    /// while building the same batch, so any one of them failing is the same failure to report.
    fn report_publish_batch_error(&self, error: Report<EmitterRuntimeError>) {
        self.report_general_error(
            std::iter::empty::<&AckSet>(),
            format!(
                "emitter '{}' failed to build publish batch: {error:#}",
                self.emitter.as_str(),
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
    use crate::runtime::test_fixtures::{input_batch, sink_context};

    fn task_state(context: &EmitterSinkContext) -> EmitterTaskState {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Immediate);
        EmitterTaskState::new(
            EmitterSinkState::Unavailable {
                reason: "test sink is unavailable".to_string(),
            },
            buffer,
            RuntimeReconnectBackoff::default(),
            context,
        )
    }

    fn output_metrics(context: &EmitterSinkContext) -> EmitterOutputMetrics {
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
        context: &'a EmitterSinkContext,
        routing: &'a mut DomainRoutingCache,
        output_metrics: &'a EmitterOutputMetrics,
        node: &'a ModelName,
        source_filters: &'a HashMap<RelayName, CompiledProgramWithMaterializedInterest>,
        materialized_state: &'a [nervix_models::MaterializedStateDependency],
    ) -> EmitterBatchContext<'a> {
        EmitterBatchContext {
            runtime: &context.runtime,
            routing,
            domain_clock: &context.clock,
            domain: &context.domain,
            emitter: &context.emitter,
            node,
            output_metrics,
            error_policies: &context.error_policies,
            source_filters,
            filter_map: None,
            ordering_group: None,
            http_requests: None,
            materialized_state,
        }
    }

    #[test]
    fn sink_context_reports_missing_node_dns_as_initialization_failure() {
        let context = sink_context();
        let error = context
            .dns()
            .err()
            .assured("the fixture runtime has no DNS resolver");
        assert!(matches!(
            error.current_context(),
            EmitterRuntimeError::InitializeSink
        ));
        assert_eq!(
            emitter_error_message(&error),
            "the node DNS resolver is not installed"
        );
    }

    #[test]
    fn emitter_error_classification_is_explicit_for_every_context() {
        for retryable in [
            EmitterRuntimeError::SinkNotInitialized,
            EmitterRuntimeError::PublishBatch,
            EmitterRuntimeError::PublishStalled,
            EmitterRuntimeError::UnansweredSinkRecords {
                unanswered: 1,
                records: 2,
            },
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
            EmitterRuntimeError::PreparedRowOutOfBounds {
                row: 1,
                row_count: 1,
            },
            EmitterRuntimeError::RowAlreadyPrepared { row: 0 },
            EmitterRuntimeError::UnknownSinkRecord {
                record: 2,
                records: 2,
            },
            EmitterRuntimeError::SinkRecordAnsweredTwice { record: 0 },
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
    async fn publish_success_clears_retry_state_and_releases_the_caller_batch() {
        let context = sink_context();
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
                    sink_label: "test",
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
                .emitter_transient_error(&context.domain, &context.emitter),
            None
        );
        let mut fresh_backoff = RuntimeReconnectBackoff::default();
        assert_eq!(
            state.backoff.take_next_delay(),
            fresh_backoff.take_next_delay(),
            "success must reset the declared retry sequence"
        );
    }

    #[tokio::test]
    async fn retryable_caller_failure_retains_the_batch_and_defers_one_retry() {
        let context = sink_context();
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
                .attach_printable("test sink rejected the attempt"),
        );

        state
            .handle_publish_result(
                Err(failure),
                &mut pending_batch,
                &context,
                &batch_context,
                EmitterPublishOutcomeContext {
                    sink_label: "test",
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
                .emitter_transient_error(&context.domain, &context.emitter),
            Some("test sink rejected the attempt".to_string())
        );
    }

    #[tokio::test]
    async fn terminal_buffer_failure_drains_and_routes_every_owned_batch() {
        let context = sink_context();
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
                    sink_label: "test",
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
                .emitter_transient_error(&context.domain, &context.emitter),
            Some("test encoding failed".to_string())
        );
    }

    #[tokio::test]
    async fn a_terminal_failure_counts_the_rows_delivered_before_it_as_sent() {
        let context = sink_context();
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
        let two_rows = RelayRecordBatch::from_messages(
            crate::runtime::test_fixtures::input_schema(),
            messages,
        )
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
                    sink_label: "test",
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
