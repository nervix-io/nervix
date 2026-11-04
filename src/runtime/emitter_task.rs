//! One emitter's task: the loop that receives its input and decides when it publishes.
//!
//! Layer: data plane.
//! - **Owns.** Spawning an emitter task from its execution plan, the loop that handles its commands,
//!   force flushes, wakes and input batches, resolving each input batch's materialized state,
//!   filters, ordering groups and HTTP request fields, the host context its connector reports
//!   through, and the emitter's failure semantics.
//! - **Depends on.** The emitter's execution and emitter start plans and compiled programs, the relay interaction that
//!   delivers its input, its buffer, retry schedule and publishing, and the node's metrics, events
//!   and error policies.
//! - **Must not know.** Which emitter crate the emitter publishes through, how its records are
//!   encoded or mapped, or how the registry validated it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "emitter task installation binds retained handles before executing its recurring \
                  body"
    )
)]

use error_stack::{AttachmentKind, FrameKind, ResultExt as _};
use nervix_connector::{
    EmitterBoundedExecution, EmitterEventReporter, EmitterFinish, EmitterFinishError,
    EmitterGeneralErrorHandler, EmitterHost, EmitterRecordAcknowledgementServices,
    EmitterRecordAcknowledgements, EmitterStagingDirectory, EmitterTransientErrorStatus,
    physical_time::actual_utc_now,
};

use super::{emitter_publishing::EmitterStopDrainOutcome, *};

pub(in crate::runtime) struct EmitterTask;

/// What an emitter connector needs to publish one emitter's output. The staging directory and the
/// event bus are read through `runtime` rather than copied in beside it, so the context carries
/// one handle to node state instead of a second view of the same values.
#[derive(Clone)]
pub(in crate::runtime) struct EmitterConnectorContext {
    /// Schedule application also owns and replaces this routing publication.
    pub(super) routing: SharedDomainRouting,
    /// Metric replication independently retains the marked state.
    pub(super) metrics_dirty: BranchMetricsMark,
    /// The status registry retains this owner for DESCRIBE and drain observations.
    pub(super) status: Arc<task_status::TaskStatus<EmitterRetryStatus>>,
    /// Drain observers retain the registered counter independently of this context.
    pub(super) confirmation_waits: Arc<AtomicUsize>,
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
    error_routing: &'a SharedDomainRouting,
    metrics_dirty: &'a BranchMetricsMark,
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
    /// The envelope the emitter evaluates for every record its route keeps, absent where it writes
    /// none.
    envelope: Option<&'a CompiledEnvelope>,
    /// The request fields an HTTP emitter evaluates for every record its route keeps, absent for
    /// every other emitter.
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
    #[error("invalid emitter connector configuration")]
    InvalidConnectorConfig,
    #[error("failed to initialize emitter connector")]
    InitializeConnector,
    #[error("emitter connector client is not initialized")]
    ConnectorNotInitialized,
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
    #[error("emitter connector answered for record {record} of a write with {records} records")]
    UnknownEmitterRecord { record: usize, records: usize },
    #[error("emitter connector answered twice for record {record}")]
    EmitterRecordAnsweredTwice { record: usize },
    #[error("emitter connector left {unanswered} of {records} records unanswered")]
    UnansweredEmitterRecords { unanswered: usize, records: usize },
    #[error("emitter connector broke its preparation of batch {batch_index}: {violation}")]
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
    #[error("the emitter could not finish its emitter's transport client")]
    FinishTransport,
    #[error("failed to encode emitter batch")]
    EncodeBatch,
    #[error("the node's bounded execution did not take the emitter's encoding")]
    EncodingRefused,
    #[error("failed to publish emitter batch")]
    PublishBatch,
    #[error("emitter publish is stalled")]
    PublishStalled,
    #[error("failed to build the publish batch")]
    BuildPublishBatch,
    /// `publish_failure` is the emitter's description of the publish failure, which its message
    /// error policy would otherwise have reported for each message of the batch.
    #[error("failed to split a batch whose publish failed ({publish_failure}) into its messages")]
    SplitFailedBatch { publish_failure: String },
    #[error("the source filter of input relay '{relay}' failed")]
    SourceFilter { relay: RelayName },
}

impl EmitterRuntimeError {
    pub(super) fn is_retryable_publish_failure(&self) -> bool {
        match self {
            Self::ConnectorNotInitialized
            | Self::PublishBatch
            | Self::PublishStalled
            | Self::EncodingRefused
            | Self::UnansweredEmitterRecords { .. } => true,
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
            | Self::UnknownEmitterRecord { .. }
            | Self::EmitterRecordAnsweredTwice { .. }
            | Self::RowPreparation { .. }
            | Self::InvalidConnectorConfig
            | Self::InitializeConnector
            | Self::FaultInjected
            | Self::ShutdownWhileStalled
            | Self::StopDeadlineElapsed
            | Self::FinalFlush
            | Self::FinishTransport
            | Self::FlushTiming
            | Self::RetryTiming
            | Self::EncodeBatch
            | Self::BuildPublishBatch
            | Self::SplitFailedBatch { .. }
            | Self::SourceFilter { .. } => false,
        }
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the connector failure is formatted through its external Display contract"
    )
)]
pub(super) fn emitter_report(
    context: EmitterRuntimeError,
    error: impl std::fmt::Display,
) -> Report<EmitterRuntimeError> {
    Report::new(context).attach_printable(error.to_string())
}

pub(super) fn emitter_init_error(error: impl std::fmt::Display) -> Report<EmitterRuntimeError> {
    emitter_report(EmitterRuntimeError::InitializeConnector, error)
}

impl EmitterConnectorContext {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the connector failure becomes a retained status string \
                                   through its external Into contract")
    )]
    pub(super) fn record_failure(&self, error: impl Into<String>) {
        self.status.record_error(error.into());
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the connector retry reason becomes a retained status string through its \
                      external Into contract"
        )
    )]
    pub(super) fn record_retry(
        &self,
        error: impl Into<String>,
        backoff: Duration,
        kind: EmitterRetryKind,
    ) {
        self.status.fail(
            error.into(),
            Some(EmitterRetryStatus {
                kind,
                reconnect: RuntimeReconnectStatus {
                    backoff,
                    retry_at: Instant::now() + backoff,
                },
            }),
        );
    }

    pub(super) fn dns(&self) -> Result<DnsResolver, Report<EmitterRuntimeError>> {
        let Some(dns) = self.runtime.dns() else {
            return Err(Report::new(EmitterRuntimeError::InitializeConnector)
                .attach_printable("the node DNS resolver is not installed"));
        };
        Ok(dns.clone())
    }

    pub(super) fn emitter_host(&self) -> EmitterHost {
        EmitterHost::new(self.clone())
    }

    pub(super) fn report_init_error(&self, connector: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to initialize {connector} emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to initialize emitter connector"
        );
    }

    fn report_publish_error(&self, connector: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to publish {connector} message for emitter '{}' in domain '{}': {error}",
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

    pub(super) fn report_flush_error(&self, connector: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to flush {connector} rows for emitter '{}' in domain '{}': {error}",
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

    /// Records what finishing the emitter's transport client did when the emitter stopped.
    fn record_finished(&self, connector: &str, ending: EmitterEnding, finished: EmitterFinish) {
        match finished {
            EmitterFinish::NothingHeld => debug!(
                domain = self.domain.as_str(),
                emitter = self.emitter.as_str(),
                connector,
                ending = ending.as_ref(),
                "emitter's transport client held no records when it stopped"
            ),
            EmitterFinish::Flushed => info!(
                domain = self.domain.as_str(),
                emitter = self.emitter.as_str(),
                connector,
                ending = ending.as_ref(),
                "emitter sent the records its transport client held"
            ),
        }
    }

    /// Records a finish that did not send every record the emitter's transport client held, which
    /// are lost with the client: as the emitter's transient error, an error event and a warning.
    fn record_unfinished(
        &self,
        connector: &str,
        ending: EmitterEnding,
        error: &Report<EmitterFinishError>,
    ) {
        let reason = format!("{error:#}");
        self.record_failure(reason.clone());
        self.runtime.events().report_error(format!(
            "emitter '{}' in domain '{}' stopped before its {connector} transport client sent \
             every record it held: {reason}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            connector,
            ending = ending.as_ref(),
            error = reason,
            "emitter stopped before its transport client sent every record it held"
        );
    }

    /// Reads the emitter's domain execution time for a cadence decision.
    pub(super) fn execution_snapshot(&self) -> EmitterRuntimeResult<DomainExecutionSnapshot> {
        self.clock
            .snapshot()
            .change_context(EmitterRuntimeError::FlushTiming)
    }

    pub(super) fn parse_flush_policy(&self, policy: &FlushPolicy) -> Option<RuntimeFlushPolicy> {
        match Runtime::parse_runtime_node_flush_policy(policy) {
            Ok(policy) => Some(policy),
            Err(error) => {
                self.runtime.events().report_error(format!(
                    "emitter '{}' in domain '{}' has an invalid flush policy: {error:#}",
                    self.emitter.as_str(),
                    self.domain.as_str()
                ));
                warn!(
                    domain = self.domain.as_str(),
                    emitter = self.emitter.as_str(),
                    error = format!("{error:#}"),
                    "failed to parse emitter flush policy"
                );
                None
            }
        }
    }
}

impl EmitterRecordAcknowledgementServices for AckSet {
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

impl EmitterTransientErrorStatus for EmitterConnectorContext {
    fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.status.fail_connector(
            reason,
            Some(EmitterRetryStatus {
                kind: EmitterRetryKind::Infrastructure,
                reconnect: RuntimeReconnectStatus {
                    backoff: retry_after,
                    retry_at: Instant::now() + retry_after,
                },
            }),
        );
    }

    fn clear_transient_error(&self) {
        self.status.clear_connector();
    }
}

impl EmitterEventReporter for EmitterConnectorContext {
    fn report_error(&self, message: String) {
        self.runtime.events().report_error(format!(
            "emitter error for emitter '{}' in domain '{}': {message}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
    }
}

impl EmitterStagingDirectory for EmitterConnectorContext {
    fn staging_directory(&self) -> PathBuf {
        self.runtime.temp_dir().to_path_buf()
    }
}

impl EmitterBoundedExecution for EmitterConnectorContext {
    fn executor(&self) -> Executor {
        self.runtime.executor().clone()
    }
}

impl EmitterGeneralErrorHandler for EmitterConnectorContext {
    fn handle_general_error(&self, acks: &EmitterRecordAcknowledgements, reason: String) {
        match self.error_policies.general {
            GeneralErrorPolicy::Ignore => acks.acknowledge(),
            GeneralErrorPolicy::Log => {
                let report = format!(
                    "emitter '{}' general error in domain '{}': {}",
                    self.emitter.as_str(),
                    self.domain.as_str(),
                    reason
                );
                self.runtime.events().report_error(report.clone());
                warn!(
                    domain = self.domain.as_str(),
                    emitter = self.emitter.as_str(),
                    reason = %reason,
                    "runtime node handled general error"
                );
                acks.reject(report);
            }
        }
    }
}

/// How an emitter describes `error`: by the first printable attachment in its chain, which carries
/// the connector's or task's own description, or else by the error's outermost context.
#[cfg_attr(
    nervix_lint,
    nervix::dispatch(reason = "the failure is formatted through its context's Display contract")
)]
pub(super) fn emitter_error_message<C: error_stack::Context>(error: &Report<C>) -> String {
    emitter_attached_message(error).unwrap_or_else(|| error.current_context().to_string())
}

/// The first printable attachment of `error`, which is how an emitter describes its own failure.
pub(super) fn emitter_attached_message<C: error_stack::Context>(
    error: &Report<C>,
) -> Option<String> {
    error.frames().find_map(|frame| match frame.kind() {
        FrameKind::Attachment(AttachmentKind::Printable(attachment)) => {
            Some(attachment.to_string())
        }
        FrameKind::Context(_) | FrameKind::Attachment(_) => None,
    })
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
    connector: EmitterConnectorState,
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

/// How an emitter task's loop ended, which decides who finishes its emitter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum EmitterEnding {
    /// A stop drained the emitter and finished its emitter before the loop returned.
    Stop,
    /// The loop's relay interaction stopped without a stop, so the loop drained its buffer and
    /// returned with the emitter unfinished.
    InteractionStopped,
    /// Terminal teardown ended the loop wherever it was, possibly in the middle of a publish.
    Teardown,
}

#[derive(Clone, Copy)]
struct EmitterPublishOutcomeContext<'a> {
    emitter_label: &'a str,
    codec_route: bool,
    error_report: EmitterPublishErrorReport,
}

impl EmitterPublishOutcomeContext<'_> {
    fn report_error(self, context: &EmitterConnectorContext, reason: &str) {
        match self.error_report {
            EmitterPublishErrorReport::Flush => {
                context.report_flush_error(self.emitter_label, reason)
            }
            EmitterPublishErrorReport::Publish => {
                context.report_publish_error(self.emitter_label, reason)
            }
        }
    }
}

impl EmitterTaskState {
    fn new(
        connector: EmitterConnectorState,
        buffer: EmitterBatchBuffer,
        mut backoff: RuntimeReconnectBackoff,
        context: &EmitterConnectorContext,
    ) -> Self {
        let reconnect_on_wake = connector.unavailable_reason().is_some();
        let mut retry = EmitterRetrySchedule::default();
        if let Some(reason) = connector.unavailable_reason() {
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
            context.status.clear();
        }
        Self {
            connector,
            buffer,
            retry,
            backoff,
            reconnect_on_wake,
        }
    }

    fn wake(&self, context: &EmitterConnectorContext) -> RuntimeWake {
        self.retry
            .wake(self.connector.cadence_wake(&context.clock, &self.buffer))
    }

    fn receives_input(&self, buffered_messages: usize) -> bool {
        !self.retry.is_active() || buffered_messages == 0
    }

    /// Finishes the emitter after the loop ended without a stop, by the deadline on the task's stop
    /// signal, and records what that did.
    ///
    /// A stopping domain execution gives every emitter that deadline before its teardown ends the
    /// task. A task ended without one, such as a task whose execution was dropped without
    /// stopping, has nothing to bound a finish by, so its emitter closes unfinished.
    async fn finish_after_loop(
        &mut self,
        context: &EmitterConnectorContext,
        connector: &str,
        ending: EmitterEnding,
        deadline: Option<Instant>,
    ) {
        let Some(deadline) = deadline else {
            debug!(
                domain = context.domain.as_str(),
                emitter = context.emitter.as_str(),
                connector,
                ending = ending.as_ref(),
                "emitter task ended without a stop deadline, so its emitter is not finished"
            );
            return;
        };
        match self.connector.finish_transport(deadline).await {
            Ok(finished) => context.record_finished(connector, ending, finished),
            Err(error) => context.record_unfinished(connector, ending, &error),
        }
    }

    async fn handle_publish_result(
        &mut self,
        result: EmitterPublishResult,
        pending_batch: &mut Option<EmitterPublishBatch>,
        context: &EmitterConnectorContext,
        batch_context: &EmitterBatchContext<'_>,
        outcome_context: EmitterPublishOutcomeContext<'_>,
    ) {
        match result {
            Ok(report) => {
                self.backoff.reset();
                self.retry.clear();
                context.status.clear();
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
                        acks: self.connector.pending_acks(&self.buffer),
                        waiting_for_stall_clear: error.current_context()
                            == &EmitterRuntimeError::PublishStalled,
                        reason: Some(&reason),
                    },
                );
                self.reconnect_on_wake = self.connector.reconnect_after(&error);
                outcome_context.report_error(context, &reason);
            }
            Err(failure) => {
                self.retry.clear();
                let (error, failed_batches) =
                    failure.drain_failed_batches(pending_batch, &mut self.buffer);
                let reason = emitter_error_message(&error);
                context.record_failure(reason.clone());
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
    context: &'a EmitterConnectorContext,
    batch_context: EmitterBatchContext<'a>,
    state: EmitterTaskState,
    interaction: RelayInteraction<EmitterTaskCommand>,
    plan: &'a EmitterStartPlan,
    connector_label: String,
    input_schema: &'a CompiledSchema,
    output_schema: &'a Arc<CompiledSchema>,
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
    ) -> error_stack::Result<ScheduledEmitterTask, EmitterStartError> {
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
        let codec = match emitter.codec.as_ref() {
            Some(codec_name) => {
                let Some(codec) = codecs.get(codec_name).cloned() else {
                    return Err(Report::new(EmitterStartError::MissingCodec {
                        codec: codec_name.clone(),
                    }));
                };
                Some(codec)
            }
            None => None,
        };
        if plan.connector.batch().is_some()
            && let Some(codec) = &codec
        {
            codec.check_batch_container().change_context_lazy(|| {
                EmitterStartError::BatchCodec {
                    codec: CodecName::from(&codec.name),
                }
            })?;
        }
        let output_compiled_schema = emitter.output_schema.clone();
        let udfs = runtime.udf_executor(domain);
        let compile_context = RuntimeVmCompileContext {
            available_materialized_streams: &materialized_stream_specs,
            available_lookups: &lookups,
            current_branching: &input_branching,
            udfs: udfs.as_ref(),
        };
        let programs = EmitterTaskPrograms::compile(
            &emitter,
            &plan,
            codec.as_ref(),
            &input_schema,
            &output_compiled_schema,
            compile_context,
        )?;
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
                        branch: None,
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
        EmitterConnectorStarter::check_client_config(&plan)
            .change_context(EmitterStartError::ClientConfig)?;
        let input_collect_policy =
            Runtime::parse_runtime_node_input_collect_policy(emitter.collect_policy.as_ref())
                .change_context(EmitterStartError::CollectPolicy)?;
        let (commands, command_rx) = mpsc::channel(4);
        let (stop_signal, mut stop_rx) = watch::channel(None);
        let task_stop_signal = stop_signal.clone();

        let task = nervix_primitives::task::spawn(async move {
            let shared_routing = match runtime
                .wait_for_domain_routing(&task_domain, &routing_relay)
                .await
            {
                Ok(routing) => routing,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "emitter '{}' in domain '{}' could not bind its routing snapshot: \
                         {error:#}",
                        task_emitter.as_str(),
                        task_domain.as_str(),
                    ));
                    return;
                }
            };
            let mut routing = DomainRoutingCache::new(shared_routing.clone());
            // The emitter's explicit FLUSH EACH and COMMIT EACH cadences are domain logical
            // durations, so every emitter binds the domain clock whether or not it collects input.
            let domain_clock = match runtime.bind_domain_clock(&task_domain) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "emitter '{}' in domain '{}' could not bind its domain clock: {error:#}",
                        task_emitter.as_str(),
                        task_domain.as_str(),
                    ));
                    return;
                }
            };
            let work_cancel_forwarder =
                AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
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
            let emitter_key = DomainNodeRef::node_in(
                task_domain.clone(),
                ModelKind::Emitter,
                task_emitter.clone(),
            );
            let status = runtime.emitter_status(&emitter_key);
            let confirmation_waits = runtime.emitter_confirmation_counter(&emitter_key);
            let context = EmitterConnectorContext {
                routing: shared_routing,
                metrics_dirty: runtime.branch_metrics_mark(
                    &task_domain,
                    ModelKind::Emitter,
                    &task_emitter,
                ),
                status,
                confirmation_waits,
                runtime: runtime.clone(),
                domain: task_domain.clone(),
                emitter: task_emitter.clone(),
                error_policies: task_error_policies.clone(),
                udfs,
                clock: domain_clock.clone(),
            };
            let _registration = super::emitter_supervision::EmitterTaskRegistration::new(&context);
            let backoff = RuntimeReconnectBackoff::from_policy(plan.retry_policy);
            let buffer =
                EmitterBatchBuffer::new(&context, &task_flush_policy, buffered_messages.clone());
            let connector = EmitterConnectorState::open_until_cancelled(
                &plan,
                &context,
                &input_schema,
                &output_compiled_schema,
                codec.as_ref(),
                &mut work_cancel_rx,
            )
            .await;
            let connector_label = connector.connector_label(&plan.label);
            let state = EmitterTaskState::new(connector, buffer, backoff, &context);
            let task_emitter_node = ModelName::from(&task_emitter);
            let batch_context = EmitterBatchContext {
                error_routing: &context.routing,
                metrics_dirty: &context.metrics_dirty,
                runtime: &runtime,
                routing: &mut routing,
                domain_clock: &domain_clock,
                domain: &task_domain,
                emitter: &task_emitter,
                node: &task_emitter_node,
                output_metrics: &task_output_metrics,
                error_policies: &task_error_policies,
                source_filters: &programs.source_filters,
                filter_map: programs.filter_map.as_ref(),
                ordering_group: programs.ordering_group.as_ref(),
                envelope: programs.envelope.as_ref(),
                http_requests: programs.http_requests.as_ref(),
                materialized_state: &task_materialized_state,
            };
            let mut task_loop = EmitterTaskLoop {
                context: &context,
                batch_context,
                state,
                interaction,
                plan: &plan,
                connector_label,
                input_schema: &input_schema,
                output_schema: &output_compiled_schema,
                codec: codec.as_ref(),
                input_metrics: &task_input_metrics,
                fault_injection: &fault_injection,
                buffered_messages: &emitter_buffer_count,
                work_cancel_rx: &mut work_cancel_rx,
                shutdown_rx: &mut shutdown_rx,
                stop_rx: &mut stop_rx,
                stop_signal: &task_stop_signal,
            };
            task_loop.run_to_end(&mut terminal_shutdown_rx).await;
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
    /// Runs the loop until a stop ends it or terminal teardown cancels it, and then finishes the
    /// emitter unless the stop already did.
    ///
    /// The interaction observes shutdown while waiting for work, but a connector can be inside an
    /// external publish attempt when terminal teardown begins. Dropping the loop at that boundary
    /// releases its volatile prepared requests and unresolved ACK guards without reporting them as
    /// delivered or waiting for the attempt timeout. The emitter's transport client is then finished,
    /// by the deadline the stopping domain execution gave this task, so records a publish
    /// delivered still reach it.
    async fn run_to_end(&mut self, terminal_shutdown_rx: &mut watch::Receiver<bool>) {
        let ending = nervix_primitives::select! {
            biased;
            _ = super::emitter_publishing::wait_for_emitter_work_cancel(terminal_shutdown_rx) => {
                EmitterEnding::Teardown
            }
            ending = self.run() => ending,
        };
        match ending {
            EmitterEnding::Stop => {}
            EmitterEnding::InteractionStopped | EmitterEnding::Teardown => {
                self.finish_after_loop(ending).await;
            }
        }
    }

    /// Finishes the emitter once the loop ended without a stop, by the deadline on the task's stop
    /// signal.
    async fn finish_after_loop(&mut self, ending: EmitterEnding) {
        let deadline = *self.stop_rx.borrow();
        self.state
            .finish_after_loop(self.context, &self.connector_label, ending, deadline)
            .await;
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the retained emitter task handles input, retries, flush and commit cadence"
        )
    )]
    async fn run(&mut self) -> EmitterEnding {
        loop {
            nervix_primitives::task::consume_budget().await;
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
                Err(failure) if failure.error.current_context().is_wake_timing() => {
                    self.context.record_failure(format!("{:#}", failure.error));
                    let acks = self.state.connector.pending_acks(&self.state.buffer);
                    RuntimeReconnectBackoff::wait_duration_with_ack_alive(
                        RETRY_ACK_ALIVE_EACH,
                        &mut *self.shutdown_rx,
                        &acks,
                    )
                    .await;
                    continue;
                }
                Err(failure) => {
                    self.context
                        .report_flush_error(&self.connector_label, &format!("{:#}", failure.error));
                    self.context
                        .runtime
                        .handle_internal_processor_error_for_acks(
                            &self.context.domain,
                            ModelKind::Emitter,
                            &self.context.emitter,
                            &self.context.error_policies,
                            [&failure.acks],
                            &failure.error,
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
                        self.context.record_failure(reason.clone());
                        self.context
                            .report_flush_error(&self.connector_label, &reason);
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
                            &self.state.connector,
                            self.fault_injection,
                            &self.context.emitter,
                        )
                    {
                        self.context.record_failure(reason.clone());
                        self.context
                            .report_flush_error(&self.connector_label, &reason);
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
                        connector,
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
                    let drained = connector
                        .drain_for_stop(
                            &self.connector_label,
                            self.context,
                            &mut control,
                            buffer,
                            deadline,
                        )
                        .await;
                    let result = match drained {
                        EmitterStopDrainOutcome::Finished { report, finished } => {
                            self.state.backoff.reset();
                            self.state.retry.clear();
                            self.context.status.clear();
                            if let Some(report) = report.as_ref() {
                                self.batch_context.observe_sent(report);
                            }
                            self.context.record_finished(
                                &self.connector_label,
                                EmitterEnding::Stop,
                                finished,
                            );
                            Ok(())
                        }
                        EmitterStopDrainOutcome::Failed(error) => {
                            let reason = emitter_error_message(&error);
                            self.context.record_failure(reason.clone());
                            self.context
                                .report_flush_error(&self.connector_label, &reason);
                            Err(Report::new(EmitterRuntimeError::FinalFlush)
                                .attach_printable(format!("emitter final flush failed: {reason}")))
                        }
                        EmitterStopDrainOutcome::DeadlineElapsed => {
                            let reason = format!(
                                "emitter '{}' did not drain before its configured deadline",
                                self.context.emitter.as_str()
                            );
                            self.context
                                .report_flush_error(&self.connector_label, &reason);
                            Err(Report::new(EmitterRuntimeError::StopDeadlineElapsed)
                                .attach_printable(reason))
                        }
                    };
                    let should_stop = result.is_ok();
                    if !should_stop {
                        clear_emitter_stop_signal(self.stop_signal, deadline);
                    }
                    if response.send(result).is_ok() && should_stop {
                        break EmitterEnding::Stop;
                    }
                    if should_stop {
                        clear_emitter_stop_signal(self.stop_signal, deadline);
                    }
                }
                RelayInteractionEvent::ForceFlush(completion) => {
                    if self.buffered_messages.load(Ordering::Acquire) > 0
                        && let Some(reason) = emitter_unavailable_reason(
                            &self.state.connector,
                            self.fault_injection,
                            &self.context.emitter,
                        )
                    {
                        self.state
                            .retry
                            .include_acks(self.state.connector.pending_acks(&self.state.buffer));
                        if self.state.retry.is_active() {
                            self.context.record_retry(
                                reason.clone(),
                                self.state.backoff.next_delay(),
                                EmitterRetryKind::Infrastructure,
                            );
                        } else {
                            self.state.retry.defer(
                                self.context,
                                EmitterRetryDeferral {
                                    wait: self.state.backoff.take_next_delay(),
                                    acks: self.state.connector.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: self
                                        .fault_injection
                                        .emitter_should_stall(&self.context.emitter),
                                    reason: Some(&reason),
                                },
                            );
                        }
                        self.context
                            .report_flush_error(&self.connector_label, &reason);
                        completion.complete();
                        continue;
                    }
                    let publish_result = {
                        let EmitterTaskState {
                            connector,
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
                        connector
                            .flush_all(&self.connector_label, self.context, &mut control, buffer)
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
                                emitter_label: &self.connector_label,
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
                            &self.state.connector,
                            self.fault_injection,
                            &self.context.emitter,
                        )
                    {
                        self.context.record_failure(reason.clone());
                        self.context
                            .report_flush_error(&self.connector_label, &reason);
                        let pending = self.state.buffer.drain_pending();
                        self.batch_context
                            .handle_publish_error_batches(
                                pending,
                                reason,
                                MessageErrorOperation::Publish,
                            )
                            .await;
                        break EmitterEnding::InteractionStopped;
                    }
                    let flush_result = {
                        let EmitterTaskState {
                            connector,
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
                        connector
                            .flush_all(&self.connector_label, self.context, &mut control, buffer)
                            .await
                    };
                    match flush_result {
                        Ok(Some(report)) => self.batch_context.observe_sent(&report),
                        Ok(None) => {}
                        Err(error) => {
                            let reason = emitter_error_message(&error);
                            self.context.record_failure(reason.clone());
                            self.context
                                .report_flush_error(&self.connector_label, &reason);
                            let pending = self.state.buffer.drain_pending();
                            let operation =
                                emitter_message_error_operation(&error, self.codec.is_some());
                            self.batch_context
                                .handle_publish_error_batches(pending, reason, operation)
                                .await;
                        }
                    }
                    break EmitterEnding::InteractionStopped;
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
                        || self.state.connector.unavailable_reason().is_some()
                    {
                        self.state.connector = EmitterConnectorState::open_until_cancelled(
                            self.plan,
                            self.context,
                            self.input_schema,
                            self.output_schema,
                            self.codec,
                            &mut *self.work_cancel_rx,
                        )
                        .await;
                        self.state
                            .buffer
                            .report_staged_messages(self.state.connector.staged_messages());
                        if let Some(reason) = self.state.connector.unavailable_reason() {
                            self.state.retry.defer(
                                self.context,
                                EmitterRetryDeferral {
                                    wait: self.state.backoff.take_next_delay(),
                                    acks: self.state.connector.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: false,
                                    reason: Some(reason),
                                },
                            );
                            self.state.reconnect_on_wake = true;
                            continue;
                        }
                        self.state.reconnect_on_wake = false;
                        self.context.status.clear();
                    }
                    let publish_result = {
                        let EmitterTaskState {
                            connector,
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
                        connector
                            .flush_due(
                                &self.connector_label,
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
                                emitter_label: &self.connector_label,
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
                    self.context.metrics_dirty.mark();
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
                            &self.state.connector,
                            self.fault_injection,
                            &self.context.emitter,
                        )
                        .is_some()
                    {
                        let unavailable = emitter_unavailable_reason(
                            &self.state.connector,
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
                                    acks: self.state.connector.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: self
                                        .fault_injection
                                        .emitter_should_stall(&self.context.emitter),
                                    reason: unavailable.as_deref(),
                                },
                            );
                            if let Some(reason) = unavailable.as_deref() {
                                self.context
                                    .report_publish_error(&self.connector_label, reason);
                            }
                        }
                        self.state.reconnect_on_wake |=
                            self.state.connector.unavailable_reason().is_some();
                        continue;
                    }

                    let mut pending_batch = Some(publish_batch);
                    let publish_result = {
                        let EmitterTaskState {
                            connector,
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
                        connector
                            .publish_batch(
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
                                emitter_label: &self.connector_label,
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
        self.metrics_dirty.mark();
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies iteration over this admitted set of error batches"
        )
    )]
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
                    &failure
                        .error
                        .change_context(EmitterRuntimeError::SplitFailedBatch {
                            publish_failure: reason,
                        }),
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
                    routing: Some(&self.error_routing.load()),
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
    fn report_general_error<'a, C: error_stack::Context>(
        &self,
        acks: impl IntoIterator<Item = &'a AckSet>,
        error: &Report<C>,
    ) {
        self.runtime.handle_general_error_for_acks(
            self.domain,
            ModelKind::Emitter,
            self.emitter,
            self.error_policies,
            acks,
            error,
        );
    }

    /// Deliver the message errors a plan recorded, one for each row its program rejected.
    async fn deliver_planned_message_errors(&self, errors: Vec<PlannedMessageError>) {
        self.runtime
            .handle_planned_message_errors(
                Some(&self.error_routing.load()),
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
                    &error,
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
                    .evaluate(
                        self.runtime.executor(),
                        &batch,
                        execution_now,
                        &materialized_values,
                    )
                    .await;
                match evaluated {
                    Ok(groups) => Some(groups),
                    Err(failure) => {
                        self.report_general_error(failure.acks.iter(), &failure.error);
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
            // already align with the batch row for row, and each row is its own envelope input.
            let publish_batch =
                EmitterPublishBatch::from_input(input_relay.clone(), batch, execution_now);
            let publish_batch = self
                .with_envelopes(
                    publish_batch,
                    EnvelopeInput::Published,
                    &materialized_values,
                    execution_now,
                )
                .await?;
            return self.with_ordering_groups(publish_batch, ordering_groups);
        };

        // The request fields of an HTTP emitter with a codec body, and the envelope of any emitter
        // whose route constructs its records, read the original source record of every finalized
        // record the route keeps, and the route consumes the source batch.
        let reads_source_records = self.http_requests.is_some() || self.envelope.is_some();
        let source_records = match (reads_source_records, filter_map.codec_route) {
            (true, true) => Some(SourceRecords::of(&batch)),
            (true, false) | (false, _) => None,
        };
        let planned = plan_emitter_filter_map_batch(
            self.runtime.executor(),
            self.emitter,
            filter_map,
            batch,
            execution_now,
            &materialized_values,
        )
        .await;
        let plan = match planned {
            Ok(plan) => plan,
            Err(failure) => {
                self.report_general_error(failure.acks.iter(), &failure.error);
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
        let envelope_input = match &source_records {
            Some(records) => EnvelopeInput::Source {
                records,
                rows: &plan.source_rows,
            },
            // A route without a codec only filters, so the rows it keeps are source records.
            None => EnvelopeInput::Published,
        };
        let publish_batch = self
            .with_envelopes(
                publish_batch,
                envelope_input,
                &materialized_values,
                execution_now,
            )
            .await?;

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
                self.runtime.executor(),
                self.emitter,
                batch,
                HttpRequestEvaluation {
                    input,
                    side_inputs: materialized_values,
                    execution_now,
                    header_limits: self.runtime.cluster_parameters().http_header_limits(),
                },
            )
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(failure) => {
                self.report_general_error(failure.acks.iter(), &failure.error);
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

    /// `publish_batch` with the envelope of each of its rows, evaluated after the route kept and
    /// finalized them, when the emitter writes one.
    ///
    /// A batch whose envelopes cannot be evaluated at all is reported and dropped. A row whose own
    /// envelope cannot be evaluated keeps its reason and is rejected at the send.
    async fn with_envelopes(
        &self,
        publish_batch: EmitterPublishBatch,
        input: EnvelopeInput<'_>,
        side_inputs: &HashMap<String, RuntimeValue>,
        execution_now: Timestamp,
    ) -> Option<EmitterPublishBatch> {
        let Some(envelope) = self.envelope else {
            return Some(publish_batch);
        };
        let enveloped = publish_batch
            .with_evaluated_envelopes(
                envelope,
                self.runtime.executor(),
                input,
                side_inputs,
                execution_now,
            )
            .await;
        match enveloped {
            Ok(publish_batch) => Some(publish_batch),
            Err(failure) => {
                self.report_general_error(failure.acks.iter(), &failure.error);
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
            &error.change_context(EmitterRuntimeError::BuildPublishBatch),
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
            ProgramRun {
                executor: self.runtime.executor(),
                now: execution_now,
            },
            "emitter",
            self.emitter,
            MessageErrorOperation::SourceWhere,
            program,
            batch,
            side_inputs,
        )
        .await
        {
            Ok(plan) => plan,
            Err(failure) => {
                self.report_general_error(
                    failure.acks.iter(),
                    &failure
                        .error
                        .change_context(EmitterRuntimeError::SourceFilter {
                            relay: input_relay.clone(),
                        }),
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_connector::{EmitterFinishResult, EmitterLifecycle};

    use super::*;
    use crate::runtime::test_fixtures::{emitter_context, input_batch};

    fn task_state(context: &EmitterConnectorContext) -> EmitterTaskState {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.set_flush_policy(RuntimeFlushPolicy::Immediate);
        EmitterTaskState::new(
            EmitterConnectorState::Unavailable {
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
        let attached = Report::new(EmitterRuntimeError::PublishBatch)
            .attach_printable("specific broker failure");
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
        let connector_reason =
            "resolving MQTT host 'mqtt.nervix.test' failed: the name does not exist";
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
            "emitter error for emitter 'output' in domain 'emitter_tests': connector background \
             error"
        );
        assert_eq!(host.staging_directory(), context.runtime.temp_dir());

        let (logged_acks, logged_completion) = AckSet::root();
        let logged_acks = EmitterRecordAcknowledgements::new(logged_acks);
        host.handle_general_error(&logged_acks, "publish failed".to_string());
        let logged_report =
            "emitter 'output' general error in domain 'emitter_tests': publish failed";
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

    /// A emitter whose transport client still holds records once the loop ends. It answers its
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
                    deadline + PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE + Duration::from_millis(1),
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
            .drain_for_stop("test", &context, &mut control, &mut buffer, deadline)
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
                    "test emitter's transport client still held 3 records when its finish \
                     deadline passed"
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
            "emitter 'output' in domain 'emitter_tests' stopped before its test transport client \
             sent every record it held: test emitter's transport client still held 2 records when \
             its finish deadline passed"
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
        let mut connector = EmitterConnectorState::Unavailable {
            reason: "test emitter is unavailable".to_string(),
        };

        let finished = connector
            .finish_transport(Instant::now())
            .await
            .expect("an emitter without a client has nothing to send");

        assert_eq!(finished, EmitterFinish::NothingHeld);
    }
}
