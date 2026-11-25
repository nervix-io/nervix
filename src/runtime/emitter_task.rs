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
    physical_time::{PhysicalDeadline, actual_utc_now},
};
use nervix_models::ExternalErrorCode;

use super::{
    emitter_publishing::{EmitterPublishAttemptKind, EmitterStopDrainOutcome},
    emitter_retry::{EmitterBufferedKeepalive, EmitterGiveUp, GivenUpWork},
    *,
};

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the emitter command protocol retains four control requests independently of \
                  record buffering"
    )
)]
const EMITTER_COMMAND_CAPACITY: usize = 4;

#[path = "emitter_branch_retention.rs"]
pub(super) mod branch_retention;
use branch_retention::EmitterBranchRetention;

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
    /// Cold observers retain this exact task's readiness, separately from pending-work errors.
    pub(super) readiness: Arc<AtomicBool>,
    /// Drain observers retain the registered counter independently of this context.
    pub(super) confirmation_waits: Arc<AtomicUsize>,
    /// The process-local metric store independently retains these children for inspection.
    pub(super) interactions: EmitterInteractionMetrics,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) emitter: EmitterName,
    /// The unit of the emitter this task runs: its `Single` unit, or one of its instances.
    pub(super) unit: Unit,
    pub(super) error_policies: ErrorPolicies,
    pub(super) udfs: Option<UdfExecutor>,
    /// The bound clock that resolves this emitter's explicit `FLUSH EACH` and `COMMIT EACH`
    /// cadences. Publish attempts, retry backoff, acknowledgement keepalive and stop deadlines
    /// stay on the monotonic clock and never read it.
    pub(super) clock: DomainClock,
    /// The node's installed remote acknowledgement timing, kept from the task's start. Every beat
    /// that keeps the emitter's held acknowledgements alive reads its committed alive interval
    /// here once that work waits.
    pub(super) remote_ack_timing: SharedRemoteAckTiming,
}

impl EmitterConnectorContext {
    /// The key of this exact emitter unit in the node's emitter registries.
    pub(super) fn registry_key(&self) -> DomainNodeUnitRef {
        DomainNodeUnitRef::new(
            DomainNodeRef::node_in(
                self.domain.clone(),
                ModelKind::Emitter,
                self.emitter.clone(),
            ),
            self.unit,
        )
    }
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
    interactions: &'a EmitterInteractionMetrics,
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
    /// The emitter task's kept remote acknowledgement timing, which every program it runs over a
    /// batch reports the batch's acknowledgements alive by while it waits.
    remote_ack_timing: &'a SharedRemoteAckTiming,
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
    #[error("the emitter's destination cannot accept the write as it is configured")]
    DestinationMisconfigured,
    #[error("failed to initialize emitter connector")]
    InitializeConnector,
    #[error("failed to resume emitter connector")]
    ResumeConnector,
    #[error("emitter transport is waiting for readiness")]
    ConnectorWaiting,
    #[error("failed to close emitter connector")]
    CloseConnector,
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
    #[error("the emitter gave up committing the rows its connector staged")]
    CommitGivenUp,
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
    pub(super) fn retry_cause(&self) -> EmitterRetryCause {
        match self {
            Self::ConnectorNotInitialized
            | Self::InitializeConnector
            | Self::ResumeConnector
            | Self::ConnectorWaiting => EmitterRetryCause::Connect,
            _ => EmitterRetryCause::Publish,
        }
    }
    pub(super) fn is_retryable_publish_failure(&self) -> bool {
        match self {
            Self::ConnectorNotInitialized
            | Self::ResumeConnector
            | Self::ConnectorWaiting
            | Self::PublishBatch
            | Self::PublishStalled
            | Self::EncodingRefused
            | Self::DestinationMisconfigured
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
            | Self::CloseConnector
            | Self::FaultInjected
            | Self::ShutdownWhileStalled
            | Self::CommitGivenUp
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

    /// Whether a retry of this failure waits for the destination, so that the emitter's
    /// `GIVE UP AFTER` counts the wait. An encoding the node's bounded execution could not take yet
    /// judged nothing about the destination: the node holds the work back and the wait is its own.
    pub(super) fn fails_the_destination(&self) -> bool {
        !matches!(self, Self::EncodingRefused)
    }

    /// Whether a retry of this failure waits for the operator to repair the destination's setup,
    /// which status and inspection show as a misconfiguration rather than a transient error.
    pub(super) fn is_misconfiguration(&self) -> bool {
        matches!(self, Self::DestinationMisconfigured)
    }

    /// The class the rows of a batch this failure ended carry: the destination or its connector
    /// failing the batch for good is a refusal, a stop or shutdown deadline is Nervix cutting the
    /// work, a batch the codec could not encode is a codec failure, a source filter that could not
    /// run is an evaluation failure, and every broken invariant of the emitter's own bookkeeping
    /// is Nervix breaking its contract.
    pub(super) fn batch_failure_class(&self) -> MessageErrorClass {
        match self {
            Self::ConnectorNotInitialized
            | Self::ResumeConnector
            | Self::ConnectorWaiting
            | Self::PublishBatch
            | Self::PublishStalled
            | Self::InvalidConnectorConfig
            | Self::DestinationMisconfigured
            | Self::InitializeConnector
            | Self::CloseConnector
            | Self::FaultInjected
            | Self::FinalFlush
            | Self::FinishTransport => MessageErrorClass::REFUSED,
            Self::ShutdownWhileStalled | Self::StopDeadlineElapsed => {
                MessageErrorClass::INTERRUPTED
            }
            Self::CommitGivenUp => MessageErrorClass::UNAVAILABLE,
            Self::EncodeBatch => MessageErrorClass::CODEC,
            Self::SourceFilter { .. } => MessageErrorClass::EVALUATION,
            Self::EncodingRefused
            | Self::UnansweredEmitterRecords { .. }
            | Self::FlushPolicyNotInitialized
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
            | Self::FlushTiming
            | Self::RetryTiming
            | Self::BuildPublishBatch
            | Self::SplitFailedBatch { .. } => MessageErrorClass::INTERNAL,
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
    pub(super) fn record_confirmation_timeout(&self, error: &Report<EmitterRuntimeError>) {
        let timed_out = error.frames().any(|frame| {
            matches!(
                frame.downcast_ref::<nervix_connector::ConfirmationWindowError>(),
                Some(nervix_connector::ConfirmationWindowError::AnswerTimedOut { .. })
            )
        });
        if timed_out {
            self.interactions.record_confirmation_timeout();
        }
    }
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
        give_up_at: Option<PhysicalDeadline>,
    ) {
        self.status.fail(
            error.into(),
            Some(EmitterRetryStatus {
                kind,
                reconnect: RuntimeReconnectStatus {
                    backoff,
                    retry_at: Instant::now() + backoff,
                },
                give_up_at,
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

    pub(super) fn report_close_error(
        &self,
        kind: nervix_models::EmitterEntityKind,
        error: &Report<EmitterRuntimeError>,
    ) {
        let reason = emitter_error_message(error);
        self.runtime.events().report_error(format!(
            "failed to close {} emitter '{}' in domain '{}': {reason}",
            kind.connector().to_string().to_ascii_lowercase(),
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(domain = self.domain.as_str(), emitter = self.emitter.as_str(), error = %reason,
            "failed to close emitter connector");
        self.record_failure(reason);
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
        match Runtime::parse_runtime_node_flush_policy(
            policy,
            self.runtime.data_plane_settings().immediate_flush_minimum(),
        ) {
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
                give_up_at: None,
            }),
        );
    }

    fn clear_transient_error(&self) {
        self.status.clear_connector();
    }
}

impl nervix_connector::EmitterReadiness for EmitterConnectorContext {
    fn mark_ready(&self) {
        self.readiness.store(true, Ordering::Relaxed);
    }

    fn mark_unready(&self) {
        self.readiness.store(false, Ordering::Relaxed);
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

impl nervix_connector::ConnectorEventProvider for EmitterConnectorContext {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "connector construction borrows its retained interaction children"
        )
    )]
    fn event_counter(
        &self,
        event: nervix_models::ConnectorEvent,
    ) -> error_stack::Result<
        nervix_connector::ConnectorEventCounter,
        nervix_connector::UndeclaredConnectorEvent,
    > {
        self.interactions.event_counter(event)
    }
}

impl nervix_connector::EmitterPayloadObserver for EmitterConnectorContext {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "records native measurements through the retained metric handle"
        )
    )]
    fn record_prepared_payload(&self, measurement: nervix_connector::EmitterPayloadMeasurement) {
        self.interactions.record_payload(
            measurement.source_messages,
            measurement.encoded_bytes,
            measurement.prepared_bytes,
        );
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "records native subdivisions without resolving labels"
        )
    )]
    fn record_payload_subdivisions(&self, subdivisions: u64) {
        self.interactions.record_splits(subdivisions);
    }
}

impl EmitterBoundedExecution for EmitterConnectorContext {
    fn executor(&self) -> Executor {
        self.runtime.executor().clone()
    }
}

impl EmitterGeneralErrorHandler for EmitterConnectorContext {
    fn handle_general_error(
        &self,
        acks: &EmitterRecordAcknowledgements,
        class: MessageErrorClass,
        reason: String,
    ) {
        match self.error_policies.general {
            GeneralErrorPolicy::Ignore => acks.acknowledge(),
            GeneralErrorPolicy::Log => {
                let report = format!(
                    "emitter '{}' general error in domain '{}': {}",
                    self.emitter.as_str(),
                    self.domain.as_str(),
                    reason
                );
                self.runtime.events().report_node_error(
                    report.clone(),
                    ReportedError {
                        class,
                        node: NodeRef::new(ModelKind::Emitter, self.emitter.clone()),
                    },
                );
                warn!(
                    domain = self.domain.as_str(),
                    emitter = self.emitter.as_str(),
                    error_class = class.as_str(),
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

/// How a failure of a whole publish batch reads in the message error of each of its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EmitterBatchFailure {
    class: MessageErrorClass,
    operation: MessageErrorOperation,
}

impl EmitterBatchFailure {
    /// A batch the emitter's destination failed for good.
    const REFUSED: Self = Self {
        class: MessageErrorClass::REFUSED,
        operation: MessageErrorOperation::Publish,
    };

    /// A batch the emitter stopped retrying once its `GIVE UP AFTER` passed.
    const GIVEN_UP: Self = Self {
        class: MessageErrorClass::UNAVAILABLE,
        operation: MessageErrorOperation::Publish,
    };

    fn of(error: &Report<EmitterRuntimeError>, codec_route: bool) -> Self {
        let context = error.current_context();
        let operation = match (context, codec_route) {
            (EmitterRuntimeError::EncodeBatch, true) => MessageErrorOperation::Encode,
            (EmitterRuntimeError::EncodeBatch, false) => MessageErrorOperation::Values,
            _ => MessageErrorOperation::Publish,
        };
        Self {
            class: context.batch_failure_class(),
            operation,
        }
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
    keepalive: EmitterBufferedKeepalive,
    backoff: RuntimeReconnectBackoff,
    reconnect_on_wake: bool,
    give_up: EmitterGiveUp,
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
        give_up: EmitterGiveUp,
        context: &EmitterConnectorContext,
    ) -> Self {
        let reconnect_on_wake = connector.unavailable_reason().is_some();
        let mut retry = EmitterRetrySchedule::new(context.remote_ack_timing.clone());
        if let Some(reason) = connector.unavailable_reason() {
            retry.defer(
                context,
                EmitterRetryDeferral {
                    cause: EmitterRetryCause::Connect,
                    wait: connector.installation_retry_delay(&mut backoff),
                    acks: EmitterAcknowledgements::default(),
                    waiting_for_stall_clear: false,
                    reason: Some(reason),
                    kind: EmitterRetryKind::Infrastructure,
                    give_up_at: None,
                },
            );
        } else {
            context.status.clear();
        }
        Self {
            connector,
            buffer,
            retry,
            keepalive: EmitterBufferedKeepalive::new(context.remote_ack_timing.clone()),
            backoff,
            reconnect_on_wake,
            give_up,
        }
    }

    fn wake(&self, context: &EmitterConnectorContext) -> RuntimeWake {
        let wake = self
            .retry
            .wake(self.connector.cadence_wake(&context.clock, &self.buffer));
        match self.next_give_up() {
            Some(deadline) => wake.with_physical(deadline),
            None => wake,
        }
    }

    /// When the emitter next gives work up: the earliest of its buffered batches' give-up clocks
    /// and the clock of what its connector staged.
    fn next_give_up(&self) -> Option<PhysicalDeadline> {
        let buffered = self.give_up.next_deadline(&self.buffer);
        let staged = self.connector.staged_give_up().deadline();
        match (buffered, staged) {
            (Some(buffered), Some(staged)) => Some(buffered.min(staged)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    /// Whether the emitter holds no work at all: nothing buffered and nothing staged.
    fn holds_nothing(&self) -> bool {
        self.buffer.is_empty() && self.connector.staged_messages() == 0
    }

    /// Gives up every buffered batch and the staged rows whose `GIVE UP AFTER` has passed, and
    /// delivers the failures of everything given up, including what a publish call gave up.
    /// Answers whether anything was given up.
    async fn give_up_due_work(
        &mut self,
        context: &EmitterConnectorContext,
        batch_context: &EmitterBatchContext<'_>,
    ) -> bool {
        self.give_up.collect(&mut self.buffer).await;
        self.connector
            .give_up_staged_if_due(&mut self.give_up, &mut self.buffer)
            .await;
        self.deliver_given_up(context, batch_context).await
    }

    /// Delivers the failures of everything the emitter gave up since it last delivered, and
    /// answers whether there was any.
    ///
    /// When the emitter then holds nothing while its connector is open, no retry is left to run:
    /// the next input is published at once. The status keeps saying why the work was given up,
    /// without a wait, until an attempt succeeds or fails again.
    async fn deliver_given_up(
        &mut self,
        context: &EmitterConnectorContext,
        batch_context: &EmitterBatchContext<'_>,
    ) -> bool {
        let Some(given_up) = self.give_up.take_given_up() else {
            return false;
        };
        let message = given_up.message.clone();
        batch_context.fail_given_up(given_up).await;
        if self.holds_nothing() && self.connector.unavailable_reason().is_none() {
            self.retry.clear();
            context.status.fail(message, None);
        }
        true
    }

    fn keep_buffered_work_alive(&mut self) {
        if !self.retry.is_active() && self.keepalive.take_due() {
            self.connector.pending_acks(&self.buffer).keep_alive();
        }
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
            self.connector.record_unfinished_ending();
            debug!(
                domain = context.domain.as_str(),
                emitter = context.emitter.as_str(),
                connector,
                ending = ending.as_ref(),
                "emitter task ended without a stop deadline, so its emitter is not finished"
            );
            return;
        };
        match self.connector.finish_transport(context, deadline).await {
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
                let cause = failure.retry_cause();
                let (error, batch_owner) = failure.into_parts();
                let wait = emitter_retry_delay(&mut self.backoff, &error);
                if let EmitterPublishBatchOwner::Caller = batch_owner {
                    let batch = pending_batch.take().verified(
                        "a caller-owned publish failure retains the batch passed to the attempt",
                    );
                    if let Err(retain_error) = self.buffer.push(context, batch.clone()) {
                        self.retry.clear();
                        let reason = emitter_error_message(&retain_error);
                        let failure =
                            EmitterBatchFailure::of(&retain_error, outcome_context.codec_route);
                        batch_context
                            .handle_publish_error_batch(batch, reason, failure)
                            .await;
                        self.deliver_given_up(context, batch_context).await;
                        return;
                    }
                } else {
                    pending_batch.take();
                }
                // The attempt held every buffered batch, so their clocks start with it. What has
                // already been retried for as long as `GIVE UP AFTER` allows is not retried again:
                // with `GIVE UP AFTER 0s` that is everything the attempt held.
                self.give_up.record_failure(&mut self.buffer, &error);
                self.give_up.collect(&mut self.buffer).await;
                self.connector
                    .give_up_staged_if_due(&mut self.give_up, &mut self.buffer)
                    .await;
                let reason = emitter_error_message(&error);
                self.reconnect_on_wake = self.connector.reconnect_after(&error, context);
                outcome_context.report_error(context, &reason);
                if !self.holds_nothing() || self.connector.unavailable_reason().is_some() {
                    let kind = match cause {
                        EmitterRetryCause::Commit => EmitterRetryKind::commit(&error),
                        EmitterRetryCause::Publish | EmitterRetryCause::Connect => {
                            EmitterRetryKind::publish(&error)
                        }
                    };
                    self.retry.defer(
                        context,
                        EmitterRetryDeferral {
                            cause,
                            wait,
                            acks: self.connector.pending_acks(&self.buffer),
                            waiting_for_stall_clear: error.current_context()
                                == &EmitterRuntimeError::PublishStalled,
                            reason: Some(&reason),
                            kind,
                            give_up_at: self.next_give_up(),
                        },
                    );
                }
            }
            Err(failure) => {
                self.retry.clear();
                let (error, failed_batches) =
                    failure.drain_failed_batches(pending_batch, &mut self.buffer);
                let reason = emitter_error_message(&error);
                context.record_failure(reason.clone());
                outcome_context.report_error(context, &reason);
                let failure = EmitterBatchFailure::of(&error, outcome_context.codec_route);
                batch_context
                    .handle_publish_error_batches(failed_batches, reason, failure)
                    .await;
            }
        }
        self.deliver_given_up(context, batch_context).await;
    }
}

/// The already prepared dependencies one emitter's event loop drives.
struct EmitterTaskLoop<'a> {
    context: &'a EmitterConnectorContext,
    batch_context: EmitterBatchContext<'a>,
    state: EmitterTaskState,
    retention: EmitterBranchRetention,
    interaction: RelayInteraction<EmitterTaskCommand>,
    connector_label: String,
    plan: &'a EmitterStartPlan,
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
        unit: Unit,
        inputs: Vec<super::emitter_supervision::EmitterInput>,
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
            settings: runtime.inner.vm_settings,
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
        let task_interactions = runtime.inner.metrics.register_emitter_interactions(
            domain,
            &task_emitter,
            unit,
            physical_node_id,
            plan.connector.kind(),
        );
        let task_node = ModelName::from(&emitter.name);
        let mut task_input_metrics = HashMap::new();
        for input in &inputs {
            if task_input_metrics.contains_key(&input.relay) {
                continue;
            }
            let metrics =
                runtime
                    .inner
                    .metrics
                    .resolve_unit_node_input_metrics(NodeBatchMetricsSpec {
                        unit,
                        domain,
                        kind: ModelKind::Emitter,
                        node: &task_node,
                        relay: &input.relay,
                        physical_node_id,
                        direction: "received",
                        branch: None,
                    });
            task_input_metrics.insert(input.relay.clone(), metrics);
        }
        let task_output_metrics = match &task_metric_relay {
            Some(relay) => {
                EmitterOutputMetrics::Relay(runtime.inner.metrics.resolve_node_batch_metrics(
                    NodeBatchMetricsSpec {
                        unit,
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
                runtime.inner.metrics.resolve_unit_node_message_metrics(
                    domain,
                    ModelKind::Emitter,
                    &ModelName::from(&emitter.name),
                    unit,
                    physical_node_id,
                    "sent",
                ),
            ),
        };
        let task_flush_policy = emitter.flush_policy.clone();
        let task_error_policies = emitter.error_policies.clone();
        let task_materialized_state = emitter.materialized_state.clone();
        let retention = EmitterBranchRetention::new(
            &emitter.inputs,
            runtime.inner.branch_instance_expiration_scan_interval,
        );
        let routing_relay = inputs
            .first()
            .map(|input| input.relay.clone())
            .verified("a started emitter unit reads at least one relay unit");
        let fault_injection = runtime.inner.fault_injection.clone();
        let runtime = runtime.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let interaction_shutdown_rx = shutdown_tx.subscribe();
        let mut domain_work_cancel_rx = shutdown_tx.subscribe();
        let mut terminal_shutdown_rx = shutdown_tx.subscribe();
        let (work_cancel, mut work_cancel_rx) = watch::channel(false);
        let task_work_cancel = work_cancel.clone();
        let quiesce_counters = runtime.unit_quiesce_counters(
            domain,
            NodeUnitRef::new(NodeRef::new(ModelKind::Emitter, &emitter.name), unit),
        );
        let force_flush = runtime.force_flush_participant(domain, quiesce_counters.clone());
        let emitter_buffer_count = runtime
            .inner
            .emitter_buffers
            .entry(DomainNodeUnitRef::new(
                DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.name.clone()),
                unit,
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
        let (commands, command_rx) = mpsc::channel(EMITTER_COMMAND_CAPACITY);
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
            for input in inputs {
                let interaction_input = match input_collect_policy {
                    Some(policy) => RelayInteractionInput::collecting(
                        input.relay,
                        input.fan_in,
                        policy,
                        domain_clock.clone(),
                    ),
                    None => RelayInteractionInput::immediate(input.relay, input.fan_in),
                };
                let interaction_input = match input.relay_unit {
                    Some(relay_unit) => interaction_input.of_relay_unit(relay_unit),
                    None => interaction_input,
                };
                interaction_inputs.push(interaction_input);
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
            let emitter_key = DomainNodeUnitRef::new(
                DomainNodeRef::node_in(
                    task_domain.clone(),
                    ModelKind::Emitter,
                    task_emitter.clone(),
                ),
                unit,
            );
            let status = runtime.emitter_status(&emitter_key);
            let confirmation_waits = runtime.emitter_confirmation_counter(&emitter_key);
            let context = EmitterConnectorContext {
                readiness: runtime.register_emitter_readiness(&emitter_key),
                routing: shared_routing,
                metrics_dirty: runtime.unit_branch_metrics_mark(
                    &task_domain,
                    &NodeUnitRef::new(NodeRef::new(ModelKind::Emitter, &task_emitter), unit),
                ),
                status,
                confirmation_waits,
                interactions: task_interactions,
                runtime: runtime.clone(),
                domain: task_domain.clone(),
                emitter: task_emitter.clone(),
                unit,
                error_policies: task_error_policies.clone(),
                udfs,
                clock: domain_clock.clone(),
                remote_ack_timing: runtime.remote_ack_timing().clone(),
            };
            let _registration = super::emitter_supervision::EmitterTaskRegistration::new(&context);
            let failure_status = context.status.clone();
            let _failure_observation = context
                .interactions
                .bind_failure_source(move || failure_status.failures().is_some());
            let backoff = RuntimeReconnectBackoff::from_policy(
                plan.retry_policy,
                runtime.inner.connector_host.retry,
            );
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
            let state = EmitterTaskState::new(
                connector,
                buffer,
                backoff,
                EmitterGiveUp::new(plan.give_up_after),
                &context,
            );
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
                interactions: &context.interactions,
                error_policies: &task_error_policies,
                source_filters: &programs.source_filters,
                filter_map: programs.filter_map.as_ref(),
                ordering_group: programs.ordering_group.as_ref(),
                envelope: programs.envelope.as_ref(),
                http_requests: programs.http_requests.as_ref(),
                materialized_state: &task_materialized_state,
                remote_ack_timing: &context.remote_ack_timing,
            };
            let mut task_loop = EmitterTaskLoop {
                context: &context,
                batch_context,
                state,
                retention,
                interaction,
                connector_label,
                plan: &plan,
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
        self.state.connector.close_transport(self.context).await;
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
            if self.state.buffer.is_empty() {
                self.retention.clear();
                if self.state.connector.staged_messages() == 0 {
                    self.state.keepalive.clear();
                }
            }
            // Retry replaces ordinary flush wakes. Maintenance joins that final wake so it
            // still runs while a physical retry owns publication.
            let wake = self.state.keepalive.wake(
                self.retention.wake(self.state.wake(self.context)),
                self.state.retry.is_active(),
            );
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
                    RuntimeReconnectBackoff::wait_ack_alive_interval(
                        &self.context.remote_ack_timing,
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
                    let kind = EmitterPublishAttemptKind::from_retry(self.state.retry.is_active());
                    let EmitterTaskState {
                        connector,
                        buffer,
                        backoff,
                        give_up,
                        ..
                    } = &mut self.state;
                    let mut control = EmitterPublishControl {
                        fault_injection: self.fault_injection,
                        shutdown_rx: &mut *self.shutdown_rx,
                        stop_rx: &mut *self.stop_rx,
                        backoff,
                        give_up,
                    };
                    let drained = connector
                        .drain_for_stop(
                            &self.connector_label,
                            self.context,
                            &mut control,
                            buffer,
                            deadline,
                            kind,
                        )
                        .await;
                    self.state
                        .deliver_given_up(self.context, &self.batch_context)
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
                        // Not reaching the destination fails every batch the emitter holds.
                        self.state
                            .give_up
                            .record_unavailable(&mut self.state.buffer, &reason);
                        self.state
                            .give_up_due_work(self.context, &self.batch_context)
                            .await;
                        if self.state.retry.is_active() {
                            self.context.record_retry(
                                reason.clone(),
                                self.state.backoff.next_delay(),
                                EmitterRetryKind::Infrastructure,
                                self.state.next_give_up(),
                            );
                        } else {
                            self.state.retry.defer(
                                self.context,
                                EmitterRetryDeferral {
                                    cause: self.state.connector.retry_cause(),
                                    wait: self.state.backoff.take_next_delay(),
                                    acks: self.state.connector.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: self
                                        .fault_injection
                                        .emitter_should_stall(&self.context.emitter),
                                    reason: Some(&reason),
                                    kind: EmitterRetryKind::Infrastructure,
                                    give_up_at: self.state.next_give_up(),
                                },
                            );
                        }
                        self.context
                            .report_flush_error(&self.connector_label, &reason);
                        completion.complete();
                        continue;
                    }
                    // A stopping domain's drain waits for no record a retry holds: for its
                    // generations, a scheduled retry owns publication and publishes the buffer
                    // when it is due, so the generation releases nothing and completes at once.
                    let stopping = self.context.clock.is_stopping();
                    if stopping && self.state.retry.is_active() {
                        completion.complete();
                        continue;
                    }
                    let publish_result = {
                        let EmitterTaskState {
                            connector,
                            buffer,
                            backoff,
                            retry,
                            give_up,
                            ..
                        } = &mut self.state;
                        let mut control = EmitterPublishControl {
                            fault_injection: self.fault_injection,
                            shutdown_rx: &mut *self.shutdown_rx,
                            stop_rx: &mut *self.stop_rx,
                            backoff,
                            give_up,
                        };
                        if stopping {
                            // One attempt: a failure goes back to the retry schedule, so the
                            // stop's generation never waits out an unavailable destination.
                            connector
                                .flush_now(
                                    &self.connector_label,
                                    self.context,
                                    &mut control,
                                    buffer,
                                    EmitterPublishAttemptKind::from_retry(false),
                                )
                                .await
                                .map_err(EmitterPublishFailure::buffer)
                        } else {
                            // Every other drain waits for what the emitter holds: its generation
                            // publishes a record a retry holds at once and retries inline until
                            // the destination takes it, the stop deadline passes or the task
                            // shuts down.
                            connector
                                .flush_all(
                                    &self.connector_label,
                                    self.context,
                                    &mut control,
                                    buffer,
                                    EmitterPublishAttemptKind::from_retry(retry.is_active()),
                                )
                                .await
                                .map_err(EmitterPublishFailure::buffer)
                        }
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
                                EmitterBatchFailure::REFUSED,
                            )
                            .await;
                        break EmitterEnding::InteractionStopped;
                    }
                    let flush_result = {
                        let kind =
                            EmitterPublishAttemptKind::from_retry(self.state.retry.is_active());
                        let EmitterTaskState {
                            connector,
                            buffer,
                            backoff,
                            give_up,
                            ..
                        } = &mut self.state;
                        let mut control = EmitterPublishControl {
                            fault_injection: self.fault_injection,
                            shutdown_rx: &mut *self.shutdown_rx,
                            stop_rx: &mut *self.stop_rx,
                            backoff,
                            give_up,
                        };
                        connector
                            .flush_all(
                                &self.connector_label,
                                self.context,
                                &mut control,
                                buffer,
                                kind,
                            )
                            .await
                    };
                    self.state
                        .deliver_given_up(self.context, &self.batch_context)
                        .await;
                    match flush_result {
                        Ok(Some(report)) => self.batch_context.observe_sent(&report),
                        Ok(None) => {}
                        Err(error) => {
                            let reason = emitter_error_message(&error);
                            self.context.record_failure(reason.clone());
                            self.context
                                .report_flush_error(&self.connector_label, &reason);
                            let pending = self.state.buffer.drain_pending();
                            let failure = EmitterBatchFailure::of(&error, self.codec.is_some());
                            self.batch_context
                                .handle_publish_error_batches(pending, reason, failure)
                                .await;
                        }
                    }
                    break EmitterEnding::InteractionStopped;
                }
                RelayInteractionEvent::Wake => {
                    self.maintain_branches().await;
                    self.state.keep_buffered_work_alive();
                    // Maintenance and liveness alone are not publication attempts: they must
                    // not reset retry backoff, clear status or advance a logical cadence.
                    // An unreadable logical clock returns through the interaction's existing
                    // timing-error path on the next iteration, retaining all buffered work.
                    if !matches!(self.state.wake(self.context).is_reached(), Ok(true)) {
                        continue;
                    }
                    // Work the emitter has retried for as long as its `GIVE UP AFTER` allows fails
                    // now, before any further attempt carries it.
                    let gave_up = self
                        .state
                        .give_up_due_work(self.context, &self.batch_context)
                        .await;
                    if gave_up && !self.state.retry.is_active() {
                        continue;
                    }
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
                        let acks = self.state.connector.pending_acks(&self.state.buffer);
                        let context = self.context;
                        context
                            .remote_ack_timing
                            .keep_alive_while(&acks, async {
                                if self.state.connector.retries_installation() {
                                    self.state.connector =
                                        EmitterConnectorState::open_until_cancelled(
                                            self.plan,
                                            self.context,
                                            self.input_schema,
                                            self.output_schema,
                                            self.codec,
                                            &mut *self.work_cancel_rx,
                                        )
                                        .await;
                                } else {
                                    self.state
                                        .connector
                                        .resume_until_cancelled(
                                            self.context,
                                            &mut *self.work_cancel_rx,
                                        )
                                        .await
                                        .discarded(
                                            "the retained connector records its resume failure \
                                             for the host retry",
                                        );
                                }
                            })
                            .await;
                        self.state
                            .buffer
                            .report_staged_messages(self.state.connector.staged_messages());
                        if let Some(reason) = self.state.connector.unavailable_reason() {
                            let reason = reason.to_owned();
                            // A reconnect that failed is a failed attempt for every batch held.
                            self.state
                                .give_up
                                .record_unavailable(&mut self.state.buffer, &reason);
                            self.state
                                .give_up_due_work(self.context, &self.batch_context)
                                .await;
                            self.state.retry.defer(
                                self.context,
                                EmitterRetryDeferral {
                                    cause: EmitterRetryCause::Connect,
                                    wait: self
                                        .state
                                        .connector
                                        .installation_retry_delay(&mut self.state.backoff),
                                    acks: self.state.connector.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: false,
                                    reason: Some(&reason),
                                    kind: EmitterRetryKind::Infrastructure,
                                    give_up_at: self.state.next_give_up(),
                                },
                            );
                            self.state.reconnect_on_wake = true;
                            continue;
                        }
                        self.state.reconnect_on_wake = false;
                    }
                    let publish_result = {
                        let EmitterTaskState {
                            connector,
                            buffer,
                            backoff,
                            give_up,
                            ..
                        } = &mut self.state;
                        let mut control = EmitterPublishControl {
                            fault_injection: self.fault_injection,
                            shutdown_rx: &mut *self.shutdown_rx,
                            stop_rx: &mut *self.stop_rx,
                            backoff,
                            give_up,
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
                        .process_input(&input_relay, batch, wait_for_required_state, work.as_mut())
                        .await
                    {
                        Some(batch) => batch,
                        None => continue,
                    };
                    self.state.keepalive.include_batch(&publish_batch);

                    if self.interaction.is_draining() {
                        if let Err(error) = self
                            .state
                            .buffer
                            .retain_without_cadence(publish_batch.clone())
                        {
                            let reason = emitter_error_message(&error);
                            let failure = EmitterBatchFailure::of(&error, self.codec.is_some());
                            self.batch_context
                                .handle_publish_error_batch(publish_batch, reason, failure)
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
                            let failure = EmitterBatchFailure::of(&error, self.codec.is_some());
                            self.batch_context
                                .handle_publish_error_batch(publish_batch, reason, failure)
                                .await;
                            continue;
                        }
                        self.state.retry.include_acks(publish_batch.merged_acks());
                        // A batch the emitter holds because it cannot reach its destination at all
                        // has failed its first attempt. One that only waits for a retry already
                        // scheduled has not been attempted yet.
                        if let Some(reason) = unavailable.as_deref() {
                            self.state
                                .give_up
                                .record_unavailable(&mut self.state.buffer, reason);
                        }
                        if !self.state.retry.is_active() {
                            self.state.retry.defer(
                                self.context,
                                EmitterRetryDeferral {
                                    cause: self.state.connector.retry_cause(),
                                    wait: self.state.backoff.take_next_delay(),
                                    acks: self.state.connector.pending_acks(&self.state.buffer),
                                    waiting_for_stall_clear: self
                                        .fault_injection
                                        .emitter_should_stall(&self.context.emitter),
                                    reason: unavailable.as_deref(),
                                    kind: EmitterRetryKind::Infrastructure,
                                    give_up_at: self.state.next_give_up(),
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
                            give_up,
                            ..
                        } = &mut self.state;
                        let mut control = EmitterPublishControl {
                            fault_injection: self.fault_injection,
                            shutdown_rx: &mut *self.shutdown_rx,
                            stop_rx: &mut *self.stop_rx,
                            backoff,
                            give_up,
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
        failure: EmitterBatchFailure,
    ) {
        for batch in batches {
            self.handle_publish_error_batch(batch, reason.clone(), failure)
                .await;
        }
    }

    async fn handle_publish_error_batch(
        &self,
        batch: EmitterPublishBatch,
        reason: String,
        failure: EmitterBatchFailure,
    ) {
        self.fail_unresolved_rows(batch, reason, failure, None)
            .await;
    }

    /// Fails what the emitter gave up: every unresolved row of a given-up batch with an
    /// `unavailable` message error naming the latest failure, and the rows a connector staged for a
    /// commit it gave up through the node's general error policy, since the host no longer holds
    /// those rows and the failure names none of them.
    async fn fail_given_up(&self, given_up: GivenUpWork) {
        let GivenUpWork {
            batches,
            staged,
            message,
            external_code,
        } = given_up;
        let staged_commits = u64::from(!staged.is_empty());
        let batch_count: u64 = batches.len().arch_into();
        self.interactions.record_give_ups(
            batch_count
                .checked_add(staged_commits)
                .assured("a give-up counts the batches one emitter buffer held, and one commit"),
        );
        for batch in batches {
            self.fail_unresolved_rows(
                batch,
                message.clone(),
                EmitterBatchFailure::GIVEN_UP,
                external_code.clone(),
            )
            .await;
        }
        if !staged.is_empty() {
            let error = Report::new(EmitterRuntimeError::CommitGivenUp).attach_printable(message);
            self.runtime.handle_general_error_for_staged_acks(
                self.domain,
                &NodeRef::new(ModelKind::Emitter, self.emitter),
                self.error_policies,
                &staged,
                MessageErrorClass::UNAVAILABLE,
                &error,
            );
        }
    }

    /// Sends every row of `batch` an earlier attempt left unresolved to the emitter's message-error
    /// policy, each with the class and operation of `failure`, `reason` as its message and
    /// `external_code` as the outside system's code.
    async fn fail_unresolved_rows(
        &self,
        batch: EmitterPublishBatch,
        reason: String,
        failure: EmitterBatchFailure,
        external_code: Option<ExternalErrorCode>,
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
                    MessageErrorClass::INTERNAL,
                    &failure
                        .error
                        .change_context(EmitterRuntimeError::SplitFailedBatch {
                            publish_failure: reason,
                        }),
                );
                return;
            }
        };
        let mut outcomes = self.interactions.record_outcomes();
        for (row, message) in messages.into_iter().enumerate() {
            if resolved.get(row).copied().unwrap_or(false) {
                continue;
            }
            let mut error = raised_message_error(
                execution_now,
                failure.class,
                reason.clone(),
                failure.operation,
                None,
                std::iter::empty(),
            );
            error.external_code = external_code.clone();
            self.runtime
                .handle_structured_message_error(MessageErrorHandling {
                    routing: Some(&self.error_routing.load()),
                    domain: self.domain,
                    node_kind: ModelKind::Emitter,
                    node: self.node,
                    source_route: None,
                    policy: &self.error_policies.message,
                    message,
                    error,
                    partial_output: None,
                    materialized_state: HashMap::default(),
                    ingest_metadata: None,
                    execution_now,
                })
                .await;
            outcomes.fail();
        }
    }

    /// Report a failure that no single message owns, so the node-wide general error policy
    /// decides what happens to the acknowledgments the failed work was holding.
    fn report_general_error<'a, C: error_stack::Context>(
        &self,
        acks: impl IntoIterator<Item = &'a AckSet>,
        class: MessageErrorClass,
        error: &Report<C>,
    ) {
        self.runtime.handle_general_error_for_acks(
            self.domain,
            &NodeRef::new(ModelKind::Emitter, self.emitter),
            self.error_policies,
            acks,
            class,
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
                    stop: None,
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
                        self.remote_ack_timing,
                        input_relay,
                        &batch,
                        execution_now,
                        &materialized_values,
                    )
                    .await;
                match evaluated {
                    Ok(groups) => Some(groups),
                    Err(failure) => {
                        self.report_general_error(
                            failure.acks.iter(),
                            failure.class(),
                            &failure.error,
                        );
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
            self.remote_ack_timing,
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
                self.report_general_error(failure.acks.iter(), failure.class(), &failure.error);
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
                self.remote_ack_timing,
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
                self.report_general_error(failure.acks.iter(), failure.class(), &failure.error);
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
                self.remote_ack_timing,
                input,
                side_inputs,
                execution_now,
            )
            .await;
        match enveloped {
            Ok(publish_batch) => Some(publish_batch),
            Err(failure) => {
                self.report_general_error(failure.acks.iter(), failure.class(), &failure.error);
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
            MessageErrorClass::INTERNAL,
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
                ack_timing: self.remote_ack_timing,
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
                let class = failure.class();
                self.report_general_error(
                    failure.acks.iter(),
                    class,
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
mod tests;
