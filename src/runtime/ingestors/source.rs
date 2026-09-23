//! The host-owned broker source loop, the launcher that starts its instances, and its runtime
//! intake adapter.
//!
//! Layer: data plane.
//!
//! - **Owns.** Source task lifecycle, instance startup and registration, the acknowledgement
//!   policy a declared delivery mode parses into, quiesce and readiness, grouping policy, runtime
//!   decoding and dispatch, acknowledgement waiting, retry cadence, and connector error reporting.
//! - **Depends on.** The connector source contract, declared delivery modes, and pre-resolved
//!   runtime execution handles.
//! - **Must not know.** A broker driver, connector-specific configuration, NSPL parsing, registry
//!   validation, or placement computation.

use std::{future, time::Duration};

use async_trait::async_trait;
use error_stack::{FrameKind, Report, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, ClientResourceMounts, IngestMetadataRow, PacedSourceConnector,
    ParsedRetryPolicy, RetainedIngestHeaders, SourceAckPolicy, SourceAcknowledgement,
    SourceAcknowledgementOutcome, SourceAcknowledgementServices, SourceBatch, SourceBatchRequest,
    SourceCapabilities, SourceConnector, SourceError, SourceHost, SourceHostServices,
    SourceIntakeBatch, SourceIntakeError, SourceIntakeMessage, SourceIntakeMode,
    SourceIntakeOutcome, SourceIntakeResult, SourceMessage, SourcePlan, SourcePoll, SourceResume,
    next_retry_delay, physical_time::actual_utc_now,
};
use nervix_models::{NatsIngestMode, RedisPubSubIngestMode, ZeroMqIngestMode};
use tokio_util::sync::CancellationToken;

use super::super::{domain_clock::DomainCadence, *};

const SOURCE_ERROR_RETRY: Duration = Duration::from_millis(100);

/// How a source whose delivery mode declares no retry policy reopens after a failure: the delay
/// starts here and doubles up to the ceiling.
pub(super) const SOURCE_RECONNECT_POLICY: ParsedRetryPolicy = ParsedRetryPolicy {
    backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(30),
};

/// The acknowledgement a source's delivery mode declares, with its durations still as written.
///
/// The launcher parses it into the policy the source loop runs, so a mode that fails to parse
/// fails the start before any instance opens.
#[derive(Debug, Clone, Copy)]
pub(super) enum DeclaredSourceAcknowledgement<'a> {
    Unacknowledged,
    Sequential {
        timeout: &'a str,
        retry: &'a RetryPolicy,
    },
    Parallel {
        max: NonZeroU64,
        batch_timeout: &'a str,
        timeout: &'a str,
        retry: &'a RetryPolicy,
    },
}

/// Kafka and Pulsar declare their delivery modes through the same `KafkaIngestMode`.
impl<'a> From<&'a KafkaIngestMode> for DeclaredSourceAcknowledgement<'a> {
    fn from(mode: &'a KafkaIngestMode) -> Self {
        match mode {
            KafkaIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => Self::Parallel {
                max: *max,
                batch_timeout,
                timeout,
                retry: retry_policy,
            },
            KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::Sequential {
                timeout,
                retry: retry_policy,
            },
            KafkaIngestMode::NoAckParallel => Self::Unacknowledged,
        }
    }
}

impl IngestorSpec {
    /// The start failure this ingestor reports, naming why it could not start.
    pub(super) fn start_failure(&self, reason: impl Into<String>) -> RuntimeError {
        RuntimeError::StartIngestor {
            domain: self.domain.as_str().to_string(),
            ingestor: self.name.as_str().to_string(),
            reason: reason.into(),
        }
    }
}

impl<'a> From<&'a MqttIngestMode> for DeclaredSourceAcknowledgement<'a> {
    fn from(mode: &'a MqttIngestMode) -> Self {
        match mode {
            MqttIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => Self::Parallel {
                max: *max,
                batch_timeout,
                timeout,
                retry: retry_policy,
            },
            MqttIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::Sequential {
                timeout,
                retry: retry_policy,
            },
            MqttIngestMode::NoAckSequential { .. } | MqttIngestMode::NoAckParallel { .. } => {
                Self::Unacknowledged
            }
        }
    }
}

impl<'a> From<&'a RabbitMqIngestMode> for DeclaredSourceAcknowledgement<'a> {
    fn from(mode: &'a RabbitMqIngestMode) -> Self {
        match mode {
            RabbitMqIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::Sequential {
                timeout,
                retry: retry_policy,
            },
        }
    }
}

impl<'a> From<&'a SqsIngestMode> for DeclaredSourceAcknowledgement<'a> {
    fn from(mode: &'a SqsIngestMode) -> Self {
        match mode {
            SqsIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::Sequential {
                timeout,
                retry: retry_policy,
            },
        }
    }
}

impl From<&NatsIngestMode> for DeclaredSourceAcknowledgement<'_> {
    fn from(mode: &NatsIngestMode) -> Self {
        match mode {
            NatsIngestMode::NoAckSequential => Self::Unacknowledged,
        }
    }
}

impl From<&RedisPubSubIngestMode> for DeclaredSourceAcknowledgement<'_> {
    fn from(mode: &RedisPubSubIngestMode) -> Self {
        match mode {
            RedisPubSubIngestMode::NoAckSequential => Self::Unacknowledged,
        }
    }
}

impl From<&ZeroMqIngestMode> for DeclaredSourceAcknowledgement<'_> {
    fn from(mode: &ZeroMqIngestMode) -> Self {
        match mode {
            ZeroMqIngestMode::NoAckSequential => Self::Unacknowledged,
        }
    }
}

/// One broker source ready to start: its connector plan and the host settings it runs under.
pub(super) struct BrokerSourceStart<'a, P> {
    pub(super) ingestor: &'a IngestorSpec,
    pub(super) connector: P,
    pub(super) instances: NonZeroU64,
    pub(super) acknowledgement: DeclaredSourceAcknowledgement<'a>,
    /// Whether what the source reads while quiesced passes through the quiesce control, which may
    /// buffer or drop it.
    pub(super) buffered_intake: bool,
    /// Whether every accepted batch flushes its ingest group at once.
    pub(super) flush_each_intake: bool,
    /// The resource mounts the connector's resolved configuration reads its files from, held for
    /// as long as an instance runs.
    pub(super) client_mounts: Vec<Arc<ClientResourceMounts>>,
    /// The connector's name in the instance lifecycle log.
    pub(super) connector_label: &'static str,
}

impl Runtime {
    /// Starts every instance of one broker source under the host source loop.
    ///
    /// The delivery mode parses and every instance opens before anything is registered, so a
    /// source that cannot start leaves no running ingestor behind.
    pub(super) async fn start_broker_source<C>(
        &self,
        start: BrokerSourceStart<'_, C::Plan>,
    ) -> Result<(), RuntimeError>
    where
        C: BrokerSourceConnector,
    {
        let BrokerSourceStart {
            ingestor,
            connector,
            instances,
            acknowledgement,
            buffered_intake,
            flush_each_intake,
            client_mounts,
            connector_label,
        } = start;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if self.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }
        let acknowledgement = match acknowledgement {
            DeclaredSourceAcknowledgement::Unacknowledged => SourceAckPolicy::None,
            DeclaredSourceAcknowledgement::Sequential { timeout, retry } => {
                SourceAckPolicy::Sequential {
                    timeout: Self::parse_ack_timeout(domain, &ingestor.name, timeout)?,
                    retry: Self::parse_retry_policy(domain, &ingestor.name, retry)?,
                }
            }
            DeclaredSourceAcknowledgement::Parallel {
                max,
                batch_timeout,
                timeout,
                retry,
            } => SourceAckPolicy::Parallel {
                max_in_flight: addressable_count(max),
                batch_timeout: Self::parse_duration_setting(
                    domain,
                    &ingestor.name,
                    "batch timeout",
                    batch_timeout,
                )?,
                timeout: Self::parse_ack_timeout(domain, &ingestor.name, timeout)?,
                retry: Self::parse_retry_policy(domain, &ingestor.name, retry)?,
            },
        };
        // An acknowledged mode retries by its declared policy; a source without one reopens on
        // the host's reconnect cadence.
        let retry = match acknowledgement {
            SourceAckPolicy::None => SOURCE_RECONNECT_POLICY,
            SourceAckPolicy::Sequential { retry, .. } | SourceAckPolicy::Parallel { retry, .. } => {
                retry
            }
        };
        let plan = SourcePlan {
            connector,
            capabilities: SourceCapabilities::new(
                ingestor.allow_header_reads,
                ingestor.metadata_kind.source_scope(),
                ingestor.quiesce.supports(ingestor.quiesce.mode()),
                instances,
                acknowledgement.support(),
            ),
            acknowledgement,
        };
        let dependencies = self.ingestor_dependencies(domain, ingestor).await?;
        let mut sources = Vec::with_capacity(instances.get().arch_into());
        for instance_index in 0..instances.get() {
            tokio::task::consume_budget().await;
            let source = C::open(&plan.connector, instance_index)
                .await
                .map_err(|error| ingestor.start_failure(format!("{error:#}")))?;
            sources.push(source);
        }

        let branched_runtime = self.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let quiesce = self
            .ingestor_quiesce_control(domain, &ingestor.name)
            .verified(
                "the runtime registers quiesce control for an ingestor before it starts the task",
            );
        let (shutdown_tx, _) = watch::channel(false);
        self.prepare_ingestor_readiness(domain, &ingestor.name, plan.capabilities.instances());
        let mut tasks = Vec::with_capacity(sources.len());
        for (instance_index, source) in (0_u64..).zip(sources) {
            let host = RuntimeSourceHost::build(RuntimeSourceHostSpec {
                runtime: self.clone(),
                domain: domain.clone(),
                ingestor: ingestor.name.clone(),
                timestamp_source: ingestor.timestamp_source.clone(),
                output_routes: dependencies.output_routes.clone(),
                filter_where: dependencies.filter_where.clone(),
                codec: dependencies.codec.clone(),
                metrics: dependencies.metrics.clone(),
                branched_senders: branched_runtime.senders.clone(),
                quiesce: quiesce.clone(),
                shutdown: shutdown_tx.subscribe(),
                instance_index,
                metadata_kind: ingestor.metadata_kind,
                buffered_intake,
                flush_each_intake,
            });
            let shutdown = shutdown_tx.subscribe();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_client_mounts = client_mounts.clone();
            let acknowledgement = plan.acknowledgement;
            tasks.push(tokio::spawn(async move {
                let _client_mounts = task_client_mounts;
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    connector = connector_label,
                    instance = instance_index,
                    "started source ingestor instance"
                );
                run_source_instance_with_retry(source, host, acknowledgement, retry, shutdown)
                    .await;
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    connector = connector_label,
                    instance = instance_index,
                    "stopped source ingestor instance"
                );
            }));
        }

        self.inner.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks,
            },
        );
        Ok(())
    }
}

/// What a source failure says to an operator: the most specific cause beneath it.
///
/// The contract's own context names only the operation that failed, such as a resume. The cause a
/// connector reports beneath it, such as a refused connection or a conflicting client identity, is
/// what the ingestor's transient status shows.
fn source_failure_reason(error: &Report<SourceError>) -> String {
    let mut cause = None;
    for frame in error.frames() {
        if let FrameKind::Context(context) = frame.kind() {
            cause = Some(context);
        }
    }
    match cause {
        Some(cause) => cause.to_string(),
        None => error.to_string(),
    }
}

pub(super) struct RuntimeSourceHostSpec {
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) timestamp_source: Option<IngestTimestampSource>,
    pub(super) output_routes: RelayProcessorOutputsNode,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) codec: Arc<CompiledCodec>,
    pub(super) metrics: MessageMetricsHandle,
    pub(super) branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    pub(super) quiesce: Arc<IngestorQuiesceControl>,
    pub(super) shutdown: watch::Receiver<bool>,
    pub(super) instance_index: u64,
    pub(super) metadata_kind: IngestMetadataKind,
    pub(super) buffered_intake: bool,
    pub(super) flush_each_intake: bool,
}

pub(super) struct RuntimeSourceHost {
    runtime: Runtime,
    domain: DomainName,
    ingestor: IngestorName,
    timestamp_source: Option<IngestTimestampSource>,
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    codec: Arc<CompiledCodec>,
    metrics: MessageMetricsHandle,
    branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    quiesce: Arc<IngestorQuiesceControl>,
    ack_root_trackers: IngestorAckRootTrackers,
    shutdown: watch::Receiver<bool>,
    instance_index: u64,
    collector: IngestRouteCollector,
    buffered_intake: bool,
    flush_each_intake: bool,
}

impl RuntimeSourceHost {
    pub(super) fn build(spec: RuntimeSourceHostSpec) -> SourceHost {
        SourceHost::new(Self::new(spec))
    }

    pub(super) fn new(spec: RuntimeSourceHostSpec) -> Self {
        let ack_root_trackers = spec
            .runtime
            .ingestor_ack_root_trackers(&spec.domain, &spec.ingestor);
        let collector = IngestRouteCollector::new(
            spec.metadata_kind,
            INGEST_GROUP_MAX_ROWS,
            spec.metrics.clone(),
        );
        Self {
            runtime: spec.runtime,
            domain: spec.domain,
            ingestor: spec.ingestor,
            timestamp_source: spec.timestamp_source,
            output_routes: spec.output_routes,
            filter_where: spec.filter_where,
            codec: spec.codec,
            metrics: spec.metrics,
            branched_senders: spec.branched_senders,
            quiesce: spec.quiesce,
            ack_root_trackers,
            shutdown: spec.shutdown,
            instance_index: spec.instance_index,
            collector,
            buffered_intake: spec.buffered_intake,
            flush_each_intake: spec.flush_each_intake,
        }
    }

    async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
        for failure in poll.failures {
            tokio::task::consume_budget().await;
            self.report_error(format!(
                "source poll could not materialize one message: {failure:?}"
            ));
        }
        if poll.messages.is_empty() {
            return Ok(false);
        }
        let entries = poll
            .messages
            .into_iter()
            .map(|message| {
                (
                    message.payload,
                    BufferedIngestMetadata::Headers(message.headers),
                )
            })
            .collect();
        let payload = BufferedIngestPayload::batch(entries, poll.observed_at);
        let payload = match self.quiesce.intake(self.instance_index, payload, false) {
            IngestorQuiesceIntake::Dispatch(payload) => payload,
            IngestorQuiesceIntake::Buffered
            | IngestorQuiesceIntake::Dropped
            | IngestorQuiesceIntake::Rejected { .. } => return Ok(false),
        };
        self.dispatch_polled_payload(&payload).await?;
        Ok(true)
    }

    async fn replay_buffered_poll(&mut self) -> SourceIntakeResult<bool> {
        let Some(payload) = self.quiesce.pop_buffered(self.instance_index) else {
            return Ok(false);
        };
        self.dispatch_polled_payload(&payload).await?;
        Ok(true)
    }

    async fn dispatch_polled_payload(
        &mut self,
        payload: &BufferedIngestPayload,
    ) -> SourceIntakeResult<()> {
        self.runtime
            .dispatch_raw_ingest_payload(RawIngestDispatch {
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                branched_senders: &self.branched_senders,
                codec: self.codec.clone(),
                payload,
                collector: &mut self.collector,
                flush: false,
            })
            .await
            .change_context(SourceIntakeError::Dispatch)
    }

    fn should_skip_poll(&self) -> bool {
        self.quiesce.should_skip_poll()
    }

    fn record_poll_error(&self, reason: String) {
        self.runtime
            .record_ingestor_transient_error(&self.domain, &self.ingestor, reason.clone());
        self.report_error(reason);
    }
}

struct RuntimeSourceAcknowledgement {
    completion: AckCompletion,
    shutdown: watch::Receiver<bool>,
}

#[async_trait]
impl SourceAcknowledgementServices for RuntimeSourceAcknowledgement {
    async fn wait(mut self: Box<Self>, timeout: Duration) -> SourceAcknowledgementOutcome {
        match Runtime::await_ack_completion(&mut self.shutdown, self.completion, timeout).await {
            Some(AckOutcome::Ack) => SourceAcknowledgementOutcome::Ack,
            Some(AckOutcome::NoAck(reason)) => SourceAcknowledgementOutcome::NoAck(reason),
            None => SourceAcknowledgementOutcome::Shutdown,
        }
    }
}

/// What the quiesce control decided about one acknowledged message read while quiesced.
enum QuiescedIntake {
    /// The message dispatches with its own acknowledgement root, as it would unquiesced.
    Admitted,
    /// A buffer or the drop policy took the message over, so nothing downstream acknowledges it.
    TakenOver,
}

/// The acknowledgement of a message a quiesce buffer or drop policy took over from the source.
///
/// The source is told the message is done with at once: what a buffer retains is replayed without
/// it, and what the policy dropped was dropped deliberately.
struct QuiescedSourceAcknowledgement;

#[async_trait]
impl SourceAcknowledgementServices for QuiescedSourceAcknowledgement {
    async fn wait(self: Box<Self>, _timeout: Duration) -> SourceAcknowledgementOutcome {
        SourceAcknowledgementOutcome::Ack
    }
}

impl RuntimeSourceHost {
    /// The metadata a quiesce buffer keeps for a message after the source message is gone.
    fn retained_metadata(
        metadata: &IngestMetadataRow<'_>,
    ) -> SourceIntakeResult<BufferedIngestMetadata> {
        match metadata {
            IngestMetadataRow::Syslog { peer_addr } => Ok(BufferedIngestMetadata::Syslog {
                peer_addr: *peer_addr,
            }),
            IngestMetadataRow::Headers { headers } => Ok(BufferedIngestMetadata::Headers(
                RetainedIngestHeaders::capture(*headers),
            )),
            IngestMetadataRow::Kafka { .. } => Err(Report::new(SourceIntakeError::Retain)
                .attach_printable(
                    "Kafka source input supports suspension instead of quiesce buffering",
                )),
        }
    }

    /// Unacknowledged input read while the ingestor is quiesced enters the quiesce control as one
    /// payload, which buffers it, drops it, or admits it for dispatch.
    async fn retain_unacknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<SourceIntakeOutcome> {
        let mut entries = Vec::with_capacity(messages.len());
        for message in messages {
            tokio::task::consume_budget().await;
            let metadata = Self::retained_metadata(&message.metadata)?;
            entries.push((message.payload.to_vec(), metadata));
        }
        let payload = BufferedIngestPayload::batch(entries, actual_utc_now());
        if let IngestorQuiesceIntake::Dispatch(payload) =
            self.quiesce.intake(self.instance_index, payload, false)
        {
            self.dispatch_buffered_payload(&payload).await?;
        }
        Ok(SourceIntakeOutcome {
            acknowledgements: Vec::new(),
        })
    }

    /// Acknowledged input read while the ingestor is quiesced.
    ///
    /// The quiesce control decides each message on its own. A message it admits is dispatched
    /// with its acknowledgement root as it would be unquiesced; one it buffers or drops is
    /// acknowledged at once, because the buffer or the drop policy has taken it over from the
    /// source. The acknowledgements keep the batch's order.
    async fn retain_acknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<SourceIntakeOutcome> {
        let mut admitted = Vec::with_capacity(messages.len());
        let mut decisions = Vec::with_capacity(messages.len());
        for message in messages {
            tokio::task::consume_budget().await;
            let metadata = Self::retained_metadata(&message.metadata)?;
            let retained = BufferedIngestPayload::new(message.payload, metadata, actual_utc_now());
            match self.quiesce.intake(self.instance_index, retained, false) {
                IngestorQuiesceIntake::Dispatch(_) => {
                    admitted.push(message);
                    decisions.push(QuiescedIntake::Admitted);
                }
                IngestorQuiesceIntake::Buffered
                | IngestorQuiesceIntake::Dropped
                | IngestorQuiesceIntake::Rejected { .. } => {
                    decisions.push(QuiescedIntake::TakenOver);
                }
            }
        }
        let mut dispatched = Vec::new();
        if !admitted.is_empty() {
            let outcome = self
                .dispatch_batch(SourceIntakeBatch {
                    messages: admitted,
                    mode: SourceIntakeMode::Acknowledged,
                })
                .await?;
            dispatched = outcome.acknowledgements;
        }
        let mut dispatched = dispatched.into_iter();
        let mut acknowledgements = Vec::with_capacity(decisions.len());
        for decision in decisions {
            tokio::task::consume_budget().await;
            let acknowledgement = match decision {
                QuiescedIntake::Admitted => dispatched.next().verified(
                    "an acknowledged dispatch returns one acknowledgement per admitted message",
                ),
                QuiescedIntake::TakenOver => {
                    SourceAcknowledgement::new(QuiescedSourceAcknowledgement)
                }
            };
            acknowledgements.push(acknowledgement);
        }
        Ok(SourceIntakeOutcome { acknowledgements })
    }

    /// Decodes a batch into its ingest group and dispatches it, returning one acknowledgement per
    /// message for an acknowledged batch.
    async fn dispatch_batch(
        &mut self,
        batch: SourceIntakeBatch<'_>,
    ) -> SourceIntakeResult<SourceIntakeOutcome> {
        let acknowledged = batch.mode == SourceIntakeMode::Acknowledged;
        let row_bound = if acknowledged {
            batch.messages.len().max(1)
        } else {
            INGEST_GROUP_MAX_ROWS
        };
        let mut acknowledged_collector = acknowledged.then(|| {
            IngestRouteCollector::new(self.collector.kind, row_bound, self.metrics.clone())
        });
        let collector = match acknowledged_collector.as_mut() {
            Some(collector) => collector,
            None => &mut self.collector,
        };

        let mut metadata = Vec::with_capacity(batch.messages.len());
        for message in batch.messages {
            tokio::task::consume_budget().await;
            if let Err(error) = collector
                .decode_payload(&self.codec, std::borrow::Cow::Borrowed(message.payload))
                .await
            {
                collector.discard_undispatched_payloads();
                return Err(Report::new(error).change_context(SourceIntakeError::Decode));
            }
            metadata.push(message.metadata);
        }

        let mut roots = Vec::new();
        let mut completions = Vec::new();
        let acks = if acknowledged {
            roots.reserve(metadata.len());
            completions.reserve(metadata.len());
            let mut acks = Vec::with_capacity(metadata.len());
            for _ in &metadata {
                tokio::task::consume_budget().await;
                let (root, completion) = self.ack_root_trackers.tracked_root();
                if self.branched_senders.is_empty() {
                    acks.push(root.clone());
                } else {
                    acks.push(root.attached());
                }
                roots.push(root);
                completions.push(completion);
            }
            acks
        } else {
            metadata.iter().map(|_| AckSet::empty()).collect()
        };

        self.runtime
            .dispatch_ingested_records(IngestGroupDispatch {
                collector,
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                metadata: &metadata,
                ingested_at: actual_utc_now(),
                acks,
            })
            .await
            .change_context(SourceIntakeError::Dispatch)?;

        if acknowledged || self.flush_each_intake || collector.len() >= INGEST_GROUP_MAX_ROWS {
            self.runtime
                .flush_ingest_collector(
                    &self.domain,
                    &self.ingestor,
                    &self.branched_senders,
                    collector,
                )
                .await
                .change_context(SourceIntakeError::Flush)?;
        }

        for root in roots {
            root.ack_success();
        }
        let acknowledgements = completions
            .into_iter()
            .map(|completion| {
                SourceAcknowledgement::new(RuntimeSourceAcknowledgement {
                    completion,
                    shutdown: self.shutdown.clone(),
                })
            })
            .collect();
        Ok(SourceIntakeOutcome { acknowledgements })
    }
}

#[async_trait]
impl SourceHostServices for RuntimeSourceHost {
    async fn intake(
        &mut self,
        batch: SourceIntakeBatch<'_>,
    ) -> SourceIntakeResult<SourceIntakeOutcome> {
        if self.buffered_intake && self.quiesce.is_quiesced() {
            return match batch.mode {
                SourceIntakeMode::Unacknowledged => {
                    self.retain_unacknowledged(batch.messages).await
                }
                SourceIntakeMode::Acknowledged => self.retain_acknowledged(batch.messages).await,
            };
        }
        self.dispatch_batch(batch).await
    }

    async fn flush(&mut self) -> SourceIntakeResult<()> {
        self.runtime
            .flush_ingest_collector(
                &self.domain,
                &self.ingestor,
                &self.branched_senders,
                &mut self.collector,
            )
            .await
            .change_context(SourceIntakeError::Flush)
    }

    async fn replay_buffered(&mut self) -> SourceIntakeResult<bool> {
        let Some(payload) = self.quiesce.pop_buffered(self.instance_index) else {
            return Ok(false);
        };
        self.dispatch_buffered_payload(&payload).await?;
        Ok(true)
    }

    fn next_flush(&self) -> Option<Instant> {
        self.collector.next_flush()
    }

    fn should_suspend_intake(&self) -> bool {
        self.quiesce.should_suspend_intake()
    }

    async fn wait_for_quiesce_change(&mut self) {
        self.quiesce.wait_for_change().await;
    }

    async fn wait_until_not_suspended(&mut self) {
        self.quiesce.wait_until_not_suspended().await;
    }

    async fn wait_until_active(&mut self) -> bool {
        loop {
            tokio::task::consume_budget().await;
            if !self
                .runtime
                .inner
                .fault_injection
                .ingestor_is_failed(&self.ingestor)
            {
                return true;
            }
            if self
                .runtime
                .wait_if_ingestor_faulted(&self.domain, &self.ingestor, &mut self.shutdown)
                .await
            {
                return false;
            }
        }
    }

    fn mark_ready(&self) {
        self.runtime.mark_ingestor_instance_ready(
            &self.domain,
            &self.ingestor,
            self.instance_index,
        );
    }

    fn mark_unready(&self) {
        self.runtime.mark_ingestor_instance_unready(
            &self.domain,
            &self.ingestor,
            self.instance_index,
        );
    }

    fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.runtime.record_ingestor_transient_error_with_backoff(
            &self.domain,
            &self.ingestor,
            reason,
            retry_after,
        );
    }

    fn clear_transient_error(&self) {
        self.runtime
            .clear_ingestor_transient_error(&self.domain, &self.ingestor);
    }

    fn report_error(&self, message: String) {
        self.runtime.events().report_error(format!(
            "source error for ingestor '{}' in domain '{}': {message}",
            self.ingestor.as_str(),
            self.domain.as_str(),
        ));
    }

    fn handle_ack_failure(&self, reason: String) {
        self.runtime.events().report_error(format!(
            "source ACK chain failed for ingestor '{}' in domain '{}': {reason}",
            self.ingestor.as_str(),
            self.domain.as_str(),
        ));
    }
}

impl RuntimeSourceHost {
    async fn dispatch_buffered_payload(
        &mut self,
        payload: &BufferedIngestPayload,
    ) -> SourceIntakeResult<()> {
        self.dispatch_polled_payload(payload).await?;
        if self.flush_each_intake || self.collector.len() >= INGEST_GROUP_MAX_ROWS {
            self.flush().await?;
        }
        Ok(())
    }
}

trait PacedSourceHostServices: SourceHostServices {
    fn record_poll_error(&self, reason: String);
}

impl PacedSourceHostServices for RuntimeSourceHost {
    fn record_poll_error(&self, reason: String) {
        RuntimeSourceHost::record_poll_error(self, reason);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PacedSourceAction {
    Poll,
    Restart,
    Stop,
}

async fn prepare_paced_source<C, H>(
    source: &mut C,
    host: &mut H,
    ready: &mut bool,
    shutdown: &mut watch::Receiver<bool>,
) -> PacedSourceAction
where
    C: SourceConnector,
    H: PacedSourceHostServices,
{
    if !host.wait_until_active().await {
        return PacedSourceAction::Stop;
    }
    if host.should_suspend_intake() {
        flush_paced_source(host).await;
        if *ready {
            if let Err(error) = source.suspend().await {
                host.report_error(error.to_string());
            }
            *ready = false;
            host.mark_unready();
        }
        let keep_running = tokio::select! {
            changed = shutdown.changed() => !(changed.is_err() || *shutdown.borrow()),
            _ = host.wait_until_not_suspended() => true,
        };
        return if keep_running {
            PacedSourceAction::Restart
        } else {
            PacedSourceAction::Stop
        };
    }

    if !*ready || source.needs_resume() {
        flush_paced_source(host).await;
        match source.resume().await {
            Ok(SourceResume::Ready) => {
                *ready = true;
                host.mark_ready();
                host.clear_transient_error();
            }
            Ok(SourceResume::Waiting { retry_after }) => {
                *ready = false;
                host.mark_unready();
                return if wait_for_paced_retry(host, shutdown, retry_after).await {
                    PacedSourceAction::Restart
                } else {
                    PacedSourceAction::Stop
                };
            }
            Err(error) => {
                *ready = false;
                host.mark_unready();
                host.record_poll_error(error.to_string());
                return if wait_for_paced_retry(host, shutdown, SOURCE_ERROR_RETRY).await {
                    PacedSourceAction::Restart
                } else {
                    PacedSourceAction::Stop
                };
            }
        }
    }

    PacedSourceAction::Poll
}

pub(super) async fn run_paced_source<C>(
    mut source: C,
    mut host: RuntimeSourceHost,
    mut cadence: DomainCadence,
    mut shutdown: watch::Receiver<bool>,
) where
    C: PacedSourceConnector,
{
    let cadence_cancellation = CancellationToken::new();
    let mut ready = false;

    loop {
        tokio::task::consume_budget().await;
        match prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await {
            PacedSourceAction::Poll => {}
            PacedSourceAction::Restart => continue,
            PacedSourceAction::Stop => break,
        }

        match host.replay_buffered_poll().await {
            Ok(true) => {
                flush_paced_source(&mut host).await;
                continue;
            }
            Ok(false) => {}
            Err(error) => host.report_error(error.to_string()),
        }

        let occurrence = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            _ = host.wait_for_quiesce_change() => {
                continue;
            }
            occurrence = cadence.next(&cadence_cancellation) => occurrence,
        };
        let occurrence = match occurrence {
            Ok(occurrence) => occurrence,
            Err(error) => {
                host.report_error(format!("could not advance source cadence: {error}"));
                break;
            }
        };
        if host.should_skip_poll() {
            continue;
        }

        let poll = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            _ = host.wait_for_quiesce_change() => {
                continue;
            }
            poll = source.poll(occurrence.due_at()) => poll,
        };
        let poll = match poll {
            Ok(poll) => poll,
            Err(error) => {
                host.record_poll_error(error.to_string());
                continue;
            }
        };
        host.clear_transient_error();
        match host.intake_poll(poll).await {
            Ok(true) => flush_paced_source(&mut host).await,
            Ok(false) => {}
            Err(error) => host.report_error(error.to_string()),
        }
    }

    flush_paced_source(&mut host).await;
    if let Err(error) = source.close().await {
        host.report_error(error.to_string());
    }
    host.mark_unready();
}

async fn flush_paced_source<H>(host: &mut H)
where
    H: SourceHostServices,
{
    if let Err(error) = host.flush().await {
        host.report_error(error.to_string());
    }
}

async fn wait_for_paced_retry<H>(
    host: &mut H,
    shutdown: &mut watch::Receiver<bool>,
    delay: Duration,
) -> bool
where
    H: SourceHostServices,
{
    tokio::select! {
        changed = shutdown.changed() => !(changed.is_err() || *shutdown.borrow()),
        _ = host.wait_for_quiesce_change() => true,
        _ = sleep(delay) => true,
    }
}

enum BatchDisposition {
    Accepted,
    Retry,
    Shutdown,
}

pub(super) async fn run_source_instance<C>(
    source: C,
    host: SourceHost,
    acknowledgement: SourceAckPolicy,
    shutdown: watch::Receiver<bool>,
) where
    C: BrokerSourceConnector,
{
    run_source_instance_with_retry(
        source,
        host,
        acknowledgement,
        acknowledgement.retry(),
        shutdown,
    )
    .await;
}

pub(super) async fn run_source_instance_with_retry<C>(
    mut source: C,
    mut host: SourceHost,
    acknowledgement: SourceAckPolicy,
    retry_policy: ParsedRetryPolicy,
    mut shutdown: watch::Receiver<bool>,
) where
    C: BrokerSourceConnector,
{
    let mut retry_delay = retry_policy.backoff;
    let mut ready = false;

    loop {
        tokio::task::consume_budget().await;
        if !host.wait_until_active().await {
            break;
        }
        match host.replay_buffered().await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => host.report_error(format!("{error:#}")),
        }
        if host.should_suspend_intake() {
            flush_for_lifecycle(&mut host).await;
            if ready {
                if let Err(error) = source.suspend().await {
                    host.report_error(format!("{error:#}"));
                }
                ready = false;
                host.mark_unready();
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = host.wait_until_not_suspended() => {}
            }
            continue;
        }

        if !ready || source.needs_resume() {
            flush_for_lifecycle(&mut host).await;
            match source.resume().await {
                Ok(SourceResume::Ready) => {
                    ready = true;
                    retry_delay = retry_policy.backoff;
                    host.mark_ready();
                    host.clear_transient_error();
                }
                Ok(SourceResume::Waiting { retry_after }) => {
                    ready = false;
                    host.mark_unready();
                    if !wait_for_retry(&mut host, &mut shutdown, retry_after).await {
                        break;
                    }
                    continue;
                }
                Err(error) => {
                    ready = false;
                    host.mark_unready();
                    let delay = source_retry_delay(retry_delay);
                    host.record_transient_error(source_failure_reason(&error), delay);
                    host.report_error(format!("{error:#}"));
                    if !wait_for_retry(&mut host, &mut shutdown, delay).await {
                        break;
                    }
                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                    continue;
                }
            }
        }

        let request = batch_request(acknowledgement);
        let next_flush = host.next_flush();
        let batch = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            _ = host.wait_for_quiesce_change() => {
                continue;
            }
            _ = wait_for_flush(next_flush) => {
                flush_for_lifecycle(&mut host).await;
                continue;
            }
            batch = source.next_batch(request) => batch,
        };

        let messages = match batch {
            Ok(SourceBatch::Messages(messages)) => messages,
            Ok(SourceBatch::ResumeRequired) => {
                ready = false;
                host.mark_unready();
                continue;
            }
            Ok(SourceBatch::Closed) => break,
            Err(error) => {
                host.record_transient_error(source_failure_reason(&error), SOURCE_ERROR_RETRY);
                host.report_error(format!("{error:#}"));
                if !wait_for_retry(&mut host, &mut shutdown, SOURCE_ERROR_RETRY).await {
                    break;
                }
                continue;
            }
        };
        if messages.is_empty() {
            host.report_error("source returned an empty message batch".to_string());
            continue;
        }
        host.clear_transient_error();

        match handle_batch(&mut source, &mut host, acknowledgement, messages).await {
            BatchDisposition::Accepted => {
                retry_delay = retry_policy.backoff;
            }
            BatchDisposition::Retry => {
                let delay = source_retry_delay(retry_delay);
                if !wait_for_retry(&mut host, &mut shutdown, delay).await {
                    break;
                }
                retry_delay = next_retry_delay(retry_delay, retry_policy);
            }
            BatchDisposition::Shutdown => break,
        }
    }

    flush_for_lifecycle(&mut host).await;
    if let Err(error) = source.close().await {
        host.report_error(format!("{error:#}"));
    }
    host.mark_unready();
}

fn batch_request(acknowledgement: SourceAckPolicy) -> SourceBatchRequest {
    match acknowledgement {
        SourceAckPolicy::None | SourceAckPolicy::Sequential { .. } => SourceBatchRequest {
            max_messages: NonZeroUsize::MIN,
            batch_timeout: None,
        },
        SourceAckPolicy::Parallel {
            max_in_flight,
            batch_timeout,
            ..
        } => SourceBatchRequest {
            max_messages: max_in_flight,
            batch_timeout: Some(batch_timeout),
        },
    }
}

async fn handle_batch<C>(
    source: &mut C,
    host: &mut SourceHost,
    acknowledgement: SourceAckPolicy,
    messages: Vec<C::Message>,
) -> BatchDisposition
where
    C: BrokerSourceConnector,
{
    let positions = messages
        .iter()
        .map(|message| message.position().clone())
        .collect::<Vec<_>>();
    let intake_messages = messages
        .iter()
        .map(|message| SourceIntakeMessage {
            payload: message.payload(),
            metadata: message.metadata(),
        })
        .collect();
    let mode = match acknowledgement {
        SourceAckPolicy::None => SourceIntakeMode::Unacknowledged,
        SourceAckPolicy::Sequential { .. } | SourceAckPolicy::Parallel { .. } => {
            SourceIntakeMode::Acknowledged
        }
    };
    let intake = host
        .intake(SourceIntakeBatch {
            messages: intake_messages,
            mode,
        })
        .await;
    let outcome = match intake {
        Ok(outcome) => outcome,
        Err(error) => {
            host.report_error(format!("{error:#}"));
            if mode == SourceIntakeMode::Acknowledged {
                reject_batch(source, host, &positions).await;
                return BatchDisposition::Retry;
            }
            return BatchDisposition::Accepted;
        }
    };

    let timeout = match acknowledgement {
        SourceAckPolicy::None => None,
        SourceAckPolicy::Sequential { timeout, .. } | SourceAckPolicy::Parallel { timeout, .. } => {
            Some(timeout)
        }
    };
    if let Some(timeout) = timeout {
        if outcome.acknowledgements.len() != positions.len() {
            host.report_error(format!(
                "source intake returned {} ACKs for {} messages",
                outcome.acknowledgements.len(),
                positions.len(),
            ));
            reject_batch(source, host, &positions).await;
            return BatchDisposition::Retry;
        }
        for acknowledgement in outcome.acknowledgements {
            tokio::task::consume_budget().await;
            match acknowledgement.wait(timeout).await {
                SourceAcknowledgementOutcome::Ack => {}
                SourceAcknowledgementOutcome::NoAck(reason) => {
                    host.handle_ack_failure(reason);
                    reject_batch(source, host, &positions).await;
                    return BatchDisposition::Retry;
                }
                SourceAcknowledgementOutcome::Shutdown => return BatchDisposition::Shutdown,
            }
        }
    } else if !outcome.acknowledgements.is_empty() {
        host.report_error("unacknowledged source intake returned ACK handles".to_string());
    }

    if let Err(error) = source.acknowledge(&positions).await {
        host.report_error(format!("{error:#}"));
        reject_batch(source, host, &positions).await;
        return BatchDisposition::Retry;
    }
    BatchDisposition::Accepted
}

async fn reject_batch<C>(source: &mut C, host: &mut SourceHost, positions: &[C::Position])
where
    C: BrokerSourceConnector,
{
    if let Err(error) = source.reject(positions).await {
        host.report_error(format!("{error:#}"));
    }
}

async fn flush_for_lifecycle(host: &mut SourceHost) {
    if let Err(error) = host.flush().await {
        host.report_error(format!("{error:#}"));
    }
}

fn source_retry_delay(delay: Duration) -> Duration {
    delay.max(SOURCE_ERROR_RETRY)
}

async fn wait_for_retry(
    host: &mut SourceHost,
    shutdown: &mut watch::Receiver<bool>,
    delay: Duration,
) -> bool {
    tokio::select! {
        changed = shutdown.changed() => !(changed.is_err() || *shutdown.borrow()),
        _ = host.wait_for_quiesce_change() => true,
        _ = sleep(delay) => true,
    }
}

async fn wait_for_flush(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, future};

    use nervix_connector::{
        IngestMessageHeaders, IngestMetadataRow, SourceConnector, SourceError, SourceResult,
    };
    use parking_lot::Mutex;

    use super::*;

    #[derive(Default)]
    struct SourceLoopObservations {
        requests: Vec<SourceBatchRequest>,
        intake: Vec<(SourceIntakeMode, usize)>,
        acknowledged: Vec<Vec<u64>>,
        rejected: Vec<Vec<u64>>,
        poll_errors: Vec<String>,
        reported_errors: Vec<String>,
        ack_waits: usize,
        resumes: usize,
        suspends: usize,
        flushes: usize,
        quiesce_waits: usize,
        suspension_waits: usize,
        transient_clears: usize,
        ready: usize,
        unready: usize,
    }

    struct FakeMessage {
        position: u64,
        payload: Vec<u8>,
    }

    impl IngestMessageHeaders for FakeMessage {
        fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
    }

    impl SourceMessage for FakeMessage {
        type Position = u64;

        fn payload(&self) -> &[u8] {
            &self.payload
        }

        fn position(&self) -> &Self::Position {
            &self.position
        }

        fn headers(&self) -> &dyn IngestMessageHeaders {
            self
        }

        fn metadata(&self) -> IngestMetadataRow<'_> {
            IngestMetadataRow::Headers { headers: self }
        }
    }

    struct FakeSource {
        messages: VecDeque<FakeMessage>,
        resume_required: bool,
        resume_results: VecDeque<SourceResult<SourceResume>>,
        observations: Arc<Mutex<SourceLoopObservations>>,
    }

    #[async_trait]
    impl SourceConnector for FakeSource {
        type Plan = ();

        async fn open(_plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
            Err(Report::new(SourceError::Open { connector: "fake" }))
        }

        async fn resume(&mut self) -> SourceResult<SourceResume> {
            self.observations.lock().resumes += 1;
            match self.resume_results.pop_front() {
                Some(result) => result,
                None => Ok(SourceResume::Ready),
            }
        }

        async fn suspend(&mut self) -> SourceResult<()> {
            self.observations.lock().suspends += 1;
            Ok(())
        }
    }

    #[async_trait]
    impl BrokerSourceConnector for FakeSource {
        type Message = FakeMessage;
        type Position = u64;

        async fn next_batch(
            &mut self,
            request: SourceBatchRequest,
        ) -> SourceResult<SourceBatch<Self::Message>> {
            self.observations.lock().requests.push(request);
            if self.resume_required {
                self.resume_required = false;
                return Ok(SourceBatch::ResumeRequired);
            }
            if self.messages.is_empty() {
                return Ok(SourceBatch::Closed);
            }
            let count = self.messages.len().min(request.max_messages.get());
            let mut messages = Vec::with_capacity(count);
            for _ in 0..count {
                messages.push(
                    self.messages
                        .pop_front()
                        .verified("the count is bounded by the queue length above"),
                );
            }
            Ok(SourceBatch::Messages(messages))
        }

        async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
            self.observations
                .lock()
                .acknowledged
                .push(positions.to_vec());
            Ok(())
        }

        async fn reject(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
            self.observations.lock().rejected.push(positions.to_vec());
            Ok(())
        }
    }

    struct ImmediateAcknowledgement {
        observations: Arc<Mutex<SourceLoopObservations>>,
    }

    #[async_trait]
    impl SourceAcknowledgementServices for ImmediateAcknowledgement {
        async fn wait(self: Box<Self>, _timeout: Duration) -> SourceAcknowledgementOutcome {
            self.observations.lock().ack_waits += 1;
            SourceAcknowledgementOutcome::Ack
        }
    }

    struct FakeHost {
        observations: Arc<Mutex<SourceLoopObservations>>,
        suspend_intake: bool,
        wake_quiesce: bool,
        wake_suspension: bool,
        active: bool,
    }

    impl FakeHost {
        fn running(observations: Arc<Mutex<SourceLoopObservations>>) -> Self {
            Self {
                observations,
                suspend_intake: false,
                wake_quiesce: false,
                wake_suspension: false,
                active: true,
            }
        }
    }

    #[async_trait]
    impl SourceHostServices for FakeHost {
        async fn intake(
            &mut self,
            batch: SourceIntakeBatch<'_>,
        ) -> SourceIntakeResult<SourceIntakeOutcome> {
            let count = batch.messages.len();
            self.observations.lock().intake.push((batch.mode, count));
            let acknowledgements = match batch.mode {
                SourceIntakeMode::Unacknowledged => Vec::new(),
                SourceIntakeMode::Acknowledged => (0..count)
                    .map(|_| {
                        SourceAcknowledgement::new(ImmediateAcknowledgement {
                            observations: self.observations.clone(),
                        })
                    })
                    .collect(),
            };
            Ok(SourceIntakeOutcome { acknowledgements })
        }

        async fn flush(&mut self) -> SourceIntakeResult<()> {
            self.observations.lock().flushes += 1;
            Ok(())
        }

        async fn replay_buffered(&mut self) -> SourceIntakeResult<bool> {
            Ok(false)
        }

        fn next_flush(&self) -> Option<Instant> {
            None
        }

        fn should_suspend_intake(&self) -> bool {
            self.suspend_intake
        }

        async fn wait_for_quiesce_change(&mut self) {
            if self.wake_quiesce {
                self.observations.lock().quiesce_waits += 1;
                return;
            }
            future::pending().await
        }

        async fn wait_until_not_suspended(&mut self) {
            if self.wake_suspension {
                self.observations.lock().suspension_waits += 1;
                return;
            }
            future::pending().await
        }

        async fn wait_until_active(&mut self) -> bool {
            self.active
        }

        fn mark_ready(&self) {
            self.observations.lock().ready += 1;
        }

        fn mark_unready(&self) {
            self.observations.lock().unready += 1;
        }

        fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}
        fn clear_transient_error(&self) {
            self.observations.lock().transient_clears += 1;
        }
        fn report_error(&self, message: String) {
            self.observations.lock().reported_errors.push(message);
        }
        fn handle_ack_failure(&self, reason: String) {
            self.observations.lock().reported_errors.push(reason);
        }
    }

    impl PacedSourceHostServices for FakeHost {
        fn record_poll_error(&self, reason: String) {
            self.observations.lock().poll_errors.push(reason.clone());
            self.report_error(reason);
        }
    }

    async fn run_policy_with_refresh(
        policy: SourceAckPolicy,
        resume_required: bool,
    ) -> Arc<Mutex<SourceLoopObservations>> {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let source = FakeSource {
            messages: (0..3)
                .map(|position| FakeMessage {
                    position,
                    payload: vec![
                        u8::try_from(position).verified("the test positions are all below 256"),
                    ],
                })
                .collect(),
            resume_required,
            resume_results: VecDeque::new(),
            observations: observations.clone(),
        };
        let host = SourceHost::new(FakeHost::running(observations.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        run_source_instance(source, host, policy, shutdown_rx).await;
        drop(shutdown_tx);
        observations
    }

    async fn run_policy(policy: SourceAckPolicy) -> Arc<Mutex<SourceLoopObservations>> {
        run_policy_with_refresh(policy, false).await
    }

    fn retry_policy() -> nervix_connector::ParsedRetryPolicy {
        nervix_connector::ParsedRetryPolicy {
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        }
    }

    fn empty_source(observations: Arc<Mutex<SourceLoopObservations>>) -> FakeSource {
        FakeSource {
            messages: VecDeque::new(),
            resume_required: false,
            resume_results: VecDeque::new(),
            observations,
        }
    }

    #[tokio::test]
    async fn paced_source_preparation_suspends_a_ready_source_until_quiesce_releases() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let mut source = empty_source(observations.clone());
        let mut host = FakeHost::running(observations.clone());
        host.suspend_intake = true;
        host.wake_suspension = true;
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let mut ready = true;

        let action = prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await;

        assert_eq!(action, PacedSourceAction::Restart);
        assert!(!ready);
        let observations = observations.lock();
        assert_eq!(observations.flushes, 1);
        assert_eq!(observations.suspends, 1);
        assert_eq!(observations.suspension_waits, 1);
        assert_eq!(observations.unready, 1);
    }

    #[tokio::test]
    async fn paced_source_preparation_retries_resume_until_the_source_is_ready() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let mut source = empty_source(observations.clone());
        source.resume_results = VecDeque::from([
            Ok(SourceResume::Waiting {
                retry_after: Duration::from_secs(60),
            }),
            Err(Report::new(SourceError::Resume { connector: "fake" })),
            Ok(SourceResume::Ready),
        ]);
        let mut host = FakeHost::running(observations.clone());
        host.wake_quiesce = true;
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let mut ready = false;

        assert_eq!(
            prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
            PacedSourceAction::Restart
        );
        assert_eq!(
            prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
            PacedSourceAction::Restart
        );
        assert_eq!(
            prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
            PacedSourceAction::Poll
        );

        assert!(ready);
        let observations = observations.lock();
        assert_eq!(observations.resumes, 3);
        assert_eq!(observations.flushes, 3);
        assert_eq!(observations.quiesce_waits, 2);
        assert_eq!(observations.ready, 1);
        assert_eq!(observations.unready, 2);
        assert_eq!(observations.transient_clears, 1);
        assert_eq!(observations.poll_errors.len(), 1);
        assert_eq!(observations.reported_errors.len(), 1);
    }

    #[tokio::test]
    async fn paced_source_preparation_stops_when_the_host_is_inactive() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let mut source = empty_source(observations.clone());
        let mut host = FakeHost::running(observations.clone());
        host.active = false;
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let mut ready = false;

        assert_eq!(
            prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
            PacedSourceAction::Stop
        );
        assert_eq!(observations.lock().resumes, 0);
    }

    #[tokio::test]
    async fn paced_source_preparation_stops_suspended_intake_after_shutdown_closes() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let mut source = empty_source(observations.clone());
        let mut host = FakeHost::running(observations.clone());
        host.suspend_intake = true;
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        drop(shutdown_tx);
        let mut ready = false;

        assert_eq!(
            prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
            PacedSourceAction::Stop
        );
        assert_eq!(observations.lock().flushes, 1);
    }

    #[tokio::test]
    async fn source_loop_accepts_no_ack_messages_one_at_a_time() {
        let observations = run_policy(SourceAckPolicy::None).await;
        let observations = observations.lock();
        assert_eq!(
            observations.intake,
            vec![(SourceIntakeMode::Unacknowledged, 1); 3]
        );
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
        assert_eq!(observations.ack_waits, 0);
        assert!(observations.rejected.is_empty());
    }

    #[tokio::test]
    async fn source_loop_waits_and_commits_each_sequential_ack() {
        let observations = run_policy(SourceAckPolicy::Sequential {
            timeout: Duration::from_secs(1),
            retry: retry_policy(),
        })
        .await;
        let observations = observations.lock();
        assert_eq!(
            observations.intake,
            vec![(SourceIntakeMode::Acknowledged, 1); 3]
        );
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
        assert_eq!(observations.ack_waits, 3);
        assert!(observations.rejected.is_empty());
    }

    #[tokio::test]
    async fn source_loop_commits_one_parallel_ack_batch() {
        let observations = run_policy(SourceAckPolicy::Parallel {
            max_in_flight: NonZeroUsize::new(3).verified("three is a nonzero source batch bound"),
            batch_timeout: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            retry: retry_policy(),
        })
        .await;
        let observations = observations.lock();
        assert_eq!(
            observations.intake,
            vec![(SourceIntakeMode::Acknowledged, 3)]
        );
        assert_eq!(observations.acknowledged, vec![vec![0, 1, 2]]);
        assert_eq!(observations.ack_waits, 3);
        assert_eq!(observations.requests[0].max_messages.get(), 3);
        assert_eq!(
            observations.requests[0].batch_timeout,
            Some(Duration::from_millis(10))
        );
        assert!(observations.rejected.is_empty());
    }

    #[tokio::test]
    async fn source_loop_resumes_after_connector_configuration_changes() {
        let observations = run_policy_with_refresh(SourceAckPolicy::None, true).await;
        let observations = observations.lock();
        assert_eq!(observations.resumes, 2);
        assert_eq!(observations.ready, 2);
        assert_eq!(observations.unready, 2);
        assert_eq!(observations.intake.len(), 3);
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
    }
}
