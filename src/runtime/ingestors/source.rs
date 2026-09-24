//! The host side of every source: the one registration a composed source goes through, the loop
//! of each source family, and the runtime intake adapter those loops drive.
//!
//! Layer: data plane.
//!
//! - **Owns.** Opening broker and paced instances, registering a composed source with its branch
//!   runtimes, readiness and tasks, the acknowledgement policy a declared delivery mode parses
//!   into, the broker, paced and request-scoped loops with their quiesce and readiness handling,
//!   grouping policy, runtime decoding and dispatch, acknowledgement waiting, retry cadence, and
//!   connector error reporting.
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
    SourceAcknowledgementOutcome, SourceAcknowledgementServices, SourceAcknowledgementSupport,
    SourceBatch, SourceBatchRequest, SourceCapabilities, SourceConnector, SourceError, SourceHost,
    SourceHostServices, SourceIntakeBatch, SourceIntakeError, SourceIntakeMessage,
    SourceIntakeMode, SourceIntakeOutcome, SourceIntakeResult, SourceMessage, SourcePlan,
    SourcePoll, SourceResume, next_retry_delay, physical_time::actual_utc_now,
};
use nervix_models::{DomainClockPeriod, IngestAcknowledgement};
use tokio_util::sync::CancellationToken;

use super::super::{
    domain_clock::{DomainCadence, DomainClockWaitResult},
    *,
};
use crate::runtime::ingestor_quiesce::IngestorQuiesceObservation;

const SOURCE_ERROR_RETRY: Duration = Duration::from_millis(100);

/// How a source whose delivery mode declares no retry policy reopens after a failure: the delay
/// starts here and doubles up to the ceiling.
pub(super) const SOURCE_RECONNECT_POLICY: ParsedRetryPolicy = ParsedRetryPolicy {
    backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(30),
};

impl IngestorSpec {
    /// The identity this ingestor's running source is registered under.
    pub(super) fn runtime_key(&self) -> DomainNodeRef {
        DomainNodeRef::node_in(self.domain.clone(), ModelKind::Ingestor, self.name.clone())
    }

    /// The start failure this ingestor reports, naming why it could not start.
    pub(super) fn start_failure(&self, reason: impl Into<String>) -> RuntimeError {
        RuntimeError::StartIngestor {
            domain: self.domain.as_str().to_string(),
            ingestor: self.name.as_str().to_string(),
            reason: reason.into(),
        }
    }

    /// The capabilities this ingestor's source runs with, derived from the source vocabulary.
    pub(super) fn source_capabilities(
        &self,
        instances: NonZeroU64,
        acknowledgement: SourceAcknowledgementSupport,
    ) -> SourceCapabilities {
        SourceCapabilities::new(
            self.allow_header_reads,
            self.metadata_kind.source_scope(),
            self.quiesce.supports(self.quiesce.mode()),
            instances,
            acknowledgement,
        )
    }
}

/// One opened source instance, which the host starts under the loop of its source family.
pub(super) trait SourceInstance: Send + 'static {
    /// Attaches the instance to its host and returns the loop that runs it until shutdown.
    ///
    /// The host calls this before the ingestor's start returns, so whatever the instance attaches
    /// here is in place by the time the ingestor counts as started.
    fn start(
        self: Box<Self>,
        host: RuntimeSourceHost,
        shutdown: watch::Receiver<bool>,
    ) -> BoxFuture<'static, ()>;
}

/// A task a source runs beside its instances for as long as the source runs.
pub(super) trait SourceCompanion: Send + 'static {
    /// Returns the task, which ends at shutdown.
    fn start(self: Box<Self>, shutdown: watch::Receiver<bool>) -> BoxFuture<'static, ()>;
}

/// One source composed from its plan: the instances its connector opened and how the host runs
/// them.
pub(super) struct SourceStart {
    /// Every opened instance, in instance order.
    pub(super) instances: Vec<Box<dyn SourceInstance>>,
    /// Tasks the source runs beside its instances, such as the watch that tells domain-offset
    /// Kafka instances their topic's partitions changed.
    pub(super) companions: Vec<Box<dyn SourceCompanion>>,
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

/// One broker source ready to open: its connector plan and the host settings it runs under.
pub(super) struct BrokerSourceStart<'a, P> {
    pub(super) connector: P,
    pub(super) instances: NonZeroU64,
    pub(super) acknowledgement: IngestAcknowledgement<'a>,
    pub(super) buffered_intake: bool,
    pub(super) flush_each_intake: bool,
    pub(super) client_mounts: Vec<Arc<ClientResourceMounts>>,
    pub(super) connector_label: &'static str,
}

impl<P> BrokerSourceStart<'_, P> {
    /// Parses the declared delivery mode and opens every instance to run under the broker loop.
    ///
    /// Nothing is registered here, so a mode that fails to parse or an instance that fails to open
    /// leaves no running ingestor behind.
    pub(super) async fn open<C>(self, ingestor: &IngestorSpec) -> Result<SourceStart, RuntimeError>
    where
        C: BrokerSourceConnector<Plan = P>,
    {
        let Self {
            connector,
            instances,
            acknowledgement,
            buffered_intake,
            flush_each_intake,
            client_mounts,
            connector_label,
        } = self;
        let acknowledgement = Runtime::parse_ingest_acknowledgement(
            &ingestor.domain,
            &ingestor.name,
            acknowledgement,
        )?;
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
            capabilities: ingestor.source_capabilities(instances, acknowledgement.support()),
            acknowledgement,
        };
        let mut opened: Vec<Box<dyn SourceInstance>> =
            Vec::with_capacity(plan.capabilities.instances().get().arch_into());
        for instance_index in 0..plan.capabilities.instances().get() {
            tokio::task::consume_budget().await;
            let source = C::open(&plan.connector, instance_index)
                .await
                .map_err(|error| ingestor.start_failure(format!("{error:#}")))?;
            opened.push(Box::new(BrokerSourceInstance {
                source,
                acknowledgement: plan.acknowledgement,
                retry,
            }));
        }
        Ok(SourceStart {
            instances: opened,
            companions: Vec::new(),
            buffered_intake,
            flush_each_intake,
            client_mounts,
            connector_label,
        })
    }
}

/// One broker source instance, which the host runs under the broker loop.
pub(super) struct BrokerSourceInstance<C> {
    pub(super) source: C,
    pub(super) acknowledgement: SourceAckPolicy,
    pub(super) retry: ParsedRetryPolicy,
}

impl<C> SourceInstance for BrokerSourceInstance<C>
where
    C: BrokerSourceConnector,
{
    fn start(
        self: Box<Self>,
        host: RuntimeSourceHost,
        shutdown: watch::Receiver<bool>,
    ) -> BoxFuture<'static, ()> {
        let Self {
            source,
            acknowledgement,
            retry,
        } = *self;
        Box::pin(run_source_instance_with_retry(
            source,
            SourceHost::new(host),
            acknowledgement,
            retry,
            shutdown,
        ))
    }
}

/// One paced source ready to open: its connector plan and the domain cadence the host polls it on.
pub(super) struct PacedSourceStart<P> {
    pub(super) connector: P,
    pub(super) every: DomainClockPeriod,
    /// Whether the first poll is due as soon as the cadence binds or one period later.
    pub(super) cadence_start: DomainCadenceStart,
    pub(super) client_mounts: Vec<Arc<ClientResourceMounts>>,
    pub(super) connector_label: &'static str,
}

impl<P> PacedSourceStart<P> {
    /// Opens the source's one instance and binds the domain cadence the host polls it on.
    pub(super) async fn open<C>(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> Result<SourceStart, RuntimeError>
    where
        C: PacedSourceConnector<Plan = P>,
    {
        let Self {
            connector,
            every,
            cadence_start,
            client_mounts,
            connector_label,
        } = self;
        let acknowledgement = SourceAckPolicy::None;
        let plan = SourcePlan {
            connector,
            capabilities: ingestor.source_capabilities(NonZeroU64::MIN, acknowledgement.support()),
            acknowledgement,
        };
        let source = C::open(&plan.connector, 0)
            .await
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        let cadence = runtime
            .bind_domain_cadence(&ingestor.domain, every, cadence_start)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        let instance: Box<dyn SourceInstance> = Box::new(PacedSourceInstance { source, cadence });
        Ok(SourceStart {
            instances: vec![instance],
            companions: Vec::new(),
            buffered_intake: false,
            flush_each_intake: false,
            client_mounts,
            connector_label,
        })
    }
}

/// One paced source instance, which the host polls on its bound domain cadence.
struct PacedSourceInstance<C> {
    source: C,
    cadence: DomainCadence,
}

impl<C> SourceInstance for PacedSourceInstance<C>
where
    C: PacedSourceConnector,
{
    fn start(
        self: Box<Self>,
        host: RuntimeSourceHost,
        shutdown: watch::Receiver<bool>,
    ) -> BoxFuture<'static, ()> {
        let Self { source, cadence } = *self;
        Box::pin(run_paced_source(source, host, cadence, shutdown))
    }
}

impl Runtime {
    /// Registers a composed source and starts it: the branch runtimes its routes feed, its
    /// readiness, a task for every companion and every instance, and the running ingestor that
    /// stopping it removes.
    ///
    /// Nothing here can fail, so a source is registered only once everything that could stop it
    /// from starting has succeeded.
    pub(super) fn host_source(
        &self,
        ingestor: &IngestorSpec,
        quiesce: Arc<IngestorQuiesceControl>,
        dependencies: IngestorDependencies,
        source: SourceStart,
    ) {
        let SourceStart {
            instances,
            companions,
            buffered_intake,
            flush_each_intake,
            client_mounts,
            connector_label,
        } = source;
        let IngestorDependencies {
            output_routes,
            filter_where,
            codec,
            branched_templates,
            metrics,
        } = dependencies;
        let domain = &ingestor.domain;
        let branched_runtime =
            self.start_branched_ingestor_runtime(domain, &ingestor.name, branched_templates);
        let instance_count: u64 = instances.len().arch_into();
        let expected_instances = NonZeroU64::new(instance_count).assured(
            "every source composition opens the non-zero instance count its source declares",
        );
        self.prepare_ingestor_readiness(domain, &ingestor.name, expected_instances);

        let (shutdown_tx, _) = watch::channel(false);
        let mut tasks = Vec::with_capacity(instances.len());
        for companion in companions {
            tasks.push(tokio::spawn(companion.start(shutdown_tx.subscribe())));
        }
        for (instance_index, instance) in (0_u64..).zip(instances) {
            let host = RuntimeSourceHost::new(RuntimeSourceHostSpec {
                runtime: self.clone(),
                domain: domain.clone(),
                ingestor: ingestor.name.clone(),
                timestamp_source: ingestor.timestamp_source.clone(),
                output_routes: output_routes.clone(),
                filter_where: filter_where.clone(),
                codec: codec.clone(),
                metrics: metrics.clone(),
                branched_senders: branched_runtime.senders.clone(),
                quiesce: quiesce.clone(),
                shutdown: shutdown_tx.subscribe(),
                instance_index,
                metadata_kind: ingestor.metadata_kind,
                buffered_intake,
                flush_each_intake,
            });
            let run = instance.start(host, shutdown_tx.subscribe());
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_client_mounts = client_mounts.clone();
            tasks.push(tokio::spawn(async move {
                let _client_mounts = task_client_mounts;
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    connector = connector_label,
                    instance = instance_index,
                    "started source ingestor instance"
                );
                run.await;
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
            ingestor.runtime_key(),
            IngestorRuntime {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks,
            },
        );
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
    quiesce_observation: IngestorQuiesceObservation,
    ack_root_trackers: IngestorAckRootTrackers,
    shutdown: watch::Receiver<bool>,
    instance_index: u64,
    collector: IngestRouteCollector,
    buffered_intake: bool,
    flush_each_intake: bool,
}

impl RuntimeSourceHost {
    pub(super) fn new(spec: RuntimeSourceHostSpec) -> Self {
        let ack_root_trackers = spec
            .runtime
            .ingestor_ack_root_trackers(&spec.domain, &spec.ingestor);
        let collector = IngestRouteCollector::new(
            spec.metadata_kind,
            INGEST_GROUP_MAX_ROWS,
            spec.metrics.clone(),
        );
        let quiesce_observation = spec.quiesce.observation();
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
            quiesce_observation,
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

    /// The intake a request-scoped source binds its routes to.
    ///
    /// Every request that arrives on those routes is admitted through this host's quiesce control
    /// and dispatched as its own ingest group through the same outputs, codec and branch senders
    /// the host holds, on the request path rather than in the source loop.
    pub(super) fn request_intake(&self) -> EndpointIngestBinding {
        EndpointIngestBinding {
            runtime_key: DomainNodeRef::node_in(
                self.domain.clone(),
                ModelKind::Ingestor,
                self.ingestor.clone(),
            ),
            quiesce: self.quiesce.clone(),
            domain: self.domain.clone(),
            ingestor: self.ingestor.clone(),
            timestamp_source: self.timestamp_source.clone(),
            output_routes: self.output_routes.clone(),
            filter_where: self.filter_where.clone(),
            codec: self.codec.clone(),
            metrics: self.metrics.clone(),
            branched_senders: self.branched_senders.clone(),
        }
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
        self.quiesce
            .wait_for_change_since(&mut self.quiesce_observation)
            .await;
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

#[async_trait]
trait PacedSourceHostServices: SourceHostServices {
    async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool>;
    async fn replay_buffered_poll(&mut self) -> SourceIntakeResult<bool>;
    fn should_skip_poll(&self) -> bool;
    fn record_poll_error(&self, reason: String);
}

#[async_trait]
impl PacedSourceHostServices for RuntimeSourceHost {
    async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
        RuntimeSourceHost::intake_poll(self, poll).await
    }

    async fn replay_buffered_poll(&mut self) -> SourceIntakeResult<bool> {
        RuntimeSourceHost::replay_buffered_poll(self).await
    }

    fn should_skip_poll(&self) -> bool {
        self.quiesce.should_skip_poll()
    }

    fn record_poll_error(&self, reason: String) {
        RuntimeSourceHost::record_poll_error(self, reason);
    }
}

#[async_trait]
trait PacedSourceCadence: Send + 'static {
    async fn next(&mut self, cancellation: &CancellationToken) -> DomainClockWaitResult<Timestamp>;
}

#[async_trait]
impl PacedSourceCadence for DomainCadence {
    async fn next(&mut self, cancellation: &CancellationToken) -> DomainClockWaitResult<Timestamp> {
        let occurrence = DomainCadence::next(self, cancellation).await?;
        Ok(occurrence.due_at())
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

async fn run_paced_source<C, H, D>(
    mut source: C,
    mut host: H,
    mut cadence: D,
    mut shutdown: watch::Receiver<bool>,
) where
    C: PacedSourceConnector,
    H: PacedSourceHostServices,
    D: PacedSourceCadence,
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

        let due_at = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            _ = host.wait_for_quiesce_change() => {
                continue;
            }
            due_at = cadence.next(&cadence_cancellation) => due_at,
        };
        let due_at = match due_at {
            Ok(due_at) => due_at,
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
            poll = source.poll(due_at) => poll,
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

/// Runs a request-scoped source until shutdown.
///
/// Its requests are admitted and dispatched on the request path through the intake the source
/// bound, so the loop reads nothing from the source. It replays what a quiesce buffer retained for
/// those requests once the buffer is released, and closes the source at shutdown so its routes stop
/// receiving requests.
pub(super) async fn run_request_source<C>(
    mut source: C,
    mut host: SourceHost,
    mut shutdown: watch::Receiver<bool>,
) where
    C: SourceConnector,
{
    loop {
        tokio::task::consume_budget().await;
        match host.replay_buffered().await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => {
                host.report_error(format!("{error:#}"));
                continue;
            }
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = host.wait_for_quiesce_change() => {}
        }
    }

    if let Err(error) = source.close().await {
        host.report_error(format!("{error:#}"));
    }
    host.mark_unready();
}

enum BatchDisposition {
    Accepted,
    Retry,
    Shutdown,
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
                    // A quiesce released while the source was resuming was not waited on by
                    // anything, so the iteration starts over and replays what it buffered
                    // before the loop blocks on the next batch.
                    continue;
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
        /// Every resume, replay and poll in the order the loop performed them.
        sequence: Vec<&'static str>,
        /// Replays the host still has to deliver, as a quiesce buffer would.
        pending_replays: usize,
        requests: Vec<SourceBatchRequest>,
        intake: Vec<(SourceIntakeMode, usize)>,
        acknowledged: Vec<Vec<u64>>,
        rejected: Vec<Vec<u64>>,
        poll_errors: Vec<String>,
        reported_errors: Vec<String>,
        ack_waits: usize,
        resumes: usize,
        suspends: usize,
        closes: usize,
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
        /// Replays each resume leaves pending, as a quiesce released during it would.
        replays_pending_after_resume: usize,
        observations: Arc<Mutex<SourceLoopObservations>>,
    }

    #[async_trait]
    impl SourceConnector for FakeSource {
        type Plan = ();

        async fn open(_plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
            Err(Report::new(SourceError::Open { connector: "fake" }))
        }

        async fn resume(&mut self) -> SourceResult<SourceResume> {
            let mut observations = self.observations.lock();
            observations.resumes += 1;
            observations.sequence.push("resume");
            observations.pending_replays = observations
                .pending_replays
                .checked_add(self.replays_pending_after_resume)
                .verified("the test leaves at most a few replays pending");
            drop(observations);
            match self.resume_results.pop_front() {
                Some(result) => result,
                None => Ok(SourceResume::Ready),
            }
        }

        async fn suspend(&mut self) -> SourceResult<()> {
            self.observations.lock().suspends += 1;
            Ok(())
        }

        async fn close(&mut self) -> SourceResult<()> {
            self.observations.lock().closes += 1;
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
            let mut observations = self.observations.lock();
            observations.requests.push(request);
            observations.sequence.push("next_batch");
            drop(observations);
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
            let mut observations = self.observations.lock();
            let Some(remaining) = observations.pending_replays.checked_sub(1) else {
                return Ok(false);
            };
            observations.pending_replays = remaining;
            observations.sequence.push("replay");
            Ok(true)
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

    #[async_trait]
    impl PacedSourceHostServices for FakeHost {
        async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
            Ok(!poll.messages.is_empty())
        }

        async fn replay_buffered_poll(&mut self) -> SourceIntakeResult<bool> {
            Ok(false)
        }

        fn should_skip_poll(&self) -> bool {
            self.suspend_intake
        }

        fn record_poll_error(&self, reason: String) {
            self.observations.lock().poll_errors.push(reason.clone());
            self.report_error(reason);
        }
    }

    fn three_messages() -> VecDeque<FakeMessage> {
        (0..3)
            .map(|position| FakeMessage {
                position,
                payload: vec![
                    u8::try_from(position).verified("the test positions are all below 256"),
                ],
            })
            .collect()
    }

    async fn run_policy_with_refresh(
        policy: SourceAckPolicy,
        resume_required: bool,
    ) -> Arc<Mutex<SourceLoopObservations>> {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let source = FakeSource {
            messages: three_messages(),
            resume_required,
            resume_results: VecDeque::new(),
            replays_pending_after_resume: 0,
            observations: observations.clone(),
        };
        let host = SourceHost::new(FakeHost::running(observations.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        run_source_instance_with_retry(source, host, policy, policy.retry(), shutdown_rx).await;
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
            replays_pending_after_resume: 0,
            observations,
        }
    }

    #[tokio::test]
    async fn source_loop_replays_a_buffer_released_during_resume_before_polling() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let source = FakeSource {
            messages: three_messages(),
            resume_required: false,
            resume_results: VecDeque::new(),
            replays_pending_after_resume: 2,
            observations: observations.clone(),
        };
        let host = SourceHost::new(FakeHost::running(observations.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        run_source_instance_with_retry(
            source,
            host,
            SourceAckPolicy::None,
            SourceAckPolicy::None.retry(),
            shutdown_rx,
        )
        .await;
        drop(shutdown_tx);

        let observations = observations.lock();
        assert_eq!(
            observations.sequence,
            vec![
                "resume",
                "replay",
                "replay",
                "next_batch",
                "next_batch",
                "next_batch",
                "next_batch",
            ]
        );
        assert_eq!(observations.pending_replays, 0);
    }

    #[tokio::test]
    async fn request_source_loop_replays_retained_requests_and_closes_the_source_at_shutdown() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        observations.lock().pending_replays = 2;
        let source = empty_source(observations.clone());
        let host = SourceHost::new(FakeHost::running(observations.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send_replace(true);

        run_request_source(source, host, shutdown_rx).await;

        // Requests dispatch on the request path, so the loop never reads from the source: it only
        // replays what the buffer retained before it observes the shutdown.
        let observations = observations.lock();
        assert_eq!(observations.sequence, vec!["replay", "replay"]);
        assert_eq!(observations.pending_replays, 0);
        assert!(observations.requests.is_empty());
        assert_eq!(observations.resumes, 0);
        assert_eq!(observations.closes, 1);
        assert_eq!(observations.unready, 1);
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

#[cfg(all(test, feature = "shuttle"))]
#[path = "source_shuttle_tests.rs"]
mod shuttle_tests;
