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

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::time::Duration;

use async_trait::async_trait;
use error_stack::{FrameKind, Report, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, ClientResourceMounts, IngestMetadataRow, PacedSourceConnector,
    ParsedRetryPolicy, RetainedIngestHeaders, SourceAckPolicy, SourceAcknowledgementOutcome,
    SourceAcknowledgementSupport, SourceBatch, SourceBatchAcknowledgement, SourceBatchRequest,
    SourceCapabilities, SourceConnector, SourceError, SourceHost, SourceHostServices,
    SourceIntakeError, SourceIntakeMessage, SourceIntakeResult, SourceMessage, SourcePlan,
    SourcePoll, SourceResume, next_retry_delay, physical_time::actual_utc_now,
};
use nervix_models::{DomainClockPeriod, IngestAcknowledgement, PositionClass};
use nervix_primitives::sync::CancellationToken;

use super::{
    super::{
        domain_clock::{DomainCadence, DomainClockWaitResult},
        *,
    },
    IngestorStartError, SourceStartError,
};
use crate::runtime::ingestor_quiesce::{IngestorQuiesceObservation, LiveDeliveryEnd};

#[path = "source_acknowledgement.rs"]
mod acknowledgement;
use acknowledgement::{BatchRoots, RuntimeBatchAcknowledgement};

#[path = "source_read.rs"]
mod read;
use read::{ReadOutcome, read_batch};

const SOURCE_ERROR_RETRY: Duration = Duration::from_millis(100);

/// How a source whose delivery mode declares no retry policy reopens after a failure: the delay
/// starts here and doubles up to the ceiling.
pub(super) const SOURCE_RECONNECT_POLICY: ParsedRetryPolicy = ParsedRetryPolicy {
    backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(30),
};

impl IngestorSpec {
    /// The identity this ingestor's running source is registered under.
    pub(in crate::runtime) fn runtime_key(&self) -> DomainNodeRef {
        DomainNodeRef::node_in(self.domain.clone(), ModelKind::Ingestor, self.name.clone())
    }

    /// The context every failure to initialize this ingestor's source is reported under.
    pub(super) fn initialize_failure(&self) -> IngestorStartError {
        IngestorStartError::Initialize {
            domain: self.domain.clone(),
            ingestor: self.name.clone(),
        }
    }

    /// This ingestor's source failing to start because of `cause`.
    pub(super) fn source_start_failure(
        &self,
        cause: SourceStartError,
    ) -> Report<IngestorStartError> {
        Report::new(cause).change_context(self.initialize_failure())
    }

    /// The metadata namespace this ingestor's messages expose to its programs, which the entity
    /// its source reads declares.
    pub(in crate::runtime) fn metadata_scope(&self) -> SourceMetadataScope {
        self.declared_input.source_kind().capabilities().metadata
    }

    /// The capabilities this ingestor's source runs with, derived from the source vocabulary.
    pub(super) fn source_capabilities(
        &self,
        instances: NonZeroU64,
        acknowledgement: SourceAcknowledgementSupport,
    ) -> SourceCapabilities {
        SourceCapabilities::new(
            self.reads_headers(),
            self.metadata_scope(),
            self.declared_input
                .supports_quiesce(self.declared_input.quiesce_mode()),
            instances,
            acknowledgement,
            self.declared_input
                .source_kind()
                .capabilities()
                .position_class,
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
    /// What a payload the source hands over without an acknowledgement does when the node's
    /// extension workers have no room for its `ON INGESTION` unfolding. The source cannot present
    /// it again, so the choice turns on what holding the source's loop costs the transport:
    /// `WaitForPlace` holds the loop until a place frees, and `RefuseWhenFull` refuses the payload
    /// and counts it.
    pub(super) unacknowledged_admission: QueueAdmission,
    /// The resource mounts the connector's resolved configuration reads its files from, held for
    /// as long as an instance runs.
    pub(super) client_mounts: Vec<Arc<ClientResourceMounts>>,
    /// The connector's name in the instance lifecycle log.
    pub(super) connector_label: &'static str,
    /// The originator a domain-offset Kafka source commits through, which schedule application
    /// replaces while the ingestor keeps running here; absent for every other source.
    pub(super) domain_offsets: Option<StdArc<KafkaOffsetOriginatorSlot>>,
}

/// One broker source ready to open: its connector plan and the host settings it runs under.
pub(super) struct BrokerSourceStart<'a, P> {
    pub(super) connector: P,
    pub(super) instances: NonZeroU64,
    pub(super) acknowledgement: IngestAcknowledgement<'a>,
    pub(super) buffered_intake: bool,
    pub(super) flush_each_intake: bool,
    pub(super) unacknowledged_admission: QueueAdmission,
    pub(super) client_mounts: Vec<Arc<ClientResourceMounts>>,
    pub(super) connector_label: &'static str,
}

impl<P> BrokerSourceStart<'_, P> {
    /// Parses the declared delivery mode and opens every instance to run under the broker loop.
    ///
    /// Nothing is registered here, so a mode that fails to parse or an instance that fails to open
    /// leaves no running ingestor behind.
    pub(super) async fn open<C>(
        self,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError>
    where
        C: BrokerSourceConnector<Plan = P>,
    {
        let Self {
            connector,
            instances,
            acknowledgement,
            buffered_intake,
            flush_each_intake,
            unacknowledged_admission,
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
            nervix_primitives::task::consume_budget().await;
            let source = C::open(&plan.connector, instance_index)
                .await
                .change_context_lazy(|| ingestor.initialize_failure())?;
            opened.push(Box::new(BrokerSourceInstance {
                source,
                policy: BrokerSourcePolicy {
                    acknowledgement: plan.acknowledgement,
                    retry,
                    position_class: plan.capabilities.position_class(),
                },
            }));
        }
        Ok(SourceStart {
            instances: opened,
            companions: Vec::new(),
            buffered_intake,
            flush_each_intake,
            unacknowledged_admission,
            client_mounts,
            connector_label,
            domain_offsets: None,
        })
    }
}

/// One broker source instance, which the host runs under the broker loop.
pub(super) struct BrokerSourceInstance<C> {
    pub(super) source: C,
    pub(super) policy: BrokerSourcePolicy,
}

/// How the host drives one broker source instance.
#[derive(Debug, Clone, Copy)]
pub(super) struct BrokerSourcePolicy {
    /// How its messages are grouped and how long their acknowledgement trees are waited on.
    pub(super) acknowledgement: SourceAckPolicy,
    /// How the host backs off after a failed resume, batch or rejection.
    pub(super) retry: ParsedRetryPolicy,
    /// What its positions mean once the connection that delivered them is gone, which decides
    /// what the host does with a rejection the source could not complete.
    pub(super) position_class: PositionClass,
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
        let Self { source, policy } = *self;
        Box::pin(run_source_instance_with_retry(
            source,
            SourceHost::new(host),
            policy,
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
    ) -> error_stack::Result<SourceStart, IngestorStartError>
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
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let cadence = runtime
            .bind_domain_cadence(&ingestor.domain, every, cadence_start)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let instance: Box<dyn SourceInstance> = Box::new(PacedSourceInstance { source, cadence });
        Ok(SourceStart {
            instances: vec![instance],
            companions: Vec::new(),
            buffered_intake: false,
            flush_each_intake: false,
            // The source has already moved past the poll it hands over, and holding its loop only
            // delays the next poll, whose missed occurrences the domain cadence coalesces.
            unacknowledged_admission: QueueAdmission::WaitForPlace,
            client_mounts,
            connector_label,
            domain_offsets: None,
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
        codec: Arc<CompiledCodec>,
        source: SourceStart,
    ) {
        let SourceStart {
            instances,
            companions,
            buffered_intake,
            flush_each_intake,
            unacknowledged_admission,
            client_mounts,
            connector_label,
            domain_offsets,
        } = source;
        let IngestorDependencies {
            handles,
            output_routes,
            filter_where,
            branched_templates,
            metrics,
        } = dependencies;
        let domain = &ingestor.domain;
        let branched_runtime = self.start_branched_entrypoint_runtimes(
            domain,
            &ModelName::from(&ingestor.name),
            branched_templates,
        );
        let instance_count: u64 = instances.len().arch_into();
        let expected_instances = NonZeroU64::new(instance_count).assured(
            "every source composition opens the non-zero instance count its source declares",
        );
        let readiness = self.prepare_ingestor_readiness(domain, &ingestor.name, expected_instances);

        let (shutdown_tx, _) = watch::channel(false);
        let mut tasks = Vec::with_capacity(instances.len());
        for companion in companions {
            tasks.push(nervix_primitives::task::spawn(
                companion.start(shutdown_tx.subscribe()),
            ));
        }
        for (instance_index, (instance, readiness)) in
            (0_u64..).zip(instances.into_iter().zip(readiness))
        {
            let host = RuntimeSourceHost::new(RuntimeSourceHostSpec {
                handles: handles.clone(),
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
                readiness,
                metadata_scope: ingestor.metadata_scope(),
                buffered_intake,
                flush_each_intake,
                unacknowledged_admission,
                general_error_policy: ingestor.general_error_policy.clone(),
            });
            let run = instance.start(host, shutdown_tx.subscribe());
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_client_mounts = client_mounts.clone();
            tasks.push(nervix_primitives::task::spawn(async move {
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
                domain_offsets,
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
    pub(super) handles: IngestTaskHandles,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) timestamp_source: Option<IngestTimestampSource>,
    pub(super) output_routes: Arc<BoundIngestorRoutes>,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) codec: Arc<CompiledCodec>,
    pub(super) metrics: MessageMetricsHandle,
    pub(super) branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    pub(super) quiesce: Arc<IngestorQuiesceControl>,
    pub(super) shutdown: watch::Receiver<bool>,
    pub(super) instance_index: u64,
    pub(super) readiness: ingestor_quiesce::SourceInstanceReadiness,
    pub(super) metadata_scope: SourceMetadataScope,
    pub(super) buffered_intake: bool,
    pub(super) flush_each_intake: bool,
    pub(super) unacknowledged_admission: QueueAdmission,
    /// The ingestor's `ON GENERAL ERROR` policy, which reports the payloads its codec refuses.
    pub(super) general_error_policy: GeneralErrorPolicy,
}

pub(super) struct RuntimeSourceHost {
    handles: IngestTaskHandles,
    status: Arc<IngestorStatus>,
    runtime: Runtime,
    domain: DomainName,
    ingestor: IngestorName,
    timestamp_source: Option<IngestTimestampSource>,
    output_routes: Arc<BoundIngestorRoutes>,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    codec: Arc<CompiledCodec>,
    metrics: MessageMetricsHandle,
    branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    quiesce: Arc<IngestorQuiesceControl>,
    quiesce_observation: IngestorQuiesceObservation,
    ack_root_trackers: IngestorAckRootTrackers,
    shutdown: watch::Receiver<bool>,
    /// Watches the same stop as `shutdown`, for the waits of live payloads alone: a wait marks the
    /// stop as seen on the receiver it watches, and the acknowledgements this host hands on clone
    /// `shutdown`.
    delivery_stop: watch::Receiver<bool>,
    instance_index: u64,
    readiness: ingestor_quiesce::SourceInstanceReadiness,
    collector: IngestRouteCollector,
    buffered_intake: bool,
    flush_each_intake: bool,
    unacknowledged_admission: QueueAdmission,
    /// Counts the unacknowledged payloads this host did not deliver because the node's extension
    /// workers had no room to unfold them.
    unfolding_refused: prometheus::IntCounter,
    /// Counts the positions this host discarded after a reconnect because the broker redelivers
    /// them.
    transport_redeliveries: prometheus::IntCounter,
    /// Counts the payloads this host's source dropped because they exceed its client's size bound.
    oversized_payloads: prometheus::IntCounter,
    /// Consumes the payloads the ingestor's codec refuses, for this host and the endpoint bindings
    /// it hands out.
    decode_rejections: DecodeRejections,
    /// How long a scenario makes this host wait between learning that a batch was acknowledged
    /// through the graph and letting its source confirm the batch, as a slow transport would.
    confirmation_delay: Option<Duration>,
}

impl RuntimeSourceHost {
    pub(super) fn new(spec: RuntimeSourceHostSpec) -> Self {
        let status_key = DomainNodeRef::node_in(
            spec.domain.clone(),
            ModelKind::Ingestor,
            spec.ingestor.clone(),
        );
        let status = spec.runtime.ingestor_status(&status_key);
        let ack_root_trackers = spec
            .runtime
            .ingestor_ack_root_trackers(&spec.domain, &spec.ingestor);
        let collector = IngestRouteCollector::new(
            spec.metadata_scope,
            INGEST_GROUP_MAX_ROWS,
            spec.metrics.clone(),
        );
        let quiesce_observation = spec.quiesce.observation();
        let dispatcher = spec.runtime.inner.remote_dispatcher.load();
        let unfolding_refused = spec.runtime.inner.metrics.ingestor_unfolding_refused(
            &spec.domain,
            &spec.ingestor,
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
        );
        let transport_redeliveries = spec.runtime.inner.metrics.ingestor_transport_redeliveries(
            &spec.domain,
            &spec.ingestor,
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
        );
        let oversized_payloads = spec.runtime.inner.metrics.ingestor_oversized_payloads(
            &spec.domain,
            &spec.ingestor,
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
        );
        let decode_rejections = DecodeRejections::new(
            spec.general_error_policy,
            spec.runtime.inner.metrics.ingestor_decode_rejections(
                &spec.domain,
                &spec.ingestor,
                dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
            ),
            status.clone(),
        );
        let delivery_stop = spec.shutdown.clone();
        let confirmation_delay = spec
            .runtime
            .inner
            .fault_injection
            .source_confirmation_delay(&status_key);
        Self {
            handles: spec.handles,
            status,
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
            delivery_stop,
            instance_index: spec.instance_index,
            readiness: spec.readiness,
            collector,
            buffered_intake: spec.buffered_intake,
            flush_each_intake: spec.flush_each_intake,
            unacknowledged_admission: spec.unacknowledged_admission,
            unfolding_refused,
            transport_redeliveries,
            oversized_payloads,
            decode_rejections,
            confirmation_delay,
        }
    }

    async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
        for failure in poll.failures {
            nervix_primitives::task::consume_budget().await;
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
        self.intake_unacknowledged_payload(payload).await
    }

    /// The intake a request-scoped source binds its routes to.
    ///
    /// Every request that arrives on those routes is admitted through this host's quiesce control
    /// and dispatched as its own ingest group through the same outputs, codec and branch senders
    /// the host holds, on the request path rather than in the source loop.
    pub(super) fn request_intake(&self) -> EndpointIngestBinding {
        EndpointIngestBinding {
            handles: self.handles.clone(),
            quiesce: self.quiesce.clone(),
            domain: self.domain.clone(),
            ingestor: self.ingestor.clone(),
            timestamp_source: self.timestamp_source.clone(),
            output_routes: self.output_routes.clone(),
            filter_where: self.filter_where.clone(),
            codec: self.codec.clone(),
            metrics: self.metrics.clone(),
            branched_senders: self.branched_senders.clone(),
            decode_rejections: self.decode_rejections.clone(),
        }
    }

    fn record_poll_error(&self, reason: String) {
        self.status.connection.record_error(reason.clone());
        self.report_error(reason);
    }

    /// Consumes the payload `refusal` names as a decode rejection, as the source took it in at
    /// `ingested_at`.
    fn consume_refused(
        &self,
        refusal: &RefusedEntry<IngestMetadataRow<'_>>,
        ingested_at: Timestamp,
    ) {
        let rejection =
            DecodeRejection::new(&self.codec, &refusal.entry, &refusal.report, ingested_at);
        self.decode_rejections
            .consume(&self.runtime, &self.domain, &self.ingestor, rejection);
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

    /// Unacknowledged input the quiesce control decides on as one payload, which it buffers,
    /// drops, or lets dispatch: input read while the ingestor is quiesced, or a batch whose
    /// decision changed while it waited for the node's extension workers.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the source host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn retain_unacknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<()> {
        let mut entries = Vec::with_capacity(messages.len());
        for message in messages {
            nervix_primitives::task::consume_budget().await;
            let metadata = Self::retained_metadata(&message.metadata)?;
            entries.push((message.payload.to_vec(), metadata));
        }
        let payload = BufferedIngestPayload::batch(entries, actual_utc_now());
        self.intake_unacknowledged_payload(payload).await?;
        Ok(())
    }

    /// Acknowledged input read while the ingestor is quiesced.
    ///
    /// The quiesce control decides each message on its own. A message it admits is dispatched
    /// with its acknowledgement root as it would be unquiesced. One it buffers or drops adds
    /// nothing for the batch to wait on, because the buffer or the drop policy has taken it over
    /// from the source, so its position is settled with the rest of the batch.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the source host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn retain_acknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<RuntimeBatchAcknowledgement> {
        let mut admitted = Vec::with_capacity(messages.len());
        for message in messages {
            nervix_primitives::task::consume_budget().await;
            let metadata = Self::retained_metadata(&message.metadata)?;
            let retained = BufferedIngestPayload::new(message.payload, metadata, actual_utc_now());
            match self.quiesce.intake(self.instance_index, retained, false) {
                IngestorQuiesceIntake::Dispatch(_) => admitted.push(message),
                IngestorQuiesceIntake::Buffered
                | IngestorQuiesceIntake::Dropped
                | IngestorQuiesceIntake::Rejected { .. } => {}
            }
        }
        if admitted.is_empty() {
            let roots = BatchRoots::new(&self.ack_root_trackers, 0);
            return Ok(self.batch_acknowledgement(roots));
        }
        self.dispatch_acknowledged(admitted).await
    }

    /// Decodes an acknowledged batch into an ingest group of its own and dispatches it, returning
    /// the acknowledgement the batch's source waits on.
    ///
    /// A payload its codec refuses is consumed as a decode rejection: its root resolves at once, so
    /// the batch's positions are acknowledged once the graph acknowledged the rest. A payload the
    /// node's extension workers cannot unfold now fails the batch, which its source rejects and
    /// presents again.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the source host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn dispatch_acknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<RuntimeBatchAcknowledgement> {
        let mut collector = IngestRouteCollector::new(
            self.collector.scope,
            messages.len().max(1),
            self.metrics.clone(),
        );
        let payloads = self
            .runtime
            .decode_raw_ingest_payload(
                &mut collector,
                QueueAdmission::RefuseWhenFull,
                &self.codec,
                messages.iter().map(batch_payload),
            )
            .await
            .map_err(|refusal| refusal.change_context(SourceIntakeError::Dispatch))?;

        let ingested_at = actual_utc_now();
        let mut roots = BatchRoots::new(&self.ack_root_trackers, messages.len());
        for refusal in &payloads.refused {
            nervix_primitives::task::consume_budget().await;
            self.consume_refused(refusal, ingested_at);
            roots.consumed();
        }
        let metadata = payloads.decoded;
        if metadata.is_empty() {
            return Ok(self.batch_acknowledgement(roots));
        }

        let mut dispatched = Vec::with_capacity(metadata.len());
        let mut acks = Vec::with_capacity(metadata.len());
        for _ in &metadata {
            nervix_primitives::task::consume_budget().await;
            let root = roots.dispatched();
            if self.branched_senders.is_empty() {
                acks.push(root.clone());
            } else {
                acks.push(root.attached());
            }
            dispatched.push(root);
        }

        self.runtime
            .dispatch_ingested_records(IngestGroupDispatch {
                handles: &self.handles,
                collector: &mut collector,
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                metadata: &metadata,
                ingested_at,
                acks,
            })
            .await
            .change_context(SourceIntakeError::Dispatch)?;
        self.runtime
            .flush_ingest_collector(
                &self.domain,
                &self.ingestor,
                &self.branched_senders,
                &mut collector,
            )
            .await
            .change_context(SourceIntakeError::Flush)?;

        for root in dispatched {
            root.ack_success();
        }
        Ok(self.batch_acknowledgement(roots))
    }

    /// The acknowledgement of a batch that waits on `roots`. They are confirmed roots, which the
    /// batch keeps outstanding until its source has settled the batch on its transport.
    fn batch_acknowledgement(&self, roots: BatchRoots<'_>) -> RuntimeBatchAcknowledgement {
        roots.acknowledgement(self.shutdown.clone(), self.confirmation_delay)
    }

    /// Decodes an unacknowledged batch read while the ingestor was not quiesced into the instance's
    /// open ingest group, and accepts it there.
    ///
    /// Its source cannot present the batch again. When the node's extension workers have no room
    /// for its unfolding, the batch waits for a place if this source's loop may be held, and is
    /// otherwise refused and counted. The wait ends when the ingestor stops, which drops the
    /// batch, or when a quiesce decision newer than `decided` arrives: the quiesce control then
    /// decides on the batch as on input read while quiesced, unless the source's input bypasses the
    /// control, in which case the batch waits again.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the source host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn dispatch_unacknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
        mut decided: IngestorQuiesceObservation,
    ) -> SourceIntakeResult<()> {
        let payloads = loop {
            nervix_primitives::task::consume_budget().await;
            let decode = self.runtime.decode_raw_ingest_payload(
                &mut self.collector,
                self.unacknowledged_admission,
                &self.codec,
                messages.iter().map(batch_payload),
            );
            let decoded = match self.unacknowledged_admission {
                QueueAdmission::RefuseWhenFull => decode.await,
                QueueAdmission::WaitForPlace => {
                    let delivery = self
                        .quiesce
                        .run_until_redecided(&mut decided, &mut self.delivery_stop, decode)
                        .await;
                    match delivery {
                        LiveDeliveryEnd::Completed(decoded) => decoded,
                        LiveDeliveryEnd::Stopped => {
                            self.collector.discard_undispatched_payloads();
                            self.report_stopped_delivery();
                            return Ok(());
                        }
                        LiveDeliveryEnd::Redecide => {
                            self.collector.discard_undispatched_payloads();
                            // A source whose input bypasses the quiesce control dispatches what
                            // it already read whatever the decision, so its batch waits again.
                            if !self.buffered_intake {
                                continue;
                            }
                            return self.retain_unacknowledged(messages).await;
                        }
                    }
                }
            };
            match decoded {
                Ok(payloads) => break payloads,
                Err(refusal) => {
                    self.count_refused(messages.len());
                    return Err(refusal.change_context(SourceIntakeError::Dispatch));
                }
            }
        };

        let ingested_at = actual_utc_now();
        let metadata = self.consume_refused_payloads(payloads, ingested_at).await;
        if metadata.is_empty() {
            return Ok(());
        }
        let acks = vec![AckSet::empty(); metadata.len()];
        self.runtime
            .dispatch_ingested_records(IngestGroupDispatch {
                handles: &self.handles,
                collector: &mut self.collector,
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                metadata: &metadata,
                ingested_at,
                acks,
            })
            .await
            .change_context(SourceIntakeError::Dispatch)?;
        if self.flush_each_intake || self.collector.len() >= INGEST_GROUP_MAX_ROWS {
            self.flush().await?;
        }
        Ok(())
    }

    /// Hands an unacknowledged payload this instance read to the quiesce control, and delivers it
    /// when the control lets it dispatch. Answers whether its messages entered the ingest group.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the source host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn intake_unacknowledged_payload(
        &mut self,
        payload: BufferedIngestPayload,
    ) -> SourceIntakeResult<bool> {
        // Observed before the decision is taken, so a newer one is never missed.
        let decided = self.quiesce.observation();
        match self.quiesce.intake(self.instance_index, payload, false) {
            IngestorQuiesceIntake::Dispatch(payload) => {
                self.deliver_unacknowledged(payload, decided).await
            }
            IngestorQuiesceIntake::Buffered
            | IngestorQuiesceIntake::Dropped
            | IngestorQuiesceIntake::Rejected { .. } => Ok(false),
        }
    }

    /// Delivers an unacknowledged payload the quiesce control let dispatch under `decided`, and
    /// answers whether its messages entered the ingest group.
    ///
    /// When the node's extension workers have no room for its unfolding, the payload waits for a
    /// place or is refused and counted, as this source's admission says. The wait ends when the
    /// ingestor stops, which drops the payload, or when a newer quiesce decision arrives, under
    /// which the control decides on the payload again: a buffer retains it, a drop policy drops
    /// and counts it, and a suspension lets it dispatch, so it waits on.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the source host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn deliver_unacknowledged(
        &mut self,
        mut payload: BufferedIngestPayload,
        mut decided: IngestorQuiesceObservation,
    ) -> SourceIntakeResult<bool> {
        let decoded = loop {
            nervix_primitives::task::consume_budget().await;
            let decode = self.runtime.decode_raw_ingest_payload(
                &mut self.collector,
                self.unacknowledged_admission,
                &self.codec,
                payload.indexed_payloads(),
            );
            let delivery = self
                .quiesce
                .run_until_redecided(&mut decided, &mut self.delivery_stop, decode)
                .await;
            let decoded = match delivery {
                LiveDeliveryEnd::Completed(decoded) => decoded,
                LiveDeliveryEnd::Stopped => {
                    self.collector.discard_undispatched_payloads();
                    self.report_stopped_delivery();
                    return Ok(false);
                }
                LiveDeliveryEnd::Redecide => {
                    self.collector.discard_undispatched_payloads();
                    decided = self.quiesce.observation();
                    match self.quiesce.intake(self.instance_index, payload, false) {
                        IngestorQuiesceIntake::Dispatch(returned) => {
                            payload = returned;
                            continue;
                        }
                        IngestorQuiesceIntake::Buffered
                        | IngestorQuiesceIntake::Dropped
                        | IngestorQuiesceIntake::Rejected { .. } => return Ok(false),
                    }
                }
            };
            match decoded {
                Ok(payloads) => break payloads,
                Err(refusal) => {
                    self.count_refused(payload.len());
                    return Err(UnfoldingNotAdmitted::group_error(refusal)
                        .change_context(SourceIntakeError::Dispatch));
                }
            }
        };

        let payloads = decoded.map(|index| payload.metadata_row(index));
        let metadata = self
            .consume_refused_payloads(payloads, payload.observed_at())
            .await;
        if metadata.is_empty() {
            return Ok(false);
        }
        self.runtime
            .accept_raw_ingest_payload(RawIngestAcceptance {
                handles: &self.handles,
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                metadata: &metadata,
                ingested_at: payload.observed_at(),
                collector: &mut self.collector,
            })
            .await
            .change_context(SourceIntakeError::Dispatch)?;
        if self.flush_each_intake || self.collector.len() >= INGEST_GROUP_MAX_ROWS {
            self.flush().await?;
        }
        Ok(true)
    }

    /// Counts `payloads` an unacknowledged intake did not deliver because the node's extension
    /// workers had no room to unfold them.
    fn count_refused(&self, payloads: usize) {
        self.unfolding_refused.inc_by(payloads.arch_into());
    }

    /// Consumes every payload of an unacknowledged or retained batch its codec refused, as the
    /// source took the batch in at `ingested_at`, and returns the metadata rows of the payloads
    /// that decoded.
    async fn consume_refused_payloads<'m>(
        &self,
        payloads: SplitPayloads<IngestMetadataRow<'m>>,
        ingested_at: Timestamp,
    ) -> Vec<IngestMetadataRow<'m>> {
        for refusal in &payloads.refused {
            nervix_primitives::task::consume_budget().await;
            self.consume_refused(refusal, ingested_at);
        }
        payloads.decoded
    }

    /// Records that the ingestor stopped while a live payload waited for the node's extension
    /// workers, which drops the payload with the rest of the ingestor's in-flight input.
    fn report_stopped_delivery(&self) {
        debug!(
            domain = self.domain.as_str(),
            ingestor = self.ingestor.as_str(),
            "dropped an unacknowledged payload that waited for the node's extension workers when \
             the ingestor stopped"
        );
    }
}

/// The payload bytes one source message lends to its ingest group, with the metadata row that stays
/// with them whatever their codec decides.
fn batch_payload<'a>(message: &SourceIntakeMessage<'a>) -> BatchPayload<'a, IngestMetadataRow<'a>> {
    BatchPayload {
        payload: message.payload,
        entry: message.metadata,
    }
}

#[async_trait]
impl SourceHostServices for RuntimeSourceHost {
    async fn intake_unacknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<()> {
        // The decision the batch is taken in under. A batch that waits for the node's extension
        // workers is decided again once a newer one arrives.
        let decided = self.quiesce.observation();
        if self.buffered_intake && decided.is_quiesced() {
            return self.retain_unacknowledged(messages).await;
        }
        self.dispatch_unacknowledged(messages, decided).await
    }

    async fn intake_acknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<SourceBatchAcknowledgement> {
        let quiesced = self.quiesce.observation().is_quiesced();
        let acknowledgement = if self.buffered_intake && quiesced {
            self.retain_acknowledged(messages).await?
        } else {
            self.dispatch_acknowledged(messages).await?
        };
        Ok(SourceBatchAcknowledgement::new(acknowledgement))
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
        self.deliver_retained().await
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
            nervix_primitives::task::consume_budget().await;
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
                .wait_if_ingestor_faulted(
                    &self.status.connection,
                    &self.ingestor,
                    &mut self.shutdown,
                )
                .await
            {
                return false;
            }
        }
    }

    fn mark_ready(&self) {
        self.readiness.mark_ready();
    }

    fn mark_unready(&self) {
        self.readiness.mark_unready();
    }

    fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.status.connection.fail(
            reason,
            Some(RuntimeReconnectStatus {
                backoff: retry_after,
                retry_at: Instant::now() + retry_after,
            }),
        );
    }

    fn clear_transient_error(&self) {
        self.status.connection.clear();
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

    fn record_transport_redeliveries(&self, positions: usize) {
        debug!(
            domain = self.domain.as_str(),
            ingestor = self.ingestor.as_str(),
            positions,
            "discarded positions of a lost connection that the broker redelivers"
        );
        self.transport_redeliveries.inc_by(positions.arch_into());
    }

    fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
        self.oversized_payloads.inc_by(payloads.get().arch_into());
    }

    fn record_unconfirmed_stop(&self, positions: usize) {
        info!(
            domain = self.domain.as_str(),
            ingestor = self.ingestor.as_str(),
            instance = self.instance_index,
            positions,
            "source instance stopped before its transport confirmed positions it read; the \
             transport presents them again"
        );
    }
}

impl RuntimeSourceHost {
    /// Delivers the oldest payload this instance's quiesce buffer retained, and answers whether it
    /// delivered one.
    ///
    /// Nothing can present a retained payload again: an endpoint already answered it and a source
    /// already moved past it. Its unfolding therefore waits for a place on the node's extension
    /// workers rather than being refused, and the payload stays retained, counted with its bytes,
    /// until its messages are accepted into the ingest group. A shutdown or a new quiesce ends
    /// that wait and leaves the payload at the front of the buffer, so it is delivered first when
    /// the buffer drains again. A payload its codec refuses leaves the buffer as a decode
    /// rejection.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the source host delivers one retained payload on each loop turn while its \
                      buffer drains"
        )
    )]
    async fn deliver_retained(&mut self) -> SourceIntakeResult<bool> {
        let Some(retained) = self.quiesce.take_buffered(self.instance_index) else {
            return Ok(false);
        };
        let decode = self.runtime.decode_raw_ingest_payload(
            &mut self.collector,
            QueueAdmission::WaitForPlace,
            &self.codec,
            retained.payload().indexed_payloads(),
        );
        let decoded = retained.run_until_interrupted(&self.shutdown, decode).await;
        let Some(decoded) = decoded else {
            // The group keeps none of the payload's messages, and dropping the delivery puts the
            // payload back at the front of the buffer.
            self.collector.discard_undispatched_payloads();
            return Ok(false);
        };
        let payloads = match decoded {
            Ok(payloads) => payloads,
            Err(refusal) => {
                // A job that waits for a place is refused only by an executor that no longer
                // admits work. The refusal judged nothing, so the payload stays retained.
                debug!(
                    domain = self.domain.as_str(),
                    ingestor = self.ingestor.as_str(),
                    error = ?refusal,
                    "the node could not take a retained payload's unfolding"
                );
                return Ok(false);
            }
        };
        // What its codec refused is consumed, and the payload's other messages are in the group, so
        // it leaves the buffer before the group accepts them.
        let payload = retained.finish();
        let payloads = payloads.map(|index| payload.metadata_row(index));
        let metadata = self
            .consume_refused_payloads(payloads, payload.observed_at())
            .await;
        if metadata.is_empty() {
            return Ok(true);
        }
        self.runtime
            .accept_raw_ingest_payload(RawIngestAcceptance {
                handles: &self.handles,
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                metadata: &metadata,
                ingested_at: payload.observed_at(),
                collector: &mut self.collector,
            })
            .await
            .change_context(SourceIntakeError::Dispatch)?;
        if self.flush_each_intake || self.collector.len() >= INGEST_GROUP_MAX_ROWS {
            self.flush().await?;
        }
        Ok(true)
    }
}

#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained paced source host handles every poll and acknowledgement"
    )
)]
trait PacedSourceHostServices: SourceHostServices {
    async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool>;
    fn should_skip_poll(&self) -> bool;
    fn record_poll_error(&self, reason: String);
}

#[async_trait]
impl PacedSourceHostServices for RuntimeSourceHost {
    async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
        RuntimeSourceHost::intake_poll(self, poll).await
    }

    fn should_skip_poll(&self) -> bool {
        self.quiesce.should_skip_poll()
    }

    fn record_poll_error(&self, reason: String) {
        RuntimeSourceHost::record_poll_error(self, reason);
    }
}

#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained cadence selects each paced source poll"
    )
)]
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
                host.report_error(format!("{error:#}"));
            }
            *ready = false;
            host.mark_unready();
        }
        let keep_running = nervix_primitives::select! {
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
                host.record_poll_error(format!("{error:#}"));
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

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained paced source handles every poll, retry and acknowledgement"
    )
)]
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
        nervix_primitives::task::consume_budget().await;
        match prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await {
            PacedSourceAction::Poll => {}
            PacedSourceAction::Restart => continue,
            PacedSourceAction::Stop => break,
        }

        match host.replay_buffered().await {
            Ok(true) => {
                flush_paced_source(&mut host).await;
                continue;
            }
            Ok(false) => {}
            Err(error) => host.report_error(format!("{error:#}")),
        }

        let due_at = nervix_primitives::select! {
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
                host.report_error(format!("could not advance source cadence: {error:#}"));
                break;
            }
        };
        if host.should_skip_poll() {
            continue;
        }

        let poll = nervix_primitives::select! {
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
                host.record_poll_error(format!("{error:#}"));
                continue;
            }
        };
        host.clear_transient_error();
        match host.intake_poll(poll).await {
            Ok(true) => flush_paced_source(&mut host).await,
            Ok(false) => {}
            Err(error) => host.report_error(format!("{error:#}")),
        }
    }

    flush_paced_source(&mut host).await;
    if let Err(error) = nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "terminal source teardown closes its exact connector instance after the polling loop ends",
        source.close()
    )
    .await
    {
        host.report_error(format!("{error:#}"));
    }
    host.mark_unready();
}

async fn flush_paced_source<H>(host: &mut H)
where
    H: SourceHostServices,
{
    if let Err(error) = host.flush().await {
        host.report_error(format!("{error:#}"));
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
    nervix_primitives::select! {
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
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained request source handles every admitted request and acknowledgement"
    )
)]
pub(super) async fn run_request_source<C>(
    mut source: C,
    mut host: SourceHost,
    mut shutdown: watch::Receiver<bool>,
) where
    C: SourceConnector,
{
    loop {
        nervix_primitives::task::consume_budget().await;
        match host.replay_buffered().await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => {
                host.report_error(format!("{error:#}"));
                continue;
            }
        }
        nervix_primitives::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = host.wait_for_quiesce_change() => {}
        }
    }

    if let Err(error) = nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "terminal source teardown closes its exact connector instance after the polling loop ends",
        source.close()
    )
    .await
    {
        host.report_error(format!("{error:#}"));
    }
    host.mark_unready();
}

enum BatchDisposition<P> {
    Accepted,
    Retry,
    RetryRejection(Vec<P>),
    Shutdown,
}

/// How the loop settles the positions of one batch on the transport.
#[derive(Debug, Clone, Copy)]
struct BatchSettlement {
    position_class: PositionClass,
    /// How long the loop waits before it retries the batch, which the transient status of a
    /// confirmation or a rejection that failed states.
    retry_after: Duration,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained source handles every poll, retry and acknowledgement"
    )
)]
pub(super) async fn run_source_instance_with_retry<C>(
    mut source: C,
    mut host: SourceHost,
    policy: BrokerSourcePolicy,
    mut shutdown: watch::Receiver<bool>,
) where
    C: BrokerSourceConnector,
{
    let BrokerSourcePolicy {
        acknowledgement,
        retry: retry_policy,
        position_class,
    } = policy;
    let mut retry_delay = retry_policy.backoff;
    let mut ready = false;
    let mut pending_rejection: Option<Vec<C::Position>> = None;

    loop {
        nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::select! {
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
            let resumed = nervix_primitives::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
                _ = host.wait_for_quiesce_change() => continue,
                resumed = source.resume() => resumed,
            };
            match resumed {
                Ok(SourceResume::Ready) => {
                    ready = true;
                    if position_class == PositionClass::BrokerRedelivered
                        && let Some(positions) = pending_rejection.take()
                    {
                        // The connection that delivered these positions is gone, and the broker
                        // redelivers everything it did not acknowledge. Rejecting them on the new
                        // connection could only fail again, so they are counted and dropped.
                        host.record_transport_redeliveries(positions.len());
                    }
                    if pending_rejection.is_none() {
                        host.mark_ready();
                    }
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

        // A failed rewind of a replayable source leaves the connector's cursor beyond an
        // unacknowledged message. Reestablish its assignment and retry that same rewind before
        // reading another batch: acknowledging a later Kafka offset would otherwise commit past the
        // missing record. Only a replayable source still holds a pending rejection here, because a
        // resume discards a broker-redelivered one and a source without positions never keeps one.
        if let Some(positions) = pending_rejection.take() {
            if let Err(error) = source.reject(&positions).await {
                let delay = source_retry_delay(retry_delay);
                host.record_transient_error(source_failure_reason(&error), delay);
                host.report_error(format!("{error:#}"));
                pending_rejection = Some(positions);
                if let Err(error) = source.suspend().await {
                    host.report_error(format!("{error:#}"));
                }
                ready = false;
                host.mark_unready();
                if !wait_for_retry(&mut host, &mut shutdown, delay).await {
                    break;
                }
                retry_delay = next_retry_delay(retry_delay, retry_policy);
                continue;
            }
            host.mark_ready();
        }

        let request = batch_request(acknowledgement);
        let read = read_batch(&mut source, &mut host, &mut shutdown, request).await;
        let end = read.end;
        let messages = match read.into_outcome(&host) {
            ReadOutcome::TakeIn(messages) => messages,
            ReadOutcome::ResumeRequired => {
                ready = false;
                host.mark_unready();
                let delay = source_retry_delay(retry_delay);
                if !end.wait_for_retry(&mut host, &mut shutdown, delay).await {
                    break;
                }
                retry_delay = next_retry_delay(retry_delay, retry_policy);
                continue;
            }
            ReadOutcome::Closed => break,
            ReadOutcome::Failed(error) => {
                let delay = source_retry_delay(retry_delay);
                host.record_transient_error(source_failure_reason(&error), delay);
                host.report_error(format!("{error:#}"));
                if !end.wait_for_retry(&mut host, &mut shutdown, delay).await {
                    break;
                }
                retry_delay = next_retry_delay(retry_delay, retry_policy);
                continue;
            }
        };

        // What a cancelled read returns is taken in before the loop acts on the quiesce change or
        // the stop that cancelled it.
        if !messages.is_empty() {
            let retry_after = source_retry_delay(retry_delay);
            match handle_batch(&mut source, &mut host, policy, retry_after, messages).await {
                BatchDisposition::Accepted => {
                    retry_delay = retry_policy.backoff;
                }
                BatchDisposition::Retry => {
                    let delay = source_retry_delay(retry_delay);
                    if !end.wait_for_retry(&mut host, &mut shutdown, delay).await {
                        break;
                    }
                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                }
                BatchDisposition::RetryRejection(positions) => {
                    pending_rejection = Some(positions);
                    if let Err(error) = source.suspend().await {
                        host.report_error(format!("{error:#}"));
                    }
                    ready = false;
                    host.mark_unready();
                    let delay = source_retry_delay(retry_delay);
                    if !end.wait_for_retry(&mut host, &mut shutdown, delay).await {
                        break;
                    }
                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                }
                BatchDisposition::Shutdown => break,
            }
        }
        if end.stops() {
            break;
        }
    }

    flush_for_lifecycle(&mut host).await;
    if let Err(error) = nervix_primitives::expect_lint!(
        nervix::lifecycle_call,
        "terminal source teardown closes its exact connector instance after the polling loop ends",
        source.close()
    )
    .await
    {
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

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the source driver owns position cloning while this admitted batch retains its \
                  acknowledgement list"
    )
)]
async fn handle_batch<C>(
    source: &mut C,
    host: &mut SourceHost,
    policy: BrokerSourcePolicy,
    retry_after: Duration,
    messages: Vec<C::Message>,
) -> BatchDisposition<C::Position>
where
    C: BrokerSourceConnector,
{
    let settlement = BatchSettlement {
        position_class: policy.position_class,
        retry_after,
    };
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
    match policy.acknowledgement {
        SourceAckPolicy::None => {
            let intake = host.intake_unacknowledged(intake_messages).await;
            if let Err(error) = intake {
                host.report_error(format!("{error:#}"));
                return BatchDisposition::Accepted;
            }
            confirm_batch(source, host, settlement, positions).await
        }
        SourceAckPolicy::Sequential { timeout, .. } | SourceAckPolicy::Parallel { timeout, .. } => {
            let intake = host.intake_acknowledged(intake_messages).await;
            let acknowledgement = match intake {
                Ok(acknowledgement) => acknowledgement,
                Err(error) => {
                    host.report_error(format!("{error:#}"));
                    return reject_batch(source, host, settlement, positions).await;
                }
            };
            settle_acknowledged_batch(
                source,
                host,
                settlement,
                acknowledgement,
                timeout,
                positions,
            )
            .await
        }
    }
}

/// Waits for an acknowledged batch's acknowledgement trees, then confirms or rejects its
/// positions on the transport.
///
/// The batch's work stays outstanding for drains and ownership handoffs until `acknowledgement`
/// drops, so it drops only once the source has settled the positions. A stop that arrives before
/// every tree resolved leaves the positions unconfirmed, which the host reports.
async fn settle_acknowledged_batch<C>(
    source: &mut C,
    host: &mut SourceHost,
    settlement: BatchSettlement,
    mut acknowledgement: SourceBatchAcknowledgement,
    timeout: Duration,
    positions: Vec<C::Position>,
) -> BatchDisposition<C::Position>
where
    C: BrokerSourceConnector,
{
    let outcome = acknowledgement.wait(timeout).await;
    let disposition = match outcome {
        SourceAcknowledgementOutcome::Ack => {
            confirm_batch(source, host, settlement, positions).await
        }
        SourceAcknowledgementOutcome::NoAck(reason) => {
            host.handle_ack_failure(reason);
            reject_batch(source, host, settlement, positions).await
        }
        SourceAcknowledgementOutcome::Shutdown => {
            host.record_unconfirmed_stop(positions.len());
            BatchDisposition::Shutdown
        }
    };
    drop(acknowledgement);
    disposition
}

/// Confirms `positions` on the transport, which is the progress the host reports.
///
/// A confirmation that fails is the ingestor's transient status until a later one succeeds, and
/// the positions are rejected for their position class to handle.
async fn confirm_batch<C>(
    source: &mut C,
    host: &mut SourceHost,
    settlement: BatchSettlement,
    positions: Vec<C::Position>,
) -> BatchDisposition<C::Position>
where
    C: BrokerSourceConnector,
{
    let confirmed = source.acknowledge(&positions).await;
    let Err(error) = confirmed else {
        host.clear_transient_error();
        return BatchDisposition::Accepted;
    };
    host.record_transient_error(source_failure_reason(&error), settlement.retry_after);
    host.report_error(format!("{error:#}"));
    reject_batch(source, host, settlement, positions).await
}

/// Rejects `positions` so the source presents them again, and decides by the source's position
/// class what a rejection that failed leaves pending.
///
/// A rejection that fails is the ingestor's transient status, as a confirmation that fails is,
/// until a later confirmation succeeds.
async fn reject_batch<C>(
    source: &mut C,
    host: &mut SourceHost,
    settlement: BatchSettlement,
    positions: Vec<C::Position>,
) -> BatchDisposition<C::Position>
where
    C: BrokerSourceConnector,
{
    let Err(error) = source.reject(&positions).await else {
        return BatchDisposition::Retry;
    };
    host.record_transient_error(source_failure_reason(&error), settlement.retry_after);
    host.report_error(format!("{error:#}"));
    match settlement.position_class {
        PositionClass::Replayable | PositionClass::BrokerRedelivered => {
            BatchDisposition::RetryRejection(positions)
        }
        // A source without positions has nothing a later rejection could name.
        PositionClass::None => BatchDisposition::Retry,
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
    nervix_primitives::select! {
        changed = shutdown.changed() => !(changed.is_err() || *shutdown.borrow()),
        _ = host.wait_for_quiesce_change() => true,
        _ = sleep(delay) => true,
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{collections::VecDeque, future};

    use nervix_connector::{
        IngestMessageHeaders, IngestMetadataRow, SourceBatchAcknowledgementServices,
        SourceConnector, SourceError, SourceReadCancellation, SourceResult,
    };
    use nervix_primitives::sync::{Notify, blocking::Mutex};

    use super::*;
    use crate::runtime::domain_clock::DomainClockWaitError;

    /// Which intake the loop asked the host for.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RecordedIntake {
        Unacknowledged,
        Acknowledged,
    }

    #[derive(Default)]
    struct SourceLoopObservations {
        /// Every resume, replay and poll in the order the loop performed them.
        sequence: Vec<&'static str>,
        /// Replays the host still has to deliver, as a quiesce buffer would.
        pending_replays: usize,
        requests: Vec<SourceBatchRequest>,
        intake: Vec<(RecordedIntake, usize)>,
        acknowledged: Vec<Vec<u64>>,
        rejected: Vec<Vec<u64>>,
        poll_errors: Vec<String>,
        reported_errors: Vec<String>,
        ack_waits: usize,
        ack_outcomes: VecDeque<SourceAcknowledgementOutcome>,
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
        reject_failures_left: usize,
        replay_rejected: bool,
        /// Replays each resume leaves pending, as a quiesce released during it would.
        replays_pending_after_resume: usize,
        resume_started: Option<Arc<Notify>>,
        block_resume: bool,
        observations: Arc<Mutex<SourceLoopObservations>>,
    }

    #[async_trait]
    impl SourceConnector for FakeSource {
        type Plan = ();

        async fn open(_plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
            Err(Report::new(SourceError::Open { connector: "fake" }))
        }

        async fn resume(&mut self) -> SourceResult<SourceResume> {
            {
                let mut observations = self.observations.lock();
                observations.resumes += 1;
                observations.sequence.push("resume");
                observations.pending_replays = observations
                    .pending_replays
                    .checked_add(self.replays_pending_after_resume)
                    .verified("the test leaves at most a few replays pending");
            }
            if let Some(started) = self.resume_started.as_ref() {
                started.notify_one();
            }
            if self.block_resume {
                future::pending::<()>().await;
            }
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
            _cancellation: &SourceReadCancellation,
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
            if self.reject_failures_left > 0 {
                self.reject_failures_left -= 1;
                return Err(Report::new(SourceError::Reject { connector: "fake" }));
            }
            if self.replay_rejected {
                for position in positions.iter().rev() {
                    self.messages.push_front(FakeMessage {
                        position: *position,
                        payload: vec![
                            u8::try_from(*position)
                                .verified("the test positions are all below 256"),
                        ],
                    });
                }
            }
            Ok(())
        }
    }

    struct ImmediateAcknowledgement {
        observations: Arc<Mutex<SourceLoopObservations>>,
    }

    #[async_trait]
    impl SourceBatchAcknowledgementServices for ImmediateAcknowledgement {
        async fn wait(&mut self, _timeout: Duration) -> SourceAcknowledgementOutcome {
            let mut observations = self.observations.lock();
            observations.ack_waits += 1;
            observations
                .ack_outcomes
                .pop_front()
                .unwrap_or(SourceAcknowledgementOutcome::Ack)
        }
    }

    struct FakeHost {
        observations: Arc<Mutex<SourceLoopObservations>>,
        suspend_intake: bool,
        wake_quiesce: bool,
        quiesce_change: Option<Arc<Notify>>,
        wake_suspension: bool,
        active: bool,
    }

    impl FakeHost {
        fn running(observations: Arc<Mutex<SourceLoopObservations>>) -> Self {
            Self {
                observations,
                suspend_intake: false,
                wake_quiesce: false,
                quiesce_change: None,
                wake_suspension: false,
                active: true,
            }
        }
    }

    #[async_trait]
    impl SourceHostServices for FakeHost {
        async fn intake_unacknowledged(
            &mut self,
            messages: Vec<SourceIntakeMessage<'_>>,
        ) -> SourceIntakeResult<()> {
            self.observations
                .lock()
                .intake
                .push((RecordedIntake::Unacknowledged, messages.len()));
            Ok(())
        }

        async fn intake_acknowledged(
            &mut self,
            messages: Vec<SourceIntakeMessage<'_>>,
        ) -> SourceIntakeResult<SourceBatchAcknowledgement> {
            self.observations
                .lock()
                .intake
                .push((RecordedIntake::Acknowledged, messages.len()));
            Ok(SourceBatchAcknowledgement::new(ImmediateAcknowledgement {
                observations: self.observations.clone(),
            }))
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
            if let Some(change) = self.quiesce_change.as_ref() {
                change.notified().await;
                self.observations.lock().quiesce_waits += 1;
                return;
            }
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
        fn record_transport_redeliveries(&self, positions: usize) {
            panic!("a replayable test source discarded {positions} positions after a reconnect");
        }
        fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
            panic!("a test source whose payloads all fit dropped {payloads} for their size");
        }
        fn record_unconfirmed_stop(&self, positions: usize) {
            panic!("a test source stopped with {positions} positions waiting on their trees");
        }
    }

    #[async_trait]
    impl PacedSourceHostServices for FakeHost {
        async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
            Ok(!poll.messages.is_empty())
        }

        fn should_skip_poll(&self) -> bool {
            self.suspend_intake
        }

        fn record_poll_error(&self, reason: String) {
            self.observations.lock().poll_errors.push(reason.clone());
            self.report_error(reason);
        }
    }

    fn replayable(acknowledgement: SourceAckPolicy) -> BrokerSourcePolicy {
        BrokerSourcePolicy {
            acknowledgement,
            retry: acknowledgement.retry(),
            position_class: PositionClass::Replayable,
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
            reject_failures_left: 0,
            replay_rejected: false,
            replays_pending_after_resume: 0,
            resume_started: None,
            block_resume: false,
            observations: observations.clone(),
        };
        let host = SourceHost::new(FakeHost::running(observations.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        run_source_instance_with_retry(source, host, replayable(policy), shutdown_rx).await;
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
            reject_failures_left: 0,
            replay_rejected: false,
            replays_pending_after_resume: 0,
            resume_started: None,
            block_resume: false,
            observations,
        }
    }

    #[async_trait]
    impl PacedSourceConnector for FakeSource {
        async fn poll(&mut self, _scheduled_at: Timestamp) -> SourceResult<SourcePoll> {
            panic!("the failed cadence ends the loop before its source is polled")
        }
    }

    struct FailedCadence;

    struct FailedPacedSource {
        shutdown: watch::Sender<bool>,
    }

    #[async_trait]
    impl SourceConnector for FailedPacedSource {
        type Plan = ();

        async fn open(_plan: &(), _instance_index: u64) -> SourceResult<Self> {
            panic!("the test installs its source directly")
        }

        async fn resume(&mut self) -> SourceResult<SourceResume> {
            Ok(SourceResume::Ready)
        }

        async fn suspend(&mut self) -> SourceResult<()> {
            Err(
                Report::new(std::io::Error::other("could not pause the connection"))
                    .change_context(SourceError::Suspend { connector: "fake" }),
            )
        }

        async fn close(&mut self) -> SourceResult<()> {
            Err(
                Report::new(std::io::Error::other("could not finish the connection"))
                    .change_context(SourceError::Close { connector: "fake" }),
            )
        }
    }

    #[async_trait]
    impl PacedSourceConnector for FailedPacedSource {
        async fn poll(&mut self, _scheduled_at: Timestamp) -> SourceResult<SourcePoll> {
            self.shutdown.send_replace(true);
            Err(
                Report::new(std::io::Error::other("the connection refused the poll"))
                    .change_context(SourceError::Read { connector: "fake" }),
            )
        }
    }

    struct SingleOccurrenceCadence {
        occurrence: Option<Timestamp>,
    }

    #[async_trait]
    impl PacedSourceCadence for SingleOccurrenceCadence {
        async fn next(
            &mut self,
            _cancellation: &CancellationToken,
        ) -> DomainClockWaitResult<Timestamp> {
            match self.occurrence.take() {
                Some(occurrence) => Ok(occurrence),
                None => std::future::pending().await,
            }
        }
    }

    #[nervix_primitives::test]
    async fn runtime_report_chain_source_lifecycle() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        let mut source = FailedPacedSource {
            shutdown: shutdown_tx,
        };
        let mut host = FakeHost::running(observations.clone());
        host.suspend_intake = true;
        host.wake_suspension = true;
        let mut ready = true;
        assert_eq!(
            prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
            PacedSourceAction::Restart
        );
        host.suspend_intake = false;
        run_paced_source(
            source,
            host,
            SingleOccurrenceCadence {
                occurrence: Some(Timestamp::from_unix_nanos(1)),
            },
            shutdown,
        )
        .await;

        let observations = observations.lock();
        assert_eq!(
            observations.poll_errors,
            ["failed to read from fake source: the connection refused the poll"]
        );
        assert_eq!(
            observations.reported_errors,
            [
                "failed to suspend fake source: could not pause the connection",
                "failed to read from fake source: the connection refused the poll",
                "failed to close fake source: could not finish the connection",
            ]
        );
    }

    #[async_trait]
    impl PacedSourceCadence for FailedCadence {
        async fn next(
            &mut self,
            _cancellation: &CancellationToken,
        ) -> DomainClockWaitResult<Timestamp> {
            let domain = domain("orders");
            Err(Report::new(DomainClockAccessError::Stopped {
                domain: domain.clone(),
                generation: 3,
            })
            .change_context(DomainClockWaitError::Clock { domain }))
        }
    }

    #[nervix_primitives::test]
    async fn runtime_report_chain_source_cadence() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let source = empty_source(observations.clone());
        let host = FakeHost::running(observations.clone());
        let (_shutdown_tx, shutdown) = watch::channel(false);

        run_paced_source(source, host, FailedCadence, shutdown).await;

        let observations = observations.lock();
        assert_eq!(
            observations.reported_errors,
            [
                "could not advance source cadence: domain 'orders' clock became unavailable while \
                 waiting for a logical deadline: domain 'orders' clock generation 3 is stopped"
            ]
        );
        assert_eq!(observations.closes, 1);
        assert_eq!(observations.unready, 1);
    }

    #[nervix_primitives::test]
    async fn runtime_report_chain_source_resume() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let mut source = empty_source(observations.clone());
        source
            .resume_results
            .push_back(Err(Report::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "broker refused the connection",
            ))
            .change_context(SourceError::Resume { connector: "fake" })));
        let mut host = FakeHost::running(observations.clone());
        host.wake_quiesce = true;
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let mut ready = false;

        assert_eq!(
            prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
            PacedSourceAction::Restart
        );
        let observations = observations.lock();
        assert_eq!(
            observations.poll_errors,
            ["failed to resume fake source: broker refused the connection"]
        );
        assert_eq!(observations.reported_errors, observations.poll_errors);
    }

    #[nervix_primitives::test]
    async fn source_loop_replays_a_buffer_released_during_resume_before_polling() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let source = FakeSource {
            messages: three_messages(),
            resume_required: false,
            resume_results: VecDeque::new(),
            reject_failures_left: 0,
            replay_rejected: false,
            replays_pending_after_resume: 2,
            resume_started: None,
            block_resume: false,
            observations: observations.clone(),
        };
        let host = SourceHost::new(FakeHost::running(observations.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        run_source_instance_with_retry(
            source,
            host,
            replayable(SourceAckPolicy::None),
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

    #[nervix_primitives::test]
    async fn source_shutdown_cancels_pending_resume_and_closes_the_source() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let started = Arc::new(Notify::new());
        let mut source = empty_source(observations.clone());
        source.resume_started = Some(started.clone());
        source.block_resume = true;
        let host = SourceHost::new(FakeHost::running(observations.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = nervix_primitives::task::spawn(run_source_instance_with_retry(
            source,
            host,
            replayable(SourceAckPolicy::None),
            shutdown_rx,
        ));
        started.notified().await;
        shutdown_tx.send_replace(true);
        nervix_primitives::time::timeout(Duration::from_secs(10), running)
            .await
            .assured("shutdown cancels the pending resume through the source-loop select")
            .assured("the source loop exits without a task panic");
        assert_eq!(observations.lock().closes, 1);
    }

    #[nervix_primitives::test]
    async fn source_quiesce_change_cancels_pending_resume_before_retrying() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        let started = Arc::new(Notify::new());
        let changed = Arc::new(Notify::new());
        let mut source = empty_source(observations.clone());
        source.resume_started = Some(started.clone());
        source.block_resume = true;
        let mut fake_host = FakeHost::running(observations.clone());
        fake_host.quiesce_change = Some(changed.clone());
        let host = SourceHost::new(fake_host);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = nervix_primitives::task::spawn(run_source_instance_with_retry(
            source,
            host,
            replayable(SourceAckPolicy::None),
            shutdown_rx,
        ));
        started.notified().await;
        changed.notify_one();
        nervix_primitives::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .assured("the quiesce notification interrupts resume so the loop retries it");
        shutdown_tx.send_replace(true);
        nervix_primitives::time::timeout(Duration::from_secs(10), running)
            .await
            .assured("shutdown cancels the retried resume through the source-loop select")
            .assured("the source loop exits without a task panic");
        let observations = observations.lock();
        assert_eq!(observations.resumes, 2);
        assert_eq!(observations.quiesce_waits, 1);
        assert_eq!(observations.closes, 1);
    }

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
    async fn source_loop_accepts_no_ack_messages_one_at_a_time() {
        let observations = run_policy(SourceAckPolicy::None).await;
        let observations = observations.lock();
        assert_eq!(
            observations.intake,
            vec![(RecordedIntake::Unacknowledged, 1); 3]
        );
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
        assert_eq!(observations.ack_waits, 0);
        assert!(observations.rejected.is_empty());
    }

    #[nervix_primitives::test]
    async fn source_loop_waits_and_commits_each_sequential_ack() {
        let observations = run_policy(SourceAckPolicy::Sequential {
            timeout: Duration::from_secs(1),
            retry: retry_policy(),
        })
        .await;
        let observations = observations.lock();
        assert_eq!(
            observations.intake,
            vec![(RecordedIntake::Acknowledged, 1); 3]
        );
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
        assert_eq!(observations.ack_waits, 3);
        assert!(observations.rejected.is_empty());
    }

    #[nervix_primitives::test]
    async fn failed_rewind_cannot_commit_past_an_unacknowledged_kafka_position() {
        let observations = Arc::new(Mutex::new(SourceLoopObservations::default()));
        observations.lock().ack_outcomes = VecDeque::from([
            SourceAcknowledgementOutcome::NoAck("relay owner crashed".to_string()),
            SourceAcknowledgementOutcome::Ack,
            SourceAcknowledgementOutcome::Ack,
        ]);
        let source = FakeSource {
            messages: three_messages().into_iter().take(2).collect(),
            resume_required: false,
            resume_results: VecDeque::new(),
            reject_failures_left: 2,
            replay_rejected: true,
            replays_pending_after_resume: 0,
            resume_started: None,
            block_resume: false,
            observations: observations.clone(),
        };
        let host = SourceHost::new(FakeHost::running(observations.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let policy = SourceAckPolicy::Sequential {
            timeout: Duration::from_secs(1),
            retry: retry_policy(),
        };
        run_source_instance_with_retry(source, host, replayable(policy), shutdown_rx).await;
        drop(shutdown_tx);

        let observations = observations.lock();
        assert_eq!(observations.rejected, vec![vec![0], vec![0], vec![0]]);
        assert_eq!(observations.acknowledged, vec![vec![0], vec![1]]);
        assert_eq!(observations.suspends, 2);
        assert_eq!(observations.resumes, 3);
    }

    #[nervix_primitives::test]
    async fn source_loop_commits_one_parallel_ack_batch() {
        let observations = run_policy(SourceAckPolicy::Parallel {
            max_in_flight: NonZeroUsize::new(3).verified("three is a nonzero source batch bound"),
            batch_timeout: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            retry: retry_policy(),
        })
        .await;
        let observations = observations.lock();
        assert_eq!(observations.intake, vec![(RecordedIntake::Acknowledged, 3)]);
        assert_eq!(observations.acknowledged, vec![vec![0, 1, 2]]);
        assert_eq!(observations.ack_waits, 1);
        assert_eq!(observations.requests[0].max_messages.get(), 3);
        assert_eq!(
            observations.requests[0].batch_timeout,
            Some(Duration::from_millis(10))
        );
        assert!(observations.rejected.is_empty());
    }

    #[nervix_primitives::test]
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

#[cfg(test)]
#[path = "source_extension_tests.rs"]
mod extension_tests;

#[cfg(test)]
#[path = "source_decode_tests.rs"]
mod decode_tests;

#[cfg(test)]
#[path = "source_reconnect_tests.rs"]
mod reconnect_tests;

#[cfg(test)]
#[path = "source_confirmation_tests.rs"]
mod confirmation_tests;

#[cfg(test)]
#[path = "source_read_tests.rs"]
mod read_tests;

impl Drop for RuntimeSourceHost {
    fn drop(&mut self) {
        self.readiness.retire();
    }
}
