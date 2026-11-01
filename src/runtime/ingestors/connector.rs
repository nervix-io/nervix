//! The host side of every ingestor: the one registration a composed ingestor goes through, the loop
//! of each ingestor family, and the runtime intake adapter those loops drive.
//!
//! Layer: data plane.
//!
//! - **Owns.** Opening broker and paced instances, registering a composed ingestor with its branch
//!   runtimes, readiness and tasks, the acknowledgement policy a declared delivery mode parses
//!   into, the broker, paced and request-scoped loops with their quiesce and readiness handling,
//!   grouping policy, runtime decoding and dispatch, acknowledgement waiting, retry cadence, and
//!   connector error reporting.
//! - **Depends on.** The connector ingestor contract, declared delivery modes, and pre-resolved
//!   runtime execution handles.
//! - **Must not know.** A broker driver, connector-specific configuration, Plexus parsing, registry
//!   validation, or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::time::Duration;

use async_trait::async_trait;
use error_stack::{FrameKind, Report, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, ClientResourceMounts, IngestMetadataRow, IngestorAckPolicy,
    IngestorAcknowledgementOutcome, IngestorAcknowledgementSupport, IngestorBatch,
    IngestorBatchAcknowledgement, IngestorBatchRequest, IngestorCapabilities, IngestorConnector,
    IngestorError, IngestorHost, IngestorHostServices, IngestorIntakeError, IngestorIntakeMessage,
    IngestorIntakeResult, IngestorMessage, IngestorPlan, IngestorPoll, IngestorResume,
    IngestorSettlement, PacedIngestorConnector, ParsedRetryPolicy, next_retry_delay,
    physical_time::actual_utc_now,
};
use nervix_models::{DomainClockPeriod, IngestAcknowledgement, PositionClass};
use nervix_primitives::sync::CancellationToken;

use super::{
    super::{
        domain_clock::{DomainCadence, DomainClockWaitResult},
        *,
    },
    IngestorConnectorStartError, IngestorStartError,
};
use crate::runtime::ingestor_quiesce::{IngestorQuiesceObservation, LiveDeliveryEnd};

#[path = "connector_acknowledgement.rs"]
mod acknowledgement;
use acknowledgement::{BatchRoots, RuntimeBatchAcknowledgement};

#[path = "individual_settlement.rs"]
mod individual_settlement;
use individual_settlement::IndividualBatch;

#[path = "connector_read.rs"]
mod read;
use read::{ReadOutcome, read_batch};

#[path = "connector_poll.rs"]
mod poll;

const SOURCE_ERROR_RETRY: Duration = Duration::from_millis(100);

/// How a ingestor whose delivery mode declares no retry policy reopens after a failure: the delay
/// starts here and doubles up to the ceiling.
pub(super) const INGESTOR_RECONNECT_POLICY: ParsedRetryPolicy = ParsedRetryPolicy {
    backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(30),
};

impl IngestorSpec {
    /// The identity this ingestor's running ingestor is registered under.
    pub(in crate::runtime) fn runtime_key(&self) -> DomainNodeRef {
        DomainNodeRef::node_in(self.domain.clone(), ModelKind::Ingestor, self.name.clone())
    }

    /// The context every failure to initialize this ingestor's ingestor is reported under.
    pub(super) fn initialize_failure(&self) -> IngestorStartError {
        IngestorStartError::Initialize {
            domain: self.domain.clone(),
            ingestor: self.name.clone(),
        }
    }

    /// This ingestor's ingestor failing to start because of `cause`.
    pub(super) fn connector_start_failure(
        &self,
        cause: IngestorConnectorStartError,
    ) -> Report<IngestorStartError> {
        Report::new(cause).change_context(self.initialize_failure())
    }

    /// The metadata fields this ingestor's messages expose to its programs, which the entity its
    /// ingestor reads declares.
    pub(in crate::runtime) fn metadata_declaration(&self) -> IngestorMetadataDeclaration {
        self.declared_input.entity_kind().capabilities().metadata
    }

    /// The capabilities this ingestor's ingestor runs with, derived from the ingestor vocabulary.
    pub(super) fn ingestor_capabilities(
        &self,
        instances: NonZeroU64,
        acknowledgement: IngestorAcknowledgementSupport,
    ) -> IngestorCapabilities {
        IngestorCapabilities::new(
            self.reads_headers(),
            self.declared_input
                .supports_quiesce(self.declared_input.quiesce_mode()),
            instances,
            acknowledgement,
            self.declared_input
                .entity_kind()
                .capabilities()
                .position_class,
        )
    }
}

/// One opened ingestor instance, which the host starts under the loop of its ingestor family.
pub(super) trait IngestorInstance: Send + 'static {
    /// Attaches the instance to its host and returns the loop that runs it until shutdown.
    ///
    /// The host calls this before the ingestor's start returns, so whatever the instance attaches
    /// here is in place by the time the ingestor counts as started.
    fn start(
        self: Box<Self>,
        host: RuntimeIngestorHost,
        shutdown: watch::Receiver<bool>,
    ) -> BoxFuture<'static, ()>;
}

/// A task a ingestor runs beside its instances for as long as the ingestor runs.
pub(super) trait IngestorCompanion: Send + 'static {
    /// Returns the task, which ends at shutdown.
    fn start(self: Box<Self>, shutdown: watch::Receiver<bool>) -> BoxFuture<'static, ()>;
}

/// One ingestor composed from its plan: the instances its connector opened and how the host runs
/// them.
pub(super) struct IngestorStart {
    /// Every opened instance, in instance order.
    pub(super) instances: Vec<Box<dyn IngestorInstance>>,
    /// Tasks the ingestor runs beside its instances, such as the watch that tells domain-offset
    /// Kafka instances their topic's partitions changed.
    pub(super) companions: Vec<Box<dyn IngestorCompanion>>,
    /// Whether what the ingestor reads while quiesced passes through the quiesce control, which may
    /// buffer or drop it.
    pub(super) buffered_intake: bool,
    /// Whether every accepted batch flushes its ingest group at once.
    pub(super) flush_each_intake: bool,
    /// What a payload the ingestor hands over without an acknowledgement does when the node's
    /// extension workers have no room for its `ON INGESTION` unfolding. The ingestor cannot present
    /// it again, so the choice turns on what holding the ingestor's loop costs the transport:
    /// `WaitForPlace` holds the loop until a place frees, and `RefuseWhenFull` refuses the payload
    /// and counts it.
    pub(super) unacknowledged_admission: QueueAdmission,
    /// The resource mounts the connector's resolved configuration reads its files from, held for
    /// as long as an instance runs.
    pub(super) client_mounts: Vec<Arc<ClientResourceMounts>>,
    /// The connector's name in the instance lifecycle log.
    pub(super) connector_label: String,
    /// The originator a domain-offset Kafka ingestor commits through, which schedule application
    /// replaces while the ingestor keeps running here; absent for every other ingestor.
    pub(super) domain_offsets: Option<StdArc<KafkaOffsetOriginatorSlot>>,
}

/// One broker ingestor ready to open: its connector plan and the host settings it runs under.
pub(super) struct BrokerIngestorStart<'a, P> {
    pub(super) connector: P,
    pub(super) instances: NonZeroU64,
    pub(super) acknowledgement: IngestAcknowledgement<'a>,
    pub(super) buffered_intake: bool,
    pub(super) flush_each_intake: bool,
    pub(super) unacknowledged_admission: QueueAdmission,
    pub(super) client_mounts: Vec<Arc<ClientResourceMounts>>,
}

impl<P> BrokerIngestorStart<'_, P> {
    /// Parses the declared delivery mode and opens every instance to run under the broker loop.
    ///
    /// Nothing is registered here, so a mode that fails to parse or an instance that fails to open
    /// leaves no running ingestor behind.
    pub(super) async fn open<C>(
        self,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError>
    where
        C: BrokerIngestorConnector<Plan = P>,
    {
        let Self {
            connector,
            instances,
            acknowledgement,
            buffered_intake,
            flush_each_intake,
            unacknowledged_admission,
            client_mounts,
        } = self;
        let acknowledgement = Runtime::parse_ingest_acknowledgement(
            &ingestor.domain,
            &ingestor.name,
            acknowledgement,
        )?;
        let plan = IngestorPlan {
            connector,
            capabilities: ingestor.ingestor_capabilities(instances, acknowledgement.support()),
            acknowledgement,
        };
        let mut opened: Vec<Box<dyn IngestorInstance>> =
            Vec::with_capacity(plan.capabilities.instances().get().arch_into());
        for instance_index in 0..plan.capabilities.instances().get() {
            nervix_primitives::task::consume_budget().await;
            let source = C::open(&plan.connector, instance_index)
                .await
                .change_context_lazy(|| ingestor.initialize_failure())?;
            opened.push(Box::new(BrokerIngestorInstance {
                source,
                policy: BrokerIngestorPolicy::new(
                    plan.acknowledgement,
                    plan.capabilities.position_class(),
                ),
            }));
        }
        Ok(IngestorStart {
            instances: opened,
            companions: Vec::new(),
            buffered_intake,
            flush_each_intake,
            unacknowledged_admission,
            client_mounts,
            connector_label: C::KIND.connector().keyword().to_ascii_lowercase(),
            domain_offsets: None,
        })
    }
}

/// One broker ingestor instance, which the host runs under the broker loop.
pub(super) struct BrokerIngestorInstance<C> {
    pub(super) source: C,
    pub(super) policy: BrokerIngestorPolicy,
}

/// How the host drives one broker ingestor instance.
#[derive(Debug, Clone, Copy)]
pub(super) struct BrokerIngestorPolicy {
    /// How its messages are grouped and how long their acknowledgement trees are waited on.
    pub(super) acknowledgement: IngestorAckPolicy,
    /// How the host backs off after a failed resume, batch or rejection.
    pub(super) retry: ParsedRetryPolicy,
    /// What its positions mean once the connection that delivered them is gone, which decides
    /// what the host does with a rejection the ingestor could not complete.
    pub(super) position_class: PositionClass,
}

impl BrokerIngestorPolicy {
    /// How the host drives a broker ingestor whose declared delivery mode is `acknowledgement` and
    /// whose positions are of `position_class`: an acknowledged mode retries by its declared
    /// policy, and an ingestor without one reopens on the host's reconnect cadence.
    pub(super) fn new(acknowledgement: IngestorAckPolicy, position_class: PositionClass) -> Self {
        let retry = match acknowledgement {
            IngestorAckPolicy::None => INGESTOR_RECONNECT_POLICY,
            IngestorAckPolicy::Sequential { retry, .. }
            | IngestorAckPolicy::Parallel { retry, .. } => retry,
        };
        Self {
            acknowledgement,
            retry,
            position_class,
        }
    }
}

impl<C> IngestorInstance for BrokerIngestorInstance<C>
where
    C: BrokerIngestorConnector,
{
    fn start(
        self: Box<Self>,
        host: RuntimeIngestorHost,
        shutdown: watch::Receiver<bool>,
    ) -> BoxFuture<'static, ()> {
        let Self { source, policy } = *self;
        Box::pin(run_ingestor_instance_with_retry(
            source,
            IngestorHost::new(host),
            policy,
            shutdown,
        ))
    }
}

/// One paced ingestor ready to open: its connector plan and the domain cadence the host polls it on.
pub(super) struct PacedIngestorStart<P> {
    pub(super) connector: P,
    pub(super) every: DomainClockPeriod,
    /// Whether the first poll is due as soon as the cadence binds or one period later.
    pub(super) cadence_start: DomainCadenceStart,
    /// The most messages of one poll the host takes in as one intake batch, which it executes as
    /// an ingest group of its own, as the ingestor's entity declares.
    pub(super) intake_batch: NonZeroUsize,
    pub(super) client_mounts: Vec<Arc<ClientResourceMounts>>,
}

impl<P> PacedIngestorStart<P> {
    /// Opens the ingestor's one instance and binds the domain cadence the host polls it on.
    pub(super) async fn open<C>(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError>
    where
        C: PacedIngestorConnector<Plan = P>,
    {
        let Self {
            connector,
            every,
            cadence_start,
            intake_batch,
            client_mounts,
        } = self;
        let acknowledgement = IngestorAckPolicy::None;
        let plan = IngestorPlan {
            connector,
            capabilities: ingestor
                .ingestor_capabilities(NonZeroU64::MIN, acknowledgement.support()),
            acknowledgement,
        };
        let source = C::open(&plan.connector, 0)
            .await
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let cadence = runtime
            .bind_domain_cadence(&ingestor.domain, every, cadence_start)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let instance: Box<dyn IngestorInstance> = Box::new(PacedIngestorInstance {
            source,
            cadence,
            intake_batch,
        });
        Ok(IngestorStart {
            instances: vec![instance],
            companions: Vec::new(),
            buffered_intake: false,
            flush_each_intake: false,
            // The ingestor has already moved past the poll it hands over, and holding its loop only
            // delays the next poll, whose missed occurrences the domain cadence coalesces.
            unacknowledged_admission: QueueAdmission::WaitForPlace,
            client_mounts,
            connector_label: C::KIND.connector().keyword().to_ascii_lowercase(),
            domain_offsets: None,
        })
    }
}

/// One paced ingestor instance, which the host polls on its bound domain cadence.
struct PacedIngestorInstance<C> {
    source: C,
    cadence: DomainCadence,
    intake_batch: NonZeroUsize,
}

impl<C> IngestorInstance for PacedIngestorInstance<C>
where
    C: PacedIngestorConnector,
{
    fn start(
        self: Box<Self>,
        host: RuntimeIngestorHost,
        shutdown: watch::Receiver<bool>,
    ) -> BoxFuture<'static, ()> {
        let Self {
            source,
            cadence,
            intake_batch,
        } = *self;
        Box::pin(run_paced_ingestor(
            source,
            host,
            cadence,
            intake_batch,
            shutdown,
        ))
    }
}

impl Runtime {
    /// Registers a composed ingestor and starts it: the branch runtimes its routes feed, its
    /// readiness, a task for every companion and every instance, and the running ingestor that
    /// stopping it removes.
    ///
    /// Nothing here can fail, so a ingestor is registered only once everything that could stop it
    /// from starting has succeeded.
    pub(super) fn host_ingestor(
        &self,
        ingestor: &IngestorSpec,
        quiesce: Arc<IngestorQuiesceControl>,
        dependencies: IngestorDependencies,
        codec: Arc<CompiledCodec>,
        source: IngestorStart,
    ) {
        let IngestorStart {
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
            let host = RuntimeIngestorHost::new(RuntimeIngestorHostSpec {
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
                buffered_intake,
                flush_each_intake,
                unacknowledged_admission,
                general_error_policy: ingestor.general_error_policy.clone(),
            });
            let run = instance.start(host, shutdown_tx.subscribe());
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_client_mounts = client_mounts.clone();
            let connector_label = connector_label.clone();
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

/// What a ingestor failure says to an operator: the most specific cause beneath it.
///
/// The contract's own context names only the operation that failed, such as a resume. The cause a
/// connector reports beneath it, such as a refused connection or a conflicting client identity, is
/// what the ingestor's transient status shows.
fn ingestor_failure_reason(error: &Report<IngestorError>) -> String {
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

pub(super) struct RuntimeIngestorHostSpec {
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
    pub(super) readiness: ingestor_quiesce::IngestorInstanceReadiness,
    pub(super) buffered_intake: bool,
    pub(super) flush_each_intake: bool,
    pub(super) unacknowledged_admission: QueueAdmission,
    /// The ingestor's `ON GENERAL ERROR` policy, which reports the payloads its codec refuses.
    pub(super) general_error_policy: GeneralErrorPolicy,
}

pub(super) struct RuntimeIngestorHost {
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
    readiness: ingestor_quiesce::IngestorInstanceReadiness,
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
    /// Counts the payloads this host's ingestor dropped because they exceed its client's size bound.
    oversized_payloads: prometheus::IntCounter,
    /// Counts header values the connector skipped because their bytes are not valid UTF-8.
    header_values_dropped: prometheus::IntCounter,
    /// Consumes the payloads the ingestor's codec refuses, for this host and the endpoint bindings
    /// it hands out.
    decode_rejections: DecodeRejections,
    /// How long a scenario makes this host wait between learning that a batch was acknowledged
    /// through the graph and letting its ingestor confirm the batch, as a slow transport would.
    confirmation_delay: Option<Duration>,
}

impl RuntimeIngestorHost {
    pub(super) fn new(spec: RuntimeIngestorHostSpec) -> Self {
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
            spec.output_routes.metadata.clone(),
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
        let header_values_dropped = spec.runtime.inner.metrics.ingestor_header_values_dropped(
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
            header_values_dropped,
            decode_rejections,
            confirmation_delay,
        }
    }

    /// The intake a request-scoped ingestor binds its routes to.
    ///
    /// Every request that arrives on those routes is admitted through this host's quiesce control
    /// and dispatched as its own ingest group through the same outputs, codec and branch senders
    /// the host holds, on the request path rather than in the ingestor loop.
    pub(super) fn request_intake(&self, request: EndpointRequestPolicy) -> EndpointIngestBinding {
        EndpointIngestBinding {
            request,
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

    /// Consumes the payload `refusal` names as a decode rejection, as the ingestor took it in at
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

impl RuntimeIngestorHost {
    /// Unacknowledged input the quiesce control decides on as one payload, which it buffers,
    /// drops, or lets dispatch: input read while the ingestor is quiesced, or a batch whose
    /// decision changed while it waited for the node's extension workers.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the ingestor host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn retain_unacknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
        let mut entries = Vec::with_capacity(messages.len());
        for message in messages {
            nervix_primitives::task::consume_budget().await;
            let metadata = BufferedIngestMetadata::retain(&message.metadata);
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
    /// from the ingestor, so its position is settled with the rest of the batch.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the ingestor host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn retain_acknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<RuntimeBatchAcknowledgement> {
        let mut admitted = Vec::with_capacity(messages.len());
        for message in messages {
            nervix_primitives::task::consume_budget().await;
            let metadata = BufferedIngestMetadata::retain(&message.metadata);
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
    /// the acknowledgement the batch's ingestor waits on.
    ///
    /// A payload its codec refuses is consumed as a decode rejection: its root resolves at once, so
    /// the batch's positions are acknowledged once the graph acknowledged the rest. A payload the
    /// node's extension workers cannot unfold now fails the batch, which its ingestor rejects and
    /// presents again.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the ingestor host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn dispatch_acknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<RuntimeBatchAcknowledgement> {
        let mut collector = IngestRouteCollector::new(
            self.collector.projection.clone(),
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
            .map_err(|refusal| refusal.change_context(IngestorIntakeError::Dispatch))?;

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
            .change_context(IngestorIntakeError::Dispatch)?;
        self.runtime
            .flush_ingest_collector(
                &self.domain,
                &self.ingestor,
                &self.branched_senders,
                &mut collector,
            )
            .await
            .change_context(IngestorIntakeError::Flush)?;

        for root in dispatched {
            root.ack_success();
        }
        Ok(self.batch_acknowledgement(roots))
    }

    /// The acknowledgement of a batch that waits on `roots`. They are confirmed roots, which the
    /// batch keeps outstanding until its ingestor has settled the batch on its transport.
    fn batch_acknowledgement(&self, roots: BatchRoots<'_>) -> RuntimeBatchAcknowledgement {
        roots.acknowledgement(self.shutdown.clone(), self.confirmation_delay)
    }

    /// Decodes an unacknowledged batch read while the ingestor was not quiesced into the instance's
    /// open ingest group, and accepts it there.
    ///
    /// Its ingestor cannot present the batch again. When the node's extension workers have no room
    /// for its unfolding, the batch waits for a place if this ingestor's loop may be held, and is
    /// otherwise refused and counted. The wait ends when the ingestor stops, which drops the
    /// batch, or when a quiesce decision newer than `decided` arrives: the quiesce control then
    /// decides on the batch as on input read while quiesced, unless the ingestor's input bypasses the
    /// control, in which case the batch waits again.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the ingestor host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn dispatch_unacknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
        mut decided: IngestorQuiesceObservation,
    ) -> IngestorIntakeResult<()> {
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
                            // A ingestor whose input bypasses the quiesce control dispatches what
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
                    return Err(refusal.change_context(IngestorIntakeError::Dispatch));
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
            .change_context(IngestorIntakeError::Dispatch)?;
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
            reason = "the ingestor host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn intake_unacknowledged_payload(
        &mut self,
        payload: BufferedIngestPayload,
    ) -> IngestorIntakeResult<bool> {
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
    /// place or is refused and counted, as this ingestor's admission says. The wait ends when the
    /// ingestor stops, which drops the payload, or when a newer quiesce decision arrives, under
    /// which the control decides on the payload again: a buffer retains it, a drop policy drops
    /// and counts it, and a suspension lets it dispatch, so it waits on.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the ingestor host routes admitted payloads and tracks their acknowledgements"
        )
    )]
    async fn deliver_unacknowledged(
        &mut self,
        mut payload: BufferedIngestPayload,
        mut decided: IngestorQuiesceObservation,
    ) -> IngestorIntakeResult<bool> {
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
                        .change_context(IngestorIntakeError::Dispatch));
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
            .change_context(IngestorIntakeError::Dispatch)?;
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
    /// ingestor took the batch in at `ingested_at`, and returns the metadata rows of the payloads
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

/// The payload bytes one ingestor message lends to its ingest group, with the metadata row that stays
/// with them whatever their codec decides.
fn batch_payload<'a>(
    message: &IngestorIntakeMessage<'a>,
) -> BatchPayload<'a, IngestMetadataRow<'a>> {
    BatchPayload {
        payload: message.payload,
        entry: message.metadata,
    }
}

#[async_trait]
impl IngestorHostServices for RuntimeIngestorHost {
    async fn intake_unacknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
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
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
        let quiesced = self.quiesce.observation().is_quiesced();
        let acknowledgement = if self.buffered_intake && quiesced {
            self.retain_acknowledged(messages).await?
        } else {
            self.dispatch_acknowledged(messages).await?
        };
        Ok(IngestorBatchAcknowledgement::new(acknowledgement))
    }

    async fn flush(&mut self) -> IngestorIntakeResult<()> {
        self.runtime
            .flush_ingest_collector(
                &self.domain,
                &self.ingestor,
                &self.branched_senders,
                &mut self.collector,
            )
            .await
            .change_context(IngestorIntakeError::Flush)
    }

    async fn replay_buffered(&mut self) -> IngestorIntakeResult<bool> {
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
            "ingestor instance stopped before its transport confirmed positions it read; the \
             transport presents them again"
        );
    }
}

impl RuntimeIngestorHost {
    /// Delivers the oldest payload this instance's quiesce buffer retained, and answers whether it
    /// delivered one.
    ///
    /// Nothing can present a retained payload again: an endpoint already answered it and a ingestor
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
            reason = "the ingestor host delivers one retained payload on each loop turn while its \
                      buffer drains"
        )
    )]
    async fn deliver_retained(&mut self) -> IngestorIntakeResult<bool> {
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
            .change_context(IngestorIntakeError::Dispatch)?;
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
        reason = "the retained paced ingestor host handles every poll and acknowledgement"
    )
)]
trait PacedIngestorHostServices: IngestorHostServices {
    /// Takes in the messages `poll` returned, in intake batches of at most `intake_batch`
    /// messages, each an ingest group of its own.
    async fn intake_poll(
        &mut self,
        poll: IngestorPoll,
        intake_batch: NonZeroUsize,
    ) -> IngestorIntakeResult<bool>;
    fn should_skip_poll(&self) -> bool;
    fn record_poll_error(&self, reason: String);
}

#[async_trait]
impl PacedIngestorHostServices for RuntimeIngestorHost {
    async fn intake_poll(
        &mut self,
        poll: IngestorPoll,
        intake_batch: NonZeroUsize,
    ) -> IngestorIntakeResult<bool> {
        RuntimeIngestorHost::intake_poll(self, poll, intake_batch).await
    }

    fn should_skip_poll(&self) -> bool {
        self.quiesce.should_skip_poll()
    }

    fn record_poll_error(&self, reason: String) {
        RuntimeIngestorHost::record_poll_error(self, reason);
    }
}

#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained cadence selects each paced ingestor poll"
    )
)]
trait PacedIngestorCadence: Send + 'static {
    async fn next(&mut self, cancellation: &CancellationToken) -> DomainClockWaitResult<Timestamp>;
}

#[async_trait]
impl PacedIngestorCadence for DomainCadence {
    async fn next(&mut self, cancellation: &CancellationToken) -> DomainClockWaitResult<Timestamp> {
        let occurrence = DomainCadence::next(self, cancellation).await?;
        Ok(occurrence.due_at())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PacedIngestorAction {
    Poll,
    Restart,
    Stop,
}

async fn prepare_paced_source<C, H>(
    source: &mut C,
    host: &mut H,
    ready: &mut bool,
    shutdown: &mut watch::Receiver<bool>,
) -> PacedIngestorAction
where
    C: IngestorConnector,
    H: PacedIngestorHostServices,
{
    if !host.wait_until_active().await {
        return PacedIngestorAction::Stop;
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
            PacedIngestorAction::Restart
        } else {
            PacedIngestorAction::Stop
        };
    }

    if !*ready || source.needs_resume() {
        flush_paced_source(host).await;
        match source.resume().await {
            Ok(IngestorResume::Ready) => {
                *ready = true;
                host.mark_ready();
                host.clear_transient_error();
            }
            Ok(IngestorResume::Waiting { retry_after }) => {
                *ready = false;
                host.mark_unready();
                return if wait_for_paced_retry(host, shutdown, retry_after).await {
                    PacedIngestorAction::Restart
                } else {
                    PacedIngestorAction::Stop
                };
            }
            Err(error) => {
                *ready = false;
                host.mark_unready();
                host.record_poll_error(format!("{error:#}"));
                return if wait_for_paced_retry(host, shutdown, SOURCE_ERROR_RETRY).await {
                    PacedIngestorAction::Restart
                } else {
                    PacedIngestorAction::Stop
                };
            }
        }
    }

    PacedIngestorAction::Poll
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained paced source handles every poll, retry and acknowledgement"
    )
)]
async fn run_paced_ingestor<C, H, D>(
    mut source: C,
    mut host: H,
    mut cadence: D,
    intake_batch: NonZeroUsize,
    mut shutdown: watch::Receiver<bool>,
) where
    C: PacedIngestorConnector,
    H: PacedIngestorHostServices,
    D: PacedIngestorCadence,
{
    let cadence_cancellation = CancellationToken::new();
    let mut ready = false;

    loop {
        nervix_primitives::task::consume_budget().await;
        match prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await {
            PacedIngestorAction::Poll => {}
            PacedIngestorAction::Restart => continue,
            PacedIngestorAction::Stop => break,
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
        match host.intake_poll(poll, intake_batch).await {
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
    H: IngestorHostServices,
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
    H: IngestorHostServices,
{
    nervix_primitives::select! {
        changed = shutdown.changed() => !(changed.is_err() || *shutdown.borrow()),
        _ = host.wait_for_quiesce_change() => true,
        _ = sleep(delay) => true,
    }
}

/// Runs a request-scoped ingestor until shutdown.
///
/// Its requests are admitted and dispatched on the request path through the intake the ingestor
/// bound, so the loop reads nothing from the ingestor. It replays what a quiesce buffer retained for
/// those requests once the buffer is released, and closes the ingestor at shutdown so its routes stop
/// receiving requests.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained request source handles every admitted request and acknowledgement"
    )
)]
pub(super) async fn run_request_ingestor<C>(
    mut source: C,
    mut host: IngestorHost,
    mut shutdown: watch::Receiver<bool>,
) where
    C: IngestorConnector,
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
pub(super) async fn run_ingestor_instance_with_retry<C>(
    mut source: C,
    mut host: IngestorHost,
    policy: BrokerIngestorPolicy,
    mut shutdown: watch::Receiver<bool>,
) where
    C: BrokerIngestorConnector,
{
    let BrokerIngestorPolicy {
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
                Ok(IngestorResume::Ready) => {
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
                    // A quiesce released while the ingestor was resuming was not waited on by
                    // anything, so the iteration starts over and replays what it buffered
                    // before the loop blocks on the next batch.
                    continue;
                }
                Ok(IngestorResume::Waiting { retry_after }) => {
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
                    let delay = ingestor_retry_delay(retry_delay);
                    host.record_transient_error(ingestor_failure_reason(&error), delay);
                    host.report_error(format!("{error:#}"));
                    if !wait_for_retry(&mut host, &mut shutdown, delay).await {
                        break;
                    }
                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                    continue;
                }
            }
        }

        // A failed rewind of a replayable ingestor leaves the connector's cursor beyond an
        // unacknowledged message. Reestablish its assignment and retry that same rewind before
        // reading another batch: acknowledging a later Kafka offset would otherwise commit past the
        // missing record. Only a replayable ingestor still holds a pending rejection here, because a
        // resume discards a broker-redelivered one and a ingestor without positions never keeps one.
        if let Some(positions) = pending_rejection.take() {
            if let Err(error) = source.reject(&positions).await {
                let delay = ingestor_retry_delay(retry_delay);
                host.record_transient_error(ingestor_failure_reason(&error), delay);
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
                let delay = ingestor_retry_delay(retry_delay);
                if !end.wait_for_retry(&mut host, &mut shutdown, delay).await {
                    break;
                }
                retry_delay = next_retry_delay(retry_delay, retry_policy);
                continue;
            }
            ReadOutcome::Closed => break,
            ReadOutcome::Failed(error) => {
                let delay = ingestor_retry_delay(retry_delay);
                host.record_transient_error(ingestor_failure_reason(&error), delay);
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
            let retry_after = ingestor_retry_delay(retry_delay);
            match handle_batch(&mut source, &mut host, policy, retry_after, messages).await {
                BatchDisposition::Accepted => {
                    retry_delay = retry_policy.backoff;
                }
                BatchDisposition::Retry => {
                    let delay = ingestor_retry_delay(retry_delay);
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
                    let delay = ingestor_retry_delay(retry_delay);
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

fn batch_request(acknowledgement: IngestorAckPolicy) -> IngestorBatchRequest {
    match acknowledgement {
        IngestorAckPolicy::None | IngestorAckPolicy::Sequential { .. } => IngestorBatchRequest {
            max_messages: NonZeroUsize::MIN,
            batch_timeout: None,
        },
        IngestorAckPolicy::Parallel {
            max_in_flight,
            batch_timeout,
            ..
        } => IngestorBatchRequest {
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
    host: &mut IngestorHost,
    policy: BrokerIngestorPolicy,
    retry_after: Duration,
    messages: Vec<C::Message>,
) -> BatchDisposition<C::Position>
where
    C: BrokerIngestorConnector,
{
    let settlement = BatchSettlement {
        position_class: policy.position_class,
        retry_after,
    };
    if let (IngestorSettlement::Individual, IngestorAckPolicy::Parallel { timeout, .. }) =
        (C::SETTLEMENT, policy.acknowledgement)
    {
        return IndividualBatch::intake(host, messages, timeout)
            .await
            .settle(source, host, settlement)
            .await;
    }
    let positions = messages
        .iter()
        .map(|message| message.position().clone())
        .collect::<Vec<_>>();
    let intake_messages = messages
        .iter()
        .map(|message| IngestorIntakeMessage {
            payload: message.payload(),
            metadata: message.metadata(),
        })
        .collect();
    match policy.acknowledgement {
        IngestorAckPolicy::None => {
            let intake = host.intake_unacknowledged(intake_messages).await;
            if let Err(error) = intake {
                host.report_error(format!("{error:#}"));
                return BatchDisposition::Accepted;
            }
            confirm_batch(source, host, settlement, positions).await
        }
        IngestorAckPolicy::Sequential { timeout, .. }
        | IngestorAckPolicy::Parallel { timeout, .. } => {
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
/// drops, so it drops only once the ingestor has settled the positions. A stop that arrives before
/// every tree resolved leaves the positions unconfirmed, which the host reports.
async fn settle_acknowledged_batch<C>(
    source: &mut C,
    host: &mut IngestorHost,
    settlement: BatchSettlement,
    mut acknowledgement: IngestorBatchAcknowledgement,
    timeout: Duration,
    positions: Vec<C::Position>,
) -> BatchDisposition<C::Position>
where
    C: BrokerIngestorConnector,
{
    let outcome = acknowledgement.wait(timeout).await;
    let disposition = match outcome {
        IngestorAcknowledgementOutcome::Ack => {
            confirm_batch(source, host, settlement, positions).await
        }
        IngestorAcknowledgementOutcome::NoAck(reason) => {
            host.handle_ack_failure(reason);
            reject_batch(source, host, settlement, positions).await
        }
        IngestorAcknowledgementOutcome::Shutdown => {
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
    host: &mut IngestorHost,
    settlement: BatchSettlement,
    positions: Vec<C::Position>,
) -> BatchDisposition<C::Position>
where
    C: BrokerIngestorConnector,
{
    let confirmed = source.acknowledge(&positions).await;
    let Err(error) = confirmed else {
        host.clear_transient_error();
        return BatchDisposition::Accepted;
    };
    host.record_transient_error(ingestor_failure_reason(&error), settlement.retry_after);
    host.report_error(format!("{error:#}"));
    reject_batch(source, host, settlement, positions).await
}

/// Rejects `positions` so the ingestor presents them again, and decides by the ingestor's position
/// class what a rejection that failed leaves pending.
///
/// A rejection that fails is the ingestor's transient status, as a confirmation that fails is,
/// until a later confirmation succeeds.
async fn reject_batch<C>(
    source: &mut C,
    host: &mut IngestorHost,
    settlement: BatchSettlement,
    positions: Vec<C::Position>,
) -> BatchDisposition<C::Position>
where
    C: BrokerIngestorConnector,
{
    let Err(error) = source.reject(&positions).await else {
        return BatchDisposition::Retry;
    };
    host.record_transient_error(ingestor_failure_reason(&error), settlement.retry_after);
    host.report_error(format!("{error:#}"));
    match settlement.position_class {
        PositionClass::Replayable | PositionClass::BrokerRedelivered => {
            BatchDisposition::RetryRejection(positions)
        }
        // A ingestor without positions has nothing a later rejection could name.
        PositionClass::None => BatchDisposition::Retry,
    }
}

async fn flush_for_lifecycle(host: &mut IngestorHost) {
    if let Err(error) = host.flush().await {
        host.report_error(format!("{error:#}"));
    }
}

fn ingestor_retry_delay(delay: Duration) -> Duration {
    delay.max(SOURCE_ERROR_RETRY)
}

async fn wait_for_retry(
    host: &mut IngestorHost,
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
#[path = "connector_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "connector_shuttle_tests.rs"]
mod shuttle_tests;

#[cfg(test)]
#[path = "connector_extension_tests.rs"]
mod extension_tests;

#[cfg(test)]
#[path = "connector_decode_tests.rs"]
mod decode_tests;

#[cfg(test)]
#[path = "connector_reconnect_tests.rs"]
mod reconnect_tests;

#[cfg(test)]
#[path = "connector_confirmation_tests.rs"]
mod confirmation_tests;

#[cfg(test)]
#[path = "connector_read_tests.rs"]
mod read_tests;

#[cfg(test)]
#[path = "connector_metadata_tests.rs"]
mod metadata_tests;

#[cfg(test)]
#[path = "connector_poll_tests.rs"]
mod poll_tests;

impl Drop for RuntimeIngestorHost {
    fn drop(&mut self) {
        self.readiness.retire();
    }
}
