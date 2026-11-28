//! Payloads whose `ON INGESTION` unfolding the node's extension workers cannot take now, which wait
//! for a place whatever their source.
//!
//! Layer: test harness.
//! - **Owns.** Driving the production ingestor host's poll and batch intake into a full extension
//!   class, and the delivery, quiesce decisions, stop, transport service and work accounting of a
//!   payload that waits.
//! - **Depends on.** The production ingestor host, the endpoint ingestor fixture that installs the
//!   domain, and a single-worker executor.
//! - **Must not know.** Broker drivers or the ingestor loops that call the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use futures_util::FutureExt as _;
use nervix_connector::{IngestorPollMessage, NoIngestHeaders, RetainedIngestHeaders};
use nervix_execution::CpuClass;
use nervix_models::{
    CreateSchema, IngestQuiesceMode, IngestQuiesceOverflow, ParseAsType, SchemaField,
};

use super::*;
use crate::runtime::ingestors::endpoint::tests::{start_endpoint_ingestor, unfolding_wire_format};

/// How a test host takes in what its ingestor hands it.
#[derive(Debug, Clone, Copy)]
pub(super) struct HostIntake {
    /// Whether what the ingestor reads while quiesced passes through the quiesce control.
    buffered: bool,
    /// Whether the host flushes the ingest group after each payload it takes in, rather than
    /// leaving the group to its ingestor loop's flush.
    flush_each_intake: bool,
}

/// The intake of an ingestor whose input passes through its quiesce control, which flushes the
/// ingest group after each payload.
pub(super) const WAITING_INTAKE: HostIntake = HostIntake {
    buffered: true,
    flush_each_intake: true,
};

/// The intake a paced ingestor's host has: its loop, not each intake, flushes the ingest group.
pub(super) const PACED_INTAKE: HostIntake = HostIntake {
    buffered: false,
    flush_each_intake: false,
};

/// A running domain whose endpoint ingestor `event_source` unfolds every payload on the extension
/// workers of a single-worker executor, and a subscriber to the relay `events` it feeds.
pub(super) struct UnfoldingDomain {
    pub(super) runtime: Runtime,
    executor: Executor,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    delivered: RelaySubscriptionReceiver<RelayRecordBatch>,
    /// The branched entrypoints the hosts this fixture made feed.
    entrypoints: Vec<Arc<IngestorRouteRuntime>>,
}

impl UnfoldingDomain {
    pub(super) async fn start() -> Self {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_source");
        let executor = single_worker_executor();
        let runtime = Runtime::with_executor(executor.clone());
        // The fixture places its relay on node-1, so the runtime joins as node-1 to own it and
        // hand what a host delivers to the relay's subscribers.
        attach_loopback_cluster(
            &runtime,
            &ClusterNodeName::parse("node-1").expect("valid name"),
        )
        .await;
        let runtime =
            start_endpoint_ingestor(runtime, &domain, &ingestor, unfolding_wire_format()).await;
        let subscriber = RelaySubscriptionDefinition::new(
            Arc::new(compile_schema(&CreateSchema {
                labels: Default::default(),
                name: named("event"),
                fields: vec![SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            ResolvedBranching::unbranched(),
        );
        let delivered = runtime
            .subscribe_stream(&domain, &named("events"), &subscriber)
            .await
            .expect("the fixture's relay accepts a subscriber that describes its rows");
        Self {
            runtime,
            executor,
            domain,
            ingestor,
            delivered,
            entrypoints: Vec::new(),
        }
    }

    /// Stops every entrypoint the fixture's hosts fed, then the runtime.
    pub(super) async fn finish(self) {
        for entrypoint in &self.entrypoints {
            entrypoint.stop(RouteStop::Finish).await;
        }
        self.runtime.shutdown().await;
    }

    /// A second host for the fixture's ingestor, beside the endpoint ingestor the fixture runs, which
    /// takes in what an ingestor hands it through `quiesce` as `intake` says, and stops when
    /// `shutdown` turns true.
    pub(super) async fn host(
        &mut self,
        quiesce: Arc<IngestorQuiesceControl>,
        shutdown: watch::Receiver<bool>,
        intake: HostIntake,
    ) -> RuntimeIngestorHost {
        let connector_host = self.connector_host(
            crate::runtime::ingestors::connector_host::IngestorEntityServices::default(),
        );
        self.host_driving(quiesce, shutdown, intake, connector_host)
            .await
    }

    /// The handle the fixture's ingestor unit hands its connector, with what `entity` prepared
    /// for it, as its composer builds it before the connector opens.
    pub(super) fn connector_host(
        &self,
        entity: crate::runtime::ingestors::connector_host::IngestorEntityServices,
    ) -> IngestorConnectorHost {
        self.runtime
            .ingestor_connector_host(&self.planned_ingestor().ingestor, entity)
    }

    /// The start plan the fixture's execution holds for its one ingestor.
    fn planned_ingestor(&self) -> Arc<IngestorStartPlan> {
        self.runtime
            .inner
            .executions
            .get(&self.domain)
            .assured("the fixture installs its domain's execution")
            .revision
            .entrypoints
            .ingestors()
            .find(|plan| plan.ingestor.name == self.ingestor)
            .cloned()
            .assured("the fixture's execution plans its one ingestor")
    }

    /// A second host for the fixture's ingestor that drives its connector with `connector_host`.
    pub(super) async fn host_driving(
        &mut self,
        quiesce: Arc<IngestorQuiesceControl>,
        shutdown: watch::Receiver<bool>,
        intake: HostIntake,
        connector_host: IngestorConnectorHost,
    ) -> RuntimeIngestorHost {
        let plan = self.planned_ingestor();
        let BoundIngestor {
            input,
            dependencies,
        } = self
            .runtime
            .ingestor_dependencies(&plan.ingestor, &plan.input)
            .await
            .expect("the fixture's ingestor binds its dependencies");
        let BoundIngestorInput::Transport { codec, .. } = input else {
            panic!("the fixture's ingestor reads a transport");
        };
        let IngestorDependencies {
            handles,
            output_routes,
            filter_where,
            rate_limit,
            branched_templates,
            metrics,
        } = dependencies;
        let entrypoints = self.runtime.start_branched_entrypoint_runtimes(
            &self.domain,
            &ModelName::from(&self.ingestor),
            branched_templates,
        );
        self.entrypoints.extend(entrypoints.runtimes);
        RuntimeIngestorHost::new(RuntimeIngestorHostSpec {
            kind: plan.ingestor.declared_input.entity_kind(),
            handles,
            runtime: self.runtime.clone(),
            domain: self.domain.clone(),
            ingestor: self.ingestor.clone(),
            timestamp_source: plan.ingestor.timestamp_source.clone(),
            output_routes,
            filter_where,
            codec,
            metrics,
            branched_senders: entrypoints.senders,
            quiesce,
            shutdown,
            unit: Unit::SINGLE,
            readiness: self.runtime.prepare_ingestor_readiness(
                &self.domain,
                &self.ingestor,
                Unit::SINGLE,
            ),
            buffered_intake: intake.buffered,
            flush_each_intake: intake.flush_each_intake,
            general_error_policy: plan.ingestor.general_error_policy.clone(),
            rate_limit,
            connector_host,
        })
    }

    /// The work the fixture's ingestor unit has admitted in the node's quiesce accounting, which
    /// a drain, an ownership handoff and a graceful stop wait for.
    fn admitted_work(&self) -> usize {
        self.runtime
            .unit_quiesce_counters(
                &self.domain,
                NodeUnitRef::new(
                    NodeRef::new(ModelKind::Ingestor, &self.ingestor),
                    Unit::SINGLE,
                ),
            )
            .admitted_work()
    }

    /// The quiesce control the fixture's running ingestor holds.
    pub(super) fn quiesce(&self) -> Arc<IngestorQuiesceControl> {
        self.runtime
            .ingestor_quiesce_control(&self.domain, &self.ingestor, nervix_models::Unit::SINGLE)
            .assured("a running ingestor holds its quiesce control")
    }

    /// Waits until a payload's unfolding reaches the full extension class. It charges the
    /// payload's memory before it asks the class for a place, and the fill charges none.
    async fn reach_unfolding(&self) {
        nervix_primitives::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = self.executor.snapshot();
                if snapshot.relay_memory.reserved_bytes > 0 {
                    return;
                }
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .expect("the payload's unfolding reaches the full extension class");
    }

    /// The `user_id` of every row the relay delivers until `rows` have arrived.
    pub(super) async fn delivered_user_ids(&mut self, rows: usize) -> Vec<Option<RuntimeValue>> {
        let mut user_ids = Vec::with_capacity(rows);
        while user_ids.len() < rows {
            nervix_primitives::task::consume_budget().await;
            let batch =
                nervix_primitives::time::timeout(Duration::from_secs(10), self.delivered.recv())
                    .await
                    .expect("the payload is delivered once the extension workers have room")
                    .expect("the relay keeps its subscriber while the ingestor runs");
            for row in 0..batch.batch.batch().num_rows() {
                user_ids.push(
                    batch
                        .batch
                        .value(row, "user_id")
                        .expect("a delivered row reads its own field"),
                );
            }
        }
        user_ids
    }
}

/// One poll that returned `{"user_id":<user_id>}`. The fixture's domain is unpaced, so the
/// instant the poll was observed at decides nothing here.
fn poll_of(user_id: i64) -> IngestorPoll {
    IngestorPoll {
        messages: vec![IngestorPollMessage {
            payload: format!(r#"{{"user_id":{user_id}}}"#).into_bytes(),
            headers: RetainedIngestHeaders::none(),
            metadata: nervix_connector::RetainedIngestMetadata::none(0),
        }],
        undecodable: Vec::new(),
        dropped_header_values: 0,
        observed_at: Timestamp::from_unix_nanos(1),
    }
}

/// A paced ingestor has already moved past the poll it hands the host, so when the extension
/// workers cannot take the poll's unfolding, the poll waits for them instead of being refused and
/// lost. Once they have room, it is delivered.
#[nervix_primitives::test]
async fn a_poll_waits_for_the_extension_workers_when_the_node_cannot_unfold_it() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let polled = nervix_primitives::task::spawn(async move {
        let intake = host.intake_poll(poll_of(7), NonZeroUsize::MIN).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    let source = fixture
        .runtime
        .describe_local_ingestor(
            &fixture.domain,
            &fixture.ingestor,
            Unit::SINGLE,
            nervix_models::IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Endpoint),
        )
        .expect("the fixture's endpoint is running");
    assert!(source.source_metrics.iter().any(|line| {
        line.starts_with("nervix_ingestor_read_payloads_total{") && line.contains(" total=1 ")
    }));
    assert!(
        source
            .source_metrics
            .iter()
            .any(|line| line.starts_with("nervix_ingestor_read_bytes_total{")
                && line.contains(" total=13 "))
    );
    assert!(
        errors.recv().now_or_never().is_none(),
        "a poll waiting for the extension workers is not reported as an ingestor error"
    );

    filled.release().await;
    let (mut host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), polled)
        .await
        .expect("the poll is unfolded once the extension workers have room")
        .expect("the polling task does not panic");
    assert!(
        matches!(intake, Ok(true)),
        "the poll's messages enter the ingest group: {intake:?}"
    );
    host.flush()
        .await
        .expect("the group holding the poll flushes");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    fixture.finish().await;
}

/// An ingestor hands the host an unacknowledged batch it cannot present again, so when the
/// extension workers cannot take the batch's unfolding, the batch waits for them instead of being
/// refused and lost, and counts meanwhile as work its unit admitted.
#[nervix_primitives::test]
async fn an_unacknowledged_batch_waits_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let payload = br#"{"user_id":7}"#.to_vec();
        let intake = host
            .intake_unacknowledged(
                vec![IngestorIntakeMessage {
                    payload: &payload,
                    metadata: IngestMetadataRow::headers(
                        &NoIngestHeaders,
                        &nervix_connector::NoIngestMetadata(0),
                    ),
                }],
                None,
            )
            .await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    assert!(
        errors.recv().now_or_never().is_none(),
        "a batch waiting for the extension workers is not reported as an ingestor error"
    );
    assert_eq!(
        fixture.admitted_work(),
        1,
        "the waiting batch is work its unit admitted, which a drain waits for"
    );

    filled.release().await;
    let (host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the batch is unfolded once the extension workers have room")
        .expect("the intake task does not panic");
    assert!(
        matches!(intake, Ok(())),
        "the unacknowledged batch is taken in: {intake:?}"
    );
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(fixture.admitted_work(), 0);
    assert_eq!(host.interactions.unfolding_refused.get(), 0);
    fixture.finish().await;
}

/// Session control of a transport the host keeps serviced while a payload waits. Each service the
/// host starts publishes its deadline, then services until the host stops polling it.
struct RecordingHold {
    services: watch::Sender<Vec<Option<Instant>>>,
}

#[async_trait]
impl IngestorReadHold for RecordingHold {
    async fn maintain(&mut self, until: Option<Instant>) -> Report<IngestorError> {
        self.services.send_modify(|services| services.push(until));
        std::future::pending().await
    }
}

/// A batch waiting for the extension workers holds its ingestor's loop, so the host keeps the
/// transport's session serviced for as long as the batch waits, with no deadline, and takes the
/// batch in once the workers have room.
#[nervix_primitives::test]
async fn the_host_services_the_transport_while_a_batch_waits_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;
    let (services, mut serviced) = watch::channel(Vec::new());

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let mut hold = RecordingHold { services };
        let payload = br#"{"user_id":7}"#.to_vec();
        let intake = host
            .intake_unacknowledged(
                vec![IngestorIntakeMessage {
                    payload: &payload,
                    metadata: IngestMetadataRow::headers(
                        &NoIngestHeaders,
                        &nervix_connector::NoIngestMetadata(0),
                    ),
                }],
                Some(&mut hold),
            )
            .await;
        (host, intake)
    });
    nervix_primitives::time::timeout(
        Duration::from_secs(10),
        serviced.wait_for(|services| !services.is_empty()),
    )
    .await
    .expect("the host services the transport while the batch waits")
    .expect("the hold publishes its services while the intake runs");
    assert_eq!(
        *serviced.borrow(),
        vec![None],
        "a batch waiting for the extension workers holds the transport with no deadline"
    );

    filled.release().await;
    let (_host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the batch is unfolded once the extension workers have room")
        .expect("the intake task does not panic");
    assert!(matches!(intake, Ok(())), "{intake:?}");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    fixture.finish().await;
}

/// Hands `host` one unacknowledged batch carrying `{"user_id":<user_id>}`.
async fn take_in_unacknowledged(
    host: &mut RuntimeIngestorHost,
    user_id: i64,
) -> IngestorIntakeResult<()> {
    let payload = format!(r#"{{"user_id":{user_id}}}"#).into_bytes();
    host.intake_unacknowledged(
        vec![IngestorIntakeMessage {
            payload: &payload,
            metadata: IngestMetadataRow::headers(
                &NoIngestHeaders,
                &nervix_connector::NoIngestMetadata(0),
            ),
        }],
        None,
    )
    .await
}

/// A quiesce control for the fixture's ingestor, outside the runtime, that `mode` governs.
pub(super) fn quiesce_control(
    fixture: &UnfoldingDomain,
    mode: IngestQuiesceMode,
) -> Arc<IngestorQuiesceControl> {
    let metrics = RuntimeMetrics::default();
    let labels = metrics.register_ingestor_quiesce(
        &fixture.domain,
        &fixture.ingestor,
        Unit::SINGLE,
        None,
        nervix_models::IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Endpoint),
    );
    Arc::new(IngestorQuiesceControl::new(mode, metrics, labels))
}

/// The ingestor stopping ends a batch's wait for the extension workers at once, and the intake ends
/// as stopped, so its loop leaves the batch's positions unconfirmed for a transport that keeps
/// them to present again. Nothing of the batch stays behind.
#[nervix_primitives::test]
async fn the_ingestor_stopping_ends_a_batch_waiting_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let (shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let intake = take_in_unacknowledged(&mut host, 7).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    shutdown_tx.send_replace(true);
    let (host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the stop ends the wait while the extension class is still full")
        .expect("the intake task does not panic");
    let stopped = intake.expect_err("the stop ends the intake");
    assert!(stopped.current_context().stopped(), "{stopped:#}");
    assert_eq!(
        host.collector.len(),
        0,
        "the undelivered batch leaves no row behind"
    );
    assert_eq!(host.interactions.unfolding_refused.get(), 0);
    assert_eq!(fixture.admitted_work(), 0);
    filled.release().await;
    fixture.finish().await;
}

/// A poll waits for the extension workers the same way, and the ingestor stopping ends that wait,
/// and the intake, at once.
#[nervix_primitives::test]
async fn the_ingestor_stopping_ends_a_poll_waiting_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let (shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let polled = nervix_primitives::task::spawn(async move {
        let intake = host.intake_poll(poll_of(7), NonZeroUsize::MIN).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    shutdown_tx.send_replace(true);
    let (host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), polled)
        .await
        .expect("the stop ends the wait while the extension class is still full")
        .expect("the polling task does not panic");
    let stopped = intake.expect_err("the stop ends the intake");
    assert!(stopped.current_context().stopped(), "{stopped:#}");
    assert_eq!(
        host.collector.len(),
        0,
        "the undelivered poll leaves no row behind"
    );
    filled.release().await;
    fixture.finish().await;
}

/// A new quiesce whose mode buffers what arrives takes over a batch waiting for the extension
/// workers: the buffer retains it, counted with its bytes, and it is delivered when the buffer
/// drains after resume.
#[nervix_primitives::test]
async fn a_new_buffering_quiesce_retains_a_batch_waiting_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(
        &fixture,
        IngestQuiesceMode::Buffer {
            max_size: "1MiB".to_string(),
            overflow: IngestQuiesceOverflow::DropOldest,
        },
    );
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(quiesce.clone(), shutdown, WAITING_INTAKE)
        .await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let intake = take_in_unacknowledged(&mut host, 7).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    quiesce.engage(IngestorQuiesceCause::EntityHold);
    let (mut host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the new quiesce ends the wait while the extension class is still full")
        .expect("the intake task does not panic");
    assert!(matches!(intake, Ok(())), "{intake:?}");
    assert_eq!(quiesce.counters().buffered_records, 1);
    assert_eq!(quiesce.counters().buffered_bytes, br#"{"user_id":7}"#.len());

    quiesce.release(IngestorQuiesceCause::EntityHold);
    filled.release().await;
    let drained = host.replay_buffered().await;
    assert!(matches!(drained, Ok(true)), "{drained:?}");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(quiesce.counters().buffered_records, 0);
    fixture.finish().await;
}

/// A new quiesce whose mode drops what arrives drops a batch waiting for the extension workers,
/// and counts it as that mode counts every payload it drops.
#[nervix_primitives::test]
async fn a_new_dropping_quiesce_drops_a_batch_waiting_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(&fixture, IngestQuiesceMode::Drop);
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(quiesce.clone(), shutdown, WAITING_INTAKE)
        .await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let intake = take_in_unacknowledged(&mut host, 7).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    quiesce.engage(IngestorQuiesceCause::EntityHold);
    let (host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the new quiesce ends the wait while the extension class is still full")
        .expect("the intake task does not panic");
    assert!(matches!(intake, Ok(())), "{intake:?}");
    assert_eq!(quiesce.counters().dropped_total, 1);
    assert_eq!(
        host.collector.len(),
        0,
        "the dropped batch leaves no row behind"
    );
    assert_eq!(host.interactions.unfolding_refused.get(), 0);
    filled.release().await;
    fixture.finish().await;
}

/// A new quiesce that suspends the ingestor still lets a payload it already handed over dispatch,
/// so a poll waiting for the extension workers waits on through it and is delivered once they
/// have room.
#[nervix_primitives::test]
async fn a_poll_waiting_for_the_extension_workers_waits_on_through_a_suspending_quiesce() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(&fixture, IngestQuiesceMode::Suspend);
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(quiesce.clone(), shutdown, WAITING_INTAKE)
        .await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let polled = nervix_primitives::task::spawn(async move {
        let intake = host.intake_poll(poll_of(7), NonZeroUsize::MIN).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    quiesce.engage(IngestorQuiesceCause::EntityHold);
    filled.release().await;
    let (mut host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), polled)
        .await
        .expect("the poll is unfolded once the extension workers have room")
        .expect("the polling task does not panic");
    assert!(matches!(intake, Ok(true)), "{intake:?}");
    host.flush()
        .await
        .expect("the group holding the poll flushes");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(quiesce.counters(), IngestorQuiesceCounters::default());
    fixture.finish().await;
}

/// A new quiesce whose mode buffers what arrives takes over a poll waiting for the extension
/// workers, and the poll drains from the buffer once the workers have room.
#[nervix_primitives::test]
async fn a_new_buffering_quiesce_retains_a_poll_waiting_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(
        &fixture,
        IngestQuiesceMode::Buffer {
            max_size: "1MiB".to_string(),
            overflow: IngestQuiesceOverflow::DropOldest,
        },
    );
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(quiesce.clone(), shutdown, WAITING_INTAKE)
        .await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let polled = nervix_primitives::task::spawn(async move {
        let intake = host.intake_poll(poll_of(7), NonZeroUsize::MIN).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    quiesce.engage(IngestorQuiesceCause::EntityHold);
    let (mut host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), polled)
        .await
        .expect("the new quiesce ends the wait while the extension class is still full")
        .expect("the polling task does not panic");
    assert!(matches!(intake, Ok(false)), "{intake:?}");
    assert_eq!(quiesce.counters().buffered_records, 1);

    quiesce.release(IngestorQuiesceCause::EntityHold);
    filled.release().await;
    let drained = host.replay_buffered().await;
    assert!(matches!(drained, Ok(true)), "{drained:?}");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(quiesce.counters().buffered_records, 0);
    fixture.finish().await;
}

/// An ingestor whose input bypasses the quiesce control dispatches what it already read whatever
/// the quiesce decides, so a batch waiting for the extension workers waits again through a new
/// quiesce and is delivered once they have room.
#[nervix_primitives::test]
async fn a_batch_whose_ingestor_bypasses_the_quiesce_control_waits_on_through_a_new_quiesce() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(&fixture, IngestQuiesceMode::Suspend);
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(
            quiesce.clone(),
            shutdown,
            HostIntake {
                buffered: false,
                flush_each_intake: true,
            },
        )
        .await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let intake = take_in_unacknowledged(&mut host, 7).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    quiesce.engage(IngestorQuiesceCause::EntityHold);
    filled.release().await;
    let (_host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the batch is unfolded once the extension workers have room")
        .expect("the intake task does not panic");
    assert!(matches!(intake, Ok(())), "{intake:?}");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(quiesce.counters(), IngestorQuiesceCounters::default());
    fixture.finish().await;
}

/// Hands `host` one acknowledged batch carrying `{"user_id":<user_id>}`, and returns the
/// acknowledgement the intake answered with.
async fn take_in_acknowledged(
    host: &mut RuntimeIngestorHost,
    user_id: i64,
) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
    let payload = format!(r#"{{"user_id":{user_id}}}"#).into_bytes();
    host.intake_acknowledged(
        vec![IngestorIntakeMessage {
            payload: &payload,
            metadata: IngestMetadataRow::headers(
                &NoIngestHeaders,
                &nervix_connector::NoIngestMetadata(0),
            ),
        }],
        None,
    )
    .await
}

/// An acknowledged batch the extension workers cannot unfold now waits for them, counting meanwhile
/// as work its unit admitted, and is delivered and acknowledged once they have room.
#[nervix_primitives::test]
async fn an_acknowledged_batch_waits_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = fixture.quiesce();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let intake = take_in_acknowledged(&mut host, 7).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    assert_eq!(fixture.admitted_work(), 1);

    filled.release().await;
    let (host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the batch is unfolded once the extension workers have room")
        .expect("the intake task does not panic");
    let mut acknowledgement = intake.expect("the acknowledged batch is taken in");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(
        acknowledgement.wait(Duration::from_secs(10)).await,
        IngestorAcknowledgementOutcome::Acknowledged
    );
    assert_eq!(host.interactions.unfolding_refused.get(), 0);
    assert_eq!(fixture.admitted_work(), 0);
    fixture.finish().await;
}

/// The ingestor stopping ends an acknowledged batch's wait for the extension workers at once, and
/// the intake ends as stopped, so its loop leaves the batch's positions unconfirmed for its
/// transport to present again.
#[nervix_primitives::test]
async fn the_ingestor_stopping_ends_an_acknowledged_batch_waiting_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = fixture.quiesce();
    let (shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let intake = take_in_acknowledged(&mut host, 7).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    shutdown_tx.send_replace(true);
    let (host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the stop ends the wait while the extension class is still full")
        .expect("the intake task does not panic");
    let stopped = match intake {
        Ok(_) => panic!("the stop ends the intake before the batch is dispatched"),
        Err(stopped) => stopped,
    };
    assert!(stopped.current_context().stopped(), "{stopped:#}");
    assert_eq!(host.interactions.unfolding_refused.get(), 0);
    assert_eq!(fixture.admitted_work(), 0);
    filled.release().await;
    fixture.finish().await;
}

/// A new quiesce whose mode buffers what arrives takes over an acknowledged batch waiting for the
/// extension workers: the buffer retains its message, which leaves the batch nothing to wait on,
/// and the message is delivered when the buffer drains after resume.
#[nervix_primitives::test]
async fn a_new_buffering_quiesce_takes_over_an_acknowledged_batch_waiting_for_the_extension_workers()
 {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(
        &fixture,
        IngestQuiesceMode::Buffer {
            max_size: "1MiB".to_string(),
            overflow: IngestQuiesceOverflow::DropOldest,
        },
    );
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(quiesce.clone(), shutdown, WAITING_INTAKE)
        .await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let intake = take_in_acknowledged(&mut host, 7).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    quiesce.engage(IngestorQuiesceCause::EntityHold);
    let (mut host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the new quiesce ends the wait while the extension class is still full")
        .expect("the intake task does not panic");
    let mut acknowledgement = intake.expect("the buffer takes the batch's message over");
    assert_eq!(
        acknowledgement.wait(Duration::from_secs(10)).await,
        IngestorAcknowledgementOutcome::Acknowledged
    );
    assert_eq!(quiesce.counters().buffered_records, 1);

    quiesce.release(IngestorQuiesceCause::EntityHold);
    filled.release().await;
    let drained = host.replay_buffered().await;
    assert!(matches!(drained, Ok(true)), "{drained:?}");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(quiesce.counters().buffered_records, 0);
    fixture.finish().await;
}

/// A suspension lets an acknowledged batch the ingestor already read dispatch, with its
/// acknowledgement, as it would unquiesced.
#[nervix_primitives::test]
async fn an_acknowledged_batch_read_while_suspended_dispatches_with_its_acknowledgement() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(&fixture, IngestQuiesceMode::Suspend);
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(quiesce.clone(), shutdown, WAITING_INTAKE)
        .await;

    quiesce.engage(IngestorQuiesceCause::EntityHold);
    let mut acknowledgement = take_in_acknowledged(&mut host, 7)
        .await
        .expect("a suspension lets the batch dispatch");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(
        acknowledgement.wait(Duration::from_secs(10)).await,
        IngestorAcknowledgementOutcome::Acknowledged
    );
    assert_eq!(quiesce.counters(), IngestorQuiesceCounters::default());
    quiesce.release(IngestorQuiesceCause::EntityHold);
    fixture.finish().await;
}

/// An acknowledged batch the graph acknowledged stays outstanding work of its ingestor until its
/// ingestor drops the batch acknowledgement, which the loop does once the transport has confirmed
/// the batch's positions, so a drain or an ownership handoff that waits for the ingestor's roots
/// also waits for that confirmation.
#[nervix_primitives::test]
async fn an_acknowledged_batch_stays_outstanding_until_its_ingestor_settles_it() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = fixture.quiesce();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let mut acknowledgement = take_in_acknowledged(&mut host, 7)
        .await
        .expect("the extension workers have room for the batch");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(
        acknowledgement.wait(Duration::from_secs(10)).await,
        IngestorAcknowledgementOutcome::Acknowledged
    );
    assert_eq!(
        host.ack_root_trackers
            .ingestor_outstanding_for_ownership_handoff(),
        1,
        "the acknowledged batch is outstanding until its ingestor has settled it"
    );

    drop(acknowledgement);
    assert_eq!(
        host.ack_root_trackers
            .ingestor_outstanding_for_ownership_handoff(),
        0
    );
    fixture.finish().await;
}
