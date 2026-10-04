//! Live unacknowledged payloads whose `ON INGESTION` unfolding the node's extension workers cannot
//! take now.
//!
//! Layer: test harness.
//! - **Owns.** Driving the production source host's poll and batch intake into a full extension
//!   class, and the delivery, refusal and accounting each source's contract promises.
//! - **Depends on.** The production source host, the endpoint ingestor fixture that installs the
//!   domain, and a single-worker executor.
//! - **Must not know.** Broker drivers or the source loops that call the host.

use futures_util::FutureExt as _;
use nervix_connector::{NoIngestHeaders, SourcePollMessage};
use nervix_execution::CpuClass;
use nervix_models::{
    CreateSchema, IngestQuiesceMode, IngestQuiesceOverflow, ParseAsType, SchemaField,
};

use super::*;
use crate::runtime::ingestors::endpoint::tests::{start_endpoint_ingestor, unfolding_wire_format};

/// How a test host takes in what its source hands it.
#[derive(Debug, Clone, Copy)]
struct HostIntake {
    /// What an unacknowledged payload does when the extension workers have no room for it.
    admission: QueueAdmission,
    /// Whether what the source reads while quiesced passes through the quiesce control.
    buffered: bool,
}

/// The intake of a source whose loop may be held while its payload waits for the extension
/// workers.
const WAITING_INTAKE: HostIntake = HostIntake {
    admission: QueueAdmission::WaitForPlace,
    buffered: true,
};

/// The intake of a source whose transport a held loop would cost its connection or unbounded
/// memory.
const REFUSING_INTAKE: HostIntake = HostIntake {
    admission: QueueAdmission::RefuseWhenFull,
    buffered: true,
};

/// A running domain whose endpoint ingestor `event_source` unfolds every payload on the extension
/// workers of a single-worker executor, and a subscriber to the relay `events` it feeds.
struct UnfoldingDomain {
    runtime: Runtime,
    executor: Executor,
    domain: DomainName,
    ingestor: IngestorName,
    delivered: RelaySubscriptionReceiver<RelayRecordBatch>,
    /// The branched entrypoints the hosts this fixture made feed.
    entrypoints: Vec<Arc<IngestorRouteRuntime>>,
}

impl UnfoldingDomain {
    async fn start() -> Self {
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
    async fn finish(self) {
        for entrypoint in &self.entrypoints {
            entrypoint.shutdown().await;
        }
        self.runtime.shutdown().await;
    }

    /// A second host for the fixture's ingestor, beside the endpoint source the fixture runs, which
    /// takes in what a source hands it through `quiesce` as `intake` says, and stops when
    /// `shutdown` turns true.
    async fn host(
        &mut self,
        quiesce: Arc<IngestorQuiesceControl>,
        shutdown: watch::Receiver<bool>,
        intake: HostIntake,
    ) -> RuntimeSourceHost {
        let plan = self
            .runtime
            .inner
            .executions
            .get(&self.domain)
            .assured("the fixture installs its domain's execution")
            .revision
            .entrypoints
            .ingestors()
            .find(|plan| plan.ingestor.name == self.ingestor)
            .cloned()
            .assured("the fixture's execution plans its one ingestor");
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
            branched_templates,
            metrics,
        } = dependencies;
        let entrypoints = self.runtime.start_branched_entrypoint_runtimes(
            &self.domain,
            &ModelName::from(&self.ingestor),
            branched_templates,
        );
        self.entrypoints.extend(entrypoints.runtimes);
        RuntimeSourceHost::new(RuntimeSourceHostSpec {
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
            instance_index: 1,
            readiness: self
                .runtime
                .prepare_ingestor_readiness(&self.domain, &self.ingestor, NonZeroU64::MIN)
                .into_iter()
                .next()
                .assured("the source fixture declares one instance"),
            metadata_kind: plan.ingestor.metadata_kind(),
            buffered_intake: intake.buffered,
            flush_each_intake: true,
            unacknowledged_admission: intake.admission,
        })
    }

    /// The quiesce control the fixture's running ingestor holds.
    fn quiesce(&self) -> Arc<IngestorQuiesceControl> {
        self.runtime
            .ingestor_quiesce_control(&self.domain, &self.ingestor)
            .assured("a running ingestor holds its quiesce control")
    }

    /// Waits until a payload's unfolding reaches the full extension class. It charges the
    /// payload's memory before it asks the class for a place, and the fill charges none.
    async fn reach_unfolding(&self) {
        nervix_primitives::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = self.executor.snapshot();
                if snapshot.extension_cpu.refused > 0 || snapshot.relay_memory.reserved_bytes > 0 {
                    return;
                }
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .expect("the payload's unfolding reaches the full extension class");
    }

    /// The `user_id` of every row the relay delivers until `rows` have arrived.
    async fn delivered_user_ids(&mut self, rows: usize) -> Vec<Option<RuntimeValue>> {
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
fn poll_of(user_id: i64) -> SourcePoll {
    SourcePoll {
        messages: vec![SourcePollMessage {
            payload: format!(r#"{{"user_id":{user_id}}}"#).into_bytes(),
            headers: RetainedIngestHeaders::none(),
        }],
        failures: Vec::new(),
        observed_at: Timestamp::from_unix_nanos(1),
    }
}

/// A paced source has already moved past the poll it hands the host, so when the extension
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
        let intake = host.intake_poll(poll_of(7)).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    assert_eq!(
        fixture.executor.snapshot().extension_cpu.refused,
        0,
        "the poll's unfolding waits for a place instead of being refused"
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

/// A source whose transport stays connected while its loop waits hands the host an unacknowledged
/// batch it cannot present again, so when the extension workers cannot take the batch's
/// unfolding, the batch waits for them instead of being refused and lost.
#[nervix_primitives::test]
async fn an_unacknowledged_batch_waits_for_the_extension_workers_when_its_source_can_hold() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let taken_in = nervix_primitives::task::spawn(async move {
        let payload = br#"{"user_id":7}"#.to_vec();
        let intake = host
            .intake(SourceIntakeBatch {
                messages: vec![SourceIntakeMessage {
                    payload: &payload,
                    metadata: IngestMetadataRow::Headers {
                        headers: &NoIngestHeaders,
                    },
                }],
                mode: SourceIntakeMode::Unacknowledged,
            })
            .await
            .map(|outcome| outcome.acknowledgements.len());
        (host, intake)
    });
    fixture.reach_unfolding().await;
    assert_eq!(
        fixture.executor.snapshot().extension_cpu.refused,
        0,
        "the batch's unfolding waits for a place instead of being refused"
    );
    assert!(
        errors.recv().now_or_never().is_none(),
        "a batch waiting for the extension workers is not reported as an ingestor error"
    );

    filled.release().await;
    let (_host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), taken_in)
        .await
        .expect("the batch is unfolded once the extension workers have room")
        .expect("the intake task does not panic");
    assert!(
        matches!(intake, Ok(0)),
        "an unacknowledged batch is taken in without acknowledgements: {intake:?}"
    );
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    fixture.finish().await;
}

/// Hands `host` one unacknowledged batch carrying `{"user_id":<user_id>}`, and answers how many
/// acknowledgements the intake returned.
async fn take_in_unacknowledged(
    host: &mut RuntimeSourceHost,
    user_id: i64,
) -> SourceIntakeResult<usize> {
    let payload = format!(r#"{{"user_id":{user_id}}}"#).into_bytes();
    host.intake(SourceIntakeBatch {
        messages: vec![SourceIntakeMessage {
            payload: &payload,
            metadata: IngestMetadataRow::Headers {
                headers: &NoIngestHeaders,
            },
        }],
        mode: SourceIntakeMode::Unacknowledged,
    })
    .await
    .map(|outcome| outcome.acknowledgements.len())
}

/// A quiesce control for the fixture's ingestor, outside the runtime, that `mode` governs.
fn quiesce_control(
    fixture: &UnfoldingDomain,
    mode: IngestQuiesceMode,
) -> Arc<IngestorQuiesceControl> {
    let metrics = RuntimeMetrics::default();
    let labels = metrics.register_ingestor_quiesce(&fixture.domain, &fixture.ingestor, None);
    Arc::new(IngestorQuiesceControl::new(mode, metrics, labels))
}

/// A source whose transport a held loop would cost its connection or unbounded memory refuses an
/// unacknowledged batch the extension workers cannot unfold now. The refusal is reported and
/// counted, and the next batch is delivered once the workers have room.
#[nervix_primitives::test]
async fn an_unacknowledged_batch_is_refused_and_counted_when_its_source_cannot_hold() {
    let mut fixture = UnfoldingDomain::start().await;
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, REFUSING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let refused = take_in_unacknowledged(&mut host, 7)
        .await
        .expect_err("a full extension class refuses the batch at once");
    assert!(
        format!("{refused:#}").contains("the node's bounded execution did not unfold the payload"),
        "the failure names the unfolding the node did not admit: {refused:#}"
    );
    assert_eq!(host.unfolding_refused.get(), 1);
    assert_eq!(
        host.collector.len(),
        0,
        "the refused batch leaves no row behind"
    );

    filled.release().await;
    let accepted = take_in_unacknowledged(&mut host, 8).await;
    assert!(matches!(accepted, Ok(0)), "{accepted:?}");
    assert_eq!(host.unfolding_refused.get(), 1);
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(8))]
    );
    fixture.finish().await;
}

/// The ingestor stopping ends a batch's wait for the extension workers at once, and the batch
/// is dropped with the rest of its in-flight input instead of being counted as refused.
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
    assert!(matches!(intake, Ok(0)), "{intake:?}");
    assert_eq!(
        host.collector.len(),
        0,
        "the dropped batch leaves no row behind"
    );
    assert_eq!(host.unfolding_refused.get(), 0);
    filled.release().await;
    fixture.finish().await;
}

/// A poll waits for the extension workers the same way, and the ingestor stopping ends that wait
/// at once.
#[nervix_primitives::test]
async fn the_ingestor_stopping_ends_a_poll_waiting_for_the_extension_workers() {
    let mut fixture = UnfoldingDomain::start().await;
    let (shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let polled = nervix_primitives::task::spawn(async move {
        let intake = host.intake_poll(poll_of(7)).await;
        (host, intake)
    });
    fixture.reach_unfolding().await;
    shutdown_tx.send_replace(true);
    let (host, intake) = nervix_primitives::time::timeout(Duration::from_secs(10), polled)
        .await
        .expect("the stop ends the wait while the extension class is still full")
        .expect("the polling task does not panic");
    assert!(matches!(intake, Ok(false)), "{intake:?}");
    assert_eq!(
        host.collector.len(),
        0,
        "the dropped poll leaves no row behind"
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
    assert!(matches!(intake, Ok(0)), "{intake:?}");
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
    assert!(matches!(intake, Ok(0)), "{intake:?}");
    assert_eq!(quiesce.counters().dropped_total, 1);
    assert_eq!(
        host.collector.len(),
        0,
        "the dropped batch leaves no row behind"
    );
    assert_eq!(host.unfolding_refused.get(), 0);
    filled.release().await;
    fixture.finish().await;
}

/// A new quiesce that suspends the source still lets a payload it already handed over dispatch,
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
        let intake = host.intake_poll(poll_of(7)).await;
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

/// A refusing source's payload read while a suspension is engaged still dispatches, and the
/// extension workers refuse it as they refuse the source's live payloads: it is reported and
/// counted, and nothing of it stays behind.
#[nervix_primitives::test]
async fn a_refusing_sources_payload_read_while_suspended_is_refused_and_counted() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(&fixture, IngestQuiesceMode::Suspend);
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(quiesce.clone(), shutdown, REFUSING_INTAKE)
        .await;

    quiesce.engage(IngestorQuiesceCause::EntityHold);
    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let refused = take_in_unacknowledged(&mut host, 7)
        .await
        .expect_err("a full extension class refuses the payload at once");
    assert!(
        format!("{refused:#}").contains("the node's bounded execution did not unfold the payload"),
        "the failure names the unfolding the node did not admit: {refused:#}"
    );
    assert_eq!(host.unfolding_refused.get(), 1);
    assert_eq!(
        host.collector.len(),
        0,
        "the refused payload leaves no row behind"
    );
    assert_eq!(quiesce.counters(), IngestorQuiesceCounters::default());
    filled.release().await;
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
        let intake = host.intake_poll(poll_of(7)).await;
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

/// A source whose input bypasses the quiesce control dispatches what it already read whatever
/// the quiesce decides, so a batch waiting for the extension workers waits again through a new
/// quiesce and is delivered once they have room.
#[nervix_primitives::test]
async fn a_batch_whose_source_bypasses_the_quiesce_control_waits_on_through_a_new_quiesce() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = quiesce_control(&fixture, IngestQuiesceMode::Suspend);
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(
            quiesce.clone(),
            shutdown,
            HostIntake {
                admission: QueueAdmission::WaitForPlace,
                buffered: false,
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
    assert!(matches!(intake, Ok(0)), "{intake:?}");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(quiesce.counters(), IngestorQuiesceCounters::default());
    fixture.finish().await;
}

/// Hands `host` one acknowledged batch carrying `{"user_id":<user_id>}`, and returns the
/// acknowledgements the intake answered with.
async fn take_in_acknowledged(
    host: &mut RuntimeSourceHost,
    user_id: i64,
) -> SourceIntakeResult<Vec<SourceAcknowledgement>> {
    let payload = format!(r#"{{"user_id":{user_id}}}"#).into_bytes();
    host.intake(SourceIntakeBatch {
        messages: vec![SourceIntakeMessage {
            payload: &payload,
            metadata: IngestMetadataRow::Headers {
                headers: &NoIngestHeaders,
            },
        }],
        mode: SourceIntakeMode::Acknowledged,
    })
    .await
    .map(|outcome| outcome.acknowledgements)
}

/// An acknowledged batch the extension workers cannot unfold now fails, so its source rejects it
/// and presents it again; the refusal is not counted as a payload lost. Once the workers have room
/// the batch is delivered and acknowledged.
#[nervix_primitives::test]
async fn an_acknowledged_batch_is_refused_for_redelivery_when_the_extension_workers_are_full() {
    let mut fixture = UnfoldingDomain::start().await;
    let quiesce = fixture.quiesce();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let filled = FilledCpuClass::fill(&fixture.executor, CpuClass::Extension).await;
    let refused = match take_in_acknowledged(&mut host, 7).await {
        Ok(_) => panic!("a full extension class refuses an acknowledged batch at once"),
        Err(refused) => refused,
    };
    assert!(
        format!("{refused:#}").contains("the node's bounded execution did not unfold the payload"),
        "the failure names the unfolding the node did not admit: {refused:#}"
    );
    assert_eq!(
        host.unfolding_refused.get(),
        0,
        "a batch its source presents again is not counted as refused"
    );

    filled.release().await;
    let acknowledgements = take_in_acknowledged(&mut host, 7)
        .await
        .expect("the extension workers have room for the batch presented again");
    assert_eq!(acknowledgements.len(), 1);
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    for acknowledgement in acknowledgements {
        assert_eq!(
            acknowledgement.wait(Duration::from_secs(10)).await,
            SourceAcknowledgementOutcome::Ack
        );
    }
    fixture.finish().await;
}

/// A suspension lets an acknowledged batch the source already read dispatch, with its
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
    let acknowledgements = take_in_acknowledged(&mut host, 7)
        .await
        .expect("a suspension lets the batch dispatch");
    assert_eq!(acknowledgements.len(), 1);
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    for acknowledgement in acknowledgements {
        assert_eq!(
            acknowledgement.wait(Duration::from_secs(10)).await,
            SourceAcknowledgementOutcome::Ack
        );
    }
    assert_eq!(quiesce.counters(), IngestorQuiesceCounters::default());
    quiesce.release(IngestorQuiesceCause::EntityHold);
    fixture.finish().await;
}
