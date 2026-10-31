//! Ingestor host loop engagement checks under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** Controlled ingestor dispatch and quiesce interleavings for the host loops, the
//!   interleavings of a stop or a suspension of intake with a broker read that gathers a batch,
//!   the interleavings of a lost broker connection with the broker loop's acknowledgements and
//!   rejections, and the interleavings of a drain's observation with a batch's confirmation.
//! - **Depends on.** The production ingestor loops, ingestor contract, quiesce control, the runtime's
//!   batch acknowledgement and confirmed ACK roots, the gathering doubles of the read tests, the
//!   connection-bound doubles of the reconnect tests and Shuttle.
//! - **Must not know.** Broker drivers, model planning or external payload transports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_connector::{IngestMetadataRow, IngestorReadCancellation};
use nervix_connector_fake::{FakeIngestor as ChannelIngestor, FakeMessage as ChannelMessage};
use nervix_model_harness::shuttle::{check_pct, check_random};
use nervix_primitives::sync::{StdArc, mpsc, oneshot, watch};
// The dispatch count is a record: an unmodeled atomic is not a Shuttle scheduling point, so it
// changes in the same scheduling step as the dispatch it counts.
use nervix_primitives::unmodeled::sync::atomic::{AtomicUsize, Ordering};

use super::{
    read_tests::{
        GatheringIngestor, HostControl, IngestorSignals, ReadEvent, ReadLog, RecordingHost,
        assert_every_taken_position_taken_in_first, gathering_policy,
    },
    reconnect_tests::{
        RedeliveringBroker, RedeliveringIngestor, RedeliveryHost, sequential_policy,
    },
    *,
};

const JOIN: &str = "the host loop runs until the check sends shutdown";

struct ChannelCadence {
    due: mpsc::Receiver<Timestamp>,
}

#[async_trait]
impl PacedIngestorCadence for ChannelCadence {
    async fn next(
        &mut self,
        _cancellation: &CancellationToken,
    ) -> DomainClockWaitResult<Timestamp> {
        Ok(self
            .due
            .recv()
            .await
            .assured("the check holds the cadence sender until it shuts the source down"))
    }
}

struct ChannelHost {
    quiesce: Arc<IngestorQuiesceControl>,
    quiesce_observation: IngestorQuiesceObservation,
    replay_messages: Option<mpsc::Receiver<ChannelMessage>>,
    dispatch_started: Option<oneshot::Sender<()>>,
    finish_dispatch: Option<oneshot::Receiver<()>>,
    engagement_observed: Option<oneshot::Sender<()>>,
    dispatched: StdArc<AtomicUsize>,
}

impl ChannelHost {
    fn mark_engagement_observed(&mut self) {
        if let Some(observed) = self.engagement_observed.take() {
            observed
                .send(())
                .assured("the check retains its engagement observer until the host reports it");
        }
    }
}

#[async_trait]
impl IngestorHostServices for ChannelHost {
    async fn intake_unacknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
        for message in messages {
            nervix_primitives::task::consume_budget().await;
            let payload = BufferedIngestPayload::new(
                message.payload,
                BufferedIngestMetadata::without_headers(),
                Timestamp::from_unix_nanos(1),
            );
            let decision = self.quiesce.intake(0, payload, false);
            if let Some(started) = self.dispatch_started.take() {
                started
                    .send(())
                    .assured("the check waits for the first dispatch to begin");
                self.finish_dispatch
                    .take()
                    .verified("the first dispatch owns its completion receiver")
                    .await
                    .assured("the check releases dispatch after engagement returns");
            }
            if let IngestorQuiesceIntake::Dispatch(_) = decision {
                self.dispatched.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    async fn intake_acknowledged(
        &mut self,
        _messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
        panic!("a channel source reads without acknowledgements");
    }

    async fn flush(&mut self) -> IngestorIntakeResult<()> {
        Ok(())
    }

    async fn replay_buffered(&mut self) -> IngestorIntakeResult<bool> {
        if self.quiesce.is_quiesced() {
            return Ok(false);
        }
        let Some(messages) = self.replay_messages.as_mut() else {
            return Ok(false);
        };
        let Ok(message) = messages.try_recv() else {
            return Ok(false);
        };
        self.intake_unacknowledged(vec![IngestorIntakeMessage {
            payload: &message.payload,
            metadata: message.metadata(),
        }])
        .await?;
        Ok(true)
    }

    fn next_flush(&self) -> Option<Instant> {
        None
    }

    fn should_suspend_intake(&self) -> bool {
        self.quiesce.should_suspend_intake()
    }

    async fn wait_for_quiesce_change(&mut self) {
        self.quiesce
            .wait_for_change_since(&mut self.quiesce_observation)
            .await;
        self.mark_engagement_observed();
    }

    async fn wait_until_not_suspended(&mut self) {
        self.mark_engagement_observed();
        self.quiesce.wait_until_not_suspended().await;
    }

    async fn wait_until_active(&mut self) -> bool {
        true
    }

    fn mark_ready(&self) {}
    fn mark_unready(&self) {}
    fn record_transient_error(&self, reason: String, _retry_after: Duration) {
        panic!("channel source had an unexpected transient error: {reason}");
    }
    fn clear_transient_error(&self) {}
    fn report_error(&self, message: String) {
        panic!("channel source reported an unexpected error: {message}");
    }
    fn handle_ack_failure(&self, reason: String) {
        panic!("unacknowledged channel source reported an ACK failure: {reason}");
    }
    fn record_transport_redeliveries(&self, positions: usize) {
        panic!("a channel source without positions discarded {positions} after a reconnect");
    }
    fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
        panic!("a channel source whose payloads all fit dropped {payloads} for their size");
    }
    fn record_unconfirmed_stop(&self, positions: usize) {
        panic!("a channel source without positions left {positions} unconfirmed");
    }
}

#[async_trait]
impl PacedIngestorHostServices for ChannelHost {
    /// The channel double takes in a whole poll at once: the checks drive polls of one message.
    async fn intake_poll(
        &mut self,
        poll: IngestorPoll,
        _intake_batch: NonZeroUsize,
    ) -> IngestorIntakeResult<bool> {
        let mut messages = Vec::with_capacity(poll.messages.len());
        for message in &poll.messages {
            nervix_primitives::task::consume_budget().await;
            messages.push(IngestorIntakeMessage {
                payload: &message.payload,
                metadata: IngestMetadataRow::headers(&message.headers),
            });
        }
        if messages.is_empty() {
            return Ok(false);
        }
        self.intake_unacknowledged(messages).await?;
        Ok(true)
    }

    fn should_skip_poll(&self) -> bool {
        self.quiesce.should_skip_poll()
    }

    fn record_poll_error(&self, reason: String) {
        panic!("channel source had an unexpected poll error: {reason}");
    }
}

#[derive(Clone, Copy)]
enum IngestorFamily {
    Broker,
    Paced,
    Request,
}

const CAUSES: [IngestorQuiesceCause; 4] = [
    IngestorQuiesceCause::MemoryPressure,
    IngestorQuiesceCause::EntityHold,
    IngestorQuiesceCause::OwnershipHandoff,
    IngestorQuiesceCause::Shutdown,
];
const OBSERVATION_STEPS: usize = 64;

async fn observe_within_steps(mut observed: oneshot::Receiver<()>) {
    for _ in 0..OBSERVATION_STEPS {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::select! {
            biased;
            result = &mut observed => {
                result.assured("the host reports observation while its check is alive");
                return;
            }
            _ = nervix_primitives::task::yield_now() => {}
        }
    }
    panic!("the host did not observe quiesce engagement within {OBSERVATION_STEPS} steps");
}

async fn send_message(messages: &mpsc::Sender<ChannelMessage>, position: u64) {
    messages
        .send(ChannelMessage::new(
            position,
            vec![u8::try_from(position).assured("the check uses positions one and two")],
        ))
        .await
        .assured("the ingestor loop keeps its payload channel open until shutdown");
}

fn engagement_during_dispatch(family: IngestorFamily, cause: IngestorQuiesceCause) {
    shuttle::future::block_on(async move {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::try_from("shuttle")
            .assured("the check's domain is an identifier-shaped name");
        let ingestor = IngestorName::try_from("source")
            .assured("the check's ingestor is an identifier-shaped name");
        let labels = metrics.register_ingestor_quiesce(&domain, &ingestor, None);
        let quiesce = Arc::new(IngestorQuiesceControl::new(
            IngestQuiesceMode::Drop,
            metrics,
            labels,
        ));
        let dispatched = StdArc::new(AtomicUsize::new(0));
        let (messages, receiver) = mpsc::channel(2);
        let (replays, replay_receiver) = mpsc::channel(2);
        let (cadence, cadence_receiver) = mpsc::channel(2);
        let (dispatch_started, first_dispatch) = oneshot::channel();
        let (finish_dispatch, dispatch_release) = oneshot::channel();
        let (engagement_observed, observed) = oneshot::channel();
        let host = ChannelHost {
            quiesce: quiesce.clone(),
            quiesce_observation: quiesce.observation(),
            replay_messages: match family {
                IngestorFamily::Request => Some(replay_receiver),
                IngestorFamily::Broker | IngestorFamily::Paced => None,
            },
            dispatch_started: Some(dispatch_started),
            finish_dispatch: Some(dispatch_release),
            engagement_observed: Some(engagement_observed),
            dispatched: dispatched.clone(),
        };
        let (shutdown, shutdown_rx) = watch::channel(false);
        let source = ChannelIngestor::new(receiver);
        if matches!(family, IngestorFamily::Request) {
            // Request sources only replay work already buffered when their loop starts.
            send_message(&replays, 1).await;
        }
        let loop_task = match family {
            IngestorFamily::Broker => {
                nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
                    source,
                    IngestorHost::new(host),
                    BrokerIngestorPolicy {
                        acknowledgement: IngestorAckPolicy::None,
                        retry: INGESTOR_RECONNECT_POLICY,
                        position_class: PositionClass::None,
                    },
                    shutdown_rx,
                ))
            }
            IngestorFamily::Paced => nervix_primitives::task::spawn(run_paced_ingestor(
                source,
                host,
                ChannelCadence {
                    due: cadence_receiver,
                },
                NonZeroUsize::MIN,
                shutdown_rx,
            )),
            IngestorFamily::Request => nervix_primitives::task::spawn(run_request_ingestor(
                source,
                IngestorHost::new(host),
                shutdown_rx,
            )),
        };

        match family {
            IngestorFamily::Broker => send_message(&messages, 1).await,
            IngestorFamily::Paced => {
                send_message(&messages, 1).await;
                cadence
                    .send(Timestamp::from_unix_nanos(1))
                    .await
                    .assured("the paced host awaits its first occurrence");
            }
            IngestorFamily::Request => {}
        }
        first_dispatch
            .await
            .assured("the source begins its first dispatch");
        let (engaged, engagement_returned) = oneshot::channel();
        let engager = nervix_primitives::task::spawn({
            let quiesce = quiesce.clone();
            async move {
                quiesce.engage(cause);
                engaged
                    .send(())
                    .assured("the check waits for engagement to return");
            }
        });
        engagement_returned
            .await
            .assured("the engagement task returns");
        match family {
            IngestorFamily::Broker => send_message(&messages, 2).await,
            IngestorFamily::Paced => {
                send_message(&messages, 2).await;
                cadence
                    .send(Timestamp::from_unix_nanos(2))
                    .await
                    .assured("the paced host keeps its cadence channel open");
            }
            IngestorFamily::Request => send_message(&replays, 2).await,
        }
        finish_dispatch
            .send(())
            .assured("the first dispatch waits for release");

        observe_within_steps(observed).await;
        assert_eq!(
            dispatched.load(Ordering::SeqCst),
            1,
            "only the pre-engagement payload may have reached dispatch"
        );
        let releaser = nervix_primitives::task::spawn(async move {
            quiesce.release(cause);
        });
        releaser.await.assured(JOIN);
        shutdown.send_replace(true);
        loop_task.await.assured(JOIN);
        engager.await.assured(JOIN);
        drop(messages);
        drop(replays);
        drop(cadence);
    });
}

fn explore_family(family: IngestorFamily) {
    for cause in CAUSES {
        check_random(move || engagement_during_dispatch(family, cause), 250);
        check_pct(move || engagement_during_dispatch(family, cause), 250, 3);
    }
}

#[test]
fn shuttle_broker_ingestor_observes_engagement_during_dispatch() {
    explore_family(IngestorFamily::Broker);
}

#[test]
fn shuttle_paced_ingestor_observes_engagement_during_dispatch() {
    explore_family(IngestorFamily::Paced);
}

#[test]
fn shuttle_request_ingestor_observes_engagement_during_dispatch() {
    explore_family(IngestorFamily::Request);
}

/// What interrupts a broker read in the gathering check.
#[derive(Clone, Copy)]
enum ReadInterruptionCause {
    /// The ingestor stops.
    Stop,
    /// Intake is suspended, as a quiesce engagement that stops reading suspends it.
    Suspension,
}

/// A broker ingestor gathering a batch of up to three messages while its transport delivers two, and
/// a stop or a suspension of intake meets the read at any point of its gathering.
fn interrupted_gathering(cause: ReadInterruptionCause) {
    shuttle::future::block_on(async move {
        let (transport, receiver) = mpsc::channel(2);
        let events = ReadLog::new();
        let signals = Arc::new(IngestorSignals::new());
        let control = Arc::new(HostControl::new());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let loop_task = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
            GatheringIngestor::new(receiver, events.clone(), signals.clone()),
            IngestorHost::new(RecordingHost::new(events.clone(), control.clone())),
            gathering_policy(3),
            shutdown_rx,
        ));
        let deliverer = nervix_primitives::task::spawn(async move {
            for position in [1, 2] {
                // The ingestor drops its transport once the loop has stopped, which may come first.
                if transport.send(position).await.is_err() {
                    break;
                }
            }
            // The transport stays open, so only the stop or the suspension ends the read.
            transport
        });

        match cause {
            ReadInterruptionCause::Stop => {}
            ReadInterruptionCause::Suspension => {
                // Only a ingestor the loop has resumed is suspended, so intake is suspended once the
                // loop reads. The batch can never fill, so the loop always reaches the suspension.
                signals.reading.notified().await;
                control.suspend_intake();
                signals.suspended.notified().await;
            }
        }
        shutdown.send_replace(true);
        loop_task.await.assured(JOIN);
        let _transport = deliverer
            .await
            .assured("the deliverer ends once it sent both messages or the source stopped");

        let events = events.events();
        assert_every_taken_position_taken_in_first(&events);
        if let ReadInterruptionCause::Suspension = cause {
            assert!(
                events.contains(&ReadEvent::Suspended),
                "the suspension reached the source"
            );
        }
    });
}

#[test]
fn shuttle_an_interrupted_broker_read_hands_the_host_every_message_it_took() {
    for cause in [
        ReadInterruptionCause::Stop,
        ReadInterruptionCause::Suspension,
    ] {
        check_random(move || interrupted_gathering(cause), 500);
        check_pct(move || interrupted_gathering(cause), 500, 3);
    }
}

/// A broker connection lost at any point of the loop, possibly twice, while the host acknowledges
/// two messages one at a time.
fn lost_connection_during_settlement(losses: usize) {
    shuttle::future::block_on(async move {
        let broker = RedeliveringBroker::shared([1, 2]);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let loop_task = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
            RedeliveringIngestor::new(broker.clone()),
            IngestorHost::new(RedeliveryHost::new(broker.clone(), false)),
            sequential_policy(PositionClass::BrokerRedelivered),
            shutdown_rx,
        ));
        let loser = nervix_primitives::task::spawn({
            let broker = broker.clone();
            async move {
                for _ in 0..losses {
                    nervix_primitives::task::yield_now().await;
                    broker.lock().lose_connection();
                }
            }
        });

        // The ingestor closes once both messages are acknowledged and nothing is left to redeliver,
        // which ends the loop without a shutdown.
        loop_task
            .await
            .assured("the host loop ends when its source closes");
        loser
            .await
            .assured("the loss task ends after its last loss");
        drop(shutdown);

        let broker = broker.lock();
        let mut acknowledged = broker.acknowledged.clone();
        acknowledged.sort_unstable();
        assert_eq!(
            acknowledged,
            vec![1, 2],
            "every message is acknowledged exactly once, on the connection that delivered it"
        );
        assert_eq!(
            broker.counted_redeliveries, broker.failed_rejections,
            "each rejection a lost connection failed is discarded and counted exactly once"
        );
    });
}

#[test]
fn shuttle_a_reconnect_discards_rejections_the_broker_redelivers() {
    for losses in [1, 2] {
        check_random(move || lost_connection_during_settlement(losses), 500);
        check_pct(move || lost_connection_during_settlement(losses), 500, 3);
    }
}

/// What one confirmation check observed of its batch, recorded without adding a scheduling point.
#[derive(Default)]
struct ConfirmationProgress {
    /// Batches whose last acknowledgement root the graph has begun to resolve.
    resolved_batches: AtomicUsize,
    /// Batches whose positions the ingestor's transport confirmed.
    confirmed_batches: AtomicUsize,
}

/// A ingestor that hands the loop its two messages as one batch, and whose transport takes a round
/// trip to confirm them.
struct ConfirmingChannelIngestor {
    queue: Vec<u64>,
    progress: StdArc<ConfirmationProgress>,
}

#[async_trait]
impl IngestorConnector for ConfirmingChannelIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = ();

    async fn open(
        _plan: &Self::Plan,
        _instance_index: u64,
    ) -> nervix_connector::IngestorResult<Self> {
        Err(Report::new(IngestorError::Open {
            connector: "confirming",
        }))
    }
}

#[async_trait]
impl BrokerIngestorConnector for ConfirmingChannelIngestor {
    type Message = ChannelMessage;
    type Position = u64;

    async fn next_batch(
        &mut self,
        _request: IngestorBatchRequest,
        _cancellation: &IngestorReadCancellation,
    ) -> nervix_connector::IngestorResult<IngestorBatch<Self::Message>> {
        if self.queue.is_empty() {
            return Ok(IngestorBatch::Closed);
        }
        let mut messages = Vec::with_capacity(self.queue.len());
        for position in self.queue.drain(..) {
            messages.push(ChannelMessage::new(
                position,
                vec![u8::try_from(position).assured("the check uses positions one and two")],
            ));
        }
        Ok(IngestorBatch::Messages(messages))
    }

    async fn acknowledge(
        &mut self,
        _positions: &[Self::Position],
    ) -> nervix_connector::IngestorResult<()> {
        // The request to the broker and its answer: a production ingestor awaits both before it
        // returns, while drains observe the node.
        nervix_primitives::task::yield_now().await;
        nervix_primitives::task::yield_now().await;
        self.progress
            .confirmed_batches
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn reject(
        &mut self,
        positions: &[Self::Position],
    ) -> nervix_connector::IngestorResult<()> {
        panic!("every acknowledgement tree of the check succeeds, but {positions:?} were rejected");
    }
}

/// A host that dispatches each acknowledged batch on the runtime's confirmed roots and hands the
/// roots to the check's graph, which resolves them.
struct GraphHost {
    trackers: IngestorAckRootTrackers,
    roots: mpsc::Sender<Vec<AckSet>>,
    shutdown: watch::Receiver<bool>,
}

#[async_trait]
impl IngestorHostServices for GraphHost {
    async fn intake_unacknowledged(
        &mut self,
        _messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
        panic!("the check's source reads in an acknowledged mode");
    }

    async fn intake_acknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
        let mut roots = Vec::with_capacity(messages.len());
        let mut completions = Vec::with_capacity(messages.len());
        for _ in &messages {
            let (root, completion) = self.trackers.confirmed_root();
            roots.push(root);
            completions.push(completion);
        }
        self.roots
            .send(roots)
            .await
            .assured("the graph receives the one batch the check reads");
        Ok(IngestorBatchAcknowledgement::new(
            RuntimeBatchAcknowledgement::new(completions, self.shutdown.clone(), None),
        ))
    }

    async fn flush(&mut self) -> IngestorIntakeResult<()> {
        Ok(())
    }

    async fn replay_buffered(&mut self) -> IngestorIntakeResult<bool> {
        Ok(false)
    }

    fn next_flush(&self) -> Option<Instant> {
        None
    }

    fn should_suspend_intake(&self) -> bool {
        false
    }

    async fn wait_for_quiesce_change(&mut self) {
        std::future::pending::<()>().await;
    }

    async fn wait_until_not_suspended(&mut self) {
        std::future::pending::<()>().await;
    }

    async fn wait_until_active(&mut self) -> bool {
        true
    }

    fn mark_ready(&self) {}
    fn mark_unready(&self) {}
    fn record_transient_error(&self, reason: String, _retry_after: Duration) {
        panic!("the check's transport confirms every acknowledgement, but it failed: {reason}");
    }
    fn clear_transient_error(&self) {}
    fn report_error(&self, message: String) {
        panic!("the check's source reported an unexpected error: {message}");
    }
    fn handle_ack_failure(&self, reason: String) {
        panic!("every acknowledgement tree of the check succeeds, but one failed: {reason}");
    }
    fn record_transport_redeliveries(&self, positions: usize) {
        panic!("a replayable check source discarded {positions} positions after a reconnect");
    }
    fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
        panic!("a check source whose payloads all fit dropped {payloads} for their size");
    }
    fn record_unconfirmed_stop(&self, positions: usize) {
        panic!("the check's source closes on its own, but {positions} positions were left");
    }
}

/// The ownership-handoff drain reads of one check, each a read-modify-write as a drain makes it.
const DRAIN_OBSERVATIONS: usize = 12;

/// A drain observes the domain at any point while the graph resolves a two-message batch in either
/// order and the ingestor's transport takes a round trip to confirm it.
fn drain_observing_a_confirmation() {
    shuttle::future::block_on(async move {
        let trackers = IngestorAckRootTrackers::detached();
        let progress = StdArc::new(ConfirmationProgress::default());
        let (roots, mut batch_roots) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let retry = ParsedRetryPolicy {
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        let policy = BrokerIngestorPolicy {
            acknowledgement: IngestorAckPolicy::Parallel {
                max_in_flight: NonZeroUsize::new(2).assured("two is a nonzero batch bound"),
                batch_timeout: Duration::from_millis(1),
                timeout: Duration::from_secs(1),
                retry,
            },
            retry,
            position_class: PositionClass::Replayable,
        };
        let loop_task = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
            ConfirmingChannelIngestor {
                queue: vec![1, 2],
                progress: progress.clone(),
            },
            IngestorHost::new(GraphHost {
                trackers: trackers.clone(),
                roots,
                shutdown: shutdown_rx.clone(),
            }),
            policy,
            shutdown_rx,
        ));
        let graph = nervix_primitives::task::spawn({
            let progress = progress.clone();
            async move {
                let roots = batch_roots
                    .recv()
                    .await
                    .assured("the host hands the graph the roots of the batch the source read");
                let unresolved = StdArc::new(AtomicUsize::new(roots.len()));
                let mut resolutions = Vec::with_capacity(roots.len());
                for root in roots {
                    let unresolved = unresolved.clone();
                    let progress = progress.clone();
                    resolutions.push(nervix_primitives::task::spawn(async move {
                        nervix_primitives::task::yield_now().await;
                        // Recorded before the last root resolves, so a drain that reads the batch
                        // resolved reads its roots afterwards.
                        if unresolved.fetch_sub(1, Ordering::SeqCst) == 1 {
                            progress.resolved_batches.fetch_add(1, Ordering::SeqCst);
                        }
                        root.ack_success();
                    }));
                }
                for resolution in resolutions {
                    resolution
                        .await
                        .assured("a root's resolution task does not panic");
                }
            }
        });
        let observer = nervix_primitives::task::spawn({
            let trackers = trackers.clone();
            let progress = progress.clone();
            async move {
                for _ in 0..DRAIN_OBSERVATIONS {
                    nervix_primitives::task::yield_now().await;
                    let resolved = progress.resolved_batches.load(Ordering::SeqCst);
                    let outstanding = trackers.domain_outstanding_for_ownership_handoff();
                    let confirmed = progress.confirmed_batches.load(Ordering::SeqCst);
                    if outstanding == 0 {
                        assert!(
                            confirmed >= resolved,
                            "a drain saw no outstanding work while a resolved batch waited for \
                             its transport to confirm it"
                        );
                    }
                }
            }
        });

        // The ingestor closes once its one batch is settled, which ends the loop without a stop.
        loop_task
            .await
            .assured("the host loop ends when its source closes");
        graph
            .await
            .assured("the graph ends once it resolved the batch");
        observer
            .await
            .assured("the drain observer ends after its reads");
        drop(shutdown);

        assert_eq!(progress.confirmed_batches.load(Ordering::SeqCst), 1);
        assert_eq!(
            trackers.domain_outstanding_for_ownership_handoff(),
            0,
            "the batch's work is released once its source confirmed it"
        );
        assert_eq!(trackers.ingestor_outstanding_for_ownership_handoff(), 0);
    });
}

#[test]
fn shuttle_a_drain_waits_for_the_transport_to_confirm_a_resolved_batch() {
    check_random(drain_observing_a_confirmation, 1_000);
    check_pct(drain_observing_a_confirmation, 1_000, 3);
}

/// The payload the refusing host's codec stand-in refuses, the one at position zero.
const REFUSED_PAYLOAD: [u8; 1] = [0];

/// A host that consumes each payload its codec stand-in refuses through the runtime's batch roots,
/// as the ingestor host consumes a decode rejection, and hands the roots of the rest to the check's
/// graph, which resolves them.
struct RefusingGraphHost {
    trackers: IngestorAckRootTrackers,
    roots: mpsc::Sender<Vec<AckSet>>,
    /// The batch's roots no one has begun to resolve yet.
    unresolved: StdArc<AtomicUsize>,
    progress: StdArc<ConfirmationProgress>,
    shutdown: watch::Receiver<bool>,
}

#[async_trait]
impl IngestorHostServices for RefusingGraphHost {
    async fn intake_unacknowledged(
        &mut self,
        _messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
        panic!("the check's source reads in an acknowledged mode");
    }

    async fn intake_acknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
        self.unresolved.store(messages.len(), Ordering::SeqCst);
        let mut roots = BatchRoots::new(&self.trackers, messages.len());
        let mut dispatched = Vec::with_capacity(messages.len());
        for message in &messages {
            if message.payload != REFUSED_PAYLOAD.as_slice() {
                dispatched.push(roots.dispatched());
                continue;
            }
            // The root is created and resolved in one step, so the batch is recorded resolved once
            // its root exists: a drain that reads the batch resolved reads that root afterwards.
            roots.consumed();
            if self.unresolved.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.progress
                    .resolved_batches
                    .fetch_add(1, Ordering::SeqCst);
            }
        }
        if !dispatched.is_empty() {
            self.roots
                .send(dispatched)
                .await
                .assured("the graph receives the one batch the check reads");
        }
        Ok(IngestorBatchAcknowledgement::new(
            roots.acknowledgement(self.shutdown.clone(), None),
        ))
    }

    async fn flush(&mut self) -> IngestorIntakeResult<()> {
        Ok(())
    }

    async fn replay_buffered(&mut self) -> IngestorIntakeResult<bool> {
        Ok(false)
    }

    fn next_flush(&self) -> Option<Instant> {
        None
    }

    fn should_suspend_intake(&self) -> bool {
        false
    }

    async fn wait_for_quiesce_change(&mut self) {
        std::future::pending::<()>().await;
    }

    async fn wait_until_not_suspended(&mut self) {
        std::future::pending::<()>().await;
    }

    async fn wait_until_active(&mut self) -> bool {
        true
    }

    fn mark_ready(&self) {}
    fn mark_unready(&self) {}
    fn record_transient_error(&self, reason: String, _retry_after: Duration) {
        panic!("the check's transport confirms every acknowledgement, but it failed: {reason}");
    }
    fn clear_transient_error(&self) {}
    fn report_error(&self, message: String) {
        panic!("the check's source reported an unexpected error: {message}");
    }
    fn handle_ack_failure(&self, reason: String) {
        panic!("a consumed payload never fails its batch, but the batch failed: {reason}");
    }
    fn record_transport_redeliveries(&self, positions: usize) {
        panic!("a replayable check source discarded {positions} positions after a reconnect");
    }
    fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
        panic!("a check source whose payloads all fit dropped {payloads} for their size");
    }
    fn record_unconfirmed_stop(&self, positions: usize) {
        panic!("the check's source closes on its own, but {positions} positions were left");
    }
}

/// A drain observes the domain at any point while the host consumes the refused payload of a
/// batch, the graph resolves the root of every other payload in it, and the ingestor's transport
/// takes a round trip to confirm the batch. `queue` is the batch's positions, and the payload at
/// position zero is the refused one.
fn drain_observing_a_batch_with_a_refused_payload(queue: &'static [u64]) {
    shuttle::future::block_on(async move {
        let trackers = IngestorAckRootTrackers::detached();
        let progress = StdArc::new(ConfirmationProgress::default());
        let unresolved = StdArc::new(AtomicUsize::new(0));
        let (roots, mut batch_roots) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let retry = ParsedRetryPolicy {
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        let policy = BrokerIngestorPolicy {
            acknowledgement: IngestorAckPolicy::Parallel {
                max_in_flight: NonZeroUsize::new(2).assured("two is a nonzero batch bound"),
                batch_timeout: Duration::from_millis(1),
                timeout: Duration::from_secs(1),
                retry,
            },
            retry,
            position_class: PositionClass::Replayable,
        };
        let loop_task = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
            ConfirmingChannelIngestor {
                queue: queue.to_vec(),
                progress: progress.clone(),
            },
            IngestorHost::new(RefusingGraphHost {
                trackers: trackers.clone(),
                roots,
                unresolved: unresolved.clone(),
                progress: progress.clone(),
                shutdown: shutdown_rx.clone(),
            }),
            policy,
            shutdown_rx,
        ));
        let graph = nervix_primitives::task::spawn({
            let progress = progress.clone();
            async move {
                // A batch whose every payload was refused hands the graph nothing, and the host's
                // sender goes once its ingestor closes.
                let Some(roots) = batch_roots.recv().await else {
                    return;
                };
                let mut resolutions = Vec::with_capacity(roots.len());
                for root in roots {
                    let unresolved = unresolved.clone();
                    let progress = progress.clone();
                    resolutions.push(nervix_primitives::task::spawn(async move {
                        nervix_primitives::task::yield_now().await;
                        if unresolved.fetch_sub(1, Ordering::SeqCst) == 1 {
                            progress.resolved_batches.fetch_add(1, Ordering::SeqCst);
                        }
                        root.ack_success();
                    }));
                }
                for resolution in resolutions {
                    resolution
                        .await
                        .assured("a root's resolution task does not panic");
                }
            }
        });
        let observer = nervix_primitives::task::spawn({
            let trackers = trackers.clone();
            let progress = progress.clone();
            async move {
                for _ in 0..DRAIN_OBSERVATIONS {
                    nervix_primitives::task::yield_now().await;
                    let resolved = progress.resolved_batches.load(Ordering::SeqCst);
                    let outstanding = trackers.domain_outstanding_for_ownership_handoff();
                    let confirmed = progress.confirmed_batches.load(Ordering::SeqCst);
                    if outstanding == 0 {
                        assert!(
                            confirmed >= resolved,
                            "a drain saw no outstanding work while a batch whose refused payload \
                             was consumed waited for its transport to confirm it"
                        );
                    }
                }
            }
        });

        // The ingestor closes once its one batch is settled, which ends the loop without a stop.
        loop_task
            .await
            .assured("the host loop ends when its source closes");
        graph
            .await
            .assured("the graph ends once it resolved the batch");
        observer
            .await
            .assured("the drain observer ends after its reads");
        drop(shutdown);

        assert_eq!(
            progress.confirmed_batches.load(Ordering::SeqCst),
            1,
            "the batch is confirmed, never rejected, with its refused payload"
        );
        assert_eq!(
            trackers.domain_outstanding_for_ownership_handoff(),
            0,
            "the batch's work is released once its source confirmed it"
        );
        assert_eq!(trackers.ingestor_outstanding_for_ownership_handoff(), 0);
    });
}

#[test]
fn shuttle_a_drain_waits_for_the_transport_to_confirm_a_batch_with_a_refused_payload() {
    for queue in [[0].as_slice(), [0, 1].as_slice()] {
        check_random(
            move || drain_observing_a_batch_with_a_refused_payload(queue),
            1_000,
        );
        check_pct(
            move || drain_observing_a_batch_with_a_refused_payload(queue),
            1_000,
            3,
        );
    }
}
