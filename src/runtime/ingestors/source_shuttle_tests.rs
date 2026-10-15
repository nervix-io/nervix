//! Source host loop engagement checks under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** Controlled source dispatch and quiesce interleavings for the host loops, the
//!   interleavings of a lost broker connection with the broker loop's acknowledgements and
//!   rejections, and the interleavings of a drain's observation with a batch's confirmation.
//! - **Depends on.** The production source loops, source contract, quiesce control, the runtime's
//!   batch acknowledgement and confirmed ACK roots, the connection-bound doubles of the reconnect
//!   tests and Shuttle.
//! - **Must not know.** Broker drivers, model planning or external payload transports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_connector::{IngestMessageHeaders, IngestMetadataRow};
use nervix_model_harness::shuttle::{check_pct, check_random};
use nervix_primitives::sync::{StdArc, mpsc, oneshot, watch};
// The dispatch count is a record: an unmodeled atomic is not a Shuttle scheduling point, so it
// changes in the same scheduling step as the dispatch it counts.
use nervix_primitives::unmodeled::sync::atomic::{AtomicUsize, Ordering};

use super::{
    reconnect_tests::{RedeliveringBroker, RedeliveringSource, RedeliveryHost, sequential_policy},
    *,
};

const JOIN: &str = "the host loop runs until the check sends shutdown";

struct ChannelMessage {
    position: u64,
    payload: Vec<u8>,
}

impl IngestMessageHeaders for ChannelMessage {
    fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
}

impl SourceMessage for ChannelMessage {
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

struct ChannelSource {
    messages: mpsc::Receiver<ChannelMessage>,
}

#[async_trait]
impl SourceConnector for ChannelSource {
    type Plan = ();

    async fn open(
        _plan: &Self::Plan,
        _instance_index: u64,
    ) -> nervix_connector::SourceResult<Self> {
        Err(Report::new(SourceError::Open {
            connector: "channel",
        }))
    }

    async fn resume(&mut self) -> nervix_connector::SourceResult<SourceResume> {
        Ok(SourceResume::Ready)
    }
}

#[async_trait]
impl BrokerSourceConnector for ChannelSource {
    type Message = ChannelMessage;
    type Position = u64;

    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> nervix_connector::SourceResult<SourceBatch<Self::Message>> {
        match self.messages.recv().await {
            Some(message) => Ok(SourceBatch::Messages(vec![message])),
            None => Ok(SourceBatch::Closed),
        }
    }

    async fn acknowledge(
        &mut self,
        _positions: &[Self::Position],
    ) -> nervix_connector::SourceResult<()> {
        Ok(())
    }

    async fn reject(
        &mut self,
        _positions: &[Self::Position],
    ) -> nervix_connector::SourceResult<()> {
        Ok(())
    }
}

#[async_trait]
impl PacedSourceConnector for ChannelSource {
    async fn poll(
        &mut self,
        scheduled_at: Timestamp,
    ) -> nervix_connector::SourceResult<SourcePoll> {
        let message = self
            .messages
            .recv()
            .await
            .assured("the check holds the payload sender until it shuts the source down");
        Ok(SourcePoll {
            messages: vec![nervix_connector::SourcePollMessage {
                payload: message.payload,
                headers: RetainedIngestHeaders::none(),
            }],
            failures: Vec::new(),
            observed_at: scheduled_at,
        })
    }
}

struct ChannelCadence {
    due: mpsc::Receiver<Timestamp>,
}

#[async_trait]
impl PacedSourceCadence for ChannelCadence {
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
impl SourceHostServices for ChannelHost {
    async fn intake_unacknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<()> {
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
        _messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<SourceBatchAcknowledgement> {
        panic!("a channel source reads without acknowledgements");
    }

    async fn flush(&mut self) -> SourceIntakeResult<()> {
        Ok(())
    }

    async fn replay_buffered(&mut self) -> SourceIntakeResult<bool> {
        if self.quiesce.is_quiesced() {
            return Ok(false);
        }
        let Some(messages) = self.replay_messages.as_mut() else {
            return Ok(false);
        };
        let Ok(message) = messages.try_recv() else {
            return Ok(false);
        };
        self.intake_unacknowledged(vec![SourceIntakeMessage {
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
    fn record_unconfirmed_stop(&self, positions: usize) {
        panic!("a channel source without positions left {positions} unconfirmed");
    }
}

#[async_trait]
impl PacedSourceHostServices for ChannelHost {
    async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
        let mut messages = Vec::with_capacity(poll.messages.len());
        for message in &poll.messages {
            nervix_primitives::task::consume_budget().await;
            messages.push(SourceIntakeMessage {
                payload: &message.payload,
                metadata: IngestMetadataRow::Headers {
                    headers: &message.headers,
                },
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
enum SourceFamily {
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
        .send(ChannelMessage {
            position,
            payload: vec![u8::try_from(position).assured("the check uses positions one and two")],
        })
        .await
        .assured("the source loop keeps its payload channel open until shutdown");
}

fn engagement_during_dispatch(family: SourceFamily, cause: IngestorQuiesceCause) {
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
                SourceFamily::Request => Some(replay_receiver),
                SourceFamily::Broker | SourceFamily::Paced => None,
            },
            dispatch_started: Some(dispatch_started),
            finish_dispatch: Some(dispatch_release),
            engagement_observed: Some(engagement_observed),
            dispatched: dispatched.clone(),
        };
        let (shutdown, shutdown_rx) = watch::channel(false);
        let source = ChannelSource { messages: receiver };
        if matches!(family, SourceFamily::Request) {
            // Request sources only replay work already buffered when their loop starts.
            send_message(&replays, 1).await;
        }
        let loop_task = match family {
            SourceFamily::Broker => nervix_primitives::task::spawn(run_source_instance_with_retry(
                source,
                SourceHost::new(host),
                BrokerSourcePolicy {
                    acknowledgement: SourceAckPolicy::None,
                    retry: SOURCE_RECONNECT_POLICY,
                    position_class: PositionClass::None,
                },
                shutdown_rx,
            )),
            SourceFamily::Paced => nervix_primitives::task::spawn(run_paced_source(
                source,
                host,
                ChannelCadence {
                    due: cadence_receiver,
                },
                shutdown_rx,
            )),
            SourceFamily::Request => nervix_primitives::task::spawn(run_request_source(
                source,
                SourceHost::new(host),
                shutdown_rx,
            )),
        };

        match family {
            SourceFamily::Broker => send_message(&messages, 1).await,
            SourceFamily::Paced => {
                send_message(&messages, 1).await;
                cadence
                    .send(Timestamp::from_unix_nanos(1))
                    .await
                    .assured("the paced host awaits its first occurrence");
            }
            SourceFamily::Request => {}
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
            SourceFamily::Broker => send_message(&messages, 2).await,
            SourceFamily::Paced => {
                send_message(&messages, 2).await;
                cadence
                    .send(Timestamp::from_unix_nanos(2))
                    .await
                    .assured("the paced host keeps its cadence channel open");
            }
            SourceFamily::Request => send_message(&replays, 2).await,
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

fn explore_family(family: SourceFamily) {
    for cause in CAUSES {
        check_random(move || engagement_during_dispatch(family, cause), 250);
        check_pct(move || engagement_during_dispatch(family, cause), 250, 3);
    }
}

#[test]
fn shuttle_broker_source_observes_engagement_during_dispatch() {
    explore_family(SourceFamily::Broker);
}

#[test]
fn shuttle_paced_source_observes_engagement_during_dispatch() {
    explore_family(SourceFamily::Paced);
}

#[test]
fn shuttle_request_source_observes_engagement_during_dispatch() {
    explore_family(SourceFamily::Request);
}

/// A broker connection lost at any point of the loop, possibly twice, while the host acknowledges
/// two messages one at a time.
fn lost_connection_during_settlement(losses: usize) {
    shuttle::future::block_on(async move {
        let broker = RedeliveringBroker::shared([1, 2]);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let loop_task = nervix_primitives::task::spawn(run_source_instance_with_retry(
            RedeliveringSource::new(broker.clone()),
            SourceHost::new(RedeliveryHost::new(broker.clone(), false)),
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

        // The source closes once both messages are acknowledged and nothing is left to redeliver,
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
    /// Batches whose positions the source's transport confirmed.
    confirmed_batches: AtomicUsize,
}

/// A source that hands the loop its two messages as one batch, and whose transport takes a round
/// trip to confirm them.
struct ConfirmingChannelSource {
    queue: Vec<u64>,
    progress: StdArc<ConfirmationProgress>,
}

#[async_trait]
impl SourceConnector for ConfirmingChannelSource {
    type Plan = ();

    async fn open(
        _plan: &Self::Plan,
        _instance_index: u64,
    ) -> nervix_connector::SourceResult<Self> {
        Err(Report::new(SourceError::Open {
            connector: "confirming",
        }))
    }
}

#[async_trait]
impl BrokerSourceConnector for ConfirmingChannelSource {
    type Message = ChannelMessage;
    type Position = u64;

    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> nervix_connector::SourceResult<SourceBatch<Self::Message>> {
        if self.queue.is_empty() {
            return Ok(SourceBatch::Closed);
        }
        let mut messages = Vec::with_capacity(self.queue.len());
        for position in self.queue.drain(..) {
            messages.push(ChannelMessage {
                position,
                payload: vec![
                    u8::try_from(position).assured("the check uses positions one and two"),
                ],
            });
        }
        Ok(SourceBatch::Messages(messages))
    }

    async fn acknowledge(
        &mut self,
        _positions: &[Self::Position],
    ) -> nervix_connector::SourceResult<()> {
        // The request to the broker and its answer: a production source awaits both before it
        // returns, while drains observe the node.
        nervix_primitives::task::yield_now().await;
        nervix_primitives::task::yield_now().await;
        self.progress
            .confirmed_batches
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn reject(&mut self, positions: &[Self::Position]) -> nervix_connector::SourceResult<()> {
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
impl SourceHostServices for GraphHost {
    async fn intake_unacknowledged(
        &mut self,
        _messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<()> {
        panic!("the check's source reads in an acknowledged mode");
    }

    async fn intake_acknowledged(
        &mut self,
        messages: Vec<SourceIntakeMessage<'_>>,
    ) -> SourceIntakeResult<SourceBatchAcknowledgement> {
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
        Ok(SourceBatchAcknowledgement::new(
            RuntimeBatchAcknowledgement::new(completions, self.shutdown.clone(), None),
        ))
    }

    async fn flush(&mut self) -> SourceIntakeResult<()> {
        Ok(())
    }

    async fn replay_buffered(&mut self) -> SourceIntakeResult<bool> {
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
    fn record_unconfirmed_stop(&self, positions: usize) {
        panic!("the check's source closes on its own, but {positions} positions were left");
    }
}

/// The ownership-handoff drain reads of one check, each a read-modify-write as a drain makes it.
const DRAIN_OBSERVATIONS: usize = 12;

/// A drain observes the domain at any point while the graph resolves a two-message batch in either
/// order and the source's transport takes a round trip to confirm it.
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
        let policy = BrokerSourcePolicy {
            acknowledgement: SourceAckPolicy::Parallel {
                max_in_flight: NonZeroUsize::new(2).assured("two is a nonzero batch bound"),
                batch_timeout: Duration::from_millis(1),
                timeout: Duration::from_secs(1),
                retry,
            },
            retry,
            position_class: PositionClass::Replayable,
        };
        let loop_task = nervix_primitives::task::spawn(run_source_instance_with_retry(
            ConfirmingChannelSource {
                queue: vec![1, 2],
                progress: progress.clone(),
            },
            SourceHost::new(GraphHost {
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

        // The source closes once its one batch is settled, which ends the loop without a stop.
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
