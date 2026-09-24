//! Source host loop engagement checks under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** Controlled source dispatch and quiesce interleavings for the host loops.
//! - **Depends on.** The production source loops, source contract, quiesce control and Shuttle.
//! - **Must not know.** Broker drivers, model planning or external payload transports.

use std::sync::{
    Arc as StdArc,
    atomic::{AtomicUsize, Ordering},
};

use nervix_connector::{IngestMessageHeaders, IngestMetadataRow};
use tokio::sync::{mpsc, oneshot, watch};

use super::*;
use crate::shuttle_test::{check_pct, check_random};

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
    async fn intake(
        &mut self,
        batch: SourceIntakeBatch<'_>,
    ) -> SourceIntakeResult<SourceIntakeOutcome> {
        for message in batch.messages {
            tokio::task::consume_budget().await;
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
        Ok(SourceIntakeOutcome {
            acknowledgements: Vec::new(),
        })
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
        self.intake(SourceIntakeBatch {
            messages: vec![SourceIntakeMessage {
                payload: &message.payload,
                metadata: message.metadata(),
            }],
            mode: SourceIntakeMode::Unacknowledged,
        })
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
}

#[async_trait]
impl PacedSourceHostServices for ChannelHost {
    async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
        let mut messages = Vec::with_capacity(poll.messages.len());
        for message in &poll.messages {
            tokio::task::consume_budget().await;
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
        self.intake(SourceIntakeBatch {
            messages,
            mode: SourceIntakeMode::Unacknowledged,
        })
        .await?;
        Ok(true)
    }

    async fn replay_buffered_poll(&mut self) -> SourceIntakeResult<bool> {
        self.replay_buffered().await
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
        tokio::task::consume_budget().await;
        tokio::select! {
            biased;
            result = &mut observed => {
                result.assured("the host reports observation while its check is alive");
                return;
            }
            _ = tokio::task::yield_now() => {}
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
            SourceFamily::Broker => tokio::spawn(run_source_instance_with_retry(
                source,
                SourceHost::new(host),
                SourceAckPolicy::None,
                SOURCE_RECONNECT_POLICY,
                shutdown_rx,
            )),
            SourceFamily::Paced => tokio::spawn(run_paced_source(
                source,
                host,
                ChannelCadence {
                    due: cadence_receiver,
                },
                shutdown_rx,
            )),
            SourceFamily::Request => tokio::spawn(run_request_source(
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
        let engager = tokio::spawn({
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
        let releaser = tokio::spawn(async move {
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
