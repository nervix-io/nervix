//! The broker and paced ingestor loops exercised through their host contract.
//!
//! Layer: test harness.
//! - **Owns.** Ingestor and host doubles and the ingestor-loop policy, acknowledgement, replay,
//!   quiesce, flush and configuration-refresh regressions they drive.
//! - **Depends on.** The production ingestor loops and the connector ingestor contract.
//! - **Must not know.** Broker drivers, registry validation or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use std::{collections::VecDeque, future};

use nervix_connector::{
    IngestMessageHeaders, IngestMetadataRow, IngestorBatchAcknowledgementServices,
    IngestorConnector, IngestorError, IngestorReadCancellation, IngestorResult,
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
struct IngestorLoopObservations {
    /// Every resume, replay and poll in the order the loop performed them.
    sequence: Vec<&'static str>,
    /// Replays the host still has to deliver, as a quiesce buffer would.
    pending_replays: usize,
    requests: Vec<IngestorBatchRequest>,
    intake: Vec<(RecordedIntake, usize)>,
    acknowledged: Vec<Vec<u64>>,
    rejected: Vec<Vec<u64>>,
    poll_errors: Vec<String>,
    reported_errors: Vec<String>,
    ack_waits: usize,
    ack_outcomes: VecDeque<IngestorAcknowledgementOutcome>,
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

impl IngestorMessage for FakeMessage {
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

struct FakeIngestor {
    messages: VecDeque<FakeMessage>,
    resume_required: bool,
    resume_results: VecDeque<IngestorResult<IngestorResume>>,
    reject_failures_left: usize,
    replay_rejected: bool,
    /// Replays each resume leaves pending, as a quiesce released during it would.
    replays_pending_after_resume: usize,
    resume_started: Option<Arc<Notify>>,
    block_resume: bool,
    observations: Arc<Mutex<IngestorLoopObservations>>,
}

#[async_trait]
impl IngestorConnector for FakeIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = ();

    async fn open(_plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        Err(Report::new(IngestorError::Open { connector: "fake" }))
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
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
            None => Ok(IngestorResume::Ready),
        }
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        self.observations.lock().suspends += 1;
        Ok(())
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.observations.lock().closes += 1;
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for FakeIngestor {
    type Message = FakeMessage;
    type Position = u64;

    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        _cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let mut observations = self.observations.lock();
        observations.requests.push(request);
        observations.sequence.push("next_batch");
        drop(observations);
        if self.resume_required {
            self.resume_required = false;
            return Ok(IngestorBatch::ResumeRequired);
        }
        if self.messages.is_empty() {
            return Ok(IngestorBatch::Closed);
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
        Ok(IngestorBatch::Messages(messages))
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        self.observations
            .lock()
            .acknowledged
            .push(positions.to_vec());
        Ok(())
    }

    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        self.observations.lock().rejected.push(positions.to_vec());
        if self.reject_failures_left > 0 {
            self.reject_failures_left -= 1;
            return Err(Report::new(IngestorError::Reject { connector: "fake" }));
        }
        if self.replay_rejected {
            for position in positions.iter().rev() {
                self.messages.push_front(FakeMessage {
                    position: *position,
                    payload: vec![
                        u8::try_from(*position).verified("the test positions are all below 256"),
                    ],
                });
            }
        }
        Ok(())
    }
}

struct ImmediateAcknowledgement {
    observations: Arc<Mutex<IngestorLoopObservations>>,
}

#[async_trait]
impl IngestorBatchAcknowledgementServices for ImmediateAcknowledgement {
    async fn wait(&mut self, _timeout: Duration) -> IngestorAcknowledgementOutcome {
        let mut observations = self.observations.lock();
        observations.ack_waits += 1;
        observations
            .ack_outcomes
            .pop_front()
            .unwrap_or(IngestorAcknowledgementOutcome::Ack)
    }
}

struct FakeHost {
    observations: Arc<Mutex<IngestorLoopObservations>>,
    suspend_intake: bool,
    wake_quiesce: bool,
    quiesce_change: Option<Arc<Notify>>,
    wake_suspension: bool,
    active: bool,
}

impl FakeHost {
    fn running(observations: Arc<Mutex<IngestorLoopObservations>>) -> Self {
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
impl IngestorHostServices for FakeHost {
    async fn intake_unacknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
        self.observations
            .lock()
            .intake
            .push((RecordedIntake::Unacknowledged, messages.len()));
        Ok(())
    }

    async fn intake_acknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
        self.observations
            .lock()
            .intake
            .push((RecordedIntake::Acknowledged, messages.len()));
        Ok(IngestorBatchAcknowledgement::new(
            ImmediateAcknowledgement {
                observations: self.observations.clone(),
            },
        ))
    }

    async fn flush(&mut self) -> IngestorIntakeResult<()> {
        self.observations.lock().flushes += 1;
        Ok(())
    }

    async fn replay_buffered(&mut self) -> IngestorIntakeResult<bool> {
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
impl PacedIngestorHostServices for FakeHost {
    async fn intake_poll(
        &mut self,
        poll: IngestorPoll,
        _intake_batch: NonZeroUsize,
    ) -> IngestorIntakeResult<bool> {
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

fn replayable(acknowledgement: IngestorAckPolicy) -> BrokerIngestorPolicy {
    BrokerIngestorPolicy::new(acknowledgement, PositionClass::Replayable)
}

fn three_messages() -> VecDeque<FakeMessage> {
    (0..3)
        .map(|position| FakeMessage {
            position,
            payload: vec![u8::try_from(position).verified("the test positions are all below 256")],
        })
        .collect()
}

async fn run_policy_with_refresh(
    policy: IngestorAckPolicy,
    resume_required: bool,
) -> Arc<Mutex<IngestorLoopObservations>> {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let source = FakeIngestor {
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
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    run_ingestor_instance_with_retry(source, host, replayable(policy), shutdown_rx).await;
    drop(shutdown_tx);
    observations
}

async fn run_policy(policy: IngestorAckPolicy) -> Arc<Mutex<IngestorLoopObservations>> {
    run_policy_with_refresh(policy, false).await
}

fn retry_policy() -> nervix_connector::ParsedRetryPolicy {
    nervix_connector::ParsedRetryPolicy {
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
    }
}

fn empty_source(observations: Arc<Mutex<IngestorLoopObservations>>) -> FakeIngestor {
    FakeIngestor {
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
impl PacedIngestorConnector for FakeIngestor {
    async fn poll(&mut self, _scheduled_at: Timestamp) -> IngestorResult<IngestorPoll> {
        panic!("the failed cadence ends the loop before its source is polled")
    }
}

struct FailedCadence;

struct FailedPacedIngestor {
    shutdown: watch::Sender<bool>,
}

#[async_trait]
impl IngestorConnector for FailedPacedIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = ();

    async fn open(_plan: &(), _instance_index: u64) -> IngestorResult<Self> {
        panic!("the test installs its source directly")
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        Ok(IngestorResume::Ready)
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        Err(
            Report::new(std::io::Error::other("could not pause the connection"))
                .change_context(IngestorError::Suspend { connector: "fake" }),
        )
    }

    async fn close(&mut self) -> IngestorResult<()> {
        Err(
            Report::new(std::io::Error::other("could not finish the connection"))
                .change_context(IngestorError::Close { connector: "fake" }),
        )
    }
}

#[async_trait]
impl PacedIngestorConnector for FailedPacedIngestor {
    async fn poll(&mut self, _scheduled_at: Timestamp) -> IngestorResult<IngestorPoll> {
        self.shutdown.send_replace(true);
        Err(
            Report::new(std::io::Error::other("the connection refused the poll"))
                .change_context(IngestorError::Read { connector: "fake" }),
        )
    }
}

struct SingleOccurrenceCadence {
    occurrence: Option<Timestamp>,
}

#[async_trait]
impl PacedIngestorCadence for SingleOccurrenceCadence {
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
async fn runtime_report_chain_ingestor_lifecycle() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    let mut source = FailedPacedIngestor {
        shutdown: shutdown_tx,
    };
    let mut host = FakeHost::running(observations.clone());
    host.suspend_intake = true;
    host.wake_suspension = true;
    let mut ready = true;
    assert_eq!(
        prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
        PacedIngestorAction::Restart
    );
    host.suspend_intake = false;
    run_paced_ingestor(
        source,
        host,
        SingleOccurrenceCadence {
            occurrence: Some(Timestamp::from_unix_nanos(1)),
        },
        NonZeroUsize::MIN,
        shutdown,
    )
    .await;

    let observations = observations.lock();
    assert_eq!(
        observations.poll_errors,
        ["failed to read from fake ingestor: the connection refused the poll"]
    );
    assert_eq!(
        observations.reported_errors,
        [
            "failed to suspend fake ingestor: could not pause the connection",
            "failed to read from fake ingestor: the connection refused the poll",
            "failed to close fake ingestor: could not finish the connection",
        ]
    );
}

#[async_trait]
impl PacedIngestorCadence for FailedCadence {
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
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let source = empty_source(observations.clone());
    let host = FakeHost::running(observations.clone());
    let (_shutdown_tx, shutdown) = watch::channel(false);

    run_paced_ingestor(source, host, FailedCadence, NonZeroUsize::MIN, shutdown).await;

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
async fn runtime_report_chain_ingestor_resume() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut source = empty_source(observations.clone());
    source
        .resume_results
        .push_back(Err(Report::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "broker refused the connection",
        ))
        .change_context(IngestorError::Resume { connector: "fake" })));
    let mut host = FakeHost::running(observations.clone());
    host.wake_quiesce = true;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let mut ready = false;

    assert_eq!(
        prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
        PacedIngestorAction::Restart
    );
    let observations = observations.lock();
    assert_eq!(
        observations.poll_errors,
        ["failed to resume fake ingestor: broker refused the connection"]
    );
    assert_eq!(observations.reported_errors, observations.poll_errors);
}

#[nervix_primitives::test]
async fn source_loop_replays_a_buffer_released_during_resume_before_polling() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let source = FakeIngestor {
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
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    run_ingestor_instance_with_retry(
        source,
        host,
        replayable(IngestorAckPolicy::None),
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
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let started = Arc::new(Notify::new());
    let mut source = empty_source(observations.clone());
    source.resume_started = Some(started.clone());
    source.block_resume = true;
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let running = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
        source,
        host,
        replayable(IngestorAckPolicy::None),
        shutdown_rx,
    ));
    started.notified().await;
    shutdown_tx.send_replace(true);
    nervix_primitives::time::timeout(Duration::from_secs(10), running)
        .await
        .assured("shutdown cancels the pending resume through the source-loop select")
        .assured("the ingestor loop exits without a task panic");
    assert_eq!(observations.lock().closes, 1);
}

#[nervix_primitives::test]
async fn source_quiesce_change_cancels_pending_resume_before_retrying() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let started = Arc::new(Notify::new());
    let changed = Arc::new(Notify::new());
    let mut source = empty_source(observations.clone());
    source.resume_started = Some(started.clone());
    source.block_resume = true;
    let mut fake_host = FakeHost::running(observations.clone());
    fake_host.quiesce_change = Some(changed.clone());
    let host = IngestorHost::new(fake_host);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let running = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
        source,
        host,
        replayable(IngestorAckPolicy::None),
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
        .assured("the ingestor loop exits without a task panic");
    let observations = observations.lock();
    assert_eq!(observations.resumes, 2);
    assert_eq!(observations.quiesce_waits, 1);
    assert_eq!(observations.closes, 1);
}

#[nervix_primitives::test]
async fn request_source_loop_replays_retained_requests_and_closes_the_source_at_shutdown() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    observations.lock().pending_replays = 2;
    let source = empty_source(observations.clone());
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send_replace(true);

    run_request_ingestor(source, host, shutdown_rx).await;

    // Requests dispatch on the request path, so the loop never reads from the ingestor: it only
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
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut source = empty_source(observations.clone());
    let mut host = FakeHost::running(observations.clone());
    host.suspend_intake = true;
    host.wake_suspension = true;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let mut ready = true;

    let action = prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await;

    assert_eq!(action, PacedIngestorAction::Restart);
    assert!(!ready);
    let observations = observations.lock();
    assert_eq!(observations.flushes, 1);
    assert_eq!(observations.suspends, 1);
    assert_eq!(observations.suspension_waits, 1);
    assert_eq!(observations.unready, 1);
}

#[nervix_primitives::test]
async fn paced_source_preparation_retries_resume_until_the_source_is_ready() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut source = empty_source(observations.clone());
    source.resume_results = VecDeque::from([
        Ok(IngestorResume::Waiting {
            retry_after: Duration::from_secs(60),
        }),
        Err(Report::new(IngestorError::Resume { connector: "fake" })),
        Ok(IngestorResume::Ready),
    ]);
    let mut host = FakeHost::running(observations.clone());
    host.wake_quiesce = true;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let mut ready = false;

    assert_eq!(
        prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
        PacedIngestorAction::Restart
    );
    assert_eq!(
        prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
        PacedIngestorAction::Restart
    );
    assert_eq!(
        prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
        PacedIngestorAction::Poll
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
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut source = empty_source(observations.clone());
    let mut host = FakeHost::running(observations.clone());
    host.active = false;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let mut ready = false;

    assert_eq!(
        prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
        PacedIngestorAction::Stop
    );
    assert_eq!(observations.lock().resumes, 0);
}

#[nervix_primitives::test]
async fn paced_source_preparation_stops_suspended_intake_after_shutdown_closes() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut source = empty_source(observations.clone());
    let mut host = FakeHost::running(observations.clone());
    host.suspend_intake = true;
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    drop(shutdown_tx);
    let mut ready = false;

    assert_eq!(
        prepare_paced_source(&mut source, &mut host, &mut ready, &mut shutdown).await,
        PacedIngestorAction::Stop
    );
    assert_eq!(observations.lock().flushes, 1);
}

#[nervix_primitives::test]
async fn source_loop_accepts_no_ack_messages_one_at_a_time() {
    let observations = run_policy(IngestorAckPolicy::None).await;
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
    let observations = run_policy(IngestorAckPolicy::Sequential {
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
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    observations.lock().ack_outcomes = VecDeque::from([
        IngestorAcknowledgementOutcome::NoAck("relay owner crashed".to_string()),
        IngestorAcknowledgementOutcome::Ack,
        IngestorAcknowledgementOutcome::Ack,
    ]);
    let source = FakeIngestor {
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
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let policy = IngestorAckPolicy::Sequential {
        timeout: Duration::from_secs(1),
        retry: retry_policy(),
    };
    run_ingestor_instance_with_retry(source, host, replayable(policy), shutdown_rx).await;
    drop(shutdown_tx);

    let observations = observations.lock();
    assert_eq!(observations.rejected, vec![vec![0], vec![0], vec![0]]);
    assert_eq!(observations.acknowledged, vec![vec![0], vec![1]]);
    assert_eq!(observations.suspends, 2);
    assert_eq!(observations.resumes, 3);
}

#[nervix_primitives::test]
async fn source_loop_commits_one_parallel_ack_batch() {
    let observations = run_policy(IngestorAckPolicy::Parallel {
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
    let observations = run_policy_with_refresh(IngestorAckPolicy::None, true).await;
    let observations = observations.lock();
    assert_eq!(observations.resumes, 2);
    assert_eq!(observations.ready, 2);
    assert_eq!(observations.unready, 2);
    assert_eq!(observations.intake.len(), 3);
    assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
}
