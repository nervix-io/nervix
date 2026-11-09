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

use std::{
    collections::{BTreeMap, VecDeque},
    future,
};

use nervix_connector::{IngestorBatchAcknowledgementServices, IngestorError};
use nervix_connector_fake::{
    FakeIngestor, FakeMessage, IngestorObservations as IngestorLoopObservations, RecordedIntake,
};
use nervix_primitives::sync::{Notify, blocking::Mutex};

use super::*;
use crate::runtime::domain_clock::DomainClockWaitError;

enum TestAcknowledgementPlan {
    Outcome(IngestorAcknowledgementOutcome),
    Completion(nervix_primitives::sync::oneshot::Receiver<IngestorAcknowledgementOutcome>),
}

struct ImmediateAcknowledgement {
    plan: Option<TestAcknowledgementPlan>,
    observations: Arc<Mutex<IngestorLoopObservations>>,
}

#[async_trait]
impl IngestorBatchAcknowledgementServices for ImmediateAcknowledgement {
    async fn wait(&mut self, _timeout: Duration) -> IngestorAcknowledgementOutcome {
        self.observations.lock().ack_waits += 1;
        if let Some(plan) = self.plan.take() {
            return match plan {
                TestAcknowledgementPlan::Outcome(outcome) => outcome,
                TestAcknowledgementPlan::Completion(completion) => completion
                    .await
                    .assured("the check completes every admitted delivery"),
            };
        }
        self.observations
            .lock()
            .ack_outcomes
            .pop_front()
            .unwrap_or(IngestorAcknowledgementOutcome::Acknowledged)
    }
}

/// One transport settlement the loop recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordedSettlement {
    operation: IngestorSettlementOperation,
    outcome: IngestorSettlementOutcome,
}

/// The acknowledgement and connector lifecycle observations the loop recorded, in order.
#[derive(Default)]
struct LifecycleObservations {
    acknowledgements: Vec<IngestorAcknowledgementOutcome>,
    settlements: Vec<RecordedSettlement>,
    resumes: Vec<IngestorResumeOutcome>,
    suspends: Vec<IngestorSuspendCause>,
}

#[derive(Default)]
struct ReadObservations {
    oversized: usize,
    reads: Vec<IngestorReadOutcome>,
    payloads: Vec<(usize, u64)>,
    polls: Vec<(IngestorReadOutcome, Duration)>,
}

struct FakeHost {
    expected_oversized: usize,
    /// The test retains metric events independently of the fake driver's observations.
    read_observations: Arc<Mutex<ReadObservations>>,
    lifecycle: Arc<Mutex<LifecycleObservations>>,
    refused_payload: Option<u8>,
    allow_unconfirmed_stop: bool,
    acknowledgements: BTreeMap<u8, TestAcknowledgementPlan>,
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
            expected_oversized: 0,
            read_observations: Arc::new(Mutex::new(ReadObservations::default())),
            lifecycle: Arc::new(Mutex::new(LifecycleObservations::default())),
            refused_payload: None,
            allow_unconfirmed_stop: false,
            acknowledgements: BTreeMap::new(),
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
    fn record_read(&self, outcome: nervix_connector::IngestorReadOutcome) {
        self.read_observations.lock().reads.push(outcome);
    }
    fn record_read_payloads(&self, payloads: usize, bytes: u64) {
        self.read_observations
            .lock()
            .payloads
            .push((payloads, bytes));
    }

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
        let plan = if self.acknowledgements.is_empty() {
            None
        } else {
            assert_eq!(messages.len(), 1, "each delivery has its own graph outcome");
            self.acknowledgements.remove(&messages[0].payload[0])
        };
        if self.refused_payload.is_some_and(|refused| {
            messages
                .first()
                .is_some_and(|message| message.payload == [refused])
        }) {
            return Err(Report::new(IngestorIntakeError::Dispatch));
        }
        Ok(IngestorBatchAcknowledgement::new(
            ImmediateAcknowledgement {
                plan,
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
    fn record_acknowledgement(&self, outcome: &IngestorAcknowledgementOutcome) {
        self.lifecycle.lock().acknowledgements.push(outcome.clone());
        if let IngestorAcknowledgementOutcome::Failed { reason } = outcome {
            self.observations
                .lock()
                .reported_errors
                .push(reason.clone());
        }
    }
    fn record_transport_settlement(
        &self,
        operation: IngestorSettlementOperation,
        outcome: IngestorSettlementOutcome,
        _elapsed: Duration,
    ) {
        self.lifecycle
            .lock()
            .settlements
            .push(RecordedSettlement { operation, outcome });
    }
    fn record_resume(&self, outcome: IngestorResumeOutcome) {
        self.lifecycle.lock().resumes.push(outcome);
    }
    fn record_suspend(&self, cause: IngestorSuspendCause) {
        self.lifecycle.lock().suspends.push(cause);
    }
    fn record_transport_redeliveries(&self, positions: usize) {
        panic!("a replayable test ingestor discarded {positions} positions after a reconnect");
    }
    fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
        assert_eq!(
            payloads.get(),
            self.expected_oversized,
            "the driver returns only its scripted oversized answer"
        );
        self.read_observations.lock().oversized += payloads.get();
    }
    fn record_unconfirmed_stop(&self, positions: usize) {
        assert!(
            self.allow_unconfirmed_stop,
            "a test ingestor stopped with {positions} positions waiting on their trees"
        );
        self.observations.lock().unconfirmed_stops.push(positions);
    }
}

#[async_trait]
impl PacedIngestorHostServices for FakeHost {
    fn record_poll(&self, outcome: nervix_connector::IngestorReadOutcome, elapsed: Duration) {
        self.read_observations.lock().polls.push((outcome, elapsed));
    }

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
        .map(|position| {
            FakeMessage::new(
                position,
                vec![u8::try_from(position).verified("the test positions are all below 256")],
            )
        })
        .collect()
}

async fn run_policy_with_refresh(
    policy: IngestorAckPolicy,
    resume_required: bool,
) -> Arc<Mutex<IngestorLoopObservations>> {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector.queued().extend(three_messages());
    connector.resume_required = resume_required;
    connector.replays_pending_after_resume = 0;
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    run_ingestor_instance_with_retry(connector, host, replayable(policy), shutdown_rx).await;
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

fn empty_ingestor(observations: Arc<Mutex<IngestorLoopObservations>>) -> FakeIngestor {
    FakeIngestor::empty(observations)
}

struct FailedCadence;

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
    let mut connector = empty_ingestor(observations.clone());
    connector.stop_after_poll = Some(shutdown_tx);
    connector
        .suspend_results
        .push_back(Err(Report::new(std::io::Error::other(
            "could not pause the connection",
        ))
        .change_context(IngestorError::Suspend { connector: "fake" })));
    connector
        .close_results
        .push_back(Err(Report::new(std::io::Error::other(
            "could not finish the connection",
        ))
        .change_context(IngestorError::Close { connector: "fake" })));
    connector
        .polls
        .push_back(Err(Report::new(std::io::Error::other(
            "the connection refused the poll",
        ))
        .change_context(IngestorError::Read { connector: "fake" })));
    let mut host = FakeHost::running(observations.clone());
    host.suspend_intake = true;
    host.wake_suspension = true;
    let mut ready = true;
    assert_eq!(
        prepare_paced_ingestor(
            &mut connector,
            &mut host,
            &mut ready,
            &mut false,
            &mut shutdown
        )
        .await,
        PacedIngestorAction::Restart
    );
    host.suspend_intake = false;
    run_paced_ingestor(
        connector,
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
async fn runtime_report_chain_ingestor_cadence() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let connector = empty_ingestor(observations.clone());
    let host = FakeHost::running(observations.clone());
    let (_shutdown_tx, shutdown) = watch::channel(false);

    run_paced_ingestor(connector, host, FailedCadence, NonZeroUsize::MIN, shutdown).await;

    let observations = observations.lock();
    assert_eq!(
        observations.reported_errors,
        [
            "could not advance ingestor cadence: domain 'orders' clock became unavailable while \
             waiting for a logical deadline: domain 'orders' clock generation 3 is stopped"
        ]
    );
    assert_eq!(observations.closes, 1);
    assert_eq!(observations.unready, 1);
}

#[nervix_primitives::test]
async fn runtime_report_chain_ingestor_resume() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector
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
        prepare_paced_ingestor(
            &mut connector,
            &mut host,
            &mut ready,
            &mut false,
            &mut shutdown
        )
        .await,
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
async fn ingestor_loop_replays_a_buffer_released_during_resume_before_polling() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector.queued().extend(three_messages());
    connector.resume_required = false;
    connector.replays_pending_after_resume = 2;
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    run_ingestor_instance_with_retry(
        connector,
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
async fn ingestor_shutdown_cancels_pending_resume_and_closes_the_ingestor() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let started = Arc::new(Notify::new());
    let mut connector = empty_ingestor(observations.clone());
    connector.resume_gate = Some(started.clone());
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let running = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
        connector,
        host,
        replayable(IngestorAckPolicy::None),
        shutdown_rx,
    ));
    started.notified().await;
    shutdown_tx.send_replace(true);
    nervix_primitives::time::timeout(Duration::from_secs(10), running)
        .await
        .assured("shutdown cancels the pending resume through the ingestor-loop select")
        .assured("the ingestor loop exits without a task panic");
    assert_eq!(observations.lock().closes, 1);
}

#[nervix_primitives::test]
async fn ingestor_quiesce_change_cancels_pending_resume_before_retrying() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let started = Arc::new(Notify::new());
    let changed = Arc::new(Notify::new());
    let mut connector = empty_ingestor(observations.clone());
    connector.resume_gate = Some(started.clone());
    let mut fake_host = FakeHost::running(observations.clone());
    fake_host.quiesce_change = Some(changed.clone());
    let host = IngestorHost::new(fake_host);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let running = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
        connector,
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
        .assured("shutdown cancels the retried resume through the ingestor-loop select")
        .assured("the ingestor loop exits without a task panic");
    let observations = observations.lock();
    assert_eq!(observations.resumes, 2);
    assert_eq!(observations.quiesce_waits, 1);
    assert_eq!(observations.closes, 1);
}

#[nervix_primitives::test]
async fn request_ingestor_loop_replays_retained_requests_and_closes_the_ingestor_at_shutdown() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    observations.lock().pending_replays = 2;
    let connector = empty_ingestor(observations.clone());
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send_replace(true);

    run_request_ingestor(connector, host, shutdown_rx).await;

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
async fn paced_ingestor_preparation_suspends_a_ready_ingestor_until_quiesce_releases() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    let mut host = FakeHost::running(observations.clone());
    host.suspend_intake = true;
    host.wake_suspension = true;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let mut ready = true;

    let action = prepare_paced_ingestor(
        &mut connector,
        &mut host,
        &mut ready,
        &mut false,
        &mut shutdown,
    )
    .await;

    assert_eq!(action, PacedIngestorAction::Restart);
    assert!(!ready);
    let observations = observations.lock();
    assert_eq!(observations.flushes, 1);
    assert_eq!(observations.suspends, 1);
    assert_eq!(observations.suspension_waits, 1);
    assert_eq!(observations.unready, 1);
}

#[nervix_primitives::test]
async fn paced_ingestor_preparation_retries_resume_until_the_ingestor_is_ready() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector.resume_results = VecDeque::from([
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
        prepare_paced_ingestor(
            &mut connector,
            &mut host,
            &mut ready,
            &mut false,
            &mut shutdown
        )
        .await,
        PacedIngestorAction::Restart
    );
    assert_eq!(
        prepare_paced_ingestor(
            &mut connector,
            &mut host,
            &mut ready,
            &mut false,
            &mut shutdown
        )
        .await,
        PacedIngestorAction::Restart
    );
    assert_eq!(
        prepare_paced_ingestor(
            &mut connector,
            &mut host,
            &mut ready,
            &mut false,
            &mut shutdown
        )
        .await,
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
async fn paced_ingestor_preparation_stops_when_the_host_is_inactive() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    let mut host = FakeHost::running(observations.clone());
    host.active = false;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let mut ready = false;

    assert_eq!(
        prepare_paced_ingestor(
            &mut connector,
            &mut host,
            &mut ready,
            &mut false,
            &mut shutdown
        )
        .await,
        PacedIngestorAction::Stop
    );
    assert_eq!(observations.lock().resumes, 0);
}

#[nervix_primitives::test]
async fn paced_ingestor_preparation_stops_suspended_intake_after_shutdown_closes() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    let mut host = FakeHost::running(observations.clone());
    host.suspend_intake = true;
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    drop(shutdown_tx);
    let mut ready = false;

    assert_eq!(
        prepare_paced_ingestor(
            &mut connector,
            &mut host,
            &mut ready,
            &mut false,
            &mut shutdown
        )
        .await,
        PacedIngestorAction::Stop
    );
    assert_eq!(observations.lock().flushes, 1);
}

#[nervix_primitives::test]
async fn ingestor_loop_accepts_no_ack_messages_one_at_a_time() {
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
async fn ingestor_loop_waits_and_commits_each_sequential_ack() {
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
        IngestorAcknowledgementOutcome::Failed {
            reason: "relay owner crashed".to_string(),
        },
        IngestorAcknowledgementOutcome::Acknowledged,
        IngestorAcknowledgementOutcome::Acknowledged,
    ]);
    let mut connector = empty_ingestor(observations.clone());
    connector
        .queued()
        .extend(three_messages().into_iter().take(2));
    connector.reject_failures_left = 2;
    connector.replay_rejected = true;
    let host = IngestorHost::new(FakeHost::running(observations.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let policy = IngestorAckPolicy::Sequential {
        timeout: Duration::from_secs(1),
        retry: retry_policy(),
    };
    run_ingestor_instance_with_retry(connector, host, replayable(policy), shutdown_rx).await;
    drop(shutdown_tx);

    let observations = observations.lock();
    assert_eq!(observations.rejected, vec![vec![0], vec![0], vec![0]]);
    assert_eq!(observations.acknowledged, vec![vec![0], vec![1]]);
    assert_eq!(observations.suspends, 2);
    assert_eq!(observations.resumes, 3);
}

#[nervix_primitives::test]
async fn ingestor_loop_commits_one_parallel_ack_batch() {
    let observations = run_policy(IngestorAckPolicy::Parallel {
        max_in_flight: NonZeroUsize::new(3).verified("three is a nonzero ingestor batch bound"),
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
async fn ingestor_loop_resumes_after_connector_configuration_changes() {
    let observations = run_policy_with_refresh(IngestorAckPolicy::None, true).await;
    let observations = observations.lock();
    assert_eq!(observations.resumes, 2);
    assert_eq!(observations.ready, 2);
    assert_eq!(observations.unready, 2);
    assert_eq!(observations.intake.len(), 3);
    assert_eq!(observations.acknowledged, vec![vec![0], vec![1], vec![2]]);
}

#[nervix_primitives::test]
async fn individual_settlement_confirms_successes_and_requeues_only_the_failed_delivery() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut services = FakeHost::running(observations.clone());
    services.acknowledgements = BTreeMap::from([
        (
            0,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Acknowledged),
        ),
        (
            1,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Failed {
                reason: "refused".into(),
            }),
        ),
        (
            2,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Acknowledged),
        ),
    ]);
    let mut host = IngestorHost::new(services);
    let mut connector = empty_ingestor(observations.clone());
    let batch =
        IndividualBatch::intake(&mut host, three_messages().into(), Duration::from_secs(1)).await;
    let result = batch
        .settle(
            &mut connector,
            &mut host,
            BatchSettlement {
                position_class: PositionClass::BrokerRedelivered,
                retry_after: Duration::from_millis(1),
            },
        )
        .await;
    assert!(matches!(result, BatchDisposition::Retry));
    let record = observations.lock();
    let mut confirmed = record.acknowledged.clone();
    confirmed.sort();
    assert_eq!(confirmed, vec![vec![0], vec![2]]);
    assert_eq!(record.rejected, vec![vec![1]]);
    assert_eq!(record.intake, vec![(RecordedIntake::Acknowledged, 1); 3]);
}

#[nervix_primitives::test]
async fn individual_settlement_retains_each_unsettled_position_once_after_a_transport_failure() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut services = FakeHost::running(observations.clone());
    services.acknowledgements = BTreeMap::from([
        (
            0,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Acknowledged),
        ),
        (
            1,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Failed {
                reason: "refused".into(),
            }),
        ),
        (
            2,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Acknowledged),
        ),
    ]);
    let mut host = IngestorHost::new(services);
    let mut connector = empty_ingestor(observations.clone());
    connector.reject_failures_left = 1;
    let batch =
        IndividualBatch::intake(&mut host, three_messages().into(), Duration::from_secs(1)).await;
    let result = batch
        .settle(
            &mut connector,
            &mut host,
            BatchSettlement {
                position_class: PositionClass::BrokerRedelivered,
                retry_after: Duration::from_millis(1),
            },
        )
        .await;
    let BatchDisposition::RetryRejection(mut retained) = result else {
        panic!("the failed rejection retains the unsettled positions for recovery");
    };
    let record = observations.lock();
    assert_eq!(record.rejected, vec![vec![1]]);
    assert!(retained.contains(&1));
    retained.extend(record.acknowledged.iter().flatten());
    retained.sort();
    assert_eq!(
        retained,
        [0, 1, 2],
        "confirmed and retained positions partition the original batch"
    );
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_individual_settlement_preserves_every_delivery_until_its_own_graph_outcome() {
    nervix_model_harness::shuttle::check_interleavings(|| {
        shuttle::future::block_on(async {
            let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
            let mut services = FakeHost::running(observations.clone());
            let mut completions = Vec::new();
            for id in 0..3u8 {
                let (sender, receiver) = nervix_primitives::sync::oneshot::channel();
                services
                    .acknowledgements
                    .insert(id, TestAcknowledgementPlan::Completion(receiver));
                completions.push(nervix_primitives::task::spawn(async move {
                    let outcome = if id == 1 {
                        IngestorAcknowledgementOutcome::Failed {
                            reason: "refused".into(),
                        }
                    } else {
                        IngestorAcknowledgementOutcome::Acknowledged
                    };
                    assert!(
                        sender.send(outcome).is_ok(),
                        "a sibling outcome cannot cancel this delivery's graph wait"
                    );
                }));
            }
            let mut host = IngestorHost::new(services);
            let mut connector = empty_ingestor(observations.clone());
            let batch = IndividualBatch::intake(
                &mut host,
                three_messages().into(),
                Duration::from_secs(3_600),
            )
            .await;
            let result = batch
                .settle(
                    &mut connector,
                    &mut host,
                    BatchSettlement {
                        position_class: PositionClass::BrokerRedelivered,
                        retry_after: Duration::from_millis(1),
                    },
                )
                .await;
            assert!(matches!(result, BatchDisposition::Retry));
            for completion in completions {
                completion
                    .await
                    .assured("a panicking modeled completion fails the check");
            }
            let record = observations.lock();
            let mut confirmed = record.acknowledged.clone();
            confirmed.sort();
            assert_eq!(confirmed, [vec![0], vec![2]]);
            assert_eq!(record.rejected, [vec![1]]);
        });
    });
}

#[nervix_primitives::test]
async fn individual_settlement_confirms_ready_siblings_while_one_graph_wait_remains_pending() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let (sender, receiver) = nervix_primitives::sync::oneshot::channel();
    let mut services = FakeHost::running(observations.clone());
    services.acknowledgements = BTreeMap::from([
        (
            0,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Acknowledged),
        ),
        (1, TestAcknowledgementPlan::Completion(receiver)),
        (
            2,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Acknowledged),
        ),
    ]);
    let completion = nervix_primitives::task::spawn({
        let observations = observations.clone();
        async move {
            nervix_primitives::time::timeout(Duration::from_secs(10), async {
                loop {
                    nervix_primitives::task::consume_budget().await;
                    if observations.lock().acknowledged.len() == 2 {
                        break;
                    }
                    sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .assured("ready siblings confirm before the pending graph responds");
            assert!(
                sender
                    .send(IngestorAcknowledgementOutcome::Failed {
                        reason: "refused".into(),
                    })
                    .is_ok()
            );
        }
    });
    let mut host = IngestorHost::new(services);
    let mut connector = empty_ingestor(observations.clone());
    let batch =
        IndividualBatch::intake(&mut host, three_messages().into(), Duration::from_secs(30)).await;
    let result = batch
        .settle(
            &mut connector,
            &mut host,
            BatchSettlement {
                position_class: PositionClass::BrokerRedelivered,
                retry_after: Duration::from_millis(1),
            },
        )
        .await;
    assert!(matches!(result, BatchDisposition::Retry));
    completion
        .await
        .assured("the completion task's assertion passes");
    assert_eq!(observations.lock().rejected, [vec![1]]);
}

async fn individual_transport_recovery_keeps_admitted_graph_work() {
    let rejection = Arc::new(Notify::new());
    let observations = Arc::new(Mutex::new(IngestorLoopObservations {
        rejection_notice: Some(rejection.clone()),
        ..IngestorLoopObservations::default()
    }));
    let (sender, receiver) = nervix_primitives::sync::oneshot::channel();
    let mut services = FakeHost::running(observations.clone());
    services.acknowledgements = BTreeMap::from([
        (
            0,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Acknowledged),
        ),
        (
            1,
            TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Failed {
                reason: "refused".into(),
            }),
        ),
        (2, TestAcknowledgementPlan::Completion(receiver)),
    ]);
    let completion = nervix_primitives::task::spawn(async move {
        rejection.notified().await;
        assert!(
            sender
                .send(IngestorAcknowledgementOutcome::Acknowledged)
                .is_ok(),
            "a failed transport must retain the graph wait of every admitted sibling"
        );
    });
    let mut host = IngestorHost::new(services);
    let mut connector = empty_ingestor(observations.clone());
    connector.reject_failures_left = 1;
    let batch =
        IndividualBatch::intake(&mut host, three_messages().into(), Duration::from_secs(30)).await;
    let result = batch
        .settle(
            &mut connector,
            &mut host,
            BatchSettlement {
                position_class: PositionClass::BrokerRedelivered,
                retry_after: Duration::from_millis(1),
            },
        )
        .await;
    completion
        .await
        .assured("every admitted graph delivery completes before recovery");
    let BatchDisposition::RetryRejection(mut retained) = result else {
        panic!("the lost transport retains every unconfirmed position");
    };
    assert!(retained.contains(&1));
    assert!(
        retained.contains(&2),
        "the sibling completed after the transport was lost"
    );
    let record = observations.lock();
    retained.extend(record.acknowledged.iter().flatten());
    retained.sort();
    assert_eq!(retained, [0, 1, 2]);
    assert_eq!(
        record.rejected,
        [vec![1]],
        "one failed settlement starts recovery once"
    );
}

#[nervix_primitives::test]
async fn individual_settlement_waits_for_admitted_graph_work_before_recovering_the_transport() {
    individual_transport_recovery_keeps_admitted_graph_work().await;
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_individual_settlement_keeps_admitted_graph_work_until_transport_recovery() {
    nervix_model_harness::shuttle::check_interleavings(|| {
        shuttle::future::block_on(individual_transport_recovery_keeps_admitted_graph_work());
    });
}

#[nervix_primitives::test]
async fn individual_settlement_requeues_only_the_delivery_the_host_refused() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut services = FakeHost::running(observations.clone());
    services.refused_payload = Some(1);
    let mut host = IngestorHost::new(services);
    let mut connector = empty_ingestor(observations.clone());
    let batch =
        IndividualBatch::intake(&mut host, three_messages().into(), Duration::from_secs(1)).await;
    let result = batch
        .settle(
            &mut connector,
            &mut host,
            BatchSettlement {
                position_class: PositionClass::BrokerRedelivered,
                retry_after: Duration::from_millis(1),
            },
        )
        .await;
    assert!(matches!(result, BatchDisposition::Retry));
    let record = observations.lock();
    let mut confirmed = record.acknowledged.clone();
    confirmed.sort();
    assert_eq!(confirmed, [vec![0], vec![2]]);
    assert_eq!(record.rejected, [vec![1]]);
    assert_eq!(record.reported_errors.len(), 1);
}

#[nervix_primitives::test]
async fn individual_settlement_counts_every_unconfirmed_position_when_shutdown_ends_its_waits() {
    for transport_failed in [false, true] {
        let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
        let mut services = FakeHost::running(observations.clone());
        services.allow_unconfirmed_stop = true;
        let (middle, last) = if transport_failed {
            (
                IngestorAcknowledgementOutcome::Failed {
                    reason: "refused".into(),
                },
                IngestorAcknowledgementOutcome::Shutdown,
            )
        } else {
            (
                IngestorAcknowledgementOutcome::Shutdown,
                IngestorAcknowledgementOutcome::Acknowledged,
            )
        };
        services.acknowledgements = BTreeMap::from([
            (
                0,
                TestAcknowledgementPlan::Outcome(IngestorAcknowledgementOutcome::Acknowledged),
            ),
            (1, TestAcknowledgementPlan::Outcome(middle)),
            (2, TestAcknowledgementPlan::Outcome(last)),
        ]);
        let mut host = IngestorHost::new(services);
        let mut connector = empty_ingestor(observations.clone());
        connector.reject_failures_left = usize::from(transport_failed);
        let batch =
            IndividualBatch::intake(&mut host, three_messages().into(), Duration::from_secs(1))
                .await;
        let result = batch
            .settle(
                &mut connector,
                &mut host,
                BatchSettlement {
                    position_class: PositionClass::BrokerRedelivered,
                    retry_after: Duration::from_millis(1),
                },
            )
            .await;
        assert!(matches!(result, BatchDisposition::Shutdown));
        let record = observations.lock();
        assert_eq!(record.unconfirmed_stops.len(), 1);
        assert_eq!(
            record.acknowledged.iter().map(Vec::len).sum::<usize>() + record.unconfirmed_stops[0],
            3
        );
        if transport_failed {
            assert_eq!(record.rejected, [vec![1]]);
        }
    }
}

#[test]
fn broker_read_observation_follows_each_typed_answer_once() {
    struct Case {
        result: nervix_connector::IngestorResult<IngestorBatch<FakeMessage>>,
        expected: IngestorReadOutcome,
    }
    let cases = [
        Case {
            result: Ok(IngestorBatch::Messages(three_messages().into())),
            expected: IngestorReadOutcome::Delivered,
        },
        Case {
            result: Ok(IngestorBatch::Messages(Vec::new())),
            expected: IngestorReadOutcome::Empty,
        },
        Case {
            result: Ok(IngestorBatch::Interrupted {
                gathered: three_messages().into(),
            }),
            expected: IngestorReadOutcome::Interrupted,
        },
        Case {
            result: Ok(IngestorBatch::Oversized {
                payloads: NonZeroUsize::MIN,
            }),
            expected: IngestorReadOutcome::Empty,
        },
        Case {
            result: Ok(IngestorBatch::ResumeRequired),
            expected: IngestorReadOutcome::Empty,
        },
        Case {
            result: Ok(IngestorBatch::Closed),
            expected: IngestorReadOutcome::Empty,
        },
        Case {
            result: Err(Report::new(IngestorError::Read { connector: "fake" })),
            expected: IngestorReadOutcome::Failed,
        },
    ];
    for case in cases {
        let expected_oversized = match &case.result {
            Ok(IngestorBatch::Oversized { payloads }) => payloads.get(),
            _ => 0,
        };
        let mut host = FakeHost::running(Arc::new(Mutex::new(IngestorLoopObservations::default())));
        host.expected_oversized = expected_oversized;
        let events = host.read_observations.clone();
        let host = IngestorHost::new(host);
        let _outcome = read::BrokerRead {
            batch: case.result,
            end: read::ReadEnd::Stopping,
        }
        .into_outcome(&host);
        assert_eq!(events.lock().reads, [case.expected]);
        assert_eq!(events.lock().oversized, expected_oversized);
    }
}

#[nervix_primitives::test]
async fn broker_payload_observation_counts_the_transport_batch_and_skips_buffer_replay() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector.queued().extend(three_messages());
    connector.replays_pending_after_resume = 2;
    let services = FakeHost::running(observations);
    let events = services.read_observations.clone();
    let (_shutdown, receiver) = watch::channel(false);
    run_ingestor_instance_with_retry(
        connector,
        IngestorHost::new(services),
        replayable(IngestorAckPolicy::None),
        receiver,
    )
    .await;
    let events = events.lock();
    assert_eq!(events.payloads, [(1, 1), (1, 1), (1, 1)]);
    assert_eq!(
        events.reads,
        [
            IngestorReadOutcome::Delivered,
            IngestorReadOutcome::Delivered,
            IngestorReadOutcome::Delivered,
            IngestorReadOutcome::Empty
        ]
    );
}

#[nervix_primitives::test]
async fn paced_reads_record_one_outcome_and_one_duration_for_each_poll_answer() {
    for expected in [
        IngestorReadOutcome::Delivered,
        IngestorReadOutcome::Empty,
        IngestorReadOutcome::Failed,
    ] {
        let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
        let mut connector = empty_ingestor(observations.clone());
        let (shutdown, receiver) = watch::channel(false);
        connector.stop_after_poll = Some(shutdown);
        let mut poll = IngestorPoll {
            messages: Vec::new(),
            failures: Vec::new(),
            dropped_header_values: 0,
            observed_at: Timestamp::from_unix_nanos(1),
        };
        if expected == IngestorReadOutcome::Delivered {
            poll.messages.push(nervix_connector::IngestorPollMessage {
                payload: vec![7],
                headers: nervix_connector::RetainedIngestHeaders::none(),
                metadata: nervix_connector::RetainedIngestMetadata::none(),
            });
        }
        connector
            .polls
            .push_back(if expected == IngestorReadOutcome::Failed {
                Err(Report::new(IngestorError::Read { connector: "fake" }))
            } else {
                Ok(poll)
            });
        let host = FakeHost::running(observations);
        let events = host.read_observations.clone();
        run_paced_ingestor(
            connector,
            host,
            SingleOccurrenceCadence {
                occurrence: Some(Timestamp::from_unix_nanos(1)),
            },
            NonZeroUsize::MIN,
            receiver,
        )
        .await;
        let events = events.lock();
        assert_eq!(events.polls.len(), 1);
        assert_eq!(events.polls[0].0, expected);
        assert!(
            events.reads.is_empty(),
            "paced reads are recorded through the timed observation once"
        );
    }
}

#[nervix_primitives::test]
async fn ending_an_inflight_paced_poll_records_interruption_once() {
    use futures_util::FutureExt as _;
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let host = FakeHost::running(observations);
    let events = host.read_observations.clone();
    let (_messages, receiver) = nervix_primitives::sync::mpsc::channel(1);
    let connector = FakeIngestor::new(receiver);
    let (shutdown, receiver) = watch::channel(false);
    let mut running = Box::pin(run_paced_ingestor(
        connector,
        host,
        SingleOccurrenceCadence {
            occurrence: Some(Timestamp::from_unix_nanos(1)),
        },
        NonZeroUsize::MIN,
        receiver,
    ));
    assert!(
        running.as_mut().now_or_never().is_none(),
        "the poll waits for a transport answer"
    );
    assert!(events.lock().polls.is_empty());
    shutdown.send_replace(true);
    running.await;
    let events = events.lock();
    assert_eq!(events.polls.len(), 1);
    assert_eq!(events.polls[0].0, IngestorReadOutcome::Interrupted);
}

fn settlement(
    operation: IngestorSettlementOperation,
    outcome: IngestorSettlementOutcome,
) -> RecordedSettlement {
    RecordedSettlement { operation, outcome }
}

fn sequential_policy() -> IngestorAckPolicy {
    IngestorAckPolicy::Sequential {
        timeout: Duration::from_secs(1),
        retry: retry_policy(),
    }
}

/// Runs the broker loop over `connector` with `host` until the connector's input closes, and
/// answers the lifecycle observations the host recorded.
async fn run_recording_broker(
    connector: FakeIngestor,
    host: FakeHost,
    policy: IngestorAckPolicy,
) -> Arc<Mutex<LifecycleObservations>> {
    let lifecycle = host.lifecycle.clone();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    run_ingestor_instance_with_retry(
        connector,
        IngestorHost::new(host),
        replayable(policy),
        shutdown_rx,
    )
    .await;
    drop(shutdown_tx);
    lifecycle
}

#[nervix_primitives::test]
async fn every_acknowledgement_wait_is_recorded_once_before_its_positions_settle() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    observations.lock().ack_outcomes = VecDeque::from([
        IngestorAcknowledgementOutcome::Failed {
            reason: "refused".to_string(),
        },
        IngestorAcknowledgementOutcome::TimedOut {
            silence: Duration::from_secs(1),
        },
        IngestorAcknowledgementOutcome::Acknowledged,
        IngestorAcknowledgementOutcome::Acknowledged,
    ]);
    let mut connector = empty_ingestor(observations.clone());
    connector
        .queued()
        .extend(three_messages().into_iter().take(2));
    connector.replay_rejected = true;

    let lifecycle = run_recording_broker(
        connector,
        FakeHost::running(observations.clone()),
        sequential_policy(),
    )
    .await;

    let lifecycle = lifecycle.lock();
    assert_eq!(
        lifecycle.acknowledgements,
        vec![
            IngestorAcknowledgementOutcome::Failed {
                reason: "refused".to_string(),
            },
            IngestorAcknowledgementOutcome::TimedOut {
                silence: Duration::from_secs(1),
            },
            IngestorAcknowledgementOutcome::Acknowledged,
            IngestorAcknowledgementOutcome::Acknowledged,
        ]
    );
    use IngestorSettlementOperation::{Acknowledge, Reject};
    use IngestorSettlementOutcome::Succeeded;
    assert_eq!(
        lifecycle.settlements,
        vec![
            settlement(Reject, Succeeded),
            settlement(Reject, Succeeded),
            settlement(Acknowledge, Succeeded),
            settlement(Acknowledge, Succeeded),
        ]
    );
    assert!(lifecycle.suspends.is_empty());
    assert_eq!(lifecycle.resumes, vec![IngestorResumeOutcome::Ready]);
    let observations = observations.lock();
    assert_eq!(observations.rejected, vec![vec![0], vec![0]]);
    assert_eq!(observations.acknowledged, vec![vec![0], vec![1]]);
    assert_eq!(observations.reported_errors, vec!["refused".to_string()]);
}

#[nervix_primitives::test]
async fn a_failed_rejection_is_recorded_and_suspends_the_connector_for_its_rejection() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    observations.lock().ack_outcomes = VecDeque::from([
        IngestorAcknowledgementOutcome::Failed {
            reason: "relay owner crashed".to_string(),
        },
        IngestorAcknowledgementOutcome::Acknowledged,
    ]);
    let mut connector = empty_ingestor(observations.clone());
    connector
        .queued()
        .extend(three_messages().into_iter().take(1));
    connector.reject_failures_left = 2;
    connector.replay_rejected = true;

    let lifecycle = run_recording_broker(
        connector,
        FakeHost::running(observations.clone()),
        sequential_policy(),
    )
    .await;

    let lifecycle = lifecycle.lock();
    use IngestorSettlementOperation::{Acknowledge, Reject};
    use IngestorSettlementOutcome::{Failed, Succeeded};
    assert_eq!(
        lifecycle.settlements,
        vec![
            settlement(Reject, Failed),
            settlement(Reject, Failed),
            settlement(Reject, Succeeded),
            settlement(Acknowledge, Succeeded),
        ],
        "the retried rejection after each resume is a transport settlement of its own"
    );
    assert_eq!(
        lifecycle.suspends,
        vec![
            IngestorSuspendCause::Rejection,
            IngestorSuspendCause::Rejection
        ]
    );
    assert_eq!(
        lifecycle.resumes,
        vec![IngestorResumeOutcome::Ready; 3],
        "the initial resume and the resume after each failed rejection"
    );
    assert_eq!(observations.lock().suspends, 2);
}

#[nervix_primitives::test]
async fn a_failed_read_that_loses_the_transport_suspends_the_connector_for_its_read_failure() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector
        .queued()
        .extend(three_messages().into_iter().take(1));
    connector.failed_reads_left = 1;
    connector.failure_loses_transport = true;

    let lifecycle = run_recording_broker(
        connector,
        FakeHost::running(observations.clone()),
        sequential_policy(),
    )
    .await;

    let lifecycle = lifecycle.lock();
    assert_eq!(lifecycle.suspends, vec![IngestorSuspendCause::ReadFailure]);
    assert_eq!(lifecycle.resumes, vec![IngestorResumeOutcome::Ready; 2]);
    assert_eq!(
        lifecycle.acknowledgements,
        vec![IngestorAcknowledgementOutcome::Acknowledged]
    );
    assert_eq!(
        observations.lock().suspends,
        0,
        "the connector suspended itself; the host only records it"
    );
}

#[nervix_primitives::test]
async fn a_failed_read_that_keeps_the_transport_and_a_requested_resume_suspend_nothing() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector
        .queued()
        .extend(three_messages().into_iter().take(1));
    connector.failed_reads_left = 1;
    connector.resume_required = true;

    let lifecycle = run_recording_broker(
        connector,
        FakeHost::running(observations.clone()),
        sequential_policy(),
    )
    .await;

    let lifecycle = lifecycle.lock();
    assert!(lifecycle.suspends.is_empty());
    assert_eq!(
        lifecycle.resumes,
        vec![IngestorResumeOutcome::Ready; 2],
        "the initial resume and the one the connector asked for"
    );
}

#[nervix_primitives::test]
async fn every_completed_broker_resume_is_recorded_with_its_outcome() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector.resume_results = VecDeque::from([
        Ok(IngestorResume::Waiting {
            retry_after: Duration::from_millis(1),
        }),
        Err(Report::new(IngestorError::Resume { connector: "fake" })),
        Ok(IngestorResume::Ready),
    ]);

    let lifecycle = run_recording_broker(
        connector,
        FakeHost::running(observations.clone()),
        sequential_policy(),
    )
    .await;

    assert_eq!(
        lifecycle.lock().resumes,
        vec![
            IngestorResumeOutcome::Waiting,
            IngestorResumeOutcome::Failed,
            IngestorResumeOutcome::Ready,
        ]
    );
}

#[nervix_primitives::test]
async fn a_stop_during_an_acknowledgement_wait_is_recorded_before_its_unconfirmed_positions() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    observations.lock().ack_outcomes = VecDeque::from([IngestorAcknowledgementOutcome::Shutdown]);
    let mut connector = empty_ingestor(observations.clone());
    connector
        .queued()
        .extend(three_messages().into_iter().take(1));
    let mut host = FakeHost::running(observations.clone());
    host.allow_unconfirmed_stop = true;

    let lifecycle = run_recording_broker(connector, host, sequential_policy()).await;

    let lifecycle = lifecycle.lock();
    assert_eq!(
        lifecycle.acknowledgements,
        vec![IngestorAcknowledgementOutcome::Shutdown]
    );
    assert!(lifecycle.settlements.is_empty());
    assert_eq!(observations.lock().unconfirmed_stops, vec![1]);
}

#[nervix_primitives::test]
async fn a_quiesce_suspends_only_a_ready_connector_and_is_recorded_once() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let connector = empty_ingestor(observations.clone());
    let mut host = FakeHost::running(observations.clone());
    host.suspend_intake = true;
    let lifecycle = host.lifecycle.clone();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let run = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
        connector,
        IngestorHost::new(host),
        replayable(sequential_policy()),
        shutdown_rx,
    ));

    // A connector the loop never resumed is not ready, so the quiesce has nothing to suspend.
    shutdown_tx.send_replace(true);
    run.await
        .assured("the loop ends at shutdown without panicking");
    assert!(lifecycle.lock().suspends.is_empty());

    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    let mut host = FakeHost::running(observations.clone());
    host.suspend_intake = true;
    host.wake_suspension = true;
    let mut ready = true;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let action = prepare_paced_ingestor(
        &mut connector,
        &mut host,
        &mut ready,
        &mut false,
        &mut shutdown,
    )
    .await;
    assert_eq!(action, PacedIngestorAction::Restart);
    assert_eq!(
        host.lifecycle.lock().suspends,
        vec![IngestorSuspendCause::Quiesce]
    );
}

#[nervix_primitives::test]
async fn paced_preparation_records_resume_outcomes_and_a_poll_failure_that_lost_the_transport() {
    let observations = Arc::new(Mutex::new(IngestorLoopObservations::default()));
    let mut connector = empty_ingestor(observations.clone());
    connector.resume_results = VecDeque::from([
        Err(Report::new(IngestorError::Resume { connector: "fake" })),
        Ok(IngestorResume::Ready),
        Ok(IngestorResume::Ready),
    ]);
    connector.failure_loses_transport = true;
    connector.polls = VecDeque::from([Err(Report::new(IngestorError::Read { connector: "fake" }))]);
    let mut host = FakeHost::running(observations.clone());
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let mut ready = false;
    let mut failed_poll = false;

    let action = prepare_paced_ingestor(
        &mut connector,
        &mut host,
        &mut ready,
        &mut failed_poll,
        &mut shutdown,
    )
    .await;
    assert_eq!(action, PacedIngestorAction::Restart);
    let action = prepare_paced_ingestor(
        &mut connector,
        &mut host,
        &mut ready,
        &mut failed_poll,
        &mut shutdown,
    )
    .await;
    assert_eq!(action, PacedIngestorAction::Poll);
    assert!(connector.poll(Timestamp::from_unix_nanos(0)).await.is_err());
    failed_poll = true;
    let action = prepare_paced_ingestor(
        &mut connector,
        &mut host,
        &mut ready,
        &mut failed_poll,
        &mut shutdown,
    )
    .await;
    assert_eq!(action, PacedIngestorAction::Poll);
    assert!(!failed_poll, "the read-failure suspension is recorded once");

    let lifecycle = host.lifecycle.lock();
    assert_eq!(
        lifecycle.resumes,
        vec![
            IngestorResumeOutcome::Failed,
            IngestorResumeOutcome::Ready,
            IngestorResumeOutcome::Ready,
        ]
    );
    assert_eq!(lifecycle.suspends, vec![IngestorSuspendCause::ReadFailure]);
}
