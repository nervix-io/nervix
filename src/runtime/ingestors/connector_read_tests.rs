//! The broker loop's reads, which the host may interrupt but never abandons.
//!
//! Layer: test harness.
//! - **Owns.** The read-in-flight assertion the loop's ingestor doubles share, a ingestor double that
//!   gathers a batch from a channel one message at a time, a host double that records what it took
//!   in, both of them recording into one ordered log with the check over that log, and the unit
//!   tests of a read that a stop, a quiesce change or a due flush meets.
//! - **Depends on.** The production broker loop and its read, and the ingestor contract.
//! - **Must not know.** Broker drivers, the runtime ingestor host or the quiesce control.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeSet;

use nervix_connector::{
    IngestMessageHeaders, IngestMetadataRow, IngestorBatchAcknowledgementServices,
    IngestorReadCancellation, IngestorResult,
};
use nervix_primitives::sync::{Notify, blocking::Mutex, mpsc};

use super::{
    read::{BrokerRead, ReadEnd},
    *,
};

/// Whether a ingestor double has a read in flight.
///
/// The host awaits every read to completion. A read whose future the host dropped leaves its flag
/// set, so a double asked to read again, to suspend or to close while its last read is still in
/// flight was dropped mid-read, which the ingestor contract forbids.
#[derive(Debug, Default)]
pub(super) struct ReadInFlight {
    in_flight: bool,
}

impl ReadInFlight {
    pub(super) fn start(&mut self) {
        assert!(
            !self.in_flight,
            "the host dropped a source read in flight and started another"
        );
        self.in_flight = true;
    }

    pub(super) fn finish(&mut self) {
        self.in_flight = false;
    }

    /// Asserts that no read is in flight when the host performs `operation` on the ingestor.
    pub(super) fn assert_none_before(&self, operation: &str) {
        assert!(
            !self.in_flight,
            "the host had the source {operation} while a read it dropped was still in flight"
        );
    }
}

/// What the gathering ingestor and the recording host did, in the order the loop drove them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReadEvent {
    /// The ingestor took this position from its transport.
    Took(u64),
    /// The host cancelled the read the ingestor was waiting in.
    Cancelled,
    /// The host took in a batch of these positions.
    TookIn(Vec<u64>),
    /// The ingestor confirmed these positions on its transport.
    Acknowledged(Vec<u64>),
    /// The host flushed the positions it had taken in since its last flush.
    Flushed,
    /// The host counted payloads the ingestor dropped for their size.
    CountedOversized(NonZeroUsize),
    Suspended,
    Closed,
}

/// The ordered log the gathering ingestor and the recording host share.
#[derive(Clone)]
pub(super) struct ReadLog {
    events: Arc<Mutex<Vec<ReadEvent>>>,
}

impl ReadLog {
    pub(super) fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record(&self, event: ReadEvent) {
        self.events.lock().push(event);
    }

    pub(super) fn events(&self) -> Vec<ReadEvent> {
        self.events.lock().clone()
    }
}

/// Checks the loop's one guarantee about interrupted reads: every position the ingestor took reached
/// the host's intake exactly once, and before the ingestor was suspended or closed.
pub(super) fn assert_every_taken_position_taken_in_first(events: &[ReadEvent]) {
    let mut taken = BTreeSet::new();
    let mut taken_in = BTreeSet::new();
    for event in events {
        match event {
            ReadEvent::Took(position) => {
                assert!(taken.insert(*position), "the source took {position} twice");
            }
            ReadEvent::TookIn(positions) => {
                for position in positions {
                    assert!(
                        taken_in.insert(*position),
                        "the host took {position} in twice: {events:?}"
                    );
                }
            }
            ReadEvent::Suspended | ReadEvent::Closed => {
                assert_eq!(
                    taken, taken_in,
                    "the source was suspended or closed before the host took in what it had read: \
                     {events:?}"
                );
            }
            ReadEvent::Cancelled
            | ReadEvent::Acknowledged(_)
            | ReadEvent::Flushed
            | ReadEvent::CountedOversized(_) => {}
        }
    }
    assert_eq!(
        taken, taken_in,
        "a position the source took was lost: {events:?}"
    );
}

/// What the tests learn of the gathering ingestor as the loop drives it.
pub(super) struct IngestorSignals {
    /// A read started waiting for its transport.
    pub(super) reading: Notify,
    /// The ingestor took a message from its transport.
    pub(super) took: Notify,
    /// The ingestor suspended.
    pub(super) suspended: Notify,
}

impl IngestorSignals {
    pub(super) fn new() -> Self {
        Self {
            reading: Notify::new(),
            took: Notify::new(),
            suspended: Notify::new(),
        }
    }
}

/// One message of the gathering ingestor, whose payload is its position.
pub(super) struct GatheredMessage {
    position: u64,
    payload: Vec<u8>,
}

impl GatheredMessage {
    fn new(position: u64) -> Self {
        Self {
            position,
            payload: vec![u8::try_from(position).assured("the tests use positions below 256")],
        }
    }
}

impl IngestMessageHeaders for GatheredMessage {
    fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
}

impl IngestorMessage for GatheredMessage {
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
        IngestMetadataRow::headers(self)
    }
}

/// A ingestor that gathers each batch from a channel one message at a time, waiting for the next one
/// until its batch is full, and that returns what it gathered once the host cancels.
pub(super) struct GatheringIngestor {
    transport: mpsc::Receiver<u64>,
    events: ReadLog,
    signals: Arc<IngestorSignals>,
    read: ReadInFlight,
    /// Ends the first read after its first message without the host cancelling it, which the ingestor
    /// contract forbids.
    interrupts_unasked: bool,
}

impl GatheringIngestor {
    pub(super) fn new(
        transport: mpsc::Receiver<u64>,
        events: ReadLog,
        signals: Arc<IngestorSignals>,
    ) -> Self {
        Self {
            transport,
            events,
            signals,
            read: ReadInFlight::default(),
            interrupts_unasked: false,
        }
    }

    async fn gather(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<GatheredMessage>> {
        self.signals.reading.notify_one();
        let mut gathered = Vec::with_capacity(request.max_messages.get());
        while gathered.len() < request.max_messages.get() {
            nervix_primitives::task::consume_budget().await;
            let Some(received) = cancellation.unless_cancelled(self.transport.recv()).await else {
                self.events.record(ReadEvent::Cancelled);
                return Ok(IngestorBatch::Interrupted { gathered });
            };
            let Some(position) = received else {
                // The transport ended: what the read gathered is still the host's to take in, and
                // the next read reports the end.
                if gathered.is_empty() {
                    return Ok(IngestorBatch::Closed);
                }
                return Ok(IngestorBatch::Messages(gathered));
            };
            self.events.record(ReadEvent::Took(position));
            gathered.push(GatheredMessage::new(position));
            self.signals.took.notify_one();
            if self.interrupts_unasked {
                self.interrupts_unasked = false;
                return Ok(IngestorBatch::Interrupted { gathered });
            }
        }
        Ok(IngestorBatch::Messages(gathered))
    }
}

#[async_trait]
impl IngestorConnector for GatheringIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = ();

    async fn open(_plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        panic!("the tests install their source directly")
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        self.read.assert_none_before("suspend");
        self.events.record(ReadEvent::Suspended);
        self.signals.suspended.notify_one();
        Ok(())
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.read.assert_none_before("close");
        self.events.record(ReadEvent::Closed);
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for GatheringIngestor {
    type Message = GatheredMessage;
    type Position = u64;

    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        self.read.start();
        let batch = self.gather(request, cancellation).await;
        self.read.finish();
        batch
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        self.events
            .record(ReadEvent::Acknowledged(positions.to_vec()));
        Ok(())
    }

    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        panic!("the tests acknowledge every batch, yet the host rejected {positions:?}")
    }
}

/// What a test changes of the recording host while the loop runs.
pub(super) struct HostControl {
    suspended: Mutex<bool>,
    quiesce_changed: Notify,
    /// The host flushed what it had taken in.
    pub(super) flushed: Notify,
}

impl HostControl {
    pub(super) fn new() -> Self {
        Self {
            suspended: Mutex::new(false),
            quiesce_changed: Notify::new(),
            flushed: Notify::new(),
        }
    }

    /// Suspends intake, as a quiesce engagement that stops reading does, and tells the loop.
    pub(super) fn suspend_intake(&self) {
        *self.suspended.lock() = true;
        self.quiesce_changed.notify_one();
    }
}

struct ImmediateAck;

#[async_trait]
impl IngestorBatchAcknowledgementServices for ImmediateAck {
    async fn wait(&mut self, _timeout: Duration) -> IngestorAcknowledgementOutcome {
        IngestorAcknowledgementOutcome::Ack
    }
}

/// A host that records each batch it takes in, whose graph acknowledges every batch at once, and
/// whose ingest group falls due for a flush `flush_every` after it took a batch in.
pub(super) struct RecordingHost {
    events: ReadLog,
    control: Arc<HostControl>,
    flush_every: Option<Duration>,
    flush_due: Option<Instant>,
    reported_errors: Arc<Mutex<Vec<String>>>,
}

impl RecordingHost {
    pub(super) fn new(events: ReadLog, control: Arc<HostControl>) -> Self {
        Self {
            events,
            control,
            flush_every: None,
            flush_due: None,
            reported_errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn take_in(&mut self, messages: &[IngestorIntakeMessage<'_>]) {
        let mut positions = Vec::with_capacity(messages.len());
        for message in messages {
            let position = message
                .payload
                .first()
                .assured("each test payload is its one-byte position");
            positions.push(u64::from(*position));
        }
        self.events.record(ReadEvent::TookIn(positions));
        let Some(flush_every) = self.flush_every else {
            return;
        };
        let due = Instant::now()
            .checked_add(flush_every)
            .assured("the tests flush within a second of the paused clock's start");
        self.flush_due = Some(due);
    }
}

#[async_trait]
impl IngestorHostServices for RecordingHost {
    async fn intake_unacknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
        self.take_in(&messages);
        Ok(())
    }

    async fn intake_acknowledged(
        &mut self,
        messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
        self.take_in(&messages);
        Ok(IngestorBatchAcknowledgement::new(ImmediateAck))
    }

    async fn flush(&mut self) -> IngestorIntakeResult<()> {
        if self.flush_due.take().is_some() {
            self.events.record(ReadEvent::Flushed);
            self.control.flushed.notify_one();
        }
        Ok(())
    }

    async fn replay_buffered(&mut self) -> IngestorIntakeResult<bool> {
        Ok(false)
    }

    fn next_flush(&self) -> Option<Instant> {
        self.flush_due
    }

    fn should_suspend_intake(&self) -> bool {
        *self.control.suspended.lock()
    }

    async fn wait_for_quiesce_change(&mut self) {
        self.control.quiesce_changed.notified().await;
    }

    async fn wait_until_not_suspended(&mut self) {
        // The tests never release a suspension: a suspended loop waits here until it stops.
        std::future::pending::<()>().await;
    }

    async fn wait_until_active(&mut self) -> bool {
        true
    }

    fn mark_ready(&self) {}
    fn mark_unready(&self) {}
    fn record_transient_error(&self, reason: String, _retry_after: Duration) {
        panic!("a gathering read recorded an unexpected transient error: {reason}");
    }
    fn clear_transient_error(&self) {}
    fn report_error(&self, message: String) {
        self.reported_errors.lock().push(message);
    }
    fn handle_ack_failure(&self, reason: String) {
        panic!("a test graph that acknowledges every batch failed one: {reason}");
    }
    fn record_transport_redeliveries(&self, positions: usize) {
        panic!("a replayable test source discarded {positions} positions after a reconnect");
    }
    fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
        self.events.record(ReadEvent::CountedOversized(payloads));
    }
    fn record_unconfirmed_stop(&self, positions: usize) {
        panic!("a test graph that acknowledges at once left {positions} positions unconfirmed");
    }
}

/// A policy that gathers up to `max_in_flight` messages in one batch.
pub(super) fn gathering_policy(max_in_flight: usize) -> BrokerIngestorPolicy {
    let acknowledgement = IngestorAckPolicy::Parallel {
        max_in_flight: NonZeroUsize::new(max_in_flight)
            .assured("the tests gather at least one message"),
        batch_timeout: Duration::from_secs(60),
        timeout: Duration::from_secs(60),
        retry: INGESTOR_RECONNECT_POLICY,
    };
    BrokerIngestorPolicy::new(acknowledgement, PositionClass::Replayable)
}

/// A ingestor the tests feed and the host it reads for, before the loop takes them.
struct Fixture {
    source: GatheringIngestor,
    host: RecordingHost,
    observers: Observers,
}

/// What a test keeps of its fixture once the loop runs the ingestor and the host.
struct Observers {
    transport: mpsc::Sender<u64>,
    events: ReadLog,
    signals: Arc<IngestorSignals>,
    control: Arc<HostControl>,
    reported_errors: Arc<Mutex<Vec<String>>>,
}

impl Fixture {
    fn new() -> Self {
        let (transport, receiver) = mpsc::channel(4);
        let events = ReadLog::new();
        let signals = Arc::new(IngestorSignals::new());
        let control = Arc::new(HostControl::new());
        let host = RecordingHost::new(events.clone(), control.clone());
        let observers = Observers {
            transport,
            events: events.clone(),
            signals: signals.clone(),
            control,
            reported_errors: host.reported_errors.clone(),
        };
        Self {
            source: GatheringIngestor::new(receiver, events, signals),
            host,
            observers,
        }
    }
}

impl Observers {
    async fn send(&self, position: u64) {
        self.transport
            .send(position)
            .await
            .assured("the source keeps its transport until the test ends");
    }

    fn events(&self) -> Vec<ReadEvent> {
        self.events.events()
    }
}

/// How long a test waits for the loop to reach a step that a working loop reaches at once. Only a
/// broken loop reaches the bound, which fails the test instead of leaving it waiting.
const STEP_BOUND: Duration = Duration::from_secs(30);

/// Waits for `step`, which a working loop reaches at once.
async fn reach(step: impl Future<Output = ()>, what: &'static str) {
    let Ok(()) = nervix_primitives::time::timeout(STEP_BOUND, step).await else {
        panic!("{what} did not happen within {STEP_BOUND:?}");
    };
}

/// A broker loop a test started, and the sender of its stop.
struct RunningLoop {
    shutdown: watch::Sender<bool>,
    task: nervix_primitives::task::JoinHandle<()>,
}

impl RunningLoop {
    fn start(source: GatheringIngestor, host: RecordingHost, policy: BrokerIngestorPolicy) -> Self {
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = nervix_primitives::task::spawn(run_ingestor_instance_with_retry(
            source,
            IngestorHost::new(host),
            policy,
            shutdown_rx,
        ));
        Self { shutdown, task }
    }

    /// Stops the loop and waits for it to end.
    async fn stop(self) {
        self.shutdown.send_replace(true);
        nervix_primitives::time::timeout(STEP_BOUND, self.task)
            .await
            .assured("the loop ends at once after its stop")
            .assured("the loop ends without a task panic");
    }
}

#[nervix_primitives::test]
async fn a_stop_during_a_read_takes_in_what_the_source_gathered_before_closing_it() {
    let Fixture {
        source,
        host,
        observers,
    } = Fixture::new();
    let running = RunningLoop::start(source, host, gathering_policy(3));
    observers.send(1).await;
    reach(
        observers.signals.took.notified(),
        "the source takes the message",
    )
    .await;
    observers.send(2).await;
    reach(
        observers.signals.took.notified(),
        "the source takes the message",
    )
    .await;

    running.stop().await;

    let events = observers.events();
    assert_eq!(
        events,
        [
            ReadEvent::Took(1),
            ReadEvent::Took(2),
            ReadEvent::Cancelled,
            ReadEvent::TookIn(vec![1, 2]),
            ReadEvent::Acknowledged(vec![1, 2]),
            ReadEvent::Closed,
        ]
    );
    assert_every_taken_position_taken_in_first(&events);
    assert!(observers.reported_errors.lock().is_empty());
}

#[nervix_primitives::test]
async fn a_quiesce_change_during_a_read_takes_in_what_the_source_gathered_before_suspending_it() {
    let Fixture {
        source,
        host,
        observers,
    } = Fixture::new();
    let running = RunningLoop::start(source, host, gathering_policy(3));
    observers.send(1).await;
    reach(
        observers.signals.took.notified(),
        "the source takes the message",
    )
    .await;

    observers.control.suspend_intake();
    reach(
        observers.signals.suspended.notified(),
        "the loop suspends the source",
    )
    .await;
    running.stop().await;

    let events = observers.events();
    assert_eq!(
        events,
        [
            ReadEvent::Took(1),
            ReadEvent::Cancelled,
            ReadEvent::TookIn(vec![1]),
            ReadEvent::Acknowledged(vec![1]),
            ReadEvent::Suspended,
            ReadEvent::Closed,
        ]
    );
    assert_every_taken_position_taken_in_first(&events);
    assert!(observers.reported_errors.lock().is_empty());
}

#[nervix_primitives::test]
async fn a_read_cancelled_before_it_took_anything_takes_nothing_in() {
    let Fixture {
        source,
        host,
        observers,
    } = Fixture::new();
    let running = RunningLoop::start(source, host, gathering_policy(3));
    reach(observers.signals.reading.notified(), "the loop reads").await;

    running.stop().await;

    assert_eq!(
        observers.events(),
        [ReadEvent::Cancelled, ReadEvent::Closed],
        "an interrupted read that gathered nothing ends the loop without a batch"
    );
    assert!(
        observers.reported_errors.lock().is_empty(),
        "a cancelled read with nothing gathered is not an empty batch"
    );
}

#[nervix_primitives::test]
async fn a_stop_signal_that_changes_without_stopping_restarts_the_read() {
    let Fixture {
        source,
        host,
        observers,
    } = Fixture::new();
    let running = RunningLoop::start(source, host, gathering_policy(3));
    reach(observers.signals.reading.notified(), "the loop reads").await;

    running.shutdown.send_replace(false);
    // The loop reads again once the change ended the first read.
    reach(observers.signals.reading.notified(), "the loop reads").await;
    running.stop().await;

    assert_eq!(
        observers.events(),
        [
            ReadEvent::Cancelled,
            ReadEvent::Cancelled,
            ReadEvent::Closed
        ],
        "the change ended the first read, and the stop the second"
    );
    assert!(observers.reported_errors.lock().is_empty());
}

#[nervix_primitives::test(start_paused = true)]
async fn a_flush_due_while_a_read_waits_happens_without_cancelling_the_read() {
    let Fixture {
        source,
        mut host,
        observers,
    } = Fixture::new();
    host.flush_every = Some(Duration::from_millis(250));
    let unacknowledged = BrokerIngestorPolicy {
        acknowledgement: IngestorAckPolicy::None,
        retry: INGESTOR_RECONNECT_POLICY,
        position_class: PositionClass::None,
    };
    let running = RunningLoop::start(source, host, unacknowledged);
    observers.send(1).await;
    reach(
        observers.signals.took.notified(),
        "the source takes the message",
    )
    .await;
    // The read after the first batch waits on an empty transport, so the paused clock reaches the
    // flush that batch made due while the read is still waiting.
    reach(
        observers.control.flushed.notified(),
        "the loop flushes while the read waits",
    )
    .await;
    observers.send(2).await;
    reach(
        observers.signals.took.notified(),
        "the source takes the message",
    )
    .await;

    running.stop().await;

    let events = observers.events();
    assert_eq!(
        events,
        [
            ReadEvent::Took(1),
            ReadEvent::TookIn(vec![1]),
            ReadEvent::Acknowledged(vec![1]),
            ReadEvent::Flushed,
            ReadEvent::Took(2),
            ReadEvent::TookIn(vec![2]),
            ReadEvent::Acknowledged(vec![2]),
            ReadEvent::Cancelled,
            ReadEvent::Flushed,
            ReadEvent::Closed,
        ],
        "the read that waited through the flush delivered its message without a cancellation"
    );
    assert_every_taken_position_taken_in_first(&events);
}

#[nervix_primitives::test]
async fn an_interruption_the_host_did_not_ask_for_is_reported_and_its_messages_taken_in() {
    let Fixture {
        mut source,
        host,
        observers,
    } = Fixture::new();
    source.interrupts_unasked = true;
    let running = RunningLoop::start(source, host, gathering_policy(3));
    reach(observers.signals.reading.notified(), "the loop reads").await;
    observers.send(1).await;
    reach(
        observers.signals.took.notified(),
        "the source takes the message",
    )
    .await;
    // The loop reads again once it took the interrupted batch in.
    reach(observers.signals.reading.notified(), "the loop reads").await;

    running.stop().await;

    let events = observers.events();
    assert_eq!(
        events,
        [
            ReadEvent::Took(1),
            ReadEvent::TookIn(vec![1]),
            ReadEvent::Acknowledged(vec![1]),
            ReadEvent::Cancelled,
            ReadEvent::Closed,
        ]
    );
    assert_every_taken_position_taken_in_first(&events);
    assert_eq!(
        *observers.reported_errors.lock(),
        ["source interrupted a read the host did not cancel"]
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn only_a_read_the_host_did_not_cancel_waits_out_a_retry_delay() {
    let Fixture { host, .. } = Fixture::new();
    let mut host = IngestorHost::new(host);
    let (_shutdown, mut shutdown_rx) = watch::channel(false);
    let delay = Duration::from_secs(3600);

    let started = Instant::now();
    assert!(
        !ReadEnd::Stopping
            .wait_for_retry(&mut host, &mut shutdown_rx, delay)
            .await,
        "a read cancelled by a stop ends the loop"
    );
    assert!(
        ReadEnd::Restarting
            .wait_for_retry(&mut host, &mut shutdown_rx, delay)
            .await,
        "a read cancelled by a quiesce change starts the next turn"
    );
    assert_eq!(
        Instant::now(),
        started,
        "the change that cancelled a read also ended its retry wait"
    );

    assert!(
        ReadEnd::Returned
            .wait_for_retry(&mut host, &mut shutdown_rx, delay)
            .await
    );
    let delay_end = started
        .checked_add(delay)
        .assured("an hour after the paused clock's start fits its instants");
    assert!(
        Instant::now() >= delay_end,
        "a read that returned on its own waits out the retry delay"
    );
}

#[nervix_primitives::test]
async fn payloads_a_source_dropped_for_their_size_are_counted_and_nothing_is_taken_in() {
    let Fixture {
        host, observers, ..
    } = Fixture::new();
    let host = IngestorHost::new(host);
    let payloads = NonZeroUsize::new(3).assured("three is nonzero");
    let read = BrokerRead::<GatheredMessage> {
        batch: Ok(IngestorBatch::Oversized { payloads }),
        end: ReadEnd::Returned,
    };

    let ReadOutcome::TakeIn(messages) = read.into_outcome(&host) else {
        panic!("a source that dropped oversized payloads leaves the loop reading");
    };

    assert!(messages.is_empty(), "a dropped payload is never taken in");
    assert_eq!(
        observers.events(),
        vec![ReadEvent::CountedOversized(payloads)]
    );
    assert_eq!(
        observers.reported_errors.lock().clone(),
        Vec::<String>::new(),
        "a dropped oversized payload is no ingestor error"
    );
}
