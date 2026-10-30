//! A broker ingestor whose positions belong to the connection that delivered them, and what the
//! broker host loop does with each position class once that connection is lost.
//!
//! Layer: test harness.
//! - **Owns.** The connection-bound broker, ingestor and host doubles that the reconnect unit tests
//!   and the Shuttle check share, and the unit tests of each position class across a lost
//!   connection, of retry backoff across successful reconnects until a batch is delivered, and of
//!   the cadence an ingestor whose delivery mode declares no retry policy reconnects on.
//! - **Depends on.** The production broker host loop, the ingestor contract and its position
//!   classes.
//! - **Must not know.** Broker drivers, the runtime ingestor host or the quiesce control.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    collections::{BTreeMap, VecDeque},
    future,
};

use nervix_connector::{
    IngestMessageHeaders, IngestMetadataRow, IngestorBatchAcknowledgementServices,
    IngestorReadCancellation, IngestorResult,
};
use nervix_primitives::sync::blocking::Mutex;

use super::*;

/// The broker a connection-bound ingestor reads through: a connection delivers from one queue, and
/// once a connection is lost the broker requeues every delivery it did not settle. It also records
/// what the ingestor and the host did, for the checks to compare once the loop has ended.
pub(super) struct RedeliveringBroker {
    connection: u64,
    alive: bool,
    queue: VecDeque<u64>,
    /// The connection each delivered and not yet settled position was delivered on.
    unacknowledged: BTreeMap<u64, u64>,
    /// Every position acknowledged, in acknowledgement order.
    pub(super) acknowledged: Vec<u64>,
    /// Rejections that failed because the connection that delivered their positions was lost.
    pub(super) failed_rejections: usize,
    /// Positions the host counted as redelivered after a reconnect.
    pub(super) counted_redeliveries: usize,
    /// How often the host suspended the ingestor.
    pub(super) suspensions: usize,
}

impl RedeliveringBroker {
    /// A broker whose queue holds `positions`, shared by the ingestor, the host and the check.
    pub(super) fn shared(positions: impl IntoIterator<Item = u64>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            connection: 0,
            alive: true,
            queue: positions.into_iter().collect(),
            unacknowledged: BTreeMap::new(),
            acknowledged: Vec::new(),
            failed_rejections: 0,
            counted_redeliveries: 0,
            suspensions: 0,
        }))
    }

    /// Loses the current connection. Its unsettled deliveries wait for the next one.
    pub(super) fn lose_connection(&mut self) {
        self.alive = false;
    }

    fn serves(&self, connection: u64) -> bool {
        self.alive && self.connection == connection
    }

    /// Settles `positions` on the current connection. A broker can settle only what that
    /// connection delivered: naming a delivery of a lost connection is the defect under check.
    fn settle(&mut self, operation: &str, positions: &[u64]) {
        for position in positions {
            let delivered_on = self.unacknowledged.remove(position);
            assert_eq!(
                delivered_on,
                Some(self.connection),
                "the host asked connection {} to {operation} position {position}, which it did \
                 not deliver",
                self.connection,
            );
        }
    }

    /// Opens the next connection, on which the broker redelivers everything the lost one did not
    /// settle, in delivery order.
    fn reconnect(&mut self) {
        self.connection = self
            .connection
            .checked_add(1)
            .verified("a check loses its connection at most twice");
        self.alive = true;
        let redelivered = std::mem::take(&mut self.unacknowledged);
        for position in redelivered.into_keys().rev() {
            self.queue.push_front(position);
        }
    }
}

pub(super) struct BrokerDelivery {
    position: u64,
    payload: Vec<u8>,
}

impl IngestMessageHeaders for BrokerDelivery {
    fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
}

impl IngestorMessage for BrokerDelivery {
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

/// A ingestor reading through one connection of a [`RedeliveringBroker`] at a time.
pub(super) struct RedeliveringIngestor {
    broker: Arc<Mutex<RedeliveringBroker>>,
    connection: u64,
}

impl RedeliveringIngestor {
    pub(super) fn new(broker: Arc<Mutex<RedeliveringBroker>>) -> Self {
        Self {
            broker,
            connection: 0,
        }
    }
}

#[async_trait]
impl IngestorConnector for RedeliveringIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = ();

    async fn open(_plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        Err(Report::new(IngestorError::Open {
            connector: "redelivering",
        }))
    }

    fn needs_resume(&mut self) -> bool {
        !self.broker.lock().serves(self.connection)
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        let mut broker = self.broker.lock();
        broker.suspensions = broker
            .suspensions
            .checked_add(1)
            .verified("a check suspends its source only a few times");
        Ok(())
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        let mut broker = self.broker.lock();
        if !broker.alive {
            broker.reconnect();
        }
        self.connection = broker.connection;
        Ok(IngestorResume::Ready)
    }
}

#[async_trait]
impl BrokerIngestorConnector for RedeliveringIngestor {
    type Message = BrokerDelivery;
    type Position = u64;

    async fn next_batch(
        &mut self,
        _request: IngestorBatchRequest,
        _cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let mut broker = self.broker.lock();
        if !broker.serves(self.connection) {
            return Ok(IngestorBatch::ResumeRequired);
        }
        let Some(position) = broker.queue.pop_front() else {
            return Ok(IngestorBatch::Closed);
        };
        let connection = broker.connection;
        broker.unacknowledged.insert(position, connection);
        Ok(IngestorBatch::Messages(vec![BrokerDelivery {
            position,
            payload: vec![u8::try_from(position).assured("the checks use positions one and two")],
        }]))
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let mut broker = self.broker.lock();
        if !broker.serves(self.connection) {
            return Err(Report::new(IngestorError::Acknowledge {
                connector: "redelivering",
            }));
        }
        broker.settle("acknowledge", positions);
        broker.acknowledged.extend_from_slice(positions);
        Ok(())
    }

    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let mut broker = self.broker.lock();
        if !broker.serves(self.connection) {
            broker.failed_rejections = broker
                .failed_rejections
                .checked_add(1)
                .verified("a check rejects only a few times");
            return Err(Report::new(IngestorError::Reject {
                connector: "redelivering",
            }));
        }
        broker.settle("reject", positions);
        for position in positions.iter().rev() {
            broker.queue.push_front(*position);
        }
        Ok(())
    }
}

struct ConfirmedAcknowledgement;

#[async_trait]
impl IngestorBatchAcknowledgementServices for ConfirmedAcknowledgement {
    async fn wait(&mut self, _timeout: Duration) -> IngestorAcknowledgementOutcome {
        IngestorAcknowledgementOutcome::Ack
    }
}

/// A host whose intake confirms every message's acknowledgement tree at once, and which records
/// the redeliveries the loop counts on its broker.
pub(super) struct RedeliveryHost {
    broker: Arc<Mutex<RedeliveringBroker>>,
    /// Whether the connection is lost while the first message is taken in, before the loop
    /// acknowledges it.
    lose_during_first_intake: bool,
}

impl RedeliveryHost {
    pub(super) fn new(
        broker: Arc<Mutex<RedeliveringBroker>>,
        lose_during_first_intake: bool,
    ) -> Self {
        Self {
            broker,
            lose_during_first_intake,
        }
    }
}

#[async_trait]
impl IngestorHostServices for RedeliveryHost {
    async fn intake_unacknowledged(
        &mut self,
        _messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<()> {
        panic!("a connection-bound source reads in an acknowledged mode");
    }

    async fn intake_acknowledged(
        &mut self,
        _messages: Vec<IngestorIntakeMessage<'_>>,
    ) -> IngestorIntakeResult<IngestorBatchAcknowledgement> {
        if self.lose_during_first_intake {
            self.lose_during_first_intake = false;
            self.broker.lock().lose_connection();
        }
        Ok(IngestorBatchAcknowledgement::new(ConfirmedAcknowledgement))
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
        future::pending::<()>().await;
    }

    async fn wait_until_not_suspended(&mut self) {
        future::pending::<()>().await;
    }

    async fn wait_until_active(&mut self) -> bool {
        true
    }

    fn mark_ready(&self) {}
    fn mark_unready(&self) {}
    fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}
    fn clear_transient_error(&self) {}
    // A lost connection fails acknowledgements and rejections, which the loop reports.
    fn report_error(&self, _message: String) {}
    fn handle_ack_failure(&self, reason: String) {
        panic!("every acknowledgement tree of the check succeeds, but one failed: {reason}");
    }
    fn record_transport_redeliveries(&self, positions: usize) {
        let mut broker = self.broker.lock();
        broker.counted_redeliveries = broker
            .counted_redeliveries
            .checked_add(positions)
            .verified("a check counts only a few redeliveries");
    }
    fn record_oversized_payloads(&self, payloads: NonZeroUsize) {
        panic!("a check broker whose payloads all fit dropped {payloads} for their size");
    }
    fn record_unconfirmed_stop(&self, positions: usize) {
        panic!(
            "every acknowledgement tree of the check resolves, but {positions} positions did not"
        );
    }
}

/// The broker loop policy of a connection-bound ingestor of `position_class`, which acknowledges one
/// message at a time.
pub(super) fn sequential_policy(position_class: PositionClass) -> BrokerIngestorPolicy {
    let retry = ParsedRetryPolicy {
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
    };
    BrokerIngestorPolicy::new(
        IngestorAckPolicy::Sequential {
            timeout: Duration::from_secs(1),
            retry,
        },
        position_class,
    )
}

/// Runs the broker loop over two messages until its ingestor closes, losing the connection while the
/// first message is taken in, and returns the broker's records.
async fn lose_the_connection_of_the_first_message(
    position_class: PositionClass,
) -> Arc<Mutex<RedeliveringBroker>> {
    let broker = RedeliveringBroker::shared([1, 2]);
    let (shutdown, shutdown_rx) = watch::channel(false);
    run_ingestor_instance_with_retry(
        RedeliveringIngestor::new(broker.clone()),
        IngestorHost::new(RedeliveryHost::new(broker.clone(), true)),
        sequential_policy(position_class),
        shutdown_rx,
    )
    .await;
    drop(shutdown);
    broker
}

#[nervix_primitives::test]
async fn a_reconnect_discards_a_rejection_the_broker_redelivers() {
    let broker = lose_the_connection_of_the_first_message(PositionClass::BrokerRedelivered).await;

    let broker = broker.lock();
    // The lost connection failed the first message's acknowledgement and then its rejection. The
    // host never asks the next connection to reject it again: it counts the position as
    // redelivered, and the broker presents it again on the new connection, where it is
    // acknowledged once.
    assert_eq!(broker.acknowledged, vec![1, 2]);
    assert_eq!(broker.failed_rejections, 1);
    assert_eq!(broker.counted_redeliveries, 1);
    assert_eq!(broker.suspensions, 1);
}

#[nervix_primitives::test]
async fn a_source_without_positions_keeps_no_failed_rejection() {
    let broker = lose_the_connection_of_the_first_message(PositionClass::None).await;

    let broker = broker.lock();
    // Nothing is pending after the failed rejection, so the ingestor is neither suspended nor asked
    // to reject again, and nothing is counted as redelivered.
    assert_eq!(broker.acknowledged, vec![1, 2]);
    assert_eq!(broker.failed_rejections, 1);
    assert_eq!(broker.counted_redeliveries, 0);
    assert_eq!(broker.suspensions, 0);
}

/// A transport that upgrades successfully but fails reads until it delivers a real batch.
struct ClosingIngestor {
    source: RedeliveringIngestor,
    reads: Arc<Mutex<Vec<Instant>>>,
    steps: VecDeque<ScriptedRead>,
    reconnect: bool,
}

#[derive(Clone, Copy, Debug)]
enum ScriptedRead {
    Failure,
    ResumeRequired,
    Deliver,
}

#[async_trait]
impl IngestorConnector for ClosingIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = ();

    async fn open(_plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        panic!("the test supplies its already constructed source");
    }

    fn needs_resume(&mut self) -> bool {
        self.reconnect
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        self.reconnect = false;
        self.source.resume().await
    }
}

#[async_trait]
impl BrokerIngestorConnector for ClosingIngestor {
    type Message = BrokerDelivery;
    type Position = u64;

    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        self.reads.lock().push(Instant::now());
        match self.steps.pop_front() {
            Some(ScriptedRead::Deliver) => self.source.next_batch(request, cancellation).await,
            Some(ScriptedRead::Failure) => {
                self.reconnect = true;
                Err(Report::new(IngestorError::Read {
                    connector: "closing",
                }))
            }
            Some(ScriptedRead::ResumeRequired) => {
                self.reconnect = true;
                Ok(IngestorBatch::ResumeRequired)
            }
            None => Ok(IngestorBatch::Closed),
        }
    }

    async fn acknowledge(&mut self, positions: &[u64]) -> IngestorResult<()> {
        self.source.acknowledge(positions).await
    }

    async fn reject(&mut self, positions: &[u64]) -> IngestorResult<()> {
        self.source.reject(positions).await
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn successful_upgrades_keep_read_failure_backoff_until_a_batch_is_delivered() {
    for interruption in [ScriptedRead::Failure, ScriptedRead::ResumeRequired] {
        let broker = RedeliveringBroker::shared([1]);
        let reads = Arc::new(Mutex::new(Vec::new()));
        let source = ClosingIngestor {
            source: RedeliveringIngestor::new(broker.clone()),
            reads: reads.clone(),
            steps: VecDeque::from([
                interruption,
                interruption,
                ScriptedRead::Deliver,
                interruption,
                interruption,
            ]),
            reconnect: true,
        };
        let mut policy = sequential_policy(PositionClass::BrokerRedelivered);
        policy.retry = ParsedRetryPolicy {
            backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(4),
        };
        let (shutdown, shutdown_rx) = watch::channel(false);
        run_ingestor_instance_with_retry(
            source,
            IngestorHost::new(RedeliveryHost::new(broker.clone(), false)),
            policy,
            shutdown_rx,
        )
        .await;
        drop(shutdown);
        let reads = reads.lock();
        let delays: Vec<_> = reads
            .windows(2)
            .map(|pair| pair[1].duration_since(pair[0]))
            .collect();
        assert_eq!(
            delays,
            [1, 2, 0, 1, 2].map(Duration::from_secs),
            "interruption: {interruption:?}"
        );
        assert_eq!(broker.lock().acknowledged, [1]);
    }
}

#[test]
fn a_broker_source_without_a_retry_policy_reopens_on_the_reconnect_cadence() {
    let unacknowledged =
        BrokerIngestorPolicy::new(IngestorAckPolicy::None, PositionClass::Replayable);
    assert_eq!(unacknowledged.retry, INGESTOR_RECONNECT_POLICY);

    let declared = ParsedRetryPolicy {
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
    };
    let acknowledged = BrokerIngestorPolicy::new(
        IngestorAckPolicy::Sequential {
            timeout: Duration::from_secs(1),
            retry: declared,
        },
        PositionClass::Replayable,
    );
    assert_eq!(acknowledged.retry, declared);
}
