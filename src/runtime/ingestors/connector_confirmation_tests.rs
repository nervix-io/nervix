//! What a broker ingestor's host reports while the ingestor's transport confirms its positions.
//!
//! Layer: test harness.
//! - **Owns.** A ingestor double whose acknowledgement a test fails or watches, a host double whose
//!   acknowledgement trees are real roots on detached trackers, and the unit tests of the broker
//!   loop's confirmed progress.
//! - **Depends on.** The production broker loop, the runtime's batch acknowledgement and the
//!   confirmed ACK roots it waits on.
//! - **Must not know.** Broker drivers, the quiesce control or the runtime ingestor host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{collections::VecDeque, future};

use nervix_connector::{
    IngestMessageHeaders, IngestMetadataRow, IngestorReadCancellation, IngestorResult,
};
use nervix_primitives::sync::blocking::Mutex;

use super::{reconnect_tests::sequential_policy, *};

/// One status change or transport operation of a check, in the order the loop made it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ConfirmationEvent {
    /// The host recorded a transient status.
    Transient,
    /// The host cleared its transient status.
    Cleared,
    /// The transport confirmed these positions.
    Acknowledged(Vec<u64>),
    /// The transport took these positions back to present them again.
    Rejected(Vec<u64>),
    /// The ingestor closed.
    Closed,
    /// The host reported positions the ingestor stopped without confirming.
    Unconfirmed(usize),
}

/// The roots a drain still waited for while one acknowledgement ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OutstandingRoots {
    domain: usize,
    ingestor: usize,
}

/// What the ingestor and the host of one check did.
#[derive(Default)]
struct ConfirmationRecord {
    events: Vec<ConfirmationEvent>,
    /// The roots outstanding as each acknowledgement began.
    outstanding_while_acknowledging: Vec<OutstandingRoots>,
}

struct ConfirmingDelivery {
    position: u64,
    payload: Vec<u8>,
}

impl IngestMessageHeaders for ConfirmingDelivery {
    fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
}

impl IngestorMessage for ConfirmingDelivery {
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

/// A ingestor whose queue the loop reads one message at a time, and whose transport fails a set
/// number of acknowledgements and rejections before it confirms or takes back the rest.
struct ConfirmingIngestor {
    queue: VecDeque<u64>,
    /// Acknowledgements the transport fails before it confirms one.
    failing_acknowledgements: usize,
    /// Rejections the transport fails before it takes positions back.
    failing_rejections: usize,
    trackers: IngestorAckRootTrackers,
    record: Arc<Mutex<ConfirmationRecord>>,
}

impl ConfirmingIngestor {
    fn new(
        positions: impl IntoIterator<Item = u64>,
        trackers: IngestorAckRootTrackers,
        record: Arc<Mutex<ConfirmationRecord>>,
    ) -> Self {
        Self {
            queue: positions.into_iter().collect(),
            failing_acknowledgements: 0,
            failing_rejections: 0,
            trackers,
            record,
        }
    }
}

#[async_trait]
impl IngestorConnector for ConfirmingIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = ();

    async fn open(_plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        Err(Report::new(IngestorError::Open {
            connector: "confirming",
        }))
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.record.lock().events.push(ConfirmationEvent::Closed);
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for ConfirmingIngestor {
    type Message = ConfirmingDelivery;
    type Position = u64;

    async fn next_batch(
        &mut self,
        _request: IngestorBatchRequest,
        _cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let Some(position) = self.queue.pop_front() else {
            return Ok(IngestorBatch::Closed);
        };
        Ok(IngestorBatch::Messages(vec![ConfirmingDelivery {
            position,
            payload: vec![u8::try_from(position).assured("the checks use positions below 256")],
        }]))
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let mut record = self.record.lock();
        record
            .outstanding_while_acknowledging
            .push(OutstandingRoots {
                domain: self.trackers.domain_outstanding_for_ownership_handoff(),
                ingestor: self.trackers.ingestor_outstanding_for_ownership_handoff(),
            });
        if let Some(failing) = self.failing_acknowledgements.checked_sub(1) {
            self.failing_acknowledgements = failing;
            return Err(Report::new(IngestorError::Acknowledge {
                connector: "confirming",
            }));
        }
        record
            .events
            .push(ConfirmationEvent::Acknowledged(positions.to_vec()));
        Ok(())
    }

    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        if let Some(failing) = self.failing_rejections.checked_sub(1) {
            self.failing_rejections = failing;
            return Err(Report::new(IngestorError::Reject {
                connector: "confirming",
            }));
        }
        self.record
            .lock()
            .events
            .push(ConfirmationEvent::Rejected(positions.to_vec()));
        for position in positions.iter().rev() {
            self.queue.push_front(*position);
        }
        Ok(())
    }
}

/// A host whose graph acknowledges every message as soon as the host takes it in, on the
/// runtime's confirmed roots counted against detached trackers, and which records its status
/// changes.
struct ConfirmingHost {
    trackers: IngestorAckRootTrackers,
    shutdown: watch::Receiver<bool>,
    /// Whether the graph acknowledges what the host takes in. Otherwise the host holds the roots
    /// unresolved, as a graph whose emitter has not answered would.
    resolve_trees: bool,
    unresolved: Vec<AckSet>,
    /// Stops the ingestor as its first batch is taken in, after that batch's acknowledgement trees
    /// resolved or were left unresolved.
    stop_after_first_intake: Option<watch::Sender<bool>>,
    record: Arc<Mutex<ConfirmationRecord>>,
}

#[async_trait]
impl IngestorHostServices for ConfirmingHost {
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
        let mut completions = Vec::with_capacity(messages.len());
        for _ in &messages {
            let (root, completion) = self.trackers.confirmed_root();
            if self.resolve_trees {
                root.ack_success();
            } else {
                self.unresolved.push(root);
            }
            completions.push(completion);
        }
        if let Some(stop) = self.stop_after_first_intake.take() {
            stop.send_replace(true);
        }
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
    fn record_transient_error(&self, _reason: String, _retry_after: Duration) {
        self.record.lock().events.push(ConfirmationEvent::Transient);
    }
    fn clear_transient_error(&self) {
        self.record.lock().events.push(ConfirmationEvent::Cleared);
    }
    // A transport that fails an acknowledgement reports it, which the checks read as a status.
    fn report_error(&self, _message: String) {}
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
        self.record
            .lock()
            .events
            .push(ConfirmationEvent::Unconfirmed(positions));
    }
}

/// A check's ingestor and host over the same trackers and record.
struct ConfirmationCheck {
    source: ConfirmingIngestor,
    host: ConfirmingHost,
    trackers: IngestorAckRootTrackers,
    record: Arc<Mutex<ConfirmationRecord>>,
    /// The stop the loop watches, held by the check unless it hands the stop to the host.
    shutdown: Option<watch::Sender<bool>>,
    loop_shutdown: watch::Receiver<bool>,
}

impl ConfirmationCheck {
    fn reading(positions: impl IntoIterator<Item = u64>) -> Self {
        let trackers = IngestorAckRootTrackers::detached();
        let record = Arc::new(Mutex::new(ConfirmationRecord::default()));
        let (shutdown, loop_shutdown) = watch::channel(false);
        Self {
            source: ConfirmingIngestor::new(positions, trackers.clone(), record.clone()),
            host: ConfirmingHost {
                trackers: trackers.clone(),
                shutdown: loop_shutdown.clone(),
                resolve_trees: true,
                unresolved: Vec::new(),
                stop_after_first_intake: None,
                record: record.clone(),
            },
            trackers,
            record,
            shutdown: Some(shutdown),
            loop_shutdown,
        }
    }

    /// Makes the host stop the loop as it takes in the first batch.
    fn stopping_after_first_intake(mut self) -> Self {
        self.host.stop_after_first_intake = self.shutdown.take();
        self
    }

    /// Runs the broker loop until its ingestor closes or the check stops it.
    async fn run(self) -> (IngestorAckRootTrackers, Arc<Mutex<ConfirmationRecord>>) {
        run_ingestor_instance_with_retry(
            self.source,
            IngestorHost::new(self.host),
            sequential_policy(PositionClass::Replayable),
            self.loop_shutdown,
        )
        .await;
        drop(self.shutdown);
        (self.trackers, self.record)
    }
}

/// A transport that failed to confirm an acknowledgement leaves the ingestor with a transient
/// status, and only an acknowledgement the transport confirmed clears it again: reading a batch
/// is not progress until its positions are confirmed.
#[nervix_primitives::test]
async fn a_failed_confirmation_is_a_transient_status_until_a_later_one_is_confirmed() {
    let mut check = ConfirmationCheck::reading([1, 2]);
    check.source.failing_acknowledgements = 1;

    let (_trackers, record) = check.run().await;

    assert_eq!(
        record.lock().events,
        vec![
            ConfirmationEvent::Transient,
            ConfirmationEvent::Rejected(vec![1]),
            ConfirmationEvent::Acknowledged(vec![1]),
            ConfirmationEvent::Cleared,
            ConfirmationEvent::Acknowledged(vec![2]),
            ConfirmationEvent::Cleared,
            ConfirmationEvent::Closed,
        ]
    );
}

/// A transport that could neither confirm a batch nor take it back shows the failed rejection as
/// the ingestor's transient status too, whether it fails as the batch is settled or as the loop
/// retries it, and only a later confirmation clears it.
#[nervix_primitives::test(start_paused = true)]
async fn a_failed_rejection_is_a_transient_status_until_a_later_confirmation() {
    let mut check = ConfirmationCheck::reading([1]);
    check.source.failing_acknowledgements = 1;
    check.source.failing_rejections = 2;

    let (_trackers, record) = check.run().await;

    assert_eq!(
        record.lock().events,
        vec![
            // The confirmation failed, and so did the rejection of the batch it settled.
            ConfirmationEvent::Transient,
            ConfirmationEvent::Transient,
            // The loop's retry of that rejection failed before the ingestor was reestablished.
            ConfirmationEvent::Transient,
            ConfirmationEvent::Rejected(vec![1]),
            ConfirmationEvent::Acknowledged(vec![1]),
            ConfirmationEvent::Cleared,
            ConfirmationEvent::Closed,
        ]
    );
}

/// The roots of a batch whose acknowledgement trees resolved stay outstanding, so a drain or an
/// ownership handoff keeps waiting for them, until the ingestor's transport confirmed the batch.
#[nervix_primitives::test]
async fn a_batch_stays_outstanding_for_a_drain_until_its_transport_confirms_it() {
    let check = ConfirmationCheck::reading([1]);

    let (trackers, record) = check.run().await;

    assert_eq!(
        record.lock().outstanding_while_acknowledging,
        vec![OutstandingRoots {
            domain: 1,
            ingestor: 1
        }],
        "the confirming batch is what the drain waits for"
    );
    assert_eq!(trackers.domain_outstanding_for_ownership_handoff(), 0);
    assert_eq!(trackers.ingestor_outstanding_for_ownership_handoff(), 0);
}

/// A stop that arrives once a batch's acknowledgement trees resolved does not abandon the batch:
/// the ingestor confirms it before the loop closes the ingestor.
#[nervix_primitives::test]
async fn a_stop_after_a_batch_resolved_still_confirms_the_batch_before_closing() {
    // Each run offers the loop a resolved batch and a stop at the same time, so a loop that picks
    // either one is caught within a few runs.
    for _ in 0..32 {
        let check = ConfirmationCheck::reading([1]).stopping_after_first_intake();

        let (trackers, record) = check.run().await;

        assert_eq!(
            record.lock().events,
            vec![
                ConfirmationEvent::Acknowledged(vec![1]),
                ConfirmationEvent::Cleared,
                ConfirmationEvent::Closed,
            ]
        );
        assert_eq!(trackers.domain_outstanding_for_ownership_handoff(), 0);
    }
}

/// A stop that arrives while a batch's acknowledgement trees are still unresolved ends the loop
/// without confirming the batch, and the host reports the positions it leaves to the transport's
/// redelivery.
#[nervix_primitives::test]
async fn a_stop_before_a_batch_resolved_reports_its_positions_unconfirmed() {
    let mut check = ConfirmationCheck::reading([1]).stopping_after_first_intake();
    check.host.resolve_trees = false;

    let (trackers, record) = check.run().await;

    assert_eq!(
        record.lock().events,
        vec![ConfirmationEvent::Unconfirmed(1), ConfirmationEvent::Closed,]
    );
    // The unresolved roots ended with the host, which releases what they counted.
    assert_eq!(trackers.domain_outstanding_for_ownership_handoff(), 0);
}

#[nervix_primitives::test]
async fn individual_settlement_keeps_each_drain_guard_until_its_transport_confirmation() {
    let check = ConfirmationCheck::reading([]);
    let mut source = check.source;
    let mut host = IngestorHost::new(check.host);
    let messages = (1..=3u64)
        .map(|position| ConfirmingDelivery {
            position,
            payload: vec![u8::try_from(position).assured("the check uses positions below 256")],
        })
        .collect();
    let batch = IndividualBatch::intake(&mut host, messages, Duration::from_secs(1)).await;
    let result = batch
        .settle(
            &mut source,
            &mut host,
            BatchSettlement {
                position_class: PositionClass::BrokerRedelivered,
                retry_after: Duration::from_millis(1),
            },
        )
        .await;
    assert!(matches!(result, BatchDisposition::Accepted));
    assert_eq!(
        check.record.lock().outstanding_while_acknowledging,
        (1..=3)
            .rev()
            .map(|count| OutstandingRoots {
                domain: count,
                ingestor: count
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(check.trackers.domain_outstanding_for_ownership_handoff(), 0);
    assert_eq!(
        check.trackers.ingestor_outstanding_for_ownership_handoff(),
        0
    );
    drop(check.shutdown);
}
