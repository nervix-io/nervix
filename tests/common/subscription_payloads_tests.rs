//! Regressions for bounded payload observations while their scenario caller is delayed.
//!
//! Layer: test harness.
//! - **Owns.** Controlled subscription rows, caller stalls and deadline discriminators.
//! - **Depends on.** The payload observation owner, channels and a paused monotonic clock.
//! - **Must not know.** Scenario steps or the product's buffering implementation.

use std::{io, time::Duration};

use futures_util::{FutureExt as _, poll};
use meticulous::ResultExt as _;
use nervix_primitives::{
    sync::mpsc,
    time::{Instant, advance, sleep, timeout},
};

use crate::subscription_payloads::{PayloadSubscription, SubscriptionPayloads};

struct ScriptedSubscription {
    rows: mpsc::Receiver<String>,
    reads: mpsc::Sender<String>,
}

impl PayloadSubscription for ScriptedSubscription {
    async fn next_payload(&mut self, budget: Duration) -> io::Result<Option<String>> {
        let payload = match timeout(budget, self.rows.recv()).await {
            Ok(Some(payload)) => payload,
            Ok(None) => return Err(io::Error::other("subscription disconnected")),
            Err(_) => return Ok(None),
        };
        self.reads
            .try_send(payload.clone())
            .assured("the test keeps space for every scripted receipt observation");
        Ok(Some(payload))
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn immediate_rows_are_received_before_the_delayed_caller_first_polls() {
    let expected = ["alpha:1", "beta:1"].map(String::from);
    let (send, rows) = mpsc::channel(2);
    let (reads, mut read) = mpsc::channel(2);
    let began_at = Instant::now();
    let observation = SubscriptionPayloads::new(&expected).observe(
        ScriptedSubscription { rows, reads },
        Duration::from_millis(500),
    );

    advance(Duration::from_millis(100)).await;
    for payload in &expected {
        nervix_primitives::task::consume_budget().await;
        send.send(payload.clone())
            .await
            .assured("the independent observer owns its receiver");
    }
    let drained = timeout(Duration::from_millis(400), async {
        for expected in &expected {
            nervix_primitives::task::consume_budget().await;
            assert_eq!(read.recv().await.as_ref(), Some(expected));
        }
    })
    .await;

    advance(Duration::from_secs(1)).await;
    let observed = observation.await;
    assert!(
        drained.is_ok(),
        "the reader must receive both rows before its caller first polls the result"
    );
    assert_eq!(observed.receipts.len(), expected.len());
    for (receipt, expected) in observed.receipts.iter().zip(expected) {
        assert_eq!(receipt.payload, expected);
        assert!(receipt.received_at.duration_since(began_at) < Duration::from_millis(500));
    }
    drop(observed.subscription);
}

#[nervix_primitives::test(start_paused = true)]
async fn timely_second_sequences_survive_a_caller_stalled_after_the_first_row() {
    let expected = ["alpha:1", "beta:1", "alpha:2", "beta:2"].map(String::from);
    let (send, rows) = mpsc::channel(4);
    let (reads, mut read) = mpsc::channel(4);
    let subscription = ScriptedSubscription { rows, reads };
    let began_at = Instant::now();
    send.send(expected[0].clone())
        .await
        .assured("the observer owns the receiver");
    let observation =
        SubscriptionPayloads::new(&expected).observe(subscription, Duration::from_millis(800));
    let mut observation = std::pin::pin!(observation);
    assert!(poll!(observation.as_mut()).is_pending());
    assert_eq!(read.recv().await, Some(expected[0].clone()));

    advance(Duration::from_millis(100)).await;
    for payload in &expected[1..] {
        nervix_primitives::task::consume_budget().await;
        send.send(payload.clone())
            .await
            .assured("the observer keeps its receiver");
    }
    // The caller stops polling its observation while the runtime can still poll the reader.
    // Receipt signals make the runtime's progress explicit before advancing past the deadline.
    let drained = timeout(Duration::from_millis(700), async {
        for expected in &expected[1..] {
            nervix_primitives::task::consume_budget().await;
            assert_eq!(read.recv().await.as_ref(), Some(expected));
        }
    })
    .await;
    advance(Duration::from_secs(1)).await;
    let observed = observation.await;
    assert!(
        drained.is_ok(),
        "the reader must drain rows while its caller is unavailable"
    );
    assert_eq!(observed.receipts.len(), expected.len());
    for (receipt, expected) in observed.receipts.iter().zip(expected) {
        assert_eq!(receipt.payload, expected);
        assert!(receipt.received_at.duration_since(began_at) < Duration::from_millis(800));
    }
    drop(observed.subscription);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_duplicate_first_row_cannot_replace_a_missing_second_sequence() {
    let expected = ["alpha:1", "beta:1", "alpha:2", "beta:2"].map(String::from);
    let (send, rows) = mpsc::channel(4);
    let (reads, _read) = mpsc::channel(4);
    let subscription = ScriptedSubscription { rows, reads };
    for payload in [&expected[0], &expected[0], &expected[1], &expected[2]] {
        nervix_primitives::task::consume_budget().await;
        send.send(payload.clone())
            .await
            .assured("the observer owns the receiver");
    }
    let began_at = Instant::now();
    let observation =
        SubscriptionPayloads::new(&expected).observe(subscription, Duration::from_millis(800));
    let result = std::panic::AssertUnwindSafe(observation)
        .catch_unwind()
        .await;
    let Err(failure) = result else {
        panic!("a missing expected row must fail the sample");
    };
    let Some(message) = failure.downcast_ref::<String>() else {
        panic!("a missing row must report the receive timeout");
    };
    assert!(message.contains("timed out waiting for subscription payloads"));
    assert!(message.contains("beta:2"));
    assert_eq!(began_at.elapsed(), Duration::from_millis(800));
    drop(send);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_disconnected_subscription_reports_its_cause_without_waiting_for_the_budget() {
    let (send, rows) = mpsc::channel(1);
    let (reads, _read) = mpsc::channel(1);
    drop(send);
    let began_at = Instant::now();
    let observation = SubscriptionPayloads::new(&["alpha:2".to_string()]).observe(
        ScriptedSubscription { rows, reads },
        Duration::from_millis(800),
    );
    let result = std::panic::AssertUnwindSafe(observation)
        .catch_unwind()
        .await;
    let Err(failure) = result else {
        panic!("a disconnected subscription must fail its sample");
    };
    let Some(message) = failure.downcast_ref::<String>() else {
        panic!("a disconnected subscription must report its transport cause");
    };
    assert!(message.contains("failed while waiting for subscription payloads"));
    assert!(message.contains("subscription disconnected"));
    assert_eq!(began_at.elapsed(), Duration::ZERO);
}

struct PhysicalCadenceSubscription;

impl PayloadSubscription for PhysicalCadenceSubscription {
    async fn next_payload(&mut self, budget: Duration) -> io::Result<Option<String>> {
        match timeout(budget, sleep(Duration::from_secs(1))).await {
            Ok(()) => Ok(Some("alpha:2".to_string())),
            Err(_) => Ok(None),
        }
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_one_second_physical_cadence_cannot_pass_either_sample_budget() {
    for budget in [Duration::from_millis(500), Duration::from_millis(800)] {
        nervix_primitives::task::consume_budget().await;
        let began_at = Instant::now();
        let observation = SubscriptionPayloads::new(&["alpha:2".to_string()])
            .observe(PhysicalCadenceSubscription, budget);
        let result = std::panic::AssertUnwindSafe(observation)
            .catch_unwind()
            .await;
        let Err(failure) = result else {
            panic!("the wrong clock must fail the same sample");
        };
        let Some(message) = failure.downcast_ref::<String>() else {
            panic!("the physical cadence must report the receive timeout");
        };
        assert!(message.contains("timed out waiting for subscription payloads"));
        assert!(message.contains("alpha:2"));
        assert_eq!(began_at.elapsed(), budget);
    }
}

struct LateReceiptSubscription(Duration);

impl PayloadSubscription for LateReceiptSubscription {
    async fn next_payload(&mut self, _budget: Duration) -> io::Result<Option<String>> {
        // A ready read may win over an expired timeout when its task resumes late.
        advance(self.0).await;
        Ok(Some("alpha:2".to_string()))
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_final_row_at_or_after_the_deadline_cannot_complete_the_sample() {
    for budget in [Duration::from_millis(500), Duration::from_millis(800)] {
        for lateness in [Duration::ZERO, Duration::from_nanos(1)] {
            nervix_primitives::task::consume_budget().await;
            let read_elapsed = budget + lateness;
            let began_at = Instant::now();
            let observation = SubscriptionPayloads::new(&["alpha:2".to_string()])
                .observe(LateReceiptSubscription(read_elapsed), budget);
            let result = std::panic::AssertUnwindSafe(observation)
                .catch_unwind()
                .await;
            let Err(failure) = result else {
                panic!("a final row recorded at or after the deadline must fail the sample");
            };
            let Some(message) = failure.downcast_ref::<String>() else {
                panic!("a late final row must report the receive timeout");
            };
            assert!(message.contains("timed out waiting for subscription payloads"));
            assert!(message.contains("alpha:2"));
            assert_eq!(began_at.elapsed(), read_elapsed);
        }
    }
}
