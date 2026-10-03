//! A producer's submission slots under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants a submission's outcome and credit are held to while its resolution
//!   races the application's wait, a cancelled wait, and a release.
//! - **Depends on.** The submission slots and the client's Shuttle runner.
//! - **Must not know.** Exchanges, frames, or how an outcome was decided.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_primitives::sync::Arc;

use super::{Credit, ProducerOutcome, SubmissionId, SubmissionSlots};

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

/// Slots granted one batch and one kilobyte, holding one submission and the credit it took.
struct HeldSubmission {
    slots: Arc<SubmissionSlots>,
    id: SubmissionId,
    credit: Credit,
}

async fn one_held_submission() -> HeldSubmission {
    let slots = Arc::new(SubmissionSlots::new(1, 1_024));
    let credit = slots
        .credit(16)
        .await
        .assured("open slots grant their one batch");
    let id = slots.hold();
    HeldSubmission { slots, id, credit }
}

fn a_wait_racing_the_resolution_takes_the_outcome_once() {
    shuttle::future::block_on(async {
        let HeldSubmission { slots, id, credit } = one_held_submission().await;
        let waiter = nervix_primitives::task::spawn({
            let slots = slots.clone();
            async move { slots.rejoin(id).await }
        });
        let resolver = nervix_primitives::task::spawn({
            let slots = slots.clone();
            async move { slots.resolve(id, ProducerOutcome::Completed, credit) }
        });
        resolver.await.assured(CHECK_TASK_JOINS);
        let taken = waiter
            .await
            .assured(CHECK_TASK_JOINS)
            .assured("a held submission's outcome reaches its wait");
        assert_eq!(taken, ProducerOutcome::Completed);
        assert!(
            slots.pending().is_empty(),
            "a taken outcome leaves nothing held"
        );
        assert_eq!(
            slots.available_batches(),
            1,
            "taking the outcome returned the batch's credit"
        );
    });
}

/// A wait registered before the resolution it races is woken by it, and the outcome is taken once.
#[test]
fn shuttle_a_wait_racing_its_resolution_takes_the_outcome_once_and_returns_the_credit() {
    check_random_and_pct(a_wait_racing_the_resolution_takes_the_outcome_once);
}

fn a_cancelled_wait_loses_neither_the_outcome_nor_the_credit() {
    shuttle::future::block_on(async {
        let HeldSubmission { slots, id, credit } = one_held_submission().await;
        let cancelled = nervix_primitives::task::spawn({
            let slots = slots.clone();
            async move { slots.rejoin(id).await }
        });
        let resolver = nervix_primitives::task::spawn({
            let slots = slots.clone();
            async move { slots.resolve(id, ProducerOutcome::Completed, credit) }
        });
        cancelled.abort();
        resolver.await.assured(CHECK_TASK_JOINS);
        // The cancelled wait may have taken the outcome before the cancellation reached it; if it
        // did not, the submission still holds its outcome and a new wait takes it.
        let first = match cancelled.await {
            Ok(taken) => Some(taken.assured("a held submission's outcome reaches its wait")),
            Err(_) => None,
        };
        match first {
            Some(taken) => {
                assert_eq!(taken, ProducerOutcome::Completed);
                assert!(
                    slots.rejoin(id).await.is_err(),
                    "an outcome is taken only once"
                );
            }
            None => {
                let pending = slots.pending();
                assert_eq!(
                    pending.len(),
                    1,
                    "a cancelled wait leaves its submission held"
                );
                let retaken = slots
                    .rejoin(id)
                    .await
                    .assured("a submission a cancelled wait left behind is rejoined");
                assert_eq!(retaken, ProducerOutcome::Completed);
            }
        }
        assert_eq!(
            slots.available_batches(),
            1,
            "the credit comes back exactly once, when the outcome is taken"
        );
    });
}

/// Cancelling a wait neither loses a submission's outcome nor keeps or doubles its credit.
#[test]
fn shuttle_a_cancelled_wait_loses_neither_the_outcome_nor_the_credit() {
    check_random_and_pct(a_cancelled_wait_loses_neither_the_outcome_nor_the_credit);
}

fn a_release_racing_the_resolution_returns_the_credit_once() {
    shuttle::future::block_on(async {
        let HeldSubmission { slots, id, credit } = one_held_submission().await;
        let releaser = nervix_primitives::task::spawn({
            let slots = slots.clone();
            async move { slots.release(id) }
        });
        let resolver = nervix_primitives::task::spawn({
            let slots = slots.clone();
            async move { slots.resolve(id, ProducerOutcome::Completed, credit) }
        });
        let released = releaser
            .await
            .assured(CHECK_TASK_JOINS)
            .assured("a held submission can be released");
        resolver.await.assured(CHECK_TASK_JOINS);
        if let Some(outcome) = released {
            assert_eq!(outcome, ProducerOutcome::Completed);
        }
        assert!(
            slots.pending().is_empty(),
            "a released submission is no longer held"
        );
        assert!(
            slots.release(id).is_err(),
            "a released submission cannot be released again"
        );
        assert_eq!(
            slots.available_batches(),
            1,
            "the released submission's credit came back once"
        );
    });
}

/// Releasing a submission while its outcome arrives returns its credit exactly once, with the
/// outcome when it had one.
#[test]
fn shuttle_a_release_racing_its_resolution_returns_the_credit_exactly_once() {
    check_random_and_pct(a_release_racing_the_resolution_returns_the_credit_once);
}
