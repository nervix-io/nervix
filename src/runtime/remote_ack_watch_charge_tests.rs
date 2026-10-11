//! The hold of a delivery for its acknowledgement watches' charge, against a real relay budget.
//!
//! Layer: test harness.
//! - **Owns.** Taking a charge that fits, holding one that does not fit until room returns or the
//!   delivery is cancelled, refusing one the budget can never hold or one still without room at
//!   the admission bound, and returning each ended watch's share.
//! - **Depends on.** The production charge owner and the default execution budgets.
//! - **Must not know.** The interconnect, relays or the rows a watch reports.

use futures_util::FutureExt as _;
use nervix_execution::MemoryBudgetSnapshot;

use super::*;

fn relay_memory(executor: &Executor) -> MemoryBudgetSnapshot {
    executor.memory(MemoryClass::Relay)
}

fn watches(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).assured("the fixture holds at least one watch")
}

#[nervix_primitives::test]
async fn a_charge_the_relay_budget_has_room_for_is_taken_at_once() {
    let executor = Executor::default();
    let hold = RemoteAckWatchCharge::hold(&executor, watches(3), std::future::pending()).await;

    let Ok(RemoteAckWatchHold::Charged(charge)) = hold else {
        panic!("an empty relay budget takes three watches' charge at once: {hold:?}");
    };
    assert_eq!(charge.bytes_held(), 3 * WATCH_BYTES + TASK_BYTES);
    assert_eq!(relay_memory(&executor).reserved_bytes, charge.bytes_held());
    assert_eq!(relay_memory(&executor).refused, 0);
}

/// A delivery that finds the relay budget full is held, not refused: the budget counts no refusal
/// while it waits, and the hold takes its charge once the memory that filled the budget returns.
#[nervix_primitives::test(start_paused = true)]
async fn a_held_delivery_takes_its_charge_once_room_returns() {
    let executor = Executor::default();
    let capacity = relay_memory(&executor).capacity_bytes;
    let occupied = executor
        .try_reserve(MemoryClass::Relay, capacity)
        .assured("an empty relay budget takes its whole capacity");
    let hold = RemoteAckWatchCharge::hold(&executor, watches(2), std::future::pending());
    let mut hold = std::pin::pin!(hold);

    assert!(hold.as_mut().now_or_never().is_none());
    nervix_primitives::time::advance(REMOTE_RELAY_TOTAL_TIMEOUT / 2).await;
    assert!(
        hold.as_mut().now_or_never().is_none(),
        "a full budget holds the delivery back"
    );
    assert_eq!(relay_memory(&executor).refused, 0);

    drop(occupied);
    nervix_primitives::time::advance(RECHECK_INTERVAL).await;
    let Some(Ok(RemoteAckWatchHold::Charged(charge))) = hold.as_mut().now_or_never() else {
        panic!("the hold takes its charge at its first recheck after room returns");
    };
    assert_eq!(charge.bytes_held(), 2 * WATCH_BYTES + TASK_BYTES);
    assert_eq!(relay_memory(&executor).reserved_bytes, charge.bytes_held());
    assert_eq!(relay_memory(&executor).refused, 0);
}

#[nervix_primitives::test]
async fn watches_the_whole_relay_budget_could_never_hold_are_refused_at_once() {
    let executor = Executor::default();
    let capacity = relay_memory(&executor).capacity_bytes;
    let too_many = usize::try_from(capacity / WATCH_BYTES + 1)
        .assured("the default relay budget's watch count fits in usize");

    let hold =
        RemoteAckWatchCharge::hold(&executor, watches(too_many), std::future::pending()).await;

    let Err(refusal) = hold else {
        panic!("a charge larger than the whole budget is refused: {hold:?}");
    };
    assert!(matches!(
        refusal.current_context(),
        AdmissionError::ExceedsBudget { .. }
    ));
    assert_eq!(relay_memory(&executor).reserved_bytes, 0);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_cancelled_delivery_ends_its_hold_without_a_charge() {
    let executor = Executor::default();
    let capacity = relay_memory(&executor).capacity_bytes;
    let occupied = executor
        .try_reserve(MemoryClass::Relay, capacity)
        .assured("an empty relay budget takes its whole capacity");
    let (cancel, cancelled) = oneshot::channel::<()>();
    let hold = RemoteAckWatchCharge::hold(&executor, watches(1), async {
        cancelled
            .await
            .discarded("a dropped sender cancels the delivery as surely as a sent value");
    });
    let mut hold = std::pin::pin!(hold);

    assert!(hold.as_mut().now_or_never().is_none());
    drop(cancel);
    let Some(Ok(RemoteAckWatchHold::Cancelled)) = hold.as_mut().now_or_never() else {
        panic!("a cancelled delivery ends its hold at once");
    };
    drop(occupied);
    assert_eq!(relay_memory(&executor).reserved_bytes, 0);
    assert_eq!(relay_memory(&executor).refused, 0);
}

/// A hold lasts no longer than its sender waits for the admission, and its refusal keeps the
/// budget's typed cause.
#[nervix_primitives::test(start_paused = true)]
async fn a_hold_still_without_room_at_the_admission_bound_is_refused() {
    let executor = Executor::default();
    let capacity = relay_memory(&executor).capacity_bytes;
    let occupied = executor
        .try_reserve(MemoryClass::Relay, capacity)
        .assured("an empty relay budget takes its whole capacity");
    let hold = RemoteAckWatchCharge::hold(&executor, watches(1), std::future::pending());
    let mut hold = std::pin::pin!(hold);

    assert!(hold.as_mut().now_or_never().is_none());
    nervix_primitives::time::advance(REMOTE_RELAY_TOTAL_TIMEOUT).await;
    let Some(Err(refusal)) = hold.as_mut().now_or_never() else {
        panic!("a hold still without room at the admission bound is refused");
    };
    assert!(matches!(
        refusal.current_context(),
        AdmissionError::BudgetExhausted { .. }
    ));
    assert_eq!(relay_memory(&executor).refused, 1);
    drop(occupied);
}

#[nervix_primitives::test]
async fn each_ended_watch_returns_its_share_and_the_task_share_returns_with_the_charge() {
    let executor = Executor::default();
    let hold = RemoteAckWatchCharge::hold(&executor, watches(3), std::future::pending()).await;
    let Ok(RemoteAckWatchHold::Charged(mut charge)) = hold else {
        panic!("an empty relay budget takes three watches' charge at once: {hold:?}");
    };

    for remaining in (0..3).rev() {
        charge = charge.release_watch();
        assert_eq!(charge.bytes_held(), remaining * WATCH_BYTES + TASK_BYTES);
        assert_eq!(relay_memory(&executor).reserved_bytes, charge.bytes_held());
    }
    drop(charge);
    assert_eq!(relay_memory(&executor).reserved_bytes, 0);
}
