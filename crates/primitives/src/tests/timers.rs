//! The contract of timers and the monotonic clock in each mode.
//!
//! Ordinary execution checks elapsed-time behavior on a runtime whose clock is paused. Such a clock
//! advances only while every task waits on a timer, and then exactly to the earliest deadline, so
//! each measurement is exact and nothing waits in real time. Shuttle does not model time: its script
//! shows that a sleep and an interval's tick are scheduling points that take no time, and that a
//! timeout expires only when the check triggers it.

use std::{future, time::Duration};

use meticulous::OptionExt as _;

use crate::time::{Instant, interval, sleep, timeout, timeout_at};

const SHORT_OFFSETS: &str = "an offset of a few seconds from the runtime's clock stays in range";

/// Every timer measures the clock of the runtime that polls it. The runtime's clock is paused, so
/// each wait advances it exactly to the wait's deadline.
#[cfg(all(feature = "test-util", not(feature = "shuttle")))]
pub(super) async fn timers_measure_the_runtime_clock() {
    use crate::time::{MissedTickBehavior, advance, sleep_until};

    let start = Instant::now();
    sleep(Duration::from_secs(5)).await;
    assert_eq!(start.elapsed(), Duration::from_secs(5));

    let expired = timeout(Duration::from_secs(1), future::pending::<()>()).await;
    assert!(expired.is_err());
    assert_eq!(start.elapsed(), Duration::from_secs(6));

    let completed = timeout(Duration::from_secs(1), future::ready(3_u8)).await;
    assert_eq!(completed, Ok(3));
    assert_eq!(start.elapsed(), Duration::from_secs(6));

    let wake_at = start
        .checked_add(Duration::from_secs(10))
        .assured(SHORT_OFFSETS);
    sleep_until(wake_at).await;
    assert_eq!(Instant::now(), wake_at);

    let deadline = start
        .checked_add(Duration::from_secs(12))
        .assured(SHORT_OFFSETS);
    let at_deadline = timeout_at(deadline, future::pending::<()>()).await;
    assert!(at_deadline.is_err());
    assert_eq!(Instant::now(), deadline);

    let mut ticks = interval(Duration::from_secs(2));
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let first = ticks.tick().await;
    let second = ticks.tick().await;
    assert_eq!(second.duration_since(first), Duration::from_secs(2));

    let before_advance = Instant::now();
    advance(Duration::from_secs(30)).await;
    assert_eq!(before_advance.elapsed(), Duration::from_secs(30));
}

/// A sleep and an interval's tick are scheduling points that take no time, and a timeout never
/// measures its deadline: its future completes whatever the deadline, and only a trigger the check
/// sets expires it.
#[cfg(feature = "shuttle")]
pub(super) async fn timers_are_scheduling_points_and_the_check_decides_every_timeout() {
    use shuttle::current::context_switches;

    use crate::time::{clear_triggers, trigger_timeouts};

    let before_sleep = context_switches();
    sleep(Duration::from_secs(3_600)).await;
    assert!(context_switches() > before_sleep);

    let mut ticks = interval(Duration::from_secs(3_600));
    let before_tick = context_switches();
    ticks.tick().await;
    assert!(context_switches() > before_tick);

    let already_passed = timeout(Duration::ZERO, future::ready(3_u8)).await;
    assert_eq!(already_passed, Ok(3));
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(1))
        .assured(SHORT_OFFSETS);
    let before_deadline = timeout_at(deadline, future::ready(4_u8)).await;
    assert_eq!(before_deadline, Ok(4));

    trigger_timeouts(|_| true);
    let triggered = timeout(Duration::from_secs(3_600), future::pending::<()>()).await;
    assert!(triggered.is_err());
    clear_triggers();
}
