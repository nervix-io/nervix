//! Timers and the monotonic clock, selected for the build's execution mode: waiting for a duration
//! or until an instant, bounding a future by a deadline, recurring intervals, and the instant all of
//! them are measured in.
//!
//! Ordinary execution, Turmoil and Loom take Tokio's timers. [`Instant::now`] and every timer follow
//! the clock of the runtime that polls them: the operating system's monotonic clock, unless a test
//! paused that runtime's clock, and the simulated clock of the host that runs the caller in a
//! Turmoil build, so a deadline a simulated host arms expires in simulated time. Loom models no
//! timer: a Loom build takes Tokio's, outside every model, and its model code may not name them.
//!
//! Shuttle does not model elapsed time. A Shuttle build takes Shuttle's timers: a sleep and an
//! interval's tick are one scheduling point each and take no time, and a timeout never measures
//! its deadline. It expires only when a check calls `trigger_timeouts` for the task that armed it,
//! which only a Shuttle build has, so a check chooses whether a deadline wins instead of waiting
//! for it. [`Instant::now`] still reads the operating system's clock there.
//!
//! A duration is a value rather than a primitive, so it is the standard library's `Duration` in
//! every mode. `pause`, `advance` and `resume` exist only with the `test-util` capability, which
//! an ordinary test enables when it checks the elapsed-time behavior of a timer on a paused
//! runtime. Reading the wall clock is not part of this family: actual UTC and physical deadlines
//! keep the owners `scripts/check_clock_boundaries.py` declares.

#[cfg(feature = "shuttle")]
pub use shuttle_tokio::time::{
    Instant, Interval, MissedTickBehavior, Sleep, Timeout, clear_triggers, error, interval,
    interval_at, sleep, sleep_until, timeout, timeout_at, trigger_timeouts,
};
#[cfg(all(feature = "shuttle", feature = "test-util"))]
pub use shuttle_tokio::time::{advance, pause, resume};
#[cfg(not(feature = "shuttle"))]
pub use tokio::time::{
    Instant, Interval, MissedTickBehavior, Sleep, Timeout, error, interval, interval_at, sleep,
    sleep_until, timeout, timeout_at,
};
#[cfg(all(not(feature = "shuttle"), feature = "test-util"))]
pub use tokio::time::{advance, pause, resume};
