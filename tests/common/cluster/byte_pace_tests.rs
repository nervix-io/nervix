//! Focused byte cadence regressions for the paced interconnect fixture.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** Deterministic assertions about a late timer, an idle read and sustained byte rate.
//! - **Depends on.** The fixture's byte schedule and the monotonic test clock.
//! - **Must not know.** TLS, HTTP/2, Raft or product progress deadlines.

use std::time::Duration;

use nervix_primitives::time::Instant;

use crate::byte_pace::BytePace;

#[test]
fn pacing_absorbs_a_late_timer_without_charging_the_same_delay_again() {
    let mut pace = BytePace::new(32_768);
    let start = Instant::now();
    let chunk = Duration::from_micros(31_250);
    let first = pace.deadline(1024, start);
    assert_eq!(first, start + chunk);

    let read_after_late_wake = first + Duration::from_millis(20);
    let second = pace.deadline(1024, read_after_late_wake);
    assert_eq!(second, start + chunk * 2);
}

#[test]
fn pacing_after_an_idle_read_sends_one_chunk_then_resumes_the_rate() {
    let mut pace = BytePace::new(32_768);
    let start = Instant::now();
    let chunk = Duration::from_micros(31_250);
    let first = pace.deadline(1024, start);
    let read_after_idle = first + Duration::from_secs(1);
    assert_eq!(pace.deadline(1024, read_after_idle), read_after_idle);
    assert_eq!(
        pace.deadline(1024, read_after_idle),
        read_after_idle + chunk
    );
}

#[test]
fn repeated_short_wake_lag_does_not_reduce_the_configured_five_second_byte_budget() {
    let mut pace = BytePace::new(32_768);
    let start = Instant::now();
    let mut observed_at = start;
    let mut deadline = start;
    for _ in 0..160 {
        deadline = pace.deadline(1024, observed_at);
        observed_at = deadline + Duration::from_millis(20);
    }
    assert_eq!(deadline, start + Duration::from_secs(5));
}
