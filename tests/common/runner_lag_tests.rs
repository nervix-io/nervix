//! Regressions for the runner-lag observer.
//!
//! Layer: test harness.
//! - **Owns.** A stand-in run that blocks the task it is polled on, the ticks and stalls observed
//!   of it, and the scheduling counters read of the thread a test runs on.
//! - **Depends on.** The runner-lag observer, a current-thread runtime and a blocking channel.
//! - **Must not know.** Which scenario step a real stall comes from.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{sync::blocking::mpsc, time::Instant};

use crate::runner_lag::{
    RunnerLag, RunnerLagObserver, RunnerStall, RunnerThread, RunnerTick, ThreadTimes,
};

/// How long the stand-in run blocks the task it is polled on.
const STALL: Duration = Duration::from_millis(400);

/// How long the thread-time regression keeps its thread on a CPU.
const BUSY: Duration = Duration::from_millis(50);

/// An observer whose bound sits well inside the stand-in stall.
const OBSERVER: RunnerLagObserver = RunnerLagObserver {
    period: Duration::from_millis(10),
    reported: Duration::from_millis(100),
};

/// A run that blocks the task it is polled on for [`STALL`], as a step doing synchronous work on
/// the runner task does, between two yields.
async fn blocking_run() -> &'static str {
    // The observer's tick is due before the run blocks, so its lateness spans the stall.
    nervix_primitives::task::yield_now().await;
    let (_held, never_sent) = mpsc::sync_channel::<()>(1);
    // Nothing sends, so this blocks the task for the whole stall.
    let blocked = never_sent.recv_timeout(STALL);
    assert!(blocked.is_err(), "nothing ends the stand-in stall early");
    nervix_primitives::task::yield_now().await;
    "finished"
}

/// The runner task is the only thread of a current-thread runtime here. A run that blocks inside
/// its poll stands in for a step doing synchronous work on the runner task.
#[nervix_primitives::test(flavor = "current_thread")]
async fn a_step_that_blocks_the_runner_task_is_reported_with_its_length() {
    let mut stalls: Vec<RunnerStall> = Vec::new();
    let output = OBSERVER
        .observe(blocking_run(), |tick| {
            if let Some(stall) = tick.stall() {
                stalls.push(stall);
            }
        })
        .await;
    assert_eq!(output, "finished");
    let longest = stalls.iter().map(|stall| stall.late).max();
    let Some(longest) = longest else {
        panic!("the observer reports the blocked runner task");
    };
    // The tick was due at most one period after the stall began.
    let shortest_measurable = STALL
        .checked_sub(OBSERVER.period)
        .assured("the stand-in stall is longer than one tick period");
    assert!(
        longest >= shortest_measurable,
        "the reported stall of {longest:?} spans the {STALL:?} the task was blocked"
    );
}

/// Every tick reaches the reporter, so the run's totals hold the lateness of ticks below the stall
/// bound as well as the stall itself.
#[nervix_primitives::test(flavor = "current_thread")]
async fn the_lateness_of_every_tick_adds_up_beside_the_stalls() {
    let mut lag = RunnerLag::default();
    let output = OBSERVER
        .observe(blocking_run(), |tick| lag.record(tick))
        .await;
    assert_eq!(output, "finished");
    assert!(
        lag.ticks >= 1,
        "the observer polls a tick before the run blocks: {lag}"
    );
    assert!(lag.stalls >= 1, "the blocking poll is a stall: {lag}");
    let shortest_measurable = STALL
        .checked_sub(OBSERVER.period)
        .assured("the stand-in stall is longer than one tick period");
    assert!(
        lag.longest >= shortest_measurable,
        "the longest lateness spans the {STALL:?} the task was blocked: {lag}"
    );
    assert!(
        lag.stalled >= lag.longest,
        "the longest tick reached the bound, so the stalls hold it: {lag}"
    );
    assert!(
        lag.late >= lag.stalled,
        "the stalls are part of the total lateness: {lag}"
    );
}

#[test]
fn a_tick_below_the_bound_counts_as_lateness_and_not_as_a_stall() {
    let bound = Duration::from_secs(1);
    let mut lag = RunnerLag::default();
    lag.record(RunnerTick {
        late: Duration::from_millis(400),
        reported: bound,
    });
    lag.record(RunnerTick {
        late: Duration::from_millis(1_500),
        reported: bound,
    });
    lag.record(RunnerTick {
        late: bound,
        reported: bound,
    });

    assert_eq!(lag.ticks, 3);
    assert_eq!(lag.late, Duration::from_millis(2_900));
    assert_eq!(
        lag.stalls, 2,
        "a tick at the bound is a stall, one below it is not"
    );
    assert_eq!(lag.stalled, Duration::from_millis(2_500));
    assert_eq!(lag.longest, Duration::from_millis(1_500));
    assert_eq!(
        lag.to_string(),
        "3 ticks polled 2.9s late in total; 2 stalls took 2.5s, the longest 1.5s"
    );
}

#[test]
fn a_thread_schedstat_reads_its_cpu_time_and_run_queue_wait() {
    let times = ThreadTimes::parse("1500000000 250000000 42\n")
        .verified("the literal is a well-formed schedstat line");
    assert_eq!(times.on_cpu, Duration::from_millis(1_500));
    assert_eq!(times.run_queue_wait, Duration::from_millis(250));

    let earlier = ThreadTimes::parse("500000000 50000000 7")
        .verified("the literal is a well-formed schedstat line");
    let since = times.since(earlier);
    assert_eq!(since.on_cpu, Duration::from_secs(1));
    assert_eq!(since.run_queue_wait, Duration::from_millis(200));

    assert!(
        ThreadTimes::parse("").is_none(),
        "an empty schedstat names no counters"
    );
    assert!(
        ThreadTimes::parse("12 not-a-number 3").is_none(),
        "a counter that is not a number is not read as zero"
    );
}

/// The runner thread is named by its own procfs directory, so its counters keep reading the
/// thread that named it, and they grow while that thread works.
#[test]
fn the_runner_thread_reads_the_cpu_time_it_spent_since_it_was_named() {
    let thread = RunnerThread::current();
    assert!(
        matches!(thread, RunnerThread::Named { .. }),
        "Linux procfs names the calling thread: {thread:?}"
    );
    let busy_until = Instant::now()
        .checked_add(BUSY)
        .assured("a fraction of a second fits after the present instant");
    let mut spins: u64 = 0;
    while Instant::now() < busy_until {
        spins = std::hint::black_box(spins)
            .checked_add(1)
            .assured("a fraction of a second of spinning counts far fewer than 2^64 turns");
    }
    let since = thread
        .since_began()
        .verified("the thread named above is still running and readable");
    assert!(
        since.on_cpu > Duration::ZERO,
        "{spins} spins over {BUSY:?} ran on a CPU: {since:?}"
    );
}
