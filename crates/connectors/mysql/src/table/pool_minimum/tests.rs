use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_primitives::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::yield_now,
    time::advance,
};

use super::*;

/// Why the stand-in pool refuses to open a connection.
#[derive(Debug, thiserror::Error)]
#[error("the stand-in pool refused to open a connection")]
struct OpenRefused;

/// A pool that hands out a connection it holds idle before it opens a new one, as the driver's pool
/// does, and counts every connection it opens.
#[derive(Clone)]
struct StandInPool {
    state: Arc<StandInState>,
}

struct StandInState {
    idle: AtomicUsize,
    checked_out: AtomicUsize,
    opened: AtomicUsize,
    /// How many connections the pool has opened when it refuses to open another, if it ever does.
    refuses_beyond: Option<usize>,
    /// Whether a writer takes one idle connection just before the next checkout is served.
    writer_takes_next: AtomicBool,
    /// The connections such writers took, which they keep.
    writers: AtomicUsize,
}

/// One connection checked out of the stand-in, which returns to its idle connections when dropped.
struct StandInCheckout {
    state: Arc<StandInState>,
}

impl Drop for StandInCheckout {
    fn drop(&mut self) {
        self.state.checked_out.fetch_sub(1, Ordering::SeqCst);
        self.state.idle.fetch_add(1, Ordering::SeqCst);
    }
}

impl StandInPool {
    /// A pool holding `idle` connections it opened, all of them idle.
    fn idle(idle: usize) -> Self {
        Self::with(idle, None)
    }

    /// A pool holding `idle` connections it opened, which refuses to open any once it has opened
    /// `opens` in all.
    fn refusing_beyond(idle: usize, opens: usize) -> Self {
        Self::with(idle, Some(opens))
    }

    fn with(idle: usize, refuses_beyond: Option<usize>) -> Self {
        Self {
            state: Arc::new(StandInState {
                idle: AtomicUsize::new(idle),
                checked_out: AtomicUsize::new(0),
                opened: AtomicUsize::new(idle),
                refuses_beyond,
                writer_takes_next: AtomicBool::new(false),
                writers: AtomicUsize::new(0),
            }),
        }
    }

    /// Check `count` connections out, as writers that hold them until they are dropped.
    fn hold(&self, count: usize) -> Vec<StandInCheckout> {
        let mut held = Vec::with_capacity(count);
        for _ in 0..count {
            held.push(
                self.take()
                    .assured("the stand-in opens every connection a test writer asks for"),
            );
        }
        held
    }

    /// Let a writer take one idle connection just before the next checkout is served.
    fn let_a_writer_take_the_next_idle_connection(&self) {
        self.state.writer_takes_next.store(true, Ordering::SeqCst);
    }

    /// Close `count` idle connections, as the driver does at a lifetime or after a failed check.
    fn close_idle(&self, count: usize) {
        self.state.idle.fetch_sub(count, Ordering::SeqCst);
    }

    fn opened(&self) -> usize {
        self.state.opened.load(Ordering::SeqCst)
    }

    fn idle_count(&self) -> usize {
        self.state.idle.load(Ordering::SeqCst)
    }

    fn writers(&self) -> usize {
        self.state.writers.load(Ordering::SeqCst)
    }

    fn take_idle(&self) -> bool {
        self.state
            .idle
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |idle| {
                idle.checked_sub(1)
            })
            .is_ok()
    }

    fn take(&self) -> Result<StandInCheckout, Report<PoolMinimumError>> {
        if self.state.writer_takes_next.swap(false, Ordering::SeqCst) && self.take_idle() {
            self.state.writers.fetch_add(1, Ordering::SeqCst);
        }
        if !self.take_idle() {
            let opened = self.opened();
            if let Some(limit) = self.state.refuses_beyond
                && opened >= limit
            {
                return Err(Report::new(OpenRefused).change_context(PoolMinimumError::CheckOut));
            }
            self.state.opened.fetch_add(1, Ordering::SeqCst);
        }
        self.state.checked_out.fetch_add(1, Ordering::SeqCst);
        Ok(StandInCheckout {
            state: self.state.clone(),
        })
    }
}

impl KeptPool for StandInPool {
    type Checkout = StandInCheckout;

    fn established(&self) -> usize {
        self.idle_count() + self.state.checked_out.load(Ordering::SeqCst) + self.writers()
    }

    async fn check_out(&self) -> Result<StandInCheckout, Report<PoolMinimumError>> {
        self.take()
    }
}

#[nervix_primitives::test]
async fn an_idle_pool_reaches_its_minimum_from_the_connection_its_open_validated() {
    for minimum in [2, 3, 4, 8] {
        nervix_primitives::task::consume_budget().await;
        let pool = StandInPool::idle(1);
        restore_minimum(&pool, minimum)
            .await
            .assured("the stand-in opens every connection the pass asks for");
        assert_eq!(
            pool.established(),
            minimum,
            "an idle pool below MIN {minimum} must establish all of it, not half"
        );
        assert_eq!(
            pool.opened(),
            minimum,
            "restoring MIN {minimum} opens exactly the connections the pool was missing"
        );
        assert_eq!(pool.idle_count(), minimum, "the pass returns what it held");
    }
}

#[nervix_primitives::test]
async fn an_empty_pool_reaches_its_minimum() {
    let pool = StandInPool::idle(0);
    restore_minimum(&pool, 3)
        .await
        .assured("the stand-in opens every connection the pass asks for");
    assert_eq!(pool.established(), 3);
    assert_eq!(pool.opened(), 3);
}

#[nervix_primitives::test]
async fn a_pool_that_holds_its_minimum_is_not_checked_out() {
    let at_minimum = StandInPool::idle(2);
    let above_minimum = StandInPool::idle(3);
    let held_by_writers = StandInPool::idle(0);
    let writers = held_by_writers.hold(2);
    for pool in [&at_minimum, &above_minimum, &held_by_writers] {
        nervix_primitives::task::consume_budget().await;
        let opened = pool.opened();
        let held = restore_minimum(pool, 2)
            .await
            .assured("a pass over a pool at its minimum checks nothing out");
        assert!(
            held.is_empty(),
            "a pool at its minimum is left alone rather than churned every pass"
        );
        assert_eq!(pool.opened(), opened, "a pool at its minimum opens nothing");
    }
    drop(writers);
}

/// One pool below its minimum while writers hold some of its connections.
struct HeldCase {
    idle: usize,
    held_by_writers: usize,
    minimum: usize,
}

#[nervix_primitives::test]
async fn connections_writers_hold_count_toward_the_minimum() {
    let cases = [
        // A writer holds the only connection: one more opens the missing one.
        HeldCase {
            idle: 0,
            held_by_writers: 1,
            minimum: 2,
        },
        // One of three connections is in use and two are idle: one more reaches four.
        HeldCase {
            idle: 2,
            held_by_writers: 1,
            minimum: 4,
        },
        // Writers hold every connection: each missing one opens.
        HeldCase {
            idle: 0,
            held_by_writers: 2,
            minimum: 4,
        },
    ];
    for case in cases {
        nervix_primitives::task::consume_budget().await;
        let pool = StandInPool::idle(case.idle + case.held_by_writers);
        let writers = pool.hold(case.held_by_writers);
        restore_minimum(&pool, case.minimum)
            .await
            .assured("the stand-in opens every connection the pass asks for");
        assert_eq!(
            pool.established(),
            case.minimum,
            "{} idle and {} held below MIN {} must reach it",
            case.idle,
            case.held_by_writers,
            case.minimum
        );
        assert_eq!(
            pool.opened(),
            case.minimum,
            "the pass opens only the connections the pool was missing"
        );
        drop(writers);
    }
}

#[nervix_primitives::test]
async fn a_writer_taking_an_idle_connection_during_a_pass_opens_nothing_beyond_the_minimum() {
    let pool = StandInPool::idle(2);
    pool.let_a_writer_take_the_next_idle_connection();
    restore_minimum(&pool, 3)
        .await
        .assured("the stand-in opens every connection the pass asks for");
    assert_eq!(
        pool.writers(),
        1,
        "the writer took one of the idle connections"
    );
    assert_eq!(
        pool.established(),
        3,
        "the connection the writer holds counts toward the minimum"
    );
    assert_eq!(
        pool.opened(),
        3,
        "the pass opens only the one missing connection"
    );
}

#[nervix_primitives::test]
async fn a_failed_checkout_ends_the_pass_with_its_cause_and_keeps_what_it_opened() {
    let pool = StandInPool::refusing_beyond(1, 2);
    let Err(report) = restore_minimum(&pool, 4).await else {
        panic!("a pass whose pool refuses to open a connection must fail");
    };
    assert!(
        matches!(
            report.current_context(),
            PoolMinimumError::NotRestored { minimum: 4 }
        ),
        "the pass names the minimum it could not restore: {report:?}"
    );
    assert!(
        report.contains::<OpenRefused>(),
        "the refusal stays beneath the pass's context: {report:?}"
    );
    assert_eq!(
        pool.established(),
        2,
        "the connection the pass opened before the refusal stays in the pool"
    );
    assert_eq!(pool.idle_count(), 2, "the pass returns what it held");
}

/// Let the keeper run every pass the clock has made due.
async fn settle() {
    for _ in 0..64 {
        yield_now().await;
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn the_keeper_restores_its_pool_after_each_interval_until_it_is_dropped() {
    let interval = Duration::from_secs(1);
    let pool = StandInPool::idle(1);
    let keeper = PoolMinimum::keep(pool.clone(), 3, interval);
    settle().await;
    assert_eq!(
        pool.established(),
        1,
        "the keeper waits one interval before its first pass"
    );

    advance(interval).await;
    settle().await;
    assert_eq!(pool.established(), 3, "the first pass restores the minimum");
    assert_eq!(pool.opened(), 3);

    pool.close_idle(2);
    advance(interval).await;
    settle().await;
    assert_eq!(
        pool.established(),
        3,
        "the next pass replaces the connections the pool closed"
    );
    assert_eq!(pool.opened(), 5);

    drop(keeper);
    pool.close_idle(2);
    advance(Duration::from_secs(5)).await;
    settle().await;
    assert_eq!(
        pool.established(),
        1,
        "a dropped keeper restores nothing more"
    );
}
