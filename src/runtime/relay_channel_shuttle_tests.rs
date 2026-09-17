//! The relay dispatch gate and the relay fan-out, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The fencing, waiter-release and admission invariants the production relay dispatch
//!   gate and relay fan-out are held to.
//! - **Depends on.** The relay channel types, the server Shuttle runner, and the task labels and
//!   timeout triggers of Shuttle's Tokio.
//! - **Must not know.** Relays, branches, batches, acknowledgements, or what a dispatch delivers.

// The standard library's atomics are not Shuttle scheduling points, so each gate and wake record
// below changes in the same scheduling step as the operation it records.
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use shuttle::rand::{Rng as _, thread_rng};
use tokio::{sync::oneshot, time::Instant};
use triomphe::Arc;

use super::{
    RelayBroadcast, RelayDispatchGate, RelayDispatchGateLease, RelayReceiver, RelayTryRecv,
};
use crate::shuttle_test::{check_pct, check_random};

const RANDOM_ITERATIONS: usize = 1_000;
const PCT_ITERATIONS: usize = 1_000;
const PCT_DEPTH: usize = 3;

/// Explores `invariant` under Shuttle's random scheduler and then under its PCT scheduler.
fn explore(invariant: fn()) {
    check_random(invariant, RANDOM_ITERATIONS);
    check_pct(invariant, PCT_ITERATIONS, PCT_DEPTH);
}

/// A fence deadline no check outlives, so only a release ends the engagement.
fn far_future_deadline() -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(86_400))
        .assured("the monotonic clock represents one day past its current reading")
}

/// What the gate check tasks hold. A task raises a record after the gate grants what it records
/// and lowers it before giving that back, so a record never exceeds what the gate has granted.
#[derive(Debug, Default)]
struct GateRecords {
    /// Dispatch permits the check tasks hold.
    live_permits: AtomicUsize,
    /// Leases whose fence reported quiescence and that have not been released.
    quiescent_leases: AtomicUsize,
}

impl GateRecords {
    fn permit_acquired(&self) {
        self.live_permits.fetch_add(1, Ordering::SeqCst);
        self.assert_no_permit_overlaps_a_quiescent_lease();
    }

    fn permit_dropping(&self) {
        self.live_permits.fetch_sub(1, Ordering::SeqCst);
    }

    fn fence_completed(&self) {
        self.quiescent_leases.fetch_add(1, Ordering::SeqCst);
        self.assert_no_permit_overlaps_a_quiescent_lease();
    }

    fn lease_releasing(&self) {
        self.quiescent_leases.fetch_sub(1, Ordering::SeqCst);
    }

    fn assert_no_permit_overlaps_a_quiescent_lease(&self) {
        let live_permits = self.live_permits.load(Ordering::SeqCst);
        let quiescent_leases = self.quiescent_leases.load(Ordering::SeqCst);
        assert!(
            live_permits == 0 || quiescent_leases == 0,
            "{live_permits} dispatch permit(s) are live while {quiescent_leases} lease(s) hold a \
             fence that observed quiescence"
        );
    }
}

/// Acquires `dispatches` permits one after another and holds each across a scheduling point.
async fn dispatch(gate: Arc<RelayDispatchGate>, records: Arc<GateRecords>, dispatches: usize) {
    for _ in 0..dispatches {
        tokio::task::consume_budget().await;
        let permit = gate.acquire_dispatch().await;
        records.permit_acquired();
        tokio::task::yield_now().await;
        records.assert_no_permit_overlaps_a_quiescent_lease();
        records.permit_dropping();
        drop(permit);
    }
}

/// Engages a far-future fence and releases its lease once `release` arrives.
async fn fence_until_released(
    gate: Arc<RelayDispatchGate>,
    records: Arc<GateRecords>,
    release: oneshot::Receiver<()>,
) {
    let mut lease = RelayDispatchGateLease::engage(gate, far_future_deadline(), "shuttle fence");
    assert!(
        lease.wait_quiescent().await,
        "a far-future fence completes once every earlier dispatch drops its permit"
    );
    records.fence_completed();
    release
        .await
        .assured("the closed-gate waiter reports before the check ends");
    records.assert_no_permit_overlaps_a_quiescent_lease();
    records.lease_releasing();
    drop(lease);
}

/// Waits for the gate to close, reports that to the fence holding it, and waits for it to reopen.
async fn observe_close_then_open(gate: Arc<RelayDispatchGate>, closed: oneshot::Sender<()>) {
    gate.wait_closed().await;
    assert!(
        gate.is_engaged(),
        "wait_closed returns only while an engagement holds the gate"
    );
    closed
        .send(())
        .assured("the fence holds its lease until this waiter reports the closed gate");
    gate.wait_open().await;
    assert!(
        !gate.is_engaged(),
        "wait_open returns only once the only engagement has been released"
    );
}

fn dispatch_permits_never_overlap_a_quiescent_lease() {
    shuttle::future::block_on(async {
        let gate = Arc::new(RelayDispatchGate::new());
        let records = Arc::new(GateRecords::default());
        let (closed, closed_is_reported) = oneshot::channel();

        let first_dispatcher = tokio::spawn(dispatch(gate.clone(), records.clone(), 2));
        let second_dispatcher = tokio::spawn(dispatch(gate.clone(), records.clone(), 2));
        let waiter = tokio::spawn(observe_close_then_open(gate.clone(), closed));
        let fence = tokio::spawn(fence_until_released(
            gate.clone(),
            records.clone(),
            closed_is_reported,
        ));

        first_dispatcher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        second_dispatcher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        waiter
            .await
            .assured("a check task that panics fails the execution before its join returns");
        fence
            .await
            .assured("a check task that panics fails the execution before its join returns");

        assert_eq!(
            gate.in_flight_dispatches(),
            0,
            "every permit and rolled-back acquisition returns its dispatch count"
        );
        assert!(!gate.is_engaged(), "the released lease reopens the gate");
    });
}

#[test]
fn shuttle_dispatch_permits_never_overlap_a_quiescent_lease_and_release_frees_every_waiter() {
    explore(dispatch_permits_never_overlap_a_quiescent_lease);
}

/// The signals one of two overlapping fences exchanges with the other.
struct FenceHandshake {
    completed: oneshot::Sender<()>,
    other_completed: oneshot::Receiver<()>,
}

/// Engages a far-future fence and releases its lease only once the other fence has completed too,
/// so both leases are held together in every schedule.
async fn fence_overlapping_another(
    gate: Arc<RelayDispatchGate>,
    records: Arc<GateRecords>,
    handshake: FenceHandshake,
) {
    let mut lease =
        RelayDispatchGateLease::engage(gate, far_future_deadline(), "shuttle overlapping fence");
    assert!(
        lease.wait_quiescent().await,
        "a far-future fence completes once every earlier dispatch drops its permit"
    );
    records.fence_completed();
    handshake
        .completed
        .send(())
        .assured("the other fence waits for this completion before it releases");
    handshake
        .other_completed
        .await
        .assured("the other fence reports its completion before it releases");
    records.assert_no_permit_overlaps_a_quiescent_lease();
    records.lease_releasing();
    drop(lease);
}

fn overlapping_leases_all_release_before_dispatch_resumes() {
    shuttle::future::block_on(async {
        let gate = Arc::new(RelayDispatchGate::new());
        let records = Arc::new(GateRecords::default());
        let (first_completed, first_is_completed) = oneshot::channel();
        let (second_completed, second_is_completed) = oneshot::channel();

        let first_dispatcher = tokio::spawn(dispatch(gate.clone(), records.clone(), 2));
        let second_dispatcher = tokio::spawn(dispatch(gate.clone(), records.clone(), 2));
        let first_fence = tokio::spawn(fence_overlapping_another(
            gate.clone(),
            records.clone(),
            FenceHandshake {
                completed: first_completed,
                other_completed: second_is_completed,
            },
        ));
        let second_fence = tokio::spawn(fence_overlapping_another(
            gate.clone(),
            records.clone(),
            FenceHandshake {
                completed: second_completed,
                other_completed: first_is_completed,
            },
        ));

        first_dispatcher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        second_dispatcher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        first_fence
            .await
            .assured("a check task that panics fails the execution before its join returns");
        second_fence
            .await
            .assured("a check task that panics fails the execution before its join returns");

        assert_eq!(
            gate.in_flight_dispatches(),
            0,
            "every permit and rolled-back acquisition returns its dispatch count"
        );
        assert!(!gate.is_engaged(), "releasing both leases reopens the gate");
    });
}

#[test]
fn shuttle_overlapping_gate_leases_all_release_before_dispatch_resumes() {
    explore(overlapping_leases_all_release_before_dispatch_resumes);
}

/// Marks a task whose gate deadline timeouts the check may fire. Shuttle does not model time, so a
/// timeout fires only once a check triggers it for the task's label.
#[derive(Debug, Clone, Copy)]
struct GateDeadlineMayFire;

fn mark_gate_deadline_may_fire() {
    shuttle::current::set_label_for_task(shuttle::current::me(), GateDeadlineMayFire);
}

async fn expect_expired_fence(mut lease: RelayDispatchGateLease) {
    assert!(
        !lease.wait_quiescent().await,
        "a fence whose deadline has passed never reports quiescence"
    );
}

fn expired_fence_releases_every_waiter_without_reporting_quiescence() {
    // Timeout triggers are thread-local rather than per execution, so every execution starts
    // without the trigger an earlier one registered.
    tokio::time::clear_triggers();
    shuttle::future::block_on(async {
        let gate = Arc::new(RelayDispatchGate::new());
        let earlier_permit = gate.acquire_dispatch().await;
        // Every later comparison finds this deadline reached, so the engagement expires as soon as
        // a gate operation inspects it, and its fence never waits for the earlier permit.
        let expired_deadline = Instant::now();
        let lease =
            RelayDispatchGateLease::engage(gate.clone(), expired_deadline, "shuttle expired fence");

        let fence = tokio::spawn(expect_expired_fence(lease));
        let dispatcher = tokio::spawn({
            let gate = gate.clone();
            async move {
                mark_gate_deadline_may_fire();
                let permit = gate.acquire_dispatch().await;
                drop(permit);
            }
        });
        let open_waiter = tokio::spawn({
            let gate = gate.clone();
            async move {
                mark_gate_deadline_may_fire();
                gate.wait_open().await;
            }
        });
        // Whether the waiters' deadline timeouts fire at all is a recorded choice. When they do
        // not, only an expiry that another gate operation observes can free the waiters.
        let deadline = tokio::spawn(async {
            if thread_rng().gen_bool(0.5) {
                tokio::time::trigger_timeouts(|labels| {
                    labels.get::<GateDeadlineMayFire>().is_some()
                });
            }
        });

        tokio::task::yield_now().await;
        drop(earlier_permit);

        fence
            .await
            .assured("a check task that panics fails the execution before its join returns");
        dispatcher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        open_waiter
            .await
            .assured("a check task that panics fails the execution before its join returns");
        deadline
            .await
            .assured("a check task that panics fails the execution before its join returns");

        assert_eq!(
            gate.in_flight_dispatches(),
            0,
            "every permit and rolled-back acquisition returns its dispatch count"
        );
        assert!(
            !gate.is_engaged(),
            "the expired engagement reopens the gate"
        );
    });
}

#[test]
fn shuttle_expired_gate_fence_frees_every_waiter_without_reporting_quiescence() {
    explore(expired_fence_releases_every_waiter_without_reporting_quiescence);
}

fn canceled_dispatch_releases_its_fence_permit() {
    shuttle::future::block_on(async {
        let gate = Arc::new(RelayDispatchGate::new());
        let dispatcher = tokio::spawn({
            let gate = gate.clone();
            async move {
                let _permit = gate.acquire_dispatch().await;
                std::future::pending::<()>().await;
            }
        });
        let fence = tokio::spawn({
            let gate = gate.clone();
            async move {
                let mut lease =
                    RelayDispatchGateLease::engage(gate, far_future_deadline(), "shuttle fence");
                assert!(
                    lease.wait_quiescent().await,
                    "canceling a dispatch returns its permit to a far-future fence"
                );
            }
        });

        tokio::task::yield_now().await;
        dispatcher.abort();
        let Err(cancellation) = dispatcher.await else {
            panic!("a dispatch that holds its permit forever completes only by cancellation");
        };
        assert!(
            cancellation.is_cancelled(),
            "the aborted dispatch ends by cancellation"
        );
        fence
            .await
            .assured("a check task that panics fails the execution before its join returns");

        assert_eq!(
            gate.in_flight_dispatches(),
            0,
            "a canceled dispatch returns its permit and any rolled-back acquisition"
        );
        assert!(!gate.is_engaged(), "the dropped lease reopens the gate");
    });
}

#[test]
fn shuttle_canceled_dispatch_returns_its_permit_to_the_gate_fence() {
    explore(canceled_dispatch_releases_its_fence_permit);
}

/// The wake-ups a future receives from other tasks, as distinct from the ones it schedules for
/// itself while it is being polled.
#[derive(Debug, Default)]
struct WakeRecord {
    polling: AtomicBool,
    external_wakes: AtomicUsize,
}

impl WakeRecord {
    fn external_wakes(&self) -> usize {
        self.external_wakes.load(Ordering::SeqCst)
    }
}

/// The waker a recorded future is polled with. `std::task::Wake` takes the standard `Arc`.
struct RecordingWaker {
    record: StdArc<WakeRecord>,
    task: Waker,
}

impl Wake for RecordingWaker {
    fn wake(self: StdArc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        if !self.record.polling.load(Ordering::SeqCst) {
            self.record.external_wakes.fetch_add(1, Ordering::SeqCst);
        }
        self.task.wake_by_ref();
    }
}

/// Polls `future` to completion and records every wake-up another task delivers to it.
async fn record_wakes<F: Future>(future: F, record: StdArc<WakeRecord>) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|context| {
        let waker = Waker::from(StdArc::new(RecordingWaker {
            record: record.clone(),
            task: context.waker().clone(),
        }));
        let mut recording = Context::from_waker(&waker);
        record.polling.store(true, Ordering::SeqCst);
        let poll = future.as_mut().poll(&mut recording);
        record.polling.store(false, Ordering::SeqCst);
        poll
    })
    .await
}

async fn dispatch_once(gate: Arc<RelayDispatchGate>) {
    let permit = gate.acquire_dispatch().await;
    drop(permit);
}

fn dispatches_parked_behind_a_lease_wake_only_on_its_release() {
    const LEASE_HOLD_YIELDS: usize = 4;

    shuttle::future::block_on(async {
        let gate = Arc::new(RelayDispatchGate::new());
        let mut lease =
            RelayDispatchGateLease::engage(gate.clone(), far_future_deadline(), "shuttle fence");
        assert!(
            lease.wait_quiescent().await,
            "no dispatch holds a permit before the check starts"
        );

        let first_wakes = StdArc::new(WakeRecord::default());
        let second_wakes = StdArc::new(WakeRecord::default());
        let first_dispatcher = tokio::spawn(record_wakes(
            dispatch_once(gate.clone()),
            first_wakes.clone(),
        ));
        let second_dispatcher = tokio::spawn(record_wakes(
            dispatch_once(gate.clone()),
            second_wakes.clone(),
        ));

        for _ in 0..LEASE_HOLD_YIELDS {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
        drop(lease);

        first_dispatcher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        second_dispatcher
            .await
            .assured("a check task that panics fails the execution before its join returns");

        // Nothing but the release changes the gate after the dispatches start, so a dispatch that
        // parked behind the lease has exactly one reason to wake.
        assert!(
            first_wakes.external_wakes() <= 1,
            "a dispatch parked behind a held lease was woken {} times, not only by the release",
            first_wakes.external_wakes()
        );
        assert!(
            second_wakes.external_wakes() <= 1,
            "a dispatch parked behind a held lease was woken {} times, not only by the release",
            second_wakes.external_wakes()
        );
        assert_eq!(
            gate.in_flight_dispatches(),
            0,
            "every permit and rolled-back acquisition returns its dispatch count"
        );
    });
}

#[test]
fn shuttle_dispatches_parked_behind_a_lease_wake_only_on_its_release() {
    explore(dispatches_parked_behind_a_lease_wake_only_on_its_release);
}

fn capacity(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).assured("check capacities are nonzero literals")
}

/// Publishes `batches` in order.
async fn publish(channel: Arc<RelayBroadcast<u32>>, batches: &'static [u32]) {
    for batch in batches {
        tokio::task::consume_budget().await;
        channel
            .broadcast(*batch)
            .await
            .assured("a consumer that takes every batch outlives every publisher");
    }
}

/// A receiver handed back by the task that drained it, with every batch it took in order.
struct DrainedReceiver {
    receiver: RelayReceiver<u32>,
    batches: Vec<u32>,
}

fn capacity_shrink_keeps_buffered_batches_and_wakes_publishers_after_the_drain() {
    const BUFFERED: [u32; 3] = [0, 1, 2];
    const FIRST_PUBLISHED: [u32; 2] = [10, 11];
    const SECOND_PUBLISHED: [u32; 2] = [20, 21];

    shuttle::future::block_on(async {
        let channel = Arc::new(RelayBroadcast::with_capacity(capacity(BUFFERED.len())));
        let mut receiver = channel.new_receiver();
        for batch in BUFFERED {
            tokio::task::consume_budget().await;
            channel
                .broadcast(batch)
                .await
                .assured("the consumer has room for every buffered batch");
        }

        let first_publisher = tokio::spawn(publish(channel.clone(), &FIRST_PUBLISHED));
        let second_publisher = tokio::spawn(publish(channel.clone(), &SECOND_PUBLISHED));
        let shrink = tokio::spawn({
            let channel = channel.clone();
            async move {
                channel.set_capacity(capacity(1));
            }
        });
        let drain = tokio::spawn({
            let channel = channel.clone();
            async move {
                let expected = BUFFERED.len() + FIRST_PUBLISHED.len() + SECOND_PUBLISHED.len();
                let mut batches = Vec::with_capacity(expected);
                while batches.len() < expected {
                    tokio::task::consume_budget().await;
                    assert!(
                        channel.len() <= BUFFERED.len(),
                        "admission never passes the largest capacity in force"
                    );
                    let batch = receiver
                        .recv()
                        .await
                        .assured("the check holds the fan-out open until the drain ends");
                    batches.push(batch);
                }
                DrainedReceiver { receiver, batches }
            }
        });

        first_publisher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        second_publisher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        shrink
            .await
            .assured("a check task that panics fails the execution before its join returns");
        let DrainedReceiver {
            mut receiver,
            batches,
        } = drain
            .await
            .assured("a check task that panics fails the execution before its join returns");

        let (buffered, published) = batches.split_at(BUFFERED.len());
        assert_eq!(
            buffered, BUFFERED,
            "batches buffered before the capacity change are all delivered first, in order"
        );
        let mut first_delivered = Vec::new();
        let mut second_delivered = Vec::new();
        for batch in published {
            if *batch < SECOND_PUBLISHED[0] {
                first_delivered.push(*batch);
            } else {
                second_delivered.push(*batch);
            }
        }
        assert_eq!(
            first_delivered, FIRST_PUBLISHED,
            "every batch of the first publisher is delivered once, in publish order"
        );
        assert_eq!(
            second_delivered, SECOND_PUBLISHED,
            "every batch of the second publisher is delivered once, in publish order"
        );

        assert_eq!(channel.capacity(), 1);
        assert_eq!(channel.len(), 0, "the drained consumer holds no admission");
        assert_eq!(
            channel.waiting_publishers(),
            0,
            "every publisher leaves its admission wait"
        );
        assert_eq!(receiver.try_recv(), RelayTryRecv::Empty);
    });
}

#[test]
fn shuttle_capacity_shrink_keeps_buffered_batches_and_wakes_publishers_after_the_drain() {
    explore(capacity_shrink_keeps_buffered_batches_and_wakes_publishers_after_the_drain);
}

fn capacity_growth_admits_waiting_publishers_without_a_take() {
    shuttle::future::block_on(async {
        let channel = Arc::new(RelayBroadcast::with_capacity(capacity(1)));
        let mut receiver = channel.new_receiver();
        channel
            .broadcast(0)
            .await
            .assured("the consumer has room for one buffered batch");

        let first_publisher = tokio::spawn(publish(channel.clone(), &[1]));
        let second_publisher = tokio::spawn(publish(channel.clone(), &[2]));
        let growth = tokio::spawn({
            let channel = channel.clone();
            async move {
                channel.set_capacity(capacity(3));
            }
        });

        // Nothing is taken until both publishers finish, so only the growth can admit them.
        first_publisher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        second_publisher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        growth
            .await
            .assured("a check task that panics fails the execution before its join returns");
        assert_eq!(
            channel.len(),
            3,
            "the grown capacity admits both publishers beside the buffered batch"
        );
        assert_eq!(
            channel.waiting_publishers(),
            0,
            "every publisher leaves its admission wait"
        );

        let mut batches = Vec::with_capacity(3);
        for _ in 0..3 {
            tokio::task::consume_budget().await;
            let batch = receiver
                .recv()
                .await
                .assured("the check holds the fan-out open while it drains");
            batches.push(batch);
        }
        assert_eq!(batches[0], 0, "the buffered batch is delivered first");
        batches[1..].sort_unstable();
        assert_eq!(
            batches[1..],
            [1, 2],
            "each admitted batch is delivered once"
        );
        assert_eq!(channel.len(), 0, "the drained consumer holds no admission");
        assert_eq!(receiver.try_recv(), RelayTryRecv::Empty);
    });
}

#[test]
fn shuttle_capacity_growth_admits_waiting_publishers_without_a_take() {
    explore(capacity_growth_admits_waiting_publishers_without_a_take);
}

/// Takes batches until `last` arrives.
async fn receive_through(mut receiver: RelayReceiver<u32>, last: u32) -> DrainedReceiver {
    let mut batches = Vec::new();
    loop {
        tokio::task::consume_budget().await;
        assert!(
            receiver.len() <= 1,
            "a consumer at capacity one never holds more than one delivered batch"
        );
        let batch = receiver
            .recv()
            .await
            .assured("the check holds the fan-out open until every consumer drains");
        batches.push(batch);
        if batch == last {
            return DrainedReceiver { receiver, batches };
        }
    }
}

/// Attaches a consumer at whatever point the scheduler chooses and takes batches until `last`.
async fn attach_and_receive_through(
    channel: Arc<RelayBroadcast<u32>>,
    attached: oneshot::Sender<()>,
    last: u32,
) -> DrainedReceiver {
    let receiver = channel.new_receiver();
    attached
        .send(())
        .assured("the check publishes its last batch only after this consumer attaches");
    receive_through(receiver, last).await
}

fn publishers_wait_for_the_slowest_consumer_and_skip_consumers_that_leave() {
    const LAST: u32 = 3;

    shuttle::future::block_on(async {
        let channel = Arc::new(RelayBroadcast::with_capacity(capacity(1)));
        let first = channel.new_receiver();
        let second = channel.new_receiver();
        let lagging = channel.new_receiver();
        channel
            .broadcast(0)
            .await
            .assured("every consumer has room for the first batch");

        let publisher = tokio::spawn(publish(channel.clone(), &[1, 2]));
        let first_consumer = tokio::spawn(receive_through(first, LAST));
        let second_consumer = tokio::spawn(receive_through(second, LAST));
        let lagging_leaves = tokio::spawn(async move {
            drop(lagging);
        });
        let (attached, late_is_attached) = oneshot::channel();
        let late_consumer =
            tokio::spawn(attach_and_receive_through(channel.clone(), attached, LAST));

        // The lagging consumer never takes the first batch, so the publisher finishes only once
        // that consumer leaves and every remaining consumer takes each batch.
        publisher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        lagging_leaves
            .await
            .assured("a check task that panics fails the execution before its join returns");
        late_is_attached
            .await
            .assured("the late consumer reports its attachment before it drains");
        channel
            .broadcast(LAST)
            .await
            .assured("the consumers that remain take the last batch");

        let first_drained = first_consumer
            .await
            .assured("a check task that panics fails the execution before its join returns");
        let second_drained = second_consumer
            .await
            .assured("a check task that panics fails the execution before its join returns");
        let late_drained = late_consumer
            .await
            .assured("a check task that panics fails the execution before its join returns");
        assert_eq!(
            first_drained.batches,
            [0, 1, 2, LAST],
            "a consumer that stays receives every batch in publish order"
        );
        assert_eq!(
            second_drained.batches,
            [0, 1, 2, LAST],
            "a consumer that stays receives every batch in publish order"
        );
        assert!(
            [1, 2, LAST].ends_with(&late_drained.batches),
            "a late consumer receives, in order, exactly the batches whose publishing began after \
             it attached, but received {:?}",
            late_drained.batches
        );

        assert_eq!(
            channel.waiting_publishers(),
            0,
            "every publisher leaves its admission wait"
        );
        assert_eq!(channel.len(), 0, "drained consumers hold no admission");
        assert_eq!(channel.receiver_count(), 3);
        drop(first_drained);
        drop(second_drained);
        drop(late_drained);
        assert_eq!(channel.receiver_count(), 0);
    });
}

#[test]
fn shuttle_publishers_wait_for_the_slowest_consumer_and_skip_consumers_that_leave() {
    explore(publishers_wait_for_the_slowest_consumer_and_skip_consumers_that_leave);
}

fn losing_every_consumer_delivers_or_returns_the_waiting_batch() {
    shuttle::future::block_on(async {
        let channel = Arc::new(RelayBroadcast::with_capacity(capacity(1)));
        let first = channel.new_receiver();
        let second = channel.new_receiver();
        channel
            .broadcast(0)
            .await
            .assured("both consumers have room for one batch");

        let publisher = tokio::spawn({
            let channel = channel.clone();
            async move { channel.broadcast(1).await }
        });
        let first_leaves = tokio::spawn(async move {
            drop(first);
        });
        let second_leaves = tokio::spawn(async move {
            drop(second);
        });
        let late_attach = tokio::spawn({
            let channel = channel.clone();
            async move { channel.new_receiver() }
        });

        // The two original consumers never take a batch, so only the late consumer can admit the
        // publisher, and only when it attached before publishing began.
        let delivery = publisher
            .await
            .assured("a check task that panics fails the execution before its join returns");
        first_leaves
            .await
            .assured("a check task that panics fails the execution before its join returns");
        second_leaves
            .await
            .assured("a check task that panics fails the execution before its join returns");
        let mut late = late_attach
            .await
            .assured("a check task that panics fails the execution before its join returns");
        match delivery {
            Ok(()) => assert_eq!(
                late.try_recv(),
                RelayTryRecv::Batch(1),
                "a batch reported delivered reached the only consumer left to take it"
            ),
            Err(returned) => assert_eq!(
                returned.batch, 1,
                "a batch no consumer is left to take returns to its publisher"
            ),
        }
        assert_eq!(
            late.try_recv(),
            RelayTryRecv::Empty,
            "a batch is delivered at most once and never both delivered and returned"
        );

        assert_eq!(
            channel.waiting_publishers(),
            0,
            "the publisher leaves its admission wait"
        );
        assert_eq!(channel.receiver_count(), 1);
        assert_eq!(channel.len(), 0, "the late consumer holds no admission");
    });
}

#[test]
fn shuttle_losing_every_consumer_delivers_or_returns_the_waiting_batch() {
    explore(losing_every_consumer_delivers_or_returns_the_waiting_batch);
}
