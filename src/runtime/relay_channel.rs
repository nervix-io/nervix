//! Relay dispatch fencing and the contentionless relay fan-out.
//!
//! Layer: data plane.
//! - **Owns.** The dispatch gate that fences relay delivery while the runtime mutates a relay, and
//!   the fan-out that hands every batch published into one relay to each of its live consumers
//!   under bounded, resizable backpressure.
//! - **Depends on.** Tokio synchronization, lock-free queues and wakers, `arc-swap`, and panic
//!   classification.
//! - **Must not know.** Relays, branches, batches, acknowledgements, placement, or any Model.

use std::{
    collections::BTreeMap,
    fmt,
    future::poll_fn,
    num::NonZeroUsize,
    task::{Context, Poll},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    collections::{ConcurrentQueue, PopError, PushError},
    publication::{ArcSwap, Guard},
    sync::{
        Arc, AtomicWaker, Notify,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        blocking::Mutex,
    },
    time::{Instant, timeout_at},
};
use tracing::debug;

#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        key = "relay pause and quiescence generation",
        bound = "one gate state per channel; finite admitted and retained counters and the \
                 engagement deadline",
        reason = "dispatch retains the exact channel gate and its generation"
    )
)]
pub(in crate::runtime) struct RelayDispatchGate {
    closed: AtomicBool,
    in_flight_dispatches: AtomicUsize,
    state: Mutex<RelayDispatchGateState>,
    /// Wakes every waiter when an engagement begins, is released, or expires.
    changed: Notify,
    /// Wakes fences waiting for quiescence when the in-flight dispatch count reaches zero while the
    /// gate is closed.
    ///
    /// An acquisition the closed gate rejects rolls its count back and can reach zero, so this stays
    /// apart from `changed`. Otherwise every rollback would wake the other acquisitions waiting for
    /// the gate to open, each would retry and roll back in turn, and they would keep waking one
    /// another for as long as the gate stayed closed.
    drained: Notify,
}

#[derive(Debug, Default)]
struct RelayDispatchGateState {
    generation: u64,
    engagements: BTreeMap<u64, RelayDispatchGateEngagement>,
}

#[derive(Debug)]
struct RelayDispatchGateEngagement {
    phase: RelayDispatchGateEngagementPhase,
    reason: String,
}

#[derive(Debug, Clone, Copy)]
enum RelayDispatchGateEngagementPhase {
    Fencing { deadline: Instant },
    Leased,
}

/// Owns one dispatch-gate engagement from fence acquisition through the protected mutation.
///
/// The deadline only bounds acquisition of the fence. Once [`Self::wait_quiescent`] succeeds, the
/// gate remains closed until this lease is explicitly released or dropped, even when the original
/// fence deadline passes.
#[derive(Debug)]
pub(in crate::runtime) struct RelayDispatchGateLease {
    gate: Arc<RelayDispatchGate>,
    generation: u64,
}

/// Proof that one relay dispatch entered before the current gate engagements.
///
/// Dispatch acquisition raises the in-flight count with a read-modify-write before it inspects the
/// closed flag, while an engagement closes the gate before it reads the count, and it reads the
/// count with a read-modify-write as well. Read-modify-writes of one count are totally ordered, so
/// one side observes the other: when the dispatch's comes first the fence counts it, and when the
/// fence's comes first the dispatch's acquires it and observes the gate closed, rolls its count
/// back and waits for the engagement to end. Dropping every permit counted by the fence completes
/// it, and releases what each dispatch did under its permit to the fence.
#[derive(Debug)]
pub(in crate::runtime) struct RelayDispatchPermit<'gate> {
    gate: &'gate RelayDispatchGate,
}

/// Owned proof that one relay dispatch entered a dynamically selected gate.
///
/// Branch-scoped holds are published through an immutable gate set. An owned permit lets a
/// dispatch retain every matching gate after that set's load guard is gone, without putting a lock
/// on the dispatch path.
#[derive(Debug)]
pub(in crate::runtime) struct OwnedRelayDispatchPermit {
    gate: Arc<RelayDispatchGate>,
}

/// What one look at an engagement's fence found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayDispatchFence {
    /// The engagement was released, or reached its deadline and was cleared, before its fence
    /// completed.
    Ended,
    /// The engagement reached its deadline before every dispatch it waits for left.
    DeadlinePassed,
    /// Every dispatch that entered before the engagement has left, and the engagement holds its
    /// lease.
    Quiescent,
    /// Dispatches that entered before the engagement are still running.
    Waiting { deadline: Instant },
}

/// Whether the dispatch that just left was the last one a closed gate's fences wait for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayDispatchLeft {
    /// The gate is closed and no dispatch is left, so every fence waiting for quiescence must look
    /// again.
    Drained,
    /// The gate is open, or dispatches are still running.
    Running,
}

impl RelayDispatchGate {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            in_flight_dispatches: AtomicUsize::new(0),
            state: Mutex::new(RelayDispatchGateState::default()),
            changed: Notify::new(),
            drained: Notify::new(),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the gate reason or a delivery callback; local callback \
                      bodies remain analyzed"
        )
    )]
    pub(super) fn engage(&self, deadline: Instant, reason: impl Into<String>) -> u64 {
        let mut state = self.state.lock();
        loop {
            state.generation = state
                .generation
                .checked_add(1)
                .assured("a relay gate cannot be engaged 2^64 times on one node");
            if !state.engagements.contains_key(&state.generation) {
                break;
            }
        }
        let generation = state.generation;
        state.engagements.insert(
            generation,
            RelayDispatchGateEngagement {
                phase: RelayDispatchGateEngagementPhase::Fencing { deadline },
                reason: reason.into(),
            },
        );
        // Relaxed: the fence reads the dispatch count with a read-modify-write after this store,
        // and a dispatch whose own read-modify-write of the count comes later acquires that one,
        // which orders this store before the dispatch's look at the flag.
        self.closed.store(true, Ordering::Relaxed);
        drop(state);
        self.changed.notify_waiters();
        generation
    }

    pub(super) fn release(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.engagements.remove(&generation).is_none() {
            return;
        }
        self.reopen_unless_engaged(&state);
        drop(state);
        self.changed.notify_waiters();
    }

    /// Reopens the gate once no engagement is left, which releases what the protected mutation
    /// changed while the gate was closed to every dispatch that observes it open.
    fn reopen_unless_engaged(&self, state: &RelayDispatchGateState) {
        self.closed
            .store(!state.engagements.is_empty(), Ordering::Release);
    }

    pub(in crate::runtime) async fn acquire_dispatch(&self) -> RelayDispatchPermit<'_> {
        self.acquire().await;
        RelayDispatchPermit { gate: self }
    }

    /// A buffered owner batch cannot wait behind a schedule swap: the swap's drain includes that
    /// batch, so waiting would hold the drain open. Reject it and let its source retry instead.
    pub(in crate::runtime) fn try_acquire_dispatch(&self) -> Option<RelayDispatchPermit<'_>> {
        if self.try_enter() {
            Some(RelayDispatchPermit { gate: self })
        } else {
            self.clear_if_expired();
            if self.try_enter() {
                Some(RelayDispatchPermit { gate: self })
            } else {
                None
            }
        }
    }

    pub(in crate::runtime) async fn acquire_owned(gate: &Arc<Self>) -> OwnedRelayDispatchPermit {
        gate.acquire().await;
        OwnedRelayDispatchPermit { gate: gate.clone() }
    }

    async fn acquire(&self) {
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.try_enter() {
                return;
            }

            self.clear_if_expired();
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let deadline = {
                let state = self.state.lock();
                if state.engagements.is_empty() {
                    drop(state);
                    continue;
                }
                state
                    .engagements
                    .values()
                    .filter_map(RelayDispatchGateEngagement::fence_deadline)
                    .min()
            };
            if let Some(deadline) = deadline {
                if timeout_at(deadline, changed.as_mut()).await.is_err() {
                    self.clear_if_expired();
                }
            } else {
                changed.await;
            }
        }
    }

    /// Counts a dispatch in, unless an engagement has closed the gate.
    ///
    /// A dispatch that observes the gate open has its count read by every later fence, and one that
    /// observes it closed leaves again before it returns.
    fn try_enter(&self) -> bool {
        self.enter();
        // Acquire: a dispatch that observes the gate reopened by its last release also observes
        // what the protected mutation changed while the gate was closed.
        if !self.closed.load(Ordering::Acquire) {
            return true;
        }
        self.leave();
        false
    }

    /// Whether an ownership or lifecycle operation currently fences this relay.
    pub(in crate::runtime) fn is_engaged(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Waits for all dispatch permits acquired before `generation` was engaged to be dropped.
    ///
    /// `false` means this engagement was released or reached its deadline before the fence
    /// completed. Callers must not tear down relay consumers when the fence did not complete.
    async fn wait_quiescent(&self, generation: u64) -> bool {
        loop {
            nervix_primitives::task::consume_budget().await;
            self.clear_if_expired();
            let changed = self.changed.notified();
            let drained = self.drained.notified();
            tokio::pin!(changed);
            tokio::pin!(drained);
            changed.as_mut().enable();
            drained.as_mut().enable();
            let deadline = match self.poll_quiescence(generation) {
                RelayDispatchFence::Ended => return false,
                RelayDispatchFence::DeadlinePassed => {
                    self.clear_if_expired();
                    return false;
                }
                RelayDispatchFence::Quiescent => return true,
                RelayDispatchFence::Waiting { deadline } => deadline,
            };
            let woken = async {
                nervix_primitives::select! {
                    () = changed.as_mut() => {}
                    () = drained.as_mut() => {}
                }
            };
            if timeout_at(deadline, woken).await.is_err() {
                self.clear_if_expired();
            }
        }
    }

    /// Looks once at the dispatches the fence of `generation` waits for, and takes the lease when
    /// none is left.
    ///
    /// The count is read with a read-modify-write, which reads the newest value of the count. A
    /// dispatch whose count comes earlier is counted here, and one whose count comes later acquires
    /// this read and observes the gate closed. A plain load could miss both: it may return a count
    /// from before an increment that preceded it, while that dispatch's own load of the flag returns
    /// the gate still open. Reading the count also acquires every dispatch that left, so what a
    /// dispatch did under its permit happens before the lease.
    fn poll_quiescence(&self, generation: u64) -> RelayDispatchFence {
        let mut state = self.state.lock();
        let in_flight_dispatches = self.in_flight_dispatches.fetch_add(0, Ordering::AcqRel);
        let Some(engagement) = state.engagements.get_mut(&generation) else {
            return RelayDispatchFence::Ended;
        };
        match engagement.phase {
            RelayDispatchGateEngagementPhase::Leased => RelayDispatchFence::Quiescent,
            RelayDispatchGateEngagementPhase::Fencing { deadline } => {
                if Instant::now() >= deadline {
                    return RelayDispatchFence::DeadlinePassed;
                }
                if in_flight_dispatches == 0 {
                    engagement.phase = RelayDispatchGateEngagementPhase::Leased;
                    return RelayDispatchFence::Quiescent;
                }
                RelayDispatchFence::Waiting { deadline }
            }
        }
    }

    pub(in crate::runtime) fn is_closed(&self) -> bool {
        self.clear_if_expired();
        self.closed.load(Ordering::Acquire)
    }

    pub(in crate::runtime) async fn wait_open(&self) {
        if !self.closed.load(Ordering::Acquire) {
            return;
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            let changed = self.changed.notified();
            let (is_open, deadline) = {
                let state = self.state.lock();
                (
                    state.engagements.is_empty(),
                    state
                        .engagements
                        .values()
                        .filter_map(RelayDispatchGateEngagement::fence_deadline)
                        .min(),
                )
            };
            if is_open {
                return;
            }
            if let Some(deadline) = deadline {
                if timeout_at(deadline, changed).await.is_err() {
                    self.clear_if_expired();
                }
            } else {
                changed.await;
            }
            if !self.closed.load(Ordering::Acquire) {
                return;
            }
        }
    }

    pub(in crate::runtime) async fn wait_closed(&self) {
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.is_closed() {
                return;
            }
            let changed = self.changed.notified();
            if self.is_closed() {
                return;
            }
            changed.await;
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reason(&self) -> Option<String> {
        self.clear_if_expired();
        self.state
            .lock()
            .engagements
            .last_key_value()
            .map(|(_, engagement)| engagement)
            .map(|engagement| engagement.reason.clone())
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::runtime) fn in_flight_dispatches(&self) -> usize {
        self.in_flight_dispatches.load(Ordering::Acquire)
    }

    /// Raises the in-flight count with a read-modify-write, which a fence's later read of the count
    /// observes, and which acquires a fence's earlier read.
    fn enter(&self) {
        self.in_flight_dispatches
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .assured("a process cannot hold usize::MAX live relay dispatch permits");
    }

    /// Lowers the in-flight count, and wakes the fences waiting for quiescence when it was the
    /// last dispatch a closed gate waited for.
    fn leave(&self) {
        if self.release_dispatch() == RelayDispatchLeft::Drained {
            self.drained.notify_waiters();
        }
    }

    /// Lowers the in-flight count and reports whether the fences waiting for it must look again.
    ///
    /// The read-modify-write releases what the dispatch did to the fence that reads the count
    /// after it. When a fence read the count before it, this one acquires that read and therefore
    /// observes the gate the fence closed, so a fence that saw this dispatch still running is
    /// always woken once it leaves.
    fn release_dispatch(&self) -> RelayDispatchLeft {
        let remaining = self
            .in_flight_dispatches
            .fetch_sub(1, Ordering::AcqRel)
            .checked_sub(1)
            .verified("this permit or rolled-back acquisition raised the count");
        // Relaxed: the read-modify-write above already orders a fence's closing store before this
        // load whenever that fence's read of the count came first.
        if remaining == 0 && self.closed.load(Ordering::Relaxed) {
            return RelayDispatchLeft::Drained;
        }
        RelayDispatchLeft::Running
    }

    fn clear_if_expired(&self) {
        if !self.closed.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let mut state = self.state.lock();
        let mut expired = Vec::new();
        for (generation, engagement) in &state.engagements {
            let Some(deadline) = engagement.fence_deadline() else {
                continue;
            };
            if now >= deadline {
                expired.push((*generation, engagement.reason.clone()));
            }
        }
        if expired.is_empty() {
            return;
        }
        for (generation, _) in &expired {
            state.engagements.remove(generation);
        }
        self.reopen_unless_engaged(&state);
        drop(state);
        for (_, reason) in expired {
            debug!(reason, "relay dispatch gate fence deadline expired");
        }
        self.changed.notify_waiters();
    }
}

impl RelayDispatchGateEngagement {
    fn fence_deadline(&self) -> Option<Instant> {
        match self.phase {
            RelayDispatchGateEngagementPhase::Fencing { deadline } => Some(deadline),
            RelayDispatchGateEngagementPhase::Leased => None,
        }
    }
}

impl RelayDispatchGateLease {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the gate reason or a delivery callback; local callback \
                      bodies remain analyzed"
        )
    )]
    pub(in crate::runtime) fn engage(
        gate: Arc<RelayDispatchGate>,
        deadline: Instant,
        reason: impl Into<String>,
    ) -> Self {
        let generation = gate.engage(deadline, reason);
        Self { gate, generation }
    }

    pub(in crate::runtime) async fn wait_quiescent(&mut self) -> bool {
        self.gate.wait_quiescent(self.generation).await
    }
}

impl Drop for RelayDispatchGateLease {
    fn drop(&mut self) {
        self.gate.release(self.generation);
    }
}

impl Default for RelayDispatchGate {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RelayDispatchPermit<'_> {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

impl Drop for OwnedRelayDispatchPermit {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

#[cfg(test)]
mod gate_tests {
    use std::time::Duration;

    use nervix_primitives::{sync::Arc, time::Instant};

    use super::{RelayDispatchGate, RelayDispatchGateLease};

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the gate reason or a delivery callback; local callback \
                      bodies remain analyzed"
        )
    )]
    fn engage(
        gate: &Arc<RelayDispatchGate>,
        deadline: Instant,
        reason: &str,
    ) -> RelayDispatchGateLease {
        RelayDispatchGateLease::engage(gate.clone(), deadline, reason)
    }

    #[nervix_primitives::test]
    async fn relay_dispatch_gate_releases_waiters_explicitly() {
        let gate = Arc::new(RelayDispatchGate::new());
        let lease = engage(&gate, Instant::now() + Duration::from_secs(1), "node swap");
        assert!(gate.is_closed());
        assert_eq!(gate.reason().as_deref(), Some("node swap"));

        drop(lease);
        gate.wait_open().await;
        assert!(!gate.is_closed());
    }

    #[nervix_primitives::test]
    async fn relay_dispatch_gate_self_clears_at_its_deadline() {
        let gate = Arc::new(RelayDispatchGate::new());
        let _lease = engage(
            &gate,
            Instant::now() + Duration::from_millis(10),
            "leader may fail",
        );

        gate.wait_open().await;
        assert!(!gate.is_closed());
    }

    #[nervix_primitives::test]
    async fn stale_gate_hold_cannot_release_a_new_engagement() {
        let gate = Arc::new(RelayDispatchGate::new());
        let stale = engage(&gate, Instant::now() + Duration::from_secs(1), "first");
        let current = engage(&gate, Instant::now() + Duration::from_secs(1), "second");

        drop(stale);
        assert!(gate.is_closed());
        assert_eq!(gate.reason().as_deref(), Some("second"));
        drop(current);
        assert!(!gate.is_closed());
    }

    #[nervix_primitives::test]
    async fn expired_engagement_does_not_report_a_completed_fence() {
        let gate = Arc::new(RelayDispatchGate::new());
        let _permit = gate.acquire_dispatch().await;
        let mut lease = engage(
            &gate,
            Instant::now() + Duration::from_millis(10),
            "graceful entity stop",
        );

        assert!(!lease.wait_quiescent().await);
        assert!(!gate.is_closed());
    }

    #[nervix_primitives::test]
    async fn acquired_gate_lease_outlives_its_fence_deadline() {
        let gate = Arc::new(RelayDispatchGate::new());
        let deadline = Instant::now() + Duration::from_millis(10);
        let mut lease = engage(&gate, deadline, "slow node swap");
        assert!(lease.wait_quiescent().await);

        nervix_primitives::time::sleep_until(deadline + Duration::from_millis(10)).await;
        assert!(gate.is_closed());

        let dispatch = nervix_primitives::task::spawn({
            let gate = gate.clone();
            async move {
                let _permit = gate.acquire_dispatch().await;
            }
        });
        nervix_primitives::task::yield_now().await;
        assert!(
            !dispatch.is_finished(),
            "the acquisition deadline must not reopen an owned gate lease"
        );

        drop(lease);
        nervix_primitives::time::timeout(Duration::from_secs(1), dispatch)
            .await
            .expect("dropping the gate lease should admit dispatch")
            .expect("dispatch task should join");
    }
}

/// Hands every batch published into one relay to each of its live consumers.
///
/// Each consumer owns a lock-free queue and an admission count, so a publisher delivering a batch
/// and a consumer taking one meet only on atomics, and publishers into the same relay never
/// serialize on a shared lock. Capacity bounds each consumer's admitted backlog: a publisher waits
/// until every consumer has room, which is the backpressure of one shared queue that keeps a batch
/// until its slowest consumer takes it. A capacity change applies to admission alone, so shrinking
/// capacity below a consumer's backlog keeps every buffered batch and holds publishers until that
/// backlog drains below the new bound.
pub(crate) struct RelayBroadcast<T> {
    fanout: Arc<RelayFanout<T>>,
}

struct RelayFanout<T> {
    /// The live consumers in registration order.
    ///
    /// A publisher reserves admission in this order, so it holds a later consumer's reservation
    /// only while it holds every earlier one. Two publishers into one relay therefore never each
    /// hold a reservation that the other is waiting for.
    consumers: ArcSwap<Vec<Arc<RelayConsumerQueue<T>>>>,
    /// The admitted backlog each consumer may hold. It changes only together with a notification,
    /// so a publisher that enables its wait before reading it cannot miss a change.
    capacity: AtomicUsize,
    receiver_count: AtomicUsize,
    /// Publishers waiting in [`Self::admit`].
    ///
    /// `Notify::notify_waiters` takes the waiter-list lock on every call, so a consumer notifies
    /// only while this count shows a waiting publisher. A waiting publisher raises this count
    /// before it reads a consumer's admission count, and a consumer lowers its admission count
    /// before it reads this one. Both reach the admission count by read-modify-write, and those
    /// are totally ordered: when the consumer's comes first the publisher reads the freed room and
    /// is admitted, and when the publisher's comes first the consumer's acquires it and reads this
    /// count raised, so it wakes the publisher.
    waiting_publishers: AtomicUsize,
    /// Wakes waiting publishers when a consumer frees admission, capacity changes, or a consumer
    /// leaves.
    admission: Notify,
}

/// One consumer's share of a relay fan-out.
struct RelayConsumerQueue<T> {
    /// Batches delivered to this consumer and not yet taken.
    batches: ConcurrentQueue<T>,
    /// Batches admitted to this consumer and not yet taken, including any that a publisher has
    /// reserved and not yet delivered. Publishers gate on this count instead of the queue length,
    /// so two publishers cannot both claim one free slot.
    admitted: AtomicUsize,
    /// The waker of this consumer's only receiver, since a [`RelayReceiver`] cannot be cloned.
    delivered: AtomicWaker,
}

/// Counts one publisher as waiting for admission until its wait returns or is cancelled.
struct RelayAdmissionWait<'fanout, T> {
    fanout: &'fanout RelayFanout<T>,
}

/// Whether the admission a consumer returned frees room a publisher waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayAdmissionFreed {
    /// A publisher waits for room, so the consumer wakes the waiting publishers.
    PublisherWaiting,
    NobodyWaiting,
}

/// One consumer of a relay fan-out.
///
/// Dropping it leaves the fan-out, and the batches it has not taken are dropped with its queue.
pub(crate) struct RelayReceiver<T> {
    consumer: Arc<RelayConsumerQueue<T>>,
    fanout: Arc<RelayFanout<T>>,
}

/// What [`RelayReceiver::try_recv`] found without waiting.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::runtime) enum RelayTryRecv<T> {
    /// The oldest batch delivered to the receiver and not yet taken.
    Batch(T),
    /// Nothing is queued, and the fan-out can still deliver more.
    Empty,
    /// The fan-out is gone, and every batch it delivered has been taken.
    Closed,
}

/// A batch returned to its publisher because no consumer was left to take it.
pub(in crate::runtime) struct RelayFanoutClosed<T> {
    pub(in crate::runtime) batch: T,
}

impl<T> RelayBroadcast<T> {
    pub(crate) fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            fanout: Arc::new(RelayFanout {
                consumers: ArcSwap::from_pointee(Vec::new()),
                capacity: AtomicUsize::new(capacity.get()),
                receiver_count: AtomicUsize::new(0),
                waiting_publishers: AtomicUsize::new(0),
                admission: Notify::new(),
            }),
        }
    }

    /// Adds a consumer that receives every batch whose publishing begins after this call.
    pub(crate) fn new_receiver(&self) -> RelayReceiver<T> {
        let consumer = Arc::new(RelayConsumerQueue::new());
        self.fanout.register(&consumer);
        RelayReceiver {
            consumer,
            fanout: self.fanout.clone(),
        }
    }

    pub(crate) fn receiver_count(&self) -> usize {
        self.fanout.receiver_count.load(Ordering::Acquire)
    }

    /// The backlog of the slowest consumer, which is the number of batches the fan-out still holds.
    ///
    /// Every consumer receives every batch, so the backlog is a maximum over all of them and this
    /// visits each consumer once.
    pub(in crate::runtime) fn len(&self) -> usize {
        let mut backlog = 0;
        for consumer in self.fanout.consumers.load().iter() {
            backlog = backlog.max(consumer.admitted.load(Ordering::Acquire));
        }
        backlog
    }

    pub(in crate::runtime) fn capacity(&self) -> usize {
        self.fanout.capacity.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn waiting_publishers(&self) -> usize {
        self.fanout.waiting_publishers.load(Ordering::Relaxed)
    }

    /// Changes the admitted backlog each consumer may hold.
    ///
    /// Buffered batches are kept. A smaller capacity holds publishers until every consumer's
    /// backlog drains below it, and a larger one admits waiting publishers at once.
    pub(in crate::runtime) fn set_capacity(&self, capacity: NonZeroUsize) {
        self.fanout
            .capacity
            .store(capacity.get(), Ordering::Release);
        self.fanout.admission.notify_waiters();
    }

    /// Ends delivery to every consumer registered so far.
    ///
    /// Each of them takes the batches already delivered to it and then observes that nothing more
    /// will come, exactly as if the fan-out had been dropped. A publisher skips a closed consumer,
    /// including one it is already waiting on, so a consumer that never drains its backlog cannot
    /// hold the fan-out. A consumer registered afterwards receives every batch whose publishing
    /// begins after it registers, as any consumer does.
    pub(in crate::runtime) fn close_receivers(&self) {
        for consumer in self.fanout.consumers.load().iter() {
            consumer.batches.close();
            consumer.delivered.wake();
        }
        self.fanout.admission.notify_waiters();
    }
}

impl<T: Clone> RelayBroadcast<T> {
    /// Delivers `batch` to every consumer registered when publishing begins.
    ///
    /// Waits while any of those consumers is at capacity. A consumer that leaves during the wait is
    /// skipped, and the batch comes back only when no consumer is left to take it.
    pub(in crate::runtime) async fn broadcast(&self, batch: T) -> Result<(), RelayFanoutClosed<T>> {
        let consumers = self.fanout.consumers.load();
        let capacity = self.fanout.capacity.load(Ordering::Acquire);
        let mut first_full = None;
        for (index, consumer) in consumers.iter().enumerate() {
            if !consumer.try_admit(capacity) {
                first_full = Some(index);
                break;
            }
        }
        let Some(first_full) = first_full else {
            return RelayConsumerQueue::deliver_each(consumers.iter(), batch)
                .map_err(|batch| RelayFanoutClosed { batch });
        };

        // A consumer is at capacity and the wait may be long, so the consumer list is held by
        // reference count rather than through the short-lived load guard. Admissions already
        // reserved stay held while the remaining consumers are admitted in registration order.
        let consumers = Guard::into_inner(consumers);
        let (reserved, remaining) = consumers.split_at(first_full);
        let mut admitted: Vec<&Arc<RelayConsumerQueue<T>>> = reserved.iter().collect();
        for consumer in remaining {
            if self.fanout.admit(consumer).await {
                admitted.push(consumer);
            }
        }
        RelayConsumerQueue::deliver_each(admitted, batch)
            .map_err(|batch| RelayFanoutClosed { batch })
    }
}

#[cfg(test)]
impl<T: Clone> RelayBroadcast<T> {
    /// Publishes `batch` as a relay does, for tests outside the runtime. `false` means no
    /// consumer was left to take it.
    pub(crate) async fn publish_for_test(&self, batch: T) -> bool {
        self.broadcast(batch).await.is_ok()
    }
}

impl<T> RelayFanout<T> {
    /// Reserves one admission on `consumer`, waiting while it is at capacity.
    ///
    /// Returns `false` once the consumer has left the fan-out. The wait keeps every admission the
    /// publisher already reserved on earlier consumers.
    async fn admit(&self, consumer: &RelayConsumerQueue<T>) -> bool {
        let wait = RelayAdmissionWait::begin(self);
        loop {
            nervix_primitives::task::consume_budget().await;
            let admission = self.admission.notified();
            tokio::pin!(admission);
            admission.as_mut().enable();
            if consumer.batches.is_closed() {
                return false;
            }
            if consumer.try_admit_while_waiting(&wait, self.capacity.load(Ordering::Acquire)) {
                return true;
            }
            admission.await;
        }
    }

    /// Takes the oldest batch delivered to `consumer`, returns its admission, and reports whether a
    /// publisher waits for the room it freed.
    fn take_from(
        &self,
        consumer: &RelayConsumerQueue<T>,
    ) -> Result<(T, RelayAdmissionFreed), PopError> {
        let batch = consumer.take()?;
        // Relaxed: returning the admission was a read-modify-write, which acquires the read of a
        // publisher that raised this count before it.
        if self.waiting_publishers.load(Ordering::Relaxed) > 0 {
            return Ok((batch, RelayAdmissionFreed::PublisherWaiting));
        }
        Ok((batch, RelayAdmissionFreed::NobodyWaiting))
    }

    fn register(&self, consumer: &Arc<RelayConsumerQueue<T>>) {
        self.consumers.rcu(|consumers| {
            let count = consumers
                .len()
                .checked_add(1)
                .assured("a node cannot hold usize::MAX consumers of one relay");
            let mut registered = Vec::with_capacity(count);
            registered.extend(consumers.iter().cloned());
            registered.push(consumer.clone());
            registered
        });
        #[allow(deprecated)] // until try_update is stabilized
        self.receiver_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .assured("a node cannot hold usize::MAX consumers of one relay");
    }

    fn deregister(&self, consumer: &Arc<RelayConsumerQueue<T>>) {
        // Copy-on-write rebuilds the whole list on every change, so finding the leaving consumer
        // is part of that copy rather than a separate lookup.
        self.consumers.rcu(|consumers| {
            let mut remaining = Vec::with_capacity(consumers.len());
            for registered in consumers.iter() {
                if !Arc::ptr_eq(registered, consumer) {
                    remaining.push(registered.clone());
                }
            }
            remaining
        });
        #[allow(deprecated)] // until try_update is stabilized
        self.receiver_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .verified("the leaving consumer raised the count when it registered");
    }
}

impl<T> RelayConsumerQueue<T> {
    fn new() -> Self {
        Self {
            batches: ConcurrentQueue::unbounded(),
            admitted: AtomicUsize::new(0),
            delivered: AtomicWaker::new(),
        }
    }

    /// Reserves one admission, or reports that this consumer already holds `capacity`.
    fn try_admit(&self, capacity: usize) -> bool {
        self.admit_from(self.admitted.load(Ordering::Acquire), capacity)
    }

    /// Reserves one admission for a publisher whose wait has raised the waiting count, or reports
    /// that this consumer already holds `capacity`.
    ///
    /// The admission count is first read with a read-modify-write, which reads its newest value.
    /// A consumer that returned an admission before it is seen here, and one that returns an
    /// admission after it acquires it and reads the waiting count `wait` raised, so it wakes this
    /// publisher. A plain load could miss both: it may return a count from before a consumer's
    /// release that preceded it, while that consumer's read of the waiting count returns none.
    fn try_admit_while_waiting(&self, _wait: &RelayAdmissionWait<'_, T>, capacity: usize) -> bool {
        let admitted = self.admitted.fetch_add(0, Ordering::AcqRel);
        self.admit_from(admitted, capacity)
    }

    /// Raises the admission count from the value last read, unless that value already holds
    /// `capacity`.
    fn admit_from(&self, mut admitted: usize, capacity: usize) -> bool {
        loop {
            if admitted >= capacity {
                return false;
            }
            let raised = admitted
                .checked_add(1)
                .verified("the comparison above holds the count below the capacity");
            match self.admitted.compare_exchange_weak(
                admitted,
                raised,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => admitted = current,
            }
        }
    }

    /// Hands one admitted batch to this consumer and wakes its receiver.
    fn deliver(&self, batch: T) {
        match self.batches.push(batch) {
            Ok(()) => self.delivered.wake(),
            // The queue is unbounded, so a push fails only once the receiver has left. The
            // rejected batch is dropped here, and its admission is returned.
            Err(PushError::Closed(_) | PushError::Full(_)) => self.release_admission(),
        }
    }

    /// Takes the oldest delivered batch and returns its admission.
    fn take(&self) -> Result<T, PopError> {
        let batch = self.batches.pop()?;
        self.release_admission();
        Ok(batch)
    }

    /// Returns one admission. The read-modify-write releases the consumer's progress to a drain
    /// that reads the count after it, and acquires the read of a publisher whose wait raised the
    /// waiting count before it.
    fn release_admission(&self) {
        self.admitted
            .fetch_sub(1, Ordering::AcqRel)
            .checked_sub(1)
            .verified("every release follows the admission that raised the count");
    }
}

impl<T: Clone> RelayConsumerQueue<T> {
    /// Delivers `batch` to each admitted consumer, cloning it for every consumer but the last.
    ///
    /// Returns the batch when there is no consumer to deliver it to.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the gate reason or a delivery callback; local callback \
                      bodies remain analyzed"
        )
    )]
    fn deliver_each<'consumer>(
        consumers: impl IntoIterator<Item = &'consumer Arc<Self>>,
        batch: T,
    ) -> Result<(), T>
    where
        T: 'consumer,
    {
        let mut consumers = consumers.into_iter();
        let Some(mut current) = consumers.next() else {
            return Err(batch);
        };
        for next in consumers {
            current.deliver(batch.clone());
            current = next;
        }
        current.deliver(batch);
        Ok(())
    }
}

impl<'fanout, T> RelayAdmissionWait<'fanout, T> {
    /// Counts one publisher as waiting. Relaxed: the publisher's read-modify-write of a consumer's
    /// admission count, which follows in program order, releases the raised count to every
    /// consumer whose return of an admission comes after it.
    fn begin(fanout: &'fanout RelayFanout<T>) -> Self {
        fanout
            .waiting_publishers
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .assured("a node cannot hold usize::MAX publishers waiting on one relay");
        Self { fanout }
    }
}

impl<T> Drop for RelayAdmissionWait<'_, T> {
    fn drop(&mut self) {
        self.fanout
            .waiting_publishers
            .fetch_sub(1, Ordering::Relaxed)
            .checked_sub(1)
            .verified("this wait raised the count when it began");
    }
}

impl<T> Drop for RelayBroadcast<T> {
    fn drop(&mut self) {
        // Publishers borrow this handle, so none outlives it. Closing every queue lets each
        // receiver take what was already delivered and then observe that nothing more will come.
        for consumer in self.fanout.consumers.load().iter() {
            consumer.batches.close();
            consumer.delivered.wake();
        }
    }
}

impl<T> fmt::Debug for RelayBroadcast<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayBroadcast")
            .field("capacity", &self.capacity())
            .field("receiver_count", &self.receiver_count())
            .field("len", &self.len())
            .finish()
    }
}

impl<T> RelayReceiver<T> {
    /// Waits for the next batch, or returns `None` once the fan-out is gone and drained.
    pub(crate) async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(in crate::runtime) fn try_recv(&mut self) -> RelayTryRecv<T> {
        match self.take() {
            Ok(batch) => RelayTryRecv::Batch(batch),
            Err(PopError::Empty) => RelayTryRecv::Empty,
            Err(PopError::Closed) => RelayTryRecv::Closed,
        }
    }

    pub(in crate::runtime) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.take() {
            Ok(batch) => return Poll::Ready(Some(batch)),
            Err(PopError::Closed) => return Poll::Ready(None),
            Err(PopError::Empty) => {}
        }
        self.consumer.delivered.register(cx.waker());
        // A delivery that landed after the attempt above and before the registration woke no
        // one, so look again now that the waker is registered.
        match self.take() {
            Ok(batch) => Poll::Ready(Some(batch)),
            Err(PopError::Closed) => Poll::Ready(None),
            Err(PopError::Empty) => Poll::Pending,
        }
    }

    /// Batches delivered to this receiver and not yet taken.
    pub(in crate::runtime) fn len(&self) -> usize {
        self.consumer.batches.len()
    }

    fn take(&self) -> Result<T, PopError> {
        let (batch, freed) = self.fanout.take_from(&self.consumer)?;
        if freed == RelayAdmissionFreed::PublisherWaiting {
            self.fanout.admission.notify_waiters();
        }
        Ok(batch)
    }
}

impl<T> Drop for RelayReceiver<T> {
    fn drop(&mut self) {
        // Leave the consumer list before closing the queue, so a publisher that loads the list
        // afterwards never admits to this receiver. A publisher still holding the earlier list
        // finds the queue closed, and a waiting one is woken to notice.
        self.fanout.deregister(&self.consumer);
        self.consumer.batches.close();
        self.fanout.admission.notify_waiters();
    }
}

impl<T> fmt::Debug for RelayReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayReceiver")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for RelayFanoutClosed<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayFanoutClosed")
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Display for RelayFanoutClosed<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("no relay consumer is left to take the batch")
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::{RelayBroadcast, RelayTryRecv};

    fn capacity(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("test capacities are nonzero")
    }

    #[nervix_primitives::test]
    async fn every_consumer_receives_each_batch_in_publish_order() {
        let channel = RelayBroadcast::with_capacity(capacity(3));
        let mut first = channel.new_receiver();
        let mut second = channel.new_receiver();
        assert_eq!(channel.receiver_count(), 2);

        for value in 1..=3 {
            channel
                .broadcast(value)
                .await
                .expect("both consumers have room");
        }
        assert_eq!(channel.len(), 3);

        for expected in 1..=3 {
            assert_eq!(first.recv().await, Some(expected));
        }
        assert_eq!(
            channel.len(),
            3,
            "the fan-out holds every batch the slower consumer has not taken"
        );
        for expected in 1..=3 {
            assert_eq!(second.recv().await, Some(expected));
        }
        assert_eq!(channel.len(), 0);
    }

    #[nervix_primitives::test]
    async fn a_receiver_only_receives_batches_published_after_it_joins() {
        let channel = RelayBroadcast::with_capacity(capacity(2));
        let mut early = channel.new_receiver();
        channel
            .broadcast(1)
            .await
            .expect("the early consumer has room");
        let mut late = channel.new_receiver();
        channel
            .broadcast(2)
            .await
            .expect("both consumers have room");

        assert_eq!(early.recv().await, Some(1));
        assert_eq!(early.recv().await, Some(2));
        assert_eq!(late.recv().await, Some(2));
        assert_eq!(late.try_recv(), RelayTryRecv::Empty);
    }

    #[nervix_primitives::test]
    async fn publishing_without_consumers_returns_the_batch() {
        let channel = RelayBroadcast::with_capacity(capacity(1));
        let returned = channel
            .broadcast(7)
            .await
            .expect_err("no consumer can take the batch");
        assert_eq!(returned.batch, 7);

        drop(channel.new_receiver());
        assert_eq!(channel.receiver_count(), 0);
        let returned = channel
            .broadcast(8)
            .await
            .expect_err("the only consumer has left");
        assert_eq!(returned.batch, 8);
    }

    #[nervix_primitives::test]
    async fn dropping_the_fanout_closes_receivers_after_they_drain() {
        let channel = RelayBroadcast::with_capacity(capacity(2));
        let mut receiver = channel.new_receiver();
        channel.broadcast(1).await.expect("the consumer has room");
        drop(channel);

        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(receiver.try_recv(), RelayTryRecv::Closed);
    }

    #[nervix_primitives::test]
    async fn closing_receivers_ends_them_after_they_drain_and_releases_their_publisher() {
        let channel = RelayBroadcast::with_capacity(capacity(1));
        let mut closed = channel.new_receiver();
        channel.broadcast(1).await.expect("the consumer has room");

        // The consumer is at capacity, so this publisher waits on it until it is closed.
        let publishing = channel.broadcast(2);
        tokio::pin!(publishing);
        assert!(
            futures_util::poll!(publishing.as_mut()).is_pending(),
            "a full consumer holds its publisher"
        );
        channel.close_receivers();
        let returned = publishing
            .await
            .expect_err("the only consumer was closed, so none is left to take the batch");
        assert_eq!(returned.batch, 2);

        assert_eq!(closed.recv().await, Some(1));
        assert_eq!(closed.recv().await, None);

        let mut joined_later = channel.new_receiver();
        channel
            .broadcast(3)
            .await
            .expect("a consumer registered after the close has room");
        assert_eq!(joined_later.recv().await, Some(3));
        assert_eq!(closed.try_recv(), RelayTryRecv::Closed);
    }

    #[nervix_primitives::test]
    async fn shrinking_capacity_preserves_buffered_batches() {
        let channel = RelayBroadcast::with_capacity(capacity(3));
        let mut receiver = channel.new_receiver();
        for value in 1..=3 {
            channel
                .broadcast(value)
                .await
                .expect("the consumer has room");
        }

        channel.set_capacity(capacity(1));
        assert_eq!(channel.capacity(), 1);
        assert_eq!(channel.len(), 3);

        for expected in 1..=3 {
            assert_eq!(receiver.recv().await, Some(expected));
        }
        assert_eq!(channel.len(), 0);
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "relay_channel_shuttle_tests.rs"]
mod shuttle_tests;

/// A fence deadline that no Loom model reaches.
///
/// An engagement's deadline is the runtime's real time, which is outside every model, so a model
/// engages far enough ahead that the deadline never decides the fence it explores.
#[cfg(all(test, feature = "loom"))]
fn deadline_no_model_reaches() -> Instant {
    Instant::now()
        .checked_add(std::time::Duration::from_secs(24 * 60 * 60))
        .assured("a day from now is representable on every supported clock")
}

#[cfg(all(test, feature = "loom"))]
#[path = "relay_channel_loom_models.rs"]
mod loom_models;
