//! Relay dispatch fencing and the contentionless relay fan-out.
//!
//! Layer: data plane.
//! - **Owns.** The dispatch gate that fences relay delivery while the runtime mutates a relay, and
//!   the fan-out that hands every batch published into one relay to each of its live consumers
//!   under bounded, resizable backpressure.
//! - **Depends on.** Tokio synchronization, lock-free queues and wakers, `arc-swap`, and panic
//!   classification.
//! - **Must not know.** Relays, branches, batches, acknowledgements, placement, or any Model.

#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{
    collections::BTreeMap,
    fmt,
    future::poll_fn,
    num::NonZeroUsize,
    task::{Context, Poll},
};

use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use futures_util::task::AtomicWaker;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::sync::{ArcSwap, Guard};
use parking_lot::Mutex;
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::{
    sync::Notify,
    time::{Instant, timeout_at},
};
use tracing::debug;
use triomphe::Arc;

#[derive(Debug)]
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
/// Dispatch acquisition increments the in-flight count before inspecting the closed flag, while
/// gate engagement closes the gate before inspecting the count. Those operations are sequentially
/// consistent, so one side must observe the other: a dispatch either receives a permit that the
/// fence counts or rolls its increment back and waits for the engagement to end. Dropping every
/// permit counted by the fence completes it.
#[derive(Debug)]
pub(in crate::runtime) struct RelayDispatchPermit<'gate> {
    gate: &'gate RelayDispatchGate,
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
        // Paired with the sequentially consistent counter increment and closed load in
        // `acquire_dispatch`, and the counter load in `wait_quiescent`. This total order prevents
        // an acquisition and an engagement from both missing one another.
        self.closed.store(true, Ordering::SeqCst);
        drop(state);
        self.changed.notify_waiters();
        generation
    }

    pub(super) fn release(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.engagements.remove(&generation).is_none() {
            return;
        }
        self.closed
            .store(!state.engagements.is_empty(), Ordering::Release);
        drop(state);
        self.changed.notify_waiters();
    }

    pub(in crate::runtime) async fn acquire_dispatch(&self) -> RelayDispatchPermit<'_> {
        loop {
            tokio::task::consume_budget().await;
            self.increment_in_flight_dispatches();
            if !self.closed.load(Ordering::SeqCst) {
                return RelayDispatchPermit { gate: self };
            }
            self.decrement_in_flight_dispatches();

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

    /// Whether an ownership or lifecycle operation currently fences this relay.
    pub(in crate::runtime) fn is_engaged(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Waits for all dispatch permits acquired before `generation` was engaged to be dropped.
    ///
    /// `false` means this engagement was released or reached its deadline before the fence
    /// completed. Callers must not tear down relay consumers when the fence did not complete.
    async fn wait_quiescent(&self, generation: u64) -> bool {
        loop {
            tokio::task::consume_budget().await;
            self.clear_if_expired();
            let changed = self.changed.notified();
            let drained = self.drained.notified();
            tokio::pin!(changed);
            tokio::pin!(drained);
            changed.as_mut().enable();
            drained.as_mut().enable();
            let deadline = {
                let mut state = self.state.lock();
                let in_flight_dispatches = self.in_flight_dispatches.load(Ordering::SeqCst);
                let Some(engagement) = state.engagements.get_mut(&generation) else {
                    return false;
                };
                match engagement.phase {
                    RelayDispatchGateEngagementPhase::Leased => return true,
                    RelayDispatchGateEngagementPhase::Fencing { deadline } => {
                        if Instant::now() >= deadline {
                            drop(state);
                            self.clear_if_expired();
                            return false;
                        }
                        if in_flight_dispatches == 0 {
                            engagement.phase = RelayDispatchGateEngagementPhase::Leased;
                            return true;
                        }
                        deadline
                    }
                }
            };
            let woken = async {
                tokio::select! {
                    () = changed.as_mut() => {}
                    () = drained.as_mut() => {}
                }
            };
            if timeout_at(deadline, woken).await.is_err() {
                self.clear_if_expired();
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
            tokio::task::consume_budget().await;
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
            tokio::task::consume_budget().await;
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
        self.in_flight_dispatches.load(Ordering::SeqCst)
    }

    fn increment_in_flight_dispatches(&self) {
        self.in_flight_dispatches
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(1)
            })
            .assured("a process cannot hold usize::MAX live relay dispatch permits");
    }

    fn decrement_in_flight_dispatches(&self) {
        let previous = self
            .in_flight_dispatches
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_sub(1)
            })
            .verified("this permit or rolled-back acquisition raised the count");
        if previous == 1 && self.closed.load(Ordering::SeqCst) {
            self.drained.notify_waiters();
        }
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
        self.closed
            .store(!state.engagements.is_empty(), Ordering::Release);
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
        self.gate.decrement_in_flight_dispatches();
    }
}

#[cfg(test)]
mod gate_tests {
    use std::time::Duration;

    use tokio::time::Instant;
    use triomphe::Arc;

    use super::{RelayDispatchGate, RelayDispatchGateLease};

    fn engage(
        gate: &Arc<RelayDispatchGate>,
        deadline: Instant,
        reason: &str,
    ) -> RelayDispatchGateLease {
        RelayDispatchGateLease::engage(gate.clone(), deadline, reason)
    }

    #[tokio::test]
    async fn relay_dispatch_gate_releases_waiters_explicitly() {
        let gate = Arc::new(RelayDispatchGate::new());
        let lease = engage(&gate, Instant::now() + Duration::from_secs(1), "node swap");
        assert!(gate.is_closed());
        assert_eq!(gate.reason().as_deref(), Some("node swap"));

        drop(lease);
        gate.wait_open().await;
        assert!(!gate.is_closed());
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn acquired_gate_lease_outlives_its_fence_deadline() {
        let gate = Arc::new(RelayDispatchGate::new());
        let deadline = Instant::now() + Duration::from_millis(10);
        let mut lease = engage(&gate, deadline, "slow node swap");
        assert!(lease.wait_quiescent().await);

        tokio::time::sleep_until(deadline + Duration::from_millis(10)).await;
        assert!(gate.is_closed());

        let dispatch = tokio::spawn({
            let gate = gate.clone();
            async move {
                let _permit = gate.acquire_dispatch().await;
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !dispatch.is_finished(),
            "the acquisition deadline must not reopen an owned gate lease"
        );

        drop(lease);
        tokio::time::timeout(Duration::from_secs(1), dispatch)
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
    /// only while this count shows a waiting publisher. A waiting publisher raises the count before
    /// it reads a consumer's admission count, and a consumer lowers its admission count before it
    /// reads this one. Both sides use sequentially consistent operations, so one of them observes
    /// the other: the publisher is admitted, or the consumer wakes it.
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

    pub(in crate::runtime) fn receiver_count(&self) -> usize {
        self.fanout.receiver_count.load(Ordering::Acquire)
    }

    /// The backlog of the slowest consumer, which is the number of batches the fan-out still holds.
    ///
    /// Every consumer receives every batch, so the backlog is a maximum over all of them and this
    /// visits each consumer once.
    pub(in crate::runtime) fn len(&self) -> usize {
        let mut backlog = 0;
        for consumer in self.fanout.consumers.load().iter() {
            backlog = backlog.max(consumer.admitted.load(Ordering::SeqCst));
        }
        backlog
    }

    pub(in crate::runtime) fn capacity(&self) -> usize {
        self.fanout.capacity.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn waiting_publishers(&self) -> usize {
        self.fanout.waiting_publishers.load(Ordering::SeqCst)
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

impl<T> RelayFanout<T> {
    /// Reserves one admission on `consumer`, waiting while it is at capacity.
    ///
    /// Returns `false` once the consumer has left the fan-out. The wait keeps every admission the
    /// publisher already reserved on earlier consumers.
    async fn admit(&self, consumer: &RelayConsumerQueue<T>) -> bool {
        let _wait = RelayAdmissionWait::begin(self);
        loop {
            tokio::task::consume_budget().await;
            let admission = self.admission.notified();
            tokio::pin!(admission);
            admission.as_mut().enable();
            if consumer.batches.is_closed() {
                return false;
            }
            if consumer.try_admit(self.capacity.load(Ordering::Acquire)) {
                return true;
            }
            admission.await;
        }
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
        let admission =
            self.admitted
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |admitted| {
                    if admitted >= capacity {
                        return None;
                    }
                    Some(
                        admitted
                            .checked_add(1)
                            .verified("the comparison above holds the count below the capacity"),
                    )
                });
        admission.is_ok()
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

    fn release_admission(&self) {
        self.admitted
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |admitted| {
                admitted.checked_sub(1)
            })
            .verified("every release follows the admission that raised the count");
    }
}

impl<T: Clone> RelayConsumerQueue<T> {
    /// Delivers `batch` to each admitted consumer, cloning it for every consumer but the last.
    ///
    /// Returns the batch when there is no consumer to deliver it to.
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
    fn begin(fanout: &'fanout RelayFanout<T>) -> Self {
        fanout
            .waiting_publishers
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |waiting| {
                waiting.checked_add(1)
            })
            .assured("a node cannot hold usize::MAX publishers waiting on one relay");
        Self { fanout }
    }
}

impl<T> Drop for RelayAdmissionWait<'_, T> {
    fn drop(&mut self) {
        self.fanout
            .waiting_publishers
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |waiting| {
                waiting.checked_sub(1)
            })
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
        let batch = self.consumer.take()?;
        // Paired with the sequentially consistent count that `RelayAdmissionWait::begin` raises.
        if self.fanout.waiting_publishers.load(Ordering::SeqCst) > 0 {
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn dropping_the_fanout_closes_receivers_after_they_drain() {
        let channel = RelayBroadcast::with_capacity(capacity(2));
        let mut receiver = channel.new_receiver();
        channel.broadcast(1).await.expect("the consumer has room");
        drop(channel);

        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(receiver.try_recv(), RelayTryRecv::Closed);
    }

    #[tokio::test]
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
