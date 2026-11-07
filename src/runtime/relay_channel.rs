//! Relay dispatch fencing and the contentionless relay fan-out.
//!
//! Layer: data plane.
//! - **Owns.** The dispatch gate that fences relay delivery while the runtime mutates a relay, and
//!   the fan-out that hands each batch to its selected live consumers
//!   under bounded, resizable backpressure.
//! - **Depends on.** The primitive boundary's synchronization, publication, queues, wakers and
//!   timers, and panic classification.
//! - **Must not know.** Relays, branches, batches, acknowledgements, placement, or any Model.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{
    collections::BTreeMap,
    fmt,
    future::poll_fn,
    num::NonZeroUsize,
    task::{Context, Poll},
};

use meticulous::OptionExt as _;
use nervix_primitives::{
    collections::{ConcurrentQueue, PopError, PushError},
    publication::{ArcSwap, Guard},
    sync::{
        Arc, AtomicWaker, Notify, StdArc,
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
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

/// One opaque audience in a fan-out. Product identities stay with the caller that owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::runtime) struct RelayAudienceId(usize);

pub(in crate::runtime) struct RelayAudience<T> {
    fanout: Arc<RelayFanout<T>>,
    id: RelayAudienceId,
}

#[derive(Debug, thiserror::Error)]
#[error("the relay fan-out exhausted its audience identity space")]
pub(in crate::runtime) struct RelayAudienceExhausted;

/// One publication of the registrations, including their audience projections.
/// An ordinary fan-out allocates no audience map. Counts and delivery select the same snapshot.
pub(in crate::runtime) struct RelayConsumerSet<T> {
    global: Vec<Arc<RelayConsumerQueue<T>>>,
    audiences: Option<Box<RelayAudiences<T>>>,
}

struct RelayAudiences<T> {
    next_id: usize,
    groups: BTreeMap<RelayAudienceId, RelayAudienceConsumers<T>>,
}

struct RelayAudienceConsumers<T> {
    dedicated: Vec<Arc<RelayConsumerQueue<T>>>,
    publication: Vec<Arc<RelayConsumerQueue<T>>>,
}

impl<T> Clone for RelayAudienceConsumers<T> {
    fn clone(&self) -> Self {
        Self {
            dedicated: self.dedicated.clone(),
            publication: self.publication.clone(),
        }
    }
}

impl<T> Clone for RelayAudiences<T> {
    fn clone(&self) -> Self {
        Self {
            next_id: self.next_id,
            groups: self.groups.clone(),
        }
    }
}

impl<T> Clone for RelayConsumerSet<T> {
    fn clone(&self) -> Self {
        Self {
            global: self.global.clone(),
            audiences: self.audiences.clone(),
        }
    }
}

impl<T> RelayConsumerSet<T> {
    fn all(&self) -> impl Iterator<Item = &Arc<RelayConsumerQueue<T>>> {
        let groups = self
            .audiences
            .iter()
            .flat_map(|audiences| audiences.groups.values());
        let dedicated = groups.flat_map(|group| group.dedicated.iter());
        self.global.iter().chain(dedicated)
    }

    fn with_receiver(
        &self,
        consumer: &Arc<RelayConsumerQueue<T>>,
        audience: Option<RelayAudienceId>,
    ) -> Self {
        if audience.is_none() && self.audiences.is_none() {
            let capacity = self
                .global
                .len()
                .checked_add(1)
                .assured("a live receiver occupies memory before it can register");
            let mut global = Vec::with_capacity(capacity);
            global.extend(self.global.iter().cloned());
            global.push(consumer.clone());
            return Self {
                global,
                audiences: None,
            };
        }
        let mut next = self.clone();
        if let Some(audience) = audience {
            let group = next
                .audiences
                .as_mut()
                .assured("an audience is published before it registers")
                .groups
                .get_mut(&audience)
                .assured("an audience remains registered for its fan-out lifetime");
            group.dedicated.push(consumer.clone());
            group.publication.push(consumer.clone());
        } else {
            next.global.push(consumer.clone());
            if let Some(audiences) = &mut next.audiences {
                for group in audiences.groups.values_mut() {
                    group.publication.push(consumer.clone());
                }
            }
        }
        next
    }

    fn without_receiver(&self, consumer: &Arc<RelayConsumerQueue<T>>) -> Self {
        if self.audiences.is_none() {
            let mut global = Vec::with_capacity(self.global.len());
            global.extend(
                self.global
                    .iter()
                    .filter(|registered| !Arc::ptr_eq(registered, consumer))
                    .cloned(),
            );
            return Self {
                global,
                audiences: None,
            };
        }
        let mut next = self.clone();
        next.global
            .retain(|registered| !Arc::ptr_eq(registered, consumer));
        if let Some(audiences) = &mut next.audiences {
            for group in audiences.groups.values_mut() {
                group
                    .dedicated
                    .retain(|registered| !Arc::ptr_eq(registered, consumer));
                group
                    .publication
                    .retain(|registered| !Arc::ptr_eq(registered, consumer));
            }
        }
        next
    }

    fn with_audience(
        &self,
    ) -> error_stack::Result<(Self, RelayAudienceId), RelayAudienceExhausted> {
        let mut next = self.clone();
        let audiences = next.audiences.get_or_insert_with(|| {
            Box::new(RelayAudiences {
                next_id: 0,
                groups: BTreeMap::new(),
            })
        });
        let id = RelayAudienceId(audiences.next_id);
        audiences.next_id = audiences
            .next_id
            .checked_add(1)
            .ok_or_else(|| error_stack::Report::new(RelayAudienceExhausted))?;
        audiences.groups.insert(
            id,
            RelayAudienceConsumers {
                dedicated: Vec::new(),
                publication: self.global.clone(),
            },
        );
        Ok((next, id))
    }
}

#[derive(Clone, Copy)]
pub(in crate::runtime) struct AllRelayConsumers;

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "selection borrows one immutable registration projection without synchronization \
                  or lifecycle work"
    )
)]
pub(in crate::runtime) trait RelayConsumerSelection<T>: Copy {
    fn select(self, registrations: &RelayConsumerSet<T>) -> &[Arc<RelayConsumerQueue<T>>];
}

impl<T> RelayConsumerSelection<T> for AllRelayConsumers {
    fn select(self, registrations: &RelayConsumerSet<T>) -> &[Arc<RelayConsumerQueue<T>>] {
        &registrations.global
    }
}

impl<T> RelayConsumerSelection<T> for RelayAudienceId {
    fn select(self, registrations: &RelayConsumerSet<T>) -> &[Arc<RelayConsumerQueue<T>>] {
        let audiences = registrations
            .audiences
            .as_ref()
            .assured("an audience was installed before use");
        &audiences
            .groups
            .get(&self)
            .assured("an audience remains installed until its fan-out ends")
            .publication
    }
}

struct RelayFanout<T> {
    /// The live consumers in registration order.
    ///
    /// A publisher reserves admission in this order, so it holds a later consumer's reservation
    /// only while it holds every earlier one. Two publishers into one relay therefore never each
    /// hold a reservation that the other is waiting for.
    consumers: ArcSwap<RelayConsumerSet<T>>,
    /// The admitted backlog each consumer may hold. It changes only together with a notification,
    /// so a publisher that enables its wait before reading it cannot miss a change.
    capacity: AtomicUsize,
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
pub(in crate::runtime) struct RelayConsumerQueue<T> {
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

/// Admissions reserved before a publisher can deliver to every selected consumer. Cancelling
/// that publisher returns them, including room a sibling audience needs in a common queue.
struct RelayPendingAdmissions<'a, T> {
    fanout: &'a RelayFanout<T>,
    consumers: Vec<&'a Arc<RelayConsumerQueue<T>>>,
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

/// One publication into a relay fan-out: the consumers registered when it began, which it both
/// counts and delivers to. A consumer that registers later receives later publications only, so
/// whatever a publisher prepares for each counted consumer matches the deliveries.
pub(in crate::runtime) struct RelayPublication<'fanout, T, Selection = AllRelayConsumers> {
    fanout: &'fanout RelayFanout<T>,
    consumers: Guard<StdArc<RelayConsumerSet<T>>>,
    selection: Selection,
}

impl<T> RelayBroadcast<T> {
    pub(crate) fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            fanout: Arc::new(RelayFanout {
                consumers: ArcSwap::from_pointee(RelayConsumerSet {
                    global: Vec::new(),
                    audiences: None,
                }),
                capacity: AtomicUsize::new(capacity.get()),
                waiting_publishers: AtomicUsize::new(0),
                admission: Notify::new(),
            }),
        }
    }

    /// Adds a consumer that receives every batch whose publishing begins after this call.
    pub(crate) fn new_receiver(&self) -> RelayReceiver<T> {
        let consumer = Arc::new(RelayConsumerQueue::new());
        self.fanout.register(&consumer, None);
        RelayReceiver {
            consumer,
            fanout: self.fanout.clone(),
        }
    }

    /// Creates a fixed audience. Its publications include the common consumers and its own
    /// consumers, each once. Common consumers keep one queue across all audiences.
    pub(in crate::runtime) fn new_audience(
        &self,
    ) -> error_stack::Result<RelayAudience<T>, RelayAudienceExhausted> {
        loop {
            let observed = self.fanout.consumers.load_full();
            let (next, id) = observed.with_audience()?;
            let published = self
                .fanout
                .consumers
                .compare_and_swap(&observed, StdArc::new(next));
            if StdArc::ptr_eq(&published, &observed) {
                return Ok(RelayAudience {
                    fanout: self.fanout.clone(),
                    id,
                });
            }
        }
    }

    pub(crate) fn receiver_count(&self) -> usize {
        self.fanout.consumers.load().global.len()
    }

    /// The maximum backlog among the common and dedicated consumers, visiting each queue once.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the closed immutable registration iterator visits only this module's queue \
                      handles; its callbacks do not run operator code"
        )
    )]
    pub(in crate::runtime) fn len(&self) -> usize {
        let mut backlog = 0;
        for consumer in self.fanout.consumers.load().all() {
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
        // Every capacity mutation participates in the lifetime handshake: a resize between an
        // audience registration and shutdown must carry that registration through to the ending.
        self.fanout.capacity.swap(capacity.get(), Ordering::AcqRel);
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
        for consumer in self.fanout.consumers.load().all() {
            consumer.batches.close();
            consumer.delivered.wake();
        }
        self.fanout.admission.notify_waiters();
    }
}

impl<T> RelayAudience<T> {
    /// Queued or reserved batches belonging only to this audience. Common consumers have their
    /// own lifecycle and cannot keep an unrelated partition's handoff waiting.
    pub(in crate::runtime) fn dedicated_len(&self) -> usize {
        let registrations = self.fanout.consumers.load();
        let audiences = registrations
            .audiences
            .as_ref()
            .verified("the audience was published");
        audiences
            .groups
            .get(&self.id)
            .verified("the audience remains published")
            .dedicated
            .iter()
            .map(|consumer| consumer.admitted.load(Ordering::Acquire))
            .max()
            .unwrap_or(0)
    }

    pub(in crate::runtime) fn new_receiver(&self) -> RelayReceiver<T> {
        let consumer = Arc::new(RelayConsumerQueue::new());
        self.fanout.register(&consumer, Some(self.id));
        if !self.fanout.audience_registration_is_live() {
            consumer.batches.close();
        }
        RelayReceiver {
            consumer,
            fanout: self.fanout.clone(),
        }
    }

    pub(in crate::runtime) fn publication(&self) -> RelayPublication<'_, T, RelayAudienceId> {
        RelayPublication {
            fanout: &self.fanout,
            consumers: self.fanout.consumers.load(),
            selection: self.id,
        }
    }
}

impl<T> Clone for RelayAudience<T> {
    fn clone(&self) -> Self {
        Self {
            fanout: self.fanout.clone(),
            id: self.id,
        }
    }
}

impl<T> fmt::Debug for RelayAudience<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayAudience")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<T> RelayBroadcast<T> {
    /// Begins one publication into the consumers registered now.
    pub(in crate::runtime) fn publication(&self) -> RelayPublication<'_, T> {
        RelayPublication {
            fanout: &self.fanout,
            consumers: self.fanout.consumers.load(),
            selection: AllRelayConsumers,
        }
    }
}

impl<T: Clone> RelayBroadcast<T> {
    /// Delivers `batch` to every consumer registered when publishing begins.
    pub(in crate::runtime) async fn broadcast(&self, batch: T) -> Result<(), RelayFanoutClosed<T>> {
        self.publication().broadcast(batch).await
    }
}

impl<T, Selection: RelayConsumerSelection<T>> RelayPublication<'_, T, Selection> {
    /// How many consumers this publication delivers to.
    pub(in crate::runtime) fn receivers(&self) -> usize {
        self.selection.select(&self.consumers).len()
    }
}

impl<T: Clone, Selection: RelayConsumerSelection<T>> RelayPublication<'_, T, Selection> {
    /// Delivers `batch` to every consumer this publication counted.
    ///
    /// Waits while any of those consumers is at capacity. A consumer that leaves during the wait is
    /// skipped, and the batch comes back only when no consumer is left to take it.
    pub(in crate::runtime) async fn broadcast(self, batch: T) -> Result<(), RelayFanoutClosed<T>> {
        let Self {
            fanout,
            consumers,
            selection,
        } = self;
        let selected = selection.select(&consumers);
        let capacity = fanout.capacity.load(Ordering::Acquire);
        let mut first_full = None;
        for (index, consumer) in selected.iter().enumerate() {
            if !consumer.try_admit(capacity) {
                first_full = Some(index);
                break;
            }
        }
        let Some(first_full) = first_full else {
            return RelayConsumerQueue::deliver_each(selected.iter(), batch)
                .map_err(|batch| RelayFanoutClosed { batch });
        };

        // A consumer is at capacity and the wait may be long, so the consumer list is held by
        // reference count rather than through the short-lived load guard. Admissions already
        // reserved stay held while the remaining consumers are admitted in registration order.
        let consumers = Guard::into_inner(consumers);
        let (reserved, remaining) = selection.select(&consumers).split_at(first_full);
        let mut admitted = RelayPendingAdmissions {
            fanout,
            consumers: reserved.iter().collect(),
        };
        for consumer in remaining {
            if fanout.admit(consumer).await {
                admitted.consumers.push(consumer);
            }
        }
        RelayConsumerQueue::deliver_each(std::mem::take(&mut admitted.consumers), batch)
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
    /// Meets the parent's ending after publishing an audience's receiver. If this read-modify-write
    /// comes first, the ending acquires the registration; otherwise this read acquires the ending
    /// and the caller closes its own queue. Resizing keeps that release sequence intact.
    fn audience_registration_is_live(&self) -> bool {
        self.capacity.fetch_add(0, Ordering::AcqRel) != 0
    }

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

    fn register(&self, consumer: &Arc<RelayConsumerQueue<T>>, audience: Option<RelayAudienceId>) {
        self.consumers
            .rcu(|consumers| consumers.with_receiver(consumer, audience));
    }

    fn deregister(&self, consumer: &Arc<RelayConsumerQueue<T>>) {
        // Copy-on-write rebuilds the whole list on every change, so finding the leaving consumer
        // is part of that copy rather than a separate lookup.
        self.consumers
            .rcu(|consumers| consumers.without_receiver(consumer));
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

impl<T> Drop for RelayPendingAdmissions<'_, T> {
    fn drop(&mut self) {
        for consumer in &self.consumers {
            consumer.release_admission();
        }
        // Returning an admission uses the same read-modify-write as a receiver taking a batch.
        // It acquires an earlier waiting publisher's raised count; a publisher that comes later
        // observes the returned room in its own read-modify-write before deciding to sleep.
        if !self.consumers.is_empty() && self.fanout.waiting_publishers.load(Ordering::Relaxed) > 0
        {
            self.fanout.admission.notify_waiters();
        }
    }
}

impl<T> Drop for RelayBroadcast<T> {
    fn drop(&mut self) {
        // Acquire a retained audience's preceding registration; a later registration acquires
        // this ending swap and closes its own queue. No retained audience can reopen delivery.
        self.fanout.capacity.swap(0, Ordering::AcqRel);
        // Each receiver takes what was already delivered and then observes the ending.
        for consumer in self.fanout.consumers.load().all() {
            consumer.batches.close();
            consumer.delivered.wake();
        }
        self.fanout.admission.notify_waiters();
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::num::NonZeroUsize;

    use meticulous::ResultExt as _;

    use super::{RelayBroadcast, RelayTryRecv};

    #[nervix_primitives::test]
    async fn audiences_isolate_dedicated_consumers_and_share_one_common_queue() {
        let channel = RelayBroadcast::with_capacity(capacity(1));
        let first = channel
            .new_audience()
            .assured("two audiences fit in memory");
        let second = channel
            .new_audience()
            .assured("two audiences fit in memory");
        let mut common = channel.new_receiver();
        let mut left = first.new_receiver();
        let mut right = second.new_receiver();
        first
            .publication()
            .broadcast(11)
            .await
            .assured("all selected queues are empty");
        assert_eq!(left.recv().await, Some(11));
        assert_eq!(right.try_recv(), RelayTryRecv::Empty);

        let publication = second.publication();
        assert_eq!(publication.receivers(), 2);
        let blocked = publication.broadcast(22);
        tokio::pin!(blocked);
        assert!(
            futures_util::poll!(blocked.as_mut()).is_pending(),
            "the common reader retains its one-slot bound across audiences"
        );
        assert_eq!(common.recv().await, Some(11));
        blocked.await.assured("the common reader returned its slot");
        assert_eq!(right.recv().await, Some(22));
        assert_eq!(common.recv().await, Some(22));
        assert_eq!(left.try_recv(), RelayTryRecv::Empty);
    }

    #[nervix_primitives::test]
    async fn an_audience_counts_and_delivers_the_same_registration_snapshot() {
        let channel = RelayBroadcast::with_capacity(capacity(2));
        let audience = channel
            .new_audience()
            .assured("one audience fits in memory");
        let mut first = audience.new_receiver();
        let publication = audience.publication();
        let mut joined = audience.new_receiver();
        let mut common = channel.new_receiver();
        assert_eq!(publication.receivers(), 1);
        publication
            .broadcast(7)
            .await
            .assured("the original receiver has room");
        assert_eq!(first.recv().await, Some(7));
        assert_eq!(joined.try_recv(), RelayTryRecv::Empty);
        assert_eq!(common.try_recv(), RelayTryRecv::Empty);
        assert_eq!(audience.publication().receivers(), 3);
    }

    #[nervix_primitives::test]
    async fn cancelling_a_blocked_audience_returns_its_common_queue_reservation() {
        let channel = RelayBroadcast::with_capacity(capacity(1));
        let first = channel
            .new_audience()
            .assured("two audiences fit in memory");
        let second = channel
            .new_audience()
            .assured("two audiences fit in memory");
        let mut common = channel.new_receiver();
        let _blocked_consumer = first.new_receiver();
        let _ready_consumer = second.new_receiver();
        first
            .publication()
            .broadcast(1)
            .await
            .assured("the queues start empty");
        assert_eq!(common.recv().await, Some(1));
        {
            let pending = first.publication().broadcast(2);
            tokio::pin!(pending);
            assert!(futures_util::poll!(pending.as_mut()).is_pending());
        }
        assert_eq!(common.try_recv(), RelayTryRecv::Empty);
        let next = second.publication().broadcast(3);
        tokio::pin!(next);
        assert!(
            futures_util::poll!(next.as_mut()).is_ready(),
            "cancellation must return the reservation for the undelivered batch"
        );
        assert_eq!(common.recv().await, Some(3));
    }

    #[cfg(feature = "shuttle")]
    #[test]
    fn shuttle_cancelling_a_partial_audience_admission_wakes_a_sibling_publisher() {
        nervix_model_harness::shuttle::check_dfs(
            || {
                shuttle::future::block_on(async {
                    let channel = RelayBroadcast::with_capacity(capacity(1));
                    let left = channel
                        .new_audience()
                        .assured("two audiences fit in memory");
                    let right = channel
                        .new_audience()
                        .assured("two audiences fit in memory");
                    let mut common = channel.new_receiver();
                    let _left_consumer = left.new_receiver();
                    let _right_consumer = right.new_receiver();
                    left.publication()
                        .broadcast(1)
                        .await
                        .assured("the queues start empty");
                    assert_eq!(common.recv().await, Some(1));
                    let mut held = Box::pin(left.publication().broadcast(2));
                    assert!(futures_util::poll!(held.as_mut()).is_pending());
                    let (started, observed) = nervix_primitives::sync::oneshot::channel();
                    let sibling = nervix_primitives::task::spawn(async move {
                        started
                            .send(())
                            .assured("the cancellation observes the sibling's attempt");
                        right.publication().broadcast(3).await
                    });
                    observed
                        .await
                        .assured("the sibling starts before the partial admission ends");
                    drop(held);
                    sibling
                        .await
                        .assured("the sibling task completes")
                        .assured("released admission reaches the sibling");
                    assert_eq!(common.recv().await, Some(3));
                });
            },
            Some(128),
        );
    }

    #[cfg(feature = "shuttle")]
    #[test]
    fn shuttle_an_audience_registering_during_parent_shutdown_observes_the_ending() {
        nervix_model_harness::shuttle::check_dfs(
            || {
                shuttle::future::block_on(async {
                    let channel = RelayBroadcast::<u8>::with_capacity(capacity(1));
                    let audience = channel
                        .new_audience()
                        .assured("one audience fits in memory");
                    let closing = nervix_primitives::task::spawn(async move {
                        channel.set_capacity(capacity(2));
                        drop(channel);
                    });
                    let registering = nervix_primitives::task::spawn(async move {
                        let mut receiver = audience.new_receiver();
                        receiver.recv().await
                    });
                    closing.await.assured("the parent ends its lifetime");
                    assert_eq!(
                        registering
                            .await
                            .assured("the new receiver observes the ending"),
                        None
                    );
                });
            },
            Some(128),
        );
    }

    #[nervix_primitives::test]
    async fn a_retained_audience_cannot_reopen_an_ended_publisher() {
        let channel = RelayBroadcast::with_capacity(capacity(1));
        let audience = channel
            .new_audience()
            .assured("one audience fits in memory");
        let mut queued = audience.new_receiver();
        audience
            .publication()
            .broadcast(9)
            .await
            .assured("the receiver has room");
        drop(channel);
        assert_eq!(queued.recv().await, Some(9));
        assert_eq!(queued.recv().await, None);
        let mut late = audience.new_receiver();
        assert_eq!(late.recv().await, None);
        assert!(audience.publication().broadcast(10).await.is_err());
    }

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
