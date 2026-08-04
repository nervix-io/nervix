use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

use async_broadcast::{
    InactiveReceiver, Receiver as AsyncBroadcastReceiver, RecvError, SendError, Sender,
    TryRecvError,
};
use parking_lot::Mutex;
use tokio::{
    sync::Notify,
    time::{Instant, timeout_at},
};
use tracing::debug;
use triomphe::Arc;

#[derive(Debug)]
pub(crate) struct RelayDispatchGate {
    closed: AtomicBool,
    state: Mutex<RelayDispatchGateState>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct RelayDispatchGateState {
    generation: u64,
    engagements: BTreeMap<u64, RelayDispatchGateEngagement>,
    in_flight_dispatches: usize,
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
pub(crate) struct RelayDispatchGateLease {
    gate: Arc<RelayDispatchGate>,
    generation: u64,
}

/// Proof that one relay dispatch entered before the current gate engagements.
///
/// Acquiring a permit and engaging the gate are serialized by the gate state lock. Once an
/// engagement wins that ordering, no later dispatch can acquire a permit until the engagement is
/// released. Dropping every permit that won before it completes the engagement fence.
#[derive(Debug)]
pub(crate) struct RelayDispatchPermit<'gate> {
    gate: &'gate RelayDispatchGate,
}

impl RelayDispatchGate {
    pub(crate) fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            state: Mutex::new(RelayDispatchGateState::default()),
            changed: Notify::new(),
        }
    }

    fn engage(&self, deadline: Instant, reason: impl Into<String>) -> u64 {
        let mut state = self.state.lock();
        loop {
            state.generation = state.generation.wrapping_add(1);
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
        self.closed.store(true, Ordering::Release);
        drop(state);
        self.changed.notify_waiters();
        generation
    }

    fn release(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.engagements.remove(&generation).is_none() {
            return;
        }
        self.closed
            .store(!state.engagements.is_empty(), Ordering::Release);
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) async fn acquire_dispatch(&self) -> RelayDispatchPermit<'_> {
        loop {
            tokio::task::consume_budget().await;
            self.clear_if_expired();
            let changed = self.changed.notified();
            let deadline = {
                let mut state = self.state.lock();
                if state.engagements.is_empty() {
                    state.in_flight_dispatches = state.in_flight_dispatches.saturating_add(1);
                    return RelayDispatchPermit { gate: self };
                }
                state
                    .engagements
                    .values()
                    .filter_map(RelayDispatchGateEngagement::fence_deadline)
                    .min()
            };
            if let Some(deadline) = deadline {
                if timeout_at(deadline, changed).await.is_err() {
                    self.clear_if_expired();
                }
            } else {
                changed.await;
            }
        }
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
            let deadline = {
                let mut state = self.state.lock();
                let in_flight_dispatches = state.in_flight_dispatches;
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
            if timeout_at(deadline, changed).await.is_err() {
                self.clear_if_expired();
            }
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.clear_if_expired();
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_open(&self) {
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

    pub(crate) async fn wait_closed(&self) {
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
    pub(crate) fn reason(&self) -> Option<String> {
        self.clear_if_expired();
        self.state
            .lock()
            .engagements
            .last_key_value()
            .map(|(_, engagement)| engagement)
            .map(|engagement| engagement.reason.clone())
    }

    #[cfg(test)]
    pub(crate) fn in_flight_dispatches(&self) -> usize {
        self.state.lock().in_flight_dispatches
    }

    fn clear_if_expired(&self) {
        if !self.closed.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let mut state = self.state.lock();
        let expired = state
            .engagements
            .iter()
            .filter_map(|(generation, engagement)| {
                engagement
                    .fence_deadline()
                    .is_some_and(|deadline| now >= deadline)
                    .then_some((*generation, engagement.reason.clone()))
            })
            .collect::<Vec<_>>();
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
    pub(crate) fn engage(
        gate: Arc<RelayDispatchGate>,
        deadline: Instant,
        reason: impl Into<String>,
    ) -> Self {
        let generation = gate.engage(deadline, reason);
        Self { gate, generation }
    }

    pub(crate) async fn wait_quiescent(&mut self) -> bool {
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
        let mut state = self.gate.state.lock();
        debug_assert!(
            state.in_flight_dispatches > 0,
            "relay dispatch permit count underflow"
        );
        state.in_flight_dispatches = state.in_flight_dispatches.saturating_sub(1);
        let quiescent = state.in_flight_dispatches == 0;
        drop(state);
        if quiescent {
            self.gate.changed.notify_waiters();
        }
    }
}

#[derive(Debug)]
pub(crate) struct RelayBroadcast<T> {
    sender: Sender<T>,
    inner: Arc<RelayBroadcastInner<T>>,
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
    async fn gate_engagement_waits_for_pre_engagement_dispatch_to_finish() {
        let gate = Arc::new(RelayDispatchGate::new());
        let permit = gate.acquire_dispatch().await;
        let mut lease = engage(
            &gate,
            Instant::now() + Duration::from_secs(1),
            "graceful entity stop",
        );

        let quiescent = tokio::spawn(async move { lease.wait_quiescent().await });
        tokio::task::yield_now().await;
        assert!(
            !quiescent.is_finished(),
            "engagement must fence a dispatch that already acquired its permit"
        );

        drop(permit);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), quiescent)
                .await
                .expect("dispatch completion should wake the gate fence")
                .expect("gate fence task should join")
        );
    }

    #[tokio::test]
    async fn engaged_gate_prevents_new_dispatch_until_release() {
        let gate = Arc::new(RelayDispatchGate::new());
        let mut lease = engage(
            &gate,
            Instant::now() + Duration::from_secs(1),
            "graceful entity stop",
        );
        assert!(lease.wait_quiescent().await);

        let dispatch = tokio::spawn({
            let gate = gate.clone();
            async move {
                let _permit = gate.acquire_dispatch().await;
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !dispatch.is_finished(),
            "dispatches that arrive after engagement must remain outside the fence"
        );

        drop(lease);
        tokio::time::timeout(Duration::from_secs(1), dispatch)
            .await
            .expect("gate release should admit the waiting dispatch")
            .expect("dispatch task should join");
    }

    #[tokio::test]
    async fn canceled_dispatch_releases_gate_fence_permit() {
        let gate = Arc::new(RelayDispatchGate::new());
        let dispatch = tokio::spawn({
            let gate = gate.clone();
            async move {
                let _permit = gate.acquire_dispatch().await;
                std::future::pending::<()>().await;
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.in_flight_dispatches() == 0 {
                tokio::task::consume_budget().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dispatch should acquire its permit");

        let mut lease = engage(
            &gate,
            Instant::now() + Duration::from_secs(1),
            "graceful entity stop",
        );
        dispatch.abort();
        let _ = dispatch.await;

        assert!(
            tokio::time::timeout(Duration::from_secs(1), lease.wait_quiescent())
                .await
                .expect("canceling dispatch should drop its permit")
        );
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

    #[tokio::test]
    async fn overlapping_gate_leases_must_all_release_before_dispatch_resumes() {
        let gate = Arc::new(RelayDispatchGate::new());
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut first = engage(&gate, deadline, "first node swap");
        let mut second = engage(&gate, deadline, "second node swap");
        assert!(first.wait_quiescent().await);
        assert!(second.wait_quiescent().await);

        drop(second);
        assert!(gate.is_closed());
        drop(first);
        assert!(!gate.is_closed());
    }
}

#[derive(Debug)]
pub(crate) struct RelayReceiver<T> {
    receiver: AsyncBroadcastReceiver<T>,
    inner: Arc<RelayBroadcastInner<T>>,
}

#[derive(Debug)]
struct RelayBroadcastInner<T> {
    control: Mutex<RelayBroadcastControl<T>>,
    changed: Notify,
    dirty: AtomicBool,
}

#[derive(Debug)]
struct RelayBroadcastControl<T> {
    guard: InactiveReceiver<T>,
    target_capacity: NonZeroUsize,
    active_publishers: usize,
    waiting_publishers: usize,
}

impl<T> RelayBroadcastControl<T> {
    fn apply_pending_capacity(&mut self) -> bool {
        let target_capacity = self.target_capacity.get();
        let current_capacity = self.guard.capacity();
        if current_capacity < target_capacity
            || current_capacity > target_capacity && self.guard.len() <= target_capacity
        {
            self.guard.set_capacity(target_capacity);
        }
        self.is_dirty()
    }

    fn is_dirty(&self) -> bool {
        self.guard.capacity() != self.target_capacity.get()
            || self.active_publishers > 0
            || self.waiting_publishers > 0
    }
}

struct RelayPublishPermit<T> {
    inner: Arc<RelayBroadcastInner<T>>,
}

struct RelayPublishWaiter<T> {
    inner: Arc<RelayBroadcastInner<T>>,
}

impl<T> RelayBroadcast<T> {
    pub(crate) fn with_capacity(capacity: NonZeroUsize) -> Self {
        let (mut sender, receiver) = async_broadcast::broadcast(capacity.get());
        sender.set_overflow(false);
        sender.set_await_active(false);
        Self {
            sender,
            inner: Arc::new(RelayBroadcastInner {
                control: Mutex::new(RelayBroadcastControl {
                    guard: receiver.deactivate(),
                    target_capacity: capacity,
                    active_publishers: 0,
                    waiting_publishers: 0,
                }),
                changed: Notify::new(),
                dirty: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn new_receiver(&self) -> RelayReceiver<T> {
        debug_assert!(self.inner.inactive_receiver_count() > 0);
        RelayReceiver {
            receiver: self.sender.new_receiver(),
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn receiver_count(&self) -> usize {
        debug_assert!(self.inner.inactive_receiver_count() > 0);
        self.sender.receiver_count()
    }

    pub(crate) fn len(&self) -> usize {
        self.sender.len()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.inner.control.lock().target_capacity.get()
    }

    pub(crate) fn set_capacity(&self, capacity: NonZeroUsize) {
        let was_dirty = self.inner.dirty.swap(true, Ordering::Relaxed);
        let is_dirty = {
            let mut control = self.inner.control.lock();
            control.target_capacity = capacity;
            control.apply_pending_capacity()
        };
        self.inner.dirty.store(is_dirty, Ordering::Relaxed);
        if was_dirty || is_dirty {
            self.inner.changed.notify_waiters();
        }
    }
}

impl<T: Clone> RelayBroadcast<T> {
    pub(crate) async fn broadcast(&self, message: T) -> Result<(), SendError<T>> {
        if !self.inner.dirty.load(Ordering::Relaxed) {
            return self.broadcast_message(message).await;
        }

        let permit = self.publish_permit().await;
        let result = self.broadcast_message(message).await;
        drop(permit);
        self.inner.maintain_dirty_capacity();
        result
    }

    async fn broadcast_message(&self, message: T) -> Result<(), SendError<T>> {
        match self.sender.broadcast(message).await {
            Ok(None) => Ok(()),
            Ok(Some(_)) => unreachable!("relay broadcast overflow must be disabled"),
            Err(error) => Err(error),
        }
    }

    async fn publish_permit(&self) -> RelayPublishPermit<T> {
        loop {
            tokio::task::consume_budget().await;
            let changed = self.inner.changed.notified();
            {
                let mut control = self.inner.control.lock();
                control.apply_pending_capacity();
                let queued_or_entering = control.guard.len() + control.active_publishers;
                if queued_or_entering < control.target_capacity.get() {
                    control.active_publishers += 1;
                    self.inner
                        .dirty
                        .store(control.is_dirty(), Ordering::Relaxed);
                    return RelayPublishPermit {
                        inner: self.inner.clone(),
                    };
                }
                control.waiting_publishers += 1;
                self.inner
                    .dirty
                    .store(control.is_dirty(), Ordering::Relaxed);
            }
            let waiter = RelayPublishWaiter {
                inner: self.inner.clone(),
            };
            changed.await;
            drop(waiter);
        }
    }
}

impl<T> RelayBroadcastInner<T> {
    fn inactive_receiver_count(&self) -> usize {
        self.control.lock().guard.inactive_receiver_count()
    }

    fn maintain_dirty_capacity(&self) {
        if !self.dirty.load(Ordering::Relaxed) {
            return;
        }
        let is_dirty = self.control.lock().apply_pending_capacity();
        self.dirty.store(is_dirty, Ordering::Relaxed);
        self.changed.notify_waiters();
    }
}

impl<T> Drop for RelayPublishPermit<T> {
    fn drop(&mut self) {
        let was_dirty = self.inner.dirty.load(Ordering::Relaxed);
        let mut control = self.inner.control.lock();
        control.active_publishers = control.active_publishers.saturating_sub(1);
        let is_dirty = control.apply_pending_capacity();
        drop(control);
        self.inner.dirty.store(is_dirty, Ordering::Relaxed);
        if was_dirty {
            self.inner.changed.notify_waiters();
        }
    }
}

impl<T> Drop for RelayPublishWaiter<T> {
    fn drop(&mut self) {
        let was_dirty = self.inner.dirty.load(Ordering::Relaxed);
        let mut control = self.inner.control.lock();
        control.waiting_publishers = control.waiting_publishers.saturating_sub(1);
        let is_dirty = control.apply_pending_capacity();
        drop(control);
        self.inner.dirty.store(is_dirty, Ordering::Relaxed);
        if was_dirty {
            self.inner.changed.notify_waiters();
        }
    }
}

impl<T: Clone> RelayReceiver<T> {
    pub(crate) async fn recv(&mut self) -> Result<T, RecvError> {
        let result = self.receiver.recv().await;
        if result.is_ok() {
            self.inner.maintain_dirty_capacity();
        }
        result
    }

    pub(crate) fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let result = self.receiver.try_recv();
        if result.is_ok() {
            self.inner.maintain_dirty_capacity();
        }
        result
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, RecvError>>> {
        let result = Pin::new(&mut self.receiver).poll_recv(cx);
        if let Poll::Ready(Some(Ok(_))) = &result {
            self.inner.maintain_dirty_capacity();
        }
        result
    }

    pub(crate) fn len(&self) -> usize {
        self.receiver.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::atomic::Ordering, time::Duration};

    use triomphe::Arc;

    use super::RelayBroadcast;

    #[tokio::test]
    async fn shrinking_capacity_preserves_buffered_messages() {
        let channel = RelayBroadcast::with_capacity(NonZeroUsize::new(3).expect("nonzero"));
        let mut receiver = channel.new_receiver();

        channel
            .broadcast(1)
            .await
            .expect("first send should succeed");
        channel
            .broadcast(2)
            .await
            .expect("second send should succeed");
        channel
            .broadcast(3)
            .await
            .expect("third send should succeed");

        channel.set_capacity(NonZeroUsize::new(1).expect("nonzero"));
        assert_eq!(channel.capacity(), 1);

        assert_eq!(
            receiver.recv().await.expect("first receive should succeed"),
            1
        );
        assert_eq!(
            receiver
                .recv()
                .await
                .expect("second receive should succeed"),
            2
        );
        assert_eq!(
            receiver.recv().await.expect("third receive should succeed"),
            3
        );
        assert!(!channel.inner.dirty.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn steady_capacity_publish_keeps_control_state_clean() {
        let channel = RelayBroadcast::with_capacity(NonZeroUsize::new(3).expect("nonzero"));
        let mut receiver = channel.new_receiver();

        assert!(!channel.inner.dirty.load(Ordering::Relaxed));
        channel
            .broadcast(1)
            .await
            .expect("send should use clean path");
        assert!(!channel.inner.dirty.load(Ordering::Relaxed));
        assert_eq!(receiver.recv().await.expect("receive should succeed"), 1);
        assert!(!channel.inner.dirty.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn shrinking_capacity_wakes_waiting_publishers_after_drain() {
        let channel = Arc::new(RelayBroadcast::with_capacity(
            NonZeroUsize::new(3).expect("nonzero"),
        ));
        let mut receiver = channel.new_receiver();

        channel
            .broadcast(1)
            .await
            .expect("first send should succeed");
        channel
            .broadcast(2)
            .await
            .expect("second send should succeed");
        channel
            .broadcast(3)
            .await
            .expect("third send should succeed");

        channel.set_capacity(NonZeroUsize::new(1).expect("nonzero"));
        let pending = tokio::spawn({
            let channel = channel.clone();
            async move { channel.broadcast(4).await.expect("fourth send should wake") }
        });
        wait_for_waiting_publishers(&channel, 1).await;

        assert_eq!(
            receiver.recv().await.expect("first receive should succeed"),
            1
        );
        assert!(!pending.is_finished());
        assert_eq!(
            receiver
                .recv()
                .await
                .expect("second receive should succeed"),
            2
        );
        assert!(!pending.is_finished());
        assert_eq!(
            receiver.recv().await.expect("third receive should succeed"),
            3
        );

        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("waiting publisher should be notified")
            .expect("waiting publisher task should join");
        assert_eq!(
            receiver
                .recv()
                .await
                .expect("fourth receive should succeed"),
            4
        );
    }

    async fn wait_for_waiting_publishers(channel: &RelayBroadcast<i32>, expected: usize) {
        for _ in 0..100 {
            tokio::task::consume_budget().await;
            if channel.inner.control.lock().waiting_publishers == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("timed out waiting for {expected} waiting publisher(s)");
    }
}
