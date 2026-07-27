use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
};

use async_broadcast::{
    InactiveReceiver, Receiver as AsyncBroadcastReceiver, RecvError, SendError, Sender,
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
    engagement: Option<RelayDispatchGateEngagement>,
}

#[derive(Debug)]
struct RelayDispatchGateEngagement {
    deadline: Instant,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelayDispatchGateToken {
    generation: u64,
}

impl RelayDispatchGate {
    pub(crate) fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            state: Mutex::new(RelayDispatchGateState::default()),
            changed: Notify::new(),
        }
    }

    pub(crate) fn engage(
        &self,
        deadline: Instant,
        reason: impl Into<String>,
    ) -> RelayDispatchGateToken {
        let mut state = self.state.lock();
        state.generation = state.generation.wrapping_add(1);
        let token = RelayDispatchGateToken {
            generation: state.generation,
        };
        state.engagement = Some(RelayDispatchGateEngagement {
            deadline,
            reason: reason.into(),
        });
        self.closed.store(true, Ordering::Release);
        token
    }

    pub(crate) fn release(&self, token: RelayDispatchGateToken) {
        let mut state = self.state.lock();
        if state.generation != token.generation || state.engagement.is_none() {
            return;
        }
        state.engagement = None;
        self.closed.store(false, Ordering::Release);
        drop(state);
        self.changed.notify_waiters();
    }

    #[cfg(test)]
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
            let Some((generation, deadline)) = ({
                let state = self.state.lock();
                state
                    .engagement
                    .as_ref()
                    .map(|engagement| (state.generation, engagement.deadline))
            }) else {
                self.closed.store(false, Ordering::Release);
                return;
            };
            if Instant::now() >= deadline {
                self.clear_generation_if_expired(generation);
                return;
            }
            if timeout_at(deadline, changed).await.is_err() {
                self.clear_generation_if_expired(generation);
            }
            if !self.closed.load(Ordering::Acquire) {
                return;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn reason(&self) -> Option<String> {
        self.clear_if_expired();
        self.state
            .lock()
            .engagement
            .as_ref()
            .map(|engagement| engagement.reason.clone())
    }

    #[cfg(test)]
    fn clear_if_expired(&self) {
        if !self.closed.load(Ordering::Acquire) {
            return;
        }
        let generation = {
            let state = self.state.lock();
            state.engagement.as_ref().and_then(|engagement| {
                (Instant::now() >= engagement.deadline).then_some(state.generation)
            })
        };
        if let Some(generation) = generation {
            self.clear_generation_if_expired(generation);
        }
    }

    fn clear_generation_if_expired(&self, generation: u64) {
        let mut state = self.state.lock();
        let should_clear = state.generation == generation
            && state
                .engagement
                .as_ref()
                .is_some_and(|engagement| Instant::now() >= engagement.deadline);
        if !should_clear {
            return;
        }
        let reason = state
            .engagement
            .take()
            .map(|engagement| engagement.reason)
            .unwrap_or_default();
        self.closed.store(false, Ordering::Release);
        drop(state);
        debug!(reason, "relay dispatch gate deadline expired; reopening");
        self.changed.notify_waiters();
    }
}

impl Default for RelayDispatchGate {
    fn default() -> Self {
        Self::new()
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

    use super::RelayDispatchGate;

    #[tokio::test]
    async fn relay_dispatch_gate_releases_waiters_explicitly() {
        let gate = RelayDispatchGate::new();
        let token = gate.engage(Instant::now() + Duration::from_secs(1), "node swap");
        assert!(gate.is_closed());
        assert_eq!(gate.reason().as_deref(), Some("node swap"));

        gate.release(token);
        gate.wait_open().await;
        assert!(!gate.is_closed());
    }

    #[tokio::test]
    async fn relay_dispatch_gate_self_clears_at_its_deadline() {
        let gate = RelayDispatchGate::new();
        gate.engage(
            Instant::now() + Duration::from_millis(10),
            "leader may fail",
        );

        gate.wait_open().await;
        assert!(!gate.is_closed());
    }

    #[tokio::test]
    async fn stale_gate_hold_cannot_release_a_new_engagement() {
        let gate = RelayDispatchGate::new();
        let stale = gate.engage(Instant::now() + Duration::from_secs(1), "first");
        let current = gate.engage(Instant::now() + Duration::from_secs(1), "second");

        gate.release(stale);
        assert!(gate.is_closed());
        assert_eq!(gate.reason().as_deref(), Some("second"));
        gate.release(current);
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
            if channel.inner.control.lock().waiting_publishers == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("timed out waiting for {expected} waiting publisher(s)");
    }
}
