use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, watch};
use triomphe::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckOutcome {
    Ack,
    NoAck(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckProgress {
    Alive,
    Complete(AckOutcome),
}

#[derive(Debug)]
pub struct AckCompletion {
    receiver: oneshot::Receiver<AckOutcome>,
    alive_rx: watch::Receiver<u64>,
}

#[derive(Debug, Clone)]
pub struct AckHandle(Arc<AckState>);

#[derive(Debug, Clone, Default)]
pub struct AckSet {
    handles: Vec<AckHandle>,
}

#[derive(Debug, Default)]
pub struct AckRootTracker {
    outstanding: AtomicUsize,
    ownership_handoff_outstanding: AtomicUsize,
}

#[derive(Debug)]
pub(crate) struct AckRequiredWaitGuard {
    handles: Vec<AckHandle>,
}

#[derive(Debug)]
struct AckHandoffState {
    required_wait_shares: usize,
    blocks_ownership_handoff: bool,
}

#[derive(Debug)]
struct AckState {
    pending: AtomicUsize,
    completed: AtomicBool,
    alive_counter: AtomicU64,
    alive_tx: watch::Sender<u64>,
    sender: Mutex<Option<oneshot::Sender<AckOutcome>>>,
    root_trackers: Vec<Arc<AckRootTracker>>,
    handoff: Mutex<AckHandoffState>,
}

impl AckRootTracker {
    pub fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::Acquire)
    }

    pub fn outstanding_for_ownership_handoff(&self) -> usize {
        self.ownership_handoff_outstanding.load(Ordering::Acquire)
    }
}

impl AckCompletion {
    pub async fn wait_for_progress(&mut self) -> AckProgress {
        tokio::select! {
            biased;
            result = &mut self.receiver => {
                AckProgress::Complete(result.unwrap_or_else(|_| {
                    AckOutcome::NoAck("ack completion sender dropped".to_string())
                }))
            }
            changed = self.alive_rx.changed() => {
                match changed {
                    Ok(()) => AckProgress::Alive,
                    Err(_) => {
                        let result = (&mut self.receiver).await;
                        AckProgress::Complete(result.unwrap_or_else(|_| {
                            AckOutcome::NoAck("ack completion sender dropped".to_string())
                        }))
                    }
                }
            }
        }
    }

    pub async fn wait(mut self) -> AckOutcome {
        loop {
            if let AckProgress::Complete(outcome) = self.wait_for_progress().await {
                return outcome;
            }
        }
    }
}

impl AckHandle {
    pub fn root() -> (Self, AckCompletion) {
        Self::new_root(Vec::new())
    }

    fn tracked_root(tracker: Arc<AckRootTracker>) -> (Self, AckCompletion) {
        Self::tracked_roots(vec![tracker])
    }

    fn tracked_roots(trackers: Vec<Arc<AckRootTracker>>) -> (Self, AckCompletion) {
        for tracker in &trackers {
            tracker.outstanding.fetch_add(1, Ordering::AcqRel);
            tracker
                .ownership_handoff_outstanding
                .fetch_add(1, Ordering::AcqRel);
        }
        Self::new_root(trackers)
    }

    fn new_root(root_trackers: Vec<Arc<AckRootTracker>>) -> (Self, AckCompletion) {
        let (sender, receiver) = oneshot::channel();
        let (alive_tx, alive_rx) = watch::channel(0);
        (
            Self(Arc::new(AckState {
                pending: AtomicUsize::new(1),
                completed: AtomicBool::new(false),
                alive_counter: AtomicU64::new(0),
                alive_tx,
                sender: Mutex::new(Some(sender)),
                handoff: Mutex::new(AckHandoffState {
                    required_wait_shares: 0,
                    blocks_ownership_handoff: !root_trackers.is_empty(),
                }),
                root_trackers,
            })),
            AckCompletion { receiver, alive_rx },
        )
    }

    pub fn clone_attached(&self) -> Self {
        self.clone_attached_for_receivers(1)
    }

    fn increment_ownership_handoff_trackers(&self) {
        for tracker in &self.0.root_trackers {
            tracker
                .ownership_handoff_outstanding
                .fetch_add(1, Ordering::AcqRel);
        }
    }

    fn decrement_ownership_handoff_trackers(&self) {
        for tracker in &self.0.root_trackers {
            tracker
                .ownership_handoff_outstanding
                .fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn mark_required_wait(&self) -> bool {
        if self.0.root_trackers.is_empty() {
            return false;
        }
        let mut handoff = self.0.handoff.lock();
        if self.0.completed.load(Ordering::Acquire) {
            return false;
        }
        let pending = self.0.pending.load(Ordering::Acquire);
        if handoff.required_wait_shares >= pending {
            debug_assert!(
                handoff.required_wait_shares < pending,
                "required-wait ACK shares must not exceed pending shares"
            );
            return false;
        }
        handoff.required_wait_shares += 1;
        if handoff.required_wait_shares == pending && handoff.blocks_ownership_handoff {
            self.decrement_ownership_handoff_trackers();
            handoff.blocks_ownership_handoff = false;
        }
        true
    }

    fn leave_required_wait(&self) {
        if self.0.root_trackers.is_empty() {
            return;
        }
        let mut handoff = self.0.handoff.lock();
        if handoff.required_wait_shares == 0 {
            debug_assert!(
                handoff.required_wait_shares > 0,
                "required-wait ACK share must be marked before it is released"
            );
            return;
        }
        if !self.0.completed.load(Ordering::Acquire) && !handoff.blocks_ownership_handoff {
            self.increment_ownership_handoff_trackers();
            handoff.blocks_ownership_handoff = true;
        }
        handoff.required_wait_shares -= 1;
    }

    fn finish_completion(&self, result: AckOutcome) {
        if let Some(sender) = self.0.sender.lock().take() {
            let _ = sender.send(result);
        }
    }

    fn release_root_trackers(&self, blocks_ownership_handoff: bool) {
        for tracker in &self.0.root_trackers {
            tracker.outstanding.fetch_sub(1, Ordering::AcqRel);
            if blocks_ownership_handoff {
                tracker
                    .ownership_handoff_outstanding
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    fn ack_success_tracked(&self) {
        let mut handoff = self.0.handoff.lock();
        if self.0.completed.load(Ordering::Acquire) {
            return;
        }
        let previous = self.0.pending.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "ack counter underflow");
        if previous == 1 {
            if self.0.completed.swap(true, Ordering::AcqRel) {
                return;
            }
            self.release_root_trackers(handoff.blocks_ownership_handoff);
            handoff.blocks_ownership_handoff = false;
            drop(handoff);
            self.finish_completion(AckOutcome::Ack);
            return;
        }
        let pending = previous - 1;
        debug_assert!(
            handoff.required_wait_shares <= pending,
            "an ACK share must leave required wait before completing"
        );
        if handoff.required_wait_shares == pending && handoff.blocks_ownership_handoff {
            self.decrement_ownership_handoff_trackers();
            handoff.blocks_ownership_handoff = false;
        }
    }

    fn complete_tracked(&self, result: AckOutcome) {
        let mut handoff = self.0.handoff.lock();
        if self.0.completed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.release_root_trackers(handoff.blocks_ownership_handoff);
        handoff.blocks_ownership_handoff = false;
        drop(handoff);
        self.finish_completion(result);
    }

    pub fn clone_attached_for_receivers(&self, receivers: usize) -> Self {
        debug_assert!(
            receivers > 0,
            "attached clone requires at least one receiver"
        );
        if self.0.root_trackers.is_empty() {
            self.0.pending.fetch_add(receivers, Ordering::AcqRel);
            return self.clone();
        }
        let mut handoff = self.0.handoff.lock();
        if self.0.completed.load(Ordering::Acquire) {
            return self.clone();
        }
        if !handoff.blocks_ownership_handoff {
            self.increment_ownership_handoff_trackers();
            handoff.blocks_ownership_handoff = true;
        }
        self.0.pending.fetch_add(receivers, Ordering::AcqRel);
        drop(handoff);
        self.clone()
    }

    pub fn ack_alive(&self) {
        if self.0.completed.load(Ordering::Acquire) {
            return;
        }

        let next = self.0.alive_counter.fetch_add(1, Ordering::AcqRel) + 1;
        self.0.alive_tx.send_replace(next);
    }

    pub fn ack_success(&self) {
        if self.0.completed.load(Ordering::Acquire) {
            return;
        }
        if !self.0.root_trackers.is_empty() {
            self.ack_success_tracked();
            return;
        }
        let previous = self.0.pending.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "ack counter underflow");
        if previous == 1 {
            self.complete(AckOutcome::Ack);
        }
    }

    pub fn no_ack(&self, reason: impl Into<String>) {
        self.complete(AckOutcome::NoAck(reason.into()));
    }

    fn complete(&self, result: AckOutcome) {
        if !self.0.root_trackers.is_empty() {
            self.complete_tracked(result);
            return;
        }
        if self.0.completed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.finish_completion(result);
    }
}

impl Drop for AckState {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::Acquire) && !self.root_trackers.is_empty() {
            let blocks_ownership_handoff = self.handoff.get_mut().blocks_ownership_handoff;
            for tracker in &self.root_trackers {
                tracker.outstanding.fetch_sub(1, Ordering::AcqRel);
                if blocks_ownership_handoff {
                    tracker
                        .ownership_handoff_outstanding
                        .fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
    }
}

impl AckRequiredWaitGuard {
    pub(crate) fn new<'a>(sets: impl IntoIterator<Item = &'a AckSet>) -> Self {
        let mut handles = Vec::new();
        for set in sets {
            for handle in &set.handles {
                if handle.mark_required_wait() {
                    handles.push(handle.clone());
                }
            }
        }
        Self { handles }
    }
}

impl Drop for AckRequiredWaitGuard {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.leave_required_wait();
        }
    }
}

impl AckSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn root() -> (Self, AckCompletion) {
        let (handle, completion) = AckHandle::root();
        (
            Self {
                handles: vec![handle],
            },
            completion,
        )
    }

    pub fn tracked_root(tracker: Arc<AckRootTracker>) -> (Self, AckCompletion) {
        let (handle, completion) = AckHandle::tracked_root(tracker);
        (
            Self {
                handles: vec![handle],
            },
            completion,
        )
    }

    pub fn tracked_roots(trackers: Vec<Arc<AckRootTracker>>) -> (Self, AckCompletion) {
        let (handle, completion) = AckHandle::tracked_roots(trackers);
        (
            Self {
                handles: vec![handle],
            },
            completion,
        )
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    pub fn attached(&self) -> Self {
        Self {
            handles: self.handles.iter().map(AckHandle::clone_attached).collect(),
        }
    }

    /// One shared attached clone delivered to `receivers` consumers, each of
    /// which resolves its own share exactly once.
    pub fn attached_for_receivers(&self, receivers: usize) -> Self {
        Self {
            handles: self
                .handles
                .iter()
                .map(|handle| handle.clone_attached_for_receivers(receivers))
                .collect(),
        }
    }

    #[cfg(test)]
    pub(crate) fn required_wait_guard(&self) -> AckRequiredWaitGuard {
        AckRequiredWaitGuard::new([self])
    }

    pub fn merged<I>(sets: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        let handles = sets
            .into_iter()
            .flat_map(|set| set.handles)
            .collect::<Vec<_>>();
        Self { handles }
    }

    pub fn ack_success(&self) {
        for handle in &self.handles {
            handle.ack_success();
        }
    }

    pub fn ack_alive(&self) {
        for handle in &self.handles {
            handle.ack_alive();
        }
    }

    pub fn no_ack(&self, reason: impl Into<String>) {
        let reason = reason.into();
        for handle in &self.handles {
            handle.no_ack(reason.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, timeout};
    use triomphe::Arc;

    use super::{AckOutcome, AckProgress, AckRootTracker, AckSet};

    #[tokio::test]
    async fn root_completes_after_manual_ack() {
        let (acks, completion) = AckSet::root();

        acks.ack_success();

        assert_eq!(completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn tracked_root_counts_until_terminal_completion() {
        let tracker = Arc::new(AckRootTracker::default());
        let (acks, completion) = AckSet::tracked_root(tracker.clone());
        let attached = acks.attached();

        assert_eq!(tracker.outstanding(), 1);
        acks.ack_success();
        assert_eq!(tracker.outstanding(), 1);
        attached.ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        assert_eq!(tracker.outstanding(), 0);
    }

    #[tokio::test]
    async fn tracked_root_updates_domain_and_ingestor_counters_together() {
        let domain = Arc::new(AckRootTracker::default());
        let ingestor = Arc::new(AckRootTracker::default());
        let (acks, completion) = AckSet::tracked_roots(vec![domain.clone(), ingestor.clone()]);
        let attached = acks.attached();

        assert_eq!(domain.outstanding(), 1);
        assert_eq!(ingestor.outstanding(), 1);
        acks.ack_success();
        assert_eq!(domain.outstanding(), 1);
        assert_eq!(ingestor.outstanding(), 1);
        attached.ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        assert_eq!(domain.outstanding(), 0);
        assert_eq!(ingestor.outstanding(), 0);
    }

    #[tokio::test]
    async fn required_wait_only_root_does_not_block_ownership_handoff() {
        let tracker = Arc::new(AckRootTracker::default());
        let (waiting, completion) = AckSet::tracked_root(tracker.clone());
        let active = waiting.attached();
        let required_wait = waiting.required_wait_guard();

        assert_eq!(tracker.outstanding(), 1);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);

        active.ack_success();
        assert_eq!(tracker.outstanding(), 1);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);

        drop(required_wait);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);

        waiting.no_ack("ownership changed while waiting for required state");
        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck("ownership changed while waiting for required state".to_string())
        );
        assert_eq!(tracker.outstanding(), 0);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    }

    #[test]
    fn dropping_unresolved_tracked_root_releases_count() {
        let tracker = Arc::new(AckRootTracker::default());
        let (acks, completion) = AckSet::tracked_root(tracker.clone());
        assert_eq!(tracker.outstanding(), 1);

        drop(acks);
        drop(completion);

        assert_eq!(tracker.outstanding(), 0);
    }

    #[tokio::test]
    async fn attached_clone_requires_both_acks() {
        let (acks, completion) = AckSet::root();
        let derived = acks.attached();

        acks.ack_success();
        derived.ack_success();

        assert_eq!(completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn merged_sets_complete_all_roots() {
        let (left, left_completion) = AckSet::root();
        let (right, right_completion) = AckSet::root();
        let merged = AckSet::merged([left.attached(), right.attached()]);

        left.ack_success();
        right.ack_success();
        merged.ack_success();

        assert_eq!(left_completion.wait().await, AckOutcome::Ack);
        assert_eq!(right_completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn no_ack_resolves_completion_with_error() {
        let (acks, completion) = AckSet::root();

        acks.no_ack("runtime stopped");

        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck("runtime stopped".to_string())
        );
    }

    #[tokio::test]
    async fn repeated_ack_success_is_idempotent() {
        let (acks, completion) = AckSet::root();

        acks.ack_success();
        acks.ack_success();

        assert_eq!(completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn no_ack_wins_over_later_ack_success() {
        let (acks, completion) = AckSet::root();
        let derived = acks.attached();

        derived.no_ack("runtime stopped");
        acks.ack_success();

        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck("runtime stopped".to_string())
        );
    }

    #[tokio::test]
    async fn root_ack_waits_for_attached_branch() {
        let (acks, completion) = AckSet::root();
        let derived = acks.attached();
        let wait = completion.wait();
        tokio::pin!(wait);

        acks.ack_success();

        assert!(
            timeout(Duration::from_millis(10), &mut wait).await.is_err(),
            "completion must stay pending until all attached branches resolve"
        );

        derived.ack_success();

        assert_eq!(
            timeout(Duration::from_secs(1), wait)
                .await
                .expect("completion should resolve after derived ack"),
            AckOutcome::Ack
        );
    }

    #[tokio::test]
    async fn ack_alive_keeps_completion_pending_without_completing() {
        let (acks, mut completion) = AckSet::root();

        acks.ack_alive();

        assert_eq!(completion.wait_for_progress().await, AckProgress::Alive);
        assert!(
            timeout(Duration::from_millis(10), completion.wait())
                .await
                .is_err(),
            "alive progress must reset waits without resolving the ack"
        );
    }

    #[tokio::test]
    async fn ack_alive_is_transitive_through_attached_branches() {
        let (acks, mut completion) = AckSet::root();
        let derived = acks.attached();

        derived.ack_alive();

        assert_eq!(completion.wait_for_progress().await, AckProgress::Alive);
    }
}

#[cfg(all(test, runtime_ack_loom))]
mod loom_tests {
    use loom::{
        model,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
    };

    #[derive(Clone)]
    struct LoomAck(Arc<LoomAckState>);

    struct LoomAckState {
        pending: AtomicUsize,
        completed: AtomicBool,
        result: Mutex<Option<bool>>,
    }

    impl LoomAck {
        fn root() -> Self {
            Self(Arc::new(LoomAckState {
                pending: AtomicUsize::new(1),
                completed: AtomicBool::new(false),
                result: Mutex::new(None),
            }))
        }

        fn attached(&self) -> Self {
            self.0.pending.fetch_add(1, Ordering::AcqRel);
            self.clone()
        }

        fn ack_success(&self) {
            if self.0.completed.load(Ordering::Acquire) {
                return;
            }

            let previous = self.0.pending.fetch_sub(1, Ordering::AcqRel);
            assert!(previous > 0, "ack counter underflow");
            if previous == 1 {
                self.complete(true);
            }
        }

        fn no_ack(&self) {
            self.complete(false);
        }

        fn complete(&self, result: bool) {
            if self.0.completed.swap(true, Ordering::AcqRel) {
                return;
            }

            let mut slot = self.0.result.lock().expect("lock should succeed");
            assert!(slot.is_none(), "result must only be written once");
            *slot = Some(result);
        }

        fn result(&self) -> Option<bool> {
            *self.0.result.lock().expect("lock should succeed")
        }
    }

    #[test]
    fn attached_branches_do_not_complete_early() {
        model(|| {
            let root = LoomAck::root();
            let attached = root.attached();

            let root_thread = {
                let root = root.clone();
                thread::spawn(move || root.ack_success())
            };

            root_thread.join().expect("root thread should join");
            assert_eq!(root.result(), None);

            let attached_thread = thread::spawn(move || attached.ack_success());
            attached_thread.join().expect("attached thread should join");

            assert_eq!(root.result(), Some(true));
        });
    }

    #[test]
    fn no_ack_is_single_winner_against_final_ack() {
        model(|| {
            let root = LoomAck::root();
            let attached = root.attached();

            let ack_thread = {
                let root = root.clone();
                thread::spawn(move || root.ack_success())
            };
            let no_ack_thread = thread::spawn(move || attached.no_ack());

            ack_thread.join().expect("ack thread should join");
            no_ack_thread.join().expect("no-ack thread should join");

            assert!(root.result().is_some(), "one completion path must win");
        });
    }
}
