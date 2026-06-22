use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, watch};

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

#[derive(Debug)]
struct AckState {
    pending: AtomicUsize,
    completed: AtomicBool,
    alive_counter: AtomicU64,
    alive_tx: watch::Sender<u64>,
    sender: Mutex<Option<oneshot::Sender<AckOutcome>>>,
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
        let (sender, receiver) = oneshot::channel();
        let (alive_tx, alive_rx) = watch::channel(0);
        (
            Self(Arc::new(AckState {
                pending: AtomicUsize::new(1),
                completed: AtomicBool::new(false),
                alive_counter: AtomicU64::new(0),
                alive_tx,
                sender: Mutex::new(Some(sender)),
            })),
            AckCompletion { receiver, alive_rx },
        )
    }

    pub fn clone_attached(&self) -> Self {
        self.0.pending.fetch_add(1, Ordering::AcqRel);
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
        if self.0.completed.swap(true, Ordering::AcqRel) {
            return;
        }

        if let Some(sender) = self.0.sender.lock().take() {
            let _ = sender.send(result);
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

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    pub fn attached(&self) -> Self {
        Self {
            handles: self.handles.iter().map(AckHandle::clone_attached).collect(),
        }
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

    use super::{AckOutcome, AckProgress, AckSet};

    #[tokio::test]
    async fn root_completes_after_manual_ack() {
        let (acks, completion) = AckSet::root();

        acks.ack_success();

        assert_eq!(completion.wait().await, AckOutcome::Ack);
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
