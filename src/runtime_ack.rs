//! The acknowledgement tree one node keeps in memory while work is outstanding.
//!
//! Layer: data plane.
//!
//! - **Owns.** The root tracker per ingestor and per domain, the sets a batch fans out into, the
//!   guard that holds a message waiting on materialized state, and the outcome each root resolves
//!   to.
//! - **Depends on.** The standard library and `triomphe`.
//! - **Must not know.** What is being acknowledged. It counts outstanding work, and ack state is
//!   hot-path memory that is never persisted.

#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use meticulous::OptionExt as _;
use nervix_recovery::NoReceiver as _;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{oneshot, watch};
use triomphe::Arc;

const HANDOFF_TRACKING_COMPLETE: usize = usize::MAX;
const ACK_SHARES_FIT_IN_MEMORY: &str =
    "every pending ACK share has an in-memory owner, so their count fits in usize";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckShareResolution {
    AlreadyComplete,
    Pending,
    Complete,
}

#[derive(Debug)]
struct AckState {
    /// Zero is terminal. Attaching a share reserves it here before publishing it as active below.
    /// An attachment that finds zero reserves nothing, yet still returns a handle to this root.
    pending: AtomicUsize,
    /// Pending shares that are not parked on `REQUIRED WAIT`. `usize::MAX` closes the counter once
    /// the root completes, so a racing wait release or attachment cannot reactivate it.
    ///
    /// Only a handle that owns a share removes one from this count. An acknowledgement resolves
    /// its share in `pending` before it removes the share here, and a share parks only while
    /// `pending` is above zero. A handle whose attachment reserved nothing finds `pending` at zero
    /// on both paths, so it cannot remove an active share that another handle still owns before
    /// the terminal transition closes this count.
    ///
    /// The root trackers count roots rather than shares. A zero-to-positive transition therefore
    /// reserves one tracker count before publishing the active share count. A positive-to-zero
    /// transition publishes zero before releasing the tracker count. The tracker may briefly
    /// overcount either transition, but it never lets an ownership handoff miss active work.
    handoff_active: AtomicUsize,
    alive_counter: AtomicU64,
    alive_tx: watch::Sender<u64>,
    sender: Mutex<Option<oneshot::Sender<AckOutcome>>>,
    root_trackers: Vec<Arc<AckRootTracker>>,
}

struct OwnershipHandoffTrackerReservation<'a> {
    handle: &'a AckHandle,
    release_on_drop: bool,
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
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(_) => AckOutcome::NoAck("ack completion sender dropped".to_string()),
                };
                AckProgress::Complete(outcome)
            }
            changed = self.alive_rx.changed() => {
                match changed {
                    Ok(()) => AckProgress::Alive,
                    Err(_) => {
                        let result = (&mut self.receiver).await;
                        let outcome = match result {
                            Ok(outcome) => outcome,
                            Err(_) => {
                                AckOutcome::NoAck("ack completion sender dropped".to_string())
                            }
                        };
                        AckProgress::Complete(outcome)
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
                handoff_active: AtomicUsize::new(usize::from(!root_trackers.is_empty())),
                alive_counter: AtomicU64::new(0),
                alive_tx,
                sender: Mutex::new(Some(sender)),
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

    fn reserve_ownership_handoff_trackers(&self) -> OwnershipHandoffTrackerReservation<'_> {
        self.increment_ownership_handoff_trackers();
        OwnershipHandoffTrackerReservation {
            handle: self,
            release_on_drop: true,
        }
    }

    fn reserve_pending_shares(&self, shares: usize) -> bool {
        let mut current = self.0.pending.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return false;
            }
            let next = current
                .checked_add(shares)
                .assured(ACK_SHARES_FIT_IN_MEMORY);
            match self.0.pending.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn resolve_pending_share(&self) -> AckShareResolution {
        let mut current = self.0.pending.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return AckShareResolution::AlreadyComplete;
            }
            let next = current
                .checked_sub(1)
                .assured("a positive pending ACK share count can be decremented");
            match self.0.pending.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) if next == 0 => return AckShareResolution::Complete,
                Ok(_) => return AckShareResolution::Pending,
                Err(observed) => current = observed,
            }
        }
    }

    fn claim_completion(&self) -> bool {
        self.0.pending.swap(0, Ordering::AcqRel) != 0
    }

    fn publish_handoff_shares(
        &self,
        shares: usize,
        reservation: OwnershipHandoffTrackerReservation<'_>,
    ) -> bool {
        let mut current = self.0.handoff_active.load(Ordering::Acquire);
        loop {
            if current == HANDOFF_TRACKING_COMPLETE {
                return false;
            }
            let next = current.checked_add(shares);
            let next = match next {
                Some(HANDOFF_TRACKING_COMPLETE) | None => None,
                Some(next) => Some(next),
            };
            let next = next.assured(ACK_SHARES_FIT_IN_MEMORY);
            match self.0.handoff_active.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if current == 0 {
                        reservation.retain();
                    }
                    return true;
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn remove_handoff_share(&self) -> bool {
        let mut current = self.0.handoff_active.load(Ordering::Acquire);
        loop {
            if current == HANDOFF_TRACKING_COMPLETE {
                return false;
            }
            if current == 0 {
                debug_assert!(
                    current > 0,
                    "an active ACK share must exist before it leaves"
                );
                return false;
            }
            let next = current
                .checked_sub(1)
                .assured("a positive active ACK share count can be decremented");
            match self.0.handoff_active.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if next == 0 {
                        self.decrement_ownership_handoff_trackers();
                    }
                    return true;
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn finish_handoff_tracking(&self) {
        let active = self
            .0
            .handoff_active
            .swap(HANDOFF_TRACKING_COMPLETE, Ordering::AcqRel);
        debug_assert_ne!(
            active, HANDOFF_TRACKING_COMPLETE,
            "ACK handoff tracking must complete only once"
        );
        if active != 0 && active != HANDOFF_TRACKING_COMPLETE {
            self.decrement_ownership_handoff_trackers();
        }
    }

    fn mark_required_wait(&self) -> bool {
        if self.0.root_trackers.is_empty() {
            return false;
        }
        // Pending shares never return from zero, so a handle that reads zero here owns no share
        // to park, including one whose attachment found its root already resolved.
        if self.0.pending.load(Ordering::Acquire) == 0 {
            return false;
        }
        self.remove_handoff_share()
    }

    fn leave_required_wait(&self) {
        if self.0.root_trackers.is_empty() {
            return;
        }
        let reservation = self.reserve_ownership_handoff_trackers();
        self.publish_handoff_shares(1, reservation);
    }

    fn finish_completion(&self, result: AckOutcome) {
        if let Some(sender) = self.0.sender.lock().take() {
            sender.send(result).means_peer_left("ack completion waiter");
        }
    }

    fn release_root_trackers(&self) {
        for tracker in &self.0.root_trackers {
            tracker.outstanding.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn ack_success_tracked(&self) {
        match self.resolve_pending_share() {
            AckShareResolution::Complete => {
                self.finish_handoff_tracking();
                self.release_root_trackers();
                self.finish_completion(AckOutcome::Ack);
            }
            AckShareResolution::Pending => {
                // The share was pending, so it is still counted as active unless a concurrent
                // terminal transition closed handoff tracking, which leaves nothing to remove.
                self.remove_handoff_share();
            }
            AckShareResolution::AlreadyComplete => {}
        }
    }

    fn complete_tracked(&self, result: AckOutcome) {
        if !self.claim_completion() {
            return;
        }
        self.finish_handoff_tracking();
        self.release_root_trackers();
        self.finish_completion(result);
    }

    pub fn clone_attached_for_receivers(&self, receivers: usize) -> Self {
        debug_assert!(
            receivers > 0,
            "attached clone requires at least one receiver"
        );
        if self.0.root_trackers.is_empty() {
            self.reserve_pending_shares(receivers);
            return self.clone();
        }
        let reservation = self.reserve_ownership_handoff_trackers();
        if !self.reserve_pending_shares(receivers) {
            return self.clone();
        }
        self.publish_handoff_shares(receivers, reservation);
        self.clone()
    }

    pub fn ack_alive(&self) {
        if self.0.pending.load(Ordering::Acquire) == 0 {
            return;
        }

        let next = self.0.alive_counter.fetch_add(1, Ordering::AcqRel) + 1;
        self.0.alive_tx.send_replace(next);
    }

    pub fn ack_success(&self) {
        if !self.0.root_trackers.is_empty() {
            self.ack_success_tracked();
            return;
        }
        if self.resolve_pending_share() == AckShareResolution::Complete {
            self.finish_completion(AckOutcome::Ack);
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
        if !self.claim_completion() {
            return;
        }
        self.finish_completion(result);
    }
}

impl Drop for AckState {
    fn drop(&mut self) {
        if self.pending.load(Ordering::Acquire) != 0 && !self.root_trackers.is_empty() {
            let handoff_active = self.handoff_active.load(Ordering::Acquire);
            for tracker in &self.root_trackers {
                tracker.outstanding.fetch_sub(1, Ordering::AcqRel);
                if handoff_active != 0 && handoff_active != HANDOFF_TRACKING_COMPLETE {
                    tracker
                        .ownership_handoff_outstanding
                        .fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
    }
}

impl OwnershipHandoffTrackerReservation<'_> {
    fn retain(mut self) {
        self.release_on_drop = false;
    }
}

impl Drop for OwnershipHandoffTrackerReservation<'_> {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.handle.decrement_ownership_handoff_trackers();
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

    /// Splits this set into `shares` sets that each resolve one share of its acknowledgement, and
    /// appends them to `sets`.
    ///
    /// The first share is this set's own and every further share is attached to it, so the
    /// acknowledgement resolves only once every share has, and a negative acknowledgement of any
    /// share resolves it negatively. Splitting into no shares leaves nothing to wait for, so this
    /// set's own share resolves here.
    pub fn split_into(self, shares: usize, sets: &mut Vec<AckSet>) {
        let Some(further_shares) = shares.checked_sub(1) else {
            self.ack_success();
            return;
        };
        if further_shares == 0 {
            sets.push(self);
            return;
        }
        let further = self.attached_for_receivers(further_shares);
        sets.push(self);
        for _ in 1..further_shares {
            sets.push(further.clone());
        }
        sets.push(further);
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
    async fn split_shares_resolve_the_set_only_once_every_share_has() {
        let tracker = Arc::new(AckRootTracker::default());
        let (acks, completion) = AckSet::tracked_root(tracker.clone());
        let mut shares = Vec::new();

        acks.split_into(3, &mut shares);

        assert_eq!(shares.len(), 3);
        shares[0].ack_success();
        shares[1].ack_success();
        assert_eq!(tracker.outstanding(), 1);
        shares[2].ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        assert_eq!(tracker.outstanding(), 0);
    }

    #[tokio::test]
    async fn a_negative_acknowledgement_of_one_split_share_resolves_the_set_negatively() {
        let (acks, completion) = AckSet::root();
        let mut shares = Vec::new();

        acks.split_into(2, &mut shares);
        shares[0].ack_success();
        shares[1].no_ack("sink rejected the message");

        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck("sink rejected the message".to_string())
        );
    }

    #[tokio::test]
    async fn splitting_into_no_shares_resolves_the_set() {
        let tracker = Arc::new(AckRootTracker::default());
        let (acks, completion) = AckSet::tracked_root(tracker.clone());
        let mut shares = Vec::new();

        acks.split_into(0, &mut shares);

        assert!(shares.is_empty());
        assert_eq!(completion.wait().await, AckOutcome::Ack);
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

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use shuttle::thread;
    use tokio::sync::oneshot::error::TryRecvError;
    use triomphe::Arc;

    use super::{
        AckCompletion, AckHandle, AckOutcome, AckRequiredWaitGuard, AckRootTracker, AckSet,
        HANDOFF_TRACKING_COMPLETE, Ordering,
    };
    use crate::shuttle_test::{check_dfs, check_pct};

    // Models with more than three tasks have too many interleavings to enumerate, so they sample
    // schedules that need up to `PCT_DEPTH` ordering constraints to fail.
    const PCT_ITERATIONS: usize = 1_000;
    const PCT_DEPTH: usize = 3;

    const JOINED: &str =
        "a modeled thread's panic fails the Shuttle execution before its joiner resumes";
    const STOPPED_WHILE_WAITING: &str =
        "node stopped while waiting for required materialized state";

    /// One acknowledgement root, observed through a handle that owns none of its shares.
    struct ObservedRoot {
        observer: AckHandle,
        completion: AckCompletion,
        /// The outcome the completion receiver delivered, once a quiescent point observed it.
        outcome: Option<AckOutcome>,
    }

    /// The trackers of one domain with two ingestors. Every root counts against the domain and
    /// against the ingestor that created it, as the runtime tracks ingested payloads.
    struct DomainTrackers {
        domain: Arc<AckRootTracker>,
        first_ingestor: Arc<AckRootTracker>,
        second_ingestor: Arc<AckRootTracker>,
    }

    impl ObservedRoot {
        /// Creates a root counted against `trackers` and returns its root share beside it.
        fn tracked(trackers: Vec<Arc<AckRootTracker>>) -> (AckSet, Self) {
            let (root, completion) = AckSet::tracked_roots(trackers);
            let observer = root
                .handles
                .first()
                .assured("a root set holds the handle of the root it creates")
                .clone();
            let observed = Self {
                observer,
                completion,
                outcome: None,
            };
            (root, observed)
        }

        fn pending_shares(&self) -> usize {
            self.observer.0.pending.load(Ordering::Acquire)
        }

        fn handoff_active_shares(&self) -> usize {
            self.observer.0.handoff_active.load(Ordering::Acquire)
        }

        fn is_outstanding(&self) -> bool {
            self.pending_shares() != 0
        }

        fn holds_ownership_handoff(&self) -> bool {
            let active = self.handoff_active_shares();
            active != 0 && active != HANDOFF_TRACKING_COMPLETE
        }

        /// The outcome a quiescent point observed this root deliver.
        fn outcome(&self) -> Option<&AckOutcome> {
            self.outcome.as_ref()
        }

        /// Asserts this root where no operation on it is in flight, while `parked_shares` of its
        /// pending shares wait on `REQUIRED WAIT`.
        ///
        /// An unresolved root counts exactly its pending shares that are not parked as active and
        /// has delivered no outcome. A resolved root won one terminal transition: it closed handoff
        /// tracking, and its receiver delivered one outcome and nothing after it.
        fn assert_quiescent(&mut self, parked_shares: usize) {
            let pending = self.pending_shares();
            let handoff_active = self.handoff_active_shares();
            if pending == 0 {
                assert_eq!(
                    handoff_active, HANDOFF_TRACKING_COMPLETE,
                    "a resolved root must have closed handoff tracking"
                );
                self.observe_single_outcome();
                return;
            }

            assert!(
                pending >= parked_shares,
                "a parked share stays pending until it is resolved, but {pending} shares are \
                 pending while {parked_shares} are parked"
            );
            let active = pending
                .checked_sub(parked_shares)
                .verified("the assertion above bounds parked shares by pending shares");
            assert_eq!(
                handoff_active, active,
                "an unresolved root must count exactly its pending shares that are not parked as \
                 active"
            );
            assert_eq!(
                self.completion.receiver.try_recv(),
                Err(TryRecvError::Empty),
                "an unresolved root must not deliver an outcome"
            );
        }

        fn observe_single_outcome(&mut self) {
            if self.outcome.is_none() {
                match self.completion.receiver.try_recv() {
                    Ok(outcome) => self.outcome = Some(outcome),
                    Err(error) => panic!(
                        "a resolved root must deliver its outcome, but its receiver reported \
                         {error:?}"
                    ),
                }
            }
            assert_eq!(
                self.completion.receiver.try_recv(),
                Err(TryRecvError::Closed),
                "a resolved root must deliver exactly one outcome"
            );
        }
    }

    impl DomainTrackers {
        fn new() -> Self {
            Self {
                domain: Arc::new(AckRootTracker::default()),
                first_ingestor: Arc::new(AckRootTracker::default()),
                second_ingestor: Arc::new(AckRootTracker::default()),
            }
        }

        fn first_ingestor_root(&self) -> (AckSet, ObservedRoot) {
            ObservedRoot::tracked(vec![self.domain.clone(), self.first_ingestor.clone()])
        }

        fn second_ingestor_root(&self) -> (AckSet, ObservedRoot) {
            ObservedRoot::tracked(vec![self.domain.clone(), self.second_ingestor.clone()])
        }

        /// Asserts at a quiescent point that the domain counts every root and that each ingestor
        /// counts exactly the roots it created.
        fn assert_counts(
            &self,
            first_ingestor_roots: &[&ObservedRoot],
            second_ingestor_roots: &[&ObservedRoot],
        ) {
            let domain_roots = first_ingestor_roots
                .iter()
                .chain(second_ingestor_roots)
                .copied()
                .collect::<Vec<_>>();
            assert_tracker_counts(&self.domain, &domain_roots);
            assert_tracker_counts(&self.first_ingestor, first_ingestor_roots);
            assert_tracker_counts(&self.second_ingestor, second_ingestor_roots);
        }
    }

    /// Asserts at a quiescent point that `tracker` counts exactly `roots`, the roots it tracks:
    /// each unresolved root is outstanding once, and each root with an active share holds
    /// ownership handoff once. A root that released its counts twice wraps the tracker below zero.
    fn assert_tracker_counts(tracker: &AckRootTracker, roots: &[&ObservedRoot]) {
        let outstanding = roots
            .iter()
            .map(|root| usize::from(root.is_outstanding()))
            .sum::<usize>();
        let ownership_handoff = roots
            .iter()
            .map(|root| usize::from(root.holds_ownership_handoff()))
            .sum::<usize>();
        assert_eq!(
            tracker.outstanding(),
            outstanding,
            "a tracker must count each unresolved root it tracks once"
        );
        assert_eq!(
            tracker.outstanding_for_ownership_handoff(),
            ownership_handoff,
            "a tracker must hold ownership handoff once for each root with an active share"
        );
    }

    #[test]
    fn concurrent_attachment_and_final_ack_leave_exact_tracking() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, mut observed) = ObservedRoot::tracked(vec![tracker.clone()]);
                let attachment_source = root.clone();

                let attach_thread = thread::spawn(move || attachment_source.attached());
                let ack_thread = thread::spawn(move || root.ack_success());

                let attached = attach_thread.join().assured(JOINED);
                ack_thread.join().assured(JOINED);
                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);

                attached.ack_success();
                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);
                assert_eq!(observed.outcome(), Some(&AckOutcome::Ack));
            },
            None,
        );
    }

    #[test]
    fn concurrent_wait_and_active_ack_exempt_the_remaining_root() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, mut observed) = ObservedRoot::tracked(vec![tracker.clone()]);
                let attached = root.attached();
                let waiting = root.clone();

                let wait_thread = thread::spawn(move || AckRequiredWaitGuard::new([&waiting]));
                let ack_thread = thread::spawn(move || attached.ack_success());

                let required_wait = wait_thread.join().assured(JOINED);
                ack_thread.join().assured(JOINED);
                observed.assert_quiescent(1);
                assert_tracker_counts(&tracker, &[&observed]);
                assert_eq!(tracker.outstanding(), 1);
                assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);

                drop(required_wait);
                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);
                assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);

                root.no_ack("test completion");
                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);
                assert_eq!(
                    observed.outcome(),
                    Some(&AckOutcome::NoAck("test completion".to_string()))
                );
            },
            None,
        );
    }

    #[test]
    fn concurrent_wait_release_and_completion_leave_no_tracking() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, mut observed) = ObservedRoot::tracked(vec![tracker.clone()]);
                let required_wait = AckRequiredWaitGuard::new([&root]);

                let release_thread = thread::spawn(move || drop(required_wait));
                let completion_thread = thread::spawn(move || root.no_ack("test completion"));

                release_thread.join().assured(JOINED);
                completion_thread.join().assured(JOINED);

                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);
                assert_eq!(
                    observed.outcome(),
                    Some(&AckOutcome::NoAck("test completion".to_string()))
                );
            },
            None,
        );
    }

    #[test]
    fn concurrent_success_and_failure_choose_one_terminal_transition() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, mut observed) = ObservedRoot::tracked(vec![tracker.clone()]);
                let competing = root.clone();

                let ack_thread = thread::spawn(move || root.ack_success());
                let no_ack_thread = thread::spawn(move || competing.no_ack("test failure"));

                ack_thread.join().assured(JOINED);
                no_ack_thread.join().assured(JOINED);

                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);
                match observed.outcome() {
                    Some(AckOutcome::Ack) => {}
                    Some(AckOutcome::NoAck(reason)) => assert_eq!(reason, "test failure"),
                    None => panic!("both terminal transitions ran, so the root must be resolved"),
                }
            },
            None,
        );
    }

    /// A processor attaches an output to its active share while another share of the root, parked
    /// on `REQUIRED WAIT`, is negatively acknowledged, as a node stopping during the wait does.
    /// When the failure claims the root first, the attachment reserves no share, so acknowledging
    /// its output must not remove the active share the processor still owns.
    #[test]
    fn attachment_losing_its_reservation_to_completion_resolves_no_share() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (processing, mut observed) = ObservedRoot::tracked(vec![tracker.clone()]);
                let waiting = processing.attached();
                let required_wait = AckRequiredWaitGuard::new([&waiting]);

                let stopping = thread::spawn(move || waiting.no_ack(STOPPED_WHILE_WAITING));
                let processor = thread::spawn(move || {
                    let output = processing.attached();
                    output.ack_success();
                    processing.ack_success();
                });

                stopping.join().assured(JOINED);
                processor.join().assured(JOINED);
                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);

                drop(required_wait);
                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);
                assert_eq!(
                    observed.outcome(),
                    Some(&AckOutcome::NoAck(STOPPED_WHILE_WAITING.to_string()))
                );
            },
            None,
        );
    }

    /// The same race as above, where the output's consumer parks on `REQUIRED WAIT` before the
    /// processor acknowledges its own share. An attachment that reserved no share must park none.
    #[test]
    fn attachment_losing_its_reservation_to_completion_parks_no_share() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (processing, mut observed) = ObservedRoot::tracked(vec![tracker.clone()]);
                let waiting = processing.attached();
                let required_wait = AckRequiredWaitGuard::new([&waiting]);

                let stopping = thread::spawn(move || waiting.no_ack(STOPPED_WHILE_WAITING));
                let processor = thread::spawn(move || {
                    let output = processing.attached();
                    let output_wait = AckRequiredWaitGuard::new([&output]);
                    processing.ack_success();
                    (output, output_wait)
                });

                stopping.join().assured(JOINED);
                let (output, output_wait) = processor.join().assured(JOINED);
                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);

                drop(output_wait);
                output.ack_success();
                drop(required_wait);
                observed.assert_quiescent(0);
                assert_tracker_counts(&tracker, &[&observed]);
                assert_eq!(
                    observed.outcome(),
                    Some(&AckOutcome::NoAck(STOPPED_WHILE_WAITING.to_string()))
                );
            },
            None,
        );
    }

    /// A parked share leaves `REQUIRED WAIT` while the root's only active share is acknowledged,
    /// so the active count drops to zero whenever the acknowledgement removes its share before
    /// the released share is published again.
    #[test]
    fn wait_release_racing_the_last_active_ack_holds_domain_and_ingestor_handoff_once() {
        check_dfs(
            || {
                let trackers = DomainTrackers::new();
                let (active, mut observed) = trackers.first_ingestor_root();
                let waiting = active.attached();
                let required_wait = AckRequiredWaitGuard::new([&waiting]);

                let release = thread::spawn(move || drop(required_wait));
                let acknowledgement = thread::spawn(move || active.ack_success());

                release.join().assured(JOINED);
                acknowledgement.join().assured(JOINED);
                observed.assert_quiescent(0);
                trackers.assert_counts(&[&observed], &[]);
                assert_eq!(trackers.domain.outstanding_for_ownership_handoff(), 1);

                waiting.ack_success();
                observed.assert_quiescent(0);
                trackers.assert_counts(&[&observed], &[]);
                assert_eq!(observed.outcome(), Some(&AckOutcome::Ack));
            },
            None,
        );
    }

    /// A share is attached while the root's only active share parks on `REQUIRED WAIT`, so the
    /// active count drops to zero whenever the share parks before the attachment publishes.
    #[test]
    fn attachment_racing_the_last_active_share_into_wait_publishes_one_active_share() {
        check_dfs(
            || {
                let trackers = DomainTrackers::new();
                let (root, mut observed) = trackers.first_ingestor_root();
                let attachment_source = root.clone();
                let waiting = root.clone();

                let attachment = thread::spawn(move || attachment_source.attached());
                let wait = thread::spawn(move || AckRequiredWaitGuard::new([&waiting]));

                let attached = attachment.join().assured(JOINED);
                let required_wait = wait.join().assured(JOINED);
                observed.assert_quiescent(1);
                trackers.assert_counts(&[&observed], &[]);
                assert_eq!(
                    trackers.first_ingestor.outstanding_for_ownership_handoff(),
                    1
                );

                drop(required_wait);
                attached.ack_success();
                root.ack_success();
                observed.assert_quiescent(0);
                trackers.assert_counts(&[&observed], &[]);
                assert_eq!(observed.outcome(), Some(&AckOutcome::Ack));
            },
            None,
        );
    }

    /// One domain ingests from two ingestors. A processor batches a message of every root and fans
    /// its output out to two consumers, one of which waits on required state. Meanwhile another
    /// message of the first payload waits on required state, and the root of the second ingestor
    /// fails while it waits, as a node stopping during the wait fails it.
    #[test]
    fn fan_out_across_ingestors_with_a_failing_root_resolves_each_root_once_with_exact_counts() {
        check_pct(
            || {
                let trackers = DomainTrackers::new();
                let (first_payload, mut first) = trackers.first_ingestor_root();
                let (second_payload, mut second) = trackers.first_ingestor_root();
                let (failing_payload, mut failing) = trackers.second_ingestor_root();

                let mut first_messages = Vec::new();
                first_payload.split_into(2, &mut first_messages);
                let waiting_message = first_messages
                    .pop()
                    .assured("splitting a payload into two shares appends two sets");
                let batched_message = first_messages
                    .pop()
                    .assured("splitting a payload into two shares appends two sets");
                let batch =
                    AckSet::merged([batched_message, second_payload, failing_payload.attached()]);

                let processor = thread::spawn(move || {
                    let output = batch.attached_for_receivers(2);
                    let waiting_consumer_share = output.clone();
                    let waiting_consumer = thread::spawn(move || {
                        let required_wait = AckRequiredWaitGuard::new([&waiting_consumer_share]);
                        drop(required_wait);
                        waiting_consumer_share.ack_success();
                    });
                    let consumer = thread::spawn(move || output.ack_success());
                    batch.ack_success();
                    waiting_consumer.join().assured(JOINED);
                    consumer.join().assured(JOINED);
                });
                let waiting = thread::spawn(move || {
                    let required_wait = AckRequiredWaitGuard::new([&waiting_message]);
                    drop(required_wait);
                    waiting_message.ack_success();
                });
                let stopping = thread::spawn(move || {
                    let required_wait = AckRequiredWaitGuard::new([&failing_payload]);
                    failing_payload.no_ack(STOPPED_WHILE_WAITING);
                    drop(required_wait);
                });

                processor.join().assured(JOINED);
                waiting.join().assured(JOINED);
                stopping.join().assured(JOINED);

                first.assert_quiescent(0);
                second.assert_quiescent(0);
                failing.assert_quiescent(0);
                trackers.assert_counts(&[&first, &second], &[&failing]);
                assert_eq!(first.outcome(), Some(&AckOutcome::Ack));
                assert_eq!(second.outcome(), Some(&AckOutcome::Ack));
                assert_eq!(
                    failing.outcome(),
                    Some(&AckOutcome::NoAck(STOPPED_WHILE_WAITING.to_string()))
                );
            },
            PCT_ITERATIONS,
            PCT_DEPTH,
        );
    }

    /// Three roots from two ingestors first park and fan out concurrently, then concurrently
    /// resolve, leave their wait and fail twice over.
    #[test]
    fn parked_and_fanned_out_roots_hold_exact_counts_at_every_quiescent_point() {
        check_pct(
            || {
                let trackers = DomainTrackers::new();
                let (batched_payload, mut batched) = trackers.first_ingestor_root();
                let (failing_payload, mut failing) = trackers.second_ingestor_root();
                let (fanned_payload, mut fanned) = trackers.first_ingestor_root();

                let rejected_by_sink = failing_payload.attached();
                let rejected_by_route = failing_payload.attached();
                let batch = AckSet::merged([batched_payload, failing_payload]);
                let fan_out_source = fanned_payload.attached();

                let processor = thread::spawn(move || {
                    let output = batch.attached();
                    batch.ack_success();
                    output
                });
                let parking = thread::spawn(move || {
                    let required_wait = AckRequiredWaitGuard::new([&fanned_payload]);
                    (fanned_payload, required_wait)
                });
                let fan_out = thread::spawn(move || {
                    let consumer_share = fan_out_source.attached_for_receivers(2);
                    fan_out_source.ack_success();
                    consumer_share
                });

                let output = processor.join().assured(JOINED);
                let (parked_message, required_wait) = parking.join().assured(JOINED);
                let first_consumer_share = fan_out.join().assured(JOINED);
                let second_consumer_share = first_consumer_share.clone();
                batched.assert_quiescent(0);
                failing.assert_quiescent(0);
                fanned.assert_quiescent(1);
                trackers.assert_counts(&[&batched, &fanned], &[&failing]);

                let output_consumer = thread::spawn(move || output.ack_success());
                let sink =
                    thread::spawn(move || rejected_by_sink.no_ack("sink rejected the message"));
                let route = thread::spawn(move || {
                    rejected_by_route.no_ack("error route rejected the message");
                });
                let release = thread::spawn(move || {
                    drop(required_wait);
                    parked_message.ack_success();
                });
                let first_consumer = thread::spawn(move || first_consumer_share.ack_success());
                let second_consumer = thread::spawn(move || second_consumer_share.ack_success());

                output_consumer.join().assured(JOINED);
                sink.join().assured(JOINED);
                route.join().assured(JOINED);
                release.join().assured(JOINED);
                first_consumer.join().assured(JOINED);
                second_consumer.join().assured(JOINED);
                batched.assert_quiescent(0);
                failing.assert_quiescent(0);
                fanned.assert_quiescent(0);
                trackers.assert_counts(&[&batched, &fanned], &[&failing]);
                assert_eq!(batched.outcome(), Some(&AckOutcome::Ack));
                assert_eq!(fanned.outcome(), Some(&AckOutcome::Ack));
                match failing.outcome() {
                    Some(AckOutcome::NoAck(reason)) => assert!(
                        reason == "sink rejected the message"
                            || reason == "error route rejected the message",
                        "the failing root must resolve with one of its two rejections, not \
                         {reason:?}"
                    ),
                    other => panic!(
                        "both rejections ran, so the failing root must resolve negatively, not \
                         {other:?}"
                    ),
                }
            },
            PCT_ITERATIONS,
            PCT_DEPTH,
        );
    }
}
