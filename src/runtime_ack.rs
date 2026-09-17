//! The acknowledgement tree one node keeps in memory while work is outstanding.
//!
//! Layer: data plane.
//!
//! - **Owns.** The root tracker per ingestor and per domain, the sets a batch fans out into, the
//!   guard that holds a message waiting on materialized state, the one-word encoding of each
//!   root's ownership-handoff tracking, and the outcome each root resolves to.
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

const ACK_SHARES_FIT_IN_MEMORY: &str =
    "every pending ACK share has an in-memory owner, so their count fits in usize";
const ACK_ACTIVE_SHARES_FIT_IN_ENCODING: &str = "every active ACK share has an in-memory owner, \
                                                 so their count stays below the word the handoff \
                                                 encoding reserves";

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

/// What one root's ownership-handoff tracking holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckHandoffState {
    /// The root still tracks shares. No active share means every share it still holds is parked on
    /// `REQUIRED WAIT`, which an ownership handoff does not wait for.
    Tracking { active_shares: usize },
    /// The root resolved, so nothing it tracked is outstanding and nothing may make it active
    /// again.
    Complete,
}

/// What publishing active shares did to one root's tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckHandoffPublication {
    /// The shares made an idle root active, so an ownership handoff waits for it once more.
    Activated,
    /// The shares joined a root an ownership handoff already waits for.
    Joined,
    /// The root resolved before the shares were published, so it tracks none of them.
    AlreadyComplete,
}

/// What removing one active share did to one root's tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckHandoffRemoval {
    /// The share left and active shares remain.
    Removed,
    /// The last active share left, so an ownership handoff no longer waits for this root.
    WentIdle,
    /// The root resolved, so it holds no share to remove.
    AlreadyComplete,
    /// The root tracks no active share, so the caller owned none to remove.
    NoActiveShare,
}

/// The [`AckHandoffState`] of one root, in one word.
///
/// An attachment, a wait release, and a completion race one another, so each transition is one
/// atomic operation on this word rather than a flag beside a count or a lock around both. The
/// encoding reserves the word's top value for [`AckHandoffState::Complete`] and holds the active
/// share count below it, and the reserved value never leaves this type.
#[derive(Debug)]
struct AckHandoffTracking(AtomicUsize);

#[derive(Debug)]
struct AckState {
    /// Zero is terminal. Attaching a share reserves it here before publishing it as active below.
    pending: AtomicUsize,
    /// The pending shares that are not parked on `REQUIRED WAIT`. Completing the root closes this
    /// tracking, so a racing wait release or attachment cannot reactivate it.
    ///
    /// The root trackers count roots rather than shares. Publishing onto an idle root therefore
    /// reserves one tracker count before publishing the active share count, and removing the last
    /// active share publishes the idle count before releasing the tracker count. The tracker may
    /// briefly overcount either transition, but it never lets an ownership handoff miss active
    /// work.
    handoff: AckHandoffTracking,
    alive_counter: AtomicU64,
    alive_tx: watch::Sender<u64>,
    sender: Mutex<Option<oneshot::Sender<AckOutcome>>>,
    root_trackers: Vec<Arc<AckRootTracker>>,
}

struct OwnershipHandoffTrackerReservation<'a> {
    handle: &'a AckHandle,
    release_on_drop: bool,
}

impl AckHandoffState {
    /// Whether an ownership handoff must still wait for this root.
    fn holds_active_shares(self) -> bool {
        match self {
            Self::Tracking { active_shares } => active_shares != 0,
            Self::Complete => false,
        }
    }
}

impl AckHandoffRemoval {
    /// Whether this removal took the caller's share off the active count.
    fn removed_share(self) -> bool {
        match self {
            Self::Removed | Self::WentIdle => true,
            Self::AlreadyComplete | Self::NoActiveShare => false,
        }
    }
}

impl AckHandoffTracking {
    /// The word this encoding reserves for [`AckHandoffState::Complete`].
    const COMPLETE: usize = usize::MAX;
    /// The greatest active-share count this encoding holds, directly below the reserved word.
    const MAX_ACTIVE_SHARES: usize = Self::COMPLETE - 1;

    fn tracking(active_shares: usize) -> Self {
        Self(AtomicUsize::new(Self::encode(AckHandoffState::Tracking {
            active_shares,
        })))
    }

    fn state(&self) -> AckHandoffState {
        Self::decode(self.0.load(Ordering::Acquire))
    }

    /// Publishes the `shares` the caller owns as active shares of a root that has not resolved.
    fn publish_shares(&self, shares: usize) -> AckHandoffPublication {
        debug_assert_ne!(
            shares, 0,
            "a caller publishes only shares it owns, and owning none publishes nothing"
        );
        let mut word = self.0.load(Ordering::Acquire);
        loop {
            let AckHandoffState::Tracking { active_shares } = Self::decode(word) else {
                return AckHandoffPublication::AlreadyComplete;
            };
            let published = Self::total_active_shares(active_shares, shares)
                .assured(ACK_ACTIVE_SHARES_FIT_IN_ENCODING);
            let next = Self::encode(AckHandoffState::Tracking {
                active_shares: published,
            });
            match self
                .0
                .compare_exchange_weak(word, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) if active_shares == 0 => return AckHandoffPublication::Activated,
                Ok(_) => return AckHandoffPublication::Joined,
                Err(observed) => word = observed,
            }
        }
    }

    /// Removes the one active share the caller owns.
    fn remove_share(&self) -> AckHandoffRemoval {
        let mut word = self.0.load(Ordering::Acquire);
        loop {
            let AckHandoffState::Tracking { active_shares } = Self::decode(word) else {
                return AckHandoffRemoval::AlreadyComplete;
            };
            let Some(remaining) = active_shares.checked_sub(1) else {
                debug_assert_ne!(
                    active_shares, 0,
                    "a handle removes an active ACK share only while it owns one"
                );
                return AckHandoffRemoval::NoActiveShare;
            };
            let next = Self::encode(AckHandoffState::Tracking {
                active_shares: remaining,
            });
            match self
                .0
                .compare_exchange_weak(word, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) if remaining == 0 => return AckHandoffRemoval::WentIdle,
                Ok(_) => return AckHandoffRemoval::Removed,
                Err(observed) => word = observed,
            }
        }
    }

    /// Closes tracking for good and reports the state that closing replaced.
    fn complete(&self) -> AckHandoffState {
        Self::decode(
            self.0
                .swap(Self::encode(AckHandoffState::Complete), Ordering::AcqRel),
        )
    }

    /// The total of `active_shares` and `published`, or `None` when that total would not stay
    /// below the reserved word.
    fn total_active_shares(active_shares: usize, published: usize) -> Option<usize> {
        let total = active_shares.checked_add(published)?;
        if total > Self::MAX_ACTIVE_SHARES {
            return None;
        }
        Some(total)
    }

    fn decode(word: usize) -> AckHandoffState {
        if word == Self::COMPLETE {
            return AckHandoffState::Complete;
        }
        AckHandoffState::Tracking {
            active_shares: word,
        }
    }

    fn encode(state: AckHandoffState) -> usize {
        match state {
            AckHandoffState::Tracking { active_shares } => {
                debug_assert!(
                    active_shares <= Self::MAX_ACTIVE_SHARES,
                    "an active ACK share count stays below the word the encoding reserves"
                );
                active_shares
            }
            AckHandoffState::Complete => Self::COMPLETE,
        }
    }
}

// The reserved word sits above every active-share count the encoding admits, so a count reaching
// it would read back as a completed root.
const _: () = assert!(AckHandoffTracking::MAX_ACTIVE_SHARES < AckHandoffTracking::COMPLETE);

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
        // A tracked root starts with its own share active. An untracked root tracks no handoff
        // share at all, so it starts idle and stays idle.
        let active_shares = usize::from(!root_trackers.is_empty());
        (
            Self(Arc::new(AckState {
                pending: AtomicUsize::new(1),
                handoff: AckHandoffTracking::tracking(active_shares),
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

    /// Publishes `shares` the caller reserved a tracker count for, releasing that reservation
    /// unless the shares are what an ownership handoff now waits for.
    fn publish_handoff_shares(
        &self,
        shares: usize,
        reservation: OwnershipHandoffTrackerReservation<'_>,
    ) {
        match self.0.handoff.publish_shares(shares) {
            AckHandoffPublication::Activated => reservation.retain(),
            AckHandoffPublication::Joined | AckHandoffPublication::AlreadyComplete => {}
        }
    }

    /// Removes one active share, and reports whether the caller's share was the one removed.
    fn remove_handoff_share(&self) -> bool {
        let removal = self.0.handoff.remove_share();
        match removal {
            AckHandoffRemoval::WentIdle => self.decrement_ownership_handoff_trackers(),
            AckHandoffRemoval::Removed
            | AckHandoffRemoval::AlreadyComplete
            | AckHandoffRemoval::NoActiveShare => {}
        }
        removal.removed_share()
    }

    fn finish_handoff_tracking(&self) {
        let previous = self.0.handoff.complete();
        debug_assert_ne!(
            previous,
            AckHandoffState::Complete,
            "ACK handoff tracking must complete only once"
        );
        if previous.holds_active_shares() {
            self.decrement_ownership_handoff_trackers();
        }
    }

    fn mark_required_wait(&self) -> bool {
        if self.0.root_trackers.is_empty() {
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
        if !self.remove_handoff_share() {
            return;
        }
        match self.resolve_pending_share() {
            AckShareResolution::Complete => {
                self.finish_handoff_tracking();
                self.release_root_trackers();
                self.finish_completion(AckOutcome::Ack);
            }
            AckShareResolution::AlreadyComplete | AckShareResolution::Pending => {}
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

    /// Whether every share of this root has resolved, which is terminal.
    fn is_resolved(&self) -> bool {
        self.0.pending.load(Ordering::Acquire) == 0
    }

    pub fn ack_alive(&self) {
        if self.is_resolved() {
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
            let handoff = self.handoff.state();
            for tracker in &self.root_trackers {
                tracker.outstanding.fetch_sub(1, Ordering::AcqRel);
                if handoff.holds_active_shares() {
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
    use shuttle::thread;
    use triomphe::Arc;

    use super::{AckHandoffState, AckRequiredWaitGuard, AckRootTracker, AckSet};
    use crate::shuttle_test::check_dfs;

    /// Asserts that `tracker` counts `root` exactly as the root's own state says it should: once
    /// while the root is unresolved, and once for ownership handoff while it holds an active
    /// share. A count released twice wraps below zero, so an undercount fails here as loudly as
    /// an overcount.
    fn assert_tracker_matches_root(root: &AckSet, tracker: &AckRootTracker) {
        let handle = &root.handles[0];
        let expected_outstanding = usize::from(!handle.is_resolved());
        let expected_handoff_outstanding =
            usize::from(handle.0.handoff.state().holds_active_shares());

        assert_eq!(tracker.outstanding(), expected_outstanding);
        assert_eq!(
            tracker.outstanding_for_ownership_handoff(),
            expected_handoff_outstanding
        );
    }

    /// Asserts that `root` resolved, closed its handoff tracking, and left nothing counted against
    /// `tracker`.
    fn assert_root_is_complete(root: &AckSet, tracker: &AckRootTracker) {
        let handle = &root.handles[0];

        assert!(
            handle.is_resolved(),
            "a completed root has resolved every share"
        );
        assert_eq!(
            handle.0.handoff.state(),
            AckHandoffState::Complete,
            "a completed root closes its handoff tracking"
        );
        assert_tracker_matches_root(root, tracker);
        assert_eq!(tracker.outstanding(), 0);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    }

    #[test]
    fn concurrent_attachment_and_final_ack_leave_exact_tracking() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, completion) = AckSet::tracked_root(tracker.clone());
                let attachment_source = root.clone();
                let observer = root.clone();

                let attach_thread = thread::spawn(move || attachment_source.attached());
                let ack_thread = thread::spawn(move || root.ack_success());

                let attached = attach_thread.join().expect("attachment thread should join");
                ack_thread.join().expect("ack thread should join");
                assert_tracker_matches_root(&observer, &tracker);

                attached.ack_success();
                assert_root_is_complete(&observer, &tracker);
                drop(completion);
            },
            None,
        );
    }

    #[test]
    fn concurrent_wait_and_active_ack_exempt_the_remaining_root() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, completion) = AckSet::tracked_root(tracker.clone());
                let attached = root.attached();
                let waiting = root.clone();
                let observer = root.clone();

                let wait_thread = thread::spawn(move || AckRequiredWaitGuard::new([&waiting]));
                let ack_thread = thread::spawn(move || attached.ack_success());

                let required_wait = wait_thread.join().expect("wait thread should join");
                ack_thread.join().expect("ack thread should join");
                assert_tracker_matches_root(&observer, &tracker);
                assert_eq!(tracker.outstanding(), 1);
                assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);

                drop(required_wait);
                assert_tracker_matches_root(&observer, &tracker);
                assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);

                observer.no_ack("test completion");
                assert_root_is_complete(&observer, &tracker);
                drop(completion);
            },
            None,
        );
    }

    #[test]
    fn concurrent_wait_release_and_completion_leave_no_tracking() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, completion) = AckSet::tracked_root(tracker.clone());
                let required_wait = AckRequiredWaitGuard::new([&root]);
                let observer = root.clone();

                let release_thread = thread::spawn(move || drop(required_wait));
                let completion_thread = thread::spawn(move || root.no_ack("test completion"));

                release_thread.join().expect("release thread should join");
                completion_thread
                    .join()
                    .expect("completion thread should join");

                assert_root_is_complete(&observer, &tracker);
                drop(completion);
            },
            None,
        );
    }

    #[test]
    fn concurrent_success_and_failure_choose_one_terminal_transition() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, completion) = AckSet::tracked_root(tracker.clone());
                let competing = root.clone();
                let observer = root.clone();

                let ack_thread = thread::spawn(move || root.ack_success());
                let no_ack_thread = thread::spawn(move || competing.no_ack("test failure"));

                ack_thread.join().expect("ack thread should join");
                no_ack_thread.join().expect("no-ack thread should join");

                assert_root_is_complete(&observer, &tracker);
                drop(completion);
            },
            None,
        );
    }

    /// An attachment races the terminal transition, so it reserves a share before the root
    /// resolves or finds it already resolved. Either way the completed root stays complete, and
    /// acknowledging the attached share afterwards publishes no active share again.
    #[test]
    fn attachment_racing_completion_cannot_reactivate_tracking() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, completion) = AckSet::tracked_root(tracker.clone());
                let attachment_source = root.clone();
                let competing = root.clone();

                let attach_thread = thread::spawn(move || attachment_source.attached());
                let no_ack_thread = thread::spawn(move || competing.no_ack("test completion"));

                let attached = attach_thread.join().expect("attachment thread should join");
                no_ack_thread.join().expect("no-ack thread should join");
                assert_root_is_complete(&root, &tracker);

                attached.ack_success();
                assert_root_is_complete(&root, &tracker);
                drop(completion);
            },
            None,
        );
    }

    /// A share leaves `REQUIRED WAIT` while the root resolves, so it is published as active before
    /// the terminal transition or after it. A release that lands after it must leave the root
    /// complete and must not leave a tracker counting the share it reserved.
    #[test]
    fn wait_release_racing_completion_cannot_reactivate_tracking() {
        check_dfs(
            || {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, completion) = AckSet::tracked_root(tracker.clone());
                let parked = root.attached();
                let required_wait = AckRequiredWaitGuard::new([&parked]);
                let competing = root.clone();

                let release_thread = thread::spawn(move || drop(required_wait));
                let no_ack_thread = thread::spawn(move || competing.no_ack("test completion"));

                release_thread.join().expect("release thread should join");
                no_ack_thread.join().expect("no-ack thread should join");
                assert_root_is_complete(&root, &tracker);

                parked.ack_success();
                assert_root_is_complete(&root, &tracker);
                drop(completion);
            },
            None,
        );
    }
}
