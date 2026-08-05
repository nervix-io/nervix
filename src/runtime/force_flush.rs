//! Generation-aware domain force-flush coordination.
//!
//! Publishing a watch value alone cannot prove that a node observed or completed a flush. This
//! coordinator records one obligation per live participant before publishing the generation. A
//! participant receives an explicit completion token and clears the obligation only after its
//! node-specific flush attempt finishes. Retained buffers remain visible through quiesce counters
//! and drive another generation after retry; the control token does not own retry policy. Dropping
//! the participant clears its outstanding obligation, while dropping an unhandled completion
//! makes the same generation deliverable again.

use std::sync::atomic::Ordering;

use ahash::{HashMap, HashMapExt};
use parking_lot::Mutex;
use tokio::sync::watch;
use triomphe::Arc;

use super::NodeQuiesceCounters;

#[derive(Debug)]
struct ForceFlushParticipantState {
    counters: Option<Arc<NodeQuiesceCounters>>,
    pending_generation: Option<u64>,
    claimed_generation: Option<u64>,
}

#[derive(Debug)]
struct DomainForceFlushState {
    generation: u64,
    active_generation: Option<u64>,
    next_participant: u64,
    sender: Option<watch::Sender<u64>>,
    participants: HashMap<u64, ForceFlushParticipantState>,
}

/// Coordinates force-flush generations for one runtime domain.
#[derive(Debug)]
pub(super) struct DomainForceFlush {
    state: Mutex<DomainForceFlushState>,
}

impl DomainForceFlush {
    pub(super) fn new() -> Arc<Self> {
        let (sender, _) = watch::channel(0_u64);
        Arc::new(Self {
            state: Mutex::new(DomainForceFlushState {
                generation: 0,
                active_generation: None,
                next_participant: 0,
                sender: Some(sender),
                participants: HashMap::new(),
            }),
        })
    }

    pub(super) fn subscribe(
        coordinator: &Arc<Self>,
        counters: Option<Arc<NodeQuiesceCounters>>,
    ) -> DomainForceFlushParticipant {
        let mut state = coordinator.state.lock();
        state.next_participant = state.next_participant.wrapping_add(1);
        let participant = state.next_participant;
        let pending_generation = state.active_generation;
        if pending_generation.is_some()
            && let Some(counters) = &counters
        {
            counters.force_flushes.fetch_add(1, Ordering::AcqRel);
        }
        state.participants.insert(
            participant,
            ForceFlushParticipantState {
                counters,
                pending_generation,
                claimed_generation: None,
            },
        );
        let receiver = if let Some(sender) = &state.sender {
            sender.subscribe()
        } else {
            let (sender, receiver) = watch::channel(state.generation);
            drop(sender);
            receiver
        };
        DomainForceFlushParticipant {
            coordinator: coordinator.clone(),
            participant,
            receiver,
        }
    }

    /// Requests a new generation and records every obligation before publishing it.
    pub(super) fn request(&self) -> u64 {
        self.request_inner(false)
    }

    /// Requests a generation only when every participant finished the previous one.
    pub(super) fn request_if_idle(&self) -> u64 {
        self.request_inner(true)
    }

    fn request_inner(&self, only_if_idle: bool) -> u64 {
        let mut state = self.state.lock();
        if state.sender.is_none() {
            return state.generation;
        }
        if only_if_idle && let Some(generation) = state.active_generation {
            return generation;
        }
        state.generation = state.generation.wrapping_add(1);
        if state.generation == 0 {
            state.generation = 1;
        }
        let generation = state.generation;
        state.active_generation = Some(generation);
        for participant in state.participants.values_mut() {
            if participant.pending_generation.is_none()
                && let Some(counters) = &participant.counters
            {
                counters.force_flushes.fetch_add(1, Ordering::AcqRel);
            }
            participant.pending_generation = Some(generation);
            participant.claimed_generation = None;
        }
        if state.participants.is_empty() {
            state.active_generation = None;
        }
        if let Some(sender) = &state.sender {
            sender.send_replace(generation);
        }
        generation
    }

    #[cfg(test)]
    pub(super) fn pending(&self) -> usize {
        self.state
            .lock()
            .participants
            .values()
            .filter(|participant| participant.pending_generation.is_some())
            .count()
    }

    pub(super) fn close(&self) {
        let sender = {
            let mut state = self.state.lock();
            for participant in state.participants.values_mut() {
                Self::clear_participant_pending(participant);
            }
            state.active_generation = None;
            state.sender.take()
        };
        drop(sender);
    }

    fn completion(
        coordinator: &Arc<Self>,
        participant: u64,
    ) -> Result<Option<DomainForceFlushCompletion>, ()> {
        let mut state = coordinator.state.lock();
        if state.sender.is_none() {
            return Err(());
        }
        let Some(participant_state) = state.participants.get_mut(&participant) else {
            return Err(());
        };
        let Some(generation) = participant_state.pending_generation else {
            return Ok(None);
        };
        if participant_state.claimed_generation == Some(generation) {
            return Ok(None);
        }
        participant_state.claimed_generation = Some(generation);
        Ok(Some(DomainForceFlushCompletion {
            coordinator: coordinator.clone(),
            participant,
            generation,
            completed: false,
        }))
    }

    fn complete(&self, participant: u64, generation: u64) -> bool {
        let mut state = self.state.lock();
        let Some(participant) = state.participants.get_mut(&participant) else {
            return false;
        };
        if participant.pending_generation != Some(generation) {
            return false;
        }
        if participant.claimed_generation != Some(generation) {
            return false;
        }
        Self::clear_participant_pending(participant);
        if state
            .participants
            .values()
            .all(|participant| participant.pending_generation.is_none())
        {
            state.active_generation = None;
        }
        true
    }

    fn release_claim(&self, participant: u64, generation: u64) {
        let mut state = self.state.lock();
        if let Some(participant) = state.participants.get_mut(&participant)
            && participant.pending_generation == Some(generation)
            && participant.claimed_generation == Some(generation)
        {
            participant.claimed_generation = None;
        }
    }

    fn unregister(&self, participant: u64) {
        let mut state = self.state.lock();
        if let Some(mut participant) = state.participants.remove(&participant) {
            Self::clear_participant_pending(&mut participant);
        }
        if state
            .participants
            .values()
            .all(|participant| participant.pending_generation.is_none())
        {
            state.active_generation = None;
        }
    }

    fn clear_participant_pending(participant: &mut ForceFlushParticipantState) {
        participant.claimed_generation = None;
        if participant.pending_generation.take().is_some()
            && let Some(counters) = &participant.counters
        {
            let previous = counters.force_flushes.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0, "force-flush obligation count underflow");
        }
    }
}

/// One live task participating in domain force flushes.
#[derive(Debug)]
pub(super) struct DomainForceFlushParticipant {
    coordinator: Arc<DomainForceFlush>,
    participant: u64,
    receiver: watch::Receiver<u64>,
}

impl DomainForceFlushParticipant {
    pub(super) fn pending_completion(&mut self) -> Result<Option<DomainForceFlushCompletion>, ()> {
        let completion = DomainForceFlush::completion(&self.coordinator, self.participant)?;
        if completion.is_some() {
            self.receiver.borrow_and_update();
        }
        Ok(completion)
    }

    pub(super) async fn changed(&mut self) -> Result<DomainForceFlushCompletion, ()> {
        if let Some(completion) = self.pending_completion()? {
            return Ok(completion);
        }
        self.receiver.changed().await.map_err(|_| ())?;
        self.pending_completion()?.ok_or(())
    }
}

impl Drop for DomainForceFlushParticipant {
    fn drop(&mut self) {
        self.coordinator.unregister(self.participant);
    }
}

/// Proof that a specific participant owes work for one force-flush generation.
#[derive(Debug)]
pub(super) struct DomainForceFlushCompletion {
    coordinator: Arc<DomainForceFlush>,
    participant: u64,
    generation: u64,
    completed: bool,
}

impl DomainForceFlushCompletion {
    /// Marks this obligation complete after the node-specific flush attempt finishes.
    pub(super) fn complete(mut self) -> bool {
        self.completed = self.coordinator.complete(self.participant, self.generation);
        self.completed
    }

    #[cfg(test)]
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
}

impl Drop for DomainForceFlushCompletion {
    fn drop(&mut self) {
        if !self.completed {
            self.coordinator
                .release_claim(self.participant, self.generation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counters() -> Arc<NodeQuiesceCounters> {
        Arc::new(NodeQuiesceCounters::default())
    }

    #[test]
    fn request_records_all_obligations_before_delivery() {
        let coordinator = DomainForceFlush::new();
        let counters = counters();
        let mut first = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
        let mut second = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));

        let generation = coordinator.request();
        assert_eq!(coordinator.pending(), 2);
        assert_eq!(counters.force_flushes.load(Ordering::Acquire), 2);
        assert_eq!(
            first
                .pending_completion()
                .expect("participant must remain open")
                .expect("first completion must be ready")
                .generation(),
            generation
        );
        assert_eq!(
            second
                .pending_completion()
                .expect("participant must remain open")
                .expect("second completion must be ready")
                .generation(),
            generation
        );
        assert_eq!(coordinator.pending(), 2, "delivery is not completion");
    }

    #[test]
    fn every_participant_must_complete() {
        let coordinator = DomainForceFlush::new();
        let counters = counters();
        let mut first = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
        let mut second = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
        coordinator.request();

        assert!(
            first
                .pending_completion()
                .expect("participant must remain open")
                .expect("first completion must be ready")
                .complete()
        );
        assert_eq!(coordinator.pending(), 1);
        assert_eq!(counters.force_flushes.load(Ordering::Acquire), 1);
        assert!(
            second
                .pending_completion()
                .expect("participant must remain open")
                .expect("second completion must be ready")
                .complete()
        );
        assert_eq!(coordinator.pending(), 0);
        assert_eq!(counters.force_flushes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn stale_completion_cannot_clear_a_newer_generation() {
        let coordinator = DomainForceFlush::new();
        let counters = counters();
        let mut participant = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
        coordinator.request();
        let stale = participant
            .pending_completion()
            .expect("participant must remain open")
            .expect("first completion must be ready");
        let newer = coordinator.request();

        assert!(!stale.complete());
        assert_eq!(coordinator.pending(), 1);
        assert_eq!(counters.force_flushes.load(Ordering::Acquire), 1);
        let current = participant
            .pending_completion()
            .expect("participant must remain open")
            .expect("newer completion must be ready");
        assert_eq!(current.generation(), newer);
        assert!(current.complete());
    }

    #[test]
    fn participant_joining_an_active_generation_is_counted() {
        let coordinator = DomainForceFlush::new();
        let counters = counters();
        let mut first = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
        coordinator.request();
        let mut participant = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));

        assert_eq!(coordinator.pending(), 2);
        assert_eq!(counters.force_flushes.load(Ordering::Acquire), 2);
        assert!(
            first
                .pending_completion()
                .expect("participant must remain open")
                .expect("first completion must be delivered")
                .complete()
        );
        assert!(
            participant
                .pending_completion()
                .expect("participant must remain open")
                .expect("active completion must be delivered")
                .complete()
        );
    }

    #[test]
    fn dropping_completion_does_not_claim_success() {
        let coordinator = DomainForceFlush::new();
        let counters = counters();
        let mut participant = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
        coordinator.request();
        drop(
            participant
                .pending_completion()
                .expect("participant must remain open")
                .expect("completion must be ready"),
        );

        assert_eq!(coordinator.pending(), 1);
        assert_eq!(counters.force_flushes.load(Ordering::Acquire), 1);
        assert!(
            participant
                .pending_completion()
                .expect("participant must remain open")
                .expect("dropped completion must be delivered again")
                .complete()
        );
        assert_eq!(coordinator.pending(), 0);
        coordinator.request();
        drop(participant);
        assert_eq!(coordinator.pending(), 0);
        assert_eq!(counters.force_flushes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn idle_request_does_not_supersede_in_flight_work() {
        let coordinator = DomainForceFlush::new();
        let mut participant = DomainForceFlush::subscribe(&coordinator, None);
        let first = coordinator.request_if_idle();
        let same = coordinator.request_if_idle();
        assert_eq!(same, first);

        assert!(
            participant
                .pending_completion()
                .expect("participant must remain open")
                .expect("completion must be ready")
                .complete()
        );
        let second = coordinator.request_if_idle();
        assert_ne!(second, first);
    }

    #[test]
    fn request_without_participants_completes_immediately() {
        let coordinator = DomainForceFlush::new();

        let first = coordinator.request_if_idle();
        let second = coordinator.request_if_idle();

        assert_ne!(second, first);
        assert_eq!(coordinator.pending(), 0);
    }

    #[test]
    fn participant_has_no_completion_until_requested_and_cannot_double_claim() {
        let coordinator = DomainForceFlush::new();
        let mut participant = DomainForceFlush::subscribe(&coordinator, None);
        assert!(
            participant
                .pending_completion()
                .expect("participant must remain open")
                .is_none()
        );

        coordinator.request();
        let completion = participant
            .pending_completion()
            .expect("participant must remain open")
            .expect("completion must be ready");
        assert!(
            participant
                .pending_completion()
                .expect("participant must remain open")
                .is_none(),
            "one generation cannot be claimed twice"
        );
        assert!(completion.complete());
    }

    #[test]
    fn generation_rollover_skips_the_reserved_zero_generation() {
        let coordinator = DomainForceFlush::new();
        coordinator.state.lock().generation = u64::MAX;

        assert_eq!(coordinator.request(), 1);
    }

    #[test]
    fn unknown_and_unclaimed_completions_cannot_succeed() {
        let coordinator = DomainForceFlush::new();
        let participant = DomainForceFlush::subscribe(&coordinator, None);
        let generation = coordinator.request();

        assert!(DomainForceFlush::completion(&coordinator, u64::MAX).is_err());
        assert!(!coordinator.complete(participant.participant, generation));
    }

    #[test]
    fn completion_cannot_succeed_after_participant_unregisters() {
        let coordinator = DomainForceFlush::new();
        let mut participant = DomainForceFlush::subscribe(&coordinator, None);
        coordinator.request();
        let completion = participant
            .pending_completion()
            .expect("participant must remain open")
            .expect("completion must be ready");

        drop(participant);

        assert!(!completion.complete());
    }

    #[tokio::test]
    async fn changed_waits_for_the_next_generation() {
        let coordinator = DomainForceFlush::new();
        let mut participant = DomainForceFlush::subscribe(&coordinator, None);
        let changed = participant.changed();
        tokio::pin!(changed);
        assert!(
            tokio::time::timeout(tokio::time::Duration::from_millis(10), &mut changed)
                .await
                .is_err()
        );

        let generation = coordinator.request();
        let completion = tokio::time::timeout(tokio::time::Duration::from_secs(1), changed)
            .await
            .expect("force generation must wake the participant")
            .expect("participant must remain open");
        assert_eq!(completion.generation(), generation);
        assert!(completion.complete());
    }

    #[tokio::test]
    async fn changed_returns_an_already_pending_generation_immediately() {
        let coordinator = DomainForceFlush::new();
        let mut participant = DomainForceFlush::subscribe(&coordinator, None);
        let generation = coordinator.request();

        let completion = participant
            .changed()
            .await
            .expect("pending generation must be returned immediately");
        assert_eq!(completion.generation(), generation);
        assert!(completion.complete());
    }

    #[tokio::test]
    async fn closed_coordinator_rejects_requests_and_new_participants() {
        let coordinator = DomainForceFlush::new();
        coordinator.close();

        assert_eq!(coordinator.request(), 0);
        let mut participant = DomainForceFlush::subscribe(&coordinator, None);
        assert!(participant.changed().await.is_err());
    }

    #[tokio::test]
    async fn close_clears_obligations_and_wakes_participants() {
        let coordinator = DomainForceFlush::new();
        let counters = counters();
        let mut participant = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
        coordinator.request();
        coordinator.close();

        assert_eq!(coordinator.pending(), 0);
        assert_eq!(counters.force_flushes.load(Ordering::Acquire), 0);
        assert!(participant.changed().await.is_err());
    }
}
