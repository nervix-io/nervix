//! Generation-aware domain force-flush coordination.
//!
//! Publishing a watch value alone cannot prove that a node observed or completed a flush. This
//! coordinator records one obligation per live participant before publishing the generation. A
//! participant receives an explicit completion token and clears the obligation only after its
//! node-specific flush attempt finishes. Retained buffers remain visible through quiesce counters
//! and drive another generation after retry; the control token does not own retry policy. Dropping
//! the participant clears its outstanding obligation, while dropping an unhandled completion
//! makes the same generation deliverable again.

use ahash::{HashMap, HashMapExt};
use parking_lot::Mutex;
use tokio::sync::watch;
use triomphe::Arc;

use super::*;

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
        state.next_participant = state
            .next_participant
            .checked_add(1)
            .assured("a domain cannot register 2^64 force-flush participants");
        let participant = state.next_participant;
        let pending_generation = state.active_generation;
        if pending_generation.is_some()
            && let Some(counters) = &counters
        {
            counters.begin_force_flush_obligation();
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
        state.generation = state
            .generation
            .checked_add(1)
            .assured("a domain cannot run 2^64 force flushes");
        let generation = state.generation;
        state.active_generation = Some(generation);
        for participant in state.participants.values_mut() {
            if participant.pending_generation.is_none()
                && let Some(counters) = &participant.counters
            {
                counters.begin_force_flush_obligation();
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
            let held = counters.complete_force_flush_obligation();
            debug_assert!(held > 0, "force-flush obligation count underflow");
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

/// The two independently owned counters every ingestor-created ACK root updates.
///
/// An ingestor resolves this pair once and holds it, so cloning it is two refcount bumps and
/// never a lookup in the shared in-flight maps.
#[derive(Clone)]
pub(in crate::runtime) struct IngestorAckRootTrackers {
    domain: Arc<AckRootTracker>,
    ingestor: Arc<AckRootTracker>,
}

impl IngestorAckRootTrackers {
    pub(in crate::runtime) fn tracked_root(&self) -> (AckSet, AckCompletion) {
        AckSet::tracked_roots(vec![self.domain.clone(), self.ingestor.clone()])
    }
}

impl Runtime {
    pub(in crate::runtime) fn tracked_ack_root(
        &self,
        domain: &DomainName,
    ) -> (AckSet, AckCompletion) {
        let tracker = self
            .inner
            .in_flight_by_domain
            .entry(domain.clone())
            .or_insert_with(|| Arc::new(AckRootTracker::default()))
            .clone();
        AckSet::tracked_root(tracker)
    }

    pub(in crate::runtime) fn ingestor_ack_root_trackers(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> IngestorAckRootTrackers {
        let domain_tracker = match self.inner.in_flight_by_domain.get(domain) {
            Some(tracker) => tracker.value().clone(),
            None => self
                .inner
                .in_flight_by_domain
                .entry(domain.clone())
                .or_insert_with(|| Arc::new(AckRootTracker::default()))
                .clone(),
        };
        let ingestor_entity =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let ingestor_tracker = match self.inner.in_flight_by_ingestor.get(&ingestor_entity) {
            Some(tracker) => tracker.value().clone(),
            None => self
                .inner
                .in_flight_by_ingestor
                .entry(ingestor_entity)
                .or_insert_with(|| Arc::new(AckRootTracker::default()))
                .clone(),
        };
        IngestorAckRootTrackers {
            domain: domain_tracker,
            ingestor: ingestor_tracker,
        }
    }

    pub(in crate::runtime) fn domain_outstanding_work(&self, domain: &DomainName) -> usize {
        match self.inner.in_flight_by_domain.get(domain) {
            Some(tracker) => tracker.outstanding(),
            None => 0,
        }
    }

    pub(in crate::runtime) fn force_flush_participant(
        &self,
        domain: &DomainName,
        counters: Arc<NodeQuiesceCounters>,
    ) -> DomainForceFlushParticipant {
        let coordinator = self
            .inner
            .force_flush_by_domain
            .entry(domain.clone())
            .or_insert_with(DomainForceFlush::new)
            .clone();
        DomainForceFlush::subscribe(&coordinator, Some(counters))
    }

    pub(in crate::runtime) fn force_flush_domain(&self, domain: &DomainName) -> u64 {
        self.inner
            .force_flush_by_domain
            .entry(domain.clone())
            .or_insert_with(DomainForceFlush::new)
            .request()
    }

    pub(crate) fn force_flush_domain_if_idle(&self, domain: &DomainName) -> u64 {
        self.inner
            .force_flush_by_domain
            .entry(domain.clone())
            .or_insert_with(DomainForceFlush::new)
            .request_if_idle()
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
        assert_eq!(counters.force_flush_obligations(), 2);
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
    fn generations_start_at_one_and_never_repeat() {
        let coordinator = DomainForceFlush::new();

        assert_eq!(coordinator.request(), 1);
        assert_eq!(coordinator.request(), 2);
        assert_eq!(coordinator.request(), 3);
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
        assert_eq!(counters.force_flush_obligations(), 0);
        assert!(participant.changed().await.is_err());
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use std::{future::Future, task::Poll};

    use super::*;
    use crate::shuttle_test::{check_dfs, check_pct, check_random};

    const DFS_ITERATIONS: usize = 1_000;
    const PCT_DEPTH: usize = 3;
    const PCT_ITERATIONS: usize = 200;
    const PCT_PARTICIPANTS: usize = 4;
    const RANDOM_ITERATIONS: usize = 200;

    fn counters() -> Arc<NodeQuiesceCounters> {
        Arc::new(NodeQuiesceCounters::default())
    }

    async fn announce_after_first_pending<F>(
        future: F,
        pending: tokio::sync::oneshot::Sender<()>,
    ) -> F::Output
    where
        F: Future,
    {
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|context| match future.as_mut().poll(context) {
            Poll::Ready(_) => panic!("changed must wait before a generation is published"),
            Poll::Pending => Poll::Ready(()),
        })
        .await;
        pending
            .send(())
            .assured("the publisher waits for changed to become pending");
        future.await
    }

    fn every_obligation_resolves_invariant() {
        shuttle::future::block_on(async {
            let coordinator = DomainForceFlush::new();
            let counters = counters();
            let mut first = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
            let mut second = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
            let (second_claimed, second_is_claimed) = tokio::sync::oneshot::channel();
            let (release_second, second_is_released) = tokio::sync::oneshot::channel();

            let first_task = tokio::spawn(async move {
                let completion = first
                    .changed()
                    .await
                    .assured("the coordinator remains open until both participants finish");
                let generation = completion.generation();
                assert!(completion.complete());
                generation
            });
            let second_task = tokio::spawn(async move {
                let completion = second
                    .changed()
                    .await
                    .assured("the coordinator remains open until both participants finish");
                let generation = completion.generation();
                drop(completion);

                let redelivery = second
                    .changed()
                    .await
                    .assured("dropping the claim leaves its obligation live");
                assert_eq!(redelivery.generation(), generation);
                second_claimed
                    .send(())
                    .assured("the observer waits for the held second obligation");
                second_is_released
                    .await
                    .assured("the observer releases the second obligation before joining");
                assert!(redelivery.complete());
                generation
            });

            let generation = coordinator.request();
            let first_generation = first_task
                .await
                .assured("the first participant completes without panicking");
            assert_eq!(first_generation, generation);
            second_is_claimed
                .await
                .assured("the second participant reports its redelivered obligation");

            assert_eq!(coordinator.pending(), 1);
            assert_eq!(counters.force_flush_obligations(), 1);
            assert_eq!(coordinator.request_if_idle(), generation);

            release_second
                .send(())
                .assured("the second participant remains blocked on its release");
            let second_generation = second_task
                .await
                .assured("the second participant completes without panicking");
            assert_eq!(second_generation, generation);
            assert_eq!(coordinator.pending(), 0);
            assert_eq!(counters.force_flush_obligations(), 0);
            assert_ne!(coordinator.request_if_idle(), generation);
        });
    }

    #[test]
    fn shuttle_two_participant_generation_waits_for_every_obligation() {
        check_dfs(every_obligation_resolves_invariant, Some(DFS_ITERATIONS));
    }

    fn stale_completions_invariant() {
        shuttle::future::block_on(async {
            let coordinator = DomainForceFlush::new();
            let counters = counters();
            let mut first = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
            let mut second = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
            let (first_claimed, first_is_claimed) = tokio::sync::oneshot::channel();
            let (second_claimed, second_is_claimed) = tokio::sync::oneshot::channel();
            let (publish_to_first, first_publication) = tokio::sync::oneshot::channel();
            let (publish_to_second, second_publication) = tokio::sync::oneshot::channel();

            let first_task = tokio::spawn(async move {
                let stale = first
                    .changed()
                    .await
                    .assured("the first generation is published before the coordinator closes");
                let stale_generation = stale.generation();
                first_claimed
                    .send(stale_generation)
                    .assured("the observer waits for the first participant's claim");
                let current_generation = first_publication
                    .await
                    .assured("the observer publishes the newer generation before joining");
                assert!(!stale.complete());

                let current = first
                    .changed()
                    .await
                    .assured("the newer generation remains available after the stale completion");
                assert_eq!(current.generation(), current_generation);
                assert!(current.complete());
            });
            let second_task = tokio::spawn(async move {
                let stale = second
                    .changed()
                    .await
                    .assured("the first generation is published before the coordinator closes");
                let stale_generation = stale.generation();
                second_claimed
                    .send(stale_generation)
                    .assured("the observer waits for the second participant's claim");
                let current_generation = second_publication
                    .await
                    .assured("the observer publishes the newer generation before joining");
                assert!(!stale.complete());

                let current = second
                    .changed()
                    .await
                    .assured("the newer generation remains available after the stale completion");
                assert_eq!(current.generation(), current_generation);
                assert!(current.complete());
            });

            let stale_generation = coordinator.request();
            assert_eq!(
                first_is_claimed
                    .await
                    .assured("the first participant claims the published generation"),
                stale_generation
            );
            assert_eq!(
                second_is_claimed
                    .await
                    .assured("the second participant claims the published generation"),
                stale_generation
            );

            let current_generation = coordinator.request();
            assert_eq!(coordinator.pending(), 2);
            assert_eq!(counters.force_flush_obligations(), 2);
            publish_to_first
                .send(current_generation)
                .assured("the first participant waits for the newer publication");
            publish_to_second
                .send(current_generation)
                .assured("the second participant waits for the newer publication");

            first_task
                .await
                .assured("the first participant completes without panicking");
            second_task
                .await
                .assured("the second participant completes without panicking");
            assert_eq!(coordinator.pending(), 0);
            assert_eq!(counters.force_flush_obligations(), 0);
        });
    }

    #[test]
    fn shuttle_stale_completions_never_clear_a_newer_generation() {
        check_dfs(stale_completions_invariant, Some(DFS_ITERATIONS));
    }

    fn waiting_participant_invariant() {
        shuttle::future::block_on(async {
            let coordinator = DomainForceFlush::new();
            let counters = counters();
            let mut participant = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
            let (waiting_started, is_waiting) = tokio::sync::oneshot::channel();
            let waiting = tokio::spawn(async move {
                let completion =
                    announce_after_first_pending(participant.changed(), waiting_started)
                        .await
                        .assured("the publication wakes the live participant");
                let generation = completion.generation();
                assert!(completion.complete());
                generation
            });
            is_waiting
                .await
                .assured("changed reports after its first pending poll");
            let publisher = {
                let coordinator = coordinator.clone();
                tokio::spawn(async move { coordinator.request() })
            };

            let generation = publisher
                .await
                .assured("the publication task does not panic");
            let observed_generation = waiting
                .await
                .assured("the waiting participant does not deadlock or panic");
            assert_eq!(observed_generation, generation);
            assert_eq!(coordinator.pending(), 0);
            assert_eq!(counters.force_flush_obligations(), 0);
        });
    }

    #[test]
    fn shuttle_published_generation_wakes_a_waiting_participant() {
        check_random(waiting_participant_invariant, RANDOM_ITERATIONS);
    }

    fn participant_lifecycle_invariant() {
        shuttle::future::block_on(async {
            let coordinator = DomainForceFlush::new();
            let counters = counters();
            let mut anchor = DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
            let initial_generation = coordinator.request();
            let anchor_completion = anchor
                .pending_completion()
                .assured("the anchor remains open while participants subscribe")
                .assured("the active generation gives the anchor an obligation");
            let (subscribed, mut subscriptions) = tokio::sync::mpsc::channel(PCT_PARTICIPANTS);
            let mut releases = Vec::with_capacity(PCT_PARTICIPANTS);
            let mut participants = Vec::with_capacity(PCT_PARTICIPANTS);

            for index in 0..PCT_PARTICIPANTS {
                tokio::task::consume_budget().await;
                let coordinator = coordinator.clone();
                let counters = counters.clone();
                let subscribed = subscribed.clone();
                let (release, released) = tokio::sync::oneshot::channel();
                releases.push(release);
                participants.push(tokio::spawn(async move {
                    let mut participant = DomainForceFlush::subscribe(&coordinator, Some(counters));
                    subscribed
                        .send(())
                        .await
                        .assured("the observer receives every subscription");
                    released
                        .await
                        .assured("the observer releases every subscribed participant");

                    match index {
                        0 => {
                            if let Ok(completion) = participant.changed().await {
                                completion.complete();
                            }
                        }
                        1 => {
                            let Ok(completion) = participant.changed().await else {
                                return;
                            };
                            drop(completion);
                            match participant.pending_completion() {
                                Ok(Some(redelivery)) => {
                                    redelivery.complete();
                                }
                                Ok(None) => {
                                    panic!("a dropped claim must remain available while open")
                                }
                                Err(()) => {}
                            }
                        }
                        2 => drop(participant),
                        3 => {
                            if let Ok(completion) = participant.changed().await {
                                drop(participant);
                                assert!(!completion.complete());
                            }
                        }
                        _ => panic!("the model creates exactly four participant roles"),
                    }
                }));
            }
            drop(subscribed);

            for _ in 0..PCT_PARTICIPANTS {
                tokio::task::consume_budget().await;
                subscriptions
                    .recv()
                    .await
                    .assured("every participant reports after subscribing");
            }
            assert_eq!(coordinator.pending(), PCT_PARTICIPANTS + 1);
            assert_eq!(counters.force_flush_obligations(), PCT_PARTICIPANTS + 1);

            let request = {
                let coordinator = coordinator.clone();
                tokio::spawn(async move { coordinator.request() })
            };
            let idle_request = {
                let coordinator = coordinator.clone();
                tokio::spawn(async move { coordinator.request_if_idle() })
            };
            let anchor_task = tokio::spawn(async move {
                let completed = anchor_completion.complete();
                drop(anchor);
                completed
            });

            for release in releases {
                tokio::task::consume_budget().await;
                release
                    .send(())
                    .assured("the participant remains blocked until lifecycle operations start");
            }
            for participant in participants {
                tokio::task::consume_budget().await;
                participant
                    .await
                    .assured("the participant lifecycle task does not panic");
            }
            request
                .await
                .assured("the force-flush request task does not panic");
            idle_request
                .await
                .assured("the idle force-flush request task does not panic");
            anchor_task
                .await
                .assured("the anchor lifecycle task does not panic");

            assert_eq!(coordinator.pending(), 0);
            assert_eq!(counters.force_flush_obligations(), 0);

            let mut first_closing =
                DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
            let mut second_closing =
                DomainForceFlush::subscribe(&coordinator, Some(counters.clone()));
            let first_waiter = tokio::spawn(async move {
                if let Ok(completion) = first_closing.changed().await {
                    completion.complete();
                }
            });
            let second_waiter = tokio::spawn(async move {
                if let Ok(completion) = second_closing.changed().await {
                    drop(completion);
                }
            });
            let closing_request = {
                let coordinator = coordinator.clone();
                tokio::spawn(async move { coordinator.request() })
            };
            let closing_idle_request = {
                let coordinator = coordinator.clone();
                tokio::spawn(async move { coordinator.request_if_idle() })
            };
            let close = {
                let coordinator = coordinator.clone();
                tokio::spawn(async move { coordinator.close() })
            };

            first_waiter
                .await
                .assured("the first close waiter does not deadlock or panic");
            second_waiter
                .await
                .assured("the second close waiter does not deadlock or panic");
            closing_request
                .await
                .assured("the request racing with close does not panic");
            closing_idle_request
                .await
                .assured("the idle request racing with close does not panic");
            close
                .await
                .assured("the coordinator close task does not panic");

            assert!(coordinator.request() >= initial_generation);
            assert_eq!(coordinator.pending(), 0);
            assert_eq!(counters.force_flush_obligations(), 0);
        });
    }

    #[test]
    fn shuttle_participant_lifecycle_balances_obligations_through_close() {
        check_pct(participant_lifecycle_invariant, PCT_ITERATIONS, PCT_DEPTH);
    }
}
