//! The subscriptions a client wants across exchange replacements.
//!
//! - **Owns.** The acknowledged creation contracts, cancellation fences, lifecycle, gaps and failed
//!   restorations of client subscriptions, and whether an open session may hold each one.
//! - **Depends on.** Typed subscription Models, wire identities and the client's event contract.
//! - **Must not know.** How a request is transported or how an exchange routes its frames.

use std::{collections::VecDeque, time::Duration};

use ahash::HashMap;
use nervix_client_wire::{SubscribeRequest, SubscriptionHandle, SubscriptionType};
use nervix_models::{CreateSubscription, DomainName, SubscriptionName};
use nervix_recovery::Discarded as _;
use parking_lot::Mutex;
use tokio::sync::watch;
use triomphe::Arc;

use crate::events::SubscriptionEvent;

/// The observable state of a subscription the client owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionLifecycle {
    Creating,
    Active(SubscriptionHandle),
    Interrupted(SubscriptionHandle),
    Restoring(SubscriptionHandle),
    DeliveryFailed(SubscriptionHandle),
    Closing,
    DeletionFailed,
}

/// A gap in delivery after the exchange holding an acknowledged subscription ended. Reopening
/// starts at the new session's live edge; no durable resume is implied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionInterruption {
    pub subscription: SubscriptionHandle,
}

/// An attempt to open an interrupted subscription again that the current session refused or did
/// not answer. The subscription stays interrupted, and the client tries again after `retry_after`
/// for as long as the subscription is desired and the session stays open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionRestorationFailure {
    /// The generation the restoration would replace, which ended with its session.
    pub subscription: SubscriptionHandle,
    /// The server's refusal, or why the request got no answer.
    pub message: String,
    /// How long the client waits before its next attempt.
    pub retry_after: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct SubscriptionContract {
    pub(crate) domain: DomainName,
    pub(crate) create: CreateSubscription,
    pub(crate) subscription_type: SubscriptionType,
}

impl SubscriptionContract {
    pub(crate) fn request(&self) -> SubscribeRequest {
        let statement = nervix_nspl::subscribe::create_subscription_query(
            self.create.name.as_str(),
            self.create.relay.as_str(),
            self.create.delivery_behavior,
            self.create.batch_sample_rate.as_deref(),
            self.create.where_clause.as_ref(),
        );
        SubscribeRequest {
            domain: self.domain.clone(),
            statement,
            subscription_type: self.subscription_type,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RestoreAttempt {
    pub(crate) contract: SubscriptionContract,
    pub(crate) ticket: Arc<()>,
    pub(crate) generation: Arc<()>,
}

struct DesiredSubscription {
    contract: SubscriptionContract,
    lifecycle: SubscriptionLifecycle,
    /// The exchange the latest creation or restoration of the entry was sent on.
    generation: Arc<()>,
    /// Fences the result of that attempt from a later use of the same name.
    ticket: Arc<()>,
    attempt_in_flight: bool,
    /// Whether the server ever acknowledged the contract, which makes the entry restorable.
    acknowledged: bool,
    /// Whether the exchange of `generation` ended, and every subscription its session held.
    generation_ended: bool,
}

impl DesiredSubscription {
    fn restore(&mut self, generation: Arc<()>) -> Option<RestoreAttempt> {
        let SubscriptionLifecycle::Interrupted(previous) = &self.lifecycle else {
            return None;
        };
        if !self.acknowledged {
            return None;
        }
        let previous = previous.clone();
        let ticket = Arc::new(());
        self.ticket = ticket.clone();
        self.generation = generation.clone();
        self.generation_ended = false;
        self.attempt_in_flight = true;
        self.lifecycle = SubscriptionLifecycle::Restoring(previous);
        Some(RestoreAttempt {
            contract: self.contract.clone(),
            ticket,
            generation,
        })
    }

    /// Whether a session that is still open may hold the subscription: one holds a generation
    /// it acknowledged, or an attempt that may still open one awaits its reply. The session of an
    /// ended exchange holds nothing, and neither does one that refused to open the subscription
    /// again.
    fn may_be_held(&self) -> bool {
        if self.generation_ended {
            return false;
        }
        match self.lifecycle {
            SubscriptionLifecycle::Creating
            | SubscriptionLifecycle::Active(_)
            | SubscriptionLifecycle::Restoring(_)
            | SubscriptionLifecycle::DeliveryFailed(_)
            | SubscriptionLifecycle::Closing
            | SubscriptionLifecycle::DeletionFailed => true,
            SubscriptionLifecycle::Interrupted(_) => false,
        }
    }
}

/// What the registry reports about a subscription without the server sending it.
enum RegistryEvent {
    Interrupted(SubscriptionInterruption),
    RestorationFailed(SubscriptionRestorationFailure),
}

impl RegistryEvent {
    fn name(&self) -> &SubscriptionName {
        match self {
            Self::Interrupted(interrupted) => &interrupted.subscription.name,
            Self::RestorationFailed(failure) => &failure.subscription.name,
        }
    }
}

impl From<RegistryEvent> for SubscriptionEvent {
    fn from(event: RegistryEvent) -> Self {
        match event {
            RegistryEvent::Interrupted(interrupted) => Self::Interrupted(interrupted),
            RegistryEvent::RestorationFailed(failure) => Self::RestorationFailed(failure),
        }
    }
}

#[derive(Default)]
struct State {
    entries: HashMap<SubscriptionName, DesiredSubscription>,
    /// The deletions awaiting their outcome. A name stays fenced while its deletion is in flight,
    /// because a deletion's outcome resolves the name, whatever entry holds it by then.
    deletions: HashMap<SubscriptionName, Deletion>,
    /// Unread events in the order they happened. Each subscription has at most one unread
    /// interruption and one unread restoration failure, so they are bounded by the entries.
    events: VecDeque<RegistryEvent>,
}

impl State {
    /// Reports a gap, which supersedes every unread event of the same subscription.
    fn interrupted(&mut self, interruption: SubscriptionInterruption) {
        let name = interruption.subscription.name.clone();
        self.events.retain(|pending| pending.name() != &name);
        self.events
            .push_back(RegistryEvent::Interrupted(interruption));
    }

    /// Reports a failed restoration, which replaces an unread earlier failure of the same
    /// subscription and follows its unread interruption.
    fn restoration_failed(&mut self, failure: SubscriptionRestorationFailure) {
        let name = failure.subscription.name.clone();
        self.events.retain(|pending| {
            let RegistryEvent::RestorationFailed(earlier) = pending else {
                return true;
            };
            earlier.subscription.name != name
        });
        self.events
            .push_back(RegistryEvent::RestorationFailed(failure));
    }

    /// Drops the unread events of a subscription its caller no longer wants.
    fn forget_events(&mut self, name: &SubscriptionName) {
        self.events.retain(|pending| pending.name() != name);
    }
}

struct Deletion {
    ticket: Arc<()>,
    generation: Arc<()>,
}

pub(crate) struct DeleteAttempt {
    pub(crate) name: SubscriptionName,
    pub(crate) ticket: Arc<()>,
    /// Whether the client held the subscription when its deletion began. A name it never held
    /// can only be answered by the server.
    pub(crate) tracked: bool,
}

/// What deleting a subscription starts with.
pub(crate) enum Cancellation {
    /// A deletion of the name is already in flight.
    InFlight,
    /// No open session holds the subscription, so the client stopped wanting it without asking
    /// the server.
    Closed,
    /// A session may hold the subscription, or the client never held the name; the attempt asks
    /// the server to delete it.
    Delete(DeleteAttempt),
}

/// Whether a deletion still has to ask the server, once the attempt it waited for resolved.
pub(crate) enum DeletionTarget {
    /// The session the deletion was issued on ended, and the subscription with it.
    SessionEnded,
    /// The creation or restoration the deletion waited for did not open the subscription.
    NotOpened,
    /// The server holds the subscription, or holds a name the client never held.
    Server,
}

/// How the server resolved a deletion request.
pub(crate) enum DeletionResolution {
    /// The server deleted the subscription.
    Deleted,
    /// The server refused, or the request failed while its session stayed open.
    Refused,
    /// The session the request was sent on ended before it answered, and every subscription it
    /// held ended with it.
    SessionEnded,
}

struct Inner {
    state: Mutex<State>,
    changed: watch::Sender<()>,
}

/// Shared by the client and its exchange readers. A ticket fences a late create result from a
/// later use of the same name, while an exchange generation fences old-session observations.
#[derive(Clone)]
pub(crate) struct DesiredSubscriptions {
    inner: Arc<Inner>,
}

impl DesiredSubscriptions {
    pub(crate) fn new() -> Self {
        let (changed, _) = watch::channel(());
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                changed,
            }),
        }
    }

    pub(crate) fn watch(&self) -> watch::Receiver<()> {
        self.inner.changed.subscribe()
    }

    pub(crate) fn lifecycle(&self, name: &SubscriptionName) -> Option<SubscriptionLifecycle> {
        self.inner
            .state
            .lock()
            .entries
            .get(name)
            .map(|entry| entry.lifecycle.clone())
    }

    pub(crate) fn has_acknowledged_desired(&self) -> bool {
        self.inner.state.lock().entries.values().any(|entry| {
            entry.acknowledged
                && !matches!(
                    entry.lifecycle,
                    SubscriptionLifecycle::Closing
                        | SubscriptionLifecycle::DeletionFailed
                        | SubscriptionLifecycle::DeliveryFailed(_)
                )
        })
    }

    pub(crate) fn begin(
        &self,
        contract: SubscriptionContract,
        generation: Arc<()>,
    ) -> Option<RestoreAttempt> {
        let mut state = self.inner.state.lock();
        if state.entries.contains_key(&contract.create.name)
            || state.deletions.contains_key(&contract.create.name)
        {
            return None;
        }
        let ticket = Arc::new(());
        state.entries.insert(
            contract.create.name.clone(),
            DesiredSubscription {
                contract: contract.clone(),
                lifecycle: SubscriptionLifecycle::Creating,
                generation: generation.clone(),
                ticket: ticket.clone(),
                attempt_in_flight: true,
                acknowledged: false,
                generation_ended: false,
            },
        );
        drop(state);
        self.inner.changed.send_replace(());
        Some(RestoreAttempt {
            contract,
            ticket,
            generation,
        })
    }

    /// Starts deleting `name` on the exchange of `generation`.
    ///
    /// A subscription no open session holds is deleted here: its session ended, or the session
    /// now open refused to open it again, so there is nothing for a server to delete.
    pub(crate) fn cancel(&self, name: &SubscriptionName, generation: Arc<()>) -> Cancellation {
        let mut state = self.inner.state.lock();
        if state.deletions.contains_key(name) {
            return Cancellation::InFlight;
        }
        state.forget_events(name);
        let tracked = state.entries.contains_key(name);
        let held = match state.entries.get_mut(name) {
            Some(entry) if entry.may_be_held() => {
                entry.lifecycle = SubscriptionLifecycle::Closing;
                true
            }
            Some(_) | None => false,
        };
        if tracked && !held {
            state.entries.remove(name);
            drop(state);
            self.inner.changed.send_replace(());
            return Cancellation::Closed;
        }
        let ticket = Arc::new(());
        state.deletions.insert(
            name.clone(),
            Deletion {
                ticket: ticket.clone(),
                generation,
            },
        );
        drop(state);
        self.inner.changed.send_replace(());
        Cancellation::Delete(DeleteAttempt {
            name: name.clone(),
            ticket,
            tracked,
        })
    }

    /// The exchange reader applies a successful reply before it can route following rows. The
    /// caller's waiter may run later, so this is the acknowledgement boundary for delivery.
    pub(crate) fn acknowledge(&self, handle: &SubscriptionHandle, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get_mut(&handle.name) else {
            return;
        };
        if !Arc::ptr_eq(&entry.generation, generation) || entry.generation_ended {
            return;
        }
        if let SubscriptionLifecycle::Creating | SubscriptionLifecycle::Restoring(_) =
            entry.lifecycle
        {
            entry.acknowledged = true;
            entry.lifecycle = SubscriptionLifecycle::Active(handle.clone());
        }
        drop(state);
        self.inner.changed.send_replace(());
    }

    pub(crate) fn overflow(&self, handle: &SubscriptionHandle, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get_mut(&handle.name) else {
            return;
        };
        if !Arc::ptr_eq(&entry.generation, generation) || entry.generation_ended {
            return;
        }
        if let SubscriptionLifecycle::Active(active) = &entry.lifecycle
            && active == handle
        {
            entry.lifecycle = SubscriptionLifecycle::DeliveryFailed(handle.clone());
        }
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Resolves a creation attempt with the generation the server opened, if it opened one.
    ///
    /// A success that arrives after its caller began deleting the name keeps the entry closing,
    /// and the deletion then asks the server to delete that generation.
    pub(crate) fn created(&self, attempt: &RestoreAttempt, opened: Option<&SubscriptionHandle>) {
        let name = &attempt.contract.create.name;
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get_mut(name) else {
            return;
        };
        if !Arc::ptr_eq(&entry.ticket, &attempt.ticket)
            || !Arc::ptr_eq(&entry.generation, &attempt.generation)
        {
            return;
        }
        entry.attempt_in_flight = false;
        match opened {
            Some(handle) if entry.generation_ended => {
                if let SubscriptionLifecycle::Closing = entry.lifecycle {
                    state.entries.remove(name);
                } else if !entry.acknowledged {
                    entry.acknowledged = true;
                    entry.lifecycle = SubscriptionLifecycle::Interrupted(handle.clone());
                    state.interrupted(SubscriptionInterruption {
                        subscription: handle.clone(),
                    });
                }
            }
            Some(_) if let SubscriptionLifecycle::Closing = entry.lifecycle => {
                entry.acknowledged = true;
            }
            Some(_) if let SubscriptionLifecycle::DeliveryFailed(_) = entry.lifecycle => {}
            Some(handle) => {
                entry.acknowledged = true;
                entry.lifecycle = SubscriptionLifecycle::Active(handle.clone());
            }
            None if entry.generation_ended
                && let SubscriptionLifecycle::Closing = entry.lifecycle =>
            {
                state.entries.remove(name);
            }
            None if entry.generation_ended && entry.acknowledged => {}
            None if let SubscriptionLifecycle::Restoring(previous) = &entry.lifecycle => {
                entry.lifecycle = SubscriptionLifecycle::Interrupted(previous.clone());
            }
            // Nothing was opened, so nothing is left for the deletion to ask the server about.
            None => {
                state.entries.remove(name);
            }
        }
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Reports that a restoration attempt did not open the subscription, which the client tries
    /// again after `retry_after`. `false` once the entry no longer waits for that attempt's
    /// exchange to open it: the caller deleted it, or the exchange ended.
    pub(crate) fn restoration_failed(
        &self,
        attempt: &RestoreAttempt,
        message: String,
        retry_after: Duration,
    ) -> bool {
        let name = &attempt.contract.create.name;
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get(name) else {
            return false;
        };
        if !Arc::ptr_eq(&entry.ticket, &attempt.ticket)
            || !Arc::ptr_eq(&entry.generation, &attempt.generation)
            || entry.generation_ended
        {
            return false;
        }
        let SubscriptionLifecycle::Interrupted(previous) = &entry.lifecycle else {
            return false;
        };
        let failure = SubscriptionRestorationFailure {
            subscription: previous.clone(),
            message,
            retry_after,
        };
        state.restoration_failed(failure);
        drop(state);
        self.inner.changed.send_replace(());
        true
    }

    pub(crate) fn ended(&self, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let mut closed = Vec::new();
        let mut interruptions = Vec::new();
        for (name, entry) in &mut state.entries {
            if !Arc::ptr_eq(&entry.generation, generation) || entry.generation_ended {
                continue;
            }
            entry.generation_ended = true;
            match &entry.lifecycle {
                SubscriptionLifecycle::Active(handle) => {
                    let handle = handle.clone();
                    entry.lifecycle = SubscriptionLifecycle::Interrupted(handle.clone());
                    interruptions.push(SubscriptionInterruption {
                        subscription: handle,
                    });
                }
                SubscriptionLifecycle::Restoring(previous) => {
                    entry.lifecycle = SubscriptionLifecycle::Interrupted(previous.clone());
                }
                SubscriptionLifecycle::Closing | SubscriptionLifecycle::DeletionFailed => {
                    closed.push(name.clone());
                }
                SubscriptionLifecycle::Creating
                | SubscriptionLifecycle::Interrupted(_)
                | SubscriptionLifecycle::DeliveryFailed(_) => {}
            }
        }
        for name in closed {
            state.entries.remove(&name);
            state.deletions.remove(&name);
        }
        state
            .deletions
            .retain(|_, deletion| !Arc::ptr_eq(&deletion.generation, generation));
        for interruption in interruptions {
            state.interrupted(interruption);
        }
        drop(state);
        self.inner.changed.send_replace(());
    }

    pub(crate) fn restore(&self, generation: Arc<()>) -> Vec<RestoreAttempt> {
        let mut state = self.inner.state.lock();
        let mut attempts = Vec::new();
        for entry in state.entries.values_mut() {
            if let Some(attempt) = entry.restore(generation.clone()) {
                attempts.push(attempt);
            }
        }
        drop(state);
        self.inner.changed.send_replace(());
        attempts
    }

    pub(crate) fn retry(
        &self,
        name: &SubscriptionName,
        generation: &Arc<()>,
    ) -> Option<RestoreAttempt> {
        let mut state = self.inner.state.lock();
        let entry = state.entries.get_mut(name)?;
        if !Arc::ptr_eq(&entry.generation, generation) || entry.generation_ended {
            return None;
        }
        let attempt = entry.restore(generation.clone());
        drop(state);
        if attempt.is_some() {
            self.inner.changed.send_replace(());
        }
        attempt
    }

    pub(crate) fn can_deliver(&self, handle: &SubscriptionHandle, generation: &Arc<()>) -> bool {
        let state = self.inner.state.lock();
        let Some(entry) = state.entries.get(&handle.name) else {
            return false;
        };
        if !Arc::ptr_eq(&entry.generation, generation) || entry.generation_ended {
            return false;
        }
        match &entry.lifecycle {
            SubscriptionLifecycle::Active(active) => active == handle,
            SubscriptionLifecycle::DeliveryFailed(active) => active == handle,
            SubscriptionLifecycle::Creating | SubscriptionLifecycle::Restoring(_) => false,
            SubscriptionLifecycle::Interrupted(_)
            | SubscriptionLifecycle::Closing
            | SubscriptionLifecycle::DeletionFailed => false,
        }
    }

    /// Takes the earliest unread interruption or restoration failure.
    pub(crate) fn take_event(&self) -> Option<SubscriptionEvent> {
        let event = self.inner.state.lock().events.pop_front()?;
        Some(SubscriptionEvent::from(event))
    }

    pub(crate) fn deletion_waits(&self, attempt: &DeleteAttempt) -> bool {
        let state = self.inner.state.lock();
        if !state
            .deletions
            .get(&attempt.name)
            .is_some_and(|deletion| Arc::ptr_eq(&deletion.ticket, &attempt.ticket))
        {
            return false;
        }
        let Some(entry) = state.entries.get(&attempt.name) else {
            return false;
        };
        entry.attempt_in_flight
    }

    /// Decides whether the deletion still has to ask the server, once no attempt it waits for is
    /// in flight. A deletion that no longer has to ask it is complete, and releases the name.
    pub(crate) fn deletion_target(&self, attempt: &DeleteAttempt) -> DeletionTarget {
        let mut state = self.inner.state.lock();
        let active = state
            .deletions
            .get(&attempt.name)
            .is_some_and(|deletion| Arc::ptr_eq(&deletion.ticket, &attempt.ticket));
        if !active {
            return DeletionTarget::SessionEnded;
        }
        if !attempt.tracked || state.entries.contains_key(&attempt.name) {
            return DeletionTarget::Server;
        }
        state
            .deletions
            .remove(&attempt.name)
            .discarded("the deletion removed is the one found active above");
        drop(state);
        self.inner.changed.send_replace(());
        DeletionTarget::NotOpened
    }

    pub(crate) fn deleted(&self, attempt: &DeleteAttempt, resolution: DeletionResolution) {
        let mut state = self.inner.state.lock();
        let active = state
            .deletions
            .get(&attempt.name)
            .is_some_and(|deletion| Arc::ptr_eq(&deletion.ticket, &attempt.ticket));
        if !active {
            return;
        }
        state
            .deletions
            .remove(&attempt.name)
            .discarded("the deletion removed is the one found active above");
        match resolution {
            DeletionResolution::Deleted | DeletionResolution::SessionEnded => {
                state.entries.remove(&attempt.name);
            }
            DeletionResolution::Refused => {
                if let Some(entry) = state.entries.get_mut(&attempt.name) {
                    entry.lifecycle = SubscriptionLifecycle::DeletionFailed;
                }
            }
        }
        drop(state);
        self.inner.changed.send_replace(());
    }
}

#[cfg(test)]
mod tests;
