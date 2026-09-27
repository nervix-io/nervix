//! The subscriptions a client wants across exchange replacements.
//!
//! - **Owns.** The acknowledged creation contracts, cancellation fences, lifecycle and gaps of
//!   client subscriptions.
//! - **Depends on.** Typed subscription Models, wire identities and the client's event contract.
//! - **Must not know.** How a request is transported or how an exchange routes its frames.

use std::collections::VecDeque;

use ahash::HashMap;
use nervix_client_wire::{SubscribeRequest, SubscriptionHandle, SubscriptionType};
use nervix_models::{CreateSubscription, DomainName, SubscriptionName};
use parking_lot::Mutex;
use tokio::sync::watch;
use triomphe::Arc;

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
    generation: Arc<()>,
    ticket: Arc<()>,
    attempt_in_flight: bool,
    acknowledged: bool,
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
}

#[derive(Default)]
struct State {
    entries: HashMap<SubscriptionName, DesiredSubscription>,
    deletions: HashMap<SubscriptionName, Deletion>,
    interruptions: VecDeque<SubscriptionInterruption>,
}

struct Deletion {
    ticket: Arc<()>,
    generation: Arc<()>,
    in_flight: bool,
}

pub(crate) struct DeleteAttempt {
    pub(crate) name: SubscriptionName,
    pub(crate) ticket: Arc<()>,
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

    pub(crate) fn cancel(
        &self,
        name: &SubscriptionName,
        generation: Arc<()>,
    ) -> Option<DeleteAttempt> {
        let mut state = self.inner.state.lock();
        if state
            .deletions
            .get(name)
            .is_some_and(|deletion| deletion.in_flight)
        {
            return None;
        }
        let ticket = Arc::new(());
        state.deletions.insert(
            name.clone(),
            Deletion {
                ticket: ticket.clone(),
                generation,
                in_flight: true,
            },
        );
        if let Some(entry) = state.entries.get_mut(name) {
            entry.lifecycle = SubscriptionLifecycle::Closing;
        }
        state
            .interruptions
            .retain(|interruption| &interruption.subscription.name != name);
        drop(state);
        self.inner.changed.send_replace(());
        Some(DeleteAttempt {
            name: name.clone(),
            ticket,
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

    /// Resolve a creation attempt. `true` means its success arrived after cancellation and the
    /// server subscription must be deleted before this name can be reused.
    pub(crate) fn created(
        &self,
        attempt: &RestoreAttempt,
        opened: Option<&SubscriptionHandle>,
    ) -> bool {
        let name = &attempt.contract.create.name;
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get_mut(name) else {
            return false;
        };
        if !Arc::ptr_eq(&entry.ticket, &attempt.ticket)
            || !Arc::ptr_eq(&entry.generation, &attempt.generation)
        {
            return false;
        }
        entry.attempt_in_flight = false;
        let cleanup = match opened {
            Some(handle) if entry.generation_ended => {
                if let SubscriptionLifecycle::Closing = entry.lifecycle {
                    state.entries.remove(name);
                } else if !entry.acknowledged {
                    entry.acknowledged = true;
                    entry.lifecycle = SubscriptionLifecycle::Interrupted(handle.clone());
                    state.interruptions.push_back(SubscriptionInterruption {
                        subscription: handle.clone(),
                    });
                }
                false
            }
            Some(_) if let SubscriptionLifecycle::Closing = entry.lifecycle => {
                entry.acknowledged = true;
                true
            }
            Some(_) if let SubscriptionLifecycle::DeliveryFailed(_) = entry.lifecycle => false,
            Some(handle) => {
                entry.acknowledged = true;
                entry.lifecycle = SubscriptionLifecycle::Active(handle.clone());
                false
            }
            None if entry.generation_ended
                && let SubscriptionLifecycle::Closing = entry.lifecycle =>
            {
                state.entries.remove(name);
                false
            }
            None if entry.generation_ended && entry.acknowledged => false,
            None if let SubscriptionLifecycle::Restoring(previous) = &entry.lifecycle => {
                entry.lifecycle = SubscriptionLifecycle::Interrupted(previous.clone());
                false
            }
            None if let SubscriptionLifecycle::Closing = entry.lifecycle => {
                if !entry.acknowledged {
                    state.entries.remove(name);
                }
                false
            }
            None => {
                state.entries.remove(name);
                false
            }
        };
        drop(state);
        self.inner.changed.send_replace(());
        cleanup
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
            state
                .interruptions
                .retain(|pending| pending.subscription.name != interruption.subscription.name);
            state.interruptions.push_back(interruption);
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

    pub(crate) fn take_interruption(&self) -> Option<SubscriptionInterruption> {
        self.inner.state.lock().interruptions.pop_front()
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

    pub(crate) fn deletion_active(&self, attempt: &DeleteAttempt) -> bool {
        self.inner
            .state
            .lock()
            .deletions
            .get(&attempt.name)
            .is_some_and(|deletion| Arc::ptr_eq(&deletion.ticket, &attempt.ticket))
    }

    pub(crate) fn deleted(&self, attempt: &DeleteAttempt, succeeded: bool) {
        let mut state = self.inner.state.lock();
        let Some(deletion) = state.deletions.get_mut(&attempt.name) else {
            return;
        };
        if !Arc::ptr_eq(&deletion.ticket, &attempt.ticket) {
            return;
        }
        if succeeded {
            state.entries.remove(&attempt.name);
            state.deletions.remove(&attempt.name);
        } else {
            deletion.in_flight = false;
            if let Some(entry) = state.entries.get_mut(&attempt.name) {
                entry.lifecycle = SubscriptionLifecycle::DeletionFailed;
            }
        }
        drop(state);
        self.inner.changed.send_replace(());
    }
}

#[cfg(test)]
mod tests;
