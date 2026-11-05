//! The subscriptions a client wants across exchange replacements.
//!
//! - **Owns.** The acknowledged creation contracts and the schema each acknowledged generation
//!   announced, cancellation fences, lifecycle, gaps, ends, restorations the server refused for
//!   good and failed restorations of client subscriptions, and whether an open session may hold
//!   each one.
//! - **Depends on.** Typed subscription Models, wire identities and the client's event contract.
//! - **Must not know.** How a request is transported or how an exchange routes its frames.

use std::{collections::VecDeque, fmt, time::Duration};

use ahash::HashMap;
use nervix_client_wire::{
    RequestRejection, RowSchema, SubscribeRequest, SubscriptionEndReason, SubscriptionEnded,
    SubscriptionHandle, SubscriptionOpened, SubscriptionType,
};
use nervix_models::{CreateSubscription, DomainName, SubscriptionName};
use nervix_primitives::sync::{Arc, blocking::Mutex, watch};
use nervix_recovery::Discarded as _;

use crate::events::SubscriptionEvent;

/// The observable state of a subscription the client owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionLifecycle {
    Creating,
    Active(SubscriptionHandle),
    Interrupted(SubscriptionHandle),
    Restoring(SubscriptionHandle),
    DeliveryFailed(SubscriptionHandle),
    /// The server ended the generation, because its relay was redefined or removed. The client
    /// never opens it again: subscribing under its name opens a new generation, and deleting it
    /// needs no server.
    Ended(SubscriptionHandle),
    Closing,
    DeletionFailed,
}

/// A gap in delivery after the exchange holding an acknowledged subscription ended. Reopening
/// starts at the new session's live edge; no durable resume is implied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionInterruption {
    pub subscription: SubscriptionHandle,
}

/// Why an attempt to open an interrupted subscription again opened nothing, when a later attempt
/// may open it. A refusal that says the relay or its domain no longer exists, or that the relay
/// announces another schema, ends the subscription instead and is never one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionRestorationCause {
    /// The session holds a transaction, which refuses every session-local request until it ends.
    InTransaction,
    /// The server refused the subscription's statement.
    InvalidStatement,
    /// The session already holds a subscription of that name.
    NameInUse,
    /// The subscription's filter does not compile against its relay's definition.
    InvalidFilter,
    /// The serving node could not open the subscription now.
    Unavailable,
    /// The session rejected the request without serving it.
    Rejected(RequestRejection),
    /// The client could not send the request, or could not use its reply.
    Failed,
}

impl fmt::Display for SubscriptionRestorationCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InTransaction => formatter.write_str("the session holds a transaction"),
            Self::InvalidStatement => formatter.write_str("the server refused its statement"),
            Self::NameInUse => formatter.write_str("the session already holds its name"),
            Self::InvalidFilter => formatter.write_str("its filter does not compile"),
            Self::Unavailable => formatter.write_str("the serving node could not open it now"),
            Self::Rejected(_) => formatter.write_str("the session rejected the request"),
            Self::Failed => formatter.write_str("the client could not complete the attempt"),
        }
    }
}

/// An attempt to open an interrupted subscription again that the current session refused for now.
/// The subscription stays interrupted, and the client tries again after `retry_after` for as long
/// as the subscription is desired and the session stays open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionRestorationFailure {
    /// The generation the restoration would replace, which ended with its session.
    pub subscription: SubscriptionHandle,
    /// Why the attempt opened nothing.
    pub cause: SubscriptionRestorationCause,
    /// The server's message, or how the attempt failed in the client.
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
    /// The request that opens the subscription, expecting `expected_schema` when it restores one.
    fn request(&self, expected_schema: Option<RowSchema>) -> SubscribeRequest {
        let statement = nervix_plexus::subscribe::create_subscription_query(
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
            expected_schema,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RestoreAttempt {
    pub(crate) contract: SubscriptionContract,
    pub(crate) ticket: Arc<()>,
    pub(crate) generation: Arc<()>,
    /// The schema the generation a restoration replaces announced, which the server must still
    /// announce for the restoration to open. `None` for a first creation.
    pub(crate) expected_schema: Option<Arc<RowSchema>>,
}

impl RestoreAttempt {
    /// The subscribe request of the attempt.
    pub(crate) fn request(&self) -> SubscribeRequest {
        let expected_schema = match &self.expected_schema {
            Some(schema) => Some(RowSchema::clone(schema)),
            None => None,
        };
        self.contract.request(expected_schema)
    }
}

struct DesiredSubscription {
    contract: SubscriptionContract,
    lifecycle: SubscriptionLifecycle,
    /// The exchange the latest creation or restoration of the entry was sent on.
    generation: Arc<()>,
    /// Fences the result of that attempt from a later use of the same name.
    ticket: Arc<()>,
    attempt_in_flight: bool,
    /// The schema the generation the server last opened for the contract announced, or `None`
    /// while the server never acknowledged it. Only an acknowledged entry is restorable, and its
    /// restoration expects this schema.
    announced: Option<Arc<RowSchema>>,
    /// Whether the exchange of `generation` ended, and every subscription its session held.
    generation_ended: bool,
    /// The end of the generation the server ended, until the caller reads it. Its event follows
    /// the generation's other events on the exchange, and the registry reports it itself when the
    /// exchange ends first. Only an ended entry holds one, and not one whose delivery overflowed,
    /// because that overflow is the last event of its generation.
    unread_end: Option<SubscriptionEnded>,
}

impl DesiredSubscription {
    fn restore(&mut self, generation: Arc<()>) -> Option<RestoreAttempt> {
        let SubscriptionLifecycle::Interrupted(previous) = &self.lifecycle else {
            return None;
        };
        let Some(announced) = &self.announced else {
            return None;
        };
        let expected_schema = Some(announced.clone());
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
            expected_schema,
        })
    }

    /// Records the schema `opened` announced, unless the reader already recorded it.
    fn announce(&mut self, opened: &SubscriptionOpened) {
        if let Some(announced) = &self.announced
            && **announced == opened.schema
        {
            return;
        }
        self.announced = Some(Arc::new(opened.schema.clone()));
    }

    /// Whether a session that is still open may hold the subscription: one holds a generation
    /// it acknowledged, or an attempt that may still open one awaits its reply. The session of an
    /// ended exchange holds nothing, and neither does one that refused to open the subscription
    /// again. A generation the server ended holds nothing either: the server keeps its name only
    /// until the name is reused or the session ends.
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
            SubscriptionLifecycle::Interrupted(_) | SubscriptionLifecycle::Ended(_) => false,
        }
    }

    /// Whether an event the exchange of `generation` queued belongs to the generation the entry
    /// delivers. An ended generation delivers what preceded its end until the caller reads the
    /// end, and a consumer overflow, which dropped the end together with the events before it.
    fn delivers(&self, event: &SubscriptionEvent, generation: &Arc<()>) -> bool {
        if !Arc::ptr_eq(&self.generation, generation) || self.generation_ended {
            return false;
        }
        let handle = event.subscription();
        match &self.lifecycle {
            SubscriptionLifecycle::Active(active)
            | SubscriptionLifecycle::DeliveryFailed(active) => active == handle,
            SubscriptionLifecycle::Ended(ended) => {
                if ended != handle {
                    return false;
                }
                if let SubscriptionEvent::ConsumerOverflow(_) = event {
                    return true;
                }
                self.unread_end.is_some()
            }
            SubscriptionLifecycle::Creating
            | SubscriptionLifecycle::Restoring(_)
            | SubscriptionLifecycle::Interrupted(_)
            | SubscriptionLifecycle::Closing
            | SubscriptionLifecycle::DeletionFailed => false,
        }
    }
}

/// What the registry reports about a subscription itself, rather than the exchange's queue.
enum RegistryEvent {
    Interrupted(SubscriptionInterruption),
    RestorationFailed(SubscriptionRestorationFailure),
    /// An end the server sent that its exchange ended before the caller read, or the end of a
    /// restoration the server refused for good.
    Ended(SubscriptionEnded),
}

impl RegistryEvent {
    fn name(&self) -> &SubscriptionName {
        match self {
            Self::Interrupted(interrupted) => &interrupted.subscription.name,
            Self::RestorationFailed(failure) => &failure.subscription.name,
            Self::Ended(ended) => &ended.subscription.name,
        }
    }
}

impl From<RegistryEvent> for SubscriptionEvent {
    fn from(event: RegistryEvent) -> Self {
        match event {
            RegistryEvent::Interrupted(interrupted) => Self::Interrupted(interrupted),
            RegistryEvent::RestorationFailed(failure) => Self::RestorationFailed(failure),
            RegistryEvent::Ended(ended) => Self::Ended(ended),
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
    /// interruption, one unread restoration failure and one unread end, so they are bounded by the
    /// entries.
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

    /// Reports an end the registry delivers itself: one whose exchange ended before the caller read
    /// it, or that of a restoration the server refused for good. It is the last event of its
    /// generation, and a generation ends once.
    fn report_end(&mut self, ended: SubscriptionEnded) {
        self.events.push_back(RegistryEvent::Ended(ended));
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
    /// The server ended the subscription's generation, so nothing is left for it to delete and
    /// the client stopped wanting it without asking.
    Ended,
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

/// The event the registry reported itself when a reader looked, or how the reader waits for one.
pub(crate) enum DesiredSubscriptionEvent {
    Event(SubscriptionEvent),
    /// Nothing was reported; the receiver observes the registry's next change.
    Waiting(watch::Receiver<()>),
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
            entry.announced.is_some()
                && !matches!(
                    entry.lifecycle,
                    SubscriptionLifecycle::Closing
                        | SubscriptionLifecycle::DeletionFailed
                        | SubscriptionLifecycle::DeliveryFailed(_)
                        | SubscriptionLifecycle::Ended(_)
                )
        })
    }

    /// Starts creating a subscription under the contract's name, on the exchange of `generation`.
    ///
    /// A name the client holds is refused, except one whose generation the server ended: the
    /// server takes the reused name as a new generation, which replaces the ended one here.
    pub(crate) fn begin(
        &self,
        contract: SubscriptionContract,
        generation: Arc<()>,
    ) -> Option<RestoreAttempt> {
        let name = contract.create.name.clone();
        let mut state = self.inner.state.lock();
        if state.deletions.contains_key(&name) {
            return None;
        }
        if let Some(existing) = state.entries.get(&name) {
            let SubscriptionLifecycle::Ended(_) = existing.lifecycle else {
                return None;
            };
            state.forget_events(&name);
        }
        let ticket = Arc::new(());
        state.entries.insert(
            name,
            DesiredSubscription {
                contract: contract.clone(),
                lifecycle: SubscriptionLifecycle::Creating,
                generation: generation.clone(),
                ticket: ticket.clone(),
                attempt_in_flight: true,
                announced: None,
                generation_ended: false,
                unread_end: None,
            },
        );
        drop(state);
        self.inner.changed.send_replace(());
        Some(RestoreAttempt {
            contract,
            ticket,
            generation,
            expected_schema: None,
        })
    }

    /// Starts deleting `name` on the exchange of `generation`.
    ///
    /// A subscription no open session holds is deleted here: its session ended, the session now
    /// open refused to open it again, or the server ended its generation, so there is nothing for
    /// a server to delete.
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
            let released = state.entries.remove(name);
            drop(state);
            self.inner.changed.send_replace(());
            if let Some(DesiredSubscription {
                lifecycle: SubscriptionLifecycle::Ended(_),
                ..
            }) = released
            {
                return Cancellation::Ended;
            }
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
    /// `schema` is the one the reply announced, which the reader shares with its stream.
    pub(crate) fn acknowledge(
        &self,
        handle: &SubscriptionHandle,
        schema: &Arc<RowSchema>,
        generation: &Arc<()>,
    ) {
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
            entry.announced = Some(schema.clone());
            entry.lifecycle = SubscriptionLifecycle::Active(handle.clone());
        }
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Records that the exchange's queue dropped the events of `handle` it could not retain, and
    /// reports the overflow as the last event of that generation's delivery.
    pub(crate) fn overflow(&self, handle: &SubscriptionHandle, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get_mut(&handle.name) else {
            return;
        };
        if !Arc::ptr_eq(&entry.generation, generation) || entry.generation_ended {
            return;
        }
        match &entry.lifecycle {
            SubscriptionLifecycle::Active(active) if active == handle => {
                entry.lifecycle = SubscriptionLifecycle::DeliveryFailed(handle.clone());
            }
            // The end itself did not fit, and was dropped with the events before it.
            SubscriptionLifecycle::Ended(ended) if ended == handle => {
                entry.unread_end = None;
            }
            _ => {}
        }
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Applies the end of a generation the server ended, which is the generation's last frame.
    ///
    /// The exchange reader applies it before it queues the end's event, so the subscription reads
    /// ended to a caller that has not read the end yet, and a session lost before the caller reads
    /// it does not restore the generation. A generation whose delivery already failed ends without
    /// an end to read: the consumer overflow stays its last event.
    pub(crate) fn end(&self, ended: &SubscriptionEnded, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get_mut(&ended.subscription.name) else {
            return;
        };
        if !Arc::ptr_eq(&entry.generation, generation) || entry.generation_ended {
            return;
        }
        match &entry.lifecycle {
            SubscriptionLifecycle::Active(active) if *active == ended.subscription => {
                entry.unread_end = Some(ended.clone());
            }
            SubscriptionLifecycle::DeliveryFailed(failed) if *failed == ended.subscription => {}
            // The entry no longer delivers the generation that ended.
            _ => return,
        }
        entry.lifecycle = SubscriptionLifecycle::Ended(ended.subscription.clone());
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Resolves a creation attempt with the generation the server opened, if it opened one.
    ///
    /// A success that arrives after its caller began deleting the name keeps the entry closing,
    /// and the deletion then asks the server to delete that generation.
    pub(crate) fn created(&self, attempt: &RestoreAttempt, opened: Option<&SubscriptionOpened>) {
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
            // The reader applied the reply that opened the generation when it arrived, before the
            // end that followed it, so the end stands.
            _ if let SubscriptionLifecycle::Ended(_) = entry.lifecycle => {}
            Some(opened) if entry.generation_ended => {
                if let SubscriptionLifecycle::Closing = entry.lifecycle {
                    state.entries.remove(name);
                } else if entry.announced.is_none() {
                    entry.announce(opened);
                    entry.lifecycle =
                        SubscriptionLifecycle::Interrupted(opened.subscription.clone());
                    state.interrupted(SubscriptionInterruption {
                        subscription: opened.subscription.clone(),
                    });
                }
            }
            Some(opened) if let SubscriptionLifecycle::Closing = entry.lifecycle => {
                entry.announce(opened);
            }
            Some(_) if let SubscriptionLifecycle::DeliveryFailed(_) = entry.lifecycle => {}
            Some(opened) => {
                entry.announce(opened);
                entry.lifecycle = SubscriptionLifecycle::Active(opened.subscription.clone());
            }
            None if entry.generation_ended
                && let SubscriptionLifecycle::Closing = entry.lifecycle =>
            {
                state.entries.remove(name);
            }
            None if entry.generation_ended && entry.announced.is_some() => {}
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

    /// Ends a subscription whose restoration the server refused for good, as the server's end of
    /// its generation would end it: the subscription reads `Ended` from then on, its end is
    /// reported once, after the events before it, and it is never opened again on its own.
    /// Nothing happens once the entry no longer waits for that attempt's exchange to open it: the
    /// caller deleted it, or the exchange ended.
    pub(crate) fn restoration_ended(
        &self,
        attempt: &RestoreAttempt,
        reason: SubscriptionEndReason,
        message: String,
    ) {
        let name = &attempt.contract.create.name;
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get_mut(name) else {
            return;
        };
        if !Arc::ptr_eq(&entry.ticket, &attempt.ticket)
            || !Arc::ptr_eq(&entry.generation, &attempt.generation)
            || entry.generation_ended
        {
            return;
        }
        let SubscriptionLifecycle::Interrupted(previous) = &entry.lifecycle else {
            return;
        };
        let previous = previous.clone();
        entry.lifecycle = SubscriptionLifecycle::Ended(previous.clone());
        state.report_end(SubscriptionEnded {
            subscription: previous,
            reason,
            message,
        });
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Reports that a restoration attempt did not open the subscription, which the client tries
    /// again after `retry_after`. `false` once the entry no longer waits for that attempt's
    /// exchange to open it: the caller deleted it, or the exchange ended.
    pub(crate) fn restoration_failed(
        &self,
        attempt: &RestoreAttempt,
        cause: SubscriptionRestorationCause,
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
            cause,
            message,
            retry_after,
        };
        state.restoration_failed(failure);
        drop(state);
        self.inner.changed.send_replace(());
        true
    }

    /// Ends every subscription the exchange of `generation` held. An acknowledged one is
    /// interrupted until a later exchange restores it, and one the server ended stays ended: its
    /// end is reported here if the caller has not read it, because the exchange's queue discards
    /// the events the caller did not read.
    pub(crate) fn ended(&self, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let mut closed = Vec::new();
        let mut interruptions = Vec::new();
        let mut unread_ends = Vec::new();
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
                SubscriptionLifecycle::Ended(_) => {
                    if let Some(unread) = entry.unread_end.take() {
                        unread_ends.push(unread);
                    }
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
        for unread in unread_ends {
            state.report_end(unread);
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

    /// Whether an event the exchange of `generation` queued reaches the caller. Handing the caller
    /// the end of a generation the server ended records that the caller read it, so the registry
    /// never reports that end again.
    pub(crate) fn admit(&self, event: &SubscriptionEvent, generation: &Arc<()>) -> bool {
        let mut state = self.inner.state.lock();
        let Some(entry) = state.entries.get_mut(&event.subscription().name) else {
            return false;
        };
        if !entry.delivers(event, generation) {
            return false;
        }
        if let SubscriptionEvent::Ended(_) = event {
            entry.unread_end = None;
        }
        true
    }

    /// Takes the earliest event the registry reports itself, or subscribes to its next change when
    /// it reports none.
    ///
    /// It subscribes before it looks: a subscription marks every change made so far as seen, so an
    /// event the registry reported between a look and a later subscription would wait for another.
    pub(crate) fn next_event_or_changes(&self) -> DesiredSubscriptionEvent {
        let changes = self.watch();
        if let Some(event) = self.take_event() {
            return DesiredSubscriptionEvent::Event(event);
        }
        DesiredSubscriptionEvent::Waiting(changes)
    }

    /// Takes the earliest event the registry reports itself: an interruption, a failed
    /// restoration, or an end its exchange ended before the caller read it.
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod shuttle_tests {
    use std::num::NonZeroU64;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_model_harness::shuttle::check_random_and_pct;
    use nervix_models::{RelayName, SubscriptionDeliveryBehavior};

    use super::*;

    fn name() -> SubscriptionName {
        SubscriptionName::parse("watch").assured("a literal subscription name")
    }

    fn contract() -> SubscriptionContract {
        SubscriptionContract {
            domain: DomainName::parse("tenant").assured("a literal domain name"),
            create: CreateSubscription {
                name: name(),
                relay: RelayName::parse("events").assured("a literal relay name"),
                delivery_behavior: SubscriptionDeliveryBehavior::Dropping,
                batch_sample_rate: None,
                where_clause: None,
            },
            subscription_type: SubscriptionType::Row,
        }
    }

    /// The reply that opened generation `generation` of the subscription.
    fn announced(generation: NonZeroU64) -> SubscriptionOpened {
        SubscriptionOpened {
            subscription: SubscriptionHandle {
                name: name(),
                generation,
            },
            domain: DomainName::parse("tenant").assured("a literal domain name"),
            relay: RelayName::parse("events").assured("a literal relay name"),
            subscription_type: SubscriptionType::Row,
            schema: RowSchema {
                fields: Vec::new(),
                branch: None,
            },
        }
    }

    /// A restoration the server refused for good, ending the subscription, races its caller
    /// deleting it. Whichever comes first, the deletion never asks the server about a subscription
    /// nothing opened, no event of the deleted subscription is left for its caller, and the name is
    /// free again once the deletion completes.
    #[test]
    fn shuttle_a_restoration_ended_for_good_racing_a_deletion_releases_the_name() {
        check_random_and_pct(|| {
            shuttle::future::block_on(async {
                let desired = DesiredSubscriptions::new();
                let first_generation = Arc::new(());
                let pending = desired
                    .begin(contract(), first_generation.clone())
                    .verified("the registry starts empty");
                desired.created(&pending, Some(&announced(NonZeroU64::MIN)));
                desired.ended(&first_generation);
                let generation = Arc::new(());
                let restoring = desired
                    .restore(generation.clone())
                    .pop()
                    .verified("the acknowledged subscription is restored");
                let ending = {
                    let desired = desired.clone();
                    nervix_primitives::task::spawn(async move {
                        desired.created(&restoring, None);
                        desired.restoration_ended(
                            &restoring,
                            SubscriptionEndReason::RelayRemoved,
                            "stream 'events' does not exist in domain 'tenant'".to_string(),
                        );
                    })
                };
                let deleting = {
                    let desired = desired.clone();
                    let generation = generation.clone();
                    nervix_primitives::task::spawn(
                        async move { desired.cancel(&name(), generation) },
                    )
                };
                ending
                    .await
                    .assured("the ending side only resolves the restoration");
                let cancellation = deleting
                    .await
                    .assured("the deleting side only starts the deletion");
                if let Cancellation::Delete(attempt) = cancellation {
                    assert!(
                        !desired.deletion_waits(&attempt),
                        "the restoration the deletion waited for is resolved"
                    );
                    assert!(
                        matches!(desired.deletion_target(&attempt), DeletionTarget::NotOpened),
                        "a deletion never asks the server about a subscription nothing opened"
                    );
                }
                assert_eq!(desired.lifecycle(&name()), None);
                let left = desired.take_event();
                assert!(
                    left.is_none(),
                    "a deleted subscription leaves no event for its caller: {left:?}"
                );
                assert!(
                    desired.begin(contract(), generation).is_some(),
                    "the name is free once the deletion completed"
                );
            });
        });
    }

    /// A reader finds no event the registry reported while the subscription's exchange ends: it
    /// takes the interruption that end reports, or its wait for the registry's next change
    /// observes it.
    #[test]
    fn shuttle_a_reader_racing_a_reported_gap_takes_it_without_another_change() {
        check_random_and_pct(|| {
            shuttle::future::block_on(async {
                let desired = DesiredSubscriptions::new();
                let generation = Arc::new(());
                let pending = desired
                    .begin(contract(), generation.clone())
                    .verified("the registry starts empty");
                desired.created(&pending, Some(&announced(NonZeroU64::MIN)));
                let ending = {
                    let desired = desired.clone();
                    nervix_primitives::task::spawn(async move { desired.ended(&generation) })
                };
                let reading = {
                    let desired = desired.clone();
                    nervix_primitives::task::spawn(async move {
                        match desired.next_event_or_changes() {
                            DesiredSubscriptionEvent::Event(_) => {}
                            DesiredSubscriptionEvent::Waiting(mut changes) => {
                                changes.changed().await.assured(
                                    "the registry keeps the sender of its own notifications",
                                );
                                assert!(
                                    desired.take_event().is_some(),
                                    "the change the reader waited for reported no event"
                                );
                            }
                        }
                    })
                };
                ending
                    .await
                    .assured("the ending side only ends the exchange");
                reading
                    .await
                    .assured("the reading side only takes the reported gap");
            });
        });
    }
}
