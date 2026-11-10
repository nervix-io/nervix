//! One exchange with a server, and the dispatcher that routes every frame it receives.
//!
//! Layer: edges.
//!
//! Every request of an exchange takes an identity that is non-zero and never reused on that
//! exchange, and its waiter is registered under that identity before its frame is sent. Every
//! reply names the identity it answers, so the reader completes exactly the waiter it belongs to,
//! whatever order replies arrive in. Each exchange owns its identities and waiters, so a reply of
//! an earlier exchange can never complete a request of a later one.
//!
//! - **Owns.** Request correlation, including the reply kind each request accepts and the execution
//!   a command's outcome must name, the waiters of an exchange, the reassembly of replies too large
//!   for one frame, the subscriptions an exchange holds, and unsolicited messages and domain clock
//!   replies to the client's event emitters.
//! - **Depends on.** The wire contract's frames and tonic's gRPC client.
//! - **Must not know.** Query semantics, how an outcome is routed, or how a lost exchange is
//!   recovered.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{collections::VecDeque, fmt::Display, num::NonZeroU64};

use ahash::{HashMap, HashSet};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    self as wire, CancelRequest, ClientFrame, ClientMessage, ClientRequest, ClientRequestKind,
    DomainInfo, EncodedFrame, Leadership, OpenIngestorDisposition, ProducerId, Reply, ReplyBody,
    ReplyBodyKind, RequestId, RowSchema, ServerFrame, ServerMessage, SessionLimits,
    SubscribeDisposition, SubscriptionHandle, SubscriptionOpened, TransferAssembly, TransferPart,
    UnsubscribeDisposition, VerifiedFrame, grpc::EXCHANGE_PATH,
};
use nervix_models::{CommandExecutionReference, RelayName};
use nervix_primitives::{
    stream::{Stream, StreamExt as _, wrappers::ReceiverStream},
    sync::{Arc, Mutex, blocking::Mutex as SyncMutex, mpsc, oneshot, watch},
    task::JoinHandle,
};
use nervix_recovery::{Discarded as _, NoReceiver as _, Reported as _};
use tonic::{Request, Status, codegen::http::uri::PathAndQuery, transport::Channel};

use crate::{
    connection::GrpcConnector,
    domain_clock::DomainClockAttachments,
    error::{ClientError, RequestKind},
    events::{ServerEvent, SubscriptionEvent, SubscriptionRowsEvent},
    producer::ProducerRegistry,
    subscriptions::DesiredSubscriptions,
};

/// Explicit default policy used by isolated protocol fixtures.
#[cfg(test)]
pub(crate) const SESSION_LIMITS: SessionLimits = SessionLimits::DEFAULT;

/// Request frames queued for an exchange before a sender waits for the transport.
const REQUEST_FRAME_CAPACITY: usize = 32;

/// Server notices retained for one exchange, bounded both by records and retained bytes.
const SERVER_NOTICE_CAPACITY: usize = 128;
pub(crate) const SERVER_NOTICE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EventQueueError {
    /// Events arrived faster than they were read, and the queue dropped the ones it held. The
    /// events that arrive after the gap follow it.
    #[error("the event queue dropped events that were not read in time")]
    Overflow,
    /// The generation the read began on ended.
    #[error("the session of the event queue ended")]
    Closed,
}

/// Whether a generation's queue delivers events, owes its reader a gap, or has ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueCondition {
    /// Events are retained in the order they arrived.
    Delivering,
    /// An event did not fit, so the queue dropped it and every event it held. Events that arrive
    /// later are retained behind the gap, which the next read reports first.
    Overflowed,
    /// The generation ended, and the queue retains nothing more for it.
    Closed,
}

struct QueuedEvent<T> {
    value: T,
    bytes: usize,
}

#[derive(Clone, Copy)]
struct QueueByteLimits {
    total: usize,
    per_subscription: Option<usize>,
}

/// A subscription budget is installed once with its exchange generation.
#[derive(Clone, Copy)]
enum QueueBudget {
    Fixed(usize),
    SessionSubscriptions {
        retained_frames: std::num::NonZeroUsize,
        subscriptions: std::num::NonZeroUsize,
    },
}

impl QueueBudget {
    fn resolve(self, limits: &SessionLimits) -> Result<QueueByteLimits, Report<ClientError>> {
        match self {
            Self::Fixed(total) => Ok(QueueByteLimits {
                total,
                per_subscription: None,
            }),
            Self::SessionSubscriptions {
                retained_frames,
                subscriptions,
            } => {
                let (bytes, total_bytes) = crate::capabilities::subscription_byte_limits(
                    limits,
                    retained_frames,
                    subscriptions,
                )?;
                Ok(QueueByteLimits {
                    total: total_bytes,
                    per_subscription: Some(bytes),
                })
            }
        }
    }
}

struct EventQueueState<T> {
    byte_limits: Option<QueueByteLimits>,
    generation: Arc<()>,
    events: VecDeque<QueuedEvent<T>>,
    bytes: usize,
    condition: QueueCondition,
    subscription_usage: HashMap<SubscriptionHandle, QueueUsage>,
    overflowed_subscriptions: HashSet<SubscriptionHandle>,
    overflow_events: VecDeque<T>,
}

impl<T> EventQueueState<T> {
    fn push_subscription(
        &mut self,
        value: T,
        bytes: usize,
        policy: &SubscriptionQueuePolicy<T>,
        max_records: usize,
        limits: QueueByteLimits,
    ) -> bool {
        let key = (policy.key)(&value);
        if self.overflowed_subscriptions.contains(&key) {
            return false;
        }
        let (records, retained_bytes) = match self.subscription_usage.get(&key) {
            Some(usage) => (usage.records, usage.bytes),
            None => (0, 0),
        };
        let next_subscription_bytes = retained_bytes.checked_add(bytes);
        let next_total_bytes = self.bytes.checked_add(bytes);
        let subscription_fits = records < policy.max_records
            && next_subscription_bytes.is_some_and(|next| {
                next <= limits.per_subscription.assured(
                    "subscription queues install a per-subscription budget with their generation",
                )
            });
        let total_fits = self.events.len() < max_records
            && next_total_bytes.is_some_and(|next| next <= limits.total);
        if !subscription_fits || !total_fits {
            let mut retained = VecDeque::new();
            while let Some(event) = self.events.pop_front() {
                if (policy.key)(&event.value) == key {
                    self.bytes = self.bytes.checked_sub(event.bytes).assured(
                        "global retained bytes include every subscription event until removal",
                    );
                } else {
                    retained.push_back(event);
                }
            }
            self.events = retained;
            self.subscription_usage.remove(&key).discarded(
                "an event overflows its subscription whether or not that subscription already \
                 retained events",
            );
            self.overflowed_subscriptions.insert(key);
            self.overflow_events.push_back((policy.overflow)(&value));
            return true;
        }
        let usage = self.subscription_usage.entry(key).or_default();
        usage.records = usage
            .records
            .checked_add(1)
            .assured("a subscription record count below its finite capacity has room for one more");
        usage.bytes =
            next_subscription_bytes.assured("a subscription event fits its retained byte capacity");
        self.bytes = next_total_bytes.assured("a subscription event fits the global byte capacity");
        self.events.push_back(QueuedEvent { value, bytes });
        false
    }

    fn pop_event(&mut self, policy: Option<&SubscriptionQueuePolicy<T>>) -> Option<T> {
        if let Some(overflow) = self.overflow_events.pop_front() {
            return Some(overflow);
        }
        let event = self.events.pop_front()?;
        self.bytes = self
            .bytes
            .checked_sub(event.bytes)
            .assured("queued bytes include every retained event until it is removed");
        if let Some(policy) = policy {
            let key = (policy.key)(&event.value);
            let usage = self
                .subscription_usage
                .get_mut(&key)
                .assured("every retained subscription event has a usage entry");
            usage.records = usage
                .records
                .checked_sub(1)
                .assured("a retained subscription event contributes one to its record count");
            usage.bytes = usage
                .bytes
                .checked_sub(event.bytes)
                .assured("a retained subscription event contributes its bytes to the subscription");
            if usage.records == 0 {
                self.subscription_usage.remove(&key).discarded(
                    "the popped event already removed the subscription's final retained record",
                );
            }
        }
        Some(event.value)
    }
}

#[derive(Default)]
struct QueueUsage {
    records: usize,
    bytes: usize,
}

struct SubscriptionQueuePolicy<T> {
    key: fn(&T) -> SubscriptionHandle,
    overflow: fn(&T) -> T,
    max_records: usize,
}

struct EventQueueInner<T> {
    state: SyncMutex<EventQueueState<T>>,
    changed: watch::Sender<()>,
    max_records: usize,
    budget: QueueBudget,
    subscription: Option<SubscriptionQueuePolicy<T>>,
}

/// A generation-scoped event queue. An unread consumer cannot hold the exchange reader: when the
/// bounded queue fills, it drops what it held and reports the gap to its reader, and the exchange
/// reader keeps routing replies.
pub(crate) struct EventQueue<T> {
    inner: Arc<EventQueueInner<T>>,
}

impl<T> Clone for EventQueue<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> EventQueue<T> {
    pub(crate) fn new(max_records: usize, max_bytes: usize) -> Self {
        Self::with_policy(max_records, QueueBudget::Fixed(max_bytes), None)
    }

    fn with_policy(
        max_records: usize,
        budget: QueueBudget,
        subscription: Option<SubscriptionQueuePolicy<T>>,
    ) -> Self {
        let (changed, _) = watch::channel(());
        Self {
            inner: Arc::new(EventQueueInner {
                state: SyncMutex::new(EventQueueState {
                    byte_limits: match budget {
                        QueueBudget::Fixed(total) => Some(QueueByteLimits {
                            total,
                            per_subscription: None,
                        }),
                        QueueBudget::SessionSubscriptions { .. } => None,
                    },
                    generation: Arc::new(()),
                    events: VecDeque::new(),
                    bytes: 0,
                    condition: match budget {
                        QueueBudget::Fixed(_) => QueueCondition::Delivering,
                        QueueBudget::SessionSubscriptions { .. } => QueueCondition::Closed,
                    },
                    subscription_usage: HashMap::default(),
                    overflowed_subscriptions: HashSet::default(),
                    overflow_events: VecDeque::new(),
                }),
                changed,
                max_records,
                budget,
                subscription,
            }),
        }
    }

    pub(crate) fn begin(
        &self,
        generation: &Arc<()>,
        limits: &SessionLimits,
    ) -> Result<(), Report<ClientError>> {
        let byte_limits = self.inner.budget.resolve(limits)?;
        let mut state = self.inner.state.lock();
        state.byte_limits = Some(byte_limits);
        state.generation = generation.clone();
        state.events.clear();
        state.bytes = 0;
        state.condition = QueueCondition::Delivering;
        state.subscription_usage.clear();
        state.overflowed_subscriptions.clear();
        state.overflow_events.clear();
        drop(state);
        self.inner.changed.send_replace(());
        Ok(())
    }

    fn close(&self, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        if !Arc::ptr_eq(&state.generation, generation) {
            return;
        }
        state.events.clear();
        state.bytes = 0;
        state.condition = QueueCondition::Closed;
        state.subscription_usage.clear();
        state.overflowed_subscriptions.clear();
        state.overflow_events.clear();
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Retains one event without waiting on its consumer. Subscription overflow ends only the
    /// affected subscription's delivery; any other overflow drops every event the queue holds and
    /// leaves a gap that its next read reports.
    pub(crate) fn push(&self, generation: &Arc<()>, value: T, bytes: usize) -> bool {
        let mut state = self.inner.state.lock();
        if !Arc::ptr_eq(&state.generation, generation) || state.condition == QueueCondition::Closed
        {
            return false;
        }
        let limits = state
            .byte_limits
            .assured("an active queue has its generation budget installed");
        if let Some(policy) = &self.inner.subscription {
            let overflowed =
                state.push_subscription(value, bytes, policy, self.inner.max_records, limits);
            drop(state);
            self.inner.changed.send_replace(());
            return overflowed;
        }
        let next_bytes = state.bytes.checked_add(bytes);
        let exceeds_bytes = match next_bytes {
            Some(next) => next > limits.total,
            None => true,
        };
        if state.events.len() >= self.inner.max_records || exceeds_bytes {
            state.events.clear();
            state.bytes = 0;
            state.condition = QueueCondition::Overflowed;
        } else if let Some(next_bytes) = next_bytes {
            state.bytes = next_bytes;
            state.events.push_back(QueuedEvent { value, bytes });
        }
        drop(state);
        self.inner.changed.send_replace(());
        false
    }

    /// Waits for the next event of the current generation. A gap is reported once, before the
    /// events that follow it; `Closed` reports that the generation the read began on ended.
    pub(crate) async fn next(&self) -> error_stack::Result<T, EventQueueError> {
        let mut changed = self.inner.changed.subscribe();
        let generation = self.inner.state.lock().generation.clone();
        loop {
            nervix_primitives::task::consume_budget().await;
            {
                let mut state = self.inner.state.lock();
                if !Arc::ptr_eq(&state.generation, &generation) {
                    return Err(error_stack::Report::new(EventQueueError::Closed));
                }
                match state.condition {
                    QueueCondition::Delivering => {}
                    QueueCondition::Overflowed => {
                        state.condition = QueueCondition::Delivering;
                        return Err(error_stack::Report::new(EventQueueError::Overflow));
                    }
                    QueueCondition::Closed => {
                        return Err(error_stack::Report::new(EventQueueError::Closed));
                    }
                }
                if let Some(event) = state.pop_event(self.inner.subscription.as_ref()) {
                    return Ok(event);
                }
            }
            if changed.changed().await.is_err() {
                return Err(error_stack::Report::new(EventQueueError::Closed));
            }
        }
    }

    /// Waits until the queue belongs to a generation that has not ended: at once while the current
    /// one delivers, and otherwise until the next exchange begins one.
    pub(crate) async fn resumed(&self) {
        let mut changed = self.inner.changed.subscribe();
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.inner.state.lock().condition != QueueCondition::Closed {
                return;
            }
            changed
                .changed()
                .await
                .assured("the queue holds the sender of its own notifications");
        }
    }

    #[cfg(test)]
    pub(crate) fn try_next(&self) -> Option<T> {
        let mut state = self.inner.state.lock();
        state.pop_event(self.inner.subscription.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn close_current(&self) {
        let generation = self.inner.state.lock().generation.clone();
        self.close(&generation);
    }

    /// Whether the generation the queue belongs to has ended.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.state.lock().condition == QueueCondition::Closed
    }
}

impl EventQueue<SubscriptionEvent> {
    pub(crate) fn for_subscriptions(capabilities: crate::ClientCapabilities) -> Self {
        let limits = capabilities.subscription_record_limits();
        Self::with_policy(
            limits.total_records,
            QueueBudget::SessionSubscriptions {
                retained_frames: capabilities.subscription_retained_frames(),
                subscriptions: capabilities.subscriptions_retained_in_full(),
            },
            Some(SubscriptionQueuePolicy {
                key: |event| event.subscription().clone(),
                overflow: |event| SubscriptionEvent::ConsumerOverflow(event.subscription().clone()),
                max_records: limits.records,
            }),
        )
    }
}

/// Where the unsolicited messages of every exchange of one client go.
#[derive(Clone)]
pub(crate) struct EventTargets {
    pub(crate) subscriptions: EventQueue<SubscriptionEvent>,
    pub(crate) desired: DesiredSubscriptions,
    pub(crate) notices: EventQueue<ServerEvent>,
    /// The latest leadership observation. It is replaced rather than queued, so an observation
    /// nobody read can never hold the exchange up.
    pub(crate) leadership: watch::Sender<Option<Leadership>>,
    /// The latest complete domain list, replaced the same way.
    pub(crate) domains: watch::Sender<Option<Vec<DomainInfo>>>,
    /// The domain clocks the client follows, and the events about them.
    pub(crate) clocks: DomainClockAttachments,
    /// The producers opened on each exchange, and the events about them.
    pub(crate) producers: ProducerRegistry,
    /// Desired native emitter consumers and their current attachments.
    pub(crate) consumers: crate::consumer::DesiredConsumers,
}

impl EventTargets {
    pub(crate) fn begin_generation(
        &self,
        limits: &SessionLimits,
    ) -> Result<Arc<()>, Report<ClientError>> {
        let generation = Arc::new(());
        self.subscriptions.begin(&generation, limits)?;
        self.notices.begin(&generation, limits)?;
        Ok(generation)
    }

    pub(crate) fn close_generation(&self, generation: &Arc<()>) {
        self.desired.ended(generation);
        self.clocks.exchange_ended(generation);
        self.producers.exchange_ended(generation);
        self.consumers.exchange_ended(generation);
        self.subscriptions.close(generation);
        self.notices.close(generation);
    }
}

/// The client's event emitters together with the receiving ends its callers read.
pub(crate) struct SessionEvents {
    pub(crate) emitters: EventTargets,
    pub(crate) leadership: watch::Receiver<Option<Leadership>>,
    pub(crate) domains: Mutex<watch::Receiver<Option<Vec<DomainInfo>>>>,
}

impl SessionEvents {
    pub(crate) fn new(capabilities: crate::ClientCapabilities) -> Self {
        let desired = DesiredSubscriptions::new();
        let subscriptions = EventQueue::for_subscriptions(capabilities);
        let notices = EventQueue::new(SERVER_NOTICE_CAPACITY, SERVER_NOTICE_BYTES);
        let (leadership, observed_leadership) = watch::channel(None);
        let (domains, observed_domains) = watch::channel(None);
        Self {
            emitters: EventTargets {
                subscriptions: subscriptions.clone(),
                desired,
                notices: notices.clone(),
                leadership,
                domains,
                clocks: DomainClockAttachments::new(),
                producers: ProducerRegistry::default(),
                consumers: crate::consumer::DesiredConsumers::default(),
            },
            leadership: observed_leadership,
            domains: Mutex::new(observed_domains),
        }
    }
}

/// One exchange with a server and its reader's lifetime.
pub(crate) struct Exchange {
    pub(crate) requests: Arc<ExchangeRequests>,
    /// The reader still to join. It is cleared after joining; dropping the exchange aborts it.
    pub(crate) reader: Option<JoinHandle<()>>,
    pub(crate) emitters: EventTargets,
    pub(crate) generation: Arc<()>,
    /// The channel the exchange runs on, which resource uploads share.
    pub(crate) channel: Channel,
}

/// What a request needs of the exchange it is sent on.
pub(crate) struct ExchangeRequests {
    pub(crate) limits: SessionLimits,
    pub(crate) frames: mpsc::Sender<EncodedFrame<ClientFrame>>,
    /// The waiters of the exchange's requests. The exchange's reader shares the registry: it
    /// completes each waiter as its reply arrives and closes the registry when the exchange ends.
    pub(crate) pending: Arc<SyncMutex<PendingReplies>>,
}

impl ExchangeRequests {
    /// Registers the waiter of `request`, with what its reply must be to answer it. The waiter is
    /// removed if its awaiting future is cancelled.
    pub(crate) fn register_for(&self, request: &ClientRequest) -> Option<PendingRequest> {
        let registered = self.pending.lock().register(ExpectedReply::of(request))?;
        Some(PendingRequest {
            request_id: registered.request_id,
            reply: registered.reply,
            pending: self.pending.clone(),
        })
    }

    /// Asks the server to cancel `target`, a request of this exchange that waits for its reply,
    /// under an identity of the cancellation's own. The server answers the cancellation and ends
    /// `target` with a terminal reply of its own, which alone decides what became of it, so the
    /// cancellation's outcome is only given a waiter to arrive at. `None` when the exchange has
    /// ended, which ends `target`'s wait as well.
    pub(crate) async fn cancel(&self, target: RequestId) -> Option<PendingRequest> {
        let request = ClientRequest::Cancel(CancelRequest { target });
        let cancellation = self.register_for(&request)?;
        let message = ClientMessage {
            request_id: cancellation.request_id,
            request,
        };
        let frame = message
            .encode(&self.limits)
            .assured("a cancellation names one request identity, far within the frame limit");
        if self.frames.send(frame).await.is_err() {
            self.pending.lock().close();
            return None;
        }
        Some(cancellation)
    }
}

pub(crate) struct PendingRequest {
    pub(crate) request_id: RequestId,
    reply: oneshot::Receiver<ReplyBody>,
    pending: Arc<SyncMutex<PendingReplies>>,
}

impl PendingRequest {
    pub(crate) async fn receive(&mut self) -> Option<ReplyBody> {
        (&mut self.reply).await.ok()
    }
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.pending
            .lock()
            .take(self.request_id)
            .discarded("the request completed or its waiter was cancelled");
    }
}

impl Exchange {
    /// Opens an exchange on `channel` and starts routing the frames the server sends on it.
    pub(crate) async fn open(
        channel: Channel,
        connector: &GrpcConnector,
        emitters: EventTargets,
    ) -> error_stack::Result<Self, ClientError> {
        let mut opening_channel = channel.clone();
        let (frames, outbound) = mpsc::channel(REQUEST_FRAME_CAPACITY);
        let mut request = Request::new(ReceiverStream::new(outbound));
        connector.authorize(&mut request);
        let opened = nervix_primitives::time::timeout(connector.connect_timeout(), async {
            crate::session_open::ready(&mut opening_channel)
                .await
                .map_err(|error| Report::new(ClientError::ConnectServer(error)))?;
            crate::session_open::open::<wire::ClientFrame, wire::ServerFrame>(
                &mut opening_channel,
                request,
                PathAndQuery::from_static(EXCHANGE_PATH),
                crate::session_open::OutboundBounds::Announced,
            )
            .await
            .map_err(|status| Report::new(ClientError::StartSession(Box::new(status))))
        })
        .await
        .map_err(|_| Report::new(ClientError::SessionOpenDeadline))??;
        let limits = opened.limits;
        let pending = Arc::new(SyncMutex::new(PendingReplies::new()));
        let generation = emitters.begin_generation(&limits)?;
        let reader = ExchangeReader::new(
            pending.clone(),
            emitters.clone(),
            generation.clone(),
            limits,
        );
        let reader = nervix_primitives::task::spawn(reader.run(opened.frames));
        Ok(Self {
            requests: Arc::new(ExchangeRequests {
                frames,
                pending,
                limits,
            }),
            reader: Some(reader),
            emitters,
            generation,
            channel,
        })
    }

    /// The request side of the exchange, for one request to be sent on.
    pub(crate) fn requests(&self) -> Arc<ExchangeRequests> {
        self.requests.clone()
    }

    /// Ends the exchange. Every request still waiting on it observes the closed session.
    ///
    /// The reader stops before the generation ends, so nothing it routes can follow that end: an
    /// attach reply or an acknowledgement it applied is interrupted with the rest of the exchange
    /// rather than left looking held by an exchange that is gone.
    pub(crate) async fn close(mut self) {
        self.interrupt(ExchangeEnd::Closed).await;
    }

    /// Stops this exact reader and waits for its lifetime cleanup, retaining the exchange handle.
    pub(crate) async fn interrupt(&mut self, end: ExchangeEnd) {
        self.requests.pending.lock().end(end);
        let Some(reader) = &mut self.reader else {
            return;
        };
        reader.abort();
        let joined = reader.await;
        self.reader = None;
        joined.discarded(
            "an aborted reader ends cancelled, and one that ended first already closed its \
             requests",
        );
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        // Nothing reads an exchange's replies once it is gone, and stopping its reader releases
        // the server's session. Its waiters observe the closed session instead of waiting out
        // their deadlines.
        if let Some(reader) = &self.reader {
            reader.abort();
        }
        self.requests.pending.lock().close();
    }
}

/// The requests of one exchange that wait for their replies.
pub(crate) enum PendingReplies {
    Open {
        /// The identity the next request takes, or `None` once the exchange has handed out every
        /// identity. Identities start at 1 and are never reused on the exchange.
        next_request_id: Option<RequestId>,
        waiters: HashMap<RequestId, PendingReply>,
        /// The open state alone keeps the frame reader waiting. Dropping it ends that wait even
        /// when the transport never sends another frame.
        reader_ending: watch::Sender<()>,
    },
    /// The exchange ended. Its waiters were dropped, so each observed the closed session once,
    /// and a request registered now would never be answered.
    Closed(ExchangeEnd),
}

/// One outstanding reply and what it must be to answer its request.
pub(crate) struct PendingReply {
    reply: oneshot::Sender<ReplyBody>,
    expected: ExpectedReply,
}

/// What a reply must be to answer the request registered under its identity.
pub(crate) struct ExpectedReply {
    request: ClientRequestKind,
    /// The execution a command's outcome must name; `None` for every other request.
    execution_reference: Option<CommandExecutionReference>,
}

impl ExpectedReply {
    pub(crate) fn of(request: &ClientRequest) -> Self {
        let execution_reference = match request {
            ClientRequest::Command(command) => Some(command.execution_reference.clone()),
            _ => None,
        };
        Self {
            request: ClientRequestKind::from(request),
            execution_reference,
        }
    }

    /// The end a reply forces on its exchange when it cannot answer this request: a reply of
    /// another kind, or a command outcome that names another execution. `None` when it answers it.
    fn violated_by(&self, body: &ReplyBody) -> Option<ExchangeEnd> {
        let reply = ReplyBodyKind::from(body);
        if !self.request.is_answered_by(reply) {
            return Some(ExchangeEnd::ReplyKindMismatch {
                request: self.request,
                reply,
            });
        }
        // Only a command is answered by a command outcome, and a command's expectation always
        // names its execution.
        match (&self.execution_reference, body) {
            (Some(expected), ReplyBody::Command(outcome))
                if expected != &outcome.execution_reference =>
            {
                Some(ExchangeEnd::ExecutionMismatch {
                    expected: expected.clone(),
                    received: outcome.execution_reference.clone(),
                })
            }
            _ => None,
        }
    }
}

/// The first reason an exchange ended, shared by every interrupted waiter.
pub(crate) enum ExchangeEnd {
    Closed,
    Transport(Status),
    ExecutionMismatch {
        expected: CommandExecutionReference,
        received: CommandExecutionReference,
    },
    /// A reply of a kind that cannot answer the request waiting under its identity.
    ReplyKindMismatch {
        request: ClientRequestKind,
        reply: ReplyBodyKind,
    },
}

/// A request registered with its exchange, before its frame is sent.
pub(crate) struct RegisteredRequest {
    pub(crate) request_id: RequestId,
    pub(crate) reply: oneshot::Receiver<ReplyBody>,
}

impl PendingReplies {
    pub(crate) fn is_open(&self) -> bool {
        matches!(self, Self::Open { .. })
    }

    pub(crate) fn new() -> Self {
        Self::Open {
            next_request_id: Some(RequestId::new(NonZeroU64::MIN)),
            waiters: HashMap::default(),
            reader_ending: watch::channel(()).0,
        }
    }

    /// Registers before the reader waits for frames. An already closed registry needs no wait.
    fn observe_end(&self) -> Option<watch::Receiver<()>> {
        match self {
            Self::Open { reader_ending, .. } => Some(reader_ending.subscribe()),
            Self::Closed(_) => None,
        }
    }

    /// Takes the next identity and registers a waiter for its reply, which must be what
    /// `expected` describes.
    ///
    /// `None` once the exchange has ended, or has handed out every identity and has to be
    /// replaced; either way a request sent on it would never be answered.
    pub(crate) fn register(&mut self, expected: ExpectedReply) -> Option<RegisteredRequest> {
        let Self::Open {
            next_request_id,
            waiters,
            ..
        } = self
        else {
            return None;
        };
        let request_id = next_request_id.take()?;
        *next_request_id = request_id.get().checked_add(1).map(RequestId::new);
        let (waiter, reply) = oneshot::channel();
        waiters
            .insert(
                request_id,
                PendingReply {
                    reply: waiter,
                    expected,
                },
            )
            .discarded("an identity is handed out once, so no earlier waiter holds it");
        Some(RegisteredRequest { request_id, reply })
    }

    /// Removes the waiter of `request_id`, because its reply arrived or its frame was never sent.
    pub(crate) fn take(&mut self, request_id: RequestId) -> Option<oneshot::Sender<ReplyBody>> {
        self.take_waiter(request_id).map(|waiter| waiter.reply)
    }

    fn take_waiter(&mut self, request_id: RequestId) -> Option<PendingReply> {
        match self {
            Self::Open { waiters, .. } => waiters.remove(&request_id),
            Self::Closed(_) => None,
        }
    }

    /// Ends the exchange's requests. Every waiter is dropped, so each observes the closed session
    /// exactly once.
    pub(crate) fn close(&mut self) {
        self.close_with(None);
    }

    pub(crate) fn close_with(&mut self, status: Option<Status>) {
        let end = match status {
            Some(status) => ExchangeEnd::Transport(status),
            None => ExchangeEnd::Closed,
        };
        self.end(end);
    }

    fn end(&mut self, end: ExchangeEnd) {
        if let Self::Closed(_) = self {
            return;
        }
        *self = Self::Closed(end);
    }

    pub(crate) fn failure(&self) -> ClientError {
        match self {
            Self::Closed(ExchangeEnd::Transport(status)) => {
                ClientError::Transport(Box::new(status.clone()))
            }
            Self::Closed(ExchangeEnd::ExecutionMismatch { expected, received }) => {
                ClientError::ExecutionReferenceMismatch {
                    expected: expected.clone(),
                    received: received.clone(),
                }
            }
            Self::Closed(ExchangeEnd::ReplyKindMismatch { request, reply }) => {
                ClientError::ReplyKindMismatch {
                    request: RequestKind::from(*request),
                    reply: *reply,
                }
            }
            Self::Open { .. } | Self::Closed(ExchangeEnd::Closed) => ClientError::SessionClosed,
        }
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "exchange/shuttle_tests.rs"]
mod shuttle_tests;

/// Whether an exchange goes on after a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReaderFlow {
    Continue,
    End,
}

/// Routes the frames of one exchange: each reply to the request that waits for it, each transfer
/// part into the reply it reassembles, and each unsolicited message to the client's event emitters.
pub(crate) struct ExchangeReader {
    limits: SessionLimits,
    pending: Arc<SyncMutex<PendingReplies>>,
    emitters: EventTargets,
    generation: Arc<()>,
    subscriptions: SubscriptionRegistry,
    transfers: HashMap<RequestId, TransferAssembly>,
    failure: Option<Status>,
}

impl Drop for ExchangeReader {
    fn drop(&mut self) {
        self.pending.lock().close_with(self.failure.take());
        self.emitters.close_generation(&self.generation);
    }
}

impl ExchangeReader {
    pub(crate) fn new(
        pending: Arc<SyncMutex<PendingReplies>>,
        emitters: EventTargets,
        generation: Arc<()>,
        limits: SessionLimits,
    ) -> Self {
        Self {
            limits,
            pending,
            emitters,
            generation,
            subscriptions: SubscriptionRegistry::default(),
            transfers: HashMap::default(),
            failure: None,
        }
    }

    /// Routes frames until the transport or the request registry ends, then interrupts this
    /// generation after routing has stopped.
    pub(crate) async fn run<S>(mut self, mut frames: S)
    where
        S: Stream<Item = Result<VerifiedFrame<ServerFrame>, Status>> + Unpin,
    {
        let ending = self.pending.lock().observe_end();
        if let Some(mut ending) = ending {
            // One registration remains live across frames. The open registry never sends a value,
            // so this wait ends with its sender.
            let mut ended = std::pin::pin!(ending.changed());
            loop {
                nervix_primitives::task::consume_budget().await;
                let received = nervix_primitives::select! {
                    biased;
                    closed = &mut ended => {
                        closed.discarded("dropping the open registry's only sender ends its frame reader");
                        break;
                    }
                    received = frames.next() => received,
                };
                let frame = match received {
                    Some(Ok(frame)) => frame,
                    Some(Err(status)) => {
                        Result::<(), _>::Err(status.clone())
                            .reported("reading the session exchange");
                        self.failure = Some(status);
                        break;
                    }
                    // The server ended the exchange.
                    None => break,
                };
                if let ReaderFlow::End = self.route(frame).await {
                    break;
                }
            }
        }
    }

    /// Routes one frame of the exchange.
    pub(crate) async fn route(&mut self, frame: VerifiedFrame<ServerFrame>) -> ReaderFlow {
        let message = match ServerMessage::decode(&frame) {
            Ok(message) => message,
            Err(violation) => return Self::violated(violation, "decoding a session frame"),
        };
        match message {
            ServerMessage::Reply(reply) => self.deliver(reply).await,
            ServerMessage::TransferPart(part) => self.assemble(part).await,
            ServerMessage::Event(event) => self.publish(event).await,
        }
    }

    /// Ends the exchange over a frame the session contract does not describe. Nothing later on the
    /// exchange can be trusted, and the exchange is the violation's only witness, so it is
    /// reported here.
    fn violated(violation: impl Display, operation: &str) -> ReaderFlow {
        Result::<(), _>::Err(violation).reported(operation);
        ReaderFlow::End
    }

    /// Completes the waiter of the request a reply answers, once the reply is shown to answer it.
    ///
    /// A reply that cannot answer the request waiting under its identity ends the exchange before
    /// anything it carries is applied. A reply no request waits for completes nothing, and what it
    /// opens is still followed.
    async fn deliver(&mut self, reply: Reply) -> ReaderFlow {
        let mut pending = self.pending.lock();
        let waiter = pending.take_waiter(reply.request_id);
        if let Some(waiter) = &waiter
            && let Some(end) = waiter.expected.violated_by(&reply.body)
        {
            pending.end(end);
            let violation = pending.failure();
            drop(pending);
            return Self::violated(
                violation,
                "validating a reply against the request it answers",
            );
        }
        drop(pending);
        if let ReplyBody::Subscribe(outcome) = &reply.body
            && let SubscribeDisposition::Opened(opened) = &outcome.disposition
        {
            // The stream reads rows against the announced schema, and the desired subscription
            // expects it when it is restored, so both share one copy of it.
            let schema = Arc::new(opened.schema.clone());
            self.emitters
                .desired
                .acknowledge(&opened.subscription, &schema, &self.generation);
            self.subscriptions.open(opened, schema);
        }
        self.subscriptions.track(&reply.body);
        match &reply.body {
            ReplyBody::DomainClockAttach(outcome) => {
                self.emitters.clocks.apply_attach(outcome, &self.generation);
            }
            ReplyBody::DomainClockDetach(outcome) => self.emitters.clocks.apply_detach(outcome),
            ReplyBody::OpenIngestor(outcome) => {
                if let OpenIngestorDisposition::Opened(opened) = &outcome.disposition {
                    self.emitters.producers.opened(
                        &self.generation,
                        ProducerId::opened_by(reply.request_id),
                        opened.description.admission,
                    );
                }
            }
            _ => {}
        }
        let Some(waiter) = waiter else {
            // No request of this exchange waits under the identity, so no one is owed the reply.
            return ReaderFlow::Continue;
        };
        waiter
            .reply
            .send(reply.body)
            .means_peer_left("session request waiter");
        ReaderFlow::Continue
    }

    /// Adds a part to the reply it reassembles, and delivers the reply once it is complete.
    async fn assemble(&mut self, part: TransferPart) -> ReaderFlow {
        let request_id = part.request_id();
        let assembly = self
            .transfers
            .entry(request_id)
            .or_insert_with(|| TransferAssembly::new(request_id, &self.limits));
        if let Err(violation) = assembly.append(&part) {
            return Self::violated(violation, "reassembling a transferred reply");
        }
        if !assembly.is_complete() {
            return ReaderFlow::Continue;
        }
        let assembly = self
            .transfers
            .remove(&request_id)
            .verified("the assembly found complete above is held under its request");
        let reply = match assembly.finish() {
            Ok(reply) => reply,
            Err(violation) => return Self::violated(violation, "finishing a transferred reply"),
        };
        self.deliver(reply).await
    }

    /// Delivers an unsolicited message to the client's event emitters.
    async fn publish(&mut self, event: wire::ServerEvent) -> ReaderFlow {
        match event {
            wire::ServerEvent::SessionOpened(_) => Self::violated(
                wire::WireDecodeError::UnexpectedMessage {
                    expected: "ordinary exchange event",
                    actual: "WebSocket session opening",
                },
                "routing an exchange event",
            ),
            wire::ServerEvent::Notice(notice) => self.notify(ServerEvent::from(notice)),
            wire::ServerEvent::Leadership(observed) => {
                self.emitters
                    .leadership
                    .send_replace(Some(observed.leadership));
                ReaderFlow::Continue
            }
            wire::ServerEvent::Domains(observed) => {
                self.emitters.domains.send_replace(Some(observed.domains));
                ReaderFlow::Continue
            }
            // Observations of one domain follow a domain selection, which this client never sends.
            wire::ServerEvent::DomainSnapshot(_) | wire::ServerEvent::Cluster(_) => {
                ReaderFlow::Continue
            }
            wire::ServerEvent::SubscriptionRows(rows) => {
                let Some(stream) = self.subscriptions.stream(rows.subscription()) else {
                    // The rows belong to a generation this client does not hold.
                    return ReaderFlow::Continue;
                };
                let event = SubscriptionEvent::Rows(SubscriptionRowsEvent {
                    relay: stream.relay.clone(),
                    schema: stream.schema.clone(),
                    rows,
                });
                self.forward(event)
            }
            wire::ServerEvent::SubscriptionDeliveryLost(lost) => {
                if !self.subscriptions.holds(&lost.subscription) {
                    return ReaderFlow::Continue;
                }
                self.forward(SubscriptionEvent::DeliveryLost(lost))
            }
            wire::ServerEvent::SubscriptionRowsSkipped(skipped) => {
                if !self.subscriptions.holds(&skipped.subscription) {
                    return ReaderFlow::Continue;
                }
                self.forward(SubscriptionEvent::RowsSkipped(skipped))
            }
            wire::ServerEvent::SubscriptionEnded(ended) => {
                if !self.subscriptions.close(&ended.subscription) {
                    return ReaderFlow::Continue;
                }
                // The end is applied before its event is queued, so no caller reads an end the
                // client does not hold, and a session lost before the end is read cannot restore
                // the generation it ended.
                self.emitters.desired.end(&ended, &self.generation);
                self.forward(SubscriptionEvent::Ended(ended))
            }
            wire::ServerEvent::DomainClockObserved(observed) => {
                self.emitters
                    .clocks
                    .apply_observed(observed, &self.generation);
                ReaderFlow::Continue
            }
            wire::ServerEvent::DomainClockTicked(ticked) => {
                self.emitters.clocks.apply_ticked(ticked, &self.generation);
                ReaderFlow::Continue
            }
            wire::ServerEvent::DomainClockAttachmentEnded(ended) => {
                self.emitters.clocks.apply_ended(ended, &self.generation);
                ReaderFlow::Continue
            }
            wire::ServerEvent::ProducerAdmissionChanged(changed) => {
                self.emitters.producers.admission(&self.generation, changed);
                ReaderFlow::Continue
            }
            wire::ServerEvent::ProducerEnded(ended) => {
                self.emitters.producers.ended(&self.generation, ended);
                ReaderFlow::Continue
            }
            // No reply follows for any request still in flight; the waiters observe the closed
            // session when the exchange ends.
            wire::ServerEvent::SessionEnding(_) => ReaderFlow::End,
        }
    }

    /// Hands a subscription event to its bounded queue without delaying another reply.
    fn forward(&self, event: SubscriptionEvent) -> ReaderFlow {
        let handle = event.subscription().clone();
        let bytes = event.queued_bytes();
        let overflowed = self
            .emitters
            .subscriptions
            .push(&self.generation, event, bytes);
        if overflowed {
            self.emitters.desired.overflow(&handle, &self.generation);
        }
        ReaderFlow::Continue
    }

    /// Hands a notice to its bounded queue without delaying another reply.
    fn notify(&self, notice: ServerEvent) -> ReaderFlow {
        let bytes = notice.queued_bytes();
        self.emitters.notices.push(&self.generation, notice, bytes);
        ReaderFlow::Continue
    }
}

/// The subscriptions an exchange holds, by handle, with what their rows are read against.
#[derive(Default)]
struct SubscriptionRegistry {
    streams: HashMap<SubscriptionHandle, SubscriptionStream>,
}

/// The relay an opened subscription reads and the schema its rows follow.
struct SubscriptionStream {
    relay: RelayName,
    schema: Arc<RowSchema>,
}

impl SubscriptionRegistry {
    /// Follows the subscriptions a reply deletes.
    fn track(&mut self, body: &ReplyBody) {
        if let ReplyBody::Unsubscribe(outcome) = body
            && let UnsubscribeDisposition::Deleted(subscription) = &outcome.disposition
        {
            self.close(subscription);
        }
    }

    /// Follows a subscription a reply opened, whose rows follow `schema`.
    ///
    /// The reader calls this before the reply reaches its waiter and before it routes any later
    /// frame, and the server sends a subscription's opening reply before any of its rows, so the
    /// rows of an opened subscription always find it here.
    fn open(&mut self, opened: &SubscriptionOpened, schema: Arc<RowSchema>) {
        let stream = SubscriptionStream {
            relay: opened.relay.clone(),
            schema,
        };
        self.streams
            .insert(opened.subscription.clone(), stream)
            .discarded(
                "a generation opens once, and an opening reply repeated for it only restates the \
                 stream it replaces",
            );
    }

    /// Stops holding a subscription, and says whether it was held.
    fn close(&mut self, subscription: &SubscriptionHandle) -> bool {
        self.streams.remove(subscription).is_some()
    }

    fn holds(&self, subscription: &SubscriptionHandle) -> bool {
        self.streams.contains_key(subscription)
    }

    fn stream(&self, subscription: &SubscriptionHandle) -> Option<&SubscriptionStream> {
        self.streams.get(subscription)
    }
}
