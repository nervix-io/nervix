//! One exchange with a server, and the dispatcher that routes every frame it receives.
//!
//! Every request of an exchange takes an identity that is non-zero and never reused on that
//! exchange, and its waiter is registered under that identity before its frame is sent. Every
//! reply names the identity it answers, so the reader completes exactly the waiter it belongs to,
//! whatever order replies arrive in. Each exchange owns its identities and waiters, so a reply of
//! an earlier exchange can never complete a request of a later one.
//!
//! - **Owns.** Request identities, the waiters of an exchange, the reassembly of replies too large
//!   for one frame, the subscriptions an exchange holds, and the delivery of unsolicited messages
//!   to the client's event sinks.
//! - **Depends on.** The wire contract's frames and tonic's gRPC client.
//! - **Must not know.** What a request means, how its outcome is routed, or how a lost exchange is
//!   recovered.

use std::{collections::VecDeque, fmt::Display, num::NonZeroU64};

use ahash::{HashMap, HashSet};
use meticulous::OptionExt as _;
use nervix_client_wire::{
    self as wire, ClientFrame, DomainInfo, EncodedFrame, Leadership, Reply, ReplyBody, RequestId,
    RowSchema, ServerFrame, ServerMessage, SessionLimits, SubscribeDisposition, SubscriptionHandle,
    SubscriptionOpened, TransferAssembly, TransferPart, UnsubscribeDisposition, VerifiedFrame,
    grpc::{ClientExchangeCodec, EXCHANGE_PATH},
};
use nervix_models::RelayName;
use nervix_recovery::{Discarded as _, NoReceiver as _, Reported as _};
use parking_lot::Mutex as SyncMutex;
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_stream::{Stream, StreamExt as _, wrappers::ReceiverStream};
use tonic::{Request, Status, codegen::http::uri::PathAndQuery, transport::Channel};
use triomphe::Arc;

use crate::{
    connection::GrpcConnector,
    error::ClientError,
    events::{ServerEvent, SubscriptionEvent, SubscriptionRowsEvent},
    subscriptions::DesiredSubscriptions,
};

/// The limits every frame of a client session is held to.
pub(crate) const SESSION_LIMITS: SessionLimits = SessionLimits::DEFAULT;

/// Request frames queued for an exchange before a sender waits for the transport.
const REQUEST_FRAME_CAPACITY: usize = 32;

/// Subscription events retained for one exchange, bounded both by records and retained bytes.
const SUBSCRIPTION_EVENT_CAPACITY: usize = 128;
pub(crate) const SUBSCRIPTION_EVENT_BYTES: usize = 8 * 1024 * 1024;
const SUBSCRIPTION_RECORD_CAPACITY: usize = 32;
const SUBSCRIPTION_RETAINED_BYTES: usize = 2 * 1024 * 1024;

/// Server notices retained for one exchange, bounded both by records and retained bytes.
const SERVER_NOTICE_CAPACITY: usize = 128;
pub(crate) const SERVER_NOTICE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EventQueueError {
    #[error("the event stream exceeded its queue")]
    Overflow,
    #[error("the event stream closed")]
    Closed,
}

struct QueuedEvent<T> {
    value: T,
    bytes: usize,
}

struct EventQueueState<T> {
    generation: Arc<()>,
    events: VecDeque<QueuedEvent<T>>,
    bytes: usize,
    terminal: Option<EventQueueError>,
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
        max_bytes: usize,
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
            && next_subscription_bytes.is_some_and(|next| next <= policy.max_bytes);
        let total_fits = self.events.len() < max_records
            && next_total_bytes.is_some_and(|next| next <= max_bytes);
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
    max_bytes: usize,
}

struct EventQueueInner<T> {
    state: SyncMutex<EventQueueState<T>>,
    changed: watch::Sender<()>,
    max_records: usize,
    max_bytes: usize,
    subscription: Option<SubscriptionQueuePolicy<T>>,
}

/// A generation-scoped event queue. An unread consumer cannot hold the exchange reader; if its
/// bounded queue fills, that generation fails visibly and the reader still routes replies.
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
        Self::with_policy(max_records, max_bytes, None)
    }

    fn with_policy(
        max_records: usize,
        max_bytes: usize,
        subscription: Option<SubscriptionQueuePolicy<T>>,
    ) -> Self {
        let (changed, _) = watch::channel(());
        Self {
            inner: Arc::new(EventQueueInner {
                state: SyncMutex::new(EventQueueState {
                    generation: Arc::new(()),
                    events: VecDeque::new(),
                    bytes: 0,
                    terminal: None,
                    subscription_usage: HashMap::default(),
                    overflowed_subscriptions: HashSet::default(),
                    overflow_events: VecDeque::new(),
                }),
                changed,
                max_records,
                max_bytes,
                subscription,
            }),
        }
    }

    pub(crate) fn begin(&self, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        state.generation = generation.clone();
        state.events.clear();
        state.bytes = 0;
        state.terminal = None;
        state.subscription_usage.clear();
        state.overflowed_subscriptions.clear();
        state.overflow_events.clear();
        drop(state);
        self.inner.changed.send_replace(());
    }

    fn close(&self, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        if !Arc::ptr_eq(&state.generation, generation) {
            return;
        }
        state.events.clear();
        state.bytes = 0;
        state.terminal = Some(EventQueueError::Closed);
        state.subscription_usage.clear();
        state.overflowed_subscriptions.clear();
        state.overflow_events.clear();
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Retains one event without waiting on its consumer. Subscription overflow ends only the
    /// affected subscription's delivery; notice overflow closes the notice stream.
    pub(crate) fn push(&self, generation: &Arc<()>, value: T, bytes: usize) -> bool {
        let mut state = self.inner.state.lock();
        if !Arc::ptr_eq(&state.generation, generation) || state.terminal.is_some() {
            return false;
        }
        if let Some(policy) = &self.inner.subscription {
            let overflowed = state.push_subscription(
                value,
                bytes,
                policy,
                self.inner.max_records,
                self.inner.max_bytes,
            );
            drop(state);
            self.inner.changed.send_replace(());
            return overflowed;
        }
        let next_bytes = state.bytes.checked_add(bytes);
        let exceeds_bytes = match next_bytes {
            Some(next) => next > self.inner.max_bytes,
            None => true,
        };
        if state.events.len() >= self.inner.max_records || exceeds_bytes {
            state.events.clear();
            state.bytes = 0;
            state.terminal = Some(EventQueueError::Overflow);
        } else if let Some(next_bytes) = next_bytes {
            state.bytes = next_bytes;
            state.events.push_back(QueuedEvent { value, bytes });
        }
        drop(state);
        self.inner.changed.send_replace(());
        false
    }

    pub(crate) async fn next(&self) -> error_stack::Result<T, EventQueueError> {
        let mut changed = self.inner.changed.subscribe();
        let generation = self.inner.state.lock().generation.clone();
        loop {
            tokio::task::consume_budget().await;
            {
                let mut state = self.inner.state.lock();
                if !Arc::ptr_eq(&state.generation, &generation) {
                    return Err(error_stack::Report::new(EventQueueError::Closed));
                }
                if let Some(terminal) = state.terminal {
                    return Err(error_stack::Report::new(terminal));
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
}

impl EventQueue<SubscriptionEvent> {
    pub(crate) fn for_subscriptions() -> Self {
        Self::with_policy(
            SUBSCRIPTION_EVENT_CAPACITY,
            SUBSCRIPTION_EVENT_BYTES,
            Some(SubscriptionQueuePolicy {
                key: |event| event.subscription().clone(),
                overflow: |event| SubscriptionEvent::ConsumerOverflow(event.subscription().clone()),
                max_records: SUBSCRIPTION_RECORD_CAPACITY,
                max_bytes: SUBSCRIPTION_RETAINED_BYTES,
            }),
        )
    }
}

/// Where the unsolicited messages of every exchange of one client go.
#[derive(Clone)]
pub(crate) struct EventSinks {
    pub(crate) subscriptions: EventQueue<SubscriptionEvent>,
    pub(crate) desired: DesiredSubscriptions,
    pub(crate) notices: EventQueue<ServerEvent>,
    /// The latest leadership observation. It is replaced rather than queued, so an observation
    /// nobody read can never hold the exchange up.
    pub(crate) leadership: watch::Sender<Option<Leadership>>,
    /// The latest complete domain list, replaced the same way.
    pub(crate) domains: watch::Sender<Option<Vec<DomainInfo>>>,
}

impl EventSinks {
    pub(crate) fn begin_generation(&self) -> Arc<()> {
        let generation = Arc::new(());
        self.subscriptions.begin(&generation);
        self.notices.begin(&generation);
        generation
    }

    pub(crate) fn close_generation(&self, generation: &Arc<()>) {
        self.desired.ended(generation);
        self.subscriptions.close(generation);
        self.notices.close(generation);
    }
}

/// The client's event sinks together with the receiving ends its callers read.
pub(crate) struct SessionEvents {
    pub(crate) sinks: EventSinks,
    pub(crate) leadership: watch::Receiver<Option<Leadership>>,
    pub(crate) domains: Mutex<watch::Receiver<Option<Vec<DomainInfo>>>>,
}

impl SessionEvents {
    pub(crate) fn new() -> Self {
        let desired = DesiredSubscriptions::new();
        let subscriptions = EventQueue::for_subscriptions();
        let notices = EventQueue::new(SERVER_NOTICE_CAPACITY, SERVER_NOTICE_BYTES);
        let (leadership, observed_leadership) = watch::channel(None);
        let (domains, observed_domains) = watch::channel(None);
        Self {
            sinks: EventSinks {
                subscriptions: subscriptions.clone(),
                desired,
                notices: notices.clone(),
                leadership,
                domains,
            },
            leadership: observed_leadership,
            domains: Mutex::new(observed_domains),
        }
    }
}

/// One open exchange with a server.
pub(crate) struct Exchange {
    pub(crate) requests: Arc<ExchangeRequests>,
    /// The task routing the exchange's frames. Dropping the exchange aborts it.
    pub(crate) reader: JoinHandle<()>,
    pub(crate) sinks: EventSinks,
    pub(crate) generation: Arc<()>,
}

/// What a request needs of the exchange it is sent on.
pub(crate) struct ExchangeRequests {
    pub(crate) frames: mpsc::Sender<EncodedFrame<ClientFrame>>,
    /// The waiters of the exchange's requests. The exchange's reader shares the registry: it
    /// completes each waiter as its reply arrives and closes the registry when the exchange ends.
    pub(crate) pending: Arc<SyncMutex<PendingReplies>>,
    /// The channel the exchange runs on, which resource uploads share.
    pub(crate) channel: Channel,
}

impl ExchangeRequests {
    /// Registers a request whose waiter is removed if its awaiting future is cancelled.
    pub(crate) fn register(&self) -> Option<PendingRequest> {
        let registered = self.pending.lock().register()?;
        Some(PendingRequest {
            request_id: registered.request_id,
            reply: registered.reply,
            pending: self.pending.clone(),
        })
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
        sinks: EventSinks,
    ) -> Result<Self, ClientError> {
        let mut client = tonic::client::Grpc::new(channel.clone())
            .max_decoding_message_size(SESSION_LIMITS.frame_bytes())
            .max_encoding_message_size(SESSION_LIMITS.frame_bytes());
        let (frames, outbound) = mpsc::channel(REQUEST_FRAME_CAPACITY);
        let mut request = Request::new(ReceiverStream::new(outbound));
        connector.authorize(&mut request);
        let response = tokio::time::timeout(connector.connect_timeout(), async {
            client.ready().await.map_err(ClientError::ConnectServer)?;
            client
                .streaming(
                    request,
                    PathAndQuery::from_static(EXCHANGE_PATH),
                    ClientExchangeCodec::new(SESSION_LIMITS),
                )
                .await
                .map_err(|status| ClientError::StartSession(Box::new(status)))
        })
        .await
        .map_err(|_| ClientError::SessionOpenDeadline)??;
        let pending = Arc::new(SyncMutex::new(PendingReplies::new()));
        let generation = sinks.begin_generation();
        let reader = ExchangeReader::new(pending.clone(), sinks.clone(), generation.clone());
        let reader = tokio::spawn(reader.run(response.into_inner()));
        Ok(Self {
            requests: Arc::new(ExchangeRequests {
                frames,
                pending,
                channel,
            }),
            reader,
            sinks,
            generation,
        })
    }

    /// The request side of the exchange, for one request to be sent on.
    pub(crate) fn requests(&self) -> Arc<ExchangeRequests> {
        self.requests.clone()
    }

    /// Ends the exchange. Every request still waiting on it observes the closed session.
    pub(crate) async fn close(self) {
        self.requests.pending.lock().close();
        self.sinks.close_generation(&self.generation);
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        // Nothing reads an exchange's replies once it is gone, and stopping its reader releases
        // the server's session.
        self.reader.abort();
    }
}

/// The requests of one exchange that wait for their replies.
pub(crate) enum PendingReplies {
    Open {
        /// The identity the next request takes, or `None` once the exchange has handed out every
        /// identity. Identities start at 1 and are never reused on the exchange.
        next_request_id: Option<RequestId>,
        waiters: HashMap<RequestId, oneshot::Sender<ReplyBody>>,
    },
    /// The exchange ended. Its waiters were dropped, so each observed the closed session once,
    /// and a request registered now would never be answered.
    Closed(Option<Status>),
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
        }
    }

    /// Takes the next identity and registers a waiter for its reply.
    ///
    /// `None` once the exchange has ended, or has handed out every identity and has to be
    /// replaced; either way a request sent on it would never be answered.
    pub(crate) fn register(&mut self) -> Option<RegisteredRequest> {
        let Self::Open {
            next_request_id,
            waiters,
        } = self
        else {
            return None;
        };
        let request_id = next_request_id.take()?;
        *next_request_id = request_id.get().checked_add(1).map(RequestId::new);
        let (waiter, reply) = oneshot::channel();
        waiters
            .insert(request_id, waiter)
            .discarded("an identity is handed out once, so no earlier waiter holds it");
        Some(RegisteredRequest { request_id, reply })
    }

    /// Removes the waiter of `request_id`, because its reply arrived or its frame was never sent.
    pub(crate) fn take(&mut self, request_id: RequestId) -> Option<oneshot::Sender<ReplyBody>> {
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
        if let Self::Closed(_) = self {
            return;
        }
        *self = Self::Closed(status);
    }

    pub(crate) fn failure(&self) -> ClientError {
        match self {
            Self::Closed(Some(status)) => ClientError::Transport(Box::new(status.clone())),
            Self::Open { .. } | Self::Closed(None) => ClientError::SessionClosed,
        }
    }
}

/// Whether an exchange goes on after a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReaderFlow {
    Continue,
    End,
}

/// Routes the frames of one exchange: each reply to the request that waits for it, each transfer
/// part into the reply it reassembles, and each unsolicited message to the client's event sinks.
pub(crate) struct ExchangeReader {
    pending: Arc<SyncMutex<PendingReplies>>,
    sinks: EventSinks,
    generation: Arc<()>,
    subscriptions: SubscriptionRegistry,
    transfers: HashMap<RequestId, TransferAssembly>,
}

impl ExchangeReader {
    pub(crate) fn new(
        pending: Arc<SyncMutex<PendingReplies>>,
        sinks: EventSinks,
        generation: Arc<()>,
    ) -> Self {
        Self {
            pending,
            sinks,
            generation,
            subscriptions: SubscriptionRegistry::default(),
            transfers: HashMap::default(),
        }
    }

    /// Routes the exchange's frames until it ends, then closes its requests.
    pub(crate) async fn run<S>(mut self, mut frames: S)
    where
        S: Stream<Item = Result<VerifiedFrame<ServerFrame>, Status>> + Unpin,
    {
        let mut failure = None;
        loop {
            tokio::task::consume_budget().await;
            let received = frames.next().await;
            let frame = match received {
                Some(Ok(frame)) => frame,
                Some(Err(status)) => {
                    Result::<(), _>::Err(status.clone()).reported("reading the session exchange");
                    failure = Some(status);
                    break;
                }
                // The server ended the exchange.
                None => break,
            };
            if let ReaderFlow::End = self.route(frame).await {
                break;
            }
        }
        self.pending.lock().close_with(failure);
        self.sinks.close_generation(&self.generation);
    }

    /// Routes one frame of the exchange.
    pub(crate) async fn route(&mut self, frame: VerifiedFrame<ServerFrame>) -> ReaderFlow {
        let message = match ServerMessage::decode(&frame) {
            Ok(message) => message,
            Err(violation) => return Self::violated(violation, "decoding a session frame"),
        };
        match message {
            ServerMessage::Reply(reply) => {
                self.deliver(reply).await;
                ReaderFlow::Continue
            }
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

    /// Completes the waiter of the request a reply answers.
    async fn deliver(&mut self, reply: Reply) {
        if let ReplyBody::Subscribe(outcome) = &reply.body
            && let SubscribeDisposition::Opened(opened) = &outcome.disposition
        {
            self.sinks
                .desired
                .acknowledge(&opened.subscription, &self.generation);
        }
        self.subscriptions.track(&reply.body);
        let waiter = self.pending.lock().take(reply.request_id);
        let Some(waiter) = waiter else {
            // No request of this exchange waits under the identity, so no one is owed the reply.
            return;
        };
        waiter
            .send(reply.body)
            .means_peer_left("session request waiter");
    }

    /// Adds a part to the reply it reassembles, and delivers the reply once it is complete.
    async fn assemble(&mut self, part: TransferPart) -> ReaderFlow {
        let request_id = part.request_id();
        let assembly = self
            .transfers
            .entry(request_id)
            .or_insert_with(|| TransferAssembly::new(request_id, &SESSION_LIMITS));
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
        self.deliver(reply).await;
        ReaderFlow::Continue
    }

    /// Delivers an unsolicited message to the client's event sinks.
    async fn publish(&mut self, event: wire::ServerEvent) -> ReaderFlow {
        match event {
            wire::ServerEvent::Notice(notice) => self.notify(ServerEvent::from(notice)),
            wire::ServerEvent::Leadership(observed) => {
                self.sinks
                    .leadership
                    .send_replace(Some(observed.leadership));
                ReaderFlow::Continue
            }
            wire::ServerEvent::Domains(observed) => {
                self.sinks.domains.send_replace(Some(observed.domains));
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
                self.forward(SubscriptionEvent::Ended(ended))
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
            .sinks
            .subscriptions
            .push(&self.generation, event, bytes);
        if overflowed {
            self.sinks.desired.overflow(&handle, &self.generation);
        }
        ReaderFlow::Continue
    }

    /// Hands a notice to its bounded queue without delaying another reply.
    fn notify(&self, notice: ServerEvent) -> ReaderFlow {
        let bytes = notice.queued_bytes();
        self.sinks.notices.push(&self.generation, notice, bytes);
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
    /// Follows the subscriptions a reply opens or deletes.
    ///
    /// The reader calls this before the reply reaches its waiter and before it routes any later
    /// frame, and the server sends a subscription's opening reply before any of its rows, so the
    /// rows of an opened subscription always find it here.
    fn track(&mut self, body: &ReplyBody) {
        match body {
            ReplyBody::Subscribe(outcome) => {
                if let SubscribeDisposition::Opened(opened) = &outcome.disposition {
                    self.open(opened);
                }
            }
            ReplyBody::Unsubscribe(outcome) => {
                if let UnsubscribeDisposition::Deleted(subscription) = &outcome.disposition {
                    self.close(subscription);
                }
            }
            _ => {}
        }
    }

    fn open(&mut self, opened: &SubscriptionOpened) {
        let stream = SubscriptionStream {
            relay: opened.relay.clone(),
            schema: Arc::new(opened.schema.clone()),
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
