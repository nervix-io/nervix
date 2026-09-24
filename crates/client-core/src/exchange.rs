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

use std::{fmt::Display, num::NonZeroU64};

use ahash::HashMap;
use meticulous::OptionExt as _;
use nervix_client_wire::{
    self as wire, ClientFrame, DomainInfo, EncodedFrame, Leadership, Reply, ReplyBody, RequestId,
    RowSchema, ServerFrame, ServerMessage, SessionLimits, SubscribeDisposition, SubscriptionHandle,
    SubscriptionOpened, TransferAssembly, TransferPart, UnsubscribeDisposition, VerifiedFrame,
    grpc::{ClientExchangeCodec, EXCHANGE_PATH},
};
use nervix_models::RelayName;
use nervix_recovery::{Discarded as _, NoReceiver as _, Reported as _};
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
};

/// The limits every frame of a client session is held to.
pub(crate) const SESSION_LIMITS: SessionLimits = SessionLimits::DEFAULT;

/// Request frames queued for an exchange before a sender waits for the transport.
const REQUEST_FRAME_CAPACITY: usize = 32;

/// Subscription events queued for the caller before the exchange waits for it to read them.
const SUBSCRIPTION_EVENT_CAPACITY: usize = 128;

/// Server notices queued for the caller before the exchange waits for it to read them.
const SERVER_NOTICE_CAPACITY: usize = 128;

/// Where the unsolicited messages of every exchange of one client go.
#[derive(Clone)]
pub(crate) struct EventSinks {
    pub(crate) subscriptions: mpsc::Sender<SubscriptionEvent>,
    pub(crate) notices: mpsc::Sender<ServerEvent>,
    /// The latest leadership observation. It is replaced rather than queued, so an observation
    /// nobody read can never hold the exchange up.
    pub(crate) leadership: watch::Sender<Option<Leadership>>,
    /// The latest complete domain list, replaced the same way.
    pub(crate) domains: watch::Sender<Option<Vec<DomainInfo>>>,
}

/// The client's event sinks together with the receiving ends its callers read.
pub(crate) struct SessionEvents {
    pub(crate) sinks: EventSinks,
    pub(crate) subscriptions: Mutex<mpsc::Receiver<SubscriptionEvent>>,
    pub(crate) notices: Mutex<mpsc::Receiver<ServerEvent>>,
    pub(crate) leadership: watch::Receiver<Option<Leadership>>,
    pub(crate) domains: Mutex<watch::Receiver<Option<Vec<DomainInfo>>>>,
}

impl SessionEvents {
    pub(crate) fn new() -> Self {
        let (subscriptions, subscription_events) = mpsc::channel(SUBSCRIPTION_EVENT_CAPACITY);
        let (notices, server_notices) = mpsc::channel(SERVER_NOTICE_CAPACITY);
        let (leadership, observed_leadership) = watch::channel(None);
        let (domains, observed_domains) = watch::channel(None);
        Self {
            sinks: EventSinks {
                subscriptions,
                notices,
                leadership,
                domains,
            },
            subscriptions: Mutex::new(subscription_events),
            notices: Mutex::new(server_notices),
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
}

/// What a request needs of the exchange it is sent on.
pub(crate) struct ExchangeRequests {
    pub(crate) frames: mpsc::Sender<EncodedFrame<ClientFrame>>,
    /// The waiters of the exchange's requests. The exchange's reader shares the registry: it
    /// completes each waiter as its reply arrives and closes the registry when the exchange ends.
    pub(crate) pending: Arc<Mutex<PendingReplies>>,
    /// The channel the exchange runs on, which resource uploads share.
    pub(crate) channel: Channel,
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
        client.ready().await.map_err(ClientError::ConnectServer)?;
        let (frames, outbound) = mpsc::channel(REQUEST_FRAME_CAPACITY);
        let mut request = Request::new(ReceiverStream::new(outbound));
        connector.authorize(&mut request);
        let response = client
            .streaming(
                request,
                PathAndQuery::from_static(EXCHANGE_PATH),
                ClientExchangeCodec::new(SESSION_LIMITS),
            )
            .await
            .map_err(|status| ClientError::StartSession(Box::new(status)))?;
        let pending = Arc::new(Mutex::new(PendingReplies::new()));
        let reader = ExchangeReader::new(pending.clone(), sinks);
        let reader = tokio::spawn(reader.run(response.into_inner()));
        Ok(Self {
            requests: Arc::new(ExchangeRequests {
                frames,
                pending,
                channel,
            }),
            reader,
        })
    }

    /// The request side of the exchange, for one request to be sent on.
    pub(crate) fn requests(&self) -> Arc<ExchangeRequests> {
        self.requests.clone()
    }

    /// Ends the exchange. Every request still waiting on it observes the closed session.
    pub(crate) async fn close(self) {
        self.requests.pending.lock().await.close();
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
    Closed,
}

/// A request registered with its exchange, before its frame is sent.
pub(crate) struct RegisteredRequest {
    pub(crate) request_id: RequestId,
    pub(crate) reply: oneshot::Receiver<ReplyBody>,
}

impl PendingReplies {
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
            Self::Closed => None,
        }
    }

    /// Ends the exchange's requests. Every waiter is dropped, so each observes the closed session
    /// exactly once.
    pub(crate) fn close(&mut self) {
        *self = Self::Closed;
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
    pending: Arc<Mutex<PendingReplies>>,
    sinks: EventSinks,
    subscriptions: SubscriptionRegistry,
    transfers: HashMap<RequestId, TransferAssembly>,
}

impl ExchangeReader {
    pub(crate) fn new(pending: Arc<Mutex<PendingReplies>>, sinks: EventSinks) -> Self {
        Self {
            pending,
            sinks,
            subscriptions: SubscriptionRegistry::default(),
            transfers: HashMap::default(),
        }
    }

    /// Routes the exchange's frames until it ends, then closes its requests.
    pub(crate) async fn run<S>(mut self, mut frames: S)
    where
        S: Stream<Item = Result<VerifiedFrame<ServerFrame>, Status>> + Unpin,
    {
        loop {
            tokio::task::consume_budget().await;
            let received = frames.next().await;
            let frame = match received {
                Some(Ok(frame)) => frame,
                Some(Err(status)) => {
                    Result::<(), _>::Err(status).reported("reading the session exchange");
                    break;
                }
                // The server ended the exchange.
                None => break,
            };
            if let ReaderFlow::End = self.route(frame).await {
                break;
            }
        }
        self.pending.lock().await.close();
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
        self.subscriptions.track(&reply.body);
        let waiter = self.pending.lock().await.take(reply.request_id);
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
            wire::ServerEvent::Notice(notice) => self.notify(ServerEvent::from(notice)).await,
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
                self.forward(event).await
            }
            wire::ServerEvent::SubscriptionDeliveryLost(lost) => {
                if !self.subscriptions.holds(&lost.subscription) {
                    return ReaderFlow::Continue;
                }
                self.forward(SubscriptionEvent::DeliveryLost(lost)).await
            }
            wire::ServerEvent::SubscriptionRowsSkipped(skipped) => {
                if !self.subscriptions.holds(&skipped.subscription) {
                    return ReaderFlow::Continue;
                }
                self.forward(SubscriptionEvent::RowsSkipped(skipped)).await
            }
            wire::ServerEvent::SubscriptionEnded(ended) => {
                if !self.subscriptions.close(&ended.subscription) {
                    return ReaderFlow::Continue;
                }
                self.forward(SubscriptionEvent::Ended(ended)).await
            }
            // No reply follows for any request still in flight; the waiters observe the closed
            // session when the exchange ends.
            wire::ServerEvent::SessionEnding(_) => ReaderFlow::End,
        }
    }

    /// Hands a subscription event to the caller, waiting while its queue is full.
    async fn forward(&self, event: SubscriptionEvent) -> ReaderFlow {
        match self.sinks.subscriptions.send(event).await {
            Ok(()) => ReaderFlow::Continue,
            // The client holds the receiver for as long as it holds the exchange, so the send
            // finds none only once the client is gone, and the exchange ends with it.
            Err(_) => ReaderFlow::End,
        }
    }

    /// Hands a server notice to the caller, waiting while its queue is full.
    async fn notify(&self, notice: ServerEvent) -> ReaderFlow {
        match self.sinks.notices.send(notice).await {
            Ok(()) => ReaderFlow::Continue,
            // As for subscription events, only a client that is gone leaves no receiver.
            Err(_) => ReaderFlow::End,
        }
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
