//! Typed request and response exchange over authenticated interconnect streams.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Deadlines, reserved request quotas, handler dispatch, response validation, and
//!   membership cancellation for typed internal requests.
//! - **Depends on.** The authenticated HTTP/2 transport and rkyv payload vocabulary.
//! - **Must not know.** The runtime meaning of a request or response.

use std::{
    collections::BTreeSet,
    future::Future,
    hash::RandomState,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use dashmap::{DashMap, mapref::entry::Entry};
use error_stack::Report;
use futures_util::Stream;
use meticulous::ResultExt as _;
use nervix_execution::{BudgetedBuffer, ChargedBytes, Executor, Reservation};
use nervix_models::ClusterNodeName;
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighDeserializer, HighSerializer},
    rancor::Error as RkyvError,
    ser::{allocator::ArenaHandle, writer::IoWriter},
};
use thiserror::Error;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};
use triomphe::Arc;

use super::{
    ActivateOwnershipHandoffStateRequest, CaptureOwnershipHandoffStateRequest,
    ConfirmOwnershipHandoffStateRequest, ControlEnvelope, DescribeIngestorRequest,
    DiscardOwnershipHandoffStateRequest, ForcedOwnershipRecoveryPreparation,
    IngestorDescribeEnvelope, OwnershipHandoffCheckpoint, OwnershipHandoffResponse, PoolClass,
    PrepareForcedOwnershipRecoveryRequest, PrepareOwnershipHandoffStateRequest, Transport,
    TransportError, wire,
};
use crate::connection::OutboundByteStreamRequest;

#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestEnvelope {
    pub(crate) class: PoolClass,
    pub(crate) request: String,
    pub(crate) payload: Vec<u8>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseEnvelope {
    pub(crate) class: PoolClass,
    pub(crate) request: String,
    pub(crate) result: Result<Vec<u8>, RemoteRequestFailure>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq, Error)]
pub enum RemoteRequestFailure {
    #[error("no handler is registered")]
    HandlerNotRegistered,
    #[error("request payload is invalid: {0}")]
    InvalidPayload(String),
    #[error("response payload could not be encoded: {0}")]
    ResponseEncode(String),
    #[error("request used {actual:?} but its registered handler requires {expected:?}")]
    WrongPoolClass {
        expected: PoolClass,
        actual: PoolClass,
    },
    #[error("request payload contains {actual} bytes, exceeding the {limit}-byte class limit")]
    PayloadTooLarge { actual: u64, limit: u64 },
    #[error("the {subquota:?} request subquota is full")]
    AdmissionFull { subquota: RequestSubquota },
}

/// The authenticated peer that submitted a request to a registered handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    peer_node_id: ClusterNodeName,
    peer_advertised_host: String,
}

impl RequestContext {
    pub fn peer_node_id(&self) -> &ClusterNodeName {
        &self.peer_node_id
    }

    /// The certificate-validated host named by the peer's connection hello.
    pub fn peer_advertised_host(&self) -> &str {
        &self.peer_advertised_host
    }
}

/// A value that the transport can validate and encode as bounded rkyv.
#[doc(hidden)]
pub trait RkyvMessage: Archive + Sized + Send + 'static {
    fn encode_rkyv(
        self,
        executor: Executor,
        class: PoolClass,
        limit: u64,
    ) -> impl Future<Output = Result<(Vec<u8>, Reservation), Report<TransportError>>> + Send;

    fn decode_rkyv(
        executor: Executor,
        class: PoolClass,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(Self, Reservation), Report<TransportError>>> + Send;
}

impl<T> RkyvMessage for T
where
    T: Archive
        + Sized
        + Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + Deserialize<T, HighDeserializer<RkyvError>>,
{
    async fn encode_rkyv(
        self,
        executor: Executor,
        class: PoolClass,
        limit: u64,
    ) -> Result<(Vec<u8>, Reservation), Report<TransportError>> {
        wire::encode_rkyv_payload(
            &executor,
            class.memory_class(),
            class.cpu_class(),
            limit,
            self,
        )
        .await
        .map(wire::EncodedPayload::into_parts)
        .map_err(Report::new)
    }

    async fn decode_rkyv(
        executor: Executor,
        class: PoolClass,
        payload: Vec<u8>,
    ) -> Result<(Self, Reservation), Report<TransportError>> {
        wire::decode_rkyv_payload::<Self>(
            &executor,
            class.memory_class(),
            class.cpu_class(),
            payload,
        )
        .await
        .map(wire::Decoded::into_parts)
        .map_err(Report::new)
    }
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum RequestSubquota {
    Shared,
    Resource,
    Snapshot,
    Discovery,
    Liveness,
    Progress,
    Admission,
    Cancellation,
    Terminal,
}

/// One typed request message and the response type its handler produces.
pub trait InterconnectRequest: RkyvMessage {
    type Response: RkyvMessage;

    const NAME: &'static str;
    const CLASS: PoolClass = PoolClass::Commands;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    const TIMEOUT: Duration;

    /// Discovery requests are permitted before the first live-membership view exists.
    const REQUIRES_LIVE_TARGET: bool = true;
}

/// A typed request whose response is an incrementally flow-controlled byte stream.
pub trait InterconnectStreamRequest: RkyvMessage {
    const NAME: &'static str;
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    const TIMEOUT: Duration;
    const REQUIRES_LIVE_TARGET: bool = true;
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct StreamHandlerError {
    message: String,
}

impl StreamHandlerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub type OutgoingByteStream =
    Pin<Box<dyn Stream<Item = Result<ChargedBytes, StreamHandlerError>> + Send + 'static>>;

/// A producer-owned stream with its exact byte count declared before response headers are sent.
pub struct StreamingResponse {
    pub(crate) content_length: u64,
    pub(crate) chunks: OutgoingByteStream,
}

impl StreamingResponse {
    pub fn new<S>(content_length: u64, chunks: S) -> Self
    where
        S: Stream<Item = Result<ChargedBytes, StreamHandlerError>> + Send + 'static,
    {
        Self {
            content_length,
            chunks: Box::pin(chunks),
        }
    }
}

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("the {subquota:?} request subquota is full for '{request}'")]
    AdmissionFull {
        request: &'static str,
        subquota: RequestSubquota,
    },
    #[error("failed to encode interconnect request '{request}'")]
    Encode { request: &'static str },
    #[error("failed to decode the response for interconnect request '{request}'")]
    Decode { request: &'static str },
    #[error("transport shut down while requesting '{request}' from node '{node}'")]
    ShuttingDown {
        node: ClusterNodeName,
        request: &'static str,
    },
    #[error("target node '{node}' left the cluster while handling request '{request}'")]
    TargetLeft {
        node: ClusterNodeName,
        request: &'static str,
    },
    #[error("timed out after {timeout:?} waiting for request '{request}' on node '{node}'")]
    Timeout {
        node: ClusterNodeName,
        request: &'static str,
        timeout: Duration,
    },
    #[error("node '{node}' rejected interconnect request '{request}': {failure}")]
    RemoteRejected {
        node: ClusterNodeName,
        request: &'static str,
        failure: RemoteRequestFailure,
    },
    #[error("transport failed request '{request}' to node '{node}': {reason}")]
    Transport {
        node: ClusterNodeName,
        request: &'static str,
        reason: String,
    },
    #[error("node '{node}' returned the wrong response for request '{request}'")]
    ResponseMismatch {
        node: ClusterNodeName,
        request: &'static str,
    },
    #[error("streamed response for '{request}' from node '{node}' failed: {reason}")]
    Stream {
        node: ClusterNodeName,
        request: &'static str,
        reason: String,
    },
    #[error(
        "streamed response for '{request}' from node '{node}' declared {declared} bytes but \
         delivered {received}"
    )]
    StreamLengthMismatch {
        node: ClusterNodeName,
        request: &'static str,
        declared: u64,
        received: u64,
    },
}

#[derive(Debug, Error)]
pub enum HandlerRegistrationError {
    #[error("a handler for interconnect request '{request}' is already registered")]
    AlreadyRegistered { request: &'static str },
    #[error("{subquota:?} request '{request}' does not belong to its configured pool")]
    SubquotaClassMismatch {
        request: &'static str,
        subquota: RequestSubquota,
    },
}

pub(crate) struct HandledResponse {
    envelope: ResponseEnvelope,
    payload_reservation: Option<Reservation>,
}

impl HandledResponse {
    pub(crate) fn into_parts(self) -> (ResponseEnvelope, Option<Reservation>) {
        (self.envelope, self.payload_reservation)
    }
}

pub(crate) struct RequestState {
    handlers: DashMap<&'static str, Arc<Box<dyn ErasedRequestHandler>>, RandomState>,
    stream_handlers: DashMap<&'static str, Arc<Box<dyn ErasedStreamHandler>>, RandomState>,
    live_nodes: DashMap<ClusterNodeName, (), RandomState>,
    live_nodes_observed: AtomicBool,
    membership_changed: Notify,
    outbound: RequestQuotas,
    inbound: RequestQuotas,
}

pub(crate) struct RequestAdmission {
    _permit: OwnedSemaphorePermit,
}

struct RequestQuotas {
    shared: StdArc<Semaphore>,
    resource: StdArc<Semaphore>,
    snapshot: StdArc<Semaphore>,
    discovery: StdArc<Semaphore>,
    liveness: StdArc<Semaphore>,
    progress: StdArc<Semaphore>,
    admission: StdArc<Semaphore>,
    cancellation: StdArc<Semaphore>,
    terminal: StdArc<Semaphore>,
}

// Per-node admission limits for reserved request classes. These cap requests across all peers and
// connections on one node, separately for inbound and outbound flow. They are independent policy
// inputs from the per-connection HTTP/2 stream partition owned by `connection`.
const PER_NODE_RESERVED_REQUEST_MINIMUM: usize = 1;
const PER_NODE_STANDARD_REQUEST_LIMIT: usize = 8;
const PER_NODE_COMPLETION_REQUEST_LIMIT: usize = 4;
const _: () = assert!(
    PER_NODE_RESERVED_REQUEST_MINIMUM > 0
        && PER_NODE_STANDARD_REQUEST_LIMIT > 0
        && PER_NODE_COMPLETION_REQUEST_LIMIT > 0,
    "per-node request policy inputs must be nonzero",
);
const _: () = assert!(
    PER_NODE_STANDARD_REQUEST_LIMIT >= PER_NODE_RESERVED_REQUEST_MINIMUM
        && PER_NODE_COMPLETION_REQUEST_LIMIT >= PER_NODE_RESERVED_REQUEST_MINIMUM,
    "each per-node request limit must include the reserved minimum",
);

impl RequestQuotas {
    fn new(capacity: usize) -> Self {
        Self {
            shared: StdArc::new(Semaphore::new(capacity)),
            resource: StdArc::new(Semaphore::new(capacity.clamp(
                PER_NODE_RESERVED_REQUEST_MINIMUM,
                PER_NODE_STANDARD_REQUEST_LIMIT,
            ))),
            snapshot: StdArc::new(Semaphore::new(capacity.clamp(
                PER_NODE_RESERVED_REQUEST_MINIMUM,
                PER_NODE_STANDARD_REQUEST_LIMIT,
            ))),
            discovery: StdArc::new(Semaphore::new(capacity.clamp(
                PER_NODE_RESERVED_REQUEST_MINIMUM,
                PER_NODE_STANDARD_REQUEST_LIMIT,
            ))),
            liveness: StdArc::new(Semaphore::new(capacity.clamp(
                PER_NODE_RESERVED_REQUEST_MINIMUM,
                PER_NODE_STANDARD_REQUEST_LIMIT,
            ))),
            progress: StdArc::new(Semaphore::new(capacity.clamp(
                PER_NODE_RESERVED_REQUEST_MINIMUM,
                PER_NODE_STANDARD_REQUEST_LIMIT,
            ))),
            admission: StdArc::new(Semaphore::new(capacity.clamp(
                PER_NODE_RESERVED_REQUEST_MINIMUM,
                PER_NODE_STANDARD_REQUEST_LIMIT,
            ))),
            cancellation: StdArc::new(Semaphore::new(capacity.clamp(
                PER_NODE_RESERVED_REQUEST_MINIMUM,
                PER_NODE_COMPLETION_REQUEST_LIMIT,
            ))),
            terminal: StdArc::new(Semaphore::new(capacity.clamp(
                PER_NODE_RESERVED_REQUEST_MINIMUM,
                PER_NODE_COMPLETION_REQUEST_LIMIT,
            ))),
        }
    }

    fn for_subquota(&self, subquota: RequestSubquota) -> &StdArc<Semaphore> {
        match subquota {
            RequestSubquota::Shared => &self.shared,
            RequestSubquota::Resource => &self.resource,
            RequestSubquota::Snapshot => &self.snapshot,
            RequestSubquota::Discovery => &self.discovery,
            RequestSubquota::Liveness => &self.liveness,
            RequestSubquota::Progress => &self.progress,
            RequestSubquota::Admission => &self.admission,
            RequestSubquota::Cancellation => &self.cancellation,
            RequestSubquota::Terminal => &self.terminal,
        }
    }

    fn try_admit(&self, subquota: RequestSubquota) -> Option<RequestAdmission> {
        let permit = StdArc::clone(self.for_subquota(subquota))
            .try_acquire_owned()
            .ok()?;
        Some(RequestAdmission { _permit: permit })
    }
}

fn subquota_belongs_to_class(subquota: RequestSubquota, class: PoolClass) -> bool {
    match subquota {
        RequestSubquota::Shared => true,
        RequestSubquota::Resource | RequestSubquota::Snapshot => class == PoolClass::Bulk,
        RequestSubquota::Discovery
        | RequestSubquota::Liveness
        | RequestSubquota::Progress
        | RequestSubquota::Admission
        | RequestSubquota::Cancellation
        | RequestSubquota::Terminal => class == PoolClass::Management,
    }
}

impl RequestState {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            handlers: DashMap::default(),
            stream_handlers: DashMap::default(),
            live_nodes: DashMap::default(),
            live_nodes_observed: AtomicBool::new(false),
            membership_changed: Notify::new(),
            outbound: RequestQuotas::new(capacity),
            inbound: RequestQuotas::new(capacity),
        }
    }

    pub(crate) fn try_admit_outbound(&self, subquota: RequestSubquota) -> Option<RequestAdmission> {
        self.outbound.try_admit(subquota)
    }

    fn try_admit_inbound(&self, subquota: RequestSubquota) -> Option<RequestAdmission> {
        self.inbound.try_admit(subquota)
    }
}

type HandlerFuture = Pin<
    Box<dyn Future<Output = Result<(Vec<u8>, Reservation), RemoteRequestFailure>> + Send + 'static>,
>;

type StreamHandlerFuture =
    Pin<Box<dyn Future<Output = Result<StreamingResponse, RemoteRequestFailure>> + Send + 'static>>;

trait ErasedRequestHandler: Send + Sync {
    fn class(&self) -> PoolClass;
    fn subquota(&self) -> RequestSubquota;

    fn handle(
        &self,
        executor: Executor,
        payload_limit: u64,
        context: RequestContext,
        payload: Vec<u8>,
    ) -> HandlerFuture;
}

trait ErasedStreamHandler: Send + Sync {
    fn class(&self) -> PoolClass;
    fn subquota(&self) -> RequestSubquota;

    fn handle(
        &self,
        executor: Executor,
        context: RequestContext,
        payload: Vec<u8>,
    ) -> StreamHandlerFuture;
}

struct TypedRequestHandler<M, H> {
    handler: Arc<H>,
    request: PhantomData<fn(M)>,
}

struct TypedStreamHandler<M, H> {
    handler: Arc<H>,
    request: PhantomData<fn(M)>,
}

impl<M, H, F> ErasedStreamHandler for TypedStreamHandler<M, H>
where
    M: InterconnectStreamRequest,
    H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
    F: Future<Output = Result<StreamingResponse, StreamHandlerError>> + Send + 'static,
{
    fn class(&self) -> PoolClass {
        M::CLASS
    }

    fn subquota(&self) -> RequestSubquota {
        M::SUBQUOTA
    }

    fn handle(
        &self,
        executor: Executor,
        context: RequestContext,
        payload: Vec<u8>,
    ) -> StreamHandlerFuture {
        let handler = self.handler.clone();
        Box::pin(async move {
            let (request, _request_reservation) = M::decode_rkyv(executor, M::CLASS, payload)
                .await
                .map_err(|error| RemoteRequestFailure::InvalidPayload(error.to_string()))?;
            (handler)(context, request)
                .await
                .map_err(|error| RemoteRequestFailure::ResponseEncode(error.to_string()))
        })
    }
}

pub(crate) struct HandledByteStream {
    response: StreamingResponse,
    admission: RequestAdmission,
}

impl HandledByteStream {
    pub(crate) fn into_parts(self) -> (StreamingResponse, RequestAdmission) {
        (self.response, self.admission)
    }
}

impl<M, H, F> ErasedRequestHandler for TypedRequestHandler<M, H>
where
    M: InterconnectRequest,
    H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
    F: Future<Output = M::Response> + Send + 'static,
{
    fn class(&self) -> PoolClass {
        M::CLASS
    }

    fn subquota(&self) -> RequestSubquota {
        M::SUBQUOTA
    }

    fn handle(
        &self,
        executor: Executor,
        payload_limit: u64,
        context: RequestContext,
        payload: Vec<u8>,
    ) -> HandlerFuture {
        let handler = self.handler.clone();
        Box::pin(async move {
            let (request, _request_reservation) =
                M::decode_rkyv(executor.clone(), M::CLASS, payload)
                    .await
                    .map_err(|error| RemoteRequestFailure::InvalidPayload(error.to_string()))?;
            let response = (handler)(context, request).await;
            response
                .encode_rkyv(executor, M::CLASS, payload_limit)
                .await
                .map_err(|error| RemoteRequestFailure::ResponseEncode(error.to_string()))
        })
    }
}

impl RequestState {
    pub(crate) fn register<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = M::Response> + Send + 'static,
    {
        if !subquota_belongs_to_class(M::SUBQUOTA, M::CLASS) {
            return Err(Report::new(
                HandlerRegistrationError::SubquotaClassMismatch {
                    request: M::NAME,
                    subquota: M::SUBQUOTA,
                },
            ));
        }
        let erased: Box<dyn ErasedRequestHandler> = Box::new(TypedRequestHandler::<M, H> {
            handler: Arc::new(handler),
            request: PhantomData,
        });
        match self.handlers.entry(M::NAME) {
            Entry::Occupied(_) => Err(Report::new(HandlerRegistrationError::AlreadyRegistered {
                request: M::NAME,
            })),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(erased));
                Ok(())
            }
        }
    }

    pub(crate) fn register_stream<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectStreamRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = Result<StreamingResponse, StreamHandlerError>> + Send + 'static,
    {
        if !subquota_belongs_to_class(M::SUBQUOTA, M::CLASS) {
            return Err(Report::new(
                HandlerRegistrationError::SubquotaClassMismatch {
                    request: M::NAME,
                    subquota: M::SUBQUOTA,
                },
            ));
        }
        if self.handlers.contains_key(M::NAME) {
            return Err(Report::new(HandlerRegistrationError::AlreadyRegistered {
                request: M::NAME,
            }));
        }
        let erased: Box<dyn ErasedStreamHandler> = Box::new(TypedStreamHandler::<M, H> {
            handler: Arc::new(handler),
            request: PhantomData,
        });
        match self.stream_handlers.entry(M::NAME) {
            Entry::Occupied(_) => Err(Report::new(HandlerRegistrationError::AlreadyRegistered {
                request: M::NAME,
            })),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(erased));
                Ok(())
            }
        }
    }

    pub(crate) async fn handle_stream(
        &self,
        executor: &Executor,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        request: RequestEnvelope,
    ) -> Result<HandledByteStream, RemoteRequestFailure> {
        let handler = self
            .stream_handlers
            .get(request.request.as_str())
            .map(|handler| handler.value().clone());
        let payload_limit = request.class.payload_limit(executor);
        let payload_bytes = u64::try_from(request.payload.len())
            .assured("supported targets have a pointer width no larger than u64");
        let handler = match handler {
            Some(_) if payload_bytes > payload_limit => {
                return Err(RemoteRequestFailure::PayloadTooLarge {
                    actual: payload_bytes,
                    limit: payload_limit,
                });
            }
            Some(handler) if handler.class() == request.class => handler,
            Some(handler) => {
                return Err(RemoteRequestFailure::WrongPoolClass {
                    expected: handler.class(),
                    actual: request.class,
                });
            }
            None => return Err(RemoteRequestFailure::HandlerNotRegistered),
        };
        let subquota = handler.subquota();
        let admission = self
            .try_admit_inbound(subquota)
            .ok_or(RemoteRequestFailure::AdmissionFull { subquota })?;
        let response = handler
            .handle(
                executor.clone(),
                RequestContext {
                    peer_node_id,
                    peer_advertised_host,
                },
                request.payload,
            )
            .await?;
        Ok(HandledByteStream {
            response,
            admission,
        })
    }

    pub(crate) async fn handle(
        &self,
        executor: &Executor,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        request: RequestEnvelope,
    ) -> HandledResponse {
        let handler = self
            .handlers
            .get(request.request.as_str())
            .map(|handler| handler.value().clone());
        let payload_limit = request.class.payload_limit(executor);
        let payload_bytes = u64::try_from(request.payload.len())
            .assured("supported targets have a pointer width no larger than u64");
        let result = match handler {
            Some(_) if payload_bytes > payload_limit => {
                Err(RemoteRequestFailure::PayloadTooLarge {
                    actual: payload_bytes,
                    limit: payload_limit,
                })
            }
            Some(handler) if handler.class() == request.class => {
                let subquota = handler.subquota();
                match self.try_admit_inbound(subquota) {
                    Some(_admission) => {
                        handler
                            .handle(
                                executor.clone(),
                                payload_limit,
                                RequestContext {
                                    peer_node_id,
                                    peer_advertised_host,
                                },
                                request.payload,
                            )
                            .await
                    }
                    None => Err(RemoteRequestFailure::AdmissionFull { subquota }),
                }
            }
            Some(handler) => Err(RemoteRequestFailure::WrongPoolClass {
                expected: handler.class(),
                actual: request.class,
            }),
            None => Err(RemoteRequestFailure::HandlerNotRegistered),
        };
        let (result, payload_reservation) = match result {
            Ok((payload, reservation)) => (Ok(payload), Some(reservation)),
            Err(error) => (Err(error), None),
        };
        HandledResponse {
            envelope: ResponseEnvelope {
                class: request.class,
                request: request.request,
                result,
            },
            payload_reservation,
        }
    }

    pub(crate) fn target_is_live(&self, node: &ClusterNodeName) -> bool {
        !self.live_nodes_observed.load(Ordering::Acquire) || self.live_nodes.contains_key(node)
    }

    pub(crate) fn replace_live_nodes(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
        for node in live_nodes {
            self.live_nodes.insert(node.clone(), ());
        }
        let departed = self
            .live_nodes
            .iter()
            .filter(|node| !live_nodes.contains(node.key()))
            .map(|node| node.key().clone())
            .collect::<Vec<_>>();
        for node in departed {
            self.live_nodes.remove(&node);
        }
        self.live_nodes_observed.store(true, Ordering::Release);
        self.membership_changed.notify_waiters();
    }

    async fn target_left(&self, node: &ClusterNodeName) {
        loop {
            tokio::task::consume_budget().await;
            let changed = self.membership_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.target_is_live(node) {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn shutdown(&self) {
        self.handlers.clear();
        self.stream_handlers.clear();
        self.membership_changed.notify_waiters();
    }
}

impl Transport {
    pub fn register_handler<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = M::Response> + Send + 'static,
    {
        self.inner.requests().register::<M, H, F>(handler)
    }

    pub fn register_stream_handler<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectStreamRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = Result<StreamingResponse, StreamHandlerError>> + Send + 'static,
    {
        self.inner.requests().register_stream::<M, H, F>(handler)
    }

    pub fn replace_live_nodes(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
        self.inner.requests().replace_live_nodes(live_nodes);
        self.inner.retire_departed_connections(live_nodes);
    }

    pub async fn request<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
    ) -> Result<M::Response, Report<RequestError>>
    where
        M: InterconnectRequest,
    {
        self.request_with_timeout(node, message, M::TIMEOUT).await
    }

    pub async fn request_stream<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
    ) -> Result<super::IncomingByteStream, Report<RequestError>>
    where
        M: InterconnectStreamRequest,
    {
        if self.inner.is_shutting_down() {
            return Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        if M::REQUIRES_LIVE_TARGET && !self.inner.requests().target_is_live(node) {
            return Err(Report::new(RequestError::TargetLeft {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        let admission = self
            .inner
            .requests()
            .try_admit_outbound(M::SUBQUOTA)
            .ok_or_else(|| {
                Report::new(RequestError::AdmissionFull {
                    request: M::NAME,
                    subquota: M::SUBQUOTA,
                })
            })?;
        let (payload, _payload_reservation) = message
            .encode_rkyv(
                self.inner.executor().clone(),
                M::CLASS,
                M::CLASS.payload_limit(self.inner.executor()),
            )
            .await
            .map_err(|error| {
                Report::new(RequestError::Encode { request: M::NAME }).attach_printable(error)
            })?;
        let request = RequestEnvelope {
            class: M::CLASS,
            request: M::NAME.to_string(),
            payload,
        };
        let body = wire::encode_rkyv(
            self.inner.executor(),
            M::CLASS.memory_class(),
            M::CLASS.cpu_class(),
            M::CLASS.control_body_limit(self.inner.executor()),
            request,
        )
        .await
        .map_err(|error| {
            Report::new(RequestError::Encode { request: M::NAME }).attach_printable(error)
        })?;
        self.inner
            .open_byte_stream(
                node,
                OutboundByteStreamRequest {
                    class: M::CLASS,
                    subquota: M::SUBQUOTA,
                    name: M::NAME,
                    body,
                    timeout: M::TIMEOUT,
                    admission,
                },
            )
            .await
            .map_err(|error| {
                Report::new(RequestError::Stream {
                    node: node.clone(),
                    request: M::NAME,
                    reason: error.to_string(),
                })
            })
    }

    /// Send one typed request with a caller-owned end-to-end deadline.
    pub async fn request_with_timeout<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
        timeout_duration: Duration,
    ) -> Result<M::Response, Report<RequestError>>
    where
        M: InterconnectRequest,
    {
        if self.inner.is_shutting_down() {
            return Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        if M::REQUIRES_LIVE_TARGET && !self.inner.requests().target_is_live(node) {
            return Err(Report::new(RequestError::TargetLeft {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        let operation = async {
            let _admission = self
                .inner
                .requests()
                .try_admit_outbound(M::SUBQUOTA)
                .ok_or_else(|| {
                    Report::new(RequestError::AdmissionFull {
                        request: M::NAME,
                        subquota: M::SUBQUOTA,
                    })
                })?;
            let (payload, _payload_reservation) = message
                .encode_rkyv(
                    self.inner.executor().clone(),
                    M::CLASS,
                    M::CLASS.payload_limit(self.inner.executor()),
                )
                .await
                .map_err(|error| {
                    Report::new(RequestError::Encode { request: M::NAME }).attach_printable(error)
                })?;
            let request = ControlEnvelope::Request(RequestEnvelope {
                class: M::CLASS,
                request: M::NAME.to_string(),
                payload,
            });
            let response = self
                .inner
                .round_trip_control(node, request, M::SUBQUOTA, timeout_duration)
                .await
                .map_err(|error| match error {
                    super::TransportError::ShuttingDown => {
                        Report::new(RequestError::ShuttingDown {
                            node: node.clone(),
                            request: M::NAME,
                        })
                    }
                    super::TransportError::RequestTimeout { .. }
                    | super::TransportError::ProgressTimeout { .. } => {
                        Report::new(RequestError::Timeout {
                            node: node.clone(),
                            request: M::NAME,
                            timeout: timeout_duration,
                        })
                    }
                    error => Report::new(RequestError::Transport {
                        node: node.clone(),
                        request: M::NAME,
                        reason: error.to_string(),
                    }),
                })?;
            let (response, _response_reservation) = response.into_parts();
            let ControlEnvelope::Response(response) = response else {
                return Err(Report::new(RequestError::ResponseMismatch {
                    node: node.clone(),
                    request: M::NAME,
                }));
            };
            if response.class != M::CLASS || response.request != M::NAME {
                return Err(Report::new(RequestError::ResponseMismatch {
                    node: node.clone(),
                    request: M::NAME,
                }));
            }
            match response.result {
                Ok(payload) => {
                    M::Response::decode_rkyv(self.inner.executor().clone(), M::CLASS, payload)
                        .await
                        .map(|(response, _response_payload_reservation)| response)
                        .map_err(|error| {
                            Report::new(RequestError::Decode { request: M::NAME })
                                .attach_printable(error)
                        })
                }
                Err(failure) => Err(Report::new(RequestError::RemoteRejected {
                    node: node.clone(),
                    request: M::NAME,
                    failure,
                })),
            }
        };
        let shutdown = self.inner.shutdown_token();
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            })),
            _ = self.inner.requests().target_left(node), if M::REQUIRES_LIVE_TARGET => {
                Err(Report::new(RequestError::TargetLeft {
                    node: node.clone(),
                    request: M::NAME,
                }))
            }
            result = timeout(timeout_duration, operation) => match result {
                Ok(result) => result,
                Err(_) => Err(Report::new(RequestError::Timeout {
                    node: node.clone(),
                    request: M::NAME,
                    timeout: timeout_duration,
                })),
            },
        }
    }
}

impl InterconnectRequest for DescribeIngestorRequest {
    type Response = Result<IngestorDescribeEnvelope, String>;

    const NAME: &'static str = "describe_ingestor";
    const TIMEOUT: Duration = Duration::from_secs(10);
}

impl InterconnectRequest for CaptureOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<Vec<OwnershipHandoffCheckpoint>>;

    const NAME: &'static str = "capture_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(60);
}

impl InterconnectRequest for PrepareOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "prepare_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(60);
}

impl InterconnectRequest for ConfirmOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "confirm_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(60);
}

impl InterconnectRequest for PrepareForcedOwnershipRecoveryRequest {
    type Response = OwnershipHandoffResponse<ForcedOwnershipRecoveryPreparation>;

    const NAME: &'static str = "prepare_forced_ownership_recovery";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(60);
}

impl InterconnectRequest for ActivateOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "activate_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(60);
}

impl InterconnectRequest for DiscardOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "discard_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(60);
}
