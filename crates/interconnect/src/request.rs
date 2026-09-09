//! Typed request and response exchange over authenticated interconnect connections.
//!
//! This module owns correlation, deadlines, handler dispatch, response matching, and cancellation
//! when the transport shuts down or the target leaves the live cluster membership.

use std::{
    collections::BTreeSet,
    future::Future,
    hash::RandomState,
    marker::PhantomData,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

use dashmap::{DashMap, mapref::entry::Entry};
use error_stack::Report;
use nervix_models::ClusterNodeName;
use nervix_recovery::NoReceiver as _;
use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{Notify, oneshot},
    time::{Instant, sleep_until},
};
use tracing::debug;
use triomphe::Arc;

use super::{
    ActivateOwnershipHandoffStateRequest, CaptureOwnershipHandoffStateRequest,
    ConfirmOwnershipHandoffStateRequest, ConnectionHandle, ControlEnvelope,
    DescribeIngestorRequest, DiscardOwnershipHandoffStateRequest,
    ForcedOwnershipRecoveryPreparation, IngestorDescribeEnvelope, OwnershipHandoffCheckpoint,
    OwnershipHandoffResponse, PrepareForcedOwnershipRecoveryRequest,
    PrepareOwnershipHandoffStateRequest, Transport, TransportInner, unregister_connected_peer,
};

#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestEnvelope {
    correlation_id: u64,
    request: String,
    payload: Vec<u8>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseEnvelope {
    correlation_id: u64,
    request: String,
    result: Result<Vec<u8>, RemoteRequestFailure>,
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
}

/// The authenticated peer that submitted a request to a registered handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    peer_node_id: ClusterNodeName,
}

impl RequestContext {
    pub fn peer_node_id(&self) -> &ClusterNodeName {
        &self.peer_node_id
    }
}

/// One typed request message and the response type its handler produces.
pub trait InterconnectRequest: Sized + Send + 'static {
    type Response: Send + 'static;

    const NAME: &'static str;
    const TIMEOUT: Duration;

    #[doc(hidden)]
    fn encode_request(&self) -> Result<Vec<u8>, Report<RequestError>>;

    #[doc(hidden)]
    fn decode_request(payload: &[u8]) -> Result<Self, Report<RequestError>>;

    #[doc(hidden)]
    fn encode_response(response: &Self::Response) -> Result<Vec<u8>, Report<RequestError>>;

    #[doc(hidden)]
    fn decode_response(payload: &[u8]) -> Result<Self::Response, Report<RequestError>>;
}

/// Why a typed interconnect request did not produce its declared response.
#[derive(Debug, Error)]
pub enum RequestError {
    #[error("interconnect request correlation id space is exhausted")]
    CorrelationIdExhausted,
    #[error("interconnect request '{request}' has a deadline outside the supported time range")]
    DeadlineOverflow { request: &'static str },
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
    #[error("response channel for request '{request}' on node '{node}' closed")]
    ResponseChannelClosed {
        node: ClusterNodeName,
        request: &'static str,
    },
}

#[derive(Debug, Error)]
pub enum HandlerRegistrationError {
    #[error("a handler for interconnect request '{request}' is already registered")]
    AlreadyRegistered { request: &'static str },
}

pub(super) struct RequestState {
    next_correlation_id: AtomicU64,
    pending: DashMap<u64, PendingRequest, RandomState>,
    handlers: DashMap<&'static str, Arc<Box<dyn ErasedRequestHandler>>, RandomState>,
    live_nodes: DashMap<ClusterNodeName, (), RandomState>,
    live_nodes_observed: AtomicBool,
    connection_changed: Notify,
}

impl Default for RequestState {
    fn default() -> Self {
        Self {
            next_correlation_id: AtomicU64::new(1),
            pending: DashMap::default(),
            handlers: DashMap::default(),
            live_nodes: DashMap::default(),
            live_nodes_observed: AtomicBool::new(false),
            connection_changed: Notify::new(),
        }
    }
}

struct PendingRequest {
    node: ClusterNodeName,
    request: &'static str,
    response: oneshot::Sender<PendingResponse>,
}

enum PendingResponse {
    Response(Result<Vec<u8>, RemoteRequestFailure>),
    ShuttingDown,
    TargetLeft,
}

type HandlerFuture =
    Pin<Box<dyn Future<Output = Result<Vec<u8>, RemoteRequestFailure>> + Send + 'static>>;

trait ErasedRequestHandler: Send + Sync {
    fn handle(&self, context: RequestContext, payload: Vec<u8>) -> HandlerFuture;
}

struct TypedRequestHandler<M, H> {
    handler: H,
    request: PhantomData<fn(M)>,
}

impl<M, H, F> ErasedRequestHandler for TypedRequestHandler<M, H>
where
    M: InterconnectRequest,
    H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
    F: Future<Output = M::Response> + Send + 'static,
{
    fn handle(&self, context: RequestContext, payload: Vec<u8>) -> HandlerFuture {
        let request = match M::decode_request(&payload) {
            Ok(request) => request,
            Err(error) => {
                return Box::pin(async move {
                    Err(RemoteRequestFailure::InvalidPayload(error.to_string()))
                });
            }
        };
        let response = (self.handler)(context, request);
        Box::pin(async move {
            let response = response.await;
            M::encode_response(&response)
                .map_err(|error| RemoteRequestFailure::ResponseEncode(error.to_string()))
        })
    }
}

struct PendingRequestGuard<'transport> {
    state: &'transport RequestState,
    correlation_id: u64,
}

impl Drop for PendingRequestGuard<'_> {
    fn drop(&mut self) {
        self.state.pending.remove(&self.correlation_id);
    }
}

impl RequestState {
    pub(super) fn connection_changed(&self) {
        self.connection_changed.notify_waiters();
    }

    pub(super) fn shutdown(&self) {
        self.handlers.clear();
        let pending_requests = self
            .pending
            .iter()
            .map(|pending| *pending.key())
            .collect::<Vec<_>>();
        for correlation_id in pending_requests {
            if let Some((_, pending)) = self.pending.remove(&correlation_id) {
                pending
                    .response
                    .send(PendingResponse::ShuttingDown)
                    .means_peer_left("interconnect request awaiting a shutting-down transport");
            }
        }
        self.connection_changed.notify_waiters();
    }

    pub(super) fn route_control(
        &self,
        inner: &Arc<TransportInner>,
        peer_node_id: &ClusterNodeName,
        reply: &ConnectionHandle,
        envelope: ControlEnvelope,
    ) -> Option<ControlEnvelope> {
        match envelope {
            ControlEnvelope::Request(request) => {
                self.dispatch_request(inner, peer_node_id, reply, request);
                None
            }
            ControlEnvelope::Response(response) => {
                self.resolve_response(peer_node_id, response);
                None
            }
            envelope => Some(envelope),
        }
    }

    fn dispatch_request(
        &self,
        inner: &Arc<TransportInner>,
        peer_node_id: &ClusterNodeName,
        reply: &ConnectionHandle,
        request: RequestEnvelope,
    ) {
        if inner.admission_closed.is_cancelled() {
            return;
        }
        let handler = self
            .handlers
            .get(request.request.as_str())
            .map(|handler| handler.value().clone());
        let peer_node_id = peer_node_id.clone();
        let reply = reply.clone();
        let force_close = inner.force_close.clone();
        let tasks = inner.tasks.clone();
        tasks.spawn(async move {
            let result = if let Some(handler) = handler {
                tokio::select! {
                    _ = force_close.cancelled() => return,
                    result = handler.handle(
                        RequestContext {
                            peer_node_id: peer_node_id.clone(),
                        },
                        request.payload,
                    ) => result,
                }
            } else {
                Err(RemoteRequestFailure::HandlerNotRegistered)
            };
            let response = ControlEnvelope::Response(ResponseEnvelope {
                correlation_id: request.correlation_id,
                request: request.request,
                result,
            });
            tokio::select! {
                _ = force_close.cancelled() => {}
                result = reply.send(super::Envelope::Control(response)) => {
                    if let Err(error) = result {
                        debug!(%error, %peer_node_id, "failed to return interconnect response");
                    }
                }
            }
        });
    }

    fn resolve_response(&self, peer_node_id: &ClusterNodeName, response: ResponseEnvelope) {
        let Some(pending) = self.pending.get(&response.correlation_id) else {
            debug!(
                correlation_id = response.correlation_id,
                %peer_node_id,
                request = response.request,
                "ignored an interconnect response without a pending request"
            );
            return;
        };
        if &pending.node != peer_node_id || pending.request != response.request {
            debug!(
                correlation_id = response.correlation_id,
                %peer_node_id,
                request = response.request,
                expected_node = %pending.node,
                expected_request = pending.request,
                "ignored an interconnect response that did not match its pending request"
            );
            return;
        }
        drop(pending);
        if let Some((_, pending)) = self.pending.remove(&response.correlation_id) {
            pending
                .response
                .send(PendingResponse::Response(response.result))
                .means_peer_left("interconnect request awaiting this response");
        }
    }

    fn next_correlation_id(&self) -> Result<u64, Report<RequestError>> {
        self.next_correlation_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Report::new(RequestError::CorrelationIdExhausted))
    }

    fn target_is_live(&self, node: &ClusterNodeName) -> bool {
        !self.live_nodes_observed.load(Ordering::Acquire) || self.live_nodes.contains_key(node)
    }

    fn replace_live_nodes(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
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

        let departed_requests = self
            .pending
            .iter()
            .filter(|pending| !live_nodes.contains(&pending.node))
            .map(|pending| *pending.key())
            .collect::<Vec<_>>();
        for correlation_id in departed_requests {
            if let Some((_, pending)) = self.pending.remove(&correlation_id) {
                pending
                    .response
                    .send(PendingResponse::TargetLeft)
                    .means_peer_left("interconnect request awaiting a departed node");
            }
        }
        self.connection_changed.notify_waiters();
    }
}

impl Transport {
    /// Registers the only handler for `M` on this transport.
    pub fn register_handler<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = M::Response> + Send + 'static,
    {
        let erased: Box<dyn ErasedRequestHandler> = Box::new(TypedRequestHandler::<M, H> {
            handler,
            request: PhantomData,
        });
        match self.inner.requests.handlers.entry(M::NAME) {
            Entry::Occupied(_) => Err(Report::new(HandlerRegistrationError::AlreadyRegistered {
                request: M::NAME,
            })),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(erased));
                Ok(())
            }
        }
    }

    /// Replaces the membership snapshot used to cancel requests whose target has left.
    pub fn replace_live_nodes(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
        self.inner.requests.replace_live_nodes(live_nodes);
        self.retire_departed_connections(live_nodes);
    }

    /// Sends `message` to an authenticated cluster node and waits for its associated response.
    pub async fn request<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
    ) -> Result<M::Response, Report<RequestError>>
    where
        M: InterconnectRequest,
    {
        if self.inner.admission_closed.is_cancelled() {
            return Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        let deadline = Instant::now()
            .checked_add(M::TIMEOUT)
            .ok_or_else(|| Report::new(RequestError::DeadlineOverflow { request: M::NAME }))?;
        let payload = message.encode_request()?;
        let correlation_id = self.inner.requests.next_correlation_id()?;
        let envelope = super::Envelope::Control(ControlEnvelope::Request(RequestEnvelope {
            correlation_id,
            request: M::NAME.to_string(),
            payload,
        }));
        let (response, mut response_rx) = oneshot::channel();
        self.inner.requests.pending.insert(
            correlation_id,
            PendingRequest {
                node: node.clone(),
                request: M::NAME,
                response,
            },
        );
        let _pending = PendingRequestGuard {
            state: &self.inner.requests,
            correlation_id,
        };

        loop {
            tokio::task::consume_budget().await;
            if self.inner.admission_closed.is_cancelled() {
                return Err(Report::new(RequestError::ShuttingDown {
                    node: node.clone(),
                    request: M::NAME,
                }));
            }
            if !self.inner.requests.target_is_live(node) {
                return Err(Report::new(RequestError::TargetLeft {
                    node: node.clone(),
                    request: M::NAME,
                }));
            }

            let connection_changed = self.inner.requests.connection_changed.notified();
            let connection = self
                .inner
                .connected_peers
                .get(node)
                .and_then(|connections| connections.values().next().cloned());
            let Some(connection) = connection else {
                tokio::select! {
                    _ = self.inner.admission_closed.cancelled() => {
                        return Err(Report::new(RequestError::ShuttingDown {
                            node: node.clone(),
                            request: M::NAME,
                        }));
                    }
                    _ = sleep_until(deadline) => {
                        return Err(Report::new(RequestError::Timeout {
                            node: node.clone(),
                            request: M::NAME,
                            timeout: M::TIMEOUT,
                        }));
                    }
                    outcome = &mut response_rx => {
                        return Self::finish_request::<M>(node, outcome);
                    }
                    _ = connection_changed => {}
                }
                continue;
            };

            let send_result = tokio::select! {
                _ = self.inner.admission_closed.cancelled() => {
                    return Err(Report::new(RequestError::ShuttingDown {
                        node: node.clone(),
                        request: M::NAME,
                    }));
                }
                _ = sleep_until(deadline) => {
                    return Err(Report::new(RequestError::Timeout {
                        node: node.clone(),
                        request: M::NAME,
                        timeout: M::TIMEOUT,
                    }));
                }
                outcome = &mut response_rx => {
                    return Self::finish_request::<M>(node, outcome);
                }
                _ = connection_changed => None,
                result = connection.send(envelope.clone()) => Some(result),
            };
            match send_result {
                Some(Ok(())) => break,
                Some(Err(super::TransportError::Closed(_))) => {
                    unregister_connected_peer(&self.inner, node, &connection);
                }
                Some(Err(_)) | None => {}
            }
        }

        let outcome = tokio::select! {
            _ = self.inner.admission_closed.cancelled() => {
                return Err(Report::new(RequestError::ShuttingDown {
                    node: node.clone(),
                    request: M::NAME,
                }));
            }
            _ = sleep_until(deadline) => {
                return Err(Report::new(RequestError::Timeout {
                    node: node.clone(),
                    request: M::NAME,
                    timeout: M::TIMEOUT,
                }));
            }
            outcome = &mut response_rx => outcome,
        };
        Self::finish_request::<M>(node, outcome)
    }

    fn finish_request<M>(
        node: &ClusterNodeName,
        outcome: Result<PendingResponse, oneshot::error::RecvError>,
    ) -> Result<M::Response, Report<RequestError>>
    where
        M: InterconnectRequest,
    {
        match outcome {
            Ok(PendingResponse::Response(Ok(payload))) => M::decode_response(&payload),
            Ok(PendingResponse::Response(Err(failure))) => {
                Err(Report::new(RequestError::RemoteRejected {
                    node: node.clone(),
                    request: M::NAME,
                    failure,
                }))
            }
            Ok(PendingResponse::TargetLeft) => Err(Report::new(RequestError::TargetLeft {
                node: node.clone(),
                request: M::NAME,
            })),
            Ok(PendingResponse::ShuttingDown) => Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            })),
            Err(_) => Err(Report::new(RequestError::ResponseChannelClosed {
                node: node.clone(),
                request: M::NAME,
            })),
        }
    }
}

impl InterconnectRequest for DescribeIngestorRequest {
    type Response = Result<IngestorDescribeEnvelope, String>;

    const NAME: &'static str = "describe_ingestor";
    const TIMEOUT: Duration = Duration::from_secs(10);

    fn encode_request(&self) -> Result<Vec<u8>, Report<RequestError>> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|bytes| bytes.to_vec())
            .map_err(|error| {
                Report::new(RequestError::Encode {
                    request: Self::NAME,
                })
                .attach_printable(error.to_string())
            })
    }

    fn decode_request(payload: &[u8]) -> Result<Self, Report<RequestError>> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
        aligned.extend_from_slice(payload);
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(&aligned).map_err(|error| {
            Report::new(RequestError::Decode {
                request: Self::NAME,
            })
            .attach_printable(error.to_string())
        })
    }

    fn encode_response(response: &Self::Response) -> Result<Vec<u8>, Report<RequestError>> {
        rkyv::to_bytes::<rkyv::rancor::Error>(response)
            .map(|bytes| bytes.to_vec())
            .map_err(|error| {
                Report::new(RequestError::Encode {
                    request: Self::NAME,
                })
                .attach_printable(error.to_string())
            })
    }

    fn decode_response(payload: &[u8]) -> Result<Self::Response, Report<RequestError>> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
        aligned.extend_from_slice(payload);
        rkyv::from_bytes::<Result<IngestorDescribeEnvelope, String>, rkyv::rancor::Error>(&aligned)
            .map_err(|error| {
                Report::new(RequestError::Decode {
                    request: Self::NAME,
                })
                .attach_printable(error.to_string())
            })
    }
}

macro_rules! impl_rkyv_request {
    ($request:ty, $response:ty, $name:literal) => {
        impl InterconnectRequest for $request {
            type Response = $response;

            const NAME: &'static str = $name;
            const TIMEOUT: Duration = Duration::from_secs(60);

            fn encode_request(&self) -> Result<Vec<u8>, Report<RequestError>> {
                rkyv::to_bytes::<rkyv::rancor::Error>(self)
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| {
                        Report::new(RequestError::Encode {
                            request: Self::NAME,
                        })
                        .attach_printable(error.to_string())
                    })
            }

            fn decode_request(payload: &[u8]) -> Result<Self, Report<RequestError>> {
                let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
                aligned.extend_from_slice(payload);
                rkyv::from_bytes::<Self, rkyv::rancor::Error>(&aligned).map_err(|error| {
                    Report::new(RequestError::Decode {
                        request: Self::NAME,
                    })
                    .attach_printable(error.to_string())
                })
            }

            fn encode_response(response: &Self::Response) -> Result<Vec<u8>, Report<RequestError>> {
                rkyv::to_bytes::<rkyv::rancor::Error>(response)
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| {
                        Report::new(RequestError::Encode {
                            request: Self::NAME,
                        })
                        .attach_printable(error.to_string())
                    })
            }

            fn decode_response(payload: &[u8]) -> Result<Self::Response, Report<RequestError>> {
                let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
                aligned.extend_from_slice(payload);
                rkyv::from_bytes::<Self::Response, rkyv::rancor::Error>(&aligned).map_err(|error| {
                    Report::new(RequestError::Decode {
                        request: Self::NAME,
                    })
                    .attach_printable(error.to_string())
                })
            }
        }
    };
}

impl_rkyv_request!(
    CaptureOwnershipHandoffStateRequest,
    OwnershipHandoffResponse<Vec<OwnershipHandoffCheckpoint>>,
    "capture_ownership_handoff_state"
);
impl_rkyv_request!(
    PrepareOwnershipHandoffStateRequest,
    OwnershipHandoffResponse<()>,
    "prepare_ownership_handoff_state"
);
impl_rkyv_request!(
    ConfirmOwnershipHandoffStateRequest,
    OwnershipHandoffResponse<()>,
    "confirm_ownership_handoff_state"
);
impl_rkyv_request!(
    PrepareForcedOwnershipRecoveryRequest,
    OwnershipHandoffResponse<ForcedOwnershipRecoveryPreparation>,
    "prepare_forced_ownership_recovery"
);
impl_rkyv_request!(
    ActivateOwnershipHandoffStateRequest,
    OwnershipHandoffResponse<()>,
    "activate_ownership_handoff_state"
);
impl_rkyv_request!(
    DiscardOwnershipHandoffStateRequest,
    OwnershipHandoffResponse<()>,
    "discard_ownership_handoff_state"
);
