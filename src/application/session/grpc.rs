//! The session over gRPC.
//!
//! Layer: edges.
//!
//! - **Owns.** The `nervix.session.Session` gRPC service: routing its two methods, authenticating
//!   each call from its metadata before any frame is read, holding every message to the session
//!   frame limit, and ending a call with the status of the transport failure that ended it.
//! - **Depends on.** The session engine and upload handling, the client wire gRPC codecs, and
//!   tonic's server primitives.
//! - **Must not know.** What any request does.
//!
//! A call that fails authentication ends with `UNAUTHENTICATED`. A message above the frame limit
//! ends the call with `OUT_OF_RANGE`, the status tonic refuses it with before buffering it, and a
//! message that is not a valid frame with `INTERNAL`, the status the frame codec assigns it.
//! Everything a well-formed frame can get wrong is answered within the session instead, as a typed
//! reply to the request it names, except a frame without a request identity, which breaks the
//! protocol itself and ends the session with a `SessionEnding` that says so.

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Stream, StreamExt as _, stream};
use nervix_client_wire::{
    ClientFrame, EncodedFrame, ServerFrame, SessionLimits, UploadFrame, UploadReplyFrame,
    VerifiedFrame,
    grpc::{
        EXCHANGE_PATH, SERVICE_NAME, ServerExchangeCodec, ServerUploadCodec, UPLOAD_RESOURCE_PATH,
    },
};
use nervix_recovery::Discarded as _;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{ClientStreamingService, Grpc, NamedService, StreamingService},
};

use super::{InboundFrame, SESSION_OUTBOUND_CAPACITY, SessionTransport};
use crate::application::session_service::SessionServiceImpl;

/// The session service a gRPC server hosts.
#[derive(Clone)]
pub(in crate::application) struct SessionGrpcService {
    service: SessionServiceImpl,
    limits: SessionLimits,
}

impl SessionGrpcService {
    pub(in crate::application) fn new(service: SessionServiceImpl) -> Self {
        Self {
            service,
            limits: SessionLimits::DEFAULT,
        }
    }
}

impl NamedService for SessionGrpcService {
    const NAME: &'static str = SERVICE_NAME;
}

impl Service<http::Request<Body>> for SessionGrpcService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let service = self.service.clone();
        let limits = self.limits;
        match request.uri().path() {
            EXCHANGE_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(ServerExchangeCodec::new(limits))
                    .max_decoding_message_size(limits.frame_bytes())
                    .max_encoding_message_size(limits.frame_bytes());
                let exchange = Exchange { service, limits };
                Ok(grpc.streaming(exchange, request).await)
            }),
            UPLOAD_RESOURCE_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(ServerUploadCodec::new(limits))
                    .max_decoding_message_size(limits.frame_bytes())
                    .max_encoding_message_size(limits.frame_bytes());
                let upload = Upload { service, limits };
                Ok(grpc.client_streaming(upload, request).await)
            }),
            _ => Box::pin(async move {
                let status = Status::unimplemented("the session service serves two methods");
                Ok(status.into_http())
            }),
        }
    }
}

/// One call of the session exchange.
struct Exchange {
    service: SessionServiceImpl,
    limits: SessionLimits,
}

type ExchangeStream =
    Pin<Box<dyn Stream<Item = Result<EncodedFrame<ServerFrame>, Status>> + Send + 'static>>;

impl StreamingService<VerifiedFrame<ClientFrame>> for Exchange {
    type Response = EncodedFrame<ServerFrame>;
    type ResponseStream = ExchangeStream;
    type Future = BoxFuture<Response<ExchangeStream>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<ClientFrame>>>) -> Self::Future {
        let service = self.service.clone();
        let limits = self.limits;
        Box::pin(async move {
            let user = service
                .authenticate_grpc_metadata(request.metadata())
                .await?;
            let (failure_sender, failure) = oneshot::channel();
            let mut failure_report = FailureReport {
                sender: Some(failure_sender),
            };
            let inbound = request
                .into_inner()
                .map(move |item| failure_report.inbound_frame(item));
            let (outbound, frames) = mpsc::channel(SESSION_OUTBOUND_CAPACITY);
            let session_service = service.clone();
            service.inner.service_tasks.spawn(async move {
                session_service
                    .run_session(user, SessionTransport::Grpc, limits, inbound, outbound)
                    .await;
            });
            // The frames end once the session and every task it started have dropped their
            // senders, after everything they queued was sent. The status of the transport failure
            // that ended the session, if one did, closes the call.
            let responses = ReceiverStream::new(frames)
                .map(Ok)
                .chain(stream::once(failure).filter_map(failure_status));
            let responses: ExchangeStream = Box::pin(responses);
            Ok(Response::new(responses))
        })
    }
}

/// Where a call records the transport failure that ended it, so its response ends with the same
/// status.
struct FailureReport {
    sender: Option<oneshot::Sender<Status>>,
}

impl FailureReport {
    fn inbound_frame(&mut self, item: Result<VerifiedFrame<ClientFrame>, Status>) -> InboundFrame {
        let status = match item {
            Ok(frame) => return InboundFrame::Frame(frame),
            Err(status) => status,
        };
        if let Some(sender) = self.sender.take() {
            sender
                .send(status)
                .discarded("a response that already ended has no status left to carry");
        }
        InboundFrame::Failed
    }
}

/// The last item of a call's response: the status of the transport failure that ended the
/// session, or nothing when the session ended without one.
async fn failure_status(
    received: Result<Status, oneshot::error::RecvError>,
) -> Option<Result<EncodedFrame<ServerFrame>, Status>> {
    match received {
        Ok(status) => Some(Err(status)),
        Err(_) => None,
    }
}

/// One resource upload call.
struct Upload {
    service: SessionServiceImpl,
    limits: SessionLimits,
}

impl ClientStreamingService<VerifiedFrame<UploadFrame>> for Upload {
    type Response = EncodedFrame<UploadReplyFrame>;
    type Future = BoxFuture<Response<EncodedFrame<UploadReplyFrame>>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<UploadFrame>>>) -> Self::Future {
        let service = self.service.clone();
        let limits = self.limits;
        Box::pin(async move {
            let user = service
                .authenticate_grpc_metadata(request.metadata())
                .await?;
            let reply = service.serve_upload(user, request.into_inner()).await?;
            match reply.encode(&limits) {
                Ok(frame) => Ok(Response::new(frame)),
                Err(error) => Err(Status::internal(format!(
                    "the upload reply does not fit a frame: {error}"
                ))),
            }
        })
    }
}
