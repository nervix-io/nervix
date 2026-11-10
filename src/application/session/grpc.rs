//! The session over gRPC.
//!
//! Layer: edges.
//!
//! - **Owns.** The `nervix.session.Session` gRPC service: routing its four methods,
//!   authenticating each call from its metadata before any frame is read, holding every message to
//!   the session frame limit, ending a call with the status of the transport failure that ended
//!   it, and ending an upload, a restore or a download at once when the node closes admission.
//! - **Depends on.** The session engine, upload handling, backup downloads and restores, the
//!   client wire gRPC codecs, and tonic's server primitives.
//! - **Must not know.** What any request does.
//!
//! A call that fails authentication ends with `UNAUTHENTICATED`. A message above the frame limit
//! ends the call with `OUT_OF_RANGE`, the status tonic refuses it with before buffering it, and a
//! message that is not a valid frame with `INTERNAL`, the status the frame codec assigns it.
//! Everything a well-formed frame can get wrong is answered within the session instead, as a typed
//! reply to the request it names, except a frame without a request identity, which breaks the
//! protocol itself and ends the session with a `SessionEnding` that says so.
//!
//! When the node closes admission, a session ends itself with a `SessionEnding` that says the node
//! is shutting down, and its call ends right after that ending. An upload, a restore or a download
//! still running ends at once with `UNAVAILABLE`: it reads and admits nothing more and does not wait
//! for its client, so no client holds the stopping node open, and its client sends it again to
//! another node as it does after a transport failure.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use error_stack::Report;
use futures_util::{Stream, StreamExt as _, stream};
use nervix_client_wire::{
    BackupDownloadFrame, BackupDownloadRequestFrame, ClientFrame, EncodedFrame, RestoreFrame,
    RestoreReplyFrame, ServerFrame, SessionLimits, UploadFrame, UploadReplyFrame, VerifiedFrame,
    grpc::{
        DOWNLOAD_BACKUP_PATH, EXCHANGE_PATH, RESTORE_BACKUP_PATH, SERVICE_NAME,
        ServerBackupDownloadCodec, ServerExchangeCodec, ServerRestoreCodec, ServerUploadCodec,
        UPLOAD_RESOURCE_PATH,
    },
};
use nervix_models::UserName;
use nervix_primitives::sync::{CancellationToken, oneshot};
use nervix_recovery::Discarded as _;
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{
        ClientStreamingService, Grpc, NamedService, ServerStreamingService, StreamingService,
    },
};

use super::{InboundFrame, SessionTransport, download::DownloadFrameStream, outbound};
use crate::application::{
    authentication::{BasicAuthCredentials, GrpcAuthenticationError, credentials_from_metadata},
    client_traffic::ClientCall,
    session_service::SessionServiceImpl,
};

/// What a gRPC call presents to be authenticated: where it came from and the credentials its
/// metadata carries.
struct PresentedCall {
    call: ClientCall,
    credentials: Option<BasicAuthCredentials>,
}

impl PresentedCall {
    fn of<T>(request: &Request<T>) -> Self {
        Self {
            call: ClientCall {
                transport: SessionTransport::Grpc,
                peer: request.remote_addr(),
            },
            credentials: credentials_from_metadata(request.metadata()),
        }
    }

    /// Authenticates the presented credentials, or reports why they do not authenticate the call.
    async fn authenticate(
        self,
        service: &SessionServiceImpl,
    ) -> Result<UserName, Report<GrpcAuthenticationError>> {
        service
            .authenticate_client(self.call, self.credentials.as_ref())
            .await
            .map_err(|refusal| Report::new(refusal.into()))
    }
}

/// The status of a call the node ended because it closed admission.
fn shutting_down() -> Status {
    Status::unavailable("the node is shutting down")
}

/// The future of a call that runs until it answers or the node closes admission. A call that
/// admission ends stops at once with [`shutting_down`]: it reads and admits nothing more, and work
/// it already handed to a task of its own goes on without it.
fn until_admission_closes<T>(
    admission: CancellationToken,
    call: impl Future<Output = Result<T, Status>> + Send + 'static,
) -> BoxFuture<T, Status>
where
    T: Send + 'static,
{
    Box::pin(async move {
        nervix_primitives::select! {
            biased;
            answered = call => answered,
            _ = admission.cancelled() => Err(shutting_down()),
        }
    })
}

/// The session service a gRPC server hosts.
#[derive(Clone)]
pub(in crate::application) struct SessionGrpcService {
    service: SessionServiceImpl,
    limits: SessionLimits,
}

impl SessionGrpcService {
    pub(in crate::application) fn new(service: SessionServiceImpl) -> Self {
        let limits = service.session_limits();
        Self { service, limits }
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
            DOWNLOAD_BACKUP_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(ServerBackupDownloadCodec::new(limits))
                    .max_decoding_message_size(limits.frame_bytes())
                    .max_encoding_message_size(limits.frame_bytes());
                let download = Download { service, limits };
                Ok(grpc.server_streaming(download, request).await)
            }),
            RESTORE_BACKUP_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(ServerRestoreCodec::new(limits))
                    .max_decoding_message_size(limits.frame_bytes())
                    .max_encoding_message_size(limits.frame_bytes());
                let restore = Restore { service, limits };
                Ok(grpc.client_streaming(restore, request).await)
            }),
            _ => Box::pin(async move {
                let status = Status::unimplemented("the session service serves four methods");
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
            let user = PresentedCall::of(&request)
                .authenticate(&service)
                .await
                .map_err(GrpcAuthenticationError::status)?;
            let (failure_sender, failure) = oneshot::channel();
            let mut failure_report = FailureReport {
                sender: Some(failure_sender),
            };
            let inbound = request
                .into_inner()
                .map(move |item| failure_report.inbound_frame(item));
            let (outbound, frames) =
                outbound::channel(CancellationToken::new(), &service.inner.operational.session);
            let session_service = service.clone();
            service.inner.service_tasks.spawn(async move {
                session_service
                    .run_session(user, SessionTransport::Grpc, limits, inbound, outbound)
                    .await;
            });
            // The frames end once the session wrote its ending, or once it and every task it
            // started have dropped their lanes after everything they queued was sent. The status
            // of the transport failure that ended the session, if one did, closes the call.
            let responses = frames
                .into_stream()
                .map(Ok)
                .chain(stream::once(failure).filter_map(failure_status));
            let responses: ExchangeStream = Box::pin(responses);
            let mut response = Response::new(responses);
            nervix_client_wire::grpc::limits::write(response.metadata_mut(), limits);
            Ok(response)
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
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<UploadFrame>>>) -> Self::Future {
        let service = self.service.clone();
        let limits = self.limits;
        let admission = service.inner.admission_shutdown.clone();
        until_admission_closes(admission, async move {
            let user = PresentedCall::of(&request)
                .authenticate(&service)
                .await
                .map_err(GrpcAuthenticationError::status)?;
            if let Some(refusal) =
                nervix_client_wire::grpc::limits::offer_refusal(request.metadata(), limits)
            {
                return Err(refusal);
            }
            let reply = service.serve_upload(user, request.into_inner()).await?;
            #[cfg(feature = "testing")]
            if service.loses_installed_upload_reply(&reply) {
                return Err(Status::unavailable(
                    "the reply to an upload call that installed its version was lost",
                ));
            }
            let frame = match reply.encode(&limits) {
                Ok(frame) => frame,
                Err(error) => {
                    return Err(Status::internal(format!(
                        "the upload reply does not fit a frame: {error:#}"
                    )));
                }
            };
            let mut response = Response::new(frame);
            nervix_client_wire::grpc::limits::write(response.metadata_mut(), limits);
            Ok(response)
        })
    }
}

/// One restore call.
struct Restore {
    service: SessionServiceImpl,
    limits: SessionLimits,
}

impl ClientStreamingService<VerifiedFrame<RestoreFrame>> for Restore {
    type Response = EncodedFrame<RestoreReplyFrame>;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<RestoreFrame>>>) -> Self::Future {
        let service = self.service.clone();
        let limits = self.limits;
        let admission = service.inner.admission_shutdown.clone();
        until_admission_closes(admission, async move {
            let user = PresentedCall::of(&request)
                .authenticate(&service)
                .await
                .map_err(GrpcAuthenticationError::status)?;
            if let Some(refusal) =
                nervix_client_wire::grpc::limits::offer_refusal(request.metadata(), limits)
            {
                return Err(refusal);
            }
            let reply = service.serve_restore(user, request.into_inner()).await?;
            let frame = match reply.encode(&limits) {
                Ok(frame) => frame,
                Err(error) => {
                    return Err(Status::internal(format!(
                        "the restore reply does not fit a frame: {error:#}"
                    )));
                }
            };
            let mut response = Response::new(frame);
            nervix_client_wire::grpc::limits::write(response.metadata_mut(), limits);
            Ok(response)
        })
    }
}

/// One backup download call.
struct Download {
    service: SessionServiceImpl,
    limits: SessionLimits,
}

type DownloadStream =
    Pin<Box<dyn Stream<Item = Result<EncodedFrame<BackupDownloadFrame>, Status>> + Send + 'static>>;

impl ServerStreamingService<VerifiedFrame<BackupDownloadRequestFrame>> for Download {
    type Response = EncodedFrame<BackupDownloadFrame>;
    type ResponseStream = DownloadStream;
    type Future = BoxFuture<Response<DownloadStream>, Status>;

    fn call(
        &mut self,
        request: Request<VerifiedFrame<BackupDownloadRequestFrame>>,
    ) -> Self::Future {
        let service = self.service.clone();
        let limits = self.limits;
        let admission = service.inner.admission_shutdown.clone();
        until_admission_closes(admission.clone(), async move {
            let user = PresentedCall::of(&request)
                .authenticate(&service)
                .await
                .map_err(GrpcAuthenticationError::status)?;
            let frames = service
                .serve_backup_download(user, request.get_ref(), limits)
                .await
                .map_err(|error| {
                    Status::internal(format!(
                        "the download answer does not fit a frame: {error:#}"
                    ))
                })?;
            let frames = frames.into_stream();
            let responses = DownloadResponse::Frames { frames, admission }.into_stream();
            let mut response = Response::new(responses);
            nervix_client_wire::grpc::limits::write(response.metadata_mut(), limits);
            Ok(response)
        })
    }
}

/// What one download call still has to send its client: the frames of its answer, until the node
/// closes admission, which ends the call at once with [`shutting_down`].
enum DownloadResponse {
    Frames {
        frames: DownloadFrameStream,
        admission: CancellationToken,
    },
    /// The call sent the status it ends with.
    Ended,
}

/// What a download call takes next.
enum DownloadStep {
    /// The answer's next frame, or its end.
    Frame(Option<EncodedFrame<BackupDownloadFrame>>),
    /// The node closed admission.
    AdmissionClosed,
}

impl DownloadResponse {
    fn into_stream(self) -> DownloadStream {
        Box::pin(stream::unfold(self, Self::next))
    }

    async fn next(self) -> Option<(Result<EncodedFrame<BackupDownloadFrame>, Status>, Self)> {
        let Self::Frames {
            mut frames,
            admission,
        } = self
        else {
            return None;
        };
        let step = nervix_primitives::select! {
            biased;
            _ = admission.cancelled() => DownloadStep::AdmissionClosed,
            frame = frames.next() => DownloadStep::Frame(frame),
        };
        match step {
            DownloadStep::Frame(Some(frame)) => {
                Some((Ok(frame), Self::Frames { frames, admission }))
            }
            DownloadStep::Frame(None) => None,
            DownloadStep::AdmissionClosed => Some((Err(shutting_down()), Self::Ended)),
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{
        pin::pin,
        task::{Context, Waker},
    };

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_client_wire::{BackupDownloadFailed, BackupDownloadFailure, BackupDownloadMessage};
    use nervix_primitives::sync::mpsc;

    use super::*;
    use crate::application::session::download::DownloadFrames;

    fn download_frame() -> EncodedFrame<BackupDownloadFrame> {
        let failed = BackupDownloadFailed {
            failure: BackupDownloadFailure::InvalidRequest,
            message: "a test frame".to_string(),
        };
        BackupDownloadMessage::encode_failed(&failed, &SessionLimits::DEFAULT)
            .assured("a short failure fits a frame")
    }

    #[nervix_primitives::test]
    async fn a_call_still_running_when_admission_closes_ends_at_once_as_unavailable() {
        let admission = CancellationToken::new();
        let call = until_admission_closes(
            admission.clone(),
            std::future::pending::<Result<(), Status>>(),
        );
        let mut call = pin!(call);
        let mut context = Context::from_waker(Waker::noop());
        assert!(
            call.as_mut().poll(&mut context).is_pending(),
            "a call runs while admission is open"
        );

        admission.cancel();

        let ended = call.await.expect_err("closing admission ends the call");
        assert_eq!(ended.code(), tonic::Code::Unavailable);
    }

    #[nervix_primitives::test]
    async fn a_call_that_has_answered_keeps_its_answer_when_admission_closes() {
        let admission = CancellationToken::new();
        admission.cancel();

        let answered =
            until_admission_closes(admission, std::future::ready(Ok::<_, Status>(7))).await;

        assert_eq!(answered.assured("an answered call keeps its answer"), 7);
    }

    #[nervix_primitives::test]
    async fn a_download_stops_sending_and_ends_as_unavailable_once_admission_closes() {
        let admission = CancellationToken::new();
        let (frames, archive) = mpsc::channel(4);
        frames
            .send(download_frame())
            .await
            .assured("the test channel has room");
        let mut responses = DownloadResponse::Frames {
            frames: DownloadFrames::Archive(archive).into_stream(),
            admission: admission.clone(),
        }
        .into_stream();

        let first = responses
            .next()
            .await
            .assured("the frame queued before admission closed is sent");
        assert!(
            first.is_ok(),
            "a frame queued while admission is open is sent"
        );
        frames
            .send(download_frame())
            .await
            .assured("the test channel has room");
        admission.cancel();

        let Some(Err(ended)) = responses.next().await else {
            panic!("closing admission must end the download with a status, not another frame");
        };
        assert_eq!(ended.code(), tonic::Code::Unavailable);
        assert!(
            responses.next().await.is_none(),
            "nothing follows the status that ends the call"
        );
    }

    #[nervix_primitives::test]
    async fn a_download_answered_whole_ends_without_a_status() {
        let admission = CancellationToken::new();
        let mut responses = DownloadResponse::Frames {
            frames: DownloadFrames::Answer(download_frame()).into_stream(),
            admission,
        }
        .into_stream();

        let answer = responses.next().await.assured("the answer is sent");
        assert!(answer.is_ok(), "the answer is a frame");
        assert!(
            responses.next().await.is_none(),
            "an answered download ends without a status"
        );
    }
}
