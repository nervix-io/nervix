//! Installing authenticated per-call bounds for native session traffic.
//!
//! Layer: edges.
//! - **Owns.** The response-header boundary and one publication of immutable per-call limits.
//! - **Depends on.** The wire codec, primitive channels and maintained tonic bodies and decoders.
//! - **Must not know.** Requests, query routing, attachment recovery or server runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        edge,
        reason = "owns native call opening and immutable bound installation"
    )
)]

use std::{
    future::{Future as _, poll_fn},
    mem,
    pin::Pin,
    task::{Context, Poll, ready},
};

use bytes::Bytes;
use http_body::Frame;
use meticulous::ResultExt as _;
use nervix_client_wire::{
    EncodedFrame, FrameRoot, SessionLimits, VerifiedFrame,
    grpc::{
        FrameCodec, FrameEncoder,
        limits::{self, SessionOpeningRejection},
    },
};
use nervix_primitives::{stream::Stream, sync::oneshot};
use nervix_recovery::Discarded as _;
use tonic::{
    Code, Request, Status, Streaming,
    body::Body,
    codec::{Codec as _, EncodeBody},
    codegen::{Service as _, http},
    metadata::MetadataMap,
    transport::Channel,
};

/// The download request fits the fixed opening envelope. A main exchange waits for authenticated
/// headers before its request body is configured or polled.
pub(crate) enum OutboundBounds {
    Announced,
    Opening,
}

pub(crate) struct OpenedStream<Inbound: FrameRoot> {
    pub(crate) limits: SessionLimits,
    pub(crate) frames: Streaming<VerifiedFrame<Inbound>>,
    metadata: MetadataMap,
    extensions: http::Extensions,
}

impl<Inbound: FrameRoot> OpenedStream<Inbound> {
    pub(crate) fn into_response(self) -> tonic::Response<Streaming<VerifiedFrame<Inbound>>> {
        tonic::Response::from_parts(self.metadata, self.frames, self.extensions)
    }
}

/// The caller keeps readiness in its existing connection-error and deadline boundary.
pub(crate) async fn ready(channel: &mut Channel) -> Result<(), tonic::transport::Error> {
    poll_fn(|context| channel.poll_ready(context)).await
}

#[expect(
    clippy::result_large_err,
    reason = "preserve tonic's complete status at the foreign boundary without an extra error \
              allocation"
)]
pub(crate) async fn open<Outbound, Inbound>(
    channel: &mut Channel,
    request: Request<impl Stream<Item = EncodedFrame<Outbound>> + Send + Unpin + 'static>,
    path: http::uri::PathAndQuery,
    bounds: OutboundBounds,
) -> Result<OpenedStream<Inbound>, Status>
where
    Outbound: FrameRoot + Send + Sync + Unpin,
    Inbound: FrameRoot + Send + Sync + Unpin,
{
    open_call(channel, request, path, CallBounds::Initial(bounds)).await
}

/// Offers complete bounds learned from a session, so a matching companion streams immediately.
/// The inner error is a checked pre-admission refusal with the bounds for the next attempt.
#[expect(
    clippy::result_large_err,
    reason = "preserve tonic's complete status at the foreign boundary without an extra error \
              allocation"
)]
pub(crate) async fn open_offered<Outbound, Inbound>(
    channel: &mut Channel,
    request: Request<impl Stream<Item = EncodedFrame<Outbound>> + Send + Unpin + 'static>,
    path: http::uri::PathAndQuery,
    bounds: SessionLimits,
) -> Result<Result<OpenedStream<Inbound>, SessionLimits>, Status>
where
    Outbound: FrameRoot + Send + Sync + Unpin,
    Inbound: FrameRoot + Send + Sync + Unpin,
{
    match open_call(channel, request, path, CallBounds::Offered(bounds)).await {
        Ok(opened) => Ok(Ok(opened)),
        Err(status) => match limits::changed(&status) {
            Ok(Some(current)) => Ok(Err(current)),
            Ok(None) => Err(status),
            Err(report) => Err(SessionOpeningRejection::with_possible_admission(report)),
        },
    }
}

enum CallBounds {
    Initial(OutboundBounds),
    Offered(SessionLimits),
}

#[expect(
    clippy::result_large_err,
    reason = "preserve tonic's complete status at the foreign boundary without an extra error \
              allocation"
)]
async fn open_call<Outbound, Inbound>(
    channel: &mut Channel,
    request: Request<impl Stream<Item = EncodedFrame<Outbound>> + Send + Unpin + 'static>,
    path: http::uri::PathAndQuery,
    bounds: CallBounds,
) -> Result<OpenedStream<Inbound>, Status>
where
    Outbound: FrameRoot + Send + Sync + Unpin,
    Inbound: FrameRoot + Send + Sync + Unpin,
{
    let (mut metadata, extensions, source) = request.into_parts();
    let source = FrameSource { source };
    let offered = match &bounds {
        CallBounds::Offered(bounds) => {
            limits::write(&mut metadata, *bounds);
            Some(*bounds)
        }
        CallBounds::Initial(_) => None,
    };
    let (body, publication) = match bounds {
        CallBounds::Initial(OutboundBounds::Announced) => {
            let (publish, limits) = oneshot::channel();
            (
                OpeningBody {
                    state: BodyState::Waiting { limits, source },
                },
                Some(publish),
            )
        }
        CallBounds::Initial(OutboundBounds::Opening) => {
            (OpeningBody::encoding(source, SessionLimits::OPENING), None)
        }
        CallBounds::Offered(bounds) => (OpeningBody::encoding(source, bounds), None),
    };
    let mut request = http::Request::new(Body::new(body));
    *request.method_mut() = http::Method::POST;
    *request.version_mut() = http::Version::HTTP_2;
    *request.uri_mut() = http::Uri::from(path);
    *request.headers_mut() = metadata.into_headers();
    // Match tonic's native request boundary: application metadata cannot supply reserved gRPC
    // status/content headers. The maintained transport still owns authority and user-agent.
    for reserved in [
        "te",
        "user-agent",
        "content-type",
        "grpc-message",
        "grpc-message-type",
        "grpc-status",
    ] {
        request.headers_mut().remove(reserved);
    }
    *request.extensions_mut() = extensions;
    request
        .headers_mut()
        .insert(http::header::TE, http::HeaderValue::from_static("trailers"));
    request.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/grpc"),
    );
    let response = channel
        .call(request)
        .await
        .map_err(|error| Status::from_error(Box::new(error)))?;
    if let Some(encoding) = response.headers().get("grpc-encoding")
        && encoding != "identity"
    {
        let name = match std::str::from_utf8(encoding.as_bytes()) {
            Ok(name) => name.to_string(),
            Err(_) => format!("{:?}", encoding.as_bytes()),
        };
        let mut status = Status::unimplemented(format!(
            "Content is compressed with `{name}` which isn't supported"
        ));
        status.metadata_mut().insert(
            "grpc-accept-encoding",
            "identity"
                .parse()
                .assured("identity is fixed valid ASCII metadata"),
        );
        return Err(status);
    }
    let terminal_headers = match Status::from_header_map(response.headers()) {
        Some(status) if status.code() != Code::Ok => return Err(status),
        Some(_) => true,
        None => false,
    };
    let (parts, body) = response.into_parts();
    let metadata = MetadataMap::from_headers(parts.headers);
    let limits = match limits::read(&metadata) {
        Ok(limits) => limits,
        Err(report) if !terminal_headers && parts.status != http::StatusCode::OK => {
            return Err(SessionOpeningRejection::attach(
                report,
                http_failure(parts.status),
            ));
        }
        Err(report) => {
            return Err(if offered.is_some() {
                SessionOpeningRejection::with_possible_admission(report)
            } else {
                SessionOpeningRejection::into_status(report)
            });
        }
    };
    if let Some(offered) = offered
        && offered != limits
    {
        return Err(SessionOpeningRejection::with_possible_admission(
            error_stack::Report::new(limits::SessionAnnouncementError::OfferMismatch {
                offered,
                announced: limits,
            }),
        ));
    }
    let mut codec = FrameCodec::<Outbound, Inbound>::new(limits);
    let frames = if terminal_headers {
        // grpc-status in the initial headers is a trailers-only response. Its body carries no
        // messages; drop it without buffering data under an unconfigured empty-stream decoder.
        drop(body);
        Streaming::new_empty(codec.decoder(), Body::empty())
    } else {
        Streaming::new_response(
            codec.decoder(),
            body,
            parts.status,
            None,
            Some(limits.frame_bytes()),
        )
    };
    if let Some(publication) = publication {
        publication
            .send(limits)
            .discarded("a peer may have ended the request body while returning its response");
    }
    Ok(OpenedStream {
        limits,
        frames,
        metadata,
        extensions: parts.extensions,
    })
}

/// The gRPC HTTP-status fallback, also used by tonic when a response has no grpc-status. Header
/// failures must keep their retry classification before a bounded decoder can be installed.
fn http_failure(status: http::StatusCode) -> Status {
    let code = match status {
        http::StatusCode::BAD_REQUEST => Code::Internal,
        http::StatusCode::UNAUTHORIZED => Code::Unauthenticated,
        http::StatusCode::FORBIDDEN => Code::PermissionDenied,
        http::StatusCode::NOT_FOUND => Code::Unimplemented,
        http::StatusCode::TOO_MANY_REQUESTS
        | http::StatusCode::BAD_GATEWAY
        | http::StatusCode::SERVICE_UNAVAILABLE
        | http::StatusCode::GATEWAY_TIMEOUT => Code::Unavailable,
        _ => Code::Unknown,
    };
    Status::new(
        code,
        format!(
            "grpc-status header missing, mapped from HTTP status code {}",
            status.as_u16()
        ),
    )
}

/// The same channel stream tonic normally maps to successful encoded items.
struct FrameSource<Source> {
    source: Source,
}

impl<Outbound: FrameRoot, Source: Stream<Item = EncodedFrame<Outbound>> + Unpin> Stream
    for FrameSource<Source>
{
    type Item = Result<EncodedFrame<Outbound>, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.source)
            .poll_next(context)
            .map(|frame| frame.map(Ok))
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "tonic already boxes this HTTP body once; retaining its encoder inline avoids a \
              second allocation per call, with inline overhead checked at construction"
)]
enum BodyState<Outbound: FrameRoot + Unpin, Source> {
    Waiting {
        limits: oneshot::Receiver<SessionLimits>,
        source: FrameSource<Source>,
    },
    Encoding(EncodeBody<FrameEncoder<Outbound>, FrameSource<Source>>),
    Ended,
}

struct OpeningBody<Outbound: FrameRoot + Unpin, Source> {
    state: BodyState<Outbound, Source>,
}

impl<Outbound: FrameRoot + Unpin, Source: Stream<Item = EncodedFrame<Outbound>> + Unpin>
    OpeningBody<Outbound, Source>
{
    fn encoding(source: FrameSource<Source>, limits: SessionLimits) -> Self {
        const {
            assert!(
                std::mem::size_of::<Self>()
                    <= std::mem::size_of::<EncodeBody<FrameEncoder<Outbound>, FrameSource<Source>>>(
                    ) + 2 * std::mem::size_of::<usize>()
            );
        }
        let mut codec = FrameCodec::<Outbound, Outbound>::new(limits);
        Self {
            state: BodyState::Encoding(EncodeBody::new_client(
                codec.encoder(),
                source,
                None,
                Some(limits.frame_bytes()),
            )),
        }
    }
}

impl<Outbound: FrameRoot + Unpin, Source: Stream<Item = EncodedFrame<Outbound>> + Unpin>
    http_body::Body for OpeningBody<Outbound, Source>
{
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        match &mut self.state {
            BodyState::Encoding(body) => Pin::new(body).poll_frame(context),
            BodyState::Ended => Poll::Ready(None),
            BodyState::Waiting { limits, .. } => {
                let limits = ready!(Pin::new(limits).poll(context));
                let BodyState::Waiting { source, .. } =
                    mem::replace(&mut self.state, BodyState::Ended)
                else {
                    unreachable!("this task exclusively owns the waiting body it just polled");
                };
                match limits {
                    Ok(limits) => self.state = Self::encoding(source, limits).state,
                    Err(_) => {
                        return Poll::Ready(Some(Err(Status::cancelled(
                            "the call ended before its limits were installed",
                        ))));
                    }
                }
                self.poll_frame(context)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.state {
            BodyState::Waiting { .. } => false,
            BodyState::Encoding(body) => body.is_end_stream(),
            BodyState::Ended => true,
        }
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "checks the production request-body opening gate")
)]
#[path = "session_open/shuttle_tests.rs"]
mod shuttle_tests;
