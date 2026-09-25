//! The session protocol over gRPC through a hand-written tonic service.
//!
//! Nothing here is generated from a service definition: the service routes the two method paths
//! itself and hands tonic the frame codec, and the client addresses the same paths with the same
//! codec. This is the whole integration a transport needs.

use std::{
    convert::Infallible,
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    task::{Context, Poll},
    time::Duration,
};

use bytes::{BufMut, Bytes};
use error_stack::Report;
use futures_util::StreamExt;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientFrame, ClientMessage, ClientRequest, CommandDisposition, CommandOutcome, CommandRequest,
    DomainInfo, DomainList, EncodedFrame, InspectTransactionRequest, InspectionOutcome,
    NoticeLevel, OutcomeOrigin, Reply, ReplyBody, ReplyDelivery, RequestId, RequestRejected,
    RequestRejection, ServerEvent, ServerFrame, ServerMessage, ServerNotice, SessionLimitSettings,
    SessionLimits, TransferAssembly, UploadChunk, UploadDisposition, UploadFrame, UploadMessage,
    UploadReply, UploadReplyFrame, UploadStart, VerifiedFrame, WireDecodeError,
    grpc::{
        ClientExchangeCodec, ClientUploadCodec, EXCHANGE_PATH, FrameDecoder, SERVICE_NAME,
        ServerExchangeCodec, ServerUploadCodec, UPLOAD_RESOURCE_PATH,
    },
};
use nervix_models::{
    CommandExecutionReference, DomainPace, DomainStatus, ImpactPlanningBasis,
    ImpactReportCompleteness, ResourceUploadIdentity, TransactionImpactReport,
    TransactionInspection, TransactionInspectionTarget, TransactionLifecycle, TransactionPosition,
    TransactionStatus,
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{
    Code, Request, Response, Status, Streaming,
    body::Body,
    codec::{Codec, EncodeBuf, Encoder},
    codegen::{BoxFuture, Service, http},
    server::{ClientStreamingService, Grpc, NamedService, StreamingService},
    transport::{Channel, Server},
};

const DEADLINE: Duration = Duration::from_secs(30);

/// The limits both ends use: small frames, so a large reply is transferred in parts.
fn limits() -> SessionLimits {
    let defaults = SessionLimits::DEFAULT;
    let size = |value: usize| NonZeroUsize::new(value).assured("a non-zero test limit");
    SessionLimits::try_from(SessionLimitSettings {
        frame_bytes: size(2048),
        transfer_bytes: size(defaults.transfer_bytes()),
        nesting_depth: size(defaults.nesting_depth()),
        collection_entries: size(defaults.collection_entries()),
        string_bytes: size(2048),
    })
    .assured("the test limits pass their checks")
}

fn request_id(id: u64) -> RequestId {
    RequestId::new(NonZeroU64::new(id).assured("a non-zero request identity"))
}

/// The session service: it answers each request with a reply that names it.
#[derive(Clone)]
struct SessionService {
    limits: SessionLimits,
}

impl NamedService for SessionService {
    const NAME: &'static str = SERVICE_NAME;
}

impl Service<http::Request<Body>> for SessionService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let limits = self.limits;
        match request.uri().path() {
            EXCHANGE_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(ServerExchangeCodec::new(limits));
                Ok(grpc.streaming(Exchange { limits }, request).await)
            }),
            UPLOAD_RESOURCE_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(ServerUploadCodec::new(limits));
                Ok(grpc.client_streaming(Upload { limits }, request).await)
            }),
            _ => Box::pin(async move {
                Ok(Status::unimplemented("the session serves two methods").into_http())
            }),
        }
    }
}

struct Exchange {
    limits: SessionLimits,
}

type ServerStream = ReceiverStream<Result<EncodedFrame<ServerFrame>, Status>>;

impl StreamingService<VerifiedFrame<ClientFrame>> for Exchange {
    type Response = EncodedFrame<ServerFrame>;
    type ResponseStream = ServerStream;
    type Future = BoxFuture<Response<ServerStream>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<ClientFrame>>>) -> Self::Future {
        let limits = self.limits;
        Box::pin(async move {
            let mut inbound = request.into_inner();
            let (outbound, receiver) = mpsc::channel(64);
            tokio::spawn(async move {
                let notice = ServerNotice {
                    level: NoticeLevel::Info,
                    message: "connected to leader 'node-1'".to_string(),
                }
                .encode(&limits)
                .assured("a notice fits the test limits");
                if outbound.send(Ok(notice)).await.is_err() {
                    return;
                }
                while let Some(frame) = inbound.next().await {
                    tokio::task::consume_budget().await;
                    let frame = match frame {
                        Ok(frame) => frame,
                        Err(status) => {
                            drop(outbound.send(Err(status)).await);
                            return;
                        }
                    };
                    let Some(reply) = answer(&frame) else {
                        let status = Status::invalid_argument("a request without an identity");
                        drop(outbound.send(Err(status)).await);
                        return;
                    };
                    match reply
                        .encode(&limits)
                        .assured("test replies fit the transfer limit")
                    {
                        ReplyDelivery::Frame(frame) => {
                            if outbound.send(Ok(frame)).await.is_err() {
                                return;
                            }
                        }
                        ReplyDelivery::Transfer(parts) => {
                            for part in parts {
                                tokio::task::consume_budget().await;
                                if outbound.send(Ok(part)).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            });
            Ok(Response::new(ReceiverStream::new(receiver)))
        })
    }
}

/// The reply to one request frame, or `None` when the frame names no request.
fn answer(frame: &VerifiedFrame<ClientFrame>) -> Option<Reply> {
    let request_id = frame.request_id()?;
    let message = match ClientMessage::decode(frame) {
        Ok(message) => message,
        Err(error) => return Some(rejection(request_id, &error)),
    };
    let body = match message.request {
        ClientRequest::ListDomains => ReplyBody::DomainList(DomainList {
            domains: vec![DomainInfo {
                domain: nervix_models::DomainName::parse("tenant").assured("a valid domain"),
                status: DomainStatus::Running,
                pace: DomainPace::Unpaced,
            }],
        }),
        ClientRequest::Command(command) => ReplyBody::Command(Box::new(CommandOutcome {
            execution_reference: command.execution_reference,
            origin: OutcomeOrigin::Executed,
            disposition: CommandDisposition::Completed {
                already_existed: false,
            },
            message: format!("executed {}", command.query),
            diagnostics: Vec::new(),
            statements: Vec::new(),
            transaction: None,
            transaction_admission: None,
            inspection: None,
            wasm_state: None,
        })),
        ClientRequest::InspectTransaction(_) => ReplyBody::Inspection(inspection()),
        _ => ReplyBody::Rejected(RequestRejected {
            rejection: RequestRejection::UnsupportedRequest,
            field: None,
            message: "not served by the test service".to_string(),
        }),
    };
    Some(Reply {
        request_id: message.request_id,
        body,
    })
}

fn rejection(request_id: RequestId, error: &Report<WireDecodeError>) -> Reply {
    let field = match error.current_context() {
        WireDecodeError::InvalidValue { field, .. } => Some((*field).to_string()),
        _ => None,
    };
    Reply {
        request_id,
        body: ReplyBody::Rejected(RequestRejected {
            rejection: RequestRejection::InvalidRequest,
            field,
            message: error.current_context().to_string(),
        }),
    }
}

/// An inspection whose report is too large for one 2 KiB frame.
fn inspection() -> InspectionOutcome {
    let domain = nervix_models::DomainName::parse("tenant").assured("a valid domain");
    let diagnostics = (0..64)
        .map(|index| nervix_models::ImpactDiagnostic {
            kind: nervix_models::ImpactDiagnosticKind::Topology,
            operation: None,
            message: format!("unresolved reference number {index} in the final model run"),
        })
        .collect::<Vec<_>>();
    let report = TransactionImpactReport::new(
        domain.clone(),
        TransactionPosition::new(0),
        ImpactPlanningBasis::new([3; 32]),
        ImpactReportCompleteness::incomplete(diagnostics).assured("diagnostics are present"),
        Vec::new(),
        Vec::new(),
    )
    .assured("an empty transaction's report is consistent");
    InspectionOutcome::Inspected(Box::new(TransactionInspection {
        transaction: TransactionStatus::new(
            "transaction".to_string(),
            domain,
            TransactionLifecycle::Open,
            TransactionPosition::new(0),
            0,
        )
        .assured("an empty open transaction is consistent"),
        operation: None,
        report,
    }))
}

struct Upload {
    limits: SessionLimits,
}

impl ClientStreamingService<VerifiedFrame<UploadFrame>> for Upload {
    type Response = EncodedFrame<UploadReplyFrame>;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<UploadFrame>>>) -> Self::Future {
        let limits = self.limits;
        Box::pin(async move {
            let mut inbound = request.into_inner();
            let Some(first) = inbound.message().await? else {
                return Err(Status::invalid_argument("an empty upload stream"));
            };
            let UploadMessage::Start(start) = UploadMessage::decode(&first)
                .map_err(|error| Status::invalid_argument(error.to_string()))?
            else {
                return Err(Status::invalid_argument(
                    "an upload starts with its metadata",
                ));
            };
            let mut received = Vec::new();
            while let Some(frame) = inbound.message().await? {
                tokio::task::consume_budget().await;
                let UploadMessage::Chunk(chunk) = UploadMessage::decode(&frame)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
                else {
                    return Err(Status::invalid_argument("chunks follow the upload start"));
                };
                received.put(chunk.shared_bytes());
            }
            let disposition = if u64::try_from(received.len()).ok() == Some(start.total_bytes.get())
            {
                UploadDisposition::Installed {
                    upload_identity: start.upload_identity,
                    version: NonZeroU64::new(7).assured("a non-zero version"),
                    origin: OutcomeOrigin::Executed,
                }
            } else {
                UploadDisposition::Failed {
                    upload_identity: Some(start.upload_identity),
                    failure: nervix_client_wire::UploadFailure::SizeMismatch,
                    assigned_version: None,
                }
            };
            let reply = UploadReply {
                request_id: Some(start.request_id),
                disposition,
                message: format!("received {} bytes", received.len()),
                diagnostics: Vec::new(),
            };
            Ok(Response::new(
                reply
                    .encode(&limits)
                    .map_err(|error| Status::internal(error.to_string()))?,
            ))
        })
    }
}

struct TestServer {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(limits: SessionLimits) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("the loopback interface accepts a listener");
    let address = listener
        .local_addr()
        .assured("a bound listener has an address");
    let service = SessionService { limits };
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .assured("the test server serves until it is aborted");
    });
    TestServer { address, task }
}

async fn channel(server: &TestServer) -> Channel {
    Channel::from_shared(format!("http://{}", server.address))
        .assured("a loopback URI")
        .connect()
        .await
        .assured("the test server accepts connections")
}

/// An open exchange: frames sent through `requests`, frames received from `replies`.
struct Session<C: Codec> {
    requests: mpsc::Sender<C::Encode>,
    replies: Streaming<VerifiedFrame<ServerFrame>>,
}

async fn open<C>(server: &TestServer, codec: C) -> Session<C>
where
    C: Codec<Decode = VerifiedFrame<ServerFrame>> + Send + 'static,
    C::Encode: Send + Sync + 'static,
{
    let mut client = tonic::client::Grpc::new(channel(server).await);
    client.ready().await.assured("the channel becomes ready");
    let (requests, receiver) = mpsc::channel(16);
    let response = client
        .streaming(
            Request::new(ReceiverStream::new(receiver)),
            http::uri::PathAndQuery::from_static(EXCHANGE_PATH),
            codec,
        )
        .await
        .assured("the exchange opens");
    Session {
        requests,
        replies: response.into_inner(),
    }
}

async fn next_message(replies: &mut Streaming<VerifiedFrame<ServerFrame>>) -> ServerMessage {
    let frame = tokio::time::timeout(DEADLINE, replies.message())
        .await
        .assured("the server answers within the deadline")
        .assured("the exchange stays open")
        .assured("the exchange has more frames");
    ServerMessage::decode(&frame).assured("a server frame decodes")
}

fn command(id: u64, query: &str) -> ClientMessage {
    ClientMessage {
        request_id: request_id(id),
        request: ClientRequest::Command(CommandRequest {
            query: query.to_string(),
            domain: None,
            execution_reference: CommandExecutionReference::parse(format!("reference-{id}"))
                .assured("a valid reference"),
            expected_transaction_position: None,
            expected_preview: None,
        }),
    }
}

#[tokio::test]
async fn replies_name_their_requests_and_large_replies_arrive_in_parts() {
    let limits = limits();
    let server = serve(limits).await;
    let mut session = open(&server, ClientExchangeCodec::new(limits)).await;

    let requests = [
        command(1, "SHOW DOMAINS;"),
        ClientMessage {
            request_id: request_id(2),
            request: ClientRequest::ListDomains,
        },
        ClientMessage {
            request_id: request_id(3),
            request: ClientRequest::InspectTransaction(InspectTransactionRequest {
                target: TransactionInspectionTarget::Attached,
                operation: None,
            }),
        },
        command(u64::MAX, "SHOW CLUSTER;"),
    ];
    for request in &requests {
        tokio::task::consume_budget().await;
        let frame = request
            .encode(&limits)
            .assured("a request fits the test limits");
        session
            .requests
            .send(frame)
            .await
            .assured("the exchange accepts requests");
    }

    let ServerMessage::Event(ServerEvent::Notice(notice)) =
        next_message(&mut session.replies).await
    else {
        panic!("the unsolicited notice arrives first");
    };
    assert_eq!(notice.level, NoticeLevel::Info);

    let mut transfers = std::collections::BTreeMap::new();
    let mut replies = std::collections::BTreeMap::new();
    while replies.len() < requests.len() {
        tokio::task::consume_budget().await;
        match next_message(&mut session.replies).await {
            ServerMessage::Reply(reply) => {
                replies.insert(reply.request_id, reply.body);
            }
            ServerMessage::TransferPart(part) => {
                let assembly = transfers
                    .entry(part.request_id())
                    .or_insert_with(|| TransferAssembly::new(part.request_id(), &limits));
                assembly.append(&part).assured("parts arrive in order");
                if assembly.is_complete() {
                    let assembly = transfers
                        .remove(&part.request_id())
                        .assured("the assembly was just completed");
                    let reply = assembly.finish().assured("a complete transfer is a reply");
                    replies.insert(reply.request_id, reply.body);
                }
            }
            ServerMessage::Event(event) => panic!("an unexpected event {event:?}"),
        }
    }

    let Some(ReplyBody::Command(first)) = replies.get(&request_id(1)) else {
        panic!("request 1 is answered by a command outcome");
    };
    assert_eq!(first.execution_reference.as_str(), "reference-1");
    assert_eq!(first.message, "executed SHOW DOMAINS;");
    assert!(matches!(
        replies.get(&request_id(2)),
        Some(ReplyBody::DomainList(list)) if list.domains.len() == 1
    ));
    assert_eq!(
        replies.get(&request_id(3)),
        Some(&ReplyBody::Inspection(inspection()))
    );
    let Some(ReplyBody::Command(last)) = replies.get(&request_id(u64::MAX)) else {
        panic!("the last request is answered by a command outcome");
    };
    assert_eq!(last.message, "executed SHOW CLUSTER;");
    assert!(transfers.is_empty());
}

/// A client codec that sends raw bytes, for frames a well-behaved client never sends.
struct RawCodec {
    limits: SessionLimits,
}

struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&item);
        Ok(())
    }
}

impl Codec for RawCodec {
    type Encode = Bytes;
    type Decode = VerifiedFrame<ServerFrame>;
    type Encoder = RawEncoder;
    type Decoder = FrameDecoder<ServerFrame>;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        ClientExchangeCodec::new(self.limits).decoder()
    }
}

async fn status_after(session: &mut Session<RawCodec>) -> Status {
    loop {
        tokio::task::consume_budget().await;
        let message = tokio::time::timeout(DEADLINE, session.replies.message())
            .await
            .assured("the server answers within the deadline");
        match message {
            Ok(Some(_)) => {}
            Ok(None) => panic!("the call ended without a status"),
            Err(status) => return status,
        }
    }
}

#[tokio::test]
async fn a_malformed_frame_ends_the_call_with_an_internal_error() {
    let limits = limits();
    let server = serve(limits).await;
    let mut session = open(&server, RawCodec { limits }).await;
    session
        .requests
        .send(Bytes::from_static(b"this is not a flatbuffer frame"))
        .await
        .assured("the exchange accepts bytes");
    let status = status_after(&mut session).await;
    assert_eq!(status.code(), Code::Internal);
    assert!(status.message().contains("ClientMessage"), "{status:?}");
}

#[tokio::test]
async fn a_frame_above_the_servers_limit_ends_the_call_with_resource_exhausted() {
    let limits = limits();
    let server = serve(limits).await;
    let mut session = open(&server, RawCodec { limits }).await;
    let oversized = command(1, &"x".repeat(4096))
        .encode(&SessionLimits::DEFAULT)
        .assured("the request fits the default limits");
    session
        .requests
        .send(oversized.into_bytes())
        .await
        .assured("the exchange accepts bytes");
    let status = status_after(&mut session).await;
    assert_eq!(status.code(), Code::ResourceExhausted);
}

#[tokio::test]
async fn an_undecodable_request_is_rejected_and_the_session_continues() {
    let limits = limits();
    let server = serve(limits).await;
    let mut session = open(&server, RawCodec { limits }).await;

    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let domain = builder.create_string("not a domain");
    let select = raw_frames::select_domain(&mut builder, domain);
    let frame = raw_frames::finish_client(builder, 41, select);
    session
        .requests
        .send(frame)
        .await
        .assured("the exchange accepts bytes");
    let valid = ClientMessage {
        request_id: request_id(42),
        request: ClientRequest::ListDomains,
    }
    .encode(&limits)
    .assured("a request fits");
    session
        .requests
        .send(valid.into_bytes())
        .await
        .assured("the exchange accepts bytes");

    assert!(matches!(
        next_message(&mut session.replies).await,
        ServerMessage::Event(ServerEvent::Notice(_))
    ));
    let ServerMessage::Reply(rejected) = next_message(&mut session.replies).await else {
        panic!("the undecodable request is answered");
    };
    assert_eq!(rejected.request_id, request_id(41));
    assert_eq!(
        rejected.body,
        ReplyBody::Rejected(RequestRejected {
            rejection: RequestRejection::InvalidRequest,
            field: Some("SelectDomainRequest.domain".to_string()),
            message: "`SelectDomainRequest.domain` is not a valid name".to_string(),
        })
    );
    let ServerMessage::Reply(listed) = next_message(&mut session.replies).await else {
        panic!("the following request is answered");
    };
    assert_eq!(listed.request_id, request_id(42));
    assert!(matches!(listed.body, ReplyBody::DomainList(_)));
}

/// Builds frames the typed encoders refuse to produce.
mod raw_frames {
    use bytes::Bytes;
    use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

    /// The field layout of the schema's `SelectDomainRequest` and `ClientMessage` tables.
    const SELECT_DOMAIN_DOMAIN: u16 = 4;
    const CLIENT_MESSAGE_REQUEST_ID: u16 = 4;
    const CLIENT_MESSAGE_REQUEST_TYPE: u16 = 6;
    const CLIENT_MESSAGE_REQUEST: u16 = 8;
    const SELECT_DOMAIN_REQUEST: u8 = 4;

    pub(super) fn select_domain(
        builder: &mut FlatBufferBuilder<'static>,
        domain: WIPOffset<&'static str>,
    ) -> WIPOffset<UnionWIPOffset> {
        let table = builder.start_table();
        builder.push_slot_always(SELECT_DOMAIN_DOMAIN, domain);
        builder.end_table(table).as_union_value()
    }

    pub(super) fn finish_client(
        mut builder: FlatBufferBuilder<'static>,
        request_id: u64,
        request: WIPOffset<UnionWIPOffset>,
    ) -> Bytes {
        let table = builder.start_table();
        builder.push_slot_always(CLIENT_MESSAGE_REQUEST_ID, request_id);
        builder.push_slot_always(CLIENT_MESSAGE_REQUEST_TYPE, SELECT_DOMAIN_REQUEST);
        builder.push_slot_always(CLIENT_MESSAGE_REQUEST, request);
        let root = builder.end_table(table);
        builder.finish(root, Some("NXCM"));
        Bytes::copy_from_slice(builder.finished_data())
    }
}

#[tokio::test]
async fn an_upload_stream_is_answered_by_one_reply() {
    let limits = limits();
    let server = serve(limits).await;
    let mut client = tonic::client::Grpc::new(channel(&server).await);
    client.ready().await.assured("the channel becomes ready");

    let archive = (0..=255_u8).cycle().take(5000).collect::<Vec<_>>();
    let upload_identity = ResourceUploadIdentity::parse("upload-7").assured("a valid identity");
    let start = UploadStart {
        request_id: request_id(7),
        domain: nervix_models::DomainName::parse("tenant").assured("a valid domain"),
        resource: nervix_models::ResourceName::parse("model").assured("a valid resource"),
        upload_identity: upload_identity.clone(),
        total_bytes: NonZeroU64::new(5000).assured("a non-zero size"),
    };
    let mut frames = vec![start.encode(&limits).assured("a start fits")];
    for chunk in archive.chunks(1500) {
        frames.push(UploadChunk::encode(chunk, &limits).assured("a chunk fits"));
    }
    let response = client
        .client_streaming(
            Request::new(tokio_stream::iter(frames)),
            http::uri::PathAndQuery::from_static(UPLOAD_RESOURCE_PATH),
            ClientUploadCodec::new(limits),
        )
        .await
        .assured("the upload is answered");
    let reply = UploadReply::decode(response.get_ref()).assured("the reply decodes");
    assert_eq!(reply.request_id, Some(request_id(7)));
    assert_eq!(reply.message, "received 5000 bytes");
    assert_eq!(
        reply.disposition,
        UploadDisposition::Installed {
            upload_identity,
            version: NonZeroU64::new(7).assured("a non-zero version"),
            origin: OutcomeOrigin::Executed,
        }
    );
}

#[tokio::test]
async fn an_unknown_method_is_unimplemented() {
    let limits = limits();
    let server = serve(limits).await;
    let mut client = tonic::client::Grpc::new(channel(&server).await);
    client.ready().await.assured("the channel becomes ready");
    let error = client
        .unary(
            Request::new(
                ClientMessage {
                    request_id: request_id(1),
                    request: ClientRequest::ListDomains,
                }
                .encode(&limits)
                .assured("a request fits"),
            ),
            http::uri::PathAndQuery::from_static("/nervix.session.Session/Unknown"),
            ClientExchangeCodec::new(limits),
        )
        .await
        .expect_err("the service serves two methods");
    assert_eq!(error.code(), Code::Unimplemented);
}
