//! The client against an in-process session server over a real gRPC connection.
//!
//! The server is a hand-written tonic service that routes the two session methods itself, exactly
//! as a transport integrates the session protocol. Every exchange the client opens is handed to
//! the test, which plays the server's side inline: it reads the requests the client sends and
//! answers them in whatever order and shape the behavior under test needs. Uploads are answered by
//! a fixed handler that installs whatever archive arrives whole.

use std::{
    convert::Infallible,
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClusterNodeName, CommandExecutionReference, DomainPace, DomainStatus, FieldName, ParseAsType,
    RelayName, SchemaField, SubscriptionName,
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{ClientStreamingService, Grpc, NamedService, StreamingService},
    transport::Server,
};
use triomphe::Arc;

use crate::{
    Client, CommandDisposition, DomainName, Leadership, OutcomeOrigin, ResourceUploadIdentity,
    ResourceUploadOutcome, SubscriptionEvent, SubscriptionRequest,
    wire::{
        ClientFrame, ClientMessage, ClientRequest, DomainInfo, DomainList, DomainsObserved,
        EncodedFrame, LeaderRedirect, LeadershipObserved, NoticeLevel, Reply, ReplyBody,
        ReplyDelivery, RequestId, RowSchema, ServerFrame, ServerNotice, SessionLimitSettings,
        SessionLimits, SubscribeDisposition, SubscribeOutcome, SubscriptionEndReason,
        SubscriptionEnded, SubscriptionHandle, SubscriptionOpened, SubscriptionRowsEncoder,
        SubscriptionType, UploadDisposition, UploadFailure, UploadFrame, UploadMessage,
        UploadReply, UploadReplyFrame, UploadStart, VerifiedFrame,
        grpc::{
            EXCHANGE_PATH, SERVICE_NAME, ServerExchangeCodec, ServerUploadCodec,
            UPLOAD_RESOURCE_PATH,
        },
    },
};

/// Every wait in these tests ends when its condition holds, so the bound only has to be generous.
const DEADLINE: Duration = Duration::from_secs(30);

fn limits() -> SessionLimits {
    SessionLimits::DEFAULT
}

/// Limits whose small frames make a server transfer any sizeable reply in parts.
fn small_frames() -> SessionLimits {
    let defaults = SessionLimits::DEFAULT;
    let size = |value: usize| NonZeroUsize::new(value).assured("a non-zero test limit");
    SessionLimits::try_from(SessionLimitSettings {
        frame_bytes: size(2048),
        transfer_bytes: size(defaults.transfer_bytes()),
        nesting_depth: size(defaults.nesting_depth()),
        collection_entries: size(defaults.collection_entries()),
        string_bytes: size(defaults.string_bytes()),
    })
    .assured("the test limits pass their checks")
}

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test domain name is valid")
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test node name is valid")
}

fn tenant_domains() -> Vec<DomainInfo> {
    vec![DomainInfo {
        domain: domain("tenant"),
        status: DomainStatus::Running,
        pace: DomainPace::Unpaced,
    }]
}

fn command_outcome(
    execution_reference: &CommandExecutionReference,
    disposition: CommandDisposition,
    message: &str,
) -> ReplyBody {
    ReplyBody::Command(Box::new(crate::wire::CommandOutcome {
        execution_reference: execution_reference.clone(),
        origin: OutcomeOrigin::Executed,
        disposition,
        message: message.to_string(),
        diagnostics: Vec::new(),
        statements: Vec::new(),
        transaction: None,
        transaction_admission: None,
        inspection: None,
    }))
}

fn completed() -> CommandDisposition {
    CommandDisposition::Completed {
        already_existed: false,
    }
}

async fn within_deadline<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(DEADLINE, future)
        .await
        .assured("the awaited step completes within the generous test deadline")
}

/// The server's side of one exchange the client opened.
struct ServerExchange {
    requests: Streaming<VerifiedFrame<ClientFrame>>,
    frames: mpsc::Sender<Result<EncodedFrame<ServerFrame>, Status>>,
}

impl ServerExchange {
    /// The next request the client sends, decoded.
    async fn next_request(&mut self) -> ClientMessage {
        let frame = within_deadline(self.requests.message())
            .await
            .assured("the exchange stays open")
            .assured("the client sends another request");
        ClientMessage::decode(&frame).assured("a request the client encoded decodes")
    }

    async fn send(&self, frame: EncodedFrame<ServerFrame>) {
        self.frames
            .send(Ok(frame))
            .await
            .assured("the client keeps reading its exchange");
    }

    /// Answers a request, in parts when the reply does not fit one frame of `limits`.
    async fn reply(&self, request_id: RequestId, body: ReplyBody, limits: &SessionLimits) {
        let delivery = Reply { request_id, body }
            .encode(limits)
            .assured("a test reply fits the transfer limit");
        match delivery {
            ReplyDelivery::Frame(frame) => self.send(frame).await,
            ReplyDelivery::Transfer(parts) => {
                for part in parts {
                    tokio::task::consume_budget().await;
                    self.send(part).await;
                }
            }
        }
    }
}

/// An upload the fixed upload handler received.
struct ReceivedUpload {
    start: UploadStart,
    archive: Vec<u8>,
}

#[derive(Clone)]
struct SessionService {
    exchanges: mpsc::Sender<ServerExchange>,
    uploads: mpsc::Sender<ReceivedUpload>,
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
        let service = self.clone();
        match request.uri().path() {
            EXCHANGE_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(ServerExchangeCodec::new(limits()));
                let exchange = OpenExchange {
                    exchanges: service.exchanges,
                };
                Ok(grpc.streaming(exchange, request).await)
            }),
            UPLOAD_RESOURCE_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(ServerUploadCodec::new(limits()));
                let upload = InstallUpload {
                    uploads: service.uploads,
                };
                Ok(grpc.client_streaming(upload, request).await)
            }),
            _ => Box::pin(async move {
                Ok(Status::unimplemented("the session serves two methods").into_http())
            }),
        }
    }
}

/// Hands every exchange the client opens to the test.
struct OpenExchange {
    exchanges: mpsc::Sender<ServerExchange>,
}

impl StreamingService<VerifiedFrame<ClientFrame>> for OpenExchange {
    type Response = EncodedFrame<ServerFrame>;
    type ResponseStream = ReceiverStream<Result<EncodedFrame<ServerFrame>, Status>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<ClientFrame>>>) -> Self::Future {
        let exchanges = self.exchanges.clone();
        Box::pin(async move {
            let (frames, outbound) = mpsc::channel(64);
            let exchange = ServerExchange {
                requests: request.into_inner(),
                frames,
            };
            if exchanges.send(exchange).await.is_err() {
                return Err(Status::unavailable("the test takes no more exchanges"));
            }
            Ok(Response::new(ReceiverStream::new(outbound)))
        })
    }
}

/// Installs an upload whose chunks add up to its declared size as version 7.
struct InstallUpload {
    uploads: mpsc::Sender<ReceivedUpload>,
}

impl ClientStreamingService<VerifiedFrame<UploadFrame>> for InstallUpload {
    type Response = EncodedFrame<UploadReplyFrame>;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<UploadFrame>>>) -> Self::Future {
        let uploads = self.uploads.clone();
        Box::pin(async move {
            let mut inbound = request.into_inner();
            let Some(first) = inbound.message().await? else {
                return Err(Status::invalid_argument("an empty upload stream"));
            };
            let decoded = UploadMessage::decode(&first)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let UploadMessage::Start(start) = decoded else {
                return Err(Status::invalid_argument(
                    "an upload starts with its metadata",
                ));
            };
            let mut archive = Vec::new();
            while let Some(frame) = inbound.message().await? {
                tokio::task::consume_budget().await;
                let decoded = UploadMessage::decode(&frame)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let UploadMessage::Chunk(chunk) = decoded else {
                    return Err(Status::invalid_argument("chunks follow the upload start"));
                };
                archive.extend_from_slice(chunk.bytes());
            }
            let received = u64::try_from(archive.len()).assured("a test archive fits u64");
            let disposition = if received == start.total_bytes.get() {
                UploadDisposition::Installed {
                    upload_identity: start.upload_identity.clone(),
                    version: NonZeroU64::new(7).assured("a non-zero version"),
                    origin: OutcomeOrigin::Executed,
                }
            } else {
                UploadDisposition::Failed {
                    upload_identity: Some(start.upload_identity.clone()),
                    failure: UploadFailure::SizeMismatch,
                    assigned_version: None,
                }
            };
            let reply = UploadReply {
                request_id: Some(start.request_id),
                disposition,
                message: format!("received {received} bytes"),
                diagnostics: Vec::new(),
            };
            let reply = reply
                .encode(&limits())
                .map_err(|error| Status::internal(error.to_string()))?;
            if uploads
                .send(ReceivedUpload { start, archive })
                .await
                .is_err()
            {
                return Err(Status::unavailable("the test takes no more uploads"));
            }
            Ok(Response::new(reply))
        })
    }
}

struct TestServer {
    address: SocketAddr,
    exchanges: mpsc::Receiver<ServerExchange>,
    uploads: mpsc::Receiver<ReceivedUpload>,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .assured("the loopback interface accepts a listener");
        let address = listener
            .local_addr()
            .assured("a bound listener has an address");
        let (exchange_sender, exchanges) = mpsc::channel(4);
        let (upload_sender, uploads) = mpsc::channel(4);
        let service = SessionService {
            exchanges: exchange_sender,
            uploads: upload_sender,
        };
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .assured("the test server serves until it is aborted");
        });
        Self {
            address,
            exchanges,
            uploads,
            task,
        }
    }

    /// Connects a client whose session starts in the `tenant` domain.
    async fn connect(&self) -> Client {
        within_deadline(Client::connect(
            format!("http://{}", self.address),
            Some(domain("tenant")),
        ))
        .await
        .assured("the client connects to the test server")
    }

    async fn next_exchange(&mut self) -> ServerExchange {
        within_deadline(self.exchanges.recv())
            .await
            .assured("the server keeps handing exchanges to the test")
    }
}

#[tokio::test]
async fn replies_reach_their_requests_in_whatever_order_they_arrive() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    exchange
        .send(
            LeadershipObserved {
                leadership: Leadership::ServingNode(node("node-1")),
            }
            .encode(&limits())
            .assured("a leadership observation fits a frame"),
        )
        .await;
    exchange
        .send(
            DomainsObserved {
                domains: tenant_domains(),
            }
            .encode(&limits())
            .assured("a domain observation fits a frame"),
        )
        .await;

    let listing_client = client.clone();
    let listing = tokio::spawn(async move { listing_client.list_domains().await });
    let command_client = client.clone();
    let command = tokio::spawn(async move { command_client.execute("SHOW CLUSTER STATUS;").await });
    let first = exchange.next_request().await;
    let second = exchange.next_request().await;
    assert_ne!(first.request_id, second.request_id);

    // The request that arrived last is answered first.
    for request in [second, first] {
        let body = match request.request {
            ClientRequest::ListDomains => ReplyBody::DomainList(DomainList {
                domains: tenant_domains(),
            }),
            ClientRequest::Command(command) => {
                assert_eq!(command.query, "SHOW CLUSTER STATUS;");
                assert_eq!(command.domain, Some(domain("tenant")));
                command_outcome(
                    &command.execution_reference,
                    completed(),
                    "cluster is healthy",
                )
            }
            other => panic!("the client sent an unexpected request: {other:?}"),
        };
        exchange.reply(request.request_id, body, &limits()).await;
    }

    let outcome = within_deadline(command)
        .await
        .assured("the command task completes")
        .assured("the command is answered");
    assert!(outcome.succeeded());
    assert_eq!(outcome.message, "cluster is healthy");
    assert_eq!(outcome.origin, Some(OutcomeOrigin::Executed));
    let domains = within_deadline(listing)
        .await
        .assured("the listing task completes")
        .assured("the listing is answered");
    assert_eq!(domains, tenant_domains());

    // Both observations preceded the replies on the exchange, so they are already current.
    assert_eq!(
        client.leadership(),
        Some(Leadership::ServingNode(node("node-1")))
    );
    let observed = within_deadline(client.next_domain_list())
        .await
        .assured("the observed domain list is current");
    assert_eq!(observed, tenant_domains());
}

#[tokio::test]
async fn a_reply_larger_than_a_frame_arrives_in_parts() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let command_client = client.clone();
    let command = tokio::spawn(async move { command_client.execute("SHOW DOMAINS;").await });
    let request = exchange.next_request().await;
    let ClientRequest::Command(command_request) = request.request else {
        panic!("the statement is sent as a command");
    };
    let message = "domain tenant is running\n".repeat(1000);
    let delivery = Reply {
        request_id: request.request_id,
        body: command_outcome(&command_request.execution_reference, completed(), &message),
    }
    .encode(&small_frames())
    .assured("the reply fits the transfer limit");
    let ReplyDelivery::Transfer(parts) = delivery else {
        panic!("a reply far larger than a 2 KiB frame is transferred in parts");
    };
    assert!(parts.len() > 2);
    for (index, part) in parts.enumerate() {
        tokio::task::consume_budget().await;
        exchange.send(part).await;
        if index == 0 {
            // An unsolicited message may arrive between the parts of a reply.
            exchange
                .send(
                    ServerNotice {
                        level: NoticeLevel::Info,
                        message: "raft transition: state=Leader".to_string(),
                    }
                    .encode(&limits())
                    .assured("a notice fits a frame"),
                )
                .await;
        }
    }

    let outcome = within_deadline(command)
        .await
        .assured("the command task completes")
        .assured("the transferred reply completes the command");
    assert!(outcome.succeeded());
    assert_eq!(outcome.message, message);
    let notice = within_deadline(client.next_server_event())
        .await
        .assured("the notice between the parts is delivered");
    assert_eq!(notice.level, NoticeLevel::Info);
    assert_eq!(notice.message, "raft transition: state=Leader");
}

fn orders_schema() -> RowSchema {
    let field = |name: &str, ty: ParseAsType| SchemaField {
        name: FieldName::parse(name).assured("the test field name is valid"),
        ty,
        optional: false,
        sensitive: false,
    };
    RowSchema {
        fields: vec![
            field("name", ParseAsType::String),
            field("id", ParseAsType::U64),
        ],
        branch: None,
    }
}

fn subscription(generation: u64) -> SubscriptionHandle {
    SubscriptionHandle {
        name: SubscriptionName::parse("live").assured("the test subscription name is valid"),
        generation: NonZeroU64::new(generation).assured("test generations are non-zero"),
    }
}

fn rows_frame(handle: SubscriptionHandle, rows: &[(u64, &str)]) -> EncodedFrame<ServerFrame> {
    let mut batch = SubscriptionRowsEncoder::unbranched(handle, &limits())
        .assured("an unbranched batch starts within the limits");
    for (id, name) in rows {
        batch
            .push_row(|cells| {
                cells.push_string(name)?;
                cells.push_u64(*id)
            })
            .assured("a test row fits the limits");
    }
    batch.finish().assured("a test batch finishes")
}

#[tokio::test]
async fn subscription_rows_render_against_the_schema_the_subscription_opened_with() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let subscribe_client = client.clone();
    let subscribe = tokio::spawn(async move {
        subscribe_client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let request = exchange.next_request().await;
    let ClientRequest::Subscribe(subscribe_request) = request.request else {
        panic!("a CREATE SUBSCRIPTION statement is sent as a subscribe request");
    };
    assert_eq!(subscribe_request.domain, domain("tenant"));
    assert_eq!(
        subscribe_request.statement,
        "CREATE SUBSCRIPTION live TO orders;"
    );
    assert_eq!(subscribe_request.subscription_type, SubscriptionType::Row);
    let opened = SubscribeOutcome {
        disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
            subscription: subscription(1),
            domain: domain("tenant"),
            relay: RelayName::parse("orders").assured("the test relay name is valid"),
            subscription_type: SubscriptionType::Row,
            schema: orders_schema(),
        })),
        message: "subscription 'live' opened".to_string(),
        diagnostics: Vec::new(),
    };
    exchange
        .reply(request.request_id, ReplyBody::Subscribe(opened), &limits())
        .await;
    // Rows of a generation the client does not hold are dropped.
    exchange
        .send(rows_frame(subscription(2), &[(9, "stale")]))
        .await;
    exchange
        .send(rows_frame(subscription(1), &[(1, "first"), (2, "second")]))
        .await;
    exchange
        .send(
            SubscriptionEnded {
                subscription: subscription(1),
                reason: SubscriptionEndReason::RelayChanged,
                message: "relay 'orders' was redefined".to_string(),
            }
            .encode(&limits())
            .assured("a subscription end fits a frame"),
        )
        .await;

    let outcome = within_deadline(subscribe)
        .await
        .assured("the subscribe task completes")
        .assured("the subscription is answered");
    assert!(outcome.succeeded());
    assert_eq!(outcome.message, "subscription 'live' opened");
    let opened = outcome
        .subscription
        .verified("an opened subscription is reported");
    assert_eq!(opened.subscription, subscription(1));

    let SubscriptionEvent::Rows(rows) = within_deadline(client.next_subscription())
        .await
        .assured("the rows of the opened subscription are delivered")
    else {
        panic!("the first event of the subscription is its rows");
    };
    assert_eq!(rows.relay.as_str(), "orders");
    assert_eq!(rows.rows.subscription(), &subscription(1));
    assert_eq!(
        rows.display_lines()
            .assured("the rows follow the schema the subscription opened with"),
        [
            "{\"id\":1,\"name\":\"first\"}",
            "{\"id\":2,\"name\":\"second\"}"
        ]
    );
    let SubscriptionEvent::Ended(ended) = within_deadline(client.next_subscription())
        .await
        .assured("the end of the subscription is delivered")
    else {
        panic!("the subscription's end follows its rows");
    };
    assert_eq!(ended.subscription, subscription(1));
}

#[tokio::test]
async fn a_command_waits_for_an_election_and_is_sent_again_with_its_reference() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let command_client = client.clone();
    let command =
        tokio::spawn(async move { command_client.execute("CREATE DOMAIN orders;").await });
    let first = exchange.next_request().await;
    let ClientRequest::Command(first_command) = first.request else {
        panic!("the statement is sent as a command");
    };
    exchange
        .reply(
            first.request_id,
            command_outcome(
                &first_command.execution_reference,
                CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
                "no leader is known",
            ),
            &limits(),
        )
        .await;

    let retry = exchange.next_request().await;
    let ClientRequest::Command(retry_command) = retry.request else {
        panic!("the command is sent again once the election may have settled");
    };
    assert!(
        retry.request_id > first.request_id,
        "a retry takes a new request identity"
    );
    assert_eq!(
        retry_command.execution_reference, first_command.execution_reference,
        "a retry repeats the execution reference"
    );
    exchange
        .reply(
            retry.request_id,
            command_outcome(
                &retry_command.execution_reference,
                completed(),
                "domain 'orders' created",
            ),
            &limits(),
        )
        .await;

    let outcome = within_deadline(command)
        .await
        .assured("the command task completes")
        .assured("the retried command is answered");
    assert!(outcome.succeeded());
    assert_eq!(outcome.message, "domain 'orders' created");
    assert_eq!(
        outcome.execution_reference,
        Some(first_command.execution_reference)
    );
}

fn write_file(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).assured("the test directory accepts files");
}

#[tokio::test]
async fn an_upload_streams_its_archive_and_reports_the_installed_version() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let _exchange = server.next_exchange().await;
    let directory = tempfile::tempdir().assured("a temporary directory is available");
    write_file(&directory.path().join("model.onnx"), &[7_u8; 1000]);
    std::fs::create_dir(directory.path().join("weights"))
        .assured("the test directory accepts directories");
    // Larger than one upload chunk, so the archive is streamed in several.
    write_file(
        &directory.path().join("weights").join("layer.bin"),
        &[3_u8; 100_000],
    );
    let identity =
        ResourceUploadIdentity::parse("upload-1").assured("the test upload identity is valid");
    let progress = Arc::new(AtomicU64::new(0));

    let reported = progress.clone();
    let outcome = within_deadline(client.upload_resource_from_directory_with_identity(
        "model",
        directory.path(),
        identity.clone(),
        move |bytes| {
            reported.fetch_add(bytes, Ordering::Relaxed);
        },
    ))
    .await
    .assured("the upload is answered");

    let received = within_deadline(server.uploads.recv())
        .await
        .assured("the server received the upload");
    assert_eq!(received.start.resource.as_str(), "model");
    assert_eq!(received.start.domain, domain("tenant"));
    assert_eq!(received.start.upload_identity, identity);
    let archive_bytes = u64::try_from(received.archive.len()).assured("a test archive fits u64");
    assert_eq!(received.start.total_bytes.get(), archive_bytes);
    assert!(archive_bytes > 101_000, "the archive holds both files");
    assert_eq!(progress.load(Ordering::Relaxed), archive_bytes);

    assert!(outcome.succeeded());
    assert_eq!(outcome.message, format!("received {archive_bytes} bytes"));
    assert_eq!(outcome.execution_reference, None);
    assert_eq!(
        outcome.resource_upload,
        Some(ResourceUploadOutcome {
            identity,
            version: NonZeroU64::new(7),
            origin: Some(OutcomeOrigin::Executed),
            failure: None,
        })
    );
}
