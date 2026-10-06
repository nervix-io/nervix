//! The client against an in-process session server over a real gRPC connection.
//!
//! The server is a hand-written tonic service that routes the two session methods itself, exactly
//! as a transport integrates the session protocol. Every exchange the client opens is handed to
//! the test, which plays the server's side inline: it reads the requests the client sends and
//! answers them in whatever order and shape the behavior under test needs. Uploads are answered by
//! a fixed handler that installs whatever archive arrives whole.

use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    task::{Context, Poll},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_dns::{DnsConfiguration, DnsLookupError, DnsLookupFailure, DnsResolver, NameServers};
use nervix_models::{
    ClusterNodeName, CommandExecutionReference, DomainPace, DomainStatus, FieldName, ParseAsType,
    PlacementPolicy, RelayName, SchemaField, SubscriptionName, TransactionInspection,
    TransactionInspectionRejection, TransactionInspectionTarget,
};
use nervix_primitives::{
    net::TcpListener,
    stream::wrappers::{ReceiverStream, TcpListenerStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    task::JoinHandle,
};
use nervix_recovery::Discarded as _;
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{ClientStreamingService, Grpc, NamedService, StreamingService},
    transport::Server,
};
use url::Url;

#[cfg(feature = "autocomplete")]
use crate::wire::{SuggestOutcome, Suggestion, SuggestionKind, SuggestionStatus, TextEdit};
use crate::{
    Client, ClientError, CommandDisposition, ConnectDns, ConnectOptions, DomainName, Leadership,
    OutcomeOrigin, ResourceUploadIdentity, ResourceUploadOutcome, SubscriptionEvent,
    SubscriptionLifecycle, SubscriptionRequest,
    wire::{
        Choice, ChoiceLookupRequest, ChoiceOutcome, ChoicePresentation, ChoiceSelection,
        ChoiceStatus, ChoiceTarget, ChoiceValue, ClientFrame, ClientMessage, ClientRequest,
        DomainInfo, DomainList, DomainPaceChoice, DomainsObserved, EncodedFrame, InspectionOutcome,
        LeaderEndpoints, LeaderRedirect, LeadershipObserved, NoticeLevel, Reply, ReplyBody,
        ReplyDelivery, RequestId, RowSchema, ServerFrame, ServerNotice, SessionLimitSettings,
        SessionLimits, SubscribeDisposition, SubscribeOutcome, SubscriptionEndReason,
        SubscriptionEnded, SubscriptionHandle, SubscriptionOpened, SubscriptionRowsEncoder,
        SubscriptionType, UploadDisposition, UploadFailure, UploadFrame, UploadMessage,
        UploadReply, UploadReplyFrame, UploadStart, VerifiedFrame, WireEncodeError,
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
        start_version: 1,
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
        wasm_state: None,
        resource: None,
        backup: None,
        restore: None,
    }))
}

fn completed() -> CommandDisposition {
    CommandDisposition::Completed {
        already_existed: false,
    }
}

async fn within_deadline<F: Future>(future: F) -> F::Output {
    nervix_primitives::time::timeout(DEADLINE, future)
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
                    nervix_primitives::task::consume_budget().await;
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
    fail_upload_replies: Arc<AtomicU64>,
    upload_reply_mode: Arc<Mutex<Option<UploadReplyMode>>>,
}

enum UploadReplyMode {
    WrongIdentity,
    WrongRequestId,
    Redirect(Url),
    Reject,
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
                    fail_upload_replies: service.fail_upload_replies,
                    upload_reply_mode: service.upload_reply_mode,
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
    fail_upload_replies: Arc<AtomicU64>,
    upload_reply_mode: Arc<Mutex<Option<UploadReplyMode>>>,
}

impl ClientStreamingService<VerifiedFrame<UploadFrame>> for InstallUpload {
    type Response = EncodedFrame<UploadReplyFrame>;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<UploadFrame>>>) -> Self::Future {
        let uploads = self.uploads.clone();
        let fail_upload_replies = self.fail_upload_replies.clone();
        let upload_reply_mode = self.upload_reply_mode.clone();
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
                nervix_primitives::task::consume_budget().await;
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
            let mut reply = UploadReply {
                request_id: Some(start.request_id),
                disposition,
                message: format!("received {received} bytes"),
                diagnostics: Vec::new(),
            };
            let mode = upload_reply_mode.lock().await.take();
            let reject = matches!(mode.as_ref(), Some(UploadReplyMode::Reject));
            match mode {
                Some(UploadReplyMode::WrongIdentity) => {
                    if let UploadDisposition::Installed {
                        upload_identity, ..
                    } = &mut reply.disposition
                    {
                        *upload_identity = ResourceUploadIdentity::parse("another-upload")
                            .assured("the test upload identity is valid");
                    }
                }
                Some(UploadReplyMode::WrongRequestId) => {
                    reply.request_id = Some(RequestId::new(
                        NonZeroU64::new(2).assured("the test request identity is non-zero"),
                    ));
                }
                Some(UploadReplyMode::Redirect(leader)) => {
                    reply.disposition = UploadDisposition::NotLeader(LeaderRedirect {
                        leader: Some(LeaderEndpoints {
                            node: node("leader"),
                            grpc_uri: Some(leader),
                            web_console_uri: None,
                        }),
                    });
                }
                Some(UploadReplyMode::Reject) | None => {}
            }
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
            if reject {
                return Err(Status::permission_denied("the upload is not authorized"));
            }
            #[allow(deprecated)] // until try_update is stabilized
            if fail_upload_replies
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(Status::unavailable("the installed upload's reply was lost"));
            }
            Ok(Response::new(reply))
        })
    }
}

struct TestServer {
    address: SocketAddr,
    exchanges: mpsc::Receiver<ServerExchange>,
    uploads: mpsc::Receiver<ReceivedUpload>,
    fail_upload_replies: Arc<AtomicU64>,
    upload_reply_mode: Arc<Mutex<Option<UploadReplyMode>>>,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestServer {
    async fn start() -> Self {
        Self::start_at(SocketAddr::from(([127, 0, 0, 1], 0))).await
    }

    /// Starts a server listening on `address`, a loopback address with a free port or port 0.
    async fn start_at(address: SocketAddr) -> Self {
        let listener = TcpListener::bind(address)
            .await
            .assured("the loopback interface accepts a listener");
        let address = listener
            .local_addr()
            .assured("a bound listener has an address");
        let (exchange_sender, exchanges) = mpsc::channel(4);
        let (upload_sender, uploads) = mpsc::channel(4);
        let fail_upload_replies = Arc::new(AtomicU64::new(0));
        let upload_reply_mode = Arc::new(Mutex::new(None));
        let service = SessionService {
            exchanges: exchange_sender,
            uploads: upload_sender,
            fail_upload_replies: fail_upload_replies.clone(),
            upload_reply_mode: upload_reply_mode.clone(),
        };
        let task = nervix_primitives::task::spawn(async move {
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
            fail_upload_replies,
            upload_reply_mode,
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

    /// Stops serving and waits until the listener is closed, so a client connecting afterwards is
    /// refused.
    async fn stop(mut self) {
        self.task.abort();
        (&mut self.task)
            .await
            .discarded("the server task only ever ends by being aborted");
    }
}

#[nervix_primitives::test]
async fn native_session_uses_ordered_fixture_addresses_and_original_host() {
    let mut server = TestServer::start().await;
    let authority = DnsAuthority::start_on_loopback()
        .await
        .assured("the fixture can bind a local DNS port");
    let name = "session.nervix.test";
    authority.set(
        name,
        DnsAnswer::Addresses {
            addresses: vec![
                IpAddr::V4(Ipv4Addr::new(127, 0, 5, 3)),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ],
            ttl: Duration::from_secs(1),
        },
    );
    let files = tempfile::tempdir().assured("a DNS fixture directory can be created");
    let resolver_configuration = files.path().join("resolv.conf");
    let hosts_file = files.path().join("hosts");
    std::fs::write(
        &resolver_configuration,
        "search --\noptions ndots:1 timeout:1 attempts:1\n",
    )
    .assured("the fixture resolver configuration can be written");
    std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
    let dns = DnsResolver::load(DnsConfiguration {
        resolver_configuration,
        hosts_file,
        name_servers: NameServers::Explicit(vec![authority.address()]),
    })
    .await
    .assured("the fixture DNS configuration is valid");
    let options = ConnectOptions {
        dns: ConnectDns::Resolver(dns),
        connect_timeout: Duration::from_secs(10),
        ..ConnectOptions::default()
    };
    let endpoint = format!("http://{name}:{}", server.address.port());
    let _client = within_deadline(Client::connect_with_options(endpoint, None, options))
        .await
        .assured("the second DNS answer reaches the session server");
    let _exchange = server.next_exchange().await;
    assert!(authority.questions_for(name) > 0);
}

#[nervix_primitives::test]
async fn native_session_connection_deadline_cancels_a_silent_dns_lookup() {
    let authority = DnsAuthority::start_on_loopback()
        .await
        .assured("the fixture can bind a local DNS port");
    let name = "silent-session.nervix.test";
    authority.set(name, DnsAnswer::Silent);
    let files = tempfile::tempdir().assured("a DNS fixture directory can be created");
    let resolver_configuration = files.path().join("resolv.conf");
    let hosts_file = files.path().join("hosts");
    std::fs::write(
        &resolver_configuration,
        "search --\noptions ndots:1 timeout:20 attempts:1\n",
    )
    .assured("the fixture resolver configuration can be written");
    std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
    let dns = DnsResolver::load(DnsConfiguration {
        resolver_configuration,
        hosts_file,
        name_servers: NameServers::Explicit(vec![authority.address()]),
    })
    .await
    .assured("the fixture DNS configuration is valid");
    let options = ConnectOptions {
        dns: ConnectDns::Resolver(dns),
        connect_timeout: Duration::from_millis(200),
        retry_timeout: Duration::from_secs(1),
        ..ConnectOptions::default()
    };
    let result = nervix_primitives::time::timeout(
        Duration::from_secs(10),
        Client::connect_with_options(format!("http://{name}:4317"), None, options),
    )
    .await
    .assured(
        "the client's connection deadline cancels DNS before the authority's 20-second silence",
    );
    assert!(matches!(result, Err(ClientError::ConnectServer(_))));
    assert!(authority.questions_for(name) > 0);
}

#[nervix_primitives::test]
async fn native_session_connection_error_preserves_the_typed_dns_failure() {
    let authority = DnsAuthority::start_on_loopback()
        .await
        .assured("the fixture can bind a local DNS port");
    let name = "missing-session.nervix.test";
    authority.set(
        name,
        DnsAnswer::NameNotFound {
            negative_ttl: Duration::from_secs(1),
        },
    );
    let files = tempfile::tempdir().assured("a DNS fixture directory can be created");
    let resolver_configuration = files.path().join("resolv.conf");
    let hosts_file = files.path().join("hosts");
    std::fs::write(
        &resolver_configuration,
        "search --\noptions ndots:1 timeout:1 attempts:1\n",
    )
    .assured("the fixture resolver configuration can be written");
    std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
    let dns = DnsResolver::load(DnsConfiguration {
        resolver_configuration,
        hosts_file,
        name_servers: NameServers::Explicit(vec![authority.address()]),
    })
    .await
    .assured("the fixture DNS configuration is valid");
    let Err(error) = Client::connect_with_options(
        format!("http://{name}:4317"),
        None,
        ConnectOptions {
            dns: ConnectDns::Resolver(dns),
            connect_timeout: Duration::from_secs(2),
            ..ConnectOptions::default()
        },
    )
    .await
    else {
        panic!("the missing name cannot open a native session");
    };
    let ClientError::ConnectServer(connect_error) = error else {
        panic!("DNS resolution should be classified as a connection failure");
    };
    let lookup = DnsLookupError::find_in(&connect_error)
        .assured("the Tonic connection error retains the resolver's typed cause");
    assert_eq!(lookup.name(), name);
    assert_eq!(lookup.failure(), DnsLookupFailure::NameNotFound);
    assert!(authority.questions_for(name) > 0);
}

#[nervix_primitives::test]
async fn configured_seed_connects_when_the_primary_is_unavailable() {
    let mut seed = TestServer::start().await;
    let unused = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("loopback accepts a test listener");
    let primary = unused
        .local_addr()
        .assured("a bound listener has an address");
    drop(unused);
    let seed_url =
        Url::parse(&format!("http://{}", seed.address)).assured("the test seed is an HTTP origin");
    let options = ConnectOptions {
        seed_servers: vec![seed_url],
        connect_timeout: Duration::from_millis(250),
        ..ConnectOptions::default()
    };
    let client = within_deadline(Client::connect_with_options(
        format!("http://{primary}"),
        Some(domain("tenant")),
        options,
    ))
    .await
    .assured("the configured seed accepts the session");
    let mut exchange = seed.next_exchange().await;
    let listing_client = client.clone();
    let listing =
        nervix_primitives::task::spawn(async move { listing_client.list_domains().await });
    let request = exchange.next_request().await;
    assert!(matches!(request.request, ClientRequest::ListDomains));
    exchange
        .reply(
            request.request_id,
            ReplyBody::DomainList(DomainList {
                domains: tenant_domains(),
            }),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(listing)
            .await
            .assured("the listing task completes")
            .assured("the seed answers the listing"),
        tenant_domains()
    );
}

#[nervix_primitives::test]
async fn a_closed_session_recovers_through_a_configured_seed() {
    let mut primary = TestServer::start().await;
    let mut seed = TestServer::start().await;
    let seed_url =
        Url::parse(&format!("http://{}", seed.address)).assured("the test seed is an HTTP origin");
    let options = ConnectOptions {
        seed_servers: vec![seed_url],
        retry_timeout: Duration::from_secs(5),
        ..ConnectOptions::default()
    };
    let client = within_deadline(Client::connect_with_options(
        format!("http://{}", primary.address),
        Some(domain("tenant")),
        options,
    ))
    .await
    .assured("the primary accepts the session");
    let exchange = primary.next_exchange().await;
    let reading = client.clone();
    let notice = nervix_primitives::task::spawn(async move { reading.next_server_event().await });
    drop(exchange);

    let command_client = client.clone();
    let command = nervix_primitives::task::spawn(async move {
        command_client.execute("SHOW CLUSTER STATUS;").await
    });
    let mut recovered = seed.next_exchange().await;
    let request = recovered.next_request().await;
    let ClientRequest::Command(sent) = request.request else {
        panic!("the command is retried through the seed");
    };
    assert_eq!(sent.domain, Some(domain("tenant")));
    recovered
        .reply(
            request.request_id,
            command_outcome(&sent.execution_reference, completed(), "recovered"),
            &limits(),
        )
        .await;
    let outcome = within_deadline(command)
        .await
        .assured("the command task completes")
        .assured("the seed answers the command");
    assert!(outcome.succeeded());
    recovered
        .send(
            ServerNotice {
                level: NoticeLevel::Warning,
                message: "relay 'orders' is behind".to_string(),
            }
            .encode(&limits())
            .assured("a notice fits a frame"),
        )
        .await;
    let notice = within_deadline(notice)
        .await
        .assured("the notice task completes")
        .assured("the notice stream continues on the recovered session");
    assert_eq!(notice.level, NoticeLevel::Warning);
    assert_eq!(notice.message, "relay 'orders' is behind");
    let recovered_requests = client.inner.exchange.lock().await.requests();
    assert!(matches!(
        client
            .recover_session(crate::client::RecoveryMode::IfClosed)
            .await,
        Ok(crate::client::SessionRecovery::Ready)
    ));
    let current_requests = client.inner.exchange.lock().await.requests();
    assert!(
        Arc::ptr_eq(&recovered_requests, &current_requests),
        "a concurrent caller must keep the session already recovered by another caller"
    );
}

#[nervix_primitives::test]
async fn inspection_recovers_its_typed_reply_after_the_exchange_closes() {
    let mut primary = TestServer::start().await;
    let mut seed = TestServer::start().await;
    let seed_url =
        Url::parse(&format!("http://{}", seed.address)).assured("the test seed is an HTTP origin");
    let options = ConnectOptions {
        seed_servers: vec![seed_url],
        retry_timeout: Duration::from_secs(5),
        ..ConnectOptions::default()
    };
    let client = within_deadline(Client::connect_with_options(
        format!("http://{}", primary.address),
        Some(domain("tenant")),
        options,
    ))
    .await
    .assured("the primary accepts the session");
    let mut first_exchange = primary.next_exchange().await;
    let target = TransactionInspectionTarget::Transaction {
        transaction_id: "tx-1".to_string(),
    };
    let inspector = client.clone();
    let inspection =
        nervix_primitives::task::spawn(
            async move { inspector.inspect_transaction(target, None).await },
        );
    let first = first_exchange.next_request().await;
    assert!(matches!(
        first.request,
        ClientRequest::InspectTransaction(_)
    ));
    drop(first_exchange);

    let mut recovered_exchange = seed.next_exchange().await;
    let retried = recovered_exchange.next_request().await;
    let ClientRequest::InspectTransaction(request) = retried.request else {
        panic!("inspection must be retried on the recovered exchange");
    };
    assert_eq!(
        request.target,
        TransactionInspectionTarget::Transaction {
            transaction_id: "tx-1".to_string(),
        }
    );
    let rejected = InspectionOutcome::Rejected {
        rejection: TransactionInspectionRejection::TransactionNotFound,
        message: "transaction not found".to_string(),
    };
    recovered_exchange
        .reply(
            retried.request_id,
            ReplyBody::Inspection(rejected.clone()),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(inspection)
            .await
            .assured("the inspection task completes")
            .assured("the seed answers the inspection"),
        rejected
    );
}

#[nervix_primitives::test]
async fn typed_inspection_refreshes_the_attached_preview() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    client
        .adopt_transaction_status(super::open_transaction("tx-1", 0))
        .await;
    let mut exchange = server.next_exchange().await;
    let inspecting = client.clone();
    let task = nervix_primitives::task::spawn(async move {
        inspecting
            .inspect_transaction(
                TransactionInspectionTarget::Transaction {
                    transaction_id: "tx-1".to_string(),
                },
                None,
            )
            .await
    });
    let request = exchange.next_request().await;
    assert!(matches!(
        request.request,
        ClientRequest::InspectTransaction(_)
    ));
    let inspection = TransactionInspection {
        transaction: super::open_transaction("tx-1", 0),
        operation: None,
        report: super::empty_report(),
    };
    exchange
        .reply(
            request.request_id,
            ReplyBody::Inspection(InspectionOutcome::Inspected(Box::new(inspection.clone()))),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(task)
            .await
            .assured("the inspection task completes")
            .assured("the server returns the typed inspection"),
        InspectionOutcome::Inspected(Box::new(inspection))
    );
    assert_eq!(
        client.transaction_expectation().await.preview,
        Some(super::test_preview("tx-1", 0))
    );
}

#[nervix_primitives::test]
async fn attaching_a_transaction_adopts_its_domain_and_status() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    let attaching = client.clone();
    let task =
        nervix_primitives::task::spawn(async move { attaching.attach_transaction("tx-1").await });
    let request = exchange.next_request().await;
    let ClientRequest::AttachTransaction(sent) = request.request else {
        panic!("an attach request is sent");
    };
    assert_eq!(sent.transaction_id, "tx-1");
    let status = crate::TransactionStatus::new(
        "tx-1".to_string(),
        domain("bound"),
        crate::TransactionLifecycle::Open,
        crate::TransactionPosition::new(0),
        0,
    )
    .assured("a newly opened test transaction is consistent");
    exchange
        .reply(
            request.request_id,
            ReplyBody::Attach(crate::wire::AttachOutcome {
                disposition: crate::wire::AttachDisposition::Attached(status.clone()),
                message: "attached".to_string(),
                diagnostics: Vec::new(),
            }),
            &limits(),
        )
        .await;
    assert!(
        within_deadline(task)
            .await
            .assured("the attach task completes")
            .assured("the server answers the attach")
            .succeeded()
    );
    assert_eq!(client.domain().await, Some(domain("bound")));
    assert_eq!(client.transaction_status().await, Some(status));
}

#[nervix_primitives::test]
async fn lost_begin_append_and_commit_replies_retry_the_exact_request() {
    for query in [
        "BEGIN TRANSACTION;",
        "CREATE SCHEMA recovered_record (id I64);",
        "COMMIT;",
    ] {
        nervix_primitives::task::consume_budget().await;
        let mut primary = TestServer::start().await;
        let mut seed = TestServer::start().await;
        let seed_url = Url::parse(&format!("http://{}", seed.address))
            .assured("the test seed is an HTTP origin");
        let options = ConnectOptions {
            seed_servers: vec![seed_url],
            retry_timeout: Duration::from_secs(5),
            ..ConnectOptions::default()
        };
        let client = within_deadline(Client::connect_with_options(
            format!("http://{}", primary.address),
            Some(domain("tenant")),
            options,
        ))
        .await
        .assured("the primary accepts the session");
        let mut first_exchange = primary.next_exchange().await;
        let execution = client.prepare_execution(query).await;
        let command_client = client.clone();
        let task = nervix_primitives::task::spawn(async move {
            command_client.execute_prepared(&execution).await
        });
        let first = first_exchange.next_request().await;
        let ClientRequest::Command(first_command) = first.request else {
            panic!("the client submitted a command");
        };
        drop(first_exchange);

        let mut recovered_exchange = seed.next_exchange().await;
        let recovered = recovered_exchange.next_request().await;
        let ClientRequest::Command(recovered_command) = recovered.request else {
            panic!("the client retried the lost command");
        };
        assert_eq!(recovered_command.query, first_command.query);
        assert_eq!(recovered_command.domain, first_command.domain);
        assert_eq!(
            recovered_command.execution_reference,
            first_command.execution_reference
        );
        assert_eq!(
            recovered_command.expected_transaction_position,
            first_command.expected_transaction_position
        );
        assert_eq!(
            recovered_command.expected_preview,
            first_command.expected_preview
        );
        let mut outcome = command_outcome(
            &recovered_command.execution_reference,
            completed(),
            "recovered exact command",
        );
        if let ReplyBody::Command(command) = &mut outcome {
            command.origin = OutcomeOrigin::Recovered;
        }
        recovered_exchange
            .reply(recovered.request_id, outcome, &limits())
            .await;
        let result = within_deadline(task)
            .await
            .assured("the command task completes")
            .assured("the exact retry is answered");
        assert!(result.succeeded());
        assert_eq!(result.origin, Some(OutcomeOrigin::Recovered));
    }
}

#[nervix_primitives::test]
async fn an_unanswered_command_ends_with_a_reusable_uncertain_identity() {
    let mut server = TestServer::start().await;
    let options = ConnectOptions {
        request_timeout: Duration::from_millis(50),
        retry_timeout: Duration::from_millis(250),
        ..ConnectOptions::default()
    };
    let client = within_deadline(Client::connect_with_options(
        format!("http://{}", server.address),
        Some(domain("tenant")),
        options,
    ))
    .await
    .assured("the test server accepts the session");
    let mut exchange = server.next_exchange().await;
    let execution = client.prepare_execution("SHOW CLUSTER STATUS;").await;
    let expected = execution.reference().clone();
    let command_client = client.clone();
    let task =
        nervix_primitives::task::spawn(
            async move { command_client.execute_prepared(&execution).await },
        );
    let request = exchange.next_request().await;
    let ClientRequest::Command(sent) = request.request else {
        panic!("the command was sent");
    };
    assert_eq!(sent.execution_reference, expected);
    let error = within_deadline(task)
        .await
        .assured("the command task ends")
        .expect_err("no reply can prove success");
    let ClientError::UncertainCommand { reference, .. } = error else {
        panic!("the deadline must report uncertainty");
    };
    assert_eq!(reference, expected);
}

#[nervix_primitives::test]
async fn backup_waits_beyond_ordinary_request_and_retry_deadlines() {
    for prepared in [false, true] {
        nervix_primitives::task::consume_budget().await;
        let mut server = TestServer::start().await;
        let options = ConnectOptions {
            request_timeout: Duration::from_millis(50),
            retry_timeout: Duration::from_millis(50),
            backup_wait_timeout: Duration::from_secs(2),
            ..ConnectOptions::default()
        };
        let client = Client::connect_with_options(
            format!("http://{}", server.address),
            Some(domain("tenant")),
            options,
        )
        .await
        .assured("the backup client connects");
        let mut exchange = server.next_exchange().await;
        let task = nervix_primitives::task::spawn(async move {
            let query = "BACKUP CLUSTER TO 'cluster.nvxb' TIMEOUT 1s;";
            if prepared {
                let execution = client.prepare_execution(query).await;
                client.execute_prepared(&execution).await
            } else {
                client.execute(query).await
            }
        });
        let request = exchange.next_request().await;
        let ClientRequest::Command(command) = request.request else {
            panic!("a backup is a command");
        };
        // Inject a slow command after observing admission, beyond both ordinary deadlines.
        nervix_primitives::time::sleep(Duration::from_millis(150)).await;
        exchange
            .reply(
                request.request_id,
                command_outcome(
                    &command.execution_reference,
                    CommandDisposition::Failed,
                    "a domain cut failed",
                ),
                &limits(),
            )
            .await;
        let outcome = within_deadline(task)
            .await
            .assured("the waiter finishes")
            .assured("the backup receives its terminal outcome");
        assert_eq!(outcome.disposition, CommandDisposition::Failed);
        assert_eq!(
            outcome.execution_reference,
            Some(command.execution_reference)
        );
    }
}

#[nervix_primitives::test]
async fn backup_reconnects_with_the_same_reference_within_its_wait_budget() {
    let mut server = TestServer::start().await;
    let client = Client::connect_with_options(
        format!("http://{}", server.address),
        Some(domain("tenant")),
        ConnectOptions {
            request_timeout: Duration::from_millis(50),
            retry_timeout: Duration::from_millis(50),
            backup_wait_timeout: Duration::from_secs(2),
            ..ConnectOptions::default()
        },
    )
    .await
    .assured("the backup client connects");
    let mut exchange = server.next_exchange().await;
    let task = nervix_primitives::task::spawn(async move {
        client.execute("BACKUP CLUSTER TO 'cluster.nvxb';").await
    });
    let first = exchange.next_request().await;
    let ClientRequest::Command(first_command) = first.request else {
        panic!("a backup is a command");
    };
    drop(exchange);
    let mut recovered_exchange = server.next_exchange().await;
    let recovered = recovered_exchange.next_request().await;
    let ClientRequest::Command(command) = recovered.request else {
        panic!("the backup is retried");
    };
    assert_eq!(command.query, first_command.query);
    assert_eq!(command.domain, first_command.domain);
    assert_eq!(
        command.execution_reference,
        first_command.execution_reference
    );
    nervix_primitives::time::sleep(Duration::from_millis(150)).await;
    recovered_exchange
        .reply(
            recovered.request_id,
            command_outcome(
                &command.execution_reference,
                CommandDisposition::Failed,
                "a recovered terminal failure",
            ),
            &limits(),
        )
        .await;
    let outcome = within_deadline(task)
        .await
        .assured("the waiter finishes")
        .assured("the backup survives the session loss");
    assert_eq!(outcome.disposition, CommandDisposition::Failed);
}

#[nervix_primitives::test]
async fn backup_wait_expiry_preserves_the_reference_for_explicit_recovery() {
    let mut server = TestServer::start().await;
    let client = Client::connect_with_options(
        format!("http://{}", server.address),
        Some(domain("tenant")),
        ConnectOptions {
            request_timeout: Duration::from_secs(3),
            retry_timeout: Duration::from_secs(3),
            backup_wait_timeout: Duration::from_millis(150),
            ..ConnectOptions::default()
        },
    )
    .await
    .assured("the backup client connects");
    let mut exchange = server.next_exchange().await;
    let execution = client
        .prepare_execution("BACKUP CLUSTER TO 'cluster.nvxb';")
        .await;
    let expected = execution.reference().clone();
    let command_client = client.clone();
    let task =
        nervix_primitives::task::spawn(
            async move { command_client.execute_prepared(&execution).await },
        );
    let request = exchange.next_request().await;
    let ClientRequest::Command(command) = request.request else {
        panic!("a backup is a command");
    };
    assert_eq!(command.execution_reference, expected);
    let result = nervix_primitives::time::timeout(Duration::from_secs(1), task)
        .await
        .assured("the backup wait ends before either ordinary deadline")
        .assured("the waiter finishes");
    let Err(ClientError::UncertainCommand { reference, source }) = result else {
        panic!("an unanswered backup reports its uncertain reference");
    };
    assert_eq!(reference, expected);
    assert!(matches!(*source, ClientError::RetryDeadline));
    let backup = nervix_models::Backup {
        scope: nervix_models::BackupScope::Cluster,
        destination: "recovered.nvxb".to_string(),
        resources: nervix_models::BackupResources::Included,
        capture: nervix_models::BackupCapture::default(),
    };
    let mut recovery_server = TestServer::start().await;
    let recovery_client = recovery_server.connect().await;
    recovery_client.set_domain(Some(domain("tenant"))).await;
    let mut exchange = recovery_server.next_exchange().await;
    let recovered = recovery_client
        .prepare_backup_with_reference(&backup, &reference)
        .await;
    assert_eq!(recovered.reference(), &expected);
    assert_eq!(recovered.domain(), Some(&domain("tenant")));
    let task = nervix_primitives::task::spawn(async move {
        recovery_client.execute_prepared(&recovered).await
    });
    let request = exchange.next_request().await;
    let ClientRequest::Command(command) = request.request else {
        panic!("the same backup is recovered");
    };
    assert_eq!(command.execution_reference, expected);
    assert!(command.query.contains("recovered.nvxb"));
    exchange
        .reply(
            request.request_id,
            command_outcome(
                &expected,
                CommandDisposition::ExecutionReferenceExpired,
                "the reference expired",
            ),
            &limits(),
        )
        .await;
    let outcome = within_deadline(task)
        .await
        .assured("the recovery finishes")
        .assured("the server's refusal is an outcome");
    assert_eq!(
        outcome.disposition,
        CommandDisposition::ExecutionReferenceExpired
    );
}

#[nervix_primitives::test]
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
    let listing =
        nervix_primitives::task::spawn(async move { listing_client.list_domains().await });
    let command_client = client.clone();
    let command = nervix_primitives::task::spawn(async move {
        command_client.execute("SHOW CLUSTER STATUS;").await
    });
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

#[nervix_primitives::test]
async fn a_domain_list_recovers_after_its_session_closes() {
    let mut primary = TestServer::start().await;
    let mut seed = TestServer::start().await;
    let seed_url =
        Url::parse(&format!("http://{}", seed.address)).assured("the seed is an HTTP origin");
    let client = Client::connect_with_options(
        format!("http://{}", primary.address),
        Some(domain("tenant")),
        ConnectOptions {
            seed_servers: vec![seed_url],
            retry_timeout: Duration::from_secs(5),
            ..ConnectOptions::default()
        },
    )
    .await
    .assured("the primary accepts the session");
    let mut first_exchange = primary.next_exchange().await;
    let listing_client = client.clone();
    let listing =
        nervix_primitives::task::spawn(async move { listing_client.list_domains().await });
    let first = first_exchange.next_request().await;
    assert!(matches!(first.request, ClientRequest::ListDomains));
    drop(first_exchange);

    let mut recovered_exchange = seed.next_exchange().await;
    let retried = recovered_exchange.next_request().await;
    assert!(matches!(retried.request, ClientRequest::ListDomains));
    recovered_exchange
        .reply(
            retried.request_id,
            ReplyBody::DomainList(DomainList {
                domains: tenant_domains(),
            }),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(listing)
            .await
            .assured("the listing task finishes")
            .assured("the recovered listing succeeds"),
        tenant_domains()
    );
}

#[nervix_primitives::test]
async fn a_typed_choice_lookup_preserves_dependencies_and_returns_typed_values() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    let lookup = ChoiceLookupRequest::new(
        ChoiceTarget::PlacementPolicy,
        vec![ChoiceSelection {
            value: ChoiceValue::DomainPace(DomainPaceChoice::Paced),
        }],
        "colo".to_string(),
    )
    .with_page(2, None)
    .assured("two choices fit the bounded page size");
    let choice_client = client.clone();
    let choices =
        nervix_primitives::task::spawn(async move { choice_client.lookup_choices(lookup).await });

    let request = exchange.next_request().await;
    let ClientRequest::Choice(lookup) = request.request else {
        panic!("the client sends a typed choice lookup");
    };
    assert_eq!(lookup.target(), ChoiceTarget::PlacementPolicy);
    assert_eq!(
        lookup.dependencies(),
        [ChoiceSelection {
            value: ChoiceValue::DomainPace(DomainPaceChoice::Paced),
        }]
    );
    assert_eq!(lookup.search(), "colo");
    exchange
        .reply(
            request.request_id,
            ReplyBody::Choice(ChoiceOutcome {
                status: ChoiceStatus::Ready,
                choices: vec![Choice {
                    value: ChoiceValue::PlacementPolicy(PlacementPolicy::PreferColocation),
                    presentation: ChoicePresentation {
                        label: "PREFER COLOCATION".to_string(),
                        detail: Some("Prefer placing domain work together".to_string()),
                        group: Some("Placement".to_string()),
                    },
                }],
                page_cursor: Some("next".to_string()),
            }),
            &limits(),
        )
        .await;

    let outcome = within_deadline(choices)
        .await
        .assured("the choice task finishes")
        .assured("the choice lookup succeeds");
    assert_eq!(outcome.status, ChoiceStatus::Ready);
    assert_eq!(
        outcome.choices[0].value,
        ChoiceValue::PlacementPolicy(PlacementPolicy::PreferColocation)
    );
    assert_eq!(outcome.page_cursor.as_deref(), Some("next"));
}

#[cfg(feature = "autocomplete")]
#[nervix_primitives::test]
async fn a_suggestion_recovers_after_its_session_closes() {
    let mut primary = TestServer::start().await;
    let mut seed = TestServer::start().await;
    let seed_url =
        Url::parse(&format!("http://{}", seed.address)).assured("the seed is an HTTP origin");
    let client = Client::connect_with_options(
        format!("http://{}", primary.address),
        Some(domain("tenant")),
        ConnectOptions {
            seed_servers: vec![seed_url],
            retry_timeout: Duration::from_secs(5),
            ..ConnectOptions::default()
        },
    )
    .await
    .assured("the primary accepts the session");
    let mut first_exchange = primary.next_exchange().await;
    let suggestion_client = client.clone();
    let suggestion = nervix_primitives::task::spawn(async move {
        suggestion_client.suggest("CREATE ", 7, 64, None).await
    });
    let first = first_exchange.next_request().await;
    assert!(matches!(first.request, ClientRequest::Suggest(_)));
    drop(first_exchange);

    let mut recovered_exchange = seed.next_exchange().await;
    let retried = recovered_exchange.next_request().await;
    assert!(matches!(retried.request, ClientRequest::Suggest(_)));
    recovered_exchange
        .reply(
            retried.request_id,
            ReplyBody::Suggest(SuggestOutcome {
                status: SuggestionStatus::Ready,
                continuation: None,
                suggestions: vec![Suggestion {
                    value: "SCHEMA".to_string(),
                    kind: SuggestionKind::Text,
                    edit: TextEdit {
                        start: 7,
                        end: 7,
                        replacement: "SCHEMA".to_string(),
                    },
                }],
            }),
            &limits(),
        )
        .await;
    let suggestions = within_deadline(suggestion)
        .await
        .assured("the suggestion task finishes")
        .assured("the recovered suggestion succeeds");
    assert_eq!(suggestions.status, SuggestionStatus::Ready);
    assert_eq!(suggestions.suggestions[0].value, "SCHEMA");
}

#[cfg(feature = "autocomplete")]
#[nervix_primitives::test]
async fn concurrent_suggestions_lists_and_commands_follow_their_request_ids() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    for order in [[2, 0, 1], [1, 2, 0], [0, 1, 2]] {
        let suggestion_client = client.clone();
        let suggestion = nervix_primitives::task::spawn(async move {
            suggestion_client.suggest("CREATE ", 7, 64, None).await
        });
        let listing_client = client.clone();
        let listing =
            nervix_primitives::task::spawn(async move { listing_client.list_domains().await });
        let command_client = client.clone();
        let command = nervix_primitives::task::spawn(async move {
            command_client.execute("SHOW CLUSTER STATUS;").await
        });
        let mut requests = Vec::new();
        for _ in 0..3 {
            nervix_primitives::task::consume_budget().await;
            requests.push(exchange.next_request().await);
        }
        for index in order {
            nervix_primitives::task::consume_budget().await;
            let request = &requests[index];
            let body = match &request.request {
                ClientRequest::Suggest(suggest) => {
                    assert_eq!(suggest.input(), "CREATE ");
                    ReplyBody::Suggest(SuggestOutcome {
                        status: SuggestionStatus::Ready,
                        continuation: None,
                        suggestions: vec![Suggestion {
                            value: "SCHEMA".to_string(),
                            kind: SuggestionKind::Text,
                            edit: TextEdit {
                                start: 7,
                                end: 7,
                                replacement: "SCHEMA".to_string(),
                            },
                        }],
                    })
                }
                ClientRequest::ListDomains => ReplyBody::DomainList(DomainList {
                    domains: tenant_domains(),
                }),
                ClientRequest::Command(command) => command_outcome(
                    &command.execution_reference,
                    completed(),
                    "cluster is healthy",
                ),
                other => panic!("the client sent an unexpected request: {other:?}"),
            };
            exchange.reply(request.request_id, body, &limits()).await;
        }
        let suggestions = within_deadline(suggestion)
            .await
            .assured("the suggestion task finishes")
            .assured("the suggestion request succeeds");
        assert_eq!(suggestions.status, SuggestionStatus::Ready);
        assert_eq!(suggestions.suggestions.len(), 1);
        assert_eq!(suggestions.suggestions[0].value, "SCHEMA");
        assert_eq!(
            within_deadline(listing)
                .await
                .assured("the listing task finishes")
                .assured("the listing request succeeds"),
            tenant_domains()
        );
        assert!(
            within_deadline(command)
                .await
                .assured("the command task finishes")
                .assured("the command request succeeds")
                .succeeded()
        );
    }
}

#[cfg(feature = "autocomplete")]
#[nervix_primitives::test]
async fn suggestion_finishes_while_a_command_reply_is_pending() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let command_client = client.clone();
    let command = nervix_primitives::task::spawn(async move {
        command_client.execute("SHOW CLUSTER STATUS;").await
    });
    let command_request = exchange.next_request().await;
    let ClientRequest::Command(command_body) = &command_request.request else {
        panic!("the first request is the pending command");
    };
    let execution_reference = command_body.execution_reference.clone();

    let suggestion_client = client.clone();
    let suggestion = nervix_primitives::task::spawn(async move {
        suggestion_client.suggest("CREATE ", 7, 64, None).await
    });
    let suggestion_request = exchange.next_request().await;
    assert!(matches!(
        suggestion_request.request,
        ClientRequest::Suggest(_)
    ));
    exchange
        .reply(
            suggestion_request.request_id,
            ReplyBody::Suggest(SuggestOutcome {
                status: SuggestionStatus::Ready,
                continuation: None,
                suggestions: vec![Suggestion {
                    value: "SCHEMA".to_string(),
                    kind: SuggestionKind::Text,
                    edit: TextEdit {
                        start: 7,
                        end: 7,
                        replacement: "SCHEMA".to_string(),
                    },
                }],
            }),
            &limits(),
        )
        .await;
    let response = within_deadline(suggestion)
        .await
        .assured("the suggestion completes before the command replies")
        .assured("the suggestion succeeds");
    assert_eq!(response.suggestions[0].value, "SCHEMA");
    assert!(!command.is_finished());

    exchange
        .reply(
            command_request.request_id,
            command_outcome(&execution_reference, completed(), "cluster is healthy"),
            &limits(),
        )
        .await;
    within_deadline(command)
        .await
        .assured("the command completes after its reply")
        .assured("the command succeeds");
}

#[nervix_primitives::test]
async fn a_reply_larger_than_a_frame_arrives_in_parts() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let command_client = client.clone();
    let command =
        nervix_primitives::task::spawn(
            async move { command_client.execute("SHOW DOMAINS;").await },
        );
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
        nervix_primitives::task::consume_budget().await;
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

/// The text of each row of a batch that fills its frame.
const WIDE_ROW_BYTES: usize = 32 * 1024;

/// A batch of wide rows that the server fills until its next row no longer fits the frame limit,
/// together with the number of rows it holds.
fn full_rows_frame(handle: SubscriptionHandle) -> (EncodedFrame<ServerFrame>, usize) {
    let wide = "w".repeat(WIDE_ROW_BYTES);
    let mut batch = SubscriptionRowsEncoder::unbranched(handle, &limits())
        .assured("an unbranched batch starts within the limits");
    loop {
        let id = u64::try_from(batch.rows()).assured("a frame holds far fewer than u64::MAX rows");
        let pushed = batch.push_row(|cells| {
            cells.push_string(&wide)?;
            cells.push_u64(id)
        });
        if let Err(refused) = pushed {
            assert!(
                matches!(
                    refused.current_context(),
                    WireEncodeError::FrameTooLarge { .. }
                ),
                "only the frame limit ends a batch of wide rows: {refused:?}"
            );
            break;
        }
    }
    let rows = batch.rows();
    let frame = batch
        .finish()
        .assured("the rows accepted before the refused one finish their frame");
    (frame, rows)
}

/// The reply that opens `handle` over the `orders` relay.
fn opened_reply(handle: SubscriptionHandle) -> ReplyBody {
    ReplyBody::Subscribe(SubscribeOutcome {
        disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
            subscription: handle,
            domain: domain("tenant"),
            relay: RelayName::parse("orders").assured("the test relay name is valid"),
            subscription_type: SubscriptionType::Row,
            schema: orders_schema(),
        })),
        message: "subscription 'live' opened".to_string(),
        diagnostics: Vec::new(),
    })
}

#[nervix_primitives::test]
async fn subscription_rows_render_against_the_schema_the_subscription_opened_with() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let subscribe_client = client.clone();
    let subscribe = nervix_primitives::task::spawn(async move {
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
    exchange
        .reply(request.request_id, opened_reply(subscription(1)), &limits())
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

#[nervix_primitives::test]
async fn a_row_frame_filled_to_the_frame_limit_reaches_an_active_subscription() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let subscribe_client = client.clone();
    let subscribe = nervix_primitives::task::spawn(async move {
        subscribe_client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let request = exchange.next_request().await;
    exchange
        .reply(request.request_id, opened_reply(subscription(1)), &limits())
        .await;
    let (frame, rows) = full_rows_frame(subscription(1));
    assert!(
        frame.len() > limits().frame_bytes() - 2 * WIDE_ROW_BYTES,
        "the batch fills its frame to within two rows of the frame limit: {} bytes",
        frame.len()
    );
    exchange.send(frame).await;
    let outcome = within_deadline(subscribe)
        .await
        .assured("the subscribe task completes")
        .assured("the subscription is answered");
    assert!(outcome.succeeded());

    let event = within_deadline(client.next_subscription())
        .await
        .assured("the rows of the opened subscription are delivered");
    let SubscriptionEvent::Rows(delivered) = event else {
        panic!("a frame of the frame limit reaches its subscription as rows: {event:?}");
    };
    assert_eq!(delivered.rows.subscription(), &subscription(1));
    assert_eq!(delivered.rows.batch().len(), rows);
    let lifecycle = client.subscription_lifecycle(&subscription(1).name);
    assert_eq!(
        lifecycle,
        Some(SubscriptionLifecycle::Active(subscription(1))),
        "retaining the frame leaves its subscription active"
    );
}

#[nervix_primitives::test]
async fn a_command_waits_for_an_election_and_is_sent_again_with_its_reference() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let command_client = client.clone();
    let command = nervix_primitives::task::spawn(async move {
        command_client.execute("CREATE DOMAIN orders;").await
    });
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

#[nervix_primitives::test]
async fn a_command_redirect_from_a_preopened_channel_keeps_its_execution_reference() {
    let mut primary = TestServer::start().await;
    let mut leader = TestServer::start().await;
    let channel = within_deadline(
        tonic::transport::Endpoint::from_shared(format!("http://{}", primary.address))
            .assured("the primary server has an HTTP origin")
            .connect(),
    )
    .await
    .assured("the primary server accepts a preopened channel");
    let client = within_deadline(Client::from_channel(channel, Some(domain("tenant"))))
        .await
        .assured("the preopened channel starts a session");
    let mut first_exchange = primary.next_exchange().await;
    let command_client = client.clone();
    let command = nervix_primitives::task::spawn(async move {
        command_client
            .execute("CREATE SCHEMA redirected (id I64);")
            .await
    });
    let first = first_exchange.next_request().await;
    let ClientRequest::Command(first_command) = first.request else {
        panic!("the command is sent to the first node");
    };
    let original_domain = first_command.domain.clone();
    client.set_domain(Some(domain("another_domain"))).await;
    let leader_url =
        Url::parse(&format!("http://{}", leader.address)).assured("the leader is an HTTP origin");
    first_exchange
        .reply(
            first.request_id,
            command_outcome(
                &first_command.execution_reference,
                CommandDisposition::NotLeader(LeaderRedirect {
                    leader: Some(LeaderEndpoints {
                        node: node("leader"),
                        grpc_uri: Some(leader_url),
                        web_console_uri: None,
                    }),
                }),
                "follow the leader",
            ),
            &limits(),
        )
        .await;
    let mut redirected_exchange = leader.next_exchange().await;
    let retried = redirected_exchange.next_request().await;
    let ClientRequest::Command(retried_command) = retried.request else {
        panic!("the command is retried on the leader");
    };
    assert_eq!(retried_command.query, first_command.query);
    assert_eq!(retried_command.domain, original_domain);
    assert_eq!(
        retried_command.execution_reference,
        first_command.execution_reference
    );
    redirected_exchange
        .reply(
            retried.request_id,
            command_outcome(&retried_command.execution_reference, completed(), "created"),
            &limits(),
        )
        .await;
    assert!(
        within_deadline(command)
            .await
            .assured("the command task completes")
            .assured("the leader answers the retry")
            .succeeded()
    );
}

fn write_file(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).assured("the test directory accepts files");
}

#[nervix_primitives::test]
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
        domain("tenant"),
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

#[nervix_primitives::test]
async fn a_lost_upload_reply_retries_with_the_same_identity_and_archive() {
    let mut primary = TestServer::start().await;
    let mut seed = TestServer::start().await;
    primary.fail_upload_replies.store(1, Ordering::SeqCst);
    let seed_url =
        Url::parse(&format!("http://{}", seed.address)).assured("the test seed is an HTTP origin");
    let options = ConnectOptions {
        seed_servers: vec![seed_url],
        retry_timeout: Duration::from_secs(5),
        ..ConnectOptions::default()
    };
    let client = within_deadline(Client::connect_with_options(
        format!("http://{}", primary.address),
        Some(domain("tenant")),
        options,
    ))
    .await
    .assured("the primary accepts the session");
    let _exchange = primary.next_exchange().await;
    let directory = tempfile::tempdir().assured("a temporary directory is available");
    write_file(&directory.path().join("payload.bin"), &[3_u8; 1000]);
    let identity =
        ResourceUploadIdentity::parse("stable-upload").assured("the test identity is valid");

    let outcome = within_deadline(client.upload_resource_from_directory_with_identity(
        "model",
        directory.path(),
        domain("tenant"),
        identity.clone(),
        |_| {},
    ))
    .await
    .assured("the upload retry completes");

    let first = within_deadline(primary.uploads.recv())
        .await
        .assured("the primary received the first upload");
    let second = within_deadline(seed.uploads.recv())
        .await
        .assured("the seed received the retry");
    assert_eq!(first.start.upload_identity, identity);
    assert_eq!(second.start.upload_identity, identity);
    assert_eq!(second.start.domain, first.start.domain);
    assert_eq!(second.start.resource, first.start.resource);
    assert_eq!(second.archive, first.archive);
    assert!(outcome.succeeded());
}

#[nervix_primitives::test]
async fn an_upload_redirect_keeps_its_identity_and_archive() {
    let mut primary = TestServer::start().await;
    let mut leader = TestServer::start().await;
    let leader_url =
        Url::parse(&format!("http://{}", leader.address)).assured("the leader is an HTTP origin");
    *primary.upload_reply_mode.lock().await = Some(UploadReplyMode::Redirect(leader_url));
    let client = primary.connect().await;
    let _exchange = primary.next_exchange().await;
    let directory = tempfile::tempdir().assured("a temporary directory is available");
    write_file(&directory.path().join("payload.bin"), &[7_u8; 1000]);
    let identity =
        ResourceUploadIdentity::parse("redirected-upload").assured("the test identity is valid");

    let outcome = within_deadline(client.upload_resource_from_directory_with_identity(
        "model",
        directory.path(),
        domain("tenant"),
        identity.clone(),
        |_| {},
    ))
    .await
    .assured("the redirected upload completes");

    let first = within_deadline(primary.uploads.recv())
        .await
        .assured("the primary received the first attempt");
    let second = within_deadline(leader.uploads.recv())
        .await
        .assured("the leader received the retry");
    assert_eq!(first.start.upload_identity, identity);
    assert_eq!(second.start.upload_identity, identity);
    assert_eq!(second.archive, first.archive);
    assert!(outcome.succeeded());
}

#[nervix_primitives::test]
async fn a_prepared_upload_uses_its_captured_domain() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let _exchange = server.next_exchange().await;
    let directory = tempfile::tempdir().assured("a temporary directory is available");
    write_file(&directory.path().join("payload.bin"), &[7_u8; 1000]);
    let query = format!(
        "UPLOAD RESOURCE model VERSION '{}';",
        directory.path().display()
    );
    let prepared = client.prepare_execution(query).await;
    client.set_domain(Some(domain("another_domain"))).await;

    let outcome = within_deadline(client.execute_prepared(&prepared))
        .await
        .assured("the upload completes");
    let received = within_deadline(server.uploads.recv())
        .await
        .assured("the server received the upload");
    assert!(outcome.succeeded());
    assert_eq!(received.start.domain, domain("tenant"));
    assert_eq!(received.start.upload_identity, *prepared.upload_identity());
}

#[nervix_primitives::test]
async fn malformed_upload_replies_are_rejected_by_their_correlations() {
    for mode in [
        UploadReplyMode::WrongIdentity,
        UploadReplyMode::WrongRequestId,
    ] {
        nervix_primitives::task::consume_budget().await;
        let mut server = TestServer::start().await;
        *server.upload_reply_mode.lock().await = Some(mode);
        let client = server.connect().await;
        let _exchange = server.next_exchange().await;
        let directory = tempfile::tempdir().assured("a temporary directory is available");
        write_file(&directory.path().join("payload.bin"), &[7_u8; 1000]);
        let identity = ResourceUploadIdentity::parse("correlated-upload")
            .assured("the test identity is valid");
        let error = within_deadline(client.upload_resource_from_directory_with_identity(
            "model",
            directory.path(),
            domain("tenant"),
            identity,
            |_| {},
        ))
        .await
        .expect_err("a reply for another upload or request cannot report success");
        assert!(matches!(
            error,
            ClientError::UploadIdentityMismatch { .. } | ClientError::UnexpectedReply { .. }
        ));
    }
}

#[nervix_primitives::test]
async fn upload_permission_denial_is_reported_without_retry() {
    let mut server = TestServer::start().await;
    *server.upload_reply_mode.lock().await = Some(UploadReplyMode::Reject);
    let client = server.connect().await;
    let _exchange = server.next_exchange().await;
    let directory = tempfile::tempdir().assured("a temporary directory is available");
    write_file(&directory.path().join("payload.bin"), &[7_u8; 1000]);
    let identity =
        ResourceUploadIdentity::parse("denied-upload").assured("the test identity is valid");

    let error = within_deadline(client.upload_resource_from_directory_with_identity(
        "model",
        directory.path(),
        domain("tenant"),
        identity,
        |_| {},
    ))
    .await
    .expect_err("the server denied the upload");
    let ClientError::UploadResource(status) = error else {
        panic!("permission denial has a typed upload status");
    };
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

fn attach_reply(clock: crate::DomainClockObservation, message: &str) -> ReplyBody {
    ReplyBody::DomainClockAttach(crate::DomainClockAttachOutcome {
        disposition: crate::DomainClockAttachDisposition::Attached {
            domain: domain("tenant"),
            clock,
        },
        message: message.to_string(),
    })
}

fn clock_in(
    generation: u64,
    state: crate::DomainClockObservedState,
) -> crate::DomainClockObservation {
    crate::DomainClockObservation { generation, state }
}

#[nervix_primitives::test]
async fn domain_clock_statements_are_sent_as_typed_requests_for_the_active_domain() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let attaching = client.clone();
    let attach =
        nervix_primitives::task::spawn(
            async move { attaching.execute("ATTACH DOMAIN CLOCK;").await },
        );
    let request = exchange.next_request().await;
    let ClientRequest::AttachDomainClock(sent) = request.request else {
        panic!("ATTACH DOMAIN CLOCK is sent as an attach request");
    };
    assert_eq!(sent.domain, domain("tenant"));
    let unpaced = clock_in(1, crate::DomainClockObservedState::Unpaced);
    let message = "attached to the clock of domain 'tenant': generation 1, unpaced";
    exchange
        .reply(
            request.request_id,
            attach_reply(unpaced.clone(), message),
            &limits(),
        )
        .await;
    let outcome = within_deadline(attach)
        .await
        .assured("the attach task completes")
        .assured("the attachment succeeds");
    assert!(outcome.succeeded());
    assert_eq!(outcome.message, message);
    assert_eq!(outcome.execution_reference, None);
    let attached = client
        .domain_clock(&domain("tenant"))
        .assured("the client follows the attached clock");
    assert_eq!(attached.clock(), &unpaced);

    let stopped = clock_in(1, crate::DomainClockObservedState::Stopped);
    let frame = crate::DomainClockObserved {
        domain: domain("tenant"),
        clock: stopped.clone(),
    }
    .encode(&limits())
    .assured("a clock frame fits the default limits");
    exchange.send(frame).await;
    let event = within_deadline(client.next_domain_clock_event())
        .await
        .assured("the clock frame reaches the event stream");
    assert_eq!(
        event,
        crate::DomainClockEvent::Observed(crate::DomainClockObserved {
            domain: domain("tenant"),
            clock: stopped.clone(),
        })
    );
    assert_eq!(
        client
            .domain_clock(&domain("tenant"))
            .assured("the client still follows the clock")
            .clock(),
        &stopped
    );

    let detaching = client.clone();
    let detach =
        nervix_primitives::task::spawn(
            async move { detaching.execute("detach domain clock").await },
        );
    let request = exchange.next_request().await;
    let ClientRequest::DetachDomainClock(sent) = request.request else {
        panic!("DETACH DOMAIN CLOCK is sent as a detach request");
    };
    assert_eq!(sent.domain, domain("tenant"));
    exchange
        .reply(
            request.request_id,
            ReplyBody::DomainClockDetach(crate::DomainClockDetachOutcome {
                disposition: crate::DomainClockDetachDisposition::Detached(domain("tenant")),
                message: "detached from the clock of domain 'tenant'".to_string(),
            }),
            &limits(),
        )
        .await;
    let outcome = within_deadline(detach)
        .await
        .assured("the detach task completes")
        .assured("the detach succeeds");
    assert!(outcome.succeeded());
    assert_eq!(client.domain_clock(&domain("tenant")), None);

    let refusing = client.clone();
    let refused =
        nervix_primitives::task::spawn(
            async move { refusing.execute("DETACH DOMAIN CLOCK;").await },
        );
    let request = exchange.next_request().await;
    exchange
        .reply(
            request.request_id,
            ReplyBody::DomainClockDetach(crate::DomainClockDetachOutcome {
                disposition: crate::DomainClockDetachDisposition::NotAttached(domain("tenant")),
                message: "this session does not follow the clock of domain 'tenant'".to_string(),
            }),
            &limits(),
        )
        .await;
    let outcome = within_deadline(refused)
        .await
        .assured("the detach task completes")
        .assured("the refusal is an outcome");
    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.message,
        "this session does not follow the clock of domain 'tenant'"
    );
}

#[nervix_primitives::test]
async fn domain_clock_statements_need_an_active_domain_and_no_transaction() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let _exchange = server.next_exchange().await;

    client.set_domain(None).await;
    for statement in ["ATTACH DOMAIN CLOCK;", "DETACH DOMAIN CLOCK;"] {
        assert!(matches!(
            within_deadline(client.execute(statement)).await,
            Err(ClientError::NoActiveDomain)
        ));
    }

    client
        .adopt_transaction_status(
            nervix_models::TransactionStatus::new(
                "tx".to_string(),
                domain("tenant"),
                nervix_models::TransactionLifecycle::Open,
                nervix_models::TransactionPosition::new(0),
                0,
            )
            .assured("an empty open transaction is consistent"),
        )
        .await;
    let outcome = within_deadline(client.execute("ATTACH DOMAIN CLOCK;"))
        .await
        .assured("a refused local statement is an outcome");
    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.message,
        "client-local commands are not allowed while a transaction is active"
    );
    let outcome = within_deadline(client.execute("ATTACH DOMAIN CLOCK; SHOW CLUSTER STATUS;"))
        .await
        .assured("a refused batch is an outcome");
    assert_eq!(
        outcome.message,
        "client-local commands must be executed separately"
    );
}

#[nervix_primitives::test]
async fn domain_clock_requests_return_their_typed_outcomes_and_refusals() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;

    let attaching = client.clone();
    let attach = nervix_primitives::task::spawn(async move {
        attaching.attach_domain_clock(domain("missing")).await
    });
    let request = exchange.next_request().await;
    let not_found = crate::DomainClockAttachOutcome {
        disposition: crate::DomainClockAttachDisposition::DomainNotFound(domain("missing")),
        message: "domain 'missing' does not exist".to_string(),
    };
    exchange
        .reply(
            request.request_id,
            ReplyBody::DomainClockAttach(not_found.clone()),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(attach)
            .await
            .assured("the attach task completes")
            .assured("a refusal is an outcome"),
        not_found
    );
    assert_eq!(client.domain_clock(&domain("missing")), None);

    let detaching = client.clone();
    let detach = nervix_primitives::task::spawn(async move {
        detaching.detach_domain_clock(domain("tenant")).await
    });
    let request = exchange.next_request().await;
    exchange
        .reply(
            request.request_id,
            ReplyBody::Rejected(crate::wire::RequestRejected {
                rejection: crate::wire::RequestRejection::UnsupportedRequest,
                field: None,
                message: "not served".to_string(),
            }),
            &limits(),
        )
        .await;
    let error = within_deadline(detach)
        .await
        .assured("the detach task completes")
        .expect_err("a rejected request is an error");
    assert!(matches!(
        error.current_context(),
        ClientError::RequestRejected {
            request: crate::RequestKind::DetachDomainClock,
            ..
        }
    ));

    let attaching = client.clone();
    let attach = nervix_primitives::task::spawn(async move {
        attaching.attach_domain_clock(domain("tenant")).await
    });
    let request = exchange.next_request().await;
    exchange
        .reply(
            request.request_id,
            ReplyBody::DomainClockDetach(crate::DomainClockDetachOutcome {
                disposition: crate::DomainClockDetachDisposition::Failed,
                message: String::new(),
            }),
            &limits(),
        )
        .await;
    let error = within_deadline(attach)
        .await
        .assured("the attach task completes")
        .expect_err("a reply of another kind is an error");
    assert!(matches!(
        error.current_context(),
        ClientError::UnexpectedReply {
            request: crate::RequestKind::AttachDomainClock,
        }
    ));
}

#[nervix_primitives::test]
async fn an_attached_clock_is_attached_again_on_a_new_session_and_reports_its_clock() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    let attaching = client.clone();
    let attach = nervix_primitives::task::spawn(async move {
        attaching.attach_domain_clock(domain("tenant")).await
    });
    let request = exchange.next_request().await;
    exchange
        .reply(
            request.request_id,
            attach_reply(clock_in(1, crate::DomainClockObservedState::Unpaced), ""),
            &limits(),
        )
        .await;
    within_deadline(attach)
        .await
        .assured("the attach task completes")
        .assured("the attachment succeeds");

    drop(exchange);
    assert_eq!(
        within_deadline(client.next_domain_clock_event())
            .await
            .assured("the interruption is an event"),
        crate::DomainClockEvent::Interrupted(crate::DomainClockInterruption {
            domain: domain("tenant"),
        })
    );

    let reading = client.clone();
    let next =
        nervix_primitives::task::spawn(async move { reading.next_domain_clock_event().await });
    let mut restored = server.next_exchange().await;
    let request = restored.next_request().await;
    let ClientRequest::AttachDomainClock(sent) = request.request else {
        panic!("the first request on the new session attaches the followed clock again");
    };
    assert_eq!(sent.domain, domain("tenant"));
    let stopped = clock_in(2, crate::DomainClockObservedState::Stopped);
    restored
        .reply(
            request.request_id,
            attach_reply(stopped.clone(), ""),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(next)
            .await
            .assured("the event task completes")
            .assured("the restored clock is reported"),
        crate::DomainClockEvent::Observed(crate::DomainClockObserved {
            domain: domain("tenant"),
            clock: stopped.clone(),
        })
    );
    assert_eq!(
        client
            .domain_clock(&domain("tenant"))
            .assured("the restored clock is followed")
            .clock(),
        &stopped
    );
}

/// The reply that opens `live` at `generation` on `orders`.
fn opened(generation: u64) -> ReplyBody {
    ReplyBody::Subscribe(SubscribeOutcome {
        disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
            subscription: subscription(generation),
            domain: domain("tenant"),
            relay: RelayName::parse("orders").assured("the test relay name is valid"),
            subscription_type: SubscriptionType::Row,
            schema: orders_schema(),
        })),
        message: "subscription 'live' opened".to_string(),
        diagnostics: Vec::new(),
    })
}

/// Waits until the client observes that its session ended: the exchange's reader closed the
/// registry of the requests waiting on it.
async fn session_closed(client: &Client) {
    within_deadline(async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let requests = client.inner.exchange.lock().await.requests();
            if !requests.pending.lock().is_open() {
                return;
            }
            nervix_primitives::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[nervix_primitives::test]
async fn a_subscription_requested_on_a_closed_session_opens_on_a_new_session() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let exchange = server.next_exchange().await;
    drop(exchange);
    session_closed(&client).await;

    let subscribing = client.clone();
    let mut subscribe = nervix_primitives::task::spawn(async move {
        subscribing
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let mut reopened = nervix_primitives::select! {
        exchange = server.next_exchange() => exchange,
        finished = &mut subscribe => {
            panic!("the subscription ended without opening a new session: {finished:?}")
        }
    };
    let request = reopened.next_request().await;
    let ClientRequest::Subscribe(sent) = request.request else {
        panic!("the subscription is requested on the new session");
    };
    assert_eq!(sent.statement, "CREATE SUBSCRIPTION live TO orders;");
    reopened
        .reply(request.request_id, opened(1), &limits())
        .await;
    let outcome = within_deadline(subscribe)
        .await
        .assured("the subscribe task completes")
        .assured("the subscription opens on the new session");
    assert!(outcome.succeeded(), "{}", outcome.message);
    assert_eq!(
        client.subscription_lifecycle(
            &SubscriptionName::parse("live").assured("the test name is valid")
        ),
        Some(crate::SubscriptionLifecycle::Active(subscription(1)))
    );
}

#[nervix_primitives::test]
async fn deleting_an_unknown_subscription_on_a_closed_session_asks_a_new_session() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let exchange = server.next_exchange().await;
    drop(exchange);
    session_closed(&client).await;

    let deleting = client.clone();
    let mut unsubscribe =
        nervix_primitives::task::spawn(async move { deleting.unsubscribe("ghost").await });
    let mut reopened = nervix_primitives::select! {
        exchange = server.next_exchange() => exchange,
        finished = &mut unsubscribe => {
            panic!("the deletion ended without opening a new session: {finished:?}")
        }
    };
    let request = reopened.next_request().await;
    let ClientRequest::Unsubscribe(sent) = request.request else {
        panic!("the deletion is requested on the new session");
    };
    assert_eq!(sent.subscription.as_str(), "ghost");
    reopened
        .reply(
            request.request_id,
            ReplyBody::Unsubscribe(crate::wire::UnsubscribeOutcome {
                disposition: crate::wire::UnsubscribeDisposition::Failed,
                message: "session subscription 'ghost' does not exist".to_string(),
                diagnostics: Vec::new(),
            }),
            &limits(),
        )
        .await;
    let outcome = within_deadline(unsubscribe)
        .await
        .assured("the unsubscribe task completes")
        .assured("the refusal is an outcome");
    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.message,
        "session subscription 'ghost' does not exist"
    );

    let subscribing = client.clone();
    let subscribe = nervix_primitives::task::spawn(async move {
        subscribing
            .subscribe(&SubscriptionRequest::new("ghost", "orders"))
            .await
    });
    let request = reopened.next_request().await;
    assert!(
        matches!(request.request, ClientRequest::Subscribe(_)),
        "a refused deletion of a name the client never held leaves the name free"
    );
    reopened
        .reply(
            request.request_id,
            ReplyBody::Subscribe(SubscribeOutcome {
                disposition: SubscribeDisposition::Failed,
                message: "relay 'orders' does not exist".to_string(),
                diagnostics: Vec::new(),
            }),
            &limits(),
        )
        .await;
    within_deadline(subscribe)
        .await
        .assured("the subscribe task completes")
        .assured("the refusal is an outcome");
}

#[nervix_primitives::test]
async fn a_reconnected_session_restores_subscriptions_before_it_attaches_its_transaction() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    let subscribing = client.clone();
    let subscribe = nervix_primitives::task::spawn(async move {
        subscribing
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let request = exchange.next_request().await;
    exchange
        .reply(request.request_id, opened(1), &limits())
        .await;
    within_deadline(subscribe)
        .await
        .assured("the subscribe task completes")
        .assured("the subscription opens");
    let transaction = nervix_models::TransactionStatus::new(
        "tx".to_string(),
        domain("tenant"),
        nervix_models::TransactionLifecycle::Open,
        nervix_models::TransactionPosition::new(0),
        0,
    )
    .assured("an empty open transaction is consistent");
    client.adopt_transaction_status(transaction.clone()).await;

    drop(exchange);
    let interrupted = within_deadline(client.next_subscription())
        .await
        .assured("the lost session reports the subscription's gap");
    assert!(matches!(interrupted, SubscriptionEvent::Interrupted(_)));

    let recovering = client.clone();
    let recovery = nervix_primitives::task::spawn(async move {
        recovering
            .recover_session(crate::client::RecoveryMode::IfClosed)
            .await
            .map(|_| ())
    });
    let mut restored = server.next_exchange().await;
    let first = restored.next_request().await;
    assert!(
        matches!(first.request, ClientRequest::Subscribe(_)),
        "a session that holds a transaction refuses subscriptions, so restoration comes first: \
         {:?}",
        first.request
    );
    restored.reply(first.request_id, opened(2), &limits()).await;
    let second = restored.next_request().await;
    let ClientRequest::AttachTransaction(attach) = second.request else {
        panic!("the transaction is attached again after the subscription");
    };
    assert_eq!(attach.transaction_id, "tx");
    restored
        .reply(
            second.request_id,
            ReplyBody::Attach(crate::wire::AttachOutcome {
                disposition: crate::wire::AttachDisposition::Attached(transaction),
                message: "attached".to_string(),
                diagnostics: Vec::new(),
            }),
            &limits(),
        )
        .await;
    within_deadline(recovery)
        .await
        .assured("the recovery task completes")
        .assured("the session recovers");
}

/// The frame that ends `handle` because its relay was redefined.
fn relay_changed(handle: SubscriptionHandle) -> EncodedFrame<ServerFrame> {
    SubscriptionEnded {
        subscription: handle,
        reason: SubscriptionEndReason::RelayChanged,
        message: "session subscription 'live' ended because relay 'orders' was redefined"
            .to_string(),
    }
    .encode(&limits())
    .assured("a subscription end fits a frame")
}

/// Subscribes `live` to `orders` on `exchange`, which opens it as `generation`.
async fn subscribe_live(client: &Client, exchange: &mut ServerExchange, generation: u64) {
    let subscribing = client.clone();
    let subscribe = nervix_primitives::task::spawn(async move {
        subscribing
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let request = exchange.next_request().await;
    let ClientRequest::Subscribe(sent) = request.request else {
        panic!("a subscription is requested with a subscribe request");
    };
    assert_eq!(sent.statement, "CREATE SUBSCRIPTION live TO orders;");
    exchange
        .reply(request.request_id, opened(generation), &limits())
        .await;
    let outcome = within_deadline(subscribe)
        .await
        .assured("the subscribe task completes")
        .assured("the subscription is answered");
    assert!(outcome.succeeded(), "{}", outcome.message);
}

#[nervix_primitives::test]
async fn a_subscription_the_server_ended_is_not_opened_again_on_a_new_session() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    subscribe_live(&client, &mut exchange, 1).await;
    exchange.send(relay_changed(subscription(1))).await;
    let event = within_deadline(client.next_subscription())
        .await
        .assured("the end of the subscription is delivered");
    let SubscriptionEvent::Ended(ended) = event else {
        panic!("the server's end is the subscription's next event, not {event:?}");
    };
    assert_eq!(ended.subscription, subscription(1));
    let live = SubscriptionName::parse("live").assured("the test name is valid");
    assert_eq!(
        client.subscription_lifecycle(&live),
        Some(SubscriptionLifecycle::Ended(subscription(1)))
    );

    drop(exchange);
    session_closed(&client).await;
    let listing_client = client.clone();
    let listing =
        nervix_primitives::task::spawn(async move { listing_client.list_domains().await });
    let mut reopened = server.next_exchange().await;
    let first = reopened.next_request().await;
    assert!(
        matches!(first.request, ClientRequest::ListDomains),
        "nothing is restored on the new session, so its first request is the caller's: {:?}",
        first.request
    );
    reopened
        .reply(
            first.request_id,
            ReplyBody::DomainList(DomainList {
                domains: tenant_domains(),
            }),
            &limits(),
        )
        .await;
    within_deadline(listing)
        .await
        .assured("the listing task finishes")
        .assured("the listing succeeds on the new session");
    assert_eq!(
        client.subscription_lifecycle(&live),
        Some(SubscriptionLifecycle::Ended(subscription(1))),
        "a new session leaves an ended subscription ended"
    );

    subscribe_live(&client, &mut reopened, 2).await;
    assert_eq!(
        client.subscription_lifecycle(&live),
        Some(SubscriptionLifecycle::Active(subscription(2))),
        "subscribing under the ended name opens a new generation"
    );
}

#[nervix_primitives::test]
async fn an_end_its_session_lost_before_it_was_read_is_still_reported() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    subscribe_live(&client, &mut exchange, 1).await;
    exchange
        .send(rows_frame(subscription(1), &[(1, "before the end")]))
        .await;
    exchange.send(relay_changed(subscription(1))).await;
    drop(exchange);
    within_deadline(async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if client.inner.events.sinks.subscriptions.is_closed() {
                return;
            }
            nervix_primitives::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    let event = within_deadline(client.next_subscription())
        .await
        .assured("the end is reported after its session ended");
    let SubscriptionEvent::Ended(ended) = event else {
        panic!(
            "the rows ended with their session, and the subscription's end is reported, not \
             {event:?}"
        );
    };
    assert_eq!(ended.subscription, subscription(1));
    assert_eq!(ended.reason, SubscriptionEndReason::RelayChanged);
    let live = SubscriptionName::parse("live").assured("the test name is valid");
    assert_eq!(
        client.subscription_lifecycle(&live),
        Some(SubscriptionLifecycle::Ended(subscription(1)))
    );

    let outcome = within_deadline(client.unsubscribe("live"))
        .await
        .assured("deleting an ended subscription needs no session");
    assert!(outcome.succeeded(), "{}", outcome.message);
    assert_eq!(
        outcome.message,
        "subscription 'live' deleted; the server had already ended it"
    );
    assert_eq!(client.subscription_lifecycle(&live), None);
}

#[nervix_primitives::test]
async fn a_refused_clock_restoration_is_repeated_on_the_same_session() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    let attaching = client.clone();
    let attach = nervix_primitives::task::spawn(async move {
        attaching.attach_domain_clock(domain("tenant")).await
    });
    let request = exchange.next_request().await;
    exchange
        .reply(
            request.request_id,
            attach_reply(clock_in(1, crate::DomainClockObservedState::Unpaced), ""),
            &limits(),
        )
        .await;
    within_deadline(attach)
        .await
        .assured("the attach task completes")
        .assured("the attachment succeeds");

    drop(exchange);
    assert_eq!(
        within_deadline(client.next_domain_clock_event())
            .await
            .assured("the interruption is an event"),
        crate::DomainClockEvent::Interrupted(crate::DomainClockInterruption {
            domain: domain("tenant"),
        })
    );
    let reading = client.clone();
    let next =
        nervix_primitives::task::spawn(async move { reading.next_domain_clock_event().await });
    let mut restored = server.next_exchange().await;
    let request = restored.next_request().await;
    assert!(matches!(
        request.request,
        ClientRequest::AttachDomainClock(_)
    ));
    restored
        .reply(
            request.request_id,
            ReplyBody::DomainClockAttach(crate::DomainClockAttachOutcome {
                disposition: crate::DomainClockAttachDisposition::Failed,
                message: "the session holds a transaction".to_string(),
            }),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(next)
            .await
            .assured("the event task completes")
            .assured("the refusal is reported"),
        crate::DomainClockEvent::RestorationFailed(crate::DomainClockRestorationFailure {
            domain: domain("tenant"),
            message: "the session holds a transaction".to_string(),
            retry_after: Duration::from_secs(1),
        })
    );
    let request = restored.next_request().await;
    assert!(
        matches!(request.request, ClientRequest::AttachDomainClock(_)),
        "a refused restoration is sent again on the same session"
    );
    restored
        .reply(
            request.request_id,
            ReplyBody::Rejected(crate::wire::RequestRejected {
                rejection: crate::wire::RequestRejection::TooManyRequestsInFlight,
                field: None,
                message: "the session already has 64 requests in flight".to_string(),
            }),
            &limits(),
        )
        .await;
    let rejected = within_deadline(client.next_domain_clock_event())
        .await
        .assured("the rejection is reported");
    let crate::DomainClockEvent::RestorationFailed(rejected) = rejected else {
        panic!("a rejected restoration is reported as a failed restoration: {rejected:?}");
    };
    assert_eq!(rejected.retry_after, Duration::from_secs(2));
    assert!(
        rejected.message.contains("TooManyRequestsInFlight"),
        "{}",
        rejected.message
    );
    let request = restored.next_request().await;
    assert!(
        matches!(request.request, ClientRequest::AttachDomainClock(_)),
        "a rejected restoration is sent again on the same session"
    );
    let stopped = clock_in(2, crate::DomainClockObservedState::Stopped);
    restored
        .reply(
            request.request_id,
            attach_reply(stopped.clone(), ""),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(client.next_domain_clock_event())
            .await
            .assured("the restored clock is reported"),
        crate::DomainClockEvent::Observed(crate::DomainClockObserved {
            domain: domain("tenant"),
            clock: stopped.clone(),
        })
    );
    assert_eq!(
        client
            .domain_clock(&domain("tenant"))
            .assured("the restored clock is followed")
            .clock(),
        &stopped
    );
}

#[nervix_primitives::test]
async fn a_clock_restoration_answered_already_attached_follows_the_new_session() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let mut exchange = server.next_exchange().await;
    let attaching = client.clone();
    let attach = nervix_primitives::task::spawn(async move {
        attaching.attach_domain_clock(domain("tenant")).await
    });
    let request = exchange.next_request().await;
    exchange
        .reply(
            request.request_id,
            attach_reply(clock_in(1, crate::DomainClockObservedState::Unpaced), ""),
            &limits(),
        )
        .await;
    within_deadline(attach)
        .await
        .assured("the attach task completes")
        .assured("the attachment succeeds");

    drop(exchange);
    assert_eq!(
        within_deadline(client.next_domain_clock_event())
            .await
            .assured("the interruption is an event"),
        crate::DomainClockEvent::Interrupted(crate::DomainClockInterruption {
            domain: domain("tenant"),
        })
    );
    let reading = client.clone();
    let next =
        nervix_primitives::task::spawn(async move { reading.next_domain_clock_event().await });
    let mut restored = server.next_exchange().await;
    let request = restored.next_request().await;
    restored
        .reply(
            request.request_id,
            ReplyBody::DomainClockAttach(crate::DomainClockAttachOutcome {
                disposition: crate::DomainClockAttachDisposition::AlreadyAttached(domain("tenant")),
                message: "this session already follows the clock of domain 'tenant'".to_string(),
            }),
            &limits(),
        )
        .await;
    let stopped = clock_in(1, crate::DomainClockObservedState::Stopped);
    let frame = crate::DomainClockObserved {
        domain: domain("tenant"),
        clock: stopped.clone(),
    }
    .encode(&limits())
    .assured("a clock frame fits the default limits");
    restored.send(frame).await;
    assert_eq!(
        within_deadline(next)
            .await
            .assured("the event task completes")
            .assured("the clock frame is reported"),
        crate::DomainClockEvent::Observed(crate::DomainClockObserved {
            domain: domain("tenant"),
            clock: stopped,
        })
    );
}

#[nervix_primitives::test]
async fn a_clock_restoration_that_reaches_no_server_is_tried_again_by_the_next_read() {
    let mut primary = TestServer::start().await;
    let unused = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("loopback accepts a test listener");
    let seed_address = unused
        .local_addr()
        .assured("a bound listener has an address");
    drop(unused);
    let seed_url =
        Url::parse(&format!("http://{seed_address}")).assured("the test seed is an HTTP origin");
    let options = ConnectOptions {
        seed_servers: vec![seed_url],
        connect_timeout: Duration::from_millis(250),
        retry_timeout: Duration::from_secs(1),
        ..ConnectOptions::default()
    };
    let client = within_deadline(Client::connect_with_options(
        format!("http://{}", primary.address),
        Some(domain("tenant")),
        options,
    ))
    .await
    .assured("the primary accepts the session");
    let mut exchange = primary.next_exchange().await;
    let attaching = client.clone();
    let attach = nervix_primitives::task::spawn(async move {
        attaching.attach_domain_clock(domain("tenant")).await
    });
    let request = exchange.next_request().await;
    let unpaced = clock_in(1, crate::DomainClockObservedState::Unpaced);
    exchange
        .reply(
            request.request_id,
            attach_reply(unpaced.clone(), ""),
            &limits(),
        )
        .await;
    within_deadline(attach)
        .await
        .assured("the attach task completes")
        .assured("the attachment succeeds");

    drop(exchange);
    primary.stop().await;
    assert_eq!(
        within_deadline(client.next_domain_clock_event())
            .await
            .assured("the interruption is an event"),
        crate::DomainClockEvent::Interrupted(crate::DomainClockInterruption {
            domain: domain("tenant"),
        })
    );
    let failure = within_deadline(client.next_domain_clock_event())
        .await
        .expect_err("no server accepts a session within the retry deadline");
    assert!(
        matches!(failure.current_context(), ClientError::ConnectServer(_)),
        "the reopening fails to connect: {failure:?}"
    );
    assert_eq!(
        client
            .domain_clock(&domain("tenant"))
            .assured("the clock is still followed")
            .clock(),
        &unpaced
    );

    let mut seed = TestServer::start_at(seed_address).await;
    let reading = client.clone();
    let next =
        nervix_primitives::task::spawn(async move { reading.next_domain_clock_event().await });
    let mut restored = seed.next_exchange().await;
    let request = restored.next_request().await;
    let ClientRequest::AttachDomainClock(sent) = request.request else {
        panic!("the first request on the reopened session attaches the followed clock again");
    };
    assert_eq!(sent.domain, domain("tenant"));
    let stopped = clock_in(2, crate::DomainClockObservedState::Stopped);
    restored
        .reply(
            request.request_id,
            attach_reply(stopped.clone(), ""),
            &limits(),
        )
        .await;
    assert_eq!(
        within_deadline(next)
            .await
            .assured("the event task completes")
            .assured("the restored clock is reported"),
        crate::DomainClockEvent::Observed(crate::DomainClockObserved {
            domain: domain("tenant"),
            clock: stopped,
        })
    );
}
