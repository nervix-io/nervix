//! A deterministic public-session peer for the Ruby boundary's lifetime regression.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** Answering one producer's open, submit and close with typed wire replies.
//! - **Depends on.** The session codec, tonic, selected primitives and peer-withdrawal reporting.
//! - **Must not know.** Ruby values, binding layouts or the client's recovery state machine.

use std::{
    convert::Infallible,
    num::{NonZeroU32, NonZeroU64},
    task::{Context, Poll},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    AckWindow, ClientAttachmentId, ClientEndpointContract, ClientProducerAdmission,
    ClientProducerDescription, ClientProducerGrant, ClientProducerPolicy, DomainName, IngestorName,
    wire::{
        ClientFrame, ClientMessage, ClientRequest, CloseIngestorDisposition, CloseIngestorOutcome,
        EncodedFrame, OpenIngestorDisposition, OpenIngestorOutcome, ProducerOpened, Reply,
        ReplyBody, ReplyDelivery, ServerFrame, SessionLimits, SubmissionOutcome, VerifiedFrame,
        grpc::{EXCHANGE_PATH, SERVICE_NAME, ServerExchangeCodec},
    },
};
use nervix_models::{ClientSubmissionOutcome, ParseAsType, SchemaField};
use nervix_primitives::{
    net::TcpListener,
    runtime::Runtime,
    stream::wrappers::{ReceiverStream, TcpListenerStream},
    sync::mpsc,
};
use nervix_recovery::NoReceiver as _;
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{Grpc, NamedService, StreamingService},
    transport::Server,
};

pub(super) struct Peer {
    _runtime: Runtime,
    pub(super) uri: String,
}
impl Peer {
    pub(super) fn start() -> Self {
        let runtime = nervix_primitives::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .assured("the fixture runtime starts");
        let address = runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .assured("a fixture listener binds");
            let address = listener
                .local_addr()
                .assured("a bound listener has an address");
            nervix_primitives::task::spawn(async move {
                Server::builder()
                    .add_service(PeerService)
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .assured("the peer serves until its runtime ends");
            });
            address
        });
        Self {
            _runtime: runtime,
            uri: format!("http://{address}"),
        }
    }
}

#[derive(Clone)]
struct PeerService;
impl NamedService for PeerService {
    const NAME: &'static str = SERVICE_NAME;
}
impl Service<http::Request<Body>> for PeerService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        Box::pin(async move {
            if request.uri().path() != EXCHANGE_PATH {
                return Ok(Status::unimplemented("exchange only").into_http());
            }
            Ok(Grpc::new(ServerExchangeCodec::new(SessionLimits::DEFAULT))
                .streaming(PeerService, request)
                .await)
        })
    }
}
impl StreamingService<VerifiedFrame<ClientFrame>> for PeerService {
    type Response = EncodedFrame<ServerFrame>;
    type ResponseStream = ReceiverStream<Result<Self::Response, Status>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;
    fn call(&mut self, request: Request<Streaming<VerifiedFrame<ClientFrame>>>) -> Self::Future {
        Box::pin(async move {
            let mut requests = request.into_inner();
            let (sender, receiver) = mpsc::channel(8);
            nervix_primitives::task::spawn(async move {
                while let Ok(Some(frame)) = requests.message().await {
                    let message =
                        ClientMessage::decode(&frame).assured("a client-encoded request decodes");
                    let body = match message.request {
                        ClientRequest::OpenIngestor(_) => opened(),
                        ClientRequest::SubmitBatch(_) => ReplyBody::Submission(SubmissionOutcome {
                            outcome: ClientSubmissionOutcome::Completed,
                            message: String::new(),
                        }),
                        ClientRequest::CloseIngestor(_) => {
                            ReplyBody::CloseIngestor(CloseIngestorOutcome {
                                disposition: CloseIngestorDisposition::Closed,
                                message: String::new(),
                            })
                        }
                        _ => {
                            sender
                                .send(Err(Status::invalid_argument("unexpected fixture request")))
                                .await
                                .means_peer_left("the Ruby lifetime fixture client");
                            return;
                        }
                    };
                    let reply = Reply {
                        request_id: message.request_id,
                        body,
                    }
                    .encode(&SessionLimits::DEFAULT)
                    .assured("the fixture reply fits one frame");
                    let ReplyDelivery::Frame(reply) = reply else {
                        panic!("the small fixture reply fits one frame");
                    };
                    let sent = sender.send(Ok(reply)).await;
                    let peer_left = sent.is_err();
                    sent.means_peer_left("the Ruby lifetime fixture client");
                    if peer_left {
                        return;
                    }
                }
            });
            let mut response = Response::new(ReceiverStream::new(receiver));
            nervix_client_core::wire::grpc::limits::write(
                response.metadata_mut(),
                SessionLimits::DEFAULT,
            );
            Ok(response)
        })
    }
}
fn opened() -> ReplyBody {
    let fields = vec![SchemaField {
        name: "id".try_into().assured("a valid fixture field"),
        ty: ParseAsType::U32,
        optional: false,
        sensitive: false,
    }];
    let description = ClientProducerDescription {
        attachment: ClientAttachmentId::from_u128(7),
        fields,
        generation: 3,
        contract: ClientEndpointContract::from_digest([5; 32]),
        policy: ClientProducerPolicy {
            window: AckWindow::Parallel {
                max: NonZeroU64::new(2).assured("two is positive"),
            },
            ack_timeout: Duration::from_secs(30),
            retry_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_secs(1),
        },
        grant: ClientProducerGrant {
            batches: NonZeroU32::new(2).assured("two is positive"),
            bytes: NonZeroU64::new(4096).assured("a positive capacity"),
            max_batch_bytes: NonZeroU64::new(4096).assured("a positive capacity"),
            max_batch_rows: NonZeroU32::new(16).assured("a positive row count"),
        },
        admission: ClientProducerAdmission::Open,
    };
    ReplyBody::OpenIngestor(OpenIngestorOutcome {
        disposition: OpenIngestorDisposition::Opened(Box::new(ProducerOpened {
            domain: DomainName::parse("ruby_test").assured("a valid domain"),
            ingestor: IngestorName::parse("input").assured("a valid ingestor"),
            description,
        })),
        message: "opened".to_string(),
    })
}
