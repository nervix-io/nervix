//! An in-process session server a test answers by hand, while the session a Python thread opened
//! on it waits with the GIL released.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use std::{
    convert::Infallible,
    net::SocketAddr,
    task::{Context, Poll},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::wire::{
    ClientFrame, ClientMessage, ClientRequest, EncodedFrame, Reply, ReplyBody, ReplyDelivery,
    RequestId, ServerFrame, SessionLimits, VerifiedFrame,
    grpc::{EXCHANGE_PATH, SERVICE_NAME, ServerExchangeCodec},
};
use nervix_primitives::{
    net::TcpListener,
    runtime::Runtime,
    stream::wrappers::{ReceiverStream, TcpListenerStream},
    sync::mpsc,
};
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{Grpc, NamedService, StreamingService},
    transport::Server,
};

/// How long a test waits for the session to do what it waits for.
const DEADLINE: Duration = Duration::from_secs(30);

/// The server's side of one exchange a session opened.
pub(super) struct Exchange {
    requests: Streaming<VerifiedFrame<ClientFrame>>,
    frames: mpsc::Sender<Result<EncodedFrame<ServerFrame>, Status>>,
}

/// Serves the session exchange and hands every exchange a session opens to the test.
#[derive(Clone)]
struct SessionService {
    exchanges: mpsc::Sender<Exchange>,
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
        let exchanges = self.exchanges.clone();
        Box::pin(async move {
            if request.uri().path() != EXCHANGE_PATH {
                return Ok(Status::unimplemented("the test serves the exchange only").into_http());
            }
            let mut grpc = Grpc::new(ServerExchangeCodec::new(SessionLimits::DEFAULT));
            Ok(grpc.streaming(OpenExchange { exchanges }, request).await)
        })
    }
}

struct OpenExchange {
    exchanges: mpsc::Sender<Exchange>,
}

impl StreamingService<VerifiedFrame<ClientFrame>> for OpenExchange {
    type Response = EncodedFrame<ServerFrame>;
    type ResponseStream = ReceiverStream<Result<EncodedFrame<ServerFrame>, Status>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<ClientFrame>>>) -> Self::Future {
        let exchanges = self.exchanges.clone();
        Box::pin(async move {
            let (frames, outbound) = mpsc::channel(8);
            let exchange = Exchange {
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

/// An in-process session server, run by its own runtime while the test answers it.
pub(super) struct TestServer {
    runtime: Runtime,
    pub(super) address: SocketAddr,
    exchanges: mpsc::Receiver<Exchange>,
}

impl TestServer {
    pub(super) fn start() -> Self {
        let runtime = nervix_primitives::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .assured("a test runtime starts");
        let (sender, exchanges) = mpsc::channel(4);
        let address = runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .assured("the loopback interface accepts a listener");
            let address = listener
                .local_addr()
                .assured("a bound listener has an address");
            let service = SessionService { exchanges: sender };
            nervix_primitives::task::spawn(async move {
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .assured("the test server serves until its runtime shuts down");
            });
            address
        });
        Self {
            runtime,
            address,
            exchanges,
        }
    }

    /// The next exchange a session opens.
    pub(super) fn next_exchange(&mut self) -> Exchange {
        let exchanges = &mut self.exchanges;
        self.runtime.block_on(async {
            nervix_primitives::time::timeout(DEADLINE, exchanges.recv())
                .await
                .assured("the session opens an exchange within the test deadline")
                .assured("the server keeps handing exchanges to the test")
        })
    }

    /// The next request the session sends on `exchange`.
    pub(super) fn next_request(&self, exchange: &mut Exchange) -> (RequestId, ClientRequest) {
        let message = self.runtime.block_on(async {
            let frame = nervix_primitives::time::timeout(DEADLINE, exchange.requests.message())
                .await
                .assured("the session sends its request within the test deadline")
                .assured("the exchange stays open")
                .assured("the session sends a request");
            ClientMessage::decode(&frame).assured("a request the client encoded decodes")
        });
        (message.request_id, message.request)
    }

    /// Waits until the session ends `exchange` without sending another request.
    pub(super) fn ends(&self, exchange: &mut Exchange) {
        let next = self.runtime.block_on(async {
            nervix_primitives::time::timeout(DEADLINE, exchange.requests.message())
                .await
                .assured("the session ends its exchange within the test deadline")
        });
        if let Ok(Some(_)) = next {
            panic!("the session sent a request where it had to end its exchange");
        }
    }

    /// Answers request `request_id` on `exchange` with `body`.
    pub(super) fn answer(&self, exchange: &Exchange, request_id: RequestId, body: ReplyBody) {
        let delivery = Reply { request_id, body }
            .encode(&SessionLimits::DEFAULT)
            .assured("a test reply fits the default limits");
        let ReplyDelivery::Frame(frame) = delivery else {
            panic!("a test reply fits one frame");
        };
        self.runtime.block_on(async {
            exchange
                .frames
                .send(Ok(frame))
                .await
                .assured("the session keeps reading its exchange");
        });
    }
}
