//! A gRPC peer that forwards real session traffic and substitutes one completed restore reply.
//!
//! Layer: test harness.
//! - **Owns.** A loopback listener, bounded exact restore frames, and the real reply it changes.
//! - **Depends on.** Tonic, the public wire contract, and a channel to the scenario's cluster.
//! - **Must not know.** Client retry logic or server restore execution internals.

#[path = "../fixtures/restore_reply_frames.rs"]
pub(crate) mod frames;

use std::{
    convert::Infallible,
    net::Ipv4Addr,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Stream, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientFrame, RestoreDisposition, RestoreFrame, RestoreMessage, RestoreReply, RestoreReplyFrame,
    RestoreStart, ServerFrame, VerifiedFrame,
    grpc::{EXCHANGE_PATH, RESTORE_BACKUP_PATH, SERVICE_NAME},
};
use nervix_primitives::{
    net::TcpListener,
    stream::wrappers::TcpListenerStream,
    sync::{Arc, Mutex, oneshot},
    task::JoinHandle,
};
use nervix_recovery::Discarded as _;
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{ClientStreamingService, Grpc, NamedService, StreamingService},
    transport::{Channel, Server},
};

/// Archive payloads are capped at 8 MiB; the raw-frame list also has a count bound.
const MAX_ARCHIVE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RESTORE_FRAMES: usize = 64;

#[derive(Debug)]
pub(crate) struct RestoreReceipt {
    pub(crate) start: RestoreStart,
    pub(crate) archive: Vec<u8>,
    pub(crate) outcome: nervix_client_wire::CommandOutcome,
}

struct PeerState {
    channel: Channel,
    fault: frames::ReplyFault,
    receipt: Mutex<Option<oneshot::Sender<RestoreReceipt>>>,
}

#[derive(Clone)]
struct PeerService {
    state: Arc<PeerState>,
}

impl NamedService for PeerService {
    const NAME: &'static str = SERVICE_NAME;
}

impl Service<http::Request<Body>> for PeerService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let state = self.state.clone();
        match request.uri().path() {
            EXCHANGE_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(frames::RawCodec::<ClientFrame>::new());
                Ok(grpc.streaming(ForwardExchange { state }, request).await)
            }),
            RESTORE_BACKUP_PATH => Box::pin(async move {
                let mut grpc = Grpc::new(frames::RawCodec::<RestoreFrame>::new());
                Ok(grpc
                    .client_streaming(ForwardRestore { state }, request)
                    .await)
            }),
            _ => Box::pin(async move {
                Ok(
                    Status::unimplemented("this fixture forwards sessions and restores")
                        .into_http(),
                )
            }),
        }
    }
}

struct ForwardExchange {
    state: Arc<PeerState>,
}

impl ForwardExchange {
    fn outbound<R: nervix_client_wire::FrameRoot>(
        frame: Result<VerifiedFrame<R>, Status>,
    ) -> Result<Vec<u8>, Status> {
        frame.map(|frame| frame.into_bytes().to_vec())
    }
}

impl StreamingService<VerifiedFrame<ClientFrame>> for ForwardExchange {
    type Response = Vec<u8>;
    type ResponseStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<ClientFrame>>>) -> Self::Future {
        let channel = self.state.channel.clone();
        Box::pin(async move {
            let (metadata, extensions, incoming) = request.into_parts();
            // A client dropping its session ends forwarded input. Only successfully decoded
            // messages reach tonic's outbound message stream.
            let incoming = incoming.take_while(|frame| futures_util::future::ready(frame.is_ok()));
            let outgoing = incoming.map(ForwardExchange::outbound::<ClientFrame>);
            let outgoing = outgoing
                .map(|frame| frame.assured("the preceding take_while retained only valid frames"));
            let request = Request::from_parts(metadata, extensions, outgoing);
            let mut client = tonic::client::Grpc::new(channel);
            client
                .ready()
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            let response = client
                .streaming(
                    request,
                    http::uri::PathAndQuery::from_static(EXCHANGE_PATH),
                    frames::RawCodec::<ServerFrame>::new(),
                )
                .await?;
            let outgoing = response
                .into_inner()
                .map(ForwardExchange::outbound::<ServerFrame>);
            let outgoing: Self::ResponseStream = Box::pin(outgoing);
            Ok(Response::new(outgoing))
        })
    }
}

struct ForwardRestore {
    state: Arc<PeerState>,
}

impl ClientStreamingService<VerifiedFrame<RestoreFrame>> for ForwardRestore {
    type Response = Vec<u8>;
    type Future = BoxFuture<Response<Vec<u8>>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<RestoreFrame>>>) -> Self::Future {
        let state = self.state.clone();
        Box::pin(async move {
            let (metadata, extensions, mut incoming) = request.into_parts();
            let first = incoming
                .message()
                .await?
                .assured("the client sends the restore start");
            let RestoreMessage::Start(start) =
                RestoreMessage::decode(&first).assured("the restore start decodes")
            else {
                panic!("the first restore frame is its start");
            };
            let mut frames = vec![first.into_bytes().to_vec()];
            let mut archive = Vec::new();
            while let Some(frame) = incoming.message().await? {
                nervix_primitives::task::consume_budget().await;
                let RestoreMessage::Chunk(chunk) =
                    RestoreMessage::decode(&frame).assured("the restore chunk decodes")
                else {
                    panic!("only chunks follow the restore start");
                };
                let next = archive
                    .len()
                    .checked_add(chunk.bytes().len())
                    .assured("a fixture archive length fits usize");
                assert!(
                    next <= MAX_ARCHIVE_BYTES && frames.len() < MAX_RESTORE_FRAMES,
                    "the fixture's archive and frame buffers are bounded"
                );
                archive.extend_from_slice(chunk.bytes());
                frames.push(frame.into_bytes().to_vec());
            }
            let request =
                Request::from_parts(metadata, extensions, futures_util::stream::iter(frames));
            let mut client = tonic::client::Grpc::new(state.channel.clone());
            client
                .ready()
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            let response = client
                .client_streaming(
                    request,
                    http::uri::PathAndQuery::from_static(RESTORE_BACKUP_PATH),
                    frames::RawCodec::<RestoreReplyFrame>::new(),
                )
                .await?;
            let reply =
                RestoreReply::decode(response.get_ref()).assured("the real Nervix reply decodes");
            // Routing and an applying execution still use the production client's normal
            // repeat. Keep the fault armed for the eventual terminal answer.
            if let RestoreDisposition::Outcome(outcome) = &reply.disposition
                && matches!(
                    outcome.disposition,
                    nervix_client_wire::CommandDisposition::OutcomeUnknown(_)
                        | nervix_client_wire::CommandDisposition::NotLeader(_)
                )
            {
                return Ok(Response::new(response.into_inner().into_bytes().to_vec()));
            }
            let RestoreDisposition::Outcome(outcome) = &reply.disposition else {
                panic!("the real restore was admitted: {reply:?}");
            };
            let receipt = RestoreReceipt {
                start,
                archive,
                outcome: *outcome.clone(),
            };
            let observer = state
                .receipt
                .lock()
                .await
                .take()
                .assured("the peer substitutes exactly one restore reply");
            observer
                .send(receipt)
                .assured("the scenario retains its real restore witness");
            Ok(Response::new(state.fault.substitute(reply)))
        })
    }
}

pub(crate) struct RestoreReplyPeer {
    pub(crate) uri: String,
    received: oneshot::Receiver<RestoreReceipt>,
    task: JoinHandle<()>,
}

impl RestoreReplyPeer {
    pub(crate) async fn start(channel: Channel, fault: frames::ReplyFault) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the fixture can bind loopback");
        let address = listener
            .local_addr()
            .assured("a bound listener has its address");
        let (receipt, received) = oneshot::channel();
        let service = PeerService {
            state: Arc::new(PeerState {
                channel,
                fault,
                receipt: Mutex::new(Some(receipt)),
            }),
        };
        let task = nervix_primitives::task::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .assured("the fixture serves until stopped");
        });
        Self {
            uri: format!("http://{address}"),
            received,
            task,
        }
    }

    pub(crate) async fn finish(mut self) -> RestoreReceipt {
        let receipt = (&mut self.received)
            .await
            .assured("the real restore returned its reply");
        self.task.abort();
        (&mut self.task)
            .await
            .discarded("the fixture listener is deliberately stopped");
        receipt
    }
}

impl Drop for RestoreReplyPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
