//! A bounded gRPC peer changes a restore reply only after a real cluster completed the restore.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Native and CLI observations of unreadable replies and original-reference recovery.
//! - **Depends on.** Public clients, tonic transport, the current wire contract and backup steps.
//! - **Must not know.** Server restore execution, persisted command records or session internals.

use std::{
    convert::Infallible,
    marker::PhantomData,
    process::Output,
    task::{Context, Poll},
};

use bytes::{BufMut as _, Bytes};
use futures_util::stream;
use nervix_client_core::{Client, ClientError, CommandOutcome};
use nervix_client_wire::{
    FrameRoot, RestoreDisposition, RestoreFrame, RestoreMessage, RestoreReply, RestoreReplyFrame,
    RestoreStart, SessionLimits, VerifiedFrame,
    grpc::{FrameCodec, FrameDecoder, RESTORE_BACKUP_PATH, SERVICE_NAME},
};
use nervix_models::{Restore, RestoreLifecycle, RestoreMode, RestoreScope, RestoreState};
use nervix_primitives::{
    net::TcpListener,
    stream::wrappers::TcpListenerStream,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codec::{Codec, EncodeBuf, Encoder},
    codegen::{BoxFuture, Service, http},
    server::{ClientStreamingService, Grpc, NamedService},
    transport::{Channel, Endpoint, Server},
};

use super::*;
use crate::common::{cluster::client_connect_options, raw_session::fresh_execution_reference};

const LIMITS: SessionLimits = SessionLimits::DEFAULT;

#[derive(Debug, Clone, Copy)]
enum Corruption {
    Truncated,
    Oversized,
}

impl Corruption {
    fn parse(name: &str) -> Self {
        match name {
            "truncated" => Self::Truncated,
            "oversized" => Self::Oversized,
            _ => panic!("the scenario names an unknown reply fixture: {name}"),
        }
    }

    fn bytes(self) -> Bytes {
        match self {
            Self::Truncated => Bytes::from_static(b"bad"),
            Self::Oversized => Bytes::from(vec![
                0;
                LIMITS.frame_bytes().checked_add(1).assured(
                    "the fixture adds one byte to the bounded frame limit"
                )
            ]),
        }
    }
}

enum ClientResult {
    Native(error_stack::Result<CommandOutcome, ClientError>),
    Cli(Output),
}

pub(crate) struct Observation {
    result: Option<ClientResult>,
    reference: CommandExecutionReference,
    restore: Restore,
    upstream: String,
    received: RestoreStart,
}

/// Tonic owns all external framing; only deliberately malformed test message bytes vary.
struct RawCodec<Inbound: FrameRoot> {
    inbound: PhantomData<fn() -> Inbound>,
}
struct RawEncoder;

impl<Inbound: FrameRoot> RawCodec<Inbound> {
    fn new() -> Self {
        Self {
            inbound: PhantomData,
        }
    }
}

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, bytes: Bytes, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&bytes);
        Ok(())
    }
}

impl<Inbound: FrameRoot> Codec for RawCodec<Inbound> {
    type Encode = Bytes;
    type Decode = VerifiedFrame<Inbound>;
    type Encoder = RawEncoder;
    type Decoder = FrameDecoder<Inbound>;
    fn encoder(&mut self) -> RawEncoder {
        RawEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        FrameCodec::<RestoreFrame, Inbound>::new(LIMITS).decoder()
    }
}

#[derive(Clone)]
struct ProxyService {
    upstream: Channel,
    corruption: Corruption,
    observed: mpsc::Sender<RestoreStart>,
}

impl NamedService for ProxyService {
    const NAME: &'static str = SERVICE_NAME;
}

impl Service<http::Request<Body>> for ProxyService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let mut service = self.clone();
        Box::pin(async move {
            if request.uri().path() == RESTORE_BACKUP_PATH {
                let mut grpc = Grpc::new(RawCodec::<RestoreFrame>::new())
                    .max_decoding_message_size(LIMITS.frame_bytes())
                    .max_encoding_message_size(
                        LIMITS
                            .frame_bytes()
                            .checked_add(1)
                            .assured("the fixture response limit fits usize"),
                    );
                return Ok(grpc.client_streaming(service, request).await);
            }
            std::future::poll_fn(|context| service.upstream.poll_ready(context))
                .await
                .assured("the real cluster accepts the forwarded session");
            let response = service
                .upstream
                .call(request)
                .await
                .assured("the cluster's session response reaches the peer");
            Ok(response)
        })
    }
}

impl ClientStreamingService<VerifiedFrame<RestoreFrame>> for ProxyService {
    type Response = Bytes;
    type Future = BoxFuture<Response<Bytes>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<RestoreFrame>>>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let (metadata, extensions, mut inbound) = request.into_parts();
            let first = inbound
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("no restore start"))?;
            let RestoreMessage::Start(start) = RestoreMessage::decode(&first)
                .assured("the public client sends a current restore start")
            else {
                return Err(Status::invalid_argument("a restore starts with metadata"));
            };
            let frames =
                stream::once(async move { first.into_bytes() }).chain(inbound.map(|frame| {
                    frame
                        .assured("the public client streams complete frames")
                        .into_bytes()
                }));
            let request = Request::from_parts(metadata, extensions, frames);
            let mut client = tonic::client::Grpc::new(service.upstream)
                .max_encoding_message_size(LIMITS.frame_bytes())
                .max_decoding_message_size(LIMITS.frame_bytes());
            client
                .ready()
                .await
                .assured("the upstream channel remains ready");
            let response = nervix_primitives::time::timeout(
                CLI_BACKUP_TIMEOUT,
                client.client_streaming(
                    request,
                    http::uri::PathAndQuery::from_static(RESTORE_BACKUP_PATH),
                    RawCodec::<RestoreReplyFrame>::new(),
                ),
            )
            .await
            .assured("the upstream restore finishes within the scenario's restore budget")?;
            let reply = RestoreReply::decode(response.get_ref())
                .assured("the real server sends a current restore reply");
            let RestoreDisposition::Outcome(outcome) = reply.disposition else {
                panic!("the real restore must complete before its reply is changed");
            };
            assert!(
                matches!(
                    outcome.disposition,
                    nervix_client_wire::CommandDisposition::Completed { .. }
                ),
                "the real restore completed: {outcome:?}"
            );
            assert_eq!(outcome.execution_reference, start.execution_reference);
            service
                .observed
                .send(start)
                .await
                .assured("the scenario retains the peer's observation");
            Ok(Response::new(service.corruption.bytes()))
        })
    }
}

struct Proxy {
    uri: String,
    observed: mpsc::Receiver<RestoreStart>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Proxy {
    async fn start(upstream: &str, corruption: Corruption) -> Self {
        let uri = url::Url::parse(upstream).assured("the cluster exposes a valid URI");
        assert!(
            matches!(uri.host(), Some(url::Host::Ipv4(_) | url::Host::Ipv6(_))),
            "the fixture forwards to a literal loopback address without name resolution"
        );
        let endpoint = Endpoint::from_shared(upstream.to_string())
            .assured("the cluster's URI is a tonic endpoint");
        let upstream =
            nervix_primitives::time::timeout(Duration::from_secs(30), endpoint.connect())
                .await
                .assured("the peer's connection attempt is bounded")
                .assured("the forwarding peer reaches the real node");
        let listener = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .assured("the fixture binds loopback");
        let address = listener.local_addr().assured("the listener has an address");
        let (observed, received) = mpsc::channel(1);
        let (shutdown, stopped) = oneshot::channel();
        let task = nervix_primitives::task::spawn(async move {
            Server::builder()
                .add_service(ProxyService {
                    upstream,
                    corruption,
                    observed,
                })
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    stopped
                        .await
                        .discarded("dropping the fixture also ends its server");
                })
                .await
                .assured("the scripted peer serves until shutdown");
        });
        Self {
            uri: format!("http://{address}"),
            observed: received,
            shutdown: Some(shutdown),
            task,
        }
    }

    async fn stop(mut self) {
        self.shutdown
            .take()
            .assured("the proxy is stopped once")
            .send(())
            .assured("the proxy is serving until its owner stops it");
        nervix_primitives::time::timeout(Duration::from_secs(30), &mut self.task)
            .await
            .assured("the peer ends after its client's streams close")
            .assured("the peer's server completes without panicking");
    }
}

#[when(
    expr = "the {string} client restores archive {string} through a peer returning a {string} \
            restore reply on node {string}"
)]
async fn when_restore_reply_is_unreadable(
    world: &mut ScenarioWorld,
    kind: String,
    archive: String,
    corruption: String,
    node: String,
) {
    let node = expand_placeholders(world, &node);
    let upstream = world
        .cluster()
        .grpc_uri(&node)
        .assured("the selected node has a gRPC URI");
    let archive = archive_path(world, &archive);
    let restore = Restore {
        scope: RestoreScope::Domain {
            domain: scenario_domain(world, &world.domain),
            target: Some(
                DomainName::parse(&format!("{}_copy", world.domain))
                    .assured("the restored fixture domain is a valid name"),
            ),
        },
        source: archive
            .to_str()
            .assured("the archive fixture path is UTF-8")
            .to_string(),
        mode: RestoreMode::Apply,
        state: RestoreState::All,
        lifecycle: RestoreLifecycle::Stopped,
    };
    let reference = CommandExecutionReference::parse(fresh_execution_reference())
        .assured("a fresh UUID is a valid execution reference");
    let mut proxy = Proxy::start(&upstream, Corruption::parse(&corruption)).await;
    let result = match kind.as_str() {
        "native" => {
            let options = client_connect_options(&proxy.uri)
                .assured("the fixture credentials are configured");
            let client = Client::connect_with_options(&proxy.uri, None, options)
                .await
                .assured("the native client connects through the peer");
            let result = nervix_primitives::time::timeout(
                CLI_BACKUP_TIMEOUT,
                client.restore_with_reference(&restore, &reference, |_| {}),
            )
            .await
            .assured("the bounded restore attempt ends");
            drop(client);
            ClientResult::Native(result)
        }
        "CLI" => {
            let mut command = tokio::process::Command::new(scenario_cli_binary());
            command
                .args([
                    "--server",
                    &proxy.uri,
                    "--username",
                    TEST_AUTH_USERNAME,
                    "--password",
                    TEST_AUTH_PASSWORD,
                    "--command",
                    &restore.to_canonical_plexus(),
                    "--execution-reference",
                    reference.as_str(),
                ])
                .kill_on_drop(true);
            let output = nervix_primitives::time::timeout(CLI_BACKUP_TIMEOUT, command.output())
                .await
                .assured("the bounded CLI restore ends")
                .assured("the CLI starts and is reaped");
            ClientResult::Cli(output)
        }
        _ => panic!("the scenario selects a supported client: {kind}"),
    };
    let received = nervix_primitives::time::timeout(Duration::from_secs(30), proxy.observed.recv())
        .await
        .assured("the peer observed the completed restore")
        .assured("the peer retained its observation");
    proxy.stop().await;
    world.restore_codec_result = Some(Observation {
        result: Some(result),
        reference,
        restore,
        upstream,
        received,
    });
}

#[then(
    "the unreadable restore reply left its outcome uncertain under its original execution \
     reference"
)]
fn then_restore_outcome_is_uncertain(world: &mut ScenarioWorld) {
    let observation = world
        .restore_codec_result
        .as_mut()
        .assured("the restore was attempted");
    assert_eq!(
        observation.received.execution_reference,
        observation.reference
    );
    assert_eq!(
        observation.received.statement,
        observation.restore.to_canonical_plexus()
    );
    match observation
        .result
        .take()
        .assured("the client result is observed once")
    {
        ClientResult::Native(Err(report)) => {
            assert!(
                matches!(report.current_context(),
                ClientError::UncertainCommand { reference } if reference == &observation.reference),
                "{report:#}"
            );
            let failure = Box::new(nervix_client_ffi::Failure::from(report));
            assert_eq!(failure.kind(), nervix_client_ffi::FailureKind::Uncertain);
            assert_eq!(failure.execution_reference(), Some(&observation.reference));
            let mut reference_bytes = std::ptr::null();
            let mut reference_length = 0;
            // SAFETY: the library-created failure stays live in its Box, both out-parameters are
            // writable, and the returned bytes remain borrowed from it until after this block.
            unsafe {
                assert_eq!(
                    nervix_client_ffi::nx_error_kind_of(&*failure),
                    nervix_client_ffi::FailureKind::Uncertain
                );
                assert!(nervix_client_ffi::nx_error_execution_reference(
                    &*failure,
                    &mut reference_bytes,
                    &mut reference_length
                ));
                assert_eq!(
                    std::slice::from_raw_parts(reference_bytes, reference_length),
                    observation.reference.as_str().as_bytes()
                );
            }
        }
        ClientResult::Native(Ok(outcome)) => {
            panic!("an unreadable reply established an outcome: {outcome:?}")
        }
        ClientResult::Cli(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert_eq!(
                output.status.code(),
                Some(3),
                "stdout: {stdout}; stderr: {stderr}"
            );
            assert!(stdout.contains("outcome: not known yet"), "{stdout}");
            assert!(stdout.contains(observation.reference.as_str()), "{stdout}");
        }
    }
}

#[then("repeating the restore under that execution reference recovers its completed outcome")]
async fn then_original_restore_is_recovered(world: &mut ScenarioWorld) {
    let observation = world
        .restore_codec_result
        .as_ref()
        .assured("the restore was attempted");
    let options = client_connect_options(&observation.upstream)
        .assured("the fixture credentials are configured");
    let client = Client::connect_with_options(&observation.upstream, None, options)
        .await
        .assured("the recovery client connects directly to the real node");
    let outcome = nervix_primitives::time::timeout(
        CLI_BACKUP_TIMEOUT,
        client.restore_with_reference(&observation.restore, &observation.reference, |_| {}),
    )
    .await
    .assured("the bounded recovery finishes")
    .assured("the original restore is recovered");
    assert!(outcome.succeeded(), "{outcome:?}");
    assert_eq!(
        outcome.execution_reference.as_ref(),
        Some(&observation.reference)
    );
    assert_eq!(
        outcome.origin,
        Some(nervix_client_wire::OutcomeOrigin::Recovered)
    );
}
