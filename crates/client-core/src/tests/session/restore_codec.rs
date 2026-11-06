//! Real gRPC restore replies rejected by the production receive path.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Bounded scripted restore replies after receiving the complete original archive.
//! - **Depends on.** The session fixture, tonic's framing, and the current restore wire contract.
//! - **Must not know.** Server restore admission, planning, or persistence.

mod restore_uncertainty;

use std::{
    convert::Infallible,
    task::{Context, Poll},
};

use bytes::{BufMut as _, Bytes};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientRequest, DomainList, FrameError, ReplyBody, RestoreChunk, RestoreDisposition,
    RestoreFrame, RestoreMessage, RestoreReply, RestoreStart, RestoreUploadFailure, VerifiedFrame,
    grpc::{
        ClientRestoreCodec, FrameDecoder, FrameRejection, RESTORE_BACKUP_PATH, SERVICE_NAME,
        ServerRestoreCodec,
    },
};
use nervix_models::{
    CommandExecutionReference, Restore, RestoreLifecycle, RestoreMode, RestoreScope, RestoreState,
};
use nervix_primitives::{
    sync::{Arc, mpsc, oneshot},
    task,
};
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codec::{Codec, EncodeBuf, Encoder},
    codegen::{BoxFuture, Service, http},
    server::{Grpc, NamedService, StreamingService},
};

use super::{ServerExchange, SessionService, TestServer, domain, limits, within_deadline};
use crate::{
    ClientError, ConnectOptions,
    exchange::{Exchange, PendingReplies},
};

/// A complete received restore, held before its scripted answer is sent.
#[derive(Debug)]
struct ReceivedRestore {
    start: RestoreStart,
    archive: Vec<u8>,
    answer: oneshot::Sender<ScriptedReply>,
}

#[derive(Debug)]
enum ScriptedReply {
    Message(Bytes),
    Refusal(Status),
    BodyFailure(Status),
    Empty,
}

#[derive(Clone)]
struct RestoreService {
    session: SessionService,
    restores: mpsc::Sender<ReceivedRestore>,
    archive_read: ArchiveRead,
}

#[derive(Clone, Copy)]
enum ArchiveRead {
    Complete,
    StartOnly,
}

impl NamedService for RestoreService {
    const NAME: &'static str = SERVICE_NAME;
}

impl Service<http::Request<Body>> for RestoreService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.session.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        if request.uri().path() != RESTORE_BACKUP_PATH {
            return self.session.call(request);
        }
        let restores = self.restores.clone();
        let archive_read = self.archive_read;
        Box::pin(async move {
            let mut grpc = Grpc::new(RawRestoreCodec).max_encoding_message_size(
                limits()
                    .frame_bytes()
                    .checked_add(1)
                    .assured("one byte beyond the bounded fixture frame fits usize"),
            );
            Ok(grpc
                .streaming(
                    ReceiveRestore {
                        restores,
                        archive_read,
                    },
                    request,
                )
                .await)
        })
    }
}

struct ReceiveRestore {
    restores: mpsc::Sender<ReceivedRestore>,
    archive_read: ArchiveRead,
}

impl StreamingService<VerifiedFrame<RestoreFrame>> for ReceiveRestore {
    type Response = Bytes;
    type ResponseStream =
        nervix_primitives::stream::wrappers::ReceiverStream<Result<Bytes, Status>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<RestoreFrame>>>) -> Self::Future {
        let restores = self.restores.clone();
        let archive_read = self.archive_read;
        Box::pin(async move {
            let mut stream = request.into_inner();
            let first = stream
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("no start"))?;
            let RestoreMessage::Start(start) = RestoreMessage::decode(&first)
                .assured("the production client encodes its restore start")
            else {
                return Err(Status::invalid_argument("a restore starts with metadata"));
            };
            let mut archive = Vec::new();
            if let ArchiveRead::Complete = archive_read {
                while let Some(frame) = stream.message().await? {
                    task::consume_budget().await;
                    let RestoreMessage::Chunk(chunk) = RestoreMessage::decode(&frame)
                        .assured("the production client encodes its restore chunks")
                    else {
                        return Err(Status::invalid_argument("chunks follow the start"));
                    };
                    archive.extend_from_slice(chunk.bytes());
                }
            }
            let (answer, reply) = oneshot::channel();
            restores
                .send(ReceivedRestore {
                    start,
                    archive,
                    answer,
                })
                .await
                .assured("the test receives the restore before answering it");
            let item = match reply.await.assured("the test sends the scripted answer") {
                ScriptedReply::Message(bytes) => Some(Ok(bytes)),
                ScriptedReply::Refusal(status) => return Err(status),
                ScriptedReply::BodyFailure(status) => Some(Err(status)),
                ScriptedReply::Empty => None,
            };
            let (outbound, receiver) = mpsc::channel(1);
            if let Some(item) = item {
                outbound
                    .send(item)
                    .await
                    .assured("one answer fits the bounded reply channel");
            }
            let mut response = Response::new(
                nervix_primitives::stream::wrappers::ReceiverStream::new(receiver),
            );
            for value in ["first", "second"] {
                response.metadata_mut().append(
                    "restore-peer",
                    value.parse().assured("the fixture metadata is ASCII"),
                );
            }
            Ok(response)
        })
    }
}

/// Tonic owns all gRPC framing; only the restore reply's deliberately malformed bytes vary.
struct RawRestoreCodec;

struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, bytes: Bytes, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&bytes);
        Ok(())
    }
}

impl Codec for RawRestoreCodec {
    type Encode = Bytes;
    type Decode = VerifiedFrame<RestoreFrame>;
    type Encoder = RawEncoder;
    type Decoder = FrameDecoder<RestoreFrame>;

    fn encoder(&mut self) -> RawEncoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        ServerRestoreCodec::new(limits()).decoder()
    }
}

impl TestServer {
    async fn with_restore_codec() -> (Self, mpsc::Receiver<ReceivedRestore>) {
        Self::with_restore_reading(ArchiveRead::Complete).await
    }

    async fn with_restore_reading(
        archive_read: ArchiveRead,
    ) -> (Self, mpsc::Receiver<ReceivedRestore>) {
        let (restores, received) = mpsc::channel(4);
        let server = Self::start_service(([127, 0, 0, 1], 0).into(), false, |session| {
            RestoreService {
                session,
                restores,
                archive_read,
            }
        })
        .await;
        (server, received)
    }
}

#[derive(Debug, Clone, Copy)]
enum RejectedReply {
    Truncated,
    WrongIdentifier,
    Invalid,
    Oversized,
}

impl RejectedReply {
    fn bytes(self) -> Bytes {
        match self {
            Self::Truncated => Bytes::from_static(b"bad"),
            Self::WrongIdentifier => Bytes::from_static(&[0; 8]),
            Self::Invalid => Bytes::from_static(b"\xff\xff\xff\xffNXRR"),
            Self::Oversized => Bytes::from(vec![
                0;
                limits().frame_bytes().checked_add(1).assured(
                    "the bounded fixture adds one byte to the configured frame limit"
                )
            ]),
        }
    }
}

async fn restore_request() -> (tempfile::TempDir, Restore, Vec<u8>) {
    let directory = tempfile::tempdir().assured("the test creates its archive directory");
    let path = directory.path().join("restore.bin");
    let archive = b"the exact archive bytes of one logical restore".to_vec();
    tokio::fs::write(&path, &archive)
        .await
        .assured("the test writes its archive");
    let restore = Restore {
        scope: RestoreScope::Domain {
            domain: domain("archived"),
            target: Some(domain("restored")),
        },
        source: path
            .to_str()
            .assured("the temporary path is UTF-8")
            .to_string(),
        mode: RestoreMode::Apply,
        state: RestoreState::All,
        lifecycle: RestoreLifecycle::Stopped,
    };
    (directory, restore, archive)
}

async fn assert_exchange_answers(client: &crate::Client, exchange: &mut ServerExchange) {
    let caller = client.clone();
    let call = task::spawn(async move { caller.list_domains().await });
    let request = exchange.next_request().await;
    assert!(matches!(request.request, ClientRequest::ListDomains));
    exchange
        .reply(
            request.request_id,
            ReplyBody::DomainList(DomainList {
                domains: Vec::new(),
            }),
            &limits(),
        )
        .await;
    assert!(
        within_deadline(call)
            .await
            .assured("the read task completes")
            .assured("the surviving exchange answers its read")
            .is_empty()
    );
}

async fn assert_rejected_reply_is_uncertain(rejected: Option<RejectedReply>) {
    let (mut server, mut restores) = TestServer::with_restore_codec().await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let _exchange = server.next_exchange().await;
    let pending = client.inner.exchange.lock().await.requests.pending.clone();
    let (_directory, restore, archive) = restore_request().await;
    let reference = CommandExecutionReference::parse("restore-codec-rejection")
        .assured("the test execution reference is valid");
    let caller = client.clone();
    let sent_restore = restore.clone();
    let sent_reference = reference.clone();
    let call = task::spawn(async move {
        caller
            .restore_with_reference(&sent_restore, &sent_reference, |_| {})
            .await
    });
    let received = within_deadline(restores.recv())
        .await
        .assured("the complete restore arrives");
    assert_eq!(received.archive, archive);
    assert_eq!(
        received.start.archive.total_bytes.get(),
        u64::try_from(archive.len()).assured("the bounded fixture archive fits u64")
    );
    assert_eq!(
        received.start.archive.digest.as_bytes(),
        blake3::hash(&archive).as_bytes()
    );
    assert_eq!(received.start.execution_reference, reference);
    assert_eq!(received.start.statement, restore.to_canonical_plexus());
    received
        .answer
        .send(match rejected {
            Some(rejected) => ScriptedReply::Message(rejected.bytes()),
            None => ScriptedReply::Empty,
        })
        .assured("the restore waits for its reply");
    let result = within_deadline(call)
        .await
        .assured("the restore task completes");
    let error = result
        .err()
        .assured("the invalid reply cannot establish the restore outcome");
    assert!(
        matches!(error.current_context(), ClientError::UncertainCommand { reference: actual }
            if actual == &reference),
        "{rejected:?}: {error:#}"
    );
    let status = error
        .downcast_ref::<Status>()
        .assured("the uncertain report retains the original receive status");
    if rejected.is_some() {
        assert_eq!(
            status
                .metadata()
                .get_all("restore-peer")
                .iter()
                .map(|value| value.to_str().assured("the peer metadata is ASCII"))
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }
    match rejected {
        Some(RejectedReply::Oversized) => {
            assert_eq!(status.code(), tonic::Code::OutOfRange);
            assert!(FrameRejection::find_in(status).is_none());
        }
        Some(rejection) => {
            let cause = FrameRejection::find_in(status)
                .assured("local codec provenance and the full report remain typed");
            assert!(matches!(
                (rejection, cause.report().current_context()),
                (RejectedReply::Truncated, FrameError::Truncated { .. })
                    | (
                        RejectedReply::WrongIdentifier,
                        FrameError::WrongIdentifier { .. }
                    )
                    | (RejectedReply::Invalid, FrameError::Invalid { .. })
            ));
        }
        None => {
            assert_eq!(status.code(), tonic::Code::Internal);
            assert!(FrameRejection::find_in(status).is_none());
        }
    }
    assert!(
        matches!(&*pending.lock(), PendingReplies::Closed(..)),
        "{rejected:?}: a protocol violation ends the exact selected session"
    );
    let replacement = Exchange::open(
        client.current_channel().await,
        &client.inner.connector,
        client.inner.events.emitters.clone(),
    )
    .await
    .assured("the server accepts a subsequent session");
    client.install(replacement).await;
    let mut replacement = server.next_exchange().await;
    assert_exchange_answers(&client, &mut replacement).await;
}

#[nervix_primitives::test]
async fn a_truncated_restore_reply_is_uncertain() {
    assert_rejected_reply_is_uncertain(Some(RejectedReply::Truncated)).await;
}

#[nervix_primitives::test]
async fn a_wrong_identifier_restore_reply_is_uncertain() {
    assert_rejected_reply_is_uncertain(Some(RejectedReply::WrongIdentifier)).await;
}

#[nervix_primitives::test]
async fn an_invalid_restore_reply_is_uncertain() {
    assert_rejected_reply_is_uncertain(Some(RejectedReply::Invalid)).await;
}

#[nervix_primitives::test]
async fn an_oversized_restore_reply_is_uncertain() {
    assert_rejected_reply_is_uncertain(Some(RejectedReply::Oversized)).await;
}

#[nervix_primitives::test]
async fn an_empty_restore_response_is_uncertain() {
    assert_rejected_reply_is_uncertain(None).await;
}

#[nervix_primitives::test]
async fn a_restore_codec_rejection_preserves_a_replacement_session() {
    let (mut server, mut restores) = TestServer::with_restore_codec().await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let _original = server.next_exchange().await;
    let (_directory, restore, _) = restore_request().await;
    let reference = CommandExecutionReference::parse("restore-codec-replacement")
        .assured("the test execution reference is valid");
    let caller = client.clone();
    let call = task::spawn(async move {
        caller
            .restore_with_reference(&restore, &reference, |_| {})
            .await
    });
    let received = within_deadline(restores.recv())
        .await
        .assured("the complete restore arrives");
    let replacement = Exchange::open(
        client.current_channel().await,
        &client.inner.connector,
        client.inner.events.emitters.clone(),
    )
    .await
    .assured("the server accepts the replacement");
    let generation = replacement.generation.clone();
    client.install(replacement).await;
    let mut replacement = server.next_exchange().await;
    received
        .answer
        .send(ScriptedReply::Message(RejectedReply::Truncated.bytes()))
        .assured("the restore waits for its reply");
    let result = within_deadline(call)
        .await
        .assured("the restore task completes");
    let error = result
        .err()
        .assured("the invalid reply cannot establish the restore outcome");
    assert!(
        matches!(
            error.current_context(),
            ClientError::UncertainCommand { .. }
        ),
        "{error:#}"
    );
    let exchange = client.inner.exchange.lock().await;
    assert!(Arc::ptr_eq(&exchange.generation, &generation));
    assert!(!matches!(
        &*exchange.requests.pending.lock(),
        PendingReplies::Closed(..)
    ));
    drop(exchange);
    assert_exchange_answers(&client, &mut replacement).await;
}

#[nervix_primitives::test]
async fn a_server_refusal_of_a_restore_stays_definitive() {
    for status in [
        Status::internal("server refusal"),
        Status::resource_exhausted("server quota"),
        Status::out_of_range("server range refusal"),
        Status::permission_denied("server authorization"),
        Status::unauthenticated("server credentials"),
    ] {
        task::consume_budget().await;
        let code = status.code();
        assert_server_refusal_stays_definitive(ScriptedReply::Refusal(status), code).await;
    }
}

#[nervix_primitives::test]
async fn authentication_refused_after_restore_headers_stays_definitive() {
    for status in [
        Status::permission_denied("server authorization"),
        Status::unauthenticated("server credentials"),
    ] {
        task::consume_budget().await;
        let code = status.code();
        assert_server_refusal_stays_definitive(ScriptedReply::BodyFailure(status), code).await;
    }
}

async fn assert_server_refusal_stays_definitive(reply: ScriptedReply, code: tonic::Code) {
    let (mut server, mut restores) = TestServer::with_restore_codec().await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let _exchange = server.next_exchange().await;
    let generation = client.inner.exchange.lock().await.generation.clone();
    let (_directory, restore, _) = restore_request().await;
    let reference = CommandExecutionReference::parse("restore-server-refusal")
        .assured("the test execution reference is valid");
    let caller = client.clone();
    let call = task::spawn(async move {
        caller
            .restore_with_reference(&restore, &reference, |_| {})
            .await
    });
    let received = within_deadline(restores.recv())
        .await
        .assured("the complete restore arrives");
    received
        .answer
        .send(reply)
        .assured("the restore waits for its reply");
    let result = within_deadline(call)
        .await
        .assured("the restore task completes");
    let error = result.err().assured("the server refuses this call");
    assert!(
        matches!(error.current_context(), ClientError::Restore(status) if status.code() == code),
        "{error:#}"
    );
    let exchange = client.inner.exchange.lock().await;
    assert!(Arc::ptr_eq(&exchange.generation, &generation));
    assert!(!matches!(
        &*exchange.requests.pending.lock(),
        PendingReplies::Closed(..)
    ));
}

#[nervix_primitives::test]
async fn a_declared_restore_stream_refusal_stays_definitive() {
    for failure in [
        RestoreUploadFailure::InvalidStream,
        RestoreUploadFailure::InvalidStatement,
        RestoreUploadFailure::SizeMismatch,
        RestoreUploadFailure::DigestMismatch,
        RestoreUploadFailure::QuotaExceeded,
        RestoreUploadFailure::StagingFailed,
    ] {
        task::consume_budget().await;
        let (mut server, mut restores) = TestServer::with_restore_codec().await;
        let client = server.connect_with(ConnectOptions::default()).await;
        let mut exchange = server.next_exchange().await;
        let generation = client.inner.exchange.lock().await.generation.clone();
        let (_directory, restore, _) = restore_request().await;
        let reference = CommandExecutionReference::parse("restore-stream-refusal")
            .assured("the test execution reference is valid");
        let caller = client.clone();
        let expected_reference = reference.clone();
        let call = task::spawn(async move {
            caller
                .restore_with_reference(&restore, &reference, |_| {})
                .await
        });
        let received = within_deadline(restores.recv())
            .await
            .assured("the restore arrives");
        let reply = RestoreReply {
            request_id: Some(received.start.request_id),
            disposition: RestoreDisposition::UploadFailed {
                failure,
                message: "the stream was refused before restore admission".to_string(),
            },
        }
        .encode(&limits())
        .assured("the declared refusal fits one frame");
        received
            .answer
            .send(ScriptedReply::Message(reply.into_bytes()))
            .assured("the restore awaits its reply");
        let outcome = within_deadline(call)
            .await
            .assured("the restore task completes")
            .assured("the valid refusal establishes an outcome");
        assert!(!outcome.succeeded(), "{failure:?}: {outcome:?}");
        assert_eq!(
            outcome.execution_reference.as_ref(),
            Some(&expected_reference)
        );
        assert!(Arc::ptr_eq(
            &client.inner.exchange.lock().await.generation,
            &generation
        ));
        assert_exchange_answers(&client, &mut exchange).await;
    }
}

#[nervix_primitives::test]
async fn a_local_restore_archive_refusal_never_starts_a_stream() {
    let (mut server, mut restores) = TestServer::with_restore_codec().await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let mut exchange = server.next_exchange().await;
    let (_directory, restore, _) = restore_request().await;
    let reference = CommandExecutionReference::parse("restore-local-refusal")
        .assured("the test execution reference is valid");
    tokio::fs::remove_file(&restore.source)
        .await
        .assured("the archive fixture exists");
    let missing = client
        .restore_with_reference(&restore, &reference, |_| {})
        .await
        .err()
        .assured("the missing archive is refused locally");
    assert!(
        matches!(
            missing.current_context(),
            ClientError::ReadRestoreArchive { .. }
        ),
        "{missing:#}"
    );
    tokio::fs::write(&restore.source, [])
        .await
        .assured("the fixture writes an empty archive");
    let empty = client
        .restore_with_reference(&restore, &reference, |_| {})
        .await
        .err()
        .assured("the empty archive is refused locally");
    assert!(
        matches!(
            empty.current_context(),
            ClientError::EmptyRestoreArchive { .. }
        ),
        "{empty:#}"
    );
    assert!(matches!(
        restores.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_exchange_answers(&client, &mut exchange).await;
}

async fn assert_retry_preserves_the_original_archive(reply: ScriptedReply) {
    let (mut server, mut restores) = TestServer::with_restore_codec().await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let _original = server.next_exchange().await;
    let (_directory, restore, archive) = restore_request().await;
    let reference = CommandExecutionReference::parse("restore-retry-original")
        .assured("the test execution reference is valid");
    let caller = client.clone();
    let call = task::spawn(async move {
        caller
            .restore_with_reference(&restore, &reference, |_| {})
            .await
    });
    let first = within_deadline(restores.recv())
        .await
        .assured("the first complete attempt arrives");
    assert_eq!(first.archive, archive);
    first
        .answer
        .send(reply)
        .assured("the first call waits for its status");
    let _replacement = server.next_exchange().await;
    let retried = within_deadline(restores.recv())
        .await
        .assured("the retried complete attempt arrives");
    assert_eq!(retried.start, first.start);
    assert_eq!(retried.archive, archive);
    let reply = RestoreReply {
        request_id: Some(retried.start.request_id),
        disposition: RestoreDisposition::Outcome(Box::new(nervix_client_wire::CommandOutcome {
            execution_reference: retried.start.execution_reference.clone(),
            origin: nervix_client_wire::OutcomeOrigin::Recovered,
            disposition: nervix_client_wire::CommandDisposition::Completed {
                already_existed: false,
            },
            message: "the original restore completed".to_string(),
            diagnostics: Vec::new(),
            statements: Vec::new(),
            transaction: None,
            transaction_admission: None,
            inspection: None,
            wasm_state: None,
            resource: None,
            backup: None,
            restore: None,
        })),
    }
    .encode(&limits())
    .assured("the authoritative outcome fits one frame");
    retried
        .answer
        .send(ScriptedReply::Message(reply.into_bytes()))
        .assured("the retry waits for its reply");
    let outcome = within_deadline(call)
        .await
        .assured("the restore task completes")
        .assured("the original execution's outcome is recovered");
    assert!(outcome.succeeded(), "{outcome:?}");
    assert_eq!(
        outcome.execution_reference.as_ref(),
        Some(&retried.start.execution_reference)
    );
    assert_eq!(
        outcome.origin,
        Some(nervix_client_wire::OutcomeOrigin::Recovered)
    );
}

#[nervix_primitives::test]
async fn a_retryable_restore_call_resends_the_original_archive_and_reference() {
    assert_retry_preserves_the_original_archive(ScriptedReply::Refusal(Status::unavailable(
        "the transport ended",
    )))
    .await;
}

#[nervix_primitives::test]
async fn a_restore_body_transport_failure_retries_the_original_archive() {
    assert_retry_preserves_the_original_archive(ScriptedReply::BodyFailure(Status::unavailable(
        "the reply transport ended",
    )))
    .await;
}

#[nervix_primitives::test]
async fn a_local_restore_decoder_failure_retains_its_cause() {
    let (server, mut restores) = TestServer::with_restore_codec().await;
    let (_directory, restore, archive) = restore_request().await;
    let start = RestoreStart {
        request_id: nervix_client_wire::RequestId::new(std::num::NonZeroU64::MIN),
        execution_reference: CommandExecutionReference::parse("restore-decoder-cause")
            .assured("the test execution reference is valid"),
        statement: restore.to_canonical_plexus(),
        archive: nervix_models::RestoreArchive {
            total_bytes: std::num::NonZeroU64::new(
                u64::try_from(archive.len()).assured("the bounded archive fits u64"),
            )
            .assured("the archive is nonempty"),
            digest: nervix_models::ArchiveDigest::from_bytes(*blake3::hash(&archive).as_bytes()),
        },
    };
    let frames = [
        start
            .encode(&limits())
            .assured("the restore start fits one frame"),
        RestoreChunk::encode(&archive, &limits()).assured("the tiny archive fits one chunk"),
    ];
    let (outbound, receiver) = mpsc::channel(2);
    for frame in frames {
        task::consume_budget().await;
        outbound
            .send(frame)
            .await
            .assured("the bounded channel holds the start and chunk");
    }
    drop(outbound);
    let channel = within_deadline(
        tonic::transport::Channel::from_shared(format!("http://{}", server.address))
            .assured("the fixture has a valid literal loopback URI")
            .connect(),
    )
    .await
    .assured("the scripted peer accepts the codec connection");
    let call = task::spawn(async move {
        let mut client =
            tonic::client::Grpc::new(channel).max_decoding_message_size(limits().frame_bytes());
        client
            .ready()
            .await
            .assured("the scripted peer remains ready");
        client
            .client_streaming(
                Request::new(nervix_primitives::stream::wrappers::ReceiverStream::new(
                    receiver,
                )),
                http::uri::PathAndQuery::from_static(RESTORE_BACKUP_PATH),
                ClientRestoreCodec::new(limits()),
            )
            .await
    });
    let received = within_deadline(restores.recv())
        .await
        .assured("the whole archive reaches the peer");
    assert_eq!(received.archive, archive);
    received
        .answer
        .send(ScriptedReply::Message(RejectedReply::Truncated.bytes()))
        .assured("the codec call awaits its reply");
    let status = within_deadline(call)
        .await
        .assured("the codec call task completes")
        .err()
        .assured("the production decoder rejects the frame");
    assert_eq!(status.code(), tonic::Code::Internal);
    let rejection = FrameRejection::find_in(&status)
        .assured("the local decoder preserves its typed verification report");
    assert!(matches!(
        rejection.report().current_context(),
        FrameError::Truncated { actual: 3, .. }
    ));
}
