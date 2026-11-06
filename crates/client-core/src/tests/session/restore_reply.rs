//! Real restore streams whose replies fail semantic or transport identity validation.
//!
//! Layer: test harness.
//! - **Owns.** Complete archive witnesses, exact-session interruption and replacement preservation.
//! - **Depends on.** The production client and the session fixture's real gRPC listener.
//! - **Must not know.** Server restore implementation or a second client recovery algorithm.

#[path = "../../../../../tests/fixtures/restore_reply_frames.rs"]
pub(super) mod frames;

use nervix_models::{ArchiveDigest, Restore, RestoreScope};

use super::*;
use crate::wire::{RestoreDisposition, RestoreFrame, RestoreMessage, RestoreReply, RestoreStart};

struct ReplacementSession {
    requests: nervix_primitives::sync::Arc<crate::exchange::ExchangeRequests>,
    server: ServerExchange,
}

#[derive(Debug)]
pub(super) struct ReceivedRestore {
    start: RestoreStart,
    archive: Vec<u8>,
    reply: oneshot::Sender<Vec<u8>>,
}

pub(super) struct ReceiveRestore {
    pub(super) restores: mpsc::Sender<ReceivedRestore>,
}

impl ClientStreamingService<VerifiedFrame<RestoreFrame>> for ReceiveRestore {
    type Response = Vec<u8>;
    type Future = BoxFuture<Response<Vec<u8>>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<RestoreFrame>>>) -> Self::Future {
        let restores = self.restores.clone();
        Box::pin(async move {
            let mut inbound = request.into_inner();
            let first = inbound
                .message()
                .await?
                .assured("the restore sends its start");
            let RestoreMessage::Start(start) =
                RestoreMessage::decode(&first).assured("the client's restore start decodes")
            else {
                panic!("the first restore frame is its start");
            };
            let mut archive = Vec::new();
            while let Some(frame) = inbound.message().await? {
                nervix_primitives::task::consume_budget().await;
                let RestoreMessage::Chunk(chunk) =
                    RestoreMessage::decode(&frame).assured("the client's restore chunk decodes")
                else {
                    panic!("only archive chunks follow the restore start");
                };
                archive.extend_from_slice(chunk.bytes());
            }
            let (reply, answer) = oneshot::channel();
            restores
                .send(ReceivedRestore {
                    start,
                    archive,
                    reply,
                })
                .await
                .assured("the owner test waits for its restore");
            let answer = answer.await.assured("the owner test answers the restore");
            Ok(Response::new(answer))
        })
    }
}

impl ReceivedRestore {
    fn complete(self, fault: frames::ReplyFault) {
        let ReplyBody::Command(outcome) = command_outcome(
            &self.start.execution_reference,
            completed(),
            "the restore completed",
        ) else {
            panic!("the fixture builds a command outcome");
        };
        let answer = fault.substitute(RestoreReply {
            request_id: Some(self.start.request_id),
            disposition: RestoreDisposition::Outcome(outcome),
        });
        self.reply
            .send(answer)
            .assured("the client waits for the restore reply");
    }
}

async fn assert_restore_reply_is_uncertain(fault: frames::ReplyFault, replace: bool) {
    let mut server = TestServer::start_recording_restores().await;
    let client = server.connect().await;
    let _original_server = server.next_exchange().await;
    let original = client.inner.exchange.lock().await.requests();
    let mut pending = original.register().assured("the original session is open");
    let files = tempfile::tempdir().assured("the test can create its archive directory");
    let path = files.path().join("restore.nvxb");
    let archive = vec![19_u8; 300_000];
    tokio::fs::write(&path, &archive)
        .await
        .assured("the fixture archive is writable");
    let restore = Restore {
        source: path.to_string_lossy().into_owned(),
        scope: RestoreScope::Domain {
            domain: domain("tenant"),
            target: None,
        },
        mode: nervix_models::RestoreMode::Apply,
        state: nervix_models::RestoreState::All,
        lifecycle: nervix_models::RestoreLifecycle::Stopped,
    };
    let reference = super::super::reference("restore-validation-own-reference");
    let restoring_client = client.clone();
    let restoring_reference = reference.clone();
    let restoring = nervix_primitives::task::spawn(async move {
        restoring_client
            .restore_with_reference(&restore, &restoring_reference, |_| {})
            .await
    });
    let received = within_deadline(server.restores.recv())
        .await
        .assured("the server received the complete restore");
    assert_eq!(received.archive, archive);
    assert_eq!(received.start.execution_reference, reference);
    assert_eq!(received.start.archive.total_bytes.get(), 300_000);
    assert_eq!(
        received.start.archive.digest,
        ArchiveDigest::from_bytes(*blake3::hash(&archive).as_bytes())
    );
    let replacement = if replace {
        let replacement = within_deadline(Exchange::open(
            client.current_channel().await,
            &client.inner.connector,
            client.inner.events.emitters.clone(),
        ))
        .await
        .assured("the replacement session opens");
        let requests = replacement.requests();
        client.install(replacement).await;
        let replacement_server = server.next_exchange().await;
        Some(ReplacementSession {
            requests,
            server: replacement_server,
        })
    } else {
        None
    };
    received.complete(fault);
    let result = within_deadline(restoring)
        .await
        .assured("the restore task completes");
    let report = result.expect_err("a reply that cannot answer this restore leaves it uncertain");
    assert!(
        matches!(report.current_context(), ClientError::UncertainCommand { reference: own } if *own == reference),
        "{report:#}"
    );
    match fault {
        frames::ReplyFault::Undecodable => {
            assert!(matches!(
                cause_beneath(&report),
                Some(ClientError::InvalidRestoreReply)
            ));
            assert!(report.contains::<crate::wire::WireDecodeError>());
        }
        frames::ReplyFault::AnotherRequest => assert!(matches!(
            cause_beneath(&report),
            Some(ClientError::UnexpectedReply {
                request: crate::RequestKind::Restore
            }),
        )),
    }
    assert!(!original.pending.lock().is_open());
    assert!(within_deadline(pending.receive()).await.is_none());
    let mut replacement_server = if let Some(replacement) = replacement {
        assert!(
            replacement.requests.pending.lock().is_open(),
            "a failure on the previous session preserves its replacement"
        );
        assert!(!replacement.server.frames.is_closed());
        replacement.server
    } else {
        assert!(client.inner.exchange.lock().await.reader.is_none());
        let replacement = within_deadline(Exchange::open(
            client.current_channel().await,
            &client.inner.connector,
            client.inner.events.emitters.clone(),
        ))
        .await
        .assured("the interrupted client's next exchange opens");
        let requests = replacement.requests();
        client.install(replacement).await;
        let replacement_server = server.next_exchange().await;
        assert!(requests.pending.lock().is_open());
        replacement_server
    };
    let listing_client = client.clone();
    let listing =
        nervix_primitives::task::spawn(async move { listing_client.list_domains().await });
    let request = replacement_server.next_request().await;
    assert!(matches!(request.request, ClientRequest::ListDomains));
    replacement_server
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
            .assured("the replacement's request finishes")
            .assured("the replacement serves its request after the invalid reply"),
        tenant_domains()
    );
}

#[nervix_primitives::test]
async fn an_undecodable_restore_reply_is_uncertain_and_ends_its_session() {
    assert_restore_reply_is_uncertain(frames::ReplyFault::Undecodable, false).await;
}

#[nervix_primitives::test]
async fn another_requests_restore_reply_is_uncertain_and_ends_its_session() {
    assert_restore_reply_is_uncertain(frames::ReplyFault::AnotherRequest, false).await;
}

#[nervix_primitives::test]
async fn an_undecodable_restore_reply_preserves_a_replacement_session() {
    assert_restore_reply_is_uncertain(frames::ReplyFault::Undecodable, true).await;
}

#[nervix_primitives::test]
async fn another_requests_restore_reply_preserves_a_replacement_session() {
    assert_restore_reply_is_uncertain(frames::ReplyFault::AnotherRequest, true).await;
}

#[nervix_primitives::test]
async fn local_restore_archive_failures_stay_definitive() {
    let mut server = TestServer::start_recording_restores().await;
    let client = server.connect().await;
    let _exchange = server.next_exchange().await;
    let files = tempfile::tempdir().assured("the test archive directory can be created");
    tokio::fs::write(files.path().join("empty.nvxb"), [])
        .await
        .assured("the empty archive is writable");
    let reference = super::super::reference("restore-local-failure");
    for file in ["missing.nvxb", "empty.nvxb"] {
        nervix_primitives::task::consume_budget().await;
        let restore = Restore {
            source: files.path().join(file).display().to_string(),
            scope: RestoreScope::Domain {
                domain: domain("tenant"),
                target: None,
            },
            mode: nervix_models::RestoreMode::Apply,
            state: nervix_models::RestoreState::All,
            lifecycle: nervix_models::RestoreLifecycle::Stopped,
        };
        let report = client
            .restore_with_reference(&restore, &reference, |_| {})
            .await
            .expect_err("a missing or empty local archive is refused before sending");
        assert!(
            matches!(
                report.current_context(),
                ClientError::ReadRestoreArchive { .. } | ClientError::EmptyRestoreArchive { .. }
            ),
            "{report:#}"
        );
        assert!(matches!(
            server.restores.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(
            client
                .inner
                .exchange
                .lock()
                .await
                .requests
                .pending
                .lock()
                .is_open()
        );
    }
}

#[nervix_primitives::test]
async fn known_restore_stream_refusals_stay_definitive() {
    let mut server = TestServer::start_recording_restores().await;
    let client = server.connect().await;
    let _exchange = server.next_exchange().await;
    let files = tempfile::tempdir().assured("the test archive directory can be created");
    let path = files.path().join("refused.nvxb");
    tokio::fs::write(&path, [7; 32])
        .await
        .assured("the fixture archive is writable");
    let restore = Restore {
        source: path.display().to_string(),
        scope: RestoreScope::Domain {
            domain: domain("tenant"),
            target: None,
        },
        mode: nervix_models::RestoreMode::Apply,
        state: nervix_models::RestoreState::All,
        lifecycle: nervix_models::RestoreLifecycle::Stopped,
    };
    let reference = super::super::reference("restore-known-refusal");
    for include_request in [true, false] {
        nervix_primitives::task::consume_budget().await;
        let restoring_client = client.clone();
        let restore = restore.clone();
        let reference = reference.clone();
        let restoring = nervix_primitives::task::spawn(async move {
            restoring_client
                .restore_with_reference(&restore, &reference, |_| {})
                .await
        });
        let received = within_deadline(server.restores.recv())
            .await
            .assured("the server receives the restore");
        let request_id = if include_request {
            Some(received.start.request_id)
        } else {
            None
        };
        let reply = RestoreReply {
            request_id,
            disposition: RestoreDisposition::UploadFailed {
                failure: crate::wire::RestoreUploadFailure::InvalidStream,
                message: "the scripted server refused the stream".to_string(),
            },
        }
        .encode(&limits())
        .assured("the current stream refusal encodes");
        received
            .reply
            .send(reply.into_bytes().to_vec())
            .assured("the client awaits its refusal");
        let outcome = within_deadline(restoring)
            .await
            .assured("the restore task ends")
            .assured("a known refusal has a definitive outcome");
        assert!(!outcome.succeeded());
        assert!(
            client
                .inner
                .exchange
                .lock()
                .await
                .requests
                .pending
                .lock()
                .is_open()
        );
    }
}
