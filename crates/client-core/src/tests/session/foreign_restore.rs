//! A current restore reply with correct transport correlation and a foreign execution reference.
//!
//! Layer: test harness.
//! - **Owns.** Archive-consuming reply fixtures and a controlled late restore reply.
//! - **Depends on.** The production client, typed restore frames and the gRPC test service.
//! - **Must not know.** Server restore planning or persistence.

use super::*;
use crate::wire::{
    RestoreDisposition, RestoreFrame, RestoreMessage, RestoreReply, RestoreReplyFrame,
};

pub(super) struct RestoreReplyGate {
    arrived: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

pub(super) struct RestoreWithForeignReference {
    pub(super) gate: Arc<Mutex<Option<RestoreReplyGate>>>,
}

impl StreamingService<VerifiedFrame<RestoreFrame>> for RestoreWithForeignReference {
    type Response = EncodedFrame<RestoreReplyFrame>;
    type ResponseStream = AnnouncedAnswers<Self::Response>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<RestoreFrame>>>) -> Self::Future {
        let gate = self.gate.clone();
        let completion = async move {
            let mut stream = request.into_inner();
            let first = stream
                .message()
                .await?
                .assured("the client sends the start");
            let RestoreMessage::Start(start) =
                RestoreMessage::decode(&first).assured("a valid start")
            else {
                panic!("a restore starts with metadata");
            };
            let mut received = 0;
            while let Some(frame) = stream.message().await? {
                nervix_primitives::task::consume_budget().await;
                let RestoreMessage::Chunk(chunk) =
                    RestoreMessage::decode(&frame).assured("a valid chunk")
                else {
                    panic!("chunks follow the start");
                };
                received += u64::try_from(chunk.bytes().len()).assured("a bounded chunk");
            }
            assert_eq!(received, start.archive.total_bytes.get());
            let gate = gate.lock().await.take();
            if let Some(gate) = gate {
                gate.arrived
                    .send(())
                    .assured("the test waits for the archive");
                gate.release.await.assured("the test releases the reply");
            }
            let foreign =
                CommandExecutionReference::parse("foreign-restore").assured("a valid reference");
            let ReplyBody::Command(outcome) =
                command_outcome(&foreign, completed(), "foreign restore success")
            else {
                panic!("the fixture makes a command outcome");
            };
            let reply = RestoreReply {
                request_id: Some(start.request_id),
                disposition: RestoreDisposition::Outcome(outcome),
            };
            Ok(reply.encode(&limits()).assured("the current reply encodes"))
        };
        let response = announced_answer(completion, limits());
        Box::pin(async move { Ok(response) })
    }
}

#[nervix_primitives::test]
async fn a_foreign_restore_reply_ends_its_session_and_reports_its_own_reference_uncertain() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let _exchange = server.next_exchange().await;
    let requests = client.inner.exchange.lock().await.requests();
    let (_directory, restore, reference) = restore_fixture().await;
    let error = within_deadline(client.restore_with_reference(&restore, &reference, |_| {}))
        .await
        .expect_err("another execution cannot answer this restore");
    assert!(
        matches!(error.current_context(), ClientError::UncertainCommand { reference: actual } if actual == &reference),
        "the restore may have been applied: {error:?}"
    );
    assert!(
        matches!(cause_beneath(&error), Some(ClientError::ExecutionReferenceMismatch { expected, received }) if expected == &reference && received.as_str() == "foreign-restore")
    );
    assert!(
        !requests.pending.lock().is_open(),
        "the untrusted session ends"
    );
    within_deadline(client.recover_session(
        crate::client::RecoveryMode::IfClosed,
        crate::client::RecoveryDeadline::RetryBudget,
    ))
    .await
    .assured("a stopped restore session can be replaced");
    let mut replacement = server.next_exchange().await;
    let listing = {
        let client = client.clone();
        nervix_primitives::task::spawn(async move { client.list_domains().await })
    };
    let request = replacement.next_request().await;
    replacement
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
            .assured("the call ends")
            .assured("the replacement answers"),
        tenant_domains()
    );
}

#[nervix_primitives::test]
async fn a_foreign_restore_reply_cannot_end_a_replacement_session() {
    let mut server = TestServer::start().await;
    let client = server.connect().await;
    let _original = server.next_exchange().await;
    let (arrived, received) = oneshot::channel();
    let (release, held) = oneshot::channel();
    *server.restore_reply_gate.lock().await = Some(RestoreReplyGate {
        arrived,
        release: held,
    });
    let (_directory, restore, reference) = restore_fixture().await;
    let restoring = {
        let client = client.clone();
        nervix_primitives::task::spawn(async move {
            client
                .restore_with_reference(&restore, &reference, |_| {})
                .await
        })
    };
    within_deadline(received)
        .await
        .assured("the stream sent its archive");
    within_deadline(client.recover_session(
        crate::client::RecoveryMode::Replace,
        crate::client::RecoveryDeadline::RetryBudget,
    ))
    .await
    .assured("an independent recovery replaces the session");
    let mut replacement = server.next_exchange().await;
    let requests = client.inner.exchange.lock().await.requests();
    release.send(()).assured("the stream awaits its reply");
    let error = within_deadline(restoring)
        .await
        .assured("the restore call ends")
        .expect_err("the foreign outcome cannot answer the restore");
    assert!(matches!(
        error.current_context(),
        ClientError::UncertainCommand { .. }
    ));
    assert!(
        requests.pending.lock().is_open(),
        "the replacement remains open"
    );
    let listing = {
        let client = client.clone();
        nervix_primitives::task::spawn(async move { client.list_domains().await })
    };
    let request = replacement.next_request().await;
    replacement
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
            .assured("the call ends")
            .assured("the replacement answers"),
        tenant_domains()
    );
}

async fn restore_fixture() -> (
    tempfile::TempDir,
    nervix_models::Restore,
    CommandExecutionReference,
) {
    let directory = tempfile::tempdir().assured("a temporary archive directory");
    let path = directory.path().join("restore.nvxb");
    tokio::fs::write(&path, b"a bounded archive fixture")
        .await
        .assured("the archive is written");
    let restore = nervix_models::Restore {
        scope: nervix_models::RestoreScope::Domain {
            domain: domain("tenant"),
            target: Some(domain("copy")),
        },
        source: path.to_string_lossy().into_owned(),
        mode: nervix_models::RestoreMode::Apply,
        state: nervix_models::RestoreState::All,
        lifecycle: nervix_models::RestoreLifecycle::Stopped,
    };
    let reference =
        CommandExecutionReference::parse("own-restore").assured("a valid own reference");
    (directory, restore, reference)
}
