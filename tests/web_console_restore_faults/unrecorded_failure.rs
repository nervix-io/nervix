//! A completed browser restore whose retry receives a valid, unrecorded request failure.
//!
//! Layer: test harness.
//! - **Owns.** The one-shot typed failure substitution and exact-reference recovery assertions.
//! - **Depends on.** Public wire codecs, the browser page and the raw public restore client.
//! - **Must not know.** The browser's admission history or server restore implementation.

use cucumber::{given, then};
use nervix_client_wire::{
    CommandOutcome, Diagnostic, OutcomeOrigin, RestoreMessage, RestoreStart, SourceSpan,
    websocket::ServerRestoreWebSocketCodec,
};

use super::*;
use crate::common::raw_session::{TestRestore, TestRestoreEnd, send_restore};

const LATER_FAILURE: &str = "the later restore request failed before admission";
const LATER_DIAGNOSTIC: &str = "the later request's structured diagnostic";

#[given(
    "the web console's next restore attempt loses its reply, after which a later attempt receives \
     an unrecorded request failure"
)]
async fn given_later_unrecorded_failure(world: &mut ScenarioWorld) {
    install_fault(world, "unrecorded").await;
}

fn lost_outcome(record: &RestoreFaultRecord) -> RestoreReply {
    let lost = record
        .lost_reply
        .as_ref()
        .assured("the wait observed the lost reply");
    let codec = ClientRestoreWebSocketCodec::new(SessionLimits::DEFAULT);
    let frame = codec
        .decode(WebSocketData::Binary(lost.clone().into()))
        .assured("the fixture retained the real node's restore frame");
    RestoreReply::decode(&frame).assured("the real restore reply decodes")
}

fn completed_outcome(reply: &RestoreReply) -> &CommandOutcome {
    let RestoreDisposition::Outcome(outcome) = &reply.disposition else {
        panic!("the lost reply must describe an actual restore: {reply:?}");
    };
    assert!(
        matches!(outcome.disposition, CommandDisposition::Completed { .. }),
        "{outcome:?}"
    );
    assert!(
        outcome.restore.is_some(),
        "the actual restore has its step report"
    );
    outcome
}

fn captured_start(record: &RestoreFaultRecord) -> RestoreStart {
    let bytes = record
        .start
        .as_ref()
        .assured("the original start was retained");
    let codec = ServerRestoreWebSocketCodec::new(SessionLimits::DEFAULT);
    let frame = codec
        .decode(WebSocketData::Binary(bytes.clone().into()))
        .assured("the original start is a current restore frame");
    let RestoreMessage::Start(start) =
        RestoreMessage::decode(&frame).assured("the browser sent a valid restore start")
    else {
        panic!("the browser began with its restore start");
    };
    start
}

#[then(
    expr = "the web console's completed restore reply is lost and its unrecorded failure is \
            substituted, with execution reference saved as placeholder {string}"
)]
async fn then_later_failure_is_substituted(world: &mut ScenarioWorld, placeholder: String) {
    let page = browser_page(world);
    let record = nervix_primitives::time::timeout(LOST_REPLY_TIMEOUT, async {
        let mut inspected_lost_replies = 0;
        loop {
            nervix_primitives::task::consume_budget().await;
            let record = read_record(page).await;
            assert!(record.failure.is_none(), "{record:?}");
            if record.lost_reply.is_some()
                && record.start.is_some()
                && record.lost_replies > inspected_lost_replies
            {
                assert_eq!(record.lost_replies, inspected_lost_replies + 1);
                inspected_lost_replies = record.lost_replies;
                let reply = lost_outcome(&record);
                let start = captured_start(&record);
                assert_eq!(reply.request_id, start.request_id);
                if let RestoreDisposition::Outcome(outcome) = &reply.disposition {
                    assert_eq!(outcome.execution_reference, start.execution_reference);
                    if matches!(outcome.disposition, CommandDisposition::OutcomeUnknown(_)) {
                        page.evaluate::<_, ()>(
                            "(count) => window.__nervixRestoreFault.loseNextReply(count)",
                            Some(&inspected_lost_replies),
                        )
                        .await
                        .assured("the bounded peer advances past an uncertain lost reply");
                        continue;
                    }
                }
                completed_outcome(&reply);
                return record;
            }
            nervix_primitives::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured(
        "the restore completed and the fixture retained its original start and completed reply",
    );
    let reply = lost_outcome(&record);
    let outcome = completed_outcome(&reply);
    let reference = outcome.execution_reference.clone();
    let mut later = outcome.clone();
    later.origin = OutcomeOrigin::Executed;
    later.disposition = CommandDisposition::Failed;
    later.message = LATER_FAILURE.to_string();
    later.diagnostics = vec![Diagnostic {
        message: LATER_DIAGNOSTIC.to_string(),
        span: Some(
            SourceSpan::new(0, 7).assured("RESTORE occupies the statement's first seven bytes"),
        ),
    }];
    later.restore = None;
    let replacement = RestoreReply {
        request_id: reply.request_id,
        disposition: RestoreDisposition::Outcome(Box::new(later)),
    }
    .encode(&SessionLimits::DEFAULT)
    .assured("the typed failure fits the normal frame limit");
    let codec = ServerRestoreWebSocketCodec::new(SessionLimits::DEFAULT);
    let bytes = Vec::from(codec.encode(replacement));
    page.evaluate::<_, ()>(
        "(bytes) => window.__nervixRestoreFault.refuseNext(bytes)",
        Some(&bytes),
    )
    .await
    .assured("the bounded peer substitutes the later failure once");
    nervix_primitives::time::timeout(LOST_REPLY_TIMEOUT, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let record = read_record(page).await;
            assert!(record.failure.is_none(), "{record:?}");
            if record.substituted_failures == 1 {
                assert_eq!(
                    record.restore_connections,
                    record.lost_replies + 1,
                    "{record:?}"
                );
                assert_eq!(record.failed_reads, 0, "the archive remains readable");
                return;
            }
            nervix_primitives::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("the console retried and received the unrecorded failure");
    world
        .placeholders
        .insert(placeholder, reference.to_string());
}

#[then(expr = "repeating the console restore from archive {string} recovers its recorded outcome")]
async fn then_original_outcome_is_recovered(world: &mut ScenarioWorld, file: String) {
    let record = read_record(browser_page(world)).await;
    assert_eq!(record.substituted_failures, 1);
    let reply = lost_outcome(&record);
    let original = completed_outcome(&reply);
    let start = captured_start(&record);
    assert_eq!(start.execution_reference, original.execution_reference);
    let path = world
        .backup_directory
        .as_ref()
        .assured("the scenario wrote its backup archive")
        .path()
        .join(crate::expand_placeholders(world, &file));
    let archive = tokio::fs::read(path)
        .await
        .assured("the archive still exists");
    assert_eq!(
        start.archive.total_bytes.get(),
        u64::try_from(archive.len()).assured("a small fixture fits u64")
    );
    // BLAKE3 checks only content equality between this test's controlled file and captured start.
    // Neither input is adversarial, and the check makes no authenticity or authorization decision.
    assert_eq!(
        start.archive.digest.as_bytes(),
        blake3::hash(&archive).as_bytes()
    );
    let leader = crate::current_leader_node(world).await;
    let uri = world
        .cluster()
        .grpc_uri(&leader)
        .assured("the leader has its public endpoint");
    let recovered = send_restore(
        &uri,
        TestRestore {
            reference: &start.execution_reference,
            statement: &start.statement,
            archive: &archive,
            declared_digest: None,
            abandon_after_chunks: None,
        },
    )
    .await
    .assured("the public restore call recovers the original execution");
    let TestRestoreEnd::Replied(RestoreReply {
        disposition: RestoreDisposition::Outcome(recovered),
        ..
    }) = recovered
    else {
        panic!("the original restore has a recorded outcome: {recovered:?}");
    };
    let mut expected = original.clone();
    expected.origin = OutcomeOrigin::Recovered;
    assert_eq!(
        *recovered, expected,
        "recovery returns every original outcome field and step"
    );
}
