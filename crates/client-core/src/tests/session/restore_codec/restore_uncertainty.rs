//! Restore attempt history exercised through the production client and a real gRPC peer.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Later local failures and refusals after an earlier restore outcome became unknown.
//! - **Depends on.** The scripted restore peer and the current public client and wire contracts.
//! - **Must not know.** Server restore admission, execution, or persistence.

use nervix_client_wire::{
    CommandDisposition, LeaderRedirect, NodeEndpoints, OutcomeOrigin, UnknownOutcomeCause,
};
use url::Url;

use super::*;

#[derive(Debug, Clone, Copy)]
enum PriorAttempt {
    TransportFailure,
    OutcomeUnknown,
    Redirect,
    AwaitElection,
}

impl PriorAttempt {
    fn answer(self, start: &RestoreStart, server: &TestServer) -> ScriptedReply {
        match self {
            Self::TransportFailure => {
                ScriptedReply::Refusal(Status::unavailable("the first answer was lost"))
            }
            Self::OutcomeUnknown => outcome_reply(
                start,
                CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::StillApplying),
            ),
            Self::Redirect => outcome_reply(
                start,
                CommandDisposition::NotLeader(LeaderRedirect {
                    leader: Some(NodeEndpoints {
                        node: super::super::node("restore-leader"),
                        grpc_uri: Some(
                            Url::parse(&format!("http://{}", server.address))
                                .assured("the scripted leader is a loopback HTTP origin"),
                        ),
                        web_console_uri: None,
                    }),
                }),
            ),
            Self::AwaitElection => outcome_reply(
                start,
                CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum LaterFailure {
    MissingArchive,
    UnreadableArchive,
    StreamRefusal,
    RequestRefusal,
    ReferenceExpired,
}

impl LaterFailure {
    async fn prepare(self, restore: &Restore) {
        match self {
            Self::MissingArchive | Self::UnreadableArchive => {
                tokio::fs::remove_file(&restore.source)
                    .await
                    .assured("the first attempt already read the fixture archive");
                if let Self::UnreadableArchive = self {
                    tokio::fs::create_dir(&restore.source)
                        .await
                        .assured("a directory replaces the file before the next read");
                }
            }
            Self::StreamRefusal | Self::RequestRefusal | Self::ReferenceExpired => {}
        }
    }

    fn answer(self, start: &RestoreStart) -> Option<ScriptedReply> {
        match self {
            Self::MissingArchive | Self::UnreadableArchive => None,
            Self::StreamRefusal => {
                let reply = RestoreReply {
                    request_id: Some(start.request_id),
                    disposition: RestoreDisposition::UploadFailed {
                        failure: RestoreUploadFailure::StagingFailed,
                        message: "the later stream could not be staged".to_string(),
                    },
                }
                .encode(&limits())
                .assured("the bounded current refusal fits one reply");
                Some(ScriptedReply::Message(reply.into_bytes()))
            }
            Self::ReferenceExpired => Some(outcome_reply(
                start,
                CommandDisposition::ExecutionReferenceExpired,
            )),
            Self::RequestRefusal => Some(outcome_reply(start, CommandDisposition::Failed)),
        }
    }
}

fn outcome_reply(start: &RestoreStart, disposition: CommandDisposition) -> ScriptedReply {
    let origin = match disposition {
        CommandDisposition::Completed { .. } | CommandDisposition::OutcomeUnknown(_) => {
            OutcomeOrigin::Recovered
        }
        _ => OutcomeOrigin::Executed,
    };
    outcome_reply_with_origin(start, disposition, origin)
}

fn outcome_reply_with_origin(
    start: &RestoreStart,
    disposition: CommandDisposition,
    origin: OutcomeOrigin,
) -> ScriptedReply {
    let ReplyBody::Command(mut outcome) = super::super::command_outcome(
        &start.execution_reference,
        disposition,
        "the scripted restore answer",
    ) else {
        panic!("the command fixture builds a command outcome");
    };
    outcome.origin = origin;
    let reply = RestoreReply {
        request_id: Some(start.request_id),
        disposition: RestoreDisposition::Outcome(outcome),
    }
    .encode(&limits())
    .assured("the bounded current command outcome fits one restore reply");
    ScriptedReply::Message(reply.into_bytes())
}

async fn assert_later_failure_stays_uncertain(first: PriorAttempt, later: LaterFailure) {
    let (mut server, mut restores) = TestServer::with_restore_codec().await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let _original = server.next_exchange().await;
    let (_directory, restore, _) = restore_request().await;
    let archive = vec![0x7a; 800_017];
    tokio::fs::write(&restore.source, &archive)
        .await
        .assured("the test writes an archive spanning several production chunks");
    let reference = CommandExecutionReference::parse("restore-attempt-history")
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
        .assured("the first complete restore reaches the real peer");
    assert_eq!(received.archive, archive);
    assert_eq!(received.start.execution_reference, reference);
    assert_eq!(received.start.statement, restore.to_canonical_plexus());
    assert_eq!(
        received.start.archive.digest.as_bytes(),
        blake3::hash(&archive).as_bytes()
    );
    later.prepare(&restore).await;
    let answer = first.answer(&received.start, &server);
    received
        .answer
        .send(answer)
        .assured("the first attempt still awaits its scripted answer");
    let _replacement = match first {
        PriorAttempt::TransportFailure | PriorAttempt::Redirect => {
            Some(server.next_exchange().await)
        }
        PriorAttempt::OutcomeUnknown | PriorAttempt::AwaitElection => None,
    };
    if let Some(answer) = later.answer(&received.start) {
        let retried = within_deadline(restores.recv())
            .await
            .assured("the second complete restore reaches the real peer");
        assert_eq!(retried.start, received.start);
        assert_eq!(retried.archive, archive);
        retried
            .answer
            .send(answer)
            .assured("the later attempt awaits its scripted answer");
    }
    let result = within_deadline(call)
        .await
        .assured("the restore task completes");
    let report = result
        .err()
        .assured("a later refusal cannot settle an earlier uncertain restore");
    assert!(
        matches!(report.current_context(), ClientError::UncertainCommand { reference: actual }
            if actual == &reference),
        "{first:?}, {later:?}: {report:#}"
    );
    if let LaterFailure::MissingArchive | LaterFailure::UnreadableArchive = later {
        assert!(
            report.downcast_ref::<std::io::Error>().is_some(),
            "the original local read error remains typed beneath uncertainty: {report:#}"
        );
        assert!(report.frames().any(|frame| matches!(
            frame.downcast_ref::<ClientError>(),
            Some(ClientError::ReadRestoreArchive { .. })
        )));
    }
    match later {
        LaterFailure::StreamRefusal => assert!(report.frames().any(|frame| matches!(
            frame.downcast_ref::<ClientError>(),
            Some(ClientError::RestoreStreamRefused {
                failure: RestoreUploadFailure::StagingFailed,
                ..
            })
        ))),
        LaterFailure::RequestRefusal => assert!(report.frames().any(|frame| matches!(
            frame.downcast_ref::<ClientError>(),
            Some(ClientError::LastUnresolvedAnswer {
                disposition: CommandDisposition::Failed,
                ..
            })
        ))),
        LaterFailure::ReferenceExpired => assert!(report.frames().any(|frame| matches!(
            frame.downcast_ref::<ClientError>(),
            Some(ClientError::LastUnresolvedAnswer {
                disposition: CommandDisposition::ExecutionReferenceExpired,
                ..
            })
        ))),
        LaterFailure::MissingArchive | LaterFailure::UnreadableArchive => {}
    }
}

macro_rules! uncertain_restore_cases {
    ($($name:ident: $first:ident, $later:ident;)*) => {$ (
        #[nervix_primitives::test]
        async fn $name() {
            assert_later_failure_stays_uncertain(
                PriorAttempt::$first,
                LaterFailure::$later,
            ).await;
        }
    )*};
}

uncertain_restore_cases! {
    a_missing_archive_after_transport_loss_keeps_the_restore_uncertain: TransportFailure, MissingArchive;
    an_unreadable_archive_after_transport_loss_keeps_the_restore_uncertain: TransportFailure, UnreadableArchive;
    a_stream_refusal_after_transport_loss_keeps_the_restore_uncertain: TransportFailure, StreamRefusal;
    a_request_refusal_after_transport_loss_keeps_the_restore_uncertain: TransportFailure, RequestRefusal;
    an_expired_reference_after_transport_loss_keeps_the_restore_uncertain: TransportFailure, ReferenceExpired;
    a_missing_archive_after_outcome_unknown_keeps_the_restore_uncertain: OutcomeUnknown, MissingArchive;
    an_unreadable_archive_after_outcome_unknown_keeps_the_restore_uncertain: OutcomeUnknown, UnreadableArchive;
    a_stream_refusal_after_outcome_unknown_keeps_the_restore_uncertain: OutcomeUnknown, StreamRefusal;
    a_request_refusal_after_outcome_unknown_keeps_the_restore_uncertain: OutcomeUnknown, RequestRefusal;
    an_expired_reference_after_outcome_unknown_keeps_the_restore_uncertain: OutcomeUnknown, ReferenceExpired;
    a_missing_archive_after_a_whole_redirect_keeps_the_restore_uncertain: Redirect, MissingArchive;
    a_missing_archive_after_a_whole_election_wait_keeps_the_restore_uncertain: AwaitElection, MissingArchive;
}

#[nervix_primitives::test]
async fn a_later_completed_restore_settles_earlier_uncertainty() {
    assert_recovered_restore_settles_uncertainty(super::super::completed()).await;
}

#[nervix_primitives::test]
async fn a_later_recorded_failure_settles_earlier_uncertainty() {
    assert_recovered_restore_settles_uncertainty(CommandDisposition::Failed).await;
}

async fn assert_recovered_restore_settles_uncertainty(disposition: CommandDisposition) {
    let (mut server, mut restores) = TestServer::with_restore_codec().await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let _original = server.next_exchange().await;
    let (_directory, restore, archive) = restore_request().await;
    let reference = CommandExecutionReference::parse("restore-attempt-recovered")
        .assured("the test execution reference is valid");
    let caller = client.clone();
    let sent_reference = reference.clone();
    let call = task::spawn(async move {
        caller
            .restore_with_reference(&restore, &sent_reference, |_| {})
            .await
    });
    let first = within_deadline(restores.recv())
        .await
        .assured("the first complete restore arrives");
    first
        .answer
        .send(PriorAttempt::OutcomeUnknown.answer(&first.start, &server))
        .assured("the first attempt waits for its answer");
    let second = within_deadline(restores.recv())
        .await
        .assured("the second complete restore arrives");
    assert_eq!(second.start, first.start);
    assert_eq!(second.archive, archive);
    second
        .answer
        .send(outcome_reply_with_origin(
            &second.start,
            disposition.clone(),
            OutcomeOrigin::Recovered,
        ))
        .assured("the second attempt waits for its answer");
    let outcome = within_deadline(call)
        .await
        .assured("the restore task completes")
        .assured("the matching recorded outcome settles the restore");
    assert_eq!(outcome.disposition, disposition);
    assert_eq!(outcome.execution_reference.as_ref(), Some(&reference));
    assert_eq!(outcome.origin, Some(OutcomeOrigin::Recovered));
}

#[nervix_primitives::test]
async fn a_pre_admission_expired_reference_stays_definitive() {
    let (mut server, mut restores) = TestServer::with_restore_codec().await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let _original = server.next_exchange().await;
    let (_directory, restore, _) = restore_request().await;
    let reference = CommandExecutionReference::parse("restore-attempt-expired")
        .assured("the test execution reference is valid");
    let caller = client.clone();
    let call = task::spawn(async move {
        caller
            .restore_with_reference(&restore, &reference, |_| {})
            .await
    });
    let received = within_deadline(restores.recv())
        .await
        .assured("the single restore attempt arrives");
    received
        .answer
        .send(outcome_reply(
            &received.start,
            CommandDisposition::ExecutionReferenceExpired,
        ))
        .assured("the restore attempt waits for its answer");
    let outcome = within_deadline(call)
        .await
        .assured("the restore task completes")
        .assured("the first reply establishes that the reference is expired");
    assert_eq!(
        outcome.disposition,
        CommandDisposition::ExecutionReferenceExpired
    );
}

async fn assert_prefix_then_missing_archive(first: PriorAttempt) {
    use nervix_primitives::sync::atomic::{AtomicU64, Ordering};

    let (mut server, mut restores) = TestServer::with_restore_reading(ArchiveRead::StartOnly).await;
    let client = server.connect_with(ConnectOptions::default()).await;
    let _original = server.next_exchange().await;
    let (_directory, restore, _) = restore_request().await;
    // The peer holds the body after its start, so transport flow control and the existing
    // eight-frame queue cannot take this whole bounded archive before the peer answers.
    let archive = vec![0x61; 32 * 1024 * 1024];
    tokio::fs::write(&restore.source, &archive)
        .await
        .assured("the fixture writes its bounded multi-window archive");
    let reference = CommandExecutionReference::parse("restore-prefix-history")
        .assured("the test execution reference is valid");
    let supplied = Arc::new(AtomicU64::new(0));
    let observed = supplied.clone();
    let caller = client.clone();
    let sent_restore = restore.clone();
    let sent_reference = reference.clone();
    let call = task::spawn(async move {
        caller
            .restore_with_reference(&sent_restore, &sent_reference, move |bytes| {
                // Only this bounded archive is read; the next open fails, so this total is
                // at most 32 MiB and cannot overflow u64.
                observed.fetch_add(bytes, Ordering::Relaxed);
            })
            .await
    });
    let received = within_deadline(restores.recv())
        .await
        .assured("the peer received the restore's start");
    assert!(received.archive.is_empty());
    tokio::fs::remove_file(&restore.source)
        .await
        .assured("the archive disappears before the next attempt opens it");
    received
        .answer
        .send(first.answer(&received.start, &server))
        .assured("the peer holds the incomplete upload until this answer");
    let _replacement = match first {
        PriorAttempt::TransportFailure | PriorAttempt::Redirect => {
            Some(server.next_exchange().await)
        }
        PriorAttempt::OutcomeUnknown | PriorAttempt::AwaitElection => None,
    };
    let report = within_deadline(call)
        .await
        .assured("the restore task completes")
        .err()
        .assured("the next attempt cannot open its archive");
    assert!(
        supplied.load(Ordering::Relaxed)
            < u64::try_from(archive.len()).assured("the bounded archive length fits u64"),
        "this control must not have handed over the whole archive"
    );
    match first {
        PriorAttempt::TransportFailure | PriorAttempt::Redirect | PriorAttempt::AwaitElection => {
            assert!(
                matches!(
                    report.current_context(),
                    ClientError::ReadRestoreArchive { .. }
                ),
                "an incomplete upload cannot hide admission: {report:#}"
            )
        }
        PriorAttempt::OutcomeUnknown => assert!(
            matches!(report.current_context(), ClientError::UncertainCommand { reference: actual }
                if actual == &reference),
            "the server knows an earlier restore is applying even before this upload: {report:#}"
        ),
    }
}

#[nervix_primitives::test]
async fn an_incomplete_upload_then_a_local_read_failure_stays_definitive() {
    assert_prefix_then_missing_archive(PriorAttempt::TransportFailure).await;
}

#[nervix_primitives::test]
async fn an_unknown_outcome_before_a_complete_upload_keeps_the_restore_uncertain() {
    assert_prefix_then_missing_archive(PriorAttempt::OutcomeUnknown).await;
}

#[nervix_primitives::test]
async fn an_election_wait_before_a_complete_upload_then_a_read_failure_stays_definitive() {
    assert_prefix_then_missing_archive(PriorAttempt::AwaitElection).await;
}
