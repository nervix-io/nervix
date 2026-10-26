//! How a failed client call reports its cause: each failure is the current context of a report,
//! and the status, I/O error, download failure or interruption behind it stays beneath that
//! context instead of repeating in its message or being dropped.

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{ClientRequest, ReplyBody, RequestRejected, RequestRejection};
use nervix_models::CommandExecutionReference;
use tonic::Status;

use super::{Loopback, cause_beneath, domain, test_client};
use crate::{ClientError, RequestKind, SubscriptionRequest};

#[test]
fn a_transport_failure_names_its_status_once_in_its_chain() {
    let failures = [
        ClientError::StartSession(Box::new(Status::unauthenticated("bad credentials"))),
        ClientError::Transport(Box::new(Status::unauthenticated("bad credentials"))),
        ClientError::UploadResource(Box::new(Status::unauthenticated("bad credentials"))),
        ClientError::Restore(Box::new(Status::unauthenticated("bad credentials"))),
    ];
    for failure in failures {
        let operation = failure.to_string();
        let rendered = format!("{:#}", Report::new(failure));
        assert!(
            rendered.starts_with(&format!("{operation}: ")),
            "the operation leads its chain: {rendered}"
        );
        assert_eq!(
            rendered.matches("bad credentials").count(),
            1,
            "the status appears once, beneath its operation: {rendered}"
        );
    }
}

#[nervix_primitives::test]
async fn a_command_whose_session_ends_unanswered_is_uncertain_above_the_interruption() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let execution = client.prepare_execution("SHOW CLUSTER STATUS;").await;
    let expected = execution.reference().clone();
    let command =
        nervix_primitives::task::spawn(async move { client.execute_prepared(&execution).await });

    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::Command(_)));
    loopback.pending.lock().close();

    let error = command
        .await
        .assured("the command task completes")
        .expect_err("a command whose session ended unanswered may have been admitted");
    let ClientError::UncertainCommand { reference } = error.current_context() else {
        panic!("the interruption leaves the command uncertain, not {error:?}");
    };
    assert_eq!(reference, &expected);
    assert!(
        matches!(
            cause_beneath(&error),
            Some(ClientError::RequestInterrupted {
                request: RequestKind::Command
            })
        ),
        "the interruption that left the outcome unknown stays beneath: {error:?}"
    );
}

#[nervix_primitives::test]
async fn a_failed_backup_download_keeps_the_download_failure_beneath() {
    let client = test_client("tenant");
    let directory = tempfile::tempdir().assured("a temporary directory can be created");
    let destination = directory.path().join("backup.nvxb");
    let reference = CommandExecutionReference::parse("download-reference")
        .assured("the reference is an accepted literal");
    let summary = nervix_models::BackupArchiveSummary {
        total_bytes: std::num::NonZeroU64::new(4096).assured("a non-zero size"),
        digest: nervix_models::ArchiveDigest::from_bytes([9; 32]),
        captured_at: nervix_models::Timestamp::from_unix_nanos(1),
        retained_until: nervix_models::Timestamp::from_unix_nanos(2),
        resources: nervix_models::BackupResources::Included,
        users: Some(1),
        domains: Vec::new(),
    };
    let error = client
        .download_backup(&reference, &summary, &destination)
        .await
        .expect_err("no server serves the download");
    let ClientError::BackupDownload { reference: failed } = error.current_context() else {
        panic!("the failed download is reported as such, not as {error:?}");
    };
    assert_eq!(failed, &reference);
    assert!(
        error.downcast_ref::<crate::BackupDownloadError>().is_some(),
        "the download's own failure stays beneath: {error:?}"
    );
    assert!(!destination.exists(), "a failed download writes no archive");
}

#[nervix_primitives::test]
async fn an_unreadable_restore_archive_keeps_its_io_error_beneath() {
    let client = test_client("tenant");
    let directory = tempfile::tempdir().assured("a temporary directory can be created");
    let missing = directory.path().join("absent.nvxb");
    let error = client
        .execute(format!(
            "RESTORE DOMAIN payments FROM '{}';",
            missing.display()
        ))
        .await
        .expect_err("an archive that does not exist cannot be restored");
    let ClientError::ReadRestoreArchive { path, kind } = error.current_context() else {
        panic!("the unreadable archive is reported as such, not as {error:?}");
    };
    assert_eq!(path, &missing);
    assert_eq!(*kind, std::io::ErrorKind::NotFound);
    let cause = error
        .downcast_ref::<std::io::Error>()
        .assured("the archive's read failure stays beneath its context");
    assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
}

#[nervix_primitives::test]
async fn a_rejected_subscription_is_reported_as_its_own_rejection() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let subscribing = nervix_primitives::task::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });

    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::Subscribe(_)));
    loopback
        .answer(
            request.request_id,
            ReplyBody::Rejected(RequestRejected {
                rejection: RequestRejection::ServerBusy,
                field: None,
                message: "the server is busy".to_string(),
            }),
        )
        .await;

    let error = subscribing
        .await
        .assured("the subscribe task completes")
        .expect_err("a rejected subscription fails");
    let ClientError::RequestRejected {
        request,
        rejection,
        message,
        ..
    } = error.current_context()
    else {
        panic!("the subscription's own rejection is the failure, not {error:?}");
    };
    assert_eq!(*request, RequestKind::Subscribe);
    assert_eq!(*rejection, RequestRejection::ServerBusy);
    assert_eq!(message, "the server is busy");
}
