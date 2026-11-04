//! A caller's cancellation of a prepared command: the `CancelRequest` the client sends for the
//! command it has in flight, the terminal reply that decides the call, and the waits a cancelled
//! call stops without sending anything more. Also the identities an earlier run reported, under
//! which a new handle repeats a command or an upload, and the upload a transaction refuses.

use meticulous::ResultExt as _;
use nervix_client_wire::{
    CancelOutcome, CancelRequest, CancelState, CancellationStage, ClientRequest,
    CommandDisposition, ReplyBody, RequestCancelled, UnknownOutcomeCause,
};
use nervix_models::ResourceUploadIdentity;
use nervix_primitives::sync::CancellationToken;

use super::{
    Loopback, cause_beneath, command_reply, completed, domain, open_transaction, reference,
};
use crate::{ClientError, client::LOCAL_STATEMENT_IN_TRANSACTION};

/// The reference `command_reply` answers under.
const COMMAND_REFERENCE: &str = "command-1";

/// Starts a prepared `CREATE RESOURCE` under `COMMAND_REFERENCE` that `cancellation` cancels, and
/// returns the command request the client sent for it.
async fn start_cancellable_command(
    loopback: &mut Loopback,
    cancellation: &CancellationToken,
) -> (
    nervix_primitives::task::JoinHandle<error_stack::Result<crate::CommandOutcome, ClientError>>,
    nervix_client_wire::ClientMessage,
) {
    let client = loopback.client.clone();
    let execution = client
        .prepare_execution("CREATE RESOURCE models;")
        .await
        .with_reference(reference(COMMAND_REFERENCE));
    let cancellation = cancellation.clone();
    let waiting = nervix_primitives::task::spawn(async move {
        client
            .execute_prepared_with_cancellation(&execution, &cancellation)
            .await
    });
    let command = loopback.next_request().await;
    assert!(
        matches!(command.request, ClientRequest::Command(_)),
        "the statement is sent as a command"
    );
    (waiting, command)
}

/// Cancels `cancellation`, checks the `CancelRequest` the client sends for `command`, and answers
/// it the way the server answers the cancellation of a request in flight.
async fn cancel_in_flight(
    loopback: &mut Loopback,
    cancellation: &CancellationToken,
    command: &nervix_client_wire::ClientMessage,
) {
    cancellation.cancel();
    let cancel = loopback.next_request().await;
    let ClientRequest::Cancel(CancelRequest { target }) = cancel.request else {
        panic!("a command cancelled while it waits is followed by its cancellation");
    };
    assert_eq!(
        target, command.request_id,
        "the cancellation names the command"
    );
    assert_ne!(
        cancel.request_id, command.request_id,
        "a cancellation takes an identity of its own"
    );
    loopback
        .answer(
            cancel.request_id,
            ReplyBody::Cancel(CancelOutcome {
                target,
                state: CancelState::Requested,
            }),
        )
        .await;
}

#[nervix_primitives::test]
async fn a_command_cancelled_before_admission_fails_as_cancelled_with_no_effect() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let cancellation = CancellationToken::new();
    let (waiting, command) = start_cancellable_command(&mut loopback, &cancellation).await;
    cancel_in_flight(&mut loopback, &cancellation, &command).await;
    loopback
        .answer(
            command.request_id,
            ReplyBody::Cancelled(RequestCancelled {
                stage: CancellationStage::BeforeAdmission,
            }),
        )
        .await;

    let error = waiting
        .await
        .assured("the command task runs to its end")
        .expect_err("a command the server cancelled before admission fails");
    assert!(
        matches!(
            error.current_context(),
            ClientError::RequestCancelled {
                stage: CancellationStage::BeforeAdmission,
                ..
            }
        ),
        "{error:?}"
    );
}

#[nervix_primitives::test]
async fn a_command_cancelled_after_admission_is_uncertain_under_its_reference() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let cancellation = CancellationToken::new();
    let (waiting, command) = start_cancellable_command(&mut loopback, &cancellation).await;
    cancel_in_flight(&mut loopback, &cancellation, &command).await;
    loopback
        .answer(
            command.request_id,
            ReplyBody::Cancelled(RequestCancelled {
                stage: CancellationStage::AfterAdmission,
            }),
        )
        .await;

    let error = waiting
        .await
        .assured("the command task runs to its end")
        .expect_err("a command that keeps running after its cancellation is uncertain");
    let ClientError::UncertainCommand {
        reference: uncertain,
    } = error.current_context()
    else {
        panic!("an admitted command's cancellation leaves it uncertain: {error:?}");
    };
    assert_eq!(uncertain, &reference(COMMAND_REFERENCE));
    assert!(
        matches!(
            cause_beneath(&error),
            Some(ClientError::RequestCancelled {
                stage: CancellationStage::AfterAdmission,
                ..
            })
        ),
        "the server's cancellation stays beneath the uncertainty: {error:?}"
    );
}

#[nervix_primitives::test]
async fn a_command_that_finished_before_its_cancellation_returns_its_outcome() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let cancellation = CancellationToken::new();
    let (waiting, command) = start_cancellable_command(&mut loopback, &cancellation).await;
    cancel_in_flight(&mut loopback, &cancellation, &command).await;
    loopback
        .answer(
            command.request_id,
            command_reply(completed(), "created resource 'models'"),
        )
        .await;

    let outcome = waiting
        .await
        .assured("the command task runs to its end")
        .assured("a command that finished first returns its outcome");
    assert!(outcome.succeeded());
    assert_eq!(outcome.message, "created resource 'models'");
}

#[nervix_primitives::test]
async fn a_call_cancelled_between_attempts_sends_nothing_more() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let cancellation = CancellationToken::new();
    let (waiting, command) = start_cancellable_command(&mut loopback, &cancellation).await;
    // The answer reaches the client before the cancellation does: no task runs between them, and
    // the client takes a reply that arrived over the cancellation of its wait.
    loopback
        .answer(
            command.request_id,
            command_reply(
                CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::StillApplying),
                "the command is still applying",
            ),
        )
        .await;
    cancellation.cancel();

    let error = waiting
        .await
        .assured("the command task runs to its end")
        .expect_err("a call cancelled while it waits to retry stops");
    let ClientError::UncertainCommand {
        reference: uncertain,
    } = error.current_context()
    else {
        panic!("a command that may have been admitted stays uncertain: {error:?}");
    };
    assert_eq!(uncertain, &reference(COMMAND_REFERENCE));
    assert!(
        matches!(cause_beneath(&error), Some(ClientError::CallCancelled)),
        "{error:?}"
    );
    assert!(
        loopback.requests.try_recv().is_err(),
        "a cancelled call sends no further attempt and no cancellation"
    );
}

#[nervix_primitives::test]
async fn a_cancellation_the_ended_session_cannot_send_leaves_the_command_uncertain() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let cancellation = CancellationToken::new();
    let (waiting, _command) = start_cancellable_command(&mut loopback, &cancellation).await;
    // The session's transport ends after the command went out, so its cancellation cannot be sent
    // and nothing answers the command.
    loopback.requests.close();
    cancellation.cancel();

    let error = waiting
        .await
        .assured("the command task runs to its end")
        .expect_err("a command whose session ended while it waited has no known outcome");
    let ClientError::UncertainCommand { reference: uncertain } = error.current_context() else {
        panic!("a command sent before its session ended may have been admitted: {error:?}");
    };
    assert_eq!(uncertain, &reference(COMMAND_REFERENCE));
}

#[nervix_primitives::test]
async fn a_handle_sends_the_identities_an_earlier_run_reported() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let identity = ResourceUploadIdentity::parse("upload-1").assured("the test identity is valid");
    let execution = loopback
        .client
        .prepare_execution("CREATE RESOURCE models;")
        .await
        .with_reference(reference(COMMAND_REFERENCE))
        .with_upload_identity(identity.clone());
    assert_eq!(execution.reference(), &reference(COMMAND_REFERENCE));
    assert_eq!(execution.upload_identity(), &identity);
    assert_eq!(execution.domain(), Some(&domain("tenant")));

    let client = loopback.client.clone();
    let waiting =
        nervix_primitives::task::spawn(async move { client.execute_prepared(&execution).await });
    let command = loopback.next_request().await;
    let ClientRequest::Command(sent) = &command.request else {
        panic!("the statement is sent as a command");
    };
    assert_eq!(sent.execution_reference, reference(COMMAND_REFERENCE));
    loopback
        .answer(
            command.request_id,
            command_reply(completed(), "created resource 'models'"),
        )
        .await;
    let outcome = waiting
        .await
        .assured("the command task runs to its end")
        .assured("the repeated command returns the outcome recorded under its reference");
    assert!(outcome.succeeded());
}

#[nervix_primitives::test]
async fn an_upload_is_refused_while_the_session_holds_a_transaction() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback
        .client
        .adopt_transaction_status(open_transaction("tx-1", 1))
        .await;
    let identity = ResourceUploadIdentity::parse("upload-1").assured("the test identity is valid");
    let outcome = loopback
        .client
        .upload_resource_from_directory_with_identity(
            "models",
            "no-directory-is-read",
            domain("tenant"),
            identity,
            |_| {},
        )
        .await
        .assured("a refused upload is a failed outcome");
    assert!(!outcome.succeeded());
    assert_eq!(outcome.message, LOCAL_STATEMENT_IN_TRANSACTION);
    assert!(
        loopback.requests.try_recv().is_err(),
        "a refused upload sends nothing on the session"
    );
}
