//! The reports a server sends while it still serves a request: the request asks for them, every
//! report starts its wait and its call's budget again, and a request the server stops reporting
//! gives up one whole wait after its last report.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{ClientMessage, ClientRequest, ProgressInterval};
use nervix_primitives::{task::JoinHandle, time::Instant};

use super::{Loopback, command_reply, completed, reference};
use crate::{ClientError, CommandOutcome};

/// The request and retry timeouts of the options the loopback's client uses.
const WAIT: Duration = Duration::from_secs(120);

/// How many reports the server sends before the tests stop sending them or answer: together they
/// span more than one whole wait, so the request outlives both of its timeouts only through them.
const REPORTS: u32 = 6;

/// Starts a prepared `CREATE RESOURCE` under `command-1`, the reference `command_reply` answers,
/// and returns the command request the client sent for it.
async fn start_command(
    loopback: &mut Loopback,
) -> (
    JoinHandle<error_stack::Result<CommandOutcome, ClientError>>,
    ClientMessage,
) {
    let client = loopback.client.clone();
    let execution = client
        .prepare_execution("CREATE RESOURCE models;")
        .await
        .with_reference(reference("command-1"));
    let waiting =
        nervix_primitives::task::spawn(async move { client.execute_prepared(&execution).await });
    let command = loopback.next_request().await;
    assert!(
        matches!(command.request, ClientRequest::Command(_)),
        "the statement is sent as a command"
    );
    (waiting, command)
}

/// Sends `REPORTS` reports for `command`, one interval apart, as a server serving it does.
async fn report_while_serving(loopback: &Loopback, command: &ClientMessage) {
    let interval = command
        .progress
        .assured("every request the client waits on asks for progress reports")
        .get();
    for _ in 0..REPORTS {
        nervix_primitives::time::sleep(interval).await;
        loopback.pending.lock().report_progress(command.request_id);
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_command_the_server_keeps_reporting_outlives_its_request_and_call_timeouts() {
    let mut loopback = Loopback::new(None);
    let started = Instant::now();
    let (waiting, command) = start_command(&mut loopback).await;
    assert_eq!(
        command.progress,
        Some(ProgressInterval::within(WAIT)),
        "a command asks for four reports within the shorter of its request and call timeouts"
    );

    report_while_serving(&loopback, &command).await;
    assert!(
        started.elapsed() > WAIT,
        "the reports span more than the request and call timeouts"
    );
    loopback
        .answer(
            command.request_id,
            command_reply(completed(), "created resource 'models'"),
        )
        .await;

    let outcome = match waiting.await.assured("the command's task runs to its end") {
        Ok(outcome) => outcome,
        Err(failure) => panic!("a command the server kept reporting is answered: {failure:?}"),
    };
    assert!(
        outcome.succeeded(),
        "the command's own answer ends the call"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_command_the_server_stops_reporting_gives_up_one_whole_wait_after_its_last_report() {
    let mut loopback = Loopback::new(None);
    let (waiting, command) = start_command(&mut loopback).await;
    report_while_serving(&loopback, &command).await;
    let last_report = Instant::now();

    let failure = match waiting.await.assured("the command's task runs to its end") {
        Ok(outcome) => panic!("a command nothing answers has no outcome: {outcome:?}"),
        Err(failure) => failure,
    };
    let silence = last_report.elapsed();
    assert!(
        matches!(
            failure.current_context(),
            ClientError::UncertainCommand { reference } if reference.as_str() == "command-1"
        ),
        "a command that may have been admitted is uncertain under its reference: {failure:?}"
    );
    assert_eq!(
        silence, WAIT,
        "the call gives up exactly one whole wait after the server's last report"
    );
}
