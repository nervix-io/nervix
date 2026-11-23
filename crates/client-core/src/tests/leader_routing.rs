//! Routing a command while the cluster elects no leader: the client waits for an election and sends
//! the command again until its retry timeout passes, however short its election waits are.
//!
//! Layer: test harness.
//! - **Owns.** Leaderless routing fixtures served by hand over a loopback exchange, on a paused
//!   clock.
//! - **Depends on.** The production client, its connection capabilities and the wire contract.
//! - **Must not know.** How a server elects a leader.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{ClientMessage, ClientRequest, CommandDisposition, LeaderRedirect};
use nervix_primitives::time::{Instant, sleep};

use super::{Loopback, command_reply, reference};
use crate::{ClientError, ConnectOptions, connection::ServerDirectory, exchange::SESSION_LIMITS};

/// How long the leaderless node takes to answer each attempt. With a one-millisecond election
/// wait after each answer, the default 120-second retry timeout holds about 2,400 attempts.
const ANSWER_DELAY: Duration = Duration::from_millis(49);

#[nervix_primitives::test(start_paused = true)]
async fn a_leaderless_command_is_sent_again_for_its_whole_retry_timeout_at_a_one_millisecond_ceiling()
 {
    let options = ConnectOptions {
        retry_backoff: Duration::from_millis(1),
        retry_max_backoff: Duration::from_millis(1),
        ..ConnectOptions::default()
    };
    let retry_timeout = options.retry_timeout;
    let election_wait = options.retry_max_backoff;
    let mut loopback = Loopback::configured(None, ServerDirectory::default(), options);
    let client = loopback.client.clone();
    let execution = client
        .prepare_execution("CREATE DOMAIN orders;")
        .await
        .with_reference(reference("command-1"));
    let started = Instant::now();
    let mut command =
        nervix_primitives::task::spawn(async move { client.execute_prepared(&execution).await });

    // Every attempt is answered the way a node answers while no leader is elected, until the
    // client stops sending the command.
    let mut attempts = 0_u64;
    let ended = loop {
        nervix_primitives::task::consume_budget().await;
        let received = nervix_primitives::select! {
            biased;
            ended = &mut command => break ended.assured("the command's task runs to its end"),
            received = loopback.requests.recv() => received,
        };
        let frame = received
            .assured("the client keeps its exchange open while it routes the command")
            .verify(&SESSION_LIMITS)
            .assured("a request the client encoded verifies");
        let message = ClientMessage::decode(&frame).assured("a request the client encoded decodes");
        assert!(
            matches!(message.request, ClientRequest::Command(_)),
            "the client sends nothing but the routed command: {:?}",
            message.request
        );
        attempts = attempts
            .checked_add(1)
            .assured("a test that answers one attempt per answer delay counts far below u64");
        // The client may give up while the node takes its time, and then no longer waits for
        // the answer.
        nervix_primitives::select! {
            biased;
            ended = &mut command => break ended.assured("the command's task runs to its end"),
            () = sleep(ANSWER_DELAY) => {}
        }
        loopback
            .answer(
                message.request_id,
                command_reply(
                    CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
                    "no leader is elected",
                ),
            )
            .await;
    };

    let elapsed = started.elapsed();
    let unused = retry_timeout.saturating_sub(elapsed);
    assert!(
        unused <= election_wait,
        "a leaderless command is sent again until its {retry_timeout:?} retry timeout has no room \
         for another {election_wait:?} election wait, but the client stopped after {attempts} \
         attempts, {elapsed:?} into it"
    );
    // The command ends with the last answer it had when no wait is left, or at the retry deadline.
    match ended {
        Ok(outcome) => assert_eq!(
            outcome.disposition,
            CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
            "a command no leader answered ends with the last answer it had"
        ),
        Err(failure) => assert!(
            failure.frames().any(|frame| matches!(
                frame.downcast_ref::<ClientError>(),
                Some(ClientError::RetryDeadline)
            )),
            "a leaderless command fails only at its retry deadline: {failure:?}"
        ),
    }
}
