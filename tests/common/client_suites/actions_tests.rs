//! The scenario action owner: only declared, ordered, performed actions receive confirmation.

use std::{io, time::Duration};

use nervix_primitives::time::Instant;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::client_suite_actions::{SuiteAction, SuiteActions};

#[nervix_primitives::test]
async fn each_performed_action_is_confirmed_once_in_request_order() -> io::Result<()> {
    let mut actions = SuiteActions::new();
    let mut input = Vec::new();
    let mut performed = Vec::new();
    for (sequence, name) in [
        (1, "refuse-next-subscription-opening"),
        (2, "clear-subscription-opening-refusals"),
    ] {
        nervix_primitives::task::consume_budget().await;
        let line = format!("NERVIX-SUITE-ACTION {sequence} {name}");
        assert!(
            actions
                .handle(
                    &line,
                    &mut input,
                    Instant::now() + Duration::from_secs(10),
                    &mut |action| {
                        performed.push(action);
                        Ok(())
                    }
                )
                .await?
        );
    }
    assert_eq!(
        performed,
        [
            SuiteAction::RefuseNextSubscriptionOpening,
            SuiteAction::ClearSubscriptionOpeningRefusals
        ]
    );
    assert_eq!(input, b"NERVIX-SUITE-CONFIRMED 1 refuse-next-subscription-opening\nNERVIX-SUITE-CONFIRMED 2 clear-subscription-opening-refusals\n");
    assert!(
        actions
            .handle(
                "NERVIX-SUITE-ACTION 2 refuse-next-subscription-opening",
                &mut input,
                Instant::now() + Duration::from_secs(10),
                &mut |_| panic!("a repeated action is not performed")
            )
            .await
            .is_err()
    );
    Ok(())
}

#[nervix_primitives::test]
async fn malformed_unknown_and_out_of_sequence_requests_never_perform_an_action() {
    let oversized = format!("NERVIX-SUITE-ACTION 1 {}", "x".repeat(256));
    for line in [
        "NERVIX-SUITE-ACTION",
        "NERVIX-SUITE-ACTION 1 unknown",
        "NERVIX-SUITE-ACTION 0 refuse-next-subscription-opening",
        "NERVIX-SUITE-ACTION 2 refuse-next-subscription-opening",
        "NERVIX-SUITE-ACTION 1 refuse-next-subscription-opening extra",
        "NERVIX-SUITE-ACTION 1 refuse-next-subscription-openingé",
        &oversized,
    ] {
        nervix_primitives::task::consume_budget().await;
        let mut input = Vec::new();
        let result = SuiteActions::new()
            .handle(
                line,
                &mut input,
                Instant::now() + Duration::from_secs(10),
                &mut |_| panic!("invalid action performed"),
            )
            .await;
        assert!(result.is_err(), "{line}");
        assert!(input.is_empty());
    }
}

#[nervix_primitives::test]
async fn ordinary_output_does_not_request_an_action() -> io::Result<()> {
    let mut input = Vec::new();
    let handled = SuiteActions::new()
        .handle(
            "NERVIX-SUITE passed cluster/test.py::case",
            &mut input,
            Instant::now() + Duration::from_secs(10),
            &mut |_| panic!("ordinary output performed an action"),
        )
        .await?;
    assert!(!handled);
    assert!(input.is_empty());
    Ok(())
}

#[nervix_primitives::test]
async fn a_failed_action_is_never_confirmed() {
    let mut input = Vec::new();
    let result = SuiteActions::new()
        .handle(
            "NERVIX-SUITE-ACTION 1 refuse-next-subscription-opening",
            &mut input,
            Instant::now() + Duration::from_secs(10),
            &mut |_| Err(io::Error::other("action refused")),
        )
        .await;
    let Err(error) = result else {
        panic!("the failed action must end the run");
    };
    assert!(error.to_string().contains("action refused"));
    assert!(input.is_empty());
}

#[nervix_primitives::test]
async fn a_closed_input_does_not_confirm_the_action() {
    let (mut input, receiver) = tokio::io::duplex(256);
    drop(receiver);
    assert!(
        SuiteActions::new()
            .handle(
                "NERVIX-SUITE-ACTION 1 refuse-next-subscription-opening",
                &mut input,
                Instant::now() + Duration::from_secs(10),
                &mut |_| Ok(())
            )
            .await
            .is_err()
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn an_unread_confirmation_is_bounded_by_the_runs_deadline() -> io::Result<()> {
    let (mut input, mut receiver) = tokio::io::duplex(1);
    // Occupy the one-byte pipe so no confirmation byte can be delivered.
    input.write_all(b"x").await?;
    let result = SuiteActions::new()
        .handle(
            "NERVIX-SUITE-ACTION 1 refuse-next-subscription-opening",
            &mut input,
            Instant::now() + Duration::from_secs(10),
            &mut |_| Ok(()),
        )
        .await;
    let Err(error) = result else {
        panic!("unread input must end at the deadline");
    };
    assert!(error.to_string().contains("deadline"));
    let mut occupied = [0; 1];
    receiver.read_exact(&mut occupied).await?;
    assert_eq!(&occupied, b"x");
    Ok(())
}
