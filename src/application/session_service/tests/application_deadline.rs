//! Deadline precedence at the application owner.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Paused-clock regressions for available and held installation slots and an
//!   application deadline expiring between polls.
//! - **Depends on.** The production application bound and runtime admission, cancellation,
//!   and the selected test clock.
//! - **Must not know.** Domain replication, backup coordination or archive contents.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "application deadline regressions remain outside product execution"
    )
)]

use meticulous::OptionExt as _;
use nervix_primitives::sync::CancellationToken;

use super::super::{ApplicationBound, ApplicationWait, RuntimeAdmission};

#[nervix_primitives::test(start_paused = true)]
async fn an_elapsed_application_deadline_refuses_an_available_installation_before_a_timer_tick() {
    let admission = RuntimeAdmission::new();
    let shutdown = CancellationToken::new();
    // Put the deadline strictly inside the driver's first millisecond, so its rounded
    // timer cannot already be ready at the runtime's initial clock origin.
    nervix_primitives::time::advance(std::time::Duration::from_nanos(1)).await;
    let deadline = nervix_primitives::time::Instant::now();
    let installation = admission.begin_installation(&shutdown);
    tokio::pin!(installation);

    let error = nervix_primitives::select! {
        biased;
        error = ApplicationBound::Deadline(deadline).ends(ApplicationWait::Installation) => error,
        _ = &mut installation => panic!("an elapsed cut must refuse before taking an available installation slot"),
    };

    assert!(
        matches!(
            error.current_context(),
            crate::runtime::RuntimeError::ApplicationDeadline {
                wait: ApplicationWait::Installation,
            }
        ),
        "the elapsed cut retains its installation refusal: {error:#}"
    );
    assert_eq!(nervix_primitives::time::Instant::now(), deadline);
}

#[nervix_primitives::test(start_paused = true)]
async fn an_elapsed_application_deadline_refuses_a_held_installation_before_a_timer_tick() {
    let admission = RuntimeAdmission::new();
    let shutdown = CancellationToken::new();
    let held = admission
        .begin_installation(&shutdown)
        .await
        .assured("the first installation takes the available slot");
    let installation = admission.begin_installation(&shutdown);
    tokio::pin!(installation);
    assert!(futures_util::poll!(&mut installation).is_pending());
    nervix_primitives::time::advance(std::time::Duration::from_nanos(1)).await;
    let deadline = nervix_primitives::time::Instant::now();
    let ended = ApplicationBound::Deadline(deadline).ends(ApplicationWait::Installation);
    tokio::pin!(ended);

    let std::task::Poll::Ready(error) = futures_util::poll!(&mut ended) else {
        panic!("an elapsed cut must refuse a held installation without waiting for a timer tick");
    };
    assert!(
        matches!(
            error.current_context(),
            crate::runtime::RuntimeError::ApplicationDeadline {
                wait: ApplicationWait::Installation,
            }
        ),
        "the elapsed cut retains its installation refusal: {error:#}"
    );
    assert_eq!(nervix_primitives::time::Instant::now(), deadline);
    drop(held);
}

#[nervix_primitives::test(start_paused = true)]
async fn an_application_deadline_expiring_between_polls_precedes_the_timer_tick() {
    use std::time::Duration;

    for wait in [
        ApplicationWait::Installation,
        ApplicationWait::Preparation { revision: 7 },
        ApplicationWait::Activation { revision: 7 },
        ApplicationWait::Readiness { revision: 7 },
    ] {
        nervix_primitives::task::consume_budget().await;
        let until_deadline = Duration::from_nanos(1);
        let deadline = nervix_primitives::time::Instant::now()
            .checked_add(until_deadline)
            .assured("one nanosecond fits the test clock");
        let ended = ApplicationBound::Deadline(deadline).ends(wait);
        tokio::pin!(ended);
        assert!(futures_util::poll!(&mut ended).is_pending());

        nervix_primitives::time::advance(until_deadline).await;

        let std::task::Poll::Ready(error) = futures_util::poll!(&mut ended) else {
            panic!("the elapsed deadline must end {wait} before the timer's rounded tick");
        };
        assert!(
            matches!(
                error.current_context(),
                crate::runtime::RuntimeError::ApplicationDeadline { wait: ended_wait }
                    if *ended_wait == wait
            ),
            "the deadline retains the wait it ended: {error:#}"
        );
        assert_eq!(nervix_primitives::time::Instant::now(), deadline);
    }
}
