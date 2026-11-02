//! Ordinary tests of whole-window attribution, deadlines and interrupted confirmation collection.
//!
//! Layer: test harness.
//! - **Owns.** Complete settlement and retry boundaries through the production confirmation owner.
//! - **Depends on.** That owner, current messages and primitive channels/timers.
//! - **Must not know.** Real sockets, broker internals or the graph.

use std::{future::Future, num::NonZeroUsize, pin::Pin, time::Duration};

use futures_util::{
    FutureExt as _,
    future::{pending, ready},
};
use lapin::{ChannelState, Confirmation, ErrorKind};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{AckConfirmation, EmitterRecordId, PerRecordOutcome};
use nervix_primitives::{sync::oneshot, time::Instant};

use super::{AnsweredMessage, ConfirmedStop, RabbitMqConfirmationWindow, RabbitMqPublisher};
use crate::publisher::tests::{keyed, no_route, returned, settled};

type Answer = Pin<Box<dyn Future<Output = lapin::Result<Confirmation>> + Send>>;

fn settings() -> AckConfirmation {
    AckConfirmation {
        max_in_flight: NonZeroUsize::new(2).assured("two messages are outstanding"),
        timeout: Duration::from_secs(1),
    }
}

fn window() -> RabbitMqConfirmationWindow<Answer> {
    RabbitMqConfirmationWindow::new(settings())
}

fn deadline() -> Instant {
    Instant::now()
        .checked_add(settings().timeout)
        .assured("one second fits the monotonic clock")
}

#[nervix_primitives::test]
async fn every_return_is_attributed_before_a_mandatory_ack_is_delivered() {
    for carrier in 0..2 {
        let mut first = keyed(0, "order.created", b"first");
        first.record.envelope.mandatory = true;
        let mut second = keyed(1, "order.unrouted", b"second");
        second.record.envelope.mandatory = true;
        let returned = returned("events", &second);
        let mut window = window();
        let (first_answer, second_answer) = if carrier == 0 {
            (Confirmation::Ack(Some(returned)), Confirmation::Ack(None))
        } else {
            (Confirmation::Ack(None), Confirmation::Ack(Some(returned)))
        };
        window.push(first, deadline(), ready(Ok(first_answer)).boxed());
        window.push(second, deadline(), ready(Ok(second_answer)).boxed());
        assert!(window.is_full());
        let mut outcome = PerRecordOutcome::empty();
        let result = window.settle("events", &mut outcome).await;
        assert!(result.is_ok(), "both publishes were confirmed");
        assert_eq!(settled(outcome), (vec![0], vec![(1, no_route())]));
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn an_incomplete_window_keeps_a_mandatory_ack_unresolved() {
    let mut first = keyed(0, "order.created", b"first");
    first.record.envelope.mandatory = true;
    let second = keyed(1, "order.created", b"second");
    let mut window = window();
    window.push(
        first,
        deadline(),
        ready(Ok(Confirmation::Ack(None))).boxed(),
    );
    window.push(second, deadline(), pending().boxed());
    let mut outcome = PerRecordOutcome::empty();
    assert!(matches!(
        window.settle("events", &mut outcome).await,
        Err(ConfirmedStop::Failed(_))
    ));
    assert_eq!(settled(outcome), (Vec::new(), Vec::new()));
}

#[nervix_primitives::test]
async fn queue_returns_match_the_body_under_the_same_default_exchange_key() {
    let first = keyed(0, "notifications", b"first");
    let second = keyed(1, "notifications", b"second");
    let returned = returned("", &second);
    let mut window = window();
    window.push(
        first,
        deadline(),
        ready(Ok(Confirmation::Ack(Some(returned)))).boxed(),
    );
    window.push(
        second,
        deadline(),
        ready(Ok(Confirmation::Ack(None))).boxed(),
    );
    let mut outcome = PerRecordOutcome::empty();
    assert!(window.settle("", &mut outcome).await.is_ok());
    assert_eq!(settled(outcome), (vec![0], vec![(1, no_route())]));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_timeout_keeps_a_known_return_and_a_safe_nonmandatory_ack() {
    let mut first = keyed(0, "order.unrouted", b"first");
    first.record.envelope.mandatory = true;
    let mut second = keyed(1, "order.created", b"second");
    second.record.envelope.mandatory = false;
    let returned = returned("events", &first);
    let mut third = keyed(2, "order.created", b"third");
    third.record.envelope.mandatory = false;
    let mut settings = settings();
    settings.max_in_flight = NonZeroUsize::new(3).assured("three messages are outstanding");
    let mut window = RabbitMqConfirmationWindow::<Answer>::new(settings);
    window.push(first, deadline(), pending().boxed());
    window.push(
        second,
        deadline(),
        ready(Ok(Confirmation::Ack(Some(returned)))).boxed(),
    );
    window.push(
        third,
        deadline(),
        ready(Ok(Confirmation::Ack(None))).boxed(),
    );
    let mut outcome = PerRecordOutcome::empty();
    assert!(matches!(
        window.settle("events", &mut outcome).await,
        Err(ConfirmedStop::Failed(_))
    ));
    assert_eq!(settled(outcome), (vec![1, 2], vec![(0, no_route())]));
}

#[nervix_primitives::test]
async fn a_channel_failure_keeps_returns_carried_by_other_ready_answers() {
    let first = keyed(0, "order.created", b"first");
    let second = keyed(1, "order.unrouted", b"second");
    let returned = returned("events", &second);
    let mut window = window();
    window.push(
        first,
        deadline(),
        ready(Err(lapin::Error::from(ErrorKind::InvalidChannelState(
            ChannelState::Closed,
            "basic.publish",
        ))))
        .boxed(),
    );
    window.push(
        second,
        deadline(),
        ready(Ok(Confirmation::Ack(Some(returned)))).boxed(),
    );
    let mut outcome = PerRecordOutcome::empty();
    let Err(ConfirmedStop::ChannelLost { answered, .. }) =
        window.settle("events", &mut outcome).await
    else {
        panic!("the channel failure must retain its window");
    };
    RabbitMqPublisher::settle_answered("events", &answered, &mut outcome)
        .assured("the returned message is in the window");
    assert_eq!(settled(outcome), (Vec::new(), vec![(1, no_route())]));
}

#[test]
fn a_publish_failure_collects_ready_returns_without_losing_unanswered_records() {
    let first = keyed(0, "order.created", b"first");
    let second = keyed(1, "order.unrouted", b"second");
    let returned = returned("events", &second);
    let lost = || {
        lapin::Error::from(ErrorKind::InvalidChannelState(
            ChannelState::Closed,
            "basic.publish",
        ))
    };
    let mut window = window();
    assert!(window.is_empty());
    window.push(
        first,
        deadline(),
        ready(Ok(Confirmation::Ack(Some(returned)))).boxed(),
    );
    window.push(second, deadline(), ready(Err(lost())).boxed());
    let ConfirmedStop::ChannelLost { answered, .. } = window.channel_lost(lost()) else {
        panic!("a failed publish ends the current channel's window");
    };
    let mut outcome = PerRecordOutcome::empty();
    let attribution = RabbitMqPublisher::settle_answered("events", &answered, &mut outcome)
        .assured("the return belongs to the unanswered message");
    assert!(attribution.require_decided().is_err());
    assert_eq!(settled(outcome), (Vec::new(), vec![(1, no_route())]));
}

#[nervix_primitives::test]
async fn disabled_confirms_fail_without_hiding_another_messages_return() {
    let first = keyed(0, "order.created", b"first");
    let second = keyed(1, "order.unrouted", b"second");
    let returned = returned("events", &second);
    let mut window = window();
    window.push(
        first,
        deadline(),
        ready(Ok(Confirmation::NotRequested)).boxed(),
    );
    window.push(
        second,
        deadline(),
        ready(Ok(Confirmation::Ack(Some(returned)))).boxed(),
    );
    let mut outcome = PerRecordOutcome::empty();
    assert!(matches!(
        window.settle("events", &mut outcome).await,
        Err(ConfirmedStop::Failed(_))
    ));
    assert_eq!(settled(outcome), (Vec::new(), vec![(1, no_route())]));
}

#[nervix_primitives::test]
async fn a_nack_fails_its_record_while_an_attached_return_rejects_its_actual_message() {
    let first = keyed(0, "order.created", b"first");
    let second = keyed(1, "order.unrouted", b"second");
    let returned = returned("events", &second);
    let mut window = window();
    window.push(
        first,
        deadline(),
        ready(Ok(Confirmation::Nack(Some(returned)))).boxed(),
    );
    window.push(
        second,
        deadline(),
        ready(Ok(Confirmation::Ack(None))).boxed(),
    );
    let mut outcome = PerRecordOutcome::empty();
    assert!(matches!(
        window.settle("events", &mut outcome).await,
        Err(ConfirmedStop::Failed(_))
    ));
    assert_eq!(settled(outcome), (Vec::new(), vec![(1, no_route())]));
}

#[test]
fn an_unattributed_return_fails_without_settling_any_record() {
    let message = keyed(0, "order.created", b"first");
    let stray = keyed(9, "elsewhere", b"other");
    let answered = [AnsweredMessage {
        message,
        answer: Some(Confirmation::Ack(Some(returned("events", &stray)))),
    }];
    let mut outcome = PerRecordOutcome::empty();
    let Err(error) = RabbitMqPublisher::settle_answered("events", &answered, &mut outcome) else {
        panic!("a return outside the window cannot be attributed");
    };
    assert!(error.contains::<super::RabbitMqConfirmationError>());
    assert_eq!(settled(outcome), (Vec::new(), Vec::new()));
}

#[nervix_primitives::test]
async fn a_later_return_is_not_lost_after_an_earlier_positive_answer() {
    let mut first = keyed(0, "order.unrouted", b"first");
    first.record.envelope.mandatory = true;
    let second = keyed(1, "order.created", b"second");
    let returned = returned("events", &first);
    let (sender, receiver) = oneshot::channel();
    let broker = nervix_primitives::task::spawn(async move {
        nervix_primitives::task::yield_now().await;
        sender
            .send(Ok(Confirmation::Ack(Some(returned))))
            .assured("the window keeps its receiver");
    });
    let mut window = window();
    window.push(
        first,
        deadline(),
        ready(Ok(Confirmation::Ack(None))).boxed(),
    );
    window.push(
        second,
        deadline(),
        receiver
            .map(|result| result.assured("the broker sends the answer"))
            .boxed(),
    );
    let mut outcome = PerRecordOutcome::<EmitterRecordId>::empty();
    assert!(window.settle("events", &mut outcome).await.is_ok());
    broker.await.assured("the broker task must complete");
    assert_eq!(settled(outcome), (vec![1], vec![(0, no_route())]));
}
