//! Interleavings of the production RabbitMQ confirmation owner.
//!
//! Layer: test harness.
//! - **Owns.** Exact return attribution across grouped-confirm callbacks, and retention when a
//!   deadline races a partially answered mandatory window.
//! - **Depends on.** The production window/settlement owner, selected channels and timeout triggers,
//!   current message fixtures and the canonical Shuttle runner.
//! - **Must not know.** Sockets, Lapin's internal locks or AMQP frame parsing. A modeled callback
//!   is the answer the opaque driver makes available; real-broker scenarios qualify the wire.

use std::{
    future::{Future, pending},
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::FutureExt as _;
use lapin::Confirmation;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{AckConfirmation, PerRecordOutcome};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::{
    sync::oneshot,
    time::{Instant, clear_triggers, trigger_timeouts},
};

use super::{ConfirmedStop, RabbitMqConfirmationWindow};
use crate::publisher::tests::{keyed, no_route, returned, settled};

struct ChannelAnswer(oneshot::Receiver<lapin::Result<Confirmation>>);

impl Future for ChannelAnswer {
    type Output = lapin::Result<Confirmation>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.get_mut().0).poll(context) {
            Poll::Ready(answer) => {
                Poll::Ready(answer.assured("the modeled driver always answers before ending"))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

type Answer = Pin<Box<dyn Future<Output = lapin::Result<Confirmation>> + Send>>;

fn settings() -> AckConfirmation {
    AckConfirmation {
        max_in_flight: NonZeroUsize::new(2).assured("two publishes share the acknowledgement"),
        timeout: Duration::from_secs(3600),
    }
}

fn deadline() -> Instant {
    Instant::now()
        .checked_add(settings().timeout)
        .assured("one hour fits the monotonic clock")
}

#[test]
fn shuttle_a_return_followed_by_a_multiple_ack_rejects_only_its_message() {
    check_interleavings(|| {
        clear_triggers();
        shuttle::future::block_on(async {
            for returned_id in 0..2 {
                for carrier in 0..2 {
                    let mut first = keyed(0, "order.first", b"first");
                    let mut second = keyed(1, "order.second", b"second");
                    first.record.envelope.mandatory = true;
                    second.record.envelope.mandatory = true;
                    let returned_message = if returned_id == 0 {
                        returned("events", &first)
                    } else {
                        returned("events", &second)
                    };
                    let (first_sender, first_answer) = oneshot::channel();
                    let (second_sender, second_answer) = oneshot::channel();
                    // One grouped acknowledgement resolves both callbacks. Its arbitrary driver
                    // iteration can give the queued return to either, with a wakeup between them.
                    let broker = nervix_primitives::task::spawn(async move {
                        if carrier == 0 {
                            first_sender
                                .send(Ok(Confirmation::Ack(Some(returned_message))))
                                .assured("the window retains both futures until attribution");
                            nervix_primitives::task::yield_now().await;
                            second_sender
                                .send(Ok(Confirmation::Ack(None)))
                                .assured("the window retains both futures until attribution");
                        } else {
                            second_sender
                                .send(Ok(Confirmation::Ack(Some(returned_message))))
                                .assured("the window retains both futures until attribution");
                            nervix_primitives::task::yield_now().await;
                            first_sender
                                .send(Ok(Confirmation::Ack(None)))
                                .assured("the window retains both futures until attribution");
                        }
                    });
                    let mut window = RabbitMqConfirmationWindow::<Answer>::new(settings());
                    window.push(first, deadline(), ChannelAnswer(first_answer).boxed());
                    window.push(second, deadline(), ChannelAnswer(second_answer).boxed());
                    let mut outcome = PerRecordOutcome::empty();
                    assert!(window.settle("events", &mut outcome).await.is_ok());
                    broker
                        .await
                        .assured("a panicking modeled driver fails the check");
                    assert_eq!(
                        settled(outcome),
                        (vec![1 - returned_id], vec![(returned_id, no_route())])
                    );
                }
            }
        });
    });
}

#[derive(Clone, Copy, Debug)]
struct WindowDeadline;

#[test]
fn shuttle_a_deadline_never_delivers_a_partially_attributed_mandatory_window() {
    check_interleavings(|| {
        clear_triggers();
        shuttle::future::block_on(async {
            shuttle::current::set_label_for_task(shuttle::current::me(), WindowDeadline);
            let mut first = keyed(0, "order.first", b"first");
            let mut second = keyed(1, "order.second", b"second");
            first.record.envelope.mandatory = true;
            second.record.envelope.mandatory = true;
            let (sender, answer) = oneshot::channel();
            let (given, observed) = oneshot::channel();
            let broker = nervix_primitives::task::spawn(async move {
                sender
                    .send(Ok(Confirmation::Ack(None)))
                    .assured("the clock waits for this answer before ending the window");
                given
                    .send(())
                    .assured("the clock awaits the driver observation");
            });
            let clock = nervix_primitives::task::spawn(async move {
                observed
                    .await
                    .assured("the driver signals its completed callback");
                trigger_timeouts(|labels| labels.get::<WindowDeadline>().is_some());
            });
            let mut window = RabbitMqConfirmationWindow::<Answer>::new(settings());
            window.push(first, deadline(), ChannelAnswer(answer).boxed());
            window.push(second, deadline(), pending().boxed());
            let mut outcome = PerRecordOutcome::empty();
            assert!(matches!(
                window.settle("events", &mut outcome).await,
                Err(ConfirmedStop::Failed(_))
            ));
            broker
                .await
                .assured("a panicking modeled driver fails the check");
            clock
                .await
                .assured("a panicking modeled clock fails the check");
            assert_eq!(settled(outcome), (Vec::new(), Vec::new()));
        });
    });
}
