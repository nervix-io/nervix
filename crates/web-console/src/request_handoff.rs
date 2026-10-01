//! The hand-off of requests from the console's controls to its session loop.
//!
//! Layer: edges.
//!
//! - **Owns.** The requests the controls issued that the session loop has not taken yet, and the
//!   bound that keeps them few and small: a request that does not fit is refused to the control
//!   that issued it, which reports the refusal where the request's outcome would have been shown.
//! - **Depends on.** The console request vocabulary and its refusals, owned by the parent module.
//! - **Must not know.** Connections, replies, or how a control shows a refusal.
//!
//! The session loop takes requests only while it serves a connection. Every request a control
//! issues while the console connects, waits to reconnect, or checks its credentials waits here
//! until the next connection opens, so this is where an unbounded burst would accumulate.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use error_stack::Report;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_util::{Stream, StreamExt as _};
use leptos::prelude::*;
use meticulous::OptionExt as _;
use nervix_recovery::Discarded as _;

use crate::{ConsoleRequest, RequestRefusal, SESSION_LIMITS};

/// The most requests waiting for the session loop.
pub(crate) const MAX_WAITING_REQUESTS: usize = 64;
/// The most text the waiting requests carry together: one frame's worth, so every request a
/// session frame can carry fits while nothing else waits.
pub(crate) const MAX_WAITING_REQUEST_BYTES: usize = SESSION_LIMITS.frame_bytes();
const _: () = assert!(MAX_WAITING_REQUESTS > 0);

/// Creates the two ends of the hand-off. Every control holds a clone of the sender; the session
/// loop owns the receiver, which ends once every sender is gone.
pub(crate) fn request_handoff() -> (RequestSender, RequestReceiver) {
    let (sender, receiver) = unbounded();
    let load = StoredValue::new(WaitingLoad::default());
    (
        RequestSender { sender, load },
        RequestReceiver { receiver, load },
    )
}

/// The requests waiting for the session loop and the text they carry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct WaitingLoad {
    requests: usize,
    bytes: usize,
}

impl WaitingLoad {
    /// Takes room for one more request carrying `bytes` of text, or says which bound it would
    /// cross.
    fn admit(&mut self, bytes: usize) -> Result<(), Report<RequestRefusal>> {
        if self.requests >= MAX_WAITING_REQUESTS {
            return Err(Report::new(RequestRefusal::TooManyWaiting {
                limit: MAX_WAITING_REQUESTS,
            }));
        }
        let total_bytes = match self.bytes.checked_add(bytes) {
            Some(total) if total <= MAX_WAITING_REQUEST_BYTES => total,
            // A sum that overflows is past the bound as well.
            Some(_) | None => {
                return Err(Report::new(RequestRefusal::TooMuchWaitingText {
                    limit: MAX_WAITING_REQUEST_BYTES,
                }));
            }
        };
        self.requests = self
            .requests
            .checked_add(1)
            .verified("the request count was checked above against its bound");
        self.bytes = total_bytes;
        Ok(())
    }

    /// Returns the room a request that left the hand-off held.
    fn release(&mut self, bytes: usize) {
        self.requests = self
            .requests
            .checked_sub(1)
            .assured("only a request the hand-off admitted leaves it");
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .assured("a request leaves with the text it was admitted with");
    }
}

/// A request on its way to the session loop, with the text it was admitted for.
struct WaitingRequest {
    request: ConsoleRequest,
    bytes: usize,
}

/// A control's end of the hand-off.
#[derive(Clone)]
pub(crate) struct RequestSender {
    sender: UnboundedSender<WaitingRequest>,
    /// Shared with the receiver, which releases the room of every request the loop takes.
    load: StoredValue<WaitingLoad>,
}

impl RequestSender {
    /// Hands a request to the session loop, or refuses it when the waiting requests leave no room
    /// for it or the session loop has stopped.
    pub(crate) fn send(&self, request: ConsoleRequest) -> Result<(), Report<RequestRefusal>> {
        let bytes = request.text_bytes();
        let Some(admitted) = self.load.try_update_value(|load| load.admit(bytes)) else {
            return Err(Report::new(RequestRefusal::Closed));
        };
        admitted?;
        let waiting = WaitingRequest { request, bytes };
        if self.sender.unbounded_send(waiting).is_err() {
            self.load
                .try_update_value(|load| load.release(bytes))
                .discarded("a console whose session state is gone has no room left to return");
            return Err(Report::new(RequestRefusal::Closed));
        }
        Ok(())
    }
}

/// The session loop's end of the hand-off. It yields requests in the order the controls issued
/// them, and ends once every sender is gone.
pub(crate) struct RequestReceiver {
    receiver: UnboundedReceiver<WaitingRequest>,
    load: StoredValue<WaitingLoad>,
}

impl RequestReceiver {
    /// Drops every request still waiting, because the session they were issued for is over.
    pub(crate) fn discard_waiting(&mut self) {
        while let Ok(waiting) = self.receiver.try_recv() {
            self.release(waiting.bytes);
        }
    }

    #[allow(deprecated)] // until try_update is stabilized
    fn release(&self, bytes: usize) {
        self.load
            .try_update_value(|load| load.release(bytes))
            .discarded("a console whose session state is gone has no room left to return");
    }
}

#[cfg(test)]
impl RequestReceiver {
    /// The next waiting request, without waiting for one to arrive. A hand-off with nothing
    /// waiting and one whose senders are gone both leave nothing to take.
    pub(crate) fn try_take(&mut self) -> Option<ConsoleRequest> {
        use futures_util::FutureExt as _;

        self.next().now_or_never().flatten()
    }
}

impl Stream for RequestReceiver {
    type Item = ConsoleRequest;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<ConsoleRequest>> {
        let Poll::Ready(next) = self.receiver.poll_next_unpin(cx) else {
            return Poll::Pending;
        };
        let Some(waiting) = next else {
            return Poll::Ready(None);
        };
        self.release(waiting.bytes);
        Poll::Ready(Some(waiting.request))
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;
    use leptos::prelude::Owner;
    use meticulous::ResultExt as _;
    use nervix_client_wire::CommandRequest;
    use nervix_models::CommandExecutionReference;

    use super::*;
    use crate::CommandPurpose;

    fn command(text: String) -> ConsoleRequest {
        ConsoleRequest::Command {
            request: CommandRequest {
                query: text,
                domain: None,
                execution_reference: CommandExecutionReference::parse(
                    "018f5b6e-7a1c-7c3e-9d2a-1b2c3d4e5f60".to_string(),
                )
                .assured("a literal UUID is a valid execution reference"),
                expected_transaction_position: None,
                expected_preview: None,
            },
            purpose: CommandPurpose::Repl,
        }
    }

    #[test]
    fn the_hand_off_refuses_a_request_past_its_count_and_takes_one_again_once_the_loop_took_one() {
        Owner::new().with(|| {
            let (sender, mut receiver) = request_handoff();
            for _ in 0..MAX_WAITING_REQUESTS {
                sender
                    .send(ConsoleRequest::ListDomains)
                    .assured("the hand-off has room for each request below its bound");
            }
            let refused = sender
                .send(ConsoleRequest::ListDomains)
                .expect_err("one request past the bound is refused");
            assert_eq!(
                refused.current_context(),
                &RequestRefusal::TooManyWaiting {
                    limit: MAX_WAITING_REQUESTS
                }
            );

            assert!(receiver.try_take().is_some());
            sender
                .send(ConsoleRequest::ListDomains)
                .assured("the request the loop took left room for one more");
        });
    }

    #[test]
    fn the_hand_off_bounds_the_text_its_waiting_requests_carry() {
        Owner::new().with(|| {
            let (sender, mut receiver) = request_handoff();
            let half = MAX_WAITING_REQUEST_BYTES / 2;
            sender
                .send(command("a".repeat(half)))
                .assured("half of the text bound fits");
            let refused = sender
                .send(command("b".repeat(MAX_WAITING_REQUEST_BYTES - half + 1)))
                .expect_err("text past the bound is refused");
            assert_eq!(
                refused.current_context(),
                &RequestRefusal::TooMuchWaitingText {
                    limit: MAX_WAITING_REQUEST_BYTES
                }
            );
            sender
                .send(command("c".repeat(MAX_WAITING_REQUEST_BYTES - half)))
                .assured("text up to the bound fits");

            receiver.discard_waiting();
            assert!(receiver.try_take().is_none());
            sender
                .send(command("d".repeat(MAX_WAITING_REQUEST_BYTES)))
                .assured("discarding the waiting requests returned all of their room");
        });
    }

    #[test]
    fn a_stopped_session_loop_refuses_requests_without_keeping_their_room() {
        Owner::new().with(|| {
            let (sender, receiver) = request_handoff();
            let load = sender.load;
            drop(receiver);
            let refused = sender
                .send(command("CREATE DOMAIN tenant;".to_string()))
                .expect_err("nothing takes requests once the loop is gone");
            assert_eq!(refused.current_context(), &RequestRefusal::Closed);
            assert_eq!(load.get_value(), WaitingLoad::default());
        });
    }

    #[test]
    fn the_loop_receives_requests_in_the_order_they_were_issued_and_ends_with_its_senders() {
        Owner::new().with(|| {
            let (sender, mut receiver) = request_handoff();
            sender
                .send(command("first".to_string()))
                .assured("the hand-off is empty");
            sender
                .send(command("second".to_string()))
                .assured("the hand-off has room");
            for expected in ["first", "second"] {
                let Some(ConsoleRequest::Command { request, .. }) = receiver.try_take() else {
                    panic!("the loop receives the commands it was handed");
                };
                assert_eq!(request.query, expected);
            }
            drop(sender);
            assert!(matches!(receiver.next().now_or_never(), Some(None)));
        });
    }
}
