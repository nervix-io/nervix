//! The requests the dispatcher tests register waiters for, so that a reply must answer each one as
//! a server answers that request.
//!
//! Layer: test harness.
//! - **Owns.** Request fixtures and the registration of their waiters.
//! - **Depends on.** The production pending-request registry and the wire contract's requests.
//! - **Must not know.** How replies are routed or what they carry.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientRequest, CommandRequest, SubscribeRequest, SubscriptionType, UnsubscribeRequest,
};
use nervix_models::SubscriptionName;
use nervix_primitives::sync::blocking::Mutex;

use super::{domain, reference};
use crate::exchange::{ExpectedReply, PendingReplies, RegisteredRequest};

/// What a listing's reply must be: a domain list, a rejection or a cancellation.
pub(super) fn listing() -> ExpectedReply {
    ExpectedReply::of(&ClientRequest::ListDomains)
}

/// Registers a waiter for `request`, which a reply must answer as a server answers that request.
pub(super) async fn register(
    pending: &Mutex<PendingReplies>,
    request: &ClientRequest,
) -> RegisteredRequest {
    pending
        .lock()
        .register(ExpectedReply::of(request))
        .assured("an open exchange registers requests")
}

/// A command whose waiter an outcome under `execution_reference` answers.
pub(super) fn command_request(execution_reference: &str) -> ClientRequest {
    ClientRequest::Command(CommandRequest {
        query: "SHOW CLUSTER STATUS;".to_string(),
        domain: None,
        execution_reference: reference(execution_reference),
        expected_transaction_position: None,
        expected_preview: None,
    })
}

/// A subscription opening, whose waiter a subscribe outcome answers.
pub(super) fn subscribe_request() -> ClientRequest {
    ClientRequest::Subscribe(SubscribeRequest {
        domain: domain("tenant"),
        statement: "CREATE SUBSCRIPTION live FROM orders;".to_string(),
        subscription_type: SubscriptionType::Row,
        expected_schema: None,
    })
}

/// A subscription deletion, whose waiter an unsubscribe outcome answers.
pub(super) fn unsubscribe_request() -> ClientRequest {
    ClientRequest::Unsubscribe(UnsubscribeRequest {
        subscription: SubscriptionName::parse("live")
            .assured("the test subscription name is valid"),
    })
}

#[cfg(not(feature = "shuttle"))]
mod ordering {
    use std::{
        future::Future as _,
        pin::Pin,
        task::{Context, Poll, Wake, Waker},
    };

    use nervix_client_wire::RequestId;
    use nervix_primitives::sync::{Arc, StdArc, blocking::Mutex};

    use super::*;

    struct RecordedWake {
        request_id: RequestId,
        order: Arc<Mutex<Vec<RequestId>>>,
    }

    impl Wake for RecordedWake {
        fn wake(self: StdArc<Self>) {
            self.order.lock().push(self.request_id);
        }
    }

    #[test]
    fn ending_an_exchange_wakes_remaining_requests_in_identity_order() {
        let mut pending = PendingReplies::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut requests = Vec::new();
        for _ in 0..32 {
            let mut request = pending.register(listing()).assured("the exchange is open");
            // The standard Wake interface requires standard shared ownership only at this API.
            let waker = Waker::from(StdArc::new(RecordedWake {
                request_id: request.request_id,
                order: order.clone(),
            }));
            assert!(
                Pin::new(&mut request.reply)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            requests.push(request);
        }
        let cancelled = requests.remove(5);
        drop(cancelled.reply);
        assert!(pending.take(cancelled.request_id).is_some());
        let completed = requests.remove(12);
        drop(pending.take(completed.request_id));
        assert_eq!(*order.lock(), [completed.request_id]);
        order.lock().clear();

        pending.close_with(Some(tonic::Status::unavailable("the first ending cause")));
        let expected: Vec<_> = requests.iter().map(|request| request.request_id).collect();
        assert_eq!(*order.lock(), expected);
        for request in &mut requests {
            assert!(matches!(
                Pin::new(&mut request.reply).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(_))
            ));
        }
        pending.close();
        let crate::ClientError::Transport(status) = pending.failure() else {
            panic!("a later close preserves the first transport cause");
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(*order.lock(), expected, "a second close wakes nothing");
        assert!(pending.register(listing()).is_none());
    }
}
