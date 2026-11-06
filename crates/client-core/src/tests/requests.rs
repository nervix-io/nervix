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
    })
}

/// A subscription deletion, whose waiter an unsubscribe outcome answers.
pub(super) fn unsubscribe_request() -> ClientRequest {
    ClientRequest::Unsubscribe(UnsubscribeRequest {
        subscription: SubscriptionName::parse("live")
            .assured("the test subscription name is valid"),
    })
}
