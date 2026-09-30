//! Restoring desired subscriptions on a replacement exchange, and deleting a subscription that no
//! session holds.
//!
//! The server is played by hand over a loopback exchange. The server's answers follow what a real
//! session answers: a session that never opened a subscription refuses to delete it.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientRequest, ReplyBody, SubscribeDisposition, SubscribeOutcome, UnsubscribeDisposition,
    UnsubscribeOutcome,
};
use nervix_models::SubscriptionName;
use nervix_primitives::time::Instant;

use super::{DEADLINE, Loopback, domain, opened_reply, subscription};
use crate::{SubscriptionEvent, SubscriptionLifecycle, SubscriptionRequest};

fn name(value: &str) -> SubscriptionName {
    SubscriptionName::parse(value).assured("the test subscription name is valid")
}

/// The reply of a session that refuses to open a subscription.
fn refused(message: &str) -> ReplyBody {
    ReplyBody::Subscribe(SubscribeOutcome {
        disposition: SubscribeDisposition::Failed,
        message: message.to_string(),
        diagnostics: Vec::new(),
    })
}

/// The reply of a session that holds no subscription of the name it was asked to delete.
fn not_held(subscription_name: &str) -> ReplyBody {
    ReplyBody::Unsubscribe(UnsubscribeOutcome {
        disposition: UnsubscribeDisposition::Failed,
        message: format!("session subscription '{subscription_name}' does not exist"),
        diagnostics: Vec::new(),
    })
}

impl Loopback {
    /// Opens `subscription_name` on `orders`, answering the request with `generation`.
    async fn open(&mut self, subscription_name: &str, generation: u64) {
        let client = self.client.clone();
        let request = SubscriptionRequest::new(subscription_name, "orders");
        let creating =
            nervix_primitives::task::spawn(async move { client.subscribe(&request).await });
        let sent = self.next_request().await;
        let ClientRequest::Subscribe(subscribe) = sent.request else {
            panic!("a subscription opens with a subscribe request");
        };
        assert_eq!(
            subscribe.statement,
            format!("CREATE SUBSCRIPTION {subscription_name} TO orders;")
        );
        self.answer(
            sent.request_id,
            opened_reply(subscription(subscription_name, generation)),
        )
        .await;
        let outcome = creating
            .await
            .assured("the create task completes")
            .assured("the subscription opens");
        assert!(outcome.succeeded(), "{}", outcome.message);
    }

    /// Ends the current exchange the way a lost transport does, without replacing it.
    async fn end_exchange(&self) {
        let generation = self.client.inner.exchange.lock().await.generation.clone();
        self.pending.lock().close();
        self.client.inner.events.sinks.close_generation(&generation);
    }

    /// Waits until the lifecycle of `subscription_name` is `expected`.
    async fn wait_for_lifecycle(
        &self,
        subscription_name: &str,
        expected: Option<SubscriptionLifecycle>,
    ) {
        let mut changed = self.client.inner.events.sinks.desired.watch();
        nervix_primitives::time::timeout(DEADLINE, async {
            loop {
                nervix_primitives::task::consume_budget().await;
                if self.client.subscription_lifecycle(&name(subscription_name)) == expected {
                    return;
                }
                changed
                    .changed()
                    .await
                    .assured("the registry remains alive while the client is held");
            }
        })
        .await
        .assured("the subscription reaches the expected lifecycle within the deadline");
    }

    /// Answers every request the way a session that holds no subscription does, until `task`
    /// finishes, and returns what it returned.
    async fn serve_until<T>(&mut self, mut task: nervix_primitives::task::JoinHandle<T>) -> T {
        loop {
            nervix_primitives::task::consume_budget().await;
            let received = nervix_primitives::select! {
                finished = &mut task => {
                    return finished.assured("the served task completes");
                }
                received = self.requests.recv() => received,
            };
            let frame = received.assured("the client keeps its exchange open");
            let frame = frame
                .verify(&crate::exchange::SESSION_LIMITS)
                .assured("a request the client encoded verifies");
            let request = nervix_client_wire::ClientMessage::decode(&frame)
                .assured("a request the client encoded decodes");
            let reply = match &request.request {
                ClientRequest::Unsubscribe(unsubscribe) => {
                    not_held(unsubscribe.subscription.as_str())
                }
                ClientRequest::Subscribe(_) => refused("relay 'orders' does not exist"),
                other => panic!("the test serves subscription requests only, not {other:?}"),
            };
            self.answer(request.request_id, reply).await;
        }
    }
}

#[nervix_primitives::test]
async fn deleting_a_subscription_its_ended_session_held_releases_the_name() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback.open("live", 1).await;
    loopback.end_exchange().await;
    let interrupted = loopback
        .client
        .next_subscription()
        .await
        .assured("the lost session reports the subscription's gap");
    assert!(matches!(interrupted, SubscriptionEvent::Interrupted(_)));

    let deleted = loopback
        .client
        .unsubscribe("live")
        .await
        .assured("a subscription no session holds is deleted without the server");
    assert!(deleted.succeeded(), "{}", deleted.message);
    assert_eq!(loopback.client.subscription_lifecycle(&name("live")), None);

    loopback.replace_exchange().await;
    loopback.open("live", 2).await;
    assert!(
        loopback.requests.try_recv().is_err(),
        "the deleted subscription is not restored on the next exchange"
    );
}

#[nervix_primitives::test]
async fn deleting_a_failed_delivery_whose_session_ended_releases_the_name() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback.open("live", 1).await;
    let generation = loopback
        .client
        .inner
        .exchange
        .lock()
        .await
        .generation
        .clone();
    loopback
        .client
        .inner
        .events
        .sinks
        .desired
        .overflow(&subscription("live", 1), &generation);
    assert_eq!(
        loopback.client.subscription_lifecycle(&name("live")),
        Some(SubscriptionLifecycle::DeliveryFailed(subscription(
            "live", 1
        )))
    );
    loopback.end_exchange().await;

    let deleted = loopback
        .client
        .unsubscribe("live")
        .await
        .assured("a subscription no session holds is deleted without the server");
    assert!(deleted.succeeded(), "{}", deleted.message);
    assert_eq!(loopback.client.subscription_lifecycle(&name("live")), None);

    loopback.replace_exchange().await;
    loopback.open("live", 2).await;
}

#[nervix_primitives::test]
async fn deleting_a_subscription_whose_restoration_was_refused_needs_no_server() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback.open("live", 1).await;
    loopback.replace_exchange().await;
    let interrupted = loopback
        .client
        .next_subscription()
        .await
        .assured("the lost session reports the subscription's gap");
    assert!(matches!(interrupted, SubscriptionEvent::Interrupted(_)));
    let restore = loopback.next_request().await;
    assert!(matches!(restore.request, ClientRequest::Subscribe(_)));
    loopback
        .answer(restore.request_id, refused("relay 'orders' does not exist"))
        .await;
    loopback
        .wait_for_lifecycle(
            "live",
            Some(SubscriptionLifecycle::Interrupted(subscription("live", 1))),
        )
        .await;

    let client = loopback.client.clone();
    let deleting = nervix_primitives::task::spawn(async move { client.unsubscribe("live").await });
    let deleted = loopback
        .serve_until(deleting)
        .await
        .assured("a subscription no session holds is deleted without the server");
    assert!(deleted.succeeded(), "{}", deleted.message);
    assert_eq!(loopback.client.subscription_lifecycle(&name("live")), None);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_refused_restoration_is_repeated_after_a_growing_delay() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback.open("live", 1).await;
    loopback.replace_exchange().await;
    let interrupted = loopback
        .client
        .next_subscription()
        .await
        .assured("the lost session reports the subscription's gap");
    assert!(matches!(interrupted, SubscriptionEvent::Interrupted(_)));

    let mut sent_at = Vec::new();
    let mut reported_waits = Vec::new();
    for _ in 0..3 {
        let restore = loopback.next_request().await;
        assert!(matches!(restore.request, ClientRequest::Subscribe(_)));
        sent_at.push(Instant::now());
        loopback
            .answer(restore.request_id, refused("relay 'orders' is starting"))
            .await;
        let reported = loopback
            .client
            .next_subscription()
            .await
            .assured("the refused restoration is reported");
        let SubscriptionEvent::RestorationFailed(failure) = reported else {
            panic!("a refused restoration is reported as such: {reported:?}");
        };
        assert_eq!(failure.subscription, subscription("live", 1));
        assert_eq!(failure.message, "relay 'orders' is starting");
        reported_waits.push(failure.retry_after);
        assert_eq!(
            loopback.client.subscription_lifecycle(&name("live")),
            Some(SubscriptionLifecycle::Interrupted(subscription("live", 1)))
        );
    }
    let mut intervals = Vec::new();
    for pair in sent_at.windows(2) {
        intervals.push(pair[1].duration_since(pair[0]));
    }
    assert_eq!(
        intervals,
        [Duration::from_secs(1), Duration::from_secs(2)],
        "each refusal on one exchange doubles the wait before the next attempt"
    );
    assert_eq!(
        reported_waits,
        [
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4)
        ],
        "each report names the wait before the next attempt"
    );

    let restore = loopback.next_request().await;
    loopback
        .answer(restore.request_id, opened_reply(subscription("live", 2)))
        .await;
    loopback
        .wait_for_lifecycle(
            "live",
            Some(SubscriptionLifecycle::Active(subscription("live", 2))),
        )
        .await;
}

#[nervix_primitives::test]
async fn a_restoration_whose_exchange_ends_unanswered_is_sent_again_on_the_next_exchange() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback.open("live", 1).await;
    loopback.replace_exchange().await;
    let unanswered = loopback.next_request().await;
    assert!(matches!(unanswered.request, ClientRequest::Subscribe(_)));

    loopback.replace_exchange().await;
    let restore = loopback.next_request().await;
    assert!(
        matches!(restore.request, ClientRequest::Subscribe(_)),
        "the next exchange opens the subscription again"
    );
    loopback
        .answer(restore.request_id, opened_reply(subscription("live", 2)))
        .await;
    loopback
        .wait_for_lifecycle(
            "live",
            Some(SubscriptionLifecycle::Active(subscription("live", 2))),
        )
        .await;
}

#[nervix_primitives::test(start_paused = true)]
async fn a_rejected_restoration_is_reported_and_sent_again() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    loopback.open("live", 1).await;
    loopback.replace_exchange().await;
    let interrupted = loopback
        .client
        .next_subscription()
        .await
        .assured("the lost session reports the subscription's gap");
    assert!(matches!(interrupted, SubscriptionEvent::Interrupted(_)));
    let restore = loopback.next_request().await;
    loopback
        .answer(
            restore.request_id,
            ReplyBody::Rejected(nervix_client_wire::RequestRejected {
                rejection: nervix_client_wire::RequestRejection::TooManyRequestsInFlight,
                field: None,
                message: "the session already has 64 requests in flight".to_string(),
            }),
        )
        .await;
    let reported = loopback
        .client
        .next_subscription()
        .await
        .assured("the rejected restoration is reported");
    let SubscriptionEvent::RestorationFailed(failure) = reported else {
        panic!("a rejected restoration is reported as a failed restoration: {reported:?}");
    };
    assert!(
        failure.message.contains("TooManyRequestsInFlight"),
        "{}",
        failure.message
    );
    assert_eq!(failure.retry_after, Duration::from_secs(1));

    let restore = loopback.next_request().await;
    assert!(matches!(restore.request, ClientRequest::Subscribe(_)));
    loopback
        .answer(restore.request_id, opened_reply(subscription("live", 2)))
        .await;
    loopback
        .wait_for_lifecycle(
            "live",
            Some(SubscriptionLifecycle::Active(subscription("live", 2))),
        )
        .await;
}

#[nervix_primitives::test]
async fn a_deletion_waiting_on_a_creation_completes_when_its_session_ends() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let creating = nervix_primitives::task::spawn(async move {
        client
            .subscribe(&SubscriptionRequest::new("live", "orders"))
            .await
    });
    let unanswered = loopback.next_request().await;
    assert!(matches!(unanswered.request, ClientRequest::Subscribe(_)));
    let client = loopback.client.clone();
    let deleting = nervix_primitives::task::spawn(async move { client.unsubscribe("live").await });
    loopback
        .wait_for_lifecycle("live", Some(SubscriptionLifecycle::Closing))
        .await;
    let repeated = loopback
        .client
        .unsubscribe("live")
        .await
        .assured("a second deletion is refused locally");
    assert!(!repeated.succeeded());
    assert_eq!(
        repeated.message,
        "subscription 'live' is already being deleted"
    );

    loopback.end_exchange().await;
    let deleted = deleting
        .await
        .assured("the delete task completes")
        .assured("the subscription ended with its session");
    assert!(deleted.succeeded());
    assert_eq!(
        deleted.message,
        "subscription 'live' closed with its session"
    );
    assert_eq!(loopback.client.subscription_lifecycle(&name("live")), None);
    assert!(
        creating.await.assured("the create task completes").is_err(),
        "a client with no server to reconnect to reports the lost session"
    );
}
