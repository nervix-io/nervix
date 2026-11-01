//! Regressions for the sequence assertion used by broker scenarios.
//!
//! Layer: test harness.
//! - **Owns.** A stalled scenario caller and complete sequence/header failure oracles.
//! - **Depends on.** The real broker observer, a loopback NATS peer and controlled deliveries.
//! - **Must not know.** Product execution, placement or connector implementation.

use std::{future::poll_fn, panic::AssertUnwindSafe, pin::pin, task::Poll, time::Duration};

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    sync::mpsc,
    task::{consume_budget, spawn, yield_now},
    time::{Instant, pause, sleep, timeout},
};
use rstest::rstest;

use crate::{
    broker_observer::{BrokerMessage, BrokerObserver, TypedBrokerHeader},
    nats_observer::NatsObserver,
    nats_observer_tests::StandInNatsServer,
};

const BURST: usize = 32_768;

fn headers() -> Vec<(String, String)> {
    vec![
        ("route".to_string(), "primary".to_string()),
        ("route".to_string(), "columnar".to_string()),
    ]
}

#[nervix_primitives::test(flavor = "current_thread")]
async fn a_buffered_sequence_meets_its_deadline_while_the_runner_is_stalled() {
    let server = StandInNatsServer::start_sequence(BURST)
        .await
        .assured("the regression binds an ephemeral loopback port");
    let observer = NatsObserver::connect(&server.address.to_string(), "observed")
        .await
        .assured("the real client opens its observing subscription");
    timeout(Duration::from_secs(60), async {
        while observer.received() < 32_768 {
            consume_budget().await;
            yield_now().await;
        }
    })
    .await
    .assured("the real observing client buffers the complete burst before the controlled stall");

    // All network input is buffered. Virtual time now controls the caller's scheduling delay;
    // no tolerance, real sleep or machine load decides whether it exceeds the assertion budget.
    pause();
    let deadline = Instant::now() + Duration::from_secs(10);
    let observation = BrokerObserver::nats(observer).observe_sequence(
        deadline,
        BURST,
        "sequence".to_string(),
        headers(),
    );
    let mut observation = pin!(observation);
    poll_fn(|context| {
        assert!(
            observation.as_mut().poll(context).is_pending(),
            "the burst and trailing duplicate check require more than the caller's first poll"
        );
        Poll::Ready(())
    })
    .await;

    // Deliberately leave the step future unpolled beyond its captured deadline. The runtime can
    // keep polling its independent observation, just as while Cucumber polls other scenarios.
    sleep(Duration::from_secs(20)).await;
    assert!(Instant::now() > deadline);
    let observed = observation.await;
    let last = observed
        .last_message
        .assured("a nonempty sequence retains its final message");
    assert_eq!(last.payload, "{\"sequence\":32768}");
    assert_eq!(last.bytes, b"{\"sequence\":32768}");
    assert_eq!(last.headers, headers());
    assert!(
        observed
            .observer
            .describe_client_receipts()
            .contains("32768 messages")
    );
}

#[rstest]
#[case::gap(vec!["{\"sequence\":1}", "{\"sequence\":3}"], false, "duplicated, lost, or reordered")]
#[case::duplicate(vec!["{\"sequence\":1}", "{\"sequence\":1}"], false, "duplicated, lost, or reordered")]
#[case::reorder(vec!["{\"sequence\":2}", "{\"sequence\":1}"], false, "duplicated, lost, or reordered")]
#[case::invalid_json(vec!["{"], false, "not valid JSON")]
#[case::wrong_type(vec!["{\"sequence\":\"1\"}"], false, "not an unsigned integer")]
#[case::changed_headers(vec!["{\"sequence\":1}"], true, "headers changed")]
#[case::extra(vec!["{\"sequence\":1}", "{\"sequence\":2}", "{\"sequence\":3}"], false, "duplicate broker message after the expected sequence")]
#[nervix_primitives::test]
async fn a_sequence_keeps_every_message_and_header_oracle(
    #[case] payloads: Vec<&'static str>,
    #[case] change_headers: bool,
    #[case] expected_failure: &str,
) {
    let (messages, receiver) = mpsc::channel(4);
    let reader = spawn(async move {
        for payload in payloads {
            consume_budget().await;
            let delivered_headers = if change_headers {
                Vec::new()
            } else {
                headers()
            };
            messages
                .send(BrokerMessage::from_bytes(
                    payload.as_bytes(),
                    delivered_headers,
                ))
                .await
                .assured("the observer retains its receiver while reading the supplied sequence");
        }
        std::future::pending::<()>().await;
    });
    let observation = BrokerObserver::forwarded(receiver, reader).observe_sequence(
        Instant::now() + Duration::from_secs(60),
        2,
        "sequence".to_string(),
        headers(),
    );
    let Err(failure) = AssertUnwindSafe(observation).catch_unwind().await else {
        panic!("an invalid sequence must fail its full oracle");
    };
    let text = if let Some(text) = failure.downcast_ref::<String>() {
        text.as_str()
    } else {
        failure
            .downcast_ref::<&str>()
            .assured("the assertion panics with its diagnostic")
    };
    assert!(text.contains(expected_failure), "{text}");
}

#[nervix_primitives::test(start_paused = true)]
async fn a_stalled_delivery_still_fails_at_the_captured_sequence_deadline() {
    let (messages, receiver) = mpsc::channel(1);
    let reader = spawn(async move {
        let _messages = messages;
        std::future::pending::<()>().await;
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    let observation = BrokerObserver::forwarded(receiver, reader).observe_sequence(
        deadline,
        1,
        "sequence".to_string(),
        headers(),
    );
    let Err(failure) = AssertUnwindSafe(observation).catch_unwind().await else {
        panic!("no delivery cannot satisfy a sequence");
    };
    let text = failure
        .downcast_ref::<String>()
        .assured("the failed read identifies the missing sequence number");
    assert!(
        text.contains("timed out waiting for broker message 1 of 1"),
        "{text}"
    );
    assert_eq!(Instant::now(), deadline);
}

#[nervix_primitives::test]
async fn forwarded_reads_preserve_the_complete_message_and_observer() {
    let (messages, receiver) = mpsc::channel(2);
    let typed_headers = vec![TypedBrokerHeader {
        name: "user".to_string(),
        data_type: "Number".to_string(),
        value: "7".to_string(),
    }];
    let message = BrokerMessage::from_bytes(b"\xff", headers())
        .with_key(Some("key".to_string()))
        .with_group(Some("group".to_string()))
        .with_typed_headers(typed_headers.clone());
    messages.send(message).await.assured("the channel has room");
    messages
        .send(BrokerMessage::from_bytes(b"next", Vec::new()))
        .await
        .assured("the channel has room for the second message");
    let reader = spawn(async move {
        let _messages = messages;
        std::future::pending::<()>().await;
    });
    let mut observer = BrokerObserver::forwarded(receiver, reader);
    let first = observer
        .next_message()
        .await
        .assured("the first message is buffered");
    assert_eq!(first.bytes, b"\xff");
    assert_eq!(first.payload, "\u{fffd}");
    assert_eq!(first.headers, headers());
    assert_eq!(first.key.as_deref(), Some("key"));
    assert_eq!(first.group.as_deref(), Some("group"));
    assert_eq!(first.typed_headers, typed_headers);
    assert_eq!(
        observer
            .try_next_payload(Duration::ZERO)
            .await
            .assured("the next message is buffered"),
        Some("next".to_string())
    );
}
