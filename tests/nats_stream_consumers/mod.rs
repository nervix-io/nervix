//! Public NATS JetStream consumer proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** The durable pull consumer a scenario provisions as an operator does, the JetStream
//!   publication a scenario stores once, and the observations of where a consumer stands and of
//!   the streams and consumers the NATS server holds.
//! - **Depends on.** The scenario's cluster and its NATS dependency, and the placeholder, docstring
//!   and duration steps of the shared scenario module.
//! - **Must not know.** The NATS STREAM ingestor's private state or how it pulls, acknowledges or
//!   rejects a delivery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use cucumber::{gherkin::Step, given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};

use super::{ScenarioWorld, docstring, expand_placeholders, parse_duration_text};
use crate::common::cluster::NatsConsumerPosition;

/// The consumer exists before the scenario's ingestor reads it, as an operator provisions it.
#[given(expr = "NATS JetStream durable pull consumer {string} exists on stream {string}")]
async fn durable_pull_consumer_exists(world: &mut ScenarioWorld, consumer: String, stream: String) {
    let consumer = expand_placeholders(world, &consumer);
    let stream = expand_placeholders(world, &stream);
    world
        .cluster()
        .provision_nats_pull_consumer(&stream, &consumer)
        .await
        .assured("the scenario provisions its NATS JetStream consumer on its own stream");
}

/// The message is stored exactly once, because the steps after it count what the stream holds and
/// what its consumer acknowledged.
#[given(expr = "NATS JetStream message is published to subject {string}")]
#[when(expr = "NATS JetStream message is published to subject {string}")]
async fn jetstream_message_is_published(
    world: &mut ScenarioWorld,
    subject: String,
    #[step] step: &Step,
) {
    let subject = expand_placeholders(world, &subject);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_nats_jetstream(&subject, &payload)
        .await
        .assured("a stream the scenario provisioned captures the subject and stores the message");
}

#[then(
    expr = "within {string} NATS JetStream consumer {string} of stream {string} acknowledges \
            through stream sequence {int} with nothing pending"
)]
async fn consumer_acknowledges_through(
    world: &mut ScenarioWorld,
    duration: String,
    consumer: String,
    stream: String,
    sequence: u64,
) {
    let expected = NatsConsumerPosition {
        pending: 0,
        awaiting_acknowledgement: 0,
        acknowledged_through: sequence,
    };
    wait_for_consumer_position(world, &duration, &consumer, &stream, expected).await;
}

/// What a suspended ingestor leaves in its stream: messages the consumer has not delivered, while
/// everything it delivered before is acknowledged.
#[then(
    expr = "within {string} NATS JetStream consumer {string} of stream {string} holds {int} \
            messages for delivery and none awaiting acknowledgement"
)]
async fn consumer_holds_for_delivery(
    world: &mut ScenarioWorld,
    duration: String,
    consumer: String,
    stream: String,
    pending: u64,
) {
    let consumer_name = expand_placeholders(world, &consumer);
    let stream_name = expand_placeholders(world, &stream);
    let duration = parse_duration_text(&duration).assured("the step supplies a valid duration");
    let stored = world
        .cluster()
        .nats_stream_message_count(&stream_name)
        .await
        .assured("the scenario's stream answers its state");
    let acknowledged_through = stored
        .checked_sub(pending)
        .assured("a stream holds at least the messages its consumer still holds for delivery");
    let expected = NatsConsumerPosition {
        pending,
        awaiting_acknowledgement: 0,
        acknowledged_through,
    };
    world
        .cluster()
        .wait_for_nats_consumer_position(&stream_name, &consumer_name, expected, duration)
        .await
        .assured("the NATS JetStream consumer reaches the expected position");
}

#[then(expr = "NATS JetStream holds no stream {string}")]
async fn holds_no_stream(world: &mut ScenarioWorld, stream: String) {
    let stream = expand_placeholders(world, &stream);
    let exists = world
        .cluster()
        .nats_stream_exists(&stream)
        .await
        .assured("the NATS server lists its streams");
    assert!(!exists, "NATS JetStream stream '{stream}' exists");
}

#[then(expr = "NATS JetStream stream {string} holds no consumer {string}")]
async fn stream_holds_no_consumer(world: &mut ScenarioWorld, stream: String, consumer: String) {
    let stream = expand_placeholders(world, &stream);
    let consumer = expand_placeholders(world, &consumer);
    let exists = world
        .cluster()
        .nats_consumer_exists(&stream, &consumer)
        .await
        .assured("the scenario's stream lists its consumers");
    assert!(
        !exists,
        "NATS JetStream stream '{stream}' holds consumer '{consumer}'"
    );
}

async fn wait_for_consumer_position(
    world: &mut ScenarioWorld,
    duration: &str,
    consumer: &str,
    stream: &str,
    expected: NatsConsumerPosition,
) {
    let consumer = expand_placeholders(world, consumer);
    let stream = expand_placeholders(world, stream);
    let duration = parse_duration_text(duration).assured("the step supplies a valid duration");
    world
        .cluster()
        .wait_for_nats_consumer_position(&stream, &consumer, expected, duration)
        .await
        .assured("the NATS JetStream consumer reaches the expected position");
}
