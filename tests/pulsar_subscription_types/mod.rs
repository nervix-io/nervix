//! Public Pulsar subscription type proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** Consumers a scenario holds on a subscription beside its ingestors, keyed
//!   publications, and what the broker reports about a subscription's type and active consumer.
//! - **Depends on.** The scenario's cluster, its dependency broker, its active session and the
//!   broker's admin API.
//! - **Must not know.** The Pulsar connector's private state or how a subscription is joined.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use std::collections::BTreeMap;

use cucumber::{gherkin::Step, given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;
use pulsar::SubType as PulsarSubType;

use super::{ScenarioWorld, docstring, expand_placeholders, parse_duration_text};
use crate::common::cluster::{HeldPulsarConsumer, KeyedPulsarMessage};

/// The level a standby consumer holds, which gives way to a consumer of the default level 0.
const STANDBY_PRIORITY_LEVEL: i32 = 1;

/// The driver's subscription type the broker's stats report as `reported`.
fn driver_type(reported: &str) -> PulsarSubType {
    match reported {
        "Exclusive" => PulsarSubType::Exclusive,
        "Failover" => PulsarSubType::Failover,
        "Key_Shared" => PulsarSubType::KeyShared,
        "Shared" => PulsarSubType::Shared,
        other => panic!("the broker reports no subscription type '{other}'"),
    }
}

async fn join_subscription(
    world: &mut ScenarioWorld,
    consumer: String,
    subscription: String,
    topic: String,
    subscription_type: String,
    priority_level: Option<i32>,
) {
    let consumer = expand_placeholders(world, &consumer);
    let subscription = expand_placeholders(world, &subscription);
    let topic = expand_placeholders(world, &topic);
    let held = world
        .cluster()
        .hold_pulsar_consumer(HeldPulsarConsumer {
            name: &consumer,
            topic: &topic,
            subscription: &subscription,
            subscription_type: driver_type(&subscription_type),
            priority_level,
        })
        .await
        .unwrap_or_else(|error| panic!("consumer '{consumer}' did not join: {error}"));
    world.held_pulsar_consumers.insert(consumer, held);
}

#[given(
    expr = "Pulsar consumer {string} joins subscription {string} of topic {string} as {string}"
)]
async fn consumer_joins_subscription(
    world: &mut ScenarioWorld,
    consumer: String,
    subscription: String,
    topic: String,
    subscription_type: String,
) {
    join_subscription(
        world,
        consumer,
        subscription,
        topic,
        subscription_type,
        None,
    )
    .await;
}

#[given(
    expr = "Pulsar consumer {string} joins subscription {string} of topic {string} as a {string} \
            standby"
)]
async fn standby_joins_subscription(
    world: &mut ScenarioWorld,
    consumer: String,
    subscription: String,
    topic: String,
    subscription_type: String,
) {
    join_subscription(
        world,
        consumer,
        subscription,
        topic,
        subscription_type,
        Some(STANDBY_PRIORITY_LEVEL),
    )
    .await;
}

#[when(expr = "Pulsar consumer {string} leaves")]
async fn consumer_leaves(world: &mut ScenarioWorld, consumer: String) {
    let consumer = expand_placeholders(world, &consumer);
    world
        .held_pulsar_consumers
        .remove(&consumer)
        .unwrap_or_else(|| panic!("no step joined consumer '{consumer}'"));
}

#[then(expr = "within {string} Pulsar consumer {string} receives a payload")]
async fn consumer_receives_payload(
    world: &mut ScenarioWorld,
    within: String,
    consumer: String,
    #[step] step: &Step,
) {
    let within = parse_duration_text(&within).assured("the scenario declares a valid duration");
    let consumer = expand_placeholders(world, &consumer);
    let expected = expand_placeholders(world, docstring(step));
    let held = world
        .held_pulsar_consumers
        .get_mut(&consumer)
        .unwrap_or_else(|| panic!("no step joined consumer '{consumer}'"));
    let payload = held
        .try_next_payload(within)
        .await
        .assured("a held consumer reads for as long as its observer lives")
        .unwrap_or_else(|| panic!("consumer '{consumer}' received nothing within {within:?}"));
    assert!(
        payload.contains(expected.trim()),
        "expected {expected:?} in what consumer '{consumer}' received, got {payload}"
    );
}

#[then(expr = "within {string} Pulsar subscription {string} of topic {string} has type {string}")]
async fn subscription_has_type(
    world: &mut ScenarioWorld,
    within: String,
    subscription: String,
    topic: String,
    subscription_type: String,
) {
    let within = parse_duration_text(&within).assured("the scenario declares a valid duration");
    let subscription = expand_placeholders(world, &subscription);
    let topic = expand_placeholders(world, &topic);
    world
        .cluster()
        .wait_for_pulsar_subscription(&topic, &subscription, within, |observed| {
            let Some(observation) = observed else {
                return false;
            };
            !observation.consumer_names.is_empty()
                && observation.subscription_type.as_deref() == Some(subscription_type.as_str())
        })
        .await
        .unwrap_or_else(|error| {
            panic!("the subscription is not read as '{subscription_type}': {error}")
        });
}

#[then(
    expr = "within {string} Pulsar subscription {string} of topic {string} has active consumer \
            {string}"
)]
async fn subscription_has_active_consumer(
    world: &mut ScenarioWorld,
    within: String,
    subscription: String,
    topic: String,
    consumer: String,
) {
    let within = parse_duration_text(&within).assured("the scenario declares a valid duration");
    let subscription = expand_placeholders(world, &subscription);
    let topic = expand_placeholders(world, &topic);
    let consumer = expand_placeholders(world, &consumer);
    world
        .cluster()
        .wait_for_pulsar_subscription(&topic, &subscription, within, |observed| {
            let Some(observation) = observed else {
                return false;
            };
            observation.active_consumer.as_deref() == Some(consumer.as_str())
        })
        .await
        .unwrap_or_else(|error| {
            panic!("the broker does not deliver to consumer '{consumer}': {error}")
        });
}

#[then(
    expr = "within {string} every consumer of Pulsar subscription {string} of topic {string} was \
            delivered messages"
)]
async fn every_consumer_was_delivered_messages(
    world: &mut ScenarioWorld,
    within: String,
    subscription: String,
    topic: String,
) {
    let within = parse_duration_text(&within).assured("the scenario declares a valid duration");
    let subscription = expand_placeholders(world, &subscription);
    let topic = expand_placeholders(world, &topic);
    world
        .cluster()
        .wait_for_pulsar_subscription(&topic, &subscription, within, |observed| {
            let Some(observation) = observed else {
                return false;
            };
            !observation.consumer_deliveries.is_empty()
                && observation
                    .consumer_deliveries
                    .iter()
                    .all(|delivered| *delivered > 0)
        })
        .await
        .unwrap_or_else(|error| panic!("a consumer was delivered no message: {error}"));
}

/// The payload of message `sequence` of key `key`.
fn keyed_payload(key: usize, sequence: usize) -> KeyedPulsarMessage {
    KeyedPulsarMessage {
        key: format!("key-{key}"),
        payload: format!(r#"{{"key_id":{key},"sequence":{sequence}}}"#),
    }
}

#[when(expr = "{int} keys each publish {int} sequenced messages to Pulsar topic {string}")]
async fn keys_publish_sequences(
    world: &mut ScenarioWorld,
    keys: usize,
    sequences: usize,
    topic: String,
) {
    let topic = expand_placeholders(world, &topic);
    // Each round publishes one message of every key, so a consumer that does not keep a key's
    // messages together sees every key interleaved.
    let mut messages = Vec::new();
    for sequence in 0..sequences {
        for key in 0..keys {
            messages.push(keyed_payload(key, sequence));
        }
    }
    world
        .cluster()
        .publish_keyed_pulsar(&topic, messages)
        .await
        .unwrap_or_else(|error| panic!("failed to publish the keyed messages: {error}"));
}

#[then(
    expr = "within {string} the relay subscription receives {int} messages of each of {int} keys \
            in publication order"
)]
async fn relay_subscription_receives_each_key_in_order(
    world: &mut ScenarioWorld,
    within: String,
    sequences: usize,
    keys: usize,
) {
    let within = parse_duration_text(&within).assured("the scenario declares a valid duration");
    let session = world
        .active_session
        .as_mut()
        .verified("the scenario created its relay subscription on the active session");
    let deadline = Instant::now() + within;
    let mut received: BTreeMap<u64, u64> = BTreeMap::new();
    let total = sequences
        .checked_mul(keys)
        .assured("the scenario declares small counts");
    for delivered in 0..total {
        nervix_primitives::task::consume_budget().await;
        let budget = deadline.saturating_duration_since(Instant::now());
        let event = session
            .try_next_subscription(budget)
            .await
            .expect("failed while waiting for subscription payloads")
            .unwrap_or_else(|| {
                panic!("the relay subscription delivered {delivered} messages within {within:?}")
            });
        let payload: serde_json::Value =
            serde_json::from_str(&event.payload).assured("a relay row renders as JSON");
        let key = payload["key_id"]
            .as_u64()
            .assured("every row names its key");
        let sequence = payload["sequence"]
            .as_u64()
            .assured("every row names its sequence");
        let next = received.get(&key).copied().unwrap_or_default();
        assert_eq!(
            sequence, next,
            "key {key} delivered sequence {sequence} where {next} comes next: {received:?}"
        );
        let after = next
            .checked_add(1)
            .assured("a scenario's sequence stays far below the last count");
        received.insert(key, after);
    }
    assert_eq!(received.len(), keys, "every key delivers: {received:?}");
}
