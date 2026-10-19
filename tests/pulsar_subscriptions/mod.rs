//! Public Pulsar subscription ownership checks.
//!
//! Outside the layer order: a scenario harness.
//!
//! - **Owns.** Graph setup through Plexus for Pulsar ingestors, provisioning a subscription as an
//!   operator does, and broker observations of the subscriptions an ingestor reads and the names
//!   its consumers register under.
//! - **Depends on.** The public client, scenario fixtures and the broker's admin API.
//! - **Must not know.** Product runtime state or connector implementation details.

use super::*;
use crate::common::cluster::PulsarSubscriptionObservation;

/// The delivery mode every scenario ingestor reads in. Its retry policy also paces how often a
/// missing provisioned subscription is checked again.
const SCENARIO_MODE: &str = "MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s";

#[given(expr = "a Pulsar domain graph reads through {string}")]
async fn given_pulsar_domain_graph(world: &mut ScenarioWorld, addr: String) {
    let commands = expand_placeholders(
        world,
        &format!(
            r#"
            CREATE UNPACED DOMAIN {{{{domain}}}};
            CREATE SCHEMA item (user_id I64);
            CREATE WIRE JSON SCHEMA item_wire MODE STRICT (user_id integer);
            CREATE CODEC item_codec FROM WIRE JSON SCHEMA item_wire TO SCHEMA item;
            CREATE RELAY output SCHEMA item UNBRANCHED;
            CREATE CLIENT broker TYPE PULSAR CONFIG {{ 'addr' = '{addr}' }};
            START;
            "#
        ),
    );
    let leader = current_leader_node(world).await;
    execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .assured("the public Pulsar graph is valid");
}

#[given(
    expr = "Pulsar ingestor {string} reads subscription {string} of topic {string} declared \
            {string}"
)]
#[when(
    expr = "Pulsar ingestor {string} reads subscription {string} of topic {string} declared \
            {string}"
)]
async fn pulsar_ingestor_reads_subscription(
    world: &mut ScenarioWorld,
    name: String,
    subscription: String,
    topic: String,
    ownership: String,
) {
    let commands = expand_placeholders(
        world,
        &format!(
            r#"
            CREATE PULSAR TOPIC INGESTOR {name}
              FROM broker TOPIC '{topic}' SUBSCRIPTION '{subscription}' {ownership}
              {SCENARIO_MODE}
              ON QUIESCE SUSPEND DECODE USING item_codec
              TO output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
              ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#
        ),
    );
    let leader = current_leader_node(world).await;
    execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .assured("the public Pulsar ingestor is valid");
}

#[given(expr = "Pulsar subscription {string} of topic {string} is provisioned")]
#[when(expr = "Pulsar subscription {string} of topic {string} is provisioned")]
async fn pulsar_subscription_is_provisioned(
    world: &mut ScenarioWorld,
    subscription: String,
    topic: String,
) {
    let subscription = expand_placeholders(world, &subscription);
    let topic = expand_placeholders(world, &topic);
    world
        .cluster()
        .provision_pulsar_subscription(&topic, &subscription)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to provision Pulsar subscription '{subscription}': {error}")
        });
}

#[then(
    expr = "within {string} Pulsar subscription {string} of topic {string} is consumed by {string}"
)]
async fn then_subscription_is_consumed_by(
    world: &mut ScenarioWorld,
    duration: String,
    subscription: String,
    topic: String,
    consumer: String,
) {
    let within = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let subscription = expand_placeholders(world, &subscription);
    let topic = expand_placeholders(world, &topic);
    let consumer = expand_placeholders(world, &consumer);
    let expected = [consumer];
    world
        .cluster()
        .wait_for_pulsar_subscription(&topic, &subscription, within, |observed| {
            let Some(observation) = observed else {
                return false;
            };
            observation.durable && observation.consumer_names == expected
        })
        .await
        .unwrap_or_else(|error| {
            panic!("the subscription is not consumed by exactly {expected:?}: {error}")
        });
}

#[then(
    expr = "within {string} Pulsar subscription {string} of topic {string} has {int} consumer(s)"
)]
async fn then_subscription_has_consumers(
    world: &mut ScenarioWorld,
    duration: String,
    subscription: String,
    topic: String,
    consumers: usize,
) {
    let within = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let subscription = expand_placeholders(world, &subscription);
    let topic = expand_placeholders(world, &topic);
    world
        .cluster()
        .wait_for_pulsar_subscription(&topic, &subscription, within, |observed| {
            let Some(observation) = observed else {
                return false;
            };
            observation.durable && observation.consumer_names.len() == consumers
        })
        .await
        .unwrap_or_else(|error| {
            panic!("the subscription does not have {consumers} consumers: {error}")
        });
}

#[then(expr = "within {string} Pulsar subscription {string} of topic {string} has no consumers")]
async fn then_subscription_has_no_consumers(
    world: &mut ScenarioWorld,
    duration: String,
    subscription: String,
    topic: String,
) {
    then_subscription_has_consumers(world, duration, subscription, topic, 0).await;
}

#[then(expr = "Pulsar topic {string} holds durable subscription {string}")]
async fn then_topic_holds_durable_subscription(
    world: &mut ScenarioWorld,
    topic: String,
    subscription: String,
) {
    let topic = expand_placeholders(world, &topic);
    let subscription = expand_placeholders(world, &subscription);
    let observed = observe_subscription(world, &topic, &subscription).await;
    assert!(
        matches!(
            observed,
            Some(PulsarSubscriptionObservation { durable: true, .. })
        ),
        "Pulsar topic '{topic}' must hold durable subscription '{subscription}', observed \
         {observed:?}"
    );
}

#[then(expr = "Pulsar topic {string} has no durable subscription {string}")]
async fn then_topic_has_no_durable_subscription(
    world: &mut ScenarioWorld,
    topic: String,
    subscription: String,
) {
    let topic = expand_placeholders(world, &topic);
    let subscription = expand_placeholders(world, &subscription);
    let observed = observe_subscription(world, &topic, &subscription).await;
    assert!(
        !matches!(
            observed,
            Some(PulsarSubscriptionObservation { durable: true, .. })
        ),
        "Pulsar topic '{topic}' still holds durable subscription '{subscription}': {observed:?}"
    );
}

#[then(expr = "within {string} Pulsar topic {string} has no durable subscription {string}")]
async fn then_topic_eventually_has_no_durable_subscription(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    subscription: String,
) {
    let within = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let subscription = expand_placeholders(world, &subscription);
    world
        .cluster()
        .wait_for_pulsar_subscription(&topic, &subscription, within, |observed| {
            !matches!(
                observed,
                Some(PulsarSubscriptionObservation { durable: true, .. })
            )
        })
        .await
        .unwrap_or_else(|error| panic!("the durable subscription was not removed: {error}"));
}

#[then(
    expr = "Pulsar ingestors in domains {string} and {string} both deliver what topic {string} \
            carries"
)]
async fn then_both_domains_deliver(
    world: &mut ScenarioWorld,
    first: String,
    second: String,
    topic: String,
) {
    struct DomainSubscription {
        domain: String,
        session: TestSession,
    }

    let leader = current_leader_node(world).await;
    let mut subscriptions = Vec::new();
    for domain in [first, second] {
        nervix_primitives::task::consume_budget().await;
        let domain = expand_placeholders(world, &domain);
        let mut session = world
            .cluster()
            .open_session(&leader, &domain)
            .await
            .assured("each declared domain accepts a public session");
        session
            .run_command("CREATE SUBSCRIPTION observed TO output;")
            .await
            .assured("the graph exposes its output relay");
        subscriptions.push(DomainSubscription { domain, session });
    }
    let topic = expand_placeholders(world, &topic);
    world
        .cluster()
        .publish_pulsar(&topic, r#"{"user_id":41}"#)
        .await
        .assured("the fixture broker accepts the input");
    for mut subscription in subscriptions {
        nervix_primitives::task::consume_budget().await;
        let event = subscription
            .session
            .try_next_subscription(Duration::from_secs(30))
            .await
            .assured("the public subscription remains open");
        let event = event.assured("both domains deliver the published input");
        assert!(
            event.payload.contains(r#""user_id":41"#),
            "domain {} delivered {}",
            subscription.domain,
            event.payload
        );
    }
}

/// What the broker's admin API reports of `subscription` of `topic` now.
async fn observe_subscription(
    world: &ScenarioWorld,
    topic: &str,
    subscription: &str,
) -> Option<PulsarSubscriptionObservation> {
    world
        .cluster()
        .pulsar_subscription(topic, subscription)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to observe Pulsar subscription '{subscription}' of '{topic}': {error}")
        })
}
