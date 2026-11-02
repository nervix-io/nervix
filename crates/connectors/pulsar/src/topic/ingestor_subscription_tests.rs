//! Tests the resolved subscription comparison used before retiring a Pulsar input.
//!
//! Layer: test harness.
//! - **Owns.** Examples of equivalent and distinct service, topic and subscription identities.
//! - **Depends on.** The production ingestor settings and vocabulary.
//! - **Must not know.** Runtime schedules, graph installation or a live broker.

use meticulous::ResultExt as _;

use super::*;

fn entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn settings<'a>(
    config: &'a [ClientConfigEntry],
    topic: &'a PulsarTopicName,
) -> PulsarTopicIngestorSettings<'a> {
    PulsarTopicIngestorSettings {
        config,
        topic,
        subscription: PulsarSubscription {
            name: PulsarSubscriptionName::try_from("readers")
                .assured("the fixture subscription name is valid"),
            ownership: PulsarSubscriptionOwnership::Ingestor {
                start_at: nervix_models::PulsarStartPosition::Earliest,
            },
        },
        consumer_name: "domain~events".to_string(),
        ack_receipts: PulsarAckReceipts::Awaited,
    }
}

#[test]
fn equivalent_topics_and_changed_consumer_policies_keep_the_subscription() {
    let config = [entry("addr", "pulsar://broker:6650")];
    let explicit_config = [
        entry("addr", "pulsar://broker:6650"),
        entry("namespace", "public/default"),
    ];
    let topic = PulsarTopicName::try_from("events").assured("the short topic is valid");
    let qualified = PulsarTopicName::try_from("persistent://public/default/events")
        .assured("the fully qualified topic is valid");
    let previous = settings(&config, &topic);
    let mut replacement = settings(&explicit_config, &qualified);
    replacement.subscription.ownership = PulsarSubscriptionOwnership::Ingestor {
        start_at: nervix_models::PulsarStartPosition::Latest,
    };
    assert!(
        previous
            .reads_same_subscription(&replacement)
            .assured("changing a creation policy does not change a broker subscription")
    );
    replacement.subscription.ownership = PulsarSubscriptionOwnership::Provisioned;
    replacement.consumer_name = "another~consumer".to_string();
    replacement.ack_receipts = PulsarAckReceipts::Unawaited;
    assert!(
        previous
            .reads_same_subscription(&replacement)
            .assured("both settings are valid")
    );
    assert!(
        replacement
            .reads_same_subscription(&previous)
            .assured("both settings are valid")
    );

    replacement.subscription.name = PulsarSubscriptionName::try_from("different")
        .assured("the replacement subscription name is valid");
    assert!(
        !previous
            .reads_same_subscription(&replacement)
            .assured("both settings are valid")
    );
}

#[test]
fn a_changed_service_endpoint_or_qualified_topic_reads_another_subscription() {
    let config = [entry("addr", "pulsar://broker:6650")];
    let topic = PulsarTopicName::try_from("events").assured("the short topic is valid");
    let previous = settings(&config, &topic);
    for addr in [
        "pulsar://another:6650",
        "pulsar://broker:6652",
        "pulsar+ssl://broker:6650",
    ] {
        let config = [entry("addr", addr)];
        let replacement = settings(&config, &topic);
        assert!(
            !previous
                .reads_same_subscription(&replacement)
                .assured("both settings are valid")
        );
    }
    let another_namespace = [
        entry("addr", "pulsar://broker:6650"),
        entry("namespace", "tenant/ns"),
    ];
    let replacement = settings(&another_namespace, &topic);
    assert!(
        !previous
            .reads_same_subscription(&replacement)
            .assured("both settings are valid")
    );
    let another_topic = PulsarTopicName::try_from("different").assured("the topic is valid");
    let replacement = settings(&config, &another_topic);
    assert!(
        !previous
            .reads_same_subscription(&replacement)
            .assured("both settings are valid")
    );
}

#[test]
fn an_implicit_service_port_identifies_the_same_broker_as_its_explicit_port() {
    let topic = PulsarTopicName::try_from("events").assured("the topic is valid");
    for (implicit, explicit) in [
        ("pulsar://broker", "pulsar://broker:6650"),
        ("pulsar+ssl://broker", "pulsar+ssl://broker:6651"),
    ] {
        let implicit_config = [entry("addr", implicit)];
        let explicit_config = [entry("addr", explicit)];
        let previous = settings(&implicit_config, &topic);
        let replacement = settings(&explicit_config, &topic);
        assert!(
            previous
                .reads_same_subscription(&replacement)
                .assured("both settings are valid")
        );
    }
}

#[test]
fn an_invalid_service_address_cannot_decide_subscription_retirement() {
    let config = [entry("addr", "pulsar://broker:6650")];
    let topic = PulsarTopicName::try_from("events").assured("the topic is valid");
    let previous = settings(&config, &topic);
    for addr in ["not a service URL", "pulsar:///", "https://broker"] {
        let config = [entry("addr", addr)];
        let replacement = settings(&config, &topic);
        assert!(previous.reads_same_subscription(&replacement).is_err());
    }
}
