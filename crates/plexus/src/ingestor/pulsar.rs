//! The Pulsar connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `PULSAR TOPIC` ingestor: its client, topic, subscription and who
//!   owns it, instances, delivery mode and quiesce mode, and the delivery modes a Pulsar
//!   subscription offers.
//! - **Depends on.** The shared ingestor clauses and the Pulsar vocabulary.
//! - **Must not know.** The Pulsar driver or how a subscription is acknowledged, verified or
//!   removed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    PulsarIngestMode, PulsarSubscription, PulsarSubscriptionName, PulsarSubscriptionOwnership,
    PulsarTopicIngestor, PulsarTopicName,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase3},
};

/// `FROM <client> TOPIC '<topic>' SUBSCRIPTION '<subscription>' OWNED BY INGESTOR|PROVISIONED
/// [INSTANCES <n>] MODE … ON QUIESCE SUSPEND`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], PulsarTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<PulsarTopicName>())
        .then(subscription())
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(mode())
        .then(suspend_only_quiesce_clause())
        .map(
            |(((((client, topic), subscription), instances), mode), quiesce)| PulsarTopicIngestor {
                client,
                topic,
                subscription,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}

/// The delivery modes of a Pulsar subscription: `NO ACK`, `ACK SEQUENTIAL …` or `ACK PARALLEL …`.
fn mode<'src>()
-> impl Parser<'src, &'src [Token], PulsarIngestMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        ack_parallel().map(
            |ParallelAck {
                 max,
                 batch_timeout,
                 timeout,
                 retry_policy,
             }| PulsarIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            },
        ),
        ack_sequential().map(
            |SequentialAck {
                 timeout,
                 retry_policy,
             }| PulsarIngestMode::AckSequential {
                timeout,
                retry_policy,
            },
        ),
        no_ack().to(PulsarIngestMode::NoAck),
    ))
}

/// `SUBSCRIPTION '<subscription>' OWNED BY INGESTOR|PROVISIONED`: the subscription's name, then
/// who owns it, which every statement declares.
fn subscription<'src>()
-> impl Parser<'src, &'src [Token], PulsarSubscription, extra::Err<ParseError<'src>>> + Clone {
    let ownership = choice((
        kw_phrase3(Identifier::Owned, Identifier::By, Identifier::Ingestor)
            .to(PulsarSubscriptionOwnership::Ingestor),
        kw(Identifier::Provisioned).to(PulsarSubscriptionOwnership::Provisioned),
    ));
    kw(Identifier::Subscription)
        .ignore_then(external_name::<PulsarSubscriptionName>())
        .then(ownership)
        .map(|(name, ownership)| PulsarSubscription { name, ownership })
        .boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        IngestorEntity, IngestorInput, Model, PulsarIngestMode, PulsarIngestor,
        PulsarSubscriptionOwnership, PulsarTopicIngestor,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// A `PULSAR TOPIC` ingestor statement reading `topic` through `subscription` in the delivery
    /// mode `mode`.
    fn statement(topic: &str, subscription: &str, mode: &str) -> String {
        format!(
            "CREATE PULSAR TOPIC INGESTOR intake FROM pulsar_main TOPIC '{topic}' {subscription} \
             INSTANCES 2 MODE {mode} ON QUIESCE SUSPEND DECODE USING event_codec TO events \
             INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The Pulsar topic source `statement` creates.
    fn created_source(statement: &str) -> PulsarTopicIngestor {
        let Some(Model::Ingestor(ingestor)) = created_model(statement) else {
            panic!("the composed grammar accepts the statement: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the PULSAR TOPIC head names a transport");
        };
        let IngestorEntity::Pulsar(PulsarIngestor::Topic(source)) = input.source else {
            panic!("the PULSAR TOPIC head constructs the Pulsar topic Model");
        };
        source
    }

    const OWNED: &str = "SUBSCRIPTION 'Readers (eu)' OWNED BY INGESTOR";

    #[rstest]
    #[case("Orders-EU_1")]
    #[case("persistent://tenant-a/orders/Placed")]
    #[case("non-persistent://public/default/orders")]
    fn topic_literals_reach_the_ingestor_model_unchanged(#[case] topic: &str) {
        let source = created_source(&statement(topic, OWNED, "NO ACK"));

        assert_eq!(source.topic.as_str(), topic);
        assert_eq!(source.instances.get(), 2);
    }

    #[rstest]
    #[case("OWNED BY INGESTOR", PulsarSubscriptionOwnership::Ingestor)]
    #[case("PROVISIONED", PulsarSubscriptionOwnership::Provisioned)]
    fn the_subscription_keeps_its_name_and_its_declared_owner(
        #[case] owner: &str,
        #[case] ownership: PulsarSubscriptionOwnership,
    ) {
        let subscription = format!("SUBSCRIPTION 'Readers (eu)' {owner}");
        let source = created_source(&statement("orders", &subscription, "NO ACK"));

        assert_eq!(source.subscription.name.as_str(), "Readers (eu)");
        assert_eq!(source.subscription.ownership, ownership);
    }

    #[test]
    fn every_delivery_mode_parses_into_the_pulsar_mode() {
        let no_ack = created_source(&statement("orders", OWNED, "NO ACK"));
        assert_eq!(no_ack.mode, PulsarIngestMode::NoAck);

        let sequential = created_source(&statement(
            "orders",
            OWNED,
            "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        ));
        let PulsarIngestMode::AckSequential {
            timeout,
            retry_policy,
        } = sequential.mode
        else {
            panic!("ACK SEQUENTIAL parses into the sequential Pulsar mode");
        };
        assert_eq!(timeout, "5s");
        assert_eq!(retry_policy.backoff, "1s");
        assert_eq!(retry_policy.max_backoff, "30s");

        let parallel = created_source(&statement(
            "orders",
            OWNED,
            "ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        ));
        let PulsarIngestMode::AckParallel {
            max,
            batch_timeout,
            timeout,
            retry_policy,
        } = parallel.mode
        else {
            panic!("ACK PARALLEL parses into the parallel Pulsar mode");
        };
        assert_eq!(max.get(), 8);
        assert_eq!(batch_timeout, "100ms");
        assert_eq!(timeout, "5s");
        assert_eq!(retry_policy.max_backoff, "30s");
    }

    #[test]
    fn completion_names_the_topic_and_subscription_rules_then_the_owners() {
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR intake FROM pulsar_main TOPIC ",
            &["pulsar_topic"],
        );
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR intake FROM pulsar_main TOPIC 'orders' SUBSCRIPTION ",
            &["pulsar_subscription"],
        );
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR intake FROM pulsar_main TOPIC 'orders' SUBSCRIPTION \
             'readers' ",
            &["OWNED BY INGESTOR", "PROVISIONED"],
        );
    }

    #[test]
    fn the_composed_grammar_refuses_a_subscription_without_its_owner() {
        for subscription in ["", "SUBSCRIPTION 'readers'", "OWNED BY INGESTOR"] {
            let statement = statement("orders", subscription, "NO ACK");
            assert!(
                refused(&statement),
                "every Pulsar ingestor names its subscription and who owns it: {statement}"
            );
        }
    }

    #[test]
    fn the_composed_grammar_applies_the_topic_and_subscription_rules() {
        let topic_too_long = "t".repeat(1025);
        for topic in [
            "",
            "public/default/orders",
            "persistent://public/orders",
            "persistent://public//orders",
            "kafka://public/default/orders",
            "Orders EU",
            topic_too_long.as_str(),
        ] {
            let statement = statement(topic, OWNED, "NO ACK");
            assert!(
                refused(&statement),
                "topic '{topic}' is outside the Pulsar topic rule"
            );
        }

        let subscription_too_long = format!("SUBSCRIPTION '{}' PROVISIONED", "s".repeat(256));
        for subscription in [
            "SUBSCRIPTION '' PROVISIONED",
            subscription_too_long.as_str(),
        ] {
            let statement = statement("orders", subscription, "NO ACK");
            assert!(
                refused(&statement),
                "the subscription is outside the Pulsar subscription rule: {statement}"
            );
        }
    }
}
