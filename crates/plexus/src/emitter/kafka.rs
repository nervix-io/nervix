//! KAFKA emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of KAFKA emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], KafkaTopicPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    broker_mode!(KafkaTopicPublishingMode)
}

pub(super) fn topic_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<KafkaTopicEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Topic))
            .then(external_name::<KafkaTopicName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, topic), mode)| KafkaTopicEmitter {
                client,
                topic,
                mode,
            }),
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use meticulous::OptionExt as _;
    use nervix_models::{AckWindow, EmitterEntity, KafkaEmitter, KafkaTopicPublishingMode, Model};
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// Every statement below up to its topic literal.
    const HEAD: &str = "CREATE KAFKA TOPIC EMITTER publishing FROM events TO kafka_main TOPIC";

    const UNCONFIRMED: &str = "NO ACK RETRY POLICY BACKOFF 250ms MAX 30s";

    /// A `KAFKA TOPIC` emitter statement producing to `topic` in the publishing mode `mode`.
    fn statement(topic: &str, mode: &str) -> String {
        format!(
            "{HEAD} '{topic}' MODE {mode} ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The Kafka topic emitter Model `statement` creates.
    fn created_destination(statement: &str) -> nervix_models::KafkaTopicEmitter {
        let Some(Model::Emitter(emitter)) = created_model(statement) else {
            panic!("a literal topic is accepted by the composed grammar: {statement}");
        };
        let EmitterEntity::Kafka(KafkaEmitter::Topic(destination)) = *emitter.connector else {
            panic!("the KAFKA TOPIC head constructs the Kafka topic Model");
        };
        destination
    }

    #[rstest]
    #[case("orders")]
    #[case("Orders.EU_v2-1")]
    #[case("..orders")]
    fn topic_literals_reach_the_emitter_model_unchanged(#[case] topic: &str) {
        let destination = created_destination(&statement(topic, UNCONFIRMED));

        assert_eq!(destination.topic.as_str(), topic);
        assert!(matches!(
            destination.mode,
            KafkaTopicPublishingMode::NoAck { .. }
        ));
    }

    #[rstest]
    #[case(
        "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        AckWindow::Sequential
    )]
    #[case(
        "ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        AckWindow::Parallel { max: NonZeroU64::new(8).assured("eight is nonzero") }
    )]
    fn a_confirmed_publish_keeps_its_window_and_timeout(
        #[case] mode: &str,
        #[case] expected: AckWindow,
    ) {
        let destination = created_destination(&statement("orders", mode));
        let KafkaTopicPublishingMode::Ack {
            window,
            ack_timeout,
            ..
        } = destination.mode
        else {
            panic!("MODE ACK constructs the confirmed publishing mode");
        };

        assert_eq!(window, expected);
        assert_eq!(ack_timeout, "5s");
    }

    #[test]
    fn completion_names_the_topic_then_the_declared_publishing_modes() {
        assert_offers_exactly(&format!("{HEAD} "), &["kafka_topic"]);
        assert_offers_exactly(
            &format!("{HEAD} 'orders' MODE "),
            &["NO ACK", "ACK SEQUENTIAL", "ACK PARALLEL"],
        );
    }

    #[rstest]
    #[case("SINGLE RETRY POLICY BACKOFF 250ms MAX 30s")]
    #[case("ACK RETRY POLICY BACKOFF 1s MAX 30s")]
    fn the_composed_grammar_refuses_publishing_the_topic_does_not_declare(#[case] mode: &str) {
        let statement = statement("orders", mode);

        assert!(
            refused(&statement),
            "a Kafka topic emitter publishes NO ACK or ACK with a window: {statement}"
        );
    }

    #[test]
    fn the_composed_grammar_applies_the_topic_rule() {
        let too_long = "t".repeat(250);
        for topic in [
            "",
            ".",
            "..",
            "orders/eu",
            "orders eu",
            "commandes-été",
            too_long.as_str(),
        ] {
            let statement = statement(topic, UNCONFIRMED);
            assert!(
                refused(&statement),
                "topic '{topic}' is outside the Kafka topic rule"
            );
        }
    }
}
