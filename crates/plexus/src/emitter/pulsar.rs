//! PULSAR emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of PULSAR emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], PulsarTopicPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    broker_mode!(PulsarTopicPublishingMode)
}

pub(super) fn topic_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<PulsarTopicEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Topic))
            .then(external_name::<PulsarTopicName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, topic), mode)| PulsarTopicEmitter {
                client,
                topic,
                mode,
            }),
    )
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        AckWindow, EmitterEntity, Model, PulsarEmitter, PulsarTopicPublishingMode,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// A `PULSAR TOPIC` emitter statement producing to `topic` in the publishing mode `mode`.
    fn statement(topic: &str, mode: &str) -> String {
        format!(
            "CREATE PULSAR TOPIC EMITTER publishing FROM events TO pulsar_main TOPIC '{topic}' \
             MODE {mode} ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The Pulsar emitter Model `statement` creates.
    fn created_emitter(statement: &str) -> nervix_models::PulsarTopicEmitter {
        let Some(Model::Emitter(emitter)) = created_model(statement) else {
            panic!("the composed grammar accepts the statement: {statement}");
        };
        let EmitterEntity::Pulsar(PulsarEmitter::Topic(destination)) = *emitter.connector else {
            panic!("the PULSAR TOPIC head constructs the Pulsar topic Model");
        };
        destination
    }

    const NO_ACK: &str = "NO ACK RETRY POLICY BACKOFF 250ms MAX 30s";

    #[rstest]
    #[case("Orders-EU_1")]
    #[case("persistent://tenant-a/orders/Placed")]
    #[case("non-persistent://public/default/orders")]
    fn topic_literals_reach_the_emitter_model_unchanged(#[case] topic: &str) {
        let destination = created_emitter(&statement(topic, NO_ACK));

        assert_eq!(destination.topic.as_str(), topic);
        assert_eq!(destination.client.as_str(), "pulsar_main");
    }

    #[test]
    fn every_publishing_mode_parses_into_its_window() {
        let unconfirmed = created_emitter(&statement("orders", NO_ACK));
        let PulsarTopicPublishingMode::NoAck { retry_policy } = unconfirmed.mode else {
            panic!("NO ACK parses into the unconfirmed Pulsar mode");
        };
        assert_eq!(retry_policy.backoff, "250ms");

        let sequential = created_emitter(&statement(
            "orders",
            "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s",
        ));
        let PulsarTopicPublishingMode::Ack {
            window,
            ack_timeout,
            ..
        } = sequential.mode
        else {
            panic!("ACK SEQUENTIAL parses into the confirmed Pulsar mode");
        };
        assert_eq!(window, AckWindow::Sequential);
        assert_eq!(ack_timeout, "5s");

        let parallel = created_emitter(&statement(
            "orders",
            "ACK PARALLEL MAX 4 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s",
        ));
        let PulsarTopicPublishingMode::Ack { window, .. } = parallel.mode else {
            panic!("ACK PARALLEL parses into the confirmed Pulsar mode");
        };
        let AckWindow::Parallel { max } = window else {
            panic!("ACK PARALLEL keeps up to MAX records awaiting their receipts");
        };
        assert_eq!(max.get(), 4);
    }

    #[test]
    fn completion_names_the_topic_rule() {
        assert_offers_exactly(
            "CREATE PULSAR TOPIC EMITTER publishing FROM events TO pulsar_main TOPIC ",
            &["pulsar_topic"],
        );
    }

    #[test]
    fn the_composed_grammar_applies_the_topic_rule() {
        let too_long = "t".repeat(1025);
        for topic in [
            "",
            "public/default/orders",
            "persistent://public/orders",
            "persistent://public//orders",
            "kafka://public/default/orders",
            "Orders EU",
            too_long.as_str(),
        ] {
            let statement = statement(topic, NO_ACK);
            assert!(
                refused(&statement),
                "topic '{topic}' is outside the Pulsar topic rule"
            );
        }
    }
}
