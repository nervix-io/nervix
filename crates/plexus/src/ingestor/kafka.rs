//! The Kafka connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `KAFKA TOPIC` ingestor: its client, topic, offset ownership,
//!   instances, delivery mode and quiesce mode, and the delivery modes a Kafka consumer offers.
//! - **Depends on.** The shared ingestor clauses and the Kafka vocabulary.
//! - **Must not know.** The Kafka driver, consumer groups at run time or offset storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    KafkaConsumerGroupName, KafkaIngestMode, KafkaOffsetMode, KafkaTopicIngestor, KafkaTopicName,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase2},
};

/// `FROM <client> TOPIC '<topic>' OFFSET BY DOMAIN|CONSUMER GROUP '<group>' [INSTANCES <n>] MODE …
/// ON QUIESCE SUSPEND`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], KafkaTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    let offset_mode = kw(Identifier::Offset)
        .ignore_then(kw(Identifier::By))
        .ignore_then(choice((
            kw_phrase2(Identifier::Consumer, Identifier::Group)
                .ignore_then(external_name::<KafkaConsumerGroupName>())
                .map(KafkaOffsetMode::ConsumerGroup),
            kw(Identifier::Domain).to(KafkaOffsetMode::Domain),
        )));
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<KafkaTopicName>())
        .then(offset_mode)
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(mode())
        .then(suspend_only_quiesce_clause())
        .map(
            |(((((client, topic), offset_mode), instances), mode), quiesce)| KafkaTopicIngestor {
                client,
                topic,
                offset_mode,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}

/// The delivery modes of a Kafka consumer: `NO ACK`, `ACK SEQUENTIAL …` or `ACK PARALLEL …`.
fn mode<'src>()
-> impl Parser<'src, &'src [Token], KafkaIngestMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        ack_parallel().map(
            |ParallelAck {
                 max,
                 batch_timeout,
                 timeout,
                 retry_policy,
             }| KafkaIngestMode::AckParallel {
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
             }| KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            },
        ),
        no_ack().to(KafkaIngestMode::NoAck),
    ))
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        IngestorEntity, IngestorInput, KafkaIngestMode, KafkaIngestor, KafkaOffsetMode,
        KafkaTopicIngestor, Model,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// Every statement below up to its topic literal.
    const HEAD: &str = "CREATE KAFKA TOPIC INGESTOR intake FROM kafka_main TOPIC";

    const DOMAIN_OFFSETS: &str = "OFFSET BY DOMAIN";

    const SEQUENTIAL: &str = "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s";

    /// A `KAFKA TOPIC` ingestor statement reading `topic`, whose offsets `offsets` owns, in the
    /// delivery mode `mode`.
    fn statement(topic: &str, offsets: &str, mode: &str) -> String {
        format!(
            "{HEAD} '{topic}' {offsets} INSTANCES 2 MODE {mode} ON QUIESCE SUSPEND DECODE USING \
             event_codec TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
             GENERAL ERROR LOG;"
        )
    }

    /// The Kafka topic Model `statement` creates.
    fn created_source(statement: &str) -> KafkaTopicIngestor {
        let Some(Model::Ingestor(ingestor)) = created_model(statement) else {
            panic!("a literal topic is accepted by the composed grammar: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the KAFKA TOPIC head names a transport");
        };
        let IngestorEntity::Kafka(KafkaIngestor::Topic(source)) = input.source else {
            panic!("the KAFKA TOPIC head constructs the Kafka topic Model");
        };
        source
    }

    #[rstest]
    #[case("orders")]
    #[case("Orders.EU_v2-1")]
    #[case("..orders")]
    fn topic_literals_reach_the_ingestor_model_unchanged(#[case] topic: &str) {
        let source = created_source(&statement(topic, DOMAIN_OFFSETS, SEQUENTIAL));

        assert_eq!(source.topic.as_str(), topic);
        assert_eq!(source.instances.get(), 2);
    }

    #[test]
    fn a_topic_at_the_kafka_length_bound_is_accepted() {
        let topic = "t".repeat(249);
        let source = created_source(&statement(&topic, DOMAIN_OFFSETS, SEQUENTIAL));

        assert_eq!(source.topic.as_str(), topic);
    }

    #[test]
    fn offsets_belong_to_the_domain_or_to_a_named_consumer_group() {
        let domain = created_source(&statement("orders", DOMAIN_OFFSETS, SEQUENTIAL));
        assert_eq!(domain.offset_mode, KafkaOffsetMode::Domain);

        let grouped = created_source(&statement(
            "orders",
            "OFFSET BY CONSUMER GROUP 'Readers EU'",
            SEQUENTIAL,
        ));
        let KafkaOffsetMode::ConsumerGroup(group) = grouped.offset_mode else {
            panic!("OFFSET BY CONSUMER GROUP names the group that owns the offsets");
        };
        assert_eq!(group.as_str(), "Readers EU");
    }

    #[test]
    fn parallel_acknowledgement_keeps_its_window_and_batch_timeout() {
        let source = created_source(&statement(
            "orders",
            DOMAIN_OFFSETS,
            "ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        ));
        let KafkaIngestMode::AckParallel {
            max,
            batch_timeout,
            timeout,
            ..
        } = source.mode
        else {
            panic!("ACK PARALLEL constructs the parallel acknowledgement mode");
        };

        assert_eq!(max.get(), 8);
        assert_eq!(batch_timeout, "100ms");
        assert_eq!(timeout, "5s");
    }

    #[test]
    fn completion_names_the_topic_then_who_owns_its_offsets() {
        assert_offers_exactly(&format!("{HEAD} "), &["kafka_topic"]);
        assert_offers_exactly(&format!("{HEAD} 'orders' "), &["OFFSET BY"]);
        assert_offers_exactly(
            &format!("{HEAD} 'orders' OFFSET BY "),
            &["DOMAIN", "CONSUMER GROUP"],
        );
        assert_offers_exactly(
            &format!("{HEAD} 'orders' OFFSET BY CONSUMER GROUP "),
            &["kafka_consumer_group"],
        );
    }

    #[test]
    fn the_composed_grammar_requires_offset_ownership() {
        let statement = statement("orders", "", SEQUENTIAL);

        assert!(
            refused(&statement),
            "a Kafka topic ingestor names who owns its offsets: {statement}"
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
            let statement = statement(topic, DOMAIN_OFFSETS, SEQUENTIAL);
            assert!(
                refused(&statement),
                "topic '{topic}' is outside the Kafka topic rule"
            );
        }
    }

    #[test]
    fn the_composed_grammar_applies_the_consumer_group_rule() {
        let too_long = "g".repeat(256);
        for group in ["", too_long.as_str()] {
            let offsets = format!("OFFSET BY CONSUMER GROUP '{group}'");
            let statement = statement("orders", &offsets, SEQUENTIAL);
            assert!(
                refused(&statement),
                "consumer group '{group}' is outside the Kafka consumer-group rule"
            );
        }
    }
}
