//! The Kafka connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `KAFKA TOPIC` ingestor: its client, topic, offset ownership, a
//!   consumer group's start position, instances, delivery mode and quiesce mode, the delivery modes
//!   a Kafka consumer offers, and its alteration.
//! - **Depends on.** The shared ingestor clauses and the Kafka vocabulary.
//! - **Must not know.** The Kafka driver, consumer groups at run time or offset storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    KafkaConsumerGroupName, KafkaIngestMode, KafkaOffsetMode, KafkaTopicIngestor,
    KafkaTopicIngestorOperation, KafkaTopicName,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    start_position, suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase2},
};

/// `FROM <client> TOPIC '<topic>'
/// OFFSET BY DOMAIN|CONSUMER GROUP '<group>' START AT EARLIEST|LATEST
/// [INSTANCES <n>] MODE … ON QUIESCE SUSPEND`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], KafkaTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    let consumer_group = kw_phrase2(Identifier::Consumer, Identifier::Group)
        .ignore_then(external_name::<KafkaConsumerGroupName>())
        .then(start_position())
        .map(|(group, start_at)| KafkaOffsetMode::ConsumerGroup { group, start_at });
    let offset_mode = kw(Identifier::Offset)
        .ignore_then(kw(Identifier::By))
        .ignore_then(choice((
            consumer_group,
            kw(Identifier::Domain).to(KafkaOffsetMode::Domain),
        )));
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<KafkaTopicName>())
        .then(offset_mode)
        .then(optional_instances::<KafkaTopicIngestor>())
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

pub(super) fn alteration<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::AlterIngestor, extra::Err<ParseError<'src>>> + Clone
{
    let operation = choice((
        kw(Identifier::Set)
            .ignore_then(start_position())
            .map(|position| KafkaTopicIngestorOperation::SetStartAt { position }),
        super::transport_operation(topic_body()).map(KafkaTopicIngestorOperation::Transport),
    ));
    super::alteration::<KafkaTopicIngestor, _, _>(operation)
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        AlterIngestor, AlterIngestorError, CreateIngestor, IngestStartPosition, IngestorEntity,
        IngestorInput, KafkaIngestMode, KafkaIngestor, KafkaOffsetMode, KafkaTopicIngestor, Model,
        Statement,
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
        assert_eq!(
            source.instances,
            nervix_models::IngestorInstances::Count(nonzero_ext::nonzero!(2u64))
        );
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
            "OFFSET BY CONSUMER GROUP 'Readers EU' START AT LATEST",
            SEQUENTIAL,
        ));
        let KafkaOffsetMode::ConsumerGroup { group, .. } = grouped.offset_mode else {
            panic!("OFFSET BY CONSUMER GROUP names the group that owns the offsets");
        };
        assert_eq!(group.as_str(), "Readers EU");
    }

    /// The start position of the consumer group `source` declares.
    fn start_position_of(source: &KafkaTopicIngestor) -> IngestStartPosition {
        let KafkaOffsetMode::ConsumerGroup { start_at, .. } = &source.offset_mode else {
            panic!("the statement names a consumer group");
        };
        *start_at
    }

    #[rstest]
    #[case("EARLIEST", IngestStartPosition::Earliest)]
    #[case("LATEST", IngestStartPosition::Latest)]
    #[case("earliest", IngestStartPosition::Earliest)]
    #[case("latest", IngestStartPosition::Latest)]
    fn a_consumer_group_declares_where_it_starts(
        #[case] written: &str,
        #[case] position: IngestStartPosition,
    ) {
        let offsets = format!("OFFSET BY CONSUMER GROUP 'readers' START AT {written}");
        let source = created_source(&statement("orders", &offsets, SEQUENTIAL));

        assert_eq!(start_position_of(&source), position);
    }

    #[rstest]
    #[case::group_without_a_position("OFFSET BY CONSUMER GROUP 'readers'")]
    #[case::position_without_a_value("OFFSET BY CONSUMER GROUP 'readers' START AT")]
    #[case::position_outside_its_choices("OFFSET BY CONSUMER GROUP 'readers' START AT NOW")]
    #[case::position_before_the_group("OFFSET BY CONSUMER GROUP START AT EARLIEST 'readers'")]
    #[case::domain_with_a_position("OFFSET BY DOMAIN START AT EARLIEST")]
    #[case::repeated_position(
        "OFFSET BY CONSUMER GROUP 'readers' START AT EARLIEST START AT LATEST"
    )]
    fn the_composed_grammar_ties_a_start_position_to_a_consumer_group(#[case] offsets: &str) {
        let statement = statement("orders", offsets, SEQUENTIAL);

        assert!(refused(&statement), "this offset clause reads: {statement}");
    }

    /// The ingestor `offsets` declares, ready for an alteration to change.
    fn created_ingestor(offsets: &str) -> CreateIngestor {
        let text = statement("orders", offsets, SEQUENTIAL);
        let Statement::Create(create) =
            crate::statement::parse_statement(&text).verified("the current statement parses")
        else {
            panic!("the statement creates a Model");
        };
        let Model::Ingestor(ingestor) = *create.body else {
            panic!("the statement creates an ingestor");
        };
        ingestor
    }

    /// The Kafka topic source `ingestor` reads.
    fn source_of(ingestor: &CreateIngestor) -> &KafkaTopicIngestor {
        let IngestorInput::Transport(input) = &ingestor.input else {
            panic!("Kafka is a transport");
        };
        let IngestorEntity::Kafka(KafkaIngestor::Topic(source)) = &input.source else {
            panic!("the source is a Kafka topic");
        };
        source
    }

    fn alteration(text: &str) -> AlterIngestor {
        let statement = crate::statement::parse_statement(text).verified("current ALTER parses");
        let Statement::AlterIngestor(alteration) = statement else {
            panic!("the statement alters an ingestor");
        };
        alteration
    }

    #[rstest]
    #[case(IngestStartPosition::Earliest)]
    #[case(IngestStartPosition::Latest)]
    fn alteration_changes_only_the_start_position(#[case] position: IngestStartPosition) {
        let mut ingestor = created_ingestor("OFFSET BY CONSUMER GROUP 'readers' START AT EARLIEST");
        let expected = {
            let mut source = source_of(&ingestor).clone();
            let KafkaOffsetMode::ConsumerGroup { start_at, .. } = &mut source.offset_mode else {
                panic!("the statement names a consumer group");
            };
            *start_at = position;
            source
        };

        let alter = alteration(&format!(
            "ALTER KAFKA TOPIC INGESTOR intake SET START AT {position}"
        ));
        ingestor
            .apply_alter(&alter)
            .verified("a consumer group's position changes");

        assert_eq!(source_of(&ingestor), &expected);
    }

    #[test]
    fn mixed_alteration_operations_keep_their_order() {
        let mut ingestor = created_ingestor("OFFSET BY CONSUMER GROUP 'readers' START AT LATEST");
        let alter = alteration(
            "ALTER KAFKA TOPIC INGESTOR intake SET START AT LATEST, SET DECODE USING other_codec, \
             SET START AT EARLIEST",
        );

        ingestor
            .apply_alter(&alter)
            .verified("the operations of a consumer-group ingestor apply");

        assert_eq!(
            start_position_of(source_of(&ingestor)),
            IngestStartPosition::Earliest
        );
        assert_eq!(
            ingestor.input.codec().map(|codec| codec.as_str()),
            Some("other_codec")
        );
    }

    #[test]
    fn a_domain_offset_ingestor_refuses_a_start_position_and_stays_as_it_was() {
        let mut ingestor = created_ingestor(DOMAIN_OFFSETS);
        let stored = ingestor.clone();

        let refusal = ingestor
            .apply_alter(&alteration(
                "ALTER KAFKA TOPIC INGESTOR intake SET START AT EARLIEST",
            ))
            .expect_err("the domain's start point owns a domain-offset ingestor's position");

        assert_eq!(
            refusal.current_context(),
            &AlterIngestorError::DomainKafkaStartPosition
        );
        assert_eq!(ingestor, stored);
    }

    #[test]
    fn the_alteration_refuses_a_position_outside_its_choices() {
        for clause in ["SET START AT NOW", "SET START AT", "SET START EARLIEST"] {
            let statement = format!("ALTER KAFKA TOPIC INGESTOR intake {clause}");
            assert!(refused(&statement), "this alteration reads: {statement}");
        }
    }

    #[test]
    fn completion_offers_the_alteration_start_position_once_its_set_is_read() {
        assert_offers_exactly(
            "ALTER KAFKA TOPIC INGESTOR intake SET START AT ",
            &["EARLIEST", "LATEST"],
        );
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
    fn instancing_accepts_full_width_counts_or_the_complete_partition_phrase() {
        let text = statement("orders", DOMAIN_OFFSETS, SEQUENTIAL);
        let partitioned = text.replace("INSTANCES 2", "INSTANCES PER PARTITION");
        assert_eq!(
            created_source(&partitioned).instances,
            nervix_models::IngestorInstances::PerPartition
        );
        let bounded = text.replace("INSTANCES 2", "INSTANCES 18446744073709551615");
        assert_eq!(
            created_source(&bounded).instances,
            nervix_models::IngestorInstances::Count(std::num::NonZeroU64::MAX)
        );
        for invalid in ["0", "18446744073709551616", "PER"] {
            assert!(
                created_model(&text.replace("INSTANCES 2", &format!("INSTANCES {invalid}")))
                    .is_none()
            );
        }
        assert_offers_exactly(
            &format!("{HEAD} 'orders' {DOMAIN_OFFSETS} INSTANCES "),
            &["PER PARTITION", "instance_count"],
        );
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
        assert_offers_exactly(
            &format!("{HEAD} 'orders' OFFSET BY CONSUMER GROUP 'readers' "),
            &["START AT"],
        );
        assert_offers_exactly(
            &format!("{HEAD} 'orders' OFFSET BY CONSUMER GROUP 'readers' START AT "),
            &["EARLIEST", "LATEST"],
        );
    }

    #[test]
    fn completion_keeps_the_start_position_off_a_domain_offset_ingestor() {
        assert_offers_exactly(
            &format!("{HEAD} 'orders' OFFSET BY DOMAIN "),
            &["INSTANCES", "MODE"],
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
            let offsets = format!("OFFSET BY CONSUMER GROUP '{group}' START AT LATEST");
            let statement = statement("orders", &offsets, SEQUENTIAL);
            assert!(
                refused(&statement),
                "consumer group '{group}' is outside the Kafka consumer-group rule"
            );
        }
    }
}
