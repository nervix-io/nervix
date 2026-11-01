//! The NATS connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `NATS SUBJECT` ingestor: its client, subject, queue group, instances,
//!   delivery mode and quiesce mode; and the body of a `NATS STREAM` ingestor: its client, stream,
//!   consumer, acknowledged delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the NATS vocabulary.
//! - **Must not know.** The NATS driver or how a queue subscription is held at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    NatsConsumerName, NatsIngestMode, NatsQueueGroupName, NatsStreamIngestMode, NatsStreamIngestor,
    NatsStreamName, NatsSubjectIngestor, NatsSubjectName,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, flexible_quiesce_clause,
    instance_count, no_ack, push_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase2},
};

/// `FROM <client> SUBJECT '<subject>' QUEUE GROUP '<group>' INSTANCES <n> MODE NO ACK ON QUIESCE
/// BUFFER …|DROP`.
pub(super) fn subject_body<'src>()
-> impl Parser<'src, &'src [Token], NatsSubjectIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Subject))
        .then(external_name::<NatsSubjectName>())
        .then_ignore(kw_phrase2(Identifier::Queue, Identifier::Group))
        .then(external_name::<NatsQueueGroupName>())
        .then_ignore(kw(Identifier::Instances))
        .then(instance_count())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(NatsIngestMode::NoAck))
        .then(push_quiesce_clause())
        .map(
            |(((((client, subject), queue_group), instances), mode), quiesce)| {
                NatsSubjectIngestor {
                    client,
                    subject,
                    queue_group,
                    instances,
                    mode,
                    quiesce,
                }
            },
        )
        .boxed()
}

/// `FROM <client> STREAM '<stream>' CONSUMER '<consumer>' MODE ACK SEQUENTIAL …|ACK PARALLEL MAX
/// <n> … ON QUIESCE SUSPEND|BUFFER …|DROP`.
pub(super) fn stream_body<'src>()
-> impl Parser<'src, &'src [Token], NatsStreamIngestor, extra::Err<ParseError<'src>>> + Clone {
    let sequential = ack_sequential().map(
        |SequentialAck {
             timeout,
             retry_policy,
         }| NatsStreamIngestMode::AckSequential {
            timeout,
            retry_policy,
        },
    );
    let parallel = ack_parallel().map(
        |ParallelAck {
             max,
             batch_timeout,
             timeout,
             retry_policy,
         }| NatsStreamIngestMode::AckParallel {
            max,
            batch_timeout,
            timeout,
            retry_policy,
        },
    );
    client_ref()
        .then_ignore(kw(Identifier::Stream))
        .then(external_name::<NatsStreamName>())
        .then_ignore(kw(Identifier::Consumer))
        .then(external_name::<NatsConsumerName>())
        .then_ignore(kw(Identifier::Mode))
        .then(choice((parallel, sequential)))
        .then(flexible_quiesce_clause())
        .map(
            |((((client, stream), consumer), mode), quiesce)| NatsStreamIngestor {
                client,
                stream,
                consumer,
                mode,
                quiesce,
            },
        )
        .boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        IngestQuiesceMode, IngestorEntity, IngestorInput, Model, NatsIngestor,
        NatsStreamIngestMode, NatsStreamIngestor,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// A complete `NATS SUBJECT` ingestor statement that subscribes to `subject` as a member of
    /// `queue_group`.
    fn statement(subject: &str, queue_group: &str) -> String {
        format!(
            "CREATE NATS SUBJECT INGESTOR intake FROM nats_main SUBJECT '{subject}' QUEUE GROUP \
             '{queue_group}' INSTANCES 2 MODE NO ACK ON QUIESCE DROP DECODE USING event_codec TO \
             events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR \
             LOG;"
        )
    }

    #[rstest]
    #[case("Orders.Created")]
    #[case("orders.*.EU")]
    #[case("orders.>")]
    #[case("telemetry.Événements.*")]
    fn subscription_literals_reach_the_ingestor_model_unchanged(#[case] subject: &str) {
        let statement = statement(subject, "Readers-EU");
        let Some(Model::Ingestor(ingestor)) = created_model(&statement) else {
            panic!("a subscription subject is accepted by the composed grammar: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the NATS SUBJECT head names a transport");
        };
        let IngestorEntity::Nats(NatsIngestor::Subject(source)) = input.source else {
            panic!("the NATS SUBJECT head constructs the NATS subject Model");
        };
        assert_eq!(source.subject.as_str(), subject);
        assert_eq!(source.queue_group.as_str(), "Readers-EU");
        assert_eq!(source.instances.get(), 2);
    }

    #[test]
    fn subject_queue_group_and_quiesce_complete_from_the_subject_declaration() {
        let head = "CREATE NATS SUBJECT INGESTOR intake FROM nats_main SUBJECT ";
        assert_offers_exactly(head, &["nats_subject"]);
        assert_offers_exactly(&format!("{head}'orders.*' "), &["QUEUE GROUP"]);
        assert_offers_exactly(
            &format!("{head}'orders.*' QUEUE GROUP "),
            &["nats_queue_group"],
        );
        assert_offers_exactly(
            &format!("{head}'orders.*' QUEUE GROUP 'Readers' INSTANCES 2 MODE "),
            &["NO ACK"],
        );
        assert_offers_exactly(
            &format!("{head}'orders.*' QUEUE GROUP 'Readers' INSTANCES 2 MODE NO ACK ON QUIESCE "),
            &["BUFFER", "DROP"],
        );
    }

    #[rstest]
    #[case("")]
    #[case("orders..created")]
    #[case("orders.cre*")]
    #[case("orders.>.created")]
    #[case("orders created")]
    fn the_composed_grammar_applies_the_subscription_subject_rule(#[case] subject: &str) {
        let statement = statement(subject, "Readers");
        assert!(
            refused(&statement),
            "the subject rule excludes '{subject}': {statement}"
        );
    }

    #[test]
    fn the_composed_grammar_applies_the_subject_and_queue_group_byte_bounds() {
        let subject_at_bound = format!("{}.{}", "é".repeat(256), "x".repeat(511));
        let group_at_bound = "q".repeat(255);
        assert!(created_model(&statement(&subject_at_bound, &group_at_bound)).is_some());
        assert!(refused(&statement(
            &format!("{subject_at_bound}x"),
            "Readers"
        )));
        assert!(refused(&statement(
            "orders.*",
            &format!("{group_at_bound}q")
        )));
        assert!(refused(&statement("orders.*", "")));
        assert!(refused(&statement("orders.*", "work ers")));
    }

    const SEQUENTIAL: &str = "ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s";
    const PARALLEL: &str =
        "ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s";

    /// A complete `NATS STREAM` ingestor statement that reads `consumer` of `stream` in `mode`.
    fn stream_statement(stream: &str, consumer: &str, mode: &str) -> String {
        format!(
            "CREATE NATS STREAM INGESTOR intake FROM nats_main STREAM '{stream}' CONSUMER \
             '{consumer}' MODE {mode} ON QUIESCE SUSPEND DECODE USING event_codec TO events \
             INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The NATS STREAM source `statement` creates.
    fn stream_source(statement: &str) -> NatsStreamIngestor {
        let Some(Model::Ingestor(ingestor)) = created_model(statement) else {
            panic!("a stream ingestor statement is accepted by the composed grammar: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the NATS STREAM head names a transport");
        };
        let IngestorEntity::Nats(NatsIngestor::Stream(source)) = input.source else {
            panic!("the NATS STREAM head constructs the NATS stream Model");
        };
        source
    }

    #[test]
    fn stream_and_consumer_literals_reach_the_ingestor_model_unchanged() {
        let source = stream_source(&stream_statement("Orders-EU", "Readers_2026", SEQUENTIAL));
        assert_eq!(source.stream.as_str(), "Orders-EU");
        assert_eq!(source.consumer.as_str(), "Readers_2026");
        assert_eq!(source.quiesce, IngestQuiesceMode::Suspend);
        let NatsStreamIngestMode::AckSequential {
            timeout,
            retry_policy,
        } = source.mode
        else {
            panic!("ACK SEQUENTIAL constructs the sequential stream mode");
        };
        assert_eq!(timeout, "10s");
        assert_eq!(retry_policy.backoff, "100ms");
        assert_eq!(retry_policy.max_backoff, "1s");
    }

    #[test]
    fn a_parallel_stream_mode_keeps_its_window_and_batch_timeout() {
        let source = stream_source(&stream_statement("orders", "nervix", PARALLEL));
        let NatsStreamIngestMode::AckParallel {
            max,
            batch_timeout,
            timeout,
            ..
        } = source.mode
        else {
            panic!("ACK PARALLEL constructs the parallel stream mode");
        };
        assert_eq!(max.get(), 4);
        assert_eq!(batch_timeout, "50ms");
        assert_eq!(timeout, "10s");
    }

    #[test]
    fn stream_consumer_modes_and_quiesce_complete_from_the_stream_declaration() {
        let head = "CREATE NATS STREAM INGESTOR intake FROM nats_main STREAM ";
        assert_offers_exactly(head, &["nats_stream"]);
        assert_offers_exactly(&format!("{head}'orders' "), &["CONSUMER"]);
        assert_offers_exactly(&format!("{head}'orders' CONSUMER "), &["nats_consumer"]);
        assert_offers_exactly(
            &format!("{head}'orders' CONSUMER 'nervix' MODE "),
            &["ACK SEQUENTIAL", "ACK PARALLEL"],
        );
        assert_offers_exactly(
            &format!("{head}'orders' CONSUMER 'nervix' MODE {SEQUENTIAL} ON QUIESCE "),
            &["SUSPEND", "BUFFER", "DROP"],
        );
    }

    #[rstest]
    #[case::empty("")]
    #[case::subject_separator("orders.eu")]
    #[case::token_wildcard("orders*")]
    #[case::tail_wildcard("orders>")]
    #[case::path_separator("orders/eu")]
    #[case::whitespace("orders eu")]
    fn the_composed_grammar_applies_the_jetstream_name_rule(#[case] name: &str) {
        assert!(
            refused(&stream_statement(name, "nervix", SEQUENTIAL)),
            "the stream name rule excludes '{name}'"
        );
        assert!(
            refused(&stream_statement("orders", name, SEQUENTIAL)),
            "the consumer name rule excludes '{name}'"
        );
    }

    #[test]
    fn a_stream_ingestor_names_its_consumer_and_acknowledges_every_delivery() {
        assert!(refused(
            "CREATE NATS STREAM INGESTOR intake FROM nats_main STREAM 'orders' MODE ACK \
             SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND \
             DECODE USING event_codec TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE \
             ERROR LOG ON GENERAL ERROR LOG;"
        ));
        assert!(refused(&stream_statement("orders", "nervix", "NO ACK")));
        assert!(refused(
            "CREATE NATS STREAM INGESTOR intake FROM nats_main STREAM 'orders' CONSUMER 'nervix' \
             INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s ON \
             QUIESCE SUSPEND DECODE USING event_codec TO events INHERIT ALL UNBRANCHED FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        ));
    }

    #[test]
    fn a_stream_ingestor_alters_under_its_own_head() {
        let alteration = crate::statement::parse_statement(
            "ALTER NATS STREAM INGESTOR intake SET DECODE USING other_codec;",
        );
        assert!(alteration.is_ok(), "{alteration:?}");
    }
}
