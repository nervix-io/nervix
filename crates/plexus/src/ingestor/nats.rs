//! The NATS connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `NATS SUBJECT` ingestor: its client, subject, queue group, instances,
//!   delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the NATS vocabulary.
//! - **Must not know.** The NATS driver or how a queue subscription is held at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{NatsIngestMode, NatsQueueGroupName, NatsSubjectIngestor, NatsSubjectName};

use super::delivery::{instance_count, no_ack, push_quiesce_clause};
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

#[cfg(test)]
mod tests {
    use nervix_models::{IngestorEntity, IngestorInput, Model, NatsIngestor};
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
}
