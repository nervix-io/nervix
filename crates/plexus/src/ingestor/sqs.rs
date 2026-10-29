//! The SQS connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of an `SQS QUEUE` ingestor: its client, queue, instances, delivery mode
//!   and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the SQS vocabulary.
//! - **Must not know.** The SQS SDK or how a received message is deleted at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{SqsIngestMode, SqsQueueIngestor, SqsQueueName};

use super::delivery::{
    SequentialAck, ack_sequential, optional_instances, suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw},
};

/// `FROM <client> QUEUE '<queue>' [INSTANCES <n>] MODE ACK SEQUENTIAL … ON QUIESCE SUSPEND`.
pub(super) fn queue_body<'src>()
-> impl Parser<'src, &'src [Token], SqsQueueIngestor, extra::Err<ParseError<'src>>> + Clone {
    let mode = ack_sequential().map(
        |SequentialAck {
             timeout,
             retry_policy,
         }| SqsIngestMode::AckSequential {
            timeout,
            retry_policy,
        },
    );
    client_ref()
        .then_ignore(kw(Identifier::Queue))
        .then(external_name::<SqsQueueName>())
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(mode)
        .then(suspend_only_quiesce_clause())
        .map(
            |((((client, queue), instances), mode), quiesce)| SqsQueueIngestor {
                client,
                queue,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{IngestorEntity, IngestorInput, Model, SqsIngestor};
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// An `SQS QUEUE` ingestor statement reading `queue` in the delivery mode `mode`.
    fn statement(queue: &str, mode: &str) -> String {
        format!(
            "CREATE SQS QUEUE INGESTOR intake FROM sqs_main QUEUE '{queue}' INSTANCES 2 MODE \
             {mode} ON QUIESCE SUSPEND DECODE USING event_codec TO events INHERIT ALL UNBRANCHED \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    const SEQUENTIAL: &str = "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s";

    #[rstest]
    #[case("Orders-EU_1")]
    #[case("Orders-EU_1.fifo")]
    fn queue_literals_reach_the_ingestor_model_unchanged(#[case] queue: &str) {
        let statement = statement(queue, SEQUENTIAL);
        let Some(Model::Ingestor(ingestor)) = created_model(&statement) else {
            panic!("a literal queue is accepted by the composed grammar: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the SQS QUEUE head names a transport");
        };
        let IngestorEntity::Sqs(SqsIngestor::Queue(source)) = input.source else {
            panic!("the SQS QUEUE head constructs the SQS queue Model");
        };
        assert_eq!(source.queue.as_str(), queue);
        assert_eq!(source.instances.get(), 2);
    }

    #[test]
    fn queue_completion_modes_and_quiesce_follow_the_queue_declaration() {
        assert_offers_exactly(
            "CREATE SQS QUEUE INGESTOR intake FROM sqs_main QUEUE ",
            &["sqs_queue"],
        );
        assert_offers_exactly(
            "CREATE SQS QUEUE INGESTOR intake FROM sqs_main QUEUE 'Orders-EU_1' MODE ",
            &["ACK SEQUENTIAL"],
        );
        assert_offers_exactly(
            "CREATE SQS QUEUE INGESTOR intake FROM sqs_main QUEUE 'Orders-EU_1' MODE ACK \
             SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE ",
            &["SUSPEND"],
        );
    }

    #[rstest]
    #[case("NO ACK")]
    #[case("ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case("ACK PARALLEL MAX 11 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s")]
    fn the_composed_grammar_refuses_delivery_the_queue_does_not_declare(#[case] mode: &str) {
        let statement = statement("orders", mode);
        assert!(
            refused(&statement),
            "an SQS queue is received one acknowledged message at a time: {statement}"
        );
    }

    #[test]
    fn the_composed_grammar_applies_the_queue_rule() {
        let too_long = format!("{}.fifo", "q".repeat(76));
        for queue in [
            "",
            ".fifo",
            "orders.v2",
            "orders.FIFO",
            "Orders EU",
            too_long.as_str(),
        ] {
            let statement = statement(queue, SEQUENTIAL);
            assert!(
                refused(&statement),
                "queue '{queue}' is outside the SQS name rule"
            );
        }
    }
}
