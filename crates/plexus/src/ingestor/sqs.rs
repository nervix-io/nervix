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
use nervix_models::{SqsIngestMode, SqsQueueIngestor, SqsQueueName, SqsReceiveMax};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel_within, ack_sequential, optional_instances,
    suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, client_ref, external_name, kw, parallel_ack_window_of, parallel_max_count,
    },
};

/// The `n` of an SQS queue's `PARALLEL MAX <n>`: a window one receive can fill. A larger `n` is
/// refused where it is written, naming the most messages one receive returns.
fn receive_window<'src>()
-> impl Parser<'src, &'src [Token], SqsReceiveMax, extra::Err<ParseError<'src>>> + Clone {
    parallel_max_count()
        .try_map(|max, span| {
            SqsReceiveMax::try_from(max.get())
                .map_err(|error| Rich::custom(span, error.to_string()))
        })
        .boxed()
}

/// `FROM <client> QUEUE '<queue>' [INSTANCES <n>] MODE ACK SEQUENTIAL … | ACK PARALLEL MAX <n> …
/// ON QUIESCE SUSPEND`, where a parallel window takes at most the messages one receive returns.
pub(super) fn queue_body<'src>()
-> impl Parser<'src, &'src [Token], SqsQueueIngestor, extra::Err<ParseError<'src>>> + Clone {
    let sequential = ack_sequential().map(
        |SequentialAck {
             timeout,
             retry_policy,
         }| SqsIngestMode::AckSequential {
            timeout,
            retry_policy,
        },
    );
    let parallel = ack_parallel_within(parallel_ack_window_of(receive_window())).map(
        |ParallelAck {
             max,
             batch_timeout,
             timeout,
             retry_policy,
         }| SqsIngestMode::AckParallel {
            max,
            batch_timeout,
            timeout,
            retry_policy,
        },
    );
    let mode = choice((parallel, sequential));
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
    use nervix_models::{
        IngestorEntity, IngestorInput, Model, RetryPolicy, SqsIngestMode, SqsIngestor,
        SqsQueueIngestor, SqsReceiveMax,
    };
    use rstest::rstest;

    use crate::{
        conformance_support::{assert_offers_exactly, created_model, refused},
        parser_support::ParseFromSourceError,
        statement::parse_statement,
    };

    /// An `SQS QUEUE` ingestor statement reading `queue` in the delivery mode `mode`.
    fn statement(queue: &str, mode: &str) -> String {
        format!(
            "CREATE SQS QUEUE INGESTOR intake FROM sqs_main QUEUE '{queue}' INSTANCES 2 MODE \
             {mode} ON QUIESCE SUSPEND DECODE USING event_codec TO events INHERIT ALL UNBRANCHED \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    const SEQUENTIAL: &str = "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s";

    /// The SQS queue Model the composed grammar builds from `statement`.
    fn queue_source(statement: &str) -> SqsQueueIngestor {
        let Some(Model::Ingestor(ingestor)) = created_model(statement) else {
            panic!("the composed grammar accepts {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the SQS QUEUE head names a transport");
        };
        let IngestorEntity::Sqs(SqsIngestor::Queue(source)) = input.source else {
            panic!("the SQS QUEUE head constructs the SQS queue Model");
        };
        source
    }

    #[rstest]
    #[case("Orders-EU_1")]
    #[case("Orders-EU_1.fifo")]
    fn queue_literals_reach_the_ingestor_model_unchanged(#[case] queue: &str) {
        let statement = statement(queue, SEQUENTIAL);
        let source = queue_source(&statement);
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
            &["ACK SEQUENTIAL", "ACK PARALLEL"],
        );
        // A parallel window's batch timeout follows its count; the sequential mode's ACK TIMEOUT
        // does not leak in here.
        assert_offers_exactly(
            "CREATE SQS QUEUE INGESTOR intake FROM sqs_main QUEUE 'Orders-EU_1' MODE ACK PARALLEL \
             MAX 10 ",
            &["BATCH"],
        );
        assert_offers_exactly(
            "CREATE SQS QUEUE INGESTOR intake FROM sqs_main QUEUE 'Orders-EU_1' MODE ACK PARALLEL \
             MAX 10 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE ",
            &["SUSPEND"],
        );
        assert_offers_exactly(
            "CREATE SQS QUEUE INGESTOR intake FROM sqs_main QUEUE 'Orders-EU_1' MODE ACK \
             SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE ",
            &["SUSPEND"],
        );
    }

    #[rstest]
    #[case("NO ACK")]
    #[case("ACK MAX 2 ACK TIMEOUT 5s")]
    #[case("ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s")]
    fn the_composed_grammar_refuses_delivery_the_queue_does_not_declare(#[case] mode: &str) {
        let statement = statement("orders", mode);
        assert!(
            refused(&statement),
            "an SQS queue is received only in an acknowledged mode with its declared clauses: \
             {statement}"
        );
    }

    #[rstest]
    #[case(1)]
    #[case(7)]
    #[case(10)]
    fn a_parallel_window_up_to_the_receive_maximum_reaches_the_model(#[case] max: u64) {
        let mode = format!(
            "ACK PARALLEL MAX {max} BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s \
             MAX 30s"
        );
        let source = queue_source(&statement("orders", &mode));
        assert_eq!(
            source.mode,
            SqsIngestMode::AckParallel {
                max: SqsReceiveMax::try_from(max).expect("the window is within the maximum"),
                batch_timeout: "100ms".to_string(),
                timeout: "5s".to_string(),
                retry_policy: RetryPolicy {
                    backoff: "1s".to_string(),
                    max_backoff: "30s".to_string(),
                },
            }
        );
    }

    #[test]
    fn a_parallel_window_beyond_the_receive_maximum_is_refused_with_its_bound() {
        let statement = statement(
            "orders",
            "ACK PARALLEL MAX 11 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX \
             30s",
        );
        let error = parse_statement(&statement).expect_err("MAX 11 exceeds the receive maximum");
        let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
            panic!("expected a parse error, got {error:?}");
        };
        let expected = "SQS ACK PARALLEL MAX must be between 1 and 10, found 11";
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message == expected),
            "{expected} in {diagnostics:?}"
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
