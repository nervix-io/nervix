//! SQS QUEUE emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of SQS QUEUE emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn sqs_fifo_group_expression<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::Expression, extra::Err<ParseError<'src>>> + Clone
{
    embedded_expression("fifo_group_expression")
}
pub(super) fn sqs_fifo_group_clause<'src>()
-> impl Parser<'src, &'src [Token], SqsFifoGroup, extra::Err<ParseError<'src>>> + Clone {
    // The expression is tried first: an embedded rejection raised at the token where `FROM BRANCH`
    // had already failed would take that failure's span.
    kw_phrase2(Identifier::Fifo, Identifier::Group)
        .ignore_then(choice((
            sqs_fifo_group_expression().map(SqsFifoGroup::Expression),
            kw_phrase2(Identifier::From, Identifier::Branch).to(SqsFifoGroup::FromBranch),
        )))
        .boxed()
}
pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], SqsQueuePublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Single)
            .ignore_then(retry_policy())
            .map(|retry_policy| SqsQueuePublishingMode::Single { retry_policy }),
        kw(Identifier::Batch)
            .ignore_then(retry_policy())
            .map(|retry_policy| SqsQueuePublishingMode::Batch { retry_policy }),
    ))
    .boxed()
}
pub(super) fn queue_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<SqsQueueEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Queue))
            .then(external_name::<SqsQueueName>())
            .then(sqs_fifo_group_clause().or_not())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|(((client, queue), fifo_group), mode)| SqsQueueEmitter {
                client,
                queue,
                fifo_group,
                mode,
            }),
    )
}

#[cfg(test)]
mod tests {
    use nervix_models::{EmitterEntity, Model, SqsEmitter, SqsFifoGroup, SqsQueuePublishingMode};
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// An `SQS QUEUE` emitter statement sending to `queue` with `options` in the publishing mode
    /// `mode`.
    fn statement(queue: &str, options: &str, mode: &str) -> String {
        format!(
            "CREATE SQS QUEUE EMITTER publishing FROM events TO sqs_main QUEUE '{queue}' \
             {options} MODE {mode} ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The SQS emitter Model `statement` creates.
    fn created_emitter(statement: &str) -> nervix_models::SqsQueueEmitter {
        let Some(Model::Emitter(emitter)) = created_model(statement) else {
            panic!("a literal queue is accepted by the composed grammar: {statement}");
        };
        let EmitterEntity::Sqs(SqsEmitter::Queue(destination)) = *emitter.connector else {
            panic!("the SQS QUEUE head constructs the SQS queue Model");
        };
        destination
    }

    #[test]
    fn a_standard_queue_literal_reaches_the_emitter_model_unchanged() {
        let destination = created_emitter(&statement(
            "Orders-EU_1",
            "",
            "SINGLE RETRY POLICY BACKOFF 250ms MAX 30s",
        ));

        assert_eq!(destination.queue.as_str(), "Orders-EU_1");
        assert_eq!(destination.fifo_group, None);
        assert!(matches!(
            destination.mode,
            SqsQueuePublishingMode::Single { .. }
        ));
    }

    #[test]
    fn a_fifo_queue_literal_reaches_the_emitter_model_with_its_group() {
        let destination = created_emitter(&statement(
            "Orders-EU_1.fifo",
            "FIFO GROUP FROM BRANCH",
            "BATCH RETRY POLICY BACKOFF 250ms MAX 30s",
        ));

        assert_eq!(destination.queue.as_str(), "Orders-EU_1.fifo");
        assert_eq!(destination.fifo_group, Some(SqsFifoGroup::FromBranch));
        assert!(matches!(
            destination.mode,
            SqsQueuePublishingMode::Batch { .. }
        ));
    }

    #[test]
    fn queue_completion_and_modes_follow_the_queue_declaration() {
        assert_offers_exactly(
            "CREATE SQS QUEUE EMITTER publishing FROM events TO sqs_main QUEUE ",
            &["sqs_queue"],
        );
        assert_offers_exactly(
            "CREATE SQS QUEUE EMITTER publishing FROM events TO sqs_main QUEUE 'Orders-EU_1' MODE ",
            &["SINGLE", "BATCH"],
        );
    }

    #[rstest]
    #[case("NO ACK RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case("ACK RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case("ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s")]
    fn the_composed_grammar_refuses_publishing_the_queue_does_not_declare(#[case] mode: &str) {
        let statement = statement("orders", "", mode);
        assert!(
            refused(&statement),
            "an SQS queue emitter publishes SINGLE or BATCH: {statement}"
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
            let statement = statement(queue, "", "SINGLE RETRY POLICY BACKOFF 250ms MAX 30s");
            assert!(
                refused(&statement),
                "queue '{queue}' is outside the SQS name rule"
            );
        }
    }
}
