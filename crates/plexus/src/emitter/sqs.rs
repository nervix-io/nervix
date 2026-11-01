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
    request_mode!(SqsQueuePublishingMode)
}
/// `SEND SINGLE | BATCH`: the request shape the emitter sends its messages in.
fn send_clause<'src>()
-> impl Parser<'src, &'src [Token], SqsSend, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Send)
        .ignore_then(choice((
            kw(Identifier::Single).to(SqsSend::Single),
            kw(Identifier::Batch).to(SqsSend::Batch),
        )))
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
            .then(send_clause())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(
                |((((client, queue), fifo_group), send), mode)| SqsQueueEmitter {
                    client,
                    queue,
                    fifo_group,
                    send,
                    mode,
                },
            ),
    )
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        CreateEmitter, EmitterEntity, Model, SqsEmitter, SqsFifoGroup, SqsQueuePublishingMode,
        SqsSend,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// An `SQS QUEUE` emitter statement sending to `queue` with `options`, the request shape and
    /// publishing mode `publishing`, and the envelope clauses `envelope`.
    fn statement(queue: &str, options: &str, publishing: &str, envelope: &str) -> String {
        format!(
            "CREATE SQS QUEUE EMITTER publishing FROM events TO sqs_main QUEUE '{queue}' \
             {options} {publishing} ENCODE USING event_codec {envelope} INHERIT ALL FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The emitter Model `statement` creates.
    fn created(statement: &str) -> CreateEmitter {
        let Some(Model::Emitter(emitter)) = created_model(statement) else {
            panic!("the composed grammar accepts the statement: {statement}");
        };
        emitter
    }

    /// The SQS emitter entity `statement` creates.
    fn created_emitter(statement: &str) -> nervix_models::SqsQueueEmitter {
        let EmitterEntity::Sqs(SqsEmitter::Queue(destination)) = *created(statement).connector
        else {
            panic!("the SQS QUEUE head constructs the SQS queue Model");
        };
        destination
    }

    #[test]
    fn a_standard_queue_literal_reaches_the_emitter_model_with_its_request_shape() {
        let destination = created_emitter(&statement(
            "Orders-EU_1",
            "",
            "SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "",
        ));

        assert_eq!(destination.queue.as_str(), "Orders-EU_1");
        assert_eq!(destination.fifo_group, None);
        assert_eq!(destination.send, SqsSend::Single);
        let SqsQueuePublishingMode::Ack { retry_policy } = destination.mode;
        assert_eq!(retry_policy.backoff, "250ms");
        assert_eq!(retry_policy.max_backoff, "30s");
    }

    #[test]
    fn a_fifo_queue_literal_reaches_the_emitter_model_with_its_group_and_request_shape() {
        let destination = created_emitter(&statement(
            "Orders-EU_1.fifo",
            "FIFO GROUP FROM BRANCH",
            "SEND BATCH MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "",
        ));

        assert_eq!(destination.queue.as_str(), "Orders-EU_1.fifo");
        assert_eq!(destination.fifo_group, Some(SqsFifoGroup::FromBranch));
        assert_eq!(destination.send, SqsSend::Batch);
    }

    #[test]
    fn the_envelope_writes_a_delay_or_a_deduplication_id_after_the_payload() {
        let delayed = created(&statement(
            "orders",
            "",
            "SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "DELAY 5s",
        ));
        assert_eq!(delayed.envelope.delay.as_deref(), Some("5s"));
        assert_eq!(delayed.envelope.deduplication_id, None);

        let deduplicated = created(&statement(
            "orders.fifo",
            "FIFO GROUP input.tenant",
            "SEND BATCH MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "DEDUPLICATION ID input.order_id",
        ));
        assert_eq!(deduplicated.envelope.delay, None);
        assert!(deduplicated.envelope.deduplication_id.is_some());
    }

    #[test]
    fn queue_completion_options_and_modes_follow_the_queue_declaration() {
        let head = "CREATE SQS QUEUE EMITTER publishing FROM events TO sqs_main QUEUE ";
        assert_offers_exactly(head, &["sqs_queue"]);
        assert_offers_exactly(&format!("{head}'Orders-EU_1' "), &["FIFO GROUP", "SEND"]);
        assert_offers_exactly(&format!("{head}'Orders-EU_1' SEND "), &["SINGLE", "BATCH"]);
        assert_offers_exactly(&format!("{head}'Orders-EU_1' SEND SINGLE "), &["MODE"]);
        assert_offers_exactly(&format!("{head}'Orders-EU_1' SEND BATCH MODE "), &["ACK"]);
    }

    #[rstest]
    #[case("MODE ACK RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case("SEND PARALLEL MODE ACK RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case("SEND SINGLE MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case("SEND BATCH MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s")]
    fn the_composed_grammar_refuses_request_shapes_and_modes_the_queue_does_not_declare(
        #[case] publishing: &str,
    ) {
        let statement = statement("orders", "", publishing, "");
        assert!(
            refused(&statement),
            "an SQS queue emitter states SEND SINGLE or SEND BATCH, then MODE ACK: {statement}"
        );
    }

    #[rstest]
    #[case("MESSAGE ID input.id")]
    #[case("TIMESTAMP input.at")]
    #[case("DEDUPLICATION ID input.id DELAY 5s")]
    fn the_composed_grammar_refuses_envelope_clauses_the_queue_does_not_declare(
        #[case] envelope: &str,
    ) {
        let statement = statement(
            "orders",
            "",
            "SEND SINGLE MODE ACK RETRY POLICY BACKOFF 1s MAX 30s",
            envelope,
        );
        assert!(
            refused(&statement),
            "an SQS queue emitter writes DELAY, then DEDUPLICATION ID: {statement}"
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
            let statement = statement(
                queue,
                "",
                "SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
                "",
            );
            assert!(
                refused(&statement),
                "queue '{queue}' is outside the SQS name rule"
            );
        }
    }
}
