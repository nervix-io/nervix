//! RABBITMQ emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of RABBITMQ emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], RabbitMqQueuePublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    broker_mode!(RabbitMqQueuePublishingMode)
}

pub(super) fn queue_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<RabbitMqQueueEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Queue))
            .then(external_name::<RabbitMqQueueName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, queue), mode)| RabbitMqQueueEmitter {
                client,
                queue,
                mode,
            }),
    )
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{AckWindow, EmitterEntity, Model, RabbitMqEmitter, Statement};
    use rstest::rstest;

    use super::RabbitMqQueuePublishingMode;
    use crate::{conformance_support::assert_offers_exactly, statement::parse_statement};

    fn queue_statement(queue: &str, mode: &str, batch: &str) -> String {
        format!(
            "CREATE RABBITMQ QUEUE EMITTER outgoing FROM events TO broker QUEUE '{queue}' MODE \
             {mode} RETRY POLICY BACKOFF 200ms MAX 3s ENCODE USING events_codec INHERIT ALL \
             {batch} FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    #[rstest]
    #[case::no_ack("Orders.Mixed-01".to_owned(), "NO ACK", "", None)]
    #[case::sequential("Orders.Mixed-01".to_owned(), "ACK SEQUENTIAL ACK TIMEOUT 7s", "", Some(AckWindow::Sequential))]
    #[case::parallel_batch("Orders.Mixed-01".to_owned(), "ACK PARALLEL MAX 2 ACK TIMEOUT 7s", "BATCH MAX MESSAGES 10 MAX SIZE 64KiB", Some(AckWindow::Parallel { max: nonzero_ext::nonzero!(2_u64) }))]
    #[case::ascii_limit("Q".repeat(255), "NO ACK", "", None)]
    #[case::utf8_limit(format!("{}Z", "é".repeat(127)), "NO ACK", "", None)]
    fn queue_emitter_preserves_its_locator_policy_and_canonical_model(
        #[case] queue: String,
        #[case] mode: &str,
        #[case] batch: &str,
        #[case] window: Option<AckWindow>,
    ) {
        let text = queue_statement(&queue, mode, batch);
        let statement = parse_statement(&text).assured("the current RabbitMQ declaration parses");
        let canonical = statement
            .to_canonical_plexus()
            .assured("the complete statement has a canonical Plexus representation");
        assert_eq!(
            parse_statement(&canonical).assured("canonical Plexus reparses"),
            statement
        );
        let Statement::Create(create) = statement else {
            panic!("the statement creates an emitter");
        };
        let Model::Emitter(emitter) = *create.body else {
            panic!("the statement creates an emitter");
        };
        assert_eq!(emitter.batch.is_some(), !batch.is_empty());
        if let Some(policy) = emitter.batch {
            assert_eq!(policy.to_string(), "MAX MESSAGES 10 MAX SIZE 64KiB");
        }
        let EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(sink)) = *emitter.connector else {
            panic!("the composed head selects the RabbitMQ queue emitter");
        };
        assert_eq!(sink.queue.as_str(), queue);
        assert_eq!(sink.client.as_str(), "broker");
        match (sink.mode, window) {
            (RabbitMqQueuePublishingMode::NoAck { retry_policy }, None) => {
                assert_eq!(retry_policy.backoff, "200ms");
                assert_eq!(retry_policy.max_backoff, "3s");
            }
            (
                RabbitMqQueuePublishingMode::Ack {
                    window,
                    ack_timeout,
                    retry_policy,
                },
                Some(expected),
            ) => {
                assert_eq!(window, expected);
                assert_eq!(ack_timeout, "7s");
                assert_eq!(retry_policy.backoff, "200ms");
                assert_eq!(retry_policy.max_backoff, "3s");
            }
            _ => panic!("the complete publishing policy must match the statement"),
        }
    }

    #[rstest]
    #[case::empty(String::new())]
    #[case::ascii_over_limit("Q".repeat(256))]
    #[case::utf8_over_limit("é".repeat(128))]
    fn queue_emitter_refuses_names_outside_the_byte_contract(#[case] queue: String) {
        assert!(parse_statement(&queue_statement(&queue, "NO ACK", "")).is_err());
    }

    #[test]
    fn queue_emitter_completion_uses_its_external_name_contract() {
        assert_offers_exactly(
            "CREATE RABBITMQ QUEUE EMITTER outgoing FROM events TO broker QUEUE ",
            &["rabbitmq_queue"],
        );
        assert_offers_exactly(
            "CREATE RABBITMQ QUEUE EMITTER outgoing FROM events TO broker QUEUE 'Orders' ",
            &["MODE"],
        );
    }
}
