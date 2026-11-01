//! RABBITMQ emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination bodies and typed publication grammar of RABBITMQ QUEUE and RABBITMQ
//!   EXCHANGE emitters.
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

pub(super) fn exchange_mode<'src>()
-> impl Parser<'src, &'src [Token], RabbitMqExchangePublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    broker_mode!(RabbitMqExchangePublishingMode)
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

/// `EXCHANGE '<name>' ROUTING KEY <expression>`: an exchange the broker routes every record of
/// the emitter through, by the key the expression computes for it.
pub(super) fn exchange_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<RabbitMqExchangeEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Exchange))
            .then(external_name::<RabbitMqExchangeName>())
            .then_ignore(kw_phrase2(Identifier::Routing, Identifier::Key))
            .then(embedded_expression("routing_key_expression"))
            .then_ignore(kw(Identifier::Mode))
            .then(exchange_mode())
            .map(
                |(((client, exchange), routing_key), mode)| RabbitMqExchangeEmitter {
                    client,
                    exchange,
                    routing_key,
                    mode,
                },
            ),
    )
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{AckWindow, EmitterEntity, Model, RabbitMqEmitter, Statement};
    use rstest::rstest;

    use super::{RabbitMqExchangePublishingMode, RabbitMqQueuePublishingMode};
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

    fn exchange_statement(exchange: &str, mode: &str, envelope: &str, batch: &str) -> String {
        format!(
            "CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events TO broker EXCHANGE \
             '{exchange}' ROUTING KEY concat('orders.', input.category) MODE {mode} RETRY POLICY \
             BACKOFF 200ms MAX 3s ENCODE USING events_codec {envelope} INHERIT ALL {batch} FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    #[rstest]
    #[case::no_ack("Events.Mixed-01".to_owned(), "NO ACK", "", "", None)]
    #[case::mandatory("amq.topic".to_owned(), "ACK SEQUENTIAL ACK TIMEOUT 7s", "MANDATORY", "", Some(AckWindow::Sequential))]
    #[case::parallel_batch("Events".to_owned(), "ACK PARALLEL MAX 2 ACK TIMEOUT 7s", "MANDATORY", "BATCH MAX MESSAGES 10 MAX SIZE 64KiB", Some(AckWindow::Parallel { max: nonzero_ext::nonzero!(2_u64) }))]
    #[case::ascii_limit("X".repeat(255), "NO ACK", "", "", None)]
    #[case::utf8_limit(format!("{}Z", "é".repeat(127)), "NO ACK", "", "", None)]
    fn exchange_emitter_preserves_its_exchange_routing_key_and_canonical_model(
        #[case] exchange: String,
        #[case] mode: &str,
        #[case] envelope: &str,
        #[case] batch: &str,
        #[case] window: Option<AckWindow>,
    ) {
        let text = exchange_statement(&exchange, mode, envelope, batch);
        let statement = parse_statement(&text).assured("the RabbitMQ exchange declaration parses");
        let canonical = statement
            .to_canonical_plexus()
            .assured("the complete statement has a canonical Plexus representation");
        assert_eq!(
            parse_statement(&canonical).assured("canonical Plexus reparses"),
            statement
        );
        assert!(
            canonical.contains(&format!(
                "EXCHANGE '{exchange}' ROUTING KEY concat('orders.', input.category)"
            )),
            "{canonical}"
        );
        let Statement::Create(create) = statement else {
            panic!("the statement creates an emitter");
        };
        let Model::Emitter(emitter) = *create.body else {
            panic!("the statement creates an emitter");
        };
        assert_eq!(emitter.envelope.mandatory, !envelope.is_empty());
        assert_eq!(emitter.batch.is_some(), !batch.is_empty());
        let EmitterEntity::RabbitMq(RabbitMqEmitter::Exchange(sink)) = *emitter.connector else {
            panic!("the composed head selects the RabbitMQ exchange emitter");
        };
        assert_eq!(sink.exchange.as_str(), exchange);
        assert_eq!(sink.client.as_str(), "broker");
        match (sink.mode, window) {
            (RabbitMqExchangePublishingMode::NoAck { retry_policy }, None) => {
                assert_eq!(retry_policy.backoff, "200ms");
                assert_eq!(retry_policy.max_backoff, "3s");
            }
            (
                RabbitMqExchangePublishingMode::Ack {
                    window,
                    ack_timeout,
                    ..
                },
                Some(expected),
            ) => {
                assert_eq!(window, expected);
                assert_eq!(ack_timeout, "7s");
            }
            _ => panic!("the complete publishing policy must match the statement"),
        }
    }

    #[rstest]
    #[case::default_exchange(exchange_statement("", "NO ACK", "", ""))]
    #[case::ascii_over_limit(exchange_statement(&"X".repeat(256), "NO ACK", "", ""))]
    #[case::utf8_over_limit(exchange_statement(&"é".repeat(128), "NO ACK", "", ""))]
    #[case::without_routing_key(
        "CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events TO broker EXCHANGE 'events' MODE NO \
         ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec INHERIT ALL FLUSH IMMEDIATE \
         ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            .to_owned()
    )]
    #[case::undeclared_envelope_clause(exchange_statement("events", "NO ACK", "PERSISTENT", ""))]
    #[case::mandatory_before_the_payload(
        "CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events TO broker EXCHANGE 'events' ROUTING \
         KEY input.category MANDATORY MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING \
         events_codec INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            .to_owned()
    )]
    fn exchange_emitter_refuses_what_its_declaration_does_not_admit(#[case] text: String) {
        assert!(parse_statement(&text).is_err(), "{text}");
    }

    #[test]
    fn queue_emitter_writes_mandatory_after_its_payload() {
        let text = queue_statement("orders", "NO ACK", "").replace(
            "ENCODE USING events_codec INHERIT ALL",
            "ENCODE USING events_codec MANDATORY INHERIT ALL",
        );
        let statement = parse_statement(&text).assured("MANDATORY follows the payload");
        let Statement::Create(create) = statement else {
            panic!("the statement creates an emitter");
        };
        let Model::Emitter(emitter) = *create.body else {
            panic!("the statement creates an emitter");
        };
        assert!(emitter.envelope.mandatory);
    }

    #[test]
    fn exchange_emitter_completion_follows_its_declaration() {
        assert_offers_exactly(
            "CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events TO broker EXCHANGE ",
            &["rabbitmq_exchange"],
        );
        assert_offers_exactly(
            "CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events TO broker EXCHANGE 'events' ",
            &["ROUTING KEY"],
        );
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
