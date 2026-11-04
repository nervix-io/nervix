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

/// `SET QUEUE '<name>'` or `SET MODE <mode>`: one part of a `RABBITMQ QUEUE` body.
pub(super) fn queue_change<'src>()
-> impl Parser<'src, &'src [Token], RabbitMqQueueEmitterChange, extra::Err<ParseError<'src>>> + Clone
{
    let locator = kw(Identifier::Queue)
        .ignore_then(external_name::<RabbitMqQueueName>())
        .map(RabbitMqQueueEmitterChange::Queue);
    let publishing = kw(Identifier::Mode)
        .ignore_then(mode())
        .map(RabbitMqQueueEmitterChange::Mode);
    kw(Identifier::Set)
        .ignore_then(choice((locator, publishing)))
        .boxed()
}

/// `SET EXCHANGE '<name>'`, `SET ROUTING KEY <expression>` or `SET MODE <mode>`: one part of a
/// `RABBITMQ EXCHANGE` body.
pub(super) fn exchange_change<'src>()
-> impl Parser<'src, &'src [Token], RabbitMqExchangeEmitterChange, extra::Err<ParseError<'src>>> + Clone
{
    let exchange = kw(Identifier::Exchange)
        .ignore_then(external_name::<RabbitMqExchangeName>())
        .map(RabbitMqExchangeEmitterChange::Exchange);
    let routing_key = kw_phrase2(Identifier::Routing, Identifier::Key)
        .ignore_then(embedded_expression("routing_key_expression"))
        .map(RabbitMqExchangeEmitterChange::RoutingKey);
    let publishing = kw(Identifier::Mode)
        .ignore_then(exchange_mode())
        .map(RabbitMqExchangeEmitterChange::Mode);
    kw(Identifier::Set)
        .ignore_then(choice((exchange, routing_key, publishing)))
        .boxed()
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

    #[rstest]
    #[case::queue("QUEUE", "QUEUE 'orders'")]
    #[case::exchange("EXCHANGE", "EXCHANGE 'events' ROUTING KEY 'orders'")]
    fn rabbitmq_envelope_is_a_canonical_semantic_model(
        #[case] entity: &str,
        #[case] destination: &str,
    ) {
        let text = format!(
            "CREATE RABBITMQ {entity} EMITTER outgoing FROM events TO broker {destination} MODE \
             NO ACK RETRY POLICY BACKOFF 1s MAX 3s ENCODE USING events_codec PERSISTENT MANDATORY \
             TIMESTAMP from_unix('second', 1700000000) PROPERTIES {{'content_type' = \
             'application/json', 'priority' = 7 AS U8}} INHERIT ALL FLUSH IMMEDIATE ON MESSAGE \
             ERROR LOG ON GENERAL ERROR LOG;"
        );
        let statement = parse_statement(&text).assured("the declared RabbitMQ envelope parses");
        let canonical = statement
            .to_canonical_plexus()
            .assured("the envelope has canonical Plexus");
        assert_eq!(
            parse_statement(&canonical).assured("the canonical statement reparses"),
            statement
        );
        let Statement::Create(create) = statement else {
            panic!("the statement creates an emitter");
        };
        let Model::Emitter(emitter) = *create.body else {
            panic!("the statement creates an emitter");
        };
        assert!(emitter.envelope.persistent);
        assert!(emitter.envelope.mandatory);
        assert!(emitter.envelope.timestamp.is_some());
        assert_eq!(emitter.envelope.properties.len(), 2);
        assert_eq!(emitter.envelope.properties[0].name, "content_type");
        assert_eq!(emitter.envelope.properties[1].name, "priority");
    }

    #[rstest]
    #[case("SET PERSISTENT")]
    #[case("DROP PERSISTENT")]
    #[case("SET TIMESTAMP from_unix('second', 1700000000)")]
    #[case("DROP TIMESTAMP")]
    #[case("SET PROPERTIES {'priority' = 9 AS U8}")]
    #[case("DROP PROPERTIES")]
    fn rabbitmq_targeted_envelope_alterations_round_trip(#[case] operation: &str) {
        let text = format!("ALTER RABBITMQ QUEUE EMITTER outgoing {operation};");
        let statement =
            parse_statement(&text).assured("the declared RabbitMQ envelope alteration parses");
        let canonical = statement
            .to_canonical_plexus()
            .assured("the alteration has canonical Plexus");
        assert_eq!(
            parse_statement(&canonical).assured("the canonical alteration reparses"),
            statement
        );
    }

    #[rstest]
    #[case("QUEUE", "QUEUE 'orders'")]
    #[case("EXCHANGE", "EXCHANGE 'events' ROUTING KEY 'orders'")]
    fn rabbitmq_targeted_alterations_preserve_the_rest_of_the_emitter(
        #[case] entity: &str,
        #[case] destination: &str,
    ) {
        let text = format!(
            "CREATE RABBITMQ {entity} EMITTER outgoing FROM events TO broker {destination} MODE \
             ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 1s MAX 3s ENCODE USING \
             events_codec MANDATORY INHERIT ALL BATCH MAX MESSAGES 10 MAX SIZE 64KiB FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        let Statement::Create(create) = parse_statement(&text).assured("the emitter parses") else {
            panic!("CREATE is an emitter");
        };
        let Model::Emitter(mut emitter) = *create.body else {
            panic!("CREATE is an emitter");
        };
        let original = emitter.clone();
        for operations in [
            "SET PERSISTENT, SET TIMESTAMP from_unix('second', 0), SET PROPERTIES {'content_type' \
             = 'application/json', 'priority' = 7 AS U8}",
            "SET PROPERTIES {'message_id' = 'next'}",
            "DROP PERSISTENT, DROP TIMESTAMP, DROP PROPERTIES",
        ] {
            let text = format!("ALTER RABBITMQ {entity} EMITTER outgoing {operations};");
            let Statement::AlterEmitter(alter) =
                parse_statement(&text).assured("the alterations parse")
            else {
                panic!("ALTER targets an emitter");
            };
            emitter
                .apply_alter(&alter)
                .assured("the declared operations apply atomically");
            let mut without_envelope = emitter.clone();
            without_envelope.envelope = original.envelope.clone();
            assert_eq!(
                without_envelope, original,
                "the destination, body, route and publishing policy survive"
            );
            if operations.starts_with("SET PERSISTENT") {
                assert!(emitter.envelope.persistent);
                assert!(emitter.envelope.timestamp.is_some());
                assert_eq!(emitter.envelope.properties.len(), 2);
            } else if operations.starts_with("SET PROPERTIES") {
                assert!(emitter.envelope.persistent);
                assert!(emitter.envelope.timestamp.is_some());
                assert_eq!(emitter.envelope.properties.len(), 1);
                assert_eq!(emitter.envelope.properties[0].name, "message_id");
            } else {
                assert_eq!(emitter, original);
            }
        }
    }

    #[rstest]
    #[case("PERSISTENT PERSISTENT")]
    #[case("TIMESTAMP from_unix('second', 0) PERSISTENT")]
    #[case("PROPERTIES {'content_type' = 'one', 'content_type' = 'two'}")]
    #[case("PROPERTIES {'unrecognized' = 'value'}")]
    #[case("PROPERTIES {}")]
    fn rabbitmq_envelopes_refuse_duplicate_out_of_order_or_undeclared_values(
        #[case] envelope: &str,
    ) {
        assert!(parse_statement(&exchange_statement("events", "NO ACK", envelope, "")).is_err());
        for operation in [
            "SET PROPERTIES {'unrecognized' = 'value'}",
            "SET PROPERTIES {'priority' = 1 AS U8, 'priority' = 2 AS U8}",
        ] {
            assert!(
                parse_statement(&format!(
                    "ALTER RABBITMQ QUEUE EMITTER outgoing {operation};"
                ))
                .is_err()
            );
        }
    }

    #[rstest]
    #[case("QUEUE")]
    #[case("EXCHANGE")]
    fn rabbitmq_envelope_alteration_completion_offers_only_declared_clauses(#[case] entity: &str) {
        let prefix = format!("ALTER RABBITMQ {entity} EMITTER outgoing ");
        let offered = crate::statement::suggest_statement(&prefix, prefix.len());
        for expected in [
            "SET PERSISTENT",
            "DROP PERSISTENT",
            "SET TIMESTAMP",
            "DROP TIMESTAMP",
            "SET PROPERTIES",
            "DROP PROPERTIES",
        ] {
            assert!(
                offered.iter().any(|item| item == expected),
                "missing {expected}: {offered:?}"
            );
        }
        for other in [
            "SET RETAIN",
            "DROP RETAIN",
            "SET DELAY",
            "DROP DELAY",
            "SET DELIVER AFTER",
            "DROP DELIVER AFTER",
        ] {
            assert!(
                !offered.iter().any(|item| item == other),
                "undeclared {other}: {offered:?}"
            );
        }
        let properties = format!("{prefix}SET PROPERTIES {{");
        assert_offers_exactly(
            &properties,
            &[
                "\"content_type\"",
                "\"content_encoding\"",
                "\"message_id\"",
                "\"correlation_id\"",
                "\"reply_to\"",
                "\"expiration\"",
                "\"priority\"",
                "\"type\"",
                "\"app_id\"",
                "\"user_id\"",
            ],
        );
    }

    #[rstest]
    #[case("QUEUE")]
    #[case("EXCHANGE")]
    fn property_alteration_completion_continues_with_unwritten_keys(#[case] entity: &str) {
        let prefix = format!(
            "ALTER RABBITMQ {entity} EMITTER outgoing SET PROPERTIES {{ 'app_id' = 'app', "
        );
        assert_offers_exactly(
            &prefix,
            &[
                "\"content_type\"",
                "\"content_encoding\"",
                "\"message_id\"",
                "\"correlation_id\"",
                "\"reply_to\"",
                "\"expiration\"",
                "\"priority\"",
                "\"type\"",
                "\"user_id\"",
                "}",
            ],
        );
    }
}
