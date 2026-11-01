//! Ingestor statement grammar.
//!
//! Layer: language.
//! - **Owns.** The heads that name an ingestor's connector and entity, the statement tail every
//!   ingestor shares — timestamp, filter, routes and error policy — and the alterations every
//!   entity accepts.
//! - **Depends on.** Shared Plexus grammar primitives, the connector grammar modules that own each
//!   entity's body, and the connector vocabulary.
//! - **Must not know.** Connector clients, runtime polling or broker state.
//!
//! Each `<CONNECTOR> <ENTITY>` pair owns its body in its connector's module, and its head is the
//! phrase the vocabulary declares for it, so a statement can never hold the body of another pair.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

mod client;
mod delivery;
mod endpoint;
mod http;
mod kafka;
mod mqtt;
mod nats;
mod prometheus;
mod pulsar;
mod rabbitmq;
mod redis;
mod sqs;
mod syslog;
mod websockets;
mod zeromq;

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    AlterIngestor, AlterIngestorOperation, AlterTransportIngestorOperation, ClientProducerIngestor,
    CreateIngestor, CreateStatement, GeneralErrorPolicy, IngestTimestampSource, IngestorAlteration,
    IngestorEntity, IngestorEntityKind, IngestorEntityModel, IngestorInput, IngestorName,
    ShowIngestors, TransportIngestorInput,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, alter_ingestor_route_body,
        alter_op_separator, boxed_choice, codec_ref, field_ref, filter_where_clause,
        flushed_ingestor_outputs, general_error_policy, if_not_exists_clause, ingestor_name,
        into_parse_error, kw, kw_phrase2, lex_input, relay_ref, suggest_from, tok,
        where_expression,
    },
};

/// `SHOW INGESTORS`: the ingestors of the selected domain.
pub fn show_ingestors_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowIngestors, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Ingestors))
        .to(ShowIngestors)
        .then_ignore(tok(Token::Semicolon).or_not())
}

/// `<CONNECTOR> <ENTITY> INGESTOR`: the head that names the pair an ingestor reads, one composed
/// phrase so completion offers it as one item.
fn head<'src>(
    kind: IngestorEntityKind,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    crate::parser_support::connector_entity_head(
        kind.connector(),
        kind.entity_keyword(),
        Identifier::Ingestor,
    )
}

/// The connector and entity a statement's head names, for completion that reads the head before
/// the rest of the statement is written.
pub(crate) fn head_kind<'src>()
-> impl Parser<'src, &'src [Token], IngestorEntityKind, extra::Err<ParseError<'src>>> + Clone {
    let mut heads = Vec::new();
    for kind in IngestorEntityKind::all() {
        heads.push(head(kind).to(kind).boxed());
    }
    choice(heads).boxed()
}

/// `TIMESTAMP NOW`, `TIMESTAMP AT <field>` for a payload field, or `TIMESTAMP AT metadata.<field>`
/// for a metadata field. A payload field may itself be named `metadata`: only the dot makes the
/// metadata form.
fn timestamp_source<'src>()
-> impl Parser<'src, &'src [Token], IngestTimestampSource, extra::Err<ParseError<'src>>> + Clone {
    let metadata_field = kw(Identifier::Metadata)
        .ignore_then(tok(Token::Dot))
        .ignore_then(field_ref())
        .map(IngestTimestampSource::AtMetadata);
    let payload_field = field_ref().map(IngestTimestampSource::At);
    let at_field = kw(Identifier::At).ignore_then(choice((metadata_field, payload_field)));

    kw(Identifier::Timestamp).ignore_then(choice((
        kw(Identifier::Now).to(IngestTimestampSource::Now),
        at_field,
    )))
}

/// The name an ingestor's head declares and the input its body writes.
struct DeclaredInput {
    name: IngestorName,
    input: IngestorInput,
}

/// `<CONNECTOR> <ENTITY> INGESTOR <name> FROM <body> DECODE USING <codec>`.
fn transport_input<'src, S, P>(
    body: P,
) -> impl Parser<'src, &'src [Token], DeclaredInput, extra::Err<ParseError<'src>>> + Clone
where
    S: IngestorEntityModel + Into<IngestorEntity> + 'src,
    P: Parser<'src, &'src [Token], S, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    head(S::KIND)
        .ignore_then(ingestor_name())
        .then_ignore(kw(Identifier::From))
        .then(body)
        .then_ignore(kw_phrase2(Identifier::Decode, Identifier::Using))
        .then(codec_ref())
        .map(|((name, source), codec)| DeclaredInput {
            name,
            input: IngestorInput::Transport(TransportIngestorInput {
                source: source.into(),
                codec,
            }),
        })
        .boxed()
}

/// `CLIENT PRODUCER INGESTOR <name> FROM <body>`, which decodes nothing and so names no codec.
fn client_input<'src>()
-> impl Parser<'src, &'src [Token], DeclaredInput, extra::Err<ParseError<'src>>> + Clone {
    head(ClientProducerIngestor::KIND)
        .ignore_then(ingestor_name())
        .then_ignore(kw(Identifier::From))
        .then(client::producer_body())
        .map(|(name, source)| DeclaredInput {
            name,
            input: IngestorInput::Client(source),
        })
        .boxed()
}

/// Every connector and entity an ingestor reads, each with the body its own grammar owns.
fn declared_input<'src>()
-> impl Parser<'src, &'src [Token], DeclaredInput, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        transport_input(http::request_body()),
        transport_input(http::endpoint_body()),
        transport_input(kafka::topic_body()),
        transport_input(pulsar::topic_body()),
        transport_input(mqtt::topic_body()),
        transport_input(nats::subject_body()),
        transport_input(rabbitmq::queue_body()),
        transport_input(redis::pubsub_body()),
        transport_input(prometheus::query_body()),
        transport_input(zeromq::pull_body()),
        transport_input(sqs::queue_body()),
        transport_input(websockets::connection_body()),
        transport_input(websockets::endpoint_body()),
        transport_input(syslog::listener_body()),
        client_input(),
    )
}

pub fn create_ingestor_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateIngestor>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(declared_input())
        .boxed()
        .then(timestamp_source().or_not())
        .then(filter_where_clause().or_not())
        .boxed()
        .then(flushed_ingestor_outputs())
        .then(general_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                ((((if_not_exists, declared), timestamp_source), filter_where), output_routes),
                general_error_policy,
            )| {
                CreateStatement::new(
                    CreateIngestor {
                        name: declared.name,
                        output_routes,
                        input: declared.input,
                        timestamp_source,
                        general_error_policy,
                        filter_where,
                    },
                    if_not_exists,
                )
            },
        )
        .boxed()
}

/// The operations every ingestor accepts whatever it reads.
fn shared_operation<'src, S>()
-> impl Parser<'src, &'src [Token], AlterIngestorOperation<S>, extra::Err<ParseError<'src>>> + Clone
where
    S: Clone + 'src,
{
    let set_quiesce = kw(Identifier::Set)
        .ignore_then(kw(Identifier::Quiesce))
        .ignore_then(delivery::alter_quiesce_body())
        .map(|quiesce| AlterIngestorOperation::SetQuiesce { quiesce });
    let set_timestamp = kw(Identifier::Set)
        .ignore_then(timestamp_source())
        .map(|source| AlterIngestorOperation::SetTimestamp { source });
    let drop_timestamp = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Timestamp))
        .to(AlterIngestorOperation::DropTimestamp);
    let set_filter = kw(Identifier::Set)
        .ignore_then(kw(Identifier::Filter))
        .ignore_then(where_expression())
        .map(|where_clause| AlterIngestorOperation::SetFilterWhere { where_clause });
    let drop_filter = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Filter))
        .then_ignore(kw(Identifier::Where))
        .to(AlterIngestorOperation::DropFilterWhere);
    let add_route = kw(Identifier::Add)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(alter_ingestor_route_body())
        .map(|route| AlterIngestorOperation::AddRoute { route });
    let drop_route = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(kw(Identifier::To))
        .ignore_then(relay_ref())
        .map(|relay| AlterIngestorOperation::DropRoute { relay });
    let replace_route = kw(Identifier::Replace)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(alter_ingestor_route_body())
        .map(|route| AlterIngestorOperation::ReplaceRoute { route });
    let set_general_error = kw(Identifier::Set)
        .ignore_then(kw(Identifier::General))
        .then_ignore(kw(Identifier::Error))
        .ignore_then(choice((
            kw(Identifier::Ignore).to(GeneralErrorPolicy::Ignore),
            kw(Identifier::Log).to(GeneralErrorPolicy::Log),
        )))
        .map(|policy| AlterIngestorOperation::SetGeneralError { policy });

    choice((
        set_quiesce,
        set_timestamp,
        drop_timestamp,
        set_filter,
        drop_filter,
        add_route,
        drop_route,
        replace_route,
        set_general_error,
    ))
    .boxed()
}

/// `SET FROM <body>`, which reads only the body of the pair the head names.
fn set_source<'src, S, P>(
    body: P,
) -> impl Parser<'src, &'src [Token], AlterIngestorOperation<S>, extra::Err<ParseError<'src>>> + Clone
where
    S: Clone + 'src,
    P: Parser<'src, &'src [Token], S, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    kw(Identifier::Set)
        .ignore_then(kw(Identifier::From))
        .ignore_then(body)
        .map(|source| AlterIngestorOperation::SetSource { source })
        .boxed()
}

/// `SET DECODE USING <codec>`, which only a transport's ingestor has.
fn set_decode<'src, S>()
-> impl Parser<'src, &'src [Token], AlterTransportIngestorOperation<S>, extra::Err<ParseError<'src>>>
+ Clone
where
    S: Clone + 'src,
{
    kw(Identifier::Set)
        .ignore_then(kw_phrase2(Identifier::Decode, Identifier::Using))
        .ignore_then(codec_ref())
        .map(|codec| AlterTransportIngestorOperation::SetDecodeUsing { codec })
        .boxed()
}

/// `<CONNECTOR> <ENTITY> INGESTOR <name> <operation>, …` after `ALTER`, for the entity Model `S`
/// whose operations are `Operation`.
fn alteration<'src, S, Operation, O>(
    operation: O,
) -> impl Parser<'src, &'src [Token], AlterIngestor, extra::Err<ParseError<'src>>> + Clone
where
    S: IngestorEntityModel + 'src,
    Operation: 'src,
    Vec<Operation>: Into<IngestorAlteration>,
    O: Parser<'src, &'src [Token], Operation, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    head(S::KIND)
        .ignore_then(ingestor_name())
        .then(
            operation
                .separated_by(alter_op_separator())
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .map(|(ingestor, operations)| AlterIngestor {
            ingestor,
            alteration: operations.into(),
        })
        .boxed()
}

/// The alteration of a transport's ingestor, whose `SET FROM` reads `body`.
fn transport_alteration<'src, S, P>(
    body: P,
) -> impl Parser<'src, &'src [Token], AlterIngestor, extra::Err<ParseError<'src>>> + Clone
where
    S: IngestorEntityModel + Clone + 'src,
    Vec<AlterTransportIngestorOperation<S>>: Into<IngestorAlteration>,
    P: Parser<'src, &'src [Token], S, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let operation = choice((
        set_source(body).map(AlterTransportIngestorOperation::from),
        set_decode(),
        shared_operation().map(AlterTransportIngestorOperation::from),
    ));
    alteration::<S, _, _>(operation)
}

/// The alteration of a client ingestor, which decodes nothing and so has no codec to set.
fn client_alteration<'src>()
-> impl Parser<'src, &'src [Token], AlterIngestor, extra::Err<ParseError<'src>>> + Clone {
    let operation = choice((set_source(client::producer_body()), shared_operation()));
    alteration::<ClientProducerIngestor, _, _>(operation)
}

pub fn alter_ingestor_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterIngestor, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Alter)
        .ignore_then(boxed_choice!(
            transport_alteration(http::request_body()),
            transport_alteration(http::endpoint_body()),
            transport_alteration(kafka::topic_body()),
            transport_alteration(pulsar::topic_body()),
            transport_alteration(mqtt::topic_body()),
            transport_alteration(nats::subject_body()),
            transport_alteration(rabbitmq::queue_body()),
            transport_alteration(redis::pubsub_body()),
            transport_alteration(prometheus::query_body()),
            transport_alteration(zeromq::pull_body()),
            transport_alteration(sqs::queue_body()),
            transport_alteration(websockets::connection_body()),
            transport_alteration(websockets::endpoint_body()),
            transport_alteration(syslog::listener_body()),
            client_alteration(),
        ))
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

pub fn parse_create_ingestor_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateIngestor>, Vec<ParseError<'_>>> {
    let out = create_ingestor_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_create_ingestor(
    input: &str,
) -> error_stack::Result<CreateStatement<CreateIngestor>, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_create_ingestor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_alter_ingestor_tokens(tokens: &[Token]) -> Result<AlterIngestor, Vec<ParseError<'_>>> {
    let out = alter_ingestor_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_alter_ingestor(
    input: &str,
) -> error_stack::Result<AlterIngestor, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_alter_ingestor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_ingestor(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_ingestor_parser())
}

pub fn suggest_alter_ingestor(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, alter_ingestor_parser())
}

#[cfg(test)]
mod conformance_tests;

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use meticulous::ResultExt as _;
    use nervix_models::{
        ClientIngestMode, ClientIngestorAlteration, ClientProducerIngestor, CodecName,
        EndpointIngestMode, FlushPolicy, HttpEndpointIngestor, HttpIngestor, HttpRequestIngestMode,
        HttpRequestIngestor, HttpRequestIngestorAlteration, IngestQuiesceMode,
        IngestQuiesceOverflow, KafkaIngestMode, KafkaIngestor, KafkaOffsetMode, KafkaTopicIngestor,
        MqttIngestMode, MqttIngestor, MqttQos, MqttSession, MqttTopicIngestor,
        MqttTopicIngestorAlteration, NatsIngestMode, NatsIngestor, NatsSubjectIngestor,
        PrometheusIngestMode, PrometheusIngestor, PrometheusQueryIngestor, PulsarIngestMode,
        PulsarIngestor, PulsarSubscription, PulsarSubscriptionOwnership, PulsarTopicIngestor,
        RabbitMqIngestMode, RabbitMqIngestor, RabbitMqQueueIngestor, RedisIngestor,
        RedisPubSubIngestMode, RedisPubSubIngestor, RetryPolicy, SchemaName, SqsIngestMode,
        SqsIngestor, SqsQueueIngestor, SyslogIngestMode, SyslogIngestor, SyslogListenerIngestor,
        WebsocketsConnectionIngestor, WebsocketsEndpointIngestor, WebsocketsIngestMode,
        WebsocketsIngestor, ZeroMqIngestMode, ZeroMqIngestor, ZeroMqPullIngestor,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::lexer::lex;

    /// The transport a parsed ingestor reads, which every transport fixture below declares.
    fn transport(parsed: &CreateIngestor) -> &TransportIngestorInput {
        let IngestorInput::Transport(input) = &parsed.input else {
            panic!("the fixture ingestor reads a transport");
        };
        input
    }

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    /// A whole statement around `source`, which names its head and its body: the head, the
    /// ingestor's name and `FROM` the body.
    fn ingestor_with_source(source: &str) -> String {
        format!(
            "CREATE {source} DECODE USING sch TO s UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG ON GENERAL ERROR LOG;"
        )
    }

    #[test]
    fn every_pair_keyword_is_an_plexus_keyword() {
        for kind in IngestorEntityKind::all() {
            for word in [kind.connector().keyword(), kind.entity_keyword()] {
                assert!(
                    Identifier::from_str(word).is_ok(),
                    "{word} of {kind} is not a Plexus keyword"
                );
            }
        }
    }

    #[test]
    fn parses_every_source_specific_quiesce_form() {
        let sources = [
            "KAFKA TOPIC INGESTOR i FROM c TOPIC 't' OFFSET BY DOMAIN MODE NO ACK ON QUIESCE \
             SUSPEND",
            "PULSAR TOPIC INGESTOR i FROM c TOPIC 't' SUBSCRIPTION 's' OWNED BY INGESTOR MODE NO \
             ACK ON QUIESCE SUSPEND",
            "PULSAR TOPIC INGESTOR i FROM c TOPIC 't' SUBSCRIPTION 's' PROVISIONED MODE NO ACK ON \
             QUIESCE SUSPEND",
            "RABBITMQ QUEUE INGESTOR i FROM c QUEUE 'q' MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY \
             POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND",
            "SQS QUEUE INGESTOR i FROM c QUEUE 'q' MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY \
             POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND",
            "MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION PERSISTENT QOS 1 MODE NO ACK ON \
             QUIESCE SUSPEND",
            "MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE \
             BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST",
            "MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE \
             DROP",
            "NATS SUBJECT INGESTOR i FROM c SUBJECT 's' QUEUE GROUP 'g' INSTANCES 1 MODE NO ACK \
             ON QUIESCE BUFFER MAX SIZE 64MiB ON OVERFLOW DROP NEWEST",
            "NATS SUBJECT INGESTOR i FROM c SUBJECT 's' QUEUE GROUP 'g' INSTANCES 1 MODE NO ACK \
             ON QUIESCE DROP",
            "REDIS PUBSUB INGESTOR i FROM c CHANNEL 'ch' MODE NO ACK ON QUIESCE BUFFER MAX SIZE \
             64MiB ON OVERFLOW DROP OLDEST",
            "WEBSOCKETS CONNECTION INGESTOR i FROM c MODE NO ACK ON QUIESCE DROP",
            "ZEROMQ PULL INGESTOR i FROM c MODE NO ACK ON QUIESCE SUSPEND",
            "ZEROMQ PULL INGESTOR i FROM c MODE NO ACK ON QUIESCE BUFFER MAX SIZE 64MiB ON \
             OVERFLOW DROP NEWEST",
            "HTTP REQUEST INGESTOR i FROM c METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND",
            "HTTP REQUEST INGESTOR i FROM c METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE BUFFER \
             MAX SIZE 64MiB ON OVERFLOW DROP OLDEST",
            "PROMETHEUS QUERY INGESTOR i FROM c QUERY 'up' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND",
            "HTTP ENDPOINT INGESTOR i FROM e MODE NO ACK ON QUIESCE REJECT RETRY AFTER 5s",
            "HTTP ENDPOINT INGESTOR i FROM e MODE NO ACK ON QUIESCE BUFFER MAX SIZE 64MiB",
        ];

        for source in sources {
            parse_create_ingestor(&ingestor_with_source(source))
                .unwrap_or_else(|error| panic!("source `{source}` should parse: {error:?}"));
        }
    }

    #[test]
    fn rejects_an_external_name_its_entity_refuses() {
        let cases = [
            (
                "KAFKA TOPIC INGESTOR i FROM c TOPIC 'orders eu' OFFSET BY DOMAIN MODE NO ACK ON \
                 QUIESCE SUSPEND",
                "invalid kafka_topic 'orders eu': holds ' ', which it may not hold",
            ),
            (
                "SQS QUEUE INGESTOR i FROM c QUEUE '.fifo' MODE ACK SEQUENTIAL ACK TIMEOUT 1s \
                 RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND",
                "invalid sqs_queue '.fifo': holds nothing before its '.fifo' suffix",
            ),
            (
                "NATS SUBJECT INGESTOR i FROM c SUBJECT 'orders.>.created' QUEUE GROUP 'g' \
                 INSTANCES 1 MODE NO ACK ON QUIESCE DROP",
                "invalid nats_subject 'orders.>.created': holds the wildcard '>' before its last \
                 level",
            ),
            (
                "MQTT TOPIC INGESTOR i FROM c TOPIC 'devices/#/orders' SESSION CLEAN QOS 0 MODE \
                 NO ACK ON QUIESCE DROP",
                "invalid mqtt_topic_filter 'devices/#/orders': holds the wildcard '#' before its \
                 last level",
            ),
            (
                "PULSAR TOPIC INGESTOR i FROM c TOPIC 'public/default/orders' SUBSCRIPTION 's' \
                 PROVISIONED MODE NO ACK ON QUIESCE SUSPEND",
                "invalid pulsar_topic 'public/default/orders': holds '/' without naming its \
                 domain, one of persistent, non-persistent",
            ),
        ];
        for (source, message) in cases {
            let error = parse_create_ingestor(&ingestor_with_source(source))
                .expect_err("the name is not one its entity admits");
            let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
                panic!("expected a parse error, got {error:?}");
            };
            assert!(
                diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.message == message),
                "{message} in {diagnostics:?}"
            );
        }
    }

    #[test]
    fn keeps_an_external_name_exactly_as_written() {
        let parsed = parse_create_ingestor(&ingestor_with_source(
            "KAFKA TOPIC INGESTOR i FROM c TOPIC 'Orders.EU' OFFSET BY CONSUMER GROUP 'Readers of \
             Orders' MODE NO ACK ON QUIESCE SUSPEND",
        ))
        .expect("a case-preserving topic and group parse");
        let IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
            topic,
            offset_mode,
            ..
        })) = &transport(&parsed).source
        else {
            panic!("expected a Kafka topic source");
        };
        assert_eq!(topic.as_str(), "Orders.EU");
        let KafkaOffsetMode::ConsumerGroup(group) = offset_mode else {
            panic!("expected a consumer group");
        };
        assert_eq!(group.as_str(), "Readers of Orders");
    }

    #[test]
    fn completion_names_the_external_name_each_entity_expects() {
        for (input, label) in [
            ("CREATE KAFKA TOPIC INGESTOR i FROM c TOPIC ", "kafka_topic"),
            (
                "CREATE KAFKA TOPIC INGESTOR i FROM c TOPIC 't' OFFSET BY CONSUMER GROUP ",
                "kafka_consumer_group",
            ),
            (
                "CREATE PULSAR TOPIC INGESTOR i FROM c TOPIC ",
                "pulsar_topic",
            ),
            (
                "CREATE RABBITMQ QUEUE INGESTOR i FROM c QUEUE ",
                "rabbitmq_queue",
            ),
            ("CREATE SQS QUEUE INGESTOR i FROM c QUEUE ", "sqs_queue"),
            (
                "CREATE REDIS PUBSUB INGESTOR i FROM c CHANNEL ",
                "redis_channel",
            ),
            (
                "CREATE NATS SUBJECT INGESTOR i FROM c SUBJECT ",
                "nats_subject",
            ),
            (
                "CREATE NATS SUBJECT INGESTOR i FROM c SUBJECT 's' QUEUE GROUP ",
                "nats_queue_group",
            ),
            (
                "CREATE MQTT TOPIC INGESTOR i FROM c TOPIC ",
                "mqtt_topic_filter",
            ),
        ] {
            let suggestions = suggest_create_ingestor(input, input.len());
            assert_eq!(suggestions, [label.to_string()], "{input}");
        }
    }

    #[test]
    fn requires_quiesce_clause_and_all_mode_variables() {
        let invalid_sources = [
            "KAFKA TOPIC INGESTOR i FROM c TOPIC 't' OFFSET BY DOMAIN MODE NO ACK",
            "NATS SUBJECT INGESTOR i FROM c SUBJECT 's' QUEUE GROUP 'g' INSTANCES 1 MODE NO ACK \
             ON QUIESCE BUFFER",
            "NATS SUBJECT INGESTOR i FROM c SUBJECT 's' QUEUE GROUP 'g' INSTANCES 1 MODE NO ACK \
             ON QUIESCE BUFFER MAX SIZE 64MiB",
            "NATS SUBJECT INGESTOR i FROM c SUBJECT 's' QUEUE GROUP 'g' INSTANCES 1 MODE NO ACK \
             ON QUIESCE BUFFER MAX SIZE 0B ON OVERFLOW DROP OLDEST",
            "HTTP ENDPOINT INGESTOR i FROM e MODE NO ACK ON QUIESCE REJECT",
        ];

        for source in invalid_sources {
            assert!(
                parse_create_ingestor(&ingestor_with_source(source)).is_err(),
                "source `{source}` should be rejected"
            );
        }
    }

    #[test]
    fn rejects_quiesce_modes_foreign_to_the_source() {
        let invalid_sources = [
            "KAFKA TOPIC INGESTOR i FROM c TOPIC 't' OFFSET BY DOMAIN MODE NO ACK ON QUIESCE \
             BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST",
            "HTTP ENDPOINT INGESTOR i FROM e MODE NO ACK ON QUIESCE SUSPEND",
            "HTTP REQUEST INGESTOR i FROM c METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE DROP",
            "NATS SUBJECT INGESTOR i FROM c SUBJECT 's' QUEUE GROUP 'g' INSTANCES 1 MODE NO ACK \
             ON QUIESCE REJECT RETRY AFTER 1s",
        ];

        for source in invalid_sources {
            assert!(
                parse_create_ingestor(&ingestor_with_source(source)).is_err(),
                "source `{source}` should be rejected"
            );
        }
    }

    #[test]
    fn mqtt_suspend_requires_a_persistent_session_at_qos_one_or_two() {
        for source in [
            "MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE \
             SUSPEND",
            "MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION CLEAN QOS 1 MODE NO ACK ON QUIESCE \
             SUSPEND",
            "MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION CLEAN QOS 2 MODE NO ACK ON QUIESCE \
             SUSPEND",
            "MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION PERSISTENT QOS 0 MODE NO ACK ON \
             QUIESCE SUSPEND",
        ] {
            assert!(
                parse_create_ingestor(&ingestor_with_source(source)).is_err(),
                "source `{source}` should be rejected"
            );
        }
    }

    #[test]
    fn quiesce_completion_is_composed_and_source_specific() {
        let kafka = "CREATE KAFKA TOPIC INGESTOR i FROM c TOPIC 't' OFFSET BY DOMAIN MODE NO ACK ";
        let kafka_suggestions = suggest_create_ingestor(kafka, kafka.len());
        assert!(kafka_suggestions.contains(&"ON QUIESCE".to_string()));
        assert!(!kafka_suggestions.contains(&"DECODE USING".to_string()));

        let nats = "CREATE NATS SUBJECT INGESTOR i FROM c SUBJECT 's' QUEUE GROUP 'g' INSTANCES 1 \
                    MODE NO ACK ON QUIESCE ";
        let nats_suggestions = suggest_create_ingestor(nats, nats.len());
        assert!(nats_suggestions.contains(&"BUFFER".to_string()));
        assert!(nats_suggestions.contains(&"DROP".to_string()));
        assert!(!nats_suggestions.contains(&"SUSPEND".to_string()));
        assert!(!nats_suggestions.contains(&"REJECT".to_string()));

        let endpoint = "CREATE HTTP ENDPOINT INGESTOR i FROM e MODE NO ACK ON QUIESCE ";
        let endpoint_suggestions = suggest_create_ingestor(endpoint, endpoint.len());
        assert!(endpoint_suggestions.contains(&"BUFFER".to_string()));
        assert!(endpoint_suggestions.contains(&"REJECT".to_string()));
        assert!(!endpoint_suggestions.contains(&"DROP".to_string()));
        assert!(!endpoint_suggestions.contains(&"SUSPEND".to_string()));

        let clean_mqtt = "CREATE MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION CLEAN QOS 0 MODE \
                          NO ACK ON QUIESCE ";
        let clean_mqtt_suggestions = suggest_create_ingestor(clean_mqtt, clean_mqtt.len());
        assert!(clean_mqtt_suggestions.contains(&"BUFFER".to_string()));
        assert!(clean_mqtt_suggestions.contains(&"DROP".to_string()));
        assert!(!clean_mqtt_suggestions.contains(&"SUSPEND".to_string()));

        let persistent_mqtt = "CREATE MQTT TOPIC INGESTOR i FROM c TOPIC 't' SESSION PERSISTENT \
                               QOS 1 MODE NO ACK ON QUIESCE ";
        let persistent_mqtt_suggestions =
            suggest_create_ingestor(persistent_mqtt, persistent_mqtt.len());
        assert!(persistent_mqtt_suggestions.contains(&"BUFFER".to_string()));
        assert!(persistent_mqtt_suggestions.contains(&"DROP".to_string()));
        assert!(persistent_mqtt_suggestions.contains(&"SUSPEND".to_string()));
    }

    #[test]
    fn parses_set_quiesce_alter_operation() {
        let parsed = parse_alter_ingestor(
            "ALTER MQTT TOPIC INGESTOR i SET QUIESCE BUFFER MAX SIZE 32MiB ON OVERFLOW DROP \
             NEWEST;",
        )
        .expect("SET QUIESCE should parse");
        let IngestorAlteration::Mqtt(MqttTopicIngestorAlteration::Topic(operations)) =
            &parsed.alteration
        else {
            panic!("the head names an MQTT topic");
        };
        assert_eq!(operations.len(), 1);
    }

    #[test]
    fn parses_alter_ingestor_full_operations_and_expression_commas() {
        let parsed = parse_alter_ingestor(
            "ALTER HTTP ENDPOINT INGESTOR event_source SET FROM ingress_b MODE NO ACK ON QUIESCE \
             BUFFER MAX SIZE 1MiB, SET DECODE USING event_codec_v2, SET TIMESTAMP NOW, SET FILTER \
             WHERE concat(input.kind, ',') != '', REPLACE ROUTE TO events INHERIT ALL UNBRANCHED \
             FLUSH IMMEDIATE ON MESSAGE ERROR SEND TO errors SET code = concat(error.code, \
             ',bad'), ADD ROUTE TO audit INHERIT ALL UNBRANCHED FLUSH EACH 10ms MAX BATCH SIZE \
             1MiB ON MESSAGE ERROR LOG, SET GENERAL ERROR IGNORE;",
        )
        .expect("ALTER INGESTOR should parse all operations");

        assert_eq!(parsed.ingestor.as_str(), "event_source");
        let IngestorAlteration::Http(HttpRequestIngestorAlteration::Endpoint(operations)) =
            &parsed.alteration
        else {
            panic!("the head names an HTTP endpoint");
        };
        assert_eq!(operations.len(), 7);
        let AlterTransportIngestorOperation::Ingestor(first) = &operations[0] else {
            panic!("the first operation is one every ingestor accepts");
        };
        let AlterIngestorOperation::SetSource { source } = first.as_ref() else {
            panic!("the first operation sets the source");
        };
        assert_eq!(source.endpoint.as_str(), "ingress_b");
        assert_eq!(
            operations[1],
            AlterTransportIngestorOperation::SetDecodeUsing {
                codec: CodecName::parse("event_codec_v2").assured("a valid codec name"),
            }
        );
        let AlterTransportIngestorOperation::Ingestor(fifth) = &operations[4] else {
            panic!("the fifth operation is one every ingestor accepts");
        };
        assert!(matches!(
            fifth.as_ref(),
            AlterIngestorOperation::ReplaceRoute { .. }
        ));
        assert_eq!(
            operations[6],
            AlterTransportIngestorOperation::from(AlterIngestorOperation::SetGeneralError {
                policy: GeneralErrorPolicy::Ignore
            })
        );
    }

    #[test]
    fn rejects_alter_ingestor_without_operations() {
        assert!(
            parse_alter_ingestor("ALTER KAFKA TOPIC INGESTOR event_source;").is_err(),
            "ALTER requires at least one operation"
        );
    }

    #[test]
    fn alter_ingestor_completion_exposes_operation_heads() {
        let transport = "ALTER KAFKA TOPIC INGESTOR event_source SET ";
        let suggestions = suggest_alter_ingestor(transport, transport.len());
        assert!(suggestions.contains(&"FROM".to_string()));
        assert!(suggestions.contains(&"DECODE USING".to_string()));
        assert!(suggestions.contains(&"TIMESTAMP".to_string()));
        assert!(suggestions.contains(&"FILTER".to_string()));
        assert!(suggestions.contains(&"GENERAL".to_string()));
        assert!(!suggestions.contains(&"SCHEMA".to_string()));

        let client = "ALTER CLIENT PRODUCER INGESTOR event_source SET ";
        let suggestions = suggest_alter_ingestor(client, client.len());
        assert!(suggestions.contains(&"FROM".to_string()));
        assert!(!suggestions.contains(&"DECODE USING".to_string()));

        let heads = suggest_alter_ingestor("ALTER ", "ALTER ".len());
        assert!(heads.contains(&"KAFKA TOPIC INGESTOR".to_string()));
        assert!(heads.contains(&"CLIENT PRODUCER INGESTOR".to_string()));
    }

    #[test]
    fn parses_route_message_policies_and_node_general_policy() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR route_ingestor
                FROM input_endpoint MODE NO ACK
                ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING input_codec
                TO accepted UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR IGNORE
                TO rejected UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
                ON GENERAL ERROR IGNORE;
        "#;

        let parsed = parse_create_ingestor(input).expect("route policies should parse");
        assert_eq!(
            parsed.output_routes.routes[0].message_error_policy,
            nervix_models::MessageErrorPolicy::Ignore
        );
        assert_eq!(
            parsed.output_routes.routes[1].message_error_policy,
            nervix_models::MessageErrorPolicy::Log
        );
        assert_eq!(
            parsed.general_error_policy,
            nervix_models::GeneralErrorPolicy::Ignore
        );
    }

    #[test]
    fn parses_create_ingestor_ack_parallel() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR kafka_notifications
                FROM
                    kafka_main
                    TOPIC 'notifications'
                    OFFSET BY CONSUMER GROUP 'nervix_consumer'
                    MODE ACK PARALLEL MAX 10 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
                ON QUIESCE SUSPEND DECODE USING notification_kafka_message
                TO notifications
                BRANCHED BY notification_params SET user_id = message.user_id
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "kafka_notifications");
        assert_eq!(
            parsed
                .output_routes
                .outputs()
                .map(|output| output.relay.as_str())
                .collect::<Vec<_>>(),
            vec!["notifications"]
        );
        assert_eq!(
            transport(&parsed).codec.as_str(),
            "notification_kafka_message"
        );
        assert_eq!(
            parsed.output_routes.routes[0]
                .branch
                .as_ref()
                .and_then(nervix_models::OutputBranch::branch)
                .map(|branch| branch.as_str()),
            Some("notification_params")
        );
        assert_eq!(parsed.timestamp_source, None);
        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
                client: nervix_models::ClientName::try_from("kafka_main")
                    .expect("valid client identifier"),
                topic: nervix_models::KafkaTopicName::try_from("notifications")
                    .expect("valid topic identifier"),
                offset_mode: KafkaOffsetMode::ConsumerGroup(
                    nervix_models::KafkaConsumerGroupName::try_from("nervix_consumer")
                        .expect("valid consumer group identifier"),
                ),
                instances: nonzero!(1u64),
                mode: KafkaIngestMode::AckParallel {
                    max: nonzero!(10u64),
                    batch_timeout: "500ms".to_string(),
                    timeout: "30s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "200ms".to_string(),
                        max_backoff: "5s".to_string(),
                    },
                },
                quiesce: IngestQuiesceMode::Suspend,
            }))
        );
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse"),
            &FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string()
            }
        );
    }

    #[test]
    fn parses_create_ingestor_unbranched() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR http_notifications
                FROM http_notifications_endpoint
                MODE NO ACK
                ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
                TO notifications UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.output_routes.routes[0].branch,
            Some(nervix_models::OutputBranch::Unbranched)
        );
    }

    #[test]
    fn parses_route_local_branch_set() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR i
              FROM ep MODE NO ACK
              ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sch
              TO s BRANCHED BY u_branch SET u = message.u
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(
            parsed.output_routes.routes[0]
                .branch
                .as_ref()
                .and_then(nervix_models::OutputBranch::branch)
                .map(|branch| branch.as_str()),
            Some("u_branch")
        );
        assert_eq!(
            parsed.output_routes.routes[0]
                .branch
                .as_ref()
                .expect("branch must parse")
                .assignments()
                .len(),
            1
        );
    }

    #[test]
    fn rejects_bare_by() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR i
              FROM ep MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sch
              TO s BY u_branch
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("bare BY is not a branch selection mode");
    }

    #[test]
    fn rejects_unbranched_with_ttl() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR i
              FROM ep MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sch
              TO s UNBRANCHED TTL 5m
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("TTL must not follow UNBRANCHED");
    }

    #[test]
    fn parses_create_ingestor_ack_sequential() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR i
              FROM t TOPIC 'top' OFFSET BY CONSUMER GROUP 'g' MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s
              ON QUIESCE SUSPEND DECODE USING sch
              TO s BRANCHED BY u_branch SET u = message.u
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
            instances, mode, ..
        })) = &transport(&parsed).source
        else {
            panic!("expected kafka ingestor source");
        };
        assert_eq!(*instances, nonzero!(1u64));
        assert_eq!(
            mode,
            &KafkaIngestMode::AckSequential {
                timeout: "15s".to_string(),
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "8s".to_string(),
                }
            }
        );
    }

    #[test]
    fn parses_create_ingestor_kafka_no_ack() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR i
              FROM t TOPIC 'top' OFFSET BY CONSUMER GROUP 'g' MODE NO ACK
              ON QUIESCE SUSPEND DECODE USING sch
              TO s BRANCHED BY u_branch SET u = message.u
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor { mode, .. })) =
            &transport(&parsed).source
        else {
            panic!("expected kafka ingestor source");
        };
        assert!(matches!(mode, KafkaIngestMode::NoAck));
    }

    #[test]
    fn rejects_create_ingestor_kafka_no_ack_max() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR i
              FROM t TOPIC 'top' OFFSET BY CONSUMER GROUP 'g' MODE NO ACK MAX 20
              ON QUIESCE SUSPEND DECODE USING sch
              TO s BRANCHED BY u_branch SET u = message.u
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("NO ACK must not accept an in-flight ACK bound");
    }

    #[test]
    fn no_ack_completion_continues_to_quiesce_without_max() {
        let input = "CREATE KAFKA TOPIC INGESTOR i FROM t TOPIC 'top' OFFSET BY CONSUMER GROUP \
                     'g' MODE NO ACK ";
        let suggestions = suggest_create_ingestor(input, input.len());

        assert!(suggestions.contains(&"ON QUIESCE".to_string()));
        assert!(!suggestions.contains(&"MAX".to_string()));
    }

    #[test]
    fn parses_create_ingestor_pulsar_ack_sequential() {
        let input = r#"
            CREATE PULSAR TOPIC INGESTOR pulsar_notifications
              FROM pulsar_main TOPIC 'notifications' SUBSCRIPTION 'nervix_subscription' OWNED BY INGESTOR MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s
              ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Pulsar(PulsarIngestor::Topic(PulsarTopicIngestor {
                client: nervix_models::ClientName::try_from("pulsar_main")
                    .expect("valid client identifier"),
                topic: nervix_models::PulsarTopicName::try_from("notifications")
                    .expect("valid topic identifier"),
                subscription: PulsarSubscription {
                    name: nervix_models::PulsarSubscriptionName::try_from("nervix_subscription")
                        .expect("valid subscription identifier"),
                    ownership: PulsarSubscriptionOwnership::Ingestor,
                },
                instances: nonzero!(1u64),
                mode: PulsarIngestMode::AckSequential {
                    timeout: "15s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "8s".to_string(),
                    },
                },
                quiesce: IngestQuiesceMode::Suspend,
            }))
        );
    }

    #[test]
    fn rejects_create_ingestor_pulsar_without_subscription() {
        let input = r#"
            CREATE PULSAR TOPIC INGESTOR pulsar_notifications
              FROM pulsar_main TOPIC 'notifications'
              MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s
              ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let error = parse_create_ingestor_tokens(&tokens).expect_err("parse should fail");
        assert!(!error.is_empty());
    }

    #[test]
    fn parses_create_ingestor_rabbitmq_ack_sequential() {
        let input = r#"
            CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
              FROM rabbit_main QUEUE 'notifications' MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s
              ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "rabbit_notifications");
        assert_eq!(
            parsed
                .output_routes
                .outputs()
                .map(|output| output.relay.as_str())
                .collect::<Vec<_>>(),
            vec!["notifications"]
        );
        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::RabbitMq(RabbitMqIngestor::Queue(RabbitMqQueueIngestor {
                client: nervix_models::ClientName::try_from("rabbit_main")
                    .expect("valid client identifier"),
                queue: nervix_models::RabbitMqQueueName::try_from("notifications")
                    .expect("valid queue identifier"),
                instances: nonzero!(1u64),
                mode: RabbitMqIngestMode::AckSequential {
                    timeout: "20s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "1s".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                quiesce: IngestQuiesceMode::Suspend,
            }))
        );
    }

    #[test]
    fn parses_create_ingestor_redis_pubsub_no_ack() {
        let input = r#"
            CREATE REDIS PUBSUB INGESTOR redis_notifications
              FROM redis_main CHANNEL 'notifications' MODE NO ACK
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "redis_notifications");
        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Redis(RedisIngestor::PubSub(RedisPubSubIngestor {
                client: nervix_models::ClientName::try_from("redis_main")
                    .expect("valid client identifier"),
                subscription: nervix_models::RedisSubscriptionTarget::Channel(
                    nervix_models::RedisChannelName::try_from("notifications")
                        .expect("valid channel identifier")
                ),
                mode: RedisPubSubIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Drop,
            }))
        );
    }

    #[test]
    fn suggests_the_entity_after_a_connector_keyword() {
        let input = "CREATE REDIS ";
        let suggestions = suggest_create_ingestor(input, input.len());

        assert!(suggestions.contains(&"PUBSUB".to_string()));
        assert!(!suggestions.contains(&"CHANNEL".to_string()));

        let input = "CREATE HTTP ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"REQUEST".to_string()));
        assert!(suggestions.contains(&"ENDPOINT".to_string()));
    }

    #[test]
    fn suggests_mode_options() {
        let input =
            "CREATE KAFKA TOPIC INGESTOR i FROM t TOPIC 'top' OFFSET BY CONSUMER GROUP 'g' MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO ACK".to_string()));
    }

    #[test]
    fn suggests_every_kinded_head_after_create() {
        let suggestions = suggest_create_ingestor("CREATE ", "CREATE ".len());
        for kind in IngestorEntityKind::all() {
            let head = format!("{} INGESTOR", kind.phrase());
            assert!(suggestions.contains(&head), "{head} in {suggestions:?}");
        }

        let input = "CREATE KAFKA TOPIC INGESTOR i FROM ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ref:client".to_string()));
        assert!(!suggestions.contains(&"KAFKA".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));
    }

    #[test]
    fn suggests_set_after_branched_by() {
        let input = "CREATE HTTP ENDPOINT INGESTOR i FROM ep MODE NO ACK ON QUIESCE BUFFER MAX \
                     SIZE 1MiB DECODE USING sch TO s BRANCHED BY u_branch ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"SET".to_string()));
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"VALUES".to_string()));
        assert!(!suggestions.contains(&"TTL".to_string()));
        assert!(!suggestions.contains(&"TIMESTAMP".to_string()));
    }

    #[test]
    fn suggests_flush_on_output_without_transport_leakage() {
        let input = "CREATE HTTP ENDPOINT INGESTOR i FROM ep MODE NO ACK ON QUIESCE BUFFER MAX \
                     SIZE 1MiB DECODE USING sch TO s UNBRANCHED FL";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"TIMESTAMP".to_string()));
        assert!(!suggestions.contains(&"FROM".to_string()));
    }

    #[test]
    fn pulsar_mode_context_does_not_offer_kafka_offset_keywords() {
        let input = "CREATE PULSAR TOPIC INGESTOR i FROM p TOPIC 'top' SUBSCRIPTION 'sub' OWNED \
                     BY INGESTOR MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO ACK".to_string()));
        assert!(!suggestions.contains(&"OFFSET".to_string()));
        assert!(!suggestions.contains(&"CONSUMER GROUP".to_string()));
    }

    #[test]
    fn pulsar_subscription_context_expects_subscription_name() {
        let input = "CREATE PULSAR TOPIC INGESTOR i FROM p TOPIC 'top' SUBSCRIPTION ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"pulsar_subscription".to_string()));
        assert!(!suggestions.contains(&"SHARED".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));
    }

    #[test]
    fn parses_who_owns_a_pulsar_subscription_in_any_case() {
        for (ownership, expected) in [
            ("OWNED BY INGESTOR", PulsarSubscriptionOwnership::Ingestor),
            ("owned by Ingestor", PulsarSubscriptionOwnership::Ingestor),
            ("PROVISIONED", PulsarSubscriptionOwnership::Provisioned),
            ("Provisioned", PulsarSubscriptionOwnership::Provisioned),
        ] {
            let source = format!(
                "PULSAR TOPIC INGESTOR i FROM c TOPIC 'orders' SUBSCRIPTION 'Readers' {ownership} \
                 INSTANCES 2 MODE NO ACK ON QUIESCE SUSPEND"
            );
            let parsed = parse_create_ingestor(&ingestor_with_source(&source))
                .unwrap_or_else(|error| panic!("`{source}` should parse: {error:?}"));
            let IngestorEntity::Pulsar(PulsarIngestor::Topic(PulsarTopicIngestor {
                subscription,
                instances,
                ..
            })) = &transport(&parsed).source
            else {
                panic!("expected a Pulsar topic source from `{source}`");
            };
            assert_eq!(subscription.name.as_str(), "Readers", "{source}");
            assert_eq!(subscription.ownership, expected, "{source}");
            assert_eq!(*instances, nonzero!(2u64), "{source}");
        }
    }

    #[test]
    fn refuses_a_pulsar_subscription_whose_ownership_is_missing_or_misspelled() {
        for ownership in [
            "",
            "OWNED",
            "OWNED BY",
            "OWNED BY DOMAIN",
            "BY INGESTOR",
            "PROVISIONED OWNED BY INGESTOR",
            "OWNED BY INGESTOR PROVISIONED",
        ] {
            let source = format!(
                "PULSAR TOPIC INGESTOR i FROM c TOPIC 'orders' SUBSCRIPTION 'readers' {ownership} \
                 MODE NO ACK ON QUIESCE SUSPEND"
            );
            let error = parse_create_ingestor(&ingestor_with_source(&source)).expect_err(&format!(
                "`{source}` declares no single ownership of its subscription"
            ));
            assert!(
                matches!(error.current_context(), ParseFromSourceError::Parse { .. }),
                "the grammar refuses `{source}`, got {error:?}"
            );
        }
    }

    #[test]
    fn pulsar_subscription_name_is_followed_by_exactly_its_ownership_choices() {
        let input = "CREATE PULSAR TOPIC INGESTOR i FROM p TOPIC 'top' SUBSCRIPTION 'sub' ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"OWNED BY INGESTOR".to_string()));
        assert!(suggestions.contains(&"PROVISIONED".to_string()));
        for leaked in ["MODE", "INSTANCES", "OWNED", "BY", "INGESTOR", "OFFSET BY"] {
            assert!(
                !suggestions.contains(&leaked.to_string()),
                "{leaked} offered after the subscription name: {suggestions:?}"
            );
        }
    }

    #[test]
    fn pulsar_ownership_is_followed_by_the_instances_and_the_mode() {
        for ownership in ["OWNED BY INGESTOR", "PROVISIONED"] {
            let input = format!(
                "CREATE PULSAR TOPIC INGESTOR i FROM p TOPIC 'top' SUBSCRIPTION 'sub' {ownership} "
            );
            let suggestions = suggest_create_ingestor(&input, input.len());
            assert!(suggestions.contains(&"INSTANCES".to_string()), "{input}");
            assert!(suggestions.contains(&"MODE".to_string()), "{input}");
            assert!(
                !suggestions.contains(&"PROVISIONED".to_string()),
                "{input}: {suggestions:?}"
            );
        }
    }

    #[test]
    fn rejects_create_ingestor_pulsar_shared_subscription_keyword() {
        let input = r#"
            CREATE PULSAR TOPIC INGESTOR pulsar_notifications
              FROM pulsar_main TOPIC 'notifications' SUBSCRIPTION SHARED 'nervix_subscription'
              MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s
              ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("Pulsar subscription type is not part of the public grammar");
    }

    #[test]
    fn parses_create_ingestor_flush_each() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR kafka_notifications
                FROM kafka_main
                    TOPIC 'notifications'
                    OFFSET BY CONSUMER GROUP 'nervix_consumer'
                    MODE ACK PARALLEL MAX 10 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
                ON QUIESCE SUSPEND DECODE USING notification_kafka_message
                TO notifications BRANCHED BY user_id_kind_branch SET user_id = message.user_id
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse"),
            &FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string()
            }
        );
    }

    #[test]
    fn parses_create_ingestor_flush_immediate() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR i
              FROM ep MODE NO ACK
              ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sch
              TO s BRANCHED BY u_branch SET u = message.u
              FLUSH IMMEDIATE ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse"),
            &FlushPolicy::Immediate
        );
    }

    #[test]
    fn rejects_input_collection() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR i
              FROM ep MODE NO ACK
              COLLECT FOR 1s
              ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sch
              TO s UNBRANCHED
              FLUSH IMMEDIATE ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("ingestors do not read relay inputs and cannot collect them");
    }

    #[test]
    fn input_context_does_not_suggest_collection() {
        let input = "CREATE HTTP ENDPOINT INGESTOR i FROM ep MODE NO ACK ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(!suggestions.contains(&"COLLECT FOR".to_string()));
    }

    #[test]
    fn suggests_branched_by_as_compound_keyword() {
        let input = "CREATE HTTP ENDPOINT INGESTOR i FROM ep MODE NO ACK ON QUIESCE BUFFER MAX \
                     SIZE 1MiB DECODE USING sch TO s ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
        assert!(!suggestions.contains(&"BRANCHED_BY".to_string()));
    }

    #[test]
    fn rejects_unbranched_with_values_block() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR i
              FROM ep MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sch
              TO s UNBRANCHED VALUES {}
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("VALUES block must not follow UNBRANCHED");
    }

    #[test]
    fn output_context_suggests_construction_and_branch_not_values() {
        let input = "CREATE HTTP ENDPOINT INGESTOR i FROM ep MODE NO ACK ON QUIESCE BUFFER MAX \
                     SIZE 1MiB DECODE USING sch TO s ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"SET".to_string()));
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
        assert!(!suggestions.contains(&"VALUES".to_string()));
    }

    #[test]
    fn suggests_to_keyword() {
        let input = "CREATE HTTP ENDPOINT INGESTOR i FROM ep MODE NO ACK ON QUIESCE BUFFER MAX \
                     SIZE 1MiB DECODE USING sch ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"RELAY".to_string()));
    }

    #[test]
    fn suggests_decode_using_as_compound_keyword() {
        let input =
            "CREATE HTTP ENDPOINT INGESTOR i FROM ep MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"DECODE USING".to_string()));
    }

    #[test]
    fn rabbitmq_mode_context_offers_declared_consumer_modes() {
        let input = "CREATE RABBITMQ QUEUE INGESTOR i FROM t QUEUE 'q' MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO ACK".to_string()));

        let input = format!("{input}ACK ");
        let suggestions = suggest_create_ingestor(&input, input.len());
        assert!(suggestions.contains(&"SEQUENTIAL".to_string()));
        assert!(suggestions.contains(&"PARALLEL".to_string()));
    }

    #[test]
    fn redis_pubsub_mode_context_does_not_offer_ack_or_parallel() {
        let input = "CREATE REDIS PUBSUB INGESTOR i FROM t CHANNEL 'c' MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"NO ACK".to_string()));
        assert!(!suggestions.contains(&"ACK".to_string()));
        assert!(!suggestions.contains(&"PARALLEL".to_string()));
    }

    #[test]
    fn parses_create_ingestor_mqtt_no_ack() {
        let input = r#"
            CREATE MQTT TOPIC INGESTOR mqtt_notifications
              FROM mqtt_main TOPIC 'notifications' SESSION CLEAN QOS 0 MODE NO ACK
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "mqtt_notifications");
        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
                client: nervix_models::ClientName::try_from("mqtt_main")
                    .expect("valid client identifier"),
                topic: nervix_models::MqttTopicFilter::try_from("notifications")
                    .expect("valid topic filter"),
                instances: nonzero!(1u64),
                session: MqttSession::Clean,
                qos: MqttQos::Qos0,
                mode: MqttIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Drop,
            }))
        );
    }

    #[test]
    fn parses_create_ingestor_mqtt_no_ack_with_qos1() {
        let input = r#"
            CREATE MQTT TOPIC INGESTOR mqtt_notifications
              FROM mqtt_main TOPIC 'notifications' SESSION CLEAN QOS 1 MODE NO ACK
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");

        let IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
            client,
            topic,
            instances,
            session,
            qos,
            mode: MqttIngestMode::NoAck,
            ..
        })) = &transport(&parsed).source
        else {
            panic!("expected an unacknowledged MQTT source");
        };
        assert_eq!(client.as_str(), "mqtt_main");
        assert_eq!(topic.as_str(), "notifications");
        assert_eq!(*instances, nonzero!(1u64));
        assert_eq!(*session, MqttSession::Clean);
        assert_eq!(*qos, MqttQos::Qos1);
    }

    #[test]
    fn parses_create_ingestor_mqtt_ack_parallel_instances() {
        let input = r#"
            CREATE MQTT TOPIC INGESTOR mqtt_notifications
              FROM mqtt_main
              TOPIC 'devices/+/notifications'
              INSTANCES 3
              SESSION PERSISTENT QOS 1
              MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 250ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
                client: nervix_models::ClientName::try_from("mqtt_main")
                    .expect("valid client identifier"),
                topic: nervix_models::MqttTopicFilter::try_from("devices/+/notifications")
                    .expect("valid topic filter"),
                instances: nonzero!(3u64),
                session: MqttSession::Persistent,
                qos: MqttQos::Qos1,
                mode: MqttIngestMode::AckParallel {
                    max: nonzero!(8u64),
                    batch_timeout: "250ms".to_string(),
                    timeout: "5s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "100ms".to_string(),
                        max_backoff: "2s".to_string(),
                    },
                },
                quiesce: IngestQuiesceMode::Drop,
            }))
        );
    }

    #[test]
    fn rejects_create_ingestor_mqtt_subscription_clause() {
        let input = r#"
            CREATE MQTT TOPIC INGESTOR mqtt_notifications
              FROM mqtt_main
              TOPIC 'notifications'
              SUBSCRIPTION SHARED nervix_group
              SESSION CLEAN QOS 0 MODE NO ACK
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input)).expect_err(
            "MQTT TOPIC INGESTOR i FROM subscription mode is not part of the public grammar",
        );
    }

    #[test]
    fn parses_create_ingestor_mqtt_ack_sequential() {
        let input = r#"
            CREATE MQTT TOPIC INGESTOR mqtt_notifications
              FROM mqtt_main TOPIC 'notifications' SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");
        let IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor { mode, .. })) =
            &transport(&parsed).source
        else {
            panic!("expected mqtt ingestor source");
        };

        assert_eq!(
            mode,
            &MqttIngestMode::AckSequential {
                timeout: "5s".to_string(),
                retry_policy: RetryPolicy {
                    backoff: "100ms".to_string(),
                    max_backoff: "2s".to_string(),
                },
            }
        );
    }

    #[test]
    fn rejects_create_ingestor_mqtt_ack_without_persistent_session() {
        let input = r#"
            CREATE MQTT TOPIC INGESTOR mqtt_notifications
              FROM mqtt_main TOPIC 'notifications' SESSION CLEAN QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("ACK mode must require SESSION PERSISTENT");
    }

    #[test]
    fn rejects_create_ingestor_mqtt_ack_at_qos_zero() {
        let input = r#"
            CREATE MQTT TOPIC INGESTOR mqtt_notifications
              FROM mqtt_main TOPIC 'notifications' SESSION PERSISTENT QOS 0 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("ACK mode must require QOS 1 or 2");
    }

    #[test]
    fn rejects_create_ingestor_mqtt_no_ack_max() {
        let input = r#"
            CREATE MQTT TOPIC INGESTOR mqtt_notifications
              FROM mqtt_main TOPIC 'notifications' SESSION CLEAN QOS 0 MODE NO ACK MAX 4
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("NO ACK must not accept an in-flight ACK bound");
    }

    #[test]
    fn parses_create_ingestor_nats_no_ack() {
        let input = r#"
            CREATE NATS SUBJECT INGESTOR nats_notifications
              FROM nats_main SUBJECT 'notifications' QUEUE GROUP 'nats_notifications_group' INSTANCES 3 MODE NO ACK
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Nats(NatsIngestor::Subject(NatsSubjectIngestor {
                client: nervix_models::ClientName::try_from("nats_main")
                    .expect("valid client identifier"),
                subject: nervix_models::NatsSubjectName::try_from("notifications")
                    .expect("valid topic identifier"),
                queue_group: nervix_models::NatsQueueGroupName::try_from(
                    "nats_notifications_group"
                )
                .expect("valid queue group identifier"),
                instances: nonzero!(3u64),
                mode: NatsIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Drop,
            }))
        );
    }

    #[test]
    fn rejects_create_ingestor_nats_without_queue_group() {
        let input = r#"
            CREATE NATS SUBJECT INGESTOR nats_notifications
              FROM nats_main SUBJECT 'notifications' MODE NO ACK
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input)).expect_err(
            "NATS SUBJECT INGESTOR i FROM ingestors must require QUEUE GROUP 'before' MODE",
        );
    }

    #[test]
    fn rejects_create_ingestor_nats_zero_instances() {
        let input = r#"
            CREATE NATS SUBJECT INGESTOR nats_notifications
              FROM nats_main SUBJECT 'notifications' QUEUE GROUP 'nats_notifications_group' INSTANCES 0 MODE NO ACK
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let error = parse_create_ingestor_tokens(&tokens)
            .expect_err("NATS SUBJECT INGESTOR i FROM INSTANCES must be greater than zero");
        assert!(
            format!("{error:?}").contains("instances must be greater than 0"),
            "expected instances validation error, got {error:?}"
        );
    }

    #[test]
    fn nats_subject_context_requires_queue_group_before_mode() {
        let input = "CREATE NATS SUBJECT INGESTOR i FROM t SUBJECT 'top' ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"QUEUE GROUP".to_string()));
        assert!(!suggestions.contains(&"INSTANCES".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));
    }

    #[test]
    fn mqtt_mode_context_offers_ack_and_no_ack() {
        let input =
            "CREATE MQTT TOPIC INGESTOR i FROM t TOPIC 'top' SESSION PERSISTENT QOS 1 MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO ACK".to_string()));
        assert!(!suggestions.contains(&"PARALLEL".to_string()));
    }

    #[test]
    fn mqtt_session_context_offers_session_values() {
        let input = "CREATE MQTT TOPIC INGESTOR i FROM t TOPIC 'top' SESSION ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"CLEAN".to_string()));
        assert!(suggestions.contains(&"PERSISTENT".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));
    }

    #[test]
    fn mqtt_subscription_context_requires_session_then_qos_before_mode() {
        let topic = "CREATE MQTT TOPIC INGESTOR i FROM t TOPIC 'top' ";
        let suggestions = suggest_create_ingestor(topic, topic.len());
        assert!(suggestions.contains(&"SESSION".to_string()));
        assert!(!suggestions.contains(&"QOS".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));

        let session = "CREATE MQTT TOPIC INGESTOR i FROM t TOPIC 'top' SESSION CLEAN ";
        let suggestions = suggest_create_ingestor(session, session.len());
        assert!(suggestions.contains(&"QOS".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));
    }

    #[test]
    fn parses_create_ingestor_prometheus_query() {
        let input = r#"
            CREATE PROMETHEUS QUERY INGESTOR prom_samples
              FROM prom_main
              QUERY 'label_replace(vector(42.5), "source", "local", "", "")'
              EVERY 15s
              MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
              TO samples BRANCHED BY source_branch SET source = message.source
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Prometheus(PrometheusIngestor::Query(PrometheusQueryIngestor {
                client: nervix_models::ClientName::try_from("prom_main")
                    .expect("valid client identifier"),
                query: r#"label_replace(vector(42.5), "source", "local", "", "")"#.to_string(),
                max_body_size: None,
                every: "15s"
                    .parse()
                    .assured("the fixture cadence is a positive duration"),
                mode: PrometheusIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            }))
        );
    }

    #[test]
    fn a_prometheus_query_bounds_its_response_between_query_and_every() {
        let parsed = parse_create_ingestor(&ingestor_with_source(
            "PROMETHEUS QUERY INGESTOR i FROM c QUERY 'up' max body size 4mib EVERY 15s MODE NO \
             ACK ON QUIESCE SUSPEND",
        ))
        .expect("a positive whole size between QUERY and EVERY parses");
        let IngestorEntity::Prometheus(PrometheusIngestor::Query(source)) =
            &transport(&parsed).source
        else {
            panic!("the head names a Prometheus query");
        };

        assert_eq!(
            source.max_body_size,
            Some(
                "4MiB"
                    .parse()
                    .assured("the fixture size is a whole number of mebibytes")
            )
        );
    }

    #[test]
    fn a_prometheus_response_bound_is_a_positive_whole_size_written_before_every() {
        for (body, diagnostic) in [
            (
                "QUERY 'up' MAX BODY SIZE 0B EVERY 15s",
                "MAX BODY SIZE must be greater than zero",
            ),
            (
                "QUERY 'up' MAX BODY SIZE 1.5MiB EVERY 15s",
                "MAX BODY SIZE '1.5MiB' must be a whole number",
            ),
            (
                "QUERY 'up' MAX BODY SIZE 16777216TiB EVERY 15s",
                "MAX BODY SIZE '16777216TiB' exceeds the largest size",
            ),
            ("QUERY 'up' MAX BODY SIZE EVERY 15s", "byte_size_literal"),
            ("QUERY 'up' MAX BODY 4MiB EVERY 15s", "SIZE"),
            ("QUERY 'up' EVERY 15s MAX BODY SIZE 4MiB", "found MAX"),
            ("MAX BODY SIZE 4MiB QUERY 'up' EVERY 15s", "QUERY"),
        ] {
            let source = ingestor_with_source(&format!(
                "PROMETHEUS QUERY INGESTOR i FROM c {body} MODE NO ACK ON QUIESCE SUSPEND"
            ));
            let error = parse_create_ingestor(&source).expect_err("the bound must be refused");

            assert!(
                format!("{error:?}").contains(diagnostic),
                "expected `{diagnostic}` for `{body}`, got {error:?}"
            );
        }
    }

    #[test]
    fn prometheus_completion_offers_the_response_bound_once_before_every() {
        let after_query = "CREATE PROMETHEUS QUERY INGESTOR i FROM c QUERY 'up' ";
        let suggestions = suggest_create_ingestor(after_query, after_query.len());
        assert!(
            suggestions.contains(&"MAX BODY SIZE".to_string()),
            "{suggestions:?}"
        );
        assert!(
            suggestions.contains(&"EVERY".to_string()),
            "{suggestions:?}"
        );
        assert!(!suggestions.contains(&"MAX".to_string()), "{suggestions:?}");

        let after_bound =
            "CREATE PROMETHEUS QUERY INGESTOR i FROM c QUERY 'up' MAX BODY SIZE 4MiB ";
        let suggestions = suggest_create_ingestor(after_bound, after_bound.len());
        assert_eq!(suggestions, vec!["EVERY".to_string()]);

        let after_every = "CREATE PROMETHEUS QUERY INGESTOR i FROM c QUERY 'up' EVERY 15s ";
        let suggestions = suggest_create_ingestor(after_every, after_every.len());
        assert!(
            !suggestions.contains(&"MAX BODY SIZE".to_string()),
            "{suggestions:?}"
        );

        let http = "CREATE HTTP REQUEST INGESTOR i FROM c ";
        let suggestions = suggest_create_ingestor(http, http.len());
        assert_eq!(suggestions, vec!["METHOD".to_string()]);
    }

    #[test]
    fn parses_create_ingestor_http_poll() {
        let input = r#"
            CREATE HTTP REQUEST INGESTOR http_notifications
              FROM http_main METHOD 'GET' EVERY 1s
              MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Http(HttpIngestor::Request(HttpRequestIngestor {
                client: nervix_models::ClientName::try_from("http_main")
                    .expect("valid client identifier"),
                method: "GET".to_owned(),
                headers: Vec::new(),
                max_body_size: None,
                every: "1s"
                    .parse()
                    .assured("the fixture cadence is a positive duration"),
                mode: HttpRequestIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            }))
        );
    }

    #[test]
    fn parses_create_ingestor_with_timestamp_at_field() {
        let input = r#"
            CREATE HTTP REQUEST INGESTOR http_notifications
              FROM http_main METHOD 'GET' EVERY 1s
              MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_codec
              TIMESTAMP AT occurred_at
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.timestamp_source,
            Some(IngestTimestampSource::At(
                nervix_models::FieldName::try_from("occurred_at").expect("valid field name")
            ))
        );
    }

    fn timestamp_source_of(clause: &str) -> Option<IngestTimestampSource> {
        let input = format!(
            "CREATE HTTP REQUEST INGESTOR polled FROM http_main METHOD 'GET' EVERY 1s MODE NO ACK \
             ON QUIESCE SUSPEND DECODE USING event_codec {clause} TO events UNBRANCHED FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        let tokens = to_tokens(&input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        parsed.timestamp_source.clone()
    }

    #[test]
    fn parses_create_ingestor_with_timestamp_at_a_metadata_field() {
        let polled_at = nervix_models::FieldName::try_from("polled_at").expect("valid field name");
        assert_eq!(
            timestamp_source_of("TIMESTAMP AT metadata.polled_at"),
            Some(IngestTimestampSource::AtMetadata(polled_at.clone()))
        );
        assert_eq!(
            timestamp_source_of("timestamp at METADATA.polled_at"),
            Some(IngestTimestampSource::AtMetadata(polled_at))
        );
    }

    #[test]
    fn a_payload_field_named_metadata_stays_a_payload_timestamp() {
        assert_eq!(
            timestamp_source_of("TIMESTAMP AT metadata"),
            Some(IngestTimestampSource::At(
                nervix_models::FieldName::try_from("metadata").expect("valid field name")
            ))
        );
    }

    #[test]
    fn rejects_a_metadata_timestamp_without_exactly_one_field() {
        for clause in [
            "TIMESTAMP AT metadata.",
            "TIMESTAMP AT metadata.polled_at.instant",
            "TIMESTAMP AT metadata polled_at",
        ] {
            let input = format!(
                "CREATE HTTP REQUEST INGESTOR polled FROM http_main METHOD 'GET' EVERY 1s MODE NO \
                 ACK ON QUIESCE SUSPEND DECODE USING event_codec {clause} TO events UNBRANCHED \
                 FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            );
            let tokens = to_tokens(&input);
            assert!(
                parse_create_ingestor_tokens(&tokens).is_err(),
                "{clause} names no single metadata field"
            );
        }
    }

    #[test]
    fn suggests_the_metadata_namespace_and_a_field_after_timestamp_at() {
        let head = "CREATE HTTP REQUEST INGESTOR polled FROM http_main METHOD 'GET' EVERY 1s MODE \
                    NO ACK ON QUIESCE SUSPEND DECODE USING event_codec TIMESTAMP AT ";
        let suggestions = suggest_create_ingestor(head, head.len());
        assert!(
            suggestions.contains(&"METADATA".to_string()),
            "{suggestions:?}"
        );
        assert!(
            suggestions.contains(&"field_name".to_string()),
            "{suggestions:?}"
        );
        assert!(!suggestions.contains(&"NOW".to_string()), "{suggestions:?}");
        assert!(!suggestions.contains(&"TO".to_string()), "{suggestions:?}");

        let metadata = format!("{head}metadata.");
        let suggestions = suggest_create_ingestor(&metadata, metadata.len());
        assert!(
            suggestions.contains(&"field_name".to_string()),
            "{suggestions:?}"
        );
        assert!(
            !suggestions.contains(&"METADATA".to_string()),
            "{suggestions:?}"
        );
        assert!(!suggestions.contains(&"TO".to_string()), "{suggestions:?}");
    }

    #[test]
    fn parses_set_timestamp_at_a_metadata_field_in_alter_ingestor() {
        let parsed = parse_alter_ingestor(
            "ALTER HTTP REQUEST INGESTOR polled SET TIMESTAMP AT metadata.polled_at;",
        )
        .expect("parse should succeed");
        let rendered = parsed
            .to_canonical_plexus()
            .expect("the alteration renders");
        assert!(
            rendered.contains("SET TIMESTAMP AT metadata.polled_at"),
            "{rendered}"
        );
    }

    #[test]
    fn parses_create_ingestor_with_timestamp_now() {
        let input = r#"
            CREATE HTTP REQUEST INGESTOR http_notifications
              FROM http_main METHOD 'GET' EVERY 1s
              MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_codec
              TIMESTAMP NOW
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.timestamp_source, Some(IngestTimestampSource::Now));
    }

    #[test]
    fn parses_create_ingestor_http_endpoint() {
        let input = r#"
            CREATE HTTP ENDPOINT INGESTOR http_notifications
              FROM http_notifications_endpoint MODE NO ACK
              ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
                endpoint: nervix_models::EndpointName::try_from("http_notifications_endpoint")
                    .expect("valid endpoint identifier"),
                mode: EndpointIngestMode::NoAck,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_string(),
                },
            }))
        );
    }

    #[test]
    fn parses_create_ingestor_websockets_endpoint() {
        let input = r#"
            CREATE WEBSOCKETS ENDPOINT INGESTOR ws_notifications
              FROM ws_notifications_endpoint MODE NO ACK
              ON QUIESCE REJECT RETRY AFTER 2s DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor(input).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Websockets(WebsocketsIngestor::Endpoint(WebsocketsEndpointIngestor {
                endpoint: nervix_models::EndpointName::try_from("ws_notifications_endpoint")
                    .expect("valid endpoint identifier"),
                mode: EndpointIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Reject {
                    retry_after: "2s".to_string(),
                },
            }))
        );
    }

    #[test]
    fn parses_create_ingestor_websockets_no_ack() {
        let input = r#"
            CREATE WEBSOCKETS CONNECTION INGESTOR ws_notifications
              FROM ws_main MODE NO ACK
              ON QUIESCE DROP DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Websockets(WebsocketsIngestor::Connection(
                WebsocketsConnectionIngestor {
                    headers: Vec::new(),
                    client: nervix_models::ClientName::try_from("ws_main")
                        .expect("valid client identifier"),
                    mode: WebsocketsIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Drop,
                }
            ))
        );
    }

    #[test]
    fn parses_create_ingestor_zeromq_no_ack() {
        let input = r#"
            CREATE ZEROMQ PULL INGESTOR zmq_notifications
              FROM zmq_main MODE NO ACK
              ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::ZeroMq(ZeroMqIngestor::Pull(ZeroMqPullIngestor {
                client: nervix_models::ClientName::try_from("zmq_main")
                    .expect("valid client identifier"),
                mode: ZeroMqIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            }))
        );
    }

    #[test]
    fn parses_create_ingestor_syslog_no_ack() {
        let input = r#"
            CREATE SYSLOG LISTENER INGESTOR syslog_events
              FROM syslog_listener MODE NO ACK
              ON QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST
              DECODE USING syslog_codec
              TO events INHERIT ALL UNBRANCHED
              FLUSH IMMEDIATE ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor(input).expect("parse should succeed");
        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Syslog(SyslogIngestor::Listener(SyslogListenerIngestor {
                client: nervix_models::ClientName::try_from("syslog_listener")
                    .expect("valid client identifier"),
                mode: SyslogIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Buffer {
                    max_size: "1MiB".to_string(),
                    overflow: IngestQuiesceOverflow::DropOldest,
                },
            }))
        );
    }

    #[test]
    fn rejects_syslog_instances_and_acknowledgement() {
        assert!(
            parse_create_ingestor(
                "CREATE SYSLOG LISTENER INGESTOR i FROM c INSTANCES 2 MODE NO ACK ON QUIESCE \
                 SUSPEND DECODE USING codec TO out INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            )
            .is_err()
        );
        assert!(
            parse_create_ingestor(
                "CREATE SYSLOG LISTENER INGESTOR i FROM c MODE ACK SEQUENTIAL ACK TIMEOUT 1s \
                 RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND DECODE USING codec TO out \
                 INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            )
            .is_err()
        );
    }

    #[test]
    fn syslog_completion_offers_only_fixed_delivery_mode() {
        let input = "CREATE SYSLOG LISTENER INGESTOR i FROM c MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"NO ACK".to_string()));
        assert!(!suggestions.contains(&"ACK".to_string()));
        assert!(!suggestions.contains(&"PARALLEL".to_string()));
    }

    #[test]
    fn parses_create_ingestor_sqs_ack_sequential() {
        let input = r#"
            CREATE SQS QUEUE INGESTOR sqs_notifications
              FROM sqs_main QUEUE 'notifications' MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m
              ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            transport(&parsed).source,
            IngestorEntity::Sqs(SqsIngestor::Queue(SqsQueueIngestor {
                client: nervix_models::ClientName::try_from("sqs_main")
                    .expect("valid client identifier"),
                queue: nervix_models::SqsQueueName::try_from("notifications")
                    .expect("valid queue identifier"),
                instances: nonzero!(1u64),
                mode: SqsIngestMode::AckSequential {
                    timeout: "45s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "2s".to_string(),
                        max_backoff: "1m".to_string(),
                    },
                },
                quiesce: IngestQuiesceMode::Suspend,
            }))
        );
    }

    #[test]
    fn parses_create_ingestor_kafka_instances() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR i
              FROM t TOPIC 'top' OFFSET BY CONSUMER GROUP 'g' INSTANCES 3 MODE NO ACK
              ON QUIESCE SUSPEND DECODE USING sch
              TO s BRANCHED BY u_branch SET u = message.u
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor { instances, .. })) =
            transport(&parsed).source.clone()
        else {
            panic!("expected kafka ingestor source");
        };
        assert_eq!(instances, nonzero!(3u64));
    }

    #[test]
    fn parses_create_ingestor_rabbitmq_instances() {
        let input = r#"
            CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
              FROM rabbit_main QUEUE 'notifications' INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s
              ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestorEntity::RabbitMq(RabbitMqIngestor::Queue(RabbitMqQueueIngestor {
            instances,
            ..
        })) = transport(&parsed).source.clone()
        else {
            panic!("expected rabbitmq ingestor source");
        };
        assert_eq!(instances, nonzero!(2u64));
    }

    #[test]
    fn parses_create_ingestor_sqs_instances() {
        let input = r#"
            CREATE SQS QUEUE INGESTOR sqs_notifications
              FROM sqs_main QUEUE 'notifications' INSTANCES 4 MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m
              ON QUIESCE SUSPEND DECODE USING notification_codec
              TO notifications BRANCHED BY user_id_branch SET user_id = message.user_id
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestorEntity::Sqs(SqsIngestor::Queue(SqsQueueIngestor { instances, .. })) =
            transport(&parsed).source.clone()
        else {
            panic!("expected sqs ingestor source");
        };
        assert_eq!(instances, nonzero!(4u64));
    }

    #[test]
    fn rejects_zero_instances() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR i
              FROM t TOPIC 'top' OFFSET BY CONSUMER GROUP 'g' INSTANCES 0 MODE NO ACK
              ON QUIESCE SUSPEND DECODE USING sch
              TO s BRANCHED BY u_branch SET u = message.u
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let error = parse_create_ingestor_tokens(&tokens).expect_err("parse should fail");
        assert!(
            format!("{error:?}").contains("instances must be greater than 0"),
            "expected instances validation error, got {error:?}"
        );
    }

    #[test]
    fn parses_ingestor_with_filter_where_and_output_routes() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR kafka_notifications
              FROM kafka_main TOPIC 'notifications' OFFSET BY CONSUMER GROUP 'nervix_consumer' MODE NO ACK
              ON QUIESCE SUSPEND DECODE USING notification_kafka_message
              FILTER WHERE message.active
              TO notifications
                SET normalized = lower(message.name), total = message.amount AS INT64
                WHERE message.kind = "audit"
                BRANCHED BY user_id_branch SET user_id = message.user_id
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG
              TO audit_notifications INHERIT ALL
                BRANCHED BY user_id_branch SET user_id = message.user_id
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor(input).expect("parse should succeed");

        assert_eq!(
            parsed.filter_where,
            Some(crate::parse_expression("message.active").expect("valid expression"))
        );
        assert_eq!(parsed.output_routes.routes.len(), 2);
        assert_eq!(
            parsed.output_routes.routes[0].relay.as_str(),
            "notifications"
        );
        assert_eq!(
            parsed.output_routes.routes[0]
                .construction
                .assignments
                .len(),
            2
        );
        assert!(
            parsed.output_routes.routes[0]
                .construction
                .where_clause
                .is_some()
        );
        assert_eq!(
            parsed.output_routes.routes[1].relay.as_str(),
            "audit_notifications"
        );
        assert_eq!(
            parsed.output_routes.routes[1].construction.inherit,
            Some(nervix_models::Inheritance::All)
        );
    }

    #[test]
    fn rejects_invalid_ingestor_output_route_program() {
        let input = r#"
            CREATE KAFKA TOPIC INGESTOR kafka_notifications
              FROM kafka_main TOPIC 'notifications' OFFSET BY CONSUMER GROUP 'nervix_consumer'
              MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_kafka_message
              TO notifications UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                SET notifications.normalized = ON MESSAGE ERROR LOG
              ON GENERAL ERROR LOG;
        "#;

        let error = parse_create_ingestor(input).expect_err("parse should fail");
        match error.current_context() {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    /// The body a client producer ingestor writes after `FROM`.
    const CLIENT_BODY: &str = "SCHEMA event MODE ACK PARALLEL MAX 64 ACK TIMEOUT 30s RETRY POLICY \
                               BACKOFF 100ms MAX 5s ON QUIESCE SUSPEND";

    /// The head, the name and the body of a client producer ingestor.
    fn client_entity() -> String {
        format!("CLIENT PRODUCER INGESTOR submit_events FROM {CLIENT_BODY}")
    }

    /// A whole statement around `source`, which names its head, the ingestor's name and `FROM`
    /// its body.
    fn client_ingestor(source: &str) -> String {
        format!(
            "CREATE {source} TIMESTAMP NOW TO events INHERIT ALL UNBRANCHED FLUSH EACH 10ms MAX \
             BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    #[test]
    fn parses_a_client_source_without_a_codec() {
        let parsed = parse_create_ingestor(&client_ingestor(&client_entity()))
            .expect("a client source should parse");
        assert_eq!(
            parsed.input,
            IngestorInput::Client(ClientProducerIngestor {
                schema: SchemaName::parse("event").expect("valid schema name"),
                mode: ClientIngestMode {
                    window: nervix_models::AckWindow::Parallel {
                        max: nonzero!(64u64),
                    },
                    ack_timeout: "30s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "100ms".to_string(),
                        max_backoff: "5s".to_string(),
                    },
                },
            })
        );
        assert_eq!(parsed.timestamp_source, Some(IngestTimestampSource::Now));
        assert_eq!(parsed.input.codec(), None);

        let sequential = parse_create_ingestor(&client_ingestor(
            "client producer ingestor submit_events from schema event mode ack sequential ack \
             timeout 5s retry policy backoff 1s max 2s on quiesce suspend",
        ))
        .expect("keywords are case-insensitive and ACK SEQUENTIAL is a client window");
        let IngestorInput::Client(source) = &sequential.input else {
            panic!("the statement declares a client source");
        };
        assert_eq!(source.mode.window, nervix_models::AckWindow::Sequential);
    }

    #[test]
    fn rejects_client_sources_outside_their_contract() {
        let invalid_sources = [
            // A client source decodes nothing, so it names no codec.
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 5s \
             RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND DECODE USING event_codec",
            // Suspension is the only quiesce mode a client source honors.
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 5s \
             RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP \
             OLDEST",
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 5s \
             RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE DROP",
            // The mode acknowledges and states both its timeout and its whole retry policy.
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE NO ACK ON QUIESCE SUSPEND",
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK SEQUENTIAL RETRY POLICY \
             BACKOFF 1s MAX 2s ON QUIESCE SUSPEND",
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK PARALLEL MAX 4 ACK TIMEOUT 5s \
             ON QUIESCE SUSPEND",
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK PARALLEL MAX 0 ACK TIMEOUT 5s \
             RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND",
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK PARALLEL MAX 4 BATCH TIMEOUT \
             1s ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND",
            // One scheduled execution serves every producer, and the quiesce clause is required.
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event INSTANCES 2 MODE ACK SEQUENTIAL ACK \
             TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND",
            "CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 5s \
             RETRY POLICY BACKOFF 1s MAX 2s",
            // The body names its schema after SCHEMA.
            "CLIENT PRODUCER INGESTOR i FROM event MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY \
             POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND",
        ];
        for source in invalid_sources {
            assert!(
                parse_create_ingestor(&client_ingestor(source)).is_err(),
                "source `{source}` should be rejected"
            );
        }
        // A transport still requires its codec.
        assert!(
            parse_create_ingestor(
                "CREATE KAFKA TOPIC INGESTOR i FROM c TOPIC 't' OFFSET BY DOMAIN MODE NO ACK ON \
                 QUIESCE SUSPEND TO s UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL \
                 ERROR LOG;"
            )
            .is_err()
        );
    }

    #[test]
    fn parses_set_from_client_as_its_own_alteration() {
        let parsed = parse_alter_ingestor(&format!(
            "ALTER CLIENT PRODUCER INGESTOR submit_events SET FROM {CLIENT_BODY}, SET QUIESCE \
             SUSPEND;"
        ))
        .expect("SET FROM of a client producer should parse");
        let IngestorAlteration::Client(ClientIngestorAlteration::Producer(operations)) =
            &parsed.alteration
        else {
            panic!("the head names a client producer");
        };
        let [
            AlterIngestorOperation::SetSource { source },
            AlterIngestorOperation::SetQuiesce { quiesce },
        ] = operations.as_slice()
        else {
            panic!("expected a client source and a quiesce operation");
        };
        assert_eq!(source.schema.as_str(), "event");
        assert_eq!(quiesce, &IngestQuiesceMode::Suspend);
    }

    #[test]
    fn client_source_completion_follows_its_own_grammar() {
        let after_from = "CREATE CLIENT PRODUCER INGESTOR i FROM ";
        assert_eq!(
            suggest_create_ingestor(after_from, after_from.len()),
            ["SCHEMA".to_string()]
        );

        let after_mode = "CREATE CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ";
        assert_eq!(
            suggest_create_ingestor(after_mode, after_mode.len()),
            ["ACK".to_string()]
        );

        let after_ack = "CREATE CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK ";
        let window = suggest_create_ingestor(after_ack, after_ack.len());
        assert!(window.contains(&"SEQUENTIAL".to_string()));
        assert!(window.contains(&"PARALLEL".to_string()));

        let after_window =
            "CREATE CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK PARALLEL MAX 4 ";
        assert_eq!(
            suggest_create_ingestor(after_window, after_window.len()),
            ["ACK TIMEOUT".to_string()]
        );

        let quiesce = "CREATE CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK SEQUENTIAL \
                       ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE ";
        assert_eq!(
            suggest_create_ingestor(quiesce, quiesce.len()),
            ["SUSPEND".to_string()]
        );

        let after_source = "CREATE CLIENT PRODUCER INGESTOR i FROM SCHEMA event MODE ACK \
                            SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE \
                            SUSPEND ";
        let next = suggest_create_ingestor(after_source, after_source.len());
        assert!(next.contains(&"TIMESTAMP".to_string()));
        assert!(next.contains(&"TO".to_string()));
        assert!(!next.contains(&"DECODE USING".to_string()));

        let alter = "ALTER CLIENT PRODUCER INGESTOR i SET FROM ";
        assert_eq!(
            suggest_alter_ingestor(alter, alter.len()),
            ["SCHEMA".to_string()]
        );
    }
}
