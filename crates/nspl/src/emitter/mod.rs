//! Plexus emitter statement grammar.
//!
//! Layer: language.
//! - **Owns.** Shared emitter statement construction, alteration, batching and error clauses.
//! - **Depends on.** Shared grammar primitives and each connector's destination grammar.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

mod kafka;
use kafka::kafka_emit_sink_parser;
mod client;
use client::{client_emit_sink_parser, native_client_sink};
mod pulsar;
use pulsar::pulsar_emit_sink_parser;
mod mqtt;
use mqtt::{mqtt_emit_sink_parser, mqtt_publishing_mode};
mod nats;
use nats::{nats_emit_sink_parser, nats_jetstream_publishing_mode, nats_publishing_mode};
mod rabbitmq;
use rabbitmq::rabbitmq_emit_sink_parser;
mod redis;
use redis::redis_emit_sink_parser;
mod zeromq;
use zeromq::zeromq_emit_sink_parser;
mod syslog;
use syslog::syslog_emit_sink_parser;
mod sqs;
use sqs::{sqs_emit_sink_parser, sqs_publishing_mode};
mod sentry;
use sentry::sentry_emit_sink_parser;
mod otel;
use otel::otel_emit_sink_parser;
mod clickhouse;
use clickhouse::clickhouse_emit_sink_parser;
mod postgres;
use postgres::postgres_emit_sink_parser;
mod mysql;
use mysql::mysql_emit_sink_parser;
mod mongodb;
use mongodb::mongodb_emit_sink_parser;
mod iceberg;
use iceberg::{iceberg_commit_each, iceberg_emit_sink_parser};
mod http;
use std::borrow::Cow;

use http::{http_body_selection, http_create_sink_parser, http_sink_parser};

use ahash_compile_time::HashSet;
use chumsky::{error::LabelError, prelude::*, util::MaybeRef};
use meticulous::OptionExt as _;
use nervix_models::{
    AckMode, AlterEmitter, AlterEmitterOperation, BatchMessageLimit, ClickHouseSink,
    ClickHouseTableSink, ClickHouseValueMapping, ClientConsumerSink, ClientSink, CodecName,
    CreateEmitter, CreateStatement, EmitSink, EmitterBatchPolicy, EmitterBody, EmitterName,
    EmitterPublishingMode, HttpRequestSink, HttpSink, IcebergCatalog, IcebergSink,
    IcebergStorageBackend, IcebergTableSink, IcebergValueMapping, KafkaSink, KafkaTopicSink,
    MaterializedStateDependency, MongoDbCollectionSink, MongoDbConflictAction, MongoDbSink,
    MqttSink, MqttTopicSink, MySqlConflictAction, MySqlSink, MySqlTableSink, NatsSink,
    NatsSubjectSink, OtelAggregationTemporality, OtelLogsSink, OtelMappings, OtelMetric,
    OtelMetricKind, OtelMetricsSink, OtelScope, OtelTracesSink, PayloadSizeLimit,
    PostgresConflictAction, PostgresSink, PostgresTableSink, ProcessorInputs, PulsarSink,
    PulsarTopicSink, RabbitMqQueueSink, RabbitMqSink, RedisPubSubSink, RedisSink, SentryEventSink,
    SentrySink, SqsFifoGroup, SqsQueueSink, SqsSink, SyslogCollectorSink, SyslogSink,
    ZeroMqPushSink, ZeroMqSink,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, ack_mode, ack_timeout, ack_window,
        alter_op_separator, bodyless_route_construction, boxed_choice, byte_size_lit, channel_ref,
        client_ref, codec_ref, collect_for, collection_ref, duration_lit, embedded_expression,
        emitter_name, emitter_ref, flush_each, from_relay_clauses, general_error_policy,
        if_not_exists_clause, into_parse_error, kw, kw_phrase2, kw_phrase3, lex_input,
        materialized_state_dependencies, message_error_policy, queue_ref, relay_ref, retry_policy,
        route_construction, schema_ref, string_lit, subject_ref, suggest_from, table_ref, tok,
        topic_ref, u64_value, where_expression, where_only_route_construction, word_raw,
    },
};

fn no_ack_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(retry_policy())
        .map(|retry_policy| EmitterPublishingMode::NoAck { retry_policy })
        .boxed()
}

fn broker_ack_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(ack_window())
        .then(ack_timeout())
        .then(retry_policy())
        .map(
            |((window, ack_timeout), retry_policy)| EmitterPublishingMode::BrokerAck {
                window,
                ack_timeout,
                retry_policy,
            },
        )
        .boxed()
}

fn client_ack_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(ack_window())
        .then(ack_timeout())
        .then(retry_policy())
        .map(
            |((window, ack_timeout), retry_policy)| EmitterPublishingMode::ClientAck {
                window,
                ack_timeout,
                retry_policy,
            },
        )
        .boxed()
}

fn broker_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((no_ack_publishing_mode(), broker_ack_publishing_mode())).boxed()
}

fn request_ack_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(retry_policy())
        .map(|retry_policy| EmitterPublishingMode::RequestAck { retry_policy })
        .boxed()
}

fn any_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        no_ack_publishing_mode(),
        broker_ack_publishing_mode(),
        mqtt_publishing_mode(),
        nats_jetstream_publishing_mode(),
        sqs_publishing_mode(),
        request_ack_publishing_mode(),
    )
}

fn clickhouse_value_expr<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::Expression, extra::Err<ParseError<'src>>> + Clone
{
    embedded_expression("value_expression")
}

fn clickhouse_value_mapping<'src>()
-> impl Parser<'src, &'src [Token], ClickHouseValueMapping, extra::Err<ParseError<'src>>> + Clone {
    string_lit()
        .labelled("column_name")
        .then_ignore(tok(Token::Eq))
        .then(clickhouse_value_expr())
        .map(|(column, expression)| ClickHouseValueMapping { column, expression })
}

fn clickhouse_values<'src>()
-> impl Parser<'src, &'src [Token], Vec<ClickHouseValueMapping>, extra::Err<ParseError<'src>>> + Clone
{
    clickhouse_value_mapping()
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LBrace), tok(Token::RBrace))
}

/// `BATCH MAX MESSAGES <n> MAX SIZE <bytes>`, the two hard limits of every batch an emitter
/// publishes.
///
/// Both limits are checked where they are written: a count outside `1..=65536`, a size of zero,
/// a fractional size and a size past the 64-bit byte range are rejected here rather than narrowed,
/// rounded or saturated later.
fn batch_clause<'src>()
-> impl Parser<'src, &'src [Token], EmitterBatchPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Identifier::Batch, Identifier::Max, Identifier::Messages)
        .ignore_then(u64_value("message_count").try_map(|value, span| {
            BatchMessageLimit::try_from(value)
                .map_err(|error| Rich::custom(span, error.to_string()))
        }))
        .then_ignore(kw_phrase2(Identifier::Max, Identifier::Size))
        .then(byte_size_lit().try_map(|literal, span| {
            literal
                .parse::<PayloadSizeLimit>()
                .map_err(|error| Rich::custom(span, error.to_string()))
        }))
        .map(|(max_messages, max_size)| EmitterBatchPolicy {
            max_messages,
            max_size,
        })
        .boxed()
}

#[derive(Clone, Copy)]
enum ConflictVerb {
    DoNothing,
    DoUpdate,
}

fn conflict_target<'src>()
-> impl Parser<'src, &'src [Token], Vec<String>, extra::Err<ParseError<'src>>> + Clone {
    string_lit()
        .labelled("column_name")
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LParen), tok(Token::RParen))
}

fn token_is_keyword(token: &Token, expected: Identifier) -> bool {
    if let Token::Word(Word::KnownWord { iden, .. }) = token {
        *iden == expected
    } else {
        false
    }
}

fn expected_label_error<'src>(
    label: &'static str,
    found: Option<MaybeRef<'src, Token>>,
    span: <&'src [Token] as chumsky::input::Input<'src>>::Span,
) -> ParseError<'src> {
    <ParseError<'src> as LabelError<
        'src,
        &'src [Token],
        chumsky::error::RichPattern<'src, Token>,
    >>::expected_found(
        [chumsky::error::RichPattern::Label(Cow::Borrowed(label))],
        found,
        span,
    )
}

/// `ON CONFLICT`, matched as one unit.
///
/// The expectation is always the whole phrase, never a bare `ON`: a lone `ON` is not something the
/// user can type here, and offering it sends them into a statement that cannot be completed.
fn on_conflict_phrase<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    custom(|inp| {
        let before = inp.save();
        let start = inp.cursor();
        let first = inp.next_maybe();
        let first_span = inp.span_since(&start);
        let Some(first_token) = first.as_deref() else {
            return Err(expected_label_error("ON CONFLICT", first, first_span));
        };

        if !token_is_keyword(first_token, Identifier::On) {
            inp.rewind(before);
            return Err(expected_label_error("ON CONFLICT", first, first_span));
        }

        let second = inp.next_maybe();
        let Some(second_token) = second.as_deref() else {
            inp.rewind(before);
            return Err(expected_label_error("ON CONFLICT", first, first_span));
        };

        if !token_is_keyword(second_token, Identifier::Conflict) {
            inp.rewind(before);
            return Err(expected_label_error("ON CONFLICT", first, first_span));
        }

        Ok(())
    })
}

/// `ENCODE USING <codec>`, written after the sink it encodes for.
///
/// Whether a codec is required, optional or meaningless is a property of the sink, so the clause
/// belongs to the sink clause rather than ahead of it. Written before `TO`, the grammar cannot know
/// at `TO` which sinks are still reachable, and offers sinks that can never complete. This also
/// matches ingestors, which already read `FROM <source> DECODE USING <codec>`.
fn encode_using_clause<'src>()
-> impl Parser<'src, &'src [Token], CodecName, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Encode, Identifier::Using)
        .ignore_then(codec_ref())
        .boxed()
}

/// A sink together with the publishing mode written after it, which every sink accepts.
struct SinkWithPublishingMode {
    sink: EmitSink,
    publishing_mode: EmitterPublishingMode,
}

fn sink_with_publishing_mode<'src>(
    sink: impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone + 'src,
    mode: impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>>
    + Clone
    + 'src,
) -> impl Parser<'src, &'src [Token], SinkWithPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    sink.then_ignore(kw(Identifier::Mode))
        .then(mode)
        .map(|(sink, publishing_mode)| SinkWithPublishingMode {
            sink,
            publishing_mode,
        })
        .boxed()
}

/// A sink that writes an encoded payload, and so requires a codec and supports transforming route
/// construction.
fn encoded_sink<'src>(
    sink: impl Parser<'src, &'src [Token], SinkWithPublishingMode, extra::Err<ParseError<'src>>>
    + Clone
    + 'src,
) -> impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone {
    sink.then(encode_using_clause())
        .then(route_construction().or_not())
        .map(|((sink, codec), construction)| ParsedSink {
            sink: sink.sink,
            publishing_mode: sink.publishing_mode,
            body: EmitterBody::Codec { codec },
            construction,
            batch: None,
        })
        .boxed()
}

/// A direct sink takes no codec and its `VALUES` mapping owns output construction, leaving only a
/// route-local `WHERE` clause available here.
fn codec_free_sink<'src>(
    sink: impl Parser<'src, &'src [Token], SinkWithPublishingMode, extra::Err<ParseError<'src>>>
    + Clone
    + 'src,
) -> impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone {
    sink.then(where_only_route_construction().or_not())
        .map(|(sink, construction)| ParsedSink {
            sink: sink.sink,
            publishing_mode: sink.publishing_mode,
            body: EmitterBody::Values,
            construction,
            batch: None,
        })
        .boxed()
}

/// A complete sink clause followed by the batching clause its sink cannot do without.
fn batch_required<'src>(
    sink: impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone + 'src,
) -> impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone {
    sink.then(batch_clause())
        .map(|(sink, batch)| ParsedSink {
            batch: Some(batch),
            ..sink
        })
        .boxed()
}

/// A complete sink clause optionally followed by the batching clause.
fn batch_optional<'src>(
    sink: impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone + 'src,
) -> impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone {
    sink.then(batch_clause().or_not())
        .map(|(sink, batch)| ParsedSink { batch, ..sink })
        .boxed()
}

/// A parsed sink together with the route surface that sink supports.
struct ParsedSink {
    sink: EmitSink,
    publishing_mode: EmitterPublishingMode,
    body: EmitterBody,
    construction: Option<nervix_models::RouteConstruction>,
    batch: Option<EmitterBatchPolicy>,
}

fn emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        http_create_sink_parser(),
        batch_required(native_client_sink()),
        batch_optional(codec_free_sink(sink_with_publishing_mode(
            otel_emit_sink_parser(),
            request_ack_publishing_mode(),
        ))),
        batch_required(codec_free_sink(sink_with_publishing_mode(
            clickhouse_emit_sink_parser(),
            request_ack_publishing_mode(),
        ))),
        batch_required(codec_free_sink(sink_with_publishing_mode(
            postgres_emit_sink_parser(),
            request_ack_publishing_mode(),
        ))),
        batch_required(codec_free_sink(sink_with_publishing_mode(
            mysql_emit_sink_parser(),
            request_ack_publishing_mode(),
        ))),
        batch_required(codec_free_sink(sink_with_publishing_mode(
            mongodb_emit_sink_parser(),
            request_ack_publishing_mode(),
        ))),
        batch_optional(codec_free_sink(sink_with_publishing_mode(
            iceberg_emit_sink_parser(),
            request_ack_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            kafka_emit_sink_parser(),
            broker_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            pulsar_emit_sink_parser(),
            broker_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            rabbitmq_emit_sink_parser(),
            broker_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            redis_emit_sink_parser(),
            no_ack_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            mqtt_emit_sink_parser(),
            mqtt_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            nats_emit_sink_parser(),
            nats_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            zeromq_emit_sink_parser(),
            no_ack_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            syslog_emit_sink_parser(),
            no_ack_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            sqs_emit_sink_parser(),
            sqs_publishing_mode(),
        ))),
        batch_optional(encoded_sink(sink_with_publishing_mode(
            sentry_emit_sink_parser(),
            request_ack_publishing_mode(),
        ))),
    )
}

/// The complete sink and publishing mode for `ALTER EMITTER ... SET TO <sink>`, which replaces the
/// destination and its complete publishing contract while leaving the route codec and flush policy
/// in place.
fn alter_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], SinkWithPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        sink_with_publishing_mode(client_emit_sink_parser(), client_ack_publishing_mode()),
        sink_with_publishing_mode(otel_emit_sink_parser(), request_ack_publishing_mode()),
        sink_with_publishing_mode(clickhouse_emit_sink_parser(), request_ack_publishing_mode()),
        sink_with_publishing_mode(postgres_emit_sink_parser(), request_ack_publishing_mode()),
        sink_with_publishing_mode(mysql_emit_sink_parser(), request_ack_publishing_mode()),
        sink_with_publishing_mode(mongodb_emit_sink_parser(), request_ack_publishing_mode()),
        sink_with_publishing_mode(iceberg_emit_sink_parser(), request_ack_publishing_mode()),
        sink_with_publishing_mode(kafka_emit_sink_parser(), broker_publishing_mode()),
        sink_with_publishing_mode(pulsar_emit_sink_parser(), broker_publishing_mode()),
        sink_with_publishing_mode(rabbitmq_emit_sink_parser(), broker_publishing_mode()),
        sink_with_publishing_mode(redis_emit_sink_parser(), no_ack_publishing_mode()),
        sink_with_publishing_mode(mqtt_emit_sink_parser(), mqtt_publishing_mode()),
        sink_with_publishing_mode(nats_emit_sink_parser(), nats_publishing_mode()),
        sink_with_publishing_mode(zeromq_emit_sink_parser(), no_ack_publishing_mode()),
        sink_with_publishing_mode(syslog_emit_sink_parser(), no_ack_publishing_mode()),
        sink_with_publishing_mode(sqs_emit_sink_parser(), sqs_publishing_mode()),
        sink_with_publishing_mode(sentry_emit_sink_parser(), request_ack_publishing_mode()),
    )
}

pub fn alter_emitter_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterEmitter, extra::Err<ParseError<'src>>> + Clone {
    let add_from = kw(Identifier::Add)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .then(where_expression().or_not())
        .map(|(relay, where_clause)| AlterEmitterOperation::AddFrom {
            relay,
            where_clause,
        });
    let drop_from = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .map(|relay| AlterEmitterOperation::DropFrom { relay });
    let alter_from = kw(Identifier::Alter)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .then(choice((
            kw(Identifier::Set)
                .ignore_then(where_expression())
                .map(Some),
            kw(Identifier::Drop)
                .ignore_then(kw(Identifier::Where))
                .to(None),
        )))
        .map(|(relay, where_clause)| match where_clause {
            Some(where_clause) => AlterEmitterOperation::AlterFromSetWhere {
                relay,
                where_clause,
            },
            None => AlterEmitterOperation::AlterFromDropWhere { relay },
        });
    let set_sink = kw(Identifier::Set)
        .ignore_then(kw(Identifier::To))
        .ignore_then(alter_emit_sink_parser())
        .map(|sink| {
            let body = match &sink.sink {
                EmitSink::Client(_) => Some(EmitterBody::Client),
                _ => None,
            };
            AlterEmitterOperation::SetSink {
                sink: Box::new(sink.sink),
                publishing_mode: sink.publishing_mode,
                body,
            }
        });
    let set_http_sink = kw(Identifier::Set)
        .ignore_then(kw(Identifier::To))
        .ignore_then(http_sink_parser())
        .then(http_body_selection())
        .map(|(sink, body)| AlterEmitterOperation::SetSink {
            sink: Box::new(sink.sink),
            publishing_mode: sink.publishing_mode,
            body: Some(body),
        });
    let set_client = kw(Identifier::Set)
        .ignore_then(kw(Identifier::Client))
        .ignore_then(client_ref())
        .map(|client| AlterEmitterOperation::SetClient { client });
    let set_encode = kw(Identifier::Set)
        .ignore_then(kw_phrase2(Identifier::Encode, Identifier::Using))
        .ignore_then(codec_ref())
        .map(|codec| AlterEmitterOperation::SetEncodeUsing { codec });
    let drop_encode = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Encode))
        .to(AlterEmitterOperation::DropEncode);
    let set_collect = kw(Identifier::Set)
        .ignore_then(collect_for())
        .map(|policy| AlterEmitterOperation::SetCollect { policy });
    let drop_collect = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Collect))
        .to(AlterEmitterOperation::DropCollect);
    let set_attachment = kw(Identifier::Set)
        .ignore_then(ack_mode())
        .map(|mode| AlterEmitterOperation::SetAttachment { mode });
    let set_publishing_mode = kw_phrase2(Identifier::Set, Identifier::Mode)
        .ignore_then(any_publishing_mode())
        .map(|mode| AlterEmitterOperation::SetPublishingMode { mode });
    let set_batch = kw(Identifier::Set)
        .ignore_then(batch_clause())
        .map(|policy| AlterEmitterOperation::SetBatch { policy });
    let drop_batch = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Batch))
        .to(AlterEmitterOperation::DropBatch);
    let set_flush = kw(Identifier::Set)
        .ignore_then(flush_each())
        .map(|flush_policy| AlterEmitterOperation::SetFlush { flush_policy });
    let set_commit = kw(Identifier::Set).ignore_then(iceberg_commit_each()).map(
        |(commit_each, max_commit_size)| AlterEmitterOperation::SetCommit {
            commit_each,
            max_commit_size,
        },
    );
    let operation = choice((
        add_from,
        drop_from,
        alter_from,
        set_sink,
        set_http_sink,
        set_client,
        set_encode,
        drop_encode,
        set_collect,
        drop_collect,
        set_attachment,
        set_publishing_mode,
        set_batch,
        drop_batch,
        set_flush,
        set_commit,
    ))
    .boxed();

    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Emitter))
        .ignore_then(emitter_ref())
        .then(
            operation
                .separated_by(alter_op_separator())
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(emitter, operations)| AlterEmitter {
            emitter,
            operations,
        })
        .boxed()
}

/// Everything `CREATE EMITTER` states before its flush and error clauses, gathered so the tail of
/// the grammar threads one named value instead of a tuple that grows with each clause.
struct EmitterHead {
    if_not_exists: bool,
    mode: Option<AckMode>,
    name: EmitterName,
    from: ProcessorInputs,
    materialized_state: Vec<MaterializedStateDependency>,
    sink: EmitSink,
    publishing_mode: EmitterPublishingMode,
    body: EmitterBody,
    construction: Option<nervix_models::RouteConstruction>,
    batch: Option<EmitterBatchPolicy>,
}

pub fn create_emitter_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateEmitter>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Emitter))
        .then(emitter_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .boxed()
        .then(materialized_state_dependencies())
        .then_ignore(kw(Identifier::To))
        .then(emit_sink_parser())
        .map(|((head, materialized_state), parsed_sink)| {
            let (((if_not_exists, mode), name), from) = head;
            EmitterHead {
                if_not_exists,
                mode,
                name,
                from,
                materialized_state,
                sink: parsed_sink.sink,
                publishing_mode: parsed_sink.publishing_mode,
                body: parsed_sink.body,
                construction: parsed_sink.construction,
                batch: parsed_sink.batch,
            }
        })
        .boxed()
        .then(flush_each())
        .boxed()
        .then(message_error_policy())
        .then(general_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(((head, flush_policy), message_error_policy), general_error_policy)| {
                let EmitterHead {
                    if_not_exists,
                    mode,
                    name,
                    from,
                    materialized_state,
                    sink,
                    publishing_mode,
                    body,
                    construction,
                    batch,
                } = head;
                CreateStatement::new(
                    CreateEmitter {
                        name,
                        from,
                        body,
                        sink: Box::new(sink),
                        batch,
                        flush_policy,
                        error_policies: nervix_models::ErrorPolicies {
                            message: message_error_policy,
                            general: general_error_policy,
                        },
                        publishing_mode,
                        mode: mode.unwrap_or(AckMode::Attached),
                        construction: construction.unwrap_or_default(),
                        materialized_state,
                    },
                    if_not_exists,
                )
            },
        )
        .boxed()
}

pub fn parse_create_emitter_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateEmitter>, Vec<ParseError<'_>>> {
    let out = create_emitter_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_alter_emitter_tokens(tokens: &[Token]) -> Result<AlterEmitter, Vec<ParseError<'_>>> {
    let out = alter_emitter_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_create_emitter(
    input: &str,
) -> error_stack::Result<CreateStatement<CreateEmitter>, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_create_emitter_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_alter_emitter(input: &str) -> error_stack::Result<AlterEmitter, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_alter_emitter_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_emitter(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_emitter_parser())
}

pub fn suggest_alter_emitter(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, alter_emitter_parser())
}

#[cfg(test)]
mod conformance_tests;

#[cfg(test)]
mod tests;
