use std::borrow::Cow;

use chumsky::{error::LabelError, prelude::*, util::MaybeRef};
use nervix_models::{
    AckMode, AlterEmitter, AlterEmitterOperation, ClickHouseValueMapping, CreateEmitter,
    CreateStatement, EmitSink, EmitterPublishingMode, IcebergCatalog, IcebergStorageBackend,
    IcebergValueMapping, MongoDbConflictAction, MySqlConflictAction, OtelAggregationTemporality,
    OtelMetric, OtelMetricKind, OtelScope, OtelSignal, PostgresConflictAction, SqsFifoGroup,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, ack_timeout, alter_op_separator, boxed_choice,
        byte_size_lit, channel_ref, client_ref, codec_ref, collect_for, duration_lit,
        emitter_ack_window, emitter_name, emitter_ref, flush_each, from_relay_clauses,
        general_error_policy, if_not_exists_clause, into_parse_error, kw, kw_phrase2, kw_phrase3,
        lex_input, materialized_state_dependencies, message_error_policy, queue_ref, relay_ref,
        render_vm_program_tokens, retry_policy, route_construction, string_lit, suggest_from,
        table_ref, tok, topic_ref, where_expression, where_only_route_construction, word_raw,
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
        .ignore_then(emitter_ack_window())
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

fn broker_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((no_ack_publishing_mode(), broker_ack_publishing_mode())).boxed()
}

fn mqtt_qos_level<'src>(
    expected: &'static str,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Qos)
        .ignore_then(
            select! { Token::NumberLiteral(raw) => raw }
                .try_map(move |raw, span| {
                    if raw == expected {
                        Ok(())
                    } else {
                        Err(Rich::custom(span, "MQTT emitter QOS must be 0, 1, or 2"))
                    }
                })
                .labelled("mqtt_qos"),
        )
        .boxed()
}

fn mqtt_confirming_publishing_mode<'src>(
    qos: &'static str,
) -> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    mqtt_qos_level(qos)
        .ignore_then(kw(Identifier::Ack))
        .ignore_then(emitter_ack_window())
        .then(ack_timeout())
        .then(retry_policy())
        .map(move |((window, ack_timeout), retry_policy)| match qos {
            "1" => EmitterPublishingMode::MqttQos1 {
                window,
                ack_timeout,
                retry_policy,
            },
            "2" => EmitterPublishingMode::MqttQos2 {
                window,
                ack_timeout,
                retry_policy,
            },
            _ => unreachable!("confirming MQTT publishing mode must use QOS 1 or 2"),
        })
        .boxed()
}

fn mqtt_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        mqtt_qos_level("0")
            .ignore_then(retry_policy())
            .map(|retry_policy| EmitterPublishingMode::MqttQos0 { retry_policy }),
        mqtt_confirming_publishing_mode("1"),
        mqtt_confirming_publishing_mode("2"),
    )
}

fn nats_jetstream_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Jetstream, Identifier::Ack)
        .ignore_then(emitter_ack_window())
        .then(ack_timeout())
        .then(retry_policy())
        .map(
            |((window, ack_timeout), retry_policy)| EmitterPublishingMode::NatsJetStream {
                window,
                ack_timeout,
                retry_policy,
            },
        )
        .boxed()
}

fn nats_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((no_ack_publishing_mode(), nats_jetstream_publishing_mode())).boxed()
}

fn sqs_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Single)
            .ignore_then(retry_policy())
            .map(|retry_policy| EmitterPublishingMode::SqsSingle { retry_policy }),
        kw(Identifier::Batch)
            .ignore_then(retry_policy())
            .map(|retry_policy| EmitterPublishingMode::SqsBatch { retry_policy }),
    ))
    .boxed()
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

fn kafka_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Kafka)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .map(|(client, topic)| EmitSink::Kafka { client, topic })
}

fn pulsar_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Pulsar)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .map(|(client, topic)| EmitSink::Pulsar { client, topic })
}

fn mqtt_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mqtt)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .map(|(client, topic)| EmitSink::Mqtt { client, topic })
}

fn nats_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Nats)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Subject))
        .then(topic_ref())
        .map(|(client, subject)| EmitSink::Nats { client, subject })
}

fn rabbitmq_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Rabbitmq)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Queue))
        .then(queue_ref())
        .map(|(client, queue)| EmitSink::RabbitMq { client, queue })
}

fn redis_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Redis)
        .ignore_then(kw(Identifier::Pubsub))
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Channel))
        .then(channel_ref())
        .map(|(client, channel)| EmitSink::Redis { client, channel })
}

fn zeromq_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Zeromq)
        .ignore_then(client_ref())
        .map(|client| EmitSink::ZeroMq { client })
}

fn sqs_fifo_group_expression<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::Expression, extra::Err<ParseError<'src>>> + Clone
{
    any()
        .and_is(kw(Identifier::Mode).not())
        .filter(|token: &Token| !matches!(token, Token::Semicolon))
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .labelled("fifo_group_expression")
        .try_map(|tokens, span| {
            let source = render_vm_program_tokens(&tokens);
            crate::parse_expression(&source).map_err(|error| {
                Rich::custom(span, crate::parser_support::vm_program_error_message(error))
            })
        })
        .boxed()
}

fn sqs_fifo_group_clause<'src>()
-> impl Parser<'src, &'src [Token], SqsFifoGroup, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Fifo, Identifier::Group)
        .ignore_then(choice((
            kw_phrase2(Identifier::From, Identifier::Branch).to(SqsFifoGroup::FromBranch),
            sqs_fifo_group_expression().map(SqsFifoGroup::Expression),
        )))
        .boxed()
}

fn sqs_queue_name<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    let atom = choice((select! { Token::NumberLiteral(value) => value }, word_raw()));
    let first = atom.clone().labelled("queue_name");
    let continuation = atom.labelled("queue_name");
    let base = first
        .then(
            tok(Token::Hyphen)
                .ignore_then(continuation)
                .repeated()
                .collect::<Vec<_>>(),
        )
        .map(|(first, rest)| {
            let mut queue = first;
            for part in rest {
                queue.push('-');
                queue.push_str(&part);
            }
            queue
        });

    base.then(tok(Token::Dot).ignore_then(kw(Identifier::Fifo)).or_not())
        .map(|(mut queue, fifo)| {
            if fifo.is_some() {
                queue.push_str(".fifo");
            }
            queue
        })
        .boxed()
}

fn sqs_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Sqs)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Queue))
        .then(sqs_queue_name())
        .then(sqs_fifo_group_clause().or_not())
        .map(|((client, queue), fifo_group)| EmitSink::Sqs {
            client,
            queue,
            fifo_group,
        })
}

fn sentry_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Sentry)
        .ignore_then(client_ref())
        .map(|client| EmitSink::Sentry { client })
}

fn otel_temporality_parser<'src>()
-> impl Parser<'src, &'src [Token], OtelAggregationTemporality, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        kw(Identifier::Delta).to(OtelAggregationTemporality::Delta),
        kw(Identifier::Cumulative).to(OtelAggregationTemporality::Cumulative),
    ))
}

fn otel_metric_kind_parser<'src>()
-> impl Parser<'src, &'src [Token], OtelMetricKind, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Gauge).to(OtelMetricKind::Gauge),
        kw(Identifier::Sum)
            .ignore_then(kw(Identifier::Monotonic).or_not())
            .then(otel_temporality_parser())
            .map(|(monotonic, temporality)| OtelMetricKind::Sum {
                monotonic: monotonic.is_some(),
                temporality,
            }),
        kw(Identifier::Histogram)
            .ignore_then(otel_temporality_parser())
            .map(|temporality| OtelMetricKind::Histogram { temporality }),
    ))
}

fn otel_signal_parser<'src>()
-> impl Parser<'src, &'src [Token], OtelSignal, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Logs).to(OtelSignal::Logs),
        kw(Identifier::Traces).to(OtelSignal::Traces),
        kw(Identifier::Metric)
            .ignore_then(string_lit().labelled("otel_metric_name"))
            .then_ignore(kw(Identifier::Unit))
            .then(string_lit().labelled("otel_metric_unit"))
            .then(
                kw(Identifier::Description)
                    .ignore_then(string_lit().labelled("otel_metric_description"))
                    .or_not(),
            )
            .then(otel_metric_kind_parser())
            .map(|(((name, unit), description), kind)| {
                OtelSignal::Metric(OtelMetric {
                    name,
                    unit,
                    description,
                    kind,
                })
            }),
    ))
}

fn otel_literal_expression(expression: &nervix_models::Expression) -> bool {
    match expression {
        nervix_models::Expression::Literal(_) => true,
        nervix_models::Expression::Array(items) => items.iter().all(otel_literal_expression),
        nervix_models::Expression::Field(_)
        | nervix_models::Expression::Unary { .. }
        | nervix_models::Expression::Binary { .. }
        | nervix_models::Expression::Cast { .. }
        | nervix_models::Expression::Call { .. }
        | nervix_models::Expression::UdfCall { .. }
        | nervix_models::Expression::If { .. }
        | nervix_models::Expression::Case { .. } => false,
    }
}

fn otel_resource_values<'src>()
-> impl Parser<'src, &'src [Token], Vec<ClickHouseValueMapping>, extra::Err<ParseError<'src>>> + Clone
{
    clickhouse_values().try_map(|values, span| {
        if values
            .iter()
            .all(|mapping| otel_literal_expression(&mapping.expression))
        {
            Ok(values)
        } else {
            Err(Rich::custom(
                span,
                "OTEL RESOURCE values must be literal expressions",
            ))
        }
    })
}

fn otel_scope_parser<'src>()
-> impl Parser<'src, &'src [Token], OtelScope, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Scope)
        .ignore_then(string_lit().labelled("otel_scope_name"))
        .then(
            kw(Identifier::Version)
                .ignore_then(string_lit().labelled("otel_scope_version"))
                .or_not(),
        )
        .map(|(name, version)| OtelScope { name, version })
}

fn otel_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Otel)
        .ignore_then(client_ref())
        .then(otel_signal_parser())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(
            kw(Identifier::Attributes)
                .ignore_then(clickhouse_values())
                .or_not(),
        )
        .then(
            kw(Identifier::Resource)
                .ignore_then(otel_resource_values())
                .or_not(),
        )
        .then(otel_scope_parser().or_not())
        .map(
            |(((((client, signal), values), attributes), resource), scope)| EmitSink::Otel {
                client,
                signal,
                values,
                attributes: attributes.unwrap_or_default(),
                resource: resource.unwrap_or_default(),
                scope,
            },
        )
}

fn balanced_value_expression_group<'src>()
-> impl Parser<'src, &'src [Token], Vec<Token>, extra::Err<ParseError<'src>>> + Clone {
    recursive(|element| {
        let contents = element
            .repeated()
            .collect::<Vec<_>>()
            .map(|parts| parts.into_iter().flatten().collect::<Vec<_>>());
        let parenthesized = contents
            .delimited_by(tok(Token::LParen), tok(Token::RParen))
            .map(|mut tokens| {
                tokens.insert(0, Token::LParen);
                tokens.push(Token::RParen);
                tokens
            });
        let leaf = any()
            .filter(|token: &Token| !matches!(token, Token::LParen | Token::RParen | Token::RBrace))
            .map(|token| vec![token]);
        // Naming the slot stops the raw delimiters leaking out as suggestions: a bare "(" offered
        // here cannot be completed into anything the expression grammar accepts.
        choice((parenthesized, leaf)).labelled("value_expression")
    })
}

fn clickhouse_value_expr<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::Expression, extra::Err<ParseError<'src>>> + Clone
{
    balanced_value_expression_group()
        .filter(|tokens| !matches!(tokens.as_slice(), [Token::Comma]))
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(|parts| parts.into_iter().flatten().collect::<Vec<_>>())
        .try_map(|tokens, span| {
            let source = render_vm_program_tokens(&tokens);
            crate::parse_expression(&source).map_err(|error| {
                Rich::custom(span, crate::parser_support::vm_program_error_message(error))
            })
        })
}

fn clickhouse_value_mapping<'src>()
-> impl Parser<'src, &'src [Token], ClickHouseValueMapping, extra::Err<ParseError<'src>>> + Clone {
    string_lit()
        .labelled("column_name")
        .then_ignore(tok(Token::Eq))
        .then(clickhouse_value_expr().labelled("value_expression"))
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

fn iceberg_values<'src>()
-> impl Parser<'src, &'src [Token], Vec<IcebergValueMapping>, extra::Err<ParseError<'src>>> + Clone
{
    clickhouse_values()
}

fn clickhouse_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Clickhouse)
        .ignore_then(client_ref())
        .then_ignore(kw_phrase2(Identifier::Insert, Identifier::To))
        .then_ignore(kw(Identifier::Table))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(max_batch())
        .map(
            |(((client, table), values), max_batch)| EmitSink::ClickHouse {
                client,
                table,
                values,
                max_batch,
                flush_each: String::new(),
            },
        )
}

fn max_batch<'src>() -> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone
{
    kw_phrase3(Identifier::With, Identifier::Max, Identifier::Batch)
        .ignore_then(select! { Token::NumberLiteral(value) => value }.labelled("batch_size"))
        .try_map(|value, span| {
            value
                .parse::<u64>()
                .map_err(|_| Rich::custom(span, format!("invalid max batch size '{value}'")))
                .and_then(|value| {
                    if value == 0 {
                        Err(Rich::custom(
                            span,
                            "max batch size must be greater than zero",
                        ))
                    } else {
                        Ok(value)
                    }
                })
        })
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

fn postgres_conflict_action<'src>()
-> impl Parser<'src, &'src [Token], PostgresConflictAction, extra::Err<ParseError<'src>>> + Clone {
    on_conflict_phrase()
        .ignore_then(conflict_target().or_not())
        .then_ignore(kw(Identifier::Do))
        .then(choice((
            kw(Identifier::Nothing).to(ConflictVerb::DoNothing),
            kw(Identifier::Update).to(ConflictVerb::DoUpdate),
        )))
        .try_map(|(target, verb), span| match verb {
            ConflictVerb::DoNothing => Ok(PostgresConflictAction::DoNothing {
                target: target.unwrap_or_default(),
            }),
            ConflictVerb::DoUpdate => match target {
                Some(target) => Ok(PostgresConflictAction::DoUpdate { target }),
                None => Err(Rich::custom(
                    span,
                    "Postgres ON CONFLICT DO UPDATE requires a conflict target",
                )),
            },
        })
        .or_not()
        .map(|action| action.unwrap_or(PostgresConflictAction::None))
}

fn mysql_conflict_action<'src>()
-> impl Parser<'src, &'src [Token], MySqlConflictAction, extra::Err<ParseError<'src>>> + Clone {
    on_conflict_phrase()
        .ignore_then(kw(Identifier::Do))
        .ignore_then(choice((
            kw(Identifier::Nothing).to(MySqlConflictAction::DoNothing),
            kw(Identifier::Update).to(MySqlConflictAction::DoUpdate),
        )))
        .or_not()
        .map(|action| action.unwrap_or(MySqlConflictAction::None))
}

fn mongodb_conflict_action<'src>()
-> impl Parser<'src, &'src [Token], MongoDbConflictAction, extra::Err<ParseError<'src>>> + Clone {
    on_conflict_phrase()
        .ignore_then(conflict_target())
        .then_ignore(kw(Identifier::Do))
        .then(choice((
            kw(Identifier::Nothing).to(ConflictVerb::DoNothing),
            kw(Identifier::Update).to(ConflictVerb::DoUpdate),
        )))
        .map(|(target, verb)| match verb {
            ConflictVerb::DoNothing => MongoDbConflictAction::DoNothing { target },
            ConflictVerb::DoUpdate => MongoDbConflictAction::DoUpdate { target },
        })
        .or_not()
        .map(|action| action.unwrap_or(MongoDbConflictAction::None))
}

fn validate_mongodb_conflict_action<'src>(
    values: &[ClickHouseValueMapping],
    conflict_action: &MongoDbConflictAction,
    span: chumsky::span::SimpleSpan,
) -> Result<(), ParseError<'src>> {
    let target = match conflict_action {
        MongoDbConflictAction::None => return Ok(()),
        MongoDbConflictAction::DoNothing { target }
        | MongoDbConflictAction::DoUpdate { target } => target,
    };
    for column in target {
        let is_mapped = values.iter().any(|mapping| mapping.column == *column);
        if !is_mapped {
            return Err(Rich::custom(
                span,
                format!("MongoDB ON CONFLICT target column '{column}' is not mapped in VALUES"),
            ));
        }
    }
    if let MongoDbConflictAction::DoUpdate { target } = conflict_action {
        let has_update_column = values
            .iter()
            .any(|mapping| !target.contains(&mapping.column));
        if !has_update_column {
            return Err(Rich::custom(
                span,
                "MongoDB ON CONFLICT DO UPDATE requires at least one non-conflict VALUES field to \
                 update",
            ));
        }
    }
    Ok(())
}

fn postgres_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Postgres)
        .ignore_then(client_ref())
        .then_ignore(kw_phrase2(Identifier::Insert, Identifier::To))
        .then_ignore(kw(Identifier::Table))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(postgres_conflict_action())
        .then(max_batch())
        .try_map(
            |((((client, table), values), conflict_action), max_batch), span| {
                if let PostgresConflictAction::DoUpdate { target } = &conflict_action {
                    let has_update_column = values
                        .iter()
                        .any(|mapping| !target.contains(&mapping.column));
                    if !has_update_column {
                        return Err(Rich::custom(
                            span,
                            "Postgres ON CONFLICT DO UPDATE requires at least one non-conflict \
                             VALUES column to update",
                        ));
                    }
                }
                Ok(EmitSink::Postgres {
                    client,
                    table,
                    values,
                    conflict_action,
                    max_batch,
                    flush_each: String::new(),
                })
            },
        )
}

fn mysql_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mysql)
        .ignore_then(client_ref())
        .then_ignore(kw_phrase2(Identifier::Insert, Identifier::To))
        .then_ignore(kw(Identifier::Table))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(mysql_conflict_action())
        .then(max_batch())
        .map(
            |((((client, table), values), conflict_action), max_batch)| EmitSink::MySql {
                client,
                table,
                values,
                conflict_action,
                max_batch,
                flush_each: String::new(),
            },
        )
}

fn mongodb_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mongodb)
        .ignore_then(client_ref())
        .then_ignore(kw_phrase2(Identifier::Insert, Identifier::To))
        .then_ignore(kw(Identifier::Collection))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(mongodb_conflict_action())
        .then(max_batch())
        .try_map(
            |((((client, collection), values), conflict_action), max_batch), span| {
                validate_mongodb_conflict_action(&values, &conflict_action, span)?;
                Ok(EmitSink::MongoDb {
                    client,
                    collection,
                    values,
                    conflict_action,
                    max_batch,
                    flush_each: String::new(),
                })
            },
        )
}

fn iceberg_catalog_parser<'src>()
-> impl Parser<'src, &'src [Token], IcebergCatalog, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Catalog)
        .ignore_then(client_ref())
        .map(|client| IcebergCatalog::Rest { client })
}

/// The Iceberg sink before its required commit cadence is attached.
fn iceberg_sink_shape<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Iceberg)
        .ignore_then(kw(Identifier::On))
        .ignore_then(iceberg_storage_backend_parser().then(client_ref()))
        .then_ignore(kw(Identifier::Table))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(iceberg_values())
        .then_ignore(kw(Identifier::Location))
        .then(string_lit().labelled("iceberg_location"))
        .then(iceberg_catalog_parser())
        .map(
            |(((((backend, client), table), values), location), catalog)| EmitSink::Iceberg {
                backend,
                client,
                table,
                values,
                location,
                catalog,
                flush_each: String::new(),
                max_batch_size: None,
                commit_each: String::new(),
                max_commit_size: String::new(),
            },
        )
}

/// `COMMIT EACH <duration> MAX SIZE <bytes>` is required by Iceberg and meaningless anywhere else,
/// so it is part of the Iceberg sink rather than a statement-level clause every sink is offered.
fn iceberg_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    iceberg_sink_shape()
        .then(iceberg_commit_each())
        .map(|(sink, (commit_each, max_commit_size))| match sink {
            EmitSink::Iceberg {
                backend,
                client,
                table,
                values,
                location,
                catalog,
                flush_each,
                max_batch_size,
                ..
            } => EmitSink::Iceberg {
                backend,
                client,
                table,
                values,
                location,
                catalog,
                flush_each,
                max_batch_size,
                commit_each,
                max_commit_size,
            },
            other => other,
        })
        .boxed()
}

fn iceberg_storage_backend_parser<'src>()
-> impl Parser<'src, &'src [Token], IcebergStorageBackend, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::S3).to(IcebergStorageBackend::S3),
        kw(Identifier::Gcs).to(IcebergStorageBackend::Gcs),
        kw(Identifier::AzureBlob).to(IcebergStorageBackend::AzureBlob),
    ))
}

fn iceberg_commit_each<'src>()
-> impl Parser<'src, &'src [Token], (String, String), extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Commit, Identifier::Each)
        .ignore_then(duration_lit())
        .then_ignore(kw_phrase2(Identifier::Max, Identifier::Size))
        .then(byte_size_lit())
}

/// `ENCODE USING <codec>`, written after the sink it encodes for.
///
/// Whether a codec is required, optional or meaningless is a property of the sink, so the clause
/// belongs to the sink clause rather than ahead of it. Written before `TO`, the grammar cannot know
/// at `TO` which sinks are still reachable, and offers sinks that can never complete. This also
/// matches ingestors, which already read `FROM <source> DECODE USING <codec>`.
fn encode_using_clause<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::Identifier, extra::Err<ParseError<'src>>> + Clone
{
    kw_phrase2(Identifier::Encode, Identifier::Using)
        .ignore_then(codec_ref())
        .boxed()
}

type SinkWithPublishingMode = (EmitSink, EmitterPublishingMode);

fn sink_with_publishing_mode<'src>(
    sink: impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone + 'src,
    mode: impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>>
    + Clone
    + 'src,
) -> impl Parser<'src, &'src [Token], SinkWithPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    sink.then_ignore(kw(Identifier::Mode)).then(mode).boxed()
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
        .map(|(((sink, mode), codec), construction)| (sink, mode, Some(codec), construction))
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
        .map(|((sink, mode), construction)| (sink, mode, None, construction))
        .boxed()
}

/// A parsed sink together with the route surface that sink supports.
type ParsedSink = (
    EmitSink,
    EmitterPublishingMode,
    Option<nervix_models::Identifier>,
    Option<nervix_models::RouteConstruction>,
);

fn emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        codec_free_sink(sink_with_publishing_mode(
            otel_emit_sink_parser(),
            request_ack_publishing_mode(),
        )),
        codec_free_sink(sink_with_publishing_mode(
            clickhouse_emit_sink_parser(),
            request_ack_publishing_mode(),
        )),
        codec_free_sink(sink_with_publishing_mode(
            postgres_emit_sink_parser(),
            request_ack_publishing_mode(),
        )),
        codec_free_sink(sink_with_publishing_mode(
            mysql_emit_sink_parser(),
            request_ack_publishing_mode(),
        )),
        codec_free_sink(sink_with_publishing_mode(
            mongodb_emit_sink_parser(),
            request_ack_publishing_mode(),
        )),
        codec_free_sink(sink_with_publishing_mode(
            iceberg_emit_sink_parser(),
            request_ack_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            kafka_emit_sink_parser(),
            broker_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            pulsar_emit_sink_parser(),
            broker_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            rabbitmq_emit_sink_parser(),
            broker_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            redis_emit_sink_parser(),
            no_ack_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            mqtt_emit_sink_parser(),
            mqtt_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            nats_emit_sink_parser(),
            nats_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            zeromq_emit_sink_parser(),
            no_ack_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            sqs_emit_sink_parser(),
            sqs_publishing_mode(),
        )),
        encoded_sink(sink_with_publishing_mode(
            sentry_emit_sink_parser(),
            request_ack_publishing_mode(),
        )),
    )
}

/// The complete sink and publishing mode for `ALTER EMITTER ... SET TO <sink>`, which replaces the
/// destination and its complete publishing contract while leaving the route codec and flush policy
/// in place.
fn alter_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], SinkWithPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
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
        sink_with_publishing_mode(sqs_emit_sink_parser(), sqs_publishing_mode()),
        sink_with_publishing_mode(sentry_emit_sink_parser(), request_ack_publishing_mode()),
    )
}

pub fn alter_emitter_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterEmitter, extra::Err<ParseError<'src>>> + Clone {
    let add_from = kw(Identifier::Add)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .then(where_expression(alter_op_separator()).or_not())
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
                .ignore_then(where_expression(alter_op_separator()))
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
        .map(|(sink, publishing_mode)| AlterEmitterOperation::SetSink {
            sink: Box::new(sink),
            publishing_mode,
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
    let set_flush =
        kw(Identifier::Set)
            .ignore_then(flush_each())
            .map(
                |(flush_each, max_batch_size)| AlterEmitterOperation::SetFlush {
                    flush_each,
                    max_batch_size,
                },
            );
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
        set_client,
        set_encode,
        drop_encode,
        set_collect,
        drop_collect,
        set_attachment,
        set_publishing_mode,
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
        .map(
            |((head, state), (sink, publishing_mode, codec, construction))| {
                (
                    ((((head, codec), publishing_mode), state), sink),
                    construction,
                )
            },
        )
        .boxed()
        .then(flush_each())
        .boxed()
        .then(message_error_policy())
        .then(general_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(parsed, general_error_policy)| {
            let (parsed, message_error_policy) = parsed;
            let (parsed, sink_flush_each) = parsed;
            let (parsed, construction) = parsed;
            let (parsed, sink) = parsed;
            let (parsed, materialized_state) = parsed;
            let (parsed, publishing_mode) = parsed;
            let (parsed, encode_using_codec) = parsed;
            let (((if_not_exists, mode), name), from) = parsed;
            let construction = construction.unwrap_or_default();
            let sink = match (sink, sink_flush_each.clone()) {
                (
                    EmitSink::ClickHouse {
                        client,
                        table,
                        values,
                        max_batch,
                        ..
                    },
                    (flush_each, _max_batch_size),
                ) => EmitSink::ClickHouse {
                    client,
                    table,
                    values,
                    max_batch,
                    flush_each,
                },
                (
                    EmitSink::Postgres {
                        client,
                        table,
                        values,
                        conflict_action,
                        max_batch,
                        ..
                    },
                    (flush_each, _max_batch_size),
                ) => EmitSink::Postgres {
                    client,
                    table,
                    values,
                    conflict_action,
                    max_batch,
                    flush_each,
                },
                (
                    EmitSink::MySql {
                        client,
                        table,
                        values,
                        conflict_action,
                        max_batch,
                        ..
                    },
                    (flush_each, _max_batch_size),
                ) => EmitSink::MySql {
                    client,
                    table,
                    values,
                    conflict_action,
                    max_batch,
                    flush_each,
                },
                (
                    EmitSink::MongoDb {
                        client,
                        collection,
                        values,
                        conflict_action,
                        max_batch,
                        ..
                    },
                    (flush_each, _max_batch_size),
                ) => EmitSink::MongoDb {
                    client,
                    collection,
                    values,
                    conflict_action,
                    max_batch,
                    flush_each,
                },
                (
                    // The commit cadence arrives with the sink, which is where it is written.
                    EmitSink::Iceberg {
                        backend,
                        client,
                        table,
                        values,
                        location,
                        catalog,
                        commit_each,
                        max_commit_size,
                        ..
                    },
                    (flush_each, max_batch_size),
                ) => EmitSink::Iceberg {
                    backend,
                    client,
                    table,
                    values,
                    location,
                    catalog,
                    flush_each,
                    max_batch_size,
                    commit_each,
                    max_commit_size,
                },
                (sink, _) => sink,
            };
            let (flush_each, max_batch_size) = sink_flush_each;
            CreateStatement::new(
                CreateEmitter {
                    name,
                    from,
                    encode_using_codec,
                    sink: Box::new(sink),
                    flush_each,
                    max_batch_size,
                    error_policies: nervix_models::ErrorPolicies {
                        message: message_error_policy,
                        general: general_error_policy,
                    },
                    publishing_mode,
                    mode: mode.unwrap_or(AckMode::Attached),
                    construction,
                    materialized_state,
                },
                if_not_exists,
            )
        })
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
            .expect("successful parse must have output"))
    }
}

pub fn parse_alter_emitter_tokens(tokens: &[Token]) -> Result<AlterEmitter, Vec<ParseError<'_>>> {
    let out = alter_emitter_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_emitter(
    input: &str,
) -> Result<CreateStatement<CreateEmitter>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_emitter_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_alter_emitter(input: &str) -> Result<AlterEmitter, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
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
mod tests {
    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    fn otel_emitter(signal: &str, values: &str, tail: &str) -> String {
        format!(
            "CREATE EMITTER telemetry FROM telemetry_relay TO OTEL otel_main {signal} VALUES \
             {{{values}}} {tail} MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    #[test]
    fn parses_otel_logs_traces_and_metric_shapes() {
        let logs = parse_create_emitter(&otel_emitter(
            "LOGS",
            "'time' = input.event_ts, 'body' = input.message",
            "ATTRIBUTES {'service.instance.id' = input.instance_id} RESOURCE {'service.name' = \
             'checkout'} SCOPE 'nervix/audit' VERSION '1.0'",
        ))
        .expect("OTEL logs emitter should parse");
        let EmitSink::Otel {
            signal,
            attributes,
            resource,
            scope,
            ..
        } = logs.sink.as_ref()
        else {
            panic!("expected OTEL logs sink");
        };
        assert!(matches!(signal, OtelSignal::Logs));
        assert_eq!(attributes.len(), 1);
        assert_eq!(resource.len(), 1);
        assert_eq!(
            scope.as_ref().map(|scope| scope.name.as_str()),
            Some("nervix/audit")
        );

        let traces = parse_create_emitter(&otel_emitter(
            "TRACES",
            "'trace_id' = input.trace_id, 'span_id' = input.span_id, 'name' = input.name, \
             'start_time' = input.started_at, 'end_time' = input.finished_at",
            "",
        ))
        .expect("OTEL traces emitter should parse");
        assert!(matches!(
            traces.sink.as_ref(),
            EmitSink::Otel {
                signal: OtelSignal::Traces,
                ..
            }
        ));

        let gauge = parse_create_emitter(&otel_emitter(
            "METRIC 'queue.depth' UNIT '1' DESCRIPTION 'Pending work' GAUGE",
            "'time' = input.observed_at, 'value' = input.depth",
            "",
        ))
        .expect("OTEL gauge emitter should parse");
        assert!(matches!(
            gauge.sink.as_ref(),
            EmitSink::Otel {
                signal: OtelSignal::Metric(OtelMetric {
                    kind: OtelMetricKind::Gauge,
                    ..
                }),
                ..
            }
        ));

        let sum = parse_create_emitter(&otel_emitter(
            "METRIC 'http.requests' UNIT '1' SUM MONOTONIC DELTA",
            "'time' = input.finished_at, 'start_time' = input.started_at, 'value' = input.count",
            "",
        ))
        .expect("OTEL sum emitter should parse");
        assert!(matches!(
            sum.sink.as_ref(),
            EmitSink::Otel {
                signal: OtelSignal::Metric(OtelMetric {
                    kind: OtelMetricKind::Sum {
                        monotonic: true,
                        temporality: OtelAggregationTemporality::Delta,
                    },
                    ..
                }),
                ..
            }
        ));

        let histogram = parse_create_emitter(&otel_emitter(
            "METRIC 'http.duration' UNIT 'ms' HISTOGRAM CUMULATIVE",
            "'time' = input.observed_at, 'count' = input.count, 'bucket_counts' = \
             input.bucket_counts, 'explicit_bounds' = input.bounds",
            "",
        ))
        .expect("OTEL histogram emitter should parse");
        assert!(matches!(
            histogram.sink.as_ref(),
            EmitSink::Otel {
                signal: OtelSignal::Metric(OtelMetric {
                    kind: OtelMetricKind::Histogram {
                        temporality: OtelAggregationTemporality::Cumulative,
                    },
                    ..
                }),
                ..
            }
        ));
    }

    #[test]
    fn rejects_invalid_otel_sink_clause_surfaces() {
        assert!(
            parse_create_emitter(
                "CREATE EMITTER telemetry FROM telemetry_relay TO OTEL otel_main LOGS MODE ACK \
                 RETRY POLICY BACKOFF 250ms MAX 30s FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
                 GENERAL ERROR LOG;"
            )
            .is_err()
        );
        assert!(
            parse_create_emitter(&otel_emitter(
                "LOGS",
                "'time' = input.event_ts, 'body' = input.message",
                "RESOURCE {'service.name' = input.service_name}",
            ))
            .is_err()
        );
        assert!(
            parse_create_emitter(
                "CREATE EMITTER telemetry FROM telemetry_relay TO OTEL otel_main LOGS VALUES \
                 {'time' = input.event_ts, 'body' = input.message} MODE NO_ACK RETRY POLICY \
                 BACKOFF 250ms MAX 30s FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            )
            .is_err()
        );
        assert!(
            parse_create_emitter(
                "CREATE EMITTER telemetry FROM telemetry_relay TO OTEL otel_main LOGS VALUES \
                 {'time' = input.event_ts, 'body' = input.message} MODE ACK RETRY POLICY BACKOFF \
                 250ms MAX 30s ENCODE USING telemetry_codec FLUSH IMMEDIATE ON MESSAGE ERROR LOG \
                 ON GENERAL ERROR LOG;"
            )
            .is_err()
        );
    }

    #[test]
    fn otel_completion_stays_within_the_selected_signal_grammar() {
        let signal = "CREATE EMITTER telemetry FROM telemetry_relay TO OTEL otel_main ";
        let signal_suggestions = suggest_create_emitter(signal, signal.len());
        assert!(signal_suggestions.contains(&"LOGS".to_string()));
        assert!(signal_suggestions.contains(&"TRACES".to_string()));
        assert!(signal_suggestions.contains(&"METRIC".to_string()));
        assert!(!signal_suggestions.contains(&"TOPIC".to_string()));

        let after_values = "CREATE EMITTER telemetry FROM telemetry_relay TO OTEL otel_main LOGS \
                            VALUES {'time' = input.event_ts, 'body' = input.message} ";
        let route_suggestions = suggest_create_emitter(after_values, after_values.len());
        assert!(route_suggestions.contains(&"ATTRIBUTES".to_string()));
        assert!(route_suggestions.contains(&"RESOURCE".to_string()));
        assert!(route_suggestions.contains(&"SCOPE".to_string()));
        assert!(route_suggestions.contains(&"MODE".to_string()));
        assert!(!route_suggestions.contains(&"ENCODE USING".to_string()));
    }

    #[test]
    fn parses_alter_emitter_operations_in_written_order() {
        let parsed = parse_alter_emitter(
            "ALTER EMITTER event_sink ADD FROM backup WHERE input.kind = 'backup', ALTER FROM \
             backup SET WHERE input.kind = 'current', ALTER FROM backup DROP WHERE, DROP FROM \
             backup, SET TO ZEROMQ sink_b MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s, SET \
             CLIENT sink_c, SET ENCODE USING event_codec, DROP ENCODE, SET COLLECT FOR 10ms MAX \
             BATCH SIZE 1MiB, DROP COLLECT, SET DETACHED, SET FLUSH IMMEDIATE;",
        )
        .expect("ALTER EMITTER should parse");

        assert_eq!(parsed.operations.len(), 12);
        assert!(matches!(
            parsed.operations[0],
            AlterEmitterOperation::AddFrom { .. }
        ));
        assert!(matches!(
            parsed.operations[1],
            AlterEmitterOperation::AlterFromSetWhere { .. }
        ));
        assert!(matches!(
            parsed.operations[2],
            AlterEmitterOperation::AlterFromDropWhere { .. }
        ));
        assert!(matches!(
            parsed.operations[3],
            AlterEmitterOperation::DropFrom { .. }
        ));
        assert!(matches!(
            parsed.operations[4],
            AlterEmitterOperation::SetSink {
                ref sink,
                ..
            } if matches!(sink.as_ref(), EmitSink::ZeroMq { .. })
        ));
        assert!(matches!(
            parsed.operations[5],
            AlterEmitterOperation::SetClient { .. }
        ));
        assert!(matches!(
            parsed.operations[6],
            AlterEmitterOperation::SetEncodeUsing { .. }
        ));
        assert_eq!(parsed.operations[7], AlterEmitterOperation::DropEncode);
        assert!(matches!(
            parsed.operations[8],
            AlterEmitterOperation::SetCollect { .. }
        ));
        assert_eq!(parsed.operations[9], AlterEmitterOperation::DropCollect);
        assert_eq!(
            parsed.operations[10],
            AlterEmitterOperation::SetAttachment {
                mode: AckMode::Detached
            }
        );
        assert_eq!(
            parsed.operations[11],
            AlterEmitterOperation::SetFlush {
                flush_each: "IMMEDIATE".to_string(),
                max_batch_size: None,
            }
        );
    }

    #[test]
    fn parses_alter_emitter_direct_sink_values_and_iceberg_commit_policy() {
        let direct = parse_alter_emitter(
            "ALTER EMITTER event_sink SET TO POSTGRES postgres_main INSERT TO TABLE events VALUES \
             { 'seq' = concat(input.kind, ','), 'value' = input.value } WITH MAX BATCH 100 MODE \
             ACK RETRY POLICY BACKOFF 250ms MAX 30s, SET FLUSH EACH 1s MAX BATCH SIZE 1MiB;",
        )
        .expect("direct sink expressions should preserve internal commas");
        assert_eq!(direct.operations.len(), 2);

        let iceberg =
            parse_alter_emitter("ALTER EMITTER event_sink SET COMMIT EACH 30s MAX SIZE 64MiB;")
                .expect("Iceberg commit policy should parse");
        assert_eq!(
            iceberg.operations,
            vec![AlterEmitterOperation::SetCommit {
                commit_each: "30s".to_string(),
                max_commit_size: "64MiB".to_string(),
            }]
        );

        let replace_iceberg = parse_alter_emitter(
            "ALTER EMITTER event_sink SET TO ICEBERG ON S3 s3_main TABLE events VALUES { 'value' \
             = input.value } LOCATION 's3://warehouse/events' CATALOG iceberg_catalog COMMIT EACH \
             1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s;",
        )
        .expect("SET TO Iceberg should require and retain its complete commit contract");
        assert!(matches!(
            replace_iceberg.operations.as_slice(),
            [AlterEmitterOperation::SetSink {
                sink,
                ..
            }] if matches!(
                sink.as_ref(),
                EmitSink::Iceberg {
                    commit_each,
                    max_commit_size,
                    ..
                } if commit_each == "1m" && max_commit_size == "512MiB"
            )
        ));
    }

    #[test]
    fn rejects_alter_emitter_without_operations_or_complete_flush_policy() {
        assert!(parse_alter_emitter("ALTER EMITTER event_sink;").is_err());
        assert!(parse_alter_emitter("ALTER EMITTER event_sink SET FLUSH EACH 1s;").is_err());
    }

    #[test]
    fn alter_emitter_completion_comes_from_its_operation_grammar() {
        let suggestions = suggest_alter_emitter("ALTER EMITTER event_sink ", usize::MAX);
        for expected in ["ADD", "ALTER", "SET", "DROP"] {
            assert!(
                suggestions.contains(&expected.to_string()),
                "missing {expected}: {suggestions:?}"
            );
        }
        assert!(!suggestions.contains(&"SCHEMA".to_string()));
    }

    fn expression(source: &str) -> nervix_models::Expression {
        crate::parse_expression(source).expect("valid structured expression")
    }

    fn complete_codec_emitter(sink: &str) -> String {
        format!(
            "CREATE EMITTER emit FROM p99 TO {sink} ENCODE USING my_codec FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    fn assert_canonical_emitter_roundtrip(source: &str, mode: &str) {
        let parsed = parse_create_emitter(source)
            .unwrap_or_else(|error| panic!("publishing mode must parse for `{mode}`: {error:?}"));
        let canonical = parsed
            .to_canonical_nspl()
            .unwrap_or_else(|error| panic!("publishing mode must render for `{mode}`: {error:?}"));
        let reparsed = parse_create_emitter(&canonical).unwrap_or_else(|error| {
            panic!("canonical publishing mode must reparse for `{mode}`: {error:?}\n{canonical}")
        });
        assert_eq!(reparsed, parsed, "canonical round-trip changed `{mode}`");
    }

    #[test]
    fn every_publishing_mode_body_parses_and_canonical_round_trips() {
        for sink in [
            "KAFKA broker TOPIC events MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "KAFKA broker TOPIC events MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF \
             250ms MAX 30s",
            "PULSAR broker TOPIC events MODE ACK PARALLEL MAX 16 ACK TIMEOUT 30s RETRY POLICY \
             BACKOFF 250ms MAX 30s",
            "RABBITMQ broker QUEUE events MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "MQTT broker TOPIC events MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s",
            "MQTT broker TOPIC events MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY \
             BACKOFF 250ms MAX 30s",
            "MQTT broker TOPIC events MODE QOS 2 ACK PARALLEL MAX 8 ACK TIMEOUT 30s RETRY POLICY \
             BACKOFF 250ms MAX 30s",
            "NATS broker SUBJECT events MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "NATS broker SUBJECT events MODE JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 30s RETRY \
             POLICY BACKOFF 250ms MAX 30s",
            "REDIS PUBSUB broker CHANNEL events MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "ZEROMQ broker MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "SQS broker QUEUE events MODE SINGLE RETRY POLICY BACKOFF 250ms MAX 30s",
            "SQS broker QUEUE events MODE BATCH RETRY POLICY BACKOFF 250ms MAX 30s",
            "SENTRY broker MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
        ] {
            assert_canonical_emitter_roundtrip(&complete_codec_emitter(sink), sink);
        }

        for sink in [
            "CLICKHOUSE db INSERT TO TABLE events VALUES { 'id' = input.id } WITH MAX BATCH 100 \
             MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "POSTGRES db INSERT TO TABLE events VALUES { 'id' = input.id } WITH MAX BATCH 100 \
             MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "MYSQL db INSERT TO TABLE events VALUES { 'id' = input.id } WITH MAX BATCH 100 MODE \
             ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "MONGODB db INSERT TO COLLECTION events VALUES { 'id' = input.id } WITH MAX BATCH 100 \
             MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "ICEBERG ON S3 store TABLE events VALUES { 'id' = input.id } LOCATION \
             's3://bucket/events' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB MODE ACK RETRY \
             POLICY BACKOFF 250ms MAX 30s",
            "OTEL otel_main LOGS VALUES { 'time' = input.time, 'body' = input.body } ATTRIBUTES { \
             'service.instance.id' = input.instance_id } RESOURCE { 'service.name' = 'checkout' } \
             SCOPE 'nervix/logs' VERSION '1.0' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "OTEL otel_main TRACES VALUES { 'trace_id' = input.trace_id, 'span_id' = \
             input.span_id, 'name' = input.name, 'start_time' = input.start_time, 'end_time' = \
             input.end_time } MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
            "OTEL otel_main METRIC 'request.duration' UNIT 'ms' DESCRIPTION 'Request duration' \
             HISTOGRAM CUMULATIVE VALUES { 'time' = input.time, 'count' = input.count, \
             'bucket_counts' = input.bucket_counts, 'explicit_bounds' = input.explicit_bounds } \
             MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
        ] {
            let source = format!(
                "CREATE EMITTER emit FROM p99 TO {sink} FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
                 GENERAL ERROR LOG;"
            );
            assert_canonical_emitter_roundtrip(&source, sink);
        }
    }

    #[test]
    fn rejects_missing_incomplete_or_foreign_publishing_modes() {
        for source in [
            complete_codec_emitter("KAFKA broker TOPIC events"),
            complete_codec_emitter("KAFKA broker TOPIC events MODE NO_ACK"),
            complete_codec_emitter(
                "KAFKA broker TOPIC events MODE ACK SEQUENTIAL RETRY POLICY BACKOFF 250ms MAX 30s",
            ),
            complete_codec_emitter(
                "KAFKA broker TOPIC events MODE ACK PARALLEL MAX 0 ACK TIMEOUT 30s RETRY POLICY \
                 BACKOFF 250ms MAX 30s",
            ),
            complete_codec_emitter(
                "KAFKA broker TOPIC events MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY \
                 BACKOFF 250ms MAX 30s",
            ),
            "CREATE EMITTER emit FROM p99 TO CLICKHOUSE db INSERT TO TABLE events VALUES { 'id' = \
             input.id } MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s FLUSH IMMEDIATE ON MESSAGE \
             ERROR LOG ON GENERAL ERROR LOG;"
                .to_string(),
        ] {
            assert!(
                parse_create_emitter(&source).is_err(),
                "invalid emitter unexpectedly parsed: {source}"
            );
        }
    }

    #[test]
    fn publishing_mode_completion_stays_within_the_selected_sink() {
        let before_mode = "CREATE EMITTER emit FROM p99 TO KAFKA broker TOPIC events ";
        let suggestions = suggest_create_emitter(before_mode, before_mode.len());
        assert_eq!(suggestions, vec!["MODE".to_string()]);

        let kafka_mode = format!("{before_mode}MODE ");
        let suggestions = suggest_create_emitter(&kafka_mode, kafka_mode.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO_ACK".to_string()));
        assert!(!suggestions.contains(&"QOS".to_string()));
        assert!(!suggestions.contains(&"JETSTREAM".to_string()));
        assert!(!suggestions.contains(&"SINGLE".to_string()));
    }

    #[test]
    fn nats_jetstream_completion_uses_the_complete_mode_phrase() {
        let input = "CREATE EMITTER emit FROM p99 TO NATS broker SUBJECT events MODE ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"NO_ACK".to_string()));
        assert!(suggestions.contains(&"JETSTREAM ACK".to_string()));
        assert!(!suggestions.contains(&"JETSTREAM".to_string()));
        assert!(!suggestions.contains(&"QOS".to_string()));
        assert!(!suggestions.contains(&"SINGLE".to_string()));
    }

    #[test]
    fn mqtt_mode_completion_requires_qos_before_the_level() {
        let input = "CREATE EMITTER emit FROM p99 TO MQTT broker TOPIC events MODE ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"QOS".to_string()));
        assert!(!suggestions.contains(&"mqtt_qos".to_string()));

        let input = "CREATE EMITTER emit FROM p99 TO MQTT broker TOPIC events MODE QOS ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert_eq!(suggestions, vec!["mqtt_qos".to_string()]);
    }

    #[test]
    fn sqs_fifo_queue_suffix_has_a_completable_keyword() {
        let input = "ALTER EMITTER emit SET TO SQS broker QUEUE events . ";
        let suggestions = suggest_alter_emitter(input, input.len());

        assert_eq!(suggestions, vec!["FIFO".to_string()]);
    }

    #[test]
    fn direct_emitter_completion_only_offers_where_construction() {
        let input = "CREATE EMITTER emit FROM p99 TO CLICKHOUSE db INSERT TO TABLE events VALUES \
                     { 'id' = input.id } WITH MAX BATCH 10 MODE ACK RETRY POLICY BACKOFF 250ms \
                     MAX 30s ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"WHERE".to_string()));
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
        assert!(!suggestions.contains(&"INHERIT".to_string()));
        assert!(!suggestions.contains(&"SET".to_string()));
        assert!(!suggestions.contains(&"INVOKE".to_string()));
    }

    #[test]
    fn parses_sqs_fifo_group_forms() {
        let from_branch = parse_create_emitter(&complete_codec_emitter(
            "SQS broker QUEUE events.fifo FIFO GROUP FROM BRANCH MODE SINGLE RETRY POLICY BACKOFF \
             250ms MAX 30s",
        ))
        .expect("SQS FIFO FROM BRANCH mode should parse");
        assert!(matches!(
            from_branch.sink.as_ref(),
            EmitSink::Sqs {
                queue,
                fifo_group: Some(nervix_models::SqsFifoGroup::FromBranch),
                ..
            } if queue == "events.fifo"
        ));
        let canonical = from_branch
            .to_canonical_nspl()
            .expect("SQS FIFO emitter should render canonically");
        let round_tripped = parse_create_emitter(&canonical)
            .expect("canonical SQS FIFO emitter should parse again");
        assert_eq!(round_tripped, from_branch);

        let expression = parse_create_emitter(&complete_codec_emitter(
            "SQS broker QUEUE events.fifo FIFO GROUP concat(input.tenant, '-', input.region) MODE \
             BATCH RETRY POLICY BACKOFF 250ms MAX 30s",
        ))
        .expect("SQS FIFO expression mode should parse");
        assert!(matches!(
            expression.sink.as_ref(),
            EmitSink::Sqs {
                fifo_group: Some(nervix_models::SqsFifoGroup::Expression(_)),
                ..
            }
        ));
    }

    #[test]
    fn rejects_fifo_group_on_non_sqs_sink() {
        let source = complete_codec_emitter(
            "KAFKA broker TOPIC events FIFO GROUP FROM BRANCH MODE NO_ACK RETRY POLICY BACKOFF \
             250ms MAX 30s",
        );
        assert!(parse_create_emitter(&source).is_err());
    }

    #[test]
    fn parses_alter_emitter_publishing_and_attachment_modes() {
        let parsed = parse_alter_emitter(
            "ALTER EMITTER event_sink SET MODE ACK PARALLEL MAX 32 ACK TIMEOUT 10s RETRY POLICY \
             BACKOFF 250ms MAX 30s, SET DETACHED, SET TO ZEROMQ sink_b MODE NO_ACK RETRY POLICY \
             BACKOFF 1s MAX 1m;",
        )
        .expect("ALTER EMITTER mode operations should parse");

        assert!(matches!(
            parsed.operations[0],
            AlterEmitterOperation::SetPublishingMode {
                mode: nervix_models::EmitterPublishingMode::BrokerAck { .. }
            }
        ));
        assert!(matches!(
            parsed.operations[1],
            AlterEmitterOperation::SetAttachment {
                mode: AckMode::Detached
            }
        ));
        assert!(matches!(
            parsed.operations[2],
            AlterEmitterOperation::SetSink {
                publishing_mode: nervix_models::EmitterPublishingMode::NoAck { .. },
                ..
            }
        ));

        let request_ack = parse_alter_emitter(
            "ALTER EMITTER database_sink SET MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s;",
        )
        .expect("request/response ACK mode should parse without a confirmation window");
        assert!(matches!(
            request_ack.operations.as_slice(),
            [AlterEmitterOperation::SetPublishingMode {
                mode: nervix_models::EmitterPublishingMode::RequestAck { .. }
            }]
        ));
    }

    #[test]
    fn direct_database_emitters_reject_codecs() {
        let source = complete_codec_emitter(
            "POSTGRES database INSERT TO TABLE events VALUES { 'value' = input.value } WITH MAX \
             BATCH 100 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
        );
        assert!(parse_create_emitter(&source).is_err());
    }

    #[test]
    fn parses_create_emitter_kafka() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                TO KAFKA broker1 TOPIC topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "emit");
        assert_eq!(
            parsed
                .from
                .first()
                .expect("emitter must have an input")
                .as_str(),
            "p99"
        );
        assert_eq!(
            parsed
                .encode_using_codec
                .as_ref()
                .map(|codec| codec.as_str()),
            Some("my_codec")
        );
        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Kafka {
                client: nervix_models::Identifier::try_from("broker1")
                    .expect("valid client identifier"),
                topic: nervix_models::Identifier::try_from("topic")
                    .expect("valid topic identifier"),
            }
        );
        assert_eq!(parsed.mode, AckMode::Attached);
    }

    #[test]
    fn parses_emitter_input_collection() {
        let parsed = parse_create_emitter(
            "CREATE EMITTER emit FROM p99 COLLECT FOR 1s MAX BATCH SIZE 10MiB TO KAFKA broker1 \
             TOPIC topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        )
        .expect("emitter input collection must parse");
        let policy = parsed
            .from
            .collect_policy
            .as_ref()
            .expect("emitter collection policy must be structured");
        assert_eq!(policy.collect_for, "1s");
        assert_eq!(policy.max_batch_size.as_deref(), Some("10MiB"));
    }

    #[test]
    fn suggests_input_collection_after_emitter_source() {
        let input = "CREATE EMITTER emit FROM p99 COL";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"COLLECT FOR".to_string()));
    }

    #[test]
    fn parses_multiple_emitter_inputs_with_source_where() {
        let parsed = parse_create_emitter(
            "CREATE EMITTER emit FROM source_a WHERE input.kind = 'a', source_b WHERE input.kind \
             = 'b' COLLECT FOR 10ms MAX BATCH SIZE 1MiB TO ZEROMQ sink MODE NO_ACK RETRY POLICY \
             BACKOFF 250ms MAX 30s ENCODE USING my_codec FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
             GENERAL ERROR LOG;",
        )
        .expect("multiple emitter inputs should parse");

        assert_eq!(
            parsed
                .from
                .relays()
                .iter()
                .map(|relay| relay.as_str())
                .collect::<Vec<_>>(),
            vec!["source_a", "source_b"]
        );
        assert_eq!(parsed.from.where_clauses().len(), 2);
        assert!(parsed.from.collect_policy.is_some());
    }

    #[test]
    fn rejects_incomplete_multiple_emitter_inputs() {
        assert!(
            parse_create_emitter(
                "CREATE EMITTER emit FROM source_a, TO ZEROMQ sink ENCODE USING my_codec FLUSH \
                 IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            )
            .is_err()
        );
        assert!(
            parse_create_emitter(
                "CREATE EMITTER emit FROM source_a WHERE, source_b TO ZEROMQ sink ENCODE USING \
                 my_codec FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            )
            .is_err()
        );
    }

    #[test]
    fn emitter_input_completion_does_not_leak_unrelated_grammar() {
        let input = "CREATE EMITTER emit FROM source_a WHE";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"WHERE".to_string()));
        assert!(!suggestions.contains(&"SCHEMA".to_string()));
    }

    #[test]
    fn parses_create_emitter_sentry() {
        let input = r#"
            CREATE EMITTER emit
                FROM errors
                TO SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING error_event_codec
                INHERIT ALL
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");

        assert_eq!(parsed.sink.transport_label(), "SENTRY");
        assert_eq!(parsed.sink.client().as_str(), "sentry_main");
    }

    #[test]
    fn rejects_sentry_emitter_without_codec() {
        let input = r#"
            CREATE EMITTER emit
                FROM errors
                TO SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                INHERIT ALL
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#;

        let error = parse_create_emitter(input).expect_err("parse must fail");
        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error, got {error:?}");
        };
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("ENCODE USING")),
            "expected codec diagnostic, got {diagnostics:?}"
        );
    }

    #[test]
    fn parses_create_emitter_clickhouse() {
        let input = r#"
            CREATE EMITTER to_ch
                FROM notifications
                TO CLICKHOUSE clickhouse_client INSERT TO TABLE my_table
                VALUES {
                    "clickhouse_user_id" = input.user_id,
                    "clickhouse_now" = NOW(),
                    "clickhouse_action" = LOWER(input.action)
                }
                WITH MAX BATCH 100
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::ClickHouse {
                client: nervix_models::Identifier::try_from("clickhouse_client")
                    .expect("valid client identifier"),
                table: nervix_models::Identifier::try_from("my_table")
                    .expect("valid table identifier"),
                values: vec![
                    ClickHouseValueMapping {
                        column: "clickhouse_user_id".to_string(),
                        expression: expression("input.user_id"),
                    },
                    ClickHouseValueMapping {
                        column: "clickhouse_now".to_string(),
                        expression: expression("NOW ( )"),
                    },
                    ClickHouseValueMapping {
                        column: "clickhouse_action".to_string(),
                        expression: expression("LOWER ( input.action )"),
                    },
                ],
                max_batch: 100,
                flush_each: "10s".to_string(),
            }
        );
    }

    #[test]
    fn rejects_clickhouse_emitter_without_flush_policy() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_ch FROM notifications
            TO CLICKHOUSE clickhouse_client INSERT TO TABLE my_table
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 100
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
            "expected ClickHouse flush diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn parses_create_emitter_iceberg() {
        let input = r#"
            CREATE DETACHED EMITTER to_iceberg
                FROM notifications
                TO ICEBERG ON S3 s3_client TABLE notifications
                VALUES {
                    "user_id" = input.user_id,
                    "action" = input.action
                }
                LOCATION 's3://nervix-iceberg/tables/notifications'
                CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 64MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(parsed.mode, AckMode::Detached);
        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Iceberg {
                backend: IcebergStorageBackend::S3,
                client: nervix_models::Identifier::try_from("s3_client")
                    .expect("valid client identifier"),
                table: nervix_models::Identifier::try_from("notifications")
                    .expect("valid table identifier"),
                values: vec![
                    ClickHouseValueMapping {
                        column: "user_id".to_string(),
                        expression: expression("input.user_id"),
                    },
                    ClickHouseValueMapping {
                        column: "action".to_string(),
                        expression: expression("input.action"),
                    },
                ],
                location: "s3://nervix-iceberg/tables/notifications".to_string(),
                catalog: IcebergCatalog::Rest {
                    client: nervix_models::Identifier::try_from("iceberg_catalog")
                        .expect("valid catalog client identifier"),
                },
                flush_each: "10s".to_string(),
                max_batch_size: Some("64MiB".to_string()),
                commit_each: "1m".to_string(),
                max_commit_size: "512MiB".to_string(),
            }
        );
    }

    #[test]
    fn parses_create_emitter_iceberg_gcs() {
        let input = r#"
            CREATE EMITTER to_iceberg
                FROM notifications
                TO ICEBERG ON GCS gcs_client TABLE notifications
                VALUES {
                    "user_id" = input.user_id,
                    "action" = input.action
                }
                LOCATION 'gs://nervix-iceberg/tables/notifications'
                CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Iceberg {
                backend: IcebergStorageBackend::Gcs,
                client: nervix_models::Identifier::try_from("gcs_client")
                    .expect("valid client identifier"),
                table: nervix_models::Identifier::try_from("notifications")
                    .expect("valid table identifier"),
                values: vec![
                    ClickHouseValueMapping {
                        column: "user_id".to_string(),
                        expression: expression("input.user_id"),
                    },
                    ClickHouseValueMapping {
                        column: "action".to_string(),
                        expression: expression("input.action"),
                    },
                ],
                location: "gs://nervix-iceberg/tables/notifications".to_string(),
                catalog: IcebergCatalog::Rest {
                    client: nervix_models::Identifier::try_from("iceberg_catalog")
                        .expect("valid catalog client identifier"),
                },
                flush_each: "IMMEDIATE".to_string(),
                max_batch_size: None,
                commit_each: "1m".to_string(),
                max_commit_size: "512MiB".to_string(),
            }
        );
    }

    #[test]
    fn parses_create_emitter_iceberg_azure_blob() {
        let input = r#"
            CREATE EMITTER to_iceberg
                FROM notifications
                TO ICEBERG ON AZURE_BLOB azure_client TABLE notifications
                VALUES {
                    "user_id" = input.user_id,
                    "action" = input.action
                }
                LOCATION 'wasb://nervix-iceberg@devstoreaccount1.blob.core.windows.net/tables/notifications'
                CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Iceberg {
                backend: IcebergStorageBackend::AzureBlob,
                client: nervix_models::Identifier::try_from("azure_client")
                    .expect("valid client identifier"),
                table: nervix_models::Identifier::try_from("notifications")
                    .expect("valid table identifier"),
                values: vec![
                    ClickHouseValueMapping {
                        column: "user_id".to_string(),
                        expression: expression("input.user_id"),
                    },
                    ClickHouseValueMapping {
                        column: "action".to_string(),
                        expression: expression("input.action"),
                    },
                ],
                location: "wasb://nervix-iceberg@devstoreaccount1.blob.core.windows.net/tables/\
                           notifications"
                    .to_string(),
                catalog: IcebergCatalog::Rest {
                    client: nervix_models::Identifier::try_from("iceberg_catalog")
                        .expect("valid catalog client identifier"),
                },
                flush_each: "IMMEDIATE".to_string(),
                max_batch_size: None,
                commit_each: "1m".to_string(),
                max_commit_size: "512MiB".to_string(),
            }
        );
    }

    #[test]
    fn rejects_iceberg_emitter_without_flush_policy() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_iceberg FROM notifications
            TO ICEBERG ON S3 s3_client TABLE notifications
            VALUES { "user_id" = input.user_id }
            LOCATION 's3://nervix-iceberg/tables/notifications'
            CATALOG iceberg_catalog
            ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter()
                .any(|err| format!("{err:?}").contains("expected COMMIT EACH")),
            "Iceberg requires a commit cadence in its sink clause: {errs:?}"
        );
    }

    #[test]
    fn rejects_iceberg_emitter_without_storage_backend() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_iceberg FROM notifications
            TO ICEBERG ON s3_client TABLE notifications
            LOCATION 's3://nervix-iceberg/tables/notifications'
            CATALOG iceberg_catalog
            FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn rejects_iceberg_emitter_with_encode_using() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_iceberg FROM notifications ENCODE USING json_codec
            TO ICEBERG ON S3 s3_client TABLE notifications
            VALUES { "user_id" = input.user_id }
            LOCATION 's3://nervix-iceberg/tables/notifications'
            CATALOG iceberg_catalog
            FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter()
                .any(|err| format!("{err:?}").contains("iden: Encode")),
            "Iceberg takes no codec, so ENCODE USING must not be accepted before TO: {errs:?}"
        );
    }

    #[test]
    fn rejects_iceberg_emitter_without_values_mapping() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_iceberg FROM notifications
            TO ICEBERG ON S3 s3_client TABLE notifications
            LOCATION 's3://nervix-iceberg/tables/notifications'
            CATALOG iceberg_catalog
            FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter().any(|err| format!("{err:?}").contains("VALUES")),
            "expected Iceberg VALUES diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn rejects_iceberg_emitter_without_commit_policy() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_iceberg FROM notifications
            TO ICEBERG ON S3 s3_client TABLE notifications
            VALUES { "user_id" = input.user_id }
            LOCATION 's3://nervix-iceberg/tables/notifications'
            CATALOG iceberg_catalog
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter()
                .any(|err| format!("{err:?}").contains("COMMIT EACH")),
            "expected Iceberg COMMIT EACH diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn rejects_non_iceberg_emitter_with_commit_policy() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_ch FROM notifications
            TO CLICKHOUSE clickhouse_client INSERT TO TABLE my_table
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 100
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB COMMIT EACH 1m MAX SIZE 512MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter()
                .any(|err| format!("{err:?}").contains("iden: Commit")),
            "COMMIT EACH belongs to the Iceberg sink and must not be accepted here: {errs:?}"
        );
    }

    #[test]
    fn rejects_iceberg_same_client_catalog_syntax() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_iceberg FROM notifications
            TO ICEBERG ON S3 s3_client TABLE notifications
            VALUES { "user_id" = input.user_id }
            LOCATION 's3://nervix-iceberg/tables/notifications'
            CATALOG SAME COMMIT EACH 1m MAX SIZE 512MiB CLIENT LOCATION 's3://nervix-iceberg/catalogs/input.catalog.json' FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn iceberg_catalog_context_suggestions_do_not_leak_sink_keywords() {
        let input = "CREATE EMITTER to_iceberg FROM notifications TO ICEBERG ON S3 s3_client \
                     TABLE notifications VALUES { \"user_id\" = input.user_id } LOCATION \
                     's3://bucket/table' CATALOG ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"ref:client".to_string()));
        assert!(!suggestions.contains(&"SAME".to_string()));
        assert!(!suggestions.contains(&"KAFKA".to_string()));
        assert!(!suggestions.contains(&"CLICKHOUSE".to_string()));
    }

    #[test]
    fn iceberg_table_context_suggests_values_before_location() {
        let input =
            "CREATE EMITTER to_iceberg FROM notifications TO ICEBERG ON S3 s3_client TABLE tbl ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"VALUES".to_string()));
        assert!(!suggestions.contains(&"LOCATION".to_string()));
    }

    #[test]
    fn iceberg_backend_context_suggestions_do_not_leak_sink_keywords() {
        let input = "CREATE EMITTER to_iceberg FROM notifications TO ICEBERG ON ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"S3".to_string()));
        assert!(suggestions.contains(&"GCS".to_string()));
        assert!(suggestions.contains(&"AZURE_BLOB".to_string()));
        assert!(!suggestions.contains(&"KAFKA".to_string()));
        assert!(!suggestions.contains(&"CLICKHOUSE".to_string()));
    }

    #[test]
    fn rejects_database_emitters_without_insert_action() {
        for input in [
            r#"
            CREATE EMITTER to_ch FROM notifications
            TO CLICKHOUSE clickhouse_client TABLE my_table
            VALUES { "user_id" = input.user_id }
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
            r#"
            CREATE EMITTER to_pg FROM notifications
            TO POSTGRES postgres_client TABLE my_table
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 25
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
            r#"
            CREATE EMITTER to_mysql FROM notifications
            TO MYSQL mysql_client TABLE my_table
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 25
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
            r#"
            CREATE EMITTER to_mongodb FROM notifications
            TO MONGODB mongodb_client COLLECTION my_collection
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 25
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        ] {
            let tokens = to_tokens(input);
            let errs = parse_create_emitter_tokens(&tokens).expect_err("old syntax must fail");
            assert!(!errs.is_empty());
        }
    }

    #[test]
    fn parses_create_emitter_postgres() {
        let input = r#"
            CREATE EMITTER to_pg
                FROM notifications
                TO POSTGRES postgres_client INSERT TO TABLE my_table
                VALUES {
                    "postgres_user_id" = input.user_id,
                    "postgres_now" = NOW() AS STRING,
                    "postgres_action" = LOWER(input.action)
                }
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Postgres {
                client: nervix_models::Identifier::try_from("postgres_client")
                    .expect("valid client identifier"),
                table: nervix_models::Identifier::try_from("my_table")
                    .expect("valid table identifier"),
                values: vec![
                    ClickHouseValueMapping {
                        column: "postgres_user_id".to_string(),
                        expression: expression("input.user_id"),
                    },
                    ClickHouseValueMapping {
                        column: "postgres_now".to_string(),
                        expression: expression("NOW ( ) AS STRING"),
                    },
                    ClickHouseValueMapping {
                        column: "postgres_action".to_string(),
                        expression: expression("LOWER ( input.action )"),
                    },
                ],
                conflict_action: PostgresConflictAction::None,
                max_batch: 25,
                flush_each: "10s".to_string(),
            }
        );
    }

    #[test]
    fn parses_postgres_emitter_on_conflict_do_update() {
        let input = r#"
            CREATE EMITTER to_pg
                FROM notifications
                TO POSTGRES postgres_client INSERT TO TABLE my_table
                VALUES {
                    "postgres_user_id" = input.user_id,
                    "postgres_action" = LOWER(input.action)
                }
                ON CONFLICT ("postgres_user_id") DO UPDATE
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::Postgres {
            conflict_action, ..
        } = parsed.sink.as_ref()
        else {
            panic!("expected postgres emitter sink");
        };
        assert_eq!(
            conflict_action,
            &PostgresConflictAction::DoUpdate {
                target: vec!["postgres_user_id".to_string()]
            }
        );
    }

    #[test]
    fn parses_postgres_emitter_on_conflict_do_nothing_without_target() {
        let input = r#"
            CREATE EMITTER to_pg
                FROM notifications
                TO POSTGRES postgres_client INSERT TO TABLE my_table
                VALUES {
                    "postgres_user_id" = input.user_id,
                    "postgres_action" = LOWER(input.action)
                }
                ON CONFLICT DO NOTHING
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::Postgres {
            conflict_action, ..
        } = parsed.sink.as_ref()
        else {
            panic!("expected postgres emitter sink");
        };
        assert_eq!(
            conflict_action,
            &PostgresConflictAction::DoNothing { target: Vec::new() }
        );
    }

    #[test]
    fn rejects_postgres_emitter_on_conflict_do_update_without_target() {
        let input = r#"
            CREATE EMITTER to_pg
                FROM notifications
                TO POSTGRES postgres_client INSERT TO TABLE my_table
                VALUES {
                    "postgres_user_id" = input.user_id,
                    "postgres_action" = LOWER(input.action)
                }
                ON CONFLICT DO UPDATE
                WITH MAX BATCH 25
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let error = parse_create_emitter(input).expect_err("parse must fail");
        match error {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(
                    diagnostics.iter().any(|diagnostic| diagnostic
                        .message
                        .contains("requires a conflict target")),
                    "expected conflict target diagnostic, got {diagnostics:?}"
                );
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[test]
    fn suggests_postgres_conflict_clause_before_max_batch() {
        let input = "CREATE EMITTER to_pg FROM notifications TO POSTGRES postgres_client INSERT \
                     TO TABLE my_table VALUES { \"postgres_user_id\" = input.user_id } ";
        let suggestions = suggest_create_emitter(input, input.len());

        // The whole phrase, not a bare `ON`: `ON` alone cannot be continued from here.
        assert!(suggestions.contains(&"ON CONFLICT".to_string()));
        assert!(!suggestions.contains(&"ON".to_string()));
        assert!(suggestions.contains(&"WITH MAX BATCH".to_string()));
        assert!(!suggestions.contains(&"WITH".to_string()));
    }

    #[test]
    fn suggests_postgres_conflict_actions_after_do() {
        let input = "CREATE EMITTER to_pg FROM notifications TO POSTGRES postgres_client INSERT \
                     TO TABLE my_table VALUES { \"postgres_user_id\" = input.user_id, \
                     \"postgres_action\" = input.action } ON CONFLICT (\"postgres_user_id\") DO ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"UPDATE".to_string()));
        assert!(suggestions.contains(&"NOTHING".to_string()));
    }

    #[test]
    fn rejects_postgres_emitter_without_max_batch() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_pg FROM notifications
            TO POSTGRES postgres_client INSERT TO TABLE my_table
            VALUES { "user_id" = input.user_id }
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter().any(|err| format!("{err:?}").contains("WITH")),
            "expected WITH MAX BATCH diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn rejects_postgres_emitter_without_flush_policy() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_pg FROM notifications
            TO POSTGRES postgres_client INSERT TO TABLE my_table
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 25
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
            "expected Postgres flush diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn rejects_postgres_emitter_with_zero_max_batch() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_pg FROM notifications
            TO POSTGRES postgres_client INSERT TO TABLE my_table
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 0
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter()
                .any(|err| format!("{err:?}").contains("greater than zero")),
            "expected max batch diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn parses_create_emitter_mysql() {
        let input = r#"
            CREATE EMITTER to_mysql
                FROM notifications
                TO MYSQL mysql_client INSERT TO TABLE my_table
                VALUES {
                    "mysql_user_id" = input.user_id,
                    "mysql_now" = NOW() AS STRING,
                    "mysql_action" = LOWER(input.action)
                }
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::MySql {
                client: nervix_models::Identifier::try_from("mysql_client")
                    .expect("valid client identifier"),
                table: nervix_models::Identifier::try_from("my_table")
                    .expect("valid table identifier"),
                values: vec![
                    ClickHouseValueMapping {
                        column: "mysql_user_id".to_string(),
                        expression: expression("input.user_id"),
                    },
                    ClickHouseValueMapping {
                        column: "mysql_now".to_string(),
                        expression: expression("NOW ( ) AS STRING"),
                    },
                    ClickHouseValueMapping {
                        column: "mysql_action".to_string(),
                        expression: expression("LOWER ( input.action )"),
                    },
                ],
                conflict_action: MySqlConflictAction::None,
                max_batch: 25,
                flush_each: "10s".to_string(),
            }
        );
    }

    #[test]
    fn parses_mysql_emitter_on_conflict_do_update() {
        let input = r#"
            CREATE EMITTER to_mysql
                FROM notifications
                TO MYSQL mysql_client INSERT TO TABLE my_table
                VALUES {
                    "mysql_user_id" = input.user_id,
                    "mysql_action" = LOWER(input.action)
                }
                ON CONFLICT DO UPDATE
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::MySql {
            conflict_action, ..
        } = parsed.sink.as_ref()
        else {
            panic!("expected mysql emitter sink");
        };
        assert_eq!(conflict_action, &MySqlConflictAction::DoUpdate);
    }

    #[test]
    fn parses_mysql_emitter_on_conflict_do_nothing() {
        let input = r#"
            CREATE EMITTER to_mysql
                FROM notifications
                TO MYSQL mysql_client INSERT TO TABLE my_table
                VALUES {
                    "mysql_user_id" = input.user_id,
                    "mysql_action" = LOWER(input.action)
                }
                ON CONFLICT DO NOTHING
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::MySql {
            conflict_action, ..
        } = parsed.sink.as_ref()
        else {
            panic!("expected mysql emitter sink");
        };
        assert_eq!(conflict_action, &MySqlConflictAction::DoNothing);
    }

    #[test]
    fn rejects_mysql_emitter_on_conflict_target() {
        let input = r#"
            CREATE EMITTER to_mysql
                FROM notifications
                TO MYSQL mysql_client INSERT TO TABLE my_table
                VALUES { "mysql_user_id" = input.user_id }
                ON CONFLICT ("mysql_user_id") DO UPDATE
                WITH MAX BATCH 25
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_emitter(input).expect_err("mysql conflict target must fail");
    }

    #[test]
    fn suggests_mysql_conflict_clause_before_max_batch() {
        let input = "CREATE EMITTER to_mysql FROM notifications TO MYSQL mysql_client INSERT TO \
                     TABLE my_table VALUES { \"mysql_user_id\" = input.user_id } ";
        let suggestions = suggest_create_emitter(input, input.len());

        // The whole phrase, not a bare `ON`: `ON` alone cannot be continued from here.
        assert!(suggestions.contains(&"ON CONFLICT".to_string()));
        assert!(!suggestions.contains(&"ON".to_string()));
        assert!(suggestions.contains(&"WITH MAX BATCH".to_string()));
        assert!(!suggestions.contains(&"WITH".to_string()));
    }

    #[test]
    fn suggests_mysql_conflict_actions_after_do() {
        let input = "CREATE EMITTER to_mysql FROM notifications TO MYSQL mysql_client INSERT TO \
                     TABLE my_table VALUES { \"mysql_user_id\" = input.user_id } ON CONFLICT DO ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"UPDATE".to_string()));
        assert!(suggestions.contains(&"NOTHING".to_string()));
    }

    #[test]
    fn rejects_mysql_emitter_without_max_batch() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_mysql FROM notifications
            TO MYSQL mysql_client INSERT TO TABLE my_table
            VALUES { "user_id" = input.user_id }
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter().any(|err| format!("{err:?}").contains("WITH")),
            "expected WITH MAX BATCH diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn rejects_mysql_emitter_without_flush_policy() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_mysql FROM notifications
            TO MYSQL mysql_client INSERT TO TABLE my_table
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 25
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
            "expected MySQL flush diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn parses_create_emitter_mongodb() {
        let input = r#"
            CREATE EMITTER to_mongodb
                FROM notifications
                TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
                VALUES {
                    "mongodb_user_id" = input.user_id,
                    "mongodb_now" = NOW() AS STRING,
                    "mongodb_action" = LOWER(input.action)
                }
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::MongoDb {
                client: nervix_models::Identifier::try_from("mongodb_client")
                    .expect("valid client identifier"),
                collection: nervix_models::Identifier::try_from("my_collection")
                    .expect("valid collection identifier"),
                values: vec![
                    ClickHouseValueMapping {
                        column: "mongodb_user_id".to_string(),
                        expression: expression("input.user_id"),
                    },
                    ClickHouseValueMapping {
                        column: "mongodb_now".to_string(),
                        expression: expression("NOW ( ) AS STRING"),
                    },
                    ClickHouseValueMapping {
                        column: "mongodb_action".to_string(),
                        expression: expression("LOWER ( input.action )"),
                    },
                ],
                conflict_action: MongoDbConflictAction::None,
                max_batch: 25,
                flush_each: "10s".to_string(),
            }
        );
    }

    #[test]
    fn parses_mongodb_emitter_on_conflict_do_update() {
        let input = r#"
            CREATE EMITTER to_mongodb
                FROM notifications
                TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
                VALUES {
                    "mongodb_user_id" = input.user_id,
                    "mongodb_action" = LOWER(input.action)
                }
                ON CONFLICT ("mongodb_user_id") DO UPDATE
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::MongoDb {
            conflict_action, ..
        } = parsed.sink.as_ref()
        else {
            panic!("expected mongodb emitter sink");
        };
        assert_eq!(
            conflict_action,
            &MongoDbConflictAction::DoUpdate {
                target: vec!["mongodb_user_id".to_string()]
            }
        );
    }

    #[test]
    fn parses_mongodb_emitter_on_conflict_do_nothing() {
        let input = r#"
            CREATE EMITTER to_mongodb
                FROM notifications
                TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
                VALUES {
                    "mongodb_user_id" = input.user_id,
                    "mongodb_action" = LOWER(input.action)
                }
                ON CONFLICT ("mongodb_user_id") DO NOTHING
                WITH MAX BATCH 25
                MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::MongoDb {
            conflict_action, ..
        } = parsed.sink.as_ref()
        else {
            panic!("expected mongodb emitter sink");
        };
        assert_eq!(
            conflict_action,
            &MongoDbConflictAction::DoNothing {
                target: vec!["mongodb_user_id".to_string()]
            }
        );
    }

    #[test]
    fn rejects_mongodb_emitter_on_conflict_do_update_without_target() {
        let input = r#"
            CREATE EMITTER to_mongodb
                FROM notifications
                TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
                VALUES {
                    "mongodb_user_id" = input.user_id,
                    "mongodb_action" = LOWER(input.action)
                }
                ON CONFLICT DO UPDATE
                WITH MAX BATCH 25
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_emitter(input).expect_err("mongodb conflict target must fail");
    }

    #[test]
    fn rejects_mongodb_emitter_on_conflict_target_not_mapped() {
        let input = r#"
            CREATE EMITTER to_mongodb
                FROM notifications
                TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
                VALUES {
                    "mongodb_action" = LOWER(input.action)
                }
                ON CONFLICT ("mongodb_user_id") DO UPDATE
                WITH MAX BATCH 25
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let error = parse_create_emitter(input).expect_err("parse must fail");
        match error {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(
                    diagnostics
                        .iter()
                        .any(|diagnostic| diagnostic.message.contains("is not mapped in VALUES")),
                    "expected unmapped target diagnostic, got {diagnostics:?}"
                );
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[test]
    fn suggests_mongodb_conflict_clause_before_max_batch() {
        let input = "CREATE EMITTER to_mongodb FROM notifications TO MONGODB mongodb_client \
                     INSERT TO COLLECTION my_collection VALUES { \"mongodb_user_id\" = \
                     input.user_id } ";
        let suggestions = suggest_create_emitter(input, input.len());

        // The whole phrase, not a bare `ON`: `ON` alone cannot be continued from here.
        assert!(suggestions.contains(&"ON CONFLICT".to_string()));
        assert!(!suggestions.contains(&"ON".to_string()));
        assert!(suggestions.contains(&"WITH MAX BATCH".to_string()));
        assert!(!suggestions.contains(&"WITH".to_string()));
    }

    #[test]
    fn suggests_mongodb_conflict_actions_after_do() {
        let input = "CREATE EMITTER to_mongodb FROM notifications TO MONGODB mongodb_client \
                     INSERT TO COLLECTION my_collection VALUES { \"mongodb_user_id\" = \
                     input.user_id, \"mongodb_action\" = input.action } ON CONFLICT \
                     (\"mongodb_user_id\") DO ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"UPDATE".to_string()));
        assert!(suggestions.contains(&"NOTHING".to_string()));
    }

    #[test]
    fn rejects_mongodb_emitter_without_flush_policy() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_mongodb FROM notifications
            TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
            VALUES { "user_id" = input.user_id }
            WITH MAX BATCH 25
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
            "expected MongoDB flush diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn rejects_mongodb_emitter_without_max_batch() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER to_mongodb FROM notifications
            TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
            VALUES { "user_id" = input.user_id }
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter().any(|err| format!("{err:?}").contains("WITH")),
            "expected WITH MAX BATCH diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn suggests_clickhouse_after_to_without_schema_leakage() {
        let input = "CREATE EMITTER to_ch FROM notifications TO ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"CLICKHOUSE".to_string()));
        assert!(suggestions.contains(&"POSTGRES".to_string()));
        assert!(suggestions.contains(&"MYSQL".to_string()));
        assert!(suggestions.contains(&"MONGODB".to_string()));
        assert!(!suggestions.contains(&"SCHEMA".to_string()));
    }

    #[test]
    fn suggests_database_insert_action_after_client() {
        let input = "CREATE EMITTER to_pg FROM notifications TO POSTGRES postgres_client ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"INSERT TO".to_string()));
        assert!(!suggestions.contains(&"TABLE".to_string()));
    }

    #[test]
    fn suggests_database_target_after_insert_action() {
        let postgres_input =
            "CREATE EMITTER to_pg FROM notifications TO POSTGRES postgres_client INSERT TO ";
        let postgres_suggestions = suggest_create_emitter(postgres_input, postgres_input.len());
        assert!(postgres_suggestions.contains(&"TABLE".to_string()));
        assert!(!postgres_suggestions.contains(&"COLLECTION".to_string()));

        let mongodb_input =
            "CREATE EMITTER to_mongodb FROM notifications TO MONGODB mongodb_client INSERT TO ";
        let mongodb_suggestions = suggest_create_emitter(mongodb_input, mongodb_input.len());
        assert!(mongodb_suggestions.contains(&"COLLECTION".to_string()));
        assert!(!mongodb_suggestions.contains(&"TABLE".to_string()));
    }

    #[test]
    fn parses_create_emitter_pulsar() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                TO PULSAR pulsar1 TOPIC topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Pulsar {
                client: nervix_models::Identifier::try_from("pulsar1")
                    .expect("valid client identifier"),
                topic: nervix_models::Identifier::try_from("topic")
                    .expect("valid topic identifier"),
            }
        );
    }

    #[test]
    fn parses_create_emitter_detached() {
        let input = r#"
            CREATE DETACHED EMITTER emit
                FROM p99
                TO KAFKA broker1 TOPIC topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.mode, AckMode::Detached);
    }

    #[test]
    fn fails_without_to_clause() {
        let tokens = to_tokens("CREATE ATTACHED EMITTER emit FROM p99 ENCODE USING my_codec;");
        let errs = parse_create_emitter_tokens(&tokens).expect_err("must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn rejects_pulsar_emitter_without_topic() {
        let tokens = to_tokens("CREATE ATTACHED EMITTER emit FROM p99 TO PULSAR pulsar1;");
        let errs = parse_create_emitter_tokens(&tokens).expect_err("must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn rejects_encoded_emitter_without_codec() {
        let tokens = to_tokens(
            r#"
            CREATE EMITTER emit
                FROM p99
                TO KAFKA broker1 TOPIC topic
                MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );
        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter()
                .any(|err| format!("{err:?}").contains("ENCODE USING")),
            "expected codec diagnostic, got {errs:?}"
        );
    }

    #[test]
    fn suggests_mode_after_create() {
        let input = "CREATE ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"ATTACHED".to_string()));
        assert!(suggestions.contains(&"DETACHED".to_string()));
        assert!(!suggestions.contains(&"FROM".to_string()));
    }

    #[test]
    fn suggests_encode_using_as_compound_keyword() {
        // The codec follows the sink it encodes for, so it is offered once the sink is named.
        let input = "CREATE ATTACHED EMITTER emit FROM p99 TO KAFKA broker1 TOPIC topic MODE \
                     NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"ENCODE USING".to_string()));

        let before_sink = "CREATE ATTACHED EMITTER emit FROM p99 ";
        let suggestions = suggest_create_emitter(before_sink, before_sink.len());
        assert!(!suggestions.contains(&"ENCODE USING".to_string()));
    }

    #[test]
    fn suggests_sink_after_to() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 TO ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"KAFKA".to_string()));
        assert!(suggestions.contains(&"PULSAR".to_string()));
        assert!(suggestions.contains(&"RABBITMQ".to_string()));
        assert!(suggestions.contains(&"REDIS".to_string()));
        assert!(suggestions.contains(&"MQTT".to_string()));
        assert!(suggestions.contains(&"NATS".to_string()));
        assert!(suggestions.contains(&"ZEROMQ".to_string()));
        assert!(suggestions.contains(&"SQS".to_string()));
        assert!(suggestions.contains(&"CLICKHOUSE".to_string()));
        assert!(suggestions.contains(&"POSTGRES".to_string()));
        assert!(suggestions.contains(&"MYSQL".to_string()));
        assert!(suggestions.contains(&"MONGODB".to_string()));
        assert!(suggestions.contains(&"SENTRY".to_string()));
    }

    #[test]
    fn sentry_sink_completion_context_does_not_leak_transport_qualifiers() {
        let client_input = "CREATE EMITTER emit FROM errors TO SENTRY ";
        let client_suggestions = suggest_create_emitter(client_input, client_input.len());
        assert!(client_suggestions.contains(&"ref:client".to_string()));
        assert!(!client_suggestions.contains(&"TOPIC".to_string()));
        assert!(!client_suggestions.contains(&"QUEUE".to_string()));

        let route_input = "CREATE EMITTER emit FROM errors TO SENTRY sentry_main MODE ACK RETRY \
                           POLICY BACKOFF 250ms MAX 30s ENCODE USING error_event_codec ";
        let route_suggestions = suggest_create_emitter(route_input, route_input.len());
        assert!(route_suggestions.contains(&"INHERIT".to_string()));
        assert!(route_suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!route_suggestions.contains(&"TOPIC".to_string()));
        assert!(!route_suggestions.contains(&"QUEUE".to_string()));
        assert!(!route_suggestions.contains(&"SUBJECT".to_string()));
    }

    #[test]
    fn suggests_flush_after_emitter_error_policies() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 TO KAFKA broker1 TOPIC topic MODE \
                     NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec ON MESSAGE \
                     ERROR LOG ON GENERAL ERROR LOG ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
        assert!(!suggestions.contains(&"COMMIT EACH".to_string()));
    }

    #[test]
    fn parses_create_emitter_mqtt() {
        let input = r#"
            CREATE ATTACHED EMITTER emit
                FROM p99
                TO MQTT broker1 TOPIC topic MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Mqtt {
                client: nervix_models::Identifier::try_from("broker1")
                    .expect("valid client identifier"),
                topic: nervix_models::Identifier::try_from("topic")
                    .expect("valid topic identifier"),
            }
        );
    }

    #[test]
    fn parses_create_emitter_nats() {
        let input = r#"
            CREATE ATTACHED EMITTER emit
                FROM p99
                TO NATS nats_main SUBJECT notifications MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Nats {
                client: nervix_models::Identifier::try_from("nats_main")
                    .expect("valid client identifier"),
                subject: nervix_models::Identifier::try_from("notifications")
                    .expect("valid subject identifier"),
            }
        );
    }

    #[test]
    fn parses_create_emitter_rabbitmq() {
        let input = r#"
            CREATE ATTACHED EMITTER emit
                FROM p99
                TO RABBITMQ broker1 QUEUE queue1 MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::RabbitMq {
                client: nervix_models::Identifier::try_from("broker1")
                    .expect("valid client identifier"),
                queue: nervix_models::Identifier::try_from("queue1")
                    .expect("valid queue identifier"),
            }
        );
    }

    #[test]
    fn parses_create_emitter_redis() {
        let input = r#"
            CREATE ATTACHED EMITTER emit
                FROM p99
                TO REDIS PUBSUB broker1 CHANNEL out MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Redis {
                client: nervix_models::Identifier::try_from("broker1")
                    .expect("valid client identifier"),
                channel: nervix_models::Identifier::try_from("out")
                    .expect("valid channel identifier"),
            }
        );
    }

    #[test]
    fn rejects_redis_emitter_without_pubsub_action() {
        let tokens = to_tokens(
            r#"
            CREATE ATTACHED EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO REDIS broker1 CHANNEL out FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("old syntax must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn suggests_pubsub_action_after_redis_sink() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 TO REDIS ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"PUBSUB".to_string()));
        assert!(!suggestions.contains(&"CHANNEL".to_string()));
    }

    #[test]
    fn parses_create_emitter_zeromq() {
        let input = r#"
            CREATE ATTACHED EMITTER emit
                FROM p99
                TO ZEROMQ zmq_out MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::ZeroMq {
                client: nervix_models::Identifier::try_from("zmq_out")
                    .expect("valid client identifier"),
            }
        );
    }

    #[test]
    fn parses_create_emitter_sqs() {
        let input = r#"
            CREATE ATTACHED EMITTER emit
                FROM p99
                TO SQS sqs_main QUEUE queue1 MODE SINGLE RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink.as_ref(),
            &EmitSink::Sqs {
                client: nervix_models::Identifier::try_from("sqs_main")
                    .expect("valid client identifier"),
                queue: "queue1".to_string(),
                fifo_group: None,
            }
        );
    }

    #[test]
    fn parses_codec_emitter_route_construction() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                TO KAFKA broker1 TOPIC topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec
                INHERIT ALL EXCEPT raw
                SET normalized = lower(input.name), score = input.score AS FLOAT64
                WHERE output.active
                INVOKE write_header(lower("TENANT"), input.tenant), write_header("route", output.normalized)
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");

        assert!(matches!(
            parsed.construction.inherit,
            Some(nervix_models::Inheritance::AllExcept(ref fields)) if fields.len() == 1
        ));
        assert_eq!(parsed.construction.assignments.len(), 2);
        assert_eq!(parsed.construction.invocations.len(), 2);
    }

    #[test]
    fn parses_emitter_with_invoke_only_route() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                TO KAFKA broker1 TOPIC topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec
                INVOKE write_header("route", input.route)
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        assert_eq!(parsed.construction.invocations.len(), 1);
    }

    #[test]
    fn rejects_invalid_emitter_route_construction() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                TO KAFKA broker1 TOPIC topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec
                SET normalized = FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let error = parse_create_emitter(input).expect_err("parse should fail");
        match error {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[test]
    fn does_not_leak_sink_suggestions_inside_filter_map_program() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 TO KAFKA broker1 TOPIC topic MODE \
                     NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec WHERE ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(!suggestions.contains(&"MQTT".to_string()));
        assert!(!suggestions.contains(&"NATS".to_string()));
    }

    #[test]
    fn emitter_filter_map_context_suggests_invoke_without_sink_leakage() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 TO KAFKA broker1 TOPIC topic MODE \
                     NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"INVOKE".to_string()));
        assert!(!suggestions.contains(&"SUBJECT".to_string()));
        assert!(!suggestions.contains(&"QUEUE".to_string()));
    }

    #[test]
    fn pulsar_sink_context_does_not_offer_other_transport_keywords() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 TO PULSAR pulsar1 ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"TOPIC".to_string()));
        assert!(!suggestions.contains(&"QUEUE".to_string()));
        assert!(!suggestions.contains(&"SUBJECT".to_string()));
        assert!(!suggestions.contains(&"CHANNEL".to_string()));
    }
}
