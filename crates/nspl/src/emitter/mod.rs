use std::borrow::Cow;

use chumsky::{error::LabelError, prelude::*, util::MaybeRef};
use nervix_models::{
    AckMode, ClickHouseValueMapping, CreateEmitter, CreateStatement, EmitSink, IcebergCatalog,
    IcebergStorageBackend, IcebergValueMapping, MongoDbConflictAction, MySqlConflictAction,
    PostgresConflictAction,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, byte_size_lit, channel_ref, client_ref,
        codec_ref, current_word_prefix, duration_lit, emitter_name, flush_each,
        general_error_policy, if_not_exists_clause, into_parse_error, kw, kw_phrase2, lex_input,
        materialized_state_dependencies, message_error_policy, queue_ref, relay_ref,
        render_vm_program_tokens, route_construction, string_lit, suggestions_from_errors,
        table_ref, tok, topic_ref,
    },
};

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

fn kinesis_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Kinesis)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Relay))
        .then(relay_ref())
        .map(|(client, relay)| EmitSink::Kinesis { client, relay })
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

fn sqs_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Sqs)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Queue))
        .then(queue_ref())
        .map(|(client, queue)| EmitSink::Sqs { client, queue })
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
        choice((parenthesized, leaf))
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
        .map(|((client, table), values)| EmitSink::ClickHouse {
            client,
            table,
            values,
            flush_each: String::new(),
        })
}

fn max_batch<'src>() -> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::With)
        .ignore_then(kw(Identifier::Max))
        .ignore_then(kw(Identifier::Batch))
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

fn on_conflict_phrase<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    custom(|inp| {
        let before = inp.save();
        let start = inp.cursor();
        let first = inp.next_maybe();
        let first_span = inp.span_since(&start);
        let Some(first_token) = first.as_deref() else {
            return Err(expected_label_error("ON", first, first_span));
        };

        if !token_is_keyword(first_token, Identifier::On) {
            inp.rewind(before);
            return Err(expected_label_error("ON", first, first_span));
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

fn iceberg_emit_sink_parser<'src>()
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

fn emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    choice((
        clickhouse_emit_sink_parser(),
        postgres_emit_sink_parser(),
        mysql_emit_sink_parser(),
        mongodb_emit_sink_parser(),
        iceberg_emit_sink_parser(),
        kinesis_emit_sink_parser(),
        kafka_emit_sink_parser(),
        pulsar_emit_sink_parser(),
        rabbitmq_emit_sink_parser(),
        redis_emit_sink_parser(),
        mqtt_emit_sink_parser(),
        nats_emit_sink_parser(),
        zeromq_emit_sink_parser(),
        sqs_emit_sink_parser(),
    ))
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
        .then(relay_ref())
        .then(
            kw_phrase2(Identifier::Encode, Identifier::Using)
                .ignore_then(codec_ref())
                .or_not(),
        )
        .then(materialized_state_dependencies())
        .then_ignore(kw(Identifier::To))
        .then(emit_sink_parser())
        .then(route_construction().or_not())
        .then(flush_each())
        .then(iceberg_commit_each().or_not())
        .then(message_error_policy())
        .then(general_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .try_map(
            |(
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (((if_not_exists, mode), name), from_relay),
                                            encode_using_codec,
                                        ),
                                        materialized_state,
                                    ),
                                    sink,
                                ),
                                construction,
                            ),
                            sink_flush_each,
                        ),
                        sink_commit_each,
                    ),
                    message_error_policy,
                ),
                general_error_policy,
            ),
             span| {
                if let EmitSink::Iceberg { .. } = &sink
                    && encode_using_codec.is_some()
                {
                    return Err(Rich::custom(
                        span,
                        "Iceberg emitters write typed records and do not support ENCODE USING",
                    ));
                }
                if sink.requires_codec() && encode_using_codec.is_none() {
                    return Err(Rich::custom(
                        span,
                        "encoded emitters require ENCODE USING <codec>",
                    ));
                }
                let construction = construction.unwrap_or_default();
                if encode_using_codec.is_none()
                    && (construction.inherit.is_some()
                        || !construction.assignments.is_empty()
                        || !construction.invocations.is_empty())
                {
                    return Err(Rich::custom(
                        span,
                        "direct emitter routes support VALUES and WHERE only",
                    ));
                }
                let iceberg_commit_each = if let Some(commit_each) = sink_commit_each {
                    if let EmitSink::Iceberg { .. } = &sink {
                        Some(commit_each)
                    } else {
                        return Err(Rich::custom(
                            span,
                            "COMMIT EACH is only supported by Iceberg emitters",
                        ));
                    }
                } else {
                    None
                };
                let sink = match (sink, sink_flush_each.clone()) {
                    (
                        EmitSink::ClickHouse {
                            client,
                            table,
                            values,
                            ..
                        },
                        (flush_each, _max_batch_size),
                    ) => EmitSink::ClickHouse {
                        client,
                        table,
                        values,
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
                        EmitSink::Iceberg {
                            backend,
                            client,
                            table,
                            values,
                            location,
                            catalog,
                            ..
                        },
                        (flush_each, max_batch_size),
                    ) => {
                        let Some((commit_each, max_commit_size)) = iceberg_commit_each else {
                            return Err(Rich::custom(
                                span,
                                "Iceberg emitters require COMMIT EACH <duration> MAX SIZE <bytes>",
                            ));
                        };
                        EmitSink::Iceberg {
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
                        }
                    }
                    (sink, _) => sink,
                };
                let (flush_each, max_batch_size) = sink_flush_each;
                Ok(CreateStatement::new(
                    CreateEmitter {
                        name,
                        from_relay,
                        encode_using_codec,
                        sink,
                        flush_each,
                        max_batch_size,
                        error_policies: nervix_models::ErrorPolicies {
                            message: message_error_policy,
                            general: general_error_policy,
                        },
                        mode: mode.unwrap_or(AckMode::Attached),
                        construction,
                        materialized_state,
                    },
                    if_not_exists,
                ))
            },
        )
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

pub fn parse_create_emitter(
    input: &str,
) -> Result<CreateStatement<CreateEmitter>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_emitter_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_emitter(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_emitter_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
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

    fn expression(source: &str) -> nervix_models::Expression {
        crate::parse_expression(source).expect("valid structured expression")
    }

    #[test]
    fn parses_create_emitter_kafka() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO KAFKA broker1 TOPIC topic FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "emit");
        assert_eq!(parsed.from_relay.as_str(), "p99");
        assert_eq!(
            parsed
                .encode_using_codec
                .as_ref()
                .map(|codec| codec.as_str()),
            Some("my_codec")
        );
        assert_eq!(
            parsed.sink,
            EmitSink::Kafka {
                client: nervix_models::Identifier::try_from("broker1")
                    .expect("valid client identifier"),
                topic: nervix_models::Identifier::try_from("topic")
                    .expect("valid topic identifier"),
            }
        );
        assert_eq!(parsed.mode, AckMode::Attached);
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(
            parsed.sink,
            EmitSink::ClickHouse {
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
                CATALOG iceberg_catalog
                FLUSH EACH 10s MAX BATCH SIZE 64MiB COMMIT EACH 1m MAX SIZE 512MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(parsed.mode, AckMode::Detached);
        assert_eq!(
            parsed.sink,
            EmitSink::Iceberg {
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
                CATALOG iceberg_catalog
                FLUSH IMMEDIATE COMMIT EACH 1m MAX SIZE 512MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::Iceberg {
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
                CATALOG iceberg_catalog
                FLUSH IMMEDIATE COMMIT EACH 1m MAX SIZE 512MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::Iceberg {
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
            errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
            "expected Iceberg flush diagnostic, got {errs:?}"
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
                .any(|err| format!("{err:?}").contains("do not support ENCODE USING")),
            "expected Iceberg ENCODE USING diagnostic, got {errs:?}"
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
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB COMMIT EACH 1m MAX SIZE 512MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            "#,
        );

        let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
        assert!(
            errs.iter()
                .any(|err| format!("{err:?}").contains("only supported by Iceberg")),
            "expected non-Iceberg COMMIT EACH diagnostic, got {errs:?}"
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
            CATALOG SAME CLIENT LOCATION 's3://nervix-iceberg/catalogs/input.catalog.json'
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB COMMIT EACH 1m MAX SIZE 512MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(
            parsed.sink,
            EmitSink::Postgres {
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::Postgres {
            conflict_action, ..
        } = &parsed.sink
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::Postgres {
            conflict_action, ..
        } = &parsed.sink
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

        assert!(suggestions.contains(&"ON".to_string()));
        assert!(suggestions.contains(&"WITH".to_string()));
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(
            parsed.sink,
            EmitSink::MySql {
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::MySql {
            conflict_action, ..
        } = &parsed.sink
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::MySql {
            conflict_action, ..
        } = &parsed.sink
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

        assert!(suggestions.contains(&"ON".to_string()));
        assert!(suggestions.contains(&"WITH".to_string()));
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.encode_using_codec, None);
        assert_eq!(
            parsed.sink,
            EmitSink::MongoDb {
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::MongoDb {
            conflict_action, ..
        } = &parsed.sink
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
                FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_emitter(input).expect("parse should succeed");
        let EmitSink::MongoDb {
            conflict_action, ..
        } = &parsed.sink
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

        assert!(suggestions.contains(&"ON".to_string()));
        assert!(suggestions.contains(&"WITH".to_string()));
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
                ENCODE USING my_codec
                TO PULSAR pulsar1 TOPIC topic FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::Pulsar {
                client: nervix_models::Identifier::try_from("pulsar1")
                    .expect("valid client identifier"),
                topic: nervix_models::Identifier::try_from("topic")
                    .expect("valid topic identifier"),
            }
        );
    }

    #[test]
    fn parses_create_emitter_kinesis() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO KINESIS kinesis_main RELAY events FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::Kinesis {
                client: nervix_models::Identifier::try_from("kinesis_main")
                    .expect("valid client identifier"),
                relay: nervix_models::Identifier::try_from("events")
                    .expect("valid relay identifier"),
            }
        );
    }

    #[test]
    fn parses_create_emitter_detached() {
        let input = r#"
            CREATE DETACHED EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO KAFKA broker1 TOPIC topic FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
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
        let tokens = to_tokens(
            "CREATE ATTACHED EMITTER emit FROM p99 ENCODE USING my_codec TO PULSAR pulsar1;",
        );
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
        let input = "CREATE ATTACHED EMITTER emit FROM p99 ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"ENCODE USING".to_string()));
    }

    #[test]
    fn suggests_sink_after_to() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 ENCODE USING my_codec TO ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"KINESIS".to_string()));
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
    }

    #[test]
    fn suggests_flush_after_emitter_error_policies() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 ENCODE USING my_codec TO KAFKA broker1 \
                     TOPIC topic ON MESSAGE ERROR LOG ON GENERAL ERROR LOG ";
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
                ENCODE USING my_codec
                TO MQTT broker1 TOPIC topic FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::Mqtt {
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
                ENCODE USING my_codec
                TO NATS nats_main SUBJECT notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::Nats {
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
                ENCODE USING my_codec
                TO RABBITMQ broker1 QUEUE queue1 FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::RabbitMq {
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
                ENCODE USING my_codec
                TO REDIS PUBSUB broker1 CHANNEL out FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::Redis {
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
        let input = "CREATE ATTACHED EMITTER emit FROM p99 ENCODE USING my_codec TO REDIS ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"PUBSUB".to_string()));
        assert!(!suggestions.contains(&"CHANNEL".to_string()));
    }

    #[test]
    fn parses_create_emitter_zeromq() {
        let input = r#"
            CREATE ATTACHED EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO ZEROMQ zmq_out FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::ZeroMq {
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
                ENCODE USING my_codec
                TO SQS sqs_main QUEUE queue1 FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.sink,
            EmitSink::Sqs {
                client: nervix_models::Identifier::try_from("sqs_main")
                    .expect("valid client identifier"),
                queue: nervix_models::Identifier::try_from("queue1")
                    .expect("valid queue identifier"),
            }
        );
    }

    #[test]
    fn parses_codec_emitter_route_construction() {
        let input = r#"
            CREATE EMITTER emit
                FROM p99
                ENCODE USING my_codec
                TO KAFKA broker1 TOPIC topic
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
                ENCODE USING my_codec
                TO KAFKA broker1 TOPIC topic
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
                ENCODE USING my_codec
                TO KAFKA broker1 TOPIC topic
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
        let input = "CREATE ATTACHED EMITTER emit FROM p99 ENCODE USING my_codec TO KAFKA broker1 \
                     TOPIC topic WHERE ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(!suggestions.contains(&"MQTT".to_string()));
        assert!(!suggestions.contains(&"NATS".to_string()));
    }

    #[test]
    fn emitter_filter_map_context_suggests_invoke_without_sink_leakage() {
        let input = "CREATE ATTACHED EMITTER emit FROM p99 ENCODE USING my_codec TO KAFKA broker1 \
                     TOPIC topic ";
        let suggestions = suggest_create_emitter(input, input.len());

        assert!(suggestions.contains(&"INVOKE".to_string()));
        assert!(!suggestions.contains(&"SUBJECT".to_string()));
        assert!(!suggestions.contains(&"QUEUE".to_string()));
    }

    #[test]
    fn pulsar_sink_context_does_not_offer_other_transport_keywords() {
        let input =
            "CREATE ATTACHED EMITTER emit FROM p99 ENCODE USING my_codec TO PULSAR pulsar1 ";
        let suggestions = suggest_create_emitter(input, input.len());
        assert!(suggestions.contains(&"TOPIC".to_string()));
        assert!(!suggestions.contains(&"QUEUE".to_string()));
        assert!(!suggestions.contains(&"SUBJECT".to_string()));
        assert!(!suggestions.contains(&"CHANNEL".to_string()));
    }
}
