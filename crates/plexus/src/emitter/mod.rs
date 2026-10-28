//! Shared emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** Kinded emitter heads, shared statement tails, ALTER operations and connector dispatch.
//! - **Depends on.** Shared parser clauses and each connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::borrow::Cow;

use ahash_compile_time::HashSet;
use chumsky::{error::LabelError, prelude::*, util::MaybeRef};
use delivery::{AcknowledgedPolicy, acknowledged_policy, request_policy, unconfirmed_policy};
use envelope::entity_envelope;
use meticulous::OptionExt as _;
use nervix_models::*;

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::*,
};

/// SHOW EMITTERS lists the configured emitters of the selected domain.
pub fn show_emitters_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowEmitters, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Show, Identifier::Emitters)
        .to(ShowEmitters)
        .then_ignore(tok(Token::Semicolon).or_not())
}

// Typed constructors share lexical patterns; each entity's closed enum owns its mode set.
macro_rules! broker_mode {
    ($Mode:ident) => {
        choice((
            unconfirmed_policy().map(|retry_policy| $Mode::NoAck { retry_policy }),
            acknowledged_policy().map(
                |AcknowledgedPolicy {
                     window,
                     ack_timeout,
                     retry_policy,
                 }| $Mode::Ack {
                    window,
                    ack_timeout,
                    retry_policy,
                },
            ),
        ))
        .boxed()
    };
}
macro_rules! unconfirmed_mode {
    ($Mode:ident) => {
        unconfirmed_policy()
            .map(|retry_policy| $Mode::NoAck { retry_policy })
            .boxed()
    };
}
macro_rules! request_mode {
    ($Mode:ident) => {
        request_policy()
            .map(|retry_policy| $Mode::Ack { retry_policy })
            .boxed()
    };
}
macro_rules! consumer_mode {
    ($Mode:ident) => {
        acknowledged_policy()
            .map(
                |AcknowledgedPolicy {
                     window,
                     ack_timeout,
                     retry_policy,
                 }| $Mode::Ack {
                    window,
                    ack_timeout,
                    retry_policy,
                },
            )
            .boxed()
    };
}

mod clickhouse;
mod client;
mod delivery;
mod envelope;
mod http;
mod iceberg;
mod kafka;
mod mongodb;
mod mqtt;
mod mysql;
mod nats;
mod otel;
mod postgres;
mod pulsar;
mod rabbitmq;
mod redis;
mod sentry;
mod sqs;
mod syslog;
mod zeromq;

#[cfg(test)]
mod kind_tests;

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

fn batch_clause<'src>()
-> impl Parser<'src, &'src [Token], EmitterBatchPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Identifier::Batch, Identifier::Max, Identifier::Messages)
        .ignore_then(u64_value("message_count").try_map(|value, span| {
            BatchMessageLimit::try_from(value)
                .map_err(|error| Rich::custom(span, error.to_string()))
        }))
        .then_ignore(kw_phrase2(Identifier::Max, Identifier::Size))
        .then(payload_size_lit("BATCH MAX SIZE"))
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

fn encode_using_clause<'src>()
-> impl Parser<'src, &'src [Token], CodecName, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Encode, Identifier::Using)
        .ignore_then(codec_ref())
        .boxed()
}
/// A complete body of one statically selected emitter entity. The statement converts the
/// concrete entity into the public union exactly once, after its kinded head has parsed.
struct ParsedDestination<S> {
    destination: S,
    body: EmitterBody,
    envelope: EmitterEnvelope,
    construction: Option<RouteConstruction>,
    batch: Option<EmitterBatchPolicy>,
}

/// The enclosing statement determines where a construction's comma-separated lists end.
#[derive(Clone, Copy)]
enum EmitterContext {
    Create,
    Alter,
}

impl EmitterContext {
    fn construction<'src>(
        self,
    ) -> Boxed<'src, 'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> {
        match self {
            Self::Create => route_construction().boxed(),
            Self::Alter => alter_route_construction().boxed(),
        }
    }

    fn bodyless_construction<'src>(
        self,
    ) -> Boxed<'src, 'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> {
        match self {
            Self::Create => bodyless_route_construction().boxed(),
            Self::Alter => bodyless_alter_route_construction().boxed(),
        }
    }
}

fn encoded_destination<'src, S, P>(
    context: EmitterContext,
    definition: P,
) -> impl Parser<'src, &'src [Token], ParsedDestination<S>, extra::Err<ParseError<'src>>> + Clone
where
    S: EmitterEntityModel + Clone + 'src,
    P: Parser<'src, &'src [Token], S, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    definition
        .then(encode_using_clause())
        .then(entity_envelope::<S>())
        .then(context.construction().or_not())
        .map(
            |(((destination, codec), envelope), construction)| ParsedDestination {
                destination,
                body: EmitterBody::Codec { codec },
                envelope,
                construction,
                batch: None,
            },
        )
        .boxed()
}

fn values_destination<'src, S, P>(
    definition: P,
) -> impl Parser<'src, &'src [Token], ParsedDestination<S>, extra::Err<ParseError<'src>>> + Clone
where
    S: EmitterEntityModel + Clone + 'src,
    P: Parser<'src, &'src [Token], S, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    definition
        .then(entity_envelope::<S>())
        .then(where_only_route_construction().or_not())
        .map(
            |((destination, envelope), construction)| ParsedDestination {
                destination,
                body: EmitterBody::Values,
                envelope,
                construction,
                batch: None,
            },
        )
        .boxed()
}

fn head<'src>(
    kind: EmitterEntityKind,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    connector_entity_head(kind.connector(), kind.entity_keyword(), Identifier::Emitter)
}

pub(crate) fn head_kind<'src>()
-> impl Parser<'src, &'src [Token], EmitterEntityKind, extra::Err<ParseError<'src>>> + Clone {
    let heads = EmitterEntityKind::all()
        .map(|kind| head(kind).to(kind).boxed())
        .collect::<Vec<_>>();
    choice(heads).boxed()
}

fn with_batch<'src, S, P>(
    definition: P,
) -> Boxed<'src, 'src, &'src [Token], ParsedDestination<S>, extra::Err<ParseError<'src>>>
where
    S: EmitterEntityModel + Clone + 'src,
    P: Parser<'src, &'src [Token], ParsedDestination<S>, extra::Err<ParseError<'src>>>
        + Clone
        + 'src,
{
    match S::KIND.capabilities().batch {
        EmitterBatchLegality::Required { .. } => definition
            .then(batch_clause())
            .map(|(destination, batch)| ParsedDestination {
                batch: Some(batch),
                ..destination
            })
            .boxed(),
        EmitterBatchLegality::Optional { .. } => definition
            .then(batch_clause().or_not())
            .map(|(destination, batch)| ParsedDestination {
                batch,
                ..destination
            })
            .boxed(),
        EmitterBatchLegality::Unavailable { .. } => definition.boxed(),
    }
}

struct DeclaredEmitter {
    name: EmitterName,
    from: ProcessorInputs,
    materialized_state: Vec<MaterializedStateDependency>,
    destination: ParsedDestination<EmitterEntity>,
}

fn creation<'src, S, P>(
    definition: P,
) -> impl Parser<'src, &'src [Token], DeclaredEmitter, extra::Err<ParseError<'src>>> + Clone
where
    S: EmitterEntityModel + Into<EmitterEntity> + Clone + 'src,
    P: Parser<'src, &'src [Token], ParsedDestination<S>, extra::Err<ParseError<'src>>>
        + Clone
        + 'src,
{
    head(S::KIND)
        .ignore_then(emitter_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(materialized_state_dependencies())
        .then_ignore(kw(Identifier::To))
        .then(with_batch::<S, _>(definition))
        .map(
            |(((name, from), materialized_state), parsed)| DeclaredEmitter {
                name,
                from,
                materialized_state,
                destination: ParsedDestination {
                    destination: parsed.destination.into(),
                    body: parsed.body,
                    envelope: parsed.envelope,
                    construction: parsed.construction,
                    batch: parsed.batch,
                },
            },
        )
        .boxed()
}

pub fn create_emitter_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateEmitter>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then(declared_creation())
        .then(flush_each())
        .then(message_error_policy())
        .then(general_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(((((if_not_exists, mode), declared), flush_policy), message), general)| {
                let parsed = declared.destination;
                CreateStatement::new(
                    CreateEmitter {
                        name: declared.name,
                        from: declared.from,
                        materialized_state: declared.materialized_state,
                        connector: Box::new(parsed.destination),
                        body: parsed.body,
                        envelope: Box::new(parsed.envelope),
                        batch: parsed.batch,
                        construction: parsed.construction.unwrap_or_default(),
                        mode: mode.unwrap_or(AckMode::Attached),
                        flush_policy,
                        error_policies: ErrorPolicies { message, general },
                    },
                    if_not_exists,
                )
            },
        )
        .boxed()
}

fn alteration<'src, S, P, M>(
    definition: P,
    mode: M,
) -> impl Parser<'src, &'src [Token], AlterEmitter, extra::Err<ParseError<'src>>> + Clone
where
    S: EmitterEntityModel + Into<EmitterEntity> + Clone + 'src,
    S::PublishingMode: Into<EmitterModeChange> + Clone + 'src,
    P: Parser<'src, &'src [Token], ParsedDestination<S>, extra::Err<ParseError<'src>>>
        + Clone
        + 'src,
    M: Parser<'src, &'src [Token], S::PublishingMode, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    head(S::KIND)
        .ignore_then(emitter_ref())
        .then(
            operation::<S, _, _>(definition, mode)
                .separated_by(alter_op_separator())
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .map(|(emitter, operations)| AlterEmitter {
            emitter,
            kind: S::KIND,
            operations,
        })
        .boxed()
}

pub fn alter_emitter_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterEmitter, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Alter)
        .ignore_then(declared_alteration())
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

fn operation<'src, S, P, M>(
    definition: P,
    mode: M,
) -> impl Parser<'src, &'src [Token], AlterEmitterOperation, extra::Err<ParseError<'src>>> + Clone
where
    S: EmitterEntityModel + Into<EmitterEntity> + Clone + 'src,
    S::PublishingMode: Into<EmitterModeChange> + Clone + 'src,
    P: Parser<'src, &'src [Token], ParsedDestination<S>, extra::Err<ParseError<'src>>>
        + Clone
        + 'src,
    M: Parser<'src, &'src [Token], S::PublishingMode, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let add_from = kw_phrase2(Identifier::Add, Identifier::From)
        .ignore_then(relay_ref())
        .then(where_expression().or_not())
        .map(|(relay, where_clause)| AlterEmitterOperation::AddFrom {
            relay,
            where_clause,
        });
    let drop_from = kw_phrase2(Identifier::Drop, Identifier::From)
        .ignore_then(relay_ref())
        .map(|relay| AlterEmitterOperation::DropFrom { relay });
    let alter_from = kw_phrase2(Identifier::Alter, Identifier::From)
        .ignore_then(relay_ref())
        .then(choice((
            kw(Identifier::Set)
                .ignore_then(where_expression())
                .map(Some),
            kw_phrase2(Identifier::Drop, Identifier::Where).to(None),
        )))
        .map(|(relay, clause)| match clause {
            Some(where_clause) => AlterEmitterOperation::AlterFromSetWhere {
                relay,
                where_clause,
            },
            None => AlterEmitterOperation::AlterFromDropWhere { relay },
        });
    let set_to = kw_phrase2(Identifier::Set, Identifier::To)
        .ignore_then(definition)
        .map(|parsed| AlterEmitterOperation::SetTo {
            destination: Box::new(parsed.destination.into()),
            body: parsed.body,
            envelope: Box::new(parsed.envelope),
            construction: parsed.construction,
        });
    let set_collect = kw(Identifier::Set)
        .ignore_then(collect_for())
        .map(|policy| AlterEmitterOperation::SetCollect { policy });
    let drop_collect =
        kw_phrase2(Identifier::Drop, Identifier::Collect).to(AlterEmitterOperation::DropCollect);
    let set_attachment = kw(Identifier::Set)
        .ignore_then(ack_mode())
        .map(|mode| AlterEmitterOperation::SetAttachment { mode });
    let set_mode = kw_phrase2(Identifier::Set, Identifier::Mode)
        .ignore_then(mode)
        .map(|mode| AlterEmitterOperation::SetPublishingMode { mode: mode.into() });
    let set_flush = kw(Identifier::Set)
        .ignore_then(flush_each())
        .map(|flush_policy| AlterEmitterOperation::SetFlush { flush_policy });
    let mut choices = vec![
        add_from.boxed(),
        drop_from.boxed(),
        alter_from.boxed(),
        set_to.boxed(),
        set_collect.boxed(),
        drop_collect.boxed(),
        set_attachment.boxed(),
        set_mode.boxed(),
        set_flush.boxed(),
    ];
    let capabilities = S::KIND.capabilities();
    if let EmitterReferenceKind::Client(_) | EmitterReferenceKind::StorageClient(_) =
        capabilities.reference
    {
        choices.push(
            kw_phrase2(Identifier::Set, Identifier::Client)
                .ignore_then(client_ref())
                .map(|client| AlterEmitterOperation::SetClient { client })
                .boxed(),
        );
    }
    if let EmitterBodyForm::Codec | EmitterBodyForm::CodecOrWithoutBody = capabilities.body {
        choices.push(
            kw(Identifier::Set)
                .ignore_then(encode_using_clause())
                .map(|codec| AlterEmitterOperation::SetEncodeUsing { codec })
                .boxed(),
        );
        choices.push(
            kw_phrase2(Identifier::Drop, Identifier::Encode)
                .to(AlterEmitterOperation::DropEncode)
                .boxed(),
        );
    }
    if !matches!(capabilities.batch, EmitterBatchLegality::Unavailable { .. }) {
        choices.push(
            kw(Identifier::Set)
                .ignore_then(batch_clause())
                .map(|policy| AlterEmitterOperation::SetBatch { policy })
                .boxed(),
        );
        choices.push(
            kw_phrase2(Identifier::Drop, Identifier::Batch)
                .to(AlterEmitterOperation::DropBatch)
                .boxed(),
        );
    }
    if capabilities.option("COMMIT EACH").is_some() {
        choices.push(
            kw(Identifier::Set)
                .ignore_then(iceberg::commit_each())
                .map(
                    |(commit_each, max_commit_size)| AlterEmitterOperation::SetCommit {
                        commit_each,
                        max_commit_size,
                    },
                )
                .boxed(),
        );
    }
    choice(choices).boxed()
}

fn declared_creation<'src>()
-> impl Parser<'src, &'src [Token], DeclaredEmitter, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        creation::<ClientConsumerEmitter, _>(client::consumer_body(EmitterContext::Create)),
        creation::<HttpRequestEmitter, _>(http::request_body(EmitterContext::Create)),
        creation::<KafkaTopicEmitter, _>(kafka::topic_body(EmitterContext::Create)),
        creation::<PulsarTopicEmitter, _>(pulsar::topic_body(EmitterContext::Create)),
        creation::<RabbitMqQueueEmitter, _>(rabbitmq::queue_body(EmitterContext::Create)),
        creation::<RedisPubSubEmitter, _>(redis::pubsub_body(EmitterContext::Create)),
        creation::<MqttTopicEmitter, _>(mqtt::topic_body(EmitterContext::Create)),
        creation::<NatsSubjectEmitter, _>(nats::subject_body(EmitterContext::Create)),
        creation::<ZeroMqPushEmitter, _>(zeromq::push_body(EmitterContext::Create)),
        creation::<SqsQueueEmitter, _>(sqs::queue_body(EmitterContext::Create)),
        creation::<SentryEventEmitter, _>(sentry::event_body(EmitterContext::Create)),
        creation::<SyslogCollectorEmitter, _>(syslog::collector_body(EmitterContext::Create)),
        creation::<OtelLogsEmitter, _>(otel::logs_body()),
        creation::<OtelTracesEmitter, _>(otel::traces_body()),
        creation::<OtelMetricsEmitter, _>(otel::metrics_body()),
        creation::<ClickHouseTableEmitter, _>(clickhouse::table_body()),
        creation::<PostgresTableEmitter, _>(postgres::table_body()),
        creation::<MySqlTableEmitter, _>(mysql::table_body()),
        creation::<MongoDbCollectionEmitter, _>(mongodb::collection_body()),
        creation::<IcebergTableEmitter, _>(iceberg::table_body())
    )
}
fn declared_alteration<'src>()
-> impl Parser<'src, &'src [Token], AlterEmitter, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        alteration::<ClientConsumerEmitter, _, _>(
            client::consumer_body(EmitterContext::Alter),
            client::mode()
        ),
        alteration::<HttpRequestEmitter, _, _>(
            http::request_body(EmitterContext::Alter),
            http::mode()
        ),
        alteration::<KafkaTopicEmitter, _, _>(
            kafka::topic_body(EmitterContext::Alter),
            kafka::mode()
        ),
        alteration::<PulsarTopicEmitter, _, _>(
            pulsar::topic_body(EmitterContext::Alter),
            pulsar::mode()
        ),
        alteration::<RabbitMqQueueEmitter, _, _>(
            rabbitmq::queue_body(EmitterContext::Alter),
            rabbitmq::mode()
        ),
        alteration::<RedisPubSubEmitter, _, _>(
            redis::pubsub_body(EmitterContext::Alter),
            redis::mode()
        ),
        alteration::<MqttTopicEmitter, _, _>(mqtt::topic_body(EmitterContext::Alter), mqtt::mode()),
        alteration::<NatsSubjectEmitter, _, _>(
            nats::subject_body(EmitterContext::Alter),
            nats::mode()
        ),
        alteration::<ZeroMqPushEmitter, _, _>(
            zeromq::push_body(EmitterContext::Alter),
            zeromq::mode()
        ),
        alteration::<SqsQueueEmitter, _, _>(sqs::queue_body(EmitterContext::Alter), sqs::mode()),
        alteration::<SentryEventEmitter, _, _>(
            sentry::event_body(EmitterContext::Alter),
            sentry::mode()
        ),
        alteration::<SyslogCollectorEmitter, _, _>(
            syslog::collector_body(EmitterContext::Alter),
            syslog::mode()
        ),
        alteration::<OtelLogsEmitter, _, _>(otel::logs_body(), otel::logs_mode()),
        alteration::<OtelTracesEmitter, _, _>(otel::traces_body(), otel::traces_mode()),
        alteration::<OtelMetricsEmitter, _, _>(otel::metrics_body(), otel::metrics_mode()),
        alteration::<ClickHouseTableEmitter, _, _>(clickhouse::table_body(), clickhouse::mode()),
        alteration::<PostgresTableEmitter, _, _>(postgres::table_body(), postgres::mode()),
        alteration::<MySqlTableEmitter, _, _>(mysql::table_body(), mysql::mode()),
        alteration::<MongoDbCollectionEmitter, _, _>(mongodb::collection_body(), mongodb::mode()),
        alteration::<IcebergTableEmitter, _, _>(iceberg::table_body(), iceberg::mode())
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
