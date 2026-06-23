use std::ops::Range;

use chumsky::{
    error::{RichPattern, RichReason},
    prelude::*,
};
use nervix_models::{
    AckMode, BranchParameterization, Domain, ErrorFieldMapping, ErrorPolicies, GeneralErrorPolicy,
    Identifier as ModelIdentifier, MessageErrorPolicy, ParameterValueMapping,
};
use sorted_vec::SortedSet;

use crate::lexer::{Identifier, SpannedToken, Token, Word, lex};

pub type ParseError<'src> = Rich<'src, Token>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseFromSourceError {
    Lex {
        source: String,
        diagnostics: Vec<Diagnostic>,
    },
    Parse {
        source: String,
        diagnostics: Vec<Diagnostic>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub message: String,
    pub span: Range<usize>,
}

pub fn kw<'src>(
    iden: Identifier,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    let label: &'static str = iden.into();
    select! {
        Token::Word(Word::KnownWord { iden: got, .. }) if got == iden => ()
    }
    .labelled(label)
}

pub fn kw_phrase2<'src>(
    first: Identifier,
    second: Identifier,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    let first_label: &'static str = first.into();
    let second_label: &'static str = second.into();
    let label = format!("{first_label} {second_label}");
    kw(first).ignore_then(kw(second)).labelled(label)
}

pub fn kw_phrase3<'src>(
    first: Identifier,
    second: Identifier,
    third: Identifier,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    let first_label: &'static str = first.into();
    let second_label: &'static str = second.into();
    let third_label: &'static str = third.into();
    let label = format!("{first_label} {second_label} {third_label}");
    kw(first)
        .ignore_then(kw(second))
        .ignore_then(kw(third))
        .labelled(label)
}

pub fn if_not_exists_clause<'src>()
-> impl Parser<'src, &'src [Token], bool, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Identifier::If, Identifier::Not, Identifier::Exists)
        .or_not()
        .map(|present| present.is_some())
}

pub fn tok<'src>(
    token: Token,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    let label = match token {
        Token::LBrace => "{",
        Token::RBrace => "}",
        Token::LBracket => "[",
        Token::RBracket => "]",
        Token::LParen => "(",
        Token::RParen => ")",
        Token::Comma => ",",
        Token::Semicolon => ";",
        Token::Colon => ":",
        Token::Dot => ".",
        Token::Hyphen => "-",
        Token::Eq => "=",
        Token::NotEq => "!=",
        Token::Gt => ">",
        Token::Lt => "<",
        Token::GtEq => ">=",
        Token::LtEq => "<=",
        Token::Plus => "+",
        Token::Star => "*",
        Token::Slash => "/",
        Token::Percent => "%",
        Token::Word(_) => "word",
        Token::StringLiteral(_) => "string",
        Token::NumberLiteral(_) => "number",
    };

    just(token).ignored().labelled(label)
}

pub fn ack_mode<'src>()
-> impl Parser<'src, &'src [Token], AckMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Attached).to(AckMode::Attached),
        kw(Identifier::Detached).to(AckMode::Detached),
    ))
}

pub fn word_raw<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::Word(Word::KnownWord { raw, .. }) => raw,
        Token::Word(Word::UnknownWord(raw)) => raw,
    }
}

pub fn u64_value<'src>()
-> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone {
    choice((select! { Token::NumberLiteral(v) => v }, word_raw())).try_map(|raw, span| {
        raw.parse::<u64>()
            .map_err(|_| Rich::custom(span, format!("invalid integer '{raw}'")))
    })
}

pub fn schema_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:schema")
}

fn parameterized_by_phrase<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw_phrase2(Identifier::Parameterized, Identifier::By),
        kw_phrase2(Identifier::Parametrized, Identifier::By),
    ))
}

fn explicit_branch_parameterized_with_values<'src>()
-> impl Parser<'src, &'src [Token], BranchParameterization, extra::Err<ParseError<'src>>> + Clone {
    let ident = parse_identifier("field_name");
    let source = parse_identifier("relay_ref")
        .then_ignore(tok(Token::Dot))
        .then(parse_identifier("field_name"))
        .map(|(relay, relay_field)| (relay, relay_field));
    let value =
        ident
            .then_ignore(tok(Token::Eq))
            .then(source)
            .map(|(field, (relay, relay_field))| ParameterValueMapping {
                field,
                relay,
                relay_field,
            });
    let values = kw(Identifier::Values).ignore_then(
        value
            .separated_by(tok(Token::Comma))
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(tok(Token::LBrace), tok(Token::RBrace)),
    );

    parameterized_by_phrase()
        .ignore_then(schema_ref())
        .then(values)
        .then_ignore(kw(Identifier::Ttl))
        .then(duration_lit())
        .map(|((schema, values), ttl)| {
            BranchParameterization::parameterized_with_ttl(schema, values, ttl)
        })
}

pub fn explicit_branch_parameterization<'src>()
-> impl Parser<'src, &'src [Token], BranchParameterization, extra::Err<ParseError<'src>>> + Clone {
    parameterized_by_phrase()
        .ignore_then(schema_ref())
        .map(|schema| BranchParameterization::parameterized(schema, Vec::new()))
}

pub fn branch_parameterization<'src>()
-> impl Parser<'src, &'src [Token], BranchParameterization, extra::Err<ParseError<'src>>> + Clone {
    let unparameterized =
        kw(Identifier::Unparameterized).to(BranchParameterization::unparameterized());

    choice((explicit_branch_parameterization(), unparameterized))
}

pub fn branch_parameterization_with_values<'src>()
-> impl Parser<'src, &'src [Token], BranchParameterization, extra::Err<ParseError<'src>>> + Clone {
    let parameterized = explicit_branch_parameterized_with_values();
    let unparameterized =
        kw(Identifier::Unparameterized).to(BranchParameterization::unparameterized());

    choice((parameterized, unparameterized))
}

pub fn domain_name<'src>()
-> impl Parser<'src, &'src [Token], Domain, extra::Err<ParseError<'src>>> + Clone {
    parse_domain("domain_name")
}

pub fn user_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("user_name")
}

pub fn schema_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("schema_name")
}

pub fn wire_schema_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:wire_schema")
}

pub fn wire_schema_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("wire_schema_name")
}

pub fn codec_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:codec")
}

pub fn codec_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("codec_name")
}

pub fn field_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("field_name")
}

pub fn relay_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier_excluding_reserved("ref:relay", &[Identifier::Message, Identifier::Branch])
}

pub fn dlq_relay_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier_excluding_reserved("ref:relay", &[Identifier::Message, Identifier::Branch])
}

pub fn resource_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:resource")
}

pub fn relay_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier_excluding_reserved("relay_name", &[Identifier::Message, Identifier::Branch])
}

pub fn unifier_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:unifier")
}

pub fn unifier_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("unifier_name")
}

pub fn deduplicator_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:deduplicator")
}

pub fn deduplicator_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("deduplicator_name")
}

pub fn correlator_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:correlator")
}

pub fn correlator_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("correlator_name")
}

pub fn window_processor_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("window_processor_name")
}

pub fn window_processor_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:window_processor")
}

pub fn client_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:client")
}

pub fn client_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("client_name")
}

pub fn vhost_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:vhost")
}

pub fn vhost_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("vhost_name")
}

pub fn endpoint_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:endpoint")
}

pub fn endpoint_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("endpoint_name")
}

pub fn signaling_protocol_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:signaling_protocol")
}

pub fn signaling_protocol_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("signaling_protocol_name")
}

pub fn signaling_protocol_clause<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(
        Identifier::With,
        Identifier::Signaling,
        Identifier::Protocol,
    )
    .ignore_then(signaling_protocol_ref())
}

pub fn generator_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("generator_name")
}

pub fn generator_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:generator")
}

pub fn inferencer_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("inferencer_name")
}

pub fn wasm_processor_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("wasm_processor_name")
}

pub fn wasm_processor_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:wasm_processor")
}

pub fn inferencer_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:inferencer")
}

pub fn ingestor_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:ingestor")
}

pub fn ingestor_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ingestor_name")
}

pub fn reingestor_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:reingestor")
}

pub fn router_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:router")
}

pub fn lookup_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:lookup")
}

pub fn lookup_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("lookup_name")
}

pub fn reingestor_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("reingestor_name")
}

pub fn router_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("router_name")
}

pub fn reorderer_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("reorderer_name")
}

pub fn reorderer_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:reorderer")
}

pub fn emitter_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:emitter")
}

pub fn emitter_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("emitter_name")
}

pub fn topic_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("topic_name")
}

pub fn mqtt_topic_filter<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((
        string_lit(),
        parse_identifier("mqtt_topic_filter").map(|topic| topic.as_str().to_string()),
    ))
}

pub fn queue_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("queue_name")
}

pub fn nats_queue_group_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("queue_group")
}

pub fn channel_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("channel_name")
}

pub fn table_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("table_name")
}

pub fn consumer_group_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("consumer_group")
}

pub fn subscription_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("subscription_name")
}

pub fn string_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::StringLiteral(value) => value,
    }
    .labelled("string_literal")
}

pub fn duration_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((
        select! { Token::NumberLiteral(value) => value }
            .then(word_raw())
            .map(|(number, unit)| format!("{number}{unit}")),
        word_raw(),
    ))
    .labelled("duration_literal")
}

pub fn byte_size_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((
        select! { Token::NumberLiteral(value) => value }
            .then(word_raw())
            .map(|(number, unit)| format!("{number}{unit}")),
        word_raw(),
    ))
    .try_map(|value, span| {
        value
            .parse::<ubyte::ByteUnit>()
            .map(|_| value)
            .map_err(|error| Rich::custom(span, format!("invalid byte_size_literal: {error}")))
    })
    .labelled("byte_size_literal")
}

pub fn max_batch_size_clause<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Identifier::Max, Identifier::Batch, Identifier::Size).ignore_then(byte_size_lit())
}

pub fn flush_each<'src>()
-> impl Parser<'src, &'src [Token], (String, Option<String>), extra::Err<ParseError<'src>>> + Clone
{
    choice((
        kw_phrase2(Identifier::Flush, Identifier::Each)
            .ignore_then(duration_lit())
            .then(max_batch_size_clause())
            .map(|(flush_each, max_batch_size)| (flush_each, Some(max_batch_size))),
        kw_phrase2(Identifier::Flush, Identifier::Immediate).to(("IMMEDIATE".to_string(), None)),
    ))
}

fn error_value_ref<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    word_raw()
        .separated_by(tok(Token::Dot))
        .at_least(1)
        .collect::<Vec<_>>()
        .map(|parts| parts.join("."))
        .labelled("error_value")
}

fn error_field_mapping<'src>()
-> impl Parser<'src, &'src [Token], ErrorFieldMapping, extra::Err<ParseError<'src>>> + Clone {
    field_ref()
        .then_ignore(tok(Token::Eq))
        .then(error_value_ref())
        .map(|(field, value)| ErrorFieldMapping { field, value })
}

pub fn message_error_policy<'src>()
-> impl Parser<'src, &'src [Token], MessageErrorPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::On)
        .ignore_then(kw(Identifier::Message))
        .then_ignore(kw(Identifier::Error))
        .ignore_then(choice((
            kw(Identifier::Ignore).to(MessageErrorPolicy::Ignore),
            kw(Identifier::Log).to(MessageErrorPolicy::Log),
            kw(Identifier::Dlq)
                .ignore_then(dlq_relay_ref())
                .then_ignore(kw(Identifier::Set))
                .then(
                    error_field_mapping()
                        .separated_by(tok(Token::Comma))
                        .at_least(1)
                        .collect::<Vec<_>>(),
                )
                .map(|(relay, mappings)| MessageErrorPolicy::Dlq { relay, mappings }),
        )))
}

pub fn general_error_policy<'src>()
-> impl Parser<'src, &'src [Token], GeneralErrorPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::On)
        .ignore_then(kw(Identifier::General))
        .then_ignore(kw(Identifier::Error))
        .ignore_then(choice((
            kw(Identifier::Ignore).to(GeneralErrorPolicy::Ignore),
            kw(Identifier::Log).to(GeneralErrorPolicy::Log),
        )))
}

pub fn error_policies<'src>()
-> impl Parser<'src, &'src [Token], ErrorPolicies, extra::Err<ParseError<'src>>> + Clone {
    message_error_policy()
        .then(general_error_policy())
        .map(|(message, general)| ErrorPolicies { message, general })
}

fn vm_program_head<'src>()
-> impl Parser<'src, &'src [Token], Token, extra::Err<ParseError<'src>>> + Clone {
    select! {
        token @ Token::Word(Word::KnownWord { iden, .. })
            if matches!(iden, Identifier::Where | Identifier::Set | Identifier::Unset) => token
    }
}

fn router_boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::To | Identifier::Default | Identifier::Match | Identifier::On,
                ..
            })
    )
}

fn inference_boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::Inputs | Identifier::On,
                ..
            })
    )
}

fn validated_vm_program_until<'src>(
    stop: fn(&Token) -> bool,
) -> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    vm_program_head()
        .then(
            any()
                .filter(move |token: &Token| !stop(token))
                .repeated()
                .collect::<Vec<_>>(),
        )
        .try_map(|(head, tail), span| {
            let mut tokens = Vec::with_capacity(tail.len() + 1);
            tokens.push(head);
            tokens.extend(tail);
            let source = render_vm_program_tokens(&tokens);
            crate::vm_program::parse_program(&source)
                .map(|_| source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
}

pub fn filter_map_program<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    validated_vm_program_until(|token| {
        matches!(
            token,
            Token::Semicolon
                | Token::Word(Word::KnownWord {
                    iden: Identifier::On,
                    ..
                })
        )
    })
}

pub fn set_only_program<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    filter_map_program().try_map(
        |source, span| match crate::vm_program::parse_program(&source) {
            Ok(parsed)
                if parsed.inner.filter.is_none()
                    && parsed.inner.branch_filters.is_empty()
                    && parsed.inner.unset.is_empty()
                    && !parsed.inner.set.is_empty() =>
            {
                Ok(source)
            }
            Ok(_) => Err(Rich::custom(
                span,
                "generator program must contain SET only".to_string(),
            )),
            Err(error) => Err(Rich::custom(span, vm_program_error_message(error))),
        },
    )
}

pub fn filter_map_program_until_router_clause<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    validated_vm_program_until(router_boundary_token)
}

pub fn filter_map_program_until_inference_clause<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    validated_vm_program_until(inference_boundary_token)
}

pub fn where_expr_until_router_clause<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Where)
        .ignore_then(
            any()
                .filter(|token: &Token| !router_boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .try_map(|tokens, span| {
            let source = render_vm_program_tokens(&tokens);
            crate::vm_program::parse_program(&format!("WHERE {source}"))
                .map(|_| source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
}

pub fn hostname_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    let atom = choice((select! { Token::NumberLiteral(value) => value }, word_raw()));
    let label = atom
        .clone()
        .then(
            tok(Token::Hyphen)
                .ignore_then(atom)
                .repeated()
                .collect::<Vec<_>>(),
        )
        .map(|(first, rest)| {
            let mut label = first;
            for part in rest {
                label.push('-');
                label.push_str(&part);
            }
            label
        });

    label
        .clone()
        .then(
            tok(Token::Dot)
                .ignore_then(label)
                .repeated()
                .collect::<Vec<_>>(),
        )
        .map(|(first, rest)| {
            let mut hostname = first;
            for part in rest {
                hostname.push('.');
                hostname.push_str(&part);
            }
            hostname
        })
        .labelled("hostname")
}

pub fn node_id<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    hostname_lit().labelled("node_id")
}

fn parse_identifier<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    word_raw()
        .try_map(move |raw, span| {
            ModelIdentifier::try_from(raw.as_str())
                .map_err(|err| Rich::custom(span, format!("invalid {label}: {err}")))
        })
        .labelled(label)
}

fn parse_identifier_excluding_reserved<'src>(
    label: &'static str,
    reserved: &'static [Identifier],
) -> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    word_raw()
        .try_map(move |raw, span| {
            if reserved
                .iter()
                .any(|reserved| raw.eq_ignore_ascii_case((*reserved).into()))
            {
                return Err(Rich::custom(
                    span,
                    format!("invalid {label}: '{raw}' is reserved"),
                ));
            }
            ModelIdentifier::try_from(raw.as_str())
                .map_err(|err| Rich::custom(span, format!("invalid {label}: {err}")))
        })
        .labelled(label)
}

fn parse_domain<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], Domain, extra::Err<ParseError<'src>>> + Clone {
    word_raw()
        .try_map(move |raw, span| {
            Domain::try_from(raw.as_str())
                .map_err(|err| Rich::custom(span, format!("invalid {label}: {err}")))
        })
        .labelled(label)
}

pub fn lex_input(
    input: &str,
) -> Result<(String, Vec<SpannedToken>, Vec<Token>), ParseFromSourceError> {
    let source = input.to_string();
    let spanned_tokens = lex(input).map_err(|errs| ParseFromSourceError::Lex {
        source: source.clone(),
        diagnostics: errs
            .into_iter()
            .map(|err| Diagnostic {
                message: err.to_string(),
                span: err.span().into_range(),
            })
            .collect(),
    })?;

    let tokens = spanned_tokens
        .iter()
        .map(|t| t.token.clone())
        .collect::<Vec<_>>();

    Ok((source, spanned_tokens, tokens))
}

pub fn into_parse_error(
    source: String,
    spanned_tokens: &[SpannedToken],
    source_len: usize,
    errs: Vec<ParseError<'_>>,
) -> ParseFromSourceError {
    ParseFromSourceError::Parse {
        source,
        diagnostics: errs
            .into_iter()
            .map(|err| Diagnostic {
                message: format_parse_error(&err),
                span: token_span_to_source_span(
                    err.span().into_range(),
                    spanned_tokens,
                    source_len,
                ),
            })
            .collect(),
    }
}

pub fn current_word_prefix(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars().rev() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.insert(0, ch);
        } else {
            break;
        }
    }
    out
}

pub fn suggestions_from_errors(mut errors: Vec<ParseError<'_>>, prefix: &str) -> Vec<String> {
    errors.sort_by_key(|e| {
        let s = e.span().into_range();
        (s.end, s.start)
    });

    let Some(best) = errors.last() else {
        return Vec::new();
    };
    let best_span = best.span().into_range();

    let candidates = SortedSet::from_unsorted(
        errors
            .iter()
            .rev()
            .take_while(|error| error.span().into_range() == best_span)
            .flat_map(|error| error.expected())
            .map(expected_to_suggestion)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>(),
    )
    .into_vec();

    if prefix.is_empty() {
        candidates
    } else {
        candidates
            .into_iter()
            .filter(|s| {
                if s.starts_with("ref:") {
                    return true;
                }
                s.to_ascii_lowercase()
                    .starts_with(&prefix.to_ascii_lowercase())
            })
            .collect()
    }
}

fn token_span_to_source_span(
    token_span: Range<usize>,
    spanned_tokens: &[SpannedToken],
    source_len: usize,
) -> Range<usize> {
    if spanned_tokens.is_empty() {
        return 0..0;
    }

    let start = if token_span.start < spanned_tokens.len() {
        spanned_tokens[token_span.start].span.into_range().start
    } else {
        source_len
    };

    let mut end = if token_span.end == 0 {
        start
    } else if token_span.end - 1 < spanned_tokens.len() {
        spanned_tokens[token_span.end - 1].span.into_range().end
    } else {
        source_len
    };

    if end < start {
        end = start;
    }

    start..end
}

fn format_parse_error(err: &ParseError<'_>) -> String {
    match err.reason() {
        RichReason::Custom(msg) => msg.clone(),
        RichReason::ExpectedFound { expected, found } => {
            let expected = expected
                .iter()
                .map(format_expected_pattern)
                .collect::<Vec<_>>();

            let expected_text = if expected.is_empty() {
                "something else".to_string()
            } else {
                expected.join(" | ")
            };

            let found_text = found
                .as_deref()
                .map(format_found_token)
                .unwrap_or_else(|| "end of input".to_string());

            format!("expected {expected_text}, found {found_text}")
        }
    }
}

fn expected_to_suggestion(pattern: &RichPattern<'_, Token>) -> String {
    match pattern {
        RichPattern::Label(label) => label.to_string(),
        RichPattern::Identifier(identifier) => identifier.to_string(),
        RichPattern::Token(token) => format_found_token(token),
        RichPattern::Any => String::new(),
        RichPattern::SomethingElse => String::new(),
        RichPattern::EndOfInput => String::new(),
        _ => String::new(),
    }
}

fn format_expected_pattern(pattern: &RichPattern<'_, Token>) -> String {
    match pattern {
        RichPattern::Label(label) => label.to_string(),
        RichPattern::Identifier(identifier) => identifier.to_string(),
        RichPattern::Token(token) => format_found_token(token),
        RichPattern::Any => "any token".to_string(),
        RichPattern::SomethingElse => "something else".to_string(),
        RichPattern::EndOfInput => "end of input".to_string(),
        _ => format!("{pattern:?}"),
    }
}

fn format_found_token(token: &Token) -> String {
    match token {
        Token::Word(Word::KnownWord { raw, .. }) => raw.clone(),
        Token::Word(Word::UnknownWord(raw)) => raw.clone(),
        Token::StringLiteral(value) => format!("\"{value}\""),
        Token::NumberLiteral(value) => value.clone(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::LBracket => "[".to_string(),
        Token::RBracket => "]".to_string(),
        Token::Comma => ",".to_string(),
        Token::Semicolon => ";".to_string(),
        Token::Colon => ":".to_string(),
        Token::Dot => ".".to_string(),
        Token::Hyphen => "-".to_string(),
        Token::LBrace => "{".to_string(),
        Token::RBrace => "}".to_string(),
        Token::Eq => "=".to_string(),
        Token::NotEq => "!=".to_string(),
        Token::Gt => ">".to_string(),
        Token::Lt => "<".to_string(),
        Token::GtEq => ">=".to_string(),
        Token::LtEq => "<=".to_string(),
        Token::Plus => "+".to_string(),
        Token::Star => "*".to_string(),
        Token::Slash => "/".to_string(),
        Token::Percent => "%".to_string(),
    }
}

pub fn render_vm_program_tokens(tokens: &[Token]) -> String {
    let mut rendered = String::new();
    for (index, token) in tokens.iter().enumerate() {
        if index > 0 && !matches!(tokens[index - 1], Token::Dot) && !matches!(token, Token::Dot) {
            rendered.push(' ');
        }
        rendered.push_str(&vm_program_token_to_source(token));
    }
    rendered
}

fn vm_program_token_to_source(token: &Token) -> String {
    match token {
        Token::Word(Word::KnownWord { raw, .. }) => raw.clone(),
        Token::Word(Word::UnknownWord(raw)) => raw.clone(),
        Token::StringLiteral(value) => {
            format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
        }
        Token::NumberLiteral(value) => value.clone(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::LBracket => "[".to_string(),
        Token::RBracket => "]".to_string(),
        Token::Comma => ",".to_string(),
        Token::Semicolon => ";".to_string(),
        Token::Colon => ":".to_string(),
        Token::Dot => ".".to_string(),
        Token::Hyphen => "-".to_string(),
        Token::LBrace => "{".to_string(),
        Token::RBrace => "}".to_string(),
        Token::Eq => "=".to_string(),
        Token::NotEq => "!=".to_string(),
        Token::Gt => ">".to_string(),
        Token::Lt => "<".to_string(),
        Token::GtEq => ">=".to_string(),
        Token::LtEq => "<=".to_string(),
        Token::Plus => "+".to_string(),
        Token::Star => "*".to_string(),
        Token::Slash => "/".to_string(),
        Token::Percent => "%".to_string(),
    }
}

pub fn vm_program_error_message(error: crate::vm_program::ParseFromSourceError) -> String {
    match error {
        crate::vm_program::ParseFromSourceError::Lex { diagnostics, .. }
        | crate::vm_program::ParseFromSourceError::Parse { diagnostics, .. } => diagnostics
            .first()
            .map(|diagnostic| diagnostic.message.clone())
            .unwrap_or_else(|| "invalid FILTER-MAP program".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use chumsky::prelude::*;

    use super::{
        ParseError, current_word_prefix, format_parse_error, into_parse_error, kw, lex_input,
        schema_ref, suggestions_from_errors, token_span_to_source_span,
    };
    use crate::lexer::Identifier;

    #[test]
    fn current_word_prefix_stops_at_non_identifier_boundary() {
        assert_eq!(current_word_prefix("CREATE JSON sch"), "sch");
        assert_eq!(current_word_prefix("CREATE JSON schema-"), "");
        assert_eq!(
            current_word_prefix("CREATE JSON schema_name"),
            "schema_name"
        );
        assert_eq!(current_word_prefix("CREATE JSON schema "), "");
    }

    #[test]
    fn suggestions_filter_by_prefix_but_keep_reference_placeholders() {
        let (_, _, tokens) = lex_input("").expect("lex should succeed");
        let output = choice((
            kw(Identifier::Create),
            kw(Identifier::Client),
            schema_ref().to(()),
        ))
        .then_ignore(end())
        .parse(tokens.as_slice());

        assert_eq!(
            suggestions_from_errors(output.into_errors(), "cr"),
            vec!["CREATE", "ref:schema"]
        );
    }

    #[test]
    fn into_parse_error_maps_parse_spans_back_to_source() {
        let (source, spanned_tokens, tokens) =
            lex_input("create kafka").expect("lex should succeed");
        let output = kw(Identifier::Create)
            .ignore_then(kw(Identifier::Json))
            .then_ignore(end())
            .parse(tokens.as_slice());
        assert!(output.has_errors(), "parser should produce an error");

        let err = into_parse_error(
            source,
            &spanned_tokens,
            "create kafka".len(),
            output.into_errors(),
        );
        let diagnostics = match err {
            super::ParseFromSourceError::Parse { diagnostics, .. } => diagnostics,
            other => panic!("expected parse error, got {other:?}"),
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].span, 7..12);
        assert_eq!(diagnostics[0].message, "expected JSON, found kafka");
    }

    #[test]
    fn token_span_to_source_span_handles_empty_and_out_of_bounds_ranges() {
        let (_, spanned_tokens, _) = lex_input("create json").expect("lex should succeed");

        assert_eq!(token_span_to_source_span(0..0, &[], 42), 0..0);
        assert_eq!(
            token_span_to_source_span(3..4, &spanned_tokens, "create json".len()),
            "create json".len().."create json".len()
        );
        assert_eq!(
            token_span_to_source_span(1..1, &spanned_tokens, "create json".len()),
            7..7
        );
    }

    #[test]
    fn format_parse_error_preserves_custom_messages() {
        let err: ParseError<'_> = chumsky::error::Rich::custom((2..4).into(), "custom failure");
        assert_eq!(format_parse_error(&err), "custom failure");
    }
}
