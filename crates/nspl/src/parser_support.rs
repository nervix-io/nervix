use std::ops::Range;

use chumsky::{
    error::{RichPattern, RichReason},
    prelude::*,
};
use nervix_models::{
    AckMode, AlterProcessorOperation, AssignmentTargetScope, BranchSelection, Domain, Expression,
    GeneralErrorPolicy, Identifier as ModelIdentifier, InputCollectPolicy,
    MaterializedStateDependency, MaterializedStatePolicy, MessageErrorPolicy, OutputBranch,
    OutputFlushPolicy, ProcessorInputWhere, ProcessorInputs, ProcessorOutput, ProcessorOutputs,
    RouteConstruction,
};
use sorted_vec::SortedSet;

use crate::lexer::{Identifier, SpannedToken, Token, Word, lex};

macro_rules! boxed_choice {
    ($($parser:expr),+ $(,)?) => {
        chumsky::Parser::boxed(chumsky::primitive::choice(vec![
            $(chumsky::Parser::boxed($parser)),+
        ]))
    };
}
pub(crate) use boxed_choice;

/// Derive completion suggestions for `input` at `cursor` from a statement grammar.
///
/// Two parses can be needed, and each covers a case the other gets wrong.
///
/// The partial word under the cursor is removed first. A half-typed word is not something the
/// grammar can make sense of, and inside a free-form expression region it is swallowed into the
/// region and handed to the expression grammar, which fails with a custom error carrying no
/// expectations at all — so completion falls silent the moment the user starts typing.
///
/// But with that word gone the statement may already be complete, and a complete statement has no
/// expectations either: end of input is not representable as a suggestion. So when the stripped
/// source offers nothing and a partial word exists, the grammar is asked again with the word in
/// place, because the alternatives that could still continue the statement are exactly the ones
/// that fail at that token.
macro_rules! suggest_from {
    ($input:expr, $cursor:expr, $parser:expr $(,)?) => {{
        let input: &str = $input;
        let cursor: usize = $cursor;
        let (source, prefix) = $crate::parser_support::completion_context(input, cursor);

        let offer = |source: &str| match $crate::parser_support::lex_input(source) {
            Ok((_, _, tokens)) => {
                let out = $parser
                    .then_ignore(chumsky::prelude::end())
                    .parse(tokens.as_slice());
                if out.has_errors() {
                    $crate::parser_support::suggestions_from_errors(out.into_errors(), &prefix)
                } else {
                    Vec::new()
                }
            }
            Err(_) => Vec::new(),
        };

        let suggestions = offer(&source);
        if suggestions.is_empty() && !prefix.is_empty() {
            offer(&input[..cursor.min(input.len())])
        } else {
            suggestions
        }
    }};
}
pub(crate) use suggest_from;

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
    .boxed()
}

pub fn kw_phrase2<'src>(
    first: Identifier,
    second: Identifier,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    let first_label: &'static str = first.into();
    let second_label: &'static str = second.into();
    let label = format!("{first_label} {second_label}");
    kw(first).ignore_then(kw(second)).labelled(label).boxed()
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
        .boxed()
}

pub fn alter_op_separator<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    let operation_head = choice((
        kw(Identifier::Add),
        kw(Identifier::Drop),
        kw(Identifier::Alter),
        kw(Identifier::Set),
        kw(Identifier::Replace),
    ));
    tok(Token::Comma)
        .and_is(tok(Token::Comma).ignore_then(operation_head))
        .labelled("alter_operation_separator")
        .boxed()
}

pub fn if_not_exists_clause<'src>()
-> impl Parser<'src, &'src [Token], bool, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Identifier::If, Identifier::Not, Identifier::Exists)
        .or_not()
        .map(|present| present.is_some())
        .boxed()
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
        Token::DoubleColon => "::",
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

    just(token).ignored().labelled(label).boxed()
}

pub fn ack_mode<'src>()
-> impl Parser<'src, &'src [Token], AckMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Attached).to(AckMode::Attached),
        kw(Identifier::Detached).to(AckMode::Detached),
    ))
    .boxed()
}

pub fn word_raw<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::Word(Word::KnownWord { raw, .. }) => raw,
        Token::Word(Word::UnknownWord(raw)) => raw,
    }
    .boxed()
}

/// A word that is not a language keyword.
///
/// Where a value is written as a bare word, accepting keywords too lets the value swallow whatever
/// follows it: `STEP 1s UNBRANCHED` reads `UNBRANCHED` as the start of another bound, and the
/// statement then fails somewhere far from the real mistake.
pub fn unknown_word<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::Word(Word::UnknownWord(raw)) => raw,
    }
    .boxed()
}

pub fn u64_value<'src>()
-> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone {
    // Each alternative carries the label: chumsky only rewrites an alternative error whose position
    // matches the start of the labelled parser, so labelling the choice as a whole leaves the
    // branches' own expectations in place and completion has nothing to offer here.
    choice((
        select! { Token::NumberLiteral(v) => v }.labelled("integer_literal"),
        word_raw().labelled("integer_literal"),
    ))
    .try_map(|raw, span| {
        raw.parse::<u64>()
            .map_err(|_| Rich::custom(span, format!("invalid integer '{raw}'")))
    })
    .boxed()
}

pub fn schema_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:schema")
}

pub fn branch_definition_header<'src>()
-> impl Parser<'src, &'src [Token], (ModelIdentifier, String), extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Schema)
        .ignore_then(schema_ref())
        .then_ignore(kw(Identifier::Ttl))
        .then(duration_lit())
        .boxed()
}

pub fn branch_selection<'src>()
-> impl Parser<'src, &'src [Token], BranchSelection, extra::Err<ParseError<'src>>> + Clone {
    let branched = kw_phrase2(Identifier::Branched, Identifier::By)
        .ignore_then(branch_ref())
        .map(BranchSelection::branched_by);
    let unbranched = kw(Identifier::Unbranched).to(BranchSelection::unbranched());

    choice((branched, unbranched)).boxed()
}

pub fn output_branch<'src>()
-> impl Parser<'src, &'src [Token], OutputBranch, extra::Err<ParseError<'src>>> + Clone {
    let branched = kw_phrase2(Identifier::Branched, Identifier::By)
        .ignore_then(branch_ref())
        .then(set_only_route_construction().or_not())
        .try_map(|(branch, construction), span| {
            let construction = construction.unwrap_or_default();
            if construction.inherit.is_some()
                || construction.where_clause.is_some()
                || !construction.invocations.is_empty()
                || construction.assignments.iter().any(|assignment| {
                    !matches!(
                        assignment.target.scope,
                        AssignmentTargetScope::Bare | AssignmentTargetScope::Branch
                    )
                })
            {
                return Err(Rich::custom(
                    span,
                    "branch construction accepts SET assignments with bare or branch targets only",
                ));
            }
            Ok(OutputBranch::BranchedBy {
                branch,
                assignments: construction.assignments,
            })
        });
    let unbranched = kw(Identifier::Unbranched).to(OutputBranch::Unbranched);

    choice((branched, unbranched)).boxed()
}

fn materialized_default_assignments<'src>()
-> impl Parser<'src, &'src [Token], Vec<nervix_models::Assignment>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Default)
        .ignore_then(
            any()
                .filter(|token: &Token| !matches!(token, Token::RBrace))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>()
                .labelled("default_assignments")
                .delimited_by(tok(Token::LBrace), tok(Token::RBrace)),
        )
        .try_map(|tokens, span| {
            let source = format!("SET {}", render_vm_program_tokens(&tokens));
            let construction = crate::semantic_program::parse_route_construction(&source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))?;
            if construction.inherit.is_some()
                || construction.where_clause.is_some()
                || !construction.invocations.is_empty()
                || construction
                    .assignments
                    .iter()
                    .any(|assignment| assignment.target.scope != AssignmentTargetScope::Bare)
            {
                return Err(Rich::custom(
                    span,
                    "materialized-state DEFAULT requires bare constant assignments",
                ));
            }
            Ok(construction.assignments)
        })
        .boxed()
}

pub fn materialized_state_dependency<'src>()
-> impl Parser<'src, &'src [Token], MaterializedStateDependency, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Using)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(relay_ref())
        .then(materialized_state_policy())
        .map(|(relay, policy)| MaterializedStateDependency { relay, policy })
        .boxed()
}

pub fn materialized_state_policy<'src>()
-> impl Parser<'src, &'src [Token], MaterializedStatePolicy, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw_phrase2(Identifier::Required, Identifier::Skip)
            .to(MaterializedStatePolicy::RequiredSkip),
        kw_phrase2(Identifier::Required, Identifier::Wait)
            .to(MaterializedStatePolicy::RequiredWait),
        materialized_default_assignments().map(MaterializedStatePolicy::Default),
    ))
    .boxed()
}

pub fn materialized_state_dependencies<'src>()
-> impl Parser<'src, &'src [Token], Vec<MaterializedStateDependency>, extra::Err<ParseError<'src>>>
+ Clone {
    materialized_state_dependency()
        .repeated()
        .collect::<Vec<_>>()
        .boxed()
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

pub fn wire_json_schema_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:wire_json_schema")
}

pub fn wire_cbor_schema_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:wire_cbor_schema")
}

pub fn wire_avro_schema_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:wire_avro_schema")
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

pub fn message_error_relay_ref<'src>()
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

pub fn junction_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:junction")
}

pub fn junction_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("junction_name")
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

pub fn branch_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:branch")
}

pub fn branch_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("branch_name")
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
    .boxed()
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

pub fn udf_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("udf_name")
}

pub fn udf_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:udf")
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

pub fn session_subscription_name<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("session_subscription_name")
}

pub fn session_subscription_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelIdentifier, extra::Err<ParseError<'src>>> + Clone {
    parse_identifier("ref:session_subscription")
}

pub fn string_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::StringLiteral(value) => value,
    }
    .labelled("string_literal")
    .boxed()
}

pub fn duration_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    // A number followed by a unit is unambiguously meant as a duration, so it is checked here: the
    // unit has to match any word — `min` is a keyword — and without the check `WIDTH 100 MESSAGES`
    // reads as the duration `100MESSAGES` and swallows the bound that follows it.
    //
    // A lone word is different. `oops` is an ordinary identifier a user could type, and whichever
    // setting consumes it validates it; checking it here would take over validation the runtime has
    // to do anyway, since models also arrive from persisted state. Only keywords are ruled out,
    // because a keyword is never a value.
    choice((
        select! { Token::NumberLiteral(value) => value }
            .then(word_raw())
            .map(|(number, unit)| format!("{number}{unit}"))
            .try_map(|raw, span| {
                humantime::parse_duration(&raw)
                    .map(|_| raw.clone())
                    .map_err(|error| {
                        Rich::custom(span, format!("invalid duration '{raw}': {error}"))
                    })
            }),
        unknown_word(),
    ))
    .labelled("duration_literal")
    .boxed()
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
    .boxed()
}

pub fn max_batch_size_clause<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Identifier::Max, Identifier::Batch, Identifier::Size)
        .ignore_then(byte_size_lit())
        .boxed()
}

pub fn collect_for<'src>()
-> impl Parser<'src, &'src [Token], InputCollectPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Collect, Identifier::For)
        .ignore_then(duration_lit())
        .then(max_batch_size_clause().or_not())
        .map(|(collect_for, max_batch_size)| InputCollectPolicy {
            collect_for,
            max_batch_size,
        })
        .boxed()
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
    .boxed()
}

pub fn message_error_policy<'src>()
-> impl Parser<'src, &'src [Token], MessageErrorPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::On)
        .ignore_then(kw(Identifier::Message))
        .then_ignore(kw(Identifier::Error))
        .ignore_then(choice((
            kw(Identifier::Ignore).to(MessageErrorPolicy::Ignore),
            kw(Identifier::Log).to(MessageErrorPolicy::Log),
            kw_phrase2(Identifier::Send, Identifier::To)
                .ignore_then(message_error_relay_ref())
                .then(set_only_route_construction())
                .try_map(|(relay, construction), span| {
                    if construction.inherit.is_some()
                        || construction.where_clause.is_some()
                        || !construction.invocations.is_empty()
                        || construction.assignments.is_empty()
                        || construction.assignments.iter().any(|assignment| {
                            assignment.target.scope != AssignmentTargetScope::Bare
                        })
                    {
                        return Err(Rich::custom(
                            span,
                            "ON MESSAGE ERROR SEND TO requires bare SET assignments only",
                        ));
                    }
                    Ok(MessageErrorPolicy::Dlq {
                        relay,
                        assignments: construction.assignments,
                    })
                }),
        )))
        .boxed()
}

fn alter_message_error_policy<'src>()
-> impl Parser<'src, &'src [Token], MessageErrorPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::On)
        .ignore_then(kw(Identifier::Message))
        .then_ignore(kw(Identifier::Error))
        .ignore_then(choice((
            kw(Identifier::Ignore).to(MessageErrorPolicy::Ignore),
            kw(Identifier::Log).to(MessageErrorPolicy::Log),
            kw_phrase2(Identifier::Send, Identifier::To)
                .ignore_then(message_error_relay_ref())
                .then(set_only_alter_route_construction())
                .try_map(|(relay, construction), span| {
                    if construction.inherit.is_some()
                        || construction.where_clause.is_some()
                        || !construction.invocations.is_empty()
                        || construction.assignments.is_empty()
                        || construction.assignments.iter().any(|assignment| {
                            assignment.target.scope != AssignmentTargetScope::Bare
                        })
                    {
                        return Err(Rich::custom(
                            span,
                            "ON MESSAGE ERROR SEND TO requires bare SET assignments only",
                        ));
                    }
                    Ok(MessageErrorPolicy::Dlq {
                        relay,
                        assignments: construction.assignments,
                    })
                }),
        )))
        .boxed()
}

pub fn alter_flushed_route_body<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(alter_route_construction().or_not())
        .then(flush_each())
        .then(alter_message_error_policy())
        .map(
            |(((relay, construction), (flush_each, max_batch_size)), message_error_policy)| {
                ProcessorOutput {
                    relay,
                    construction: construction.unwrap_or_default(),
                    flush_policy: Some(OutputFlushPolicy {
                        flush_each,
                        max_batch_size,
                    }),
                    message_error_policy,
                    branch: None,
                }
            },
        )
        .boxed()
}

pub fn alter_generator_route_body<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(
            set_only_alter_route_construction().try_map(|construction, span| {
                if construction.assignments.is_empty() {
                    Err(Rich::custom(
                        span,
                        "output route must contain SET assignments and may contain WHERE",
                    ))
                } else if construction.inherit.is_some() || !construction.invocations.is_empty() {
                    Err(Rich::custom(
                        span,
                        "set-only output route may contain SET assignments and WHERE only",
                    ))
                } else {
                    Ok(construction)
                }
            }),
        )
        .then(flush_each())
        .then(alter_message_error_policy())
        .map(
            |(((relay, construction), (flush_each, max_batch_size)), message_error_policy)| {
                ProcessorOutput {
                    relay,
                    construction,
                    flush_policy: Some(OutputFlushPolicy {
                        flush_each,
                        max_batch_size,
                    }),
                    message_error_policy,
                    branch: None,
                }
            },
        )
        .boxed()
}

pub fn alter_ingestor_route_body<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(alter_route_construction().or_not())
        .then(output_branch())
        .then(flush_each())
        .then(alter_message_error_policy())
        .map(
            |(
                (((relay, construction), branch), (flush_each, max_batch_size)),
                message_error_policy,
            )| {
                ProcessorOutput {
                    relay,
                    construction: construction.unwrap_or_default(),
                    flush_policy: Some(OutputFlushPolicy {
                        flush_each,
                        max_batch_size,
                    }),
                    message_error_policy,
                    branch: Some(branch),
                }
            },
        )
        .boxed()
}

pub fn alter_processor_operation<'src>()
-> impl Parser<'src, &'src [Token], AlterProcessorOperation, extra::Err<ParseError<'src>>> + Clone {
    alter_processor_operation_with_routes(alter_flushed_route_body(), true)
}

pub fn alter_reingestor_operation<'src>()
-> impl Parser<'src, &'src [Token], AlterProcessorOperation, extra::Err<ParseError<'src>>> + Clone {
    alter_processor_operation_with_routes(alter_ingestor_route_body(), false)
}

fn alter_processor_operation_with_routes<'src, P>(
    route_body: P,
    supports_node_branching: bool,
) -> impl Parser<'src, &'src [Token], AlterProcessorOperation, extra::Err<ParseError<'src>>> + Clone
where
    P: Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let add_from = kw(Identifier::Add)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .then(where_expression(alter_op_separator()).or_not())
        .map(|(relay, where_clause)| AlterProcessorOperation::AddFrom {
            relay,
            where_clause,
        });
    let drop_from = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::From))
        .ignore_then(relay_ref())
        .map(|relay| AlterProcessorOperation::DropFrom { relay });
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
            Some(where_clause) => AlterProcessorOperation::AlterFromSetWhere {
                relay,
                where_clause,
            },
            None => AlterProcessorOperation::AlterFromDropWhere { relay },
        });
    let set_collect = kw(Identifier::Set)
        .ignore_then(collect_for())
        .map(|policy| AlterProcessorOperation::SetCollect { policy });
    let drop_collect = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Collect))
        .to(AlterProcessorOperation::DropCollect);
    let set_filter = kw(Identifier::Set)
        .ignore_then(kw(Identifier::Filter))
        .ignore_then(where_expression(alter_op_separator()))
        .map(|where_clause| AlterProcessorOperation::SetFilterWhere { where_clause });
    let drop_filter = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Filter))
        .ignore_then(kw(Identifier::Where))
        .to(AlterProcessorOperation::DropFilterWhere);
    let set_mode = kw(Identifier::Set)
        .ignore_then(ack_mode())
        .map(|mode| AlterProcessorOperation::SetMode { mode });
    let set_branching = kw(Identifier::Set)
        .ignore_then(branch_selection())
        .map(|branching| AlterProcessorOperation::SetBranching { branching });
    let add_materialized = kw(Identifier::Add)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(relay_ref())
        .then(materialized_state_policy())
        .map(
            |(relay, policy)| AlterProcessorOperation::AddMaterializedState {
                dependency: MaterializedStateDependency { relay, policy },
            },
        );
    let drop_materialized = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(relay_ref())
        .map(|relay| AlterProcessorOperation::DropMaterializedState { relay });
    let alter_materialized = kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(relay_ref())
        .then_ignore(kw(Identifier::Set))
        .then(materialized_state_policy())
        .map(|(relay, policy)| AlterProcessorOperation::AlterMaterializedState { relay, policy });
    let add_route = kw(Identifier::Add)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(route_body.clone())
        .map(|route| AlterProcessorOperation::AddRoute { route });
    let drop_route = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(kw(Identifier::To))
        .ignore_then(relay_ref())
        .map(|relay| AlterProcessorOperation::DropRoute { relay });
    let replace_route = kw(Identifier::Replace)
        .ignore_then(kw(Identifier::Route))
        .ignore_then(route_body)
        .map(|route| AlterProcessorOperation::ReplaceRoute { route });

    let common = choice((
        add_from,
        drop_from,
        alter_from,
        set_collect,
        drop_collect,
        set_filter,
        drop_filter,
        set_mode,
        add_materialized,
        drop_materialized,
        alter_materialized,
        add_route,
        drop_route,
        replace_route,
    ))
    .boxed();

    if supports_node_branching {
        choice((common, set_branching)).boxed()
    } else {
        common
    }
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
        .boxed()
}

fn processor_output_boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::To
                    | Identifier::Branched
                    | Identifier::Unbranched
                    | Identifier::Collect
                    | Identifier::Flush
                    | Identifier::On
                    | Identifier::Using
                    | Identifier::Inputs
                    | Identifier::File
                    | Identifier::Deduplicate
                    | Identifier::Max
                    | Identifier::By
                    | Identifier::Width
                    | Identifier::Step
                    | Identifier::Output,
                ..
            })
    )
}

fn route_boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::To
                    | Identifier::Branched
                    | Identifier::Unbranched
                    | Identifier::Flush
                    | Identifier::On,
                ..
            })
    )
}

pub(crate) fn from_where_boundary_token(token: &Token) -> bool {
    processor_output_boundary_token(token)
        || matches!(
            token,
            Token::Comma
                | Token::Word(Word::KnownWord {
                    iden: Identifier::Filter,
                    ..
                })
        )
}

/// The body of a route-construction clause: every token up to a boundary, handed to the expression
/// grammar as one unit.
///
/// The run is labelled, and required to be non-empty, because completion is derived from what the
/// grammar still expects. Left unlabelled and optional, a bare `SET` parses far enough to fail in
/// the expression grammar with a custom error that carries no expectations at all, so completion
/// falls silent exactly where the user needs it. Each clause head gets its own label so the offer
/// names what belongs there.
fn route_construction_body<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], Vec<Token>, extra::Err<ParseError<'src>>> + Clone {
    any()
        .filter(|token: &Token| !route_boundary_token(token))
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .labelled(label)
        .boxed()
}

/// One `<head> <body>` clause. Only the body carries a label: labelling the clause as a whole would
/// replace the head keyword's own expectation, and completion would offer the body placeholder
/// before the user has typed the keyword that introduces it.
fn route_construction_clause<'src>(
    head: Identifier,
    body: impl Parser<'src, &'src [Token], Vec<Token>, extra::Err<ParseError<'src>>> + Clone + 'src,
) -> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    kw(head)
        .ignore_then(body)
        .try_map(move |tail, span| {
            let head: &'static str = head.into();
            let source = format!("{head} {}", render_vm_program_tokens(&tail));
            crate::semantic_program::parse_route_construction(&source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
        .boxed()
}

/// A construction limited to `SET`, for contexts that accept no other clause.
///
/// Offering `INHERIT`, `WHERE` or `INVOKE` here and rejecting them afterwards makes completion
/// propose clauses the position cannot hold. `WHERE` is still reachable inside the `SET` body,
/// where the expression grammar handles it.
fn set_only_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    route_construction_clause(Identifier::Set, route_construction_body("set_assignments"))
}

/// A construction limited to `SET` or `WHERE`, for contexts that reject `INHERIT` and `INVOKE`.
pub fn set_or_where_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        route_construction_clause(Identifier::Set, route_construction_body("set_assignments")),
        route_construction_clause(
            Identifier::Where,
            route_construction_body("where_expression")
        ),
    )
}

fn set_only_alter_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    route_construction_clause(
        Identifier::Set,
        alter_route_construction_body("set_assignments"),
    )
}

pub fn route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        route_construction_clause(
            Identifier::Inherit,
            route_construction_body("inherit_targets"),
        ),
        route_construction_clause(Identifier::Set, route_construction_body("set_assignments"),),
        route_construction_clause(
            Identifier::Where,
            route_construction_body("where_expression"),
        ),
        route_construction_clause(Identifier::Invoke, route_construction_body("invocations"),),
    )
}

fn alter_route_construction_body<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], Vec<Token>, extra::Err<ParseError<'src>>> + Clone {
    any()
        .and_is(alter_op_separator().not())
        .filter(|token: &Token| !route_boundary_token(token))
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .labelled(label)
        .boxed()
}

fn alter_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        route_construction_clause(
            Identifier::Inherit,
            alter_route_construction_body("inherit_targets"),
        ),
        route_construction_clause(
            Identifier::Set,
            alter_route_construction_body("set_assignments"),
        ),
        route_construction_clause(
            Identifier::Where,
            alter_route_construction_body("where_expression"),
        ),
        route_construction_clause(
            Identifier::Invoke,
            alter_route_construction_body("invocations"),
        ),
    )
}

fn explicit_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Set)
        .ignore_then(route_construction_body("set_assignments"))
        .try_map(|tokens, span| {
            let source = format!("SET {}", render_vm_program_tokens(&tokens));
            crate::semantic_program::parse_route_construction(&source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
        .try_map(|construction, span| {
            if construction.assignments.is_empty() {
                Err(Rich::custom(
                    span,
                    "output route must contain SET assignments and may contain WHERE",
                ))
            } else if construction.inherit.is_some() || !construction.invocations.is_empty() {
                Err(Rich::custom(
                    span,
                    "set-only output route may contain SET assignments and WHERE only",
                ))
            } else {
                Ok(construction)
            }
        })
        .boxed()
}

fn source_where_clause_with_boundary<'src>(
    boundary: fn(&Token) -> bool,
) -> impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Where)
        .ignore_then(
            any()
                .filter(move |token: &Token| !boundary(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>()
                .labelled("where_expression"),
        )
        .try_map(|tokens, span| {
            let source = render_vm_program_tokens(&tokens);
            crate::parse_expression(&source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
        .boxed()
}

pub fn where_expression<'src, P>(
    boundary: P,
) -> impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone
where
    P: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    kw(Identifier::Where)
        .ignore_then(
            any()
                .and_is(boundary.not())
                .filter(|token: &Token| !matches!(token, Token::Semicolon))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>()
                .labelled("where_expression"),
        )
        .try_map(|tokens, span| {
            let source = render_vm_program_tokens(&tokens);
            crate::parse_expression(&source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
        .boxed()
}

pub fn alter_expression_list<'src, P>(
    boundary: P,
    label: &'static str,
) -> impl Parser<'src, &'src [Token], Vec<Expression>, extra::Err<ParseError<'src>>> + Clone
where
    P: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    any()
        .and_is(boundary.not())
        .filter(|token: &Token| !matches!(token, Token::Semicolon))
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .try_map(|tokens, span| {
            crate::parse_expression_list(&render_vm_program_tokens(&tokens))
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
        .labelled(label)
        .boxed()
}

pub fn from_relay_clause_with_boundary<'src>(
    boundary: fn(&Token) -> bool,
) -> impl Parser<
    'src,
    &'src [Token],
    (ModelIdentifier, Vec<ProcessorInputWhere>),
    extra::Err<ParseError<'src>>,
> + Clone {
    relay_ref()
        .then(source_where_clause_with_boundary(boundary).or_not())
        .map(|(relay, where_clause)| {
            let from_where = where_clause
                .map(|where_clause| ProcessorInputWhere {
                    relay: relay.clone(),
                    where_clause,
                })
                .into_iter()
                .collect();
            (relay, from_where)
        })
        .boxed()
}

pub fn from_relay_clause<'src>() -> impl Parser<
    'src,
    &'src [Token],
    (ModelIdentifier, Vec<ProcessorInputWhere>),
    extra::Err<ParseError<'src>>,
> + Clone {
    from_relay_clause_with_boundary(from_where_boundary_token)
}

pub fn from_relay_clauses<'src>()
-> impl Parser<'src, &'src [Token], ProcessorInputs, extra::Err<ParseError<'src>>> + Clone {
    from_relay_clause()
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
        .then(collect_for().or_not())
        .map(|(inputs, collect_policy)| {
            let mut from_relays = Vec::with_capacity(inputs.len());
            let mut from_where = Vec::new();
            for (relay, mut relay_where) in inputs {
                from_relays.push(relay);
                from_where.append(&mut relay_where);
            }
            let mut inputs = ProcessorInputs::new(from_relays, from_where);
            inputs.collect_policy = collect_policy;
            inputs
        })
        .boxed()
}

pub fn filter_where_clause<'src>()
-> impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Filter)
        .ignore_then(kw(Identifier::Where))
        .ignore_then(
            any()
                .filter(|token: &Token| !processor_output_boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>()
                .labelled("where_expression"),
        )
        .try_map(|tokens, span| {
            let source = render_vm_program_tokens(&tokens);
            crate::parse_expression(&source)
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
        .boxed()
}

fn explicit_processor_output_route<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(explicit_route_construction())
        .then(message_error_policy())
        .map(
            |((relay, construction), message_error_policy)| ProcessorOutput {
                relay,
                construction,
                flush_policy: None,
                message_error_policy,
                branch: None,
            },
        )
        .boxed()
}

fn flushed_processor_output_route<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(route_construction().or_not())
        .then(flush_each())
        .then(message_error_policy())
        .map(
            |(((relay, construction), (flush_each, max_batch_size)), message_error_policy)| {
                ProcessorOutput {
                    relay,
                    construction: construction.unwrap_or_default(),
                    flush_policy: Some(OutputFlushPolicy {
                        flush_each,
                        max_batch_size,
                    }),
                    message_error_policy,
                    branch: None,
                }
            },
        )
        .boxed()
}

fn flushed_explicit_processor_output_route<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(explicit_route_construction())
        .then(flush_each())
        .then(message_error_policy())
        .map(
            |(((relay, construction), (flush_each, max_batch_size)), message_error_policy)| {
                ProcessorOutput {
                    relay,
                    construction,
                    flush_policy: Some(OutputFlushPolicy {
                        flush_each,
                        max_batch_size,
                    }),
                    message_error_policy,
                    branch: None,
                }
            },
        )
        .boxed()
}

fn flushed_ingestor_output_route<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutput, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::To)
        .ignore_then(relay_ref())
        .then(route_construction().or_not())
        .then(output_branch())
        .then(flush_each())
        .then(message_error_policy())
        .map(
            |(
                (((relay, construction), branch), (flush_each, max_batch_size)),
                message_error_policy,
            )| {
                ProcessorOutput {
                    relay,
                    construction: construction.unwrap_or_default(),
                    flush_policy: Some(OutputFlushPolicy {
                        flush_each,
                        max_batch_size,
                    }),
                    message_error_policy,
                    branch: Some(branch),
                }
            },
        )
        .boxed()
}

pub fn explicit_processor_outputs<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutputs, extra::Err<ParseError<'src>>> + Clone {
    explicit_processor_output_route()
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(ProcessorOutputs::new)
        .boxed()
}

pub fn flushed_processor_outputs<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutputs, extra::Err<ParseError<'src>>> + Clone {
    flushed_processor_output_route()
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(ProcessorOutputs::new)
        .boxed()
}

pub fn flushed_explicit_processor_outputs<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutputs, extra::Err<ParseError<'src>>> + Clone {
    flushed_explicit_processor_output_route()
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(ProcessorOutputs::new)
        .boxed()
}

pub fn flushed_ingestor_outputs<'src>()
-> impl Parser<'src, &'src [Token], ProcessorOutputs, extra::Err<ParseError<'src>>> + Clone {
    flushed_ingestor_output_route()
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(ProcessorOutputs::new)
        .boxed()
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
        .boxed()
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
        .boxed()
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
        .boxed()
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

/// Split completion input at the cursor into the source the grammar should parse and the partial
/// word the user is part-way through typing.
///
/// The partial word is removed before parsing rather than left in place. A half-typed word is not a
/// token the grammar can make sense of, and inside a free-form expression region it is swallowed
/// into the region and re-parsed by the expression grammar, which fails with a custom error that
/// carries no expectations — so completion falls silent the moment the user starts typing. The
/// prefix comes back separately and only filters the result.
pub fn completion_context(input: &str, cursor: usize) -> (String, String) {
    let safe_cursor = cursor.min(input.len());
    let head = &input[..safe_cursor];
    let prefix = current_word_prefix(head);
    (head[..head.len() - prefix.len()].to_string(), prefix)
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

    filter_by_prefix(candidates, prefix)
}

/// Keep only the suggestions the partial word could still grow into.
///
/// Semantic references survive regardless: the server expands them into concrete object names and
/// filters those by the same prefix itself.
pub fn filter_by_prefix(suggestions: Vec<String>, prefix: &str) -> Vec<String> {
    if prefix.is_empty() {
        return suggestions;
    }
    let prefix = prefix.to_ascii_lowercase();
    suggestions
        .into_iter()
        .filter(|suggestion| {
            suggestion.starts_with("ref:") || suggestion.to_ascii_lowercase().starts_with(&prefix)
        })
        .collect()
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
        Token::DoubleColon => "::".to_string(),
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
        Token::DoubleColon => "::".to_string(),
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
