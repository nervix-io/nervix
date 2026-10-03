//! Shared NSPL grammar primitives.
//!
//! Layer: language.
//! - **Owns.** Reusable lexical parsers, semantic reference parsers and parse diagnostics.
//! - **Depends on.** Shared language tokens and vocabulary models.
//! - **Must not know.** Registry state, runtime execution or connector lifecycle.

use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    ops::Range,
};

use chumsky::{
    error::{LabelError, RichPattern, RichReason},
    input::InputRef,
    prelude::*,
    util::MaybeRef,
};
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::{
    AckMode, AckWindow, AlterProcessorOperation, AssignmentTargetScope, BranchName,
    BranchSelection, ChannelName, ClientConfigEntry, ClientName, ClusterNodeName, CodecName,
    CollectionName, ConsumerGroupName, CorrelatorName, DeduplicatorName, DomainClockPeriod,
    DomainName, EmitterName, EndpointName, Expression, FieldName, FlushPolicy, GeneralErrorPolicy,
    GeneratorName, InferencerName, IngestorName, InputCollectPolicy, InspectionFormat,
    JunctionName, LookupName, MaterializedStateDependency, MaterializedStatePolicy,
    MessageErrorPolicy, ModelName, NameError, OutputBranch, PlacementName, ProcessorInputWhere,
    ProcessorInputs, ProcessorOutput, ProcessorOutputs, PulsarSubscriptionName, QueueGroupName,
    QueueName, ReingestorName, RelayName, ReordererName, RequestedResourceVersion, ResourceName,
    RetryPolicy, RouteConstruction, SchemaName, SignalingProtocolName, SubjectName,
    SubscriptionName, TableName, TopicName, TransactionOperationNumber, UdfName, UserName,
    VhostName, WasmProcessorName, WindowProcessorName, WireSchemaName,
};
use sorted_vec::SortedSet;

use crate::{
    lexer::{Identifier, SpannedToken, Token, Word, lex},
    semantic_program::{
        Prefix, RouteClause, read_assignments_prefix, read_expression_list_prefix,
        read_expression_prefix, read_route_construction_prefix,
    },
};

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
/// region and read by the expression grammar, which fails with a custom error carrying no
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

        let offer = |source: &str| match $crate::parser_support::completion_tokens(source) {
            Some(tokens) => {
                let out = $parser
                    .then_ignore(chumsky::prelude::end())
                    .parse(tokens.as_slice());
                if out.has_errors() {
                    $crate::parser_support::suggestions_from_errors(out.into_errors(), &prefix)
                } else {
                    Vec::new()
                }
            }
            None => Vec::new(),
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

/// Why NSPL source text was rejected, and where.
///
/// The variant is the stage that rejected the text: lexing stops before any token exists, parsing
/// after every token was read. Each diagnostic's span is a byte range into `text`, the text that
/// stage was given. A parse function creates the report of this error at the failing stage; a
/// caller that owns a larger operation adds its own context above it and reads the stage, text and
/// diagnostics back from the report.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseFromSourceError {
    /// The text could not be split into tokens.
    #[error("lex error: {}", DiagnosticMessages(.diagnostics))]
    Lex {
        text: String,
        diagnostics: Vec<Diagnostic>,
    },
    /// The tokens do not form what the grammar expects.
    #[error("parse error: {}", DiagnosticMessages(.diagnostics))]
    Parse {
        text: String,
        diagnostics: Vec<Diagnostic>,
    },
}

impl ParseFromSourceError {
    /// The diagnostics describing why the source was rejected.
    pub fn diagnostics(&self) -> &[Diagnostic] {
        match self {
            Self::Lex { diagnostics, .. } | Self::Parse { diagnostics, .. } => diagnostics,
        }
    }

    /// The source text that was rejected.
    pub fn source_text(&self) -> &str {
        match self {
            Self::Lex { text, .. } | Self::Parse { text, .. } => text,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub message: String,
    pub span: Range<usize>,
}

/// The messages of a rejection's diagnostics on one line, as its report displays them.
struct DiagnosticMessages<'diagnostics>(&'diagnostics [Diagnostic]);

impl std::fmt::Display for DiagnosticMessages<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, diagnostic) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            f.write_str(&diagnostic.message)?;
        }
        Ok(())
    }
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

/// The shared rendering choice for read-only inspection statements.
pub fn inspection_format<'src>()
-> impl Parser<'src, &'src [Token], InspectionFormat, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Text).to(InspectionFormat::Text),
        kw(Identifier::Json).to(InspectionFormat::Json),
    ))
    .boxed()
}

/// A keyword written as several words, parsed as one grammar unit and labelled with the whole
/// phrase, so completion offers it as one item.
pub fn kw_phrase<'src, const WORDS: usize>(
    words: [Identifier; WORDS],
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    let spellings: [&'static str; WORDS] = words.map(<&'static str>::from);
    let label = spellings.join(" ");
    let mut phrase = empty().boxed();
    for word in words {
        phrase = phrase.ignore_then(kw(word)).boxed();
    }
    phrase.labelled(label).boxed()
}

pub fn kw_phrase2<'src>(
    first: Identifier,
    second: Identifier,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw_phrase([first, second])
}

pub fn kw_phrase3<'src>(
    first: Identifier,
    second: Identifier,
    third: Identifier,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw_phrase([first, second, third])
}

/// The keywords that head an operation of an `ALTER` statement.
pub(crate) const ALTER_OPERATION_HEADS: [Identifier; 6] = [
    Identifier::Add,
    Identifier::Drop,
    Identifier::Alter,
    Identifier::Set,
    Identifier::Replace,
    Identifier::Rename,
];

/// The comma between two operations of an `ALTER` statement: one followed by the keyword that
/// heads the next operation.
///
/// A comma inside an operation's expression is followed by a term instead, which may be a call to
/// a builtin that spells an operation keyword, as `replace(...)` does. Such a call heads no
/// operation.
pub fn alter_op_separator<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    let operation_head = choice(ALTER_OPERATION_HEADS.map(kw)).and_is(expression_term_word().not());
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

/// The `SEQUENTIAL` confirmation window shared by ingestor and emitter modes.
pub fn sequential_ack_window<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Sequential).boxed()
}

/// The positive bound in `PARALLEL MAX <n>`, shared by ingestor and emitter modes.
pub fn parallel_ack_window<'src>()
-> impl Parser<'src, &'src [Token], NonZeroU64, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Parallel)
        .ignore_then(kw(Identifier::Max))
        .ignore_then(nonzero_u64_value(
            "max_in_flight",
            "parallel max in-flight must be greater than zero",
        ))
        .boxed()
}

pub fn ack_window<'src>()
-> impl Parser<'src, &'src [Token], AckWindow, extra::Err<ParseError<'src>>> + Clone {
    choice((
        sequential_ack_window().to(AckWindow::Sequential),
        parallel_ack_window().map(|max| AckWindow::Parallel { max }),
    ))
    .boxed()
}

pub fn ack_timeout<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Ack, Identifier::Timeout)
        .ignore_then(duration_lit())
        .boxed()
}

pub fn retry_policy<'src>()
-> impl Parser<'src, &'src [Token], RetryPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Retry, Identifier::Policy)
        .ignore_then(kw(Identifier::Backoff))
        .ignore_then(duration_lit())
        .then_ignore(kw(Identifier::Max))
        .then(duration_lit())
        .map(|(backoff, max_backoff)| RetryPolicy {
            backoff,
            max_backoff,
        })
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

/// An integer written where the count is bounded by the 64-bit range its Model uses.
///
/// The slot names itself with `label`. Each alternative carries the label rather than the parser
/// as a whole: chumsky only rewrites an alternative error whose position matches the start of the
/// labelled parser, so labelling the choice leaves the branches' own expectations in place and
/// completion has nothing to offer, and labelling the checked parser would rewrite the invalid
/// integer this reports into a bare expectation.
pub fn u64_value<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone {
    choice((
        select! { Token::NumberLiteral(v) => v }.labelled(label),
        word_raw().labelled(label),
    ))
    .try_map(|raw, span| {
        raw.parse::<u64>()
            .map_err(|_| Rich::custom(span, format!("invalid integer '{raw}'")))
    })
    .boxed()
}

/// The version clause every resource binding requires: `VERSION <n>` or `VERSION LATEST`.
///
/// Binding kinds share this one clause, so `LATEST` means the same thing wherever a resource is
/// bound. The number carries its own label, which lets completion offer the resource's completed
/// versions beside `LATEST`.
pub fn resource_version_clause<'src>()
-> impl Parser<'src, &'src [Token], RequestedResourceVersion, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Version)
        .ignore_then(choice((
            kw(Identifier::Latest).to(RequestedResourceVersion::Latest),
            u64_value("completed_resource_version").map(RequestedResourceVersion::Number),
        )))
        .boxed()
}

/// An integer written where zero has no meaning, parsed straight into `NonZeroU64`.
///
/// The bound belongs to the value, so this is the one place NSPL text turns into such a count and
/// the only place that has to say what zero would mean. Downstream Models, the registry, and the
/// runtime take the non-zero type and never re-check it.
///
/// The label goes on the integer itself, not on the checked parser: labelling the check would
/// rewrite `zero_reason` into a bare expectation and lose the explanation.
pub fn nonzero_u64_value<'src>(
    label: &'static str,
    zero_reason: &'static str,
) -> impl Parser<'src, &'src [Token], NonZeroU64, extra::Err<ParseError<'src>>> + Clone {
    u64_value(label)
        .try_map(move |value, span| {
            NonZeroU64::new(value).ok_or_else(|| Rich::custom(span, zero_reason))
        })
        .boxed()
}

/// An integer written where the count is bounded by the 32-bit range its Model uses.
///
/// The bound belongs to the value, so a count outside it is rejected where it is written rather
/// than narrowed later: the message names the range instead of leaving a truncated number behind.
pub fn u32_value<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], u32, extra::Err<ParseError<'src>>> + Clone {
    // Each alternative carries the label for the same reason `u64_value` does: chumsky only
    // rewrites an alternative error whose position matches the start of the labelled parser.
    choice((
        select! { Token::NumberLiteral(v) => v }.labelled(label),
        word_raw().labelled(label),
    ))
    .try_map(|raw, span| {
        raw.parse::<u32>().map_err(|_| {
            Rich::custom(
                span,
                format!("invalid integer '{raw}'; expected 0 through {}", u32::MAX),
            )
        })
    })
    .boxed()
}

/// A 32-bit count written where zero has no meaning, parsed straight into `NonZeroU32`.
///
/// The label goes on the integer itself, not on the checked parser: labelling the check would
/// rewrite `zero_reason` into a bare expectation and lose the explanation.
pub fn nonzero_u32_value<'src>(
    label: &'static str,
    zero_reason: &'static str,
) -> impl Parser<'src, &'src [Token], NonZeroU32, extra::Err<ParseError<'src>>> + Clone {
    u32_value(label)
        .try_map(move |value, span| {
            NonZeroU32::new(value).ok_or_else(|| Rich::custom(span, zero_reason))
        })
        .boxed()
}

pub fn schema_ref<'src>()
-> impl Parser<'src, &'src [Token], SchemaName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:schema", SchemaName::parse)
}

pub fn placement_name<'src>()
-> impl Parser<'src, &'src [Token], PlacementName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("placement_name", PlacementName::parse)
}

pub fn placement_ref<'src>()
-> impl Parser<'src, &'src [Token], PlacementName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:placement", PlacementName::parse)
}

pub fn runtime_node_ref<'src>()
-> impl Parser<'src, &'src [Token], ModelName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:runtime_node", ModelName::parse)
}

pub fn branch_definition_header<'src>()
-> impl Parser<'src, &'src [Token], (SchemaName, String), extra::Err<ParseError<'src>>> + Clone {
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
            embedded("default_assignments", read_assignments_prefix)
                .delimited_by(tok(Token::LBrace), tok(Token::RBrace)),
        )
        .try_map(|assignments, span| {
            if assignments
                .iter()
                .any(|assignment| assignment.target.scope != AssignmentTargetScope::Bare)
            {
                return Err(Rich::custom(
                    span,
                    "materialized-state DEFAULT requires bare constant assignments",
                ));
            }
            Ok(assignments)
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
-> impl Parser<'src, &'src [Token], DomainName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("domain_name", DomainName::parse)
}

pub fn domain_ref<'src>()
-> impl Parser<'src, &'src [Token], DomainName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:domain", DomainName::parse)
}

pub fn user_name<'src>()
-> impl Parser<'src, &'src [Token], UserName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("user_name", UserName::parse)
}

pub fn schema_name<'src>()
-> impl Parser<'src, &'src [Token], SchemaName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("schema_name", SchemaName::parse)
}

pub fn wire_json_schema_ref<'src>()
-> impl Parser<'src, &'src [Token], WireSchemaName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:wire_json_schema", WireSchemaName::parse)
}

pub fn wire_cbor_schema_ref<'src>()
-> impl Parser<'src, &'src [Token], WireSchemaName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:wire_cbor_schema", WireSchemaName::parse)
}

pub fn wire_avro_schema_ref<'src>()
-> impl Parser<'src, &'src [Token], WireSchemaName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:wire_avro_schema", WireSchemaName::parse)
}

pub fn wire_schema_name<'src>()
-> impl Parser<'src, &'src [Token], WireSchemaName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("wire_schema_name", WireSchemaName::parse)
}

pub fn codec_ref<'src>()
-> impl Parser<'src, &'src [Token], CodecName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:codec", CodecName::parse)
}

pub fn codec_name<'src>()
-> impl Parser<'src, &'src [Token], CodecName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("codec_name", CodecName::parse)
}

pub fn field_ref<'src>()
-> impl Parser<'src, &'src [Token], FieldName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("field_name", FieldName::parse)
}

pub fn schema_field_ref<'src>()
-> impl Parser<'src, &'src [Token], FieldName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:schema_field", FieldName::parse)
}

pub fn relay_ref<'src>()
-> impl Parser<'src, &'src [Token], RelayName, extra::Err<ParseError<'src>>> + Clone {
    parse_name_excluding_reserved(
        "ref:relay",
        &[Identifier::Message, Identifier::Branch],
        RelayName::parse,
    )
}

pub fn message_error_relay_ref<'src>()
-> impl Parser<'src, &'src [Token], RelayName, extra::Err<ParseError<'src>>> + Clone {
    parse_name_excluding_reserved(
        "ref:relay",
        &[Identifier::Message, Identifier::Branch],
        RelayName::parse,
    )
}

pub fn resource_ref<'src>()
-> impl Parser<'src, &'src [Token], ResourceName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:resource", ResourceName::parse)
}

pub fn relay_name<'src>()
-> impl Parser<'src, &'src [Token], RelayName, extra::Err<ParseError<'src>>> + Clone {
    parse_name_excluding_reserved(
        "relay_name",
        &[Identifier::Message, Identifier::Branch],
        RelayName::parse,
    )
}

pub fn junction_ref<'src>()
-> impl Parser<'src, &'src [Token], JunctionName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:junction", JunctionName::parse)
}

pub fn junction_name<'src>()
-> impl Parser<'src, &'src [Token], JunctionName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("junction_name", JunctionName::parse)
}

pub fn deduplicator_ref<'src>()
-> impl Parser<'src, &'src [Token], DeduplicatorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:deduplicator", DeduplicatorName::parse)
}

pub fn deduplicator_name<'src>()
-> impl Parser<'src, &'src [Token], DeduplicatorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("deduplicator_name", DeduplicatorName::parse)
}

pub fn correlator_ref<'src>()
-> impl Parser<'src, &'src [Token], CorrelatorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:correlator", CorrelatorName::parse)
}

pub fn correlator_name<'src>()
-> impl Parser<'src, &'src [Token], CorrelatorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("correlator_name", CorrelatorName::parse)
}

pub fn window_processor_name<'src>()
-> impl Parser<'src, &'src [Token], WindowProcessorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("window_processor_name", WindowProcessorName::parse)
}

pub fn window_processor_ref<'src>()
-> impl Parser<'src, &'src [Token], WindowProcessorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:window_processor", WindowProcessorName::parse)
}

pub fn client_ref<'src>()
-> impl Parser<'src, &'src [Token], ClientName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:client", ClientName::parse)
}

pub fn client_name<'src>()
-> impl Parser<'src, &'src [Token], ClientName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("client_name", ClientName::parse)
}

pub fn vhost_ref<'src>()
-> impl Parser<'src, &'src [Token], VhostName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:vhost", VhostName::parse)
}

pub fn vhost_name<'src>()
-> impl Parser<'src, &'src [Token], VhostName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("vhost_name", VhostName::parse)
}

pub fn branch_ref<'src>()
-> impl Parser<'src, &'src [Token], BranchName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:branch", BranchName::parse)
}

pub fn branch_name<'src>()
-> impl Parser<'src, &'src [Token], BranchName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("branch_name", BranchName::parse)
}

pub fn endpoint_ref<'src>()
-> impl Parser<'src, &'src [Token], EndpointName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:endpoint", EndpointName::parse)
}

pub fn endpoint_name<'src>()
-> impl Parser<'src, &'src [Token], EndpointName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("endpoint_name", EndpointName::parse)
}

pub fn signaling_protocol_ref<'src>()
-> impl Parser<'src, &'src [Token], SignalingProtocolName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:signaling_protocol", SignalingProtocolName::parse)
}

pub fn signaling_protocol_name<'src>()
-> impl Parser<'src, &'src [Token], SignalingProtocolName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("signaling_protocol_name", SignalingProtocolName::parse)
}

pub fn signaling_protocol_clause<'src>()
-> impl Parser<'src, &'src [Token], SignalingProtocolName, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(
        Identifier::With,
        Identifier::Signaling,
        Identifier::Protocol,
    )
    .ignore_then(signaling_protocol_ref())
    .boxed()
}

pub fn config_entries_block<'src>()
-> impl Parser<'src, &'src [Token], Vec<ClientConfigEntry>, extra::Err<ParseError<'src>>> + Clone {
    let value = choice((
        string_lit(),
        select! { Token::NumberLiteral(v) => v },
        word_raw(),
    ));
    let entry = string_lit()
        .labelled("config_key")
        .then_ignore(tok(Token::Eq))
        .then(value.labelled("config_value"))
        .map(|(key, value)| ClientConfigEntry { key, value });
    kw(Identifier::Config)
        .ignore_then(
            entry
                .separated_by(tok(Token::Comma))
                .allow_trailing()
                .collect::<Vec<_>>()
                .delimited_by(tok(Token::LBrace), tok(Token::RBrace)),
        )
        .boxed()
}

pub fn generator_name<'src>()
-> impl Parser<'src, &'src [Token], GeneratorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("generator_name", GeneratorName::parse)
}

pub fn generator_ref<'src>()
-> impl Parser<'src, &'src [Token], GeneratorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:generator", GeneratorName::parse)
}

pub fn inferencer_name<'src>()
-> impl Parser<'src, &'src [Token], InferencerName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("inferencer_name", InferencerName::parse)
}

pub fn wasm_processor_name<'src>()
-> impl Parser<'src, &'src [Token], WasmProcessorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("wasm_processor_name", WasmProcessorName::parse)
}

pub fn wasm_processor_ref<'src>()
-> impl Parser<'src, &'src [Token], WasmProcessorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:wasm_processor", WasmProcessorName::parse)
}

pub fn udf_name<'src>()
-> impl Parser<'src, &'src [Token], UdfName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("udf_name", UdfName::parse)
}

pub fn udf_ref<'src>()
-> impl Parser<'src, &'src [Token], UdfName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:udf", UdfName::parse)
}

pub fn inferencer_ref<'src>()
-> impl Parser<'src, &'src [Token], InferencerName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:inferencer", InferencerName::parse)
}

pub fn ingestor_ref<'src>()
-> impl Parser<'src, &'src [Token], IngestorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:ingestor", IngestorName::parse)
}

pub fn ingestor_name<'src>()
-> impl Parser<'src, &'src [Token], IngestorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ingestor_name", IngestorName::parse)
}

pub fn reingestor_ref<'src>()
-> impl Parser<'src, &'src [Token], ReingestorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:reingestor", ReingestorName::parse)
}

pub fn lookup_ref<'src>()
-> impl Parser<'src, &'src [Token], LookupName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:lookup", LookupName::parse)
}

pub fn lookup_name<'src>()
-> impl Parser<'src, &'src [Token], LookupName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("lookup_name", LookupName::parse)
}

pub fn reingestor_name<'src>()
-> impl Parser<'src, &'src [Token], ReingestorName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("reingestor_name", ReingestorName::parse)
}

pub fn reorderer_name<'src>()
-> impl Parser<'src, &'src [Token], ReordererName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("reorderer_name", ReordererName::parse)
}

pub fn reorderer_ref<'src>()
-> impl Parser<'src, &'src [Token], ReordererName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:reorderer", ReordererName::parse)
}

pub fn emitter_ref<'src>()
-> impl Parser<'src, &'src [Token], EmitterName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:emitter", EmitterName::parse)
}

pub fn emitter_name<'src>()
-> impl Parser<'src, &'src [Token], EmitterName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("emitter_name", EmitterName::parse)
}

pub fn topic_ref<'src>()
-> impl Parser<'src, &'src [Token], TopicName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("topic_name", TopicName::parse)
}

pub fn mqtt_topic_filter<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((
        string_lit(),
        parse_name("mqtt_topic_filter", TopicName::parse).map(|topic| topic.as_str().to_string()),
    ))
}

pub fn subject_ref<'src>()
-> impl Parser<'src, &'src [Token], SubjectName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("subject_name", SubjectName::parse)
}

pub fn collection_ref<'src>()
-> impl Parser<'src, &'src [Token], CollectionName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("collection_name", CollectionName::parse)
}

pub fn queue_ref<'src>()
-> impl Parser<'src, &'src [Token], QueueName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("queue_name", QueueName::parse)
}

pub fn nats_queue_group_ref<'src>()
-> impl Parser<'src, &'src [Token], QueueGroupName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("queue_group", QueueGroupName::parse)
}

pub fn channel_ref<'src>()
-> impl Parser<'src, &'src [Token], ChannelName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("channel_name", ChannelName::parse)
}

pub fn table_ref<'src>()
-> impl Parser<'src, &'src [Token], TableName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("table_name", TableName::parse)
}

pub fn consumer_group_ref<'src>()
-> impl Parser<'src, &'src [Token], ConsumerGroupName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("consumer_group", ConsumerGroupName::parse)
}

pub fn subscription_ref<'src>()
-> impl Parser<'src, &'src [Token], PulsarSubscriptionName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("subscription_name", PulsarSubscriptionName::parse)
}

pub fn session_subscription_name<'src>()
-> impl Parser<'src, &'src [Token], SubscriptionName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("session_subscription_name", SubscriptionName::parse)
}

pub fn session_subscription_ref<'src>()
-> impl Parser<'src, &'src [Token], SubscriptionName, extra::Err<ParseError<'src>>> + Clone {
    parse_name("ref:session_subscription", SubscriptionName::parse)
}

pub fn string_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::StringLiteral(value) => value,
    }
    .labelled("string_literal")
    .boxed()
}

/// A path on the client's own filesystem, written as a quoted literal.
///
/// Client-local statements name the file or directory they read or write this way, and the server
/// never interprets the path. Completion recognizes a position that expects one by this label, so
/// every such path is parsed here rather than as a plain string literal. An empty literal names no
/// file, so it is rejected where it is written.
///
/// The label goes on the literal itself, not on the checked parser, so the emptiness check keeps
/// its explanation.
pub fn local_path<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::StringLiteral(value) => value,
    }
    .labelled("local_path")
    .try_map(|path, span| {
        if path.is_empty() {
            return Err(Rich::custom(span, "local_path must not be empty"));
        }
        Ok(path)
    })
    .boxed()
}

/// A transaction named by its identity, written as a quoted literal.
///
/// The server issues transaction identities, and they are not NSPL names: they are quoted exactly
/// as `SHOW TRANSACTIONS` and `BEGIN` print them. An empty literal names no transaction, so it is
/// rejected where it is written rather than reported later as an unknown identity.
///
/// The label goes on the literal itself, not on the checked parser, so the emptiness check keeps
/// its explanation.
pub fn transaction_id<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::StringLiteral(value) => value,
    }
    .labelled("transaction_id")
    .try_map(|transaction_id, span| {
        if transaction_id.is_empty() {
            return Err(Rich::custom(span, "transaction_id must not be empty"));
        }
        Ok(transaction_id)
    })
    .boxed()
}

/// The one-based number of an accepted transaction operation.
///
/// Operation numbers start at one, so zero is rejected with the reason rather than read as an
/// operation that cannot exist. A number past this platform's address space names no accepted
/// operation either, and says so here instead of wrapping into one that might.
pub fn transaction_operation_number<'src>()
-> impl Parser<'src, &'src [Token], TransactionOperationNumber, extra::Err<ParseError<'src>>> + Clone
{
    nonzero_u64_value(
        "operation_number",
        "operation_number must be greater than zero; operations are numbered from 1",
    )
    .try_map(|number, span| {
        let Ok(number) = NonZeroUsize::try_from(number) else {
            return Err(Rich::custom(
                span,
                format!("operation_number {number} exceeds this platform's address space"),
            ));
        };
        Ok(TransactionOperationNumber::new(number))
    })
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
                nervix_models::parse_duration_text(&raw)
                    .map(|_| raw.clone())
                    .map_err(|report| {
                        Rich::custom(
                            span,
                            format!("invalid duration '{raw}': {}", report.current_context()),
                        )
                    })
            }),
        unknown_word(),
    ))
    .labelled("duration_literal")
    .boxed()
}

pub fn domain_clock_period_lit<'src>()
-> impl Parser<'src, &'src [Token], DomainClockPeriod, extra::Err<ParseError<'src>>> + Clone {
    duration_lit().try_map(|raw, span| {
        raw.parse::<DomainClockPeriod>()
            .map_err(|error| Rich::custom(span, error.to_string()))
    })
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
-> impl Parser<'src, &'src [Token], FlushPolicy, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw_phrase2(Identifier::Flush, Identifier::Each)
            .ignore_then(duration_lit())
            .then(max_batch_size_clause())
            .map(|(interval, max_batch_size)| FlushPolicy::Each {
                interval,
                max_batch_size,
            }),
        kw_phrase2(Identifier::Flush, Identifier::Immediate).to(FlushPolicy::Immediate),
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
            |(((relay, construction), flush_policy), message_error_policy)| ProcessorOutput {
                relay,
                construction: construction.unwrap_or_default(),
                flush_policy: Some(flush_policy),
                message_error_policy,
                branch: None,
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
            |(((relay, construction), flush_policy), message_error_policy)| ProcessorOutput {
                relay,
                construction,
                flush_policy: Some(flush_policy),
                message_error_policy,
                branch: None,
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
            |((((relay, construction), branch), flush_policy), message_error_policy)| {
                ProcessorOutput {
                    relay,
                    construction: construction.unwrap_or_default(),
                    flush_policy: Some(flush_policy),
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
        .then(where_expression().or_not())
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
                .ignore_then(where_expression())
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
        .ignore_then(where_expression())
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

/// A comma inside an `ALTER` operation that does not begin the next operation, so a list the
/// operation holds goes on past it.
fn alter_list_separator<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    tok(Token::Comma).and_is(alter_op_separator().not()).boxed()
}

/// The route construction a route writes from `first`'s keyword on: that clause's body, then each
/// clause the route may write after it. A list the construction holds goes on past a comma only
/// where `separator` accepts it.
///
/// Only the body carries a label: labelling the clause as a whole would replace the keyword's own
/// expectation, and completion would offer the body placeholder before the user has typed the
/// keyword that introduces it.
fn route_construction_clause<'src, S>(
    first: RouteClause,
    separator: S,
) -> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone
where
    S: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    kw(first.keyword())
        .ignore_then(embedded(first.body_label(), move |tokens| {
            read_route_construction_prefix(tokens, first, separator.clone())
        }))
        .boxed()
}

/// A construction limited to `SET`, for contexts that accept no other clause.
///
/// Offering `INHERIT`, `WHERE` or `INVOKE` here and rejecting them afterwards makes completion
/// propose clauses the position cannot hold. `WHERE` and `INVOKE` are still read after the `SET`
/// body, so the caller can reject them with its own reason.
fn set_only_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    route_construction_clause(RouteClause::Set, tok(Token::Comma))
}

/// A construction limited to `SET` or `WHERE`, for contexts that reject `INHERIT` and `INVOKE`.
pub fn set_or_where_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        route_construction_clause(RouteClause::Set, tok(Token::Comma)),
        where_only_route_construction(),
    )
}

/// A construction limited to `WHERE`, for direct emitter sinks whose output is already fully
/// described by their `VALUES` mapping.
pub fn where_only_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    route_construction_clause(RouteClause::Where, tok(Token::Comma))
}

/// HTTP requests without a body may filter records and set outgoing headers, but construct no
/// output fields.
pub fn bodyless_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        route_construction_clause(RouteClause::Where, tok(Token::Comma)),
        route_construction_clause(RouteClause::Invoke, tok(Token::Comma)),
    )
}

fn set_only_alter_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    route_construction_clause(RouteClause::Set, alter_list_separator())
}

pub fn route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        route_construction_clause(RouteClause::Inherit, tok(Token::Comma)),
        route_construction_clause(RouteClause::Set, tok(Token::Comma)),
        route_construction_clause(RouteClause::Where, tok(Token::Comma)),
        route_construction_clause(RouteClause::Invoke, tok(Token::Comma)),
    )
}

/// A route construction inside an `ALTER` operation, whose lists end where the next operation
/// begins.
fn alter_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        route_construction_clause(RouteClause::Inherit, alter_list_separator()),
        route_construction_clause(RouteClause::Set, alter_list_separator()),
        route_construction_clause(RouteClause::Where, alter_list_separator()),
        route_construction_clause(RouteClause::Invoke, alter_list_separator()),
    )
}

fn explicit_route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    route_construction_clause(RouteClause::Set, tok(Token::Comma))
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

/// A word written as part of an expression term: the name of a call, followed by the `(` that opens
/// its arguments, or the scope of a field, followed by the `.` before the field.
///
/// Such a word begins no `ALTER` operation, whatever keyword it spells. Callers read this only
/// under `not`, which consumes nothing and discards what it expected, so completion never offers
/// the bracket or the dot it looks for.
fn expression_term_word<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    any()
        .filter(|token: &Token| matches!(token, Token::Word(_)))
        .then(any().filter(|token: &Token| matches!(token, Token::LParen | Token::Dot)))
        .ignored()
        .boxed()
}

/// Reads an embedded part, such as an expression, an expression list, a route construction or the
/// assignments of a `DEFAULT`, from the tokens that remain in the statement, and goes on right
/// after the tokens `read` took.
///
/// The part's own grammar decides where it ends: `read` takes the longest run of tokens it
/// accepts, and the statement's next clause begins right after them. A statement keyword is an
/// ordinary name inside an expression, so `to`, `on` or `by` written where an operand belongs is a
/// field, and the clause it would begin starts only after a complete expression. Nothing is
/// rendered back to text or lexed again, and a standalone entry point reads the same text with the
/// same grammar.
///
/// Completion sees none of the part's grammar. Where the part cannot begin, the rejection expects
/// `label`, which completion offers. Any other rejection keeps the reader's message, located at
/// the statement's tokens where the reader failed, and expects nothing, so an unfinished
/// expression offers nothing and a finished one offers the clauses that may follow it.
pub fn embedded<'src, O, R>(
    label: &'static str,
    read: R,
) -> impl Parser<'src, &'src [Token], O, extra::Err<ParseError<'src>>> + Clone
where
    O: 'src,
    R: Fn(&'src [Token]) -> Result<Prefix<O>, Vec<ParseError<'src>>> + Clone + 'src,
{
    custom(
        move |input: &mut InputRef<'src, '_, &'src [Token], extra::Err<ParseError<'src>>>| {
            let start = input.cursor();
            let offset = input.span_since(&start).start;
            let remaining: &'src [Token] = input.slice_from(&start..);
            match read(remaining) {
                Ok(Prefix { output, length }) => {
                    for _ in 0..length {
                        input.skip();
                    }
                    Ok(output)
                }
                Err(errors) => Err(embedded_rejection(errors, label, offset, remaining)),
            }
        },
    )
    .boxed()
}

/// The rejection a statement reports for what an embedded reader rejected, moved from the
/// positions of the tokens the reader was handed onto those of the statement, which begin at
/// `offset`.
///
/// A reader that expected something else at its very first token found nothing of the part, so
/// the rejection expects the part's `label` there. Any other rejection is the reader's first
/// diagnostic, kept as its message.
fn embedded_rejection<'src>(
    errors: Vec<ParseError<'src>>,
    label: &'static str,
    offset: usize,
    remaining: &'src [Token],
) -> ParseError<'src> {
    let error = errors
        .into_iter()
        .next()
        .assured("chumsky reports at least one error for every failed parse");
    let within = error.span().into_range();
    let start = offset
        .checked_add(within.start)
        .assured("a position inside the remaining tokens is a token index of the statement");
    let end = offset
        .checked_add(within.end)
        .assured("a position inside the remaining tokens is a token index of the statement");
    let span = SimpleSpan::from(start..end);
    if within.start == 0
        && let RichReason::ExpectedFound { .. } = error.reason()
    {
        let found = remaining.first().map(MaybeRef::Ref);
        return <ParseError<'src> as LabelError<'src, &'src [Token], &'static str>>::expected_found(
            [label],
            found,
            span,
        );
    }
    Rich::custom(span, format_parse_error(&error))
}

/// An expression embedded in a statement, whose body completion names `label`.
pub fn embedded_expression<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone {
    embedded(label, read_expression_prefix)
}

/// A `WHERE` clause: the keyword, then the expression that filters.
pub fn where_expression<'src>()
-> impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Where)
        .ignore_then(embedded_expression("where_expression"))
        .boxed()
}

/// The comma-separated expressions an `ALTER` operation sets, ending where the next operation
/// begins.
pub fn alter_expression_list<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], Vec<Expression>, extra::Err<ParseError<'src>>> + Clone {
    embedded(label, |tokens| {
        read_expression_list_prefix(tokens, alter_list_separator())
    })
}

/// The comma-separated expressions a statement clause holds, such as a deduplicator's keys or a
/// reorderer's ordering.
pub fn expression_list<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], Vec<Expression>, extra::Err<ParseError<'src>>> + Clone {
    embedded(label, |tokens| {
        read_expression_list_prefix(tokens, tok(Token::Comma))
    })
}

pub fn from_relay_clause<'src>()
-> impl Parser<'src, &'src [Token], (RelayName, Vec<ProcessorInputWhere>), extra::Err<ParseError<'src>>>
+ Clone {
    relay_ref()
        .then(where_expression().or_not())
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
        .ignore_then(where_expression())
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
            |(((relay, construction), flush_policy), message_error_policy)| ProcessorOutput {
                relay,
                construction: construction.unwrap_or_default(),
                flush_policy: Some(flush_policy),
                message_error_policy,
                branch: None,
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
            |(((relay, construction), flush_policy), message_error_policy)| ProcessorOutput {
                relay,
                construction,
                flush_policy: Some(flush_policy),
                message_error_policy,
                branch: None,
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
            |((((relay, construction), branch), flush_policy), message_error_policy)| {
                ProcessorOutput {
                    relay,
                    construction: construction.unwrap_or_default(),
                    flush_policy: Some(flush_policy),
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
    // Each alternative carries the label, because chumsky only rewrites an alternative error whose
    // position matches the start of the labelled parser. Without it, a hostname continued after
    // `-` or `.` reports raw token expectations and completion has nothing to offer there.
    let atom = choice((
        select! { Token::NumberLiteral(value) => value }.labelled("hostname_label"),
        word_raw().labelled("hostname_label"),
    ));
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

pub fn cluster_node_name<'src>()
-> impl Parser<'src, &'src [Token], ClusterNodeName, extra::Err<ParseError<'src>>> + Clone {
    hostname_lit()
        .try_map(|raw: String, span| {
            ClusterNodeName::parse(raw.as_str())
                .map_err(|err| Rich::custom(span, format!("invalid node_id: {err}")))
        })
        .labelled("node_id")
}

/// Parse one word as the named concept `N`, validated by the name type itself.
///
/// `parse` is the name type's own constructor, so the grammar never restates what a name of that
/// kind may contain; it only says which kind of name this position holds.
fn parse_name<'src, N: Clone + 'static>(
    label: &'static str,
    parse: fn(&str) -> Result<N, Report<NameError>>,
) -> impl Parser<'src, &'src [Token], N, extra::Err<ParseError<'src>>> + Clone {
    word_raw()
        .try_map(move |raw: String, span| {
            parse(raw.as_str()).map_err(|err| Rich::custom(span, format!("invalid {label}: {err}")))
        })
        .labelled(label)
        .boxed()
}

/// Parse one word as the named concept `N`, rejecting words the surrounding grammar reserves.
fn parse_name_excluding_reserved<'src, N: Clone + 'static>(
    label: &'static str,
    reserved: &'static [Identifier],
    parse: fn(&str) -> Result<N, Report<NameError>>,
) -> impl Parser<'src, &'src [Token], N, extra::Err<ParseError<'src>>> + Clone {
    word_raw()
        .try_map(move |raw: String, span| {
            if reserved
                .iter()
                .any(|reserved| raw.eq_ignore_ascii_case((*reserved).into()))
            {
                return Err(Rich::custom(
                    span,
                    format!("invalid {label}: '{raw}' is reserved"),
                ));
            }
            parse(raw.as_str()).map_err(|err| Rich::custom(span, format!("invalid {label}: {err}")))
        })
        .labelled(label)
        .boxed()
}

/// One lexed statement, kept together because every grammar entry point needs all three views of
/// it: the original source for diagnostics, the spanned tokens for mapping token spans back onto
/// that source, and the bare tokens the parser consumes.
pub struct LexedInput {
    pub source: String,
    pub spanned_tokens: Vec<SpannedToken>,
    pub tokens: Vec<Token>,
}

/// The tokens a completion offer is derived from, or none when `source` does not lex.
///
/// Completion runs on text the user is still typing, so a source that does not lex is the ordinary
/// case rather than a failure: an unterminated string or a half-written number leaves the lexer
/// with nothing, and a caller with no tokens has no expectations to turn into suggestions. The
/// diagnostics the lexer would produce belong to parsing, which reports them to the user; a
/// completion offer is not the place to raise them, so it builds neither them nor a report.
pub fn completion_tokens(source: &str) -> Option<Vec<Token>> {
    let Ok(spanned_tokens) = lex(source) else {
        return None;
    };
    Some(
        spanned_tokens
            .into_iter()
            .map(|spanned| spanned.token)
            .collect(),
    )
}

pub fn lex_input(input: &str) -> error_stack::Result<LexedInput, ParseFromSourceError> {
    let source = input.to_string();
    let spanned_tokens = lex(input).map_err(|errs| {
        Report::new(ParseFromSourceError::Lex {
            text: source.clone(),
            diagnostics: errs
                .into_iter()
                .map(|err| Diagnostic {
                    message: err.to_string(),
                    span: err.span().into_range(),
                })
                .collect(),
        })
    })?;

    let tokens = spanned_tokens
        .iter()
        .map(|t| t.token.clone())
        .collect::<Vec<_>>();

    Ok(LexedInput {
        source,
        spanned_tokens,
        tokens,
    })
}

pub fn into_parse_error(
    source: String,
    spanned_tokens: &[SpannedToken],
    source_len: usize,
    errs: Vec<ParseError<'_>>,
) -> Report<ParseFromSourceError> {
    Report::new(ParseFromSourceError::Parse {
        text: source,
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
    })
}

/// Split completion input at the cursor into the source the grammar should parse and the partial
/// word the user is part-way through typing.
///
/// The partial word is removed before parsing rather than left in place. A half-typed word is not a
/// token the grammar can make sense of, and inside a free-form expression region it is swallowed
/// into the region and read by the expression grammar, which fails with a custom error that
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
/// Semantic references and assignment bodies survive regardless: the server expands them into
/// concrete object names and filters those by the same prefix itself.
pub fn filter_by_prefix(suggestions: Vec<String>, prefix: &str) -> Vec<String> {
    if prefix.is_empty() {
        return suggestions;
    }
    let prefix = prefix.to_ascii_lowercase();
    suggestions
        .into_iter()
        .filter(|suggestion| {
            suggestion.starts_with("ref:")
                || matches!(
                    suggestion.as_str(),
                    "resource_version" | "completed_resource_version" | "set_assignments"
                )
                || suggestion.to_ascii_lowercase().starts_with(&prefix)
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

            let found_text = match found.as_deref() {
                Some(found) => format_found_token(found),
                None => "end of input".to_string(),
            };

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
        Token::Word(Word::Quoted(raw)) => format!("`{raw}`"),
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

#[cfg(test)]
mod tests {
    use chumsky::{
        error::{RichPattern, RichReason},
        prelude::*,
    };
    use meticulous::OptionExt as _;
    use nervix_models::NamePosition;
    use strum::IntoEnumIterator as _;

    use super::{
        ALTER_OPERATION_HEADS, LexedInput, ParseError, current_word_prefix, filter_where_clause,
        format_parse_error, into_parse_error, kw, lex_input, schema_ref, suggestions_from_errors,
        token_span_to_source_span,
    };
    use crate::lexer::{Identifier, Token, Word};

    #[test]
    fn canonical_nspl_quotes_a_keyword_exactly_where_the_grammar_reads_it_as_one() {
        for keyword in Identifier::iter() {
            let spelling = <&'static str>::from(keyword).to_ascii_lowercase();
            let quoted = format!("`{spelling}`");
            assert_eq!(
                NamePosition::Qualified.spell(&spelling),
                spelling,
                "{spelling} after a scope"
            );
            assert_eq!(
                NamePosition::Called.spell(&spelling) == quoted,
                keyword.is_expression_keyword(),
                "{spelling} as a call"
            );
            let heads_an_operation = ALTER_OPERATION_HEADS.contains(&keyword);
            assert_eq!(
                NamePosition::Bare.spell(&spelling) == quoted,
                keyword.is_expression_keyword() || heads_an_operation,
                "{spelling} as a bare name"
            );
        }
    }

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
        let LexedInput { tokens, .. } = lex_input("").expect("lex should succeed");
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
        let LexedInput {
            source,
            spanned_tokens,
            tokens,
        } = lex_input("create kafka").expect("lex should succeed");
        let output = kw(Identifier::Create)
            .ignore_then(kw(Identifier::Json))
            .then_ignore(end())
            .parse(tokens.as_slice());
        assert!(output.has_errors(), "parser should produce an error");

        let errors = output.into_errors();
        let error = errors
            .first()
            .verified("the has_errors check above established at least one diagnostic");
        let RichReason::ExpectedFound { expected, found } = error.reason() else {
            panic!("a keyword mismatch must retain expected and found tokens");
        };
        let mut expects_json = false;
        for pattern in expected {
            if let RichPattern::Label(label) = pattern
                && label.as_ref() == "JSON"
            {
                expects_json = true;
            }
        }
        assert!(expects_json);
        assert!(matches!(
            found.as_deref(),
            Some(Token::Word(Word::KnownWord {
                iden: Identifier::Kafka,
                ..
            }))
        ));

        let err = into_parse_error(source, &spanned_tokens, "create kafka".len(), errors);
        let diagnostics = match err.current_context() {
            super::ParseFromSourceError::Parse { diagnostics, .. } => diagnostics,
            other => panic!("expected parse error, got {other:?}"),
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].span, 7..12);
    }

    #[test]
    fn token_span_to_source_span_handles_empty_and_out_of_bounds_ranges() {
        let LexedInput { spanned_tokens, .. } =
            lex_input("create json").expect("lex should succeed");

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

    /// The filter `source` writes before its last word, when the filter's region ends right in
    /// front of that word and nothing else is left over.
    fn filter_before_the_last_word(source: &str) -> Option<nervix_models::Expression> {
        let LexedInput { tokens, .. } = lex_input(source).expect("lex should succeed");
        filter_where_clause()
            .then_ignore(any())
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .ok()
    }

    #[test]
    fn a_keyword_written_as_a_call_a_scope_or_a_field_stays_in_the_filter() {
        for (source, expected) in [
            (
                "FILTER WHERE max(input.readings) > 10 UNBRANCHED",
                "max(input.readings) > 10",
            ),
            (
                "FILTER WHERE (max(input.readings) > 10) USING",
                "max(input.readings) > 10",
            ),
            ("FILTER WHERE output.total > 0 BY", "output.total > 0"),
            (
                "FILTER WHERE input.max > 0 AND input.output < 1 MAX",
                "input.max > 0 AND input.output < 1",
            ),
        ] {
            let filter = filter_before_the_last_word(source)
                .unwrap_or_else(|| panic!("{source} must end its filter at the last word"));
            let expected =
                crate::parse_expression(expected).expect("the expected filter is an expression");
            assert_eq!(filter, expected, "{source}");
        }
    }

    #[test]
    fn a_keyword_where_an_operand_belongs_is_a_field_and_the_filter_ends_after_it() {
        for (source, expected) in [
            ("FILTER WHERE input.total > max TIME", "input.total > max"),
            (
                "FILTER WHERE input.total > output SCHEMA",
                "input.total > output",
            ),
            (
                "FILTER WHERE by(input.total) > to BY",
                "by(input.total) > to",
            ),
            (
                "FILTER WHERE `end` > input.end UNBRANCHED",
                "`end` > input.end",
            ),
        ] {
            let filter = filter_before_the_last_word(source)
                .unwrap_or_else(|| panic!("{source} must end its filter at the last word"));
            let expected =
                crate::parse_expression(expected).expect("the expected filter is an expression");
            assert_eq!(filter, expected, "{source}");
        }
    }

    #[test]
    fn a_filter_its_grammar_goes_on_with_and_then_rejects_is_malformed() {
        for source in [
            "FILTER WHERE max(input.readings > 10 UNBRANCHED",
            "FILTER WHERE input.total IS UNBRANCHED",
            "FILTER WHERE input.status IN ('void', UNBRANCHED",
            "FILTER WHERE end > 0 UNBRANCHED",
        ] {
            assert!(
                filter_before_the_last_word(source).is_none(),
                "{source} must be rejected"
            );
        }
    }

    /// The first diagnostic of `rejection`, the message a caller reads.
    fn first_message(rejection: &error_stack::Report<super::ParseFromSourceError>) -> String {
        let diagnostics = rejection.current_context().diagnostics();
        let first = diagnostics
            .first()
            .verified("a rejection carries at least one diagnostic");
        first.message.clone()
    }

    /// A statement whose embedded text is rejected, the one token of the statement the rejection
    /// points at, and the message a standalone reader gives the same embedded text where the reader
    /// itself rejects it. Where the embedded part is complete before that token, as a key list is
    /// before a second string written after its last key, the statement rejects the token with its
    /// own message instead.
    struct EmbeddedRejection {
        statement: &'static str,
        token: &'static str,
        standalone: Option<String>,
    }

    #[test]
    fn an_embedded_rejection_is_reported_at_its_token_with_the_standalone_message() {
        let expression = |text: &str| {
            let rejection = crate::parse_expression(text).expect_err("the text is rejected alone");
            Some(first_message(&rejection))
        };
        let list = |text: &str| {
            let rejection =
                crate::parse_expression_list(text).expect_err("the list is rejected alone");
            Some(first_message(&rejection))
        };
        let construction = |text: &str| {
            let rejection = crate::parse_route_construction(text)
                .expect_err("the construction is rejected alone");
            Some(first_message(&rejection))
        };
        let cases = [
            EmbeddedRejection {
                statement: "CREATE SUBSCRIPTION s TO r WHERE input.a > * 1;",
                token: "*",
                standalone: expression("input.a > * 1"),
            },
            EmbeddedRejection {
                statement: "CREATE JUNCTION peaks FROM sensors WHERE input.a < 1e3 UNBRANCHED TO \
                            alerts INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
                token: "1e3",
                standalone: expression("input.a < 1e3"),
            },
            EmbeddedRejection {
                statement: "CREATE JUNCTION peaks FROM sensors FILTER WHERE input.a IS NOT 'x' \
                            UNBRANCHED TO alerts INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
                token: "'x'",
                standalone: expression("input.a IS NOT 'x'"),
            },
            EmbeddedRejection {
                statement: "CREATE JUNCTION j FROM r UNBRANCHED TO o SET total = input.a / * 2 \
                            FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
                token: "*",
                standalone: construction("SET total = input.a / * 2"),
            },
            EmbeddedRejection {
                statement: "CREATE JUNCTION j FROM r UNBRANCHED TO o WHERE input.a % * 2 FLUSH \
                            IMMEDIATE ON MESSAGE ERROR LOG;",
                token: "*",
                standalone: construction("WHERE input.a % * 2"),
            },
            EmbeddedRejection {
                statement: "CREATE DEDUPLICATOR d FROM sensors DEDUPLICATE ON input.id, 'a' 'b' \
                            MAX TIME 10m UNBRANCHED TO o INHERIT ALL FLUSH IMMEDIATE ON MESSAGE \
                            ERROR LOG;",
                token: "'b'",
                standalone: None,
            },
            EmbeddedRejection {
                statement: "ALTER DEDUPLICATOR d SET DEDUPLICATE ON input.id, 'a' 'b';",
                token: "'b'",
                standalone: None,
            },
            EmbeddedRejection {
                statement: "CREATE DEDUPLICATOR d FROM sensors DEDUPLICATE ON input.id, * 1 MAX \
                            TIME 10m UNBRANCHED TO o INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
                            LOG;",
                token: "*",
                standalone: list("input.id, * 1"),
            },
            EmbeddedRejection {
                statement: "CREATE JUNCTION apply_defaults FROM ss1 BRANCHED BY by_default_state \
                            USING MATERIALIZED STATE default_preferences DEFAULT { theme = 'x' \
                            'y' } TO ss10 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
                            MESSAGE ERROR LOG;",
                token: "'y'",
                standalone: None,
            },
            EmbeddedRejection {
                statement: "CREATE EMITTER to_ch FROM notifications TO CLICKHOUSE \
                            clickhouse_client INSERT TO TABLE my_table VALUES { 'tags' = 1e9 } \
                            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s BATCH MAX MESSAGES 500 \
                            MAX SIZE 8MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR \
                            LOG;",
                token: "1e9",
                standalone: expression("1e9"),
            },
            EmbeddedRejection {
                statement: "CREATE EMITTER e FROM r TO SQS sqs_main QUEUE q.fifo FIFO GROUP \
                            input.a + / 1 MODE SINGLE RETRY POLICY BACKOFF 1s MAX 5s ENCODE USING \
                            c FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
                token: "/",
                standalone: expression("input.a + / 1"),
            },
        ];
        for case in cases {
            let EmbeddedRejection {
                statement,
                token,
                standalone,
            } = case;
            let offset = statement
                .find(token)
                .unwrap_or_else(|| panic!("{token} must occur in {statement}"));
            assert_eq!(
                statement.matches(token).count(),
                1,
                "{token} must occur once in {statement}"
            );
            let rejection = crate::client_statement::parse_client_statement(statement)
                .expect_err("the statement's embedded text is rejected");
            let diagnostics = rejection.current_context().diagnostics();
            let [diagnostic] = diagnostics else {
                panic!("{statement} must be rejected once, got {diagnostics:?}");
            };
            assert_eq!(
                (diagnostic.span.start, &statement[diagnostic.span.clone()]),
                (offset, token),
                "{statement} was rejected at another token: {}",
                diagnostic.message
            );
            if let Some(standalone) = standalone {
                assert_eq!(diagnostic.message, standalone, "{statement}");
            }
        }
    }

    #[test]
    fn completion_after_an_embedded_expression_offers_the_clauses_that_follow_it() {
        for (input, expected) in [
            (
                "CREATE JUNCTION j FROM r WHERE input.a BETWEEN 1 AND 2 ",
                vec![",", "BRANCHED BY", "COLLECT FOR", "FILTER", "UNBRANCHED"],
            ),
            (
                "CREATE JUNCTION j FROM r FILTER WHERE input.a IS DISTINCT FROM 1 ",
                vec!["BRANCHED BY", "UNBRANCHED"],
            ),
            (
                "CREATE JUNCTION j FROM r UNBRANCHED TO o SET x = CASE WHEN input.a THEN 1 END ",
                vec!["FLUSH EACH", "FLUSH IMMEDIATE"],
            ),
            (
                "CREATE DEDUPLICATOR d FROM r DEDUPLICATE ON TRY_CAST(input.a AS I64) ",
                vec!["MAX"],
            ),
            (
                "CREATE EMITTER e FROM r TO SQS sqs_main QUEUE q.fifo FIFO GROUP input.a ",
                vec!["MODE"],
            ),
        ] {
            assert_eq!(
                crate::client_statement::suggest_client_statement(input, input.len()),
                expected,
                "{input}"
            );
        }
    }

    #[test]
    fn completion_inside_an_unfinished_embedded_expression_offers_nothing() {
        for input in [
            "CREATE SUBSCRIPTION s TO r WHERE input.a IS ",
            "CREATE SUBSCRIPTION s TO r WHERE CASE WHEN input.a THEN ",
            "CREATE JUNCTION j FROM r UNBRANCHED TO o SET x = ",
            "CREATE JUNCTION j FROM r FILTER WHERE input.a NOT ",
        ] {
            assert_eq!(
                crate::client_statement::suggest_client_statement(input, input.len()),
                Vec::<String>::new(),
                "{input}"
            );
        }
    }

    #[test]
    fn completion_offers_a_keyword_only_expressions_read_at_no_statement_position() {
        let expression_keywords = [
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "BETWEEN",
            "IS",
            "DISTINCT",
            "TRY_CAST",
            "JSON_VALUE",
            "TRY_JSON_VALUE",
            "JSON_EXISTS",
        ];
        for input in [
            "",
            "CREATE ",
            "CREATE JUNCTION j FROM r ",
            "CREATE SUBSCRIPTION s TO r ",
            "CREATE EMITTER e FROM r TO SQS sqs_main QUEUE q.fifo FIFO GROUP ",
        ] {
            let suggestions = crate::client_statement::suggest_client_statement(input, input.len());
            for keyword in expression_keywords {
                assert!(
                    !suggestions.iter().any(|suggestion| suggestion == keyword),
                    "{keyword} leaked into {input:?}: {suggestions:?}"
                );
            }
        }
    }
}
