//! Structured cluster parameter assignments in the composed Plexus grammar.
//!
//! Layer: language.
//! - **Owns.** The administrative assignment grammar and its completion expectations.
//! - **Depends on.** Shared tokens and literal parsers, and semantic parameter values.
//! - **Must not know.** Graph validation, consensus or service installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    AlterCluster, ClusterParameterChange, ClusterParameterError, ClusterParameterKey,
    ClusterParameterUpdates,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, byte_size_lit, duration_lit, kw, kw_phrase3, string_lit_with_values, tok,
        u64_value,
    },
};

fn duration_value<'src>()
-> impl Parser<'src, &'src [Token], std::time::Duration, extra::Err<ParseError<'src>>> + Clone {
    duration_lit().try_map(|value, span| {
        nervix_models::parse_duration_text(&value)
            .map_err(|report| Rich::custom(span, format!("{report:#}")))
    })
}

fn byte_value<'src>() -> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone
{
    byte_size_lit().try_map(|value, span| match value.parse::<ubyte::ByteUnit>() {
        Ok(bytes) => Ok(bytes.as_u64()),
        Err(error) => Err(Rich::custom(span, error.to_string())),
    })
}

fn algorithm_names<'src>(
    names: impl IntoIterator<Item = &'static str>,
) -> impl Parser<'src, &'src [Token], Vec<String>, extra::Err<ParseError<'src>>> + Clone {
    string_lit_with_values(names)
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LParen), tok(Token::RParen))
}

/// The parser of one whole assignment, `<KEY> = <value>`, read into the change it proposes.
type AssignmentParser<'src> =
    Boxed<'src, 'src, &'src [Token], ClusterParameterChange, extra::Err<ParseError<'src>>>;

/// One parameter's assignment as the grammar offers it.
#[derive(Clone)]
struct OfferedAssignment<'src> {
    parameter: ClusterParameterKey,
    keyword: Identifier,
    parser: AssignmentParser<'src>,
}

/// `keyword = <value>` for `parameter`, read into the change `change` makes of the value.
fn assignment<'src, V: 'src>(
    parameter: ClusterParameterKey,
    keyword: Identifier,
    value: impl Parser<'src, &'src [Token], V, extra::Err<ParseError<'src>>> + Clone + 'src,
    change: impl Fn(V) -> ClusterParameterChange + Clone + 'src,
) -> OfferedAssignment<'src> {
    let parser = kw(keyword)
        .ignore_then(tok(Token::Eq))
        .ignore_then(value)
        .map(change)
        .boxed();
    OfferedAssignment {
        parameter,
        keyword,
        parser,
    }
}

/// The changes a statement has written so far, in the order written.
///
/// A statement assigns each parameter the vocabulary declares at most once, so this holds at most
/// one change per declared parameter, whatever the cluster, its domains or their graphs.
#[derive(Default)]
struct WrittenChanges(Vec<ClusterParameterChange>);

impl WrittenChanges {
    /// Whether a written change already assigns `parameter`.
    fn assigns(&self, parameter: ClusterParameterKey) -> bool {
        self.0.iter().any(|change| change.parameter() == parameter)
    }

    fn record(&mut self, change: ClusterParameterChange) {
        self.0.push(change);
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn into_changes(self) -> Vec<ClusterParameterChange> {
        self.0
    }
}

/// The assignment of every parameter, in the order the vocabulary declares the parameters.
///
/// It holds one assignment per declared parameter, so finding the one a keyword writes scans the
/// parameters the vocabulary declares, whatever the cluster, its domains or their graphs.
#[derive(Clone)]
struct OfferedAssignments<'src>(Vec<OfferedAssignment<'src>>);

impl<'src> OfferedAssignments<'src> {
    /// Every parameter's assignment, in the literal family the parameter accepts.
    fn new() -> Self {
        Self(vec![
            assignment(
                ClusterParameterKey::NodeUnavailabilityTimeout,
                Identifier::NodeUnavailabilityTimeout,
                duration_value(),
                ClusterParameterChange::NodeUnavailabilityTimeout,
            ),
            assignment(
                ClusterParameterKey::DomainDrainTimeout,
                Identifier::DomainDrainTimeout,
                duration_value(),
                ClusterParameterChange::DomainDrainTimeout,
            ),
            assignment(
                ClusterParameterKey::EntityGateDeadline,
                Identifier::EntityGateDeadline,
                duration_value(),
                ClusterParameterChange::EntityGateDeadline,
            ),
            assignment(
                ClusterParameterKey::TransactionIdleTimeout,
                Identifier::TransactionIdleTimeout,
                duration_value(),
                ClusterParameterChange::TransactionIdleTimeout,
            ),
            assignment(
                ClusterParameterKey::TransactionTombstoneRetention,
                Identifier::TransactionTombstoneRetention,
                duration_value(),
                ClusterParameterChange::TransactionTombstoneRetention,
            ),
            assignment(
                ClusterParameterKey::CommandRetryValidity,
                Identifier::CommandRetryValidity,
                duration_value(),
                ClusterParameterChange::CommandRetryValidity,
            ),
            assignment(
                ClusterParameterKey::StateSnapshotInterval,
                Identifier::StateSnapshotInterval,
                duration_value(),
                ClusterParameterChange::StateSnapshotInterval,
            ),
            assignment(
                ClusterParameterKey::TransactionMaxStatements,
                Identifier::TransactionMaxStatements,
                u64_value("integer_literal"),
                ClusterParameterChange::TransactionMaxStatements,
            ),
            assignment(
                ClusterParameterKey::TransactionMaxSourceBytes,
                Identifier::TransactionMaxSourceBytes,
                byte_value(),
                ClusterParameterChange::TransactionMaxSourceBytes,
            ),
            assignment(
                ClusterParameterKey::TransactionMaxOpen,
                Identifier::TransactionMaxOpen,
                u64_value("integer_literal"),
                ClusterParameterChange::TransactionMaxOpen,
            ),
            assignment(
                ClusterParameterKey::CommandExecutionCapacity,
                Identifier::CommandExecutionCapacity,
                u64_value("integer_literal"),
                ClusterParameterChange::CommandExecutionCapacity,
            ),
            assignment(
                ClusterParameterKey::ReplicaCount,
                Identifier::ReplicaCount,
                u64_value("integer_literal"),
                ClusterParameterChange::ReplicaCount,
            ),
            assignment(
                ClusterParameterKey::ResourceMaxArchiveBytes,
                Identifier::ResourceMaxArchiveBytes,
                byte_value(),
                ClusterParameterChange::ResourceMaxArchiveBytes,
            ),
            assignment(
                ClusterParameterKey::ResourceMaxExtractedBytes,
                Identifier::ResourceMaxExtractedBytes,
                byte_value(),
                ClusterParameterChange::ResourceMaxExtractedBytes,
            ),
            assignment(
                ClusterParameterKey::ResourceMaxFileCount,
                Identifier::ResourceMaxFileCount,
                u64_value("integer_literal"),
                ClusterParameterChange::ResourceMaxFileCount,
            ),
            assignment(
                ClusterParameterKey::MaxBatchMessages,
                Identifier::MaxBatchMessages,
                u64_value("integer_literal"),
                ClusterParameterChange::MaxBatchMessages,
            ),
            assignment(
                ClusterParameterKey::ClientProducerSessionBytes,
                Identifier::ClientProducerSessionBytes,
                byte_value(),
                ClusterParameterChange::ClientProducerSessionBytes,
            ),
            assignment(
                ClusterParameterKey::ClientConsumerSessionBytes,
                Identifier::ClientConsumerSessionBytes,
                byte_value(),
                ClusterParameterChange::ClientConsumerSessionBytes,
            ),
            assignment(
                ClusterParameterKey::UdfCodeBytes,
                Identifier::UdfCodeBytes,
                byte_value(),
                ClusterParameterChange::UdfCodeBytes,
            ),
            assignment(
                ClusterParameterKey::HttpHeaderBytes,
                Identifier::HttpHeaderBytes,
                byte_value(),
                ClusterParameterChange::HttpHeaderBytes,
            ),
            assignment(
                ClusterParameterKey::HttpHeaderFields,
                Identifier::HttpHeaderFields,
                u64_value("integer_literal"),
                ClusterParameterChange::HttpHeaderFields,
            ),
            assignment(
                ClusterParameterKey::ClientBatchRows,
                Identifier::ClientBatchRows,
                u64_value("integer_literal"),
                ClusterParameterChange::ClientBatchRows,
            ),
            assignment(
                ClusterParameterKey::TlsCipherSuites,
                Identifier::TlsCipherSuites,
                algorithm_names(nervix_models::TlsCipherSuites::names()).try_map(|names, span| {
                    nervix_models::TlsCipherSuites::parse_names(names)
                        .map_err(|error| Rich::custom(span, format!("{error:#}")))
                }),
                ClusterParameterChange::TlsCipherSuites,
            ),
            assignment(
                ClusterParameterKey::TlsKeyExchangeGroups,
                Identifier::TlsKeyExchangeGroups,
                algorithm_names(nervix_models::TlsKeyExchangeGroups::names()).try_map(
                    |names, span| {
                        nervix_models::TlsKeyExchangeGroups::parse_names(names)
                            .map_err(|error| Rich::custom(span, format!("{error:#}")))
                    },
                ),
                ClusterParameterChange::TlsKeyExchangeGroups,
            ),
        ])
    }

    /// The assignment whose keyword `token` writes, when it writes one.
    fn written(&self, token: Option<&Token>) -> Option<&OfferedAssignment<'src>> {
        let Some(Token::Word(Word::KnownWord { iden, .. })) = token else {
            return None;
        };
        self.0.iter().find(|offered| offered.keyword == *iden)
    }

    /// Whether `written` assigns every parameter, which leaves no assignment to offer.
    fn exhausted_by(&self, written: &WrittenChanges) -> bool {
        written.len() == self.0.len()
    }

    /// The assignments of the parameters `written` does not assign yet, in declaration order.
    fn remaining(&self, written: &WrittenChanges) -> Vec<AssignmentParser<'src>> {
        let mut remaining = Vec::with_capacity(self.0.len());
        for offered in &self.0 {
            if !written.assigns(offered.parameter) {
                remaining.push(offered.parser.clone());
            }
        }
        remaining
    }
}

/// Refuses the parameter written where the input stands, which the statement already assigns,
/// naming it at the word that repeats it.
fn refused_repeat<'src, O: 'src>(
    parameter: ClusterParameterKey,
) -> impl Parser<'src, &'src [Token], O, extra::Err<ParseError<'src>>> + Clone {
    let refusal = ClusterParameterError::Duplicate { parameter }.to_string();
    any().try_map(move |_, span| Err(Rich::custom(span, &refusal)))
}

/// Assignments separated by commas, each of a parameter the statement has not assigned yet, in
/// the order written.
///
/// Where an assignment begins, the grammar expects only the parameters still unassigned, so
/// completion never offers one the statement already assigns. A parameter written again is
/// refused at the word that repeats it, naming the parameter. Refused only after the whole list,
/// as the vocabulary would, the refusal would rank at the statement's start, below the
/// expectations other `ALTER` families report after `ALTER`.
///
/// The keyword a statement writes selects its parameter's assignment directly. Where no
/// parameter's keyword is written, the choice of the remaining assignments fails there, expecting
/// exactly the parameters the statement may still assign.
fn distinct_assignments<'src>()
-> impl Parser<'src, &'src [Token], Vec<ClusterParameterChange>, extra::Err<ParseError<'src>>> + Clone
{
    let offered = OfferedAssignments::new();
    custom(move |input| {
        let mut written = WrittenChanges::default();
        // Each pass assigns one parameter the statement has not assigned, so the parameters bound
        // the passes.
        loop {
            let change = match offered.written(input.peek_ref()) {
                Some(assignment) if written.assigns(assignment.parameter) => {
                    return input.parse(refused_repeat(assignment.parameter));
                }
                Some(assignment) => input.parse(assignment.parser.clone())?,
                None => input.parse(choice(offered.remaining(&written)))?,
            };
            written.record(change);
            let separator = input.save();
            if input.parse(tok(Token::Comma).or_not())?.is_none() {
                break;
            }
            // Once every parameter is assigned, a comma can begin only a repeat, which the next
            // pass refuses. Anything else leaves the comma to the statement, which reports it
            // where it is written.
            if offered.exhausted_by(&written) && offered.written(input.peek_ref()).is_none() {
                input.rewind(separator);
                break;
            }
        }
        Ok(written.into_changes())
    })
}

pub fn alter_cluster_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterCluster, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase3(Identifier::Alter, Identifier::Cluster, Identifier::Set)
        .ignore_then(distinct_assignments())
        .try_map(
            |assignments, span| match ClusterParameterUpdates::try_from(assignments) {
                Ok(parameters) => Ok(AlterCluster { parameters }),
                Err(report) => Err(Rich::custom(span, format!("{report:#}"))),
            },
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

#[cfg(test)]
mod tests;
