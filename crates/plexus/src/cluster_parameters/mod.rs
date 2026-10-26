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
use nervix_models::{AlterCluster, ClusterParameterChange, ClusterParameterUpdates};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, boxed_choice, byte_size_lit, duration_lit, kw, kw_phrase3, tok, u64_value,
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

fn assignment<'src, V: 'src>(
    parameter: Identifier,
    value: impl Parser<'src, &'src [Token], V, extra::Err<ParseError<'src>>> + Clone + 'src,
    change: impl Fn(V) -> ClusterParameterChange + Clone + 'src,
) -> impl Parser<'src, &'src [Token], ClusterParameterChange, extra::Err<ParseError<'src>>> + Clone
{
    kw(parameter)
        .ignore_then(tok(Token::Eq))
        .ignore_then(value)
        .map(change)
}

pub fn alter_cluster_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterCluster, extra::Err<ParseError<'src>>> + Clone {
    let assignments = boxed_choice!(
        assignment(
            Identifier::NodeUnavailabilityTimeout,
            duration_value(),
            ClusterParameterChange::NodeUnavailabilityTimeout
        ),
        assignment(
            Identifier::DomainDrainTimeout,
            duration_value(),
            ClusterParameterChange::DomainDrainTimeout
        ),
        assignment(
            Identifier::EntityGateDeadline,
            duration_value(),
            ClusterParameterChange::EntityGateDeadline
        ),
        assignment(
            Identifier::TransactionIdleTimeout,
            duration_value(),
            ClusterParameterChange::TransactionIdleTimeout
        ),
        assignment(
            Identifier::TransactionTombstoneRetention,
            duration_value(),
            ClusterParameterChange::TransactionTombstoneRetention
        ),
        assignment(
            Identifier::CommandRetryValidity,
            duration_value(),
            ClusterParameterChange::CommandRetryValidity
        ),
        assignment(
            Identifier::StateSnapshotInterval,
            duration_value(),
            ClusterParameterChange::StateSnapshotInterval
        ),
        assignment(
            Identifier::TransactionMaxStatements,
            u64_value("integer_literal"),
            ClusterParameterChange::TransactionMaxStatements
        ),
        assignment(
            Identifier::TransactionMaxSourceBytes,
            byte_value(),
            ClusterParameterChange::TransactionMaxSourceBytes
        ),
        assignment(
            Identifier::TransactionMaxOpen,
            u64_value("integer_literal"),
            ClusterParameterChange::TransactionMaxOpen
        ),
        assignment(
            Identifier::CommandExecutionCapacity,
            u64_value("integer_literal"),
            ClusterParameterChange::CommandExecutionCapacity
        ),
        assignment(
            Identifier::ReplicaCount,
            u64_value("integer_literal"),
            ClusterParameterChange::ReplicaCount
        ),
        assignment(
            Identifier::ResourceMaxArchiveBytes,
            byte_value(),
            ClusterParameterChange::ResourceMaxArchiveBytes
        ),
        assignment(
            Identifier::ResourceMaxExtractedBytes,
            byte_value(),
            ClusterParameterChange::ResourceMaxExtractedBytes
        ),
        assignment(
            Identifier::ResourceMaxFileCount,
            u64_value("integer_literal"),
            ClusterParameterChange::ResourceMaxFileCount
        ),
        assignment(
            Identifier::MaxBatchMessages,
            u64_value("integer_literal"),
            ClusterParameterChange::MaxBatchMessages
        ),
        assignment(
            Identifier::ClientProducerSessionBytes,
            byte_value(),
            ClusterParameterChange::ClientProducerSessionBytes
        ),
        assignment(
            Identifier::ClientConsumerSessionBytes,
            byte_value(),
            ClusterParameterChange::ClientConsumerSessionBytes
        ),
        assignment(
            Identifier::UdfCodeBytes,
            byte_value(),
            ClusterParameterChange::UdfCodeBytes
        ),
        assignment(
            Identifier::HttpHeaderBytes,
            byte_value(),
            ClusterParameterChange::HttpHeaderBytes
        ),
        assignment(
            Identifier::HttpHeaderFields,
            u64_value("integer_literal"),
            ClusterParameterChange::HttpHeaderFields
        ),
        assignment(
            Identifier::ClientBatchRows,
            u64_value("integer_literal"),
            ClusterParameterChange::ClientBatchRows
        ),
    );
    kw_phrase3(Identifier::Alter, Identifier::Cluster, Identifier::Set)
        .ignore_then(
            assignments
                .separated_by(tok(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>(),
        )
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
