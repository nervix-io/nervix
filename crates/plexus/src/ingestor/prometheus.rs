//! The Prometheus connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `PROMETHEUS QUERY` ingestor: its client, query, response bound,
//!   cadence, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the Prometheus vocabulary.
//! - **Must not know.** The Prometheus HTTP API or how a query is evaluated.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{PrometheusIngestMode, PrometheusQueryIngestor, PrometheusQueryIngestorChange};

use super::delivery::{no_ack, polling_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, client_ref, domain_clock_period_lit, kw, kw_phrase3, max_body_size_clause,
        string_lit,
    },
};

/// `FROM <client> QUERY '<promql>' [MAX BODY SIZE <bytes>] EVERY <period> MODE NO ACK ON QUIESCE
/// SUSPEND|BUFFER …`.
pub(super) fn query_body<'src>()
-> impl Parser<'src, &'src [Token], PrometheusQueryIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Query))
        .then(string_lit())
        .then(max_body_size_clause().or_not())
        .then_ignore(kw(Identifier::Every))
        .then(domain_clock_period_lit())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(PrometheusIngestMode::NoAck))
        .then(polling_quiesce_clause())
        .map(
            |(((((client, query), max_body_size), every), mode), quiesce)| {
                PrometheusQueryIngestor {
                    client,
                    query,
                    max_body_size,
                    every,
                    mode,
                    quiesce,
                }
            },
        )
        .boxed()
}

/// `SET QUERY`, `SET MAX BODY SIZE`, `DROP MAX BODY SIZE`, `SET EVERY` or `SET MODE`: one part of
/// a `PROMETHEUS QUERY` body.
pub(super) fn change<'src>()
-> impl Parser<'src, &'src [Token], PrometheusQueryIngestorChange, extra::Err<ParseError<'src>>> + Clone
{
    let query = kw(Identifier::Query)
        .ignore_then(string_lit())
        .map(PrometheusQueryIngestorChange::Query);
    let limit = max_body_size_clause().map(PrometheusQueryIngestorChange::MaxBodySize);
    let every = kw(Identifier::Every)
        .ignore_then(domain_clock_period_lit())
        .map(PrometheusQueryIngestorChange::Every);
    let mode = kw(Identifier::Mode)
        .ignore_then(no_ack().to(PrometheusIngestMode::NoAck))
        .map(PrometheusQueryIngestorChange::Mode);
    let set = kw(Identifier::Set).ignore_then(choice((query, limit, every, mode)));
    let drop_limit = kw(Identifier::Drop)
        .ignore_then(kw_phrase3(
            Identifier::Max,
            Identifier::Body,
            Identifier::Size,
        ))
        .to(PrometheusQueryIngestorChange::DropMaxBodySize);
    choice((set, drop_limit)).boxed()
}
