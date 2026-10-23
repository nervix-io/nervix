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
use nervix_models::{PrometheusIngestMode, PrometheusQuerySource};

use super::delivery::{no_ack, polling_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, client_ref, domain_clock_period_lit, kw, max_body_size_clause, string_lit,
    },
};

/// `FROM <client> QUERY '<promql>' [MAX BODY SIZE <bytes>] EVERY <period> MODE NO ACK ON QUIESCE
/// SUSPEND|BUFFER …`.
pub(super) fn query_body<'src>()
-> impl Parser<'src, &'src [Token], PrometheusQuerySource, extra::Err<ParseError<'src>>> + Clone {
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
            |(((((client, query), max_body_size), every), mode), quiesce)| PrometheusQuerySource {
                client,
                query,
                max_body_size,
                every,
                mode,
                quiesce,
            },
        )
        .boxed()
}
