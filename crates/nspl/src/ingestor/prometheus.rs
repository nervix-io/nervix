//! The Prometheus connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a Prometheus ingestor source: its client, query, cadence and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the Prometheus vocabulary.
//! - **Must not know.** The Prometheus HTTP API or how a query is evaluated.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{IngestSource, PrometheusQuerySource, PrometheusSource};

use super::delivery::polling_quiesce_clause;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, domain_clock_period_lit, kw, string_lit},
};

pub(super) fn prometheus_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Prometheus)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Query))
        .then(string_lit())
        .then_ignore(kw(Identifier::Every))
        .then(domain_clock_period_lit())
        .then(polling_quiesce_clause())
        .map(|(((client, query), every), quiesce)| {
            IngestSource::Prometheus(PrometheusSource::Query(PrometheusQuerySource {
                client,
                query,
                every,
                quiesce,
            }))
        })
}
