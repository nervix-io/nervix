//! The HTTP connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of an HTTP polling ingestor source: its client, cadence and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the HTTP vocabulary.
//! - **Must not know.** The HTTP client library or how a request is sent.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{HttpRequestSource, HttpSource, IngestSource};

use super::delivery::polling_quiesce_clause;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, domain_clock_period_lit, kw},
};

pub(super) fn http_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Http)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Every))
        .then(domain_clock_period_lit())
        .then(polling_quiesce_clause())
        .map(|((client, every), quiesce)| {
            IngestSource::Http(HttpSource::Request(HttpRequestSource {
                client,
                every,
                quiesce,
            }))
        })
}
