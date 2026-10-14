//! The ingestor grammar of the endpoints a cluster serves.
//!
//! Layer: language.
//!
//! - **Owns.** The body of an endpoint ingestor source: the endpoint it reads, its delivery mode and
//!   quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the endpoint vocabulary.
//! - **Must not know.** How an endpoint is published, routed to or served.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{EndpointEndpointSource, EndpointIngestMode, EndpointSource, IngestSource};

use super::delivery::endpoint_quiesce_clause;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, endpoint_ref, kw, sequential_ack_window},
};

pub(super) fn endpoint_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], EndpointIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(sequential_ack_window())
        .to(EndpointIngestMode::NoAckSequential)
}

pub(super) fn endpoint_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Endpoint)
        .ignore_then(endpoint_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(endpoint_mode_parser())
        .then(endpoint_quiesce_clause())
        .map(|((endpoint, mode), quiesce)| {
            IngestSource::Endpoint(EndpointSource::Endpoint(EndpointEndpointSource {
                endpoint,
                mode,
                quiesce,
            }))
        })
}
