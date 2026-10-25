//! The HTTP connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The bodies of an `HTTP REQUEST` ingestor — its client, cadence, delivery mode and
//!   quiesce mode — and of an `HTTP ENDPOINT` ingestor.
//! - **Depends on.** The shared ingestor clauses, the endpoint intake body and the HTTP
//!   vocabulary.
//! - **Must not know.** The HTTP client library, how a request is sent, or how an endpoint is
//!   served.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{HttpEndpointIngestor, HttpRequestIngestMode, HttpRequestIngestor};

use super::{
    delivery::{no_ack, polling_quiesce_clause},
    endpoint::{EndpointIntake, intake},
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, domain_clock_period_lit, kw},
};

/// `FROM <client> EVERY <period> MODE NO ACK ON QUIESCE SUSPEND|BUFFER …`.
pub(super) fn request_body<'src>()
-> impl Parser<'src, &'src [Token], HttpRequestIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Every))
        .then(domain_clock_period_lit())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(HttpRequestIngestMode::NoAck))
        .then(polling_quiesce_clause())
        .map(|(((client, every), mode), quiesce)| HttpRequestIngestor {
            client,
            every,
            mode,
            quiesce,
        })
        .boxed()
}

/// The body of an ingestor of an HTTP endpoint this cluster serves.
pub(super) fn endpoint_body<'src>()
-> impl Parser<'src, &'src [Token], HttpEndpointIngestor, extra::Err<ParseError<'src>>> + Clone {
    intake().map(
        |EndpointIntake {
             endpoint,
             mode,
             quiesce,
         }| HttpEndpointIngestor {
            endpoint,
            mode,
            quiesce,
        },
    )
}
