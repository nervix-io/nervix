//! The WebSockets connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The bodies of a `WEBSOCKETS CONNECTION` ingestor — its client, delivery mode and
//!   quiesce mode — and of a `WEBSOCKETS ENDPOINT` ingestor.
//! - **Depends on.** The shared ingestor clauses, the endpoint intake body and the WebSockets
//!   vocabulary.
//! - **Must not know.** The WebSockets driver, how a connection is opened or signaled, or how an
//!   endpoint is served.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{WebsocketsConnectionSource, WebsocketsEndpointSource, WebsocketsIngestMode};

use super::{
    delivery::{no_ack, push_quiesce_clause},
    endpoint::{EndpointIntake, intake},
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw},
};

/// `FROM <client> MODE NO ACK ON QUIESCE BUFFER …|DROP`.
pub(super) fn connection_body<'src>()
-> impl Parser<'src, &'src [Token], WebsocketsConnectionSource, extra::Err<ParseError<'src>>> + Clone
{
    client_ref()
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(WebsocketsIngestMode::NoAck))
        .then(push_quiesce_clause())
        .map(|((client, mode), quiesce)| WebsocketsConnectionSource {
            client,
            mode,
            quiesce,
        })
        .boxed()
}

/// The body of an ingestor of a WebSockets endpoint this cluster serves.
pub(super) fn endpoint_body<'src>()
-> impl Parser<'src, &'src [Token], WebsocketsEndpointSource, extra::Err<ParseError<'src>>> + Clone
{
    intake().map(
        |EndpointIntake {
             endpoint,
             mode,
             quiesce,
         }| WebsocketsEndpointSource {
            endpoint,
            mode,
            quiesce,
        },
    )
}
