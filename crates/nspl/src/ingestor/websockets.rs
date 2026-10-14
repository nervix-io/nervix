//! The WebSockets connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a WebSockets ingestor source: its client, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the WebSockets vocabulary.
//! - **Must not know.** The WebSockets driver or how a connection is opened or signaled.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    IngestSource, WebsocketsConnectionSource, WebsocketsIngestMode, WebsocketsSource,
};

use super::delivery::push_quiesce_clause;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw, sequential_ack_window},
};

pub(super) fn websockets_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], WebsocketsIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(sequential_ack_window())
        .to(WebsocketsIngestMode::NoAckSequential)
}

pub(super) fn websockets_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Websockets)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(websockets_mode_parser())
        .then(push_quiesce_clause())
        .map(|((client, mode), quiesce)| {
            IngestSource::Websockets(WebsocketsSource::Connection(WebsocketsConnectionSource {
                client,
                mode,
                quiesce,
            }))
        })
}
