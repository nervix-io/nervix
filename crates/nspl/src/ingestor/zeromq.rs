//! The ZeroMQ connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a ZeroMQ ingestor source: its client, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the ZeroMQ vocabulary.
//! - **Must not know.** The ZeroMQ driver or how a socket is bound or connected.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{IngestSource, ZeroMqIngestMode, ZeroMqPullSource, ZeroMqSource};

use super::delivery::flexible_quiesce_clause;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw, sequential_ack_window},
};

pub(super) fn zeromq_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], ZeroMqIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(sequential_ack_window())
        .to(ZeroMqIngestMode::NoAckSequential)
}

pub(super) fn zeromq_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Zeromq)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(zeromq_mode_parser())
        .then(flexible_quiesce_clause())
        .map(|((client, mode), quiesce)| {
            IngestSource::ZeroMq(ZeroMqSource::Pull(ZeroMqPullSource {
                client,
                mode,
                quiesce,
            }))
        })
}
