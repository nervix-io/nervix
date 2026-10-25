//! The ZeroMQ connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `ZEROMQ PULL` ingestor: its client, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the ZeroMQ vocabulary.
//! - **Must not know.** The ZeroMQ driver or how a socket is bound or connected.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{ZeroMqIngestMode, ZeroMqPullIngestor};

use super::delivery::{flexible_quiesce_clause, no_ack};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw},
};

/// `FROM <client> MODE NO ACK ON QUIESCE SUSPEND|BUFFER …|DROP`.
pub(super) fn pull_body<'src>()
-> impl Parser<'src, &'src [Token], ZeroMqPullIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(ZeroMqIngestMode::NoAck))
        .then(flexible_quiesce_clause())
        .map(|((client, mode), quiesce)| ZeroMqPullIngestor {
            client,
            mode,
            quiesce,
        })
        .boxed()
}
