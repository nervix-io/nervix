//! The syslog connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `SYSLOG LISTENER` ingestor: its client, delivery mode and quiesce
//!   mode.
//! - **Depends on.** The shared ingestor clauses and the syslog vocabulary.
//! - **Must not know.** Syslog framing or how a listener is bound.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{SyslogIngestMode, SyslogListenerSource};

use super::delivery::{flexible_quiesce_clause, no_ack};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw},
};

/// `FROM <client> MODE NO ACK ON QUIESCE SUSPEND|BUFFER …|DROP`.
pub(super) fn listener_body<'src>()
-> impl Parser<'src, &'src [Token], SyslogListenerSource, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(SyslogIngestMode::NoAck))
        .then(flexible_quiesce_clause())
        .map(|((client, mode), quiesce)| SyslogListenerSource {
            client,
            mode,
            quiesce,
        })
        .boxed()
}
