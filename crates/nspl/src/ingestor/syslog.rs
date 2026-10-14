//! The syslog connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a syslog ingestor source: its client, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the syslog vocabulary.
//! - **Must not know.** Syslog framing or how a listener is bound.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{IngestSource, SyslogListenerSource, SyslogSource};

use super::delivery::flexible_quiesce_clause;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw, sequential_ack_window},
};

pub(super) fn syslog_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Syslog)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Mode))
        .then_ignore(kw(Identifier::NoAck))
        .then_ignore(sequential_ack_window())
        .then(flexible_quiesce_clause())
        .map(|(client, quiesce)| {
            IngestSource::Syslog(SyslogSource::Listener(SyslogListenerSource {
                client,
                quiesce,
            }))
        })
}
