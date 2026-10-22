//! Syslog emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Syslog emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn syslog_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Syslog)
        .ignore_then(client_ref())
        .map(|client| EmitSink::Syslog(SyslogSink::Collector(SyslogCollectorSink { client })))
}
