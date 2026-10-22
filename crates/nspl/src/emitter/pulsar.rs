//! Pulsar emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Pulsar emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn pulsar_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Pulsar)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .map(|(client, topic)| {
            EmitSink::Pulsar(PulsarSink::Topic(PulsarTopicSink { client, topic }))
        })
}
