//! Redis emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Redis emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn redis_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Redis)
        .ignore_then(kw(Identifier::Pubsub))
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Channel))
        .then(channel_ref())
        .map(|(client, channel)| {
            EmitSink::Redis(RedisSink::PubSub(RedisPubSubSink { client, channel }))
        })
}
