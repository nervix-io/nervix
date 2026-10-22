//! Rabbitmq emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Rabbitmq emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn rabbitmq_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Rabbitmq)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Queue))
        .then(queue_ref())
        .map(|(client, queue)| {
            EmitSink::RabbitMq(RabbitMqSink::Queue(RabbitMqQueueSink { client, queue }))
        })
}
