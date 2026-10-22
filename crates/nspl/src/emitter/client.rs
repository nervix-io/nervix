//! Client emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Client emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn client_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Client)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(schema_ref())
        .map(|schema| EmitSink::Client(ClientSink::Consumer(ClientConsumerSink { schema })))
}

/// Native client output is constructed against its declared schema without a codec or VALUES.
pub(super) fn native_client_sink<'src>()
-> impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone {
    sink_with_publishing_mode(client_emit_sink_parser(), client_ack_publishing_mode())
        .then(route_construction().or_not())
        .map(|(sink, construction)| ParsedSink {
            sink: sink.sink,
            publishing_mode: sink.publishing_mode,
            body: EmitterBody::Client,
            construction,
            batch: None,
        })
        .boxed()
}
