//! HTTP emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of HTTP emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn http_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], SinkWithPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Http)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Method))
        .then(embedded_expression("string_expression"))
        .then_ignore(kw(Identifier::Path))
        .then(embedded_expression("string_expression"))
        .then_ignore(kw(Identifier::Mode))
        .then(request_ack_publishing_mode())
        .map(
            |(((client, method), path), publishing_mode)| SinkWithPublishingMode {
                sink: EmitSink::Http(HttpSink::Request(HttpRequestSink {
                    client,
                    method,
                    path,
                })),
                publishing_mode,
            },
        )
        .boxed()
}

pub(super) fn http_body_selection<'src>()
-> impl Parser<'src, &'src [Token], EmitterBody, extra::Err<ParseError<'src>>> + Clone {
    choice((
        encode_using_clause().map(|codec| EmitterBody::Codec { codec }),
        kw_phrase2(Identifier::Without, Identifier::Body).to(EmitterBody::WithoutBody),
    ))
    .boxed()
}

pub(super) fn http_create_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], ParsedSink, extra::Err<ParseError<'src>>> + Clone {
    let encoded = http_sink_parser()
        .then(encode_using_clause())
        .then(route_construction().or_not())
        .map(|((head, codec), construction)| ParsedSink {
            sink: head.sink,
            publishing_mode: head.publishing_mode,
            body: EmitterBody::Codec { codec },
            construction,
            batch: None,
        });
    let bodyless = http_sink_parser()
        .then_ignore(kw_phrase2(Identifier::Without, Identifier::Body))
        .then(bodyless_route_construction().or_not())
        .map(|(head, construction)| ParsedSink {
            sink: head.sink,
            publishing_mode: head.publishing_mode,
            body: EmitterBody::WithoutBody,
            construction,
            batch: None,
        });
    choice((encoded, bodyless)).boxed()
}
