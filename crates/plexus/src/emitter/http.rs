//! HTTP REQUEST emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of HTTP REQUEST emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], HttpRequestPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(HttpRequestPublishingMode)
}

fn definition<'src>()
-> impl Parser<'src, &'src [Token], HttpRequestEmitter, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Method))
        .then(embedded_expression("string_expression"))
        .then_ignore(kw(Identifier::Path))
        .then(embedded_expression("string_expression"))
        .then_ignore(kw(Identifier::Mode))
        .then(mode())
        .map(|(((client, method), path), mode)| HttpRequestEmitter {
            client,
            method,
            path,
            mode,
        })
        .boxed()
}
pub(super) fn request_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<HttpRequestEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    let encoded = encoded_destination(context, definition());
    let absent = definition()
        .then_ignore(kw_phrase2(Identifier::Without, Identifier::Body))
        .then(entity_envelope::<HttpRequestEmitter>())
        .then(context.bodyless_construction().or_not())
        .map(
            |((destination, envelope), construction)| ParsedDestination {
                destination,
                body: EmitterBody::WithoutBody,
                envelope,
                construction,
                batch: None,
            },
        );
    choice((encoded, absent)).boxed()
}
