//! CLIENT CONSUMER emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of CLIENT CONSUMER emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], ClientConsumerPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    consumer_mode!(ClientConsumerPublishingMode)
}

pub(super) fn consumer_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<ClientConsumerEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    kw(Identifier::Schema)
        .ignore_then(schema_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(mode())
        .then(entity_envelope::<ClientConsumerEmitter>())
        .then(context.construction().or_not())
        .map(
            |(((schema, mode), envelope), construction)| ParsedDestination {
                destination: ClientConsumerEmitter { schema, mode },
                body: EmitterBody::Client,
                envelope,
                construction,
                batch: None,
            },
        )
        .boxed()
}
