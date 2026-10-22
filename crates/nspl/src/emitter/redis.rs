//! REDIS emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of REDIS emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], RedisPubSubPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    unconfirmed_mode!(RedisPubSubPublishingMode)
}

pub(super) fn pubsub_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<RedisPubSubSink>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Channel))
            .then(external_name::<RedisChannelName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, channel), mode)| RedisPubSubSink {
                client,
                channel,
                mode,
            }),
    )
}
