//! RABBITMQ emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of RABBITMQ emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], RabbitMqQueuePublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    broker_mode!(RabbitMqQueuePublishingMode)
}

pub(super) fn queue_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<RabbitMqQueueSink>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Queue))
            .then(external_name::<RabbitMqQueueName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, queue), mode)| RabbitMqQueueSink {
                client,
                queue,
                mode,
            }),
    )
}
