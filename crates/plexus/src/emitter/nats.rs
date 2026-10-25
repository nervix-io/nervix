//! NATS emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of NATS emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], NatsSubjectPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        unconfirmed_policy().map(|retry_policy| NatsSubjectPublishingMode::NoAck { retry_policy }),
        kw_phrase2(Identifier::Jetstream, Identifier::Ack)
            .ignore_then(delivery::acknowledged_policy_body())
            .map(
                |AcknowledgedPolicy {
                     window,
                     ack_timeout,
                     retry_policy,
                 }| NatsSubjectPublishingMode::JetStream {
                    window,
                    ack_timeout,
                    retry_policy,
                },
            ),
    ))
    .boxed()
}

pub(super) fn subject_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<NatsSubjectEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Subject))
            .then(external_name::<NatsPublishSubjectName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, subject), mode)| NatsSubjectEmitter {
                client,
                subject,
                mode,
            }),
    )
}
