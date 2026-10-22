//! NATS emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of NATS emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn nats_jetstream_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Jetstream, Identifier::Ack)
        .ignore_then(ack_window())
        .then(ack_timeout())
        .then(retry_policy())
        .map(
            |((window, ack_timeout), retry_policy)| EmitterPublishingMode::NatsJetStream {
                window,
                ack_timeout,
                retry_policy,
            },
        )
        .boxed()
}

pub(super) fn nats_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((no_ack_publishing_mode(), nats_jetstream_publishing_mode())).boxed()
}

pub(super) fn nats_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Nats)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Subject))
        .then(subject_ref())
        .map(|(client, subject)| {
            EmitSink::Nats(NatsSink::Subject(NatsSubjectSink { client, subject }))
        })
}
