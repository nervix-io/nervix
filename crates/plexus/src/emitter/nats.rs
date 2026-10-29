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

#[cfg(test)]
mod tests {
    use nervix_models::{
        EmitterEntity, Model, NatsEmitter, NatsSubjectEmitter, NatsSubjectPublishingMode,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    const NO_ACK: &str = "NO ACK RETRY POLICY BACKOFF 250ms MAX 30s";
    const JETSTREAM: &str =
        "JETSTREAM ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s";

    /// A complete `NATS SUBJECT` emitter statement that publishes to `subject` in `mode`.
    fn statement(subject: &str, mode: &str) -> String {
        format!(
            "CREATE NATS SUBJECT EMITTER publishing FROM events TO nats_main SUBJECT '{subject}' \
             MODE {mode} ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The emitter Model the grammar constructs from `statement`.
    fn destination(statement: &str) -> NatsSubjectEmitter {
        let Some(Model::Emitter(emitter)) = created_model(statement) else {
            panic!(
                "a literal publication subject is accepted by the composed grammar: {statement}"
            );
        };
        let EmitterEntity::Nats(NatsEmitter::Subject(destination)) = *emitter.connector else {
            panic!("the NATS SUBJECT head constructs the NATS subject emitter Model");
        };
        destination
    }

    #[rstest]
    #[case("Orders.Created")]
    #[case("$JS.API.STREAM.INFO.Orders")]
    #[case("telemetry.Événements")]
    fn publication_literals_reach_the_emitter_model_unchanged(#[case] subject: &str) {
        for mode in [NO_ACK, JETSTREAM] {
            let destination = destination(&statement(subject, mode));
            assert_eq!(destination.subject.as_str(), subject);
        }
    }

    #[test]
    fn each_declared_publishing_mode_reaches_the_emitter_model() {
        let core = destination(&statement("Orders.Created", NO_ACK));
        let NatsSubjectPublishingMode::NoAck { .. } = core.mode else {
            panic!("MODE NO ACK publishes through Core NATS: {:?}", core.mode);
        };
        let jetstream = destination(&statement("Orders.Created", JETSTREAM));
        let NatsSubjectPublishingMode::JetStream { ack_timeout, .. } = &jetstream.mode else {
            panic!(
                "MODE JETSTREAM ACK waits for JetStream: {:?}",
                jetstream.mode
            );
        };
        assert_eq!(ack_timeout, "5s");
    }

    #[test]
    fn subject_and_jetstream_windows_complete_from_the_subject_declaration() {
        let head = "CREATE NATS SUBJECT EMITTER publishing FROM events TO nats_main SUBJECT ";
        assert_offers_exactly(head, &["nats_publish_subject"]);
        assert_offers_exactly(
            &format!("{head}'Orders.Created' MODE JETSTREAM ACK "),
            &["SEQUENTIAL ACK TIMEOUT 5s", "PARALLEL MAX 8"],
        );
    }

    #[rstest]
    #[case("orders.*")]
    #[case("orders.>")]
    #[case(">")]
    #[case("orders..created")]
    #[case("orders created")]
    #[case("")]
    fn the_composed_grammar_refuses_what_a_publication_subject_cannot_hold(#[case] subject: &str) {
        for mode in [NO_ACK, JETSTREAM] {
            let statement = statement(subject, mode);
            assert!(
                refused(&statement),
                "a publication subject cannot be '{subject}': {statement}"
            );
        }
    }

    #[test]
    fn the_composed_grammar_applies_the_publication_subject_byte_bound() {
        let at_bound = format!("{}.{}", "é".repeat(256), "x".repeat(511));
        assert_eq!(
            destination(&statement(&at_bound, NO_ACK)).subject.as_str(),
            at_bound
        );
        assert!(refused(&statement(&format!("{at_bound}x"), NO_ACK)));
    }
}
