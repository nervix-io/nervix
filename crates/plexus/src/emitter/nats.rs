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

pub(super) fn subject_mode<'src>()
-> impl Parser<'src, &'src [Token], NatsSubjectPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    unconfirmed_mode!(NatsSubjectPublishingMode)
}

pub(super) fn stream_mode<'src>()
-> impl Parser<'src, &'src [Token], NatsStreamPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    consumer_mode!(NatsStreamPublishingMode)
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
            .then(subject_mode())
            .map(|((client, subject), mode)| NatsSubjectEmitter {
                client,
                subject,
                mode,
            }),
    )
}

pub(super) fn stream_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<NatsStreamEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Stream))
            .then(external_name::<NatsStreamName>())
            .then_ignore(kw(Identifier::Subject))
            .then(external_name::<NatsPublishSubjectName>())
            .then_ignore(kw(Identifier::Mode))
            .then(stream_mode())
            .map(|(((client, stream), subject), mode)| NatsStreamEmitter {
                client,
                stream,
                subject,
                mode,
            }),
    )
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        EmitterEntity, Model, NatsEmitter, NatsStreamEmitter, NatsStreamPublishingMode,
        NatsSubjectEmitter, NatsSubjectPublishingMode,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    const NO_ACK: &str = "NO ACK RETRY POLICY BACKOFF 250ms MAX 30s";
    const STREAM_ACK: &str = "ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s";

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

    fn stream_destination(subject: &str) -> NatsStreamEmitter {
        let statement = format!(
            "CREATE NATS STREAM EMITTER publishing FROM events TO nats_main STREAM 'orders' \
             SUBJECT '{subject}' MODE {STREAM_ACK} ENCODE USING event_codec MESSAGE ID input.id \
             INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        let Some(Model::Emitter(emitter)) = created_model(&statement) else {
            panic!("the NATS STREAM destination parses: {statement}");
        };
        let EmitterEntity::Nats(NatsEmitter::Stream(destination)) = *emitter.connector else {
            panic!("the NATS STREAM head constructs the stream emitter Model");
        };
        destination
    }

    #[rstest]
    #[case("Orders.Created")]
    #[case("$JS.API.STREAM.INFO.Orders")]
    #[case("telemetry.Événements")]
    fn publication_literals_reach_the_emitter_model_unchanged(#[case] subject: &str) {
        assert_eq!(
            destination(&statement(subject, NO_ACK)).subject.as_str(),
            subject
        );
        assert_eq!(stream_destination(subject).subject.as_str(), subject);
    }

    #[test]
    fn each_declared_publishing_mode_reaches_its_emitter_model() {
        let core = destination(&statement("Orders.Created", NO_ACK));
        assert!(matches!(core.mode, NatsSubjectPublishingMode::NoAck { .. }));
        let stream = stream_destination("Orders.Created");
        let NatsStreamPublishingMode::Ack { ack_timeout, .. } = &stream.mode;
        assert_eq!(stream.stream.as_str(), "orders");
        assert_eq!(ack_timeout, "5s");
    }

    #[test]
    fn subject_and_stream_windows_complete_from_their_declarations() {
        let head = "CREATE NATS SUBJECT EMITTER publishing FROM events TO nats_main SUBJECT ";
        assert_offers_exactly(head, &["nats_publish_subject"]);
        assert_offers_exactly(
            "CREATE NATS STREAM EMITTER publishing FROM events TO nats_main STREAM 'orders' \
             SUBJECT 'Orders.Created' MODE ACK ",
            &["SEQUENTIAL ACK TIMEOUT 5s", "PARALLEL MAX 8"],
        );
    }

    #[test]
    fn a_subject_emitter_keeps_its_reply_expression_in_the_shared_envelope() {
        let text = statement("orders.created", NO_ACK)
            .replace("INHERIT ALL", "REPLY TO input.reply INHERIT ALL");
        let Some(Model::Emitter(emitter)) = created_model(&text) else {
            panic!("the subject emitter declares REPLY TO: {text}");
        };
        assert_eq!(
            emitter.envelope.reply_to,
            Some(crate::parse_expression("input.reply").expect("a field expression parses"))
        );
    }

    #[test]
    fn reply_to_completion_keeps_the_composed_phrase_and_its_expression_context() {
        let prefix = format!(
            "CREATE NATS SUBJECT EMITTER publishing FROM events TO nats_main SUBJECT \
             'orders.created' MODE {NO_ACK} ENCODE USING event_codec "
        );
        assert_offers_exactly(
            &prefix,
            &[
                "REPLY TO 'replies'",
                "INHERIT ALL",
                "SET value = 1",
                "WHERE true",
                "INVOKE write_header('trace', 'one')",
                "BATCH MAX MESSAGES 2 MAX SIZE 1KiB",
                "FLUSH EACH 1s",
                "FLUSH IMMEDIATE",
            ],
        );
        assert_offers_exactly(&format!("{prefix}REPLY TO "), &["string_expression"]);
    }

    #[rstest]
    #[case("REPLY TO")]
    #[case("REPLY TO 'first' REPLY TO 'second'")]
    fn the_reply_clause_requires_one_expression(#[case] clause: &str) {
        assert!(refused(
            &statement("orders.created", NO_ACK)
                .replace("INHERIT ALL", &format!("{clause} INHERIT ALL"))
        ));
    }

    #[rstest]
    #[case("orders.*")]
    #[case("orders.>")]
    #[case(">")]
    #[case("orders..created")]
    #[case("orders created")]
    #[case("")]
    fn the_composed_grammar_refuses_what_a_publication_subject_cannot_hold(#[case] subject: &str) {
        let statement = statement(subject, NO_ACK);
        assert!(
            refused(&statement),
            "a publication subject cannot be '{subject}': {statement}"
        );
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
