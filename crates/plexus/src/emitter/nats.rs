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

/// One alteration to a NATS SUBJECT emitter's destination body.
pub(super) fn subject_change<'src>()
-> impl Parser<'src, &'src [Token], NatsSubjectEmitterChange, extra::Err<ParseError<'src>>> + Clone
{
    let subject = kw(Identifier::Subject)
        .ignore_then(external_name::<NatsPublishSubjectName>())
        .map(NatsSubjectEmitterChange::Subject);
    let publishing = kw(Identifier::Mode)
        .ignore_then(subject_mode())
        .map(NatsSubjectEmitterChange::Mode);
    kw(Identifier::Set)
        .ignore_then(choice((subject, publishing)))
        .boxed()
}

/// One alteration to a NATS STREAM emitter's destination body.
pub(super) fn stream_change<'src>()
-> impl Parser<'src, &'src [Token], NatsStreamEmitterChange, extra::Err<ParseError<'src>>> + Clone {
    let stream = kw(Identifier::Stream)
        .ignore_then(external_name::<NatsStreamName>())
        .map(NatsStreamEmitterChange::Stream);
    let subject = kw(Identifier::Subject)
        .ignore_then(external_name::<NatsPublishSubjectName>())
        .map(NatsStreamEmitterChange::Subject);
    let publishing = kw(Identifier::Mode)
        .ignore_then(stream_mode())
        .map(NatsStreamEmitterChange::Mode);
    kw(Identifier::Set)
        .ignore_then(choice((stream, subject, publishing)))
        .boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        EmitterEntity, Model, NatsEmitter, NatsStreamEmitter, NatsStreamPublishingMode,
        NatsSubjectEmitter, NatsSubjectPublishingMode,
    };
    use rstest::rstest;

    use crate::{
        conformance_support::{assert_offers_exactly, created_model, refused},
        emitter::parse_alter_emitter,
    };

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
    fn stream_destination_changes_round_trip() {
        let source = "ALTER NATS STREAM EMITTER publishing SET STREAM 'more', SET SUBJECT \
                      'orders.more', SET MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF \
                      250ms MAX 30s;";
        let parsed = parse_alter_emitter(source).expect("stream destination changes should parse");
        assert_eq!(parsed.operations.len(), 3);
        let canonical = parsed
            .to_canonical_plexus()
            .expect("stream destination changes should render");
        assert_eq!(
            parse_alter_emitter(&canonical).expect("canonical stream changes should parse"),
            parsed
        );
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
