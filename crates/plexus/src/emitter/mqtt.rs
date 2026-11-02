//! MQTT emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of MQTT emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], MqttTopicPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    broker_mode!(MqttTopicPublishingMode)
}

/// The publishing mode a statement writes and the span of its `MODE` clause, which a refused
/// pairing underlines.
struct WrittenMode {
    mode: MqttTopicPublishingMode,
    span: SimpleSpan,
}

/// `MODE …` after its quality of service.
fn written_mode<'src>()
-> impl Parser<'src, &'src [Token], WrittenMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mode)
        .ignore_then(mode())
        .map_with(|mode, extra| WrittenMode {
            mode,
            span: extra.span(),
        })
}

/// `<client> TOPIC '<topic>' QOS 0|1|2 MODE …`: every statement writes the quality of service it
/// publishes at, and names a publishing mode only beside the choices the `MQTT TOPIC` declaration
/// requires for it, QoS 0 for `NO ACK` and QoS 1 or 2 for `ACK`.
fn topic_destination<'src>()
-> impl Parser<'src, &'src [Token], MqttTopicEmitter, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<MqttTopicName>())
        .then(mqtt_qos_clause())
        .then(written_mode())
        .try_map(|(((client, topic), qos), written), _| {
            let WrittenMode { mode, span } = written;
            let destination = MqttTopicEmitter {
                client,
                topic,
                qos,
                mode,
            };
            match destination.unmet_publishing() {
                Some(unmet) => Err(Rich::custom(span, unmet.to_string())),
                None => Ok(destination),
            }
        })
}

pub(super) fn topic_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<MqttTopicEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(context, topic_destination())
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        EmitterEntity, Model, MqttEmitter, MqttQos, MqttTopicEmitter, MqttTopicPublishingMode,
    };
    use rstest::rstest;

    use crate::{
        conformance_support::{assert_offers_exactly, created_model, refused},
        parser_support::ParseFromSourceError,
        statement::parse_statement,
    };

    const QOS_0: &str = "QOS 0 MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s";
    const QOS_1: &str =
        "QOS 1 MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s";
    const QOS_2: &str =
        "QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s";

    /// A complete `MQTT TOPIC` emitter statement that publishes to `topic` as `publication` says:
    /// its quality of service and its publishing mode.
    fn statement(topic: &str, publication: &str) -> String {
        format!(
            "CREATE MQTT TOPIC EMITTER publishing FROM events TO mqtt_main TOPIC '{topic}' \
             {publication} ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG ON GENERAL ERROR LOG;"
        )
    }

    /// What the grammar says of `source` and the source text each diagnostic underlines.
    fn refusal(source: &str) -> Vec<(String, String)> {
        let error = parse_statement(source).expect_err("the grammar refuses the statement");
        let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
            panic!("expected a parse refusal, got {error:?}");
        };
        let mut refusals = Vec::new();
        for diagnostic in diagnostics {
            refusals.push((
                diagnostic.message.clone(),
                source[diagnostic.span.clone()].to_owned(),
            ));
        }
        refusals
    }

    /// The emitter Model the grammar constructs from `statement`.
    fn destination(statement: &str) -> MqttTopicEmitter {
        let Some(Model::Emitter(emitter)) = created_model(statement) else {
            panic!("the composed grammar accepts the statement: {statement}");
        };
        let EmitterEntity::Mqtt(MqttEmitter::Topic(destination)) = *emitter.connector else {
            panic!("the MQTT TOPIC head constructs the MQTT topic emitter Model");
        };
        destination
    }

    #[rstest]
    #[case("Devices/EU/Orders")]
    #[case("sensors/Raw Topic")]
    #[case("devices//orders")]
    #[case("$SYS/broker/load")]
    #[case("telemetry/Événements")]
    fn publication_topics_reach_the_emitter_model_unchanged(#[case] topic: &str) {
        for mode in [QOS_0, QOS_1, QOS_2] {
            let destination = destination(&statement(topic, mode));
            assert_eq!(destination.topic.as_str(), topic);
        }
    }

    #[test]
    fn each_quality_of_service_reaches_the_emitter_model_beside_its_publishing_mode() {
        let at_most_once = destination(&statement("devices/orders", QOS_0));
        assert_eq!(at_most_once.qos, MqttQos::Qos0);
        let MqttTopicPublishingMode::NoAck { .. } = at_most_once.mode else {
            panic!("QOS 0 publishes in NO ACK: {:?}", at_most_once.mode);
        };
        let at_least_once = destination(&statement("devices/orders", QOS_1));
        assert_eq!(at_least_once.qos, MqttQos::Qos1);
        let MqttTopicPublishingMode::Ack { ack_timeout, .. } = &at_least_once.mode else {
            panic!("QOS 1 waits for PUBACK: {:?}", at_least_once.mode);
        };
        assert_eq!(ack_timeout, "5s");
        let exactly_once = destination(&statement("devices/orders", QOS_2));
        assert_eq!(exactly_once.qos, MqttQos::Qos2);
        let MqttTopicPublishingMode::Ack { ack_timeout, .. } = &exactly_once.mode else {
            panic!(
                "QOS 2 waits for the exactly-once handshake: {:?}",
                exactly_once.mode
            );
        };
        assert_eq!(ack_timeout, "5s");
    }

    #[test]
    fn topic_quality_of_service_and_mode_complete_from_the_topic_declaration() {
        let head = "CREATE MQTT TOPIC EMITTER publishing FROM events TO mqtt_main TOPIC ";
        assert_offers_exactly(head, &["mqtt_topic"]);
        assert_offers_exactly(&format!("{head}'devices/orders' "), &["QOS"]);
        assert_offers_exactly(&format!("{head}'devices/orders' QOS "), &["mqtt_qos"]);
        assert_offers_exactly(&format!("{head}'devices/orders' QOS 1 "), &["MODE"]);
        assert_offers_exactly(
            &format!("{head}'devices/orders' QOS 1 MODE "),
            &["ACK", "NO ACK"],
        );
    }

    #[rstest]
    #[case("devices/+")]
    #[case("devices/#")]
    #[case("#")]
    #[case("")]
    fn the_composed_grammar_refuses_what_a_publication_topic_cannot_hold(#[case] topic: &str) {
        for mode in [QOS_0, QOS_1, QOS_2] {
            let statement = statement(topic, mode);
            assert!(
                refused(&statement),
                "a publication topic cannot be '{topic}': {statement}"
            );
        }
    }

    #[rstest]
    #[case::qos_one("1", "NO ACK RETRY POLICY BACKOFF 1s MAX 30s", "NO ACK", "QOS 0")]
    #[case::qos_two("2", "NO ACK RETRY POLICY BACKOFF 1s MAX 30s", "NO ACK", "QOS 0")]
    #[case::sequential(
        "0",
        "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        "ACK",
        "QOS 1 or 2"
    )]
    #[case::parallel(
        "0",
        "ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        "ACK",
        "QOS 1 or 2"
    )]
    fn an_unpaired_mode_names_the_written_quality_of_service_and_underlines_its_mode(
        #[case] qos: &str,
        #[case] mode: &str,
        #[case] phrase: &str,
        #[case] required: &str,
    ) {
        let source = statement("devices/orders", &format!("QOS {qos} MODE {mode}"));
        let expected = format!(
            "MQTT TOPIC emitters name MODE {phrase} only with {required}; the statement writes \
             QOS {qos}"
        );
        let refusals = refusal(&source);
        assert!(
            refusals.contains(&(expected, format!("MODE {mode}"))),
            "{refusals:?}"
        );
    }

    #[test]
    fn the_composed_grammar_applies_the_publication_topic_byte_bound() {
        let at_bound = format!("{}/{}", "é".repeat(16_384), "x".repeat(32_766));
        assert_eq!(at_bound.len(), 65_535);
        assert_eq!(
            destination(&statement(&at_bound, QOS_0)).topic.as_str(),
            at_bound
        );
        assert!(refused(&statement(&format!("{at_bound}x"), QOS_0)));
    }

    #[rstest::rstest]
    #[case("3")]
    #[case("256")]
    fn qos_refusal_names_the_value_and_its_exact_source_span(#[case] qos: &str) {
        let source = statement(
            "events",
            &format!("QOS {qos} MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s"),
        );
        let expected = format!("MQTT QOS must be 0, 1 or 2, found {qos}");
        let refusals = refusal(&source);
        assert!(
            refusals.contains(&(expected, qos.to_owned())),
            "{refusals:?}"
        );
    }

    #[rstest::rstest]
    #[case("QOS 0 MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s")]
    #[case("QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s")]
    #[case("QOS 1 MODE ACK PARALLEL MAX 2 ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s")]
    #[case("QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s")]
    #[case("QOS 2 MODE ACK PARALLEL MAX 2 ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s")]
    fn every_legal_pairing_parses(#[case] publication: &str) {
        let source = statement("events", publication);
        assert!(created_model(&source).is_some(), "{source}");
    }
}
