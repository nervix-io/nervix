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

pub(super) fn mqtt_qos_level<'src>(
    expected: &'static str,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Qos)
        .ignore_then(
            select! { Token::NumberLiteral(raw) => raw }
                .labelled("mqtt_qos")
                .try_map(move |raw, span| {
                    if raw == expected {
                        Ok(())
                    } else {
                        Err(Rich::custom(
                            span,
                            format!("MQTT emitter QOS must be 0, 1, or 2, found {raw}"),
                        ))
                    }
                }),
        )
        .boxed()
}
pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], MqttTopicPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        mqtt_qos_level("0")
            .ignore_then(retry_policy())
            .map(|retry_policy| MqttTopicPublishingMode::Qos0 { retry_policy }),
        mqtt_qos_level("1").ignore_then(acknowledged_policy()).map(
            |AcknowledgedPolicy {
                 window,
                 ack_timeout,
                 retry_policy,
             }| MqttTopicPublishingMode::Qos1 {
                window,
                ack_timeout,
                retry_policy,
            },
        ),
        mqtt_qos_level("2").ignore_then(acknowledged_policy()).map(
            |AcknowledgedPolicy {
                 window,
                 ack_timeout,
                 retry_policy,
             }| MqttTopicPublishingMode::Qos2 {
                window,
                ack_timeout,
                retry_policy,
            },
        ),
    ))
    .boxed()
}

pub(super) fn topic_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<MqttTopicEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Topic))
            .then(external_name::<MqttTopicName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, topic), mode)| MqttTopicEmitter {
                client,
                topic,
                mode,
            }),
    )
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        EmitterEntity, Model, MqttEmitter, MqttTopicEmitter, MqttTopicPublishingMode,
    };
    use rstest::rstest;

    use crate::{
        conformance_support::{assert_offers_exactly, created_model, refused},
        parser_support::ParseFromSourceError,
        statement::parse_statement,
    };

    const QOS_0: &str = "QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s";
    const QOS_1: &str =
        "QOS 1 ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s";
    const QOS_2: &str = "QOS 2 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s";

    /// A complete `MQTT TOPIC` emitter statement that publishes to `topic` in `mode`.
    fn statement(topic: &str, mode: &str) -> String {
        format!(
            "CREATE MQTT TOPIC EMITTER publishing FROM events TO mqtt_main TOPIC '{topic}' MODE \
             {mode} ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
             GENERAL ERROR LOG;"
        )
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
    fn each_quality_of_service_reaches_the_emitter_model_as_its_publishing_mode() {
        let at_most_once = destination(&statement("devices/orders", QOS_0));
        let MqttTopicPublishingMode::Qos0 { .. } = at_most_once.mode else {
            panic!("MODE QOS 0 publishes at most once: {:?}", at_most_once.mode);
        };
        let at_least_once = destination(&statement("devices/orders", QOS_1));
        let MqttTopicPublishingMode::Qos1 { ack_timeout, .. } = &at_least_once.mode else {
            panic!("MODE QOS 1 ACK waits for PUBACK: {:?}", at_least_once.mode);
        };
        assert_eq!(ack_timeout, "5s");
        let exactly_once = destination(&statement("devices/orders", QOS_2));
        let MqttTopicPublishingMode::Qos2 { ack_timeout, .. } = &exactly_once.mode else {
            panic!(
                "MODE QOS 2 ACK waits for the exactly-once handshake: {:?}",
                exactly_once.mode
            );
        };
        assert_eq!(ack_timeout, "5s");
    }

    #[test]
    fn topic_and_quality_of_service_complete_from_the_topic_declaration() {
        let head = "CREATE MQTT TOPIC EMITTER publishing FROM events TO mqtt_main TOPIC ";
        assert_offers_exactly(head, &["mqtt_topic"]);
        assert_offers_exactly(&format!("{head}'devices/orders' MODE "), &["QOS"]);
        assert_offers_exactly(&format!("{head}'devices/orders' MODE QOS "), &["mqtt_qos"]);
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
    #[case::qos_three("QOS 3 RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case::unacknowledged_qos_one("QOS 1 RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case::unacknowledged_qos_two("QOS 2 RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case::acknowledged_qos_zero(
        "QOS 0 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s"
    )]
    fn the_composed_grammar_pairs_each_quality_of_service_with_its_acknowledgement(
        #[case] mode: &str,
    ) {
        let statement = statement("devices/orders", mode);
        assert!(refused(&statement), "{statement}");
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

    fn mode_statement(mode: &str) -> String {
        format!(
            "CREATE MQTT TOPIC EMITTER outgoing FROM events TO broker TOPIC 'events' MODE {mode} \
             RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec FLUSH IMMEDIATE ON MESSAGE \
             ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    #[rstest::rstest]
    #[case("3")]
    #[case("256")]
    fn qos_refusal_names_the_value_and_its_exact_source_span(#[case] qos: &str) {
        let source = mode_statement(&format!("QOS {qos}"));
        let error = parse_statement(&source).expect_err("the emitter accepts only QoS 0, 1 or 2");
        let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
            panic!("expected a parse refusal, got {error:?}");
        };
        let expected = format!("MQTT emitter QOS must be 0, 1, or 2, found {qos}");
        assert!(
            diagnostics.iter().any(|diagnostic| {
                diagnostic.message == expected && &source[diagnostic.span.clone()] == qos
            }),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn qos_completion_and_supported_publication_modes_remain_available() {
        let prefix =
            "CREATE MQTT TOPIC EMITTER outgoing FROM events TO broker TOPIC 'events' MODE QOS ";
        assert_offers_exactly(prefix, &["mqtt_qos"]);
        for mode in [
            "QOS 0",
            "QOS 1 ACK SEQUENTIAL ACK TIMEOUT 1s",
            "QOS 2 ACK PARALLEL MAX 2 ACK TIMEOUT 1s",
        ] {
            let source = mode_statement(mode);
            assert!(created_model(&source).is_some(), "{source}");
        }
    }
}
