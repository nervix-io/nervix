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
    use crate::{
        conformance_support::{assert_offers_exactly, created_model},
        parser_support::ParseFromSourceError,
        statement::parse_statement,
    };

    fn statement(mode: &str) -> String {
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
        let source = statement(&format!("QOS {qos}"));
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
            let source = statement(mode);
            assert!(created_model(&source).is_some(), "{source}");
        }
    }
}
