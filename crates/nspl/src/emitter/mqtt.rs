//! MQTT emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of MQTT emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

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
                .try_map(move |raw, span| {
                    if raw == expected {
                        Ok(())
                    } else {
                        Err(Rich::custom(span, "MQTT emitter QOS must be 0, 1, or 2"))
                    }
                })
                .labelled("mqtt_qos"),
        )
        .boxed()
}

pub(super) fn mqtt_confirming_publishing_mode<'src>(
    qos: &'static str,
) -> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    mqtt_qos_level(qos)
        .ignore_then(kw(Identifier::Ack))
        .ignore_then(ack_window())
        .then(ack_timeout())
        .then(retry_policy())
        .map(move |((window, ack_timeout), retry_policy)| match qos {
            "1" => EmitterPublishingMode::MqttQos1 {
                window,
                ack_timeout,
                retry_policy,
            },
            "2" => EmitterPublishingMode::MqttQos2 {
                window,
                ack_timeout,
                retry_policy,
            },
            _ => unreachable!("confirming MQTT publishing mode must use QOS 1 or 2"),
        })
        .boxed()
}

pub(super) fn mqtt_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        mqtt_qos_level("0")
            .ignore_then(retry_policy())
            .map(|retry_policy| EmitterPublishingMode::MqttQos0 { retry_policy }),
        mqtt_confirming_publishing_mode("1"),
        mqtt_confirming_publishing_mode("2"),
    )
}

pub(super) fn mqtt_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mqtt)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .map(|(client, topic)| EmitSink::Mqtt(MqttSink::Topic(MqttTopicSink { client, topic })))
}
