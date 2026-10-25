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
