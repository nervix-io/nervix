//! The MQTT connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of an MQTT ingestor source: its client, topic filter, instances, session,
//!   quality of service, delivery mode, and the quiesce modes that session admits.
//! - **Depends on.** The shared ingestor clauses and the MQTT vocabulary.
//! - **Must not know.** The MQTT driver or how a session is resumed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::num::NonZeroU64;

use chumsky::prelude::*;
use nervix_models::{
    IngestQuiesceMode, IngestSource, MqttIngestMode, MqttQos, MqttSession, MqttSource,
    MqttTopicSource, RetryPolicy,
};

use super::delivery::{
    batch_timeout_parser, instance_count, quiesce_buffer_parser, quiesce_drop_parser,
    quiesce_suspend_parser,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ack_timeout, client_ref, kw, kw_phrase2, mqtt_topic_filter, parallel_ack_window,
        retry_policy, sequential_ack_window,
    },
};

#[derive(Clone)]
pub(super) enum ParsedMqttIngestMode {
    NoAckSequential,
    NoAckParallel,
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
    AckParallel {
        max: NonZeroU64,
        batch_timeout: String,
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

pub(super) fn mqtt_session_clause<'src>()
-> impl Parser<'src, &'src [Token], MqttSession, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Session).ignore_then(choice((
        kw(Identifier::Clean).to(MqttSession::Clean),
        kw(Identifier::Persistent).to(MqttSession::Persistent),
    )))
}

pub(super) fn mqtt_qos_clause<'src>()
-> impl Parser<'src, &'src [Token], MqttQos, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Qos).ignore_then(
        select! { Token::NumberLiteral(raw) => raw }
            .try_map(|raw, span| match raw.as_str() {
                "0" => Ok(MqttQos::AtMostOnce),
                "1" => Ok(MqttQos::AtLeastOnce),
                _ => Err(Rich::custom(span, "MQTT QOS must be 0 or 1")),
            })
            .labelled("mqtt_qos"),
    )
}

pub(super) fn mqtt_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], MqttIngestMode, extra::Err<ParseError<'src>>> + Clone {
    let ack_timeout = ack_timeout();
    let batch_timeout = batch_timeout_parser();
    let retry_policy = retry_policy();

    let raw_mode = choice((
        kw(Identifier::Ack)
            .ignore_then(parallel_ack_window())
            .then(batch_timeout)
            .then(ack_timeout.clone())
            .then(retry_policy.clone())
            .map(|(((max, batch_timeout), timeout), retry_policy)| {
                ParsedMqttIngestMode::AckParallel {
                    max,
                    batch_timeout,
                    timeout,
                    retry_policy,
                }
            }),
        kw(Identifier::Ack)
            .ignore_then(sequential_ack_window())
            .ignore_then(ack_timeout)
            .then(retry_policy)
            .map(
                |(timeout, retry_policy)| ParsedMqttIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                },
            ),
        kw(Identifier::NoAck)
            .ignore_then(kw(Identifier::Parallel))
            .to(ParsedMqttIngestMode::NoAckParallel),
        kw(Identifier::NoAck)
            .ignore_then(sequential_ack_window())
            .to(ParsedMqttIngestMode::NoAckSequential),
    ));

    mqtt_session_clause()
        .or_not()
        .map(|session| session.unwrap_or(MqttSession::Clean))
        .then(
            mqtt_qos_clause()
                .or_not()
                .map(|qos| qos.unwrap_or(MqttQos::AtMostOnce)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(raw_mode)
        .try_map(|((session, qos), mode), span| match mode {
            ParsedMqttIngestMode::NoAckSequential => {
                Ok(MqttIngestMode::NoAckSequential { session, qos })
            }
            ParsedMqttIngestMode::NoAckParallel => {
                Ok(MqttIngestMode::NoAckParallel { session, qos })
            }
            ParsedMqttIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => {
                if session != MqttSession::Persistent {
                    return Err(Rich::custom(
                        span,
                        "MQTT ACK modes require SESSION PERSISTENT",
                    ));
                }
                if qos != MqttQos::AtLeastOnce {
                    return Err(Rich::custom(span, "MQTT ACK modes require QOS 1"));
                }
                Ok(MqttIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                })
            }
            ParsedMqttIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => {
                if session != MqttSession::Persistent {
                    return Err(Rich::custom(
                        span,
                        "MQTT ACK modes require SESSION PERSISTENT",
                    ));
                }
                if qos != MqttQos::AtLeastOnce {
                    return Err(Rich::custom(span, "MQTT ACK modes require QOS 1"));
                }
                Ok(MqttIngestMode::AckParallel {
                    max,
                    batch_timeout,
                    timeout,
                    retry_policy,
                })
            }
        })
}

pub(super) type MqttQuiesceExtra<'src> = extra::Full<ParseError<'src>, (), MqttIngestMode>;

pub(super) fn with_mqtt_mode_context<'src, O, P>(
    parser: P,
) -> impl Parser<'src, &'src [Token], O, MqttQuiesceExtra<'src>> + Clone
where
    P: Parser<'src, &'src [Token], O, extra::Err<ParseError<'src>>> + Clone,
{
    map_ctx::<_, _, _, MqttQuiesceExtra<'src>, extra::Err<ParseError<'src>>, _>(|_| (), parser)
}

pub(super) fn mqtt_quiesce_clause<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, MqttQuiesceExtra<'src>> + Clone {
    let suspend = with_mqtt_mode_context(quiesce_suspend_parser())
        .contextual()
        .configure(|_, mode: &MqttIngestMode| {
            mode.session() == MqttSession::Persistent && mode.qos() == MqttQos::AtLeastOnce
        });
    let body = choice((
        suspend,
        with_mqtt_mode_context(quiesce_buffer_parser()),
        with_mqtt_mode_context(quiesce_drop_parser()),
    ));

    with_mqtt_mode_context(kw_phrase2(Identifier::On, Identifier::Quiesce)).ignore_then(body)
}

pub(super) fn mqtt_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mqtt)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(mqtt_topic_filter())
        .then(
            kw(Identifier::Instances)
                .ignore_then(instance_count())
                .or_not()
                .map(|instances| instances.unwrap_or(NonZeroU64::MIN)),
        )
        .then(mqtt_mode_parser().then_with_ctx(mqtt_quiesce_clause()))
        .map(|(((client, topic), instances), (mode, quiesce))| {
            IngestSource::Mqtt(MqttSource::Topic(MqttTopicSource {
                client,
                topic,
                instances,
                mode,
                quiesce,
            }))
        })
}
