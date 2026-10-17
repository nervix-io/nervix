//! The MQTT connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of an `MQTT TOPIC` ingestor: its client, topic filter, instances,
//!   session, quality of service, delivery mode, and the quiesce modes that session admits.
//! - **Depends on.** The shared ingestor clauses and the MQTT vocabulary.
//! - **Must not know.** The MQTT driver or how a session is resumed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    IngestQuiesceMode, MqttIngestMode, MqttQos, MqttSession, MqttTopicFilter, MqttTopicSource,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    quiesce_buffer_parser, quiesce_drop_parser, quiesce_suspend_parser,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase2},
};

/// A delivery mode as written, before its session and quality of service are checked against it.
#[derive(Clone)]
enum ParsedMqttIngestMode {
    NoAck,
    AckSequential(SequentialAck),
    AckParallel(ParallelAck),
}

fn session_clause<'src>()
-> impl Parser<'src, &'src [Token], MqttSession, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Session).ignore_then(choice((
        kw(Identifier::Clean).to(MqttSession::Clean),
        kw(Identifier::Persistent).to(MqttSession::Persistent),
    )))
}

fn qos_clause<'src>()
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

/// `[SESSION CLEAN|PERSISTENT] [QOS 0|1] MODE …`. An acknowledged mode needs a persistent session
/// at QOS 1, so it is refused with any other.
fn mode<'src>()
-> impl Parser<'src, &'src [Token], MqttIngestMode, extra::Err<ParseError<'src>>> + Clone {
    let raw_mode = choice((
        ack_parallel().map(ParsedMqttIngestMode::AckParallel),
        ack_sequential().map(ParsedMqttIngestMode::AckSequential),
        no_ack().to(ParsedMqttIngestMode::NoAck),
    ));

    session_clause()
        .or_not()
        .map(|session| session.unwrap_or(MqttSession::Clean))
        .then(
            qos_clause()
                .or_not()
                .map(|qos| qos.unwrap_or(MqttQos::AtMostOnce)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(raw_mode)
        .try_map(|((session, qos), mode), span| {
            let acknowledged = match mode {
                ParsedMqttIngestMode::NoAck => {
                    return Ok(MqttIngestMode::NoAck { session, qos });
                }
                ParsedMqttIngestMode::AckSequential(SequentialAck {
                    timeout,
                    retry_policy,
                }) => MqttIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                },
                ParsedMqttIngestMode::AckParallel(ParallelAck {
                    max,
                    batch_timeout,
                    timeout,
                    retry_policy,
                }) => MqttIngestMode::AckParallel {
                    max,
                    batch_timeout,
                    timeout,
                    retry_policy,
                },
            };
            if session != MqttSession::Persistent {
                return Err(Rich::custom(
                    span,
                    "MQTT ACK modes require SESSION PERSISTENT",
                ));
            }
            if qos != MqttQos::AtLeastOnce {
                return Err(Rich::custom(span, "MQTT ACK modes require QOS 1"));
            }
            Ok(acknowledged)
        })
}

type MqttQuiesceExtra<'src> = extra::Full<ParseError<'src>, (), MqttIngestMode>;

fn with_mode_context<'src, O, P>(
    parser: P,
) -> impl Parser<'src, &'src [Token], O, MqttQuiesceExtra<'src>> + Clone
where
    P: Parser<'src, &'src [Token], O, extra::Err<ParseError<'src>>> + Clone,
{
    map_ctx::<_, _, _, MqttQuiesceExtra<'src>, extra::Err<ParseError<'src>>, _>(|_| (), parser)
}

/// `ON QUIESCE …` after an MQTT mode: `SUSPEND` only where the broker keeps the subscriber's
/// messages, a persistent session at QOS 1.
fn quiesce_clause<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, MqttQuiesceExtra<'src>> + Clone {
    let suspend = with_mode_context(quiesce_suspend_parser())
        .contextual()
        .configure(|_, mode: &MqttIngestMode| {
            mode.session() == MqttSession::Persistent && mode.qos() == MqttQos::AtLeastOnce
        });
    let body = choice((
        suspend,
        with_mode_context(quiesce_buffer_parser()),
        with_mode_context(quiesce_drop_parser()),
    ));

    with_mode_context(kw_phrase2(Identifier::On, Identifier::Quiesce)).ignore_then(body)
}

/// `FROM <client> TOPIC '<filter>' [INSTANCES <n>] [SESSION …] [QOS …] MODE … ON QUIESCE …`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], MqttTopicSource, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<MqttTopicFilter>())
        .then(optional_instances())
        .then(mode().then_with_ctx(quiesce_clause()))
        .map(
            |(((client, topic), instances), (mode, quiesce))| MqttTopicSource {
                client,
                topic,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}
