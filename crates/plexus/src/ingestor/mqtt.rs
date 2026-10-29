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
    IngestQuiesceMode, MqttIngestMode, MqttQos, MqttSession, MqttTopicFilter, MqttTopicIngestor,
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
-> impl Parser<'src, &'src [Token], MqttTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<MqttTopicFilter>())
        .then(optional_instances())
        .then(mode().then_with_ctx(quiesce_clause()))
        .map(
            |(((client, topic), instances), (mode, quiesce))| MqttTopicIngestor {
                client,
                topic,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        IngestorEntity, IngestorInput, Model, MqttIngestMode, MqttIngestor, MqttQos, MqttSession,
        MqttTopicIngestor,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    const ACK_SEQUENTIAL: &str = "SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s \
                                  RETRY POLICY BACKOFF 1s MAX 30s";

    /// A complete `MQTT TOPIC` ingestor statement that subscribes to `filter`, writing
    /// `subscription` (its session, quality of service and delivery mode) before `quiesce`.
    fn statement(filter: &str, subscription: &str, quiesce: &str) -> String {
        format!(
            "CREATE MQTT TOPIC INGESTOR intake FROM mqtt_main TOPIC '{filter}' INSTANCES 2 \
             {subscription} ON QUIESCE {quiesce} DECODE USING event_codec TO events INHERIT ALL \
             UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The ingestor Model the grammar constructs from `statement`.
    fn source(statement: &str) -> MqttTopicIngestor {
        let Some(Model::Ingestor(ingestor)) = created_model(statement) else {
            panic!("the composed grammar accepts the statement: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the MQTT TOPIC head names a transport");
        };
        let IngestorEntity::Mqtt(MqttIngestor::Topic(source)) = input.source else {
            panic!("the MQTT TOPIC head constructs the MQTT topic Model");
        };
        source
    }

    #[rstest]
    #[case("Devices/+/Orders")]
    #[case("telemetry/#")]
    #[case("/")]
    #[case("$SYS/broker/#")]
    #[case("devices/$share/orders")]
    #[case("Sensor Readings/Événements/+")]
    fn topic_filters_reach_the_ingestor_model_unchanged(#[case] filter: &str) {
        let source = source(&statement(filter, "MODE NO ACK", "DROP"));
        assert_eq!(source.topic.as_str(), filter);
        assert_eq!(source.instances.get(), 2);
    }

    #[test]
    fn the_written_session_and_quality_of_service_reach_the_ingestor_model() {
        let defaulted = source(&statement("devices/+", "MODE NO ACK", "DROP"));
        assert_eq!(
            defaulted.mode,
            MqttIngestMode::NoAck {
                session: MqttSession::Clean,
                qos: MqttQos::AtMostOnce,
            }
        );
        let kept = source(&statement(
            "devices/+",
            "SESSION PERSISTENT QOS 1 MODE NO ACK",
            "SUSPEND",
        ));
        assert_eq!(
            kept.mode,
            MqttIngestMode::NoAck {
                session: MqttSession::Persistent,
                qos: MqttQos::AtLeastOnce,
            }
        );
        let acknowledged = source(&statement("devices/+", ACK_SEQUENTIAL, "SUSPEND"));
        let MqttIngestMode::AckSequential { timeout, .. } = &acknowledged.mode else {
            panic!(
                "MODE ACK SEQUENTIAL acknowledges one publish at a time: {:?}",
                acknowledged.mode
            );
        };
        assert_eq!(timeout, "5s");
    }

    #[test]
    fn filter_session_and_quiesce_complete_from_the_topic_declaration() {
        let head = "CREATE MQTT TOPIC INGESTOR intake FROM mqtt_main TOPIC ";
        assert_offers_exactly(head, &["mqtt_topic_filter"]);
        assert_offers_exactly(
            &format!("{head}'devices/+' SESSION "),
            &["CLEAN", "PERSISTENT"],
        );
        assert_offers_exactly(&format!("{head}'devices/+' QOS "), &["mqtt_qos"]);
        assert_offers_exactly(
            &format!("{head}'devices/+' MODE NO ACK ON QUIESCE "),
            &["BUFFER", "DROP"],
        );
        assert_offers_exactly(
            &format!("{head}'devices/+' SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE "),
            &["SUSPEND", "BUFFER", "DROP"],
        );
    }

    #[rstest]
    #[case("")]
    #[case("devices/se+nsor")]
    #[case("devices/orders#")]
    #[case("devices/#/orders")]
    #[case("$share/readers/devices")]
    #[case("$share")]
    fn the_composed_grammar_applies_the_topic_filter_rule(#[case] filter: &str) {
        let statement = statement(filter, "MODE NO ACK", "DROP");
        assert!(
            refused(&statement),
            "the filter rule excludes '{filter}': {statement}"
        );
    }

    #[rstest]
    #[case::ack_in_a_clean_session(
        "SESSION CLEAN QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        "DROP"
    )]
    #[case::ack_at_qos_zero(
        "SESSION PERSISTENT QOS 0 MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s \
         RETRY POLICY BACKOFF 1s MAX 30s",
        "DROP"
    )]
    #[case::qos_two("SESSION PERSISTENT QOS 2 MODE NO ACK", "DROP")]
    #[case::suspend_in_a_clean_session("SESSION CLEAN QOS 1 MODE NO ACK", "SUSPEND")]
    #[case::suspend_at_qos_zero("SESSION PERSISTENT QOS 0 MODE NO ACK", "SUSPEND")]
    fn the_composed_grammar_refuses_what_the_written_session_cannot_honor(
        #[case] subscription: &str,
        #[case] quiesce: &str,
    ) {
        let statement = statement("devices/+", subscription, quiesce);
        assert!(refused(&statement), "{statement}");
    }

    #[test]
    fn the_composed_grammar_applies_the_topic_filter_byte_bound() {
        let at_bound = format!("{}/{}", "é".repeat(16_384), "x".repeat(32_766));
        assert_eq!(at_bound.len(), 65_535);
        assert_eq!(
            source(&statement(&at_bound, "MODE NO ACK", "DROP"))
                .topic
                .as_str(),
            at_bound
        );
        assert!(refused(&statement(
            &format!("{at_bound}x"),
            "MODE NO ACK",
            "DROP"
        )));
    }
}
