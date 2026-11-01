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
    ChoiceRequirement, IngestQuiesceKind, IngestQuiesceMode, IngestorDeclaration as _,
    IngestorEntityModel as _, MqttIngestMode, MqttQos, MqttSession, MqttTopicFilter,
    MqttTopicIngestor,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    quiesce_buffer_parser, quiesce_drop_parser, quiesce_suspend_parser,
};
use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase2},
};

/// The session and the quality of service a statement writes before its delivery mode.
#[derive(Clone, Copy)]
struct MqttSubscriptionChoices {
    session: MqttSession,
    qos: MqttQos,
}

impl MqttSubscriptionChoices {
    /// The choice these make for the declared option opened by `option`.
    fn chosen(&self, option: &str) -> Option<&'static str> {
        MqttTopicIngestor::subscription_choice(self.session, self.qos, option)
    }
}

/// The session, quality of service and delivery mode a statement writes, in that order.
#[derive(Clone)]
struct MqttSubscription {
    choices: MqttSubscriptionChoices,
    mode: MqttIngestMode,
}

impl MqttSubscription {
    /// The choices the `MQTT TOPIC` declaration requires beside `quiesce`, and whether this
    /// subscription makes them. A quiesce mode the entity does not declare requires nothing here:
    /// the quiesce clause offers only the declared modes.
    fn quiesce_requirement(&self, quiesce: IngestQuiesceKind) -> QuiesceRequirement {
        let Some(declared) = MqttTopicIngestor::CAPABILITIES.declared_quiesce(quiesce) else {
            return QuiesceRequirement {
                requires: &[],
                met: true,
            };
        };
        let choices = self.choices;
        QuiesceRequirement {
            requires: declared.requires,
            met: ChoiceRequirement::all_met(declared.requires, &|option| choices.chosen(option)),
        }
    }
}

/// The choices a quiesce mode requires and whether a subscription makes them.
struct QuiesceRequirement {
    requires: &'static [ChoiceRequirement],
    met: bool,
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
            .labelled("mqtt_qos")
            .try_map(|raw, span| match raw.as_str() {
                "0" => Ok(MqttQos::Qos0),
                "1" => Ok(MqttQos::Qos1),
                "2" => Ok(MqttQos::Qos2),
                _ => Err(Rich::custom(
                    span,
                    format!("MQTT ingestor QOS must be 0, 1 or 2, found {raw}"),
                )),
            }),
    )
}

/// `MODE NO ACK | ACK SEQUENTIAL … | ACK PARALLEL …` after its keyword.
fn delivery<'src>()
-> impl Parser<'src, &'src [Token], MqttIngestMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        ack_parallel().map(
            |ParallelAck {
                 max,
                 batch_timeout,
                 timeout,
                 retry_policy,
             }| MqttIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            },
        ),
        ack_sequential().map(
            |SequentialAck {
                 timeout,
                 retry_policy,
             }| MqttIngestMode::AckSequential {
                timeout,
                retry_policy,
            },
        ),
        no_ack().to(MqttIngestMode::NoAck),
    ))
}

type MqttModeExtra<'src> = extra::Full<ParseError<'src>, (), MqttSubscriptionChoices>;

fn with_choices_context<'src, O, P>(
    parser: P,
) -> impl Parser<'src, &'src [Token], O, MqttModeExtra<'src>> + Clone
where
    P: Parser<'src, &'src [Token], O, extra::Err<ParseError<'src>>> + Clone,
{
    map_ctx::<_, _, _, MqttModeExtra<'src>, extra::Err<ParseError<'src>>, _>(|_| (), parser)
}

/// `MODE …` after the session and quality of service the context holds: a mode the `MQTT TOPIC`
/// declaration requires other choices beside is refused there, naming what the statement writes.
fn checked_delivery<'src>()
-> impl Parser<'src, &'src [Token], MqttIngestMode, MqttModeExtra<'src>> + Clone {
    with_choices_context(kw(Identifier::Mode).ignore_then(delivery())).try_map_with(
        |mode, extra| {
            let choices: MqttSubscriptionChoices = *extra.ctx();
            let delivery = mode.acknowledgement().kind();
            let unmet = MqttTopicIngestor::CAPABILITIES.unmet_delivery(
                MqttTopicIngestor::KIND,
                delivery,
                &|option| choices.chosen(option),
            );
            if let Some(unmet) = unmet {
                return Err(Rich::custom(extra.span(), unmet.to_string()));
            }
            Ok(mode)
        },
    )
}

/// `SESSION CLEAN|PERSISTENT QOS 0|1|2 MODE …`: every statement writes the session and the quality
/// of service it subscribes with, and names a delivery mode only beside the choices the `MQTT
/// TOPIC` declaration requires for it, a persistent session at QoS 1 or 2 for an acknowledged one.
fn subscription<'src>()
-> impl Parser<'src, &'src [Token], MqttSubscription, extra::Err<ParseError<'src>>> + Clone {
    session_clause()
        .then(qos_clause())
        .map(|(session, qos)| MqttSubscriptionChoices { session, qos })
        .then_with_ctx(checked_delivery())
        .map(|(choices, mode)| MqttSubscription { choices, mode })
}

type MqttQuiesceExtra<'src> = extra::Full<ParseError<'src>, (), MqttSubscription>;

fn with_subscription_context<'src, O, P>(
    parser: P,
) -> impl Parser<'src, &'src [Token], O, MqttQuiesceExtra<'src>> + Clone
where
    P: Parser<'src, &'src [Token], O, extra::Err<ParseError<'src>>> + Clone,
{
    map_ctx::<_, _, _, MqttQuiesceExtra<'src>, extra::Err<ParseError<'src>>, _>(|_| (), parser)
}

/// `ON QUIESCE …` after an MQTT subscription: `SUSPEND` only where the broker keeps the
/// subscriber's messages, as the `MQTT TOPIC` declaration requires for it.
fn quiesce_clause<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, MqttQuiesceExtra<'src>> + Clone {
    let suspend = custom::<_, _, _, MqttQuiesceExtra<'src>>(|input| {
        let requirement = input.ctx().quiesce_requirement(IngestQuiesceKind::Suspend);
        let allowed = requirement.met;

        // Diagnose a supplied keyword through the shared parser. With no keyword, keep the
        // contextual gate so completion offers only the modes this session can use.
        if !allowed
            && let Some(Token::Word(Word::KnownWord {
                iden: Identifier::Suspend,
                ..
            })) = input.peek_ref()
        {
            let start = input.cursor();
            input.parse(with_subscription_context(quiesce_suspend_parser()))?;
            return Err(Rich::custom(
                input.span_since(&start),
                format!(
                    "MQTT ON QUIESCE SUSPEND requires {}",
                    ChoiceRequirement::conjunction(requirement.requires)
                ),
            ));
        }

        input.parse(
            with_subscription_context(quiesce_suspend_parser())
                .contextual()
                .configure(move |_, _: &MqttSubscription| allowed),
        )
    });
    let body = choice((
        suspend,
        with_subscription_context(quiesce_buffer_parser()),
        with_subscription_context(quiesce_drop_parser()),
    ));

    with_subscription_context(kw_phrase2(Identifier::On, Identifier::Quiesce)).ignore_then(body)
}

/// `FROM <client> TOPIC '<filter>' [INSTANCES <n>] SESSION … QOS … MODE … ON QUIESCE …`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], MqttTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<MqttTopicFilter>())
        .then(optional_instances())
        .then(subscription().then_with_ctx(quiesce_clause()))
        .map(
            |(
                ((client, topic), instances),
                (
                    MqttSubscription {
                        choices: MqttSubscriptionChoices { session, qos },
                        mode,
                    },
                    quiesce,
                ),
            )| MqttTopicIngestor {
                client,
                topic,
                instances,
                session,
                qos,
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

    use crate::{
        conformance_support::{assert_offers_exactly, created_model, refused},
        parser_support::ParseFromSourceError,
        statement::parse_statement,
    };

    const NO_ACK: &str = "SESSION CLEAN QOS 0 MODE NO ACK";

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

    /// The diagnostics the grammar refuses `source` with.
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

    #[rstest]
    #[case("Devices/+/Orders")]
    #[case("telemetry/#")]
    #[case("/")]
    #[case("$SYS/broker/#")]
    #[case("devices/$share/orders")]
    #[case("Sensor Readings/Événements/+")]
    fn topic_filters_reach_the_ingestor_model_unchanged(#[case] filter: &str) {
        let source = source(&statement(filter, NO_ACK, "DROP"));
        assert_eq!(source.topic.as_str(), filter);
        assert_eq!(source.instances.get(), 2);
    }

    #[rstest]
    #[case("SESSION CLEAN QOS 0", MqttSession::Clean, MqttQos::Qos0)]
    #[case("SESSION CLEAN QOS 1", MqttSession::Clean, MqttQos::Qos1)]
    #[case("SESSION CLEAN QOS 2", MqttSession::Clean, MqttQos::Qos2)]
    #[case("SESSION PERSISTENT QOS 0", MqttSession::Persistent, MqttQos::Qos0)]
    #[case("SESSION PERSISTENT QOS 1", MqttSession::Persistent, MqttQos::Qos1)]
    #[case("SESSION PERSISTENT QOS 2", MqttSession::Persistent, MqttQos::Qos2)]
    fn the_written_session_and_quality_of_service_reach_the_ingestor_model(
        #[case] written: &str,
        #[case] session: MqttSession,
        #[case] qos: MqttQos,
    ) {
        let source = source(&statement(
            "devices/+",
            &format!("{written} MODE NO ACK"),
            "DROP",
        ));
        assert_eq!(source.session, session);
        assert_eq!(source.qos, qos);
        assert_eq!(source.mode, MqttIngestMode::NoAck);
    }

    #[rstest]
    #[case("SESSION PERSISTENT QOS 1", MqttQos::Qos1)]
    #[case("SESSION PERSISTENT QOS 2", MqttQos::Qos2)]
    fn an_acknowledged_mode_keeps_its_window_beside_the_persistent_session(
        #[case] written: &str,
        #[case] qos: MqttQos,
    ) {
        let sequential = source(&statement(
            "devices/+",
            &format!(
                "{written} MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s"
            ),
            "SUSPEND",
        ));
        assert_eq!(sequential.session, MqttSession::Persistent);
        assert_eq!(sequential.qos, qos);
        let MqttIngestMode::AckSequential { timeout, .. } = &sequential.mode else {
            panic!(
                "MODE ACK SEQUENTIAL acknowledges one publish at a time: {:?}",
                sequential.mode
            );
        };
        assert_eq!(timeout, "5s");

        let parallel = source(&statement(
            "devices/+",
            &format!(
                "{written} MODE ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY \
                 POLICY BACKOFF 1s MAX 30s"
            ),
            "DROP",
        ));
        assert_eq!(parallel.qos, qos);
        let MqttIngestMode::AckParallel { max, .. } = &parallel.mode else {
            panic!(
                "MODE ACK PARALLEL acknowledges up to MAX publishes together: {:?}",
                parallel.mode
            );
        };
        assert_eq!(max.get(), 4);
    }

    #[test]
    fn every_pairing_of_session_quality_of_service_and_mode_follows_the_declaration() {
        let modes = [
            ("MODE NO ACK", false),
            (
                "MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
                true,
            ),
            (
                "MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF \
                 1s MAX 30s",
                true,
            ),
        ];
        for session in ["CLEAN", "PERSISTENT"] {
            for qos in ["0", "1", "2"] {
                let kept = session == "PERSISTENT" && qos != "0";
                for (mode, acknowledged) in modes {
                    let text = statement(
                        "devices/+",
                        &format!("SESSION {session} QOS {qos} {mode}"),
                        "DROP",
                    );
                    if acknowledged && !kept {
                        assert!(refused(&text), "{text}");
                    } else {
                        assert!(created_model(&text).is_some(), "{text}");
                    }
                }
            }
        }
    }

    #[test]
    fn filter_session_and_quiesce_complete_from_the_topic_declaration() {
        let head = "CREATE MQTT TOPIC INGESTOR intake FROM mqtt_main TOPIC ";
        assert_offers_exactly(head, &["mqtt_topic_filter"]);
        assert_offers_exactly(
            &format!("{head}'devices/+' "),
            &["INSTANCES 2", "SESSION CLEAN", "SESSION PERSISTENT"],
        );
        assert_offers_exactly(
            &format!("{head}'devices/+' SESSION "),
            &["CLEAN", "PERSISTENT"],
        );
        assert_offers_exactly(&format!("{head}'devices/+' SESSION CLEAN "), &["QOS"]);
        assert_offers_exactly(
            &format!("{head}'devices/+' SESSION CLEAN QOS "),
            &["mqtt_qos"],
        );
        assert_offers_exactly(
            &format!("{head}'devices/+' SESSION CLEAN QOS 2 MODE NO ACK ON QUIESCE "),
            &["BUFFER", "DROP"],
        );
        assert_offers_exactly(
            &format!("{head}'devices/+' SESSION PERSISTENT QOS 2 MODE NO ACK ON QUIESCE "),
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
        let statement = statement(filter, NO_ACK, "DROP");
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
    #[case::qos_three("SESSION PERSISTENT QOS 3 MODE NO ACK", "DROP")]
    #[case::suspend_in_a_clean_session("SESSION CLEAN QOS 1 MODE NO ACK", "SUSPEND")]
    #[case::suspend_at_qos_zero("SESSION PERSISTENT QOS 0 MODE NO ACK", "SUSPEND")]
    #[case::without_session_or_qos("MODE NO ACK", "DROP")]
    #[case::without_session("QOS 0 MODE NO ACK", "DROP")]
    #[case::without_qos("SESSION CLEAN MODE NO ACK", "DROP")]
    #[case::qos_before_session("QOS 0 SESSION CLEAN MODE NO ACK", "DROP")]
    fn the_composed_grammar_refuses_what_the_written_session_cannot_honor(
        #[case] subscription: &str,
        #[case] quiesce: &str,
    ) {
        let statement = statement("devices/+", subscription, quiesce);
        assert!(refused(&statement), "{statement}");
    }

    #[rstest]
    #[case(
        "CLEAN",
        "0",
        "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        "ACK SEQUENTIAL"
    )]
    #[case(
        "CLEAN",
        "2",
        "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        "ACK SEQUENTIAL"
    )]
    #[case(
        "PERSISTENT",
        "0",
        "ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        "ACK PARALLEL"
    )]
    fn an_unkept_acknowledged_pairing_names_both_written_values_and_underlines_its_mode(
        #[case] session: &str,
        #[case] qos: &str,
        #[case] mode: &str,
        #[case] phrase: &str,
    ) {
        let subscription = format!("SESSION {session} QOS {qos} MODE {mode}");
        let source = clauses_statement(&format!("{subscription} ON QUIESCE DROP"));
        let refused_mode = format!("MODE {mode}");
        let expected = format!(
            "MQTT TOPIC ingestors name MODE {phrase} only with SESSION PERSISTENT and QOS 1 or 2; \
             the statement writes SESSION {session} and QOS {qos}"
        );
        let refusals = refusal(&source);
        assert!(refusals.contains(&(expected, refused_mode)), "{refusals:?}");
    }

    #[test]
    fn the_composed_grammar_applies_the_topic_filter_byte_bound() {
        let at_bound = format!("{}/{}", "é".repeat(16_384), "x".repeat(32_766));
        assert_eq!(at_bound.len(), 65_535);
        assert_eq!(
            source(&statement(&at_bound, NO_ACK, "DROP")).topic.as_str(),
            at_bound
        );
        assert!(refused(&statement(&format!("{at_bound}x"), NO_ACK, "DROP")));
    }

    fn clauses_statement(clauses: &str) -> String {
        format!(
            "CREATE MQTT TOPIC INGESTOR incoming FROM broker TOPIC 'events' {clauses} DECODE \
             USING event_codec TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG ON GENERAL ERROR LOG;"
        )
    }

    #[rstest::rstest]
    #[case("3")]
    #[case("10")]
    #[case("256")]
    fn qos_refusal_names_the_value_and_its_exact_source_span(#[case] qos: &str) {
        let source = clauses_statement(&format!(
            "SESSION PERSISTENT QOS {qos} MODE NO ACK ON QUIESCE DROP"
        ));
        let expected = format!("MQTT ingestor QOS must be 0, 1 or 2, found {qos}");
        let refusals = refusal(&source);
        assert!(
            refusals.contains(&(expected, qos.to_owned())),
            "{refusals:?}"
        );
    }

    #[rstest::rstest]
    #[case("SESSION CLEAN QOS 0")]
    #[case("SESSION CLEAN QOS 1")]
    #[case("SESSION CLEAN QOS 2")]
    #[case("SESSION PERSISTENT QOS 0")]
    fn suspend_refusal_names_the_session_requirement_and_source_span(#[case] session: &str) {
        let source = clauses_statement(&format!("{session} MODE NO ACK ON QUIESCE SUSPEND"));
        let expected = "MQTT ON QUIESCE SUSPEND requires SESSION PERSISTENT and QOS 1 or 2";
        let refusals = refusal(&source);
        assert!(
            refusals.contains(&(expected.to_owned(), "SUSPEND".to_owned())),
            "{refusals:?}"
        );
    }

    #[rstest::rstest]
    #[case("SESSION CLEAN QOS 0", &["BUFFER", "DROP"])]
    #[case("SESSION CLEAN QOS 1", &["BUFFER", "DROP"])]
    #[case("SESSION CLEAN QOS 2", &["BUFFER", "DROP"])]
    #[case("SESSION PERSISTENT QOS 0", &["BUFFER", "DROP"])]
    #[case("SESSION PERSISTENT QOS 1", &["SUSPEND", "BUFFER", "DROP"])]
    #[case("SESSION PERSISTENT QOS 2", &["SUSPEND", "BUFFER", "DROP"])]
    fn completion_and_valid_quiesce_modes_follow_the_session(
        #[case] session: &str,
        #[case] modes: &[&str],
    ) {
        let prefix = format!(
            "CREATE MQTT TOPIC INGESTOR incoming FROM broker TOPIC 'events' {session} MODE NO ACK \
             ON QUIESCE "
        );
        assert_offers_exactly(&prefix, modes);
        for mode in modes {
            let clause = match *mode {
                "BUFFER" => "BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST",
                clause => clause,
            };
            let source = clauses_statement(&format!("{session} MODE NO ACK ON QUIESCE {clause}"));
            assert!(created_model(&source).is_some(), "{source}");
        }
    }
}
