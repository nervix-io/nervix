//! The Pulsar connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `PULSAR TOPIC` ingestor: its client, topic, subscription and who
//!   owns it, instances, delivery mode and quiesce mode, and the delivery modes a Pulsar
//!   subscription offers.
//! - **Depends on.** The shared ingestor clauses and the Pulsar vocabulary.
//! - **Must not know.** The Pulsar driver or how a subscription is acknowledged, verified or
//!   removed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    PulsarIngestMode, PulsarStartPosition, PulsarSubscription, PulsarSubscriptionName,
    PulsarSubscriptionOwnership, PulsarTopicIngestor, PulsarTopicIngestorOperation,
    PulsarTopicName,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase2, kw_phrase3},
};

/// `FROM <client> TOPIC '<topic>' SUBSCRIPTION '<subscription>'
/// (OWNED BY INGESTOR START AT EARLIEST|LATEST)|PROVISIONED
/// [INSTANCES <n>] MODE … ON QUIESCE SUSPEND`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], PulsarTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<PulsarTopicName>())
        .then(subscription())
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(mode())
        .then(suspend_only_quiesce_clause())
        .map(
            |(((((client, topic), subscription), instances), mode), quiesce)| PulsarTopicIngestor {
                client,
                topic,
                subscription,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}

/// The delivery modes of a Pulsar subscription: `NO ACK`, `ACK SEQUENTIAL …` or `ACK PARALLEL …`.
fn mode<'src>()
-> impl Parser<'src, &'src [Token], PulsarIngestMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        ack_parallel().map(
            |ParallelAck {
                 max,
                 batch_timeout,
                 timeout,
                 retry_policy,
             }| PulsarIngestMode::AckParallel {
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
             }| PulsarIngestMode::AckSequential {
                timeout,
                retry_policy,
            },
        ),
        no_ack().to(PulsarIngestMode::NoAck),
    ))
}

/// A subscription's name and ownership, with a required creation position when ingestor-owned.
fn subscription<'src>()
-> impl Parser<'src, &'src [Token], PulsarSubscription, extra::Err<ParseError<'src>>> + Clone {
    let ownership = choice((
        kw_phrase3(Identifier::Owned, Identifier::By, Identifier::Ingestor)
            .ignore_then(start_position())
            .map(|start_at| PulsarSubscriptionOwnership::Ingestor { start_at }),
        kw(Identifier::Provisioned).to(PulsarSubscriptionOwnership::Provisioned),
    ));
    kw(Identifier::Subscription)
        .ignore_then(external_name::<PulsarSubscriptionName>())
        .then(ownership)
        .map(|(name, ownership)| PulsarSubscription { name, ownership })
        .boxed()
}

fn start_position<'src>()
-> impl Parser<'src, &'src [Token], PulsarStartPosition, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Start, Identifier::At).ignore_then(choice((
        kw(Identifier::Earliest).to(PulsarStartPosition::Earliest),
        kw(Identifier::Latest).to(PulsarStartPosition::Latest),
    )))
}

pub(super) fn alteration<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::AlterIngestor, extra::Err<ParseError<'src>>> + Clone
{
    let operation = choice((
        kw(Identifier::Set)
            .ignore_then(start_position())
            .map(|position| PulsarTopicIngestorOperation::SetStartAt { position }),
        super::transport_operation(topic_body()).map(PulsarTopicIngestorOperation::Transport),
    ));
    super::alteration::<PulsarTopicIngestor, _, _>(operation)
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests current Pulsar creation positions through the composed grammar"
    )
)]
mod start_position_tests {
    //! Layer: test harness.
    //! - **Owns.** Current start-position parsing, refusal and completion examples.
    //! - **Depends on.** The composed statement parser and its conformance checks.
    //! - **Must not know.** Driver options, broker state or runtime lifecycle.
    use meticulous::ResultExt as _;
    use nervix_models::{
        CreateIngestor, IngestorEntity, IngestorInput, Model, PulsarIngestor, PulsarStartPosition,
        PulsarSubscriptionOwnership, Statement,
    };

    use crate::conformance_support::{assert_offers_exactly, refused};

    fn ingestor(ownership: &str) -> CreateIngestor {
        let text = format!(
            "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
             'readers' {ownership} MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec TO \
             output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR \
             LOG"
        );
        let Statement::Create(create) =
            crate::statement::parse_statement(&text).verified("the current Pulsar creation parses")
        else {
            panic!("the statement creates a Model");
        };
        let Model::Ingestor(ingestor) = *create.body else {
            panic!("the statement creates an ingestor");
        };
        ingestor
    }

    fn ownership(ingestor: &CreateIngestor) -> PulsarSubscriptionOwnership {
        let IngestorInput::Transport(input) = &ingestor.input else {
            panic!("Pulsar is a transport");
        };
        let IngestorEntity::Pulsar(PulsarIngestor::Topic(source)) = &input.source else {
            panic!("the source is Pulsar");
        };
        source.subscription.ownership
    }

    fn alteration(text: &str) -> nervix_models::AlterIngestor {
        let statement = crate::statement::parse_statement(text).verified("current ALTER parses");
        let canonical = statement
            .to_canonical_plexus()
            .verified("current ALTER renders");
        assert_eq!(
            crate::statement::parse_statement(&canonical).verified("canonical ALTER parses"),
            statement
        );
        let Statement::AlterIngestor(alter) = statement else {
            panic!("the statement is an ingestor alteration");
        };
        alter
    }

    #[test]
    fn owned_subscription_start_positions_parse() {
        for (text, position) in [
            ("EARLIEST", PulsarStartPosition::Earliest),
            ("LATEST", PulsarStartPosition::Latest),
            ("earliest", PulsarStartPosition::Earliest),
            ("latest", PulsarStartPosition::Latest),
        ] {
            let model = ingestor(&format!("OWNED BY INGESTOR START AT {text}"));
            assert_eq!(
                ownership(&model),
                PulsarSubscriptionOwnership::Ingestor { start_at: position }
            );
        }
    }

    #[test]
    fn alters_only_the_declared_start_position() {
        for position in [PulsarStartPosition::Earliest, PulsarStartPosition::Latest] {
            let mut model = ingestor("OWNED BY INGESTOR START AT EARLIEST");
            let alter = alteration(&format!(
                "ALTER PULSAR TOPIC INGESTOR events SET START AT {position}"
            ));
            model.apply_alter(&alter).verified("owned position changes");
            assert_eq!(
                ownership(&model),
                PulsarSubscriptionOwnership::Ingestor { start_at: position }
            );
        }
    }

    #[test]
    fn mixed_operations_keep_their_order_and_canonical_shape() {
        let mut model = ingestor("OWNED BY INGESTOR START AT LATEST");
        let alter = alteration(
            "ALTER PULSAR TOPIC INGESTOR events SET START AT LATEST, SET DECODE USING \
             other_codec, SET START AT EARLIEST",
        );
        model.apply_alter(&alter).verified("mixed operations apply");
        assert_eq!(
            ownership(&model),
            PulsarSubscriptionOwnership::Ingestor {
                start_at: PulsarStartPosition::Earliest
            }
        );
        assert_eq!(
            model.input.codec().map(|codec| codec.as_str()),
            Some("other_codec")
        );
    }

    #[test]
    fn provisioned_position_refusal_rolls_back_preceding_operations() {
        let mut model = ingestor("PROVISIONED");
        let before = model.clone();
        let alter = alteration(
            "ALTER PULSAR TOPIC INGESTOR events SET DECODE USING other_codec, SET START AT LATEST",
        );
        let error = model
            .apply_alter(&alter)
            .expect_err("provisioned cursors have no creation position");
        assert!(
            matches!(error.current_context(), nervix_models::AlterIngestorError::ProvisionedPulsarStartPosition { subscription } if subscription.as_str() == "readers")
        );
        assert_eq!(model, before);
    }

    #[test]
    fn start_position_completion_stays_on_the_pulsar_subscription() {
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
             'readers' OWNED BY INGESTOR ",
            &["START AT"],
        );
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
             'readers' OWNED BY INGESTOR START AT ",
            &["EARLIEST", "LATEST"],
        );
        assert_offers_exactly(
            "ALTER PULSAR TOPIC INGESTOR events SET START AT ",
            &["EARLIEST", "LATEST"],
        );
    }

    #[test]
    fn start_position_refuses_unknown_values_and_a_provisioned_cursor() {
        for ownership in [
            "OWNED BY INGESTOR START AT UNKNOWN",
            "PROVISIONED START AT LATEST",
        ] {
            let statement = format!(
                "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
                 'readers' {ownership} MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec TO \
                 output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL \
                 ERROR LOG"
            );
            assert!(refused(&statement), "{statement}");
        }
        for statement in [
            "ALTER PULSAR TOPIC INGESTOR events SET START AT UNKNOWN",
            "ALTER KAFKA TOPIC INGESTOR events SET START AT LATEST",
        ] {
            assert!(refused(statement), "{statement}");
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        IngestorEntity, IngestorInput, Model, PulsarIngestMode, PulsarIngestor,
        PulsarSubscriptionOwnership, PulsarTopicIngestor,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// A `PULSAR TOPIC` ingestor statement reading `topic` through `subscription` in the delivery
    /// mode `mode`.
    fn statement(topic: &str, subscription: &str, mode: &str) -> String {
        format!(
            "CREATE PULSAR TOPIC INGESTOR intake FROM pulsar_main TOPIC '{topic}' {subscription} \
             INSTANCES 2 MODE {mode} ON QUIESCE SUSPEND DECODE USING event_codec TO events \
             INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The Pulsar topic source `statement` creates.
    fn created_source(statement: &str) -> PulsarTopicIngestor {
        let Some(Model::Ingestor(ingestor)) = created_model(statement) else {
            panic!("the composed grammar accepts the statement: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the PULSAR TOPIC head names a transport");
        };
        let IngestorEntity::Pulsar(PulsarIngestor::Topic(source)) = input.source else {
            panic!("the PULSAR TOPIC head constructs the Pulsar topic Model");
        };
        source
    }

    const OWNED: &str = "SUBSCRIPTION 'Readers (eu)' OWNED BY INGESTOR START AT EARLIEST";

    #[rstest]
    #[case("Orders-EU_1")]
    #[case("persistent://tenant-a/orders/Placed")]
    #[case("non-persistent://public/default/orders")]
    fn topic_literals_reach_the_ingestor_model_unchanged(#[case] topic: &str) {
        let source = created_source(&statement(topic, OWNED, "NO ACK"));

        assert_eq!(source.topic.as_str(), topic);
        assert_eq!(source.instances.get(), 2);
    }

    #[rstest]
    #[case("OWNED BY INGESTOR START AT EARLIEST", PulsarSubscriptionOwnership::Ingestor { start_at: nervix_models::PulsarStartPosition::Earliest })]
    #[case("OWNED BY INGESTOR START AT LATEST", PulsarSubscriptionOwnership::Ingestor { start_at: nervix_models::PulsarStartPosition::Latest })]
    #[case("PROVISIONED", PulsarSubscriptionOwnership::Provisioned)]
    fn the_subscription_keeps_its_name_and_its_declared_owner(
        #[case] owner: &str,
        #[case] ownership: PulsarSubscriptionOwnership,
    ) {
        let subscription = format!("SUBSCRIPTION 'Readers (eu)' {owner}");
        let source = created_source(&statement("orders", &subscription, "NO ACK"));

        assert_eq!(source.subscription.name.as_str(), "Readers (eu)");
        assert_eq!(source.subscription.ownership, ownership);
    }

    #[test]
    fn every_delivery_mode_parses_into_the_pulsar_mode() {
        let no_ack = created_source(&statement("orders", OWNED, "NO ACK"));
        assert_eq!(no_ack.mode, PulsarIngestMode::NoAck);

        let sequential = created_source(&statement(
            "orders",
            OWNED,
            "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        ));
        let PulsarIngestMode::AckSequential {
            timeout,
            retry_policy,
        } = sequential.mode
        else {
            panic!("ACK SEQUENTIAL parses into the sequential Pulsar mode");
        };
        assert_eq!(timeout, "5s");
        assert_eq!(retry_policy.backoff, "1s");
        assert_eq!(retry_policy.max_backoff, "30s");

        let parallel = created_source(&statement(
            "orders",
            OWNED,
            "ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        ));
        let PulsarIngestMode::AckParallel {
            max,
            batch_timeout,
            timeout,
            retry_policy,
        } = parallel.mode
        else {
            panic!("ACK PARALLEL parses into the parallel Pulsar mode");
        };
        assert_eq!(max.get(), 8);
        assert_eq!(batch_timeout, "100ms");
        assert_eq!(timeout, "5s");
        assert_eq!(retry_policy.max_backoff, "30s");
    }

    #[test]
    fn completion_names_the_topic_and_subscription_rules_then_the_owners() {
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR intake FROM pulsar_main TOPIC ",
            &["pulsar_topic"],
        );
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR intake FROM pulsar_main TOPIC 'orders' SUBSCRIPTION ",
            &["pulsar_subscription"],
        );
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR intake FROM pulsar_main TOPIC 'orders' SUBSCRIPTION \
             'readers' ",
            &["OWNED BY INGESTOR START AT EARLIEST", "PROVISIONED"],
        );
    }

    #[test]
    fn the_composed_grammar_refuses_a_subscription_without_its_owner() {
        for subscription in [
            "",
            "SUBSCRIPTION 'readers'",
            "OWNED BY INGESTOR START AT EARLIEST",
        ] {
            let statement = statement("orders", subscription, "NO ACK");
            assert!(
                refused(&statement),
                "every Pulsar ingestor names its subscription and who owns it: {statement}"
            );
        }
    }

    #[test]
    fn the_composed_grammar_applies_the_topic_and_subscription_rules() {
        let topic_too_long = "t".repeat(1025);
        for topic in [
            "",
            "public/default/orders",
            "persistent://public/orders",
            "persistent://public//orders",
            "kafka://public/default/orders",
            "Orders EU",
            topic_too_long.as_str(),
        ] {
            let statement = statement(topic, OWNED, "NO ACK");
            assert!(
                refused(&statement),
                "topic '{topic}' is outside the Pulsar topic rule"
            );
        }

        let subscription_too_long = format!("SUBSCRIPTION '{}' PROVISIONED", "s".repeat(256));
        for subscription in [
            "SUBSCRIPTION '' PROVISIONED",
            subscription_too_long.as_str(),
        ] {
            let statement = statement("orders", subscription, "NO ACK");
            assert!(
                refused(&statement),
                "the subscription is outside the Pulsar subscription rule: {statement}"
            );
        }
    }
}
