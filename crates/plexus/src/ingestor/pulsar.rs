//! The Pulsar connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `PULSAR TOPIC` ingestor: its client, topic, subscription with who
//!   owns it and the type it reads as, instances, delivery mode and quiesce mode, and the delivery
//!   modes a Pulsar subscription offers.
//! - **Depends on.** The shared ingestor clauses and the Pulsar vocabulary.
//! - **Must not know.** The Pulsar driver or how a subscription is acknowledged, verified or
//!   removed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    PulsarIngestMode, PulsarSubscription, PulsarSubscriptionName, PulsarSubscriptionOwnership,
    PulsarSubscriptionType, PulsarTopicIngestor, PulsarTopicIngestorChange, PulsarTopicName,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    start_position, suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase3},
};

/// `FROM <client> TOPIC '<topic>' SUBSCRIPTION '<subscription>'
/// (OWNED BY INGESTOR START AT EARLIEST|LATEST)|PROVISIONED
/// TYPE SHARED|KEY_SHARED|FAILOVER|EXCLUSIVE [INSTANCES <n>] MODE … ON QUIESCE SUSPEND`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], PulsarTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<PulsarTopicName>())
        .then(subscription())
        .then(optional_instances::<PulsarTopicIngestor>())
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

/// A subscription a statement names and who owns it, before the type it reads as.
struct SubscriptionOwner {
    name: PulsarSubscriptionName,
    ownership: PulsarSubscriptionOwnership,
}

/// `SUBSCRIPTION '<name>'` and who owns it, with a required creation position when
/// ingestor-owned.
fn subscription_owner<'src>()
-> impl Parser<'src, &'src [Token], SubscriptionOwner, extra::Err<ParseError<'src>>> + Clone {
    let ownership = choice((
        kw_phrase3(Identifier::Owned, Identifier::By, Identifier::Ingestor)
            .ignore_then(start_position())
            .map(|start_at| PulsarSubscriptionOwnership::Ingestor { start_at }),
        kw(Identifier::Provisioned).to(PulsarSubscriptionOwnership::Provisioned),
    ));
    kw(Identifier::Subscription)
        .ignore_then(external_name::<PulsarSubscriptionName>())
        .then(ownership)
        .map(|(name, ownership)| SubscriptionOwner { name, ownership })
        .boxed()
}

/// A subscription's name, ownership and type, with a required creation position when
/// ingestor-owned.
fn subscription<'src>()
-> impl Parser<'src, &'src [Token], PulsarSubscription, extra::Err<ParseError<'src>>> + Clone {
    subscription_owner()
        .then(subscription_type())
        .map(
            |(SubscriptionOwner { name, ownership }, kind)| PulsarSubscription {
                name,
                ownership,
                kind,
            },
        )
        .boxed()
}

/// `TYPE SHARED|KEY_SHARED|FAILOVER|EXCLUSIVE`.
fn subscription_type<'src>()
-> impl Parser<'src, &'src [Token], PulsarSubscriptionType, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Type).ignore_then(choice((
        kw(Identifier::Shared).to(PulsarSubscriptionType::Shared),
        kw(Identifier::KeyShared).to(PulsarSubscriptionType::KeyShared),
        kw(Identifier::Failover).to(PulsarSubscriptionType::Failover),
        kw(Identifier::Exclusive).to(PulsarSubscriptionType::Exclusive),
    )))
}

/// `SET TOPIC`, `SET SUBSCRIPTION`, `SET START AT`, `SET TYPE` or `SET MODE`: one part of a
/// `PULSAR TOPIC` body.
pub(super) fn change<'src>()
-> impl Parser<'src, &'src [Token], PulsarTopicIngestorChange, extra::Err<ParseError<'src>>> + Clone
{
    let topic = kw(Identifier::Topic)
        .ignore_then(external_name::<PulsarTopicName>())
        .map(PulsarTopicIngestorChange::Topic);
    let owned = subscription_owner().map(|SubscriptionOwner { name, ownership }| {
        PulsarTopicIngestorChange::Subscription { name, ownership }
    });
    let start = start_position().map(PulsarTopicIngestorChange::StartAt);
    let kind = subscription_type().map(PulsarTopicIngestorChange::Type);
    let delivery = kw(Identifier::Mode)
        .ignore_then(mode())
        .map(PulsarTopicIngestorChange::Mode);
    kw(Identifier::Set)
        .ignore_then(choice((topic, owned, start, kind, delivery)))
        .boxed()
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
        CreateIngestor, IngestStartPosition, IngestorEntity, IngestorInput, Model, PulsarIngestor,
        PulsarSubscriptionOwnership, Statement,
    };

    use crate::conformance_support::{assert_offers_exactly, refused};

    fn ingestor(ownership: &str) -> CreateIngestor {
        let text = format!(
            "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
             'readers' {ownership} TYPE SHARED MODE NO ACK ON QUIESCE SUSPEND DECODE USING \
             event_codec TO output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
             GENERAL ERROR LOG"
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
            ("EARLIEST", IngestStartPosition::Earliest),
            ("LATEST", IngestStartPosition::Latest),
            ("earliest", IngestStartPosition::Earliest),
            ("latest", IngestStartPosition::Latest),
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
        for position in [IngestStartPosition::Earliest, IngestStartPosition::Latest] {
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
                start_at: IngestStartPosition::Earliest
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
                 'readers' {ownership} TYPE SHARED MODE NO ACK ON QUIESCE SUSPEND DECODE USING \
                 event_codec TO output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
                 LOG ON GENERAL ERROR LOG"
            );
            assert!(refused(&statement), "{statement}");
        }
        for statement in [
            "ALTER PULSAR TOPIC INGESTOR events SET START AT UNKNOWN",
            "ALTER MQTT TOPIC INGESTOR events SET START AT LATEST",
        ] {
            assert!(refused(statement), "{statement}");
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests current Pulsar subscription types through the composed grammar"
    )
)]
mod subscription_type_tests {
    //! Layer: test harness.
    //! - **Owns.** Current subscription type parsing, alteration, refusal and completion examples.
    //! - **Depends on.** The composed statement parser and its conformance checks.
    //! - **Must not know.** Driver options, broker state or runtime lifecycle.
    use meticulous::ResultExt as _;
    use nervix_models::{
        CreateIngestor, IngestorEntity, IngestorInput, Model, PulsarIngestor,
        PulsarSubscriptionOwnership, PulsarSubscriptionType, Statement,
    };
    use rstest::rstest;
    use strum::IntoEnumIterator as _;

    use crate::conformance_support::{assert_offers_exactly, refused};

    const TYPES: [&str; 4] = ["SHARED", "KEY_SHARED", "FAILOVER", "EXCLUSIVE"];

    fn statement(subscription_type: &str) -> String {
        format!(
            "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
             'readers' PROVISIONED {subscription_type} MODE NO ACK ON QUIESCE SUSPEND DECODE \
             USING event_codec TO output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
             LOG ON GENERAL ERROR LOG"
        )
    }

    fn ingestor(subscription_type: &str) -> CreateIngestor {
        let Statement::Create(create) =
            crate::statement::parse_statement(&statement(subscription_type))
                .verified("the current Pulsar creation parses")
        else {
            panic!("the statement creates a Model");
        };
        let Model::Ingestor(ingestor) = *create.body else {
            panic!("the statement creates an ingestor");
        };
        ingestor
    }

    fn subscription(ingestor: &CreateIngestor) -> &nervix_models::PulsarSubscription {
        let IngestorInput::Transport(input) = &ingestor.input else {
            panic!("Pulsar is a transport");
        };
        let IngestorEntity::Pulsar(PulsarIngestor::Topic(source)) = &input.source else {
            panic!("the source is Pulsar");
        };
        &source.subscription
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

    #[rstest]
    #[case("TYPE SHARED", PulsarSubscriptionType::Shared)]
    #[case("TYPE KEY_SHARED", PulsarSubscriptionType::KeyShared)]
    #[case("TYPE FAILOVER", PulsarSubscriptionType::Failover)]
    #[case("TYPE EXCLUSIVE", PulsarSubscriptionType::Exclusive)]
    #[case("type key_shared", PulsarSubscriptionType::KeyShared)]
    fn every_type_reaches_the_subscription(
        #[case] written: &str,
        #[case] kind: PulsarSubscriptionType,
    ) {
        assert_eq!(subscription(&ingestor(written)).kind, kind);
    }

    #[test]
    fn the_grammar_spells_every_type_the_vocabulary_has() {
        let spelled: Vec<String> = PulsarSubscriptionType::iter()
            .map(|kind| kind.to_string())
            .collect();
        assert_eq!(spelled, TYPES);
    }

    #[test]
    fn a_missing_or_unknown_type_is_refused() {
        for subscription_type in ["", "TYPE", "TYPE ROUND_ROBIN", "SHARED", "TYPE 'SHARED'"] {
            let statement = statement(subscription_type);
            assert!(refused(&statement), "{statement}");
        }
    }

    #[test]
    fn the_type_follows_the_owner_and_is_written_once() {
        for clauses in [
            "TYPE SHARED PROVISIONED",
            "PROVISIONED TYPE SHARED TYPE KEY_SHARED",
        ] {
            let statement = format!(
                "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
                 'readers' {clauses} MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec TO \
                 output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL \
                 ERROR LOG"
            );
            assert!(refused(&statement), "{statement}");
        }
    }

    #[test]
    fn alters_only_the_declared_type() {
        for kind in PulsarSubscriptionType::iter() {
            let mut model = ingestor("TYPE KEY_SHARED");
            let before = subscription(&model).clone();
            let alter = alteration(&format!(
                "ALTER PULSAR TOPIC INGESTOR events SET TYPE {kind}"
            ));
            model.apply_alter(&alter).verified("the type changes");

            let altered = subscription(&model);
            assert_eq!(altered.kind, kind);
            assert_eq!(altered.name, before.name);
            assert_eq!(altered.ownership, before.ownership);
        }
    }

    #[test]
    fn type_and_start_position_alterations_keep_their_order() {
        let mut model = ingestor("TYPE SHARED");
        let alter = alteration(
            "ALTER PULSAR TOPIC INGESTOR events SET TYPE FAILOVER, SET DECODE USING other_codec, \
             SET TYPE EXCLUSIVE",
        );
        model.apply_alter(&alter).verified("mixed operations apply");
        assert_eq!(subscription(&model).kind, PulsarSubscriptionType::Exclusive);
        assert_eq!(
            subscription(&model).ownership,
            PulsarSubscriptionOwnership::Provisioned
        );
        assert_eq!(
            model.input.codec().map(|codec| codec.as_str()),
            Some("other_codec")
        );
    }

    #[test]
    fn set_type_belongs_to_the_pulsar_topic_ingestor() {
        for statement in [
            "ALTER PULSAR TOPIC INGESTOR events SET TYPE UNKNOWN",
            "ALTER PULSAR TOPIC INGESTOR events SET TYPE",
            "ALTER KAFKA TOPIC INGESTOR events SET TYPE SHARED",
            "ALTER RABBITMQ QUEUE INGESTOR events SET TYPE SHARED",
        ] {
            assert!(refused(statement), "{statement}");
        }
    }

    #[test]
    fn completion_offers_the_type_after_the_owner_and_nowhere_else() {
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
             'readers' PROVISIONED ",
            &["TYPE"],
        );
        assert_offers_exactly(
            "CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events' SUBSCRIPTION \
             'readers' OWNED BY INGESTOR START AT LATEST TYPE ",
            &TYPES,
        );
        assert_offers_exactly("ALTER PULSAR TOPIC INGESTOR events SET TYPE ", &TYPES);
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

    const OWNED: &str =
        "SUBSCRIPTION 'Readers (eu)' OWNED BY INGESTOR START AT EARLIEST TYPE SHARED";

    #[rstest]
    #[case("Orders-EU_1")]
    #[case("persistent://tenant-a/orders/Placed")]
    #[case("non-persistent://public/default/orders")]
    fn topic_literals_reach_the_ingestor_model_unchanged(#[case] topic: &str) {
        let source = created_source(&statement(topic, OWNED, "NO ACK"));

        assert_eq!(source.topic.as_str(), topic);
        assert_eq!(
            source.instances,
            nervix_models::IngestorInstances::Count(nonzero_ext::nonzero!(2u64))
        );
    }

    #[rstest]
    #[case("OWNED BY INGESTOR START AT EARLIEST", PulsarSubscriptionOwnership::Ingestor { start_at: nervix_models::IngestStartPosition::Earliest })]
    #[case("OWNED BY INGESTOR START AT LATEST", PulsarSubscriptionOwnership::Ingestor { start_at: nervix_models::IngestStartPosition::Latest })]
    #[case("PROVISIONED", PulsarSubscriptionOwnership::Provisioned)]
    fn the_subscription_keeps_its_name_and_its_declared_owner(
        #[case] owner: &str,
        #[case] ownership: PulsarSubscriptionOwnership,
    ) {
        let subscription = format!("SUBSCRIPTION 'Readers (eu)' {owner} TYPE SHARED");
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
            "SUBSCRIPTION 'readers' TYPE SHARED",
            "OWNED BY INGESTOR START AT EARLIEST TYPE SHARED",
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

        let subscription_too_long =
            format!("SUBSCRIPTION '{}' PROVISIONED TYPE SHARED", "s".repeat(256));
        for subscription in [
            "SUBSCRIPTION '' PROVISIONED TYPE SHARED",
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
