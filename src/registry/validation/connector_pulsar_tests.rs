//! Validation of the Pulsar topic ingestor's declared subscription.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions that a subscription type that reads through one consumer refuses more
//!   than one instance at the ingestor source contract, and that every other pairing passes.
//! - **Depends on.** The connector validation and the registry test fixtures.
//! - **Must not know.** How a Pulsar consumer joins or reads its subscription.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_models::{
    ClientName, IngestQuiesceMode, IngestorEntity, PulsarIngestMode, PulsarIngestor,
    PulsarSubscription, PulsarSubscriptionOwnership, PulsarSubscriptionType, PulsarTopicIngestor,
};
use nonzero_ext::nonzero;

use super::*;
use crate::registry::test_fixtures::{ingestor_statement, named};

#[test]
fn a_pulsar_subscription_type_that_reads_through_one_consumer_refuses_more_instances() {
    let domain = DomainName::parse("default").expect("valid domain");
    let mut ingestor = ingestor_statement("edge_in", "notifications", "event_codec", "kafka", &[]);
    let identifier = ModelName::from(&ingestor.name);
    for (kind, instances, refused) in [
        (PulsarSubscriptionType::Shared, nonzero!(2u64), false),
        (PulsarSubscriptionType::KeyShared, nonzero!(2u64), false),
        (PulsarSubscriptionType::Failover, nonzero!(1u64), false),
        (PulsarSubscriptionType::Exclusive, nonzero!(1u64), false),
        (PulsarSubscriptionType::Failover, nonzero!(2u64), true),
        (PulsarSubscriptionType::Exclusive, nonzero!(3u64), true),
    ] {
        let IngestorInput::Transport(input) = &mut ingestor.input else {
            unreachable!("the fixture builds a transport ingestor")
        };
        input.source = IngestorEntity::Pulsar(PulsarIngestor::Topic(PulsarTopicIngestor {
            client: ClientName::parse("pulsar_main").expect("valid identifier"),
            topic: named("orders"),
            subscription: PulsarSubscription {
                name: named("readers"),
                ownership: PulsarSubscriptionOwnership::Provisioned,
                kind,
            },
            instances: nervix_models::IngestorInstances::Count(instances),
            mode: PulsarIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        }));
        let validated = validate_ingestor_source(&domain, &identifier, &ingestor);
        if !refused {
            validated.expect("this type can serve these instances");
            continue;
        }
        let error = validated.expect_err("this type reads through one consumer");
        let RegistryError::InvalidConnectorDeclaration { issue, .. } = error.current_context()
        else {
            panic!("expected a declaration refusal: {error:?}");
        };
        assert_eq!(
            issue.to_string(),
            format!(
                "PULSAR subscription TYPE {kind} reads through one consumer, so it refuses \
                 INSTANCES {instances}"
            )
        );
    }
}
