//! Codec and relay completion through the rules the forms' typed choices apply.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Owner tests of text completion at codec and relay role positions against queued
//!   Models, and of its agreement with the choice target the forms ask at the same position.
//! - **Depends on.** The session service's suggestion and choice resolvers and its test fixtures.
//! - **Must not know.** Grammar internals, transports or browser state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "reference completion owner tests remain outside product execution"
    )
)]

use std::collections::BTreeSet;

use nervix_client_wire::{ChoiceLookupRequest, ChoiceSelection, ChoiceTarget, ChoiceValue};
use nervix_models::{DomainName, ModelKind, NodeRef, RelayName};
use nervix_recovery::Discarded as _;

use super::super::{
    super::{
        subscription::SessionSubscriptions,
        test_fixtures::{
            TestService, build_test_service, named, queue_in_transaction, suggestion_values,
        },
    },
    SessionServiceImpl,
};

const INGESTOR: &str =
    "CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB";
const EMITTER: &str = "CREATE KAFKA TOPIC EMITTER sink FROM readings TO broker TOPIC 'events' \
                       MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s";

/// Queues, in one open transaction, schemas, a branch, relays on and off it, one with materialized
/// state, and codecs that decode and encode, only decode, and only encode.
async fn queue_configuration(
    service: &SessionServiceImpl,
    subscriptions: &mut SessionSubscriptions,
) {
    for statement in [
        "BEGIN;",
        "CREATE SCHEMA reading ( id I64, value STRING );",
        "CREATE SCHEMA label ( id I64, name STRING );",
        "CREATE SCHEMA reading_key ( id I64 );",
        "CREATE BRANCH by_reading SCHEMA reading_key TTL 1h;",
        "CREATE RELAY readings SCHEMA reading UNBRANCHED;",
        "CREATE RELAY more_readings SCHEMA reading UNBRANCHED;",
        "CREATE RELAY labels SCHEMA label UNBRANCHED;",
        "CREATE RELAY branched_readings SCHEMA reading BRANCHED BY by_reading;",
        "CREATE RELAY state_readings SCHEMA reading UNBRANCHED WITH MATERIALIZED STATE LAST BY \
         TIMESTAMP;",
        "CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( id integer, value string );",
        "CREATE WIRE JSON CODEC both_ways FROM WIRE SCHEMA reading_wire TO SCHEMA reading;",
        "CREATE JAQ JSON CODEC decode_only TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON \
         INGESTION '.';",
        "CREATE JAQ JSON CODEC encode_only TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON EMITTING \
         '.';",
    ] {
        queue_in_transaction(service, subscriptions, statement).await;
    }
}

fn domain() -> ChoiceSelection {
    ChoiceSelection {
        value: ChoiceValue::Domain(named::<DomainName>("default")),
    }
}

fn relay(name: &str) -> ChoiceSelection {
    ChoiceSelection {
        value: ChoiceValue::Model(NodeRef::new(ModelKind::Relay, named::<RelayName>(name))),
    }
}

/// The labels of the choices `target` offers with `dependencies`, read as the forms read them.
async fn choice_labels(
    service: &SessionServiceImpl,
    subscriptions: &SessionSubscriptions,
    target: ChoiceTarget,
    dependencies: Vec<ChoiceSelection>,
) -> BTreeSet<String> {
    let request = ChoiceLookupRequest::new(target, dependencies, String::new());
    let outcome = service.process_choice(request, &subscriptions.view()).await;
    let mut labels = BTreeSet::new();
    for choice in outcome.choices {
        labels.insert(choice.presentation.label);
    }
    labels
}

#[nervix_primitives::test]
async fn codec_and_relay_positions_offer_what_their_statement_can_accept() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    queue_configuration(&service, &mut subscriptions).await;

    let cases = [
        (
            format!("{INGESTOR} DECODE USING "),
            vec!["both_ways", "decode_only"],
        ),
        (
            "CREATE HASH MAP lookup_by_id KEY id FROM RESOURCE bundle VERSION 1 PATH \
             'lookup.jsonl' DECODE USING "
                .to_string(),
            vec!["both_ways", "decode_only"],
        ),
        (
            format!("{EMITTER} ENCODE USING "),
            vec!["both_ways", "encode_only"],
        ),
        (
            "DROP CODEC ".to_string(),
            vec!["both_ways", "decode_only", "encode_only"],
        ),
        (
            "CREATE JUNCTION copy FROM ".to_string(),
            vec![
                "branched_readings",
                "labels",
                "more_readings",
                "readings",
                "state_readings",
            ],
        ),
        (
            "CREATE JUNCTION copy FROM readings, ".to_string(),
            vec!["more_readings", "readings", "state_readings"],
        ),
        (
            "CREATE JUNCTION copy FROM readings, mo".to_string(),
            vec!["more_readings"],
        ),
        (
            "CREATE JUNCTION copy FROM readings UNBRANCHED USING MATERIALIZED STATE ".to_string(),
            vec!["state_readings"],
        ),
        (
            "CREATE JUNCTION copy FROM readings UNBRANCHED TO ".to_string(),
            vec!["labels", "more_readings", "readings", "state_readings"],
        ),
        (
            "CREATE JUNCTION copy FROM branched_readings BRANCHED BY by_reading TO ".to_string(),
            vec!["branched_readings"],
        ),
        (
            "CREATE CORRELATOR pairs LEFT FROM readings RIGHT FROM labels, ".to_string(),
            vec!["labels"],
        ),
        (
            "CREATE KAFKA TOPIC EMITTER sink FROM readings, ".to_string(),
            vec![
                "branched_readings",
                "more_readings",
                "readings",
                "state_readings",
            ],
        ),
        (
            "CREATE KAFKA TOPIC EMITTER sink FROM readings, branched_readings USING MATERIALIZED \
             STATE "
                .to_string(),
            Vec::new(),
        ),
        (
            format!(
                "{INGESTOR} DECODE USING codec TO readings INHERIT ALL UNBRANCHED FLUSH IMMEDIATE \
                 ON MESSAGE ERROR SEND TO "
            ),
            vec!["labels", "more_readings", "readings", "state_readings"],
        ),
        (
            "CREATE JUNCTION copy FROM missing, ".to_string(),
            Vec::new(),
        ),
    ];
    for (input, expected) in cases {
        let offered = suggestion_values(&service, &subscriptions, &input).await;
        assert_eq!(offered, expected, "{input}");
    }

    subscriptions.stop_all().await;
    std::fs::remove_dir_all(&path)
        .discarded("the temporary test fixture is already isolated from the next test");
}

#[nervix_primitives::test]
async fn each_position_offers_what_the_choice_target_of_its_form_control_offers() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    queue_configuration(&service, &mut subscriptions).await;

    let cases = [
        (
            format!("{INGESTOR} DECODE USING "),
            ChoiceTarget::IngestCodec,
            vec![domain()],
        ),
        (
            format!("{EMITTER} ENCODE USING "),
            ChoiceTarget::EmitterCodec,
            vec![domain()],
        ),
        (
            "CREATE JUNCTION copy FROM readings, ".to_string(),
            ChoiceTarget::ProcessorCompatibleInputRelay,
            vec![domain(), relay("readings")],
        ),
        (
            "CREATE JUNCTION copy FROM readings UNBRANCHED TO ".to_string(),
            ChoiceTarget::ProcessorInputBranchRelay,
            vec![domain(), relay("readings")],
        ),
        (
            "CREATE JUNCTION copy FROM readings UNBRANCHED TO more_readings INHERIT ALL FLUSH \
             IMMEDIATE ON MESSAGE ERROR SEND TO "
                .to_string(),
            ChoiceTarget::ProcessorInputBranchRelay,
            vec![domain(), relay("readings")],
        ),
        (
            "CREATE JUNCTION copy FROM readings UNBRANCHED USING MATERIALIZED STATE ".to_string(),
            ChoiceTarget::ProcessorMaterializedRelay,
            vec![domain(), relay("readings")],
        ),
        (
            "CREATE KAFKA TOPIC EMITTER sink FROM readings, ".to_string(),
            ChoiceTarget::EmitterCompatibleInputRelay,
            vec![domain(), relay("readings")],
        ),
        (
            "CREATE KAFKA TOPIC EMITTER sink FROM readings USING MATERIALIZED STATE ".to_string(),
            ChoiceTarget::EmitterMaterializedRelay,
            vec![domain(), relay("readings")],
        ),
        (
            format!(
                "{EMITTER} ENCODE USING both_ways INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
                 SEND TO "
            ),
            ChoiceTarget::EmitterErrorRelay,
            vec![domain(), relay("readings")],
        ),
        (
            format!(
                "{INGESTOR} DECODE USING codec TO readings INHERIT ALL UNBRANCHED FLUSH IMMEDIATE \
                 ON MESSAGE ERROR SEND TO "
            ),
            ChoiceTarget::IngestUnbranchedRelay,
            vec![domain()],
        ),
    ];
    for (input, target, dependencies) in cases {
        let mut completed = BTreeSet::new();
        for value in suggestion_values(&service, &subscriptions, &input).await {
            completed.insert(value);
        }
        let chosen = choice_labels(&service, &subscriptions, target, dependencies).await;
        assert!(!completed.is_empty(), "{input}");
        assert_eq!(completed, chosen, "{input} against {target:?}");
    }

    subscriptions.stop_all().await;
    std::fs::remove_dir_all(&path)
        .discarded("the temporary test fixture is already isolated from the next test");
}
