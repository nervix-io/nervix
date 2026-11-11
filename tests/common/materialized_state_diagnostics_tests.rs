//! Regressions for bounded observations after a materialized fixture misses its exact rows.
//!
//! Layer: test harness.
//! - **Owns.** Query limits, observation deadlines, retained failures and compact row evidence.
//! - **Depends on.** The public-state diagnostic owner, typed node names and primitive test clocks.
//! - **Must not know.** Scenario state, runtime publications or restore installation internals.

use std::{future, io, time::Duration};

use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;
use nervix_primitives::time::Instant;

use super::materialized_state_diagnostics::{observe, summarize};

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the fixture node name is valid")
}

#[nervix_primitives::test(start_paused = true)]
async fn stalled_queries_end_at_the_shared_observation_deadline() {
    let nodes = vec![node("restored-1"), node("restored-2"), node("restored-3")];
    let mut queried = Vec::new();
    let started = Instant::now();
    let observations = observe(&nodes, 0..40, |node, relay| {
        queried.push((node.clone(), relay));
        future::pending()
    })
    .await;
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    assert_eq!(queried.len(), 5);
    assert_eq!(
        observations.last(),
        Some(&"materialized state diagnostics: total budget expired".to_string())
    );
    assert_eq!(
        observations
            .iter()
            .filter(|line| line.contains("exceeded its observation budget"))
            .count(),
        5
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn successful_observations_are_limited_to_four_relays_on_three_nodes() {
    let nodes = (1..=5)
        .map(|index| node(&format!("restored-{index}")))
        .collect::<Vec<_>>();
    let mut queried = Vec::new();
    let observations = observe(&nodes, 0..40, |node, relay| {
        queried.push((node.clone(), relay));
        future::ready(Ok(inspection(None)))
    })
    .await;
    assert_eq!(queried.len(), 12);
    assert_eq!(queried.last(), Some(&(node("restored-3"), 3)));
    assert_eq!(
        observations
            .iter()
            .filter(|line| line.contains("rows: 0"))
            .count(),
        12
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_failed_public_query_does_not_discard_the_next_nodes_observation() {
    let nodes = vec![node("restored-1"), node("restored-2")];
    let observations = observe(&nodes, [11].into_iter(), |node, _| {
        future::ready(if node.as_str() == "restored-1" {
            Err(io::Error::other("node is stopping"))
        } else {
            Ok(inspection(None))
        })
    })
    .await;
    assert!(
        observations
            .iter()
            .any(|line| line == "public materialized state query failed: node is stopping")
    );
    assert!(
        observations
            .last()
            .is_some_and(|line| line.contains("owner: restored-2"))
    );
}

fn inspection(value: Option<&str>) -> nervix_client_wire::StateInspection {
    use nervix_client_wire::*;
    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, DomainNodeRef, ModelKind, ParseAsType,
        SchemaField, Timestamp, Unit,
    };
    let schema = RowSchema {
        branch: None,
        fields: [
            ("round", ParseAsType::I64),
            ("value", ParseAsType::String),
            ("low", ParseAsType::Datetime),
            ("high", ParseAsType::Datetime),
        ]
        .into_iter()
        .map(|(name, ty)| SchemaField {
            name: nervix_models::FieldName::parse(name).assured("fixture name"),
            ty,
            optional: false,
            sensitive: false,
        })
        .collect(),
    };
    let sections = value
        .map(|value| {
            let limits = SessionLimits::DEFAULT;
            let mut rows = StateRowsEncoder::unbranched(&limits);
            rows.push_row(|cells| {
                cells.push_i64(2)?;
                cells.push_string(value)?;
                cells.push_datetime(Timestamp::from_unix_nanos(7))?;
                cells.push_datetime(Timestamp::from_unix_nanos(11))
            })
            .assured("bounded fixture cells");
            StateInspectionSection {
                name: "records".to_string(),
                schema,
                frames: vec![rows.finish(&limits).assured("accepted fixture row")],
                omitted_rows: 0,
                watermarks: Some(StateWatermarkColumns { low: 2, high: 3 }),
            }
        })
        .into_iter()
        .collect();
    let counts = StateInspectionCounts {
        rows: u64::from(value.is_some()),
        ..StateInspectionCounts::default()
    };
    StateInspection {
        entity: DomainNodeRef::node_in(
            nervix_models::DomainName::parse("fixture").assured("domain"),
            ModelKind::Relay,
            nervix_models::RelayName::parse("state_11").assured("relay"),
        )
        .into(),
        result: StateInspectionResult::Page(StateInspectionPage {
            counts,
            unit_count: 1,
            continuation: None,
            units: vec![StateInspectionUnit {
                unit: Unit::SINGLE,
                owner: node("restored-2"),
                replicas: vec![node("restored-3")],
                origin: StateInspectionOrigin {
                    node: ClusterNodeIdentity::new(
                        node("restored-2"),
                        ClusterNodeIncarnation::new(1),
                    ),
                    fingerprint: [1; 32],
                    generation: 2,
                    revision: 3,
                    stage: StateInspectionStage::Stored,
                },
                counts,
                sections,
                wasm: None,
            }],
            wasm_binding: None,
        }),
    }
}

#[test]
fn large_fixture_rows_retain_placement_round_and_watermarks_without_the_payload() {
    let value = "a".repeat(512 * 1024);
    let summarized = summarize(&inspection(Some(&value)));
    assert!(summarized.contains("owner: restored-2"));
    assert!(summarized.contains("restored-3"));
    assert!(summarized.contains("round=2 value_bytes=524288 low=Some(7) high=Some(11)"));
    assert!(summarized.len() < 1024);
}

#[test]
fn a_multibyte_fixture_keeps_its_utf8_byte_length_and_typed_watermarks() {
    assert!(
        summarize(&inspection(Some("é")))
            .contains("round=2 value_bytes=2 low=Some(7) high=Some(11)")
    );
}

#[test]
fn empty_public_state_keeps_placement_and_zero_rows() {
    let output = summarize(&inspection(None));
    assert!(output.contains("rows: 0"));
    assert!(output.contains("owner: restored-2"));
    assert!(output.contains("restored-3"));
}
