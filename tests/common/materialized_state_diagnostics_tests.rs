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
        future::ready(Ok("relay materialized state is empty".to_string()))
    })
    .await;
    assert_eq!(queried.len(), 12);
    assert_eq!(queried.last(), Some(&(node("restored-3"), 3)));
    assert_eq!(
        observations
            .iter()
            .filter(|line| *line == "relay materialized state is empty")
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
            Ok("owner: restored-2".to_string())
        })
    })
    .await;
    assert!(
        observations
            .iter()
            .any(|line| line == "public materialized state query failed: node is stopping")
    );
    assert_eq!(observations.last(), Some(&"owner: restored-2".to_string()));
}

#[test]
fn large_fixture_rows_retain_placement_round_and_watermarks() {
    let value = "a".repeat(512 * 1024);
    let payload = serde_json::json!({
        "tenant": "restore-tenant-1", "round": 2, "value": value,
        "occurred_at": "1970-01-01T00:00:02Z",
    });
    let output = format!(
        "materialized relay: state_11\nkind: RELAY\nowner: restored-2\nreplicas: \
         restored-3\nkey={{\"tenant\":\"restore-tenant-1\"}} payload={payload} low=2 high=2"
    );
    assert_eq!(
        summarize(&output),
        "materialized relay: state_11\nkind: RELAY\nowner: restored-2\nreplicas: \
         restored-3\nkey={\"tenant\":\"restore-tenant-1\"} round=2 value_bytes=524288 low=2 high=2"
    );
}

#[test]
fn every_row_keeps_its_branch_and_value_byte_length() {
    assert_eq!(
        summarize(
            "key={\"tenant\":\"restore-tenant-0\"} payload={\"round\":1,\"value\":\"é\"} low=1 \
             high=2\nkey={\"tenant\":\"restore-tenant-1\"} payload={\"round\":2,\"value\":\"text \
             low=inside\"} low=3 high=4"
        ),
        "key={\"tenant\":\"restore-tenant-0\"} round=1 value_bytes=2 low=1 \
         high=2\nkey={\"tenant\":\"restore-tenant-1\"} round=2 value_bytes=15 low=3 high=4"
    );
}

#[test]
fn empty_public_state_keeps_its_placement_and_outcome() {
    let output = "materialized relay: state_11\nowner: restored-2\nreplicas: restored-3\nrelay \
                  'state_11' materialized state is empty";
    assert_eq!(summarize(output), output);
}

#[test]
fn an_unreadable_observation_reports_the_boundary_it_could_not_read() {
    assert_eq!(
        summarize("key=(root)"),
        "unrecognized public materialized row: no payload boundary"
    );
    assert_eq!(
        summarize("key=(root) payload={}"),
        "unrecognized public materialized row: no watermark boundary"
    );
    assert!(
        summarize("key=(root) payload={ low=1 high=1")
            .starts_with("unreadable public materialized row:")
    );
    assert_eq!(
        summarize("key=(root) payload={\"value\":\"a\"} low=1 high=1"),
        "key=(root): fixture round is not a number"
    );
    assert_eq!(
        summarize("key=(root) payload={\"round\":2} low=1 high=1"),
        "key=(root): fixture value is not a string"
    );
}
