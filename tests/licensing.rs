//! Public observations of the cluster's licensing policy.
//!
//! Layer: test harness.
//! - **Owns.** Assertions over the public licensing description and quota fixtures.
//! - **Depends on.** Scenario sessions, cluster fixtures and public observations.
//! - **Must not know.** Policy evaluation, stored records or admission implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public licensing scenarios")
)]

use std::time::Duration;

use cucumber::{gherkin::Step, given, then};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::Client;
use nervix_primitives::time::timeout;

use super::{
    ScenarioWorld, common::status_request::STATUS_WAIT_BUDGET, docstring, expand_placeholders,
};

#[given(expr = "the free licensing quota {string} is limited to {int}")]
fn limit_quota(world: &mut ScenarioWorld, condition: String, limit: u64) {
    let name = match condition.as_str() {
        "cluster.nodes" => nervix_server::licensing::QuotaName::ClusterNodes,
        "cluster.domains" => nervix_server::licensing::QuotaName::ClusterDomains,
        _ => panic!("the scenario names an undeclared cluster quota: {condition}"),
    };
    world.fault_injection.set_licensing_quota(name, limit);
}

#[then(expr = "node {string} eventually describes this license without a selected domain")]
async fn observe_license(world: &mut ScenarioWorld, node: String, #[step] step: &Step) {
    let node = expand_placeholders(world, &node);
    let expected = expand_placeholders(world, docstring(step));
    let uri = world
        .cluster()
        .grpc_uri(&node)
        .assured("the scenario started this node");
    let client = Client::connect_with_options(
        &uri,
        None,
        super::client_connect_options(&uri).assured("the fixture provides native TLS"),
    )
    .await
    .assured("the public native client connects without selecting a domain");
    let mut last = None;
    let observed = timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let outcome = client
                .execute("DESCRIBE LICENSE;")
                .await
                .assured("the licensing request completes");
            let matches = outcome.succeeded()
                && expected
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .all(|line| outcome.message.lines().any(|actual| actual == line));
            last = Some(outcome);
            if matches {
                break;
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let outcome = last.verified("at least one bounded description completed");
    assert!(
        observed.is_ok(),
        "license description did not contain {expected:?}: {}",
        outcome.message
    );
    world.last_command_output = Some(outcome.message.clone());
    world.last_client_outcome = Some(outcome);
}

#[then("the last license inspection has typed cluster scope")]
fn typed_scope(world: &mut ScenarioWorld) {
    use nervix_client_wire::{StateInspectionEntity, StateInspectionResult};
    let outcome = world
        .last_client_outcome
        .as_ref()
        .verified("a native description completed");
    let state = outcome
        .state
        .as_ref()
        .verified("the server supplied its typed observation");
    assert_eq!(state.entity, StateInspectionEntity::ClusterLicense);
    assert_eq!(
        state
            .display_text()
            .assured("the typed observation renders"),
        outcome.message
    );
    let host = nervix_client_host::Inspection::new(state.as_ref().clone());
    assert_eq!(host.domain(), None);
    assert_eq!(host.entity_kind(), "LICENSE");
    assert_eq!(host.entity_name(), None);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("the completed licensing observation is a page");
    };
    assert_eq!(page.unit_count, 1);
    assert_eq!(page.counts.rows, 3);
    assert_eq!(page.counts.branches, 0);
    assert_eq!(page.units[0].unit, nervix_models::Unit::SINGLE);
    assert_eq!(page.units[0].origin.node.node_id(), &page.units[0].owner);
    for section in &page.units[0].sections {
        assert!(section.schema.branch.is_none());
        for frame in &section.frames {
            frame
                .conform(&section.schema)
                .assured("every typed frame conforms");
        }
    }
}

#[then("the license description contains these lines")]
fn description_contains(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let output = world
        .last_command_output
        .as_deref()
        .verified("the preceding public description produced output");
    for line in expected.lines().filter(|line| !line.trim().is_empty()) {
        assert!(
            output.lines().any(|actual| actual == line),
            "missing licensing line {line:?} in {output}"
        );
    }
}

/// Check the complete free-mode metric contract from one successful public scrape.
/// A missing sample differs from a zero, including the optional committed decision and deadlines.
#[then(expr = "node {string} licensing metrics match the free policy")]
async fn free_metrics(world: &mut ScenarioWorld, node: String) {
    let node = expand_placeholders(world, &node);
    let url = world
        .cluster()
        .observability_metrics_url(&node)
        .assured("the scenario started this metrics listener");
    let client = reqwest::Client::new();
    let mut last = String::new();
    let outcome = timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let response = client
                .get(&url)
                .send()
                .await
                .assured("the public metrics listener answers")
                .error_for_status()
                .assured("the public metrics scrape succeeds");
            last = response.text().await.assured("the scrape is valid text");
            if free_metric_contract(&last) {
                break;
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "free licensing metric contract missing from {last}"
    );
}

fn free_metric_contract(body: &str) -> bool {
    let sample = |name: &str| {
        body.lines().find_map(|line| {
            line.strip_prefix(name)
                .and_then(|suffix| suffix.strip_prefix(' '))
                .and_then(|value| value.parse::<f64>().ok())
        })
    };
    for state in [
        "free",
        "licensed_fresh",
        "licensed_due",
        "licensed_overdue",
        "lapsed",
        "stopped",
        "releasing",
        "degraded",
    ] {
        if sample(&format!("nervix_licensing_state{{state=\"{state}\"}}"))
            != Some(if state == "free" { 1.0 } else { 0.0 })
        {
            return false;
        }
    }
    for condition in ["cluster.nodes", "cluster.domains"] {
        if sample(&format!(
            "nervix_licensing_over_limit{{condition=\"{condition}\"}}"
        )) != Some(0.0)
        {
            return false;
        }
        for state in ["within_grace", "over_limit", "lapsed", "stopped"] {
            if sample(&format!(
                "nervix_licensing_admissions_refused_total{{condition=\"{condition}\",state=\"\
                 {state}\"}}"
            )) != Some(0.0)
            {
                return false;
            }
        }
    }
    for operation in [
        "activation.create",
        "activation.refresh",
        "activation.release",
        "activation.rotate_key",
        "events.subscribe",
    ] {
        for outcome in [
            "committed",
            "rejected_by_cluster",
            "refused_by_service",
            "unreachable",
            "timeout",
        ] {
            if sample(&format!(
                "nervix_licensing_requests_total{{operation=\"{operation}\",outcome=\"{outcome}\"\
                 }}"
            )) != Some(0.0)
            {
                return false;
            }
        }
    }
    sample("nervix_licensing_stream_connected") == Some(0.0)
        && sample("nervix_licensing_transitions_active") == Some(0.0)
        && sample("nervix_licensing_licensing_time_seconds")
            .is_some_and(|value| value.is_finite() && value > 0.0)
        && !body.lines().any(|line| {
            line.starts_with("nervix_licensing_decision_revision ")
                || line.starts_with("nervix_licensing_deadline_timestamp_seconds{")
        })
}
