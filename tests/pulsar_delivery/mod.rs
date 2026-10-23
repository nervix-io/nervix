//! Public Pulsar delivery proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** A timed broker fault concurrent with an HTTP post, and the sequenced payloads a
//!   Pulsar emitter publishes and its broker receives.
//! - **Depends on.** The scenario's cluster, dependency forwarder and shared command steps.
//! - **Must not know.** The Pulsar emitter's private state or a driver receipt.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use cucumber::{gherkin::Step, then, when};
use meticulous::{OptionExt as _, ResultExt as _};

use super::{ScenarioWorld, parse_duration_text, when_http_payload_is_posted_to_node};

#[when(expr = "this HTTP payload is posted while the Pulsar endpoint resumes after {string}")]
async fn post_during_pulsar_fault(world: &mut ScenarioWorld, delay: String, #[step] step: &Step) {
    let delay = parse_duration_text(&delay).assured("the scenario declares a valid fault duration");
    let resume = world
        .stallable_tcp_proxies
        .get("pulsar")
        .verified("the scenario configured its Pulsar endpoint")
        .resume_after(delay);
    let post = when_http_payload_is_posted_to_node(
        world,
        "node-1".to_string(),
        "pulsar-{{test_id}}.example.com".to_string(),
        "/events".to_string(),
        step,
    );
    let ((), resumed) = tokio::join!(post, resume);
    let _resumed_at = resumed.assured("the proxy remains alive while the fault runs");
}

#[when(expr = "{int} sequenced HTTP payloads are posted to the Pulsar emitter")]
async fn post_sequenced_payloads(world: &mut ScenarioWorld, count: usize, #[step] step: &Step) {
    assert!(
        (1..=100).contains(&count),
        "the fixture bounds its producer queue proof at 100 records"
    );
    let mut payloads = step.clone();
    payloads.docstring = Some(
        (1..=count)
            .map(|sequence| format!(r#"{{"sequence":{sequence}}}"#))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    super::when_http_payloads_are_posted_concurrently(
        world,
        "pulsar-{{test_id}}.example.com".to_string(),
        "/events".to_string(),
        &payloads,
    )
    .await;
}

#[then(
    expr = "within {string} the observed broker receives exactly {int} sequenced Pulsar payloads"
)]
async fn observe_sequenced_payloads(world: &mut ScenarioWorld, duration: String, count: usize) {
    assert!(
        (1..=100).contains(&count),
        "the fixture bounds its exact payload oracle at 100 records"
    );
    let expected = (1..=count)
        .map(|sequence| {
            (
                super::ExpectedBrokerPayload(format!(r#"{{"sequence":{sequence}}}"#).into_bytes()),
                1,
            )
        })
        .collect();
    super::receive_exactly_these_broker_messages(world, &duration, expected).await;
}
