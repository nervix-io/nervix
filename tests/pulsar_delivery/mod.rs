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

use super::{
    ScenarioWorld, expand_placeholders, parse_duration_text, when_http_payload_is_posted_to_node,
};

#[when(expr = "the Pulsar endpoint pauses after its producer is ready within {string}")]
async fn pause_after_producer_ready(world: &mut ScenarioWorld, within: String) {
    let within = parse_duration_text(&within).assured("the fixture declares a readiness bound");
    world
        .stallable_tcp_proxies
        .get("pulsar")
        .verified("the scenario configured its Pulsar endpoint")
        .pause_pulsar_after_producer_ready(within)
        .await
        .assured(
            "the Pulsar fault must start after the producer creation reply reached its client",
        );
}

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
async fn post_sequenced_payloads(world: &mut ScenarioWorld, count: usize) {
    assert!(
        (1..=100).contains(&count),
        "the fixture bounds its producer queue proof at 100 records"
    );
    let host = expand_placeholders(world, "pulsar-{{test_id}}.example.com");
    let uri = world
        .cluster()
        .http_uri("node-1", "/events")
        .assured("the scenario's HTTP intake is available");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .assured("the acknowledged payload client is valid");
    let posts = (1..=count).map(|sequence| {
        let (client, uri, host) = (&client, uri.as_str(), host.as_str());
        async move {
            let response = client
                .post(uri)
                .header(reqwest::header::HOST, host)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(format!(r#"{{"sequence":{sequence}}}"#))
                .send()
                .await
                .assured("the sequenced HTTP request must receive an endpoint outcome");
            let status = response.status();
            let body = response
                .bytes()
                .await
                .assured("the sequenced HTTP response body must be available");
            assert_eq!(
                status,
                reqwest::StatusCode::OK,
                "sequence {sequence} did not complete: {}",
                String::from_utf8_lossy(&body),
            );
            let outcome: serde_json::Value = serde_json::from_slice(&body)
                .assured("the HTTP intake must return a structured endpoint outcome");
            assert_eq!(
                outcome,
                serde_json::json!({ "outcome": "completed", "cause": "none" }),
                "sequence {sequence} must complete its graph acknowledgement",
            );
        }
    });
    futures_util::future::join_all(posts).await;
    world.last_publish_at = Some(nervix_primitives::time::Instant::now());
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
