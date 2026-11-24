//! Public observations of endpoint request completion.
//!
//! Layer: test harness.
//! - **Owns.** Named HTTP requests, their response observations and a causal response boundary.
//! - **Depends on.** Public HTTP listeners, the client outcome vocabulary and scenario fixtures.
//! - **Must not know.** Intake windows, ACK roots or runtime routing internals.

use std::{collections::BTreeMap, time::Duration};

use cucumber::{then, when};
use meticulous::OptionExt as _;
use nervix_primitives::{
    task::{AbortOnDropHandle, spawn},
    time::{Instant, timeout},
};

use crate::{ScenarioWorld, docstring, expand_placeholders};

const RESPONSE_OBSERVATION: Duration = Duration::from_secs(60);

struct EndpointHttpResponse {
    status: u16,
    body: Vec<u8>,
    retry_after: Option<String>,
    received_at: Instant,
}

enum RequestState {
    Pending(AbortOnDropHandle<EndpointHttpResponse>),
    Complete(EndpointHttpResponse),
}

#[derive(Default)]
pub(crate) struct EndpointRequests {
    requests: BTreeMap<String, RequestState>,
    boundary: Option<Instant>,
}

impl EndpointRequests {
    fn post(&mut self, name: String, uri: String, host: String, payload: String) {
        let task = spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(RESPONSE_OBSERVATION)
                .build()
                .unwrap_or_else(|error| panic!("failed to build endpoint observer: {error}"));
            let response = client
                .post(uri)
                .header(reqwest::header::HOST, host)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(payload)
                .send()
                .await
                .unwrap_or_else(|error| panic!("endpoint request failed: {error}"));
            let received_at = Instant::now();
            let status = response.status().as_u16();
            let retry_after = match response.headers().get(reqwest::header::RETRY_AFTER) {
                Some(value) => {
                    let value = value
                        .to_str()
                        .unwrap_or_else(|error| panic!("invalid Retry-After: {error}"));
                    Some(value.to_owned())
                }
                None => None,
            };
            let body = response
                .bytes()
                .await
                .unwrap_or_else(|error| panic!("endpoint response body failed: {error}"))
                .to_vec();
            EndpointHttpResponse {
                status,
                body,
                retry_after,
                received_at,
            }
        });
        assert!(
            self.requests
                .insert(name, RequestState::Pending(AbortOnDropHandle::new(task)))
                .is_none(),
            "each endpoint request has its own scenario name"
        );
    }

    async fn collect(&mut self, name: &str) -> &EndpointHttpResponse {
        let state = self
            .requests
            .remove(name)
            .unwrap_or_else(|| panic!("endpoint request '{name}' was not started"));
        let response = match state {
            RequestState::Pending(task) => {
                let result = timeout(RESPONSE_OBSERVATION, task)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("endpoint request '{name}' did not answer: {error}")
                    });
                result.unwrap_or_else(|error| panic!("endpoint observer '{name}' failed: {error}"))
            }
            RequestState::Complete(response) => response,
        };
        self.requests
            .insert(name.to_owned(), RequestState::Complete(response));
        let Some(RequestState::Complete(response)) = self.requests.get(name) else {
            panic!("the response was inserted immediately above");
        };
        response
    }
}

#[when(expr = "endpoint request {string} is posted on node {string} host {string} path {string}")]
async fn post(
    world: &mut ScenarioWorld,
    name: String,
    node: String,
    host: String,
    path: String,
    #[step] step: &cucumber::gherkin::Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let uri = world
        .cluster()
        .http_uri(&node, &path)
        .unwrap_or_else(|error| panic!("endpoint node '{node}' is unavailable: {error}"));
    world.endpoint_requests.post(name, uri, host, payload);
}

/// The request is still held: the node has not answered it within `duration`, which leaves the
/// request outstanding for a later observation.
#[then(expr = "endpoint request {string} has not answered within {string}")]
async fn has_not_answered_within(world: &mut ScenarioWorld, name: String, duration: String) {
    let duration = nervix_models::parse_duration_text(&duration)
        .expect("step duration must be a valid duration");
    nervix_primitives::time::sleep(duration).await;
    match world.endpoint_requests.requests.get(&name) {
        Some(RequestState::Pending(task)) => assert!(
            !task.is_finished(),
            "endpoint request '{name}' answered within {duration:?} while the node held it"
        ),
        Some(RequestState::Complete(_)) => {
            panic!("endpoint request '{name}' was already observed answered")
        }
        None => panic!("endpoint request '{name}' was not started"),
    }
}

#[when(expr = "endpoint request {string} disconnects")]
#[then(expr = "endpoint request {string} disconnects")]
async fn disconnect(world: &mut ScenarioWorld, name: String) {
    let request = world
        .endpoint_requests
        .requests
        .remove(&name)
        .assured("the scenario posted this endpoint request");
    assert!(matches!(request, RequestState::Pending(_)));
    drop(request);
}

#[when("the endpoint response boundary is recorded")]
async fn record_boundary(world: &mut ScenarioWorld) {
    world.endpoint_requests.boundary = Some(Instant::now());
}

#[then(expr = "endpoint request {string} answers HTTP {int} with {string} cause {string}")]
async fn answers(
    world: &mut ScenarioWorld,
    name: String,
    status: u16,
    class: String,
    cause: String,
) {
    let response = world.endpoint_requests.collect(&name).await;
    assert_eq!(response.status, status, "endpoint request '{name}'");
    let outcome: serde_json::Value =
        serde_json::from_slice(&response.body).unwrap_or_else(|error| {
            panic!("endpoint did not encode the client outcome vocabulary: {error}")
        });
    assert_eq!(
        outcome,
        serde_json::json!({ "outcome": class, "cause": cause })
    );
}

#[then(expr = "endpoint request {string} answers HTTP {int}")]
async fn answers_status(world: &mut ScenarioWorld, name: String, status: u16) {
    let response = world.endpoint_requests.collect(&name).await;
    assert_eq!(response.status, status, "endpoint request '{name}'");
}

#[then(expr = "endpoint request {string} answers after the recorded response boundary")]
async fn after_boundary(world: &mut ScenarioWorld, name: String) {
    let boundary = world
        .endpoint_requests
        .boundary
        .assured("the scenario recorded its response boundary");
    let response = world.endpoint_requests.collect(&name).await;
    assert!(
        response.received_at >= boundary,
        "the endpoint answered before downstream acknowledgement was released"
    );
}

#[then(expr = "endpoint request {string} answers before the recorded response boundary")]
async fn before_boundary(world: &mut ScenarioWorld, name: String) {
    let boundary = world
        .endpoint_requests
        .boundary
        .assured("the scenario recorded its response boundary");
    let response = world.endpoint_requests.collect(&name).await;
    assert!(
        response.received_at <= boundary,
        "the endpoint answer was still waiting"
    );
}

#[then(expr = "endpoint request {string} asks to retry after {string}")]
async fn retry_after(world: &mut ScenarioWorld, name: String, expected: String) {
    let response = world.endpoint_requests.collect(&name).await;
    assert_eq!(response.retry_after.as_deref(), Some(expected.as_str()));
}

#[then(expr = "endpoint request {string} supplies no retry delay")]
async fn no_retry_after(world: &mut ScenarioWorld, name: String) {
    let response = world.endpoint_requests.collect(&name).await;
    assert_eq!(response.retry_after, None);
}
