//! Generated distinct-tenant HTTP payloads acknowledged by one node, and their delivery to the
//! observed broker.
//!
//! Layer: test harness.
//! - **Owns.** Generating one payload per tenant, waiting for its public HTTP completion before
//!   posting the next, and asserting that each reaches the observed broker exactly once.
//! - **Depends on.** The cluster's public HTTP addresses, endpoint outcomes and broker observer.
//! - **Must not know.** Branches, relay channels, or the transport that carries them.

use cucumber::{then, when};
use serde_json::Value;

use super::*;

/// How many missing payloads a failure names; the rest are counted.
const MISSING_PAYLOADS_SHOWN: usize = 8;

/// The same response bound as the harness's named endpoint requests, including the response body.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);

/// The tenant names every generating step draws from. A payload whose tenant lacks this prefix,
/// such as a warm-up an earlier step posted more than once, is outside every generation.
const GENERATED_TENANT_PREFIX: &str = "tenant-";

/// One generated payload: the tenant whose branch carries it and the sequence its step gave it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GeneratedPayload {
    tenant: String,
    sequence: i64,
}

impl GeneratedPayload {
    fn new(tenant_index: usize, sequence: i64) -> Self {
        Self {
            tenant: format!("{GENERATED_TENANT_PREFIX}{tenant_index:04}"),
            sequence,
        }
    }

    fn text(&self) -> String {
        serde_json::json!({ "tenant": self.tenant, "seq": self.sequence }).to_string()
    }

    /// Complete this payload's graph acknowledgement before another tenant can add work.
    async fn post_acknowledged(&self, client: &reqwest::Client, uri: &str, host: &str) {
        let response = client
            .post(uri)
            .header(reqwest::header::HOST, host)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(self.text())
            .send()
            .await
            .unwrap_or_else(|error| panic!("failed to post generated payload {self:?}: {error}"));
        let status = response.status();
        let body = response.bytes().await.unwrap_or_else(|error| {
            panic!("failed to read generated payload {self:?} response: {error}")
        });
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "generated payload {self:?} did not complete: {}",
            String::from_utf8_lossy(&body),
        );
        let outcome: Value = serde_json::from_slice(&body).unwrap_or_else(|error| {
            panic!("generated payload {self:?} has an invalid endpoint outcome: {error}")
        });
        assert_eq!(
            outcome,
            serde_json::json!({ "outcome": "completed", "cause": "none" }),
            "generated payload {self:?} must complete its graph acknowledgement",
        );
    }

    /// The generated payload `message` carries, or `None` when it carries one no generating step
    /// posted.
    fn delivered(message: &BrokerMessage) -> Option<Self> {
        let Ok(Value::Object(fields)) = serde_json::from_slice::<Value>(&message.bytes) else {
            return None;
        };
        let Some(Value::String(tenant)) = fields.get("tenant") else {
            return None;
        };
        if !tenant.starts_with(GENERATED_TENANT_PREFIX) {
            return None;
        }
        let sequence = fields.get("seq")?.as_i64()?;
        Some(Self {
            tenant: tenant.clone(),
            sequence,
        })
    }
}

/// The payloads the last generating step posted that no assertion has received yet.
#[derive(Debug, Default)]
pub(super) struct GeneratedPayloads {
    pending: BTreeSet<GeneratedPayload>,
}

#[when(
    expr = "{int} http payloads with distinct tenants and sequence {int} are acknowledged by node \
            {string} with host {string} path {string}"
)]
async fn when_distinct_tenant_payloads_are_acknowledged(
    world: &mut ScenarioWorld,
    count: usize,
    sequence: i64,
    node_id: String,
    host: String,
    path: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let uri = world
        .cluster()
        .http_uri(&node_id, &path)
        .unwrap_or_else(|error| panic!("generated payload endpoint is unavailable: {error}"));
    let client = reqwest::Client::builder()
        .timeout(RESPONSE_TIMEOUT)
        .build()
        .unwrap_or_else(|error| panic!("failed to build generated payload client: {error}"));
    let mut generated = BTreeSet::new();
    for tenant_index in 0..count {
        generated.insert(GeneratedPayload::new(tenant_index, sequence));
    }
    append_cucumber_log_line(&format!(
        "http publish generated: node={node_id} host={host} path={path} tenants={count} \
         sequence={sequence}"
    ));
    // Reuse the connection while the completed response bounds outstanding graph work. The
    // broker assertion still checks delivery independently of the endpoint's acknowledgement.
    for payload in &generated {
        nervix_primitives::task::consume_budget().await;
        payload.post_acknowledged(&client, &uri, &host).await;
    }
    world.last_publish_at = Some(Instant::now());
    world.generated_payloads = GeneratedPayloads { pending: generated };
}

/// Every payload the last generating step posted must arrive exactly once. A payload outside every
/// generation, such as a warm-up an earlier step repeated until one arrived, is not part of this
/// assertion and is passed over; a generated payload that arrives again fails it.
#[then(expr = "within {string} the observed broker receives every generated payload exactly once")]
async fn then_every_generated_payload_arrives_once(world: &mut ScenarioWorld, duration: String) {
    let duration = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let mut pending = std::mem::take(&mut world.generated_payloads.pending);
    assert!(
        !pending.is_empty(),
        "a generating step must post payloads before this assertion"
    );
    let generated = pending.len();
    let mut received = BTreeSet::new();
    let deadline = Instant::now() + duration;
    while !pending.is_empty() {
        nervix_primitives::task::consume_budget().await;
        let now = Instant::now();
        let message = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_message(deadline.saturating_duration_since(now))
            .await
            .expect("failed while waiting for generated broker payloads");
        let Some(message) = message else {
            let missing = pending
                .iter()
                .take(MISSING_PAYLOADS_SHOWN)
                .collect::<Vec<_>>();
            panic!(
                "timed out after {duration:?}: {} of {generated} generated payloads arrived; \
                 missing {} including {missing:?}",
                received.len(),
                pending.len(),
            );
        };
        let Some(delivered) = GeneratedPayload::delivered(&message) else {
            continue;
        };
        assert!(
            pending.remove(&delivered),
            "generated payload {delivered:?} arrived again or was never posted by the last \
             generating step; {} of {generated} had arrived",
            received.len(),
        );
        received.insert(delivered);
    }
}
