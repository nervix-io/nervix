//! Public payload-size and IPv6 listener fixtures.
//!
//! Layer: test harness.
//! - **Owns.** Exact-size transport inputs, IPv6 peer selection and submitted IPC-size assertions.
//! - **Depends on.** Scenario-owned cluster listeners, producers and public subscriptions.
//! - **Must not know.** Server expression projection, connector internals or acknowledgement state.

use std::net::Ipv6Addr;

use super::*;

#[given("HTTP intake listens on IPv6 loopback")]
fn http_intake_uses_ipv6(world: &mut ScenarioWorld) {
    assert!(
        world.cluster.is_none(),
        "intake addressing precedes cluster startup"
    );
    world.cluster_config.http_intake_ip = IpAddr::V6(Ipv6Addr::LOCALHOST);
}

#[given(expr = "node {string} uses IPv6 loopback for Syslog intake")]
fn syslog_intake_uses_ipv6(world: &mut ScenarioWorld, node: String) {
    world
        .cluster
        .as_mut()
        .assured("the scenario starts its cluster before selecting a Syslog listener")
        .set_syslog_ingestor_host(&node, IpAddr::V6(Ipv6Addr::LOCALHOST))
        .assured("the scenario selects one of the nodes it started");
}

#[when(expr = "an IPv6 Syslog UDP message is published to node {string}")]
async fn publish_ipv6_syslog(world: &mut ScenarioWorld, node: String, #[step] step: &Step) {
    let address = world
        .cluster()
        .syslog_ingestor_addr(&node, &world.syslog_ingest_addr())
        .assured("the scenario selects one of the nodes it started");
    let payload = expand_placeholders(world, docstring(step));
    let socket = nervix_primitives::net::UdpSocket::bind((Ipv6Addr::LOCALHOST, 0))
        .await
        .assured("IPv6 loopback is available to the public scenario");
    let sent = socket
        .send_to(payload.as_bytes(), &address)
        .await
        .assured("the scenario's IPv6 Syslog listener accepts the datagram");
    assert_eq!(
        sent,
        payload.len(),
        "the complete frame reaches its transport"
    );
}

#[when(
    expr = "HTTP event {int} of {int} bytes is posted to node {string} host {string} path {string}"
)]
async fn publish_sized_http(
    world: &mut ScenarioWorld,
    sequence: i64,
    bytes: usize,
    node: String,
    host: String,
    path: String,
) {
    let mut payload = format!("{{\"seq\":{sequence}}}");
    let padding = bytes
        .checked_sub(payload.len())
        .assured("the scenario's byte count holds its JSON event");
    payload.extend(std::iter::repeat_n(' ', padding));
    let host = expand_placeholders(world, &host);
    world
        .cluster()
        .publish_http(&node, &host, &path, &payload)
        .await
        .assured("the public HTTP listener accepts the exact-size event");
}

#[then(
    expr = "the last relay subscription field {string} equals the IPC byte count of batch {string}"
)]
fn assert_submitted_ipc_bytes(world: &mut ScenarioWorld, field: String, batch: String) {
    let expected: u64 = client_producers::submitted_bytes(world, &batch).arch_into();
    let payload = world
        .last_subscription_payload
        .as_deref()
        .assured("the scenario receives an output row before inspecting its payload facts");
    let row: serde_json::Value =
        serde_json::from_str(payload).assured("the public subscription carries a JSON row");
    assert_eq!(
        row.get(&field).and_then(serde_json::Value::as_u64),
        Some(expected),
        "the projected byte count equals the actual submitted IPC body: {row}"
    );
}
