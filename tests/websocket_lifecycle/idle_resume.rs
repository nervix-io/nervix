//! Public evidence for a connection whose host spends its idle budget handing a payload off.
//!
//! Layer: test harness.
//! - **Owns.** Holding real decoder workers, observing a queued payload, and releasing it after
//!   the declared idle bound while a responsive protocol peer keeps its original connection.
//! - **Depends on.** The scenario world, bounded-executor fault controls and public WebSockets.
//! - **Must not know.** Connector activity state or the implementation of the idle verdict.

use std::time::Duration;

use cucumber::{given, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::CpuClass;
use nervix_models::ClusterNodeName;
use nervix_primitives::{
    net::TcpListener,
    time::{Instant, sleep, sleep_until, timeout},
};
use nervix_server::FaultInjection;

use super::{ScenarioWorld, draw_scenario_port, initialize_scenario_identity};

mod peer;
pub(crate) use peer::HandoffPeer;

#[given(expr = "a responsive WebSocket handoff peer with {word} idle bound")]
async fn responsive_handoff_peer(world: &mut ScenarioWorld, idle: String) {
    initialize_scenario_identity(world);
    let configured = match idle.as_str() {
        "derived" => "",
        "configured" => ", 'idle_timeout_ms' = '3000'",
        _ => panic!("the handoff regression declares derived or configured idle time"),
    };
    world
        .placeholders
        .insert("websocket_idle_config".to_string(), configured.to_string());
    let port = draw_scenario_port(world, "responsive WebSocket handoff peer");
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
        .await
        .assured("the scenario's reserved WebSocket port is available");
    world.placeholders.insert(
        "websocket_peer".to_string(),
        format!("ws://127.0.0.1:{port}"),
    );
    world.websocket_handoff_peer = Some(HandoffPeer::start(listener));
}

/// Releasing in Drop keeps a failed observation from leaving the cluster's extension workers
/// parked during teardown. Each node belongs to this one scenario.
struct HeldDecoders {
    faults: FaultInjection,
    nodes: Vec<ClusterNodeName>,
}

impl Drop for HeldDecoders {
    fn drop(&mut self) {
        for node in &self.nodes {
            self.faults.release_execution(node);
        }
    }
}

#[when("the WebSocket payload handoff is held beyond its three-second idle bound")]
async fn hold_payload_handoff(world: &mut ScenarioWorld) {
    let nodes = world
        .cluster()
        .node_ids()
        .iter()
        .map(|node| crate::common::cluster::node_name(node))
        .collect();
    let held = HeldDecoders {
        faults: world.fault_injection.clone(),
        nodes,
    };
    for node in &held.nodes {
        timeout(
            Duration::from_secs(60),
            held.faults.occupy_execution(node, CpuClass::Extension),
        )
        .await
        .assured("the decoder's extension workers are occupied within the fixture budget");
    }
    let peer = world
        .websocket_handoff_peer
        .as_mut()
        .assured("the scenario started its peer");
    peer.send_payload(42);
    peer.payload_sent(42).await;

    let observation_deadline = Instant::now()
        .checked_add(Duration::from_secs(60))
        .assured("the fixture budget fits a monotonic instant");
    loop {
        nervix_primitives::task::consume_budget().await;
        let queued = held.nodes.iter().any(|node| {
            held.faults
                .executor_on(node)
                .snapshot()
                .cpu_class(CpuClass::Extension)
                .pending
                > 0
        });
        if queued {
            break;
        }
        assert!(
            Instant::now() < observation_deadline,
            "the WebSocket payload never queued for its decoder"
        );
        sleep(Duration::from_millis(20)).await;
    }
    // This delay is the scenario's trigger, measured from a real queued handoff. It exceeds the
    // three-second idle bound, and the occupied workers make the host unable to start a read.
    let release_at = Instant::now()
        .checked_add(Duration::from_secs(4))
        .assured("the handoff hold fits a monotonic instant");
    sleep_until(release_at).await;
    drop(held);
    peer.send_after_ping(43);
    peer.payload_sent(43).await;
}
