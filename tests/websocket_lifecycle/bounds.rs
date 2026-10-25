//! Peers that expose the outbound source's handshake and idle bounds.
//!
//! Layer: test harness.
//! - **Owns.** A bounded handshake flood and a silent connection, observed through real sockets.
//! - **Depends on.** The scenario-local peer and public Plexus statements.
//! - **Must not know.** Connector queues, connection owners or the host's retry implementation.

use super::*;

#[given(expr = "a WebSocket bounds peer {word}")]
async fn bounds_peer(world: &mut ScenarioWorld, behavior: String) {
    initialize_scenario_identity(world);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the operating system assigns an available peer port");
    let port = listener
        .local_addr()
        .assured("the peer listener is bound")
        .port();
    world.placeholders.insert(
        "websocket_peer".to_string(),
        format!("ws://127.0.0.1:{port}"),
    );
    let peer = match behavior.as_str() {
        "floods" => WebsocketPeer::handshake_flood(listener, 32),
        "negotiates" => WebsocketPeer::handshake_flood(listener, 2),
        "stalls" => WebsocketPeer::silent_connection(listener),
        "blackholes" => WebsocketPeer::idle_blackhole(listener),
        _ => panic!("the bounds fixture declares floods, negotiates, stalls or blackholes"),
    };
    world.websocket_peer = Some(peer);
}

#[then("the WebSocket bounds peer observes a reconnect after the idle bound")]
async fn idle_reconnect(world: &mut ScenarioWorld) {
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its bounds peer");
    let PeerEvent::Connected(first) = peer.next().await else {
        panic!("the first connection was upgraded");
    };
    let PeerEvent::Connected(second) = peer.next().await else {
        panic!("a silent connection must be replaced");
    };
    assert!(
        second.duration_since(first) >= Duration::from_millis(300),
        "a 100ms ping interval allows three intervals before declaring silence"
    );
}
