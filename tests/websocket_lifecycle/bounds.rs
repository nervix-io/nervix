//! Peers that expose the outbound source's handshake, idle and message bounds.
//!
//! Layer: test harness.
//! - **Owns.** A bounded handshake flood, a signaling handshake refused or left unanswered, a
//!   silent connection and a message above the client's limit, observed through real sockets,
//!   with how the client ended each connection it gave up.
//! - **Depends on.** The scenario-local peer and public Plexus statements.
//! - **Must not know.** Connector queues, connection owners or the host's retry implementation.

use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::{
    peer::{FirstHandshake, UnreadEnding},
    *,
};

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

#[given(expr = "a WebSocket bounds peer first sends a {int}-byte message")]
async fn oversized_message_peer(world: &mut ScenarioWorld, size: usize) {
    initialize_scenario_identity(world);
    let port = draw_scenario_port(world, "WebSocket oversized-message peer");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .assured("the scenario's reserved WebSocket port is available");
    world.placeholders.insert(
        "websocket_peer".to_string(),
        format!("ws://127.0.0.1:{port}"),
    );
    world.websocket_peer = Some(WebsocketPeer::oversized_message(listener, size));
}

#[given(expr = "a WebSocket bounds peer {word} its first signaling handshake")]
async fn failed_handshake_peer(world: &mut ScenarioWorld, behavior: String) {
    let first = match behavior.as_str() {
        "refuses" => FirstHandshake::Refused,
        "stalls" => FirstHandshake::Stalled,
        _ => panic!("the failed-handshake fixture declares refuses or stalls"),
    };
    initialize_scenario_identity(world);
    let port = draw_scenario_port(world, "WebSocket failed-handshake peer");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .assured("the scenario's reserved WebSocket port is available");
    world.placeholders.insert(
        "websocket_peer".to_string(),
        format!("ws://127.0.0.1:{port}"),
    );
    world.websocket_peer = Some(WebsocketPeer::failed_handshake(listener, first));
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

/// Requires the client to have ended `connection`, a connection the peer never read, with a
/// Close frame carrying `status` before TCP ended.
fn assert_closed_with(ending: UnreadEnding, status: CloseCode, connection: &str) {
    match ending {
        UnreadEnding::Closed(Some(frame)) => assert_eq!(
            frame.code, status,
            "the client ends the {connection} connection with {status:?}, not with {frame:?}"
        ),
        UnreadEnding::Closed(None) => {
            panic!("the client closed the {connection} connection without a status")
        }
        UnreadEnding::WithoutClose => {
            panic!("the client ended the {connection} connection without a Close frame")
        }
    }
}

#[then("the WebSocket bounds peer's silent connection ends with a going-away Close frame")]
async fn silent_connection_closed(world: &mut ScenarioWorld) {
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its bounds peer");
    let PeerEvent::UnreadConnectionEnded(ending) = peer.next().await else {
        panic!("the bounds peer reports how the client ended the connection it never read");
    };
    assert_closed_with(ending, CloseCode::Away, "silent");
}

#[then(
    "the WebSocket bounds peer's oversized connection ends with a message-too-big Close frame \
     before the client reconnects"
)]
async fn oversized_connection_closed(world: &mut ScenarioWorld) {
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its bounds peer");
    let PeerEvent::Connected(_) = peer.next().await else {
        panic!("the first connection was upgraded");
    };
    let PeerEvent::UnreadConnectionEnded(ending) = peer.next().await else {
        panic!("the oversized connection ends before the client reconnects");
    };
    assert_closed_with(ending, CloseCode::Size, "oversized");
    let PeerEvent::Connected(_) = peer.next().await else {
        panic!("the client reconnects after the oversized message ends its connection");
    };
}

#[then(
    expr = "the WebSocket bounds peer's {word} handshake ends its connection with Close status \
            {int} before the client reconnects"
)]
async fn failed_handshake_closed(world: &mut ScenarioWorld, handshake: String, status: u16) {
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its bounds peer");
    let PeerEvent::Connected(_) = peer.next().await else {
        panic!("the first connection was upgraded");
    };
    let PeerEvent::UnreadConnectionEnded(ending) = peer.next().await else {
        panic!("the {handshake} handshake's connection ends before the client reconnects");
    };
    assert_closed_with(
        ending,
        CloseCode::from(status),
        &format!("{handshake} handshake's"),
    );
    let PeerEvent::Connected(_) = peer.next().await else {
        panic!("the client reconnects after its {handshake} handshake ends the connection");
    };
}
