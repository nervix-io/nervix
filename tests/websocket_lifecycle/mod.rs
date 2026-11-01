//! Public WebSocket source lifecycle regressions.
//!
//! Layer: test harness.
//! - **Owns.** A scenario-local peer, its bounded event channel, opening cancellations,
//!   graceful-close observations, measured reconnect delays, subprotocol answers and measured
//!   ping cadence.
//! - **Depends on.** The scenario world, execution primitives and the WebSocket protocol.
//! - **Must not know.** Connector internals, host retry state or runtime ownership handles.

use std::{net::Ipv4Addr, time::Duration};

use cucumber::{given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::net::{TcpListener, TcpStream};
use peer::PeerEvent;
pub(super) use peer::WebsocketPeer;

use super::{ScenarioWorld, draw_scenario_port, initialize_scenario_identity};
use crate::common::websocket_close::reply_to_close;

mod bounds;
mod peer;
mod pings;
mod subprotocol;

#[given("a WebSocket declaration peer verifies upgrade headers and sends text and binary frames")]
async fn declaration_peer(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    let port = draw_scenario_port(world, "WebSocket declaration peer");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .assured("the scenario's reserved WebSocket port is available");
    world.placeholders.insert(
        "websocket_peer".to_string(),
        format!("ws://127.0.0.1:{port}"),
    );
    world.websocket_peer = Some(WebsocketPeer::declaration(listener));
}

#[given("the WebSocket declaration peer observes a cancelled opening handshake")]
async fn declaration_opening_cancelled(world: &mut ScenarioWorld) {
    use tokio::io::AsyncWriteExt as _;

    let endpoint = url::Url::parse(
        world
            .placeholders
            .get("websocket_peer")
            .assured("the declaration peer has a public endpoint"),
    )
    .assured("the declaration peer publishes a valid endpoint");
    let port = endpoint
        .port()
        .assured("the declaration peer publishes its reserved port");
    let mut opening = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .assured("the abandoned opening reaches the declaration peer");
    opening
        .write_all(b"GET /events HTTP/1.1\r\n")
        .await
        .assured("the peer receives an incomplete upgrade request");
    opening
        .shutdown()
        .await
        .assured("the opening client ends before completing its upgrade");
    drop(opening);
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its declaration peer");
    assert!(matches!(peer.next().await, PeerEvent::OpeningCancelled));
}

#[when("the WebSocket lifecycle peer sends its payload")]
fn peer_sends_payload(world: &mut ScenarioWorld) {
    world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its peer")
        .send_payload();
}

#[given(expr = "a WebSocket lifecycle peer closes its first {int} connections")]
async fn peer_closes_first_connections(world: &mut ScenarioWorld, closes: usize) {
    initialize_scenario_identity(world);
    let port = draw_scenario_port(world, "WebSocket lifecycle peer");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .assured("the scenario's reserved WebSocket port is available");
    world.placeholders.insert(
        "websocket_peer".to_string(),
        format!("ws://127.0.0.1:{port}"),
    );
    world.websocket_peer = Some(WebsocketPeer::start(listener, closes));
}

#[then("the WebSocket lifecycle peer receives a Close frame")]
async fn peer_receives_close(world: &mut ScenarioWorld) {
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its peer");
    loop {
        nervix_primitives::task::consume_budget().await;
        match peer.next().await {
            PeerEvent::ReceivedClose => return,
            PeerEvent::Connected(_) => {}
            PeerEvent::TransportFailure(error) => {
                panic!("WebSocket did not close gracefully: {error}")
            }
            PeerEvent::Ended => panic!("TCP ended without a Close frame"),
            PeerEvent::SentClose(_) => panic!("this peer never initiates a close"),
            PeerEvent::OpeningCancelled => {
                panic!("the lifecycle peer did not complete its upgrade")
            }
            PeerEvent::Upgraded { .. }
            | PeerEvent::SignalingCompleted(_)
            | PeerEvent::ReceivedPing(_) => {
                panic!("the lifecycle peer reports neither subprotocol offers nor pings")
            }
        }
    }
}

#[then("the WebSocket lifecycle peer observes growing reconnect delays")]
async fn peer_observes_reconnect_delays(world: &mut ScenarioWorld) {
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its peer");
    assert!(matches!(peer.next().await, PeerEvent::Connected(_)));
    for minimum in [250, 500, 1000] {
        let PeerEvent::SentClose(closed) = peer.next().await else {
            panic!("the peer must send its scripted Close frame");
        };
        let PeerEvent::Connected(connected) = peer.next().await else {
            panic!("the source must reconnect after the peer closes");
        };
        let elapsed = connected.duration_since(closed);
        assert!(
            elapsed >= Duration::from_millis(minimum),
            "reconnect after {elapsed:?}, expected at least {minimum}ms"
        );
    }
}
