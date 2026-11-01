//! A peer that answers the subprotocol offer of an outbound WebSocket connection.
//!
//! Layer: test harness.
//! - **Owns.** The scripted answer to each opening upgrade's `sec-websocket-protocol` offer and
//!   the offer the peer read, observed through real sockets.
//! - **Depends on.** The scenario-local peer and public Plexus statements.
//! - **Must not know.** The connector's handshake, its failure types or the host's retry state.

use super::{peer::SubprotocolAnswer, *};

async fn start_subprotocol_peer(world: &mut ScenarioWorld, answer: SubprotocolAnswer) {
    initialize_scenario_identity(world);
    let port = draw_scenario_port(world, "WebSocket subprotocol peer");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .assured("the scenario's reserved WebSocket port is available");
    world.placeholders.insert(
        "websocket_peer".to_string(),
        format!("ws://127.0.0.1:{port}"),
    );
    world.websocket_peer = Some(WebsocketPeer::subprotocol(listener, answer));
}

#[given(expr = "a WebSocket subprotocol peer selects {string}")]
async fn peer_selects(world: &mut ScenarioWorld, protocol: String) {
    start_subprotocol_peer(world, SubprotocolAnswer::Select(protocol)).await;
}

#[given("a WebSocket subprotocol peer selects no subprotocol")]
async fn peer_selects_nothing(world: &mut ScenarioWorld) {
    start_subprotocol_peer(world, SubprotocolAnswer::Nothing).await;
}

#[then(expr = "the WebSocket subprotocol peer receives the offer {string}")]
async fn peer_receives_offer(world: &mut ScenarioWorld, expected: String) {
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its subprotocol peer");
    let PeerEvent::Upgraded { offer } = peer.next().await else {
        panic!("the subprotocol peer reports each completed upgrade");
    };
    assert_eq!(
        offer.as_deref(),
        Some(expected.as_str()),
        "the opening upgrade must carry the offer as written to the peer"
    );
}
