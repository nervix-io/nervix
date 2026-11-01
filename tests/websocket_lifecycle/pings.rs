//! A peer that measures the cadence of an outbound WebSocket connection's pings.
//!
//! Layer: test harness.
//! - **Owns.** A held signaling handshake and the measured arrival of each client ping, observed
//!   through real sockets.
//! - **Depends on.** The scenario-local peer and public Plexus statements.
//! - **Must not know.** The connector's ping timer, its connection owner or the host's read loop.

use super::*;

#[given(expr = "a WebSocket ping peer holds signaling open for {int}ms")]
async fn ping_peer(world: &mut ScenarioWorld, hold: u64) {
    initialize_scenario_identity(world);
    let port = draw_scenario_port(world, "WebSocket ping peer");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .assured("the scenario's reserved WebSocket port is available");
    world.placeholders.insert(
        "websocket_peer".to_string(),
        format!("ws://127.0.0.1:{port}"),
    );
    world.websocket_peer = Some(WebsocketPeer::ping_cadence(
        listener,
        Duration::from_millis(hold),
    ));
}

/// Load delays a ping's write and its observation, never makes one earlier, so the bound is a
/// lower bound measured from the instant before the peer answered signaling: the connection
/// cannot start its ping timer before reading that answer, and its k-th ping waits at least k
/// intervals from then.
#[then(
    expr = "the WebSocket ping peer receives {int} pings at least {int}ms apart once signaling \
            completes"
)]
async fn pings_follow_signaling(world: &mut ScenarioWorld, pings: u32, interval: u64) {
    let peer = world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its ping peer");
    let completed = loop {
        nervix_primitives::task::consume_budget().await;
        match peer.next().await {
            PeerEvent::Connected(_) => {}
            PeerEvent::SignalingCompleted(at) => break at,
            PeerEvent::ReceivedPing(_) => {
                panic!("a client ping arrived while the peer still held signaling open")
            }
            PeerEvent::TransportFailure(error) => {
                panic!("the connection failed during signaling: {error}")
            }
            PeerEvent::Ended | PeerEvent::ReceivedClose => {
                panic!("the connection ended during signaling")
            }
            PeerEvent::OpeningCancelled | PeerEvent::SentClose(_) | PeerEvent::Upgraded { .. } => {
                panic!("the ping peer reports only its connection, signaling and pings")
            }
        }
    };
    let interval = Duration::from_millis(interval);
    for ping in 1..=pings {
        nervix_primitives::task::consume_budget().await;
        let PeerEvent::ReceivedPing(arrived) = peer.next().await else {
            panic!("the connection must keep pinging after signaling completes");
        };
        let earliest = interval
            .checked_mul(ping)
            .assured("a scenario counts a few pings of a short interval");
        let elapsed = arrived.duration_since(completed);
        assert!(
            elapsed >= earliest,
            "ping {ping} arrived {elapsed:?} after signaling completed; a {interval:?} interval \
             allows it no earlier than {earliest:?}"
        );
    }
}
