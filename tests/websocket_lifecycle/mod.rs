//! Public WebSocket source lifecycle regressions.
//!
//! Layer: test harness.
//! - **Owns.** A scenario-local peer, its bounded event channel, graceful-close observations and
//!   measured reconnect delays.
//! - **Depends on.** The scenario world, execution primitives and the WebSocket protocol.
//! - **Must not know.** Connector internals, host retry state or runtime ownership handles.

use std::{io, net::Ipv4Addr, time::Duration};

use cucumber::{given, then, when};
use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::AbortOnDropHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

use super::{ScenarioWorld, draw_scenario_port, initialize_scenario_identity};

const PEER_WAIT: Duration = Duration::from_secs(60);

pub(super) struct WebsocketPeer {
    events: mpsc::Receiver<PeerEvent>,
    payload: Option<oneshot::Sender<()>>,
    task: AbortOnDropHandle<io::Result<()>>,
}

enum PeerEvent {
    Connected(Instant),
    SentClose(Instant),
    ReceivedClose,
    TransportFailure(tokio_tungstenite::tungstenite::Error),
    Ended,
}

impl WebsocketPeer {
    fn start(listener: TcpListener, mut closes: usize) -> Self {
        let (events_tx, events) = mpsc::channel(64);
        let (payload_tx, payload_rx) = oneshot::channel();
        let mut payload_release = if closes > 0 { Some(payload_rx) } else { None };
        let task = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let (stream, _) = listener.accept().await?;
                let mut socket = tokio_tungstenite::accept_async(stream)
                    .await
                    .map_err(io::Error::other)?;
                if events_tx
                    .send(PeerEvent::Connected(Instant::now()))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                if closes > 0 {
                    closes -= 1;
                    let at = Instant::now();
                    socket
                        .close(Some(CloseFrame {
                            code: CloseCode::Again,
                            reason: "fixture restart".into(),
                        }))
                        .await
                        .map_err(io::Error::other)?;
                    if events_tx.send(PeerEvent::SentClose(at)).await.is_err() {
                        return Ok(());
                    }
                    // The client can acknowledge the Close or end TCP. Neither observation is
                    // used as a timing gate: the next connection is timed from the Close sent.
                    match timeout(PEER_WAIT, socket.next()).await {
                        Ok(_) => {}
                        Err(error) => return Err(io::Error::other(error)),
                    }
                    continue;
                }
                if let Some(release) = payload_release.take()
                    && release.await.is_err()
                {
                    return Ok(());
                }
                socket
                    .send(Message::Text("{\"user_id\":42}".into()))
                    .await
                    .map_err(io::Error::other)?;
                loop {
                    nervix_primitives::task::consume_budget().await;
                    let event = match socket.next().await {
                        Some(Ok(Message::Close(_))) => {
                            match socket.flush().await {
                                Ok(())
                                | Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) => {}
                                Err(error) => return Err(io::Error::other(error)),
                            }
                            PeerEvent::ReceivedClose
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(error)) => PeerEvent::TransportFailure(error),
                        None => PeerEvent::Ended,
                    };
                    if events_tx.send(event).await.is_err() {
                        return Ok(());
                    }
                    break;
                }
            }
        });
        Self {
            events,
            payload: Some(payload_tx),
            task: AbortOnDropHandle::new(task),
        }
    }

    pub(super) async fn stop(self) {
        self.task.abort();
        match timeout(PEER_WAIT, self.task)
            .await
            .assured("the peer task observes cancellation")
        {
            Err(error) if error.is_cancelled() => {}
            Ok(Ok(())) => {}
            outcome => panic!("the WebSocket fixture failed: {outcome:?}"),
        }
    }

    async fn next(&mut self) -> PeerEvent {
        timeout(PEER_WAIT, self.events.recv())
            .await
            .assured("the WebSocket lifecycle peer makes progress within its fixture budget")
            .assured("the WebSocket lifecycle peer task remains alive")
    }
}

#[when("the WebSocket lifecycle peer sends its payload")]
fn peer_sends_payload(world: &mut ScenarioWorld) {
    world
        .websocket_peer
        .as_mut()
        .assured("the scenario started its peer")
        .payload
        .take()
        .assured("the scenario releases its payload once")
        .send(())
        .assured("the peer waits for its payload to be released");
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
