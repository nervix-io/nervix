//! The scenario-local WebSocket peer and its public transport observations.
//!
//! Layer: test harness.
//! - **Owns.** Listening connections, bounded events, opening cancellation, payload release,
//!   graceful-close observations and measured reconnect instants.
//! - **Depends on.** Execution primitives, I/O errors and the WebSocket protocol.
//! - **Must not know.** Scenario state, connector internals or runtime ownership handles.

use std::{io, time::Duration};

use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::AbortOnDropHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::tungstenite::{
    Error as WebsocketError, Message,
    error::ProtocolError,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

const PEER_WAIT: Duration = Duration::from_secs(60);
pub(crate) struct WebsocketPeer {
    events: mpsc::Receiver<PeerEvent>,
    payload: Option<oneshot::Sender<()>>,
    task: AbortOnDropHandle<io::Result<()>>,
}

pub(super) enum PeerEvent {
    OpeningCancelled,
    Connected(Instant),
    SentClose(Instant),
    ReceivedClose,
    TransportFailure(tokio_tungstenite::tungstenite::Error),
    Ended,
}

impl WebsocketPeer {
    pub(super) fn declaration(listener: TcpListener) -> Self {
        let (events_tx, events) = mpsc::channel(64);
        let (payload_tx, payload_rx) = oneshot::channel();
        let mut payload_release = Some(payload_rx);
        let task = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let (stream, _) = listener.accept().await?;
                #[expect(clippy::result_large_err, reason = "tungstenite requires an unboxed HTTP rejection response in its handshake callback")]
                let verify_upgrade = |request: &tokio_tungstenite::tungstenite::handshake::server::Request, mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    if request.headers().get("authorization") != Some(&tokio_tungstenite::tungstenite::http::HeaderValue::from_static("Bearer fixture")) {
                        return Err(tokio_tungstenite::tungstenite::http::Response::builder().status(401).body(Some("missing fixture authorization".to_string())).assured("401 and a string body form a response"));
                    }
                    response.headers_mut().append("x-upgrade", "ready".parse().assured("ready is a valid header value"));
                    Ok(response)
                };
                let mut socket =
                    match tokio_tungstenite::accept_hdr_async(stream, verify_upgrade).await {
                        Ok(socket) => socket,
                        Err(WebsocketError::Protocol(ProtocolError::HandshakeIncomplete)) => {
                            // Source replacement can cancel an opening before it becomes a WebSocket.
                            // Keep the pending payload for the next completed upgrade.
                            if events_tx.send(PeerEvent::OpeningCancelled).await.is_err() {
                                return Ok(());
                            }
                            continue;
                        }
                        Err(error) => return Err(io::Error::other(error)),
                    };
                if let Some(release) = payload_release.take() {
                    if release.await.is_err() {
                        return Ok(());
                    }
                    socket
                        .send(Message::Text("{\"user_id\":42}".into()))
                        .await
                        .map_err(io::Error::other)?;
                    socket
                        .send(Message::Binary(b"{\"user_id\":43}".to_vec()))
                        .await
                        .map_err(io::Error::other)?;
                }
                while let Some(message) = socket.next().await {
                    nervix_primitives::task::consume_budget().await;
                    match message.map_err(io::Error::other)? {
                        Message::Close(_) => {
                            socket.flush().await.map_err(io::Error::other)?;
                            if events_tx.send(PeerEvent::ReceivedClose).await.is_err() {
                                return Ok(());
                            }
                            break;
                        }
                        _ => socket.flush().await.map_err(io::Error::other)?,
                    }
                }
            }
        });
        Self {
            events,
            payload: Some(payload_tx),
            task: AbortOnDropHandle::new(task),
        }
    }

    pub(super) fn start(listener: TcpListener, mut closes: usize) -> Self {
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

    pub(super) fn send_payload(&mut self) {
        self.payload
            .take()
            .assured("the scenario releases its payload once")
            .send(())
            .assured("the peer waits for its payload to be released");
    }

    pub(crate) async fn stop(self) {
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

    pub(super) async fn next(&mut self) -> PeerEvent {
        timeout(PEER_WAIT, self.events.recv())
            .await
            .assured("the WebSocket lifecycle peer makes progress within its fixture budget")
            .assured("the WebSocket lifecycle peer task remains alive")
    }
}
