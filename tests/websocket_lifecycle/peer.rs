//! The scenario-local WebSocket peer and its public transport observations.
//!
//! Layer: test harness.
//! - **Owns.** Listening connections, bounded events, opening cancellation, public connection
//!   confirmation, payload release, graceful-close observations and measured reconnect instants.
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
const DECLARATION_PROBE: &[u8] = b"nervix declaration peer";

enum Opening {
    Confirmed,
    Cancelled,
}

fn opening_failure(error: WebsocketError) -> io::Result<Opening> {
    match &error {
        WebsocketError::ConnectionClosed
        | WebsocketError::AlreadyClosed
        | WebsocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
            Ok(Opening::Cancelled)
        }
        WebsocketError::Io(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::UnexpectedEof
            ) =>
        {
            Ok(Opening::Cancelled)
        }
        _ => Err(io::Error::other(error)),
    }
}

/// A written HTTP 101 does not prove the client completed its side of the upgrade. Its opening
/// can still be cancelled there. A matching Pong proves it read WebSocket frames before the peer
/// consumes the one payload gate; failures after that confirmation remain fixture failures.
async fn confirm_opening(
    socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
) -> io::Result<Opening> {
    timeout(PEER_WAIT, async {
        if let Err(error) = socket.send(Message::Ping(DECLARATION_PROBE.to_vec())).await {
            return opening_failure(error);
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            match socket.next().await {
                Some(Ok(Message::Pong(payload))) if payload == DECLARATION_PROBE => {
                    return Ok(Opening::Confirmed);
                }
                Some(Ok(Message::Close(_))) => {
                    if let Err(error) = socket.flush().await {
                        return opening_failure(error);
                    }
                    return Ok(Opening::Cancelled);
                }
                Some(Ok(_)) => {
                    if let Err(error) = socket.flush().await {
                        return opening_failure(error);
                    }
                }
                Some(Err(error)) => return opening_failure(error),
                None => return Ok(Opening::Cancelled),
            }
        }
    })
    .await
    .map_err(io::Error::other)?
}

pub(crate) struct WebsocketPeer {
    events: mpsc::Receiver<PeerEvent>,
    payload: Option<oneshot::Sender<()>>,
    task: AbortOnDropHandle<io::Result<()>>,
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        net::{Ipv4Addr, SocketAddr},
    };

    use futures_util::{SinkExt as _, StreamExt as _};
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_primitives::{
        net::{TcpListener, TcpStream},
        time::timeout,
    };
    use tokio_tungstenite::{WebSocketStream, tungstenite::client::IntoClientRequest as _};

    use super::{Message, PEER_WAIT, PeerEvent, ProtocolError, WebsocketError, WebsocketPeer};

    async fn connect(address: SocketAddr) -> WebSocketStream<TcpStream> {
        let mut request = format!("ws://{address}/events")
            .into_client_request()
            .assured("the bound fixture publishes a WebSocket endpoint");
        request.headers_mut().insert(
            "authorization",
            "Bearer fixture"
                .parse()
                .assured("the fixture token is a header value"),
        );
        let stream = TcpStream::connect(address)
            .await
            .assured("the client reaches the listening fixture");
        let (socket, response) = tokio_tungstenite::client_async(request, stream)
            .await
            .assured("the fixture accepts its authorized upgrade");
        assert_eq!(
            response
                .headers()
                .get("x-upgrade")
                .assured("the fixture adds its response header"),
            "ready"
        );
        socket
    }

    async fn answer_probe(socket: &mut WebSocketStream<TcpStream>) {
        assert!(matches!(
            socket
                .next()
                .await
                .assured("the fixture sends its probe")
                .assured("the probe is a valid frame"),
            Message::Ping(_)
        ));
        socket
            .flush()
            .await
            .assured("the client flushes its queued Pong");
    }

    async fn receive_payload(socket: &mut WebSocketStream<TcpStream>) {
        let text = socket
            .next()
            .await
            .assured("the text payload arrives")
            .assured("the text frame is valid");
        let binary = socket
            .next()
            .await
            .assured("the binary payload arrives")
            .assured("the binary frame is valid");
        assert!(matches!(text, Message::Text(ref value) if value == "{\"user_id\":42}"));
        assert!(matches!(binary, Message::Binary(ref value) if value == b"{\"user_id\":43}"));
    }

    #[nervix_primitives::test]
    async fn an_upgraded_opening_can_end_before_its_probe_without_taking_the_payload() {
        timeout(PEER_WAIT, async {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .await
                .assured("the fixture binds");
            let address = listener.local_addr().assured("the listener has an address");
            let mut peer = WebsocketPeer::declaration(listener);
            let opening = connect(address).await;
            drop(opening);
            assert!(matches!(peer.next().await, PeerEvent::OpeningCancelled));

            let mut socket = connect(address).await;
            answer_probe(&mut socket).await;
            peer.send_payload();
            receive_payload(&mut socket).await;
            socket.close(None).await.assured("the client sends Close");
            assert!(matches!(
                socket
                    .next()
                    .await
                    .assured("the peer acknowledges the client Close")
                    .assured("the Close acknowledgement is a valid frame"),
                Message::Close(_)
            ));
            assert!(matches!(peer.next().await, PeerEvent::ReceivedClose));
            peer.stop().await;
        })
        .await
        .assured("the fixture qualifies within its existing budget");
    }

    #[nervix_primitives::test]
    async fn an_established_declaration_connection_reset_still_fails_the_fixture() {
        timeout(PEER_WAIT, async {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .await
                .assured("the fixture binds");
            let address = listener.local_addr().assured("the listener has an address");
            let mut peer = WebsocketPeer::declaration(listener);
            let mut socket = connect(address).await;
            answer_probe(&mut socket).await;
            peer.send_payload();
            receive_payload(&mut socket).await;
            drop(socket);
            let failure = peer
                .task
                .await
                .assured("the fixture task joins")
                .expect_err("an established transport failure remains fatal");
            let cause = failure
                .get_ref()
                .and_then(|error| error.downcast_ref::<WebsocketError>())
                .assured("the fixture retains its typed protocol cause");
            match cause {
                WebsocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {}
                WebsocketError::Io(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionReset
                            | io::ErrorKind::BrokenPipe
                            | io::ErrorKind::ConnectionAborted
                    ) => {}
                other => panic!("unexpected transport ending: {other:?}"),
            }
        })
        .await
        .assured("the fixture qualifies within its existing budget");
    }
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
                match confirm_opening(&mut socket).await? {
                    Opening::Confirmed => {}
                    Opening::Cancelled => {
                        if events_tx.send(PeerEvent::OpeningCancelled).await.is_err() {
                            return Ok(());
                        }
                        continue;
                    }
                }
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
