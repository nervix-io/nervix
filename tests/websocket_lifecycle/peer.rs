//! The scenario-local WebSocket peer and its public transport observations.
//!
//! Layer: test harness.
//! - **Owns.** Listening connections, bounded events, opening cancellation, public connection
//!   confirmation, payload release, graceful-close observations, measured reconnect instants,
//!   scripted subprotocol answers and measured client ping arrivals.
//! - **Depends on.** Execution primitives, I/O errors and the WebSocket protocol.
//! - **Must not know.** Scenario state, connector internals or runtime ownership handles.

use std::{io, time::Duration};

use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{self, error::TrySendError},
        oneshot,
    },
    task::{AbortOnDropHandle, JoinError},
    time::{Instant, timeout},
};
use tokio_tungstenite::tungstenite::{
    Error as WebsocketError, Message,
    error::ProtocolError,
    handshake::server::{ErrorResponse, Request, Response},
    http::HeaderValue,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

use super::reply_to_close;

pub(super) const PEER_WAIT: Duration = Duration::from_secs(60);
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

pub(super) enum PeerEvent {
    OpeningCancelled,
    Connected(Instant),
    SentClose(Instant),
    ReceivedClose,
    TransportFailure(tokio_tungstenite::tungstenite::Error),
    Ended,
    /// An opening upgrade completed, with the client's `sec-websocket-protocol` offer exactly as
    /// it arrived, when the client made one.
    Upgraded {
        offer: Option<String>,
    },
    /// The peer is about to answer the client's signaling handshake. The instant is taken before
    /// the answer is written, so the client cannot have read the answer earlier.
    SignalingCompleted(Instant),
    /// A client ping arrived.
    ReceivedPing(Instant),
}

/// What a subprotocol peer answers to the `sec-websocket-protocol` offer of every opening upgrade.
pub(crate) enum SubprotocolAnswer {
    /// Selects this protocol, whether or not the client offered it.
    Select(String),
    /// Selects no protocol, whatever the client offered.
    Nothing,
}

/// Sends an event a test may never wait for. A full channel drops it, so a peer that keeps
/// answering connections or pings never stops on its observer; a closed one ends the peer.
fn report(events: &mpsc::Sender<PeerEvent>, event: PeerEvent) -> bool {
    match events.try_send(event) {
        Ok(()) | Err(TrySendError::Full(_)) => true,
        Err(TrySendError::Closed(_)) => false,
    }
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
                            reply_to_close(&mut socket).await?;
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
                            reply_to_close(&mut socket).await?;
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

    pub(super) async fn join(self) -> Result<io::Result<()>, JoinError> {
        self.task.await
    }

    pub(crate) async fn stop(self) {
        self.task.abort();
        match timeout(PEER_WAIT, self.join())
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

impl WebsocketPeer {
    pub(super) fn idle_blackhole(listener: TcpListener) -> Self {
        let (events_tx, events) = mpsc::channel(64);
        let task = nervix_primitives::task::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            loop {
                nervix_primitives::task::consume_budget().await;
                let socket = tokio_tungstenite::accept_async(stream)
                    .await
                    .map_err(io::Error::other)?;
                if events_tx
                    .send(PeerEvent::Connected(Instant::now()))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                // One live TCP connection, never polled for WebSocket frames or client pings.
                let (replacement, _) = listener.accept().await?;
                drop(socket);
                stream = replacement;
            }
        });
        Self {
            events,
            payload: None,
            task: AbortOnDropHandle::new(task),
        }
    }

    pub(super) fn handshake_flood(listener: TcpListener, frames: usize) -> Self {
        let (events_tx, events) = mpsc::channel(64);
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
                socket
                    .send(Message::Text("{\"open\":true}".into()))
                    .await
                    .map_err(io::Error::other)?;
                for user_id in 0..frames {
                    nervix_primitives::task::consume_budget().await;
                    socket
                        .send(Message::Text(format!("{{\"user_id\":{user_id}}}")))
                        .await
                        .map_err(io::Error::other)?;
                }
                socket
                    .send(Message::Text("{\"ready\":true}".into()))
                    .await
                    .map_err(io::Error::other)?;
                // The ready marker follows the payload frames. A full handoff holds it behind
                // backpressure until the signaling timeout.
                while let Some(message) = socket.next().await {
                    nervix_primitives::task::consume_budget().await;
                    match message {
                        Ok(Message::Close(_)) | Err(_) => break,
                        Ok(_) => socket.flush().await.map_err(io::Error::other)?,
                    }
                }
            }
        });
        Self {
            events,
            payload: None,
            task: AbortOnDropHandle::new(task),
        }
    }

    pub(super) fn silent_connection(listener: TcpListener) -> Self {
        let (events_tx, events) = mpsc::channel(2);
        let task = nervix_primitives::task::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut silent = tokio_tungstenite::accept_async(stream)
                .await
                .map_err(io::Error::other)?;
            events_tx
                .send(PeerEvent::Connected(Instant::now()))
                .await
                .map_err(io::Error::other)?;
            silent
                .send(Message::Text("{\"user_id\":42}".into()))
                .await
                .map_err(io::Error::other)?;
            // Keep TCP open without polling the WebSocket: even client pings get no pong.
            let (stream, _) = listener.accept().await?;
            let mut replacement = tokio_tungstenite::accept_async(stream)
                .await
                .map_err(io::Error::other)?;
            events_tx
                .send(PeerEvent::Connected(Instant::now()))
                .await
                .map_err(io::Error::other)?;
            replacement
                .send(Message::Text("{\"user_id\":43}".into()))
                .await
                .map_err(io::Error::other)?;
            drop(silent);
            while let Some(message) = replacement.next().await {
                nervix_primitives::task::consume_budget().await;
                match message.map_err(io::Error::other)? {
                    Message::Close(_) => {
                        replacement.flush().await.map_err(io::Error::other)?;
                        break;
                    }
                    _ => replacement.flush().await.map_err(io::Error::other)?,
                }
            }
            Ok(())
        });
        Self {
            events,
            payload: None,
            task: AbortOnDropHandle::new(task),
        }
    }
}

impl WebsocketPeer {
    /// Answers the subprotocol offer of every opening upgrade as scripted and reports the offer
    /// it read. Over each connection the client keeps, it sends one payload, then reads until the
    /// client closes. A client that refuses the answer ends its connection right after the upgrade.
    pub(crate) fn subprotocol(listener: TcpListener, answer: SubprotocolAnswer) -> Self {
        let (events_tx, events) = mpsc::channel(64);
        let task = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let (stream, _) = listener.accept().await?;
                let mut offer = None;
                #[expect(
                    clippy::result_large_err,
                    reason = "tungstenite requires an unboxed HTTP rejection response in its \
                              handshake callback"
                )]
                let answer_upgrade = |request: &Request,
                                      mut response: Response|
                 -> Result<Response, ErrorResponse> {
                    if let Some(value) = request.headers().get("sec-websocket-protocol") {
                        let value = value
                            .to_str()
                            .assured("a Nervix client offers visible-ASCII subprotocol tokens");
                        offer = Some(value.to_string());
                    }
                    if let SubprotocolAnswer::Select(protocol) = &answer {
                        response.headers_mut().insert(
                            "sec-websocket-protocol",
                            HeaderValue::from_str(protocol)
                                .assured("the scripted subprotocol is a header value"),
                        );
                    }
                    Ok(response)
                };
                let mut socket =
                    match tokio_tungstenite::accept_hdr_async(stream, answer_upgrade).await {
                        Ok(socket) => socket,
                        // A refusing client can end TCP before the upgrade's last bytes are written.
                        // Any other upgrade failure remains a fixture failure.
                        Err(error) => {
                            opening_failure(error)?;
                            continue;
                        }
                    };
                if !report(&events_tx, PeerEvent::Upgraded { offer }) {
                    return Ok(());
                }
                if socket
                    .send(Message::Text("{\"user_id\":42}".into()))
                    .await
                    .is_err()
                {
                    // The client refused the answer and ended this connection.
                    continue;
                }
                while let Some(message) = socket.next().await {
                    nervix_primitives::task::consume_budget().await;
                    match message {
                        Ok(Message::Close(_)) => {
                            reply_to_close(&mut socket).await?;
                            break;
                        }
                        Ok(_) => {
                            if socket.flush().await.is_err() {
                                break;
                            }
                        }
                        // A refused or dropped connection ends; the next one is answered the same.
                        Err(_) => break,
                    }
                }
            }
        });
        Self {
            events,
            payload: None,
            task: AbortOnDropHandle::new(task),
        }
    }

    /// Accepts one connection and holds the client's signaling handshake open for `hold` after its
    /// subscription arrives, reading all the while, before answering it with `{"ready":true}`.
    /// Then it sends one payload and reports every client ping with the instant it arrived. Pings
    /// read before the answer are reported before `SignalingCompleted`, so a ping that signaling
    /// should have withheld is visible in order.
    pub(crate) fn ping_cadence(listener: TcpListener, hold: Duration) -> Self {
        let (events_tx, events) = mpsc::channel(64);
        let task = nervix_primitives::task::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let socket = tokio_tungstenite::accept_async(stream)
                .await
                .map_err(io::Error::other)?;
            let peer = PingCadencePeer {
                socket,
                events: events_tx,
            };
            peer.run(hold).await
        });
        Self {
            events,
            payload: None,
            task: AbortOnDropHandle::new(task),
        }
    }
}

/// Whether the test still observes a peer's events. A peer whose observer left ends quietly.
enum Observation {
    Continues,
    Left,
}

/// The next client frame that is not a ping, once every ping before it is reported and answered.
enum AfterPings {
    Frame(Message),
    Failed(WebsocketError),
    Ended,
    ObserverLeft,
}

/// The one connection a ping-cadence peer accepts, with the events it reports.
struct PingCadencePeer {
    socket: tokio_tungstenite::WebSocketStream<TcpStream>,
    events: mpsc::Sender<PeerEvent>,
}

impl PingCadencePeer {
    async fn run(mut self, hold: Duration) -> io::Result<()> {
        if !report(&self.events, PeerEvent::Connected(Instant::now())) {
            return Ok(());
        }
        if let Observation::Left = self.await_subscription().await? {
            return Ok(());
        }
        if let Observation::Left = self.hold_signaling(hold).await? {
            return Ok(());
        }
        if !report(&self.events, PeerEvent::SignalingCompleted(Instant::now())) {
            return Ok(());
        }
        self.send_text("{\"ready\":true}").await?;
        self.send_text("{\"user_id\":42}").await?;
        self.record_pings_until_the_connection_ends().await
    }

    /// Reports each ping with the instant it arrived and flushes the pong the transport queued
    /// for it, until another frame, a failure or the end of the connection arrives.
    async fn next_after_pings(&mut self) -> io::Result<AfterPings> {
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.socket.next().await {
                Some(Ok(Message::Ping(_))) => {
                    if !report(&self.events, PeerEvent::ReceivedPing(Instant::now())) {
                        return Ok(AfterPings::ObserverLeft);
                    }
                    self.socket.flush().await.map_err(io::Error::other)?;
                }
                Some(Ok(frame)) => return Ok(AfterPings::Frame(frame)),
                Some(Err(error)) => return Ok(AfterPings::Failed(error)),
                None => return Ok(AfterPings::Ended),
            }
        }
    }

    /// The client's signaling opens with its subscription, its first text frame.
    async fn await_subscription(&mut self) -> io::Result<Observation> {
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.next_after_pings().await? {
                AfterPings::Frame(Message::Text(_)) => return Ok(Observation::Continues),
                AfterPings::Frame(_) => self.socket.flush().await.map_err(io::Error::other)?,
                AfterPings::Failed(error) => return Err(io::Error::other(error)),
                AfterPings::Ended => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the client ended before its signaling subscription",
                    ));
                }
                AfterPings::ObserverLeft => return Ok(Observation::Left),
            }
        }
    }

    /// Keeps reading for `hold` without answering the subscription. The hold elapsing with the
    /// connection open is the scripted path.
    async fn hold_signaling(&mut self, hold: Duration) -> io::Result<Observation> {
        let held = timeout(hold, self.read_while_held()).await;
        match held {
            Err(_) => Ok(Observation::Continues),
            Ok(ended_early) => ended_early,
        }
    }

    /// Returns only when the held connection fails, ends or loses its observer.
    async fn read_while_held(&mut self) -> io::Result<Observation> {
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.next_after_pings().await? {
                AfterPings::Frame(_) => self.socket.flush().await.map_err(io::Error::other)?,
                AfterPings::Failed(error) => return Err(io::Error::other(error)),
                AfterPings::Ended => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the client ended while its signaling was held",
                    ));
                }
                AfterPings::ObserverLeft => return Ok(Observation::Left),
            }
        }
    }

    async fn send_text(&mut self, text: &str) -> io::Result<()> {
        self.socket
            .send(Message::Text(text.into()))
            .await
            .map_err(io::Error::other)
    }

    /// Reports every later ping, then how the connection ended.
    async fn record_pings_until_the_connection_ends(&mut self) -> io::Result<()> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let ending = match self.next_after_pings().await? {
                AfterPings::Frame(Message::Close(_)) => {
                    reply_to_close(&mut self.socket).await?;
                    PeerEvent::ReceivedClose
                }
                AfterPings::Frame(_) => {
                    self.socket.flush().await.map_err(io::Error::other)?;
                    continue;
                }
                AfterPings::Failed(error) => PeerEvent::TransportFailure(error),
                AfterPings::Ended => PeerEvent::Ended,
                AfterPings::ObserverLeft => return Ok(()),
            };
            report(&self.events, ending);
            return Ok(());
        }
    }
}
