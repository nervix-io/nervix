//! One outbound WebSockets connection and the exact upgrade snapshot its frames retain.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The active transport, immutable response headers, optional ping cadence, the
//!   inbound activity deadline and bounded missed-ping response window retained across
//!   cancellation, and the Close a connection this client ends offers its peer: going away when
//!   that deadline ends it, the status of a frame the peer sent that failed the read, or the
//!   status of the failure that ended its signaling handshake.
//! - **Depends on.** Connector outcomes, signaling outcomes, execution primitives, recovery
//!   classes and WebSocket transport.
//! - **Must not know.** Configuration Models, Plexus, domains, relays, registry or host admission.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the connection engine boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the ingestor supplies validated connection settings")
)]

use std::time::Duration;

use error_stack::Report;
use futures_util::{FutureExt as _, SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    ClientConfigResult, ConfiguredDeadline, IngestMessageHeaders, IngestorError, IngestorResult,
    RetainedIngestHeaders,
};
use nervix_models::{ClientConfigEntry, WebsocketsUpgradeHeaders};
use nervix_primitives::{
    net::TcpStream,
    sync::Arc,
    task::unconstrained,
    time::{Instant, error::Elapsed, timeout_at},
};
use nervix_recovery::Reported as _;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Error as WebsocketError, Message,
        error::{CapacityError, ProtocolError},
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};

use super::{WEBSOCKETS, WebsocketsConnectionIngestorFailure, pings::WebsocketPings};
use crate::WebsocketSignalingError;

/// An explicit client deadline retains its key; a derived keep-alive/default keeps its bound.
#[derive(Debug, Clone, Copy)]
pub(super) enum WebsocketIdleDeadline {
    Configured(ConfiguredDeadline),
    Derived(Duration),
}

impl WebsocketIdleDeadline {
    pub(super) fn new(
        entries: &[ClientConfigEntry],
        derived: Duration,
    ) -> ClientConfigResult<Self> {
        match ConfiguredDeadline::idle(entries)? {
            Some(deadline) => Ok(Self::Configured(deadline)),
            None => Ok(Self::Derived(derived)),
        }
    }

    fn bound(self) -> Duration {
        match self {
            Self::Configured(deadline) => deadline.bound(),
            Self::Derived(bound) => bound,
        }
    }

    fn elapsed(self, elapsed: Elapsed) -> Report<IngestorError> {
        // Elapsed is a fieldless expiry signal. Retain it for typed inspection while the
        // actionable key/budget remains the deepest context projected into ingestor status.
        let read = IngestorError::Read {
            connector: WEBSOCKETS.keyword(),
        };
        match self {
            Self::Configured(deadline) => {
                Report::new(deadline.elapsed("reading the next WebSocket frame"))
                    .attach(elapsed)
                    .change_context(read)
            }
            Self::Derived(budget) => {
                Report::new(WebsocketsConnectionIngestorFailure::IdleTimeout { budget })
                    .attach(elapsed)
                    .change_context(read)
            }
        }
    }
}

pub(super) struct WebsocketConnection {
    pub(super) relay: WebSocketStream<MaybeTlsStream<TcpStream>>,
    // Pending and delivered messages retain this snapshot after the connection ends.
    pub(super) headers: Arc<RetainedIngestHeaders>,
    pings: Option<WebsocketPings>,
    idle_deadline: WebsocketIdleDeadline,
    last_received: Instant,
}

pub(super) enum WebsocketRead {
    Frame(Option<Message>),
    Ping,
}

/// How this client ends an established connection it gives up. An endpoint that fails an
/// established connection tells its peer why with a Close frame, unless the peer is unlikely to
/// receive it (RFC 6455 section 7.1.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectionEnding {
    /// The Close names why this client ends the connection.
    Close(CloseCode),
    /// The transport broke, already ended or is already closing, so no frame reaches the peer.
    Release,
}

impl ConnectionEnding {
    /// Offers the peer this ending's Close, waiting neither for the write nor for a reply. The
    /// frame gets one poll outside the cooperative budget, so only a transport that cannot take it
    /// at once holds it back, and the caller releases the connection either way. A write the
    /// transport refuses is reported under `operation`. A release offers nothing.
    pub(super) fn offer(
        self,
        relay: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        operation: &str,
    ) {
        match self {
            Self::Close(status) => {
                let close = Message::Close(Some(CloseFrame {
                    code: status,
                    reason: "".into(),
                }));
                let offered = unconstrained(relay.send(close)).now_or_never();
                if let Some(written) = offered {
                    written.reported(operation);
                }
            }
            Self::Release => {}
        }
    }
}

/// How a connection ends when its transport fails a read: a frame the peer sent and this client
/// refuses earns a Close naming why.
impl From<&WebsocketError> for ConnectionEnding {
    fn from(failure: &WebsocketError) -> Self {
        match failure {
            // A frame beyond max_frame_size, or a message beyond max_message_size.
            WebsocketError::Capacity(CapacityError::MessageTooLong { .. }) => {
                Self::Close(CloseCode::Size)
            }
            // Message text, or the reason of a Close, that is not UTF-8.
            WebsocketError::Utf8 => Self::Close(CloseCode::Invalid),
            WebsocketError::Protocol(violation) => Self::from(violation),
            WebsocketError::ConnectionClosed
            | WebsocketError::AlreadyClosed
            | WebsocketError::Io(_)
            | WebsocketError::Tls(_)
            | WebsocketError::WriteBufferFull(_) => Self::Release,
            // Only the opening upgrade fails with these, never an established connection's read.
            WebsocketError::Capacity(CapacityError::TooManyHeaders)
            | WebsocketError::AttackAttempt
            | WebsocketError::Url(_)
            | WebsocketError::Http(_)
            | WebsocketError::HttpFormat(_) => Self::Release,
        }
    }
}

impl From<&ProtocolError> for ConnectionEnding {
    fn from(violation: &ProtocolError) -> Self {
        match violation {
            // Frames RFC 6455 section 5 forbids the peer to send.
            ProtocolError::NonZeroReservedBits
            | ProtocolError::MaskedFrameFromServer
            | ProtocolError::UnmaskedFrameFromClient
            | ProtocolError::FragmentedControlFrame
            | ProtocolError::ControlFrameTooBig
            | ProtocolError::UnknownControlFrameType(_)
            | ProtocolError::UnknownDataFrameType(_)
            | ProtocolError::UnexpectedContinueFrame
            | ProtocolError::ExpectedFragment(_)
            | ProtocolError::InvalidOpcode(_)
            | ProtocolError::InvalidCloseSequence => Self::Close(CloseCode::Protocol),
            // TCP ended without the closing handshake, or that handshake is already under way.
            ProtocolError::ResetWithoutClosingHandshake
            | ProtocolError::ReceivedAfterClosing
            | ProtocolError::SendAfterClosing => Self::Release,
            // Only the opening upgrade fails with these, never an established connection's read.
            ProtocolError::WrongHttpMethod
            | ProtocolError::WrongHttpVersion
            | ProtocolError::MissingConnectionUpgradeHeader
            | ProtocolError::MissingUpgradeWebSocketHeader
            | ProtocolError::MissingSecWebSocketVersionHeader
            | ProtocolError::MissingSecWebSocketKey
            | ProtocolError::SecWebSocketAcceptKeyMismatch
            | ProtocolError::SecWebSocketSubProtocolError(_)
            | ProtocolError::JunkAfterRequest
            | ProtocolError::CustomResponseSuccessful
            | ProtocolError::InvalidHeader(_)
            | ProtocolError::HandshakeIncomplete
            | ProtocolError::HttparseError(_) => Self::Release,
        }
    }
}

/// How a connection ends when its signaling handshake fails, before it reaches its first read: the
/// Close names what the peer sent, what the peer left undone or which of this client's own
/// programs failed.
impl From<&WebsocketSignalingError> for ConnectionEnding {
    fn from(failure: &WebsocketSignalingError) -> Self {
        match failure {
            // A read failed while the handshake ran, on what the peer sent or on the transport.
            WebsocketSignalingError::Receive(read) => {
                let read: &WebsocketError = read;
                Self::from(read)
            }
            // A frame the peer sent matched a FAIL JAQ guard: it violates the protocol's policy.
            WebsocketSignalingError::Rejected { .. } => Self::Close(CloseCode::Policy),
            // The peer left the handshake unfinished within its TIMEOUT, and this client leaves
            // for another connection, as the idle verdict does.
            WebsocketSignalingError::Timeout { .. } => Self::Close(CloseCode::Away),
            // This client's own programs failed the handshake: a SEND JAQ program did not produce
            // exactly one value, the format could not encode it, or a CAPTURE program failed or
            // produced no object. RFC 6455 erratum 3227 extends this status to clients.
            WebsocketSignalingError::SendProgram { .. }
            | WebsocketSignalingError::SendEncode { .. }
            | WebsocketSignalingError::Capture { .. } => Self::Close(CloseCode::Error),
            // This client's own write failed, so the transport takes no Close either.
            WebsocketSignalingError::Send(_) => Self::Release,
            // TCP ended, or the peer's own Close began the closing handshake.
            WebsocketSignalingError::Closed => Self::Release,
        }
    }
}

impl WebsocketConnection {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "visits the external iterator of at most 128 immutable \
                                   validated upgrade fields; local conversions remain analyzed; \
                                   no internal runtime ownership is inferred")
    )]
    pub(super) fn install_request_headers(
        request: &mut tokio_tungstenite::tungstenite::handshake::client::Request,
        headers: &WebsocketsUpgradeHeaders,
    ) {
        for (name, value) in headers.iter() {
            let name = tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(
                name.as_str().as_bytes(),
            )
            .verified("the validated application header name is an HTTP field name");
            let value = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(value.as_str())
                .verified("the validated application header value is an HTTP field value");
            request.headers_mut().insert(name, value);
        }
    }

    pub(super) fn new(
        relay: WebSocketStream<MaybeTlsStream<TcpStream>>,
        headers: Arc<RetainedIngestHeaders>,
        ping_interval: Option<Duration>,
        idle_deadline: WebsocketIdleDeadline,
    ) -> Self {
        let pings = ping_interval.map(WebsocketPings::new);
        Self {
            relay,
            headers,
            pings,
            idle_deadline,
            last_received: Instant::now(),
        }
    }

    pub(super) fn capture_headers(
        headers: &tokio_tungstenite::tungstenite::http::HeaderMap,
    ) -> Arc<RetainedIngestHeaders> {
        Arc::new(RetainedIngestHeaders::capture(&UpgradeResponseHeaders(
            headers,
        )))
    }

    fn read_deadline(&self) -> Instant {
        if let Some(pings) = &self.pings
            && let Some(deadline) = pings.response_deadline()
        {
            return deadline;
        }
        self.last_received
            .checked_add(self.idle_deadline.bound())
            .verified("client deadlines are held to the schedulable duration bound")
    }

    pub(super) fn ping_written(&mut self) {
        self.pings
            .as_mut()
            .verified("only a configured ping cadence requests a ping write")
            .written(Instant::now(), self.idle_deadline.bound());
    }

    pub(super) async fn next(&mut self) -> IngestorResult<WebsocketRead> {
        let read = timeout_at(self.read_deadline(), self.relay.next());
        let frame = match self.pings.as_mut() {
            Some(pings) => nervix_primitives::select! {
                biased;
                frame = read => frame,
                _ = pings.tick() => return Ok(WebsocketRead::Ping),
            },
            None => read.await,
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                if let Some(pings) = self.pings.as_mut()
                    && pings.offer_after_idle(self.last_received, Instant::now())
                {
                    return Ok(WebsocketRead::Ping);
                }
                return Err(self.end_silent(error));
            }
        };
        match frame {
            Some(Ok(frame)) => {
                self.last_received = Instant::now();
                if let Some(pings) = self.pings.as_mut() {
                    pings.received();
                }
                Ok(WebsocketRead::Frame(Some(frame)))
            }
            Some(Err(failure)) => Err(self.end_failed(failure)),
            None => Ok(WebsocketRead::Frame(None)),
        }
    }

    /// Ends a connection whose peer the idle deadline found silent, returning the read failure
    /// the host reconnects after.
    ///
    /// An endpoint that gives up on an established connection sends a Close frame with a status
    /// before releasing it (RFC 6455 section 7.1.7), here 1001, going away: this client leaves the
    /// connection and opens another. The peer was just found silent and may not be reading at
    /// all, so the frame is written only if the transport takes it at once. The verdict waits for
    /// neither the write nor a reply, and the reconnect starts no later than it would without the
    /// frame. A transport that cannot take the frame now belongs to a peer that has stopped
    /// reading, which could not read the frame either; that connection is released all the same.
    fn end_silent(&mut self, elapsed: Elapsed) -> Report<IngestorError> {
        ConnectionEnding::Close(CloseCode::Away).offer(
            &mut self.relay,
            "offering a silent WebSocket peer a going-away Close",
        );
        self.idle_deadline.elapsed(elapsed)
    }

    /// Ends a connection whose transport failed a read, returning that failure, with the driver's
    /// typed cause beneath it, for the host to reconnect after.
    ///
    /// A frame the peer sent and this client refuses earns the peer a Close naming why, offered
    /// like the idle verdict's: 1009 for a frame or message beyond its limit, 1007 for text that is
    /// not UTF-8 and 1002 for any other violation of the framing rules. A transport that broke or
    /// already ended can carry no frame, and is released without one.
    fn end_failed(&mut self, failure: WebsocketError) -> Report<IngestorError> {
        let ending = ConnectionEnding::from(&failure);
        ending.offer(
            &mut self.relay,
            "offering a WebSocket peer whose frame failed the read a Close",
        );
        Report::new(failure).change_context(IngestorError::Read {
            connector: WEBSOCKETS.keyword(),
        })
    }
}

struct UpgradeResponseHeaders<'a>(&'a tokio_tungstenite::tungstenite::http::HeaderMap);

impl IngestMessageHeaders for UpgradeResponseHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in self.0 {
            if let Ok(value) = value.to_str() {
                visit(name.as_str(), value);
            }
        }
    }
}
