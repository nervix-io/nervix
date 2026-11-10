//! One outbound WebSockets connection and the exact upgrade snapshot its frames retain.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The active transport, immutable response headers, optional ping cadence, the
//!   inbound activity deadline and bounded missed-ping response window retained across cancellation,
//!   and the going-away Close a connection that deadline ends offers its peer.
//! - **Depends on.** Connector outcomes, execution primitives, recovery classes and WebSocket
//!   transport.
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
        Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};

use super::{WEBSOCKETS, WebsocketsConnectionIngestorFailure, pings::WebsocketPings};

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
            Some(Err(error)) => Err(Report::new(error).change_context(IngestorError::Read {
                connector: WEBSOCKETS.keyword(),
            })),
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
        let going_away = Message::Close(Some(CloseFrame {
            code: CloseCode::Away,
            reason: "".into(),
        }));
        // One poll outside the cooperative budget, so only a full transport holds the frame back.
        let offered = unconstrained(self.relay.send(going_away)).now_or_never();
        if let Some(written) = offered {
            written.reported("offering a silent WebSocket peer a going-away Close");
        }
        self.idle_deadline.elapsed(elapsed)
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
