//! One outbound WebSockets connection and the exact upgrade snapshot its frames retain.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The active transport, immutable response headers, optional ping cadence and the
//!   inbound activity deadline retained across read cancellation.
//! - **Depends on.** Connector outcomes, execution primitives and WebSocket transport.
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
use futures_util::StreamExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    ClientConfigResult, ConfiguredDeadline, IngestMessageHeaders, IngestorError, IngestorResult,
    RetainedIngestHeaders,
};
use nervix_models::{ClientConfigEntry, WebsocketsUpgradeHeaders};
use nervix_primitives::{
    net::TcpStream,
    sync::Arc,
    time::{Instant, Interval, MissedTickBehavior, error::Elapsed, interval, timeout_at},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use super::{WEBSOCKETS, WebsocketsConnectionIngestorFailure};

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
        // actionable key/budget remains the deepest context projected into source status.
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
    pings: Option<Interval>,
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
        let pings = match ping_interval {
            Some(period) => {
                let mut timer = interval(period);
                timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
                timer.reset();
                Some(timer)
            }
            None => None,
        };
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

    pub(super) async fn next(&mut self) -> IngestorResult<WebsocketRead> {
        // This absolute deadline survives cancelled reads and outgoing pings. Only an inbound
        // frame resets it, including peer ping/pong control frames.
        let deadline = self
            .last_received
            .checked_add(self.idle_deadline.bound())
            .verified("client deadlines are held to the schedulable duration bound");
        let read = timeout_at(deadline, self.relay.next());
        let frame = match self.pings.as_mut() {
            Some(pings) => nervix_primitives::select! {
                biased;
                frame = read => frame,
                _ = pings.tick() => return Ok(WebsocketRead::Ping),
            },
            None => read.await,
        }
        .map_err(|error| self.idle_deadline.elapsed(error))?;
        match frame {
            Some(Ok(frame)) => {
                self.last_received = Instant::now();
                Ok(WebsocketRead::Frame(Some(frame)))
            }
            Some(Err(error)) => Err(Report::new(error).change_context(IngestorError::Read {
                connector: WEBSOCKETS.keyword(),
            })),
            None => Ok(WebsocketRead::Frame(None)),
        }
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
