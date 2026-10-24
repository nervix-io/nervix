//! One outbound WebSockets connection and the exact upgrade snapshot its frames retain.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The active transport, its immutable response headers and optional ping cadence.
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
use meticulous::ResultExt as _;
use nervix_connector::{
    IngestMessageHeaders, IngestorError, IngestorResult, RetainedIngestHeaders,
};
use nervix_models::WebsocketsUpgradeHeaders;
use nervix_primitives::{
    net::TcpStream,
    sync::Arc,
    time::{Interval, MissedTickBehavior, interval},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use super::WEBSOCKETS;

pub(super) struct WebsocketConnection {
    pub(super) relay: WebSocketStream<MaybeTlsStream<TcpStream>>,
    // Pending and delivered messages retain this snapshot after the connection ends.
    pub(super) headers: Arc<RetainedIngestHeaders>,
    pings: Option<Interval>,
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
        let frame = match self.pings.as_mut() {
            Some(pings) => nervix_primitives::select! {
                frame = self.relay.next() => frame,
                _ = pings.tick() => return Ok(WebsocketRead::Ping),
            },
            None => self.relay.next().await,
        };
        match frame {
            Some(Ok(frame)) => Ok(WebsocketRead::Frame(Some(frame))),
            Some(Err(error)) => Err(Report::new(error).change_context(IngestorError::Read {
                connector: WEBSOCKETS,
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
