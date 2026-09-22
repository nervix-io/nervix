//! WebSocket client source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** WebSocket client configuration, connections, signaling before source readiness,
//!   source frame delivery, and reconnect lifecycle operations.
//! - **Depends on.** The connector contract, compiled signaling engine, typed client
//!   configuration, Tokio, and WebSocket transport.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use std::{collections::VecDeque, future::Future};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_connector::{
    BrokerSourceConnector, ClientConfigResult, IngestMessageHeaders, IngestMetadataRow,
    NoIngestHeaders, RustlsClientConfigSource, ServiceUrl, SourceBatch, SourceBatchRequest,
    SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume, client_config_value,
};
use nervix_models::ClientConfigEntry;
use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async, connect_async_tls_with_config,
    tungstenite::Message,
};
use triomphe::Arc;

use crate::{CompiledSignalingProtocol, SignalingDataSink, WebsocketSignalingSession};

const WEBSOCKETS: &str = "websockets";

#[derive(Debug, Error)]
pub enum WebsocketSourcePlanError {
    #[error("invalid WebSockets client configuration")]
    Configuration,
    #[error("invalid WebSockets endpoint")]
    Endpoint,
    #[error("unsupported WebSockets endpoint scheme '{scheme}', expected ws:// or wss://")]
    Scheme { scheme: String },
}

#[derive(Clone)]
pub struct WebsocketSourcePlan {
    endpoint: String,
    endpoint_requires_tls: bool,
    entries: Vec<ClientConfigEntry>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
}

impl WebsocketSourcePlan {
    pub fn new(
        entries: Vec<ClientConfigEntry>,
        signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
    ) -> Result<Self, Report<WebsocketSourcePlanError>> {
        let endpoint = Self::endpoint_from_config(&entries)
            .change_context(WebsocketSourcePlanError::Configuration)?;
        let scheme = ServiceUrl::new(endpoint.as_str(), "WebSockets endpoint")
            .scheme()
            .change_context(WebsocketSourcePlanError::Endpoint)?;
        let endpoint_requires_tls = match scheme.as_str() {
            "ws" => false,
            "wss" => true,
            scheme => {
                return Err(Report::new(WebsocketSourcePlanError::Scheme {
                    scheme: scheme.to_string(),
                }));
            }
        };
        Ok(Self {
            endpoint,
            endpoint_requires_tls,
            entries,
            signaling_protocol,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn endpoint_from_config(config: &[ClientConfigEntry]) -> ClientConfigResult<String> {
        client_config_value(config, "endpoint", "WebSockets")
    }
}

pub struct WebsocketSourceMessage {
    payload: Vec<u8>,
    position: (),
    headers: NoIngestHeaders,
}

impl WebsocketSourceMessage {
    fn new(payload: Vec<u8>) -> Self {
        Self {
            payload,
            position: (),
            headers: NoIngestHeaders,
        }
    }
}

impl SourceMessage for WebsocketSourceMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Headers {
            headers: &self.headers,
        }
    }
}

pub struct WebsocketSource {
    endpoint: String,
    endpoint_requires_tls: bool,
    tls_connector: Option<Connector>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
    relay: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pending: VecDeque<Vec<u8>>,
}

#[async_trait]
impl SourceConnector for WebsocketSource {
    type Plan = WebsocketSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        let tls_connector = if plan.endpoint_requires_tls {
            let config = RustlsClientConfigSource::new(&plan.entries)
                .build_with_default_roots()
                .change_context(SourceError::Open {
                    connector: WEBSOCKETS,
                })?;
            Some(Connector::Rustls(config))
        } else {
            None
        };
        Ok(Self {
            endpoint: plan.endpoint.clone(),
            endpoint_requires_tls: plan.endpoint_requires_tls,
            tls_connector,
            signaling_protocol: plan.signaling_protocol.clone(),
            relay: None,
            pending: VecDeque::new(),
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.relay.is_none()
    }

    async fn suspend(&mut self) -> SourceResult<()> {
        self.relay = None;
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.relay.is_some() {
            return Ok(SourceResume::Ready);
        }
        let connected = if self.endpoint_requires_tls {
            connect_async_tls_with_config(
                self.endpoint.as_str(),
                None,
                false,
                self.tls_connector.clone(),
            )
            .await
        } else {
            connect_async(self.endpoint.as_str()).await
        };
        let (mut relay, _) = connected.map_err(|error| {
            Report::new(SourceError::Resume {
                connector: WEBSOCKETS,
            })
            .attach_printable(error.to_string())
        })?;
        if let Some(protocol) = self.signaling_protocol.as_ref() {
            let (payloads_tx, mut payloads_rx) = mpsc::unbounded_channel();
            let sink = QueuedSignalingDataSink { payloads_tx };
            WebsocketSignalingSession::new(protocol.clone())
                .run(&mut relay, &sink)
                .await
                .map_err(|error| {
                    Report::new(SourceError::Resume {
                        connector: WEBSOCKETS,
                    })
                    .attach_printable(format!("websocket signaling failed: {error}"))
                })?;
            drop(sink);
            while let Ok(payload) = payloads_rx.try_recv() {
                self.pending.push_back(payload);
            }
        }
        self.relay = Some(relay);
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.relay = None;
        self.pending.clear();
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for WebsocketSource {
    type Message = WebsocketSourceMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        if let Some(payload) = self.pending.pop_front() {
            return Ok(SourceBatch::Messages(vec![WebsocketSourceMessage::new(
                payload,
            )]));
        }
        loop {
            tokio::task::consume_budget().await;
            let Some(relay) = self.relay.as_mut() else {
                return Ok(SourceBatch::ResumeRequired);
            };
            let message = futures_util::StreamExt::next(relay).await;
            match message {
                Some(Ok(Message::Text(text))) => {
                    return Ok(SourceBatch::Messages(vec![WebsocketSourceMessage::new(
                        text.to_string().into_bytes(),
                    )]));
                }
                Some(Ok(Message::Binary(bytes))) => {
                    return Ok(SourceBatch::Messages(vec![WebsocketSourceMessage::new(
                        bytes.to_vec(),
                    )]));
                }
                Some(Ok(Message::Close(_))) | None => {
                    self.relay = None;
                    return Ok(SourceBatch::ResumeRequired);
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
                Some(Err(error)) => {
                    self.relay = None;
                    return Err(Report::new(SourceError::Read {
                        connector: WEBSOCKETS,
                    })
                    .attach_printable(error.to_string()));
                }
            }
        }
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }
}

struct QueuedSignalingDataSink {
    payloads_tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl SignalingDataSink for QueuedSignalingDataSink {
    fn accept(&self, payload: Vec<u8>) -> impl Future<Output = ()> + Send {
        self.payloads_tx
            .send(payload)
            .assured("the signaling session retains its payload receiver while the sink is live");
        std::future::ready(())
    }
}
