//! WebSocket client source transport and runtime composition.
//!
//! Layer: data plane, pending the mechanical connector move.
//!
//! - **Owns.** Composing the WebSocket connector plan with host-owned intake, plus the client
//!   transport that moves to its connector crate in the following commit.
//! - **Depends on.** The connector source contract, compiled signaling protocols, WebSocket
//!   transport, and pre-resolved runtime execution handles.
//! - **Must not know.** NSPL parsing, registry validation, or placement computation.

use std::{collections::VecDeque, future::Future};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    ClientConfigResult, IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders,
    ParsedRetryPolicy, RustlsClientConfigSource, ServiceUrl, SourceAckPolicy, SourceBatch,
    SourceBatchRequest, SourceCapabilities, SourceConnector, SourceError, SourceMessage,
    SourcePlan, SourceResult, SourceResume, client_config_value,
};
use nervix_connector_websockets::{
    CompiledSignalingProtocol, SignalingDataSink, WebsocketSignalingSession,
};
use nervix_models::ClientConfigEntry;
use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async, connect_async_tls_with_config,
    tungstenite::Message,
};

use super::{
    super::*,
    source::{BrokerSourceHost, BrokerSourceHostSpec, run_source_instance_with_retry},
};

const WEBSOCKETS: &str = "websockets";
const WEBSOCKET_RETRY_POLICY: ParsedRetryPolicy = ParsedRetryPolicy {
    backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(30),
};

pub(in crate::runtime) struct WebsocketsIngestor;

#[derive(Debug, Error)]
enum WebsocketSourcePlanError {
    #[error("invalid WebSockets client configuration")]
    Configuration,
    #[error("invalid WebSockets endpoint")]
    Endpoint,
    #[error("unsupported WebSockets endpoint scheme '{scheme}', expected ws:// or wss://")]
    Scheme { scheme: String },
}

#[derive(Clone)]
struct WebsocketSourcePlan {
    endpoint: String,
    endpoint_requires_tls: bool,
    entries: Vec<ClientConfigEntry>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
}

impl WebsocketSourcePlan {
    fn new(
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

    fn endpoint_from_config(config: &[ClientConfigEntry]) -> ClientConfigResult<String> {
        client_config_value(config, "endpoint", "WebSockets")
    }
}

struct WebsocketSourceMessage {
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

struct WebsocketSource {
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
    type Message = WebsocketSourceMessage;
    type Position = ();

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

impl WebsocketsIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: WebsocketsIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let WebsocketsIngestorStartPlan {
            ingestor,
            client,
            mode: _,
            signaling_protocol,
        } = plan;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if runtime.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }
        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let signaling_protocol = if let Some(signaling_protocol) = signaling_protocol.as_ref() {
            Some(
                runtime
                    .signaling_protocol(domain, signaling_protocol)
                    .await
                    .ok_or_else(|| RuntimeError::StartIngestor {
                        domain: domain.as_str().to_string(),
                        ingestor: ingestor.name.as_str().to_string(),
                        reason: format!(
                            "missing signaling protocol '{}'",
                            signaling_protocol.as_str()
                        ),
                    })?,
            )
        } else {
            None
        };
        let connector = WebsocketSourcePlan::new(resolved_client.entries, signaling_protocol)
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let task_endpoint = connector.endpoint.clone();
        let acknowledgement = SourceAckPolicy::None;
        let source_plan = SourcePlan {
            connector,
            capabilities: SourceCapabilities::new(
                ingestor.allow_header_reads,
                ingestor.metadata_kind.source_scope(),
                ingestor.quiesce.supports(ingestor.quiesce.mode()),
                NonZeroU64::MIN,
                acknowledgement.support(),
            ),
            acknowledgement,
        };
        let source = WebsocketSource::open(&source_plan.connector, 0)
            .await
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .verified(
                "the runtime registers quiesce control for an ingestor before it starts the task",
            );
        let (shutdown_tx, _) = watch::channel(false);
        runtime.prepare_ingestor_readiness(
            domain,
            &ingestor.name,
            source_plan.capabilities.instances(),
        );
        let host = BrokerSourceHost::build(BrokerSourceHostSpec {
            runtime: runtime.clone(),
            domain: domain.clone(),
            ingestor: ingestor.name.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            output_routes: dependencies.output_routes,
            filter_where: dependencies.filter_where,
            codec: dependencies.codec,
            metrics: dependencies.metrics,
            branched_senders: branched_runtime.senders.clone(),
            quiesce,
            shutdown: shutdown_tx.subscribe(),
            instance_index: 0,
            metadata_kind: ingestor.metadata_kind,
            buffered_intake: true,
            flush_each_intake: true,
        });
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let shutdown = shutdown_tx.subscribe();
        let acknowledgement = source_plan.acknowledgement;
        let client_mounts = resolved_client.mounts;
        let task = tokio::spawn(async move {
            let _client_mounts = client_mounts;
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                endpoint = task_endpoint,
                "started websockets ingestor"
            );
            run_source_instance_with_retry(
                source,
                host,
                acknowledgement,
                WEBSOCKET_RETRY_POLICY,
                shutdown,
            )
            .await;
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "stopped websockets ingestor"
            );
        });
        runtime.inner.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks: vec![task],
            },
        );
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::runtime) fn endpoint_from_config(
        config: &[ClientConfigEntry],
    ) -> ClientConfigResult<String> {
        WebsocketSourcePlan::endpoint_from_config(config)
    }
}
