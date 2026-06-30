use std::{
    hash::RandomState,
    io,
    net::SocketAddr,
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};

use dashmap::DashMap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use nervix_models::{
    Domain, DomainTick, Identifier, ModelKind, RemoteAckRegistration, RemoteAckResolution,
    RemoteDecodedRecord, RemoteRuntimeElementValue, RemoteRuntimeField,
    RemoteRuntimeRecordMetadata, RemoteRuntimeValue, SubscriptionBinding, Timestamp,
};
use rand_core::OsRng;
use rkyv::{Archive, Deserialize, Serialize};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    server::WebPkiClientVerifier,
};
use rustls_pki_types::pem::{Error as PemError, PemObject};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::{Instant, MissedTickBehavior, interval, sleep, sleep_until},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, warn};

const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_SEND_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_INCOMING_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_RECONNECT_BACKOFF_MS: u64 = 200;
const PING_INTERVAL: Duration = Duration::from_millis(500);
const PING_TIMEOUT: Duration = Duration::from_secs(1);
const WIRE_TAG_INTRODUCTION: u8 = 1;
const WIRE_TAG_PING: u8 = 2;
const WIRE_TAG_RELAY_PAYLOAD: u8 = 3;
const WIRE_TAG_ACK: u8 = 4;
const WIRE_TAG_CONTROL: u8 = 5;

#[derive(Debug, Clone)]
pub struct TransportOptions {
    pub max_connections: usize,
    pub reconnect_backoff: Duration,
    pub send_queue_capacity: usize,
    pub incoming_queue_capacity: usize,
    pub max_frame_bytes: usize,
}

impl Default for TransportOptions {
    fn default() -> Self {
        Self {
            max_connections: 32,
            reconnect_backoff: Duration::from_millis(DEFAULT_RECONNECT_BACKOFF_MS),
            send_queue_capacity: DEFAULT_SEND_QUEUE_CAPACITY,
            incoming_queue_capacity: DEFAULT_INCOMING_QUEUE_CAPACITY,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportMode {
    Plain,
    Tls,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Envelope {
    RelayPayload(RelayPayload),
    Ack(RemoteAckResolution),
    Control(ControlEnvelope),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelayPayload {
    pub kind: RelayPayloadKind,
    pub domain: Domain,
    pub relay: Identifier,
    pub key: Option<Vec<RemoteRuntimeField>>,
    pub batch_ipc: Vec<u8>,
    pub metadata: Vec<RemoteRuntimeRecordMetadata>,
    pub acks: Vec<Option<RemoteAckRegistration>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayPayloadKind {
    Routed,
    SubscriptionFanout,
}

impl RelayPayloadKind {
    fn wire_tag(self) -> u8 {
        match self {
            Self::Routed => 1,
            Self::SubscriptionFanout => 2,
        }
    }

    fn from_wire_tag(tag: u8) -> Result<Self, TransportError> {
        match tag {
            1 => Ok(Self::Routed),
            2 => Ok(Self::SubscriptionFanout),
            _ => Err(TransportError::Decode(format!(
                "unknown relay payload kind tag {tag}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum ControlEnvelope {
    Terminate,
    DomainClockStart(DomainClockStart),
    DomainClockStop(DomainClockStop),
    DomainTick(DomainTickEnvelope),
    StateSyncRequest(StateSyncRequest),
    StateSyncResponse(StateSyncResponse),
    StateReplicationAck(StateReplicationAck),
    DescribeIngestorRequest(DescribeIngestorRequest),
    DescribeIngestorResponse(DescribeIngestorResponse),
    DataflowNodeStatusRequest(DataflowNodeStatusRequest),
    DataflowNodeStatusResponse(DataflowNodeStatusResponse),
    DescribeMetricsRequest(DescribeMetricsRequest),
    DescribeMetricsResponse(DescribeMetricsResponse),
    DescribeRelayRequest(DescribeRelayRequest),
    DescribeRelayResponse(DescribeRelayResponse),
    DescribeLookupRequest(DescribeLookupRequest),
    DescribeLookupResponse(DescribeLookupResponse),
    LookupRequest(LookupRequest),
    LookupResponse(LookupResponse),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainClockStart {
    pub domain_id: Domain,
    pub wall_started_at: Timestamp,
    pub logical_start: Timestamp,
    pub time_rate: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainClockStop {
    pub domain_id: Domain,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainTickEnvelope {
    pub domain_id: Domain,
    pub tick: DomainTick,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RuntimeStateKind {
    BranchAggregated = 0,
    Correlator = 1,
    Deduplicator = 2,
    KafkaOffset = 3,
    MaterializedRelay = 4,
    WasmProcessor = 5,
    WindowProcessor = 6,
    BranchLru = 7,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StatePlacementEnvelope {
    pub domain: Domain,
    pub state: RuntimeStateKind,
    pub kind: ModelKind,
    pub identifier: Identifier,
    pub branch_key: Option<Vec<RemoteRuntimeField>>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSnapshotEnvelope {
    pub lsm: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StateSyncRequest {
    pub correlation_id: u64,
    pub placement: StatePlacementEnvelope,
    pub after_lsm: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSyncResponse {
    pub correlation_id: u64,
    pub result: Result<Option<StateSnapshotEnvelope>, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StateReplicationAck {
    pub placement: StatePlacementEnvelope,
    pub lsm: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaDomainOffsetDescribeEnvelope {
    pub topic: String,
    pub instances: u64,
    pub observed_partitions: Vec<i32>,
    pub rebalance_epoch: u64,
    pub instance_assignments: Vec<Vec<i32>>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestorDescribeEnvelope {
    pub running: bool,
    pub ready: bool,
    pub memory_backpressure_paused: bool,
    pub transient_error: Option<String>,
    pub reconnect_backoff: Option<String>,
    pub reconnect_wait_millis: Option<u64>,
    pub kafka_domain_offsets: Option<KafkaDomainOffsetDescribeEnvelope>,
    pub metrics: Vec<String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataflowNodeStatusEnvelope {
    pub status: String,
    pub detail: Option<String>,
    pub transient_error: Option<String>,
    pub reconnect_backoff: Option<String>,
    pub reconnect_wait_millis: Option<u64>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataflowNodeStatusRequest {
    pub correlation_id: u64,
    pub domain: Domain,
    pub kind: ModelKind,
    pub name: Identifier,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataflowNodeStatusResponse {
    pub correlation_id: u64,
    pub result: Result<DataflowNodeStatusEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsRequest {
    pub correlation_id: u64,
    pub domain: Domain,
    pub kind: ModelKind,
    pub name: Identifier,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsResponse {
    pub correlation_id: u64,
    pub result: Result<Vec<String>, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeIngestorRequest {
    pub correlation_id: u64,
    pub domain: Domain,
    pub name: Identifier,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeIngestorResponse {
    pub correlation_id: u64,
    pub result: Result<IngestorDescribeEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeRelayRequest {
    pub correlation_id: u64,
    pub domain: Domain,
    pub relay: Identifier,
    pub bindings: Vec<SubscriptionBinding>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeRelayResponse {
    pub correlation_id: u64,
    pub result: Result<bool, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct LookupDescribeEnvelope {
    pub resource: Identifier,
    pub resource_version: u64,
    pub path: String,
    pub decode_using_codec: Identifier,
    pub key_field: Identifier,
    pub entry_count: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeLookupRequest {
    pub correlation_id: u64,
    pub domain: Domain,
    pub name: Identifier,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeLookupResponse {
    pub correlation_id: u64,
    pub result: Result<LookupDescribeEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct LookupRequest {
    pub correlation_id: u64,
    pub domain: Domain,
    pub name: Identifier,
    pub key: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct LookupResponse {
    pub correlation_id: u64,
    pub result: Result<Option<RemoteDecodedRecord>, String>,
}

#[derive(Debug, Clone)]
pub struct ReceivedEnvelope {
    pub peer_addr: SocketAddr,
    pub peer_node_id: String,
    pub envelope: Envelope,
    pub reply: ConnectionHandle,
}

#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    peer_addr: SocketAddr,
    tx: mpsc::Sender<Envelope>,
}

impl ConnectionHandle {
    pub async fn send(&self, envelope: Envelope) -> Result<(), TransportError> {
        self.tx
            .send(envelope)
            .await
            .map_err(|_| TransportError::Closed(self.peer_addr))
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }
}

#[derive(Clone)]
pub struct Transport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    mode: TransportMode,
    client_config: Option<Arc<ClientConfig>>,
    server_config: Option<Arc<ServerConfig>>,
    identity: LocalIdentity,
    peer_verifier: PeerVerifier,
    options: TransportOptions,
    local_addr: SocketAddr,
    incoming_tx: mpsc::Sender<ReceivedEnvelope>,
    outbound: DashMap<ConnectionKey, ConnectionHandle, RandomState>,
    outbound_state: DashMap<ConnectionKey, ConnectionState, RandomState>,
    connected_peers: DashMap<String, usize, RandomState>,
    outbound_permits: Arc<Semaphore>,
    shutdown: CancellationToken,
    tasks: TaskTracker,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ConnectionKey {
    addr: SocketAddr,
    server_name: String,
    mode: TransportMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Connecting,
    Connected,
    Disconnected,
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("invalid dns name '{0}'")]
    InvalidServerName(String),
    #[error("wire encode failed: {0}")]
    Encode(String),
    #[error("wire decode failed: {0}")]
    Decode(String),
    #[error("frame exceeds maximum size: {size} > {limit}")]
    FrameTooLarge { size: usize, limit: usize },
    #[error("connection pool exhausted")]
    PoolExhausted,
    #[error("transport is shutting down")]
    ShuttingDown,
    #[error("connection to {0} is closed")]
    Closed(SocketAddr),
    #[error("peer handshake is invalid: {0}")]
    InvalidHandshake(String),
    #[error("tls mode requires tls configuration")]
    MissingTlsConfig,
}

#[derive(Debug, Error)]
pub enum TlsConfigError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("missing certificate in {0}")]
    MissingCertificate(String),
    #[error("missing private key in {0}")]
    MissingPrivateKey(String),
}

#[derive(Clone)]
pub struct LocalIdentity {
    node_id: String,
    signing_key: SigningKey,
}

type PeerKeyResolver = dyn Fn(&str) -> Option<VerifyingKey> + Send + Sync;

impl std::fmt::Debug for LocalIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalIdentity")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl LocalIdentity {
    pub fn generate(node_id: impl Into<String>) -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        Self {
            node_id: node_id.into(),
            signing_key,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn public_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    fn signed_introduction(&self) -> SignedIntroduction {
        let signature = self.signing_key.sign(&introduction_message(&self.node_id));
        SignedIntroduction {
            node_id: self.node_id.clone(),
            signature: signature.to_bytes(),
        }
    }
}

#[derive(Clone)]
pub struct PeerVerifier {
    resolver: Arc<PeerKeyResolver>,
}

impl std::fmt::Debug for PeerVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerVerifier").finish_non_exhaustive()
    }
}

impl PeerVerifier {
    pub fn new(resolver: impl Fn(&str) -> Option<VerifyingKey> + Send + Sync + 'static) -> Self {
        Self {
            resolver: Arc::new(resolver),
        }
    }

    fn resolve(&self, node_id: &str) -> Option<VerifyingKey> {
        (self.resolver)(node_id)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct SignedIntroduction {
    node_id: String,
    signature: [u8; 64],
}

impl SignedIntroduction {
    fn verify(&self, verifier: &PeerVerifier) -> Result<String, TransportError> {
        let public_key = verifier.resolve(&self.node_id).ok_or_else(|| {
            TransportError::InvalidHandshake(format!(
                "no public key available for node '{}'",
                self.node_id
            ))
        })?;
        let signature = Signature::from_bytes(&self.signature);
        public_key
            .verify(&introduction_message(&self.node_id), &signature)
            .map_err(|err| TransportError::InvalidHandshake(err.to_string()))?;
        Ok(self.node_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq)]
enum WireEnvelope {
    Introduction(SignedIntroduction),
    Ping,
    Payload(Envelope),
}

impl Transport {
    pub async fn bind(
        listen_addr: SocketAddr,
        mode: TransportMode,
        tls: Option<TlsConfigBundle>,
        identity: LocalIdentity,
        peer_verifier: PeerVerifier,
        options: TransportOptions,
    ) -> Result<(Self, mpsc::Receiver<ReceivedEnvelope>), TransportError> {
        install_rustls_crypto_provider();

        let listener = TcpListener::bind(listen_addr).await?;
        let local_addr = listener.local_addr()?;
        let (incoming_tx, incoming_rx) = mpsc::channel(options.incoming_queue_capacity);

        let (client_config, server_config) = match tls {
            Some(tls) => (Some(tls.client_config), Some(tls.server_config)),
            None => (None, None),
        };
        let inner = Arc::new(TransportInner {
            mode,
            client_config,
            server_config,
            identity,
            peer_verifier,
            options: options.clone(),
            local_addr,
            incoming_tx,
            outbound: DashMap::default(),
            outbound_state: DashMap::default(),
            connected_peers: DashMap::default(),
            outbound_permits: Arc::new(Semaphore::new(options.max_connections)),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        });

        spawn_accept_loop(inner.clone(), listener);

        Ok((Self { inner }, incoming_rx))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    pub fn node_id(&self) -> &str {
        self.inner.identity.node_id()
    }

    pub async fn send(
        &self,
        target: SocketAddr,
        server_name: &str,
        mode: TransportMode,
        envelope: Envelope,
    ) -> Result<(), TransportError> {
        let handle = self.connection_for(target, server_name, mode).await?;
        handle.send(envelope).await
    }

    pub async fn connection_for(
        &self,
        target: SocketAddr,
        server_name: &str,
        mode: TransportMode,
    ) -> Result<ConnectionHandle, TransportError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(TransportError::ShuttingDown);
        }

        let key = ConnectionKey {
            addr: target,
            server_name: server_name.to_string(),
            mode,
        };
        if let Some(existing) = self
            .inner
            .outbound
            .get(&key)
            .map(|entry| entry.value().clone())
        {
            return Ok(existing);
        }

        let permit = self
            .inner
            .outbound_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransportError::PoolExhausted)?;
        let (tx, rx) = mpsc::channel(self.inner.options.send_queue_capacity);
        let handle = ConnectionHandle {
            peer_addr: target,
            tx,
        };

        let io_stream = connect_outbound_stream(&self.inner, &key).await?;

        if let Some(existing) = self
            .inner
            .outbound
            .get(&key)
            .map(|entry| entry.value().clone())
        {
            return Ok(existing);
        }
        self.inner.outbound.insert(key.clone(), handle.clone());
        self.inner
            .outbound_state
            .insert(key.clone(), ConnectionState::Connecting);

        spawn_outbound_connection(self.inner.clone(), key, rx, permit, io_stream);

        Ok(handle)
    }

    pub async fn active_outbound_connections(&self) -> usize {
        self.inner.outbound.len()
    }

    pub fn is_connected_to(&self, node_id: &str) -> bool {
        self.inner
            .connected_peers
            .get(node_id)
            .map(|count| *count)
            .unwrap_or_default()
            > 0
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        self.inner.outbound.clear();
        self.inner.outbound_state.clear();
        self.inner.connected_peers.clear();
    }
}

fn spawn_accept_loop(inner: Arc<TransportInner>, listener: TcpListener) {
    let shutdown = inner.shutdown.clone();
    let tasks = inner.tasks.clone();
    tasks.spawn(async move {
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((stream, peer_addr)) = accepted else {
                        if !shutdown.is_cancelled() {
                            warn!("interconnect accept failed");
                        }
                        continue;
                    };
                    if let Err(err) = configure_socket(&stream) {
                        warn!(?err, %peer_addr, "failed to configure accepted interconnect socket");
                        continue;
                    }
                    let inner = inner.clone();
                    let tasks = inner.tasks.clone();
                    tasks.spawn(async move {
                        match accept_inbound_stream(&inner, stream, peer_addr).await {
                            Ok(io_stream) => {
                                let (tx, mut rx) =
                                    mpsc::channel(inner.options.send_queue_capacity);
                                let handle = ConnectionHandle { peer_addr, tx };
                                let mut pending = None;
                                if let Err(err) = run_connection_loop(
                                    inner,
                                    peer_addr,
                                    handle,
                                    None,
                                    &mut rx,
                                    io_stream,
                                    &mut pending,
                                )
                                .await
                                {
                                    debug!(?err, %peer_addr, "inbound interconnect connection closed");
                                }
                            }
                            Err(err) => {
                                warn!(?err, %peer_addr, "failed to accept interconnect connection");
                            }
                        }
                    });
                }
            }
        }
    });
}

fn spawn_outbound_connection(
    inner: Arc<TransportInner>,
    key: ConnectionKey,
    rx: mpsc::Receiver<Envelope>,
    permit: OwnedSemaphorePermit,
    initial_stream: BoxedIo,
) {
    let shutdown = inner.shutdown.clone();
    let tasks = inner.tasks.clone();
    tasks.spawn(async move {
        run_outbound_connection(inner.clone(), key.clone(), rx, shutdown, initial_stream).await;
        inner.outbound.remove(&key);
        inner.outbound_state.remove(&key);
        drop(permit);
    });
}

async fn run_outbound_connection(
    inner: Arc<TransportInner>,
    key: ConnectionKey,
    mut rx: mpsc::Receiver<Envelope>,
    shutdown: CancellationToken,
    initial_stream: BoxedIo,
) {
    let mut pending = None;
    let mut current_stream = Some(initial_stream);

    loop {
        tokio::task::consume_budget().await;
        if shutdown.is_cancelled() {
            return;
        }

        let io_stream = if let Some(stream) = current_stream.take() {
            stream
        } else {
            match connect_outbound_stream(&inner, &key).await {
                Ok(next_stream) => {
                    inner
                        .outbound_state
                        .insert(key.clone(), ConnectionState::Connecting);
                    next_stream
                }
                Err(connect_err) => {
                    inner
                        .outbound_state
                        .insert(key.clone(), ConnectionState::Disconnected);
                    debug!(?connect_err, target = %key.addr, "outbound interconnect reconnect failed");
                    sleep(inner.options.reconnect_backoff).await;
                    continue;
                }
            }
        };

        let handle = ConnectionHandle {
            peer_addr: key.addr,
            tx: {
                let Some(existing) = inner.outbound.get(&key) else {
                    return;
                };
                existing.tx.clone()
            },
        };

        if let Err(err) = run_connection_loop(
            inner.clone(),
            key.addr,
            handle,
            Some(key.clone()),
            &mut rx,
            io_stream,
            &mut pending,
        )
        .await
        {
            inner
                .outbound_state
                .insert(key.clone(), ConnectionState::Disconnected);
            debug!(?err, target = %key.addr, "outbound interconnect connection closed");
            if shutdown.is_cancelled() {
                return;
            }
            sleep(inner.options.reconnect_backoff).await;
        } else {
            return;
        }
    }
}

type BoxedIo = Box<dyn AsyncReadWrite>;

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

async fn accept_inbound_stream(
    inner: &Arc<TransportInner>,
    stream: TcpStream,
    peer_addr: SocketAddr,
) -> Result<BoxedIo, TransportError> {
    match inner.mode {
        TransportMode::Plain => Ok(Box::new(stream)),
        TransportMode::Tls => {
            let acceptor = TlsAcceptor::from(
                inner
                    .server_config
                    .clone()
                    .ok_or(TransportError::MissingTlsConfig)?,
            );
            acceptor
                .accept(stream)
                .await
                .map(|stream| Box::new(stream) as BoxedIo)
                .map_err(|err| {
                    warn!(?err, %peer_addr, "failed to accept interconnect tls connection");
                    TransportError::Io(io::Error::other(err.to_string()))
                })
        }
    }
}

async fn connect_outbound_stream(
    inner: &Arc<TransportInner>,
    key: &ConnectionKey,
) -> Result<BoxedIo, TransportError> {
    let tcp = TcpStream::connect(key.addr).await.map_err(|err| {
        debug!(?err, target = %key.addr, "outbound interconnect connect failed");
        TransportError::Io(err)
    })?;
    configure_socket(&tcp)?;

    match key.mode {
        TransportMode::Plain => Ok(Box::new(tcp)),
        TransportMode::Tls => {
            let server_name = ServerName::try_from(key.server_name.clone())
                .map_err(|_| TransportError::InvalidServerName(key.server_name.clone()))?;
            let connector = TlsConnector::from(
                inner
                    .client_config
                    .clone()
                    .ok_or(TransportError::MissingTlsConfig)?,
            );
            connector
                .connect(server_name, tcp)
                .await
                .map(|stream| Box::new(stream) as BoxedIo)
                .map_err(|err| {
                    debug!(?err, target = %key.addr, "outbound interconnect tls connect failed");
                    TransportError::Io(io::Error::other(err.to_string()))
                })
        }
    }
}

async fn run_connection_loop(
    inner: Arc<TransportInner>,
    peer_addr: SocketAddr,
    reply_handle: ConnectionHandle,
    outbound_key: Option<ConnectionKey>,
    rx: &mut mpsc::Receiver<Envelope>,
    io_stream: BoxedIo,
    pending: &mut Option<Envelope>,
) -> Result<(), TransportError> {
    let (mut reader, mut writer) = tokio::io::split(io_stream);
    let mut keepalive = interval(PING_INTERVAL);
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_ping_at = Instant::now();
    let mut pending = pending.take().map(WireEnvelope::Payload);
    write_wire_envelope(
        &mut writer,
        &WireEnvelope::Introduction(inner.identity.signed_introduction()),
    )
    .await?;
    let peer_node_id = read_and_verify_introduction(
        &mut reader,
        inner.options.max_frame_bytes,
        &inner.peer_verifier,
    )
    .await?;
    if let Some(outbound_key) = outbound_key.as_ref() {
        inner
            .outbound_state
            .insert(outbound_key.clone(), ConnectionState::Connected);
    }
    register_connected_peer(&inner, &peer_node_id);

    let result = loop {
        tokio::task::consume_budget().await;
        let ping_deadline = last_ping_at + PING_TIMEOUT;
        tokio::select! {
            biased;
            _ = inner.shutdown.cancelled() => break Ok(()),
            result = read_wire_envelope(&mut reader, inner.options.max_frame_bytes) => {
                match result? {
                    WireEnvelope::Introduction(_) => {
                        break Err(TransportError::InvalidHandshake(
                            "received duplicate introduction".to_string(),
                        ));
                    }
                    WireEnvelope::Ping => {
                        last_ping_at = Instant::now();
                    }
                    WireEnvelope::Payload(envelope) => {
                        last_ping_at = Instant::now();
                        inner
                            .incoming_tx
                            .send(ReceivedEnvelope {
                                peer_addr,
                                peer_node_id: peer_node_id.clone(),
                                envelope,
                                reply: reply_handle.clone(),
                            })
                            .await
                            .map_err(|_| TransportError::ShuttingDown)?;
                    }
                }
            }
            _ = sleep_until(ping_deadline) => {
                break Err(TransportError::Closed(peer_addr));
            }
            _ = keepalive.tick(), if pending.is_none() => {
                pending = Some(WireEnvelope::Ping);
            }
            maybe_envelope = rx.recv(), if pending.is_none() => {
                match maybe_envelope {
                    Some(envelope) => pending = Some(WireEnvelope::Payload(envelope)),
                    None => break Ok(()),
                }
            }
            result = async {
                let Some(envelope) = pending.as_ref() else {
                    return Ok(());
                };
                write_wire_envelope(&mut writer, envelope).await
            }, if pending.is_some() => {
                result?;
                pending = None;
            }
        }
    };
    unregister_connected_peer(&inner, &peer_node_id);
    result
}

fn register_connected_peer(inner: &TransportInner, peer_node_id: &str) {
    inner
        .connected_peers
        .entry(peer_node_id.to_string())
        .and_modify(|count| *count += 1)
        .or_insert(1);
}

fn unregister_connected_peer(inner: &TransportInner, peer_node_id: &str) {
    let Some(mut count) = inner.connected_peers.get_mut(peer_node_id) else {
        return;
    };
    if *count <= 1 {
        drop(count);
        inner.connected_peers.remove(peer_node_id);
    } else {
        *count -= 1;
    }
}

fn configure_socket(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)
}

async fn write_wire_envelope<W>(
    writer: &mut W,
    envelope: &WireEnvelope,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = encode_wire_envelope(envelope)?;
    writer
        .write_u32(bytes.len() as u32)
        .await
        .map_err(TransportError::Io)?;
    writer.write_all(&bytes).await.map_err(TransportError::Io)?;
    writer.flush().await.map_err(TransportError::Io)
}

async fn read_wire_envelope<R>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<WireEnvelope, TransportError>
where
    R: AsyncRead + Unpin,
{
    let frame_size = reader.read_u32().await.map_err(TransportError::Io)? as usize;
    if frame_size > max_frame_bytes {
        return Err(TransportError::FrameTooLarge {
            size: frame_size,
            limit: max_frame_bytes,
        });
    }
    let mut bytes = vec![0u8; frame_size];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(TransportError::Io)?;
    decode_wire_envelope(&bytes)
}

async fn read_and_verify_introduction<R>(
    reader: &mut R,
    max_frame_bytes: usize,
    verifier: &PeerVerifier,
) -> Result<String, TransportError>
where
    R: AsyncRead + Unpin,
{
    match read_wire_envelope(reader, max_frame_bytes).await? {
        WireEnvelope::Introduction(intro) => intro.verify(verifier),
        WireEnvelope::Ping => Err(TransportError::InvalidHandshake(
            "first message must be an introduction".to_string(),
        )),
        WireEnvelope::Payload(_) => Err(TransportError::InvalidHandshake(
            "first message must be an introduction".to_string(),
        )),
    }
}

fn encode_wire_envelope(envelope: &WireEnvelope) -> Result<Vec<u8>, TransportError> {
    match envelope {
        WireEnvelope::Introduction(intro) => {
            let mut bytes = vec![WIRE_TAG_INTRODUCTION];
            bytes.extend(
                rkyv::to_bytes::<rkyv::rancor::Error>(intro)
                    .map(|value| value.to_vec())
                    .map_err(|err| TransportError::Encode(err.to_string()))?,
            );
            Ok(bytes)
        }
        WireEnvelope::Ping => Ok(vec![WIRE_TAG_PING]),
        WireEnvelope::Payload(Envelope::RelayPayload(payload)) => {
            let mut bytes = vec![WIRE_TAG_RELAY_PAYLOAD];
            encode_stream_payload(payload, &mut bytes)?;
            Ok(bytes)
        }
        WireEnvelope::Payload(Envelope::Ack(ack)) => {
            let mut bytes = vec![WIRE_TAG_ACK];
            bytes.extend(
                rkyv::to_bytes::<rkyv::rancor::Error>(ack)
                    .map(|value| value.to_vec())
                    .map_err(|err| TransportError::Encode(err.to_string()))?,
            );
            Ok(bytes)
        }
        WireEnvelope::Payload(Envelope::Control(control)) => {
            let mut bytes = vec![WIRE_TAG_CONTROL];
            bytes.extend(
                rkyv::to_bytes::<rkyv::rancor::Error>(control)
                    .map(|value| value.to_vec())
                    .map_err(|err| TransportError::Encode(err.to_string()))?,
            );
            Ok(bytes)
        }
    }
}

fn decode_wire_envelope(bytes: &[u8]) -> Result<WireEnvelope, TransportError> {
    let Some((&tag, payload)) = bytes.split_first() else {
        return Err(TransportError::Decode("wire frame is empty".to_string()));
    };
    match tag {
        WIRE_TAG_INTRODUCTION => {
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(payload);
            let introduction =
                rkyv::from_bytes::<SignedIntroduction, rkyv::rancor::Error>(&aligned)
                    .map_err(|err| TransportError::Decode(err.to_string()))?;
            Ok(WireEnvelope::Introduction(introduction))
        }
        WIRE_TAG_PING => {
            if !payload.is_empty() {
                return Err(TransportError::Decode(
                    "ping wire frame must not contain payload".to_string(),
                ));
            }
            Ok(WireEnvelope::Ping)
        }
        WIRE_TAG_RELAY_PAYLOAD => Ok(WireEnvelope::Payload(Envelope::RelayPayload(
            decode_stream_payload(payload)?,
        ))),
        WIRE_TAG_ACK => {
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(payload);
            let ack = rkyv::from_bytes::<RemoteAckResolution, rkyv::rancor::Error>(&aligned)
                .map_err(|err| TransportError::Decode(err.to_string()))?;
            Ok(WireEnvelope::Payload(Envelope::Ack(ack)))
        }
        WIRE_TAG_CONTROL => {
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(payload);
            let control = rkyv::from_bytes::<ControlEnvelope, rkyv::rancor::Error>(&aligned)
                .map_err(|err| TransportError::Decode(err.to_string()))?;
            Ok(WireEnvelope::Payload(Envelope::Control(control)))
        }
        _ => Err(TransportError::Decode(format!(
            "unknown wire envelope tag {tag}"
        ))),
    }
}

fn encode_stream_payload(
    payload: &RelayPayload,
    bytes: &mut Vec<u8>,
) -> Result<(), TransportError> {
    bytes.push(payload.kind.wire_tag());
    encode_string(bytes, payload.domain.as_str())?;
    encode_string(bytes, payload.relay.as_str())?;
    encode_branch_key(bytes, &payload.key)?;
    encode_len(bytes, payload.metadata.len())?;
    for metadata in &payload.metadata {
        bytes.extend_from_slice(
            &metadata
                .ingested_at_low_watermark
                .unix_nanos()
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &metadata
                .ingested_at_high_watermark
                .unix_nanos()
                .to_be_bytes(),
        );
    }
    encode_len(bytes, payload.acks.len())?;
    for ack in &payload.acks {
        match ack {
            Some(ack) => {
                bytes.push(1);
                bytes.extend_from_slice(&ack.ack_id.to_be_bytes());
                encode_string(bytes, ack.reply_node_id.as_str())?;
            }
            None => bytes.push(0),
        }
    }
    encode_bytes(bytes, &payload.batch_ipc)?;
    Ok(())
}

fn decode_stream_payload(bytes: &[u8]) -> Result<RelayPayload, TransportError> {
    let mut cursor = WireCursor::new(bytes);
    let kind = RelayPayloadKind::from_wire_tag(cursor.read_u8()?)?;
    let domain_raw = cursor.read_string()?;
    let relay_raw = cursor.read_string()?;
    let key = cursor.read_branch_key()?;
    let metadata_count = cursor.read_len()?;
    let mut metadata = Vec::with_capacity(metadata_count);
    for _ in 0..metadata_count {
        metadata.push(RemoteRuntimeRecordMetadata {
            ingested_at_low_watermark: Timestamp::from_unix_nanos(cursor.read_i64()?),
            ingested_at_high_watermark: Timestamp::from_unix_nanos(cursor.read_i64()?),
        });
    }
    let ack_count = cursor.read_len()?;
    let mut acks = Vec::with_capacity(ack_count);
    for _ in 0..ack_count {
        match cursor.read_u8()? {
            0 => acks.push(None),
            1 => {
                let ack_id = cursor.read_u64()?;
                let reply_node_id = cursor.read_string()?;
                acks.push(Some(RemoteAckRegistration {
                    ack_id,
                    reply_node_id,
                }));
            }
            flag => {
                return Err(TransportError::Decode(format!(
                    "invalid relay ack presence flag {flag}"
                )));
            }
        }
    }
    let batch_ipc = cursor.read_bytes()?.to_vec();
    cursor.finish()?;
    let domain = Domain::try_from(domain_raw.as_str()).map_err(|error| {
        TransportError::Decode(format!("invalid domain '{domain_raw}': {error}"))
    })?;
    let relay = Identifier::try_from(relay_raw.as_str()).map_err(|error| {
        TransportError::Decode(format!("invalid relay identifier '{relay_raw}': {error}"))
    })?;
    Ok(RelayPayload {
        kind,
        domain,
        relay,
        key,
        batch_ipc,
        metadata,
        acks,
    })
}

fn encode_len(bytes: &mut Vec<u8>, len: usize) -> Result<(), TransportError> {
    let len = u32::try_from(len)
        .map_err(|_| TransportError::Encode(format!("length {len} exceeds u32::MAX")))?;
    bytes.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn encode_branch_key(
    bytes: &mut Vec<u8>,
    key: &Option<Vec<RemoteRuntimeField>>,
) -> Result<(), TransportError> {
    let Some(fields) = key else {
        bytes.push(0);
        return Ok(());
    };
    if fields.is_empty() {
        return Err(TransportError::Encode(
            "branch key must contain at least one field".to_string(),
        ));
    }
    bytes.push(1);
    encode_len(bytes, fields.len())?;
    for field in fields {
        encode_string(bytes, field.name.as_str())?;
        encode_remote_value(bytes, &field.value)?;
    }
    Ok(())
}

fn encode_remote_value(
    bytes: &mut Vec<u8>,
    value: &RemoteRuntimeValue,
) -> Result<(), TransportError> {
    match value {
        RemoteRuntimeValue::U8(value) => {
            bytes.push(0);
            bytes.push(*value);
        }
        RemoteRuntimeValue::I8(value) => {
            bytes.push(1);
            bytes.push(value.to_be_bytes()[0]);
        }
        RemoteRuntimeValue::U16(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::I16(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::U32(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::I32(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::U64(value) => {
            bytes.push(6);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::I64(value) => {
            bytes.push(7);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::Bool(value) => {
            bytes.push(8);
            bytes.push(u8::from(*value));
        }
        RemoteRuntimeValue::String(value) => {
            bytes.push(9);
            encode_string(bytes, value)?;
        }
        RemoteRuntimeValue::Datetime(value) => {
            bytes.push(10);
            encode_string(bytes, value)?;
        }
        RemoteRuntimeValue::F32(value) => {
            bytes.push(11);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        RemoteRuntimeValue::F64(value) => {
            bytes.push(12);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        RemoteRuntimeValue::Array(values) => {
            bytes.push(13);
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
        RemoteRuntimeValue::Vec(values) => {
            bytes.push(14);
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
    }
    Ok(())
}

fn encode_remote_element_value(
    bytes: &mut Vec<u8>,
    value: &RemoteRuntimeElementValue,
) -> Result<(), TransportError> {
    match value {
        RemoteRuntimeElementValue::U8(value) => {
            bytes.push(0);
            bytes.push(*value);
        }
        RemoteRuntimeElementValue::I8(value) => {
            bytes.push(1);
            bytes.push(value.to_be_bytes()[0]);
        }
        RemoteRuntimeElementValue::U16(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::I16(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::U32(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::I32(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::U64(value) => {
            bytes.push(6);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::I64(value) => {
            bytes.push(7);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::Bool(value) => {
            bytes.push(8);
            bytes.push(u8::from(*value));
        }
        RemoteRuntimeElementValue::String(value) => {
            bytes.push(9);
            encode_string(bytes, value)?;
        }
        RemoteRuntimeElementValue::Datetime(value) => {
            bytes.push(10);
            encode_string(bytes, value)?;
        }
        RemoteRuntimeElementValue::F32(value) => {
            bytes.push(11);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        RemoteRuntimeElementValue::F64(value) => {
            bytes.push(12);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
    }
    Ok(())
}

fn encode_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), TransportError> {
    encode_len(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn encode_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), TransportError> {
    encode_bytes(bytes, value.as_bytes())
}

struct WireCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WireCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, TransportError> {
        let bytes = self.read_exact(1)?;
        Ok(bytes[0])
    }

    fn read_i8(&mut self) -> Result<i8, TransportError> {
        Ok(i8::from_be_bytes([self.read_u8()?]))
    }

    fn read_u16(&mut self) -> Result<u16, TransportError> {
        let bytes = self.read_exact(2)?;
        let mut raw = [0u8; 2];
        raw.copy_from_slice(bytes);
        Ok(u16::from_be_bytes(raw))
    }

    fn read_i16(&mut self) -> Result<i16, TransportError> {
        let bytes = self.read_exact(2)?;
        let mut raw = [0u8; 2];
        raw.copy_from_slice(bytes);
        Ok(i16::from_be_bytes(raw))
    }

    fn read_u32(&mut self) -> Result<u32, TransportError> {
        let bytes = self.read_exact(4)?;
        let mut raw = [0u8; 4];
        raw.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(raw))
    }

    fn read_i32(&mut self) -> Result<i32, TransportError> {
        let bytes = self.read_exact(4)?;
        let mut raw = [0u8; 4];
        raw.copy_from_slice(bytes);
        Ok(i32::from_be_bytes(raw))
    }

    fn read_u64(&mut self) -> Result<u64, TransportError> {
        let bytes = self.read_exact(8)?;
        let mut raw = [0u8; 8];
        raw.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(raw))
    }

    fn read_i64(&mut self) -> Result<i64, TransportError> {
        let bytes = self.read_exact(8)?;
        let mut raw = [0u8; 8];
        raw.copy_from_slice(bytes);
        Ok(i64::from_be_bytes(raw))
    }

    fn read_f32(&mut self) -> Result<f32, TransportError> {
        Ok(f32::from_bits(self.read_u32()?))
    }

    fn read_f64(&mut self) -> Result<f64, TransportError> {
        Ok(f64::from_bits(self.read_u64()?))
    }

    fn read_len(&mut self) -> Result<usize, TransportError> {
        usize::try_from(self.read_u32()?)
            .map_err(|_| TransportError::Decode("wire length does not fit usize".to_string()))
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], TransportError> {
        let len = self.read_len()?;
        self.read_exact(len)
    }

    fn read_string(&mut self) -> Result<String, TransportError> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            TransportError::Decode(format!("invalid utf-8 in wire frame: {error}"))
        })
    }

    fn read_branch_key(&mut self) -> Result<Option<Vec<RemoteRuntimeField>>, TransportError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let len = self.read_len()?;
                if len == 0 {
                    return Err(TransportError::Decode(
                        "branch key must contain at least one field".to_string(),
                    ));
                }
                let mut fields = Vec::with_capacity(len);
                for _ in 0..len {
                    fields.push(RemoteRuntimeField {
                        name: self.read_string()?,
                        value: self.read_remote_value()?,
                    });
                }
                Ok(Some(fields))
            }
            flag => Err(TransportError::Decode(format!(
                "invalid branch key presence flag {flag}"
            ))),
        }
    }

    fn read_remote_value(&mut self) -> Result<RemoteRuntimeValue, TransportError> {
        match self.read_u8()? {
            0 => Ok(RemoteRuntimeValue::U8(self.read_u8()?)),
            1 => Ok(RemoteRuntimeValue::I8(self.read_i8()?)),
            2 => Ok(RemoteRuntimeValue::U16(self.read_u16()?)),
            3 => Ok(RemoteRuntimeValue::I16(self.read_i16()?)),
            4 => Ok(RemoteRuntimeValue::U32(self.read_u32()?)),
            5 => Ok(RemoteRuntimeValue::I32(self.read_i32()?)),
            6 => Ok(RemoteRuntimeValue::U64(self.read_u64()?)),
            7 => Ok(RemoteRuntimeValue::I64(self.read_i64()?)),
            8 => match self.read_u8()? {
                0 => Ok(RemoteRuntimeValue::Bool(false)),
                1 => Ok(RemoteRuntimeValue::Bool(true)),
                value => Err(TransportError::Decode(format!(
                    "invalid bool value {value} in branch key"
                ))),
            },
            9 => Ok(RemoteRuntimeValue::String(self.read_string()?)),
            10 => Ok(RemoteRuntimeValue::Datetime(self.read_string()?)),
            11 => Ok(RemoteRuntimeValue::F32(self.read_f32()?)),
            12 => Ok(RemoteRuntimeValue::F64(self.read_f64()?)),
            13 => {
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                Ok(RemoteRuntimeValue::Array(values))
            }
            14 => {
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                Ok(RemoteRuntimeValue::Vec(values))
            }
            tag => Err(TransportError::Decode(format!(
                "unknown branch key value tag {tag}"
            ))),
        }
    }

    fn read_remote_element_value(&mut self) -> Result<RemoteRuntimeElementValue, TransportError> {
        match self.read_u8()? {
            0 => Ok(RemoteRuntimeElementValue::U8(self.read_u8()?)),
            1 => Ok(RemoteRuntimeElementValue::I8(self.read_i8()?)),
            2 => Ok(RemoteRuntimeElementValue::U16(self.read_u16()?)),
            3 => Ok(RemoteRuntimeElementValue::I16(self.read_i16()?)),
            4 => Ok(RemoteRuntimeElementValue::U32(self.read_u32()?)),
            5 => Ok(RemoteRuntimeElementValue::I32(self.read_i32()?)),
            6 => Ok(RemoteRuntimeElementValue::U64(self.read_u64()?)),
            7 => Ok(RemoteRuntimeElementValue::I64(self.read_i64()?)),
            8 => match self.read_u8()? {
                0 => Ok(RemoteRuntimeElementValue::Bool(false)),
                1 => Ok(RemoteRuntimeElementValue::Bool(true)),
                value => Err(TransportError::Decode(format!(
                    "invalid bool value {value} in branch key element"
                ))),
            },
            9 => Ok(RemoteRuntimeElementValue::String(self.read_string()?)),
            10 => Ok(RemoteRuntimeElementValue::Datetime(self.read_string()?)),
            11 => Ok(RemoteRuntimeElementValue::F32(self.read_f32()?)),
            12 => Ok(RemoteRuntimeElementValue::F64(self.read_f64()?)),
            tag => Err(TransportError::Decode(format!(
                "unknown branch key element value tag {tag}"
            ))),
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], TransportError> {
        let Some(end) = self.offset.checked_add(len) else {
            return Err(TransportError::Decode(
                "wire frame length overflow".to_string(),
            ));
        };
        if end > self.bytes.len() {
            return Err(TransportError::Decode(
                "wire frame ended unexpectedly".to_string(),
            ));
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn finish(&self) -> Result<(), TransportError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(TransportError::Decode(
                "wire frame contained trailing bytes".to_string(),
            ))
        }
    }
}

fn introduction_message(node_id: &str) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + node_id.len());
    data.extend_from_slice(&(node_id.len() as u32).to_be_bytes());
    data.extend_from_slice(node_id.as_bytes());
    data
}

pub fn install_rustls_crypto_provider() {
    static PROVIDER: OnceLock<()> = OnceLock::new();
    PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[derive(Clone)]
pub struct TlsConfigBundle {
    client_config: Arc<ClientConfig>,
    server_config: Arc<ServerConfig>,
}

impl TlsConfigBundle {
    pub fn from_pem_files(
        ca_cert_path: impl AsRef<Path>,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, TlsConfigError> {
        install_rustls_crypto_provider();

        let ca_certs = load_certificates(ca_cert_path.as_ref())?;
        let cert_chain = load_certificates(cert_path.as_ref())?;
        let private_key = load_private_key(key_path.as_ref())?;

        let mut roots = RootCertStore::empty();
        for cert in ca_certs {
            roots.add(cert)?;
        }

        let client_config = ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(cert_chain.clone(), private_key.clone_key())?;

        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|err| TlsConfigError::Io(io::Error::other(err.to_string())))?;
        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, private_key)?;

        Ok(Self {
            client_config: Arc::new(client_config),
            server_config: Arc::new(server_config),
        })
    }
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(map_pem_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_pem_error)?;
    if certs.is_empty() {
        return Err(TlsConfigError::MissingCertificate(
            path.display().to_string(),
        ));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    match PrivateKeyDer::from_pem_file(path) {
        Ok(key) => Ok(key),
        Err(PemError::NoItemsFound) => Err(TlsConfigError::MissingPrivateKey(
            path.display().to_string(),
        )),
        Err(err) => Err(map_pem_error(err)),
    }
}

fn map_pem_error(err: PemError) -> TlsConfigError {
    match err {
        PemError::NoItemsFound => TlsConfigError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "no PEM items found",
        )),
        PemError::Io(err) => TlsConfigError::Io(err),
        other => TlsConfigError::Io(io::Error::new(io::ErrorKind::InvalidData, other)),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::ErrorKind, path::PathBuf, process::Command, sync::Arc};

    use nervix_models::{Domain, Identifier};
    use tokio::time::timeout;

    use super::*;

    fn tls_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tls/dev")
            .join(name)
    }

    fn ensure_dev_tls_assets() {
        static DEV_TLS_READY: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        DEV_TLS_READY.get_or_init(|| {
            let status = Command::new("bash")
                .arg("scripts/generate_dev_tls.sh")
                .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
                .status()
                .expect("dev tls generation command should run");
            assert!(
                status.success(),
                "dev tls generation should succeed: {status}"
            );
        });
    }

    fn test_tls() -> TlsConfigBundle {
        ensure_dev_tls_assets();
        TlsConfigBundle::from_pem_files(
            tls_path("ca.pem"),
            tls_path("node.pem"),
            tls_path("node-key.pem"),
        )
        .expect("test tls should load")
    }

    fn test_identity(node_id: &str) -> LocalIdentity {
        LocalIdentity::generate(node_id)
    }

    fn verifier_for(identities: &[&LocalIdentity]) -> PeerVerifier {
        let keys = Arc::new(
            identities
                .iter()
                .map(|identity| (identity.node_id().to_string(), identity.public_key()))
                .collect::<HashMap<_, _, ahash::RandomState>>(),
        );
        PeerVerifier::new(move |node_id| keys.get(node_id).copied())
    }

    async fn recv_one(rx: &mut mpsc::Receiver<ReceivedEnvelope>) -> ReceivedEnvelope {
        timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for envelope")
            .expect("incoming channel closed")
    }

    fn dummy_stream_payload(stream: &str) -> RelayPayload {
        RelayPayload {
            kind: RelayPayloadKind::Routed,
            domain: Domain::try_from("test").expect("valid domain"),
            relay: Identifier::try_from(stream).expect("valid identifier"),
            key: None,
            batch_ipc: vec![1, 2, 3, 4],
            metadata: vec![RemoteRuntimeRecordMetadata {
                ingested_at_low_watermark: Timestamp::from_unix_nanos(1),
                ingested_at_high_watermark: Timestamp::from_unix_nanos(2),
            }],
            acks: vec![None],
        }
    }

    #[test]
    fn relay_payload_branch_key_roundtrips_native_fields() {
        let mut payload = dummy_stream_payload("orders");
        payload.key = Some(vec![
            RemoteRuntimeField {
                name: "tenant".to_string(),
                value: RemoteRuntimeValue::String("acme".to_string()),
            },
            RemoteRuntimeField {
                name: "user_id".to_string(),
                value: RemoteRuntimeValue::U32(42),
            },
        ]);

        let mut bytes = Vec::new();
        encode_stream_payload(&payload, &mut bytes).expect("payload should encode");
        let decoded = decode_stream_payload(&bytes).expect("payload should decode");

        assert_eq!(decoded, payload);
    }

    #[test]
    fn relay_payload_without_branch_key_roundtrips_as_absent() {
        let payload = dummy_stream_payload("orders");
        let mut bytes = Vec::new();
        encode_stream_payload(&payload, &mut bytes).expect("payload should encode");
        let decoded = decode_stream_payload(&bytes).expect("payload should decode");

        assert_eq!(decoded.key, None);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn relay_payload_empty_branch_key_is_rejected() {
        let mut bytes = Vec::new();
        let error = encode_branch_key(&mut bytes, &Some(Vec::new()))
            .expect_err("empty branch key must be rejected");

        assert!(error.to_string().contains("at least one field"));
    }

    #[tokio::test]
    async fn bidirectional_send_and_receive_roundtrips() {
        let options = TransportOptions::default();
        let identity_a = test_identity("node-a");
        let identity_b = test_identity("node-b");
        let (transport_a, mut incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_a.clone(),
            verifier_for(&[&identity_b]),
            options.clone(),
        )
        .await
        .expect("bind transport a");
        let (transport_b, mut incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            options,
        )
        .await
        .expect("bind transport b");

        transport_a
            .send(
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::RelayPayload(dummy_stream_payload("orders")),
            )
            .await
            .expect("send a->b");

        let first = recv_one(&mut incoming_b).await;
        assert_eq!(first.peer_node_id, "node-a");
        assert_eq!(
            first.envelope,
            Envelope::RelayPayload(dummy_stream_payload("orders"))
        );

        first
            .reply
            .send(Envelope::RelayPayload(dummy_stream_payload("orders")))
            .await
            .expect("reply b->a");

        let second = recv_one(&mut incoming_a).await;
        assert_eq!(second.peer_node_id, "node-b");
        assert_eq!(
            second.envelope,
            Envelope::RelayPayload(dummy_stream_payload("orders"))
        );

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn outbound_pool_reuses_connections() {
        let options = TransportOptions::default();
        let identity_a = test_identity("node-a");
        let identity_b = test_identity("node-b");
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_a.clone(),
            verifier_for(&[&identity_b]),
            options.clone(),
        )
        .await
        .expect("bind transport a");
        let (transport_b, mut incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            options,
        )
        .await
        .expect("bind transport b");

        for _ in 1..=2 {
            transport_a
                .send(
                    transport_b.local_addr(),
                    "localhost",
                    TransportMode::Tls,
                    Envelope::RelayPayload(dummy_stream_payload("metrics")),
                )
                .await
                .expect("send");
        }

        let _ = recv_one(&mut incoming_b).await;
        let _ = recv_one(&mut incoming_b).await;
        assert_eq!(transport_a.active_outbound_connections().await, 1);

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn connection_for_reuses_disconnected_outbound_handle() {
        let options = TransportOptions::default();
        let identity_a = test_identity("node-a");
        let identity_b = test_identity("node-b");
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_a.clone(),
            verifier_for(&[&identity_b]),
            options.clone(),
        )
        .await
        .expect("bind transport a");
        let (transport_b, mut incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            options,
        )
        .await
        .expect("bind transport b");

        transport_a
            .send(
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::RelayPayload(dummy_stream_payload("metrics")),
            )
            .await
            .expect("initial send");
        let _ = recv_one(&mut incoming_b).await;

        let key = ConnectionKey {
            addr: transport_b.local_addr(),
            server_name: "localhost".to_string(),
            mode: TransportMode::Tls,
        };
        transport_a
            .inner
            .outbound_state
            .insert(key.clone(), ConnectionState::Disconnected);

        let handle = transport_a
            .connection_for(transport_b.local_addr(), "localhost", TransportMode::Tls)
            .await
            .expect("disconnected handle should still be reusable");
        handle
            .send(Envelope::RelayPayload(dummy_stream_payload("metrics")))
            .await
            .expect("queued send should succeed");

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn both_peers_observe_active_connection() {
        let options = TransportOptions::default();
        let identity_a = test_identity("node-a");
        let identity_b = test_identity("node-b");
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_a.clone(),
            verifier_for(&[&identity_b]),
            options.clone(),
        )
        .await
        .expect("bind transport a");
        let (transport_b, mut incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            options,
        )
        .await
        .expect("bind transport b");

        transport_a
            .send(
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::Control(ControlEnvelope::Terminate),
            )
            .await
            .expect("send should establish connection");

        let _ = recv_one(&mut incoming_b).await;

        timeout(Duration::from_secs(5), async {
            loop {
                if transport_a.is_connected_to("node-b") && transport_b.is_connected_to("node-a") {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("both peers should observe the connection");

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn outbound_pool_respects_max_connections() {
        let options = TransportOptions {
            max_connections: 1,
            ..TransportOptions::default()
        };
        let identity_a = test_identity("node-a");
        let identity_b = test_identity("node-b");
        let identity_c = test_identity("node-c");
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_a.clone(),
            verifier_for(&[&identity_b, &identity_c]),
            options.clone(),
        )
        .await
        .expect("bind transport a");
        let (transport_b, _incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            options.clone(),
        )
        .await
        .expect("bind transport b");
        let (transport_c, _incoming_c) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_c.clone(),
            verifier_for(&[&identity_a]),
            options,
        )
        .await
        .expect("bind transport c");

        transport_a
            .send(
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::Control(ControlEnvelope::Terminate),
            )
            .await
            .expect("first send should acquire pool slot");

        let err = transport_a
            .send(
                transport_c.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::Control(ControlEnvelope::Terminate),
            )
            .await
            .expect_err("second distinct target should exceed pool");
        assert!(matches!(err, TransportError::PoolExhausted));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
        transport_c.shutdown().await;
    }

    #[tokio::test]
    async fn outbound_connection_reconnects_after_peer_restart() {
        let options = TransportOptions {
            reconnect_backoff: Duration::from_millis(100),
            ..TransportOptions::default()
        };
        let identity_a = test_identity("node-a");
        let identity_b = test_identity("node-b");
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_a.clone(),
            verifier_for(&[&identity_b]),
            options.clone(),
        )
        .await
        .expect("bind transport a");
        let (transport_b, mut incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            options.clone(),
        )
        .await
        .expect("bind transport b");
        let target = transport_b.local_addr();

        transport_a
            .send(
                target,
                "localhost",
                TransportMode::Tls,
                Envelope::RelayPayload(dummy_stream_payload("reconnect")),
            )
            .await
            .expect("initial send");
        let first = recv_one(&mut incoming_b).await;
        assert_eq!(first.peer_node_id, "node-a");
        assert_eq!(
            first.envelope,
            Envelope::RelayPayload(dummy_stream_payload("reconnect"))
        );

        transport_b.shutdown().await;

        let send_fut = transport_a.send(
            target,
            "localhost",
            TransportMode::Tls,
            Envelope::RelayPayload(dummy_stream_payload("reconnect")),
        );

        let (transport_b2, mut incoming_b2) = Transport::bind(
            target,
            TransportMode::Tls,
            Some(test_tls()),
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            options,
        )
        .await
        .expect("restart transport b");

        send_fut.await.expect("queued send should succeed");
        let second = recv_one(&mut incoming_b2).await;
        assert_eq!(second.peer_node_id, "node-a");
        assert_eq!(
            second.envelope,
            Envelope::RelayPayload(dummy_stream_payload("reconnect"))
        );

        transport_a.shutdown().await;
        transport_b2.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_signature_closes_connection() {
        let options = TransportOptions {
            reconnect_backoff: Duration::from_millis(50),
            ..TransportOptions::default()
        };
        let identity_a = test_identity("node-a");
        let identity_b = test_identity("node-b");
        let wrong_public = SigningKey::generate(&mut OsRng).verifying_key();
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_a,
            verifier_for(&[&identity_b]),
            options.clone(),
        )
        .await
        .expect("bind transport a");
        let (transport_b, mut incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_b,
            PeerVerifier::new(move |node_id| {
                if node_id == "node-a" {
                    Some(wrong_public)
                } else {
                    None
                }
            }),
            options,
        )
        .await
        .expect("bind transport b");

        transport_a
            .send(
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::RelayPayload(dummy_stream_payload("auth")),
            )
            .await
            .expect("enqueue send");

        let result = timeout(Duration::from_millis(500), incoming_b.recv()).await;
        assert!(
            result.is_err(),
            "peer should reject invalid signature before delivery"
        );

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn peer_that_stops_sending_pings_is_disconnected() {
        let options = TransportOptions::default();
        let identity_a = test_identity("node-a");
        let identity_b = test_identity("node-b");
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportMode::Tls,
            Some(test_tls()),
            identity_a.clone(),
            verifier_for(&[&identity_b]),
            options,
        )
        .await
        .expect("bind transport a");

        let key = ConnectionKey {
            addr: transport_a.local_addr(),
            server_name: "localhost".to_string(),
            mode: TransportMode::Tls,
        };
        let inner = Arc::new(TransportInner {
            mode: TransportMode::Tls,
            client_config: Some(test_tls().client_config.clone()),
            server_config: None,
            identity: identity_b.clone(),
            peer_verifier: verifier_for(&[&identity_a]),
            options: TransportOptions::default(),
            local_addr: "127.0.0.1:0".parse().unwrap(),
            incoming_tx: mpsc::channel(1).0,
            outbound: DashMap::default(),
            outbound_state: DashMap::default(),
            connected_peers: DashMap::default(),
            outbound_permits: Arc::new(Semaphore::new(1)),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        });
        let tls_stream = connect_outbound_stream(&inner, &key)
            .await
            .expect("connect raw tls relay");
        let (mut reader, mut writer) = tokio::io::split(tls_stream);

        write_wire_envelope(
            &mut writer,
            &WireEnvelope::Introduction(identity_b.signed_introduction()),
        )
        .await
        .expect("send introduction");
        let peer = read_and_verify_introduction(
            &mut reader,
            DEFAULT_MAX_FRAME_BYTES,
            &verifier_for(&[&identity_a]),
        )
        .await
        .expect("read server introduction");
        assert_eq!(peer, "node-a");

        timeout(Duration::from_secs(5), async {
            loop {
                match read_wire_envelope(&mut reader, DEFAULT_MAX_FRAME_BYTES).await {
                    Ok(WireEnvelope::Ping) => {}
                    Ok(other) => panic!("unexpected frame before disconnect: {other:?}"),
                    Err(TransportError::Io(err)) if err.kind() == ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(err) => panic!("unexpected read error: {err:?}"),
                }
            }
        })
        .await
        .expect("timed out waiting for ping timeout disconnect");

        transport_a.shutdown().await;
    }
}
