//! The authenticated transport between Nervix nodes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Mutual-TLS connections, Ed25519 peer authentication, framing, backpressure,
//!   reconnect, and the envelope set nodes exchange, including relay payload carriage.
//! - **Depends on.** The vocabulary for the names and values an envelope carries.
//! - **Must not know.** Why a message is sent. It has no view of domains, graphs, schedules or the
//!   runtime.
//!
//! The remaining hand-written control request pairs still break this contract and migrate to the
//! typed request primitive by attrition.

use std::{
    collections::{BTreeMap, BTreeSet},
    hash::RandomState,
    io,
    net::SocketAddr,
    path::Path,
    sync::{Arc as StdArc, OnceLock},
    time::Duration,
};

use ahash::HashMap;
use arch_into::ArchInto as _;
use dashmap::{DashMap, mapref::entry::Entry};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use meticulous::ResultExt as _;
use nervix_models::{
    ClusterNodeName, CodecName, DomainName, DomainTick, EmitterName, FieldName, IngestorName,
    LookupName, ModelKind, ModelName, NodeRef, RelayName, RemoteAckRegistration,
    RemoteAckResolution, RemoteRuntimeElementValue, RemoteRuntimeField,
    RemoteRuntimeRecordMetadata, RemoteRuntimeValue, ResourceName, SubscriptionBinding, Timestamp,
};
use nervix_recovery::{Discarded as _, Reported as _};
use rand_core::OsRng;
use rkyv::{Archive, Deserialize, Serialize};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
};
use rustls_pki_types::pem::{Error as PemError, PemObject};
use strum::{FromRepr, IntoStaticStr};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, warn};
use triomphe::Arc;

mod connection;
mod request;

#[cfg(test)]
use connection::{connect_outbound_stream, drive_connection, exchange_introductions};
use connection::{
    retire_outbound_connection, run_inbound_connection, spawn_outbound_connection,
    unregister_connected_peer,
};
pub use request::{
    HandlerRegistrationError, InterconnectRequest, RemoteRequestFailure, RequestContext,
    RequestError,
};
use request::{RequestEnvelope, RequestState, ResponseEnvelope};

const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_SEND_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_INCOMING_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_RECONNECT_BACKOFF_MS: u64 = 200;
const DEFAULT_MAX_RECONNECT_BACKOFF_MS: u64 = 5_000;
const DEFAULT_CONNECTION_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_QUEUE_ADMISSION_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
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
    pub max_reconnect_backoff: Duration,
    pub connection_setup_timeout: Duration,
    pub queue_admission_timeout: Duration,
    pub shutdown_drain_timeout: Duration,
    pub send_queue_capacity: usize,
    pub incoming_queue_capacity: usize,
    pub max_frame_bytes: usize,
}

impl Default for TransportOptions {
    fn default() -> Self {
        Self {
            max_connections: 32,
            reconnect_backoff: Duration::from_millis(DEFAULT_RECONNECT_BACKOFF_MS),
            max_reconnect_backoff: Duration::from_millis(DEFAULT_MAX_RECONNECT_BACKOFF_MS),
            connection_setup_timeout: DEFAULT_CONNECTION_SETUP_TIMEOUT,
            queue_admission_timeout: DEFAULT_QUEUE_ADMISSION_TIMEOUT,
            shutdown_drain_timeout: DEFAULT_SHUTDOWN_DRAIN_TIMEOUT,
            send_queue_capacity: DEFAULT_SEND_QUEUE_CAPACITY,
            incoming_queue_capacity: DEFAULT_INCOMING_QUEUE_CAPACITY,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

impl TransportOptions {
    fn validation_error(&self) -> Option<TransportError> {
        if self.max_connections == 0 {
            return Some(TransportError::InvalidOptions {
                reason: "max_connections must be greater than zero",
            });
        }
        if self.send_queue_capacity == 0 {
            return Some(TransportError::InvalidOptions {
                reason: "send_queue_capacity must be greater than zero",
            });
        }
        if self.incoming_queue_capacity == 0 {
            return Some(TransportError::InvalidOptions {
                reason: "incoming_queue_capacity must be greater than zero",
            });
        }
        if self.max_frame_bytes == 0 {
            return Some(TransportError::InvalidOptions {
                reason: "max_frame_bytes must be greater than zero",
            });
        }
        if self.reconnect_backoff < Duration::from_millis(1) {
            return Some(TransportError::InvalidOptions {
                reason: "reconnect_backoff must be at least one millisecond",
            });
        }
        if self.max_reconnect_backoff < self.reconnect_backoff {
            return Some(TransportError::InvalidOptions {
                reason: "max_reconnect_backoff must not be below reconnect_backoff",
            });
        }
        if u64::try_from(self.max_reconnect_backoff.as_millis()).is_err() {
            return Some(TransportError::InvalidOptions {
                reason: "max_reconnect_backoff must fit in milliseconds",
            });
        }
        if self.connection_setup_timeout.is_zero() {
            return Some(TransportError::InvalidOptions {
                reason: "connection_setup_timeout must be greater than zero",
            });
        }
        if self.queue_admission_timeout.is_zero() {
            return Some(TransportError::InvalidOptions {
                reason: "queue_admission_timeout must be greater than zero",
            });
        }
        if self.shutdown_drain_timeout.is_zero() {
            return Some(TransportError::InvalidOptions {
                reason: "shutdown_drain_timeout must be greater than zero",
            });
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransportMode {
    Plain,
    Tls,
}

/// One currently advertised address at which a live peer accepts interconnect connections.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerTarget {
    pub addr: SocketAddr,
    pub server_name: String,
    pub mode: TransportMode,
}

impl PeerTarget {
    pub fn new(addr: SocketAddr, server_name: impl Into<String>, mode: TransportMode) -> Self {
        Self {
            addr,
            server_name: server_name.into(),
            mode,
        }
    }
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
    pub domain: DomainName,
    pub relay: RelayName,
    pub key: Option<Vec<RemoteRuntimeField>>,
    pub batch_ipc: Vec<u8>,
    pub metadata: Vec<RemoteRuntimeRecordMetadata>,
    pub acks: Vec<Option<RemoteAckRegistration>>,
    pub admission: Option<RemoteAckRegistration>,
}

macro_rules! declare_relay_payload_kinds {
    ($($Kind:ident = $tag:literal,)+) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum RelayPayloadKind {
            $($Kind,)+
        }

        impl RelayPayloadKind {
            fn wire_tag(self) -> u8 {
                match self {
                    $(Self::$Kind => $tag,)+
                }
            }

            fn from_wire_tag(tag: u8) -> Result<Self, TransportError> {
                match tag {
                    $($tag => Ok(Self::$Kind),)+
                    _ => Err(TransportError::Decode(format!(
                        "unknown relay payload kind tag {tag}"
                    ))),
                }
            }
        }
    };
}

declare_relay_payload_kinds! {
    Routed = 1,
    SubscriptionFanout = 2,
    Ingress = 3,
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
    Request(RequestEnvelope),
    Response(ResponseEnvelope),
    DataflowNodeStatusRequest(DataflowNodeStatusRequest),
    DataflowNodeStatusResponse(DataflowNodeStatusResponse),
    DomainDrainStatusRequest(DomainDrainStatusRequest),
    DomainDrainStatusResponse(DomainDrainStatusResponse),
    EntityGateRequest(EntityGateRequest),
    EntityGateResponse(EntityGateResponse),
    EntityDrainStatusRequest(EntityDrainStatusRequest),
    EntityDrainStatusResponse(EntityDrainStatusResponse),
    EntityGateReleaseRequest(EntityGateReleaseRequest),
    EntityGateReleaseResponse(EntityGateReleaseResponse),
    DescribeMetricsRequest(DescribeMetricsRequest),
    DescribeMetricsResponse(DescribeMetricsResponse),
    DescribeRelayRequest(DescribeRelayRequest),
    DescribeRelayResponse(DescribeRelayResponse),
    DescribeLookupRequest(DescribeLookupRequest),
    DescribeLookupResponse(DescribeLookupResponse),
    LookupRequest(LookupRequest),
    LookupResponse(LookupResponse),
    SubscriptionInterestVisibilityRequest(SubscriptionInterestVisibilityRequest),
    SubscriptionInterestVisibilityResponse(SubscriptionInterestVisibilityResponse),
    RuntimeErrorEvent(RuntimeErrorEvent),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionInterestVisibilityRequest {
    pub correlation_id: u64,
    pub subscriber_node_id: ClusterNodeName,
    pub domain: DomainName,
    pub relay: RelayName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionInterestVisibilityResponse {
    pub correlation_id: u64,
    pub visible: bool,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeErrorEvent {
    pub message: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainClockStart {
    pub domain_id: DomainName,
    pub owner_node_id: ClusterNodeName,
    pub wall_started_at: Timestamp,
    pub logical_start: Timestamp,
    pub time_rate: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainClockStop {
    pub domain_id: DomainName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainTickEnvelope {
    pub domain_id: DomainName,
    pub tick: DomainTick,
}

macro_rules! declare_runtime_state_kinds {
    ($($Kind:ident = $tag:literal,)+) => {
        /// The kinds of runtime state a node persists, and the byte each one occupies in a
        /// storage key.
        #[derive(
            Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, Hash, FromRepr,
        )]
        #[repr(u8)]
        pub enum RuntimeStateKind {
            $($Kind = $tag,)+
        }

        impl From<RuntimeStateKind> for u8 {
            fn from(value: RuntimeStateKind) -> Self {
                match value {
                    $(RuntimeStateKind::$Kind => $tag,)+
                }
            }
        }
    };
}

declare_runtime_state_kinds! {
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
    pub domain: DomainName,
    pub state: RuntimeStateKind,
    pub kind: ModelKind,
    pub identifier: ModelName,
    pub schema_fingerprint: [u8; 32],
    pub branch_key: Option<Vec<RemoteRuntimeField>>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSnapshotEnvelope {
    pub lsm: u64,
    pub schema_fingerprint: [u8; 32],
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
    pub quiesce_state: Option<String>,
    pub quiesce_buffered_records: u64,
    pub quiesce_buffered_bytes: u64,
    pub quiesce_dropped_total: u64,
    pub quiesce_rejected_total: u64,
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
    pub domain: DomainName,
    pub kind: ModelKind,
    pub name: ModelName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataflowNodeStatusResponse {
    pub correlation_id: u64,
    pub result: Result<DataflowNodeStatusEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainDrainStatusEnvelope {
    pub active_ingestors: u64,
    pub active_generators: u64,
    pub outstanding_acks: u64,
    pub buffered_emitter_messages: u64,
    pub emitter_publishing: Vec<EmitterPublishingDrainStatusEnvelope>,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum EmitterPublishingDrainStateEnvelope {
    AwaitingConfirmation,
    RetryingInfrastructure,
    RetryingIcebergCommit,
}

impl EmitterPublishingDrainStateEnvelope {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmitterPublishingDrainStatusEnvelope {
    pub emitter: EmitterName,
    pub state: EmitterPublishingDrainStateEnvelope,
    pub pending_messages: u64,
    pub retry_backoff_millis: Option<u64>,
    pub retry_wait_millis: Option<u64>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainDrainStatusRequest {
    pub correlation_id: u64,
    pub domain: DomainName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainDrainStatusResponse {
    pub correlation_id: u64,
    pub result: Result<DomainDrainStatusEnvelope, String>,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum EntityGatePurpose {
    ModelAlteration,
    OwnershipHandoff,
}

impl EntityGatePurpose {
    pub const fn operation_name(self) -> &'static str {
        match self {
            Self::ModelAlteration => "model alteration",
            Self::OwnershipHandoff => "ownership handoff",
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateRequest {
    pub correlation_id: u64,
    pub operation_id: u64,
    pub domain: DomainName,
    pub relays: Vec<RelayName>,
    pub affected_entities: Vec<NodeRef>,
    pub purpose: EntityGatePurpose,
    pub deadline_millis: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateResponse {
    pub correlation_id: u64,
    pub result: Result<(), String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityDrainStatusEnvelope {
    pub buffered_relay_batches: u64,
    pub node_work_items: u64,
    pub outstanding_acks: u64,
    pub emitter_publishing: Vec<EmitterPublishingDrainStatusEnvelope>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityDrainStatusRequest {
    pub correlation_id: u64,
    pub domain: DomainName,
    pub relays: Vec<RelayName>,
    pub affected_entities: Vec<NodeRef>,
    pub purpose: EntityGatePurpose,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityDrainStatusResponse {
    pub correlation_id: u64,
    pub result: Result<EntityDrainStatusEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateReleaseRequest {
    pub correlation_id: u64,
    pub operation_id: u64,
    pub domain: DomainName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateReleaseResponse {
    pub correlation_id: u64,
    pub result: Result<(), String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsRequest {
    pub correlation_id: u64,
    pub domain: DomainName,
    pub kind: ModelKind,
    pub name: ModelName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsResponse {
    pub correlation_id: u64,
    pub result: Result<DescribeMetricsEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsEnvelope {
    pub metrics: Vec<String>,
    pub state: Vec<String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeIngestorRequest {
    pub domain: DomainName,
    pub name: IngestorName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeRelayRequest {
    pub correlation_id: u64,
    pub domain: DomainName,
    pub relay: RelayName,
    pub bindings: Vec<SubscriptionBinding>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeRelayResponse {
    pub correlation_id: u64,
    pub result: Result<bool, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct LookupDescribeEnvelope {
    pub resource: ResourceName,
    pub resource_version: u64,
    pub path: String,
    pub decode_using_codec: CodecName,
    pub key_field: FieldName,
    pub entry_count: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeLookupRequest {
    pub correlation_id: u64,
    pub domain: DomainName,
    pub name: LookupName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeLookupResponse {
    pub correlation_id: u64,
    pub result: Result<LookupDescribeEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct LookupRequest {
    pub correlation_id: u64,
    pub domain: DomainName,
    pub name: LookupName,
    pub key: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct LookupResponse {
    pub correlation_id: u64,
    pub result: Result<Option<Vec<u8>>, String>,
}

#[derive(Debug, Clone)]
pub struct ReceivedEnvelope {
    pub peer_addr: SocketAddr,
    pub peer_node_id: ClusterNodeName,
    pub envelope: Envelope,
    pub reply: ConnectionHandle,
}

#[derive(Debug)]
struct ConnectionHandleInner {
    peer_addr: SocketAddr,
    tx: mpsc::Sender<Envelope>,
    connection_cancel: CancellationToken,
    admission_closed: CancellationToken,
    queue_admission_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    inner: Arc<ConnectionHandleInner>,
}

impl ConnectionHandle {
    fn new(
        peer_addr: SocketAddr,
        tx: mpsc::Sender<Envelope>,
        connection_cancel: CancellationToken,
        admission_closed: CancellationToken,
        queue_admission_timeout: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(ConnectionHandleInner {
                peer_addr,
                tx,
                connection_cancel,
                admission_closed,
                queue_admission_timeout,
            }),
        }
    }

    pub async fn send(&self, envelope: Envelope) -> Result<(), TransportError> {
        if self.inner.admission_closed.is_cancelled() {
            return Err(TransportError::ShuttingDown);
        }
        if self.inner.connection_cancel.is_cancelled() {
            return Err(TransportError::Closed(self.inner.peer_addr));
        }

        tokio::select! {
            biased;
            _ = self.inner.admission_closed.cancelled() => Err(TransportError::ShuttingDown),
            _ = self.inner.connection_cancel.cancelled() => {
                Err(TransportError::Closed(self.inner.peer_addr))
            }
            result = timeout(self.inner.queue_admission_timeout, self.inner.tx.send(envelope)) => {
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(_)) => Err(TransportError::Closed(self.inner.peer_addr)),
                    Err(_) => Err(TransportError::QueueAdmissionTimeout {
                        peer: self.inner.peer_addr,
                        timeout: self.inner.queue_admission_timeout,
                    }),
                }
            }
        }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.inner.peer_addr
    }

    fn cancel(&self) {
        self.inner.connection_cancel.cancel();
    }

    fn cancellation(&self) -> &CancellationToken {
        &self.inner.connection_cancel
    }
}

#[derive(Clone)]
pub struct Transport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    mode: TransportMode,
    client_config: Option<StdArc<ClientConfig>>,
    server_config: Option<StdArc<ServerConfig>>,
    identity: LocalIdentity,
    peer_verifier: PeerVerifier,
    options: TransportOptions,
    local_addr: SocketAddr,
    incoming_tx: mpsc::Sender<ReceivedEnvelope>,
    outbound: DashMap<ConnectionKey, OutboundConnection, RandomState>,
    connected_peers:
        DashMap<ClusterNodeName, HashMap<CancellationToken, ConnectionHandle>, RandomState>,
    requests: RequestState,
    outbound_permits: StdArc<Semaphore>,
    admission_gate: parking_lot::RwLock<()>,
    admission_closed: CancellationToken,
    draining: CancellationToken,
    force_close: CancellationToken,
    tasks: TaskTracker,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct ConnectionKey {
    peer_node_id: ClusterNodeName,
    addr: SocketAddr,
    server_name: String,
    mode: TransportMode,
}

impl ConnectionKey {
    fn new(peer_node_id: ClusterNodeName, target: &PeerTarget) -> Self {
        Self {
            peer_node_id,
            addr: target.addr,
            server_name: target.server_name.clone(),
            mode: target.mode,
        }
    }
}

struct OutboundConnection {
    handle: ConnectionHandle,
    cancel: CancellationToken,
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("invalid transport options: {reason}")]
    InvalidOptions { reason: &'static str },
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
    #[error("connection setup with {peer} timed out after {timeout:?}")]
    ConnectionSetupTimeout { peer: SocketAddr, timeout: Duration },
    #[error("timed out after {timeout:?} waiting to queue data for {peer}")]
    QueueAdmissionTimeout { peer: SocketAddr, timeout: Duration },
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
    node_id: ClusterNodeName,
    signing_key: SigningKey,
}

type PeerKeyResolver = dyn Fn(&ClusterNodeName) -> Option<VerifyingKey> + Send + Sync;

impl std::fmt::Debug for LocalIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalIdentity")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl LocalIdentity {
    pub fn generate(node_id: ClusterNodeName) -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        Self {
            node_id,
            signing_key,
        }
    }

    pub fn node_id(&self) -> &ClusterNodeName {
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
    resolver: Arc<Box<PeerKeyResolver>>,
}

impl std::fmt::Debug for PeerVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerVerifier").finish_non_exhaustive()
    }
}

impl PeerVerifier {
    pub fn new(
        resolver: impl Fn(&ClusterNodeName) -> Option<VerifyingKey> + Send + Sync + 'static,
    ) -> Self {
        Self {
            resolver: Arc::new(Box::new(resolver)),
        }
    }

    fn resolve(&self, node_id: &ClusterNodeName) -> Option<VerifyingKey> {
        (self.resolver)(node_id)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct SignedIntroduction {
    node_id: ClusterNodeName,
    signature: [u8; 64],
}

impl SignedIntroduction {
    fn verify(&self, verifier: &PeerVerifier) -> Result<ClusterNodeName, TransportError> {
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
        if let Some(error) = options.validation_error() {
            return Err(error);
        }
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
            connected_peers: DashMap::default(),
            requests: RequestState::default(),
            outbound_permits: StdArc::new(Semaphore::new(options.max_connections)),
            admission_gate: parking_lot::RwLock::new(()),
            admission_closed: CancellationToken::new(),
            draining: CancellationToken::new(),
            force_close: CancellationToken::new(),
            tasks: TaskTracker::new(),
        });

        spawn_accept_loop(inner.clone(), listener);

        Ok((Self { inner }, incoming_rx))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    pub fn node_id(&self) -> &ClusterNodeName {
        self.inner.identity.node_id()
    }

    /// Queues an envelope on the connection that must authenticate as `peer_node_id`.
    pub async fn send(
        &self,
        peer_node_id: &ClusterNodeName,
        target: SocketAddr,
        server_name: &str,
        mode: TransportMode,
        envelope: Envelope,
    ) -> Result<(), TransportError> {
        let handle = self.connection_for(peer_node_id, target, server_name, mode)?;
        handle.send(envelope).await
    }

    /// Returns the single persistent driver for an expected peer and concrete target.
    pub fn connection_for(
        &self,
        peer_node_id: &ClusterNodeName,
        target: SocketAddr,
        server_name: &str,
        mode: TransportMode,
    ) -> Result<ConnectionHandle, TransportError> {
        // Shutdown takes the write side before closing the task tracker, so every admitted driver
        // is registered with the tracker before its bounded wait can observe an empty transport.
        let _admission_guard = self.inner.admission_gate.read();
        if self.inner.admission_closed.is_cancelled() {
            return Err(TransportError::ShuttingDown);
        }

        let key = ConnectionKey {
            peer_node_id: peer_node_id.clone(),
            addr: target,
            server_name: server_name.to_string(),
            mode,
        };
        match self.inner.outbound.entry(key.clone()) {
            Entry::Occupied(existing) => Ok(existing.get().handle.clone()),
            Entry::Vacant(entry) => {
                let permit = self
                    .inner
                    .outbound_permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| TransportError::PoolExhausted)?;
                let (tx, rx) = mpsc::channel(self.inner.options.send_queue_capacity);
                let cancel = CancellationToken::new();
                let handle = ConnectionHandle::new(
                    target,
                    tx,
                    cancel.clone(),
                    self.inner.admission_closed.clone(),
                    self.inner.options.queue_admission_timeout,
                );
                let inserted = entry.insert(OutboundConnection {
                    handle: handle.clone(),
                    cancel: cancel.clone(),
                });
                drop(inserted);
                spawn_outbound_connection(
                    self.inner.clone(),
                    key,
                    handle.clone(),
                    cancel,
                    rx,
                    permit,
                );
                Ok(handle)
            }
        }
    }

    pub async fn active_outbound_connections(&self) -> usize {
        self.inner.outbound.len()
    }

    /// Reconciles the concrete addresses at which this node may dial each current peer.
    ///
    /// Any connection or reconnect driver for a replaced address is retired immediately. A later
    /// completion from that driver is fenced by its cancellation identity and cannot remove or
    /// register a replacement created for the same address.
    pub fn replace_outbound_targets(
        &self,
        targets: &BTreeMap<ClusterNodeName, BTreeSet<PeerTarget>>,
    ) {
        let all_targets = targets
            .iter()
            .flat_map(|(peer_node_id, peer_targets)| {
                peer_targets
                    .iter()
                    .map(|target| ConnectionKey::new(peer_node_id.clone(), target))
            })
            .collect::<BTreeSet<_>>();
        let retired = self
            .inner
            .outbound
            .iter()
            .filter(|entry| !all_targets.contains(entry.key()))
            .map(|entry| (entry.key().clone(), entry.cancel.clone()))
            .collect::<Vec<_>>();
        for (key, cancel) in retired {
            retire_outbound_connection(&self.inner, &key, &cancel);
        }
    }

    pub fn is_connected_to(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .connected_peers
            .get(node_id)
            .is_some_and(|connections| !connections.is_empty())
    }

    pub async fn shutdown(&self) {
        {
            let _admission_guard = self.inner.admission_gate.write();
            self.inner.admission_closed.cancel();
            self.inner.tasks.close();
        }
        self.inner.requests.shutdown();
        self.inner.draining.cancel();
        if timeout(
            self.inner.options.shutdown_drain_timeout,
            self.inner.tasks.wait(),
        )
        .await
        .is_err()
        {
            self.inner.force_close.cancel();
            let connections = self
                .inner
                .connected_peers
                .iter()
                .flat_map(|peer| peer.values().cloned().collect::<Vec<_>>())
                .collect::<Vec<_>>();
            for connection in connections {
                connection.cancel();
            }
            let reconnects = self
                .inner
                .outbound
                .iter()
                .map(|entry| entry.cancel.clone())
                .collect::<Vec<_>>();
            for reconnect in reconnects {
                reconnect.cancel();
            }
            timeout(
                self.inner.options.connection_setup_timeout,
                self.inner.tasks.wait(),
            )
            .await
            .reported("waiting for cancelled interconnect tasks to finish");
        }
        self.inner.outbound.clear();
        self.inner.connected_peers.clear();
    }

    fn retire_departed_connections(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
        let connected = self
            .inner
            .connected_peers
            .iter()
            .filter(|peer| !live_nodes.contains(peer.key()))
            .flat_map(|peer| peer.values().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        for connection in connected {
            connection.cancel();
        }

        let outbound = self
            .inner
            .outbound
            .iter()
            .filter(|entry| !live_nodes.contains(&entry.key().peer_node_id))
            .map(|entry| (entry.key().clone(), entry.cancel.clone()))
            .collect::<Vec<_>>();
        for (key, cancel) in outbound {
            retire_outbound_connection(&self.inner, &key, &cancel);
        }
    }
}

fn spawn_accept_loop(inner: Arc<TransportInner>, listener: TcpListener) {
    let draining = inner.draining.clone();
    let force_close = inner.force_close.clone();
    let tasks = inner.tasks.clone();
    tasks.spawn(async move {
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                biased;
                _ = force_close.cancelled() => break,
                _ = draining.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((stream, peer_addr)) = accepted else {
                        if !draining.is_cancelled() {
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
                        if let Err(err) = run_inbound_connection(inner.clone(), stream, peer_addr).await
                            && !inner.draining.is_cancelled()
                        {
                            debug!(?err, %peer_addr, "inbound interconnect connection closed");
                        }
                    });
                }
            }
        }
    });
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
    let frame_size = u32::try_from(bytes.len()).map_err(|_| {
        TransportError::Encode(format!(
            "wire envelope length {} exceeds u32::MAX",
            bytes.len()
        ))
    })?;
    writer
        .write_u32(frame_size)
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
    let frame_size = reader
        .read_u32()
        .await
        .map_err(TransportError::Io)?
        .arch_into();
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
) -> Result<ClusterNodeName, TransportError>
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
    match &payload.admission {
        Some(admission) => {
            bytes.push(1);
            bytes.extend_from_slice(&admission.ack_id.to_be_bytes());
            encode_string(bytes, admission.reply_node_id.as_str())?;
        }
        None => bytes.push(0),
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
                let reply_node_raw = cursor.read_string()?;
                let reply_node_id =
                    ClusterNodeName::try_from(reply_node_raw.as_str()).map_err(|error| {
                        TransportError::Decode(format!(
                            "invalid node id '{reply_node_raw}': {error}"
                        ))
                    })?;
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
    let admission = match cursor.read_u8()? {
        0 => None,
        1 => {
            let ack_id = cursor.read_u64()?;
            let reply_node_raw = cursor.read_string()?;
            let reply_node_id =
                ClusterNodeName::try_from(reply_node_raw.as_str()).map_err(|error| {
                    TransportError::Decode(format!("invalid node id '{reply_node_raw}': {error}"))
                })?;
            Some(RemoteAckRegistration {
                ack_id,
                reply_node_id,
            })
        }
        flag => {
            return Err(TransportError::Decode(format!(
                "invalid relay admission presence flag {flag}"
            )));
        }
    };
    let batch_ipc = cursor.read_bytes()?.to_vec();
    cursor.finish()?;
    let domain = DomainName::try_from(domain_raw.as_str()).map_err(|error| {
        TransportError::Decode(format!("invalid domain '{domain_raw}': {error}"))
    })?;
    let relay = RelayName::try_from(relay_raw.as_str()).map_err(|error| {
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
        admission,
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
        RemoteRuntimeElementValue::Array(values) => {
            bytes.push(13);
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
        RemoteRuntimeElementValue::Vec(values) => {
            bytes.push(14);
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
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
        Ok(self.read_u32()?.arch_into())
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
            13 => {
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                Ok(RemoteRuntimeElementValue::Array(values))
            }
            14 => {
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                Ok(RemoteRuntimeElementValue::Vec(values))
            }
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

fn introduction_message(node_id: &ClusterNodeName) -> Vec<u8> {
    let node_id = node_id.as_str();
    let mut data = Vec::with_capacity(4 + node_id.len());
    let node_id_len = u32::try_from(node_id.len())
        .assured("ClusterNodeName validation bounds every node identifier below u32::MAX bytes");
    data.extend_from_slice(&node_id_len.to_be_bytes());
    data.extend_from_slice(node_id.as_bytes());
    data
}

pub fn install_rustls_crypto_provider() {
    static PROVIDER: OnceLock<()> = OnceLock::new();
    PROVIDER.get_or_init(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .discarded(
                "a provider the host installed first is the one this transport would have \
                 installed",
            );
    });
}

#[derive(Clone)]
pub struct TlsConfigBundle {
    client_config: StdArc<ClientConfig>,
    server_config: StdArc<ServerConfig>,
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

        let verifier = WebPkiClientVerifier::builder(StdArc::new(roots))
            .build()
            .map_err(|err| TlsConfigError::Io(io::Error::other(err.to_string())))?;
        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, private_key)?;

        Ok(Self {
            client_config: StdArc::new(client_config),
            server_config: StdArc::new(server_config),
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
    use std::{
        collections::BTreeSet, io::ErrorKind, path::PathBuf, process::Command, sync::Arc as StdArc,
    };

    use ahash::HashMap;
    use error_stack::Report;
    use nervix_models::{DomainName, RelayName};
    use tokio::{
        sync::Notify,
        time::{sleep, timeout},
    };

    use super::*;

    mod connection_lifetime;

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

    fn test_identity(node_id: &ClusterNodeName) -> LocalIdentity {
        LocalIdentity::generate(node_id.clone())
    }

    fn verifier_for(identities: &[&LocalIdentity]) -> PeerVerifier {
        let keys = Arc::new(
            identities
                .iter()
                .map(|identity| (identity.node_id().clone(), identity.public_key()))
                .collect::<HashMap<_, _>>(),
        );
        PeerVerifier::new(move |node_id| keys.get(node_id).copied())
    }

    fn test_inner(
        identity: LocalIdentity,
        peer_verifier: PeerVerifier,
        incoming_tx: mpsc::Sender<ReceivedEnvelope>,
        max_connections: usize,
    ) -> Arc<TransportInner> {
        Arc::new(TransportInner {
            mode: TransportMode::Plain,
            client_config: None,
            server_config: None,
            identity,
            peer_verifier,
            options: TransportOptions::default(),
            local_addr: "127.0.0.1:0".parse().expect("valid test address"),
            incoming_tx,
            outbound: DashMap::default(),
            connected_peers: DashMap::default(),
            requests: RequestState::default(),
            outbound_permits: StdArc::new(Semaphore::new(max_connections)),
            admission_gate: parking_lot::RwLock::new(()),
            admission_closed: CancellationToken::new(),
            draining: CancellationToken::new(),
            force_close: CancellationToken::new(),
            tasks: TaskTracker::new(),
        })
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
            domain: DomainName::try_from("test").expect("valid domain"),
            relay: RelayName::try_from(stream).expect("valid relay name"),
            key: None,
            batch_ipc: vec![1, 2, 3, 4],
            metadata: vec![RemoteRuntimeRecordMetadata {
                ingested_at_low_watermark: Timestamp::from_unix_nanos(1),
                ingested_at_high_watermark: Timestamp::from_unix_nanos(2),
            }],
            acks: vec![None],
            admission: None,
        }
    }

    fn dummy_ingestor_describe(metrics: Vec<String>) -> IngestorDescribeEnvelope {
        IngestorDescribeEnvelope {
            running: true,
            ready: true,
            quiesce_state: None,
            quiesce_buffered_records: 0,
            quiesce_buffered_bytes: 0,
            quiesce_dropped_total: 0,
            quiesce_rejected_total: 0,
            memory_backpressure_paused: false,
            transient_error: None,
            reconnect_backoff: None,
            reconnect_wait_millis: None,
            kafka_domain_offsets: None,
            metrics,
        }
    }

    struct HangingRequest;

    impl InterconnectRequest for HangingRequest {
        type Response = ();

        const NAME: &'static str = "hanging_test_request";
        const TIMEOUT: Duration = Duration::from_millis(100);

        fn encode_request(&self) -> Result<Vec<u8>, Report<RequestError>> {
            Ok(Vec::new())
        }

        fn decode_request(payload: &[u8]) -> Result<Self, Report<RequestError>> {
            if payload.is_empty() {
                Ok(Self)
            } else {
                Err(Report::new(RequestError::Decode {
                    request: Self::NAME,
                }))
            }
        }

        fn encode_response(_response: &Self::Response) -> Result<Vec<u8>, Report<RequestError>> {
            Ok(Vec::new())
        }

        fn decode_response(payload: &[u8]) -> Result<Self::Response, Report<RequestError>> {
            if payload.is_empty() {
                Ok(())
            } else {
                Err(Report::new(RequestError::Decode {
                    request: Self::NAME,
                }))
            }
        }
    }

    async fn connected_plain_transports() -> (Transport, Transport, ClusterNodeName, ClusterNodeName)
    {
        let node_a = ClusterNodeName::parse("node-a").expect("valid name");
        let node_b = ClusterNodeName::parse("node-b").expect("valid name");
        let identity_a = test_identity(&node_a);
        let identity_b = test_identity(&node_b);
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().expect("valid address"),
            TransportMode::Plain,
            None,
            identity_a.clone(),
            verifier_for(&[&identity_b]),
            TransportOptions::default(),
        )
        .await
        .expect("bind transport a");
        let (transport_b, _incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().expect("valid address"),
            TransportMode::Plain,
            None,
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            TransportOptions::default(),
        )
        .await
        .expect("bind transport b");

        transport_a
            .connection_for(
                &node_b,
                transport_b.local_addr(),
                "localhost",
                TransportMode::Plain,
            )
            .expect("connect transports");
        timeout(Duration::from_secs(5), async {
            loop {
                tokio::task::consume_budget().await;
                if transport_a.is_connected_to(&node_b) {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("transport should authenticate the peer");
        let live_nodes = BTreeSet::from([node_a.clone(), node_b.clone()]);
        transport_a.replace_live_nodes(&live_nodes);
        transport_b.replace_live_nodes(&live_nodes);

        (transport_a, transport_b, node_a, node_b)
    }

    #[tokio::test]
    async fn typed_request_roundtrips_through_registered_handler() {
        let (transport_a, transport_b, _node_a, node_b) = connected_plain_transports().await;
        transport_b
            .register_handler::<DescribeIngestorRequest, _, _>(|_context, request| async move {
                Ok(dummy_ingestor_describe(vec![format!(
                    "{}:{}",
                    request.domain, request.name
                )]))
            })
            .expect("register describe ingestor handler");

        let response = transport_a
            .request(
                &node_b,
                DescribeIngestorRequest {
                    domain: DomainName::parse("analytics").expect("valid domain"),
                    name: IngestorName::parse("orders").expect("valid ingestor name"),
                },
            )
            .await
            .expect("typed request should complete")
            .expect("describe handler should succeed");

        assert_eq!(response.metrics, vec!["analytics:orders"]);

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn typed_request_classifies_timeout() {
        let (transport_a, transport_b, _node_a, node_b) = connected_plain_transports().await;
        transport_b
            .register_handler::<HangingRequest, _, _>(|_context, _request| async move {
                std::future::pending().await
            })
            .expect("register hanging handler");

        let error = transport_a
            .request(&node_b, HangingRequest)
            .await
            .expect_err("request should time out");

        assert!(matches!(
            error.current_context(),
            RequestError::Timeout { node, request, .. }
                if node == &node_b && request == &HangingRequest::NAME
        ));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn typed_request_is_cancelled_when_transport_shuts_down() {
        let (transport_a, transport_b, _node_a, node_b) = connected_plain_transports().await;
        let handled = Arc::new(Notify::new());
        transport_b
            .register_handler::<HangingRequest, _, _>({
                let handled = handled.clone();
                move |_context, _request| {
                    let handled = handled.clone();
                    async move {
                        handled.notify_one();
                        std::future::pending().await
                    }
                }
            })
            .expect("register hanging handler");
        let requester = transport_a.clone();
        let target = node_b.clone();
        let request = tokio::spawn(async move { requester.request(&target, HangingRequest).await });
        timeout(Duration::from_secs(5), handled.notified())
            .await
            .expect("handler should receive the request");

        transport_a.shutdown().await;
        let error = request
            .await
            .expect("request task should join")
            .expect_err("request should be cancelled");

        assert!(matches!(
            error.current_context(),
            RequestError::ShuttingDown { node, request }
                if node == &node_b && request == &HangingRequest::NAME
        ));

        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn typed_request_is_cancelled_when_target_leaves() {
        let (transport_a, transport_b, node_a, node_b) = connected_plain_transports().await;
        let handled = Arc::new(Notify::new());
        transport_b
            .register_handler::<HangingRequest, _, _>({
                let handled = handled.clone();
                move |_context, _request| {
                    let handled = handled.clone();
                    async move {
                        handled.notify_one();
                        std::future::pending().await
                    }
                }
            })
            .expect("register hanging handler");
        let requester = transport_a.clone();
        let target = node_b.clone();
        let request = tokio::spawn(async move { requester.request(&target, HangingRequest).await });
        timeout(Duration::from_secs(5), handled.notified())
            .await
            .expect("handler should receive the request");

        transport_a.replace_live_nodes(&BTreeSet::from([node_a]));
        let error = request
            .await
            .expect("request task should join")
            .expect_err("request should be cancelled");

        assert!(matches!(
            error.current_context(),
            RequestError::TargetLeft { node, request }
                if node == &node_b && request == &HangingRequest::NAME
        ));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
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
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
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
                transport_b.node_id(),
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::RelayPayload(dummy_stream_payload("orders")),
            )
            .await
            .expect("send a->b");

        let first = recv_one(&mut incoming_b).await;
        assert_eq!(
            first.peer_node_id,
            ClusterNodeName::parse("node-a").expect("valid name")
        );
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
        assert_eq!(
            second.peer_node_id,
            ClusterNodeName::parse("node-b").expect("valid name")
        );
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
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
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
                    transport_b.node_id(),
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
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
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
                transport_b.node_id(),
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::RelayPayload(dummy_stream_payload("metrics")),
            )
            .await
            .expect("initial send");
        let _ = recv_one(&mut incoming_b).await;

        let handle = transport_a
            .connection_for(
                transport_b.node_id(),
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
            )
            .expect("outbound handle should be reusable");
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
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
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
                transport_b.node_id(),
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
                if transport_a
                    .is_connected_to(&ClusterNodeName::parse("node-b").expect("valid name"))
                    && transport_b
                        .is_connected_to(&ClusterNodeName::parse("node-a").expect("valid name"))
                {
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
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
        let identity_c = test_identity(&ClusterNodeName::parse("node-c").expect("valid name"));
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
                transport_b.node_id(),
                transport_b.local_addr(),
                "localhost",
                TransportMode::Tls,
                Envelope::Control(ControlEnvelope::Terminate),
            )
            .await
            .expect("first send should acquire pool slot");

        let err = transport_a
            .send(
                transport_c.node_id(),
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
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
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
                identity_b.node_id(),
                target,
                "localhost",
                TransportMode::Tls,
                Envelope::RelayPayload(dummy_stream_payload("reconnect")),
            )
            .await
            .expect("initial send");
        let first = recv_one(&mut incoming_b).await;
        assert_eq!(
            first.peer_node_id,
            ClusterNodeName::parse("node-a").expect("valid name")
        );
        assert_eq!(
            first.envelope,
            Envelope::RelayPayload(dummy_stream_payload("reconnect"))
        );

        transport_b.shutdown().await;

        let send_fut = transport_a.send(
            identity_b.node_id(),
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
        assert_eq!(
            second.peer_node_id,
            ClusterNodeName::parse("node-a").expect("valid name")
        );
        assert_eq!(
            second.envelope,
            Envelope::RelayPayload(dummy_stream_payload("reconnect"))
        );

        transport_a.shutdown().await;
        transport_b2.shutdown().await;
    }

    #[tokio::test]
    async fn connection_failure_retains_pending_payload_for_reconnect() {
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
        let (incoming_tx, _incoming_rx) = mpsc::channel(1);
        let inner = test_inner(identity_a, verifier_for(&[&identity_b]), incoming_tx, 1);
        let (client_io, mut peer_io) = tokio::io::duplex(64 * 1024);
        let peer_task = tokio::spawn(async move {
            let introduction = read_wire_envelope(&mut peer_io, DEFAULT_MAX_FRAME_BYTES)
                .await
                .expect("read client introduction");
            assert!(matches!(introduction, WireEnvelope::Introduction(_)));
            write_wire_envelope(
                &mut peer_io,
                &WireEnvelope::Introduction(identity_b.signed_introduction()),
            )
            .await
            .expect("write peer introduction");
        });
        let peer_addr = "127.0.0.1:12345".parse().unwrap();
        let (reply_tx, _reply_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let reply_handle = ConnectionHandle::new(
            peer_addr,
            reply_tx,
            cancel.clone(),
            inner.admission_closed.clone(),
            inner.options.queue_admission_timeout,
        );
        let (_send_tx, mut send_rx) = mpsc::channel(1);
        let expected = Envelope::RelayPayload(dummy_stream_payload("retry"));
        let mut retry_payload = Some(expected.clone());
        let established = exchange_introductions(&inner, Box::new(client_io))
            .await
            .expect("connection handshake should complete");

        let _ = drive_connection(
            inner.clone(),
            peer_addr,
            reply_handle,
            established,
            &cancel,
            &mut send_rx,
            &mut retry_payload,
        )
        .await
        .expect_err("peer disconnect should fail the connection");
        peer_task.await.expect("peer task should complete");

        assert_eq!(retry_payload, Some(expected));
        assert!(
            inner.connected_peers.is_empty(),
            "failed connection must unregister its connected peer"
        );
    }

    #[tokio::test]
    async fn invalid_signature_closes_connection() {
        let options = TransportOptions {
            reconnect_backoff: Duration::from_millis(50),
            ..TransportOptions::default()
        };
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
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
                if node_id.as_str() == "node-a" {
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
                transport_b.node_id(),
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
        let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
        let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
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
            peer_node_id: identity_a.node_id().clone(),
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
            connected_peers: DashMap::default(),
            requests: RequestState::default(),
            outbound_permits: StdArc::new(Semaphore::new(1)),
            admission_gate: parking_lot::RwLock::new(()),
            admission_closed: CancellationToken::new(),
            draining: CancellationToken::new(),
            force_close: CancellationToken::new(),
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
        assert_eq!(peer, ClusterNodeName::parse("node-a").expect("valid name"));

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
