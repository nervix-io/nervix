//! The authenticated HTTP/2 transport between Nervix nodes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Mutual TLS, class-isolated HTTP/2 pools, bounded rkyv messages, flow control,
//!   deadlines, relay transfer admission, reconciliation, and cancellation.
//! - **Depends on.** Execution admission and the vocabulary carried by internal operations.
//! - **Must not know.** Runtime graphs, schedules, or the semantic outcome of an operation.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    net::SocketAddr,
    sync::OnceLock,
    time::Duration,
};

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_execution::{ChargedBytes, CpuClass, Executor, MemoryClass, Reservation};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, CodecName, DomainClockProgress,
    DomainName, EmitterName, FieldName, IngestorName, LookupName, ModelKind, ModelName, NodeRef,
    OwnershipStateRecoveryOutcome, OwnershipStateReset, RelayName, RemoteAckRegistration,
    RemoteAckResolution, RemoteRuntimeField, RemoteRuntimeRecordMetadata, ResourceName,
    SubscriptionBinding,
};
use nervix_recovery::Discarded as _;
use rkyv::{Archive, Deserialize, Serialize};
use strum::{FromRepr, IntoStaticStr};
use thiserror::Error;
use tokio::sync::mpsc;

mod connection;
mod identity;
mod request;
mod wire;

pub use connection::{RelayAdmission, RelayCancellationGuard};
pub use identity::TlsConfigBundle;
pub use request::{
    HandlerRegistrationError, InterconnectRequest, RemoteRequestFailure, RequestContext,
    RequestError, RequestSubquota,
};
use request::{RequestEnvelope, RequestState, ResponseEnvelope};

const DEFAULT_MAX_PEERS: usize = 64;
const DEFAULT_MAX_CONNECTIONS: usize = 768;
const DEFAULT_MAX_CONCURRENT_HANDSHAKES: usize = 32;
const DEFAULT_INCOMING_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_STREAM_WINDOW_BYTES: u32 = 64 * 1024;
const DEFAULT_CONNECTION_WINDOW_BYTES: u32 = 256 * 1024;
const DEFAULT_MAX_HEADER_BYTES: u32 = 16 * 1024;
const DEFAULT_CONNECTION_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_millis(200);
const DEFAULT_MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const RELAY_GRANT_LIFETIME: Duration = Duration::from_secs(5);
pub(crate) const RKYV_RECORD_OVERHEAD_BYTES: u64 = 4 * 1024;

/// The independent connection pools that isolate internal traffic classes.
#[derive(
    Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub enum PoolClass {
    Management,
    Commands,
    Replication,
    Relay,
    Bulk,
}

impl PoolClass {
    pub const ALL: [Self; 5] = [
        Self::Management,
        Self::Commands,
        Self::Replication,
        Self::Relay,
        Self::Bulk,
    ];

    pub(crate) const PRECONNECTED: [Self; 4] = [
        Self::Management,
        Self::Commands,
        Self::Replication,
        Self::Relay,
    ];

    pub(crate) const fn is_preconnected(self) -> bool {
        matches!(
            self,
            Self::Management | Self::Commands | Self::Replication | Self::Relay
        )
    }

    pub(crate) fn preconnected_connections_per_peer() -> usize {
        let mut connections = 0usize;
        for class in Self::PRECONNECTED {
            connections = connections
                .checked_add(class.connections_per_peer())
                .assured("the fixed set of preconnected pool slots fits in usize");
        }
        connections
    }

    pub const fn connections_per_peer(self) -> usize {
        match self {
            Self::Relay => 2,
            Self::Management | Self::Commands | Self::Replication | Self::Bulk => 1,
        }
    }

    pub const fn stream_slots_per_connection(self) -> usize {
        match self {
            Self::Management | Self::Relay => 64,
            Self::Commands => 32,
            Self::Replication => 1,
            Self::Bulk => 4,
        }
    }

    pub(crate) const fn memory_class(self) -> MemoryClass {
        match self {
            Self::Management => MemoryClass::Management,
            Self::Commands | Self::Replication => MemoryClass::Commands,
            Self::Relay => MemoryClass::Relay,
            Self::Bulk => MemoryClass::Bulk,
        }
    }

    pub(crate) const fn cpu_class(self) -> CpuClass {
        match self {
            Self::Management | Self::Commands | Self::Replication => CpuClass::Control,
            Self::Relay => CpuClass::Data,
            Self::Bulk => CpuClass::Bulk,
        }
    }

    pub(crate) fn payload_limit(self, executor: &Executor) -> u64 {
        match self {
            Self::Management => executor.limits().management_event_bytes.as_u64(),
            Self::Commands | Self::Replication => executor.limits().command_bytes.as_u64(),
            Self::Relay => executor.limits().relay_encoded_bytes.as_u64(),
            Self::Bulk => executor
                .limits()
                .bulk_chunk_bytes
                .as_u64()
                .checked_add(RKYV_RECORD_OVERHEAD_BYTES)
                .assured("the bulk application limit leaves room inside a u64 for rkyv metadata"),
        }
    }

    pub(crate) fn control_body_limit(self, executor: &Executor) -> u64 {
        if self == Self::Bulk {
            return executor
                .limits()
                .bulk_chunk_bytes
                .as_u64()
                .checked_add(2 * RKYV_RECORD_OVERHEAD_BYTES)
                .assured(
                    "the bulk application limit leaves room inside a u64 for nested rkyv metadata",
                );
        }
        self.payload_limit(executor)
    }
}

#[derive(Debug, Clone)]
pub struct TransportOptions {
    pub max_peers: usize,
    pub max_connections: usize,
    pub max_concurrent_handshakes: usize,
    pub incoming_queue_capacity: usize,
    pub initial_stream_window_bytes: u32,
    pub initial_connection_window_bytes: u32,
    pub max_header_bytes: u32,
    pub connection_setup_timeout: Duration,
    pub request_timeout: Duration,
    pub progress_timeout: Duration,
    pub reconnect_backoff: Duration,
    pub max_reconnect_backoff: Duration,
    pub shutdown_drain_timeout: Duration,
}

impl Default for TransportOptions {
    fn default() -> Self {
        Self {
            max_peers: DEFAULT_MAX_PEERS,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_concurrent_handshakes: DEFAULT_MAX_CONCURRENT_HANDSHAKES,
            incoming_queue_capacity: DEFAULT_INCOMING_QUEUE_CAPACITY,
            initial_stream_window_bytes: DEFAULT_STREAM_WINDOW_BYTES,
            initial_connection_window_bytes: DEFAULT_CONNECTION_WINDOW_BYTES,
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
            connection_setup_timeout: DEFAULT_CONNECTION_SETUP_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            progress_timeout: DEFAULT_PROGRESS_TIMEOUT,
            reconnect_backoff: DEFAULT_RECONNECT_BACKOFF,
            max_reconnect_backoff: DEFAULT_MAX_RECONNECT_BACKOFF,
            shutdown_drain_timeout: DEFAULT_SHUTDOWN_DRAIN_TIMEOUT,
        }
    }
}

impl TransportOptions {
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        let nonzero = [
            (self.max_peers, "max_peers"),
            (self.max_connections, "max_connections"),
            (self.max_concurrent_handshakes, "max_concurrent_handshakes"),
            (self.incoming_queue_capacity, "incoming_queue_capacity"),
        ];
        for (value, name) in nonzero {
            if value == 0 {
                return Err(TransportError::InvalidOptions {
                    reason: format!("{name} must be greater than zero"),
                });
            }
        }
        if self.initial_stream_window_bytes == 0
            || self.initial_connection_window_bytes == 0
            || self.max_header_bytes == 0
        {
            return Err(TransportError::InvalidOptions {
                reason: "HTTP/2 windows and header limit must be greater than zero".to_string(),
            });
        }
        let preconnected_connections = self
            .max_peers
            .checked_mul(PoolClass::preconnected_connections_per_peer())
            .ok_or_else(|| TransportError::InvalidOptions {
                reason: "max_peers cannot be represented for every preconnected pool slot"
                    .to_string(),
            })?;
        let preconnected_connections =
            preconnected_connections.checked_mul(2).ok_or_else(|| {
                TransportError::InvalidOptions {
                    reason: "max_peers cannot be represented as inbound and outbound preconnected \
                             pools"
                        .to_string(),
                }
            })?;
        if self.max_connections <= preconnected_connections {
            return Err(TransportError::InvalidOptions {
                reason: "max_connections must reserve inbound and outbound management, command, \
                         replication, and relay capacity for every peer and at least one \
                         on-demand connection"
                    .to_string(),
            });
        }
        if self.connection_setup_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.progress_timeout.is_zero()
            || self.reconnect_backoff.is_zero()
            || self.shutdown_drain_timeout.is_zero()
        {
            return Err(TransportError::InvalidOptions {
                reason: "transport deadlines must be greater than zero".to_string(),
            });
        }
        if self.max_reconnect_backoff < self.reconnect_backoff {
            return Err(TransportError::InvalidOptions {
                reason: "max_reconnect_backoff must not be below reconnect_backoff".to_string(),
            });
        }
        Ok(())
    }
}

/// One advertised address and the certificate name expected there.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerTarget {
    pub addr: SocketAddr,
    pub server_name: String,
}

impl PeerTarget {
    pub fn new(addr: SocketAddr, server_name: impl Into<String>) -> Self {
        Self {
            addr,
            server_name: server_name.into(),
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
    pub delivery: RelayDelivery,
    pub kind: RelayPayloadKind,
    pub domain: DomainName,
    pub relay: RelayName,
    pub key: Option<Vec<RemoteRuntimeField>>,
    /// The batch's Arrow IPC body, encoded once and shared. Every destination in a fanout and
    /// every retry of one delivery carries this same allocation, charged once, and writes a slice
    /// of it to its socket.
    pub batch_ipc: ChargedBytes,
    pub metadata: Vec<RemoteRuntimeRecordMetadata>,
    pub acks: Vec<Option<RemoteAckRegistration>>,
    pub admission: Option<RemoteAckRegistration>,
}

/// The stable position of one relay batch in its sender-owned logical channel.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RelayDelivery {
    pub channel_incarnation: [u8; 16],
    pub sequence: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum RelayAdmissionStatus {
    Reserved,
    BodyReceived,
    Admitted,
    Rejected(String),
    Cancelled,
    Retired,
    Unknown,
    Indeterminate,
}

impl RelayAdmissionStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Admitted | Self::Rejected(_) | Self::Cancelled | Self::Retired
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayAdmissionDecision {
    Admitted,
    Cancelled,
}

#[derive(
    Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub enum RelayPayloadKind {
    Routed,
    SubscriptionFanout,
    Ingress,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum ControlEnvelope {
    Terminate,
    DomainClockProgress(DomainClockProgressEnvelope),
    StateReplicationAck(StateReplicationAck),
    StateCheckpointAvailable(StateCheckpointAvailable),
    Request(RequestEnvelope),
    Response(ResponseEnvelope),
    RuntimeErrorEvent(RuntimeErrorEvent),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionInterestVisibilityRequest {
    pub subscriber: ClusterNodeIdentity,
    pub domain: DomainName,
    pub relay: RelayName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionInterestVisibilityResponse {
    pub result: Result<(), String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeErrorEvent {
    pub message: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainClockProgressEnvelope {
    pub domain_id: DomainName,
    pub progress: DomainClockProgress,
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
    pub placement: StatePlacementEnvelope,
    pub after_lsm: Option<u64>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSyncResponse {
    pub result: Result<Option<StateSnapshotEnvelope>, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StateReplicationAck {
    pub placement: StatePlacementEnvelope,
    pub lsm: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StateCheckpointAvailable {
    pub placement: StatePlacementEnvelope,
    pub lsm: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct OwnershipHandoffCheckpoint {
    pub placement: StatePlacementEnvelope,
    pub snapshot: StateSnapshotEnvelope,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureOwnershipHandoffStateRequest {
    pub operation_id: String,
    pub source: ClusterNodeName,
    pub source_incarnation: ClusterNodeIncarnation,
    pub domain: DomainName,
    pub entity: NodeRef,
    pub base_schedule_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct PrepareOwnershipHandoffStateRequest {
    pub operation_id: String,
    pub source: ClusterNodeName,
    pub destination: ClusterNodeName,
    pub source_incarnation: ClusterNodeIncarnation,
    pub destination_incarnation: ClusterNodeIncarnation,
    pub domain: DomainName,
    pub entity: NodeRef,
    pub base_schedule_fingerprint: [u8; 32],
    pub target_schedule_fingerprint: [u8; 32],
    pub checkpoints: Vec<OwnershipHandoffCheckpoint>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfirmOwnershipHandoffStateRequest {
    pub operation_id: String,
    pub source: ClusterNodeName,
    pub destination: ClusterNodeName,
    pub source_incarnation: ClusterNodeIncarnation,
    pub destination_incarnation: ClusterNodeIncarnation,
    pub domain: DomainName,
    pub entity: NodeRef,
    pub base_schedule_fingerprint: [u8; 32],
    pub target_schedule_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrepareForcedOwnershipRecoveryRequest {
    pub operation_id: String,
    pub source: ClusterNodeName,
    pub destination: ClusterNodeName,
    pub destination_incarnation: ClusterNodeIncarnation,
    pub domain: DomainName,
    pub entity: NodeRef,
    pub base_schedule_fingerprint: [u8; 32],
    pub target_schedule_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForcedOwnershipRecoveryPreparation {
    pub state_recovery: OwnershipStateRecoveryOutcome,
    pub resets: Vec<OwnershipStateReset>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq, Error)]
pub enum OwnershipHandoffFailure {
    #[error("{reason}")]
    Rejected { reason: String },
}

impl OwnershipHandoffFailure {
    pub fn rejected(reason: impl Into<String>) -> Self {
        Self::Rejected {
            reason: reason.into(),
        }
    }
}

pub type OwnershipHandoffResponse<T> = Result<T, OwnershipHandoffFailure>;

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivateOwnershipHandoffStateRequest {
    pub operation_id: String,
    pub source: ClusterNodeName,
    pub destination: ClusterNodeName,
    pub source_incarnation: ClusterNodeIncarnation,
    pub destination_incarnation: ClusterNodeIncarnation,
    pub domain: DomainName,
    pub entity: NodeRef,
    pub base_schedule_fingerprint: [u8; 32],
    pub target_schedule_fingerprint: [u8; 32],
    pub activation_budget: Duration,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscardOwnershipHandoffStateRequest {
    pub operation_id: String,
    pub domain: DomainName,
    pub entity: NodeRef,
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
    pub domain: DomainName,
    pub kind: ModelKind,
    pub name: ModelName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataflowNodeStatusResponse {
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
    pub domain: DomainName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainDrainStatusResponse {
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
    pub domain: DomainName,
    pub relays: Vec<RelayName>,
    pub affected_entities: Vec<NodeRef>,
    pub purpose: EntityGatePurpose,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityDrainStatusResponse {
    pub result: Result<EntityDrainStatusEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateReleaseRequest {
    pub operation_id: u64,
    pub domain: DomainName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateReleaseResponse {
    pub result: Result<(), String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsRequest {
    pub domain: DomainName,
    pub kind: ModelKind,
    pub name: ModelName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsResponse {
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
    pub domain: DomainName,
    pub relay: RelayName,
    pub bindings: Vec<SubscriptionBinding>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeRelayResponse {
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
    pub domain: DomainName,
    pub name: LookupName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeLookupResponse {
    pub result: Result<LookupDescribeEnvelope, String>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct LookupRequest {
    pub domain: DomainName,
    pub name: LookupName,
    pub key: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct LookupResponse {
    pub result: Result<Option<Vec<u8>>, String>,
}

impl InterconnectRequest for StateSyncRequest {
    type Response = StateSyncResponse;

    const NAME: &'static str = "state_sync";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

impl InterconnectRequest for DataflowNodeStatusRequest {
    type Response = DataflowNodeStatusResponse;

    const NAME: &'static str = "dataflow_node_status";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

impl InterconnectRequest for DomainDrainStatusRequest {
    type Response = DomainDrainStatusResponse;

    const NAME: &'static str = "domain_drain_status";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

impl InterconnectRequest for EntityGateRequest {
    type Response = EntityGateResponse;

    const NAME: &'static str = "entity_gate";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Admission;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

impl InterconnectRequest for EntityDrainStatusRequest {
    type Response = EntityDrainStatusResponse;

    const NAME: &'static str = "entity_drain_status";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

impl InterconnectRequest for EntityGateReleaseRequest {
    type Response = EntityGateReleaseResponse;

    const NAME: &'static str = "entity_gate_release";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Cancellation;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

impl InterconnectRequest for DescribeMetricsRequest {
    type Response = DescribeMetricsResponse;

    const NAME: &'static str = "describe_metrics";
    const TIMEOUT: Duration = Duration::from_secs(5);
}

impl InterconnectRequest for DescribeRelayRequest {
    type Response = DescribeRelayResponse;

    const NAME: &'static str = "describe_relay";
    const TIMEOUT: Duration = Duration::from_secs(1);
}

impl InterconnectRequest for DescribeLookupRequest {
    type Response = DescribeLookupResponse;

    const NAME: &'static str = "describe_lookup";
    const TIMEOUT: Duration = Duration::from_secs(5);
}

impl InterconnectRequest for LookupRequest {
    type Response = LookupResponse;

    const NAME: &'static str = "lookup";
    const TIMEOUT: Duration = Duration::from_secs(5);
}

impl InterconnectRequest for SubscriptionInterestVisibilityRequest {
    type Response = SubscriptionInterestVisibilityResponse;

    const NAME: &'static str = "subscription_interest_visibility";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

#[derive(Debug)]
pub struct ReceivedEnvelope {
    pub peer_addr: SocketAddr,
    pub peer_node_id: ClusterNodeName,
    pub envelope: Envelope,
    pub relay_admission: Option<RelayAdmission>,
    _decoded: Option<Reservation>,
}

impl ReceivedEnvelope {
    pub(crate) fn new(
        peer_addr: SocketAddr,
        peer_node_id: ClusterNodeName,
        envelope: Envelope,
        decoded: Option<Reservation>,
    ) -> Self {
        Self {
            peer_addr,
            peer_node_id,
            envelope,
            relay_admission: None,
            _decoded: decoded,
        }
    }

    pub(crate) fn new_relay(
        peer_addr: SocketAddr,
        peer_node_id: ClusterNodeName,
        payload: RelayPayload,
        relay_admission: RelayAdmission,
    ) -> Self {
        Self {
            peer_addr,
            peer_node_id,
            envelope: Envelope::RelayPayload(payload),
            relay_admission: Some(relay_admission),
            _decoded: None,
        }
    }
}

#[derive(Clone)]
pub struct Transport {
    pub(crate) inner: connection::TransportState,
}

impl Transport {
    pub async fn bind(
        listen_addr: SocketAddr,
        advertised_host: impl Into<String>,
        cluster_id: impl Into<String>,
        node_id: ClusterNodeName,
        tls: TlsConfigBundle,
        options: TransportOptions,
        executor: Executor,
    ) -> Result<(Self, mpsc::Receiver<ReceivedEnvelope>), TransportError> {
        let (inner, incoming) = connection::TransportState::bind(
            listen_addr,
            advertised_host.into(),
            cluster_id.into(),
            node_id,
            tls,
            options,
            executor,
        )
        .await?;
        Ok((Self { inner }, incoming))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
    }

    pub fn node_id(&self) -> &ClusterNodeName {
        self.inner.node_id()
    }

    /// Send one operation. Its semantic type selects the pool; callers cannot select a class.
    pub async fn send(
        &self,
        peer_node_id: &ClusterNodeName,
        envelope: Envelope,
    ) -> Result<(), TransportError> {
        self.inner.send(peer_node_id, envelope).await
    }

    pub async fn cancel_relay(
        &self,
        peer_node_id: &ClusterNodeName,
        delivery: RelayDelivery,
    ) -> Result<RelayAdmissionStatus, Report<TransportError>> {
        self.inner.cancel_relay(peer_node_id, delivery).await
    }

    pub async fn relay_admission_status(
        &self,
        peer_node_id: &ClusterNodeName,
        delivery: RelayDelivery,
    ) -> Result<RelayAdmissionStatus, Report<TransportError>> {
        self.inner
            .relay_admission_status(peer_node_id, delivery)
            .await
    }

    pub fn relay_cancellation_guard(
        &self,
        peer_node_id: ClusterNodeName,
        delivery: RelayDelivery,
    ) -> RelayCancellationGuard {
        RelayCancellationGuard::new(self.inner.clone(), peer_node_id, delivery)
    }

    pub fn replace_outbound_targets(
        &self,
        targets: &BTreeMap<ClusterNodeName, BTreeSet<PeerTarget>>,
    ) {
        self.inner.replace_outbound_targets(targets);
    }

    /// Authenticate an endpoint whose node identity is not known yet, then add its pool target.
    /// Bootstrap discovery uses the identity in the peer certificate as the result.
    pub async fn bootstrap_target(
        &self,
        target: PeerTarget,
    ) -> Result<ClusterNodeName, TransportError> {
        self.inner.bootstrap_target(target).await
    }

    /// Add one discovered target. Every pool connection still verifies that its certificate names
    /// `node_id` and its endpoint before the target becomes usable.
    pub fn register_outbound_target(
        &self,
        node_id: ClusterNodeName,
        target: PeerTarget,
    ) -> Result<(), TransportError> {
        self.inner.register_outbound_target(node_id, target)
    }

    /// Reports whether every outbound pool except bulk is ready for node traffic.
    pub fn is_connected_to(&self, node_id: &ClusterNodeName) -> bool {
        self.inner.is_connected_to(node_id)
    }

    pub async fn active_outbound_connections(&self) -> usize {
        self.inner.active_outbound_connections()
    }

    pub async fn replace_tls(&self, tls: TlsConfigBundle) -> Result<(), TransportError> {
        self.inner.replace_tls(tls).await
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

impl Envelope {
    pub(crate) fn pool_class(&self) -> PoolClass {
        match self {
            Self::RelayPayload(_) => PoolClass::Relay,
            Self::Ack(_) => PoolClass::Management,
            Self::Control(control) => control.pool_class(),
        }
    }
}

impl ControlEnvelope {
    pub(crate) fn pool_class(&self) -> PoolClass {
        match self {
            Self::DomainClockProgress(_) | Self::RuntimeErrorEvent(_) => PoolClass::Management,
            Self::StateReplicationAck(_) | Self::StateCheckpointAvailable(_) => {
                PoolClass::Replication
            }
            Self::Request(request) => request.class,
            Self::Response(response) => response.class,
            Self::Terminate => PoolClass::Commands,
        }
    }
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("invalid transport options: {reason}")]
    InvalidOptions { reason: String },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("HTTP/2 error: {0}")]
    Http2(#[from] h2::Error),
    #[error("invalid DNS or IP server name '{0}'")]
    InvalidServerName(String),
    #[error("wire encode failed: {0}")]
    Encode(String),
    #[error("wire decode failed: {0}")]
    Decode(String),
    #[error("HTTP/2 request construction failed: {0}")]
    Http(String),
    #[error("payload exceeds its {class:?} limit: {size} > {limit}")]
    PayloadTooLarge {
        class: PoolClass,
        size: u64,
        limit: u64,
    },
    #[error("interconnect connection capacity is exhausted")]
    PoolExhausted,
    #[error("connection setup with {peer} timed out after {timeout:?}")]
    ConnectionSetupTimeout { peer: SocketAddr, timeout: Duration },
    #[error("request to node '{peer}' timed out after {timeout:?}")]
    RequestTimeout {
        peer: ClusterNodeName,
        timeout: Duration,
    },
    #[error("body stream made no progress for {timeout:?}")]
    ProgressTimeout { timeout: Duration },
    #[error("transport is shutting down")]
    ShuttingDown,
    #[error("connection to {0} is closed")]
    Closed(SocketAddr),
    #[error("peer handshake is invalid: {0}")]
    InvalidHandshake(String),
    #[error("peer returned HTTP status {status}: {message}")]
    RemoteRejected { status: u16, message: String },
    #[error("the application ingress queue is full")]
    IncomingQueueFull,
    #[error("relay transfer grant was refused: {0}")]
    RelayGrant(String),
    #[error("relay delivery was cancelled before runtime admission")]
    RelayCancelled,
    #[error("relay delivery outcome is indeterminate after a peer process epoch change")]
    RelayIndeterminate,
    #[error("relay delivery was rejected before runtime admission: {0}")]
    RelayRejected(String),
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
    #[error("certificate is invalid: {0}")]
    InvalidCertificate(String),
    #[error("certificate has no subjectAltName extension")]
    MissingSubjectAlternativeName,
    #[error("certificate has no nervix cluster/node URI SAN")]
    MissingIdentityUri,
    #[error("certificate has more than one nervix cluster/node URI SAN")]
    MultipleIdentityUris,
    #[error("certificate has an invalid identity URI '{uri}': {reason}")]
    InvalidIdentityUri { uri: String, reason: String },
    #[error("certificate cluster identity is '{actual}', expected '{expected}'")]
    ClusterIdentityMismatch { expected: String, actual: String },
    #[error("certificate node identity is '{actual}', expected '{expected}'")]
    NodeIdentityMismatch {
        expected: ClusterNodeName,
        actual: ClusterNodeName,
    },
    #[error("certificate does not identify advertised endpoint '{endpoint}'")]
    EndpointIdentityMismatch { endpoint: String },
    #[error("certificate contains an invalid IP subject alternative name")]
    InvalidEndpointSan,
    #[error("certificate has expired")]
    Expired,
    #[error("certificate is not valid yet")]
    NotYetValid,
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

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        process::Command,
        sync::{
            Arc as StdArc, OnceLock,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use futures_util::FutureExt as _;
    use meticulous::ResultExt as _;
    use nervix_execution::{CpuClass, MemoryClass};
    use nervix_models::RemoteAckOutcome;
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose, SanType,
    };
    use tempfile::{TempDir, tempdir};
    use tokio::{
        sync::{Notify, watch},
        time::{Instant, timeout, timeout_at},
    };

    use super::*;

    fn tls_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tls/dev")
            .join(name)
    }

    fn test_tls() -> TlsConfigBundle {
        static GENERATED: OnceLock<()> = OnceLock::new();
        GENERATED.get_or_init(|| {
            let status = Command::new("bash")
                .arg("scripts/generate_dev_tls.sh")
                .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
                .status()
                .expect("dev TLS generation command should run");
            assert!(status.success(), "dev TLS generation should succeed");
        });
        TlsConfigBundle::from_pem_files(
            tls_path("ca.pem"),
            tls_path("node.pem"),
            tls_path("node-key.pem"),
        )
        .expect("test TLS should load")
    }

    struct TestCertificateAuthority {
        _directory: TempDir,
        certificate: rcgen::Certificate,
        key: KeyPair,
        path: PathBuf,
    }

    impl TestCertificateAuthority {
        fn new() -> Self {
            let directory = tempdir().expect("test certificate directory should be created");
            let mut params = CertificateParams::default();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyCertSign,
                KeyUsagePurpose::CrlSign,
            ];
            let key = KeyPair::generate().expect("test CA key should be generated");
            let certificate = params
                .self_signed(&key)
                .expect("test CA certificate should be generated");
            let path = directory.path().join("ca.pem");
            std::fs::write(&path, certificate.pem())
                .expect("test CA certificate should be written");
            Self {
                _directory: directory,
                certificate,
                key,
                path,
            }
        }

        fn issue(&self, cluster: &str, node: &ClusterNodeName) -> TlsConfigBundle {
            let mut params = CertificateParams::new(vec!["localhost".to_string()])
                .expect("test endpoint SAN should be valid");
            params.subject_alt_names.push(SanType::URI(
                format!("nervix://cluster/{cluster}/node/{node}")
                    .try_into()
                    .expect("test identity URI should be valid"),
            ));
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let key = KeyPair::generate().expect("test node key should be generated");
            let certificate = params
                .signed_by(&key, &self.certificate, &self.key)
                .expect("test node certificate should be signed");
            let certificate_path = self
                ._directory
                .path()
                .join(format!("{node}-certificate.pem"));
            let key_path = self._directory.path().join(format!("{node}-key.pem"));
            std::fs::write(&certificate_path, certificate.pem())
                .expect("test node certificate should be written");
            std::fs::write(&key_path, key.serialize_pem())
                .expect("test node key should be written");
            TlsConfigBundle::from_pem_files(&self.path, certificate_path, key_path)
                .expect("test node TLS identity should load")
        }
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct EchoRequest {
        value: String,
    }

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct EchoResponse {
        value: String,
        peer: ClusterNodeName,
        advertised_host: String,
    }

    impl InterconnectRequest for EchoRequest {
        type Response = EchoResponse;

        const NAME: &'static str = "test_echo";
        const TIMEOUT: Duration = Duration::from_secs(2);
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct BlockingBulkRequest;

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct BlockingBulkResponse;

    impl InterconnectRequest for BlockingBulkRequest {
        type Response = BlockingBulkResponse;

        const NAME: &'static str = "test_blocking_bulk";
        const CLASS: PoolClass = PoolClass::Bulk;
        const TIMEOUT: Duration = Duration::from_secs(5);
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct ReplicationRequest {
        wait: bool,
    }

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct ReplicationResponse;

    impl InterconnectRequest for ReplicationRequest {
        type Response = ReplicationResponse;

        const NAME: &'static str = "test_replication";
        const CLASS: PoolClass = PoolClass::Replication;
        const TIMEOUT: Duration = Duration::from_secs(5);
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct ManagementRequest;

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct ManagementResponse;

    impl InterconnectRequest for ManagementRequest {
        type Response = ManagementResponse;

        const NAME: &'static str = "test_management";
        const CLASS: PoolClass = PoolClass::Management;
        const TIMEOUT: Duration = Duration::from_secs(2);
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct BlockingManagementRequest;

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct BlockingManagementResponse;

    impl InterconnectRequest for BlockingManagementRequest {
        type Response = BlockingManagementResponse;

        const NAME: &'static str = "test_blocking_management";
        const CLASS: PoolClass = PoolClass::Management;
        const TIMEOUT: Duration = Duration::from_secs(5);
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct CancellationRequest;

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct CancellationResponse;

    impl InterconnectRequest for CancellationRequest {
        type Response = CancellationResponse;

        const NAME: &'static str = "test_cancellation";
        const CLASS: PoolClass = PoolClass::Management;
        const SUBQUOTA: RequestSubquota = RequestSubquota::Cancellation;
        const TIMEOUT: Duration = Duration::from_secs(2);
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct BlockingDiscoveryRequest;

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct BlockingDiscoveryResponse;

    impl InterconnectRequest for BlockingDiscoveryRequest {
        type Response = BlockingDiscoveryResponse;

        const NAME: &'static str = "test_blocking_discovery";
        const CLASS: PoolClass = PoolClass::Management;
        const SUBQUOTA: RequestSubquota = RequestSubquota::Discovery;
        const TIMEOUT: Duration = Duration::from_secs(2);
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct BlockingProgressRequest;

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct BlockingProgressResponse;

    const LIVENESS_QUOTA_EVENT_FAILSAFE_SECONDS: u64 = 30;
    const LIVENESS_QUOTA_REQUEST_TIMEOUT_MULTIPLIER: u64 = 2;
    const LIVENESS_QUOTA_REQUEST_TIMEOUT_SECONDS: u64 = match LIVENESS_QUOTA_EVENT_FAILSAFE_SECONDS
        .checked_mul(LIVENESS_QUOTA_REQUEST_TIMEOUT_MULTIPLIER)
    {
        Some(seconds) => seconds,
        None => panic!("the fixed liveness quota test deadlines fit in u64"),
    };
    const LIVENESS_QUOTA_EVENT_FAILSAFE: Duration =
        Duration::from_secs(LIVENESS_QUOTA_EVENT_FAILSAFE_SECONDS);
    const LIVENESS_QUOTA_REQUEST_TIMEOUT: Duration =
        Duration::from_secs(LIVENESS_QUOTA_REQUEST_TIMEOUT_SECONDS);
    const _: () = assert!(
        LIVENESS_QUOTA_REQUEST_TIMEOUT_SECONDS > LIVENESS_QUOTA_EVENT_FAILSAFE_SECONDS,
        "blocked progress requests must remain active throughout the liveness observation"
    );

    impl InterconnectRequest for BlockingProgressRequest {
        type Response = BlockingProgressResponse;

        const NAME: &'static str = "test_blocking_progress";
        const CLASS: PoolClass = PoolClass::Management;
        const SUBQUOTA: RequestSubquota = RequestSubquota::Progress;
        const TIMEOUT: Duration = LIVENESS_QUOTA_REQUEST_TIMEOUT;
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct LivenessRequest;

    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct LivenessResponse;

    impl InterconnectRequest for LivenessRequest {
        type Response = LivenessResponse;

        const NAME: &'static str = "test_liveness";
        const CLASS: PoolClass = PoolClass::Management;
        const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
        const TIMEOUT: Duration = LIVENESS_QUOTA_REQUEST_TIMEOUT;
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct HangingRequest;

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct HangingResponse;

    impl InterconnectRequest for HangingRequest {
        type Response = HangingResponse;

        const NAME: &'static str = "test_hanging";
        const TIMEOUT: Duration = Duration::from_secs(30);
    }

    struct ConnectedTransports {
        _authority: TestCertificateAuthority,
        transport_a: Transport,
        transport_b: Transport,
        node_a: ClusterNodeName,
        node_b: ClusterNodeName,
        _incoming_a: mpsc::Receiver<ReceivedEnvelope>,
        incoming_b: mpsc::Receiver<ReceivedEnvelope>,
    }

    async fn connected_transports() -> ConnectedTransports {
        connected_transports_with_options(TransportOptions::default()).await
    }

    async fn connected_transports_with_options(options: TransportOptions) -> ConnectedTransports {
        let transports = bound_transports_with_options(options).await;
        transports
            .transport_a
            .register_outbound_target(
                transports.node_b.clone(),
                PeerTarget::new(transports.transport_b.local_addr(), "localhost"),
            )
            .expect("authenticated test target should register");
        transports
    }

    async fn bound_transports_with_options(options: TransportOptions) -> ConnectedTransports {
        let authority = TestCertificateAuthority::new();
        let node_a = ClusterNodeName::parse("node-a").expect("test node name should be valid");
        let node_b = ClusterNodeName::parse("node-b").expect("test node name should be valid");
        let (transport_a, incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().expect("test address should be valid"),
            "localhost",
            "test-cluster",
            node_a.clone(),
            authority.issue("test-cluster", &node_a),
            options.clone(),
            Executor::default(),
        )
        .await
        .expect("first test transport should bind");
        let (transport_b, incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().expect("test address should be valid"),
            "localhost",
            "test-cluster",
            node_b.clone(),
            authority.issue("test-cluster", &node_b),
            options,
            Executor::default(),
        )
        .await
        .expect("second test transport should bind");
        let live_nodes = BTreeSet::from([node_a.clone(), node_b.clone()]);
        transport_a.replace_live_nodes(&live_nodes);
        transport_b.replace_live_nodes(&live_nodes);
        ConnectedTransports {
            _authority: authority,
            transport_a,
            transport_b,
            node_a,
            node_b,
            _incoming_a: incoming_a,
            incoming_b,
        }
    }

    #[tokio::test]
    async fn send_waits_for_a_target_registered_after_the_operation_starts() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_b,
            mut incoming_b,
            ..
        } = bound_transports_with_options(TransportOptions::default()).await;
        let mut send =
            Box::pin(transport_a.send(&node_b, Envelope::Control(ControlEnvelope::Terminate)));

        assert!(
            send.as_mut().now_or_never().is_none(),
            "send should await the transport's target notification"
        );
        transport_a
            .register_outbound_target(
                node_b.clone(),
                PeerTarget::new(transport_b.local_addr(), "localhost"),
            )
            .expect("authenticated test target should register");

        send.await.expect("control delivery should succeed");
        let received = incoming_b
            .recv()
            .now_or_never()
            .expect("the control is queued before the successful response")
            .expect("the peer's incoming queue should remain open");
        assert!(matches!(
            received.envelope,
            Envelope::Control(ControlEnvelope::Terminate)
        ));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[test]
    fn internal_tls_negotiates_only_http2() {
        let tls = test_tls();

        assert_eq!(tls.client_config.alpn_protocols, [b"h2".to_vec()]);
        assert_eq!(tls.server_config.alpn_protocols, [b"h2".to_vec()]);
    }

    #[test]
    fn connection_limit_reserves_both_preconnected_directions() {
        let mut options = TransportOptions {
            max_peers: 2,
            max_connections: 20,
            ..TransportOptions::default()
        };
        assert!(matches!(
            options.validate(),
            Err(TransportError::InvalidOptions { .. })
        ));

        options.max_connections = 21;
        options
            .validate()
            .expect("one on-demand connection should fit after both preconnected directions");
    }

    #[test]
    fn relay_acknowledgements_use_the_management_pool() {
        let envelope = Envelope::Ack(RemoteAckResolution {
            ack_id: 1,
            outcome: RemoteAckOutcome::Ack,
        });

        assert_eq!(envelope.pool_class(), PoolClass::Management);
    }

    #[tokio::test]
    async fn connection_binding_drives_response_flow_control() {
        let options = TransportOptions {
            initial_stream_window_bytes: 1,
            ..TransportOptions::default()
        };
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_b,
            ..
        } = connected_transports_with_options(options).await;

        timeout(Duration::from_secs(2), async {
            loop {
                tokio::task::consume_budget().await;
                if transport_a.is_connected_to(&node_b) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("binding responses should advance beyond the one-byte stream window");

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[test]
    fn certificate_binds_cluster_node_and_endpoint() {
        let tls = test_tls();
        let node = ClusterNodeName::parse("node-1").expect("valid node name");

        tls.certificate
            .validate_local("default", &node, "localhost")
            .expect("certificate identity should match");
        assert!(
            tls.certificate
                .validate_local("another-cluster", &node, "localhost")
                .is_err()
        );
    }

    #[tokio::test]
    async fn typed_rkyv_requests_reuse_an_authenticated_http2_pool() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_a,
            node_b,
            ..
        } = connected_transports().await;
        transport_b
            .register_handler::<EchoRequest, _, _>(|context, request| async move {
                EchoResponse {
                    value: request.value,
                    peer: context.peer_node_id().clone(),
                    advertised_host: context.peer_advertised_host().to_string(),
                }
            })
            .expect("echo handler should register");

        timeout(Duration::from_secs(5), async {
            loop {
                tokio::task::consume_budget().await;
                if transport_a.is_connected_to(&node_b) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the target should become ready");
        assert_eq!(
            transport_a.active_outbound_connections().await,
            5,
            "readiness must include management, command, replication, and both relay connections"
        );

        for value in ["first", "second"] {
            let response = transport_a
                .request(
                    &node_b,
                    EchoRequest {
                        value: value.to_string(),
                    },
                )
                .await
                .expect("typed request should cross the interconnect");
            assert_eq!(
                response,
                EchoResponse {
                    value: value.to_string(),
                    peer: node_a.clone(),
                    advertised_host: "localhost".to_string(),
                }
            );
        }
        assert_eq!(transport_a.active_outbound_connections().await, 5);

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn queued_replication_request_wakes_when_the_stream_slot_is_released() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_b,
            ..
        } = connected_transports().await;
        let started = StdArc::new(Notify::new());
        let release = StdArc::new(Notify::new());
        transport_b
            .register_handler::<ReplicationRequest, _, _>({
                let started = StdArc::clone(&started);
                let release = StdArc::clone(&release);
                move |_context, request| {
                    let started = StdArc::clone(&started);
                    let release = StdArc::clone(&release);
                    async move {
                        if request.wait {
                            started.notify_one();
                            release.notified().await;
                        }
                        ReplicationResponse
                    }
                }
            })
            .expect("replication handler should register");
        timeout(Duration::from_secs(5), async {
            loop {
                tokio::task::consume_budget().await;
                if transport_a.is_connected_to(&node_b) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the target should become ready");

        let first_requester = transport_a.clone();
        let first_target = node_b.clone();
        let first = tokio::spawn(async move {
            first_requester
                .request(&first_target, ReplicationRequest { wait: true })
                .await
        });
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("the first replication request should hold the only stream slot");

        let second_requester = transport_a.clone();
        let second_target = node_b.clone();
        let second_started = StdArc::new(Notify::new());
        let second_started_in_task = StdArc::clone(&second_started);
        let second = tokio::spawn(async move {
            second_started_in_task.notify_one();
            second_requester
                .request(&second_target, ReplicationRequest { wait: false })
                .await
        });
        second_started.notified().await;
        tokio::task::yield_now().await;
        release.notify_one();

        timeout(Duration::from_secs(2), async {
            first
                .await
                .expect("first replication request task should join")
                .expect("first replication request should succeed");
            second
                .await
                .expect("second replication request task should join")
                .expect("queued replication request should wake and succeed");
        })
        .await
        .expect("queued replication work should advance without connection churn");

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn bulk_work_does_not_block_the_management_pool() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_b,
            ..
        } = connected_transports().await;
        let started = StdArc::new(Notify::new());
        let release = StdArc::new(Notify::new());
        transport_b
            .register_handler::<BlockingBulkRequest, _, _>({
                let started = StdArc::clone(&started);
                let release = StdArc::clone(&release);
                move |_context, _request| {
                    let started = StdArc::clone(&started);
                    let release = StdArc::clone(&release);
                    async move {
                        started.notify_one();
                        release.notified().await;
                        BlockingBulkResponse
                    }
                }
            })
            .expect("bulk handler should register");
        transport_b
            .register_handler::<ManagementRequest, _, _>(|_context, _request| async move {
                ManagementResponse
            })
            .expect("management handler should register");

        let requester = transport_a.clone();
        let bulk_target = node_b.clone();
        let bulk =
            tokio::spawn(async move { requester.request(&bulk_target, BlockingBulkRequest).await });
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("bulk handler should start");
        let management = timeout(
            Duration::from_secs(2),
            transport_a.request(&node_b, ManagementRequest),
        )
        .await
        .expect("management request should not wait for bulk work")
        .expect("management request should succeed");
        assert_eq!(management, ManagementResponse);
        release.notify_one();
        assert_eq!(
            bulk.await
                .expect("bulk request task should join")
                .expect("bulk request should succeed"),
            BlockingBulkResponse
        );

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn slow_management_work_cannot_consume_cancellation_streams() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_b,
            ..
        } = connected_transports().await;
        let started = StdArc::new(AtomicUsize::new(0));
        let (release, release_rx) = watch::channel(false);
        transport_b
            .register_handler::<BlockingManagementRequest, _, _>({
                let started = StdArc::clone(&started);
                let release_rx = release_rx.clone();
                move |_context, _request| {
                    let started = StdArc::clone(&started);
                    let mut release_rx = release_rx.clone();
                    async move {
                        started.fetch_add(1, Ordering::AcqRel);
                        release_rx
                            .wait_for(|released| *released)
                            .await
                            .expect("test release sender should remain open");
                        BlockingManagementResponse
                    }
                }
            })
            .expect("blocking management handler should register");
        transport_b
            .register_handler::<CancellationRequest, _, _>(|_context, _request| async move {
                CancellationResponse
            })
            .expect("cancellation handler should register");

        let mut blocked = Vec::new();
        for _ in 0..connection::MANAGEMENT_SHARED_STREAMS {
            let requester = transport_a.clone();
            let target = node_b.clone();
            blocked.push(tokio::spawn(async move {
                requester.request(&target, BlockingManagementRequest).await
            }));
        }
        timeout(Duration::from_secs(2), async {
            loop {
                tokio::task::consume_budget().await;
                if started.load(Ordering::Acquire) == connection::MANAGEMENT_SHARED_STREAMS {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all shared management streams should become occupied");

        assert_eq!(
            timeout(
                Duration::from_secs(2),
                transport_a.request(&node_b, CancellationRequest),
            )
            .await
            .expect("cancellation must retain a physical management stream")
            .expect("cancellation request should succeed"),
            CancellationResponse
        );

        release.send_replace(true);
        for request in blocked {
            request
                .await
                .expect("blocking management request should join")
                .expect("blocking management request should finish after release");
        }
        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn progress_work_cannot_consume_liveness_streams() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_b,
            ..
        } = connected_transports().await;
        let observation_deadline = Instant::now()
            .checked_add(LIVENESS_QUOTA_EVENT_FAILSAFE)
            .assured("the fixed liveness quota test failsafe fits in Tokio's instant range");
        let (progress_started, mut progress_started_rx) = watch::channel(0_usize);
        let (release, release_rx) = watch::channel(false);
        transport_b
            .register_handler::<BlockingProgressRequest, _, _>({
                let release_rx = release_rx.clone();
                let progress_started = progress_started.clone();
                move |_context, _request| {
                    let mut release_rx = release_rx.clone();
                    let progress_started = progress_started.clone();
                    async move {
                        progress_started.send_modify(|started| {
                            *started = started.checked_add(1).assured(
                                "the test starts only one bounded set of progress requests",
                            );
                        });
                        release_rx.wait_for(|released| *released).await.assured(
                            "the test retains its release sender until every request joins",
                        );
                        BlockingProgressResponse
                    }
                }
            })
            .assured("the fresh test transport has no progress handler with this name");
        let (liveness_entered, mut liveness_entered_rx) = watch::channel(false);
        transport_b
            .register_handler::<LivenessRequest, _, _>({
                move |_context, _request| {
                    let liveness_entered = liveness_entered.clone();
                    async move {
                        liveness_entered.send_replace(true);
                        LivenessResponse
                    }
                }
            })
            .assured("the fresh test transport has no liveness handler with this name");

        let mut blocked = Vec::new();
        for _ in 0..connection::MANAGEMENT_PROGRESS_STREAMS {
            let requester = transport_a.clone();
            let target = node_b.clone();
            blocked.push(tokio::spawn(async move {
                requester.request(&target, BlockingProgressRequest).await
            }));
        }
        timeout_at(
            observation_deadline,
            progress_started_rx
                .wait_for(|started| *started == connection::MANAGEMENT_PROGRESS_STREAMS),
        )
        .await
        .assured("every reserved progress stream enters its handler within the test failsafe")
        .assured("the registered progress handler retains its watch sender");

        let requester = transport_a.clone();
        let target = node_b.clone();
        let liveness =
            tokio::spawn(async move { requester.request(&target, LivenessRequest).await });
        timeout_at(
            observation_deadline,
            liveness_entered_rx.wait_for(|entered| *entered),
        )
        .await
        .assured(
            "the liveness handler enters while every progress handler remains blocked within the \
             test failsafe",
        )
        .assured("the registered liveness handler retains its watch sender");

        release.send_replace(true);
        let liveness = liveness
            .await
            .assured("the liveness request task contains no panic path");
        for request in blocked {
            let response = request
                .await
                .assured("the progress request task contains no panic path");
            assert!(
                response.is_ok(),
                "blocking progress request should finish after release: {response:?}"
            );
        }
        transport_a.shutdown().await;
        transport_b.shutdown().await;

        assert!(
            liveness.is_ok(),
            "liveness must retain a physical management stream: {liveness:?}"
        );
        assert_eq!(
            liveness.verified("the liveness response was checked by the assertion above"),
            LivenessResponse
        );
    }

    #[tokio::test]
    async fn discovery_subquota_cannot_crowd_out_management_requests() {
        let options = TransportOptions {
            incoming_queue_capacity: 1,
            ..TransportOptions::default()
        };
        let ConnectedTransports {
            _authority: authority,
            transport_a,
            transport_b,
            node_b,
            ..
        } = connected_transports_with_options(options.clone()).await;
        let node_c = ClusterNodeName::parse("node-c").expect("test node name should be valid");
        let (transport_c, _incoming_c) = Transport::bind(
            "127.0.0.1:0".parse().expect("test address should be valid"),
            "localhost",
            "test-cluster",
            node_c.clone(),
            authority.issue("test-cluster", &node_c),
            options,
            Executor::default(),
        )
        .await
        .expect("third test transport should bind");
        transport_c
            .register_outbound_target(
                node_b.clone(),
                PeerTarget::new(transport_b.local_addr(), "localhost"),
            )
            .expect("third test transport should register its authenticated target");
        let started = StdArc::new(Notify::new());
        let release = StdArc::new(Notify::new());
        transport_b
            .register_handler::<BlockingDiscoveryRequest, _, _>({
                let started = StdArc::clone(&started);
                let release = StdArc::clone(&release);
                move |_context, _request| {
                    let started = StdArc::clone(&started);
                    let release = StdArc::clone(&release);
                    async move {
                        started.notify_one();
                        release.notified().await;
                        BlockingDiscoveryResponse
                    }
                }
            })
            .expect("discovery handler should register");
        transport_b
            .register_handler::<ManagementRequest, _, _>(|_context, _request| async move {
                ManagementResponse
            })
            .expect("management handler should register");

        let requester = transport_a.clone();
        let target = node_b.clone();
        let discovery =
            tokio::spawn(async move { requester.request(&target, BlockingDiscoveryRequest).await });
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("the first discovery handler should start");

        let error = transport_a
            .request_with_timeout(
                &node_b,
                BlockingDiscoveryRequest,
                Duration::from_millis(250),
            )
            .await
            .expect_err("the discovery subquota should reject excess work");
        assert!(matches!(
            error.current_context(),
            RequestError::AdmissionFull {
                request,
                subquota: RequestSubquota::Discovery,
            } if request == &BlockingDiscoveryRequest::NAME
        ));

        let error = transport_c
            .request_with_timeout(
                &node_b,
                BlockingDiscoveryRequest,
                Duration::from_millis(250),
            )
            .await
            .expect_err("the receiver should reject discovery beyond its subquota");
        assert!(matches!(
            error.current_context(),
            RequestError::RemoteRejected {
                failure: RemoteRequestFailure::AdmissionFull {
                    subquota: RequestSubquota::Discovery,
                },
                ..
            }
        ));

        let management = transport_c
            .request(&node_b, ManagementRequest)
            .await
            .expect("reserved management capacity should remain available");
        assert_eq!(management, ManagementResponse);

        release.notify_waiters();
        assert_eq!(
            discovery
                .await
                .expect("discovery request task should join")
                .expect("the admitted discovery request should succeed"),
            BlockingDiscoveryResponse
        );
        transport_a.shutdown().await;
        transport_c.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn relay_terminal_capacity_is_held_until_the_application_finishes() {
        let options = TransportOptions {
            incoming_queue_capacity: 1,
            ..TransportOptions::default()
        };
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_a,
            node_b,
            _incoming_a: mut incoming_a,
            mut incoming_b,
            ..
        } = connected_transports_with_options(options).await;
        transport_b
            .register_outbound_target(
                node_a.clone(),
                PeerTarget::new(transport_a.local_addr(), "localhost"),
            )
            .expect("the response target should register");
        let payload = |ack_id, sequence, reply_node_id| RelayPayload {
            delivery: RelayDelivery {
                channel_incarnation: [1; 16],
                sequence,
            },
            kind: RelayPayloadKind::Routed,
            domain: DomainName::parse("test").expect("test domain should be valid"),
            relay: RelayName::parse("relay").expect("test relay should be valid"),
            key: None,
            batch_ipc: Executor::default()
                .try_charge_owned(MemoryClass::Relay, vec![1])
                .expect("the test relay body should fit its budget"),
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: Some(RemoteAckRegistration {
                ack_id,
                reply_node_id,
            }),
        };

        let error = transport_a
            .send(
                &node_b,
                Envelope::RelayPayload(payload(0, 0, node_b.clone())),
            )
            .await
            .expect_err("the authenticated sender must own the declared admission reply");
        assert!(matches!(
            error,
            TransportError::RemoteRejected { status: 403, .. }
        ));
        transport_a
            .send(
                &node_b,
                Envelope::RelayPayload(payload(1, 0, node_a.clone())),
            )
            .await
            .expect("the first relay should consume the sole terminal slot");
        let first = timeout(Duration::from_secs(2), incoming_b.recv())
            .await
            .expect("the first relay should enter the application queue")
            .expect("the application queue should remain open");

        let error = transport_a
            .send(
                &node_b,
                Envelope::RelayPayload(payload(2, 1, node_a.clone())),
            )
            .await
            .expect_err("a second grant must wait until the first terminal outcome is sent");
        assert!(matches!(
            error,
            TransportError::RemoteRejected { status: 429, .. }
        ));

        drop(first);
        transport_b
            .send(
                &node_a,
                Envelope::Ack(RemoteAckResolution {
                    ack_id: 1,
                    outcome: RemoteAckOutcome::Ack,
                }),
            )
            .await
            .expect("the first relay terminal outcome should be sent");
        let _terminal = timeout(Duration::from_secs(2), incoming_a.recv())
            .await
            .expect("the terminal outcome should enter the sender application queue")
            .expect("the sender application queue should remain open");
        transport_a
            .send(
                &node_b,
                Envelope::RelayPayload(payload(2, 1, node_a.clone())),
            )
            .await
            .expect("the next relay grant should fit after the terminal outcome");

        timeout(Duration::from_secs(1), async {
            transport_a.shutdown().await;
            transport_b.shutdown().await;
        })
        .await
        .expect("redeemed grant expiry work should not delay transport shutdown");
    }

    #[tokio::test]
    async fn terminal_relay_outcome_waits_for_application_queue_capacity() {
        let options = TransportOptions {
            incoming_queue_capacity: 1,
            ..TransportOptions::default()
        };
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_a,
            node_b,
            _incoming_a: mut incoming_a,
            mut incoming_b,
            ..
        } = connected_transports_with_options(options).await;
        transport_b
            .register_outbound_target(
                node_a.clone(),
                PeerTarget::new(transport_a.local_addr(), "localhost"),
            )
            .expect("the response target should register");
        transport_b
            .send(&node_a, Envelope::Control(ControlEnvelope::Terminate))
            .await
            .expect("the general message should fill the sender application queue");

        transport_a
            .send(
                &node_b,
                Envelope::RelayPayload(RelayPayload {
                    delivery: RelayDelivery {
                        channel_incarnation: [11; 16],
                        sequence: 0,
                    },
                    kind: RelayPayloadKind::Routed,
                    domain: DomainName::parse("test").expect("test domain should be valid"),
                    relay: RelayName::parse("relay").expect("test relay should be valid"),
                    key: None,
                    batch_ipc: Executor::default()
                        .try_charge_owned(MemoryClass::Relay, vec![1])
                        .expect("the test relay body should fit its budget"),
                    metadata: Vec::new(),
                    acks: Vec::new(),
                    admission: Some(RemoteAckRegistration {
                        ack_id: 52,
                        reply_node_id: node_a.clone(),
                    }),
                }),
            )
            .await
            .expect("the relay body should reach the receiver admission queue");
        let received = timeout(Duration::from_secs(2), incoming_b.recv())
            .await
            .expect("the relay body should enter the receiver application queue")
            .expect("the receiver application queue should remain open");
        assert_eq!(
            received
                .relay_admission
                .expect("a relay body must carry its reserved admission")
                .admit(),
            RelayAdmissionDecision::Admitted
        );

        let outcome_sender = transport_b.clone();
        let outcome_target = node_a.clone();
        let mut outcome_task = tokio::spawn(async move {
            outcome_sender
                .send(
                    &outcome_target,
                    Envelope::Ack(RemoteAckResolution {
                        ack_id: 52,
                        outcome: RemoteAckOutcome::Ack,
                    }),
                )
                .await
        });
        assert!(
            timeout(Duration::from_millis(100), &mut outcome_task)
                .await
                .is_err(),
            "a terminal outcome should wait while the general application queue is full"
        );
        let queued = incoming_a
            .recv()
            .await
            .expect("the sender application queue should contain the general message");
        assert!(matches!(
            queued.envelope,
            Envelope::Control(ControlEnvelope::Terminate)
        ));
        outcome_task
            .await
            .expect("the terminal outcome task should join")
            .expect("the terminal outcome should send after capacity becomes available");
        let outcome = incoming_a
            .recv()
            .await
            .expect("the terminal outcome must remain queued for the application");
        assert!(matches!(
            outcome.envelope,
            Envelope::Ack(RemoteAckResolution {
                ack_id: 52,
                outcome: RemoteAckOutcome::Ack,
            })
        ));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn confirmed_cancellation_fences_attempt_before_grant_arrives() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_a,
            node_b,
            _incoming_a: _,
            mut incoming_b,
            ..
        } = connected_transports().await;
        let delivery = RelayDelivery {
            channel_incarnation: [6; 16],
            sequence: 0,
        };

        assert_eq!(
            transport_a
                .cancel_relay(&node_b, delivery)
                .await
                .expect("relay cancellation should be answered"),
            RelayAdmissionStatus::Cancelled
        );

        let error = transport_a
            .send(
                &node_b,
                Envelope::RelayPayload(RelayPayload {
                    delivery,
                    kind: RelayPayloadKind::Routed,
                    domain: DomainName::parse("test").expect("test domain should be valid"),
                    relay: RelayName::parse("relay").expect("test relay should be valid"),
                    key: None,
                    batch_ipc: Executor::default()
                        .try_charge_owned(MemoryClass::Relay, vec![1])
                        .expect("the test relay body should fit its budget"),
                    metadata: Vec::new(),
                    acks: Vec::new(),
                    admission: Some(RemoteAckRegistration {
                        ack_id: 40,
                        reply_node_id: node_a,
                    }),
                }),
            )
            .await
            .expect_err("a confirmed cancellation must fence a later grant");
        assert!(matches!(error, TransportError::RelayCancelled));
        assert!(
            timeout(Duration::from_millis(100), incoming_b.recv())
                .await
                .is_err(),
            "a delivery cancelled before its grant must never enter the application queue"
        );

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn cancelled_relay_admission_can_never_reach_runtime() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_a,
            node_b,
            _incoming_a: _,
            mut incoming_b,
            ..
        } = connected_transports().await;
        let delivery = RelayDelivery {
            channel_incarnation: [7; 16],
            sequence: 0,
        };
        let payload = RelayPayload {
            delivery,
            kind: RelayPayloadKind::Routed,
            domain: DomainName::parse("test").expect("test domain should be valid"),
            relay: RelayName::parse("relay").expect("test relay should be valid"),
            key: None,
            batch_ipc: Executor::default()
                .try_charge_owned(MemoryClass::Relay, vec![1])
                .expect("the test relay body should fit its budget"),
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: Some(RemoteAckRegistration {
                ack_id: 41,
                reply_node_id: node_a.clone(),
            }),
        };

        transport_a
            .send(&node_b, Envelope::RelayPayload(payload.clone()))
            .await
            .expect("the relay body should reach the receiver admission queue");
        let received = timeout(Duration::from_secs(2), incoming_b.recv())
            .await
            .expect("the relay body should enter the application queue")
            .expect("the application queue should remain open");

        assert_eq!(
            transport_a
                .relay_admission_status(&node_b, delivery)
                .await
                .expect("relay status should be answered"),
            RelayAdmissionStatus::BodyReceived
        );
        transport_a
            .send(&node_b, Envelope::RelayPayload(payload.clone()))
            .await
            .expect("a same-epoch retry should reconcile the received body");
        assert!(
            timeout(Duration::from_millis(100), incoming_b.recv())
                .await
                .is_err(),
            "reconciliation must not enqueue the same delivery twice"
        );

        assert_eq!(
            transport_a
                .cancel_relay(&node_b, delivery)
                .await
                .expect("relay cancellation should be answered"),
            RelayAdmissionStatus::Cancelled
        );
        assert_eq!(
            received
                .relay_admission
                .expect("a relay body must carry its reserved admission")
                .admit(),
            RelayAdmissionDecision::Cancelled
        );
        assert_eq!(
            transport_a
                .relay_admission_status(&node_b, delivery)
                .await
                .expect("relay status should be answered"),
            RelayAdmissionStatus::Cancelled
        );

        let error = transport_a
            .send(&node_b, Envelope::RelayPayload(payload))
            .await
            .expect_err("a cancelled delivery identity must remain fenced");
        assert!(matches!(error, TransportError::RelayCancelled));
        let next_delivery = RelayDelivery {
            channel_incarnation: [7; 16],
            sequence: 1,
        };
        let next_payload = RelayPayload {
            delivery: next_delivery,
            kind: RelayPayloadKind::Routed,
            domain: DomainName::parse("test").expect("test domain should be valid"),
            relay: RelayName::parse("relay").expect("test relay should be valid"),
            key: None,
            batch_ipc: Executor::default()
                .try_charge_owned(MemoryClass::Relay, vec![1])
                .expect("the test relay body should fit its budget"),
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: Some(RemoteAckRegistration {
                ack_id: 42,
                reply_node_id: node_a.clone(),
            }),
        };
        transport_a
            .send(&node_b, Envelope::RelayPayload(next_payload))
            .await
            .expect("the next channel sequence should reach the receiver");
        let next_received = timeout(Duration::from_secs(2), incoming_b.recv())
            .await
            .expect("the next relay body should enter the application queue")
            .expect("the application queue should remain open");
        assert_eq!(
            transport_a
                .cancel_relay(&node_b, next_delivery)
                .await
                .expect("the next relay cancellation should be answered"),
            RelayAdmissionStatus::Cancelled
        );
        assert_eq!(
            next_received
                .relay_admission
                .expect("the next relay body must carry its reserved admission")
                .admit(),
            RelayAdmissionDecision::Cancelled
        );

        let retired_payload = RelayPayload {
            delivery: RelayDelivery {
                channel_incarnation: [7; 16],
                sequence: 0,
            },
            kind: RelayPayloadKind::Routed,
            domain: DomainName::parse("test").expect("test domain should be valid"),
            relay: RelayName::parse("relay").expect("test relay should be valid"),
            key: None,
            batch_ipc: Executor::default()
                .try_charge_owned(MemoryClass::Relay, vec![1])
                .expect("the test relay body should fit its budget"),
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: Some(RemoteAckRegistration {
                ack_id: 43,
                reply_node_id: node_a.clone(),
            }),
        };
        let error = transport_a
            .send(&node_b, Envelope::RelayPayload(retired_payload.clone()))
            .await
            .expect_err("a delivery below the reconciled watermark is indeterminate");
        assert!(matches!(error, TransportError::RelayIndeterminate));
        assert!(
            timeout(Duration::from_millis(100), incoming_b.recv())
                .await
                .is_err(),
            "a cancelled relay delivery must not enter the application queue again"
        );

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn same_epoch_retry_of_admitted_relay_does_not_enqueue_twice() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_a,
            node_b,
            _incoming_a: mut incoming_a,
            mut incoming_b,
            ..
        } = connected_transports().await;
        let delivery = RelayDelivery {
            channel_incarnation: [10; 16],
            sequence: 0,
        };
        let payload = RelayPayload {
            delivery,
            kind: RelayPayloadKind::Routed,
            domain: DomainName::parse("test").expect("test domain should be valid"),
            relay: RelayName::parse("relay").expect("test relay should be valid"),
            key: None,
            batch_ipc: Executor::default()
                .try_charge_owned(MemoryClass::Relay, vec![1])
                .expect("the test relay body should fit its budget"),
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: Some(RemoteAckRegistration {
                ack_id: 44,
                reply_node_id: node_a,
            }),
        };

        transport_a
            .send(&node_b, Envelope::RelayPayload(payload.clone()))
            .await
            .expect("the relay body should reach the receiver admission queue");
        let received = timeout(Duration::from_secs(2), incoming_b.recv())
            .await
            .expect("the relay body should enter the application queue")
            .expect("the application queue should remain open");
        assert_eq!(
            received
                .relay_admission
                .expect("a relay body must carry its reserved admission")
                .admit(),
            RelayAdmissionDecision::Admitted
        );

        transport_a
            .send(&node_b, Envelope::RelayPayload(payload))
            .await
            .expect("a same-epoch retry should reconcile the admitted delivery");
        assert!(
            timeout(Duration::from_millis(100), incoming_b.recv())
                .await
                .is_err(),
            "an admitted relay retry must not enter the application queue twice"
        );
        let outcome = timeout(Duration::from_secs(2), incoming_a.recv())
            .await
            .expect("the reconciled admission should return its terminal outcome")
            .expect("the sender application queue should remain open");
        assert!(matches!(
            outcome.envelope,
            Envelope::Ack(RemoteAckResolution {
                ack_id: 44,
                outcome: RemoteAckOutcome::Ack,
            })
        ));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn receiver_process_restart_makes_unresolved_relay_indeterminate() {
        let ConnectedTransports {
            _authority: authority,
            transport_a,
            transport_b,
            node_a,
            node_b,
            _incoming_a: _,
            mut incoming_b,
        } = connected_transports().await;
        let delivery = RelayDelivery {
            channel_incarnation: [8; 16],
            sequence: 0,
        };
        transport_a
            .send(
                &node_b,
                Envelope::RelayPayload(RelayPayload {
                    delivery,
                    kind: RelayPayloadKind::Routed,
                    domain: DomainName::parse("test").expect("test domain should be valid"),
                    relay: RelayName::parse("relay").expect("test relay should be valid"),
                    key: None,
                    batch_ipc: Executor::default()
                        .try_charge_owned(MemoryClass::Relay, vec![1])
                        .expect("the test relay body should fit its budget"),
                    metadata: Vec::new(),
                    acks: Vec::new(),
                    admission: Some(RemoteAckRegistration {
                        ack_id: 45,
                        reply_node_id: node_a.clone(),
                    }),
                }),
            )
            .await
            .expect("the unresolved relay should reach the receiver process");
        let unresolved = timeout(Duration::from_secs(2), incoming_b.recv())
            .await
            .expect("the unresolved relay should enter the application queue")
            .expect("the application queue should remain open");

        transport_b.shutdown().await;
        drop(unresolved);
        let (replacement_b, _replacement_incoming) = Transport::bind(
            "127.0.0.1:0".parse().expect("test address should be valid"),
            "localhost",
            "test-cluster",
            node_b.clone(),
            authority.issue("test-cluster", &node_b),
            TransportOptions::default(),
            Executor::default(),
        )
        .await
        .expect("the replacement receiver process should bind");
        replacement_b.replace_live_nodes(&BTreeSet::from([node_a, node_b.clone()]));
        transport_a
            .register_outbound_target(
                node_b.clone(),
                PeerTarget::new(replacement_b.local_addr(), "localhost"),
            )
            .expect("the replacement receiver target should register");

        assert_eq!(
            timeout(
                Duration::from_secs(5),
                transport_a.relay_admission_status(&node_b, delivery),
            )
            .await
            .expect("the sender should reconnect to the replacement process")
            .expect("the replacement process should answer relay status"),
            RelayAdmissionStatus::Indeterminate
        );

        transport_a.shutdown().await;
        replacement_b.shutdown().await;
    }

    #[tokio::test]
    async fn reserved_relay_work_reports_progress_before_runtime_admission() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_a,
            node_b,
            _incoming_a: mut incoming_a,
            mut incoming_b,
            ..
        } = connected_transports().await;
        transport_b
            .register_outbound_target(
                node_a.clone(),
                PeerTarget::new(transport_a.local_addr(), "localhost"),
            )
            .expect("the progress response target should register");
        transport_a
            .send(
                &node_b,
                Envelope::RelayPayload(RelayPayload {
                    delivery: RelayDelivery {
                        channel_incarnation: [9; 16],
                        sequence: 0,
                    },
                    kind: RelayPayloadKind::Routed,
                    domain: DomainName::parse("test").expect("test domain should be valid"),
                    relay: RelayName::parse("relay").expect("test relay should be valid"),
                    key: None,
                    batch_ipc: Executor::default()
                        .try_charge_owned(MemoryClass::Relay, vec![1])
                        .expect("the test relay body should fit its budget"),
                    metadata: Vec::new(),
                    acks: Vec::new(),
                    admission: Some(RemoteAckRegistration {
                        ack_id: 51,
                        reply_node_id: node_a.clone(),
                    }),
                }),
            )
            .await
            .expect("the relay body should reach the receiver admission queue");
        let _reserved = timeout(Duration::from_secs(2), incoming_b.recv())
            .await
            .expect("the relay body should enter the application queue")
            .expect("the application queue should remain open");

        let progress = timeout(Duration::from_secs(1), incoming_a.recv())
            .await
            .expect("reserved relay work should report queue-time progress")
            .expect("the sender application queue should remain open");
        assert!(matches!(
            progress.envelope,
            Envelope::Ack(RemoteAckResolution {
                ack_id: 51,
                outcome: RemoteAckOutcome::Alive,
            })
        ));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn bootstrap_rejects_a_certificate_from_another_cluster() {
        let authority = TestCertificateAuthority::new();
        let node_a = ClusterNodeName::parse("node-a").expect("test node name should be valid");
        let node_b = ClusterNodeName::parse("node-b").expect("test node name should be valid");
        let (transport_a, _incoming_a) = Transport::bind(
            "127.0.0.1:0".parse().expect("test address should be valid"),
            "localhost",
            "cluster-a",
            node_a.clone(),
            authority.issue("cluster-a", &node_a),
            TransportOptions::default(),
            Executor::default(),
        )
        .await
        .expect("first test transport should bind");
        let (transport_b, _incoming_b) = Transport::bind(
            "127.0.0.1:0".parse().expect("test address should be valid"),
            "localhost",
            "cluster-b",
            node_b.clone(),
            authority.issue("cluster-b", &node_b),
            TransportOptions::default(),
            Executor::default(),
        )
        .await
        .expect("second test transport should bind");

        let error = transport_a
            .bootstrap_target(PeerTarget::new(transport_b.local_addr(), "localhost"))
            .await
            .expect_err("a peer certificate from another cluster must be rejected");
        assert!(matches!(error, TransportError::InvalidHandshake(_)));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_rkyv_is_rejected_before_dispatch() {
        let executor = Executor::default();
        let bytes = executor
            .try_charge_owned(MemoryClass::Commands, vec![0xff; 32])
            .expect("test payload should fit the command budget");

        let result = wire::decode_rkyv::<ControlEnvelope>(
            &executor,
            MemoryClass::Commands,
            CpuClass::Control,
            bytes,
        )
        .await;

        assert!(matches!(result, Err(TransportError::Decode(_))));
    }

    #[tokio::test]
    async fn membership_removal_cancels_an_active_request() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_a,
            node_b,
            ..
        } = connected_transports().await;
        let started = StdArc::new(Notify::new());
        transport_b
            .register_handler::<HangingRequest, _, _>({
                let started = StdArc::clone(&started);
                move |_context, _request| {
                    let started = StdArc::clone(&started);
                    async move {
                        started.notify_one();
                        std::future::pending().await
                    }
                }
            })
            .expect("hanging handler should register");
        let requester = transport_a.clone();
        let target = node_b.clone();
        let request = tokio::spawn(async move { requester.request(&target, HangingRequest).await });
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("hanging request should reach its handler");

        transport_a.replace_live_nodes(&BTreeSet::from([node_a]));
        let error = timeout(Duration::from_secs(2), request)
            .await
            .expect("membership removal should cancel the request")
            .expect("request task should join")
            .expect_err("request should report target departure");
        assert!(matches!(
            error.current_context(),
            RequestError::TargetLeft { node, request }
                if node == &node_b && request == &HangingRequest::NAME
        ));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_cancels_an_active_request() {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_b,
            ..
        } = connected_transports().await;
        let started = StdArc::new(Notify::new());
        transport_b
            .register_handler::<HangingRequest, _, _>({
                let started = StdArc::clone(&started);
                move |_context, _request| {
                    let started = StdArc::clone(&started);
                    async move {
                        started.notify_one();
                        std::future::pending().await
                    }
                }
            })
            .expect("hanging handler should register");
        let requester = transport_a.clone();
        let target = node_b.clone();
        let request = tokio::spawn(async move { requester.request(&target, HangingRequest).await });
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("hanging request should reach its handler");

        timeout(Duration::from_secs(2), transport_a.shutdown())
            .await
            .expect("transport shutdown should respect its drain bound");
        let error = timeout(Duration::from_secs(2), request)
            .await
            .expect("shutdown should cancel the request")
            .expect("request task should join")
            .expect_err("request should report shutdown");
        assert!(matches!(
            error.current_context(),
            RequestError::ShuttingDown { node, request }
                if node == &node_b && request == &HangingRequest::NAME
        ));

        transport_b.shutdown().await;
    }
}
