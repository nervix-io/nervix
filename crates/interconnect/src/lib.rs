//! The authenticated HTTP/2 transport between Nervix nodes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Mutual TLS, class-isolated HTTP/2 pools, bounded rkyv messages, flow control,
//!   deadlines, relay transfer admission, reconciliation, and cancellation.
//! - **Depends on.** Execution admission and the vocabulary carried by internal operations.
//! - **Must not know.** Runtime graphs, schedules, or the semantic outcome of an operation.

use std::{collections::BTreeMap, io, net::SocketAddr, time::Duration};

use error_stack::Report;
use nervix_dns::{DnsLookupError, DnsLookupFailure};
use nervix_execution::{ChargedBytes, Executor, Reservation};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, CodecName, CoordinationIdentity,
    DomainClockProgress, DomainName, EmitterName, FieldName, IngestorName, LookupName, ModelKind,
    ModelName, NodeEndpoint, NodeRef, OwnershipStateRecoveryOutcome, OwnershipStateReset,
    RelayName, RemoteAckRegistration, RemoteAckResolution, RemoteRuntimeField,
    RemoteRuntimeRecordMetadata, ResourceName, SubscriptionBinding, WasmStateResetScope,
};
use nervix_primitives::{sync::mpsc, unmodeled::sync::OnceLock};
use nervix_recovery::Discarded as _;
use rkyv::{Archive, Deserialize, Serialize};
use strum::IntoStaticStr;
use thiserror::Error;

mod authentication;
mod connection;
mod entropy;
mod identity;
mod observation;
mod operation;
mod peer_resolver;
mod peer_target;
mod pool;
mod request;
mod runtime_state;
#[cfg(all(test, feature = "turmoil"))]
#[path = "../tests/simulation/runner.rs"]
mod simulation_runner;
mod wasm_state;
mod wire;

pub use authentication::TransportClock;
pub use connection::{
    ChargedItem, DuplexItems, DuplexReceiver, DuplexResponses, DuplexSendProgress, DuplexSender,
    IncomingByteStream, RelayAdmission, RelayCancellationGuard,
};
pub use entropy::TransportEntropy;
pub use identity::{TlsConfigBundle, TransportIdentity};
pub use observation::{
    ConnectionDirection, ConnectionFailureReason, RelayAdmissionOutcome, RequestOutcome,
    StreamResetReason, TransferDirection, TransportCounters, TransportSnapshot,
};
pub use operation::{RemoteOperationFailure, RemoteOperationSubject};
pub use peer_resolver::PeerResolver;
pub use peer_target::PeerTarget;
pub use pool::PoolClass;
pub use request::{
    ApplicationCompletionPeersRequest, ApplicationCompletionPeersResponse, ApplicationHealthProbe,
    ApplicationRevisionRequest, ApplicationRevisionResponse, HandlerRegistrationError,
    HttpsListenerInstallation, HttpsListenerInstallationRequest, HttpsListenerInstallationResponse,
    InterconnectDuplexRequest, InterconnectRequest, InterconnectStreamRequest,
    RemoteRequestFailure, RequestContext, RequestError, RequestSubquota, StreamHandlerError,
    StreamingResponse,
};
use request::{RequestEnvelope, RequestState, ResponseEnvelope};
pub use runtime_state::{
    OwnershipHandoffCheckpoint, RuntimeState, RuntimeStateKind, StateCheckpointAvailable,
    StatePlacementEnvelope, StateReplicationAck, StateSchema, StateSnapshotEnvelope,
    StateSyncRequest, StateSyncResponse,
};
pub use wasm_state::{
    CoordinateWasmStateResetRequest, CoordinateWasmStateResetResponse,
    RecoverWasmProcessorStateRequest, RecoverWasmProcessorStateResponse,
    WasmStateResetRuntimeAction, WasmStateResetRuntimeRequest, WasmStateResetRuntimeResponse,
    WasmStateResetTarget,
};

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
/// The node-wide application-health probe budget, independently reserved in both directions.
pub const MAX_CONCURRENT_HEALTH_PROBES: usize = 32;
pub(crate) const RELAY_GRANT_LIFETIME: Duration = Duration::from_secs(5);
pub(crate) const RKYV_RECORD_OVERHEAD_BYTES: u64 = 4 * 1024;

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
    /// The source of this transport's process epoch and relay grant identifiers.
    pub entropy: TransportEntropy,
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
            entropy: TransportEntropy::operating_system(),
        }
    }
}

impl TransportOptions {
    pub(crate) fn validate(&self) -> Result<(), Report<TransportError>> {
        let nonzero = [
            (self.max_peers, "max_peers"),
            (self.max_connections, "max_connections"),
            (self.max_concurrent_handshakes, "max_concurrent_handshakes"),
            (self.incoming_queue_capacity, "incoming_queue_capacity"),
        ];
        for (value, name) in nonzero {
            if value == 0 {
                return Err(Report::new(TransportError::InvalidOptions {
                    reason: format!("{name} must be greater than zero"),
                }));
            }
        }
        if self.initial_stream_window_bytes == 0
            || self.initial_connection_window_bytes == 0
            || self.max_header_bytes == 0
        {
            return Err(Report::new(TransportError::InvalidOptions {
                reason: "HTTP/2 windows and header limit must be greater than zero".to_string(),
            }));
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
            return Err(Report::new(TransportError::InvalidOptions {
                reason: "max_connections must reserve inbound and outbound management, command, \
                         replication, and relay capacity for every peer and at least one \
                         on-demand connection"
                    .to_string(),
            }));
        }
        if self.connection_setup_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.progress_timeout.is_zero()
            || self.reconnect_backoff.is_zero()
            || self.shutdown_drain_timeout.is_zero()
        {
            return Err(Report::new(TransportError::InvalidOptions {
                reason: "transport deadlines must be greater than zero".to_string(),
            }));
        }
        if self.max_reconnect_backoff < self.reconnect_backoff {
            return Err(Report::new(TransportError::InvalidOptions {
                reason: "max_reconnect_backoff must not be below reconnect_backoff".to_string(),
            }));
        }
        Ok(())
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
    /// The renewed interest must be visible even if an earlier advertisement is still present.
    pub minimum_version: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionInterestVisibilityResponse {
    pub result: Result<(), RemoteOperationFailure>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeErrorEvent {
    pub message: String,
}

/// One replaceable report from the committed authority for a domain clock.
///
/// The typed request contract assigns these reports to the bounded progress subquota. A response
/// confirms that the authenticated receiver evaluated the report against its current fence; it
/// does not establish or replace the receiver's committed clock mapping.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainClockProgressRequest {
    pub domain_id: DomainName,
    pub progress: DomainClockProgress,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureOwnershipHandoffStateRequest {
    pub coordination: CoordinationIdentity,
    pub operation_id: String,
    pub source: ClusterNodeName,
    pub source_incarnation: ClusterNodeIncarnation,
    pub domain: DomainName,
    pub entity: NodeRef,
    pub base_schedule_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct PrepareOwnershipHandoffStateRequest {
    pub coordination: CoordinationIdentity,
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
    pub coordination: CoordinationIdentity,
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
    pub coordination: CoordinationIdentity,
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
    pub coordination: CoordinationIdentity,
    pub operation_id: String,
    pub domain: DomainName,
    pub entity: NodeRef,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileOwnershipHandoffPreparationsRequest {
    pub coordination: CoordinationIdentity,
    /// The leader's applied consensus position. Participants wait for this position before using
    /// their local schedule as the committed authority for reconciliation.
    pub authoritative_revision: u64,
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
    pub client_producers: Option<ClientProducersDescribeEnvelope>,
    pub metrics: Vec<String>,
}

/// The producers attached to a client ingestor on the node that executes it.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientProducersDescribeEnvelope {
    pub producers: u64,
    pub forwarded_producers: u64,
    pub outstanding_batches: u64,
    pub outstanding_bytes: u64,
    /// Batches holding a slot of the ingestor's acknowledgement window.
    pub admitted_batches: u64,
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
    pub result: Result<DataflowNodeStatusEnvelope, RemoteOperationFailure>,
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
    RetryingCommit,
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
    pub result: Result<DomainDrainStatusEnvelope, RemoteOperationFailure>,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum EntityGatePurpose {
    ModelAlteration,
    OwnershipHandoff,
    WasmStateReset(WasmStateResetScope),
}

impl EntityGatePurpose {
    pub const fn operation_name(self) -> &'static str {
        match self {
            Self::ModelAlteration => "model alteration",
            Self::OwnershipHandoff => "ownership handoff",
            Self::WasmStateReset(_) => "WASM state reset",
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub relays: Vec<RelayName>,
    pub affected_entities: Vec<NodeRef>,
    pub purpose: EntityGatePurpose,
    pub deadline_millis: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateResponse {
    pub result: Result<(), RemoteOperationFailure>,
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
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub relays: Vec<RelayName>,
    pub affected_entities: Vec<NodeRef>,
    pub purpose: EntityGatePurpose,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityDrainStatusResponse {
    pub result: Result<EntityDrainStatusEnvelope, RemoteOperationFailure>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateReleaseRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityGateReleaseResponse {
    pub result: Result<(), RemoteOperationFailure>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsRequest {
    pub domain: DomainName,
    pub kind: ModelKind,
    pub name: ModelName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsResponse {
    pub result: Result<DescribeMetricsEnvelope, RemoteOperationFailure>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeMetricsEnvelope {
    pub metrics: Vec<String>,
    pub checkpoints: Vec<nervix_models::WasmCheckpointInspection>,
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
    pub result: Result<bool, RemoteOperationFailure>,
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
    pub result: Result<LookupDescribeEnvelope, RemoteOperationFailure>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct LookupRequest {
    pub domain: DomainName,
    pub name: LookupName,
    pub key: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct LookupResponse {
    pub result: Result<Option<Vec<u8>>, RemoteOperationFailure>,
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

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for EntityDrainStatusRequest {
    type Response = EntityDrainStatusResponse;

    const NAME: &'static str = "entity_drain_status";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const TIMEOUT: Duration = Duration::from_secs(2);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for EntityGateReleaseRequest {
    type Response = EntityGateReleaseResponse;

    const NAME: &'static str = "entity_gate_release";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Cancellation;
    const TIMEOUT: Duration = Duration::from_secs(2);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
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
    // Gossip convergence can keep this request open, so it must not occupy capacity reserved for
    // short liveness probes.
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    // This request spans gossip convergence during membership changes. Target departure and node
    // shutdown cancel it independently, so the deadline is only the bound for a live but
    // non-converging cluster.
    const TIMEOUT: Duration = Duration::from_secs(60);
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

#[derive(Debug, Error)]
#[error("this process exhausted its coordination identity sequence")]
pub struct CoordinationIdentityAllocationError;

impl Transport {
    /// Listen on `listen_addr` as `identity`. Peers' advertised endpoints are resolved through
    /// `resolver`, which belongs to this node for as long as the transport runs.
    pub async fn bind(
        listen_addr: SocketAddr,
        identity: TransportIdentity,
        tls: TlsConfigBundle,
        options: TransportOptions,
        executor: Executor,
        resolver: PeerResolver,
    ) -> Result<(Self, mpsc::Receiver<ReceivedEnvelope>), Report<TransportError>> {
        let (inner, incoming) = connection::TransportState::bind(
            listen_addr,
            identity,
            tls,
            options,
            executor,
            resolver,
        )
        .await?;
        Ok((Self { inner }, incoming))
    }

    /// Every target `endpoint` resolves to now through this transport's resolver, within the
    /// connection setup timeout. A successful result holds at least one target.
    pub async fn resolve(
        &self,
        endpoint: &NodeEndpoint,
    ) -> Result<Vec<PeerTarget>, Report<DnsLookupError>> {
        PeerTarget::resolve(
            self.inner.resolver(),
            endpoint,
            self.inner.connection_setup_timeout(),
        )
        .await
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
    }

    pub fn node_id(&self) -> &ClusterNodeName {
        self.inner.node_id()
    }

    /// Allocates one operation identity bound to this transport process.
    pub fn next_coordination_identity(
        &self,
    ) -> Result<CoordinationIdentity, Report<CoordinationIdentityAllocationError>> {
        self.inner.next_coordination_identity()
    }

    /// Send one operation. Its semantic type selects the pool; callers cannot select a class.
    pub async fn send(
        &self,
        peer_node_id: &ClusterNodeName,
        envelope: Envelope,
    ) -> Result<(), Report<TransportError>> {
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

    /// Make `endpoints` the complete set of peers this node dials, each at its advertised endpoint.
    /// Every connection attempt resolves the endpoint again, so a changed DNS answer reaches the
    /// next connection without retiring the established ones.
    pub fn replace_outbound_targets(&self, endpoints: &BTreeMap<ClusterNodeName, NodeEndpoint>) {
        self.inner.replace_outbound_targets(endpoints);
    }

    /// Authenticate an endpoint whose node identity is not known yet, then add its pool target.
    /// Bootstrap discovery uses the identity in the peer certificate as the result.
    pub async fn bootstrap_target(
        &self,
        target: PeerTarget,
    ) -> Result<ClusterNodeName, Report<TransportError>> {
        self.inner.bootstrap_target(target).await
    }

    /// Add one discovered peer at its advertised endpoint. Every pool connection still verifies
    /// that its certificate names `node_id` and the endpoint's host before the target becomes
    /// usable.
    pub fn register_outbound_target(
        &self,
        node_id: ClusterNodeName,
        endpoint: NodeEndpoint,
    ) -> Result<(), Report<TransportError>> {
        self.inner.register_outbound_target(node_id, endpoint)
    }

    /// Reports whether every outbound pool except bulk is ready for node traffic.
    pub fn is_connected_to(&self, node_id: &ClusterNodeName) -> bool {
        self.inner.is_connected_to(node_id)
    }

    pub async fn active_outbound_connections(&self) -> usize {
        self.inner.active_outbound_connections()
    }

    /// Everything this transport is holding and everything it has counted, in one consistent read.
    ///
    /// The node's metric exposition is the only caller. Levels are derived from the state that
    /// owns them rather than mirrored into a counter, so a series can never disagree with the
    /// pools it describes.
    pub fn snapshot(&self) -> TransportSnapshot {
        self.inner.snapshot()
    }

    pub async fn replace_tls(&self, tls: TlsConfigBundle) -> Result<(), Report<TransportError>> {
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
            Self::RuntimeErrorEvent(_) => PoolClass::Management,
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
    ConnectionSetupTimeout {
        peer: NodeEndpoint,
        timeout: Duration,
    },
    #[error("resolving {endpoint} failed: {failure}")]
    Resolution {
        endpoint: NodeEndpoint,
        failure: DnsLookupFailure,
    },
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
    Closed(NodeEndpoint),
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

impl TransportError {
    /// Preserve a typed cause while retaining the transport's existing failure wording.
    pub(crate) fn with_cause<C: error_stack::Context>(
        cause: Report<C>,
        classify: impl FnOnce(String) -> Self,
    ) -> Report<Self> {
        let message = cause.to_string();
        cause.change_context(classify(message))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum TlsPemFileKind {
    #[strum(serialize = "cluster CA certificate")]
    CaCertificate,
    #[strum(serialize = "node certificate")]
    NodeCertificate,
    #[strum(serialize = "node private key")]
    NodePrivateKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TlsPemFailureKind {
    #[error("no PEM items found")]
    NoItemsFound,
    #[error("missing section end marker")]
    MissingSectionEnd,
    #[error("invalid section start")]
    IllegalSectionStart,
    #[error("invalid base64")]
    Base64Decode,
    #[error("I/O error: {0:?}")]
    Io(io::ErrorKind),
    #[error("section exceeds size limit")]
    SectionTooLarge,
    #[error("unclassified PEM failure")]
    Unclassified,
}

#[derive(Debug, Error)]
pub enum TlsConfigError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("missing certificate in {file}")]
    MissingCertificate { file: TlsPemFileKind },
    #[error("missing private key in {file}")]
    MissingPrivateKey { file: TlsPemFileKind },
    #[error("invalid {file} PEM: {kind}")]
    MalformedPem {
        file: TlsPemFileKind,
        kind: TlsPemFailureKind,
    },
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
    #[error("the certificate clock has no current time")]
    ClockUnavailable,
    #[error("the certificate clock reads outside the representable certificate time range")]
    ClockOutOfRange,
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
mod tests;
