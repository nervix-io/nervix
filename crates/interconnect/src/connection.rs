//! HTTP/2 connection pools and stream-level operation dispatch.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** TLS/H2 connection lifetime, pool isolation, stream leases, flow control, and relay
//!   grant, admission, reconciliation, and cancellation handling.
//! - **Depends on.** Certificate identity, bounded rkyv codecs, and execution admission.
//! - **Must not know.** Runtime graphs, scheduling decisions, or connector behavior.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::poll_fn,
    hash::RandomState,
    io::Write as _,
    net::SocketAddr,
    ops::Deref,
    sync::{
        Arc as StdArc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use dashmap::{DashMap, mapref::entry::Entry};
use error_stack::Report;
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use h2::{Reason, RecvStream, SendStream, client, server};
use http::{Method, Request, Response, StatusCode, Version};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{
    BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass, Reservation,
};
use nervix_models::{ClusterNodeName, RemoteAckOutcome, RemoteAckRegistration};
use rand_core::{OsRng, RngCore as _};
use rustls::pki_types::ServerName;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
    time::{Instant, sleep, sleep_until, timeout},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, warn};
use triomphe::Arc;

use super::{
    ControlEnvelope, Envelope, PeerTarget, PoolClass, RELAY_GRANT_LIFETIME, ReceivedEnvelope,
    RelayAdmissionDecision, RelayAdmissionStatus, RelayDelivery, RelayPayload, RequestSubquota,
    TlsConfigBundle, TransportError, TransportOptions, wire,
};
use crate::{
    identity::CertificateIdentity,
    wire::{
        ConnectionAccepted, ConnectionHello, RelayAdmissionRequest, RelayAdmissionResponse,
        RelayGrantDisposition, RelayGrantRequest, RelayGrantResponse, WIRE_CONTRACT_FINGERPRINT,
    },
};

mod relay;

const CONNECT_PATH: &str = "/v1/connect";
const CONTROL_PATH: &str = "/v1/control";
const ACK_PATH: &str = "/v1/ack";
const RELAY_GRANT_PATH: &str = "/v1/relay-grants";
const RELAY_CANCEL_PATH: &str = "/v1/relay-admissions/cancel";
const RELAY_STATUS_PATH: &str = "/v1/relay-admissions/status";
const RELAY_PATH_PREFIX: &str = "/v1/relay/";
const RELAY_PROGRESS_INTERVAL: Duration = Duration::from_millis(100);
const RELAY_PROGRESS_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const RELAY_CANCELLATION_RETRY_WINDOW: Duration = Duration::from_secs(30);
const RELAY_CHANNEL_RETENTION: Duration = Duration::from_secs(600);
const RELAY_CHANNEL_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const RESPONSE_LIMIT: u64 = 1024 * 1024;
const BODY_CHUNK_BYTES: usize = 16 * 1024;
const RESET_LIMIT: usize = 128;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ConnectionSlotKey {
    node_id: ClusterNodeName,
    target: PeerTarget,
    class: PoolClass,
    slot: usize,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct InboundPoolKey {
    node_id: ClusterNodeName,
    class: PoolClass,
}

#[derive(Clone)]
struct InboundPeer {
    addr: SocketAddr,
    node_id: ClusterNodeName,
    advertised_host: String,
    process_epoch: u64,
    class: PoolClass,
}

struct SlotControl {
    cancel: CancellationToken,
}

struct CancelOnDrop {
    token: CancellationToken,
    armed: bool,
}

impl CancelOnDrop {
    fn new(token: CancellationToken) -> Self {
        Self { token, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

struct ActiveTls {
    generation: u64,
    bundle: TlsConfigBundle,
}

struct ClientConnection {
    key: ConnectionSlotKey,
    sender: client::SendRequest<Bytes>,
    stream_slots: StreamSlotQuotas,
    peer_epoch: u64,
    retiring: CancellationToken,
    cancel: CancellationToken,
    closed: CancellationToken,
}

/// Tokio's owned permits retain a `std::sync::Arc` to their one semaphore after the connection
/// quota bundle is no longer borrowed. The surrounding `triomphe::Arc` keeps cloning the complete
/// management bundle to one reference-count operation.
struct ManagementStreamSlotQuotas {
    shared: StdArc<Semaphore>,
    discovery: StdArc<Semaphore>,
    liveness: StdArc<Semaphore>,
    progress: StdArc<Semaphore>,
    admission: StdArc<Semaphore>,
    cancellation: StdArc<Semaphore>,
    terminal: StdArc<Semaphore>,
}

#[derive(Clone)]
enum StreamSlotQuotas {
    Management(Arc<ManagementStreamSlotQuotas>),
    Shared {
        class: PoolClass,
        slots: StdArc<Semaphore>,
    },
}

const MANAGEMENT_TOTAL_STREAMS: usize = PoolClass::Management.stream_slots_per_connection();
const MANAGEMENT_DISCOVERY_STREAMS: usize = 4;
const MANAGEMENT_LIVENESS_STREAMS: usize = 8;
pub(super) const MANAGEMENT_PROGRESS_STREAMS: usize = MANAGEMENT_LIVENESS_STREAMS;
const MANAGEMENT_ADMISSION_STREAMS: usize = 4;
const MANAGEMENT_CANCELLATION_STREAMS: usize = 4;
const MANAGEMENT_TERMINAL_STREAMS: usize = 4;
const MANAGEMENT_RESERVED_STREAMS: usize = MANAGEMENT_DISCOVERY_STREAMS
    + MANAGEMENT_LIVENESS_STREAMS
    + MANAGEMENT_PROGRESS_STREAMS
    + MANAGEMENT_ADMISSION_STREAMS
    + MANAGEMENT_CANCELLATION_STREAMS
    + MANAGEMENT_TERMINAL_STREAMS;
const _: () = assert!(
    MANAGEMENT_RESERVED_STREAMS < MANAGEMENT_TOTAL_STREAMS,
    "reserved management stream quotas must leave shared capacity",
);
pub(super) const MANAGEMENT_SHARED_STREAMS: usize =
    MANAGEMENT_TOTAL_STREAMS - MANAGEMENT_RESERVED_STREAMS;
const _: () = assert!(
    MANAGEMENT_SHARED_STREAMS + MANAGEMENT_RESERVED_STREAMS == MANAGEMENT_TOTAL_STREAMS,
    "management stream subquotas must exactly partition the HTTP/2 stream capacity",
);
const _: () = assert!(
    MANAGEMENT_DISCOVERY_STREAMS > 0
        && MANAGEMENT_LIVENESS_STREAMS > 0
        && MANAGEMENT_PROGRESS_STREAMS > 0
        && MANAGEMENT_ADMISSION_STREAMS > 0
        && MANAGEMENT_CANCELLATION_STREAMS > 0
        && MANAGEMENT_TERMINAL_STREAMS > 0,
    "every reserved management stream class must have capacity",
);

impl StreamSlotQuotas {
    fn new(class: PoolClass) -> Self {
        if class == PoolClass::Management {
            return Self::Management(Arc::new(ManagementStreamSlotQuotas {
                shared: StdArc::new(Semaphore::new(MANAGEMENT_SHARED_STREAMS)),
                discovery: StdArc::new(Semaphore::new(MANAGEMENT_DISCOVERY_STREAMS)),
                liveness: StdArc::new(Semaphore::new(MANAGEMENT_LIVENESS_STREAMS)),
                progress: StdArc::new(Semaphore::new(MANAGEMENT_PROGRESS_STREAMS)),
                admission: StdArc::new(Semaphore::new(MANAGEMENT_ADMISSION_STREAMS)),
                cancellation: StdArc::new(Semaphore::new(MANAGEMENT_CANCELLATION_STREAMS)),
                terminal: StdArc::new(Semaphore::new(MANAGEMENT_TERMINAL_STREAMS)),
            }));
        }
        Self::Shared {
            class,
            slots: StdArc::new(Semaphore::new(class.stream_slots_per_connection())),
        }
    }

    fn for_subquota(&self, subquota: RequestSubquota) -> Option<&StdArc<Semaphore>> {
        match self {
            Self::Management(quotas) => Some(quotas.for_subquota(subquota)),
            Self::Shared { slots, .. } => {
                if let RequestSubquota::Shared = subquota {
                    Some(slots)
                } else {
                    None
                }
            }
        }
    }

    async fn drain(&self) {
        match self {
            Self::Management(quotas) => quotas.drain().await,
            Self::Shared { class, slots } => {
                let permits: u32 = class
                    .stream_slots_per_connection()
                    .try_into()
                    .assured("stream slot counts are much smaller than u32::MAX");
                let permit = StdArc::clone(slots)
                    .acquire_many_owned(permits)
                    .await
                    .assured("interconnect stream-slot semaphores are never closed");
                drop(permit);
            }
        }
    }
}

impl ManagementStreamSlotQuotas {
    fn for_subquota(&self, subquota: RequestSubquota) -> &StdArc<Semaphore> {
        match subquota {
            RequestSubquota::Shared => &self.shared,
            RequestSubquota::Discovery => &self.discovery,
            RequestSubquota::Liveness => &self.liveness,
            RequestSubquota::Progress => &self.progress,
            RequestSubquota::Admission => &self.admission,
            RequestSubquota::Cancellation => &self.cancellation,
            RequestSubquota::Terminal => &self.terminal,
        }
    }

    async fn drain(&self) {
        let quotas = [
            (RequestSubquota::Shared, MANAGEMENT_SHARED_STREAMS),
            (RequestSubquota::Discovery, MANAGEMENT_DISCOVERY_STREAMS),
            (RequestSubquota::Liveness, MANAGEMENT_LIVENESS_STREAMS),
            (RequestSubquota::Progress, MANAGEMENT_PROGRESS_STREAMS),
            (RequestSubquota::Admission, MANAGEMENT_ADMISSION_STREAMS),
            (
                RequestSubquota::Cancellation,
                MANAGEMENT_CANCELLATION_STREAMS,
            ),
            (RequestSubquota::Terminal, MANAGEMENT_TERMINAL_STREAMS),
        ];
        let mut drained = Vec::with_capacity(quotas.len());
        for (subquota, permits) in quotas {
            tokio::task::consume_budget().await;
            let permits: u32 = permits
                .try_into()
                .assured("management stream subquotas are much smaller than u32::MAX");
            let permit = StdArc::clone(self.for_subquota(subquota))
                .acquire_many_owned(permits)
                .await
                .assured("interconnect stream-slot semaphores are never closed");
            drained.push(permit);
        }
    }
}

struct StreamLease {
    connection: Arc<ClientConnection>,
    slot: Option<OwnedSemaphorePermit>,
    state: TransportState,
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        drop(self.slot.take());
        self.state.inner.connection_changed.notify_one();
    }
}

struct RawRequest<'a> {
    path: &'a str,
    body: Option<ChargedBytes>,
    response_class: PoolClass,
    response_limit: u64,
    timeout: Duration,
    headers: &'a [(&'a str, &'a str)],
}

struct ConnectionPermits {
    _connection: OwnedSemaphorePermit,
    _non_management: Option<OwnedSemaphorePermit>,
    _non_preconnected: Option<OwnedSemaphorePermit>,
}

struct PeerConnections {
    count: usize,
    _permit: OwnedSemaphorePermit,
}

struct InboundConnectionRegistration {
    state: TransportState,
    key: InboundPoolKey,
}

struct BoundInboundConnection {
    peer: InboundPeer,
    _registration: InboundConnectionRegistration,
    _non_management: Option<OwnedSemaphorePermit>,
    _non_preconnected: Option<OwnedSemaphorePermit>,
}

impl Drop for InboundConnectionRegistration {
    fn drop(&mut self) {
        self.state.decrement_inbound_pool(&self.key);
        self.state.decrement_peer(&self.key.node_id);
    }
}

struct RelayGrant {
    expires_at: Instant,
    reservation: Reservation,
    admission: StdArc<RelayAdmissionRecord>,
    _expiry: CancelOnDrop,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RelayAdmissionKey {
    peer_node_id: ClusterNodeName,
    ack_id: u64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct OutboundRelayKey {
    peer_node_id: ClusterNodeName,
    delivery: RelayDelivery,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RelayAttemptKey {
    peer_node_id: ClusterNodeName,
    sender_epoch: u64,
    receiver_epoch: u64,
    delivery: RelayDelivery,
}

impl RelayAttemptKey {
    fn channel(&self) -> RelayChannelKey {
        RelayChannelKey {
            peer_node_id: self.peer_node_id.clone(),
            sender_epoch: self.sender_epoch,
            receiver_epoch: self.receiver_epoch,
            channel_incarnation: self.delivery.channel_incarnation,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RelayChannelKey {
    peer_node_id: ClusterNodeName,
    sender_epoch: u64,
    receiver_epoch: u64,
    channel_incarnation: [u8; 16],
}

#[derive(Debug, Clone)]
struct RelayChannelWatermark {
    sequence: u64,
    status: RelayAdmissionStatus,
    reconciled_at: Instant,
}

#[derive(Clone)]
enum RelayAttemptEntry {
    Active(StdArc<RelayAdmissionRecord>),
    CancellationFence,
}

impl RelayAttemptEntry {
    fn progress_registration(&self) -> Option<RemoteAckRegistration> {
        match self {
            Self::Active(record) => record.progress_registration(),
            Self::CancellationFence => None,
        }
    }

    fn is_unadmitted(&self) -> bool {
        match self {
            Self::Active(record) => matches!(
                record.status(),
                RelayAdmissionStatus::Reserved | RelayAdmissionStatus::BodyReceived
            ),
            Self::CancellationFence => false,
        }
    }
}

#[derive(Debug, Clone)]
enum RelayAdmissionState {
    Reserved { grant_id: u64 },
    BodyReceived,
    Admitted,
    Rejected(String),
    Cancelled,
}

struct RelayAdmissionRecord {
    attempt: RelayAttemptKey,
    admission_key: RelayAdmissionKey,
    body_bytes: u64,
    metadata: wire::RelayMetadata,
    state: parking_lot::Mutex<RelayAdmissionState>,
    cancellation: CancellationToken,
    _item: OwnedSemaphorePermit,
    _terminal: OwnedSemaphorePermit,
}

struct RelayBodyCompletionGuard {
    admission: StdArc<RelayAdmissionRecord>,
    complete: bool,
}

impl RelayBodyCompletionGuard {
    fn new(admission: StdArc<RelayAdmissionRecord>) -> Self {
        Self {
            admission,
            complete: false,
        }
    }

    fn complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for RelayBodyCompletionGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.admission
                .reject("relay body transfer did not complete".to_string());
        }
    }
}

#[derive(Clone)]
pub struct RelayAdmission {
    record: StdArc<RelayAdmissionRecord>,
}

pub struct RelayCancellationGuard {
    state: TransportState,
    peer_node_id: ClusterNodeName,
    delivery: RelayDelivery,
    armed: bool,
}

impl RelayCancellationGuard {
    pub(crate) fn new(
        state: TransportState,
        peer_node_id: ClusterNodeName,
        delivery: RelayDelivery,
    ) -> Self {
        Self {
            state,
            peer_node_id,
            delivery,
            armed: true,
        }
    }

    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RelayCancellationGuard {
    fn drop(&mut self) {
        if !self.armed || self.state.is_shutting_down() {
            return;
        }
        let state = self.state.clone();
        let peer_node_id = self.peer_node_id.clone();
        let delivery = self.delivery;
        self.state.tasks.spawn(async move {
            state
                .cancel_relay_until_resolved(peer_node_id, delivery)
                .await;
        });
    }
}

impl std::fmt::Debug for RelayAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayAdmission")
            .field("delivery", &self.record.attempt.delivery)
            .finish_non_exhaustive()
    }
}

impl RelayAdmission {
    pub fn admit(&self) -> RelayAdmissionDecision {
        let mut state = self.record.state.lock();
        match &*state {
            RelayAdmissionState::Reserved { .. } | RelayAdmissionState::BodyReceived => {
                *state = RelayAdmissionState::Admitted;
                RelayAdmissionDecision::Admitted
            }
            RelayAdmissionState::Admitted => RelayAdmissionDecision::Admitted,
            RelayAdmissionState::Rejected(_) | RelayAdmissionState::Cancelled => {
                RelayAdmissionDecision::Cancelled
            }
        }
    }
}

impl RelayAdmissionRecord {
    fn progress_registration(&self) -> Option<RemoteAckRegistration> {
        let state = self.state.lock();
        if let RelayAdmissionState::Reserved { .. } | RelayAdmissionState::BodyReceived = &*state {
            self.metadata.admission.clone()
        } else {
            None
        }
    }

    fn reserved_grant_id(&self) -> Option<u64> {
        match &*self.state.lock() {
            RelayAdmissionState::Reserved { grant_id } => Some(*grant_id),
            RelayAdmissionState::BodyReceived
            | RelayAdmissionState::Admitted
            | RelayAdmissionState::Rejected(_)
            | RelayAdmissionState::Cancelled => None,
        }
    }

    fn status(&self) -> RelayAdmissionStatus {
        match &*self.state.lock() {
            RelayAdmissionState::Reserved { .. } => RelayAdmissionStatus::Reserved,
            RelayAdmissionState::BodyReceived => RelayAdmissionStatus::BodyReceived,
            RelayAdmissionState::Admitted => RelayAdmissionStatus::Admitted,
            RelayAdmissionState::Rejected(reason) => RelayAdmissionStatus::Rejected(reason.clone()),
            RelayAdmissionState::Cancelled => RelayAdmissionStatus::Cancelled,
        }
    }

    fn grant_disposition(&self) -> RelayGrantDisposition {
        match &*self.state.lock() {
            RelayAdmissionState::Reserved { grant_id } => RelayGrantDisposition::SendBody {
                grant_id: *grant_id,
            },
            RelayAdmissionState::BodyReceived => RelayGrantDisposition::BodyReceived,
            RelayAdmissionState::Admitted => RelayGrantDisposition::Admitted,
            RelayAdmissionState::Rejected(reason) => {
                RelayGrantDisposition::Rejected(reason.clone())
            }
            RelayAdmissionState::Cancelled => RelayGrantDisposition::Cancelled,
        }
    }

    fn mark_body_received(&self) -> bool {
        let mut state = self.state.lock();
        if let RelayAdmissionState::Reserved { .. } = &*state {
            *state = RelayAdmissionState::BodyReceived;
            true
        } else {
            false
        }
    }

    fn mark_admitted(&self) {
        let mut state = self.state.lock();
        if let RelayAdmissionState::Reserved { .. } | RelayAdmissionState::BodyReceived = &*state {
            *state = RelayAdmissionState::Admitted;
        }
    }

    fn reject(&self, reason: String) {
        let mut state = self.state.lock();
        if let RelayAdmissionState::Reserved { .. } | RelayAdmissionState::BodyReceived = &*state {
            *state = RelayAdmissionState::Rejected(reason);
            self.cancellation.cancel();
        }
    }

    fn cancel(&self) -> RelayAdmissionStatus {
        let mut state = self.state.lock();
        match &*state {
            RelayAdmissionState::Reserved { .. } | RelayAdmissionState::BodyReceived => {
                *state = RelayAdmissionState::Cancelled;
                self.cancellation.cancel();
                RelayAdmissionStatus::Cancelled
            }
            RelayAdmissionState::Admitted => RelayAdmissionStatus::Admitted,
            RelayAdmissionState::Rejected(reason) => RelayAdmissionStatus::Rejected(reason.clone()),
            RelayAdmissionState::Cancelled => RelayAdmissionStatus::Cancelled,
        }
    }
}

#[derive(Clone)]
pub(crate) struct TransportState {
    inner: Arc<TransportStateInner>,
}

pub(crate) struct TransportStateInner {
    executor: Executor,
    options: TransportOptions,
    cluster_id: String,
    node_id: ClusterNodeName,
    advertised_host: String,
    process_epoch: u64,
    local_addr: SocketAddr,
    tls: parking_lot::RwLock<ActiveTls>,
    tls_changed: Notify,
    targets: DashMap<ClusterNodeName, PeerTarget, RandomState>,
    slots: DashMap<ConnectionSlotKey, SlotControl, RandomState>,
    connections: DashMap<ConnectionSlotKey, Arc<ClientConnection>, RandomState>,
    peer_connections: DashMap<ClusterNodeName, PeerConnections, RandomState>,
    inbound_pool_connections: DashMap<InboundPoolKey, usize, RandomState>,
    peer_permits: StdArc<Semaphore>,
    next_connection: AtomicUsize,
    connection_changed: Notify,
    connection_permits: StdArc<Semaphore>,
    non_management_connection_permits: StdArc<Semaphore>,
    non_preconnected_connection_permits: StdArc<Semaphore>,
    handshake_permits: StdArc<Semaphore>,
    incoming_tx: mpsc::Sender<ReceivedEnvelope>,
    requests: super::RequestState,
    grants: DashMap<u64, RelayGrant, RandomState>,
    relay_attempts: DashMap<RelayAttemptKey, RelayAttemptEntry, RandomState>,
    active_relay_channels: DashMap<RelayChannelKey, RelayAttemptKey, RandomState>,
    relay_admissions: DashMap<RelayAdmissionKey, StdArc<RelayAdmissionRecord>, RandomState>,
    relay_watermarks: DashMap<RelayChannelKey, RelayChannelWatermark, RandomState>,
    outbound_relay_epochs: DashMap<OutboundRelayKey, u64, RandomState>,
    outbound_relay_admissions: DashMap<RelayAdmissionKey, OutboundRelayKey, RandomState>,
    relay_items: StdArc<Semaphore>,
    terminal_outcomes: StdArc<Semaphore>,
    admission_closed: CancellationToken,
    force_close: CancellationToken,
    tasks: TaskTracker,
}

impl Deref for TransportState {
    type Target = TransportStateInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TransportState {
    pub(crate) async fn bind(
        listen_addr: SocketAddr,
        advertised_host: String,
        cluster_id: String,
        node_id: ClusterNodeName,
        tls: TlsConfigBundle,
        options: TransportOptions,
        executor: Executor,
    ) -> Result<(Self, mpsc::Receiver<ReceivedEnvelope>), TransportError> {
        options.validate()?;
        tls.certificate
            .validate_local(&cluster_id, &node_id, &advertised_host)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        ensure_current(&tls.certificate)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;

        let listener = TcpListener::bind(listen_addr).await?;
        let local_addr = listener.local_addr()?;
        let (incoming_tx, incoming_rx) = mpsc::channel(options.incoming_queue_capacity);
        let process_epoch = OsRng.next_u64();
        let management_connection_reserve = options
            .max_peers
            .checked_mul(2)
            .verified("transport options validated the inbound and outbound management reserve");
        let preconnected_connection_reserve = management_connection_reserve
            .checked_mul(PoolClass::preconnected_connections_per_peer())
            .verified("transport options validated every inbound and outbound preconnected slot");
        let state = Self {
            inner: Arc::new(TransportStateInner {
                executor,
                cluster_id,
                node_id,
                advertised_host,
                process_epoch,
                local_addr,
                tls: parking_lot::RwLock::new(ActiveTls {
                    generation: 1,
                    bundle: tls,
                }),
                tls_changed: Notify::new(),
                targets: DashMap::default(),
                slots: DashMap::default(),
                connections: DashMap::default(),
                peer_connections: DashMap::default(),
                inbound_pool_connections: DashMap::default(),
                peer_permits: StdArc::new(Semaphore::new(options.max_peers)),
                next_connection: AtomicUsize::new(0),
                connection_changed: Notify::new(),
                connection_permits: StdArc::new(Semaphore::new(options.max_connections)),
                non_management_connection_permits: StdArc::new(Semaphore::new(
                    options
                        .max_connections
                        .checked_sub(management_connection_reserve)
                        .verified(
                            "transport options reserve fewer management connections than the total",
                        ),
                )),
                non_preconnected_connection_permits: StdArc::new(Semaphore::new(
                    options
                        .max_connections
                        .checked_sub(preconnected_connection_reserve)
                        .verified(
                            "transport options reserve fewer preconnected connections than the \
                             total",
                        ),
                )),
                handshake_permits: StdArc::new(Semaphore::new(options.max_concurrent_handshakes)),
                requests: super::RequestState::new(options.incoming_queue_capacity),
                grants: DashMap::default(),
                relay_attempts: DashMap::default(),
                active_relay_channels: DashMap::default(),
                relay_admissions: DashMap::default(),
                relay_watermarks: DashMap::default(),
                outbound_relay_epochs: DashMap::default(),
                outbound_relay_admissions: DashMap::default(),
                relay_items: StdArc::new(Semaphore::new(options.incoming_queue_capacity)),
                terminal_outcomes: StdArc::new(Semaphore::new(options.incoming_queue_capacity)),
                admission_closed: CancellationToken::new(),
                force_close: CancellationToken::new(),
                tasks: TaskTracker::new(),
                options,
                incoming_tx,
            }),
        };
        let accept_state = state.clone();
        state.tasks.spawn(async move {
            accept_state.accept_loop(listener).await;
        });
        let progress_state = state.clone();
        state.tasks.spawn(async move {
            progress_state.report_relay_progress().await;
        });
        let retirement_state = state.clone();
        state.tasks.spawn(async move {
            retirement_state.retire_idle_relay_channels().await;
        });
        Ok((state, incoming_rx))
    }

    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn node_id(&self) -> &ClusterNodeName {
        &self.node_id
    }

    pub(crate) fn requests(&self) -> &super::RequestState {
        &self.requests
    }

    pub(crate) fn executor(&self) -> &Executor {
        &self.executor
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.admission_closed.is_cancelled()
    }

    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.admission_closed.clone()
    }

    pub(crate) fn active_outbound_connections(&self) -> usize {
        self.connections.len()
    }

    pub(crate) fn is_connected_to(&self, node_id: &ClusterNodeName) -> bool {
        let Some(target) = self.targets.get(node_id).map(|target| target.clone()) else {
            return false;
        };
        for class in PoolClass::PRECONNECTED {
            for slot in 0..class.connections_per_peer() {
                let key = ConnectionSlotKey {
                    node_id: node_id.clone(),
                    target: target.clone(),
                    class,
                    slot,
                };
                if !self.connections.contains_key(&key) {
                    return false;
                }
            }
        }
        true
    }

    pub(crate) fn replace_outbound_targets(
        &self,
        targets: &BTreeMap<ClusterNodeName, BTreeSet<PeerTarget>>,
    ) {
        let accepted = targets
            .iter()
            .take(self.options.max_peers)
            .filter_map(|(node, choices)| {
                choices.first().map(|target| (node.clone(), target.clone()))
            })
            .collect::<BTreeMap<_, _>>();

        let removed = self
            .targets
            .iter()
            .filter(|entry| accepted.get(entry.key()) != Some(entry.value()))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for node in removed {
            self.targets.remove(&node);
            self.cancel_slots_for_node(&node);
        }

        for (node, target) in accepted {
            let changed = self
                .targets
                .get(&node)
                .is_none_or(|current| *current != target);
            if changed {
                self.cancel_slots_for_node(&node);
                self.targets.insert(node.clone(), target.clone());
            }
            self.ensure_preconnected_slots(&node, &target);
        }
    }

    pub(crate) fn register_outbound_target(
        &self,
        node_id: ClusterNodeName,
        target: PeerTarget,
    ) -> Result<(), TransportError> {
        if !self.targets.contains_key(&node_id) && self.targets.len() >= self.options.max_peers {
            return Err(TransportError::PoolExhausted);
        }
        let changed = self
            .targets
            .get(&node_id)
            .is_none_or(|current| *current != target);
        if changed {
            self.cancel_slots_for_node(&node_id);
            self.targets.insert(node_id.clone(), target.clone());
        }
        self.ensure_preconnected_slots(&node_id, &target);
        Ok(())
    }

    pub(crate) async fn bootstrap_target(
        &self,
        target: PeerTarget,
    ) -> Result<ClusterNodeName, TransportError> {
        if self.admission_closed.is_cancelled() {
            return Err(TransportError::ShuttingDown);
        }
        let peer_addr = target.addr;
        let setup = async {
            let cancel = CancellationToken::new();
            let _connection_permits = self
                .acquire_connection_permits(PoolClass::Management, &cancel)
                .await
                .ok_or(TransportError::ShuttingDown)?;
            let _handshake_permit = tokio::select! {
                _ = self.admission_closed.cancelled() => {
                    return Err(TransportError::ShuttingDown);
                }
                permit = StdArc::clone(&self.handshake_permits).acquire_owned() => {
                    permit.map_err(|_| TransportError::ShuttingDown)?
                }
            };
            let tcp = TcpStream::connect(target.addr).await?;
            tcp.set_nodelay(true)?;
            let tls = self.tls.read().bundle.clone();
            ensure_current(&tls.certificate)
                .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
            let server_name = ServerName::try_from(target.server_name.clone())
                .map_err(|_| TransportError::InvalidServerName(target.server_name.clone()))?;
            let stream = TlsConnector::from(tls.client_config.clone())
                .connect(server_name, tcp)
                .await?;
            let identity = validate_tls_session(
                stream.get_ref().1.alpn_protocol(),
                stream.get_ref().1.peer_certificates(),
                &self.cluster_id,
                None,
            )?;
            if !identity.matches_endpoint(&target.server_name) {
                return Err(TransportError::InvalidHandshake(format!(
                    "peer certificate does not identify bootstrap endpoint '{}'",
                    target.server_name
                )));
            }
            let node_id = identity.node_id;
            self.register_outbound_target(node_id.clone(), target)?;
            Ok(node_id)
        };
        match timeout(self.options.connection_setup_timeout, setup).await {
            Ok(result) => result,
            Err(_) => Err(TransportError::ConnectionSetupTimeout {
                peer: peer_addr,
                timeout: self.options.connection_setup_timeout,
            }),
        }
    }

    pub(crate) fn retire_departed_connections(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
        let departed = self
            .targets
            .iter()
            .filter(|target| !live_nodes.contains(target.key()))
            .map(|target| target.key().clone())
            .collect::<Vec<_>>();
        for node in departed {
            self.targets.remove(&node);
            self.cancel_slots_for_node(&node);
        }
    }

    fn ensure_class_slots(&self, node_id: &ClusterNodeName, target: &PeerTarget, class: PoolClass) {
        for slot in 0..class.connections_per_peer() {
            let key = ConnectionSlotKey {
                node_id: node_id.clone(),
                target: target.clone(),
                class,
                slot,
            };
            self.ensure_slot(key);
        }
    }

    fn ensure_preconnected_slots(&self, node_id: &ClusterNodeName, target: &PeerTarget) {
        for class in PoolClass::PRECONNECTED {
            self.ensure_class_slots(node_id, target, class);
        }
    }

    fn ensure_slot(&self, key: ConnectionSlotKey) {
        if self.admission_closed.is_cancelled() {
            return;
        }
        match self.slots.entry(key.clone()) {
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                let cancel = CancellationToken::new();
                entry.insert(SlotControl {
                    cancel: cancel.clone(),
                });
                let state = self.clone();
                self.tasks.spawn(async move {
                    state.run_slot(key, cancel).await;
                });
            }
        }
    }

    fn cancel_slots_for_node(&self, node_id: &ClusterNodeName) {
        let slots = self
            .slots
            .iter()
            .filter(|slot| &slot.key().node_id == node_id)
            .map(|slot| (slot.key().clone(), slot.cancel.clone()))
            .collect::<Vec<_>>();
        for (key, cancel) in slots {
            self.retire_slot(&key, &cancel);
        }
    }

    fn retire_slot(&self, key: &ConnectionSlotKey, cancel: &CancellationToken) {
        cancel.cancel();
        let connection = self
            .connections
            .remove_if(key, |_, connection| connection.retiring == *cancel);
        if connection.is_some() {
            self.decrement_peer(&key.node_id);
        }
        if self
            .slots
            .get(key)
            .is_some_and(|slot| slot.cancel == *cancel)
        {
            self.slots.remove(key);
        }
        self.connection_changed.notify_waiters();
    }

    async fn run_slot(self, key: ConnectionSlotKey, slot_cancel: CancellationToken) {
        let mut backoff = self.options.reconnect_backoff;
        loop {
            tokio::task::consume_budget().await;
            if slot_cancel.is_cancelled() || self.admission_closed.is_cancelled() {
                break;
            }
            let permits = self
                .acquire_connection_permits(key.class, &slot_cancel)
                .await;
            let permits = match permits {
                Some(permits) => permits,
                None => break,
            };
            let connected = tokio::select! {
                _ = slot_cancel.cancelled() => break,
                _ = self.admission_closed.cancelled() => break,
                connected = self.connect(&key, &slot_cancel) => connected,
            };
            match connected {
                Ok(connection) => {
                    backoff = self.options.reconnect_backoff;
                    match self.register_connection(connection.clone()) {
                        Ok(()) => {
                            tokio::select! {
                                _ = slot_cancel.cancelled() => {}
                                _ = self.admission_closed.cancelled() => {}
                                _ = connection.closed.cancelled() => {}
                            }
                            self.unregister_connection(&key, &connection);
                            self.drain_outbound_connection(&connection).await;
                            connection.cancel.cancel();
                        }
                        Err(error) => {
                            debug!(
                                ?error,
                                node = %key.node_id,
                                class = ?key.class,
                                "interconnect peer capacity refused an outbound connection"
                            );
                            connection.cancel.cancel();
                        }
                    }
                }
                Err(error) => {
                    debug!(
                        ?error,
                        node = %key.node_id,
                        target = %key.target.addr,
                        class = ?key.class,
                        slot = key.slot,
                        "interconnect pool connection failed"
                    );
                }
            }
            drop(permits);
            tokio::select! {
                _ = slot_cancel.cancelled() => break,
                _ = self.admission_closed.cancelled() => break,
                _ = sleep(backoff) => {}
            }
            backoff = backoff
                .checked_mul(2)
                .unwrap_or(self.options.max_reconnect_backoff)
                .min(self.options.max_reconnect_backoff);
        }
        self.unregister_slot(&key, &slot_cancel);
    }

    async fn acquire_connection_permits(
        &self,
        class: PoolClass,
        cancel: &CancellationToken,
    ) -> Option<ConnectionPermits> {
        let non_management = if class == PoolClass::Management {
            None
        } else {
            let acquired = tokio::select! {
                _ = cancel.cancelled() => return None,
                _ = self.admission_closed.cancelled() => return None,
                acquired = StdArc::clone(&self.non_management_connection_permits).acquire_owned() => acquired,
            };
            match acquired {
                Ok(permit) => Some(permit),
                Err(_) => return None,
            }
        };
        let non_preconnected = if class.is_preconnected() {
            None
        } else {
            let acquired = tokio::select! {
                _ = cancel.cancelled() => return None,
                _ = self.admission_closed.cancelled() => return None,
                acquired = StdArc::clone(&self.non_preconnected_connection_permits).acquire_owned() => acquired,
            };
            match acquired {
                Ok(permit) => Some(permit),
                Err(_) => return None,
            }
        };
        let acquired = tokio::select! {
            _ = cancel.cancelled() => return None,
            _ = self.admission_closed.cancelled() => return None,
            acquired = StdArc::clone(&self.connection_permits).acquire_owned() => acquired,
        };
        let connection = match acquired {
            Ok(permit) => permit,
            Err(_) => return None,
        };
        Some(ConnectionPermits {
            _connection: connection,
            _non_management: non_management,
            _non_preconnected: non_preconnected,
        })
    }

    fn unregister_slot(&self, key: &ConnectionSlotKey, cancel: &CancellationToken) {
        let remove = self
            .slots
            .get(key)
            .is_some_and(|slot| slot.cancel == *cancel);
        if remove {
            self.slots.remove(key);
        }
    }

    fn register_connection(&self, connection: Arc<ClientConnection>) -> Result<(), TransportError> {
        let key = connection.key.clone();
        let Entry::Vacant(entry) = self.connections.entry(key.clone()) else {
            return Err(TransportError::PoolExhausted);
        };
        self.increment_peer(&key.node_id)?;
        entry.insert(connection);
        self.connection_changed.notify_waiters();
        Ok(())
    }

    fn unregister_connection(&self, key: &ConnectionSlotKey, connection: &Arc<ClientConnection>) {
        let remove = self
            .connections
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current.value(), connection));
        if !remove {
            return;
        }
        self.connections.remove(key);
        self.decrement_peer(&key.node_id);
        self.connection_changed.notify_waiters();
    }

    async fn drain_outbound_connection(&self, connection: &ClientConnection) {
        if connection.closed.is_cancelled() {
            return;
        }
        let drained = connection.stream_slots.drain();
        tokio::select! {
            _ = self.force_close.cancelled() => {}
            _ = sleep(self.options.shutdown_drain_timeout) => {}
            _ = drained => {}
        }
    }

    async fn connect(
        &self,
        key: &ConnectionSlotKey,
        slot_cancel: &CancellationToken,
    ) -> Result<Arc<ClientConnection>, TransportError> {
        let setup = async {
            let tcp = TcpStream::connect(key.target.addr).await?;
            tcp.set_nodelay(true)?;
            let (generation, tls) = {
                let active = self.tls.read();
                (active.generation, active.bundle.clone())
            };
            ensure_current(&tls.certificate)
                .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
            let server_name = ServerName::try_from(key.target.server_name.clone())
                .map_err(|_| TransportError::InvalidServerName(key.target.server_name.clone()))?;
            let stream = TlsConnector::from(tls.client_config.clone())
                .connect(server_name, tcp)
                .await?;
            let peer_identity = validate_tls_session(
                stream.get_ref().1.alpn_protocol(),
                stream.get_ref().1.peer_certificates(),
                &self.cluster_id,
                Some(&key.node_id),
            )?;
            let certificate_expires_at =
                certificate_expiration_deadline([&tls.certificate, &peer_identity])?;

            let mut builder = client::Builder::new();
            configure_client_builder(&mut builder, &self.options, key.class)?;
            let (sender, connection) = builder.handshake(stream).await?;
            let cancel = CancellationToken::new();
            let closed = CancellationToken::new();
            let driver_cancel = cancel.clone();
            let driver_closed = closed.clone();
            let force_close = self.force_close.clone();
            self.tasks.spawn(async move {
                tokio::select! {
                    result = connection => {
                        if let Err(error) = result {
                            debug!(?error, "outbound HTTP/2 connection closed");
                        }
                    }
                    _ = driver_cancel.cancelled() => {}
                    _ = force_close.cancelled() => {}
                    _ = sleep_until(certificate_expires_at) => {}
                }
                driver_closed.cancel();
            });
            let driver_setup_guard = CancelOnDrop::new(cancel.clone());
            let connection = Arc::new(ClientConnection {
                key: key.clone(),
                sender,
                stream_slots: StreamSlotQuotas::new(key.class),
                peer_epoch: 0,
                retiring: slot_cancel.clone(),
                cancel,
                closed,
            });
            let hello = ConnectionHello {
                fingerprint: WIRE_CONTRACT_FINGERPRINT,
                class: key.class,
                process_epoch: self.process_epoch,
                node_id: self.node_id.clone(),
                advertised_host: self.advertised_host.clone(),
            };
            let body = wire::encode_rkyv(
                &self.executor,
                MemoryClass::Management,
                CpuClass::Control,
                self.executor.limits().management_event_bytes.as_u64(),
                hello,
            )
            .await?;
            let accepted = connection
                .request_raw(
                    self,
                    RawRequest {
                        path: CONNECT_PATH,
                        body: Some(body),
                        response_class: PoolClass::Management,
                        response_limit: self.executor.limits().management_event_bytes.as_u64(),
                        timeout: self.options.connection_setup_timeout,
                        headers: &[],
                    },
                )
                .await?;
            let accepted = wire::decode_rkyv::<ConnectionAccepted>(
                &self.executor,
                MemoryClass::Management,
                CpuClass::Control,
                accepted,
            )
            .await?
            .into_value();
            if accepted.fingerprint != WIRE_CONTRACT_FINGERPRINT || accepted.node_id != key.node_id
            {
                return Err(TransportError::InvalidHandshake(
                    "wire fingerprint or addressed node identity differs".to_string(),
                ));
            }
            let connection = Arc::new(ClientConnection {
                key: connection.key.clone(),
                sender: connection.sender.clone(),
                stream_slots: connection.stream_slots.clone(),
                peer_epoch: accepted.process_epoch,
                retiring: connection.retiring.clone(),
                cancel: connection.cancel.clone(),
                closed: connection.closed.clone(),
            });
            let current_generation = self.tls.read().generation;
            if current_generation != generation || slot_cancel.is_cancelled() {
                connection.cancel.cancel();
                return Err(TransportError::Closed(key.target.addr));
            }
            driver_setup_guard.disarm();
            Ok(connection)
        };
        match timeout(self.options.connection_setup_timeout, setup).await {
            Ok(result) => result,
            Err(_) => Err(TransportError::ConnectionSetupTimeout {
                peer: key.target.addr,
                timeout: self.options.connection_setup_timeout,
            }),
        }
    }

    async fn lease(
        &self,
        node_id: &ClusterNodeName,
        class: PoolClass,
        subquota: RequestSubquota,
        deadline: Instant,
    ) -> Result<StreamLease, TransportError> {
        loop {
            tokio::task::consume_budget().await;
            let notified = self.connection_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.admission_closed.is_cancelled() {
                return Err(TransportError::ShuttingDown);
            }
            let target = if let Some(target) = self.targets.get(node_id) {
                target.value().clone()
            } else {
                return Err(TransportError::MissingTarget(node_id.clone()));
            };
            self.ensure_class_slots(node_id, &target, class);

            let count = class.connections_per_peer();
            let start = self
                .next_connection
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.checked_add(1).unwrap_or_default())
                })
                .assured("the round-robin cursor update always returns a value")
                % count;
            for offset in 0..count {
                let index = (start + offset) % count;
                let key = ConnectionSlotKey {
                    node_id: node_id.clone(),
                    target: target.clone(),
                    class,
                    slot: index,
                };
                let Some(connection) = self.connections.get(&key).map(|item| item.clone()) else {
                    continue;
                };
                let stream_slots = connection
                    .stream_slots
                    .for_subquota(subquota)
                    .assured("reserved stream subquotas are only assigned to management requests");
                let permit = match StdArc::clone(stream_slots).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => continue,
                };
                if connection.closed.is_cancelled() || connection.retiring.is_cancelled() {
                    continue;
                }
                return Ok(StreamLease {
                    connection,
                    slot: Some(permit),
                    state: self.clone(),
                });
            }

            tokio::select! {
                _ = self.admission_closed.cancelled() => {
                    return Err(TransportError::ShuttingDown);
                }
                _ = sleep_until(deadline) => {
                    return Err(TransportError::RequestTimeout {
                        peer: node_id.clone(),
                        timeout: self.options.request_timeout,
                    });
                }
                _ = &mut notified => {}
            }
        }
    }

    pub(crate) async fn send(
        &self,
        node_id: &ClusterNodeName,
        envelope: Envelope,
    ) -> Result<(), TransportError> {
        if let Envelope::RelayPayload(payload) = envelope {
            return self.send_relay(node_id, payload).await;
        }
        let completed_admission = if let Envelope::Ack(ack) = &envelope {
            if let RemoteAckOutcome::Alive = &ack.outcome {
                None
            } else {
                let key = RelayAdmissionKey {
                    peer_node_id: node_id.clone(),
                    ack_id: ack.ack_id,
                };
                if let Some(record) = self.relay_admissions.get(&key) {
                    if let RemoteAckOutcome::NoAck(reason) = &ack.outcome {
                        record.reject(reason.clone());
                    } else {
                        record.mark_admitted();
                    }
                }
                Some(key)
            }
        } else {
            None
        };
        let result = async {
            let class = envelope.pool_class();
            let subquota = match &envelope {
                Envelope::Ack(ack) => {
                    if ack.outcome == RemoteAckOutcome::Alive {
                        RequestSubquota::Progress
                    } else {
                        RequestSubquota::Terminal
                    }
                }
                Envelope::Control(_) => RequestSubquota::Shared,
                Envelope::RelayPayload(_) => RequestSubquota::Admission,
            };
            let timeout_duration = self.options.request_timeout;
            let deadline = Instant::now()
                .checked_add(timeout_duration)
                .ok_or_else(|| TransportError::InvalidOptions {
                    reason: "request deadline exceeds the monotonic clock range".to_string(),
                })?;
            let lease = self.lease(node_id, class, subquota, deadline).await?;
            match envelope {
                Envelope::Ack(ack) => {
                    let bytes = wire::encode_rkyv(
                        &self.executor,
                        class.memory_class(),
                        CpuClass::Data,
                        class.control_body_limit(&self.executor),
                        ack,
                    )
                    .await?;
                    lease
                        .request_raw(
                            self,
                            RawRequest {
                                path: ACK_PATH,
                                body: Some(bytes),
                                response_class: class,
                                response_limit: RESPONSE_LIMIT,
                                timeout: deadline.saturating_duration_since(Instant::now()),
                                headers: &[],
                            },
                        )
                        .await?;
                }
                Envelope::Control(control) => {
                    if let ControlEnvelope::Request(_) | ControlEnvelope::Response(_) = &control {
                        return Err(TransportError::Decode(
                            "typed request envelopes cannot be sent as one-way controls"
                                .to_string(),
                        ));
                    }
                    let bytes = wire::encode_rkyv(
                        &self.executor,
                        class.memory_class(),
                        class.cpu_class(),
                        class.control_body_limit(&self.executor),
                        control,
                    )
                    .await?;
                    lease
                        .request_raw(
                            self,
                            RawRequest {
                                path: CONTROL_PATH,
                                body: Some(bytes),
                                response_class: class,
                                response_limit: class.control_body_limit(&self.executor),
                                timeout: deadline.saturating_duration_since(Instant::now()),
                                headers: &[],
                            },
                        )
                        .await?;
                }
                Envelope::RelayPayload(_) => {
                    return Err(TransportError::RelayGrant(
                        "relay payload escaped relay admission".to_string(),
                    ));
                }
            }
            Ok(())
        }
        .await;
        if result.is_ok()
            && let Some(key) = completed_admission
        {
            self.retire_relay_admission(&key);
        }
        result
    }

    pub(crate) async fn round_trip_control(
        &self,
        node_id: &ClusterNodeName,
        control: ControlEnvelope,
        subquota: RequestSubquota,
        timeout_duration: Duration,
    ) -> Result<wire::Decoded<ControlEnvelope>, TransportError> {
        let class = control.pool_class();
        let deadline = Instant::now()
            .checked_add(timeout_duration)
            .ok_or_else(|| TransportError::InvalidOptions {
                reason: "request deadline exceeds the monotonic clock range".to_string(),
            })?;
        let lease = self.lease(node_id, class, subquota, deadline).await?;
        let bytes = wire::encode_rkyv(
            &self.executor,
            class.memory_class(),
            class.cpu_class(),
            class.control_body_limit(&self.executor),
            control,
        )
        .await?;
        let response = lease
            .request_raw(
                self,
                RawRequest {
                    path: CONTROL_PATH,
                    body: Some(bytes),
                    response_class: class,
                    response_limit: class.control_body_limit(&self.executor),
                    timeout: timeout_duration,
                    headers: &[],
                },
            )
            .await?;
        wire::decode_rkyv::<ControlEnvelope>(
            &self.executor,
            class.memory_class(),
            class.cpu_class(),
            response,
        )
        .await
    }

    fn deliver_incoming(
        &self,
        peer_addr: SocketAddr,
        peer_node_id: ClusterNodeName,
        envelope: Envelope,
        decoded: Option<Reservation>,
    ) -> Result<(), TransportError> {
        self.incoming_tx
            .try_send(ReceivedEnvelope::new(
                peer_addr,
                peer_node_id,
                envelope,
                decoded,
            ))
            .map_err(|_| TransportError::IncomingQueueFull)
    }

    async fn deliver_terminal_incoming(
        &self,
        peer_addr: SocketAddr,
        peer_node_id: ClusterNodeName,
        envelope: Envelope,
        decoded: Option<Reservation>,
    ) -> Result<(), TransportError> {
        let received = ReceivedEnvelope::new(peer_addr, peer_node_id, envelope, decoded);
        tokio::select! {
            _ = self.admission_closed.cancelled() => Err(TransportError::ShuttingDown),
            result = self.incoming_tx.send(received) => {
                result.map_err(|_| TransportError::ShuttingDown)
            }
        }
    }

    async fn accept_loop(self, listener: TcpListener) {
        loop {
            tokio::task::consume_budget().await;
            let accepted = tokio::select! {
                _ = self.admission_closed.cancelled() => break,
                accepted = listener.accept() => accepted,
            };
            let (tcp, peer_addr) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    warn!(?error, "interconnect listener accept failed");
                    continue;
                }
            };
            let handshake_permit = match StdArc::clone(&self.handshake_permits).try_acquire_owned()
            {
                Ok(permit) => permit,
                Err(_) => {
                    debug!(%peer_addr, "interconnect handshake quota is full");
                    continue;
                }
            };
            let connection_permit =
                match StdArc::clone(&self.connection_permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        debug!(%peer_addr, "interconnect connection quota is full");
                        continue;
                    }
                };
            let state = self.clone();
            self.tasks.spawn(async move {
                tokio::select! {
                    _ = state.admission_closed.cancelled() => {}
                    _ = state.force_close.cancelled() => {}
                    result = state.clone().accept_connection(
                        tcp,
                        peer_addr,
                        handshake_permit,
                        connection_permit,
                    ) => {
                        if let Err(error) = result {
                            debug!(?error, %peer_addr, "interconnect connection rejected");
                        }
                    }
                }
            });
        }
    }

    async fn accept_connection(
        self,
        tcp: TcpStream,
        peer_addr: SocketAddr,
        handshake_permit: OwnedSemaphorePermit,
        _connection_permit: OwnedSemaphorePermit,
    ) -> Result<(), Report<TransportError>> {
        tcp.set_nodelay(true).map_err(TransportError::from)?;
        let (generation, tls) = {
            let active = self.tls.read();
            (active.generation, active.bundle.clone())
        };
        ensure_current(&tls.certificate)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        let stream = timeout(
            self.options.connection_setup_timeout,
            TlsAcceptor::from(tls.server_config.clone()).accept(tcp),
        )
        .await
        .map_err(|_| TransportError::ConnectionSetupTimeout {
            peer: peer_addr,
            timeout: self.options.connection_setup_timeout,
        })?
        .map_err(TransportError::from)?;
        let peer_identity = validate_tls_session(
            stream.get_ref().1.alpn_protocol(),
            stream.get_ref().1.peer_certificates(),
            &self.cluster_id,
            None,
        )?;
        let certificate_expires_at =
            certificate_expiration_deadline([&tls.certificate, &peer_identity])?;
        let mut builder = server::Builder::new();
        configure_server_builder(&mut builder, &self.options)?;
        let mut connection = timeout(
            self.options.connection_setup_timeout,
            builder.handshake(stream),
        )
        .await
        .map_err(|_| TransportError::ConnectionSetupTimeout {
            peer: peer_addr,
            timeout: self.options.connection_setup_timeout,
        })?
        .map_err(TransportError::from)?;
        let first = timeout(self.options.connection_setup_timeout, connection.accept())
            .await
            .map_err(|_| TransportError::ConnectionSetupTimeout {
                peer: peer_addr,
                timeout: self.options.connection_setup_timeout,
            })?
            .ok_or_else(|| {
                TransportError::InvalidHandshake(
                    "connection closed before its class binding".to_string(),
                )
            })?
            .map_err(TransportError::from)?;
        let (request, respond) = first;
        let bound = self
            .complete_inbound_binding(
                &mut connection,
                peer_addr,
                peer_identity,
                request,
                respond,
                handshake_permit,
            )
            .await?;

        self.drive_inbound(
            connection,
            bound.peer.clone(),
            generation,
            certificate_expires_at,
        )
        .await?;
        Ok(())
    }

    async fn complete_inbound_binding<T>(
        &self,
        connection: &mut server::Connection<T, Bytes>,
        peer_addr: SocketAddr,
        peer_identity: CertificateIdentity,
        request: Request<RecvStream>,
        respond: server::SendResponse<Bytes>,
        handshake_permit: OwnedSemaphorePermit,
    ) -> Result<BoundInboundConnection, Report<TransportError>>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let binding = async {
            if request.method() != Method::POST || request.uri().path() != CONNECT_PATH {
                return Err(Report::new(TransportError::InvalidHandshake(
                    "first HTTP/2 stream must POST the connection binding".to_string(),
                )));
            }
            let hello_bytes = read_body(
                &self.executor,
                MemoryClass::Management,
                self.executor.limits().management_event_bytes.as_u64(),
                self.options.progress_timeout,
                request.into_body(),
            )
            .await?;
            let hello = wire::decode_rkyv::<ConnectionHello>(
                &self.executor,
                MemoryClass::Management,
                CpuClass::Control,
                hello_bytes,
            )
            .await?
            .into_value();
            if hello.fingerprint != WIRE_CONTRACT_FINGERPRINT
                || hello.node_id != peer_identity.node_id
                || !peer_identity.matches_endpoint(&hello.advertised_host)
            {
                return Err(Report::new(TransportError::InvalidHandshake(
                    "wire fingerprint, certificate identity, or advertised endpoint differs"
                        .to_string(),
                )));
            }
            let non_management = if hello.class == PoolClass::Management {
                None
            } else {
                match StdArc::clone(&self.non_management_connection_permits).try_acquire_owned() {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        return Err(Report::new(TransportError::PoolExhausted));
                    }
                }
            };
            let non_preconnected = if hello.class.is_preconnected() {
                None
            } else {
                match StdArc::clone(&self.non_preconnected_connection_permits).try_acquire_owned() {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        return Err(Report::new(TransportError::PoolExhausted));
                    }
                }
            };
            drop(handshake_permit);
            let registration =
                self.register_inbound_pool(peer_identity.node_id.clone(), hello.class)?;
            let accepted = ConnectionAccepted {
                fingerprint: WIRE_CONTRACT_FINGERPRINT,
                process_epoch: self.process_epoch,
                node_id: self.node_id.clone(),
            };
            let accepted = wire::encode_rkyv(
                &self.executor,
                MemoryClass::Management,
                CpuClass::Control,
                self.executor.limits().management_event_bytes.as_u64(),
                accepted,
            )
            .await?;
            send_response(
                respond,
                StatusCode::OK,
                Some(accepted),
                self.options.progress_timeout,
            )
            .await?;
            Ok(BoundInboundConnection {
                peer: InboundPeer {
                    addr: peer_addr,
                    node_id: peer_identity.node_id.clone(),
                    advertised_host: hello.advertised_host,
                    process_epoch: hello.process_epoch,
                    class: hello.class,
                },
                _registration: registration,
                _non_management: non_management,
                _non_preconnected: non_preconnected,
            })
        };

        // Request and response flow-control windows advance only while the h2 connection is polled.
        let connection_closed = poll_fn(|context| connection.poll_closed(context));
        tokio::pin!(binding);
        tokio::pin!(connection_closed);
        tokio::select! {
            result = &mut binding => result,
            result = &mut connection_closed => {
                result.map_err(TransportError::from)?;
                Err(Report::new(TransportError::InvalidHandshake(
                    "connection closed before its class binding completed".to_string(),
                )))
            }
        }
    }

    async fn drive_inbound<T>(
        &self,
        mut connection: server::Connection<T, Bytes>,
        peer: InboundPeer,
        generation: u64,
        certificate_expires_at: Instant,
    ) -> Result<(), TransportError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let stream_slots = StdArc::new(Semaphore::new(peer.class.stream_slots_per_connection()));
        let connection_force_close = CancellationToken::new();
        let _connection_force_close_guard = CancelOnDrop::new(connection_force_close.clone());
        let mut draining = false;
        let mut drain_deadline = None;
        loop {
            tokio::task::consume_budget().await;
            let accepted = if draining {
                let deadline = drain_deadline
                    .verified("entering drain always records its force-close deadline");
                tokio::select! {
                    _ = self.force_close.cancelled() => break,
                    _ = sleep_until(deadline) => break,
                    accepted = connection.accept() => accepted,
                }
            } else {
                let changed = self.tls_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.tls.read().generation != generation {
                    connection.graceful_shutdown();
                    draining = true;
                    drain_deadline = Some(
                        Instant::now()
                            .checked_add(self.options.shutdown_drain_timeout)
                            .assured(
                                "a configured transport drain timeout fits the monotonic clock",
                            ),
                    );
                    continue;
                }
                tokio::select! {
                    _ = self.admission_closed.cancelled() => {
                        connection.graceful_shutdown();
                        draining = true;
                        drain_deadline = Some(
                            Instant::now()
                                .checked_add(self.options.shutdown_drain_timeout)
                                .assured("a configured transport drain timeout fits the monotonic clock"),
                        );
                        continue;
                    }
                    _ = self.force_close.cancelled() => break,
                    _ = sleep_until(certificate_expires_at) => {
                        connection.graceful_shutdown();
                        draining = true;
                        drain_deadline = Some(
                            Instant::now()
                                .checked_add(self.options.shutdown_drain_timeout)
                                .assured("a configured transport drain timeout fits the monotonic clock"),
                        );
                        continue;
                    }
                    _ = &mut changed => {
                        if self.tls.read().generation != generation {
                            connection.graceful_shutdown();
                            draining = true;
                            drain_deadline = Some(
                                Instant::now()
                                    .checked_add(self.options.shutdown_drain_timeout)
                                    .assured("a configured transport drain timeout fits the monotonic clock"),
                            );
                        }
                        continue;
                    }
                    accepted = connection.accept() => accepted,
                }
            };
            let Some(accepted) = accepted else {
                break;
            };
            let (request, mut response) = accepted?;
            let stream_slot = match StdArc::clone(&stream_slots).try_acquire_owned() {
                Ok(stream_slot) => stream_slot,
                Err(_) => {
                    response.send_reset(Reason::REFUSED_STREAM);
                    continue;
                }
            };
            let state = self.clone();
            let peer = peer.clone();
            let stream_force_close = connection_force_close.clone();
            let transport_force_close = self.force_close.clone();
            self.tasks.spawn(async move {
                let _stream_slot = stream_slot;
                let handled = state.clone().handle_stream(peer.clone(), request, response);
                tokio::select! {
                    _ = stream_force_close.cancelled() => {}
                    _ = transport_force_close.cancelled() => {}
                    result = handled => {
                        if let Err(error) = result {
                            debug!(?error, peer_addr = %peer.addr, class = ?peer.class, "HTTP/2 operation failed");
                        }
                    }
                }
            });
        }
        Ok(())
    }

    async fn handle_stream(
        self,
        peer: InboundPeer,
        request: Request<RecvStream>,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        if request.method() != Method::POST {
            send_static_error(
                &mut respond,
                StatusCode::METHOD_NOT_ALLOWED,
                "POST required",
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        let path = request.uri().path().to_string();
        if path == CONTROL_PATH {
            self.handle_control(
                peer.addr,
                peer.node_id,
                peer.advertised_host,
                peer.class,
                request.into_body(),
                respond,
            )
            .await?;
            return Ok(());
        }
        if path == ACK_PATH {
            if peer.class != PoolClass::Management {
                send_static_error(
                    &mut respond,
                    StatusCode::FORBIDDEN,
                    "wrong pool class",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            let bytes = read_body(
                &self.executor,
                MemoryClass::Management,
                peer.class.control_body_limit(&self.executor),
                self.options.progress_timeout,
                request.into_body(),
            )
            .await?;
            let decoded = wire::decode_rkyv::<nervix_models::RemoteAckResolution>(
                &self.executor,
                MemoryClass::Management,
                CpuClass::Control,
                bytes,
            )
            .await?;
            let (ack, reservation) = decoded.into_parts();
            let terminal_admission = if ack.outcome == RemoteAckOutcome::Alive {
                None
            } else {
                Some(RelayAdmissionKey {
                    peer_node_id: peer.node_id.clone(),
                    ack_id: ack.ack_id,
                })
            };
            if ack.outcome == RemoteAckOutcome::Alive {
                self.deliver_incoming(
                    peer.addr,
                    peer.node_id,
                    Envelope::Ack(ack),
                    Some(reservation),
                )?;
            } else {
                self.deliver_terminal_incoming(
                    peer.addr,
                    peer.node_id,
                    Envelope::Ack(ack),
                    Some(reservation),
                )
                .await?;
            }
            if let Some(admission_key) = terminal_admission {
                self.retire_outbound_relay_admission(&admission_key);
            }
            send_response(
                respond,
                StatusCode::NO_CONTENT,
                None,
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        if path == RELAY_CANCEL_PATH || path == RELAY_STATUS_PATH {
            if peer.class != PoolClass::Management {
                send_static_error(
                    &mut respond,
                    StatusCode::FORBIDDEN,
                    "wrong pool class",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            return self
                .handle_relay_admission_control(
                    peer.node_id,
                    peer.process_epoch,
                    path == RELAY_CANCEL_PATH,
                    request.into_body(),
                    respond,
                )
                .await;
        }
        if path == RELAY_GRANT_PATH {
            if peer.class != PoolClass::Management {
                send_static_error(
                    &mut respond,
                    StatusCode::FORBIDDEN,
                    "wrong pool class",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            return self
                .handle_relay_grant(
                    peer.node_id,
                    peer.process_epoch,
                    request.into_body(),
                    respond,
                )
                .await;
        }
        if let Some(grant_id) = path.strip_prefix(RELAY_PATH_PREFIX) {
            if peer.class != PoolClass::Relay {
                respond.send_reset(Reason::REFUSED_STREAM);
                return Ok(());
            }
            let grant_id = grant_id.parse::<u64>().map_err(|error| {
                TransportError::RelayGrant(format!("invalid grant id: {error}"))
            })?;
            self.handle_relay_body(
                peer.addr,
                peer.node_id,
                peer.process_epoch,
                grant_id,
                request,
                respond,
            )
            .await?;
            return Ok(());
        }

        send_static_error(
            &mut respond,
            StatusCode::NOT_FOUND,
            "unknown interconnect operation",
            self.options.progress_timeout,
        )
        .await?;
        Ok(())
    }

    async fn handle_control(
        &self,
        peer_addr: SocketAddr,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        class: PoolClass,
        body: RecvStream,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), TransportError> {
        let bytes = read_body(
            &self.executor,
            class.memory_class(),
            class.control_body_limit(&self.executor),
            self.options.progress_timeout,
            body,
        )
        .await?;
        let decoded = wire::decode_rkyv::<ControlEnvelope>(
            &self.executor,
            class.memory_class(),
            class.cpu_class(),
            bytes,
        )
        .await?;
        let (control, reservation) = decoded.into_parts();
        if control.pool_class() != class {
            send_response(
                respond,
                StatusCode::FORBIDDEN,
                None,
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }

        if let ControlEnvelope::Request(request) = control {
            let response = tokio::select! {
                response = self.requests.handle(
                    &self.executor,
                    peer_node_id,
                    peer_advertised_host,
                    request,
                ) => response,
                reset = poll_fn(|context| respond.poll_reset(context)) => {
                    reset?;
                    return Ok(());
                }
            };
            let (response, _payload_reservation) = response.into_parts();
            let response = wire::encode_rkyv(
                &self.executor,
                class.memory_class(),
                class.cpu_class(),
                class.control_body_limit(&self.executor),
                ControlEnvelope::Response(response),
            )
            .await?;
            send_response(
                respond,
                StatusCode::OK,
                Some(response),
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }

        if let ControlEnvelope::Response(_) = control {
            send_response(
                respond,
                StatusCode::BAD_REQUEST,
                None,
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }

        self.deliver_incoming(
            peer_addr,
            peer_node_id,
            Envelope::Control(control),
            Some(reservation),
        )?;
        send_response(
            respond,
            StatusCode::NO_CONTENT,
            None,
            self.options.progress_timeout,
        )
        .await
    }

    fn increment_peer(&self, node_id: &ClusterNodeName) -> Result<(), TransportError> {
        match self.peer_connections.entry(node_id.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().count = entry
                    .get()
                    .count
                    .checked_add(1)
                    .assured("a peer has a bounded number of inbound and outbound connections");
            }
            Entry::Vacant(entry) => {
                let permit = StdArc::clone(&self.peer_permits)
                    .try_acquire_owned()
                    .map_err(|_| TransportError::PoolExhausted)?;
                entry.insert(PeerConnections {
                    count: 1,
                    _permit: permit,
                });
            }
        }
        self.connection_changed.notify_waiters();
        Ok(())
    }

    fn register_inbound_pool(
        &self,
        node_id: ClusterNodeName,
        class: PoolClass,
    ) -> Result<InboundConnectionRegistration, TransportError> {
        let key = InboundPoolKey { node_id, class };
        match self.inbound_pool_connections.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                if *entry.get() >= class.connections_per_peer() {
                    return Err(TransportError::PoolExhausted);
                }
                *entry.get_mut() = entry
                    .get()
                    .checked_add(1)
                    .assured("an inbound class pool is capped by its declared slot count");
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        }
        if let Err(error) = self.increment_peer(&key.node_id) {
            self.decrement_inbound_pool(&key);
            return Err(error);
        }
        Ok(InboundConnectionRegistration {
            state: self.clone(),
            key,
        })
    }

    fn decrement_inbound_pool(&self, key: &InboundPoolKey) {
        if let Some(mut count) = self.inbound_pool_connections.get_mut(key) {
            if *count <= 1 {
                drop(count);
                self.inbound_pool_connections.remove(key);
            } else {
                *count = count
                    .checked_sub(1)
                    .verified("the branch above handled the final inbound class connection");
            }
        }
    }

    fn decrement_peer(&self, node_id: &ClusterNodeName) {
        if let Some(mut connections) = self.peer_connections.get_mut(node_id) {
            if connections.count <= 1 {
                drop(connections);
                self.peer_connections.remove(node_id);
            } else {
                connections.count = connections
                    .count
                    .checked_sub(1)
                    .verified("the branch above handled the final peer connection");
            }
        }
        self.connection_changed.notify_waiters();
    }

    pub(crate) async fn replace_tls(&self, tls: TlsConfigBundle) -> Result<(), TransportError> {
        tls.certificate
            .validate_local(&self.cluster_id, &self.node_id, &self.advertised_host)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        ensure_current(&tls.certificate)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        let next_generation = self.tls.read().generation.checked_add(1).ok_or_else(|| {
            TransportError::InvalidOptions {
                reason: "TLS configuration generation is exhausted".to_string(),
            }
        })?;
        self.cancel_all_slots();
        *self.tls.write() = ActiveTls {
            generation: next_generation,
            bundle: tls,
        };
        self.tls_changed.notify_waiters();
        let peers = self
            .targets
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        for (node, target) in peers {
            self.ensure_preconnected_slots(&node, &target);
        }
        Ok(())
    }

    fn cancel_all_slots(&self) {
        let slots = self
            .slots
            .iter()
            .map(|entry| (entry.key().clone(), entry.cancel.clone()))
            .collect::<Vec<_>>();
        for (key, cancel) in slots {
            self.retire_slot(&key, &cancel);
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.admission_closed.cancel();
        self.requests.shutdown();
        self.cancel_all_slots();
        self.grants.clear();
        self.relay_attempts.clear();
        self.active_relay_channels.clear();
        self.relay_admissions.clear();
        self.relay_watermarks.clear();
        self.outbound_relay_epochs.clear();
        self.outbound_relay_admissions.clear();
        self.tasks.close();
        if timeout(self.options.shutdown_drain_timeout, self.tasks.wait())
            .await
            .is_err()
        {
            self.force_close.cancel();
            self.tasks.wait().await;
        }
    }
}

impl ClientConnection {
    async fn request_raw(
        &self,
        state: &TransportState,
        request: RawRequest<'_>,
    ) -> Result<ChargedBytes, TransportError> {
        if self.closed.is_cancelled() {
            return Err(TransportError::Closed(self.key.target.addr));
        }
        let RawRequest {
            path,
            body,
            response_class,
            response_limit,
            timeout: timeout_duration,
            headers,
        } = request;
        let operation = async {
            let sender = self.sender.clone().ready().await?;
            let mut request_url = url::Url::parse("https://localhost/")
                .assured("the fixed HTTPS request base is a valid URL");
            request_url
                .set_host(Some(&self.key.target.server_name))
                .map_err(|_| {
                    TransportError::InvalidServerName(self.key.target.server_name.clone())
                })?;
            request_url.set_path(path);
            let mut builder = Request::builder()
                .method(Method::POST)
                .version(Version::HTTP_2)
                .uri(request_url.as_str());
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            let request = builder
                .body(())
                .map_err(|error| TransportError::Http(error.to_string()))?;
            let end_stream = body.as_ref().is_none_or(ChargedBytes::is_empty);
            let (response, mut stream) = {
                let mut sender = sender;
                sender.send_request(request, end_stream)?
            };
            if let Some(body) = body
                && !body.is_empty()
            {
                send_body(&mut stream, body).await?;
            }
            let response = response.await?;
            let status = response.status();
            let response = read_body(
                &state.executor,
                response_class.memory_class(),
                response_limit,
                state.options.progress_timeout,
                response.into_body(),
            )
            .await?;
            if !status.is_success() {
                return Err(TransportError::RemoteRejected {
                    status: status.as_u16(),
                    message: String::from_utf8_lossy(response.as_ref()).into_owned(),
                });
            }
            Ok(response)
        };
        match timeout(timeout_duration, operation).await {
            Ok(result) => result,
            Err(_) => Err(TransportError::RequestTimeout {
                peer: self.key.node_id.clone(),
                timeout: timeout_duration,
            }),
        }
    }
}

impl StreamLease {
    async fn request_raw(
        &self,
        state: &TransportState,
        request: RawRequest<'_>,
    ) -> Result<ChargedBytes, TransportError> {
        self.connection.request_raw(state, request).await
    }
}

fn configure_client_builder(
    builder: &mut client::Builder,
    options: &TransportOptions,
    class: PoolClass,
) -> Result<(), TransportError> {
    let stream_slots = class.stream_slots_per_connection();
    let streams = u32::try_from(stream_slots).map_err(|_| TransportError::InvalidOptions {
        reason: "pool stream slots exceed the HTTP/2 setting width".to_string(),
    })?;
    builder
        .initial_window_size(options.initial_stream_window_bytes)
        .initial_connection_window_size(options.initial_connection_window_bytes)
        .max_header_list_size(options.max_header_bytes)
        .max_concurrent_streams(streams)
        .initial_max_send_streams(stream_slots)
        .max_local_error_reset_streams(Some(RESET_LIMIT))
        .max_pending_accept_reset_streams(RESET_LIMIT)
        .max_send_buffer_size(BODY_CHUNK_BYTES);
    Ok(())
}

fn configure_server_builder(
    builder: &mut server::Builder,
    options: &TransportOptions,
) -> Result<(), TransportError> {
    let streams =
        u32::try_from(PoolClass::Management.stream_slots_per_connection()).map_err(|_| {
            TransportError::InvalidOptions {
                reason: "pool stream slots exceed the HTTP/2 setting width".to_string(),
            }
        })?;
    builder
        .initial_window_size(options.initial_stream_window_bytes)
        .initial_connection_window_size(options.initial_connection_window_bytes)
        .max_header_list_size(options.max_header_bytes)
        .max_concurrent_streams(streams)
        .max_local_error_reset_streams(Some(RESET_LIMIT))
        .max_pending_accept_reset_streams(RESET_LIMIT)
        .max_send_buffer_size(BODY_CHUNK_BYTES);
    Ok(())
}

async fn send_body(
    stream: &mut SendStream<Bytes>,
    body: ChargedBytes,
) -> Result<(), TransportError> {
    let mut offset = 0;
    while offset < body.len() {
        tokio::task::consume_budget().await;
        let remaining = body
            .len()
            .checked_sub(offset)
            .verified("the send offset never advances beyond the body");
        let wanted = remaining.min(BODY_CHUNK_BYTES);
        stream.reserve_capacity(wanted);
        let assigned = poll_fn(|context| stream.poll_capacity(context))
            .await
            .ok_or_else(|| {
                TransportError::Decode(
                    "HTTP/2 stream closed while assigning send capacity".to_string(),
                )
            })??;
        let ready = assigned.min(wanted);
        if ready == 0 {
            continue;
        }
        let end = offset
            .checked_add(ready)
            .verified("assigned capacity is bounded by the remaining body");
        let chunk = body
            .slice(offset, end)
            .verified("the chunk bounds were checked against the body");
        offset = end;
        let end_stream = offset == body.len();
        stream.send_data(Bytes::from_owner(chunk), end_stream)?;
        if end_stream {
            break;
        }
    }
    stream.reserve_capacity(0);
    Ok(())
}

async fn send_response(
    mut respond: server::SendResponse<Bytes>,
    status: StatusCode,
    body: Option<ChargedBytes>,
    progress_timeout: Duration,
) -> Result<(), TransportError> {
    let response = Response::builder()
        .status(status)
        .version(Version::HTTP_2)
        .body(())
        .map_err(|error| TransportError::Http(error.to_string()))?;
    let end_stream = body.as_ref().is_none_or(ChargedBytes::is_empty);
    let mut stream = respond.send_response(response, end_stream)?;
    if let Some(body) = body
        && !body.is_empty()
    {
        timeout(progress_timeout, send_body(&mut stream, body))
            .await
            .map_err(|_| TransportError::ProgressTimeout {
                timeout: progress_timeout,
            })??;
    }
    Ok(())
}

async fn send_static_error(
    respond: &mut server::SendResponse<Bytes>,
    status: StatusCode,
    message: &'static str,
    progress_timeout: Duration,
) -> Result<(), TransportError> {
    let response = Response::builder()
        .status(status)
        .version(Version::HTTP_2)
        .body(())
        .map_err(|error| TransportError::Http(error.to_string()))?;
    let mut stream = respond.send_response(response, false)?;
    timeout(progress_timeout, async {
        let body = Bytes::from_static(message.as_bytes());
        let mut offset = 0;
        while offset < body.len() {
            tokio::task::consume_budget().await;
            let remaining = body
                .len()
                .checked_sub(offset)
                .verified("the send offset never advances beyond the static error body");
            let wanted = remaining.min(BODY_CHUNK_BYTES);
            stream.reserve_capacity(wanted);
            let assigned = poll_fn(|context| stream.poll_capacity(context))
                .await
                .ok_or_else(|| {
                    TransportError::Decode(
                        "HTTP/2 stream closed while assigning send capacity".to_string(),
                    )
                })??;
            let ready = assigned.min(wanted);
            if ready == 0 {
                continue;
            }
            let end = offset
                .checked_add(ready)
                .verified("assigned capacity is bounded by the remaining static error body");
            let chunk = body.slice(offset..end);
            offset = end;
            let end_stream = offset == body.len();
            stream.send_data(chunk, end_stream)?;
            if end_stream {
                break;
            }
        }
        stream.reserve_capacity(0);
        Ok::<(), TransportError>(())
    })
    .await
    .map_err(|_| TransportError::ProgressTimeout {
        timeout: progress_timeout,
    })?
}

async fn read_body(
    executor: &Executor,
    class: MemoryClass,
    limit: u64,
    progress_timeout: Duration,
    body: RecvStream,
) -> Result<ChargedBytes, TransportError> {
    let initial = limit.min(4 * 1024);
    let reservation = executor
        .reserve(class, initial)
        .await
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    let mut buffer = BudgetedBuffer::with_limit(reservation, limit);
    read_body_into(&mut buffer, progress_timeout, body).await?;
    Ok(ChargedBytes::from_buffer(buffer))
}

async fn read_body_into(
    buffer: &mut BudgetedBuffer,
    progress_timeout: Duration,
    mut body: RecvStream,
) -> Result<(), TransportError> {
    loop {
        tokio::task::consume_budget().await;
        let chunk = timeout(progress_timeout, body.data()).await.map_err(|_| {
            TransportError::ProgressTimeout {
                timeout: progress_timeout,
            }
        })?;
        let Some(chunk) = chunk else {
            break;
        };
        let chunk = chunk?;
        buffer
            .write_all(&chunk)
            .map_err(|error| TransportError::Decode(error.to_string()))?;
        body.flow_control().release_capacity(chunk.len())?;
    }
    Ok(())
}

fn validate_tls_session(
    alpn: Option<&[u8]>,
    certificates: Option<&[rustls::pki_types::CertificateDer<'static>]>,
    cluster_id: &str,
    expected_node: Option<&ClusterNodeName>,
) -> Result<CertificateIdentity, TransportError> {
    if alpn != Some(b"h2".as_slice()) {
        return Err(TransportError::InvalidHandshake(
            "TLS did not negotiate ALPN h2".to_string(),
        ));
    }
    let Some(certificates) = certificates else {
        return Err(TransportError::InvalidHandshake(
            "peer did not present a certificate".to_string(),
        ));
    };
    let Some(certificate) = certificates.first() else {
        return Err(TransportError::InvalidHandshake(
            "peer did not present a certificate".to_string(),
        ));
    };
    let identity = CertificateIdentity::from_certificate(certificate)
        .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
    ensure_current(&identity)
        .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
    if identity.cluster_id != cluster_id {
        return Err(TransportError::InvalidHandshake(format!(
            "peer certificate identifies cluster '{}', expected '{}'",
            identity.cluster_id, cluster_id
        )));
    }
    if let Some(expected_node) = expected_node
        && &identity.node_id != expected_node
    {
        return Err(TransportError::InvalidHandshake(format!(
            "peer certificate identifies node '{}', expected '{}'",
            identity.node_id, expected_node
        )));
    }
    Ok(identity)
}

fn certificate_expiration_deadline<'a>(
    identities: impl IntoIterator<Item = &'a CertificateIdentity>,
) -> Result<Instant, TransportError> {
    let expires_at = identities
        .into_iter()
        .map(|identity| identity.not_after_unix_seconds)
        .min()
        .ok_or_else(|| {
            TransportError::InvalidHandshake(
                "a connection has no certificate expiration".to_string(),
            )
        })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
    let now = i64::try_from(now.as_secs())
        .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
    let remaining = expires_at
        .checked_sub(now)
        .ok_or_else(|| TransportError::InvalidHandshake("certificate has expired".to_string()))?;
    if remaining <= 0 {
        return Err(TransportError::InvalidHandshake(
            "certificate has expired".to_string(),
        ));
    }
    let remaining = u64::try_from(remaining)
        .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
    Instant::now()
        .checked_add(Duration::from_secs(remaining))
        .ok_or_else(|| {
            TransportError::InvalidHandshake(
                "certificate expiration exceeds the monotonic clock range".to_string(),
            )
        })
}

fn ensure_current(identity: &CertificateIdentity) -> Result<(), super::TlsConfigError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| super::TlsConfigError::InvalidCertificate(error.to_string()))?;
    let now = i64::try_from(now.as_secs())
        .map_err(|error| super::TlsConfigError::InvalidCertificate(error.to_string()))?;
    if identity.not_before_unix_seconds > now {
        return Err(super::TlsConfigError::NotYetValid);
    }
    if identity.not_after_unix_seconds <= now {
        return Err(super::TlsConfigError::Expired);
    }
    Ok(())
}

fn header_u64(request: &Request<RecvStream>, name: &'static str) -> Result<u64, TransportError> {
    let value = request
        .headers()
        .get(name)
        .ok_or_else(|| TransportError::RelayGrant(format!("missing {name} header")))?;
    let value = value
        .to_str()
        .map_err(|error| TransportError::RelayGrant(error.to_string()))?;
    value
        .parse()
        .map_err(|error| TransportError::RelayGrant(format!("invalid {name} header: {error}")))
}
