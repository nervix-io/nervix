//! HTTP/2 connection pools and stream-level operation dispatch.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** TLS/H2 connection lifetime, pool isolation, stream leases, flow control, and relay
//!   grant, admission, reconciliation, and cancellation handling.
//! - **Depends on.** Certificate identity, bounded rkyv codecs, and execution admission.
//! - **Must not know.** Runtime graphs, scheduling decisions, or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "connection and peer registration install or retire concrete transport \
                  lifetimes; recurring transport operations override this default"
    )
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    future::poll_fn,
    hash::RandomState,
    net::SocketAddr,
    ops::Deref,
    time::Duration,
};

use bytes::Bytes;
use error_stack::Report;
use futures_util::stream::FuturesUnordered;
use h2::{Ping, PingPong, Reason, RecvStream, client, server};
use http::{Method, Request, StatusCode, Version};
use imbl::{GenericHashMap, shared_ptr::DefaultSharedPtr};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_dns::ConnectionBudget;
use nervix_execution::{
    BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass, Reservation,
};
use nervix_models::{
    ClusterNodeName, CoordinationIdentity, NodeEndpoint, RemoteAckOutcome, RemoteAckRegistration,
};
use nervix_primitives::{
    collections::{DashMap, dash_map::Entry},
    net::{TcpListener, TcpStream},
    publication::{ArcSwap, ArcSwapOption},
    sync::{
        Arc, CancellationToken, Notify, OwnedSemaphorePermit, Semaphore, StdArc, StdWeak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    task::TaskTracker,
    time::{Instant, sleep, sleep_until, timeout},
};
use strum::EnumCount as _;
use tracing::{debug, warn};

use self::{
    published_tls::{ActiveTls, PublishedTls},
    stream_releases::StreamReleases,
};
use super::{
    ControlEnvelope, CoordinationIdentityAllocationError, Envelope, PeerTarget, PoolClass,
    RELAY_GRANT_LIFETIME, ReceivedEnvelope, RelayAdmissionDecision, RelayAdmissionStatus,
    RelayDelivery, RelayPayload, RequestSubquota, TlsConfigBundle, TransportError,
    TransportIdentity, TransportOptions, wire,
};
use crate::{
    identity::CertificateIdentity,
    observation::{
        ConnectionDirection, ConnectionFailureReason, RelayAdmissionOutcome, StreamResetReason,
        TransportObservations, TransportSnapshot,
    },
    peer_resolver::PeerResolver,
    request::RequestAdmission,
    wire::{
        ConnectionAccepted, ConnectionHello, RelayAdmissionRequest, RelayAdmissionResponse,
        RelayGrantDisposition, RelayGrantRequest, RelayGrantResponse, WIRE_CONTRACT_FINGERPRINT,
    },
};

mod body;
mod dial;
mod duplex;
mod published_tls;
mod relay;
mod relay_admission_choice;
mod relay_owner;
use relay_admission_choice::{AdmissionChoice, ChosenAdmission};
mod stream;
mod stream_releases;
pub(crate) mod stream_slots;
mod targets;

use body::{read_body, read_body_into, send_body, send_response, send_static_error};
use dial::{DialedStream, OutboundDial};
pub(crate) use duplex::FrameReader;
pub use duplex::{
    ChargedItem, DuplexItems, DuplexReceiver, DuplexResponses, DuplexSendProgress, DuplexSender,
};
use relay_owner::{RelayOwnersPublication, RelayPeerOwner};
pub use stream::IncomingByteStream;
pub(crate) use stream::OutboundByteStreamRequest;
use stream_slots::{StreamSlotQuotas, configure_client_builder, configure_server_builder};

const CONNECT_PATH: &str = "/v1/connect";
const CONTROL_PATH: &str = "/v1/control";
const STREAM_PATH: &str = "/v1/stream";
const DUPLEX_PATH: &str = "/v1/duplex";
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
/// A blackholed established TCP session otherwise stays in the pool until the kernel abandons its
/// retransmissions, which can outlast the cluster's partition-heal convergence deadline.
const HTTP2_PING_INTERVAL: Duration = Duration::from_secs(15);
const HTTP2_PING_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct ConnectionSlotKey {
    node_id: ClusterNodeName,
    endpoint: NodeEndpoint,
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
    relay_owner: StdArc<RelayPeerOwner>,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "one concrete pool slot claims its own worker and publishes authenticated \
                  connections",
        key = "endpoint, pool class, slot and cancellation lifetime",
        bound = "one worker claim per lifetime and one current connection publication"
    )
)]
struct SlotControl {
    key: ConnectionSlotKey,
    cancel: CancellationToken,
    started: AtomicBool,
    /// The slot worker publishes each authenticated connection here. A stream lease retains that
    /// exact connection, including its cancellation and peer incarnation, after the load ends.
    connection: ArcSwapOption<ClientConnection>,
}

impl SlotControl {
    fn claim_worker(&self) -> bool {
        if self.cancel.is_cancelled() || self.started.load(Ordering::Relaxed) {
            return false;
        }
        self.started
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "this retained transport value services relay frames and their terminal outcomes"
    )
)]
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

/// One published outbound endpoint of a peer, retaining every connection slot it can hold.
///
/// The slot handles are built once when the endpoint is registered. Leasing a stream reads their
/// connection publications without acquiring a discovery map. Slot identities name the
/// endpoint, not an address, so a new DNS answer for the same endpoint changes only where the next
/// connection is dialled and never retires a connection that is already established.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "this retained transport value services relay frames and their terminal outcomes"
    )
)]
struct OutboundTarget {
    endpoint: NodeEndpoint,
    dial: OutboundDial,
    pool: Arc<OutboundPool>,
}

type OutboundTargets =
    GenericHashMap<ClusterNodeName, Arc<OutboundTarget>, RandomState, DefaultSharedPtr>;

impl OutboundTarget {
    fn new(node_id: &ClusterNodeName, endpoint: NodeEndpoint, dial: OutboundDial) -> Self {
        let slots = PoolClass::ALL.map(|class| Self::class_slots(node_id, &endpoint, class));
        Self {
            endpoint,
            dial,
            pool: Arc::new(OutboundPool {
                slots,
                releases: StreamReleases::new(),
            }),
        }
    }

    /// The same endpoint and slots, dialled through `dial`.
    fn with_dial(&self, dial: OutboundDial) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            dial,
            pool: Arc::clone(&self.pool),
        }
    }

    fn class_slots(
        node_id: &ClusterNodeName,
        endpoint: &NodeEndpoint,
        class: PoolClass,
    ) -> Box<[Arc<SlotControl>]> {
        let mut keys = Vec::with_capacity(class.connections_per_peer());
        for slot in 0..class.connections_per_peer() {
            keys.push(Arc::new(SlotControl {
                key: ConnectionSlotKey {
                    node_id: node_id.clone(),
                    endpoint: endpoint.clone(),
                    class,
                    slot,
                },
                cancel: CancellationToken::new(),
                started: AtomicBool::new(false),
                connection: ArcSwapOption::empty(),
            }));
        }
        keys.into_boxed_slice()
    }

    /// The retained connection slots in `class`, in slot order.
    fn slots(&self, class: PoolClass) -> &[Arc<SlotControl>] {
        &self.pool.slots[class.index()]
    }
}

/// What every dial of one registered endpoint shares: its connection slots, and the wakeups of the
/// operations waiting for one of their streams. The same endpoint dialled another way keeps both,
/// so a waiter of the earlier target still hears a later release.
struct OutboundPool {
    slots: [Box<[Arc<SlotControl>]>; PoolClass::COUNT],
    releases: StreamReleases,
}

struct ClientConnection {
    key: ConnectionSlotKey,
    /// The address this connection reached, one of the answers its endpoint resolved to.
    peer_addr: SocketAddr,
    /// The endpoint's host as the authority of every request on this connection writes it.
    request_host: String,
    sender: client::SendRequest<Bytes>,
    stream_slots: StreamSlotQuotas,
    peer_epoch: u64,
    /// The authenticated connection retains the peer's protocol owner for all of its frames.
    relay_owner: Option<StdArc<RelayPeerOwner>>,
    retiring: CancellationToken,
    cancel: CancellationToken,
    closed: CancellationToken,
}

/// Add one bounded count to a running total of connections, streams or slots. Every operand is
/// bounded by a configured limit that is itself far inside `usize`.
fn increment(total: usize, addition: usize) -> usize {
    total
        .checked_add(addition)
        .assured("configured connection, stream and slot limits are far inside usize")
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "this retained transport value services relay frames and their terminal outcomes"
    )
)]
pub(crate) struct StreamLease {
    connection: StdArc<ClientConnection>,
    slot: Option<OwnedSemaphorePermit>,
    state: TransportState,
    released: StreamRelease,
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        drop(self.slot.take());
        let StreamRelease {
            pool,
            class,
            subquota,
        } = &self.released;
        pool.releases.of(*class, *subquota).notify_one();
    }
}

/// Where a stream lease reports its release: the pool it leased from, and its class and subquota.
struct StreamRelease {
    pool: Arc<OutboundPool>,
    class: PoolClass,
    subquota: RequestSubquota,
}

/// What one attempt to lease a stream found.
enum StreamLeaseAttempt {
    Leased(StreamLease),
    /// Every stream the peer has in the class and subquota is leased; a release wakes a waiter
    /// through the pool's releases.
    Busy(Arc<OutboundPool>),
    /// The peer has no target to lease from.
    NoTarget,
}

struct RawRequest<'a> {
    path: &'a str,
    body: Option<ChargedBytes>,
    response_class: PoolClass,
    response_limit: u64,
    timeout: Duration,
    headers: &'a [(&'a str, &'a str)],
}

pub(crate) struct RawDuplexRequest {
    pub(crate) class: PoolClass,
    pub(crate) subquota: RequestSubquota,
    pub(crate) body: ChargedBytes,
    pub(crate) timeout: Duration,
    pub(crate) admission: RequestAdmission,
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

struct InboundBinding {
    peer_addr: SocketAddr,
    peer_identity: CertificateIdentity,
    handshake_permit: OwnedSemaphorePermit,
    binding_sequence: u64,
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
    /// The whole registration, whose registrar run keeps an admission that an earlier run of the
    /// same node numbered alike from naming this one.
    registration: RemoteAckRegistration,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct OutboundRelayKey {
    peer_node_id: ClusterNodeName,
    delivery: RelayDelivery,
}

/// One delivery attempt: a position in the receiver's view of a sender's relay channel.
///
/// The attempt holds its channel's key, so the channel bookkeeping an attempt touches never
/// rebuilds that key.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "this retained transport value services relay frames and their terminal outcomes"
    )
)]
struct RelayAttemptKey {
    channel: RelayChannelKey,
    sequence: u64,
}

impl RelayAttemptKey {
    fn delivery(&self) -> RelayDelivery {
        RelayDelivery {
            channel_incarnation: self.channel.channel_incarnation,
            sequence: self.sequence,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct RelayChannelKey {
    peer_node_id: ClusterNodeName,
    sender_epoch: u64,
    receiver_epoch: u64,
    channel_incarnation: [u8; 16],
}

#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "this retained transport value services relay frames and their terminal outcomes"
    )
)]
struct RelayChannelWatermark {
    sequence: u64,
    status: RelayAdmissionStatus,
    last_reconciled: Instant,
}

impl RelayChannelWatermark {
    fn new(sequence: u64, status: RelayAdmissionStatus) -> Self {
        Self {
            sequence,
            status,
            last_reconciled: Instant::now(),
        }
    }

    fn mark_reconciled(&mut self) {
        self.last_reconciled = Instant::now();
    }

    fn last_reconciled_at(&self) -> Instant {
        self.last_reconciled
    }
}

enum RelayGrantRegistration {
    Registered,
    Existing(StdArc<RelayAdmissionRecord>),
    Retired(RelayGrantDisposition),
    InvalidSequence,
    ChannelBusy,
    AdmissionBusy,
}

#[derive(Debug, Clone)]
enum RelayBodyPhase {
    Reserved { grant_id: u64 },
    BodyReceived,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        key = "one inbound admission record",
        bound = "one terminal transition per reserved grant and retained admission permit",
        reason = "admission records own the exact grant and its terminal resolution"
    )
)]
struct RelayAdmissionRecord {
    attempt: RelayAttemptKey,
    admission_key: RelayAdmissionKey,
    body_bytes: u64,
    metadata: wire::RelayMetadata,
    /// Decoded metadata stays allocated until the last intake or protocol owner releases it.
    _metadata_memory: Reservation,
    choice: AdmissionChoice,
    state: nervix_primitives::sync::blocking::Mutex<RelayAdmissionProtocol>,
    cancellation: CancellationToken,
    /// When the receiver accepted this attempt's reservation. Progress and admission latency are
    /// both measured from here, because that is when the sender's wait begins.
    reserved_at: Instant,
    /// Also held by the transport, which outlives every attempt it reserved. The record resolves
    /// long after the call that created it returned, so it records its own outcome.
    observations: Arc<TransportObservations>,
    /// Records borrow the transport owner weakly; the owner retains unresolved records.
    owner: StdWeak<RelayPeerOwner>,
}

struct RelayAdmissionProtocol {
    phase: RelayBodyPhase,
    rejection: Option<String>,
    capacity: Option<RelayAdmissionCapacity>,
    last_progress: Instant,
}

struct RelayAdmissionCapacity {
    _item: OwnedSemaphorePermit,
    _terminal: OwnedSemaphorePermit,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "this retained transport value services relay frames and their terminal outcomes"
    )
)]
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
#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "the intake retains its exact grant record through the terminal transition",
        key = "one admitted relay grant",
        bound = "one synchronous status transition under its retained record; no guard crosses \
                 await"
    )
)]
pub struct RelayAdmission {
    record: StdArc<RelayAdmissionRecord>,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport services each admitted body and resolves its exact cancellation \
                  guard"
    )
)]
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
            .field("delivery", &self.record.attempt.delivery())
            .finish_non_exhaustive()
    }
}

impl RelayAdmission {
    /// Resolves once this attempt can no longer be admitted: its sender cancelled it, its peer
    /// ended or changed process, or the transport rejected it. An application that holds the
    /// received batch back before it decides admission stops holding it then; [`Self::admit`]
    /// reports the same verdict.
    pub async fn cancelled(&self) {
        self.record.cancellation.cancelled().await;
    }

    pub fn admit(&self) -> RelayAdmissionDecision {
        self.record.mark_admitted();
        match self.record.choice.current() {
            ChosenAdmission::Admitted => RelayAdmissionDecision::Admitted,
            ChosenAdmission::Cancelled => RelayAdmissionDecision::Cancelled,
            ChosenAdmission::Pending => {
                unreachable!("admission selected or observed an irreversible verdict")
            }
        }
    }
}

impl RelayAdmissionRecord {
    fn release_capacity(&self) {
        self.state.lock().capacity.take();
    }
    fn report_progress(&self) {
        self.state.lock().last_progress = Instant::now();
    }
    fn last_progress(&self) -> Instant {
        self.state.lock().last_progress
    }

    fn is_unadmitted(&self) -> bool {
        self.choice.current() == ChosenAdmission::Pending
    }

    fn progress_registration(&self) -> Option<RemoteAckRegistration> {
        if self.is_unadmitted() {
            self.metadata.admission.clone()
        } else {
            None
        }
    }

    fn reserved_grant_id(&self) -> Option<u64> {
        let state = self.state.lock();
        if !self.is_unadmitted() {
            return None;
        }
        match state.phase {
            RelayBodyPhase::Reserved { grant_id } => Some(grant_id),
            RelayBodyPhase::BodyReceived => None,
        }
    }

    fn status_under_guard(&self, state: &RelayAdmissionProtocol) -> RelayAdmissionStatus {
        match self.choice.current() {
            ChosenAdmission::Pending => match state.phase {
                RelayBodyPhase::Reserved { .. } => RelayAdmissionStatus::Reserved,
                RelayBodyPhase::BodyReceived => RelayAdmissionStatus::BodyReceived,
            },
            ChosenAdmission::Admitted => RelayAdmissionStatus::Admitted,
            ChosenAdmission::Cancelled => match &state.rejection {
                Some(reason) => RelayAdmissionStatus::Rejected(reason.clone()),
                None => RelayAdmissionStatus::Cancelled,
            },
        }
    }

    fn status(&self) -> RelayAdmissionStatus {
        self.status_under_guard(&self.state.lock())
    }

    fn grant_disposition(&self) -> RelayGrantDisposition {
        let state = self.state.lock();
        match self.status_under_guard(&state) {
            RelayAdmissionStatus::Reserved => {
                let RelayBodyPhase::Reserved { grant_id } = state.phase else {
                    unreachable!("the guarded body phase is reserved");
                };
                RelayGrantDisposition::SendBody { grant_id }
            }
            RelayAdmissionStatus::BodyReceived => RelayGrantDisposition::BodyReceived,
            RelayAdmissionStatus::Admitted => RelayGrantDisposition::Admitted,
            RelayAdmissionStatus::Rejected(reason) => RelayGrantDisposition::Rejected(reason),
            RelayAdmissionStatus::Cancelled => RelayGrantDisposition::Cancelled,
            RelayAdmissionStatus::Retired
            | RelayAdmissionStatus::Unknown
            | RelayAdmissionStatus::Indeterminate => {
                unreachable!("a live record has a concrete admission phase or verdict")
            }
        }
    }

    fn mark_body_received(&self) -> bool {
        let mut state = self.state.lock();
        if self.is_unadmitted() && matches!(state.phase, RelayBodyPhase::Reserved { .. }) {
            state.phase = RelayBodyPhase::BodyReceived;
            true
        } else {
            false
        }
    }

    fn mark_admitted(&self) {
        if self.choice.admit() {
            self.observe(RelayAdmissionOutcome::Admitted);
        }
    }

    fn reject(&self, reason: String) {
        let mut state = self.state.lock();
        if self.choice.cancel() {
            state.rejection = Some(reason);
            self.observe(RelayAdmissionOutcome::Rejected);
            self.cancellation.cancel();
        }
    }

    fn cancel(&self) -> RelayAdmissionStatus {
        let state = self.state.lock();
        if self.choice.cancel() {
            self.observe(RelayAdmissionOutcome::Cancelled);
            self.cancellation.cancel();
        }
        self.status_under_guard(&state)
    }

    fn observe(&self, outcome: RelayAdmissionOutcome) {
        self.observations
            .relay_resolved(outcome, self.reserved_at.elapsed());
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
    next_coordination_sequence: AtomicU64,
    next_binding_sequence: AtomicU64,
    local_addr: SocketAddr,
    resolver: PeerResolver,
    tls: PublishedTls,
    tls_changed: Notify,
    targets: ArcSwap<OutboundTargets>,
    connections: DashMap<ConnectionSlotKey, StdArc<ClientConnection>, RandomState>,
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
    /// Immutable routing only. Connections and attempts retain their peer owner before recurring
    /// protocol work, and only peer installation and withdrawal replace this publication.
    relay_owners: ArcSwap<RelayOwnersPublication>,
    relay_items: StdArc<Semaphore>,
    terminal_outcomes: StdArc<Semaphore>,
    admission_closed: CancellationToken,
    force_close: CancellationToken,
    tasks: TaskTracker,
    observations: Arc<TransportObservations>,
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
        identity: TransportIdentity,
        tls: TlsConfigBundle,
        options: TransportOptions,
        executor: Executor,
        resolver: PeerResolver,
    ) -> Result<(Self, mpsc::Receiver<ReceivedEnvelope>), Report<TransportError>> {
        let TransportIdentity {
            cluster_id,
            node_id,
            advertised_host,
        } = identity;
        options.validate()?;
        tls.certificate
            .validate_local(&cluster_id, &node_id, &advertised_host)
            .map_err(|error| TransportError::with_cause(error, TransportError::InvalidHandshake))?;
        tls.clock
            .ensure_current(&tls.certificate)
            .map_err(|error| TransportError::with_cause(error, TransportError::InvalidHandshake))?;

        let listener = TcpListener::bind(listen_addr)
            .await
            .map_err(TransportError::from)?;
        let local_addr = listener.local_addr().map_err(TransportError::from)?;
        let (incoming_tx, incoming_rx) = mpsc::channel(options.incoming_queue_capacity);
        let process_epoch = options.entropy.next_u64();
        let management_connection_reserve = options
            .max_peers
            .checked_mul(2)
            .verified("transport options validated the inbound and outbound management reserve");
        let preconnected_connection_reserve = management_connection_reserve
            .checked_mul(PoolClass::preconnected_connections_per_peer())
            .verified("transport options validated every inbound and outbound preconnected slot");
        let observations = Arc::new(TransportObservations::default());
        let state = Self {
            inner: Arc::new(TransportStateInner {
                executor,
                cluster_id,
                node_id,
                advertised_host,
                process_epoch,
                next_coordination_sequence: AtomicU64::new(1),
                next_binding_sequence: AtomicU64::new(1),
                local_addr,
                resolver,
                tls: PublishedTls::new(tls),
                tls_changed: Notify::new(),
                targets: ArcSwap::from_pointee(OutboundTargets::default()),
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
                requests: super::RequestState::new(
                    options.incoming_queue_capacity,
                    Arc::clone(&observations),
                ),
                relay_owners: ArcSwap::from_pointee(RelayOwnersPublication::default()),
                relay_items: StdArc::new(Semaphore::new(options.incoming_queue_capacity)),
                terminal_outcomes: StdArc::new(Semaphore::new(options.incoming_queue_capacity)),
                admission_closed: CancellationToken::new(),
                force_close: CancellationToken::new(),
                tasks: TaskTracker::new(),
                observations: Arc::clone(&observations),
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

    pub(crate) fn resolver(&self) -> &PeerResolver {
        &self.resolver
    }

    pub(crate) fn connection_setup_timeout(&self) -> Duration {
        self.options.connection_setup_timeout
    }

    pub(crate) fn node_id(&self) -> &ClusterNodeName {
        &self.node_id
    }

    #[allow(deprecated)] // until try_update is stabilized
    pub(crate) fn next_coordination_identity(
        &self,
    ) -> Result<CoordinationIdentity, Report<CoordinationIdentityAllocationError>> {
        #[allow(deprecated)] // until try_update is stabilized
        let sequence = self
            .next_coordination_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Report::new(CoordinationIdentityAllocationError))?;
        Ok(CoordinationIdentity::new(
            self.node_id.clone(),
            self.process_epoch,
            sequence,
        ))
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

    /// Read every level the transport holds, then its counters, so a scrape reports one node's
    /// pools and the work that produced them together.
    ///
    /// The walks here are bounded by the configured connection cap and the unresolved relay
    /// channel and attempt limits, which is what makes them a scrape-time read rather than a scan.
    pub(crate) fn snapshot(&self) -> TransportSnapshot {
        let mut connections = [[0_usize; PoolClass::COUNT]; ConnectionDirection::COUNT];
        let mut leased_streams = [0_usize; PoolClass::COUNT];
        let outbound = ConnectionDirection::Outbound.index();
        for entry in self.connections.iter() {
            let class = entry.key().class;
            connections[outbound][class.index()] =
                increment(connections[outbound][class.index()], 1);
            leased_streams[class.index()] = increment(
                leased_streams[class.index()],
                entry.value().stream_slots.leased(class),
            );
        }
        let inbound = ConnectionDirection::Inbound.index();
        for entry in self.inbound_pool_connections.iter() {
            let class = entry.key().class;
            connections[inbound][class.index()] =
                increment(connections[inbound][class.index()], *entry.value());
        }

        let mut oldest_unresolved_outcome = Duration::ZERO;
        let mut relay_channels = 0;
        let mut relay_attempts = 0;
        let mut relay_grants = 0;
        for owner in self.relay_owners.load().owners.values() {
            let snapshot = owner.snapshot();
            relay_channels = increment(relay_channels, snapshot.channels);
            relay_attempts = increment(relay_attempts, snapshot.attempts);
            relay_grants = increment(relay_grants, snapshot.grants);
            oldest_unresolved_outcome = oldest_unresolved_outcome.max(snapshot.oldest);
        }

        TransportSnapshot {
            counters: self.observations.counters(),
            connections,
            leased_streams,
            pending_operations: self.requests.pending_operations(),
            relay_channels,
            relay_attempts,
            relay_grants,
            oldest_unresolved_outcome,
        }
    }

    pub(crate) fn is_connected_to(&self, node_id: &ClusterNodeName) -> bool {
        let Some(target) = self.targets.load().get(node_id).cloned() else {
            return false;
        };
        for class in PoolClass::PRECONNECTED {
            for slot in target.slots(class) {
                if slot.cancel.is_cancelled() || slot.connection.load().is_none() {
                    return false;
                }
            }
        }
        true
    }

    pub(crate) async fn bootstrap_target(
        &self,
        target: PeerTarget,
    ) -> Result<ClusterNodeName, Report<TransportError>> {
        if self.admission_closed.is_cancelled() {
            return Err(Report::new(TransportError::ShuttingDown));
        }
        let peer_addr = target.addr;
        let setup = async {
            let cancel = CancellationToken::new();
            let _connection_permits = self
                .acquire_connection_permits(PoolClass::Management, &cancel)
                .await
                .ok_or(TransportError::ShuttingDown)?;
            let _handshake_permit = nervix_primitives::select! {
                _ = self.admission_closed.cancelled() => {
                    return Err(Report::new(TransportError::ShuttingDown));
                }
                permit = StdArc::clone(&self.handshake_permits).acquire_owned() => {
                    permit.map_err(|_| TransportError::ShuttingDown)?
                }
            };
            let tcp = TcpStream::connect(target.addr)
                .await
                .map_err(TransportError::from)?;
            tcp.set_nodelay(true).map_err(TransportError::from)?;
            let tls = self.tls.current().bundle;
            let session = tls
                .connect(tcp, &target.server_name, &self.cluster_id, None)
                .await?;
            let identity = session.peer;
            if !identity.matches_endpoint(&target.server_name) {
                return Err(Report::new(TransportError::InvalidHandshake(format!(
                    "peer certificate does not identify bootstrap endpoint '{}'",
                    target.server_name
                ))));
            }
            let node_id = identity.node_id;
            let outbound = self.install_authenticated_target(node_id.clone(), target)?;
            self.ensure_preconnected_slots(&outbound);
            Ok::<_, Report<TransportError>>(node_id)
        };
        match timeout(self.options.connection_setup_timeout, setup).await {
            Ok(result) => result,
            Err(_) => Err(Report::new(TransportError::ConnectionSetupTimeout {
                peer: NodeEndpoint::from(peer_addr),
                timeout: self.options.connection_setup_timeout,
            })),
        }
    }

    pub(crate) fn retire_departed_connections(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
        self.retire_departed_relay_owners(live_nodes);
        let departed = self
            .targets
            .load()
            .iter()
            .filter(|target| !live_nodes.contains(target.0))
            .map(|target| target.0.clone())
            .collect::<BTreeSet<_>>();
        for node in departed {
            self.remove_target(&node);
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "this operation installs, snapshots or retires retained execution state at \
                      an explicit lifetime boundary"
        )
    )]
    fn ensure_class_slots(&self, target: &OutboundTarget, class: PoolClass) {
        for slot in target.slots(class) {
            self.ensure_slot(slot);
        }
    }

    fn ensure_preconnected_slots(&self, target: &OutboundTarget) {
        for class in PoolClass::PRECONNECTED {
            self.ensure_class_slots(target, class);
        }
    }

    /// Claims the one worker belonging to this slot's lifetime. Established requests only read
    /// the claimed bit; replacement constructs different slots and never resets this bit.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one retained pool slot admits its worker once per endpoint and TLS \
                      generation",
            key = "peer endpoint, pool class, slot and cancellation lifetime",
            bound = "one atomic claim and at most one supervised worker per retained slot"
        )
    )]
    fn ensure_slot(&self, slot: &Arc<SlotControl>) {
        if self.admission_closed.is_cancelled() || !slot.claim_worker() {
            return;
        }
        let state = self.clone();
        let slot = slot.clone();
        #[cfg_attr(
            nervix_lint,
            nervix::context(
                lifecycle,
                reason = "only the successful first-use claim constructs this retained slot's \
                          reconnect worker"
            )
        )]
        #[cfg_attr(
            nervix_lint,
            expect(
                nervix::lifecycle_call,
                reason = "constructing this reconnect task is a first-use installation: the \
                          retained slot's atomic claim has one winner and is never reset"
            )
        )]
        let worker = async move {
            state.run_slot(slot).await;
        };
        self.tasks.spawn(worker);
    }

    fn retire_slot(&self, slot: &SlotControl) {
        slot.cancel.cancel();
        slot.connection.store(None);
        let connection = self.connections.remove_if(&slot.key, |_, connection| {
            connection.retiring == slot.cancel
        });
        if connection.is_some() {
            self.decrement_peer(&slot.key.node_id);
        }
        self.connection_changed.notify_waiters();
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "this operation installs, snapshots or retires retained execution state at \
                      an explicit lifetime boundary"
        )
    )]
    async fn run_slot(self, slot: Arc<SlotControl>) {
        let key = slot.key.clone();
        let slot_cancel = slot.cancel.clone();
        let mut backoff = self.options.reconnect_backoff;
        loop {
            nervix_primitives::task::consume_budget().await;
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
            let connected = nervix_primitives::select! {
                _ = slot_cancel.cancelled() => break,
                _ = self.admission_closed.cancelled() => break,
                connected = self.connect(&key, &slot_cancel) => connected,
            };
            match connected {
                Ok(connection) => {
                    backoff = self.options.reconnect_backoff;
                    match self.register_connection(&slot, connection.clone()) {
                        Ok(()) => {
                            self.observations.connection_established(key.class);
                            nervix_primitives::select! {
                                _ = slot_cancel.cancelled() => {}
                                _ = self.admission_closed.cancelled() => {}
                                _ = connection.closed.cancelled() => {}
                            }
                            self.observations
                                .connection_failed(key.class, ConnectionFailureReason::Closed);
                            slot.connection.store(None);
                            self.unregister_connection(&key, &connection);
                            self.drain_outbound_connection(&connection).await;
                            connection.cancel.cancel();
                        }
                        Err(error) => {
                            self.observations
                                .connection_failed(key.class, ConnectionFailureReason::Capacity);
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
                    self.observations.connection_failed(
                        key.class,
                        ConnectionFailureReason::of(error.current_context()),
                    );
                    debug!(
                        ?error,
                        node = %key.node_id,
                        endpoint = %key.endpoint,
                        class = ?key.class,
                        slot = key.slot,
                        "interconnect pool connection failed"
                    );
                }
            }
            drop(permits);
            nervix_primitives::select! {
                _ = slot_cancel.cancelled() => break,
                _ = self.admission_closed.cancelled() => break,
                _ = sleep(backoff) => {}
            }
            backoff = backoff
                .checked_mul(2)
                .unwrap_or(self.options.max_reconnect_backoff)
                .min(self.options.max_reconnect_backoff);
        }
        slot.connection.store(None);
    }

    /// End a blackholed HTTP/2 connection without waiting for the kernel's TCP retransmission
    /// timeout. Both ends monitor it, so a stale inbound class slot also releases its capacity.
    async fn monitor_http2_connection(mut ping_pong: PingPong) {
        loop {
            nervix_primitives::task::consume_budget().await;
            sleep(HTTP2_PING_INTERVAL).await;
            match timeout(HTTP2_PING_TIMEOUT, ping_pong.ping(Ping::opaque())).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    debug!(?error, "interconnect HTTP/2 ping failed");
                    break;
                }
                Err(_) => {
                    debug!("interconnect HTTP/2 ping timed out");
                    break;
                }
            }
        }
    }

    async fn acquire_connection_permits(
        &self,
        class: PoolClass,
        cancel: &CancellationToken,
    ) -> Option<ConnectionPermits> {
        let non_management = if class == PoolClass::Management {
            None
        } else {
            let acquired = nervix_primitives::select! {
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
            let acquired = nervix_primitives::select! {
                _ = cancel.cancelled() => return None,
                _ = self.admission_closed.cancelled() => return None,
                acquired = StdArc::clone(&self.non_preconnected_connection_permits).acquire_owned() => acquired,
            };
            match acquired {
                Ok(permit) => Some(permit),
                Err(_) => return None,
            }
        };
        let acquired = nervix_primitives::select! {
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

    fn register_connection(
        &self,
        slot: &SlotControl,
        connection: StdArc<ClientConnection>,
    ) -> Result<(), Report<TransportError>> {
        let key = connection.key.clone();
        let Entry::Vacant(entry) = self.connections.entry(key.clone()) else {
            return Err(Report::new(TransportError::PoolExhausted));
        };
        self.increment_peer(&key.node_id)?;
        entry.insert(connection.clone());
        slot.connection.store(Some(connection));
        self.connection_changed.notify_waiters();
        Ok(())
    }

    fn unregister_connection(
        &self,
        key: &ConnectionSlotKey,
        connection: &StdArc<ClientConnection>,
    ) {
        if self
            .connections
            .remove_if(key, |_, current| StdArc::ptr_eq(current, connection))
            .is_none()
        {
            return;
        }
        self.decrement_peer(&key.node_id);
        self.connection_changed.notify_waiters();
    }

    async fn drain_outbound_connection(&self, connection: &ClientConnection) {
        if connection.closed.is_cancelled() {
            return;
        }
        let drained = connection.stream_slots.drain();
        nervix_primitives::select! {
            _ = self.force_close.cancelled() => {}
            _ = sleep(self.options.shutdown_drain_timeout) => {}
            _ = drained => {}
        }
    }

    fn allocate_binding_sequence(&self) -> u64 {
        loop {
            let current = self.next_binding_sequence.load(Ordering::Relaxed);
            let next = current
                .checked_add(1)
                .assured("a process cannot start u64::MAX connection bindings");
            if self
                .next_binding_sequence
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return current;
            }
        }
    }

    async fn connect(
        &self,
        key: &ConnectionSlotKey,
        slot_cancel: &CancellationToken,
    ) -> Result<StdArc<ClientConnection>, Report<TransportError>> {
        let binding_sequence = self.allocate_binding_sequence();
        let budget = ConnectionBudget::start(self.options.connection_setup_timeout);
        let setup = async {
            let DialedStream {
                stream: tcp,
                addr: peer_addr,
            } = self.dial(key, &budget).await?;
            tcp.set_nodelay(true).map_err(TransportError::from)?;
            let ActiveTls {
                generation,
                bundle: tls,
            } = self.tls.current();
            let session = tls
                .connect(
                    tcp,
                    key.endpoint.host(),
                    &self.cluster_id,
                    Some(&key.node_id),
                )
                .await?;
            let stream = session.stream;
            let certificate_expires_at = session.expires_at;

            let mut builder = client::Builder::new();
            configure_client_builder(&mut builder, &self.options, key.class)?;
            let (sender, mut connection) = builder
                .handshake(stream)
                .await
                .map_err(TransportError::from)?;
            let ping_pong = connection
                .ping_pong()
                .assured("a newly handshaken HTTP/2 connection has not lent out its ping handle");
            let cancel = CancellationToken::new();
            let closed = CancellationToken::new();
            let driver_cancel = cancel.clone();
            let driver_closed = closed.clone();
            let force_close = self.force_close.clone();
            self.tasks.spawn(async move {
                nervix_primitives::select! {
                    result = connection => {
                        if let Err(error) = result {
                            debug!(?error, "outbound HTTP/2 connection closed");
                        }
                    }
                    () = Self::monitor_http2_connection(ping_pong) => {}
                    _ = driver_cancel.cancelled() => {}
                    _ = force_close.cancelled() => {}
                    _ = sleep_until(certificate_expires_at) => {}
                }
                driver_closed.cancel();
            });
            let driver_setup_guard = CancelOnDrop::new(cancel.clone());
            let connection = StdArc::new(ClientConnection {
                key: key.clone(),
                peer_addr,
                request_host: key.endpoint.url_host(),
                sender,
                stream_slots: StreamSlotQuotas::new(key.class),
                peer_epoch: 0,
                relay_owner: None,
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
                return Err(Report::new(TransportError::InvalidHandshake(
                    "wire fingerprint or addressed node identity differs".to_string(),
                )));
            }
            let connection = StdArc::new(ClientConnection {
                key: connection.key.clone(),
                peer_addr: connection.peer_addr,
                request_host: connection.request_host.clone(),
                sender: connection.sender.clone(),
                stream_slots: connection.stream_slots.clone(),
                peer_epoch: accepted.process_epoch,
                relay_owner: Some(self.bind_relay_owner(
                    &key.node_id,
                    accepted.process_epoch,
                    binding_sequence,
                )?),
                retiring: connection.retiring.clone(),
                cancel: connection.cancel.clone(),
                closed: connection.closed.clone(),
            });
            let current_generation = self.tls.generation();
            if current_generation != generation || slot_cancel.is_cancelled() {
                connection.cancel.cancel();
                return Err(Report::new(TransportError::Closed(key.endpoint.clone())));
            }
            driver_setup_guard.disarm();
            Ok::<_, Report<TransportError>>(connection)
        };
        match timeout(self.options.connection_setup_timeout, setup).await {
            Ok(result) => result,
            Err(_) => Err(Report::new(TransportError::ConnectionSetupTimeout {
                peer: key.endpoint.clone(),
                timeout: self.options.connection_setup_timeout,
            })),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the transport performs this operation for each admitted frame or stream \
                      request"
        )
    )]
    pub(crate) async fn lease(
        &self,
        node_id: &ClusterNodeName,
        class: PoolClass,
        subquota: RequestSubquota,
        deadline: Instant,
    ) -> Result<StreamLease, Report<TransportError>> {
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.admission_closed.is_cancelled() {
                return Err(Report::new(TransportError::ShuttingDown));
            }
            let busy_pool = match self.try_lease(node_id, class, subquota) {
                StreamLeaseAttempt::Leased(lease) => return Ok(lease),
                StreamLeaseAttempt::Busy(pool) => Some(pool),
                StreamLeaseAttempt::NoTarget => None,
            };
            // Only an operation that found no free stream registers to wait: for a stream of its
            // own class and subquota to be released, and for any change of the peer's connections.
            // It checks the pools again once registered, so a change between the two checks still
            // wakes it.
            let changed = self.connection_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let released = async {
                match &busy_pool {
                    Some(pool) => pool.releases.of(class, subquota).notified().await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(released);
            // Polled once so the release wait registers before the second check.
            if let std::task::Poll::Ready(()) = futures_util::poll!(released.as_mut()) {
                continue;
            }
            if let StreamLeaseAttempt::Leased(lease) = self.try_lease(node_id, class, subquota) {
                return Ok(lease);
            }

            nervix_primitives::select! {
                _ = self.admission_closed.cancelled() => {
                    return Err(Report::new(TransportError::ShuttingDown));
                }
                _ = sleep_until(deadline) => {
                    return Err(Report::new(TransportError::RequestTimeout {
                        peer: node_id.clone(),
                        timeout: self.options.request_timeout,
                    }));
                }
                _ = &mut changed => {}
                () = &mut released => {}
            }
        }
    }

    /// Select from the peer's atomically published slot handles. The publication changes only
    /// with topology or credentials; connection workers replace their own connection publication.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "stream requests select a retained peer pool and lease one bounded stream"
        )
    )]
    fn try_lease(
        &self,
        node_id: &ClusterNodeName,
        class: PoolClass,
        subquota: RequestSubquota,
    ) -> StreamLeaseAttempt {
        let targets = self.targets.load();
        let Some(target) = targets.get(node_id) else {
            return StreamLeaseAttempt::NoTarget;
        };
        let slots = target.slots(class);
        let start = self.next_connection.fetch_add(1, Ordering::Relaxed) % slots.len();
        let (before_start, from_start) = slots.split_at(start);
        for slot in from_start.iter().chain(before_start) {
            self.ensure_slot(slot);
            let Some(connection) = slot.connection.load_full() else {
                continue;
            };
            let Some(permit) = connection.stream_slots.try_lease(subquota) else {
                continue;
            };
            if connection.closed.is_cancelled() || connection.retiring.is_cancelled() {
                continue;
            }
            return StreamLeaseAttempt::Leased(StreamLease {
                connection,
                slot: Some(permit),
                state: self.clone(),
                released: StreamRelease {
                    pool: Arc::clone(&target.pool),
                    class,
                    subquota,
                },
            });
        }
        StreamLeaseAttempt::Busy(Arc::clone(&target.pool))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    pub(crate) async fn send(
        &self,
        node_id: &ClusterNodeName,
        envelope: Envelope,
    ) -> Result<(), Report<TransportError>> {
        if let Envelope::RelayPayload(payload) = envelope {
            return self.send_relay(node_id, payload).await;
        }
        let mut completed_admission = None;
        let result = async {
            let class = envelope.pool_class();
            let subquota = match &envelope {
                Envelope::Ack(ack) => {
                    if ack.outcome.is_progress() {
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
                    {
                        let owner = lease
                            .connection
                            .relay_owner
                            .as_ref()
                            .assured("a leased connection completed its authenticated binding");
                        completed_admission = owner.completed_admission(
                            &RelayAdmissionKey {
                                peer_node_id: node_id.clone(),
                                registration: ack.registration.clone(),
                            },
                            &ack.outcome,
                        );
                    }
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
                        return Err(Report::new(TransportError::Decode(
                            "typed request envelopes cannot be sent as one-way controls"
                                .to_string(),
                        )));
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
                    return Err(Report::new(TransportError::RelayGrant(
                        "relay payload escaped relay admission".to_string(),
                    )));
                }
            }
            Ok(())
        }
        .await;
        if result.is_ok()
            && let Some(record) = completed_admission
        {
            self.retire_relay_record(&record, record.status());
        }
        result
    }

    pub(crate) async fn round_trip_control(
        &self,
        node_id: &ClusterNodeName,
        control: ControlEnvelope,
        subquota: RequestSubquota,
        timeout_duration: Duration,
    ) -> Result<wire::Decoded<ControlEnvelope>, Report<TransportError>> {
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
                    timeout: deadline.saturating_duration_since(Instant::now()),
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
    ) -> Result<(), Report<TransportError>> {
        self.incoming_tx
            .try_send(ReceivedEnvelope::new(
                peer_addr,
                peer_node_id,
                envelope,
                decoded,
            ))
            .map_err(|_| Report::new(TransportError::IncomingQueueFull))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the transport performs this operation for each admitted frame or stream \
                      request"
        )
    )]
    async fn deliver_terminal_incoming(
        &self,
        peer_addr: SocketAddr,
        peer_node_id: ClusterNodeName,
        envelope: Envelope,
        decoded: Option<Reservation>,
    ) -> Result<(), Report<TransportError>> {
        let received = ReceivedEnvelope::new(peer_addr, peer_node_id, envelope, decoded);
        nervix_primitives::select! {
            _ = self.admission_closed.cancelled() => Err(Report::new(TransportError::ShuttingDown)),
            result = self.incoming_tx.send(received) => {
                result.map_err(|_| Report::new(TransportError::ShuttingDown))
            }
        }
    }

    async fn accept_loop(self, listener: TcpListener) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let accepted = nervix_primitives::select! {
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
            let binding_sequence = self.allocate_binding_sequence();
            let state = self.clone();
            self.tasks.spawn(async move {
                nervix_primitives::select! {
                    _ = state.admission_closed.cancelled() => {}
                    _ = state.force_close.cancelled() => {}
                    result = state.clone().accept_connection(
                        tcp,
                        peer_addr,
                        handshake_permit,
                        connection_permit,
                        binding_sequence,
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
        binding_sequence: u64,
    ) -> Result<(), Report<TransportError>> {
        tcp.set_nodelay(true).map_err(TransportError::from)?;
        let ActiveTls {
            generation,
            bundle: tls,
        } = self.tls.current();
        let session = tls
            .accept(
                tcp,
                peer_addr,
                self.options.connection_setup_timeout,
                &self.cluster_id,
            )
            .await?;
        let stream = session.stream;
        let peer_identity = session.peer;
        let certificate_expires_at = session.expires_at;
        let mut builder = server::Builder::new();
        configure_server_builder(&mut builder, &self.options)?;
        let mut connection = timeout(
            self.options.connection_setup_timeout,
            builder.handshake(stream),
        )
        .await
        .map_err(|_| TransportError::ConnectionSetupTimeout {
            peer: NodeEndpoint::from(peer_addr),
            timeout: self.options.connection_setup_timeout,
        })?
        .map_err(TransportError::from)?;
        let first = timeout(self.options.connection_setup_timeout, connection.accept())
            .await
            .map_err(|_| TransportError::ConnectionSetupTimeout {
                peer: NodeEndpoint::from(peer_addr),
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
                request,
                respond,
                InboundBinding {
                    peer_addr,
                    peer_identity,
                    handshake_permit,
                    binding_sequence,
                },
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
        request: Request<RecvStream>,
        respond: server::SendResponse<Bytes>,
        binding: InboundBinding,
    ) -> Result<BoundInboundConnection, Report<TransportError>>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let InboundBinding {
            peer_addr,
            peer_identity,
            handshake_permit,
            binding_sequence,
        } = binding;
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
            let relay_owner =
                self.bind_relay_owner(&hello.node_id, hello.process_epoch, binding_sequence)?;
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
                    relay_owner,
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
        nervix_primitives::select! {
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
    ) -> Result<(), Report<TransportError>>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let stream_slots = StdArc::new(Semaphore::new(peer.class.stream_slots_per_connection()));
        let ping_pong = connection
            .ping_pong()
            .assured("a newly bound HTTP/2 connection has not lent out its ping handle");
        let health_probe = Self::monitor_http2_connection(ping_pong);
        tokio::pin!(health_probe);
        let connection_force_close = CancellationToken::new();
        let _connection_force_close_guard = CancelOnDrop::new(connection_force_close.clone());
        let mut draining = false;
        let mut drain_deadline = None;
        loop {
            nervix_primitives::task::consume_budget().await;
            let accepted = if draining {
                let deadline = drain_deadline
                    .verified("entering drain always records its force-close deadline");
                nervix_primitives::select! {
                    _ = peer.relay_owner.closed.cancelled() => break,
                    _ = self.force_close.cancelled() => break,
                    () = &mut health_probe => break,
                    _ = sleep_until(deadline) => break,
                    accepted = connection.accept() => accepted,
                }
            } else {
                let changed = self.tls_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.tls.generation() != generation {
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
                nervix_primitives::select! {
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
                    _ = peer.relay_owner.closed.cancelled() => break,
                    _ = self.force_close.cancelled() => break,
                    () = &mut health_probe => break,
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
                        if self.tls.generation() != generation {
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
            let (request, mut response) = accepted.map_err(TransportError::from)?;
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
                nervix_primitives::select! {
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
        if path == STREAM_PATH {
            self.handle_stream_request(
                peer.node_id,
                peer.advertised_host,
                peer.process_epoch,
                peer.class,
                request.into_body(),
                respond,
            )
            .await?;
            return Ok(());
        }
        if path == DUPLEX_PATH {
            self.handle_duplex_request(
                peer.node_id,
                peer.advertised_host,
                peer.process_epoch,
                peer.class,
                request.into_body(),
                respond,
            )
            .await?;
            return Ok(());
        }
        if path == CONTROL_PATH {
            self.handle_control(peer, request.into_body(), respond)
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
            let terminal_admission = if ack.outcome.is_progress() {
                None
            } else {
                Some(RelayAdmissionKey {
                    peer_node_id: peer.node_id.clone(),
                    registration: ack.registration.clone(),
                })
            };
            if ack.outcome.is_progress() {
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
                peer.relay_owner.retire_outbound_admission(&admission_key);
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
                    peer.relay_owner,
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
                    peer.relay_owner,
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
            self.handle_relay_body(peer, grant_id, request, respond)
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
        peer: InboundPeer,
        body: RecvStream,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        let bytes = read_body(
            &self.executor,
            peer.class.memory_class(),
            peer.class.control_body_limit(&self.executor),
            self.options.progress_timeout,
            body,
        )
        .await?;
        let decoded = wire::decode_rkyv::<ControlEnvelope>(
            &self.executor,
            peer.class.memory_class(),
            peer.class.cpu_class(),
            bytes,
        )
        .await?;
        let (control, reservation) = decoded.into_parts();
        if control.pool_class() != peer.class {
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
            let response = nervix_primitives::select! {
                response = self.requests.handle(
                    &self.executor,
                    peer.node_id,
                    peer.advertised_host,
                    peer.process_epoch,
                    request,
                ) => response,
                reset = poll_fn(|context| respond.poll_reset(context)) => {
                    reset.map_err(TransportError::from)?;
                    return Ok(());
                }
            };
            let (response, _payload_reservation) = response.into_parts();
            let response = wire::encode_rkyv(
                &self.executor,
                peer.class.memory_class(),
                peer.class.cpu_class(),
                peer.class.control_body_limit(&self.executor),
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
            peer.addr,
            peer.node_id,
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

    fn increment_peer(&self, node_id: &ClusterNodeName) -> Result<(), Report<TransportError>> {
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
    ) -> Result<InboundConnectionRegistration, Report<TransportError>> {
        let key = InboundPoolKey { node_id, class };
        match self.inbound_pool_connections.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                if *entry.get() >= class.connections_per_peer() {
                    return Err(Report::new(TransportError::PoolExhausted));
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

    pub(crate) async fn replace_tls(
        &self,
        tls: TlsConfigBundle,
    ) -> Result<(), Report<TransportError>> {
        tls.certificate
            .validate_local(&self.cluster_id, &self.node_id, &self.advertised_host)
            .map_err(|error| TransportError::with_cause(error, TransportError::InvalidHandshake))?;
        tls.clock
            .ensure_current(&tls.certificate)
            .map_err(|error| TransportError::with_cause(error, TransportError::InvalidHandshake))?;
        // Published before the slots are retired. An outbound connection set up from the replaced
        // credentials checks the generation once it is established: one that checks after this
        // publication is refused, and one that checked before it was registered on a slot that
        // existed before it, which the retirement below ends.
        self.tls.replace(tls);
        self.cancel_all_slots();
        self.tls_changed.notify_waiters();
        self.targets.rcu(|current| {
            let mut next = (**current).clone();
            for (node, target) in current.iter() {
                self.retire_target(target);
                next.insert(
                    node.clone(),
                    Arc::new(OutboundTarget::new(
                        node,
                        target.endpoint.clone(),
                        target.dial,
                    )),
                );
            }
            next
        });
        for target in self.targets.load().values() {
            self.ensure_preconnected_slots(target);
        }
        Ok(())
    }

    fn cancel_all_slots(&self) {
        for target in self.targets.load().values() {
            self.retire_target(target);
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.admission_closed.cancel();
        self.requests.shutdown();
        self.cancel_all_slots();
        loop {
            let current = self.relay_owners.load_full();
            let observed = self
                .relay_owners
                .compare_and_swap(&current, StdArc::new(RelayOwnersPublication::default()));
            if StdArc::ptr_eq(&current, &observed) {
                for owner in current.owners.values() {
                    owner.end();
                }
                break;
            }
        }
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
    async fn request_stream_raw(
        &self,
        state: &TransportState,
        request: RawRequest<'_>,
    ) -> Result<(RecvStream, u64), Report<TransportError>> {
        if self.closed.is_cancelled() {
            return Err(Report::new(TransportError::Closed(
                self.key.endpoint.clone(),
            )));
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
            let sender = self
                .sender
                .clone()
                .ready()
                .await
                .map_err(TransportError::from)?;
            let mut request_url = url::Url::parse("https://localhost/")
                .assured("the fixed HTTPS request base is a valid URL");
            request_url
                .set_host(Some(&self.request_host))
                .map_err(|error| {
                    Report::new(error).change_context(TransportError::InvalidServerName(
                        self.request_host.clone(),
                    ))
                })?;
            request_url.set_path(path);
            let mut builder = Request::builder()
                .method(Method::POST)
                .version(Version::HTTP_2)
                .uri(request_url.as_str());
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            let request = builder.body(()).map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Http)
            })?;
            let end_stream = body.as_ref().is_none_or(ChargedBytes::is_empty);
            let (response, mut send_stream) = {
                let mut sender = sender;
                sender
                    .send_request(request, end_stream)
                    .map_err(TransportError::from)?
            };
            if let Some(body) = body
                && !body.is_empty()
            {
                send_body(&mut send_stream, body).await?;
            }
            let response = response.await.map_err(TransportError::from)?;
            let status = response.status();
            if !status.is_success() {
                let message = read_body(
                    &state.executor,
                    response_class.memory_class(),
                    response_limit,
                    state.options.progress_timeout,
                    response.into_body(),
                )
                .await?;
                return Err(Report::new(TransportError::RemoteRejected {
                    status: status.as_u16(),
                    message: String::from_utf8_lossy(message.as_ref()).into_owned(),
                }));
            }
            let content_length = response
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .ok_or_else(|| {
                    TransportError::Decode(
                        "streamed response omitted its content length".to_string(),
                    )
                })?
                .to_str()
                .map_err(|error| {
                    TransportError::with_cause(Report::new(error), TransportError::Decode)
                })?
                .parse::<u64>()
                .map_err(|error| {
                    TransportError::with_cause(Report::new(error), TransportError::Decode)
                })?;
            Ok((response.into_body(), content_length))
        };
        match timeout(timeout_duration, operation).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => {
                state.observations.stream_reset(
                    self.key.class,
                    StreamResetReason::of(error.current_context()),
                );
                Err(error)
            }
            Err(_) => {
                state
                    .observations
                    .stream_reset(self.key.class, StreamResetReason::Deadline);
                Err(Report::new(TransportError::RequestTimeout {
                    peer: self.key.node_id.clone(),
                    timeout: timeout_duration,
                }))
            }
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the transport performs this operation for each admitted frame or stream \
                      request"
        )
    )]
    async fn request_raw(
        &self,
        state: &TransportState,
        request: RawRequest<'_>,
    ) -> Result<ChargedBytes, Report<TransportError>> {
        if self.closed.is_cancelled() {
            return Err(Report::new(TransportError::Closed(
                self.key.endpoint.clone(),
            )));
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
            let sender = self
                .sender
                .clone()
                .ready()
                .await
                .map_err(TransportError::from)?;
            let mut request_url = url::Url::parse("https://localhost/")
                .assured("the fixed HTTPS request base is a valid URL");
            request_url
                .set_host(Some(&self.request_host))
                .map_err(|error| {
                    Report::new(error).change_context(TransportError::InvalidServerName(
                        self.request_host.clone(),
                    ))
                })?;
            request_url.set_path(path);
            let mut builder = Request::builder()
                .method(Method::POST)
                .version(Version::HTTP_2)
                .uri(request_url.as_str());
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            let request = builder.body(()).map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Http)
            })?;
            let end_stream = body.as_ref().is_none_or(ChargedBytes::is_empty);
            let (response, mut stream) = {
                let mut sender = sender;
                sender
                    .send_request(request, end_stream)
                    .map_err(TransportError::from)?
            };
            if let Some(body) = body
                && !body.is_empty()
            {
                send_body(&mut stream, body).await?;
            }
            let response = response.await.map_err(TransportError::from)?;
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
                return Err(Report::new(TransportError::RemoteRejected {
                    status: status.as_u16(),
                    message: String::from_utf8_lossy(response.as_ref()).into_owned(),
                }));
            }
            Ok(response)
        };
        match timeout(timeout_duration, operation).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => {
                state.observations.stream_reset(
                    self.key.class,
                    StreamResetReason::of(error.current_context()),
                );
                Err(error)
            }
            Err(_) => {
                state
                    .observations
                    .stream_reset(self.key.class, StreamResetReason::Deadline);
                Err(Report::new(TransportError::RequestTimeout {
                    peer: self.key.node_id.clone(),
                    timeout: timeout_duration,
                }))
            }
        }
    }
}

impl StreamLease {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the transport performs this operation for each admitted frame or stream \
                      request"
        )
    )]
    async fn request_raw(
        &self,
        state: &TransportState,
        request: RawRequest<'_>,
    ) -> Result<ChargedBytes, Report<TransportError>> {
        self.connection.request_raw(state, request).await
    }
}

#[cfg(test)]
#[path = "connection/retained_slots_tests.rs"]
mod retained_slots_tests;
