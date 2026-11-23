//! Typed request and response exchange over authenticated interconnect streams.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Deadlines, reserved request quotas, handler dispatch, response validation, and
//!   membership cancellation for typed internal requests.
//! - **Depends on.** The authenticated HTTP/2 transport and rkyv payload vocabulary.
//! - **Must not know.** The runtime meaning of a request or response.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{collections::BTreeSet, future::Future, marker::PhantomData, pin::Pin, time::Duration};

use ahash::HashMap;
use arch_into::ArchInto as _;
use error_stack::Report;
use futures_util::{Stream, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{BudgetedBuffer, ChargedBytes, Executor, QueueAdmission, Reservation};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeName, CoordinationIdentity, DomainClockAuthorityRevision,
    DomainName,
};
use nervix_primitives::{
    publication::{ArcSwap, ArcSwapOption},
    sync::{Arc, Notify, OwnedSemaphorePermit, Semaphore, StdArc},
    time::{Instant, timeout},
};
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighDeserializer, HighSerializer},
    rancor::Error as RkyvError,
    ser::{allocator::ArenaHandle, writer::IoWriter},
    util::AlignedVec,
};
use strum::{AsRefStr, EnumCount, EnumIter, IntoEnumIterator as _};
use thiserror::Error;

use super::{
    ActivateOwnershipHandoffStateRequest, CaptureOwnershipHandoffStateRequest,
    ConfirmOwnershipHandoffStateRequest, ControlEnvelope, DescribeIngestorRequest,
    DiscardForcedOwnershipRecoveryRequest, DiscardOwnershipHandoffStateRequest,
    DomainClockProgressRequest, ForcedOwnershipRecoveryPreparation, IngestorDescribeEnvelope,
    OwnershipHandoffCheckpoint, OwnershipHandoffResponse, PoolClass,
    PrepareForcedOwnershipRecoveryRequest, PrepareOwnershipHandoffStateRequest,
    ReconcileOwnershipHandoffPreparationsRequest, RemoteOperationFailure,
    RemoteOperationFailureKind, Transport, TransportError, wire,
};
use crate::{
    connection::{
        ChargedItem, DuplexCodec, DuplexItems, DuplexReceiver, DuplexResponses, DuplexSender,
        EncodedMessage, FrameReader, OutboundByteStreamRequest, PeerProcessFence, RawDuplexRequest,
    },
    observation::{ConnectionDirection, RequestOutcome, TransportObservations},
};

#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestEnvelope {
    pub(crate) class: PoolClass,
    pub(crate) request: String,
    pub(crate) payload: Vec<u8>,
}

/// The receiver's operation bound also permits consensus snapshot transfer before it has joined.
#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamRequestEnvelope {
    pub(crate) request: RequestEnvelope,
    pub(crate) receive: nervix_models::PeerReceiveBounds,
}

#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseEnvelope {
    pub(crate) class: PoolClass,
    pub(crate) request: String,
    pub(crate) result: Result<Vec<u8>, RemoteRequestFailure>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the fixed wire response classifies remote request outcomes; local failures \
                  retain reports"
    )
)]
pub enum RemoteRequestFailure {
    #[error("no handler is registered")]
    HandlerNotRegistered,
    #[error("request payload is invalid: {0}")]
    InvalidPayload(String),
    #[error("response payload could not be encoded: {0}")]
    ResponseEncode(String),
    #[error("request used {actual:?} but its registered handler requires {expected:?}")]
    WrongPoolClass {
        expected: PoolClass,
        actual: PoolClass,
    },
    #[error("request payload contains {actual} bytes, exceeding the {limit}-byte class limit")]
    PayloadTooLarge { actual: u64, limit: u64 },
    #[error("the {subquota:?} request subquota is full")]
    AdmissionFull { subquota: RequestSubquota },
    #[error("the {class:?} memory budget has no room for another {requested} bytes")]
    MemoryBudgetFull { class: PoolClass, requested: u64 },
    #[error("a {requested}-byte decode exceeds the {capacity}-byte {class:?} memory budget")]
    MemoryBudgetExceeded {
        class: PoolClass,
        requested: u64,
        capacity: u64,
    },
    #[error("the {class:?} memory budget is closed")]
    MemoryBudgetClosed { class: PoolClass },
    #[error("the {class:?} execution queue already holds {pending} waiting jobs")]
    ExecutionQueueFull { class: PoolClass, pending: u64 },
    #[error("the {class:?} execution pool is closed")]
    ExecutionPoolClosed { class: PoolClass },
    #[error("the coordination identity does not belong to the authenticated request sender")]
    CoordinationIdentityMismatch,
    #[error("the producer's described state changed before the transfer opened")]
    PreconditionChanged,
}

impl RemoteRequestFailure {
    /// Project a decoder report at the fixed remote boundary. Admission judged no input, so it
    /// retains its class and dimensions instead of claiming malformed bytes.
    pub(crate) fn decode(error: Report<TransportError>, class: PoolClass) -> Self {
        use nervix_execution::{AdmissionError, ExecutionError};

        match error.downcast_ref::<AdmissionError>() {
            Some(AdmissionError::BudgetExhausted { requested, .. }) => {
                return Self::MemoryBudgetFull {
                    class,
                    requested: *requested,
                };
            }
            Some(AdmissionError::ExceedsBudget {
                requested,
                capacity,
                ..
            }) => {
                return Self::MemoryBudgetExceeded {
                    class,
                    requested: *requested,
                    capacity: *capacity,
                };
            }
            Some(AdmissionError::BudgetClosed { .. }) => {
                return Self::MemoryBudgetClosed { class };
            }
            Some(AdmissionError::DifferentBudget { .. }) | None => {}
        }
        match error.downcast_ref::<ExecutionError>() {
            Some(ExecutionError::QueueFull { pending, .. }) => {
                return Self::ExecutionQueueFull {
                    class,
                    pending: u64::try_from(*pending)
                        .assured("a bounded worker queue fits in the wire count"),
                };
            }
            Some(ExecutionError::PoolClosed { .. }) => return Self::ExecutionPoolClosed { class },
            Some(ExecutionError::JobPanicked { .. }) | None => {}
        }
        Self::InvalidPayload(format!("{error:#}"))
    }

    pub(crate) fn status(&self) -> http::StatusCode {
        match self {
            Self::AdmissionFull { .. }
            | Self::MemoryBudgetFull { .. }
            | Self::MemoryBudgetClosed { .. }
            | Self::ExecutionQueueFull { .. }
            | Self::ExecutionPoolClosed { .. } => http::StatusCode::SERVICE_UNAVAILABLE,
            Self::MemoryBudgetExceeded { .. } => http::StatusCode::PAYLOAD_TOO_LARGE,
            Self::PreconditionChanged => http::StatusCode::PRECONDITION_FAILED,
            _ => http::StatusCode::BAD_REQUEST,
        }
    }
}

/// The authenticated peer that submitted a request to a registered handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    peer_node_id: ClusterNodeName,
    peer_advertised_host: String,
    peer_process_epoch: u64,
}

impl RequestContext {
    pub fn peer_node_id(&self) -> &ClusterNodeName {
        &self.peer_node_id
    }

    /// The process epoch the authenticated peer supplied in this connection's hello.
    pub fn peer_process_epoch(&self) -> u64 {
        self.peer_process_epoch
    }

    /// The certificate-validated host named by the peer's connection hello.
    pub fn peer_advertised_host(&self) -> &str {
        &self.peer_advertised_host
    }

    fn authenticates(&self, identity: &CoordinationIdentity) -> bool {
        identity.coordinator() == &self.peer_node_id
            && identity.process_epoch() == self.peer_process_epoch
    }
}

/// A value that the transport can validate and encode as bounded rkyv.
///
/// [`encode_rkyv`](Self::encode_rkyv) and [`decode_rkyv`](Self::decode_rkyv) refuse at once when
/// the class's CPU queue has no place, which suits a request its caller still holds. The `_with`
/// forms take the admission from the caller, so a response, which carries work its handler already
/// did, waits for a place instead.
#[doc(hidden)]
pub trait RkyvMessage: Archive + Sized + Send + 'static {
    fn encode_rkyv(
        self,
        executor: Executor,
        class: PoolClass,
        limit: u64,
    ) -> impl Future<Output = Result<(Vec<u8>, Reservation), Report<TransportError>>> + Send {
        self.encode_rkyv_with(executor, class, limit, QueueAdmission::RefuseWhenFull)
    }

    fn encode_rkyv_with(
        self,
        executor: Executor,
        class: PoolClass,
        limit: u64,
        admission: QueueAdmission,
    ) -> impl Future<Output = Result<(Vec<u8>, Reservation), Report<TransportError>>> + Send;

    /// Serialize as [`Self::encode_rkyv_with`] does, into `charge`, memory of the class the caller
    /// already holds for it, instead of first waiting for room in the class. The message grows past
    /// `charge` only while the class can back it at once.
    fn encode_rkyv_within(
        self,
        executor: Executor,
        class: PoolClass,
        limit: u64,
        admission: QueueAdmission,
        charge: Reservation,
    ) -> impl Future<Output = Result<(Vec<u8>, Reservation), Report<TransportError>>> + Send;

    /// Validate and deserialize as [`Self::decode_rkyv_with`] does, from `payload`, bytes whose
    /// charge the caller holds, into `decoded`, memory of the class the caller already holds for
    /// the decoded value: as many bytes as the payload, the charge [`Self::decode_rkyv_with`]
    /// takes for it. Neither is reserved here, so the decoding cannot be refused for memory.
    fn decode_rkyv_within(
        executor: Executor,
        class: PoolClass,
        admission: QueueAdmission,
        payload: ChargedBytes,
        decoded: Reservation,
    ) -> impl Future<Output = Result<(Self, Reservation), Report<TransportError>>> + Send;

    fn decode_rkyv(
        executor: Executor,
        class: PoolClass,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(Self, Reservation), Report<TransportError>>> + Send {
        Self::decode_rkyv_with(executor, class, QueueAdmission::RefuseWhenFull, payload)
    }

    fn decode_rkyv_with(
        executor: Executor,
        class: PoolClass,
        admission: QueueAdmission,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(Self, Reservation), Report<TransportError>>> + Send;
}

impl<T> RkyvMessage for T
where
    T: Archive
        + Sized
        + Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + Deserialize<T, HighDeserializer<RkyvError>>,
{
    async fn encode_rkyv_with(
        self,
        executor: Executor,
        class: PoolClass,
        limit: u64,
        admission: QueueAdmission,
    ) -> Result<(Vec<u8>, Reservation), Report<TransportError>> {
        wire::encode_rkyv_payload(
            &executor,
            class.memory_class(),
            class.cpu_class(),
            limit,
            admission,
            self,
        )
        .await
        .map(wire::EncodedPayload::into_parts)
    }

    async fn encode_rkyv_within(
        self,
        executor: Executor,
        class: PoolClass,
        limit: u64,
        admission: QueueAdmission,
        charge: Reservation,
    ) -> Result<(Vec<u8>, Reservation), Report<TransportError>> {
        wire::encode_rkyv_payload_within(
            &executor,
            class.cpu_class(),
            limit,
            admission,
            self,
            charge,
        )
        .await
        .map(wire::EncodedPayload::into_parts)
    }

    async fn decode_rkyv_within(
        executor: Executor,
        class: PoolClass,
        admission: QueueAdmission,
        payload: ChargedBytes,
        decoded: Reservation,
    ) -> Result<(Self, Reservation), Report<TransportError>> {
        wire::decode_rkyv_within::<Self>(&executor, class.cpu_class(), admission, payload, decoded)
            .await
            .map(wire::Decoded::into_parts)
    }

    async fn decode_rkyv_with(
        executor: Executor,
        class: PoolClass,
        admission: QueueAdmission,
        payload: Vec<u8>,
    ) -> Result<(Self, Reservation), Report<TransportError>> {
        wire::decode_rkyv_payload_with::<Self>(
            &executor,
            class.memory_class(),
            class.cpu_class(),
            admission,
            payload,
        )
        .await
        .map(wire::Decoded::into_parts)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Archive,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    AsRefStr,
    EnumCount,
    EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum RequestSubquota {
    Shared,
    /// The one ordered append stream a leader keeps open to each follower.
    Append,
    Resource,
    Snapshot,
    Discovery,
    Liveness,
    Progress,
    Admission,
    Cancellation,
    Terminal,
}

impl RequestSubquota {
    /// This subquota's position in the fixed observation arrays it indexes.
    pub const fn index(self) -> usize {
        match self {
            Self::Shared => 0,
            Self::Append => 1,
            Self::Resource => 2,
            Self::Snapshot => 3,
            Self::Discovery => 4,
            Self::Liveness => 5,
            Self::Progress => 6,
            Self::Admission => 7,
            Self::Cancellation => 8,
            Self::Terminal => 9,
        }
    }
}

/// Why one typed request of a kind could not carry a message on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RequestFitError {
    #[error(
        "request '{request}' archives to {bytes} bytes, above its class's {limit}-byte payload \
         limit"
    )]
    Payload {
        request: &'static str,
        bytes: u64,
        limit: u64,
    },
    #[error(
        "request '{request}' needs a {bytes}-byte control body with its envelope, above its \
         class's {limit}-byte limit"
    )]
    Body {
        request: &'static str,
        bytes: u64,
        limit: u64,
    },
    #[error("request '{request}' cannot be archived")]
    Archive { request: &'static str },
}

impl RequestFitError {
    fn archive(error: RkyvError, request: &'static str) -> Report<Self> {
        Report::new(error).change_context(Self::Archive { request })
    }
}

/// One typed request message and the response type its handler produces.
pub trait InterconnectRequest: RkyvMessage {
    type Response: RkyvMessage;

    const NAME: &'static str;
    const CLASS: PoolClass = PoolClass::Commands;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    const DEADLINE: crate::RequestDeadline;

    /// Discovery requests are permitted before the first live-membership view exists.
    const REQUIRES_LIVE_TARGET: bool = true;

    /// Coordination requests name their issuer so the transport can bind the identity to the
    /// authenticated connection before application code sees the request.
    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        None
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationHealthProbe;

impl InterconnectRequest for ApplicationHealthProbe {
    type Response = ClusterNodeIdentity;

    const NAME: &'static str = "application_health_probe";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Liveness;
}

/// The application progress one process incarnation has reached for control-plane completion.
///
/// Each phase holds the newest revision the incarnation reached in it, and is absent until the
/// incarnation reaches one. A phase only advances, and a new incarnation starts with every phase
/// absent, so no incarnation reports progress its predecessor made.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationProgress {
    pub identity: ClusterNodeIdentity,
    /// The authoritative revision the incarnation applied.
    pub authoritative: Option<u64>,
    /// The runtime revision the incarnation prepared.
    pub runtime_prepared: Option<u64>,
    /// The runtime revision the incarnation made ready.
    pub runtime_ready: Option<u64>,
    /// The runtime revision every domain clock of the incarnation matches.
    pub domain_clock_installed: Option<u64>,
    /// The newest runtime revision the incarnation failed to prepare, until it prepares that
    /// revision or a later one.
    pub runtime_failure: Option<RuntimeApplicationFailure>,
}

/// A runtime revision one process incarnation failed to prepare, and why.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeApplicationFailure {
    pub revision: u64,
    /// The failing incarnation's own description of the failure, because the application ran
    /// inside that incarnation. It names the domain and the entities, never the records they
    /// carry.
    pub reason: String,
}

/// One phase of [`ApplicationProgress`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationProgressPhase {
    Authoritative,
    RuntimePrepared,
    RuntimeReady,
    DomainClockInstalled,
}

impl ApplicationProgress {
    /// The progress of an incarnation that has reached no phase.
    pub const fn new(identity: ClusterNodeIdentity) -> Self {
        Self {
            identity,
            authoritative: None,
            runtime_prepared: None,
            runtime_ready: None,
            domain_clock_installed: None,
            runtime_failure: None,
        }
    }

    /// The newest revision the incarnation reached in `phase`, absent until it reached one.
    pub const fn reached(&self, phase: ApplicationProgressPhase) -> Option<u64> {
        match phase {
            ApplicationProgressPhase::Authoritative => self.authoritative,
            ApplicationProgressPhase::RuntimePrepared => self.runtime_prepared,
            ApplicationProgressPhase::RuntimeReady => self.runtime_ready,
            ApplicationProgressPhase::DomainClockInstalled => self.domain_clock_installed,
        }
    }

    /// Records that the incarnation reached `revision` in `phase`, and answers whether that
    /// advanced the phase. A revision at or before the one already reached changes nothing.
    pub fn advance(&mut self, phase: ApplicationProgressPhase, revision: u64) -> bool {
        let reached = match phase {
            ApplicationProgressPhase::Authoritative => &mut self.authoritative,
            ApplicationProgressPhase::RuntimePrepared => &mut self.runtime_prepared,
            ApplicationProgressPhase::RuntimeReady => &mut self.runtime_ready,
            ApplicationProgressPhase::DomainClockInstalled => &mut self.domain_clock_installed,
        };
        if let Some(current) = *reached
            && current >= revision
        {
            return false;
        }
        *reached = Some(revision);
        if let ApplicationProgressPhase::RuntimePrepared = phase
            && let Some(failure) = &self.runtime_failure
            && failure.revision <= revision
        {
            // Preparing the failed revision, or a later one, ends that failure.
            self.runtime_failure = None;
        }
        true
    }

    /// Records that the incarnation failed to prepare runtime revision `revision` for `reason`,
    /// and answers whether that changed the progress. A failure of a revision the incarnation
    /// prepared since, or of one before the failure it already reports, changes nothing.
    pub fn fail_runtime(&mut self, revision: u64, reason: String) -> bool {
        if let Some(prepared) = self.runtime_prepared
            && prepared >= revision
        {
            return false;
        }
        if let Some(failure) = &self.runtime_failure
            && (failure.revision > revision
                || (failure.revision == revision && failure.reason == reason))
        {
            return false;
        }
        self.runtime_failure = Some(RuntimeApplicationFailure { revision, reason });
        true
    }

    /// The runtime revision the incarnation failed to prepare and has not prepared since.
    pub const fn runtime_failure(&self) -> Option<&RuntimeApplicationFailure> {
        self.runtime_failure.as_ref()
    }

    /// Takes every phase `report`, a later report of the same incarnation, reached beyond this
    /// progress, and answers whether any phase advanced. A report that arrives late moves no
    /// phase back.
    pub fn absorb(&mut self, report: &Self) -> bool {
        let mut advanced = false;
        for phase in [
            ApplicationProgressPhase::Authoritative,
            ApplicationProgressPhase::RuntimePrepared,
            ApplicationProgressPhase::RuntimeReady,
            ApplicationProgressPhase::DomainClockInstalled,
        ] {
            let Some(revision) = report.reached(phase) else {
                continue;
            };
            if self.advance(phase, revision) {
                advanced = true;
            }
        }
        if let Some(failure) = &report.runtime_failure
            && self.fail_runtime(failure.revision, failure.reason.clone())
        {
            advanced = true;
        }
        advanced
    }
}

/// Requests the current application progress from one authenticated node.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationRevisionRequest;

impl InterconnectRequest for ApplicationRevisionRequest {
    type Response = ApplicationProgress;

    const NAME: &'static str = "application_revision";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Progress;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Coordination;
}

/// One process incarnation's application progress, reported to the Raft leader it observes.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationProgressReport {
    pub progress: ApplicationProgress,
    /// The leader the reporting incarnation addressed.
    pub leader: ClusterNodeName,
    /// The Raft term in which the reporting incarnation observed that leader.
    pub term: u64,
}

/// What a node did with an application progress report.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum ApplicationProgressReceipt {
    /// The node leads the addressed term and holds the report.
    Accepted,
    /// The node does not lead the addressed term, so it holds nothing.
    NotLeader,
    /// The report names another node than the one that sent it, so the node holds nothing.
    ForeignReporter,
}

impl InterconnectRequest for ApplicationProgressReport {
    type Response = ApplicationProgressReceipt;

    const NAME: &'static str = "application_progress";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Progress;
    /// A report carries the record a revision query answers, over the same progress subquota, so it
    /// has the query's deadline.
    const DEADLINE: crate::RequestDeadline = ApplicationRevisionRequest::DEADLINE;
}

/// The live process incarnations a leader currently requires for command completion, and the
/// application progress it holds for them.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationCompletionPeersResponse {
    pub leader: ClusterNodeIdentity,
    pub term: u64,
    pub peers: Vec<ClusterNodeIdentity>,
    /// The leader's own progress and the latest progress each other peer reported to it. A peer
    /// that has reported nothing to this leader has no entry.
    pub progress: Vec<ApplicationProgress>,
}

/// Requests the current leader's effective application-health view. A follower must fence the
/// response against its Raft leader and term before using it for a completion barrier.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationCompletionPeersRequest;

impl InterconnectRequest for ApplicationCompletionPeersRequest {
    type Response = Option<ApplicationCompletionPeersResponse>;

    const NAME: &'static str = "application_completion_peers";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Progress;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Coordination;
}

/// Where one process incarnation's HTTPS listener stands against a requested runtime revision.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum HttpsListenerInstallation {
    /// The listener has not yet installed the TLS VHOSTs of the requested revision or a later one.
    Pending,
    /// The listener serves the TLS VHOSTs of `revision`, which is at or after the requested one.
    Installed { revision: u64 },
    /// The TLS VHOSTs of `revision`, at or after the requested one, could not be installed, so the
    /// listener still presents the certificates it presented before. The reason names the VHOST
    /// and the resource version, never the contents of their files.
    Failed { revision: u64, reason: String },
}

impl HttpsListenerInstallation {
    /// The runtime revision an installation attempt was made for, absent while one is pending.
    pub const fn revision(&self) -> Option<u64> {
        match self {
            Self::Pending => None,
            Self::Installed { revision } | Self::Failed { revision, .. } => Some(*revision),
        }
    }
}

/// The HTTPS listener installation one process incarnation reports.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpsListenerInstallationResponse {
    pub identity: ClusterNodeIdentity,
    pub installation: HttpsListenerInstallation,
}

/// Asks one authenticated node whether its HTTPS listener installed the TLS VHOSTs of a runtime
/// revision.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpsListenerInstallationRequest {
    pub revision: u64,
}

impl InterconnectRequest for HttpsListenerInstallationRequest {
    type Response = HttpsListenerInstallationResponse;

    const NAME: &'static str = "https_listener_installation";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Progress;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Coordination;
}

/// Opens progress delivery after this receiver installed the addressed clock authority.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainClockAttachRequest {
    pub domain_id: DomainName,
    pub target: ClusterNodeIdentity,
    pub installed_revision: u64,
    pub generation: u64,
    pub authority_revision: DomainClockAuthorityRevision,
    pub authority: ClusterNodeIdentity,
}

/// The addressed authority's evaluation of one receiver attachment.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum DomainClockAttachReceipt {
    Accepted,
    ForeignTarget,
    UnavailableTarget,
    SupersededAuthority,
    StaleRevision,
}

impl InterconnectRequest for DomainClockAttachRequest {
    type Response = DomainClockAttachReceipt;

    const NAME: &'static str = "domain_clock_attach";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Progress;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "clock attachment and progress requests must share one attempt deadline"
        )
    )]
    const DEADLINE: crate::RequestDeadline = DomainClockProgressRequest::DEADLINE;
}

impl InterconnectRequest for DomainClockProgressRequest {
    type Response = ();

    const NAME: &'static str = "domain_clock_progress";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Progress;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Coordination;
}

/// A typed request that opens one ordered bidirectional frame stream.
///
/// The initiator submits `Item` frames after the opening message and the responder answers with
/// `Response` frames. Both directions preserve submission order, and each side half-closes its own
/// direction independently, so an idle stream stays open.
pub trait InterconnectDuplexRequest: RkyvMessage {
    /// The frames the initiator submits after the opening message.
    type Item: RkyvMessage;
    /// The frames the responder produces.
    type Response: RkyvMessage;

    const NAME: &'static str;
    const CLASS: PoolClass;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    /// How long opening the stream may take. It is not a bound on the stream's lifetime.
    const SETUP_DEADLINE: crate::DuplexSetupDeadline;
    const REQUIRES_LIVE_TARGET: bool = true;
}

/// A typed request whose response is an incrementally flow-controlled byte stream.
pub trait InterconnectStreamRequest: RkyvMessage {
    const NAME: &'static str;
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    /// The operation's relative budget, resolved by its transport from configured settings.
    const DEADLINE: crate::RequestDeadline;
    const REQUIRES_LIVE_TARGET: bool = true;

    /// Bind a coordinated stream to the authenticated peer process before its handler runs.
    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        None
    }
}

#[derive(Debug, Error)]
pub enum StreamHandlerError {
    #[error("{message}")]
    Failed { message: String },
    #[error("the producer's described state changed before the transfer opened")]
    PreconditionChanged,
}

impl StreamHandlerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }

    /// Keep the producer's typed failure beneath the handler context sent over the stream.
    pub fn with_cause<C: error_stack::Context>(cause: Report<C>) -> Report<Self> {
        let message = cause.to_string();
        cause.change_context(Self::new(message))
    }
}

/// The opening attempt's existing absolute bound, retained only on a precondition refusal.
#[derive(Debug)]
pub(crate) struct StreamPreconditionDeadline(Instant);

impl StreamPreconditionDeadline {
    pub(crate) fn attach(
        error: Report<TransportError>,
        deadline: Instant,
    ) -> Report<TransportError> {
        if let TransportError::RemoteRejected { status, .. } = error.current_context()
            && *status == http::StatusCode::PRECONDITION_FAILED.as_u16()
        {
            return error.attach(Self(deadline));
        }
        error
    }
}

pub type OutgoingByteStream =
    Pin<Box<dyn Stream<Item = Result<ChargedBytes, Report<StreamHandlerError>>> + Send + 'static>>;

pub(crate) type OutgoingFrameStream =
    Pin<Box<dyn Stream<Item = Result<ChargedBytes, Report<StreamHandlerError>>> + Send + 'static>>;

/// A producer-owned stream with its exact byte count declared before response headers are sent.
pub struct StreamingResponse {
    pub(crate) content_length: u64,
    pub(crate) chunks: OutgoingByteStream,
}

impl StreamingResponse {
    pub fn new<S>(content_length: u64, chunks: S) -> Self
    where
        S: Stream<Item = Result<ChargedBytes, Report<StreamHandlerError>>> + Send + 'static,
    {
        Self {
            content_length,
            chunks: Box::pin(chunks),
        }
    }
}

#[derive(Debug, Error)]
pub enum RequestError {
    #[error(
        "request '{request}' to node '{node}' expected process {expected}, but the connection \
         belongs to process {actual}"
    )]
    PeerProcessChanged {
        node: ClusterNodeName,
        request: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error("the {subquota:?} request subquota is full for '{request}'")]
    AdmissionFull {
        request: &'static str,
        subquota: RequestSubquota,
    },
    #[error("interconnect connection capacity is exhausted for '{request}' on node '{node}'")]
    ConnectionCapacityExhausted {
        node: ClusterNodeName,
        request: &'static str,
    },
    #[error("failed to encode interconnect request '{request}'")]
    Encode { request: &'static str },
    #[error("failed to decode the response for interconnect request '{request}'")]
    Decode { request: &'static str },
    #[error("transport shut down while requesting '{request}' from node '{node}'")]
    ShuttingDown {
        node: ClusterNodeName,
        request: &'static str,
    },
    #[error("target node '{node}' left the cluster while handling request '{request}'")]
    TargetLeft {
        node: ClusterNodeName,
        request: &'static str,
    },
    #[error("timed out after {timeout:?} waiting for request '{request}' on node '{node}'")]
    Timeout {
        node: ClusterNodeName,
        request: &'static str,
        timeout: Duration,
    },
    #[error("node '{node}' rejected interconnect request '{request}': {failure}")]
    RemoteRejected {
        node: ClusterNodeName,
        request: &'static str,
        failure: RemoteRequestFailure,
    },
    #[error("transport failed request '{request}' to node '{node}': {reason}")]
    Transport {
        node: ClusterNodeName,
        request: &'static str,
        reason: String,
    },
    #[error("node '{node}' returned the wrong response for request '{request}'")]
    ResponseMismatch {
        node: ClusterNodeName,
        request: &'static str,
    },
    #[error("streamed response for '{request}' from node '{node}' failed: {reason}")]
    Stream {
        node: ClusterNodeName,
        request: &'static str,
        reason: String,
    },
    #[error("the producer's described state changed while opening '{request}' on node '{node}'")]
    StreamPreconditionChanged {
        node: ClusterNodeName,
        request: &'static str,
        deadline: Instant,
    },
    #[error(
        "streamed response for '{request}' from node '{node}' declared {declared} bytes but \
         delivered {received}"
    )]
    StreamLengthMismatch {
        node: ClusterNodeName,
        request: &'static str,
        declared: u64,
        received: u64,
    },
    #[error(
        "streamed response for '{request}' from node '{node}' sent a {received}-byte chunk above \
         the {limit}-byte receiver bound"
    )]
    StreamChunkBound {
        node: ClusterNodeName,
        request: &'static str,
        received: u64,
        limit: u64,
    },
}

impl RequestError {
    pub(crate) fn stream_with_cause<C: error_stack::Context>(
        cause: Report<C>,
        node: ClusterNodeName,
        request: &'static str,
    ) -> Report<Self> {
        if let Some(deadline) = cause.downcast_ref::<StreamPreconditionDeadline>() {
            let deadline = deadline.0;
            return cause.change_context(Self::StreamPreconditionChanged {
                node,
                request,
                deadline,
            });
        }
        let reason = cause.to_string();
        cause.change_context(Self::Stream {
            node,
            request,
            reason,
        })
    }

    /// A changed stream precondition may be retried only within its original opening deadline.
    pub const fn stream_precondition_deadline(&self) -> Option<Instant> {
        match self {
            Self::StreamPreconditionChanged { deadline, .. } => Some(*deadline),
            _ => None,
        }
    }

    /// Whether this request was refused solely because a local or remote admission quota was full.
    pub fn is_capacity_exhaustion(&self) -> bool {
        match self {
            Self::AdmissionFull { .. }
            | Self::ConnectionCapacityExhausted { .. }
            | Self::RemoteRejected {
                failure:
                    RemoteRequestFailure::AdmissionFull { .. }
                    | RemoteRequestFailure::MemoryBudgetFull { .. }
                    | RemoteRequestFailure::ExecutionQueueFull { .. },
                ..
            } => true,
            Self::Encode { .. }
            | Self::PeerProcessChanged { .. }
            | Self::Decode { .. }
            | Self::ShuttingDown { .. }
            | Self::TargetLeft { .. }
            | Self::Timeout { .. }
            | Self::RemoteRejected { .. }
            | Self::Transport { .. }
            | Self::ResponseMismatch { .. }
            | Self::Stream { .. }
            | Self::StreamPreconditionChanged { .. }
            | Self::StreamLengthMismatch { .. } => false,
            Self::StreamChunkBound { .. } => false,
        }
    }
}

#[derive(Debug, Error)]
pub enum HandlerRegistrationError {
    #[error("a handler for interconnect request '{request}' is already registered")]
    AlreadyRegistered { request: &'static str },
    #[error("{subquota:?} request '{request}' does not belong to its configured pool")]
    SubquotaClassMismatch {
        request: &'static str,
        subquota: RequestSubquota,
    },
}

pub(crate) struct HandledResponse {
    envelope: ResponseEnvelope,
    payload_reservation: Option<Reservation>,
}

impl HandledResponse {
    pub(crate) fn into_parts(self) -> (ResponseEnvelope, Option<Reservation>) {
        (self.envelope, self.payload_reservation)
    }
}

/// Every registered handler, by operation name within each exchange form.
#[derive(Clone, Default)]
struct HandlerTable {
    requests: HashMap<&'static str, Arc<Box<dyn ErasedRequestHandler>>>,
    streams: HashMap<&'static str, Arc<Box<dyn ErasedStreamHandler>>>,
    duplexes: HashMap<&'static str, Arc<Box<dyn ErasedDuplexHandler>>>,
}

/// One handler waiting to be published, in the exchange form it answers.
enum HandlerRegistration {
    Request(Arc<Box<dyn ErasedRequestHandler>>),
    Stream(Arc<Box<dyn ErasedStreamHandler>>),
    Duplex(Arc<Box<dyn ErasedDuplexHandler>>),
}

impl HandlerTable {
    /// Whether `registration` would reuse a name this table already holds. A request handler's
    /// name is checked against request handlers, a stream handler's also against stream handlers,
    /// and a duplex handler's against all three forms.
    fn conflicts_with(&self, name: &str, registration: &HandlerRegistration) -> bool {
        match registration {
            HandlerRegistration::Request(_) => self.requests.contains_key(name),
            HandlerRegistration::Stream(_) => {
                self.requests.contains_key(name) || self.streams.contains_key(name)
            }
            HandlerRegistration::Duplex(_) => {
                self.requests.contains_key(name)
                    || self.streams.contains_key(name)
                    || self.duplexes.contains_key(name)
            }
        }
    }

    /// A copy of this table that also holds `registration` under `name`.
    fn with(&self, name: &'static str, registration: &HandlerRegistration) -> Self {
        let mut table = self.clone();
        match registration {
            HandlerRegistration::Request(handler) => {
                table.requests.insert(name, Arc::clone(handler));
            }
            HandlerRegistration::Stream(handler) => {
                table.streams.insert(name, Arc::clone(handler));
            }
            HandlerRegistration::Duplex(handler) => {
                table.duplexes.insert(name, Arc::clone(handler));
            }
        }
        table
    }
}

pub(crate) struct RequestState {
    /// Registration publishes a complete replacement table, so dispatch reads it without a lock.
    handlers: ArcSwap<HandlerTable>,
    /// The live membership discovery published last, or `None` before its first view, while every
    /// target counts as live so bootstrap discovery can reach its peers.
    live_nodes: ArcSwapOption<BTreeSet<ClusterNodeName>>,
    membership_changed: Notify,
    outbound: RequestQuotas,
    inbound: RequestQuotas,
    /// The same observations the transport records connection and relay events into, so a node
    /// reports one view of what its pools carried.
    observations: Arc<TransportObservations>,
}

pub(crate) struct RequestAdmission {
    _permit: OwnedSemaphorePermit,
}

struct RequestQuotas {
    shared: StdArc<Semaphore>,
    append: StdArc<Semaphore>,
    resource: StdArc<Semaphore>,
    snapshot: StdArc<Semaphore>,
    discovery: StdArc<Semaphore>,
    liveness: StdArc<Semaphore>,
    progress: StdArc<Semaphore>,
    admission: StdArc<Semaphore>,
    cancellation: StdArc<Semaphore>,
    terminal: StdArc<Semaphore>,
    /// What each subquota was built with, so the requests in flight can be read back out of the
    /// permits the subquota is currently holding.
    capacities: [usize; RequestSubquota::COUNT],
}

impl RequestQuotas {
    fn new(options: &crate::TransportOptions) -> Self {
        let capacity = options.incoming_queue_capacity;
        // Each reserve is independent; a full general queue never consumes liveness or completion.
        let standard = options.request_quota.min(capacity).max(1);
        let append = options.append_request_quota.min(capacity).max(1);
        let completion = options.completion_request_quota.min(capacity).max(1);
        let liveness = options.max_concurrent_health_probes;
        Self {
            shared: StdArc::new(Semaphore::new(capacity)),
            append: StdArc::new(Semaphore::new(append)),
            resource: StdArc::new(Semaphore::new(standard)),
            snapshot: StdArc::new(Semaphore::new(standard)),
            discovery: StdArc::new(Semaphore::new(standard)),
            liveness: StdArc::new(Semaphore::new(liveness)),
            progress: StdArc::new(Semaphore::new(standard)),
            admission: StdArc::new(Semaphore::new(standard)),
            cancellation: StdArc::new(Semaphore::new(completion)),
            terminal: StdArc::new(Semaphore::new(completion)),
            capacities: [
                capacity, append, standard, standard, standard, liveness, standard, standard,
                completion, completion,
            ],
        }
    }

    fn for_subquota(&self, subquota: RequestSubquota) -> &StdArc<Semaphore> {
        match subquota {
            RequestSubquota::Shared => &self.shared,
            RequestSubquota::Append => &self.append,
            RequestSubquota::Resource => &self.resource,
            RequestSubquota::Snapshot => &self.snapshot,
            RequestSubquota::Discovery => &self.discovery,
            RequestSubquota::Liveness => &self.liveness,
            RequestSubquota::Progress => &self.progress,
            RequestSubquota::Admission => &self.admission,
            RequestSubquota::Cancellation => &self.cancellation,
            RequestSubquota::Terminal => &self.terminal,
        }
    }

    fn try_admit(&self, subquota: RequestSubquota) -> Option<RequestAdmission> {
        let permit = StdArc::clone(self.for_subquota(subquota))
            .try_acquire_owned()
            .ok()?;
        Some(RequestAdmission { _permit: permit })
    }

    /// Requests admitted into each subquota and not yet resolved.
    fn in_flight(&self) -> [usize; RequestSubquota::COUNT] {
        let mut pending = [0_usize; RequestSubquota::COUNT];
        for subquota in RequestSubquota::iter() {
            let capacity = self.capacities[subquota.index()];
            let available = self.for_subquota(subquota).available_permits();
            pending[subquota.index()] = capacity.checked_sub(available).verified(
                "subquota permits are only taken and returned by the requests this node admitted",
            );
        }
        pending
    }
}

fn subquota_belongs_to_class(subquota: RequestSubquota, class: PoolClass) -> bool {
    match subquota {
        RequestSubquota::Shared => true,
        RequestSubquota::Append => class == PoolClass::Replication,
        RequestSubquota::Resource | RequestSubquota::Snapshot => class == PoolClass::Bulk,
        RequestSubquota::Discovery
        | RequestSubquota::Liveness
        | RequestSubquota::Progress
        | RequestSubquota::Admission
        | RequestSubquota::Cancellation
        | RequestSubquota::Terminal => class == PoolClass::Management,
    }
}

impl RequestState {
    pub(crate) fn new(
        options: &crate::TransportOptions,
        observations: Arc<TransportObservations>,
    ) -> Self {
        Self {
            handlers: ArcSwap::from_pointee(HandlerTable::default()),
            live_nodes: ArcSwapOption::empty(),
            membership_changed: Notify::new(),
            outbound: RequestQuotas::new(options),
            inbound: RequestQuotas::new(options),
            observations,
        }
    }

    pub(crate) fn try_admit_outbound(&self, subquota: RequestSubquota) -> Option<RequestAdmission> {
        let admitted = self.outbound.try_admit(subquota);
        if admitted.is_none() {
            self.observations
                .quota_failure(ConnectionDirection::Outbound, subquota);
        }
        admitted
    }

    fn try_admit_inbound(&self, subquota: RequestSubquota) -> Option<RequestAdmission> {
        let admitted = self.inbound.try_admit(subquota);
        if admitted.is_none() {
            self.observations
                .quota_failure(ConnectionDirection::Inbound, subquota);
        }
        admitted
    }

    /// Count one finished outbound request and the round trip it took.
    pub(crate) fn record_request(
        &self,
        subquota: RequestSubquota,
        outcome: RequestOutcome,
        elapsed: Duration,
    ) {
        self.observations
            .request_completed(subquota, outcome, elapsed);
    }

    /// Requests admitted and not yet resolved, by direction and reserved subquota.
    pub(crate) fn pending_operations(
        &self,
    ) -> [[usize; RequestSubquota::COUNT]; ConnectionDirection::COUNT] {
        let mut pending = [[0_usize; RequestSubquota::COUNT]; ConnectionDirection::COUNT];
        pending[ConnectionDirection::Outbound.index()] = self.outbound.in_flight();
        pending[ConnectionDirection::Inbound.index()] = self.inbound.in_flight();
        pending
    }
}

type HandlerFuture = Pin<
    Box<dyn Future<Output = Result<(Vec<u8>, Reservation), RemoteRequestFailure>> + Send + 'static>,
>;

/// A stream opening either refuses the request or reports its operation's typed outcome.
#[derive(Debug, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the fixed stream-opening response distinguishes request failures from operation \
                  refusals"
    )
)]
pub(crate) enum StreamOpeningFailure {
    #[error(transparent)]
    Request(#[from] RemoteRequestFailure),
    #[error("the operation refused the stream")]
    Operation {
        kind: RemoteOperationFailureKind,
        report: Report<StreamHandlerError>,
    },
}

impl StreamOpeningFailure {
    fn handler(error: Report<StreamHandlerError>) -> Self {
        if matches!(
            error.current_context(),
            StreamHandlerError::PreconditionChanged
        ) {
            return Self::Request(RemoteRequestFailure::PreconditionChanged);
        }
        match error.downcast_ref::<RemoteOperationFailure>() {
            Some(failure) => Self::Operation {
                kind: RemoteOperationFailureKind::from(failure),
                report: error,
            },
            None => Self::Request(RemoteRequestFailure::ResponseEncode(format!("{error:#}"))),
        }
    }
}

type StreamHandlerFuture =
    Pin<Box<dyn Future<Output = Result<StreamingResponse, StreamOpeningFailure>> + Send + 'static>>;

type DuplexHandlerFuture = Pin<
    Box<dyn Future<Output = Result<OutgoingFrameStream, RemoteRequestFailure>> + Send + 'static>,
>;

trait ErasedRequestHandler: Send + Sync {
    fn class(&self) -> PoolClass;
    fn subquota(&self) -> RequestSubquota;

    fn handle(
        &self,
        executor: Executor,
        payload_limit: u64,
        context: RequestContext,
        payload: Vec<u8>,
    ) -> HandlerFuture;
}

trait ErasedStreamHandler: Send + Sync {
    fn class(&self) -> PoolClass;
    fn subquota(&self) -> RequestSubquota;

    fn handle(
        &self,
        executor: Executor,
        context: RequestContext,
        payload: Vec<u8>,
    ) -> StreamHandlerFuture;
}

trait ErasedDuplexHandler: Send + Sync {
    fn class(&self) -> PoolClass;
    fn subquota(&self) -> RequestSubquota;

    fn handle(
        &self,
        executor: Executor,
        context: RequestContext,
        payload: Vec<u8>,
        frames: FrameReader,
    ) -> DuplexHandlerFuture;
}

struct TypedRequestHandler<M, H> {
    handler: Arc<H>,
    request: PhantomData<fn(M)>,
}

struct TypedStreamHandler<M, H> {
    handler: Arc<H>,
    request: PhantomData<fn(M)>,
}

struct TypedDuplexHandler<M, H> {
    handler: Arc<H>,
    request: PhantomData<fn(M)>,
}

impl<M, H, F> ErasedDuplexHandler for TypedDuplexHandler<M, H>
where
    M: InterconnectDuplexRequest,
    H: Fn(RequestContext, M, DuplexItems<M::Item>) -> F + Send + Sync + 'static,
    F: Future<Output = Result<DuplexResponses<M::Response>, Report<StreamHandlerError>>>
        + Send
        + 'static,
{
    fn class(&self) -> PoolClass {
        M::CLASS
    }

    fn subquota(&self) -> RequestSubquota {
        M::SUBQUOTA
    }

    fn handle(
        &self,
        executor: Executor,
        context: RequestContext,
        payload: Vec<u8>,
        frames: FrameReader,
    ) -> DuplexHandlerFuture {
        let handler = self.handler.clone();
        Box::pin(async move {
            let codec = DuplexCodec::of::<M>(&executor);
            let ChargedItem {
                item: request,
                charge: _request_charge,
            } = codec
                .decode_opening::<M>(payload)
                .await
                .map_err(|error| RemoteRequestFailure::decode(error, M::CLASS))?;
            let items = DuplexItems::new(frames, codec.clone(), M::NAME);
            let responses = (handler)(context, request, items)
                .await
                .map_err(|error| RemoteRequestFailure::ResponseEncode(error.to_string()))?;
            let frames = responses.into_stream().then(move |response| {
                // The encoding starts here, so the answer keeps no borrow of the codec. It waits for
                // a place on a full class, and the stream's reset ends that wait.
                let encoding = response.map(|response| codec.encode_answer(response));
                async move {
                    let encoded = encoding?.await.map_err(StreamHandlerError::with_cause)?;
                    Ok(ChargedBytes::from_owned(encoded.bytes, encoded.charge))
                }
            });
            let frames: OutgoingFrameStream = Box::pin(frames);
            Ok(frames)
        })
    }
}

impl<M, H, F> ErasedStreamHandler for TypedStreamHandler<M, H>
where
    M: InterconnectStreamRequest,
    H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
    F: Future<Output = Result<StreamingResponse, Report<StreamHandlerError>>> + Send + 'static,
{
    fn class(&self) -> PoolClass {
        M::CLASS
    }

    fn subquota(&self) -> RequestSubquota {
        M::SUBQUOTA
    }

    fn handle(
        &self,
        executor: Executor,
        context: RequestContext,
        payload: Vec<u8>,
    ) -> StreamHandlerFuture {
        let handler = self.handler.clone();
        Box::pin(async move {
            let (request, _request_reservation) = M::decode_rkyv(executor, M::CLASS, payload)
                .await
                .map_err(|error| RemoteRequestFailure::decode(error, M::CLASS))?;
            if let Some(identity) = request.coordination_identity()
                && !context.authenticates(identity)
            {
                return Err(RemoteRequestFailure::CoordinationIdentityMismatch.into());
            }
            (handler)(context, request)
                .await
                .map_err(StreamOpeningFailure::handler)
        })
    }
}

pub(crate) struct HandledByteStream {
    response: StreamingResponse,
    admission: RequestAdmission,
}

impl HandledByteStream {
    pub(crate) fn into_parts(self) -> (StreamingResponse, RequestAdmission) {
        (self.response, self.admission)
    }
}

pub(crate) struct HandledDuplexStream {
    responses: OutgoingFrameStream,
    admission: RequestAdmission,
}

impl HandledDuplexStream {
    pub(crate) fn into_parts(self) -> (OutgoingFrameStream, RequestAdmission) {
        (self.responses, self.admission)
    }
}

impl<M, H, F> ErasedRequestHandler for TypedRequestHandler<M, H>
where
    M: InterconnectRequest,
    H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
    F: Future<Output = M::Response> + Send + 'static,
{
    fn class(&self) -> PoolClass {
        M::CLASS
    }

    fn subquota(&self) -> RequestSubquota {
        M::SUBQUOTA
    }

    fn handle(
        &self,
        executor: Executor,
        payload_limit: u64,
        context: RequestContext,
        payload: Vec<u8>,
    ) -> HandlerFuture {
        let handler = self.handler.clone();
        Box::pin(async move {
            let (request, _request_reservation) =
                M::decode_rkyv(executor.clone(), M::CLASS, payload)
                    .await
                    .map_err(|error| RemoteRequestFailure::decode(error, M::CLASS))?;
            if let Some(identity) = request.coordination_identity()
                && !context.authenticates(identity)
            {
                return Err(RemoteRequestFailure::CoordinationIdentityMismatch);
            }
            let response = (handler)(context, request).await;
            // The handler already did its work, so a full queue holds the answer back rather than
            // throwing that work away. The caller's reset still ends the wait.
            response
                .encode_rkyv_with(
                    executor,
                    M::CLASS,
                    payload_limit,
                    QueueAdmission::WaitForPlace,
                )
                .await
                .map_err(|error| RemoteRequestFailure::ResponseEncode(error.to_string()))
        })
    }
}

impl RequestState {
    pub(crate) fn register<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = M::Response> + Send + 'static,
    {
        if !subquota_belongs_to_class(M::SUBQUOTA, M::CLASS) {
            return Err(Report::new(
                HandlerRegistrationError::SubquotaClassMismatch {
                    request: M::NAME,
                    subquota: M::SUBQUOTA,
                },
            ));
        }
        let erased: Box<dyn ErasedRequestHandler> = Box::new(TypedRequestHandler::<M, H> {
            handler: Arc::new(handler),
            request: PhantomData,
        });
        self.publish_handler(M::NAME, HandlerRegistration::Request(Arc::new(erased)))
    }

    pub(crate) fn register_stream<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectStreamRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = Result<StreamingResponse, Report<StreamHandlerError>>> + Send + 'static,
    {
        if !subquota_belongs_to_class(M::SUBQUOTA, M::CLASS) {
            return Err(Report::new(
                HandlerRegistrationError::SubquotaClassMismatch {
                    request: M::NAME,
                    subquota: M::SUBQUOTA,
                },
            ));
        }
        let erased: Box<dyn ErasedStreamHandler> = Box::new(TypedStreamHandler::<M, H> {
            handler: Arc::new(handler),
            request: PhantomData,
        });
        self.publish_handler(M::NAME, HandlerRegistration::Stream(Arc::new(erased)))
    }

    /// Publish a handler table that adds `registration` under `name`. A registration racing
    /// another rebuilds from whichever table was published first, so neither one is lost.
    fn publish_handler(
        &self,
        name: &'static str,
        registration: HandlerRegistration,
    ) -> Result<(), Report<HandlerRegistrationError>> {
        loop {
            let current = self.handlers.load_full();
            if current.conflicts_with(name, &registration) {
                return Err(Report::new(HandlerRegistrationError::AlreadyRegistered {
                    request: name,
                }));
            }
            let replacement = StdArc::new(current.with(name, &registration));
            let previous = self.handlers.compare_and_swap(&current, replacement);
            if StdArc::ptr_eq(&previous, &current) {
                return Ok(());
            }
        }
    }

    pub(crate) fn register_duplex<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectDuplexRequest,
        H: Fn(RequestContext, M, DuplexItems<M::Item>) -> F + Send + Sync + 'static,
        F: Future<Output = Result<DuplexResponses<M::Response>, Report<StreamHandlerError>>>
            + Send
            + 'static,
    {
        if !subquota_belongs_to_class(M::SUBQUOTA, M::CLASS) {
            return Err(Report::new(
                HandlerRegistrationError::SubquotaClassMismatch {
                    request: M::NAME,
                    subquota: M::SUBQUOTA,
                },
            ));
        }
        let erased: Box<dyn ErasedDuplexHandler> = Box::new(TypedDuplexHandler::<M, H> {
            handler: Arc::new(handler),
            request: PhantomData,
        });
        self.publish_handler(M::NAME, HandlerRegistration::Duplex(Arc::new(erased)))
    }

    pub(crate) async fn handle_duplex(
        &self,
        executor: &Executor,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        peer_process_epoch: u64,
        request: RequestEnvelope,
        frames: FrameReader,
    ) -> Result<HandledDuplexStream, RemoteRequestFailure> {
        let handler = self
            .handlers
            .load()
            .duplexes
            .get(request.request.as_str())
            .cloned();
        let payload_limit = request.class.payload_limit(executor);
        let payload_bytes = u64::try_from(request.payload.len())
            .assured("supported targets have a pointer width no larger than u64");
        let handler = match handler {
            Some(_) if payload_bytes > payload_limit => {
                return Err(RemoteRequestFailure::PayloadTooLarge {
                    actual: payload_bytes,
                    limit: payload_limit,
                });
            }
            Some(handler) if handler.class() == request.class => handler,
            Some(handler) => {
                return Err(RemoteRequestFailure::WrongPoolClass {
                    expected: handler.class(),
                    actual: request.class,
                });
            }
            None => return Err(RemoteRequestFailure::HandlerNotRegistered),
        };
        let subquota = handler.subquota();
        let admission = self
            .try_admit_inbound(subquota)
            .ok_or(RemoteRequestFailure::AdmissionFull { subquota })?;
        let responses = handler
            .handle(
                executor.clone(),
                RequestContext {
                    peer_node_id,
                    peer_advertised_host,
                    peer_process_epoch,
                },
                request.payload,
                frames,
            )
            .await?;
        Ok(HandledDuplexStream {
            responses,
            admission,
        })
    }

    pub(crate) async fn handle_stream(
        &self,
        executor: &Executor,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        peer_process_epoch: u64,
        request: RequestEnvelope,
    ) -> Result<HandledByteStream, StreamOpeningFailure> {
        let handler = self
            .handlers
            .load()
            .streams
            .get(request.request.as_str())
            .cloned();
        let payload_limit = request.class.payload_limit(executor);
        let payload_bytes = u64::try_from(request.payload.len())
            .assured("supported targets have a pointer width no larger than u64");
        let handler = match handler {
            Some(_) if payload_bytes > payload_limit => {
                return Err(RemoteRequestFailure::PayloadTooLarge {
                    actual: payload_bytes,
                    limit: payload_limit,
                }
                .into());
            }
            Some(handler) if handler.class() == request.class => handler,
            Some(handler) => {
                return Err(RemoteRequestFailure::WrongPoolClass {
                    expected: handler.class(),
                    actual: request.class,
                }
                .into());
            }
            None => return Err(RemoteRequestFailure::HandlerNotRegistered.into()),
        };
        let subquota = handler.subquota();
        let admission = self
            .try_admit_inbound(subquota)
            .ok_or(RemoteRequestFailure::AdmissionFull { subquota })?;
        let response = handler
            .handle(
                executor.clone(),
                RequestContext {
                    peer_node_id,
                    peer_advertised_host,
                    peer_process_epoch,
                },
                request.payload,
            )
            .await?;
        Ok(HandledByteStream {
            response,
            admission,
        })
    }

    pub(crate) async fn handle(
        &self,
        executor: &Executor,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        peer_process_epoch: u64,
        request: RequestEnvelope,
    ) -> HandledResponse {
        let handler = self
            .handlers
            .load()
            .requests
            .get(request.request.as_str())
            .cloned();
        let payload_limit = request.class.payload_limit(executor);
        let payload_bytes = u64::try_from(request.payload.len())
            .assured("supported targets have a pointer width no larger than u64");
        let result = match handler {
            Some(_) if payload_bytes > payload_limit => {
                Err(RemoteRequestFailure::PayloadTooLarge {
                    actual: payload_bytes,
                    limit: payload_limit,
                })
            }
            Some(handler) if handler.class() == request.class => {
                let subquota = handler.subquota();
                match self.try_admit_inbound(subquota) {
                    Some(_admission) => {
                        handler
                            .handle(
                                executor.clone(),
                                payload_limit,
                                RequestContext {
                                    peer_node_id,
                                    peer_advertised_host,
                                    peer_process_epoch,
                                },
                                request.payload,
                            )
                            .await
                    }
                    None => Err(RemoteRequestFailure::AdmissionFull { subquota }),
                }
            }
            Some(handler) => Err(RemoteRequestFailure::WrongPoolClass {
                expected: handler.class(),
                actual: request.class,
            }),
            None => Err(RemoteRequestFailure::HandlerNotRegistered),
        };
        let (result, payload_reservation) = match result {
            Ok((payload, reservation)) => (Ok(payload), Some(reservation)),
            Err(error) => (Err(error), None),
        };
        HandledResponse {
            envelope: ResponseEnvelope {
                class: request.class,
                request: request.request,
                result,
            },
            payload_reservation,
        }
    }

    pub(crate) fn target_is_live(&self, node: &ClusterNodeName) -> bool {
        let live_nodes = self.live_nodes.load();
        match &*live_nodes {
            Some(nodes) => nodes.contains(node),
            None => true,
        }
    }

    pub(crate) fn replace_live_nodes(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
        self.live_nodes.store(Some(StdArc::new(live_nodes.clone())));
        self.membership_changed.notify_waiters();
    }

    async fn target_left(&self, node: &ClusterNodeName) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let changed = self.membership_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.target_is_live(node) {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn shutdown(&self) {
        self.handlers.store(StdArc::new(HandlerTable::default()));
        self.membership_changed.notify_waiters();
    }
}

impl Transport {
    pub fn request_deadline<M: InterconnectRequest>(&self) -> Duration {
        M::DEADLINE.resolve(self.inner.request_timeout(), self.liveness())
    }
    pub fn stream_request_deadline<M: InterconnectStreamRequest>(&self) -> Duration {
        M::DEADLINE.resolve(self.inner.request_timeout(), self.liveness())
    }
    pub fn duplex_setup_deadline<M: InterconnectDuplexRequest>(&self) -> Duration {
        M::SETUP_DEADLINE.resolve(
            self.inner.connection_setup_timeout(),
            self.inner.request_timeout(),
            self.liveness(),
        )
    }
}

impl Transport {
    pub fn register_handler<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = M::Response> + Send + 'static,
    {
        self.inner.requests().register::<M, H, F>(handler)
    }

    pub fn register_stream_handler<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectStreamRequest,
        H: Fn(RequestContext, M) -> F + Send + Sync + 'static,
        F: Future<Output = Result<StreamingResponse, Report<StreamHandlerError>>> + Send + 'static,
    {
        self.inner.requests().register_stream::<M, H, F>(handler)
    }

    /// Answer one ordered bidirectional frame stream a peer opens.
    pub fn register_duplex_handler<M, H, F>(
        &self,
        handler: H,
    ) -> Result<(), Report<HandlerRegistrationError>>
    where
        M: InterconnectDuplexRequest,
        H: Fn(RequestContext, M, DuplexItems<M::Item>) -> F + Send + Sync + 'static,
        F: Future<Output = Result<DuplexResponses<M::Response>, Report<StreamHandlerError>>>
            + Send
            + 'static,
    {
        self.inner.requests().register_duplex::<M, H, F>(handler)
    }

    /// Open one ordered bidirectional frame stream to `node`.
    ///
    /// The setup deadline covers reaching the peer and its acceptance. Once the stream is open it
    /// stays open until a half-close or a transport failure; an idle stream is not a failure, so
    /// the caller applies its own deadline to whatever work it has outstanding.
    pub async fn open_duplex_stream<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
    ) -> Result<(DuplexSender<M>, DuplexReceiver<M>), Report<RequestError>>
    where
        M: InterconnectDuplexRequest,
    {
        if self.inner.is_shutting_down() {
            return Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        if M::REQUIRES_LIVE_TARGET && !self.inner.requests().target_is_live(node) {
            return Err(Report::new(RequestError::TargetLeft {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        let admission = self
            .inner
            .requests()
            .try_admit_outbound(M::SUBQUOTA)
            .ok_or_else(|| {
                Report::new(RequestError::AdmissionFull {
                    request: M::NAME,
                    subquota: M::SUBQUOTA,
                })
            })?;
        let EncodedMessage {
            bytes: payload,
            charge: _payload_charge,
        } = DuplexCodec::of::<M>(self.inner.executor())
            .encode_opening(message)
            .await
            .map_err(|error| error.change_context(RequestError::Encode { request: M::NAME }))?;
        let opening = RequestEnvelope {
            class: M::CLASS,
            request: M::NAME.to_string(),
            payload,
        };
        let body = wire::encode_rkyv(
            self.inner.executor(),
            M::CLASS.memory_class(),
            M::CLASS.cpu_class(),
            M::CLASS.control_body_limit(self.inner.executor()),
            opening,
        )
        .await
        .map_err(|error| error.change_context(RequestError::Encode { request: M::NAME }))?;
        self.inner
            .open_duplex_stream::<M>(
                node,
                RawDuplexRequest {
                    class: M::CLASS,
                    subquota: M::SUBQUOTA,
                    body,
                    timeout: self.duplex_setup_deadline::<M>(),
                    admission,
                },
            )
            .await
            .map_err(|error| RequestError::stream_with_cause(error, node.clone(), M::NAME))
    }

    pub fn replace_live_nodes(&self, live_nodes: &BTreeSet<ClusterNodeName>) {
        self.inner.requests().replace_live_nodes(live_nodes);
        self.inner.retire_departed_connections(live_nodes);
    }

    pub async fn request<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
    ) -> Result<M::Response, Report<RequestError>>
    where
        M: InterconnectRequest,
    {
        self.request_with_timeout(node, message, self.request_deadline::<M>())
            .await
    }

    /// Refuses `message` when one typed request of its kind could not carry it on this node: its
    /// archive above the class's payload limit, or that archive inside the request envelope a send
    /// writes above the class's control body limit. It measures both encodings without sending
    /// anything or charging a budget, so an owner that holds its largest request to a bound checks
    /// that bound with it before it sends anything.
    pub fn check_request_fits<M>(&self, message: &M) -> Result<(), Report<RequestFitError>>
    where
        M: InterconnectRequest
            + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    {
        let executor = self.inner.executor();
        let payload = rkyv::to_bytes::<RkyvError>(message)
            .map_err(|error| RequestFitError::archive(error, M::NAME))?
            .into_vec();
        let payload_bytes: u64 = payload.len().arch_into();
        let payload_limit = M::CLASS.payload_limit(executor);
        if payload_bytes > payload_limit {
            return Err(Report::new(RequestFitError::Payload {
                request: M::NAME,
                bytes: payload_bytes,
                limit: payload_limit,
            }));
        }
        let control = ControlEnvelope::Request(RequestEnvelope {
            class: M::CLASS,
            request: M::NAME.to_string(),
            payload,
        });
        let body_bytes: u64 = rkyv::to_bytes::<RkyvError>(&control)
            .map_err(|error| RequestFitError::archive(error, M::NAME))?
            .len()
            .arch_into();
        let body_limit = M::CLASS.control_body_limit(executor);
        if body_bytes > body_limit {
            return Err(Report::new(RequestFitError::Body {
                request: M::NAME,
                bytes: body_bytes,
                limit: body_limit,
            }));
        }
        Ok(())
    }

    pub async fn request_stream<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
    ) -> Result<super::IncomingByteStream, Report<RequestError>>
    where
        M: InterconnectStreamRequest,
    {
        self.request_stream_with_timeout(node, message, self.stream_request_deadline::<M>())
            .await
    }

    /// Open the same admitted stream with the caller's remaining operation deadline.
    pub async fn request_stream_with_timeout<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
        timeout: Duration,
    ) -> Result<super::IncomingByteStream, Report<RequestError>>
    where
        M: InterconnectStreamRequest,
    {
        if self.inner.is_shutting_down() {
            return Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        if M::REQUIRES_LIVE_TARGET && !self.inner.requests().target_is_live(node) {
            return Err(Report::new(RequestError::TargetLeft {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        let admission = self
            .inner
            .requests()
            .try_admit_outbound(M::SUBQUOTA)
            .ok_or_else(|| {
                Report::new(RequestError::AdmissionFull {
                    request: M::NAME,
                    subquota: M::SUBQUOTA,
                })
            })?;
        let (payload, payload_reservation) = message
            .encode_rkyv(
                self.inner.executor().clone(),
                M::CLASS,
                M::CLASS.payload_limit(self.inner.executor()),
            )
            .await
            .map_err(|error| error.change_context(RequestError::Encode { request: M::NAME }))?;
        let request = StreamRequestEnvelope {
            receive: self.inner.local_receive_bounds(),
            request: RequestEnvelope {
                class: M::CLASS,
                request: M::NAME.to_string(),
                payload,
            },
        };
        let body = wire::encode_stream_request(self.inner.executor(), request, payload_reservation)
            .await
            .map_err(|error| error.change_context(RequestError::Encode { request: M::NAME }))?;
        self.inner
            .open_byte_stream(
                node,
                OutboundByteStreamRequest {
                    class: M::CLASS,
                    subquota: M::SUBQUOTA,
                    name: M::NAME,
                    body,
                    timeout,
                    admission,
                },
            )
            .await
            .map_err(|error| RequestError::stream_with_cause(error, node.clone(), M::NAME))
    }

    /// Send one typed request with a caller-owned end-to-end deadline.
    pub fn request_with_timeout<M>(
        &self,
        node: &ClusterNodeName,
        message: M,
        timeout_duration: Duration,
    ) -> impl Future<Output = Result<M::Response, Report<RequestError>>>
    where
        M: InterconnectRequest,
    {
        self.request_with_fence(node, message, timeout_duration, ())
    }

    /// Send only through a connection to the named authenticated process. A replacement process
    /// is refused before any request is sent, without adding metadata to the request payload.
    pub fn request_to_process<M>(
        &self,
        node: &ClusterNodeName,
        peer_process_epoch: u64,
        message: M,
    ) -> impl Future<Output = Result<M::Response, Report<RequestError>>>
    where
        M: InterconnectRequest,
    {
        self.request_with_fence(
            node,
            message,
            self.request_deadline::<M>(),
            peer_process_epoch,
        )
    }

    async fn request_with_fence<M, F>(
        &self,
        node: &ClusterNodeName,
        message: M,
        timeout_duration: Duration,
        fence: F,
    ) -> Result<M::Response, Report<RequestError>>
    where
        M: InterconnectRequest,
        F: PeerProcessFence,
    {
        if self.inner.is_shutting_down() {
            return Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        if M::REQUIRES_LIVE_TARGET && !self.inner.requests().target_is_live(node) {
            return Err(Report::new(RequestError::TargetLeft {
                node: node.clone(),
                request: M::NAME,
            }));
        }
        let operation = async {
            let _admission = self
                .inner
                .requests()
                .try_admit_outbound(M::SUBQUOTA)
                .ok_or_else(|| {
                    Report::new(RequestError::AdmissionFull {
                        request: M::NAME,
                        subquota: M::SUBQUOTA,
                    })
                })?;
            let (payload, _payload_reservation) = message
                .encode_rkyv(
                    self.inner.executor().clone(),
                    M::CLASS,
                    M::CLASS.payload_limit(self.inner.executor()),
                )
                .await
                .map_err(|error| error.change_context(RequestError::Encode { request: M::NAME }))?;
            let request = ControlEnvelope::Request(RequestEnvelope {
                class: M::CLASS,
                request: M::NAME.to_string(),
                payload,
            });
            let response = {
                let mapping_input = self
                    .inner
                    .round_trip_control(node, request, M::SUBQUOTA, timeout_duration, fence)
                    .await;
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(error) => Err({
                        let request_error = match error.current_context() {
                            super::TransportError::PeerProcessChanged {
                                expected, actual, ..
                            } => RequestError::PeerProcessChanged {
                                node: node.clone(),
                                request: M::NAME,
                                expected: *expected,
                                actual: *actual,
                            },
                            super::TransportError::ShuttingDown => RequestError::ShuttingDown {
                                node: node.clone(),
                                request: M::NAME,
                            },
                            super::TransportError::RequestTimeout { .. }
                            | super::TransportError::ProgressTimeout { .. } => {
                                RequestError::Timeout {
                                    node: node.clone(),
                                    request: M::NAME,
                                    timeout: timeout_duration,
                                }
                            }
                            super::TransportError::PoolExhausted => {
                                RequestError::ConnectionCapacityExhausted {
                                    node: node.clone(),
                                    request: M::NAME,
                                }
                            }
                            cause => RequestError::Transport {
                                node: node.clone(),
                                request: M::NAME,
                                reason: cause.to_string(),
                            },
                        };
                        error.change_context(request_error)
                    }),
                }
            }?;
            let (response, _response_reservation) = response.into_parts();
            let ControlEnvelope::Response(response) = response else {
                return Err(Report::new(RequestError::ResponseMismatch {
                    node: node.clone(),
                    request: M::NAME,
                }));
            };
            if response.class != M::CLASS || response.request != M::NAME {
                return Err(Report::new(RequestError::ResponseMismatch {
                    node: node.clone(),
                    request: M::NAME,
                }));
            }
            match response.result {
                Ok(payload) => {
                    // The peer already did the work, so a full queue holds the answer back until
                    // the request's own deadline rather than discarding it.
                    let mapping_input = M::Response::decode_rkyv_with(
                        self.inner.executor().clone(),
                        M::CLASS,
                        QueueAdmission::WaitForPlace,
                        payload,
                    )
                    .await
                    .map(|(response, _response_payload_reservation)| response);
                    match mapping_input {
                        Ok(mapped_value) => Ok(mapped_value),
                        Err(error) => {
                            Err(error.change_context(RequestError::Decode { request: M::NAME }))
                        }
                    }
                }
                Err(failure) => Err(Report::new(RequestError::RemoteRejected {
                    node: node.clone(),
                    request: M::NAME,
                    failure,
                })),
            }
        };
        let shutdown = self.inner.shutdown_token();
        let submitted_at = Instant::now();
        let result = nervix_primitives::select! {
            biased;
            _ = shutdown.cancelled() => Err(Report::new(RequestError::ShuttingDown {
                node: node.clone(),
                request: M::NAME,
            })),
            _ = self.inner.requests().target_left(node), if M::REQUIRES_LIVE_TARGET => {
                Err(Report::new(RequestError::TargetLeft {
                    node: node.clone(),
                    request: M::NAME,
                }))
            }
            result = timeout(timeout_duration, operation) => match result {
                Ok(result) => result,
                Err(_) => Err(Report::new(RequestError::Timeout {
                    node: node.clone(),
                    request: M::NAME,
                    timeout: timeout_duration,
                })),
            },
        };
        let outcome = match &result {
            Ok(_) => RequestOutcome::Answered,
            Err(_) => RequestOutcome::Failed,
        };
        self.inner
            .requests()
            .record_request(M::SUBQUOTA, outcome, submitted_at.elapsed());
        result
    }
}

impl InterconnectRequest for DescribeIngestorRequest {
    type Response = Result<IngestorDescribeEnvelope, RemoteOperationFailure>;

    const NAME: &'static str = "describe_ingestor";
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Request;
}

impl InterconnectRequest for CaptureOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<Vec<OwnershipHandoffCheckpoint>>;

    const NAME: &'static str = "capture_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for PrepareOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "prepare_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for ConfirmOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "confirm_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for PrepareForcedOwnershipRecoveryRequest {
    type Response = OwnershipHandoffResponse<ForcedOwnershipRecoveryPreparation>;

    const NAME: &'static str = "prepare_forced_ownership_recovery";
    const CLASS: PoolClass = PoolClass::Replication;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.round.coordination)
    }
}

impl InterconnectRequest for DiscardForcedOwnershipRecoveryRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "discard_forced_ownership_recovery";
    const CLASS: PoolClass = PoolClass::Replication;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.preparation.round.coordination)
    }
}

impl InterconnectRequest for ActivateOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "activate_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for DiscardOwnershipHandoffStateRequest {
    type Response = OwnershipHandoffResponse<()>;

    const NAME: &'static str = "discard_ownership_handoff_state";
    const CLASS: PoolClass = PoolClass::Replication;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for ReconcileOwnershipHandoffPreparationsRequest {
    type Response = OwnershipHandoffResponse<u64>;

    const NAME: &'static str = "reconcile_ownership_handoff_preparations";
    const CLASS: PoolClass = PoolClass::Replication;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod clock_attachment_properties;
#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod progress_properties;
#[cfg(all(test, feature = "shuttle"))]
mod shuttle_checks;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_precondition_refusals_retain_the_original_deadline_and_transport_cause() {
        let node = ClusterNodeName::parse("owner").assured("the fixture node name is valid");
        let deadline = Instant::now() + Duration::from_secs(7);
        let refused = Report::new(TransportError::RemoteRejected {
            status: http::StatusCode::PRECONDITION_FAILED.as_u16(),
            message: "the producer's precondition changed".to_owned(),
        });
        let refused = StreamPreconditionDeadline::attach(refused, deadline);
        let error = RequestError::stream_with_cause(refused, node.clone(), "fetch_state_snapshot");
        assert_eq!(
            error.current_context().stream_precondition_deadline(),
            Some(deadline)
        );
        assert!(error.contains::<TransportError>());
        assert!(!error.current_context().is_capacity_exhaustion());

        for status in [
            http::StatusCode::BAD_REQUEST,
            http::StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let refused = Report::new(TransportError::RemoteRejected {
                status: status.as_u16(),
                message: "the producer's precondition changed".to_owned(),
            });
            let refused = StreamPreconditionDeadline::attach(refused, deadline);
            let error =
                RequestError::stream_with_cause(refused, node.clone(), "fetch_state_snapshot");
            assert!(matches!(
                error.current_context(),
                RequestError::Stream { .. }
            ));
            assert_eq!(error.current_context().stream_precondition_deadline(), None);
        }
    }

    #[derive(Debug, Archive, Serialize, Deserialize)]
    struct BulkDecodeRequest {
        payload: Vec<u8>,
    }

    impl InterconnectStreamRequest for BulkDecodeRequest {
        const NAME: &'static str = "test_bulk_decode";
        const CLASS: PoolClass = PoolClass::Bulk;
        const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
        const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Setup;
    }

    #[nervix_primitives::test]
    async fn receive_decode_reports_memory_pressure_as_capacity_exhaustion() {
        let executor = Executor::default();
        let payload = rkyv::to_bytes::<RkyvError>(&BulkDecodeRequest {
            payload: vec![7; 1024],
        })
        .assured("the current request serializes")
        .to_vec();
        let held = executor
            .try_reserve(
                nervix_execution::MemoryClass::Bulk,
                executor
                    .memory(nervix_execution::MemoryClass::Bulk)
                    .capacity_bytes,
            )
            .assured("the fixture occupies the untouched bulk class");
        let node = ClusterNodeName::parse("node-b").assured("the fixture node name is valid");
        let handler = TypedStreamHandler::<BulkDecodeRequest, _> {
            handler: Arc::new(|_context, _request| async {
                Err(Report::new(StreamHandlerError::new(
                    "a refused request reached its handler",
                )))
            }),
            request: PhantomData,
        };
        let failure = match handler
            .handle(
                executor.clone(),
                RequestContext {
                    peer_node_id: node.clone(),
                    peer_advertised_host: "node-b".to_owned(),
                    peer_process_epoch: 1,
                },
                payload,
            )
            .await
        {
            Err(failure) => failure,
            Ok(_) => panic!("the occupied class must refuse decoding"),
        };
        let StreamOpeningFailure::Request(failure) = failure else {
            panic!("decode capacity must refuse the request before its operation runs");
        };
        let error = RequestError::RemoteRejected {
            node,
            request: BulkDecodeRequest::NAME,
            failure,
        };
        assert!(
            error.is_capacity_exhaustion(),
            "a decode that judged no bytes must report a retryable capacity refusal: {error}"
        );
        drop(held);
        assert_eq!(
            executor
                .memory(nervix_execution::MemoryClass::Bulk)
                .reserved_bytes,
            0
        );
    }

    #[test]
    fn application_health_probe_uses_reserved_management_capacity_and_returns_identity() {
        fn assert_identity_response<M>()
        where
            M: InterconnectRequest<Response = ClusterNodeIdentity>,
        {
        }

        assert_identity_response::<ApplicationHealthProbe>();
        assert_eq!(ApplicationHealthProbe::NAME, "application_health_probe");
        assert_eq!(ApplicationHealthProbe::CLASS, PoolClass::Management);
        assert_eq!(ApplicationHealthProbe::SUBQUOTA, RequestSubquota::Liveness);
        assert_eq!(
            ApplicationHealthProbe::DEADLINE.declared_default(),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn https_listener_installation_uses_reserved_management_progress_capacity() {
        fn assert_installation_response<M>()
        where
            M: InterconnectRequest<Response = HttpsListenerInstallationResponse>,
        {
        }

        assert_installation_response::<HttpsListenerInstallationRequest>();
        assert_eq!(
            HttpsListenerInstallationRequest::NAME,
            "https_listener_installation"
        );
        assert_eq!(
            HttpsListenerInstallationRequest::CLASS,
            PoolClass::Management
        );
        assert_eq!(
            HttpsListenerInstallationRequest::SUBQUOTA,
            RequestSubquota::Progress
        );
        assert_eq!(
            HttpsListenerInstallationRequest::DEADLINE.declared_default(),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn application_progress_requests_use_reserved_management_progress_capacity() {
        fn assert_progress_response<M>()
        where
            M: InterconnectRequest<Response = ApplicationProgress>,
        {
        }
        fn assert_receipt_response<M>()
        where
            M: InterconnectRequest<Response = ApplicationProgressReceipt>,
        {
        }

        assert_progress_response::<ApplicationRevisionRequest>();
        assert_eq!(ApplicationRevisionRequest::NAME, "application_revision");
        assert_eq!(ApplicationRevisionRequest::CLASS, PoolClass::Management);
        assert_eq!(
            ApplicationRevisionRequest::SUBQUOTA,
            RequestSubquota::Progress
        );
        assert_eq!(
            ApplicationRevisionRequest::DEADLINE.declared_default(),
            Duration::from_secs(2)
        );

        assert_receipt_response::<ApplicationProgressReport>();
        assert_eq!(ApplicationProgressReport::NAME, "application_progress");
        assert_eq!(ApplicationProgressReport::CLASS, PoolClass::Management);
        assert_eq!(
            ApplicationProgressReport::SUBQUOTA,
            RequestSubquota::Progress
        );
        assert_eq!(
            ApplicationProgressReport::DEADLINE.declared_default(),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn application_progress_phases_only_advance() {
        let identity = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-a")
                .assured("the test node name follows the public node-name grammar"),
            nervix_models::ClusterNodeIncarnation::new(7),
        );
        let mut progress = ApplicationProgress::new(identity.clone());
        assert_eq!(
            progress.reached(ApplicationProgressPhase::RuntimePrepared),
            None
        );
        assert!(progress.advance(ApplicationProgressPhase::RuntimePrepared, 0));
        assert_eq!(
            progress.reached(ApplicationProgressPhase::RuntimePrepared),
            Some(0),
            "runtime revision 0 is a revision the incarnation reached, not an absent phase"
        );
        assert!(progress.advance(ApplicationProgressPhase::RuntimePrepared, 5));
        assert!(
            !progress.advance(ApplicationProgressPhase::RuntimePrepared, 5),
            "reaching the same revision again advances nothing"
        );
        assert!(
            !progress.advance(ApplicationProgressPhase::RuntimePrepared, 3),
            "an earlier revision moves no phase back"
        );
        assert_eq!(
            progress.reached(ApplicationProgressPhase::RuntimePrepared),
            Some(5)
        );
        assert_eq!(
            progress.reached(ApplicationProgressPhase::RuntimeReady),
            None,
            "advancing one phase reaches no other"
        );

        let late = ApplicationProgress {
            identity: identity.clone(),
            authoritative: Some(9),
            runtime_prepared: Some(4),
            runtime_ready: None,
            domain_clock_installed: Some(2),
            runtime_failure: None,
        };
        assert!(progress.absorb(&late));
        assert_eq!(
            progress,
            ApplicationProgress {
                identity,
                authoritative: Some(9),
                runtime_prepared: Some(5),
                runtime_ready: None,
                domain_clock_installed: Some(2),
                runtime_failure: None,
            },
            "a report takes every phase it reached further and keeps every phase it reached less"
        );
        assert!(
            !progress.absorb(&late),
            "absorbing the same report again advances nothing"
        );
    }

    #[test]
    fn a_runtime_failure_holds_until_its_revision_or_a_later_one_is_prepared() {
        let identity = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-b")
                .assured("the test node name follows the public node-name grammar"),
            nervix_models::ClusterNodeIncarnation::new(3),
        );
        let mut progress = ApplicationProgress::new(identity.clone());
        assert!(progress.advance(ApplicationProgressPhase::RuntimePrepared, 4));
        assert!(
            !progress.fail_runtime(4, "late".to_owned()),
            "a revision the incarnation prepared cannot fail afterwards"
        );
        assert!(progress.fail_runtime(5, "first".to_owned()));
        assert!(
            !progress.fail_runtime(5, "first".to_owned()),
            "the same failure again changes nothing"
        );
        assert!(
            progress.fail_runtime(5, "retried".to_owned()),
            "a later attempt at the same revision reports its own reason"
        );
        assert!(
            !progress.fail_runtime(4, "earlier".to_owned()),
            "an earlier revision's failure never replaces a later one"
        );
        assert_eq!(
            progress.runtime_failure(),
            Some(&RuntimeApplicationFailure {
                revision: 5,
                reason: "retried".to_owned(),
            })
        );

        let mut held = ApplicationProgress::new(identity);
        assert!(held.absorb(&progress), "a held report takes the failure");
        assert_eq!(held.runtime_failure(), progress.runtime_failure());

        assert!(progress.advance(ApplicationProgressPhase::RuntimePrepared, 5));
        assert_eq!(
            progress.runtime_failure(),
            None,
            "preparing the failed revision ends its failure"
        );
        assert!(held.absorb(&progress));
        assert_eq!(
            held.runtime_failure(),
            None,
            "a later report that prepared the revision ends the failure it held"
        );
    }

    #[test]
    fn domain_clock_progress_uses_its_reserved_management_capacity() {
        fn assert_unit_response<M>()
        where
            M: InterconnectRequest<Response = ()>,
        {
        }

        assert_unit_response::<DomainClockProgressRequest>();
        assert_eq!(DomainClockProgressRequest::NAME, "domain_clock_progress");
        assert_eq!(DomainClockProgressRequest::CLASS, PoolClass::Management);
        assert_eq!(
            DomainClockProgressRequest::SUBQUOTA,
            RequestSubquota::Progress
        );
        assert_eq!(
            DomainClockProgressRequest::DEADLINE.declared_default(),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn request_errors_distinguish_capacity_exhaustion_from_peer_failure() {
        let node = ClusterNodeName::parse("node-b")
            .assured("the test node name follows the public node-name grammar");
        let local_capacity = RequestError::AdmissionFull {
            request: "application_health_probe",
            subquota: RequestSubquota::Liveness,
        };
        let remote_capacity = RequestError::RemoteRejected {
            node: node.clone(),
            request: "application_health_probe",
            failure: RemoteRequestFailure::AdmissionFull {
                subquota: RequestSubquota::Liveness,
            },
        };
        let connection_capacity = RequestError::ConnectionCapacityExhausted {
            node: node.clone(),
            request: "application_health_probe",
        };
        let peer_timeout = RequestError::Timeout {
            node,
            request: "application_health_probe",
            timeout: Duration::from_secs(1),
        };

        assert!(local_capacity.is_capacity_exhaustion());
        assert!(remote_capacity.is_capacity_exhaustion());
        assert!(connection_capacity.is_capacity_exhaustion());
        assert!(!peer_timeout.is_capacity_exhaustion());
    }

    #[test]
    fn configured_append_capacity_refuses_excess_without_consuming_other_reserves() {
        let settings = crate::TransportSettings::resolve(crate::TransportSettingValues {
            max_peers: Some(16),
            request_quota: Some(4),
            ..Default::default()
        })
        .assured("a four-request quota fits node admission");
        let options = settings.options(None, None);
        let quotas = RequestQuotas::new(&options);
        let held: Vec<_> = (0..4)
            .map(|_| {
                quotas
                    .try_admit(RequestSubquota::Append)
                    .assured("declared append room")
            })
            .collect();
        assert!(quotas.try_admit(RequestSubquota::Append).is_none());
        assert!(quotas.try_admit(RequestSubquota::Liveness).is_some());
        assert!(quotas.try_admit(RequestSubquota::Cancellation).is_some());
        assert!(quotas.try_admit(RequestSubquota::Shared).is_some());
        drop(held);
        assert!(quotas.try_admit(RequestSubquota::Append).is_some());
    }

    #[test]
    fn liveness_quota_supports_the_node_health_probe_limit() {
        let options = crate::TransportOptions {
            incoming_queue_capacity: 1,
            ..Default::default()
        };
        let quotas = RequestQuotas::new(&options);

        assert_eq!(
            quotas.liveness.available_permits(),
            options.max_concurrent_health_probes,
            "health capacity must not shrink with the general incoming request queue"
        );
    }

    #[test]
    fn every_target_is_live_until_membership_is_published() {
        let requests = RequestState::new(
            &crate::TransportOptions::default(),
            Arc::new(TransportObservations::default()),
        );
        let node_a = ClusterNodeName::parse("node-a")
            .assured("the test node name follows the public node-name grammar");
        let node_b = ClusterNodeName::parse("node-b")
            .assured("the test node name follows the public node-name grammar");

        assert!(requests.target_is_live(&node_a));
        assert!(requests.target_is_live(&node_b));

        requests.replace_live_nodes(&BTreeSet::from([node_a.clone()]));
        assert!(requests.target_is_live(&node_a));
        assert!(!requests.target_is_live(&node_b));

        requests.replace_live_nodes(&BTreeSet::new());
        assert!(
            !requests.target_is_live(&node_a),
            "a published empty membership has no live target"
        );
    }
}
