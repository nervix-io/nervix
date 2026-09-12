//! What the transport is holding and what it has carried, for a node's metric exposition.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The cumulative event counters the transport records, and the bounded dimensions
//!   every observation is aggregated by.
//! - **Depends on.** The traffic classes the pools are separated into and the reserved request
//!   subquotas they are partitioned by.
//! - **Must not know.** Which peer, domain, branch or operation identity produced an observation.
//!   Every dimension declared here is a closed set fixed at compile time, so no payload value can
//!   widen a series.
//!
//! Instantaneous levels — open connections, leased streams, requests in flight, unresolved relay
//! work — are read from the state that already owns them when a snapshot is taken, so they cannot
//! drift from what the transport is actually doing. Only events that leave no standing state
//! behind are counted here.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use meticulous::ResultExt as _;
use strum::{AsRefStr, EnumCount, EnumIter, FromRepr};

use crate::{PoolClass, RequestSubquota, TransportError};

/// Why a physical connection never established, or why an established one ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumCount, EnumIter, FromRepr)]
#[strum(serialize_all = "snake_case")]
pub enum ConnectionFailureReason {
    /// The TCP connect, the TLS handshake, or the HTTP/2 preface did not complete in time.
    Setup,
    /// The peer's identity, the wire contract fingerprint, or a certificate was rejected.
    Handshake,
    /// This node or its peer had no capacity for another connection of this class.
    Capacity,
    /// An established connection ended and the pool slot has to dial again.
    Closed,
}

/// Why one HTTP/2 stream ended without delivering its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumCount, EnumIter, FromRepr)]
#[strum(serialize_all = "snake_case")]
pub enum StreamResetReason {
    /// The request deadline or the byte-transfer no-progress limit expired.
    Deadline,
    /// A stream slot, a byte budget, or a connection permit was unavailable.
    Capacity,
    /// The peer reset the stream, answered with a failure status, or closed under it.
    Peer,
    /// The local transport is draining.
    Shutdown,
    /// The body could not be encoded, decoded, or framed.
    Malformed,
}

/// How one relay attempt left the transport's unresolved set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumCount, EnumIter, FromRepr)]
#[strum(serialize_all = "snake_case")]
pub enum RelayAdmissionOutcome {
    /// The receiving runtime took ownership of the batch.
    Admitted,
    /// The receiver refused the batch before admitting it.
    Rejected,
    /// The sender withdrew the attempt, or its branch was evicted, before admission.
    Cancelled,
}

/// Which way bytes crossed a bulk stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumCount, EnumIter, FromRepr)]
#[strum(serialize_all = "snake_case")]
pub enum TransferDirection {
    Sent,
    Received,
}

/// Whether this node dialled the connection or accepted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumCount, EnumIter, FromRepr)]
#[strum(serialize_all = "snake_case")]
pub enum ConnectionDirection {
    Outbound,
    Inbound,
}

impl RelayAdmissionOutcome {
    pub const fn index(self) -> usize {
        match self {
            Self::Admitted => 0,
            Self::Rejected => 1,
            Self::Cancelled => 2,
        }
    }
}

impl TransferDirection {
    pub const fn index(self) -> usize {
        match self {
            Self::Sent => 0,
            Self::Received => 1,
        }
    }
}

impl ConnectionDirection {
    pub const fn index(self) -> usize {
        match self {
            Self::Outbound => 0,
            Self::Inbound => 1,
        }
    }
}

impl ConnectionFailureReason {
    pub const fn index(self) -> usize {
        match self {
            Self::Setup => 0,
            Self::Handshake => 1,
            Self::Capacity => 2,
            Self::Closed => 3,
        }
    }

    /// The class a failed connection attempt belongs to, so the reason stays a closed set however
    /// many distinct messages the underlying error carries.
    pub(crate) fn of(error: &TransportError) -> Self {
        match error {
            TransportError::ConnectionSetupTimeout { .. }
            | TransportError::Io(_)
            | TransportError::Tls(_)
            | TransportError::Http2(_)
            | TransportError::Http(_)
            | TransportError::RequestTimeout { .. }
            | TransportError::ProgressTimeout { .. }
            | TransportError::Closed(_) => Self::Setup,
            TransportError::InvalidHandshake(_)
            | TransportError::InvalidServerName(_)
            | TransportError::InvalidOptions { .. }
            | TransportError::Encode(_)
            | TransportError::Decode(_)
            | TransportError::RemoteRejected { .. }
            | TransportError::PayloadTooLarge { .. }
            | TransportError::RelayGrant(_)
            | TransportError::RelayCancelled
            | TransportError::RelayIndeterminate
            | TransportError::RelayRejected(_) => Self::Handshake,
            TransportError::PoolExhausted | TransportError::IncomingQueueFull => Self::Capacity,
            TransportError::ShuttingDown => Self::Closed,
        }
    }
}

impl StreamResetReason {
    pub const fn index(self) -> usize {
        match self {
            Self::Deadline => 0,
            Self::Capacity => 1,
            Self::Peer => 2,
            Self::Shutdown => 3,
            Self::Malformed => 4,
        }
    }

    /// The class one failed request belongs to.
    pub(crate) fn of(error: &TransportError) -> Self {
        match error {
            TransportError::RequestTimeout { .. }
            | TransportError::ProgressTimeout { .. }
            | TransportError::ConnectionSetupTimeout { .. } => Self::Deadline,
            TransportError::PoolExhausted
            | TransportError::IncomingQueueFull
            | TransportError::PayloadTooLarge { .. }
            | TransportError::RelayGrant(_) => Self::Capacity,
            TransportError::ShuttingDown => Self::Shutdown,
            TransportError::Encode(_)
            | TransportError::Decode(_)
            | TransportError::Http(_)
            | TransportError::InvalidOptions { .. }
            | TransportError::InvalidServerName(_)
            | TransportError::InvalidHandshake(_) => Self::Malformed,
            TransportError::Io(_)
            | TransportError::Tls(_)
            | TransportError::Http2(_)
            | TransportError::Closed(_)
            | TransportError::RemoteRejected { .. }
            | TransportError::RelayCancelled
            | TransportError::RelayIndeterminate
            | TransportError::RelayRejected(_) => Self::Peer,
        }
    }
}

/// Whether one typed request produced its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumCount, EnumIter, FromRepr)]
#[strum(serialize_all = "snake_case")]
pub enum RequestOutcome {
    Answered,
    Failed,
}

impl RequestOutcome {
    pub const fn index(self) -> usize {
        match self {
            Self::Answered => 0,
            Self::Failed => 1,
        }
    }
}

/// The transport's cumulative event counters.
#[derive(Debug, Default)]
pub(crate) struct TransportObservations {
    connections_established: ClassCounters,
    connection_failures: ClassReasonCounters<{ ConnectionFailureReason::COUNT }>,
    stream_resets: ClassReasonCounters<{ StreamResetReason::COUNT }>,
    quota_failures: DirectionSubquotaCounters,
    requests: [[AtomicU64; RequestSubquota::COUNT]; RequestOutcome::COUNT],
    request_nanos: [AtomicU64; RequestSubquota::COUNT],
    relay_outcomes: [AtomicU64; RelayAdmissionOutcome::COUNT],
    relay_admission_wait_nanos: AtomicU64,
    bulk_bytes: [[AtomicU64; TransferDirection::COUNT]; PoolClass::COUNT],
}

/// One counter per traffic class.
#[derive(Debug, Default)]
struct ClassCounters([AtomicU64; PoolClass::COUNT]);

/// One counter per traffic class and reason.
#[derive(Debug)]
struct ClassReasonCounters<const REASONS: usize>([[AtomicU64; REASONS]; PoolClass::COUNT]);

/// One counter per direction and reserved request subquota.
#[derive(Debug, Default)]
struct DirectionSubquotaCounters([[AtomicU64; RequestSubquota::COUNT]; ConnectionDirection::COUNT]);

impl<const REASONS: usize> Default for ClassReasonCounters<REASONS> {
    fn default() -> Self {
        Self(std::array::from_fn(|_| {
            std::array::from_fn(|_| AtomicU64::new(0))
        }))
    }
}

impl TransportObservations {
    pub(crate) fn connection_established(&self, class: PoolClass) {
        self.connections_established.0[class.index()].fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn connection_failed(&self, class: PoolClass, reason: ConnectionFailureReason) {
        self.connection_failures.0[class.index()][reason.index()].fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn stream_reset(&self, class: PoolClass, reason: StreamResetReason) {
        self.stream_resets.0[class.index()][reason.index()].fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn quota_failure(&self, direction: ConnectionDirection, subquota: RequestSubquota) {
        self.quota_failures.0[direction.index()][subquota.index()].fetch_add(1, Ordering::AcqRel);
    }

    /// Count one finished typed request and the round trip it took. The liveness subquota is the
    /// application health probe, so its share of these two series is peer health latency.
    pub(crate) fn request_completed(
        &self,
        subquota: RequestSubquota,
        outcome: RequestOutcome,
        elapsed: Duration,
    ) {
        self.requests[outcome.index()][subquota.index()].fetch_add(1, Ordering::AcqRel);
        self.request_nanos[subquota.index()].fetch_add(nanos(elapsed), Ordering::AcqRel);
    }

    pub(crate) fn relay_resolved(&self, outcome: RelayAdmissionOutcome, waited: Duration) {
        self.relay_outcomes[outcome.index()].fetch_add(1, Ordering::AcqRel);
        self.relay_admission_wait_nanos
            .fetch_add(nanos(waited), Ordering::AcqRel);
    }

    pub(crate) fn bulk_transferred(
        &self,
        class: PoolClass,
        direction: TransferDirection,
        bytes: u64,
    ) {
        self.bulk_bytes[class.index()][direction.index()].fetch_add(bytes, Ordering::AcqRel);
    }

    pub(crate) fn counters(&self) -> TransportCounters {
        TransportCounters {
            connections_established: std::array::from_fn(|class| {
                self.connections_established.0[class].load(Ordering::Acquire)
            }),
            connection_failures: read_class_reasons(&self.connection_failures),
            stream_resets: read_class_reasons(&self.stream_resets),
            quota_failures: std::array::from_fn(|direction| {
                std::array::from_fn(|subquota| {
                    self.quota_failures.0[direction][subquota].load(Ordering::Acquire)
                })
            }),
            requests: std::array::from_fn(|outcome| {
                std::array::from_fn(|subquota| {
                    self.requests[outcome][subquota].load(Ordering::Acquire)
                })
            }),
            request_time: std::array::from_fn(|subquota| {
                Duration::from_nanos(self.request_nanos[subquota].load(Ordering::Acquire))
            }),
            relay_outcomes: std::array::from_fn(|outcome| {
                self.relay_outcomes[outcome].load(Ordering::Acquire)
            }),
            relay_admission_wait: Duration::from_nanos(
                self.relay_admission_wait_nanos.load(Ordering::Acquire),
            ),
            bulk_bytes: std::array::from_fn(|class| {
                std::array::from_fn(|direction| {
                    self.bulk_bytes[class][direction].load(Ordering::Acquire)
                })
            }),
        }
    }
}

fn read_class_reasons<const REASONS: usize>(
    counters: &ClassReasonCounters<REASONS>,
) -> [[u64; REASONS]; PoolClass::COUNT] {
    std::array::from_fn(|class| {
        std::array::from_fn(|reason| counters.0[class][reason].load(Ordering::Acquire))
    })
}

/// Everything the transport has counted since the node started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportCounters {
    pub connections_established: [u64; PoolClass::COUNT],
    pub connection_failures: [[u64; ConnectionFailureReason::COUNT]; PoolClass::COUNT],
    pub stream_resets: [[u64; StreamResetReason::COUNT]; PoolClass::COUNT],
    pub quota_failures: [[u64; RequestSubquota::COUNT]; ConnectionDirection::COUNT],
    pub requests: [[u64; RequestSubquota::COUNT]; RequestOutcome::COUNT],
    /// Time typed requests spent between submission and their result, by reserved subquota.
    pub request_time: [Duration; RequestSubquota::COUNT],
    pub relay_outcomes: [u64; RelayAdmissionOutcome::COUNT],
    /// Time relay attempts spent between an accepted reservation and their resolution.
    pub relay_admission_wait: Duration,
    pub bulk_bytes: [[u64; TransferDirection::COUNT]; PoolClass::COUNT],
}

/// Everything the transport is currently holding, beside what it has counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportSnapshot {
    pub counters: TransportCounters,
    /// Physical connections this node currently holds, by direction and class.
    pub connections: [[usize; PoolClass::COUNT]; ConnectionDirection::COUNT],
    /// HTTP/2 stream slots leased on this node's outbound connections, by class.
    pub leased_streams: [usize; PoolClass::COUNT],
    /// Requests admitted and not yet resolved, by direction and reserved subquota.
    pub pending_operations: [[usize; RequestSubquota::COUNT]; ConnectionDirection::COUNT],
    /// Logical relay channels with unresolved work.
    pub relay_channels: usize,
    /// Relay attempts whose outcome has not been retired yet.
    pub relay_attempts: usize,
    /// Transfer grants a sender holds and has not spent.
    pub relay_grants: usize,
    /// How long the oldest unresolved relay outcome has been waiting for its acknowledgement.
    pub oldest_unresolved_outcome: Duration,
}

/// Nanoseconds of an observed interval, for the cumulative time counters.
fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).assured(
        "a node would have to run for 584 years for one observed interval to overflow nanosecond \
         counting",
    )
}
