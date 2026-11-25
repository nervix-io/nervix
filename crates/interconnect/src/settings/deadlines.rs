//! The deadlines an exchange with another node may carry beyond the liveness of its peer.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The three kinds of deadline a caller may name for one exchange, and what bounds
//!   opening a duplex stream.
//! - **Depends on.** The monotonic clock of the execution primitives.
//! - **Must not know.** Message payloads, graph state, the operations deadlines bound or their
//!   retries.
#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the deadlines of node-to-node exchanges")
)]
use std::time::Duration;

use nervix_primitives::time::Instant;

/// The shortest positive duration. A delay projected from a positive interval is never shorter,
/// so an interval too short to divide still projects one that has not already passed.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "a Duration counts whole nanoseconds, so one nanosecond is the shortest positive \
                  duration"
    )
)]
pub(crate) const SHORTEST_POSITIVE_DURATION: Duration = Duration::from_nanos(1);

/// What may end one exchange with another node before it is answered, beyond liveness.
///
/// The transport ends every exchange whose peer or connection is gone: its HTTP/2 pings and
/// application-health probes decide that a connection is gone, and membership that a peer left.
/// Nothing else ends an exchange unless its caller names one of these deadlines, and the caller
/// says which kind it is. A live peer that is slow, busy or applying backpressure costs the
/// exchange latency, never failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeDeadline {
    /// The ceiling the operator set for the whole operation the exchange belongs to, such as an
    /// entity gate's deadline, a drain's or a shutdown's budget, or a backup's `TIMEOUT`.
    OperationCeiling(Instant),
    /// The deadline of a caller that still holds its input and waits for the answer, such as a
    /// client's state read. The caller answers its own caller with a typed retryable refusal.
    Caller(Instant),
    /// A timer the protocol itself defines, such as a Raft RPC's or a liveness probe's.
    Protocol(Instant),
}

impl ExchangeDeadline {
    /// The instant the exchange ends unanswered.
    pub fn at(self) -> Instant {
        match self {
            Self::OperationCeiling(at) | Self::Caller(at) | Self::Protocol(at) => at,
        }
    }

    /// How long the exchange may still wait, zero once the deadline passed.
    pub fn remaining(self) -> Duration {
        self.at().saturating_duration_since(Instant::now())
    }

    /// Which kind of deadline this is, without its instant.
    pub fn kind(self) -> ExchangeDeadlineKind {
        match self {
            Self::OperationCeiling(_) => ExchangeDeadlineKind::OperationCeiling,
            Self::Caller(_) => ExchangeDeadlineKind::Caller,
            Self::Protocol(_) => ExchangeDeadlineKind::Protocol,
        }
    }
}

/// Run `operation` to its end, or until `deadline` passes when its caller named one, reporting
/// the kind of deadline that ended it. Without a deadline only the operation itself ends it.
pub(crate) async fn within<F: std::future::Future>(
    deadline: Option<ExchangeDeadline>,
    operation: F,
) -> Result<F::Output, ExchangeDeadlineKind> {
    match deadline {
        Some(deadline) => nervix_primitives::time::timeout_at(deadline.at(), operation)
            .await
            .map_err(|_| deadline.kind()),
        None => Ok(operation.await),
    }
}

/// Resolve once `deadline` passes, with its kind; without a deadline, never. A `select!` arm on it
/// ends a wait only at the deadline its caller named.
pub(crate) async fn passed(deadline: Option<ExchangeDeadline>) -> ExchangeDeadlineKind {
    match deadline {
        Some(deadline) => {
            nervix_primitives::time::sleep_until(deadline.at()).await;
            deadline.kind()
        }
        None => std::future::pending().await,
    }
}

/// The kind of [`ExchangeDeadline`] an exchange passed, as its failure reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum ExchangeDeadlineKind {
    OperationCeiling,
    Caller,
    Protocol,
}

/// What bounds opening one duplex stream, independently of its lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplexSetupDeadline {
    /// Open the link within this transport's connection-setup budget, the liveness of dialing.
    ConnectionSetup,
    /// Open it with no bound beyond liveness: the link's owner waits for its peer while the peer
    /// and its connection live, and a deadline of its own, if any, is the one it passes.
    Liveness,
}
