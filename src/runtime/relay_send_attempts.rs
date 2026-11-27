//! What a remote delivery's sender does after each attempt to send it fails: when it sends again,
//! and when it gives the delivery up.
//!
//! Layer: data plane.
//!
//! - **Owns.** How a capacity refusal of a delivery's grant, or a send that failed in transport,
//!   holds the delivery back, how long a held delivery waits before it asks again, and the
//!   delivery's admission bound, the operator's ceiling for one delivery.
//! - **Depends on.** The typed grant refusal and the remote acknowledgement timing the delivery
//!   captured.
//! - **Must not know.** The transport, membership, payloads, relays, branches or acknowledgements.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use nervix_interconnect::RelayGrantRefusal;

use super::{remote_dispatch::RemoteDispatchError, *};

/// Why one attempt to send a delivery failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelaySendFailure {
    /// The receiver answered the delivery's grant request with a refusal.
    Refused(RelayGrantRefusal),
    /// The attempt failed in transport, without an answer from the receiver.
    Transport,
}

/// What the sender does after an attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelaySendStep {
    /// Send the delivery again at this instant.
    RetryAt(Instant),
    /// Give the delivery up.
    End(RelaySendEnd),
}

/// Why the sender gave a delivery up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelaySendEnd {
    /// The receiver refused the delivery's grant permanently.
    Refused(RelayGrantRefusal),
    /// The delivery's admission bound passed, the operator's ceiling for one delivery.
    AdmissionBound,
}

/// The waits between one delivery's attempts to send it.
///
/// A receiver that answers with a capacity refusal is applying backpressure, and a send that fails
/// in transport leaves the delivery to a receiver that liveness still keeps: both hold the
/// delivery back. It waits the dispatch retry interval, twice as long after each further failure
/// up to the retry ceiling, and asks again. No attempt has a deadline of its own: the delivery
/// ends only at a permanent refusal, at its admission bound, or when its receiver leaves or its
/// route changes, which its sender watches.
#[derive(Debug, Clone)]
pub(super) struct RelaySendAttempts {
    admission_deadline: Instant,
    waits: RuntimeReconnectBackoff,
}

impl RelaySendAttempts {
    pub(super) fn new(admission_deadline: Instant, timing: nervix_models::RemoteAckTiming) -> Self {
        Self {
            admission_deadline,
            waits: RuntimeReconnectBackoff {
                initial: timing.retry_interval(),
                next: timing.retry_interval(),
                max: timing.retry_ceiling(),
                settings: nervix_connector::RetryBackoffSettings::default(),
            },
        }
    }

    /// The instant the delivery's admission bound passes, which no attempt outlasts.
    pub(super) fn admission_deadline(&self) -> Instant {
        self.admission_deadline
    }

    /// What the sender does after an attempt that failed at `now`.
    pub(super) fn after_failure(
        &mut self,
        failure: RelaySendFailure,
        now: Instant,
    ) -> RelaySendStep {
        if let RelaySendFailure::Refused(refusal) = failure
            && !refusal.is_capacity()
        {
            return RelaySendStep::End(RelaySendEnd::Refused(refusal));
        }
        if now >= self.admission_deadline {
            return RelaySendStep::End(RelaySendEnd::AdmissionBound);
        }
        let wait = self.waits.take_next_delay();
        let retry_at = now
            .checked_add(wait)
            .assured("a wait bounded by the retry ceiling fits the monotonic clock")
            .min(self.admission_deadline);
        RelaySendStep::RetryAt(retry_at)
    }
}

impl RelaySendEnd {
    /// The dispatch failure that names this end for the delivery sent to `target`.
    pub(super) fn dispatch_error(
        self,
        target: &ClusterNodeName,
        timing: nervix_models::RemoteAckTiming,
    ) -> RemoteDispatchError {
        match self {
            Self::Refused(refusal) => RemoteDispatchError::GrantRefused {
                target: target.clone(),
                refusal,
            },
            Self::AdmissionBound => RemoteDispatchError::AdmissionTotalTimeout {
                target: target.clone(),
                timeout: timing.remote_relay_total_timeout,
            },
        }
    }
}

#[cfg(test)]
#[path = "relay_send_attempts_tests.rs"]
mod tests;

#[cfg(test)]
use super::remote_dispatch::REMOTE_RELAY_TOTAL_TIMEOUT;
