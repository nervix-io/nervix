//! What a remote delivery's sender does after each attempt to send it fails: when it sends again,
//! and when it gives the delivery up.
//!
//! Layer: data plane.
//!
//! - **Owns.** The stall bound one delivery's attempts run within, how a capacity refusal of its
//!   grant holds the delivery back and restarts that bound, how long a held delivery waits before
//!   it asks again, and the delivery's admission bound.
//! - **Depends on.** The typed grant refusal and the dispatch bounds of the remote dispatcher.
//! - **Must not know.** The transport, payloads, relays, branches or acknowledgements.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use nervix_interconnect::RelayGrantRefusal;

use super::{
    remote_dispatch::{REMOTE_RELAY_TOTAL_TIMEOUT, RemoteDispatchError},
    *,
};

/// What a delivery does while its receiver refuses its grant for capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GrantCapacityRefusals {
    /// The delivery is held back and asks for a grant again until its admission bound passes.
    Hold,
    /// The delivery treats the refusal as any failed send, which it retries within the stall bound.
    GiveUp,
}

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
    /// The receiver answered nothing within the stall bound, and the last attempt failed.
    Stalled,
    /// The stall bound passed while an attempt was still running.
    TimedOut,
    /// The delivery's admission bound passed while it was held back.
    AdmissionBound,
}

/// The bounds one delivery's attempts to send it run within.
///
/// Every attempt must end before the stall bound, five seconds after the delivery started or after
/// the receiver last answered, whichever is later. A capacity refusal is such an answer: under
/// [`GrantCapacityRefusals::Hold`] it restarts the stall bound and holds the delivery back for a
/// wait that starts at the dispatch retry interval and doubles after each further capacity refusal
/// up to [`RelaySendAttempts::capacity_wait_ceiling`]. Nothing outlasts the admission bound the
/// delivery was given.
#[derive(Debug, Clone)]
pub(super) struct RelaySendAttempts {
    capacity_refusals: GrantCapacityRefusals,
    stall_deadline: Instant,
    admission_deadline: Instant,
    capacity_waits: RuntimeReconnectBackoff,
}

impl RelaySendAttempts {
    pub(super) fn new(
        now: Instant,
        admission_deadline: Instant,
        capacity_refusals: GrantCapacityRefusals,
    ) -> Self {
        Self {
            capacity_refusals,
            stall_deadline: Self::stall_deadline_after(now, admission_deadline),
            admission_deadline,
            capacity_waits: RuntimeReconnectBackoff {
                initial: RemoteDispatcher::DISPATCH_RETRY_INTERVAL,
                next: RemoteDispatcher::DISPATCH_RETRY_INTERVAL,
                max: Self::capacity_wait_ceiling(),
            },
        }
    }

    /// The longest a held delivery waits before it asks for its grant again: half the stall
    /// bound, so a held delivery always asks again within that bound, with room left to retry an
    /// ask that failed in transport.
    pub(super) fn capacity_wait_ceiling() -> Duration {
        RemoteDispatcher::DISPATCH_TIMEOUT / 2
    }

    /// The instant the next attempt must end by.
    pub(super) fn attempt_deadline(&self) -> Instant {
        self.stall_deadline
    }

    /// What the sender does after an attempt that failed at `now`.
    pub(super) fn after_failure(
        &mut self,
        failure: RelaySendFailure,
        now: Instant,
    ) -> RelaySendStep {
        if let RelaySendFailure::Refused(refusal) = failure {
            if !refusal.is_capacity() {
                return RelaySendStep::End(RelaySendEnd::Refused(refusal));
            }
            if self.capacity_refusals == GrantCapacityRefusals::Hold {
                return self.hold(now);
            }
        }
        let retry_at = now
            .checked_add(RemoteDispatcher::DISPATCH_RETRY_INTERVAL)
            .assured("the fixed dispatch retry interval fits the monotonic clock");
        if retry_at >= self.stall_deadline {
            return RelaySendStep::End(RelaySendEnd::Stalled);
        }
        RelaySendStep::RetryAt(retry_at)
    }

    /// Why the sender gives a delivery up when an attempt still runs at its deadline, `now`.
    pub(super) fn after_attempt_deadline(&self, now: Instant) -> RelaySendEnd {
        if now >= self.admission_deadline {
            return RelaySendEnd::AdmissionBound;
        }
        RelaySendEnd::TimedOut
    }

    /// Hold the delivery back after its receiver answered with a capacity refusal at `now`.
    fn hold(&mut self, now: Instant) -> RelaySendStep {
        if now >= self.admission_deadline {
            return RelaySendStep::End(RelaySendEnd::AdmissionBound);
        }
        self.stall_deadline = Self::stall_deadline_after(now, self.admission_deadline);
        let wait = self.capacity_waits.take_next_delay();
        let retry_at = now
            .checked_add(wait)
            .assured("a wait bounded by half the stall bound fits the monotonic clock")
            .min(self.admission_deadline);
        RelaySendStep::RetryAt(retry_at)
    }

    fn stall_deadline_after(now: Instant, admission_deadline: Instant) -> Instant {
        now.checked_add(RemoteDispatcher::DISPATCH_TIMEOUT)
            .assured("the fixed dispatch stall bound fits the monotonic clock")
            .min(admission_deadline)
    }
}

impl RelaySendEnd {
    /// The dispatch failure that names this end for the delivery sent to `target`.
    pub(super) fn dispatch_error(self, target: &ClusterNodeName) -> RemoteDispatchError {
        match self {
            Self::Refused(refusal) => RemoteDispatchError::GrantRefused {
                target: target.clone(),
                refusal,
            },
            Self::Stalled => RemoteDispatchError::Send {
                target: target.clone(),
            },
            Self::TimedOut => RemoteDispatchError::SendTimeout {
                target: target.clone(),
            },
            Self::AdmissionBound => RemoteDispatchError::AdmissionTotalTimeout {
                target: target.clone(),
                timeout: REMOTE_RELAY_TOTAL_TIMEOUT,
            },
        }
    }
}

#[cfg(test)]
#[path = "relay_send_attempts_tests.rs"]
mod tests;
