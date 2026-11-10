//! Process-local licensing request outcomes and connectivity observations.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Bounded outcome counters and the latest local connection observation.
//! - **Depends on.** Protocol operation identities, timestamps and execution primitives.
//! - **Must not know.** Policy grants, retries, consensus mutation or graph execution.

use std::collections::BTreeMap;

use meticulous::OptionExt as _;
use nervix_licensing_protocol::requests::Operation;
use nervix_models::Timestamp;
use nervix_primitives::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    watch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestOutcome {
    Committed,
    RejectedByCluster,
    RefusedByService,
    Unreachable,
    Timeout,
}
impl RequestOutcome {
    pub(crate) const ALL: [Self; 5] = [
        Self::Committed,
        Self::RejectedByCluster,
        Self::RefusedByService,
        Self::Unreachable,
        Self::Timeout,
    ];
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RejectedByCluster => "rejected_by_cluster",
            Self::RefusedByService => "refused_by_service",
            Self::Unreachable => "unreachable",
            Self::Timeout => "timeout",
        }
    }
}

/// An observed request retains its typed cause; the counter's outcome is a separate fixed label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestEvent {
    Committed,
    RejectedByCluster,
    RefusedByService(nervix_licensing_vocabulary::ProblemCode),
    Unreachable,
    Timeout,
}
impl RequestEvent {
    fn outcome(self) -> RequestOutcome {
        match self {
            Self::Committed => RequestOutcome::Committed,
            Self::RejectedByCluster => RequestOutcome::RejectedByCluster,
            Self::RefusedByService(_) => RequestOutcome::RefusedByService,
            Self::Unreachable => RequestOutcome::Unreachable,
            Self::Timeout => RequestOutcome::Timeout,
        }
    }
}

pub(crate) use nervix_models::LicensingConnectivity as Connectivity;

pub(super) struct RequestObservations {
    pub(super) stream_connected: AtomicBool,
    counters: BTreeMap<(&'static str, &'static str), AtomicU64>,
    pub(super) connectivity: watch::Sender<Connectivity>,
}
impl Default for RequestObservations {
    fn default() -> Self {
        Self {
            stream_connected: AtomicBool::new(false),
            counters: Operation::ALL
                .iter()
                .flat_map(|operation| {
                    RequestOutcome::ALL
                        .map(|outcome| ((operation.as_str(), outcome.as_str()), AtomicU64::new(0)))
                })
                .collect(),
            connectivity: watch::channel(Connectivity::default()).0,
        }
    }
}
impl RequestObservations {
    pub(super) fn record(&self, operation: Operation, event: RequestEvent, at: Timestamp) {
        self.counters
            .get(&(operation.as_str(), event.outcome().as_str()))
            .verified("each declared outcome has one counter")
            .fetch_add(1, Ordering::Relaxed);
        self.connectivity.send_modify(|observation| {
            use nervix_models::{LicensingConnectionState as State, LicensingTransportFailure};
            match event {
                RequestEvent::Committed | RequestEvent::RejectedByCluster => {
                    observation.last_success = Some(at);
                    observation.state = State::Reachable;
                }
                RequestEvent::RefusedByService(code) => {
                    observation.last_success = Some(at);
                    observation.state = if code.retryable() {
                        State::Reachable
                    } else {
                        let since = match observation.state {
                            State::Refusing { since, .. } => since,
                            _ => at,
                        };
                        State::Refusing {
                            since,
                            last_refusal: code,
                        }
                    };
                }
                RequestEvent::Unreachable | RequestEvent::Timeout => {
                    let since = match observation.state {
                        State::Unreachable { since, .. } => since,
                        _ => at,
                    };
                    let last_failure = match event {
                        RequestEvent::Timeout => LicensingTransportFailure::Timeout,
                        _ => LicensingTransportFailure::Transport,
                    };
                    observation.last_failure = Some(at);
                    observation.state = State::Unreachable {
                        since,
                        last_failure,
                    };
                }
            }
        });
    }

    pub(super) fn count(&self, operation: Operation, outcome: RequestOutcome) -> u64 {
        self.counters
            .get(&(operation.as_str(), outcome.as_str()))
            .verified("each declared outcome has one counter")
            .load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use nervix_licensing_vocabulary::ProblemCode;
    use nervix_models::{LicensingConnectionState as State, LicensingTransportFailure};

    use super::*;

    #[nervix_primitives::test]
    async fn observations_keep_refusal_and_outage_causes_without_resetting_their_start() {
        let observations = RequestObservations::default();
        let at = Timestamp::from_unix_nanos;
        observations.record(
            Operation::Refresh,
            RequestEvent::RefusedByService(ProblemCode::LicenseKeyRevoked),
            at(1),
        );
        observations.record(
            Operation::Refresh,
            RequestEvent::RefusedByService(ProblemCode::KindNotAllowed),
            at(2),
        );
        assert_eq!(
            observations.connectivity.borrow().state,
            State::Refusing {
                since: at(1),
                last_refusal: ProblemCode::KindNotAllowed
            }
        );
        observations.record(Operation::Refresh, RequestEvent::Timeout, at(3));
        observations.record(Operation::Refresh, RequestEvent::Unreachable, at(4));
        assert_eq!(
            observations.connectivity.borrow().state,
            State::Unreachable {
                since: at(3),
                last_failure: LicensingTransportFailure::Transport
            }
        );
        observations.record(Operation::Refresh, RequestEvent::Committed, at(5));
        let current = *observations.connectivity.borrow();
        assert_eq!(current.state, State::Reachable);
        assert_eq!(current.last_success, Some(at(5)));
        assert_eq!(current.last_failure, Some(at(4)));
        assert_eq!(
            observations.count(Operation::Refresh, RequestOutcome::RefusedByService),
            2
        );
        assert_eq!(
            observations.count(Operation::Refresh, RequestOutcome::Timeout),
            1
        );
        assert_eq!(
            observations.count(Operation::Refresh, RequestOutcome::Unreachable),
            1
        );
        assert_eq!(
            observations.count(Operation::Refresh, RequestOutcome::Committed),
            1
        );
        observations.record(
            Operation::Refresh,
            RequestEvent::RefusedByService(ProblemCode::Unavailable),
            at(6),
        );
        assert_eq!(observations.connectivity.borrow().state, State::Reachable);
    }
}
