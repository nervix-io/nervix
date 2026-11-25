//! One immutable licensing policy publication per node.
//!
//! Layer: control plane.
//! - **Owns.** Publishing coherent committed policy snapshots and construction-time permission handles.
//! - **Depends on.** Pure policy decisions, consensus observations and execution primitives.
//! - **Must not know.** Service requests, activation orchestration or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns node licensing publication")
)]

mod requests;
pub(crate) use requests::{Connectivity, RequestEvent, RequestOutcome};
pub(crate) mod metrics;
pub mod policy;

use nervix_consensus::Observer;
pub use nervix_licensing_vocabulary::CapabilityName;
pub use nervix_models::LicensingStopReason;
use nervix_primitives::{
    publication::{ArcSwap, Guard},
    sync::{Arc, CancellationToken, StdArc},
};
pub use policy::{CapabilityVerdict, LicensingPolicy};

struct PolicyPublication {
    policy: ArcSwap<LicensingPolicy>,
    requests: requests::RequestObservations,
}

#[derive(Clone)]
pub struct PolicyHandle {
    inner: Arc<PolicyPublication>,
}

/// A retained immutable view; standard shared ownership stays inside the publication adapter.
pub struct PolicyView(Guard<StdArc<LicensingPolicy>>);

impl std::ops::Deref for PolicyView {
    type Target = LicensingPolicy;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for PolicyHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PolicyHandle")
            .finish_non_exhaustive()
    }
}

impl PolicyHandle {
    pub(crate) fn set_stream_connected(&self, connected: bool) {
        self.inner.requests.stream_connected.store(
            connected,
            nervix_primitives::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn stream_connected(&self) -> bool {
        self.inner
            .requests
            .stream_connected
            .load(nervix_primitives::sync::atomic::Ordering::Relaxed)
    }
    pub fn new(observer: &Observer) -> Self {
        let value = policy::evaluate_policy(observer.licensing_snapshot());
        Self {
            inner: Arc::new(PolicyPublication {
                policy: ArcSwap::from_pointee(value),
                requests: requests::RequestObservations::default(),
            }),
        }
    }

    pub(crate) fn record_request(
        &self,
        operation: nervix_licensing_protocol::requests::Operation,
        event: RequestEvent,
        at: nervix_models::Timestamp,
    ) {
        self.inner.requests.record(operation, event, at);
    }
    pub(crate) fn request_count(
        &self,
        operation: nervix_licensing_protocol::requests::Operation,
        outcome: RequestOutcome,
    ) -> u64 {
        self.inner.requests.count(operation, outcome)
    }
    pub(crate) fn connectivity_changes(
        &self,
    ) -> nervix_primitives::sync::watch::Receiver<Connectivity> {
        self.inner.requests.connectivity.subscribe()
    }
    pub(crate) fn connectivity(&self) -> Connectivity {
        *self.inner.requests.connectivity.borrow()
    }

    pub fn current(&self) -> PolicyView {
        PolicyView(self.inner.policy.load())
    }
    pub fn capability(&self, name: CapabilityName) -> CapabilityVerdict {
        self.current().capability(name)
    }
    pub fn processing_stop(&self) -> Option<LicensingStopReason> {
        self.current().processing_stop()
    }

    pub(crate) fn refresh(&self, observer: &Observer) {
        let value = policy::evaluate_policy(observer.licensing_snapshot());
        self.inner.policy.store(StdArc::new(value));
    }

    pub async fn follow(self, observer: Observer, shutdown: CancellationToken) {
        let mut applied = observer.subscribe_applied();
        loop {
            nervix_primitives::task::consume_budget().await;
            self.refresh(&observer);
            nervix_primitives::select! {
                _ = shutdown.cancelled() => break,
                result = applied.changed() => {
                    if result.is_err() { break; }
                },
            }
        }
    }
}
