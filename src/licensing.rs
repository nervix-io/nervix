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

mod admission_metrics;
pub(crate) mod metrics;
pub mod policy;

use admission_metrics::AdmissionMetrics;
use meticulous::OptionExt as _;
use nervix_consensus::{LicensingSnapshot, Observer};
use nervix_licensing_protocol::conditions::Capability;
pub use nervix_licensing_vocabulary::{LicensingRefusal, Quota, QuotaConstraint, QuotaName};
use nervix_primitives::{
    publication::{ArcSwap, Guard},
    sync::{Arc, CancellationToken, StdArc},
};
pub use policy::{CapabilityName, LicensingPolicy, StopReason};

use crate::{ConfiguredFaultInjection, metrics::RuntimeMetrics};

struct PolicyPublication {
    policy: ArcSwap<LicensingPolicy>,
    fault: ConfiguredFaultInjection,
    admissions: AdmissionMetrics,
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

fn for_node(snapshot: LicensingSnapshot, fault: &ConfiguredFaultInjection) -> LicensingPolicy {
    let mut policy = policy::evaluate_policy(snapshot);
    for name in QuotaName::ALL {
        let quota = fault.licensing_quota(name, policy.quota(name));
        *policy
            .conditions
            .quotas
            .get_mut(name.as_str())
            .verified("the current free catalog contains this cluster quota") =
            nervix_licensing_protocol::conditions::ConditionValue::Known(quota);
    }
    policy
}

impl PolicyHandle {
    pub fn new(
        observer: &Observer,
        metrics: &RuntimeMetrics,
        fault: ConfiguredFaultInjection,
    ) -> Self {
        let value = for_node(observer.licensing_snapshot(), &fault);
        Self {
            inner: Arc::new(PolicyPublication {
                policy: ArcSwap::from_pointee(value),
                fault,
                admissions: AdmissionMetrics::register(metrics),
            }),
        }
    }

    pub fn current(&self) -> PolicyView {
        PolicyView(self.inner.policy.load())
    }
    pub fn capability(&self, name: CapabilityName) -> Capability {
        self.current().capability(name)
    }
    pub fn quota(&self, name: QuotaName) -> Quota {
        self.current().quota(name)
    }
    pub fn processing_stop(&self) -> Option<StopReason> {
        self.current().processing_stop()
    }

    pub fn admit(
        &self,
        name: QuotaName,
        current_usage: u64,
        requested: u64,
        replacement: bool,
    ) -> error_stack::Result<QuotaConstraint, LicensingRefusal> {
        self.current()
            .admit(name, current_usage, requested, replacement)
            .inspect_err(|report| self.record_refusal(report))
    }

    pub fn record_refusal<C>(&self, report: &error_stack::Report<C>) {
        if let Some(refusal) = report.downcast_ref::<LicensingRefusal>() {
            self.inner.admissions.record(refusal);
            tracing::warn!(error = %format_args!("{report:#}"), "licensing refused admission");
        }
    }

    pub(crate) fn refresh(&self, observer: &Observer) {
        let value = for_node(observer.licensing_snapshot(), &self.inner.fault);
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
