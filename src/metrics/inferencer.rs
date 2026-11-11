//! Layer: engines and infrastructure.
//! Owns: unit-scoped inference state series, retained recorders and their exact withdrawal.
//! Depends on: vocabulary labels, Prometheus and the primitive synchronization boundary.
//! Must not know: tensors, publication, persistence or graph decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "records supplied inference state observations")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(observer, reason = "registers and inspects unit series")
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;
use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName, Unit};
use nervix_primitives::{
    collections::{DashMap, dash_map::Entry},
    sync::{Arc, StdArc, StdWeak, blocking::Mutex},
    time::Instant,
};
use prometheus::{Gauge, GaugeVec, IntCounter, IntCounterVec, Opts, Registry};

use super::{MetricKey, PROMETHEUS_LABELS, RuntimeMetrics, prometheus_label_values};

const STATE_BYTES: &str = "inferencer_state_bytes";
const SNAPSHOT_FAILURES: &str = "inferencer_snapshot_failures_total";
const RESTORED_BRANCHES: &str = "inferencer_restored_branches_total";
const RESET_BRANCHES: &str = "inferencer_reset_branches_total";

#[derive(Debug)]
pub(super) struct InferencerStateMetricSeries {
    bytes: GaugeVec,
    failures: IntCounterVec,
    restored: IntCounterVec,
    reset: IntCounterVec,
    /// Only live units and their retained reporting handles keep a child alive.
    children: DashMap<MetricKey, StdWeak<Children>>,
}

#[derive(Debug)]
struct Children {
    group: Arc<InferencerStateMetricSeries>,
    identity: StdWeak<Children>,
    key: MetricKey,
    bytes: Gauge,
    failures: IntCounter,
    restored: IntCounter,
    reset: IntCounter,
    last_warning: Mutex<Option<Instant>>,
}

/// A weak reference is needed only inside this owner to withdraw children at their last lease.
#[derive(Clone, Debug)]
pub(crate) struct InferencerStateMetrics(StdArc<Children>);

/// One active state session's logical native tensor footprint, counted once over its lifetime.
#[derive(Debug)]
pub(crate) struct InferencerBranchBytes {
    metrics: InferencerStateMetrics,
    bytes: u64,
}

impl InferencerStateMetricSeries {
    pub(super) fn register(registry: &Registry) -> Arc<Self> {
        let bytes = GaugeVec::new(
            Opts::new(
                STATE_BYTES,
                "Declared native tensor bytes held by active inference branches.",
            )
            .namespace("nervix"),
            PROMETHEUS_LABELS,
        )
        .assured("constant inference metric names and labels are valid");
        registry
            .register(Box::new(bytes.clone()))
            .assured("inference state bytes register once");
        Arc::new(Self {
            bytes,
            failures: counter(
                registry,
                SNAPSHOT_FAILURES,
                "Failed inference snapshot publication or persistence attempts.",
            ),
            restored: counter(
                registry,
                RESTORED_BRANCHES,
                "Inference branches initialized from matching tensor generations.",
            ),
            reset: counter(
                registry,
                RESET_BRANCHES,
                "Inference branches initialized from INITIAL, fresh branches included.",
            ),
            children: DashMap::new(),
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "binds a unit's retained metric children once")
    )]
    fn resolve(group: &Arc<Self>, key: MetricKey) -> InferencerStateMetrics {
        let labels = prometheus_label_values(&key).map(|label| label.into_owned());
        match group.children.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                if let Some(children) = entry.get().upgrade() {
                    return InferencerStateMetrics(children);
                }
                // A last-drop destructor can be waiting for this entry. Withdraw its labels
                // before registering a replacement; that destructor will compare identities.
                group.remove_labels(&labels);
                let children = Self::create_children(group, key, labels);
                entry.insert(StdArc::downgrade(&children));
                InferencerStateMetrics(children)
            }
            Entry::Vacant(entry) => {
                let children = Self::create_children(group, key, labels);
                entry.insert(StdArc::downgrade(&children));
                InferencerStateMetrics(children)
            }
        }
    }

    fn create_children(
        group: &Arc<Self>,
        key: MetricKey,
        labels: [String; 10],
    ) -> StdArc<Children> {
        StdArc::new_cyclic(|identity| Children {
            group: group.clone(),
            identity: identity.clone(),
            key,
            bytes: group.bytes.with_label_values(&labels),
            failures: group.failures.with_label_values(&labels),
            restored: group.restored.with_label_values(&labels),
            reset: group.reset.with_label_values(&labels),
            last_warning: Mutex::new(None),
        })
    }

    fn remove_labels(&self, labels: &[String; 10]) {
        self.bytes
            .remove_label_values(labels)
            .assured("the current unit owns its registered labels");
        self.failures
            .remove_label_values(labels)
            .assured("the current unit owns its registered labels");
        self.restored
            .remove_label_values(labels)
            .assured("the current unit owns its registered labels");
        self.reset
            .remove_label_values(labels)
            .assured("the current unit owns its registered labels");
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "withdraws the exact final unit recorder")
    )]
    fn retire(&self, key: &MetricKey, identity: &StdWeak<Children>) {
        if let Entry::Occupied(entry) = self.children.entry(key.clone())
            && StdWeak::ptr_eq(entry.get(), identity)
        {
            self.remove_labels(&prometheus_label_values(key).map(|label| label.into_owned()));
            entry.remove();
        }
    }
}

fn counter(registry: &Registry, name: &str, help: &str) -> IntCounterVec {
    let counter = IntCounterVec::new(Opts::new(name, help).namespace("nervix"), PROMETHEUS_LABELS)
        .assured("constant inference metric names and labels are valid");
    registry
        .register(Box::new(counter.clone()))
        .assured("each inference family registers once");
    counter
}

impl RuntimeMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves inference recorders at unit and branch startup"
        )
    )]
    pub(crate) fn inferencer_state_metrics(
        &self,
        domain: &DomainName,
        name: &ModelName,
        unit: Unit,
        physical_node: Option<&ClusterNodeName>,
    ) -> InferencerStateMetrics {
        let mut key = MetricKey::node_without_stream(
            domain,
            ModelKind::Inferencer,
            name,
            physical_node,
            "state",
            STATE_BYTES,
        );
        key.unit = unit;
        InferencerStateMetricSeries::resolve(&self.series.inferencer, key)
    }
}

impl InferencerStateMetrics {
    pub(crate) fn track_branch(&self, bytes: u64) -> InferencerBranchBytes {
        self.0.bytes.add(bytes.approx_into());
        InferencerBranchBytes {
            metrics: self.clone(),
            bytes,
        }
    }

    pub(crate) fn initialized(&self, restored: bool) {
        if restored {
            self.0.restored.inc();
        } else {
            self.0.reset.inc();
        }
    }

    /// Count every failed attempt; only its warning is limited to the configured cadence.
    pub(crate) fn snapshot_failed(&self, at: Instant, cadence: Duration) -> bool {
        self.0.failures.inc();
        let mut previous = self.0.last_warning.lock();
        if let Some(previous) = *previous {
            match at.checked_duration_since(previous) {
                Some(elapsed) if elapsed >= cadence => {}
                _ => return false,
            }
        }
        *previous = Some(at);
        true
    }
}

impl Drop for InferencerBranchBytes {
    fn drop(&mut self) {
        self.metrics.0.bytes.sub(self.bytes.approx_into());
    }
}

impl InferencerBranchBytes {
    pub(crate) fn recorder(&self) -> InferencerStateMetrics {
        self.metrics.clone()
    }
}

impl Drop for Children {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "retires only this unit's final metric identity")
    )]
    fn drop(&mut self) {
        self.group.retire(&self.key, &self.identity);
    }
}

#[cfg(test)]
#[path = "inferencer/tests.rs"]
mod tests;
