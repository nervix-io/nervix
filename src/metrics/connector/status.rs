//! Layer: engines and infrastructure.
//! - **Owns.** A metric projection of the current connector task's failure observation, for each
//!   transient-failure family that declares one, and withdrawal of a retired identity's series.
//! - **Depends on.** Shared ownership, atomic publication and the Prometheus collector contract.
//! - **Must not know.** Runtime status types, failure policy or connector drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "projects a caller-owned observation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(observer, reason = "samples the existing failure owner")
)]

use std::fmt;

use meticulous::ResultExt as _;
use nervix_primitives::{
    publication::ArcSwapOption,
    sync::{Arc, StdArc},
};
use prometheus::{
    Registry,
    core::{Collector, Desc},
    proto::{Gauge, Metric, MetricFamily, MetricType},
};

use super::EMITTER_LABELS;

/// One transient-failure gauge family: its name, its help and the identity labels of its series.
#[derive(Debug)]
pub(super) struct FailureFamily {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
}

impl FailureFamily {
    pub(super) const fn new(
        name: &'static str,
        help: &'static str,
        labels: &'static [&'static str],
    ) -> Self {
        Self { name, help, labels }
    }

    /// The family's metric name.
    pub(super) const fn name(&self) -> &'static str {
        self.name
    }
}

/// The emitter family, whose series each emitter identity registers once.
pub(super) const EMITTER_FAILURE: FailureFamily = FailureFamily::new(
    "nervix_emitter_transient_failure",
    "Whether the current emitter task has a published host or connector failure.",
    &EMITTER_LABELS,
);

struct FailureObservation {
    failed: Box<dyn Fn() -> bool + Send + Sync>,
}

/// Ending the task withdraws only its exact observation, even while a collection retains it.
#[must_use]
pub(crate) struct TaskFailureBinding {
    projection: Arc<FailureProjection>,
    observation: StdArc<FailureObservation>,
}

impl Drop for TaskFailureBinding {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "withdraws only the ending task's observation")
    )]
    fn drop(&mut self) {
        self.projection.current.rcu(|current| match current {
            Some(current) if StdArc::ptr_eq(current, &self.observation) => None,
            current => current.clone(),
        });
    }
}

pub(super) struct FailureProjection {
    current: ArcSwapOption<FailureObservation>,
    /// The family and identity of the one series this projection registered.
    descriptor: Desc,
}

impl fmt::Debug for FailureProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FailureProjection")
            .field("family", &self.descriptor.fq_name)
            .finish_non_exhaustive()
    }
}

impl FailureProjection {
    /// Registers the series of `family` whose identity labels take `values`, in the order the
    /// family declares them, at `0` until a task binds its failure observation.
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers one retained identity's collector")
    )]
    pub(super) fn register(
        registry: &Registry,
        family: &FailureFamily,
        values: &[&str],
    ) -> Arc<Self> {
        debug_assert_eq!(
            family.labels.len(),
            values.len(),
            "a failure series names a value for every identity label of its family"
        );
        // The descriptor constructor owns its required foreign map representation.
        let labels = family
            .labels
            .iter()
            .zip(values)
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        let descriptor = Desc::new(
            family.name.to_owned(),
            family.help.to_owned(),
            Vec::new(),
            labels,
        )
        .assured("the failure family and identity labels are fixed valid identifiers");
        let projection = Arc::new(Self {
            current: ArcSwapOption::empty(),
            descriptor,
        });
        registry
            .register(Box::new(FailureCollector {
                projection: projection.clone(),
            }))
            .assured("each distinct identity registers its failure collector once");
        projection
    }

    /// Removes the series `projection` registered, for an identity whose series retire with it.
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "withdraws one retired identity's collector")
    )]
    pub(super) fn withdraw(projection: &Arc<Self>, registry: &Registry) {
        registry
            .unregister(Box::new(FailureCollector {
                projection: projection.clone(),
            }))
            .assured("a retiring identity withdraws the collector it registered once");
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "binds the exact starting task's observation")
    )]
    pub(super) fn bind(
        projection: &Arc<Self>,
        failed: impl Fn() -> bool + Send + Sync + 'static,
    ) -> TaskFailureBinding {
        let observation = StdArc::new(FailureObservation {
            failed: Box::new(failed),
        });
        projection.current.store(Some(observation.clone()));
        TaskFailureBinding {
            projection: projection.clone(),
            observation,
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the lifecycle-bound callback reads only its caller's failure publication"
        )
    )]
    pub(super) fn value(&self) -> f64 {
        let current = self.current.load();
        let Some(observation) = current.as_ref() else {
            return 0.0;
        };
        if (observation.failed)() { 1.0 } else { 0.0 }
    }
}

struct FailureCollector {
    projection: Arc<FailureProjection>,
}

impl Collector for FailureCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.projection.descriptor]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let descriptor = &self.projection.descriptor;
        let mut gauge = Gauge::new();
        gauge.set_value(self.projection.value());
        let mut metric = Metric::new();
        metric.set_label(descriptor.const_label_pairs.clone());
        metric.set_gauge(gauge);
        let mut family = MetricFamily::new();
        family.set_name(descriptor.fq_name.clone());
        family.set_help(descriptor.help.clone());
        family.set_field_type(MetricType::GAUGE);
        family.mut_metric().push(metric);
        vec![family]
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the production observation lifetime")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** A collection overlapping the end of its task's failure observation, and the
    //!   withdrawal of a retired identity's series.
    //! - **Depends on.** The production projection and shared ownership.
    //! - **Must not know.** Runtime tasks, failure policy or connector implementations.

    use meticulous::OptionExt as _;

    use super::*;

    const IDENTITY: [&str; 5] = ["domain", "emitter", "node-1", "REDIS", "PUBSUB"];

    #[test]
    fn ending_a_task_with_an_in_flight_collection_unpublishes_its_failure() {
        let projection = FailureProjection::register(&Registry::new(), &EMITTER_FAILURE, &IDENTITY);
        let binding = FailureProjection::bind(&projection, || true);
        let collection = projection
            .current
            .load_full()
            .assured("collection borrows the running task's published observation");
        drop(binding);
        assert!(
            (collection.failed)(),
            "the in-flight collection retains its source"
        );
        assert_eq!(
            projection.value(),
            0.0,
            "a later collection sees the task ended"
        );
    }

    #[test]
    fn withdrawing_an_identity_removes_only_its_series_and_allows_registering_it_again() {
        let registry = Registry::new();
        let withdrawn = FailureProjection::register(&registry, &EMITTER_FAILURE, &IDENTITY);
        let sibling_identity = ["domain", "sibling", "node-1", "REDIS", "PUBSUB"];
        let sibling = FailureProjection::register(&registry, &EMITTER_FAILURE, &sibling_identity);
        let _sibling_binding = FailureProjection::bind(&sibling, || true);
        FailureProjection::withdraw(&withdrawn, &registry);
        let families = registry.gather();
        let series = families
            .iter()
            .filter(|family| family.name() == "nervix_emitter_transient_failure")
            .flat_map(|family| family.get_metric())
            .collect::<Vec<_>>();
        assert_eq!(series.len(), 1, "only the sibling's series remains");
        assert_eq!(series[0].get_gauge().value(), 1.0);
        let registered_again = FailureProjection::register(&registry, &EMITTER_FAILURE, &IDENTITY);
        assert_eq!(registered_again.value(), 0.0);
    }
}
