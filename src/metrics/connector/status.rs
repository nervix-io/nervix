//! Layer: engines and infrastructure.
//! - **Owns.** A metric projection of the current emitter task's failure observation.
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

pub(super) const NAME: &str = "nervix_emitter_transient_failure";
const HELP: &str = "Whether the current emitter task has a published host or connector failure.";

struct FailureObservation {
    failed: Box<dyn Fn() -> bool + Send + Sync>,
}

/// Ending the task withdraws only its exact observation, even while a collection retains it.
#[must_use]
pub(crate) struct EmitterFailureBinding {
    projection: Arc<FailureProjection>,
    observation: StdArc<FailureObservation>,
}

impl Drop for EmitterFailureBinding {
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

#[derive(Default)]
pub(super) struct FailureProjection {
    current: ArcSwapOption<FailureObservation>,
}

impl fmt::Debug for FailureProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FailureProjection")
            .finish_non_exhaustive()
    }
}

impl FailureProjection {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers one retained identity's collector")
    )]
    pub(super) fn register(registry: &Registry, values: &[&str; 6]) -> Arc<Self> {
        // The descriptor constructor owns its required foreign map representation.
        let labels = EMITTER_LABELS
            .iter()
            .zip(values)
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        let descriptor = Desc::new(NAME.to_owned(), HELP.to_owned(), Vec::new(), labels)
            .assured("the failure family and identity labels are fixed valid identifiers");
        let projection = Arc::new(Self::default());
        registry
            .register(Box::new(FailureCollector {
                descriptor,
                projection: projection.clone(),
            }))
            .assured("each distinct emitter identity registers its failure collector once");
        projection
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "binds the exact starting task's observation")
    )]
    pub(super) fn bind(
        projection: &Arc<Self>,
        failed: impl Fn() -> bool + Send + Sync + 'static,
    ) -> EmitterFailureBinding {
        let observation = StdArc::new(FailureObservation {
            failed: Box::new(failed),
        });
        projection.current.store(Some(observation.clone()));
        EmitterFailureBinding {
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
    descriptor: Desc,
    projection: Arc<FailureProjection>,
}

impl Collector for FailureCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.descriptor]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut gauge = Gauge::new();
        gauge.set_value(self.projection.value());
        let mut metric = Metric::new();
        metric.set_label(self.descriptor.const_label_pairs.clone());
        metric.set_gauge(gauge);
        let mut family = MetricFamily::new();
        family.set_name(NAME.to_owned());
        family.set_help(HELP.to_owned());
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
    //! - **Owns.** A collection overlapping the end of its task's failure observation.
    //! - **Depends on.** The production projection and shared ownership.
    //! - **Must not know.** Runtime tasks, failure policy or connector implementations.

    use meticulous::OptionExt as _;

    use super::*;

    #[test]
    fn ending_a_task_with_an_in_flight_collection_unpublishes_its_failure() {
        let projection = Arc::new(FailureProjection::default());
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
}
