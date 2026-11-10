//! Immutable observations of this node's configured control-plane cadences.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** A fixed metric family describing the local operational cadence values.
//! - **Depends on.** Vocabulary capability values and Prometheus descriptors and encodings.
//! - **Must not know.** Running tasks, consensus, graph decisions or how values were selected.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns immutable local cadence observations")
)]

use meticulous::ResultExt as _;
use nervix_models::ControlPlaneCapabilities;
use prometheus::{
    core::{Collector, Desc},
    proto::{Gauge, LabelPair, Metric, MetricFamily, MetricType},
};

/// Values are frozen at startup, so exposition needs no label map or synchronization.
#[derive(Debug)]
pub(super) struct ControlPlaneCadences {
    description: Desc,
    family: MetricFamily,
}

impl ControlPlaneCadences {
    pub(super) fn new(capabilities: &ControlPlaneCapabilities) -> Self {
        let name = "nervix_control_plane_cadence_seconds";
        let help = "Configured local control-plane cadence in seconds; changes on node restart.";
        let description = Desc::new(
            name.to_string(),
            help.to_string(),
            vec!["operation".to_string()],
            Default::default(),
        )
        .assured("the fixed metric and label names satisfy Prometheus naming rules");
        let mut family = MetricFamily::new();
        family.set_name(name.to_string());
        family.set_help(help.to_string());
        family.set_field_type(MetricType::GAUGE);
        for (operation, duration) in [
            ("membership", capabilities.membership_interval()),
            ("scheduling", capabilities.scheduling_interval()),
            (
                "resource_replication",
                capabilities.resource_replication_interval(),
            ),
            ("health", capabilities.health_probe_interval()),
            (
                "leadership_sync_retry",
                capabilities.leadership_sync_retry(),
            ),
            (
                "transaction_sweep",
                capabilities.transaction_sweep_interval(),
            ),
            (
                "restore_staging",
                capabilities.restore_staging_sweep_interval(),
            ),
        ] {
            let mut label = LabelPair::new();
            label.set_name("operation".to_string());
            label.set_value(operation.to_string());
            let mut gauge = Gauge::new();
            gauge.set_value(duration.as_secs_f64());
            let mut metric = Metric::new();
            metric.set_label(vec![label]);
            metric.set_gauge(gauge);
            family.mut_metric().push(metric);
        }
        Self {
            description,
            family,
        }
    }
}

impl Collector for ControlPlaneCadences {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.description]
    }
    fn collect(&self) -> Vec<MetricFamily> {
        vec![self.family.clone()]
    }
}
