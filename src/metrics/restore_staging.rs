//! Observations of local restore checkpoint storage.
//!
//! Layer: infrastructure.
//! - **Owns.** Storage observations and their node-local Prometheus series.
//! - **Depends on.** Metrics registration and numeric conversions.
//! - **Must not know.** Checkpoint keys, consensus retention or maintenance scheduling.

use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;
use prometheus::{Gauge, IntCounter, Registry, core::Collector};

#[derive(Debug, Default)]
pub(crate) struct RestoreStagingObservation {
    pub staged_bytes: u64,
    pub staged_keys: u64,
    pub limit_bytes: u64,
    pub reclaimed_bytes: u64,
    pub disk_bytes: u64,
}

#[derive(Debug, Clone)]
pub(super) struct RestoreStagingMetrics {
    bytes: Gauge,
    keys: Gauge,
    limit_bytes: Gauge,
    disk_bytes: Gauge,
    reclaimed_bytes: IntCounter,
    sweeps: IntCounter,
}

impl RestoreStagingMetrics {
    pub(super) fn register(registry: &Registry) -> Self {
        let gauge = |name, help| {
            Gauge::new(name, help).assured("fixed metric names and descriptions are valid")
        };
        let metrics = Self {
            bytes: gauge(
                "nervix_restore_staging_bytes",
                "Unpublished restore checkpoint key and value bytes at the last completed sweep.",
            ),
            keys: gauge(
                "nervix_restore_staging_keys",
                "Unpublished restore checkpoint keys at the last completed sweep.",
            ),
            limit_bytes: gauge(
                "nervix_restore_staging_limit_bytes",
                "Node-local unpublished restore checkpoint byte limit.",
            ),
            disk_bytes: gauge(
                "nervix_restore_keyspaces_disk_bytes",
                "SST allocation of shared checkpoint and publication keyspaces, including \
                 retained snapshots and pending compaction.",
            ),
            reclaimed_bytes: IntCounter::new(
                "nervix_restore_staging_reclaimed_bytes_total",
                "Restore checkpoint key and value bytes removed by completed maintenance sweeps.",
            )
            .assured("fixed metric names and descriptions are valid"),
            sweeps: IntCounter::new(
                "nervix_restore_staging_sweeps_total",
                "Completed restore checkpoint maintenance sweeps.",
            )
            .assured("fixed metric names and descriptions are valid"),
        };
        let collectors: [Box<dyn Collector>; 6] = [
            Box::new(metrics.bytes.clone()),
            Box::new(metrics.keys.clone()),
            Box::new(metrics.limit_bytes.clone()),
            Box::new(metrics.disk_bytes.clone()),
            Box::new(metrics.reclaimed_bytes.clone()),
            Box::new(metrics.sweeps.clone()),
        ];
        for collector in collectors {
            registry
                .register(collector)
                .assured("restore metrics are registered once with distinct fixed names");
        }
        metrics
    }

    pub(super) fn record(&self, observation: RestoreStagingObservation) {
        self.bytes.set(observation.staged_bytes.approx_into());
        self.keys.set(observation.staged_keys.approx_into());
        self.limit_bytes.set(observation.limit_bytes.approx_into());
        self.disk_bytes.set(observation.disk_bytes.approx_into());
        self.reclaimed_bytes.inc_by(observation.reclaimed_bytes);
        self.sweeps.inc();
    }
}
