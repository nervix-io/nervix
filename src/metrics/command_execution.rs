//! Process-local observations of persistent-command admission refusals.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Four bounded refusal counters, resolved once at metric registration.
//! - **Depends on.** Typed consensus admission outcomes and Prometheus handles.
//! - **Must not know.** Execution identities, users, command effects or admission policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the infrastructure boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_consensus::CommandExecutionAdmissionError;
use prometheus::{IntCounter, IntCounterVec, Opts, Registry};

#[derive(Debug, Clone)]
pub(super) struct CommandExecutionMetrics {
    expired: IntCounter,
    future: IntCounter,
    missing_timestamp: IntCounter,
    capacity: IntCounter,
}

impl CommandExecutionMetrics {
    pub(super) fn register(registry: &Registry) -> Self {
        let refusals = IntCounterVec::new(
            Opts::new(
                "nervix_command_execution_admission_refusals_total",
                "Definitive command-history admission refusals handled by this node process.",
            ),
            &["reason"],
        )
        .assured("the fixed metric and label names satisfy Prometheus naming rules");
        registry
            .register(Box::new(refusals.clone()))
            .assured("command history refusals are registered once with a distinct name");
        Self {
            expired: refusals.with_label_values(&["expired"]),
            future: refusals.with_label_values(&["future"]),
            missing_timestamp: refusals.with_label_values(&["missing_timestamp"]),
            capacity: refusals.with_label_values(&["capacity"]),
        }
    }

    pub(super) fn record(&self, refusal: &CommandExecutionAdmissionError) {
        match refusal {
            CommandExecutionAdmissionError::Expired => self.expired.inc(),
            CommandExecutionAdmissionError::Future => self.future.inc(),
            CommandExecutionAdmissionError::MissingTimestamp => self.missing_timestamp.inc(),
            CommandExecutionAdmissionError::Capacity { .. } => self.capacity.inc(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn samples(registry: &Registry) -> BTreeMap<String, f64> {
        let families = registry.gather();
        assert_eq!(families.len(), 1);
        assert_eq!(
            families[0].name(),
            "nervix_command_execution_admission_refusals_total"
        );
        let mut samples = BTreeMap::new();
        for metric in families[0].get_metric() {
            assert_eq!(metric.get_label().len(), 1);
            assert_eq!(metric.get_label()[0].name(), "reason");
            samples.insert(
                metric.get_label()[0].value().to_string(),
                metric.get_counter().value(),
            );
        }
        samples
    }

    #[test]
    fn typed_refusals_have_four_initialized_and_independent_process_counters() {
        let registry = Registry::new();
        let metrics = CommandExecutionMetrics::register(&registry);
        let empty = BTreeMap::from([
            ("expired".to_string(), 0.0),
            ("future".to_string(), 0.0),
            ("missing_timestamp".to_string(), 0.0),
            ("capacity".to_string(), 0.0),
        ]);
        assert_eq!(samples(&registry), empty);
        for refusal in [
            CommandExecutionAdmissionError::Expired,
            CommandExecutionAdmissionError::Future,
            CommandExecutionAdmissionError::MissingTimestamp,
            CommandExecutionAdmissionError::Capacity { capacity: 2 },
            CommandExecutionAdmissionError::Capacity { capacity: 100 },
        ] {
            metrics.record(&refusal);
        }
        assert_eq!(
            samples(&registry),
            BTreeMap::from([
                ("expired".to_string(), 1.0),
                ("future".to_string(), 1.0),
                ("missing_timestamp".to_string(), 1.0),
                ("capacity".to_string(), 2.0),
            ])
        );
        let another_process = Registry::new();
        let _metrics = CommandExecutionMetrics::register(&another_process);
        assert_eq!(samples(&another_process), empty);
    }
}
