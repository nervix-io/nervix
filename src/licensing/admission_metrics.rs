//! Process-local licensing admission counters.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Bounded refusal series and their resolved counter handles.
//! - **Depends on.** Shared quota/refusal vocabulary and the metrics registry.
//! - **Must not know.** Policy evaluation, membership, domains or consensus application.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns licensing admission observations")
)]

use meticulous::ResultExt as _;
use nervix_licensing_vocabulary::{LicensingRefusal, QuotaName};
use prometheus::{IntCounter, IntCounterVec, Opts};

use crate::metrics::RuntimeMetrics;

pub(super) struct AdmissionMetrics {
    nodes: IntCounter,
    domains: IntCounter,
    lapsed_nodes: IntCounter,
    lapsed_domains: IntCounter,
    stopped_nodes: IntCounter,
    stopped_domains: IntCounter,
}

impl AdmissionMetrics {
    pub(super) fn register(metrics: &RuntimeMetrics) -> Self {
        let family = IntCounterVec::new(
            Opts::new(
                "nervix_licensing_admissions_refused_total",
                "Licensing refusals returned by an admission owner on this node.",
            ),
            &["condition", "state"],
        )
        .assured("licensing admission metric names are fixed");
        for name in QuotaName::ALL {
            for state in ["within_grace", "over_limit", "lapsed", "stopped"] {
                family.with_label_values(&[name.as_str(), state]);
            }
        }
        metrics
            .register_collector(Box::new(family.clone()))
            .assured("licensing admission metrics are registered once per node");
        Self {
            lapsed_nodes: family.with_label_values(&[QuotaName::ClusterNodes.as_str(), "lapsed"]),
            lapsed_domains: family
                .with_label_values(&[QuotaName::ClusterDomains.as_str(), "lapsed"]),
            nodes: family.with_label_values(&[QuotaName::ClusterNodes.as_str(), "over_limit"]),
            domains: family.with_label_values(&[QuotaName::ClusterDomains.as_str(), "over_limit"]),
            stopped_nodes: family.with_label_values(&[QuotaName::ClusterNodes.as_str(), "stopped"]),
            stopped_domains: family
                .with_label_values(&[QuotaName::ClusterDomains.as_str(), "stopped"]),
        }
    }

    pub(super) fn record(&self, refusal: &LicensingRefusal) {
        match (refusal.condition, refusal.state) {
            (QuotaName::ClusterNodes, nervix_licensing_vocabulary::AdmissionState::OverLimit) => {
                self.nodes.inc()
            }
            (QuotaName::ClusterDomains, nervix_licensing_vocabulary::AdmissionState::OverLimit) => {
                self.domains.inc()
            }
            (QuotaName::ClusterNodes, nervix_licensing_vocabulary::AdmissionState::Lapsed) => {
                self.lapsed_nodes.inc()
            }
            (QuotaName::ClusterDomains, nervix_licensing_vocabulary::AdmissionState::Lapsed) => {
                self.lapsed_domains.inc()
            }
            (QuotaName::ClusterNodes, nervix_licensing_vocabulary::AdmissionState::Stopped) => {
                self.stopped_nodes.inc()
            }
            (QuotaName::ClusterDomains, nervix_licensing_vocabulary::AdmissionState::Stopped) => {
                self.stopped_domains.inc()
            }
        }
    }
}
