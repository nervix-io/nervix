//! Process-local observations of a host's connector interactions.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Interaction and payload families, zero registration, retained children and inspection.
//! - **Depends on.** Vocabulary identities, the existing atomic rate owner and Prometheus.
//! - **Must not know.** Connector drivers, payload values, delivery policy or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the infrastructure boundary declared above")
)]

use std::{collections::hash_map::Entry, time::Duration};

use ahash::HashMap;
use meticulous::ResultExt as _;
use nervix_models::{
    ClusterNodeName, DomainName, DomainNodeRef, EmitterEntityKind, EmitterName, ModelKind,
};
use nervix_primitives::{collections::DashMap, sync::Arc, time::Instant};
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry,
};
use strum::AsRefStr;

use super::{CounterSeries, RuntimeMetrics, format_number, format_optional, physical_node_label};

mod ingestor;
pub(crate) use ingestor::IngestorInteractionMetrics;
mod ingestor_reads;
use ingestor_reads::IngestorReadFamilies;
pub(crate) use ingestor_reads::IngestorReadMetrics;
mod payloads;
pub(crate) use payloads::{EmitterWork, EmitterWorkKind};

#[cfg(test)]
mod interaction_metrics;

const PUBLISH_ATTEMPTS: &str = "nervix_emitter_publish_attempts_total";
pub(super) const PUBLISH_DURATION: &str = "nervix_emitter_publish_duration_seconds";
const EMITTER_LABELS: [&str; 5] = [
    "domain",
    "emitter",
    "physical_node_id",
    "connector",
    "entity",
];
const ATTEMPT_LABELS: &[&str] = &[
    "domain",
    "emitter",
    "physical_node_id",
    "connector",
    "entity",
    "operation",
    "outcome",
];
const DURATION_LABELS: &[&str] = &[
    "domain",
    "emitter",
    "physical_node_id",
    "connector",
    "entity",
    "operation",
];

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the published interaction-duration resolution spans milliseconds through thirty \
                  seconds, with an unbounded final Prometheus bucket"
    )
)]
const DURATION_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0];

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum EmitterAttemptOperation {
    Publish,
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum EmitterAttemptOutcome {
    Succeeded,
    Transient,
    Misconfigured,
    Violation,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EmitterSeriesKey {
    physical_node: Option<ClusterNodeName>,
    kind: EmitterEntityKind,
}

#[derive(Debug)]
pub(super) struct ConnectorInteractionMetrics {
    /// Only registration and observation reach this registry. Tasks keep the returned handle.
    emitters: DashMap<DomainNodeRef, HashMap<EmitterSeriesKey, EmitterInteractionMetrics>>,
    attempts: IntCounterVec,
    durations: HistogramVec,
    payloads: payloads::PayloadFamilies,
    reads: IngestorReadFamilies,
}

impl ConnectorInteractionMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "registers distinct families once when the node creates its metric registry"
        )
    )]
    pub(super) fn register(registry: &Registry) -> Self {
        let attempts = IntCounterVec::new(
            Opts::new(
                PUBLISH_ATTEMPTS,
                "Completed host publish and commit attempts by their typed outcome.",
            ),
            ATTEMPT_LABELS,
        )
        .assured("fixed interaction family and label names satisfy Prometheus naming rules");
        let durations = HistogramVec::new(
            HistogramOpts::new(
                PUBLISH_DURATION,
                "Elapsed wall seconds per host publish or commit attempt.",
            )
            .buckets(DURATION_BUCKETS.to_vec()),
            DURATION_LABELS,
        )
        .assured("fixed interaction family, labels and increasing finite buckets are valid");
        registry
            .register(Box::new(attempts.clone()))
            .assured("the node registers this distinct interaction family exactly once");
        registry
            .register(Box::new(durations.clone()))
            .assured("the node registers this distinct interaction family exactly once");
        Self {
            emitters: DashMap::new(),
            attempts,
            durations,
            payloads: payloads::PayloadFamilies::register(registry),
            reads: IngestorReadFamilies::register(registry),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves every label child once for the starting emitter task"
        )
    )]
    fn register_emitter(
        &self,
        entity: DomainNodeRef,
        physical_node: Option<&ClusterNodeName>,
        kind: EmitterEntityKind,
    ) -> EmitterInteractionMetrics {
        let key = EmitterSeriesKey {
            physical_node: physical_node.cloned(),
            kind,
        };
        let mut scopes = self.emitters.entry(entity.clone()).or_default();
        match scopes.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let key = entry.key();
                let connector = key.kind.connector().to_string();
                let values = [
                    entity.domain.as_str(),
                    entity.identifier().as_str(),
                    physical_node_label(key.physical_node.as_ref()),
                    connector.as_str(),
                    key.kind.entity_keyword(),
                ];
                let metrics = EmitterInteractionMetrics {
                    series: Arc::new(EmitterInteractions {
                        selector: EmitterInteractions::selector(&values),
                        payloads: self.payloads.emitter(&values),
                        publish: OperationMetrics::register(
                            self,
                            &values,
                            EmitterAttemptOperation::Publish,
                        ),
                        commit: OperationMetrics::register(
                            self,
                            &values,
                            EmitterAttemptOperation::Commit,
                        ),
                    }),
                };
                entry.insert(metrics.clone());
                metrics
            }
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "DESCRIBE resolves one target's process-local metric history outside \
                      publication"
        )
    )]
    fn describe_emitter(&self, entity: &DomainNodeRef) -> Vec<String> {
        let Some(scopes) = self.emitters.get(entity) else {
            return Vec::new();
        };
        let mut metrics = scopes.values().cloned().collect::<Vec<_>>();
        drop(scopes);
        metrics.sort_by(|left, right| left.series.selector.cmp(&right.series.selector));
        let mut lines = vec!["destination:".to_string()];
        for metric in metrics {
            for line in metric.describe_lines() {
                lines.push(format!("  {line}"));
            }
        }
        lines
    }
}

impl RuntimeMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "the starting emitter resolves its entire interaction handle"
        )
    )]
    pub(crate) fn register_emitter_interactions(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        physical_node: Option<&ClusterNodeName>,
        kind: EmitterEntityKind,
    ) -> EmitterInteractionMetrics {
        let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.clone());
        self.series
            .connector
            .register_emitter(entity, physical_node, kind)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "owner-routed emitter inspection reads its process-local interaction counters"
        )
    )]
    pub(crate) fn describe_emitter_interactions(&self, entity: &DomainNodeRef) -> Vec<String> {
        self.series.connector.describe_emitter(entity)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EmitterInteractionMetrics {
    /// DESCRIBE and later task generations independently retain the same process-local series.
    series: Arc<EmitterInteractions>,
}

#[derive(Debug)]
struct EmitterInteractions {
    selector: String,
    publish: OperationMetrics,
    commit: OperationMetrics,
    payloads: payloads::PayloadMetrics,
}

impl EmitterInteractions {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "formats the immutable selector at task registration"
        )
    )]
    fn selector(values: &[&str; 5]) -> String {
        let mut selector = String::new();
        for (name, value) in EMITTER_LABELS.iter().zip(values) {
            if !selector.is_empty() {
                selector.push(',');
            }
            let escaped = value
                .replace('\\', "\\\\")
                .replace('\n', "\\n")
                .replace('"', "\\\"");
            selector.push_str(&format!("{name}=\"{escaped}\""));
        }
        selector
    }
}

impl EmitterInteractionMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "an attempt borrows only its pre-resolved metric children"
        )
    )]
    pub(crate) fn begin(&self, operation: EmitterAttemptOperation) -> EmitterAttempt<'_> {
        let metrics = match operation {
            EmitterAttemptOperation::Publish => &self.series.publish,
            EmitterAttemptOperation::Commit => &self.series.commit,
        };
        EmitterAttempt {
            metrics,
            started_at: Instant::now(),
            outcome: EmitterAttemptOutcome::Transient,
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "renders retained atomic counters and Prometheus histogram samples for \
                      DESCRIBE"
        )
    )]
    pub(crate) fn describe_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for (operation, metrics) in [
            (EmitterAttemptOperation::Publish, &self.series.publish),
            (EmitterAttemptOperation::Commit, &self.series.commit),
        ] {
            metrics.describe_lines(&self.series.selector, operation, &mut lines);
        }
        self.series
            .payloads
            .describe_lines(&self.series.selector, &mut lines);
        lines
    }
}

/// Records once when a completed attempt or its cancelled future leaves its owning scope.
#[must_use]
pub(crate) struct EmitterAttempt<'a> {
    metrics: &'a OperationMetrics,
    started_at: Instant,
    /// Cancellation learns no definitive destination answer and therefore remains transient.
    outcome: EmitterAttemptOutcome,
}

impl EmitterAttempt<'_> {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "settles the retained attempt without resolving labels"
        )
    )]
    pub(crate) fn finish(mut self, outcome: EmitterAttemptOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for EmitterAttempt<'_> {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "finishing or cancelling one attempt updates only retained atomic metric \
                      children"
        )
    )]
    fn drop(&mut self) {
        self.metrics.record(self.outcome, self.started_at.elapsed());
    }
}

#[derive(Debug)]
struct AttemptCounter {
    prometheus: IntCounter,
    series: CounterSeries,
}

impl AttemptCounter {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves one bounded operation/outcome child at startup"
        )
    )]
    fn register(
        store: &ConnectorInteractionMetrics,
        values: &[&str; 5],
        operation: EmitterAttemptOperation,
        outcome: EmitterAttemptOutcome,
    ) -> Self {
        let mut labels = values.to_vec();
        labels.push(operation.as_ref());
        labels.push(outcome.as_ref());
        Self {
            prometheus: store.attempts.with_label_values(&labels),
            series: CounterSeries::default(),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "increments only retained atomic counter owners")
    )]
    fn record(&self) {
        self.prometheus.inc();
        self.series.increment(1, None);
    }
}

#[derive(Debug)]
struct OperationMetrics {
    succeeded: AttemptCounter,
    transient: AttemptCounter,
    misconfigured: AttemptCounter,
    violation: AttemptCounter,
    duration: Histogram,
}

impl OperationMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves the operation's complete child set at startup"
        )
    )]
    fn register(
        store: &ConnectorInteractionMetrics,
        values: &[&str; 5],
        operation: EmitterAttemptOperation,
    ) -> Self {
        let mut labels = values.to_vec();
        labels.push(operation.as_ref());
        Self {
            succeeded: AttemptCounter::register(
                store,
                values,
                operation,
                EmitterAttemptOutcome::Succeeded,
            ),
            transient: AttemptCounter::register(
                store,
                values,
                operation,
                EmitterAttemptOutcome::Transient,
            ),
            misconfigured: AttemptCounter::register(
                store,
                values,
                operation,
                EmitterAttemptOutcome::Misconfigured,
            ),
            violation: AttemptCounter::register(
                store,
                values,
                operation,
                EmitterAttemptOutcome::Violation,
            ),
            duration: store.durations.with_label_values(&labels),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "records one result through retained atomic children"
        )
    )]
    fn record(&self, outcome: EmitterAttemptOutcome, elapsed: Duration) {
        let counter = match outcome {
            EmitterAttemptOutcome::Succeeded => &self.succeeded,
            EmitterAttemptOutcome::Transient => &self.transient,
            EmitterAttemptOutcome::Misconfigured => &self.misconfigured,
            EmitterAttemptOutcome::Violation => &self.violation,
        };
        counter.record();
        self.duration.observe(elapsed.as_secs_f64());
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "summarizes retained counters and histogram samples"
        )
    )]
    fn describe_lines(
        &self,
        selector: &str,
        operation: EmitterAttemptOperation,
        lines: &mut Vec<String>,
    ) {
        for (outcome, counter) in [
            (EmitterAttemptOutcome::Succeeded, &self.succeeded),
            (EmitterAttemptOutcome::Transient, &self.transient),
            (EmitterAttemptOutcome::Misconfigured, &self.misconfigured),
            (EmitterAttemptOutcome::Violation, &self.violation),
        ] {
            let summary = counter.series.summary();
            lines.push(format!(
                "{PUBLISH_ATTEMPTS}{{{selector},operation=\"{}\",outcome=\"{}\"}} total={} \
                 wall_rate_per_sec={} domain_rate_per_sec=- wall_rate_ema_1m_per_sec={} \
                 wall_rate_ema_15m_per_sec={} domain_rate_ema_1m_per_sec=- \
                 domain_rate_ema_15m_per_sec=-",
                operation.as_ref(),
                outcome.as_ref(),
                summary.value,
                format_number(summary.wall_rate_per_sec),
                format_optional(summary.rolling.wall_1m_per_sec),
                format_optional(summary.rolling.wall_15m_per_sec),
            ));
        }
        lines.push(format!(
            "{PUBLISH_DURATION}{{{selector},operation=\"{}\"}} count={} sum_seconds={}",
            operation.as_ref(),
            self.duration.get_sample_count(),
            format_number(self.duration.get_sample_sum()),
        ));
    }
}
