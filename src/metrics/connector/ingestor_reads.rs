//! Layer: engines and infrastructure.
//! - **Owns.** Retained read counters, paced poll durations and exact-unit inspection history,
//!   which also retains each unit's acknowledgement and lifecycle children.
//! - **Depends on.** Vocabulary identities, the existing rate owner and Prometheus.
//! - **Must not know.** Drivers, graph admission, decoding or acknowledgement policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns metric infrastructure")
)]

use std::{collections::hash_map::Entry, time::Duration};

use arch_into::ArchInto as _;
use meticulous::OptionExt as _;
use nervix_connector::IngestorReadOutcome;
use nervix_models::{DomainNodeUnitRef, IngestorEntityKind, IngestorName, Unit};
use strum::IntoEnumIterator as _;

use super::{
    ingestor_acknowledgements::{AcknowledgementFamilies, IngestorAcknowledgementMetrics},
    *,
};

const READS: &str = "nervix_ingestor_reads_total";
const PAYLOADS: &str = "nervix_ingestor_read_payloads_total";
const BYTES: &str = "nervix_ingestor_read_bytes_total";
const POLL_DURATION: &str = "nervix_ingestor_poll_duration_seconds";
const LABELS: &[&str] = &[
    "domain",
    "ingestor",
    "physical_node_id",
    "instance",
    "connector",
    "entity",
];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReadSeriesKey {
    node: Option<ClusterNodeName>,
    kind: IngestorEntityKind,
}

#[derive(Debug)]
pub(super) struct IngestorReadFamilies {
    /// Registration, withdrawal and observers alone reach this map; read loops retain children.
    scopes: DashMap<DomainNodeUnitRef, HashMap<ReadSeriesKey, IngestorReadMetrics>>,
    reads: IntCounterVec,
    payloads: IntCounterVec,
    bytes: IntCounterVec,
    duration: HistogramVec,
    acknowledgements: AcknowledgementFamilies,
}

impl IngestorReadFamilies {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers node metric families once")
    )]
    pub(super) fn register(registry: &Registry) -> Self {
        let mut outcome_labels = LABELS.to_vec();
        outcome_labels.push("outcome");
        let reads = Self::counter(
            registry,
            READS,
            "Completed broker reads and scheduled polls by outcome.",
            &outcome_labels,
        );
        let payloads = Self::counter(
            registry,
            PAYLOADS,
            "Raw payloads returned before decoding, filtering or timestamp admission.",
            LABELS,
        );
        let bytes = Self::counter(
            registry,
            BYTES,
            "Raw payload bytes returned before decoding, filtering or timestamp admission.",
            LABELS,
        );
        let duration = HistogramVec::new(
            HistogramOpts::new(
                POLL_DURATION,
                "Elapsed wall seconds per scheduled poll, including interrupted polls.",
            )
            .buckets(DURATION_BUCKETS.to_vec()),
            LABELS,
        )
        .assured("fixed names and published duration buckets satisfy Prometheus");
        registry
            .register(Box::new(duration.clone()))
            .assured("the duration family is registered once");
        Self {
            scopes: DashMap::new(),
            reads,
            payloads,
            bytes,
            duration,
            acknowledgements: AcknowledgementFamilies::register(registry, LABELS),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers one distinct metric family")
    )]
    fn counter(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
        let counter = IntCounterVec::new(Opts::new(name, help), labels)
            .assured("fixed metric names and label names satisfy Prometheus");
        registry
            .register(Box::new(counter.clone()))
            .assured("each family is registered once");
        counter
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves children once for an exact ingestor unit"
        )
    )]
    fn ingestor(
        &self,
        entity: DomainNodeUnitRef,
        node: Option<&ClusterNodeName>,
        kind: IngestorEntityKind,
    ) -> IngestorReadMetrics {
        let key = ReadSeriesKey {
            node: node.cloned(),
            kind,
        };
        let mut scopes = self.scopes.entry(entity.clone()).or_default();
        match scopes.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let connector = kind.connector().to_string();
                let instance = crate::metrics::instance_label(entity.unit);
                let values = [
                    entity.entity.domain.as_str(),
                    entity.entity.identifier().as_str(),
                    physical_node_label(node),
                    instance.as_ref(),
                    connector.as_str(),
                    kind.entity_keyword(),
                ];
                let mut selector = String::new();
                for (name, value) in LABELS.iter().zip(values) {
                    if !selector.is_empty() {
                        selector.push(',');
                    }
                    let escaped = value
                        .replace('\\', "\\\\")
                        .replace('\n', "\\n")
                        .replace('"', "\\\"");
                    selector.push_str(&format!("{name}=\"{escaped}\""));
                }
                let metrics = IngestorReadMetrics {
                    series: Arc::new(ReadSeries {
                        selector,
                        delivered: ReadCounter::new(
                            &self.reads,
                            &values,
                            Some(IngestorReadOutcome::Delivered),
                        ),
                        empty: ReadCounter::new(
                            &self.reads,
                            &values,
                            Some(IngestorReadOutcome::Empty),
                        ),
                        failed: ReadCounter::new(
                            &self.reads,
                            &values,
                            Some(IngestorReadOutcome::Failed),
                        ),
                        interrupted: ReadCounter::new(
                            &self.reads,
                            &values,
                            Some(IngestorReadOutcome::Interrupted),
                        ),
                        payloads: ReadCounter::new(&self.payloads, &values, None),
                        bytes: ReadCounter::new(&self.bytes, &values, None),
                        duration: self.duration.with_label_values(&values),
                        acknowledgements: self.acknowledgements.unit(&values),
                    }),
                };
                entry.insert(metrics.clone());
                metrics
            }
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(observer, reason = "reads retained exact-unit metric history")
    )]
    fn describe(&self, entity: &DomainNodeUnitRef) -> Vec<String> {
        let Some(scopes) = self.scopes.get(entity) else {
            return Vec::new();
        };
        let mut metrics = scopes.values().cloned().collect::<Vec<_>>();
        drop(scopes);
        metrics.sort_by(|a, b| a.series.selector.cmp(&b.series.selector));
        let mut lines = Vec::new();
        for metric in metrics {
            lines.extend(metric.describe_lines());
        }
        lines
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "withdraws only a retired unit's children")
    )]
    pub(super) fn remove(
        &self,
        entity: &DomainNodeUnitRef,
        node: Option<&ClusterNodeName>,
    ) -> Vec<IngestorEntityKind> {
        let Some(mut scopes) = self.scopes.get_mut(entity) else {
            return Vec::new();
        };
        let keys = scopes
            .keys()
            .filter(|key| key.node.as_ref() == node)
            .cloned()
            .collect::<Vec<_>>();
        let mut kinds = Vec::new();
        for key in keys {
            let connector = key.kind.connector().to_string();
            let instance = crate::metrics::instance_label(entity.unit);
            let labels = [
                entity.entity.domain.as_str(),
                entity.entity.identifier().as_str(),
                physical_node_label(node),
                instance.as_ref(),
                connector.as_str(),
                key.kind.entity_keyword(),
            ];
            for outcome in IngestorReadOutcome::iter() {
                let mut values = labels.to_vec();
                values.push(outcome.as_ref());
                self.reads
                    .remove_label_values(&values)
                    .assured("registration installs every read outcome child");
            }
            self.payloads
                .remove_label_values(&labels)
                .assured("registration installs the payload counter");
            self.bytes
                .remove_label_values(&labels)
                .assured("registration installs the byte counter");
            self.duration
                .remove_label_values(&labels)
                .assured("registration installs the duration histogram");
            let removed = scopes
                .remove(&key)
                .verified("the key was read from this scope's own entries");
            self.acknowledgements
                .withdraw(&removed.series.acknowledgements, &labels);
            kinds.push(key.kind);
        }
        let empty = scopes.is_empty();
        drop(scopes);
        if empty {
            self.scopes.remove_if(entity, |_, scopes| scopes.is_empty());
        }
        kinds
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IngestorReadMetrics {
    /// Later task generations and inspection independently retain the process-local counters.
    series: Arc<ReadSeries>,
}

#[derive(Debug)]
struct ReadSeries {
    selector: String,
    delivered: ReadCounter,
    empty: ReadCounter,
    failed: ReadCounter,
    interrupted: ReadCounter,
    payloads: ReadCounter,
    bytes: ReadCounter,
    duration: Histogram,
    /// The same unit's acknowledgement and lifecycle children, which its hosts also retain.
    acknowledgements: IngestorAcknowledgementMetrics,
}

#[derive(Debug)]
struct ReadCounter {
    prometheus: IntCounter,
    rate: CounterSeries,
}

impl ReadCounter {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves one child at unit preparation")
    )]
    fn new(family: &IntCounterVec, labels: &[&str], outcome: Option<IngestorReadOutcome>) -> Self {
        let prometheus = match outcome {
            Some(outcome) => {
                let mut values = labels.to_vec();
                values.push(outcome.as_ref());
                family.with_label_values(&values)
            }
            None => family.with_label_values(labels),
        };
        Self {
            prometheus,
            rate: CounterSeries::default(),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "updates retained atomic children only")
    )]
    fn record(&self, count: u64) {
        self.prometheus.inc_by(count);
        self.rate.increment(count, None);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(observer, reason = "projects one counter and its rate summaries")
    )]
    fn describe(&self, name: &str, selector: &str) -> String {
        let summary = self.rate.summary();
        format!(
            "{name}{{{selector}}} total={} wall_rate_per_sec={} domain_rate_per_sec=- \
             wall_rate_ema_1m_per_sec={} wall_rate_ema_15m_per_sec={} \
             domain_rate_ema_1m_per_sec=- domain_rate_ema_15m_per_sec=-",
            summary.value,
            format_number(summary.wall_rate_per_sec),
            format_optional(summary.rolling.wall_1m_per_sec),
            format_optional(summary.rolling.wall_15m_per_sec)
        )
    }
}

impl IngestorReadMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "selects only a retained read counter")
    )]
    pub(crate) fn read(&self, outcome: IngestorReadOutcome) {
        let counter = match outcome {
            IngestorReadOutcome::Delivered => &self.series.delivered,
            IngestorReadOutcome::Empty => &self.series.empty,
            IngestorReadOutcome::Failed => &self.series.failed,
            IngestorReadOutcome::Interrupted => &self.series.interrupted,
        };
        counter.record(1);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "records each handed-over batch before decoding without registry access"
        )
    )]
    pub(crate) fn payloads(&self, payloads: usize, bytes: u64) {
        self.series.payloads.record(payloads.arch_into());
        self.series.bytes.record(bytes);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "records one completed or interrupted paced poll")
    )]
    pub(crate) fn poll(&self, outcome: IngestorReadOutcome, elapsed: Duration) {
        self.read(outcome);
        self.series.duration.observe(elapsed.as_secs_f64());
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(observer, reason = "formats only retained metric snapshots")
    )]
    fn describe_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for (outcome, counter) in [
            (IngestorReadOutcome::Delivered, &self.series.delivered),
            (IngestorReadOutcome::Empty, &self.series.empty),
            (IngestorReadOutcome::Failed, &self.series.failed),
            (IngestorReadOutcome::Interrupted, &self.series.interrupted),
        ] {
            lines.push(counter.describe(
                READS,
                &format!("{},outcome=\"{}\"", self.series.selector, outcome.as_ref()),
            ));
        }
        lines.push(
            self.series
                .payloads
                .describe(PAYLOADS, &self.series.selector),
        );
        lines.push(self.series.bytes.describe(BYTES, &self.series.selector));
        lines.push(format!(
            "{POLL_DURATION}{{{}}} count={} sum_seconds={}",
            self.series.selector,
            self.series.duration.get_sample_count(),
            format_number(self.series.duration.get_sample_sum())
        ));
        self.series
            .acknowledgements
            .describe_lines(&self.series.selector, &mut lines);
        lines
    }

    /// The acknowledgement and lifecycle children of the same unit, for its host to retain.
    pub(crate) fn acknowledgements(&self) -> IngestorAcknowledgementMetrics {
        self.series.acknowledgements.clone()
    }
}

impl RuntimeMetrics {
    /// Includes registered units with no edge counters, such as an intake prepared before start.
    pub(in crate::metrics) fn ingestor_read_units(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        node: Option<&ClusterNodeName>,
    ) -> Vec<Unit> {
        self.series
            .connector
            .reads
            .scopes
            .iter()
            .filter(|scope| {
                scope.key().entity.domain == *domain
                    && scope.key().entity.kind() == ModelKind::Ingestor
                    && scope.key().entity.identifier().as_str() == ingestor.as_str()
                    && scope.value().keys().any(|key| key.node.as_ref() == node)
            })
            .map(|scope| scope.key().unit)
            .collect()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "withdraws the retired unit's exact metric children"
        )
    )]
    pub(in crate::metrics) fn remove_ingestor_read_metrics(
        &self,
        entity: &DomainNodeUnitRef,
        node: Option<&ClusterNodeName>,
    ) -> Vec<IngestorEntityKind> {
        self.series.connector.reads.remove(entity, node)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "retains the exact ingestor's metric children")
    )]
    pub(crate) fn register_ingestor_reads(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        unit: Unit,
        node: Option<&ClusterNodeName>,
        kind: IngestorEntityKind,
    ) -> IngestorReadMetrics {
        let entity = DomainNodeUnitRef::new(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor),
            unit,
        );
        self.series.connector.reads.ingestor(entity, node, kind)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "reads only the requested unit's retained observation"
        )
    )]
    pub(crate) fn describe_ingestor_interactions(&self, entity: &DomainNodeUnitRef) -> Vec<String> {
        self.series.connector.reads.describe(entity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingestor_reads_preserve_totals_and_retire_only_the_selected_unit() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("read_metrics").expect("valid domain");
        let name = IngestorName::parse("input").expect("valid ingestor");
        let node = ClusterNodeName::parse("node-1").expect("valid node");
        let kind = IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);
        let units = [Unit::SINGLE, Unit::FIRST_INSTANCE];
        let handles = units
            .map(|unit| metrics.register_ingestor_reads(&domain, &name, unit, Some(&node), kind));
        handles[0].read(IngestorReadOutcome::Delivered);
        handles[0].payloads(3, 39);
        handles[1].poll(IngestorReadOutcome::Empty, Duration::from_millis(250));
        let restarted =
            metrics.register_ingestor_reads(&domain, &name, units[0], Some(&node), kind);
        assert_eq!(restarted.series.delivered.prometheus.get(), 1);
        assert_eq!(restarted.series.payloads.rate.summary().value, 3);
        assert_eq!(restarted.series.bytes.prometheus.get(), 39);
        assert_eq!(handles[1].series.duration.get_sample_count(), 1);
        assert_eq!(handles[1].series.duration.get_sample_sum(), 0.25);
        let entity = |unit| {
            DomainNodeUnitRef::new(
                DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, &name),
                unit,
            )
        };
        assert_eq!(
            metrics
                .describe_ingestor_interactions(&entity(units[0]))
                .len(),
            25,
            "seven read lines and eighteen acknowledgement and lifecycle lines"
        );
        metrics.remove_ingestor_unit(&domain, &name, units[0], Some(&node));
        assert!(
            metrics
                .describe_ingestor_interactions(&entity(units[0]))
                .is_empty()
        );
        handles[1].payloads(2, 20);
        assert_eq!(handles[1].series.payloads.prometheus.get(), 2);
        assert_eq!(
            metrics
                .describe_ingestor_interactions(&entity(units[1]))
                .len(),
            25
        );
        let recreated =
            metrics.register_ingestor_reads(&domain, &name, units[0], Some(&node), kind);
        assert_eq!(recreated.series.delivered.prometheus.get(), 0);
        metrics.remove_ingestor_metrics(&domain, &name, Some(&node));
        assert!(
            metrics
                .describe_ingestor_interactions(&entity(units[0]))
                .is_empty()
        );
        assert!(
            metrics
                .describe_ingestor_interactions(&entity(units[1]))
                .is_empty()
        );
    }

    #[test]
    fn every_ingestor_family_has_one_vocabulary_label_shape_and_zero_children() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("read_metrics").expect("valid domain");
        let name = IngestorName::parse("input").expect("valid ingestor");
        let node = ClusterNodeName::parse("node-1").expect("valid node");
        let kind = IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Request);
        metrics.register_ingestor_quiesce(&domain, &name, Unit::SINGLE, Some(&node), kind);
        metrics.register_ingestor_interactions(&domain, &name, Unit::SINGLE, Some(&node), kind);
        metrics.ingestor_read_ahead_dropped_payloads(
            &domain,
            &name,
            Unit::SINGLE,
            Some(&node),
            kind,
        );
        let families = metrics.series.prometheus.registry.gather();
        let mut count = 0;
        for family in families
            .iter()
            .filter(|family| family.name().starts_with("nervix_ingestor_"))
        {
            count += 1;
            for sample in family.get_metric() {
                let labels = sample
                    .get_label()
                    .iter()
                    .map(|pair| (pair.name(), pair.value()))
                    .collect::<std::collections::BTreeMap<_, _>>();
                assert_eq!(labels["connector"], "HTTP");
                assert_eq!(labels["entity"], "REQUEST");
                assert_eq!(labels["domain"], "read_metrics");
                assert_eq!(labels["ingestor"], "input");
                assert_eq!(labels["physical_node_id"], "node-1");
                assert!(labels.contains_key("instance"));
                let bounded_labels = match family.name() {
                    "nervix_ingestor_transport_acknowledgements_total" => 2,
                    READS
                    | "nervix_ingestor_ack_outcomes_total"
                    | "nervix_ingestor_transport_acknowledgement_duration_seconds"
                    | "nervix_ingestor_resumes_total"
                    | "nervix_ingestor_suspends_total" => 1,
                    _ => 0,
                };
                assert_eq!(labels.len(), 6 + bounded_labels, "{}", family.name());
                match family.get_field_type() {
                    prometheus::proto::MetricType::COUNTER => {
                        assert_eq!(sample.get_counter().value(), 0.0)
                    }
                    prometheus::proto::MetricType::GAUGE => {
                        assert_eq!(sample.get_gauge().value(), 0.0)
                    }
                    prometheus::proto::MetricType::HISTOGRAM => {
                        assert_eq!(sample.get_histogram().sample_count(), 0)
                    }
                    other => panic!("unexpected ingestor family type: {other:?}"),
                }
            }
        }
        assert_eq!(count, 21);
    }
}
