//! Layer: engines and infrastructure.
//! - **Owns.** Zero registration, retained event children, sampled rates and ingestor history.
//! - **Depends on.** Typed declarations, connector host handles and the existing rate owner.
//! - **Must not know.** Drivers, Models, graph execution or delivery decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns connector event metric infrastructure")
)]

use std::collections::BTreeMap;

use nervix_connector::{ConnectorEventCounter, UndeclaredConnectorEvent};
use nervix_models::{
    ConnectorEvent, DeclaredEvents, DomainNodeUnitRef, IngestorEntityKind, IngestorName, Unit,
};

use super::{ingestor_reads::IngestorSeriesKey, *};

pub(super) const EMITTER_EVENTS: &str = "nervix_emitter_connector_events_total";
const INGESTOR_EVENTS: &str = "nervix_ingestor_connector_events_total";

#[derive(Debug)]
pub(super) struct EventFamilies {
    emitter: IntCounterVec,
    ingestor: IntCounterVec,
    /// Registration and observers alone reach history; connectors keep native counter children.
    ingestors: DashMap<DomainNodeUnitRef, HashMap<IngestorSeriesKey, IngestorEventScope>>,
}

impl EventFamilies {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers the node's two event families once")
    )]
    pub(super) fn register(registry: &Registry) -> Self {
        Self {
            emitter: Self::family(registry, EMITTER_EVENTS, &EMITTER_LABELS),
            ingestor: Self::family(registry, INGESTOR_EVENTS, ingestor_reads::LABELS),
            ingestors: DashMap::new(),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers fixed family and label names")
    )]
    fn family(registry: &Registry, name: &str, labels: &[&str]) -> IntCounterVec {
        let mut labels = labels.to_vec();
        labels.push("event");
        let counter = IntCounterVec::new(
            Opts::new(
                name,
                "Declared transport observations, independent of delivery outcomes.",
            ),
            &labels,
        )
        .assured("fixed event family and label names satisfy Prometheus naming rules");
        registry
            .register(Box::new(counter.clone()))
            .assured("each distinct event family is registered once");
        counter
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves exactly the emitter's declared event children"
        )
    )]
    pub(super) fn emitter(&self, values: &[&str], declared: DeclaredEvents) -> EventMetrics {
        EventMetrics::register(&self.emitter, values, declared)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "retains one exact ingestor unit's process-local event history"
        )
    )]
    pub(super) fn ingestor(
        &self,
        entity: DomainNodeUnitRef,
        node: Option<&ClusterNodeName>,
        kind: IngestorEntityKind,
        selector: &str,
        declared: DeclaredEvents,
    ) -> EventMetrics {
        if declared.as_slice().is_empty() {
            return EventMetrics::default();
        }
        let key = IngestorSeriesKey {
            node: node.cloned(),
            kind,
        };
        let mut scopes = self.ingestors.entry(entity.clone()).or_default();
        match scopes.entry(key) {
            Entry::Occupied(entry) => entry.get().metrics.clone(),
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
                let metrics = EventMetrics::register(&self.ingestor, &values, declared);
                entry.insert(IngestorEventScope {
                    selector: selector.to_owned(),
                    metrics: metrics.clone(),
                });
                metrics
            }
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "clones exact-unit history before rendering outside the registry guard"
        )
    )]
    pub(super) fn describe_ingestor(&self, entity: &DomainNodeUnitRef) -> Vec<String> {
        let Some(scopes) = self.ingestors.get(entity) else {
            return Vec::new();
        };
        let mut retained = scopes.values().cloned().collect::<Vec<_>>();
        drop(scopes);
        retained.sort_by(|left, right| left.selector.cmp(&right.selector));
        let mut lines = Vec::new();
        for scope in retained {
            scope
                .metrics
                .describe_lines(INGESTOR_EVENTS, &scope.selector, &mut lines);
        }
        lines
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "includes event history after its read-family unit retires"
        )
    )]
    pub(super) fn units(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        node: Option<&ClusterNodeName>,
    ) -> Vec<Unit> {
        self.ingestors
            .iter()
            .filter(|scope| {
                scope.key().entity.domain == *domain
                    && scope.key().entity.identifier().as_str() == ingestor.as_str()
                    && scope.value().keys().any(|key| key.node.as_ref() == node)
            })
            .map(|scope| scope.key().unit)
            .collect()
    }
}

#[derive(Debug, Clone)]
struct IngestorEventScope {
    selector: String,
    metrics: EventMetrics,
}

/// Empty declarations allocate nothing. Histories share their rate owner; callbacks retain only
/// the native Prometheus child, whose increment is one atomic operation.
#[derive(Debug, Clone, Default)]
pub(crate) struct EventMetrics {
    series: Option<Arc<EventCounters>>,
}

#[derive(Debug)]
struct EventCounters {
    declared: DeclaredEvents,
    counters: BTreeMap<ConnectorEvent, EventCounter>,
}

#[derive(Debug)]
struct EventCounter {
    prometheus: IntCounter,
    rate: CounterSeries,
}

impl EventMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "zero-registers only the closed declared event set"
        )
    )]
    fn register(family: &IntCounterVec, values: &[&str], declared: DeclaredEvents) -> Self {
        if declared.as_slice().is_empty() {
            return Self::default();
        }
        let counters = declared
            .as_slice()
            .iter()
            .map(|event| {
                let mut values = values.to_vec();
                values.push(event.as_ref());
                (
                    *event,
                    EventCounter {
                        prometheus: family.with_label_values(&values),
                        rate: CounterSeries::default(),
                    },
                )
            })
            .collect();
        Self {
            series: Some(Arc::new(EventCounters { declared, counters })),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a connector keeps one pre-resolved native counter at construction"
        )
    )]
    pub(crate) fn counter(
        &self,
        event: ConnectorEvent,
    ) -> error_stack::Result<ConnectorEventCounter, UndeclaredConnectorEvent> {
        let Some(series) = &self.series else {
            return Err(UndeclaredConnectorEvent { event }.into());
        };
        let Some(counter) = series.counters.get(&event) else {
            return Err(UndeclaredConnectorEvent { event }.into());
        };
        let counter = counter.prometheus.clone();
        ConnectorEventCounter::new(series.declared, event, move || counter.inc())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "samples cumulative totals into wall rates outside event recording"
        )
    )]
    pub(super) fn describe_lines(&self, name: &str, selector: &str, lines: &mut Vec<String>) {
        let Some(series) = &self.series else {
            return;
        };
        for (event, counter) in &series.counters {
            counter.rate.observe_total(counter.prometheus.get());
            let summary = counter.rate.summary();
            lines.push(format!(
                "{name}{{{selector},event=\"{event}\"}} total={} wall_rate_per_sec={} \
                 domain_rate_per_sec=- wall_rate_ema_1m_per_sec={} wall_rate_ema_15m_per_sec={} \
                 domain_rate_ema_1m_per_sec=- domain_rate_ema_15m_per_sec=-",
                summary.value,
                format_number(summary.wall_rate_per_sec),
                format_optional(summary.rolling.wall_1m_per_sec),
                format_optional(summary.rolling.wall_15m_per_sec)
            ));
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests remain outside product execution")
)]
mod tests;
