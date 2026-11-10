//! Layer: test harness.
//! - **Owns.** Zero registration, retained event totals, sampled rates and scope isolation.
//! - **Depends on.** Production metric registration and typed connector handles.
//! - **Must not know.** Brokers, publication policy or graph execution.

use meticulous::OptionExt as _;

use super::*;

fn redis() -> EmitterEntityKind {
    EmitterEntityKind::Redis(nervix_models::RedisEmitterEntity::PubSub)
}

fn emitter(
    metrics: &RuntimeMetrics,
    name: &str,
    node: &str,
    kind: EmitterEntityKind,
) -> EmitterInteractionMetrics {
    metrics.register_emitter_interactions(
        &DomainName::parse("event_metrics")
            .assured("the fixed fixture domain is a valid identifier"),
        &EmitterName::parse(name).assured("the fixed fixture emitter name is a valid identifier"),
        nervix_models::Unit::SINGLE,
        Some(
            &ClusterNodeName::parse(node)
                .assured("the fixed fixture node name is a valid identifier"),
        ),
        kind,
    )
}

const EVENT: ConnectorEvent = ConnectorEvent::PublishedWithoutSubscribers;

#[test]
fn emitter_events_register_at_zero_and_refuse_undeclared_identities() {
    let metrics = RuntimeMetrics::default();
    let declared = emitter(&metrics, "output", "node-1", redis());
    let undeclared = emitter(
        &metrics,
        "other",
        "node-1",
        EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic),
    );
    assert_eq!(
        undeclared
            .event_counter(EVENT)
            .expect_err("Kafka declares no events")
            .current_context()
            .event,
        EVENT
    );
    let families = metrics.series.prometheus.registry.gather();
    let family = families
        .iter()
        .find(|family| family.name() == EMITTER_EVENTS)
        .assured("registration installs the fixture event at zero");
    assert_eq!(family.get_metric().len(), 1);
    let sample = &family.get_metric()[0];
    assert_eq!(sample.get_counter().value(), 0.0);
    let labels: BTreeMap<_, _> = sample
        .get_label()
        .iter()
        .map(|label| (label.name(), label.value()))
        .collect();
    assert_eq!(labels.len(), 7);
    assert_eq!(labels["domain"], "event_metrics");
    assert_eq!(labels["emitter"], "output");
    assert_eq!(labels["physical_node_id"], "node-1");
    assert_eq!(labels["instance"], "-");
    assert_eq!(labels["connector"], redis().connector().to_string());
    assert_eq!(labels["entity"], redis().entity_keyword());
    assert_eq!(labels["event"], EVENT.to_string());
    let baseline = declared.describe_lines();
    assert!(
        baseline.iter().any(|line| {
            line.starts_with(EMITTER_EVENTS)
                && line.contains("total=0 wall_rate_per_sec=0")
                && line.contains("wall_rate_ema_1m_per_sec=- ")
                && line.contains("wall_rate_ema_15m_per_sec=- ")
        }),
        "baseline inspection: {baseline:#?}"
    );
    let sampled = declared.describe_lines();
    assert!(
        sampled.iter().any(|line| {
            line.starts_with(EMITTER_EVENTS)
                && line.contains("total=0 wall_rate_per_sec=0")
                && line.contains("wall_rate_ema_1m_per_sec=0 ")
                && line.contains("wall_rate_ema_15m_per_sec=0 ")
        }),
        "inspection with an elapsed interval: {sampled:#?}"
    );
}

#[test]
fn recording_keeps_rates_off_publication_and_repeated_inspection_cannot_recount() {
    let metrics = RuntimeMetrics::default();
    let handle = emitter(&metrics, "output", "node-1", redis());
    let counter = handle
        .event_counter(EVENT)
        .assured("the Redis fixture declares this event");
    counter.increment();
    counter.increment();
    let series = handle
        .series
        .events
        .series
        .as_ref()
        .assured("the Redis fixture declaration installs its event series");
    assert_eq!(series.counters[&EVENT].prometheus.get(), 2);
    assert_eq!(
        series.counters[&EVENT].rate.summary().value,
        0,
        "recording has not sampled a rate"
    );
    for _ in 0..3 {
        let lines = handle.describe_lines();
        let event = lines
            .iter()
            .find(|line| line.starts_with(EMITTER_EVENTS))
            .assured("the registered event has an inspection row");
        assert!(
            event.contains("event=\"published_without_subscribers\"} total=2 wall_rate_per_sec="),
            "{event}"
        );
        assert!(event.contains("domain_rate_per_sec=-"));
        assert!(event.contains("wall_rate_ema_1m_per_sec="));
        assert!(event.contains("wall_rate_ema_15m_per_sec="));
        assert_eq!(series.counters[&EVENT].rate.summary().value, 2);
    }
    assert_eq!(series.counters[&EVENT].prometheus.get(), 2);
}

#[test]
fn emitter_event_history_survives_task_restart_and_isolates_names_and_nodes() {
    let metrics = RuntimeMetrics::default();
    let first = emitter(&metrics, "output", "node-1", redis());
    let retained = first
        .event_counter(EVENT)
        .assured("the Redis fixture declares this event");
    drop(first);
    retained.increment();
    let restarted = emitter(&metrics, "output", "node-1", redis());
    restarted
        .event_counter(EVENT)
        .assured("the restarted fixture keeps its declared event scope")
        .increment();
    for other in [
        emitter(&metrics, "other", "node-1", redis()),
        emitter(&metrics, "output", "node-2", redis()),
    ] {
        assert_eq!(
            other
                .series
                .events
                .series
                .as_ref()
                .assured("the fixture declaration installs its event series")
                .counters[&EVENT]
                .prometheus
                .get(),
            0
        );
    }
    assert_eq!(
        restarted
            .series
            .events
            .series
            .as_ref()
            .assured("the registration retains its event series")
            .counters[&EVENT]
            .prometheus
            .get(),
        2
    );
}

#[test]
fn current_ingestor_declarations_create_no_event_children() {
    let metrics = RuntimeMetrics::default();
    let (handle, _) = metrics.register_ingestor_interactions(
        &DomainName::parse("event_metrics")
            .assured("the fixed fixture domain is a valid identifier"),
        &IngestorName::parse("input")
            .assured("the fixed fixture ingestor name is a valid identifier"),
        Unit::SINGLE,
        None,
        IngestorEntityKind::Redis(nervix_models::RedisIngestorEntity::PubSub),
    );
    assert!(handle.events.counter(EVENT).is_err());
    assert!(metrics.series.connector.events.ingestors.is_empty());
    assert!(
        !metrics
            .series
            .prometheus
            .registry
            .gather()
            .iter()
            .any(|family| family.name() == INGESTOR_EVENTS)
    );
}

#[test]
fn ingestor_event_engine_keeps_exact_unit_history_after_read_retirement() {
    let metrics = RuntimeMetrics::default();
    let domain = DomainName::parse("event_metrics")
        .assured("the fixed fixture domain is a valid identifier");
    let name = IngestorName::parse("input")
        .assured("the fixed fixture ingestor name is a valid identifier");
    let node = ClusterNodeName::parse("node-1")
        .assured("the fixed fixture node name is a valid identifier");
    let kind = IngestorEntityKind::Redis(nervix_models::RedisIngestorEntity::PubSub);
    let entity = |unit| {
        DomainNodeUnitRef::new(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, &name),
            unit,
        )
    };
    // The event engine takes the caller's typed declaration, independent of an entity's driver.
    let declarations = redis().capabilities().events;
    let register = |unit| {
        let reads = metrics.register_ingestor_reads(&domain, &name, unit, Some(&node), kind);
        metrics.series.connector.events.ingestor(
            entity(unit),
            Some(&node),
            kind,
            reads.selector(),
            declarations,
        )
    };
    let first = register(Unit::SINGLE);
    let sibling = register(Unit::FIRST_INSTANCE);
    first
        .counter(EVENT)
        .assured("the supplied fixture declaration includes this event")
        .increment();
    assert_eq!(
        metrics.remove_ingestor_read_metrics(&entity(Unit::SINGLE), Some(&node)),
        vec![kind]
    );
    let lines = metrics.describe_ingestor_interactions(&entity(Unit::SINGLE));
    assert_eq!(lines.len(), 1);
    assert!(
        lines
            .iter()
            .any(|line| line.contains(INGESTOR_EVENTS)
                && line.contains("total=1 wall_rate_per_sec="))
    );
    assert!(
        metrics
            .ingestor_read_units(&domain, &name, Some(&node))
            .contains(&Unit::SINGLE)
    );
    let restarted = register(Unit::SINGLE);
    restarted
        .counter(EVENT)
        .assured("the restarted unit retains its declared event scope")
        .increment();
    assert_eq!(
        first
            .series
            .as_ref()
            .assured("the fixture declaration installs its event series")
            .counters[&EVENT]
            .prometheus
            .get(),
        2
    );
    assert_eq!(
        sibling
            .series
            .as_ref()
            .assured("the sibling unit has its own declared event series")
            .counters[&EVENT]
            .prometheus
            .get(),
        0
    );
    let families = metrics.series.prometheus.registry.gather();
    let events = families
        .iter()
        .find(|family| family.name() == INGESTOR_EVENTS)
        .assured("registration installs the supplied event family");
    assert_eq!(events.get_metric().len(), 2);
    assert!(
        events
            .get_metric()
            .iter()
            .all(|sample| sample.get_label().len() == 7)
    );
}
